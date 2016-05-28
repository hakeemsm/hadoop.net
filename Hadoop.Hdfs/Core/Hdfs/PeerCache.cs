using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Hdfs.Net;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>A cache of input stream sockets to Data Node.</summary>
	public class PeerCache
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.PeerCache
			));

		private class Key
		{
			internal readonly DatanodeID dnID;

			internal readonly bool isDomain;

			internal Key(DatanodeID dnID, bool isDomain)
			{
				this.dnID = dnID;
				this.isDomain = isDomain;
			}

			public override bool Equals(object o)
			{
				if (!(o is PeerCache.Key))
				{
					return false;
				}
				PeerCache.Key other = (PeerCache.Key)o;
				return dnID.Equals(other.dnID) && isDomain == other.isDomain;
			}

			public override int GetHashCode()
			{
				return dnID.GetHashCode() ^ (isDomain ? 1 : 0);
			}
		}

		private class Value
		{
			private readonly Peer peer;

			private readonly long time;

			internal Value(Peer peer, long time)
			{
				this.peer = peer;
				this.time = time;
			}

			internal virtual Peer GetPeer()
			{
				return peer;
			}

			internal virtual long GetTime()
			{
				return time;
			}
		}

		private Daemon daemon;

		/// <summary>A map for per user per datanode.</summary>
		private readonly LinkedListMultimap<PeerCache.Key, PeerCache.Value> multimap = LinkedListMultimap
			.Create();

		private readonly int capacity;

		private readonly long expiryPeriod;

		public PeerCache(int c, long e)
		{
			this.capacity = c;
			this.expiryPeriod = e;
			if (capacity == 0)
			{
				Log.Info("SocketCache disabled.");
			}
			else
			{
				if (expiryPeriod == 0)
				{
					throw new InvalidOperationException("Cannot initialize expiryPeriod to " + expiryPeriod
						 + " when cache is enabled.");
				}
			}
		}

		private bool IsDaemonStarted()
		{
			return (daemon == null) ? false : true;
		}

		private void StartExpiryDaemon()
		{
			lock (this)
			{
				// start daemon only if not already started
				if (IsDaemonStarted() == true)
				{
					return;
				}
				daemon = new Daemon(new _Runnable_120(this));
				//noop
				daemon.Start();
			}
		}

		private sealed class _Runnable_120 : Runnable
		{
			public _Runnable_120(PeerCache _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public void Run()
			{
				try
				{
					this._enclosing.Run();
				}
				catch (Exception)
				{
				}
				finally
				{
					this._enclosing.Clear();
				}
			}

			public override string ToString()
			{
				return this._enclosing.ToString();
			}

			private readonly PeerCache _enclosing;
		}

		/// <summary>Get a cached peer connected to the given DataNode.</summary>
		/// <param name="dnId">The DataNode to get a Peer for.</param>
		/// <param name="isDomain">Whether to retrieve a DomainPeer or not.</param>
		/// <returns>
		/// An open Peer connected to the DN, or null if none
		/// was found.
		/// </returns>
		public virtual Peer Get(DatanodeID dnId, bool isDomain)
		{
			if (capacity <= 0)
			{
				// disabled
				return null;
			}
			return GetInternal(dnId, isDomain);
		}

		private Peer GetInternal(DatanodeID dnId, bool isDomain)
		{
			lock (this)
			{
				IList<PeerCache.Value> sockStreamList = multimap.Get(new PeerCache.Key(dnId, isDomain
					));
				if (sockStreamList == null)
				{
					return null;
				}
				IEnumerator<PeerCache.Value> iter = sockStreamList.GetEnumerator();
				while (iter.HasNext())
				{
					PeerCache.Value candidate = iter.Next();
					iter.Remove();
					long ageMs = Time.MonotonicNow() - candidate.GetTime();
					Peer peer = candidate.GetPeer();
					if (ageMs >= expiryPeriod)
					{
						try
						{
							peer.Close();
						}
						catch (IOException)
						{
							Log.Warn("got IOException closing stale peer " + peer + ", which is " + ageMs + " ms old"
								);
						}
					}
					else
					{
						if (!peer.IsClosed())
						{
							return peer;
						}
					}
				}
				return null;
			}
		}

		/// <summary>Give an unused socket to the cache.</summary>
		public virtual void Put(DatanodeID dnId, Peer peer)
		{
			Preconditions.CheckNotNull(dnId);
			Preconditions.CheckNotNull(peer);
			if (peer.IsClosed())
			{
				return;
			}
			if (capacity <= 0)
			{
				// Cache disabled.
				IOUtils.Cleanup(Log, peer);
				return;
			}
			PutInternal(dnId, peer);
		}

		private void PutInternal(DatanodeID dnId, Peer peer)
		{
			lock (this)
			{
				StartExpiryDaemon();
				if (capacity == multimap.Size())
				{
					EvictOldest();
				}
				multimap.Put(new PeerCache.Key(dnId, peer.GetDomainSocket() != null), new PeerCache.Value
					(peer, Time.MonotonicNow()));
			}
		}

		public virtual int Size()
		{
			lock (this)
			{
				return multimap.Size();
			}
		}

		/// <summary>Evict and close sockets older than expiry period from the cache.</summary>
		private void EvictExpired(long expiryPeriod)
		{
			lock (this)
			{
				while (multimap.Size() != 0)
				{
					IEnumerator<KeyValuePair<PeerCache.Key, PeerCache.Value>> iter = multimap.Entries
						().GetEnumerator();
					KeyValuePair<PeerCache.Key, PeerCache.Value> entry = iter.Next();
					// if oldest socket expired, remove it
					if (entry == null || Time.MonotonicNow() - entry.Value.GetTime() < expiryPeriod)
					{
						break;
					}
					IOUtils.Cleanup(Log, entry.Value.GetPeer());
					iter.Remove();
				}
			}
		}

		/// <summary>Evict the oldest entry in the cache.</summary>
		private void EvictOldest()
		{
			lock (this)
			{
				// We can get the oldest element immediately, because of an interesting
				// property of LinkedListMultimap: its iterator traverses entries in the
				// order that they were added.
				IEnumerator<KeyValuePair<PeerCache.Key, PeerCache.Value>> iter = multimap.Entries
					().GetEnumerator();
				if (!iter.HasNext())
				{
					throw new InvalidOperationException("Cannot evict from empty cache! " + "capacity: "
						 + capacity);
				}
				KeyValuePair<PeerCache.Key, PeerCache.Value> entry = iter.Next();
				IOUtils.Cleanup(Log, entry.Value.GetPeer());
				iter.Remove();
			}
		}

		/// <summary>
		/// Periodically check in the cache and expire the entries
		/// older than expiryPeriod minutes
		/// </summary>
		/// <exception cref="System.Exception"/>
		private void Run()
		{
			for (long lastExpiryTime = Time.MonotonicNow(); !Sharpen.Thread.Interrupted(); Sharpen.Thread
				.Sleep(expiryPeriod))
			{
				long elapsed = Time.MonotonicNow() - lastExpiryTime;
				if (elapsed >= expiryPeriod)
				{
					EvictExpired(expiryPeriod);
					lastExpiryTime = Time.MonotonicNow();
				}
			}
			Clear();
			throw new Exception("Daemon Interrupted");
		}

		/// <summary>Empty the cache, and close all sockets.</summary>
		[VisibleForTesting]
		internal virtual void Clear()
		{
			lock (this)
			{
				foreach (PeerCache.Value value in multimap.Values())
				{
					IOUtils.Cleanup(Log, value.GetPeer());
				}
				multimap.Clear();
			}
		}

		[VisibleForTesting]
		internal virtual void Close()
		{
			Clear();
			if (daemon != null)
			{
				daemon.Interrupt();
				try
				{
					daemon.Join();
				}
				catch (Exception)
				{
					throw new RuntimeException("failed to join thread");
				}
			}
			daemon = null;
		}
	}
}
