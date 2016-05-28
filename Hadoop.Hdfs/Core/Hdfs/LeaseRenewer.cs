using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>
	/// <p>
	/// Used by
	/// <see cref="DFSClient"/>
	/// for renewing file-being-written leases
	/// on the namenode.
	/// When a file is opened for write (create or append),
	/// namenode stores a file lease for recording the identity of the writer.
	/// The writer (i.e. the DFSClient) is required to renew the lease periodically.
	/// When the lease is not renewed before it expires,
	/// the namenode considers the writer as failed and then it may either let
	/// another writer to obtain the lease or close the file.
	/// </p>
	/// <p>
	/// This class also provides the following functionality:
	/// <ul>
	/// <li>
	/// It maintains a map from (namenode, user) pairs to lease renewers.
	/// The same
	/// <see cref="LeaseRenewer"/>
	/// instance is used for renewing lease
	/// for all the
	/// <see cref="DFSClient"/>
	/// to the same namenode and the same user.
	/// </li>
	/// <li>
	/// Each renewer maintains a list of
	/// <see cref="DFSClient"/>
	/// .
	/// Periodically the leases for all the clients are renewed.
	/// A client is removed from the list when the client is closed.
	/// </li>
	/// <li>
	/// A thread per namenode per user is used by the
	/// <see cref="LeaseRenewer"/>
	/// to renew the leases.
	/// </li>
	/// </ul>
	/// </p>
	/// </summary>
	internal class LeaseRenewer
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.LeaseRenewer
			));

		internal const long LeaseRenewerGraceDefault = 60 * 1000L;

		internal const long LeaseRenewerSleepDefault = 1000L;

		/// <summary>
		/// Get a
		/// <see cref="LeaseRenewer"/>
		/// instance
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		internal static Org.Apache.Hadoop.Hdfs.LeaseRenewer GetInstance(string authority, 
			UserGroupInformation ugi, DFSClient dfsc)
		{
			Org.Apache.Hadoop.Hdfs.LeaseRenewer r = LeaseRenewer.Factory.Instance.Get(authority
				, ugi);
			r.AddClient(dfsc);
			return r;
		}

		/// <summary>
		/// A factory for sharing
		/// <see cref="LeaseRenewer"/>
		/// objects
		/// among
		/// <see cref="DFSClient"/>
		/// instances
		/// so that there is only one renewer per authority per user.
		/// </summary>
		private class Factory
		{
			private static readonly LeaseRenewer.Factory Instance = new LeaseRenewer.Factory(
				);

			private class Key
			{
				/// <summary>Namenode info</summary>
				internal readonly string authority;

				/// <summary>User info</summary>
				internal readonly UserGroupInformation ugi;

				private Key(string authority, UserGroupInformation ugi)
				{
					if (authority == null)
					{
						throw new HadoopIllegalArgumentException("authority == null");
					}
					else
					{
						if (ugi == null)
						{
							throw new HadoopIllegalArgumentException("ugi == null");
						}
					}
					this.authority = authority;
					this.ugi = ugi;
				}

				public override int GetHashCode()
				{
					return authority.GetHashCode() ^ ugi.GetHashCode();
				}

				public override bool Equals(object obj)
				{
					if (obj == this)
					{
						return true;
					}
					if (obj != null && obj is LeaseRenewer.Factory.Key)
					{
						LeaseRenewer.Factory.Key that = (LeaseRenewer.Factory.Key)obj;
						return this.authority.Equals(that.authority) && this.ugi.Equals(that.ugi);
					}
					return false;
				}

				public override string ToString()
				{
					return ugi.GetShortUserName() + "@" + authority;
				}
			}

			/// <summary>A map for per user per namenode renewers.</summary>
			private readonly IDictionary<LeaseRenewer.Factory.Key, LeaseRenewer> renewers = new 
				Dictionary<LeaseRenewer.Factory.Key, LeaseRenewer>();

			/// <summary>Get a renewer.</summary>
			private LeaseRenewer Get(string authority, UserGroupInformation ugi)
			{
				lock (this)
				{
					LeaseRenewer.Factory.Key k = new LeaseRenewer.Factory.Key(authority, ugi);
					LeaseRenewer r = renewers[k];
					if (r == null)
					{
						r = new LeaseRenewer(k);
						renewers[k] = r;
					}
					return r;
				}
			}

			/// <summary>Remove the given renewer.</summary>
			private void Remove(LeaseRenewer r)
			{
				lock (this)
				{
					LeaseRenewer stored = renewers[r.factorykey];
					//Since a renewer may expire, the stored renewer can be different.
					if (r == stored)
					{
						if (!r.ClientsRunning())
						{
							Sharpen.Collections.Remove(renewers, r.factorykey);
						}
					}
				}
			}
		}

		/// <summary>The time in milliseconds that the map became empty.</summary>
		private long emptyTime = long.MaxValue;

		/// <summary>A fixed lease renewal time period in milliseconds</summary>
		private long renewal = HdfsConstants.LeaseSoftlimitPeriod / 2;

		/// <summary>A daemon for renewing lease</summary>
		private Daemon daemon = null;

		/// <summary>Only the daemon with currentId should run.</summary>
		private int currentId = 0;

		/// <summary>
		/// A period in milliseconds that the lease renewer thread should run
		/// after the map became empty.
		/// </summary>
		/// <remarks>
		/// A period in milliseconds that the lease renewer thread should run
		/// after the map became empty.
		/// In other words,
		/// if the map is empty for a time period longer than the grace period,
		/// the renewer should terminate.
		/// </remarks>
		private long gracePeriod;

		/// <summary>
		/// The time period in milliseconds
		/// that the renewer sleeps for each iteration.
		/// </summary>
		private long sleepPeriod;

		private readonly LeaseRenewer.Factory.Key factorykey;

		/// <summary>A list of clients corresponding to this renewer.</summary>
		private readonly IList<DFSClient> dfsclients = new AList<DFSClient>();

		/// <summary>
		/// A stringified stack trace of the call stack when the Lease Renewer
		/// was instantiated.
		/// </summary>
		/// <remarks>
		/// A stringified stack trace of the call stack when the Lease Renewer
		/// was instantiated. This is only generated if trace-level logging is
		/// enabled on this class.
		/// </remarks>
		private readonly string instantiationTrace;

		private LeaseRenewer(LeaseRenewer.Factory.Key factorykey)
		{
			this.factorykey = factorykey;
			UnsyncSetGraceSleepPeriod(LeaseRenewerGraceDefault);
			if (Log.IsTraceEnabled())
			{
				instantiationTrace = StringUtils.StringifyException(new Exception("TRACE"));
			}
			else
			{
				instantiationTrace = null;
			}
		}

		/// <returns>the renewal time in milliseconds.</returns>
		private long GetRenewalTime()
		{
			lock (this)
			{
				return renewal;
			}
		}

		/// <summary>Used for testing only.</summary>
		[VisibleForTesting]
		public virtual void SetRenewalTime(long renewal)
		{
			lock (this)
			{
				this.renewal = renewal;
			}
		}

		/// <summary>Add a client.</summary>
		private void AddClient(DFSClient dfsc)
		{
			lock (this)
			{
				foreach (DFSClient c in dfsclients)
				{
					if (c == dfsc)
					{
						//client already exists, nothing to do.
						return;
					}
				}
				//client not found, add it
				dfsclients.AddItem(dfsc);
				//update renewal time
				if (dfsc.GetHdfsTimeout() > 0)
				{
					long half = dfsc.GetHdfsTimeout() / 2;
					if (half < renewal)
					{
						this.renewal = half;
					}
				}
			}
		}

		private bool ClientsRunning()
		{
			lock (this)
			{
				for (IEnumerator<DFSClient> i = dfsclients.GetEnumerator(); i.HasNext(); )
				{
					if (!i.Next().IsClientRunning())
					{
						i.Remove();
					}
				}
				return !dfsclients.IsEmpty();
			}
		}

		private long GetSleepPeriod()
		{
			lock (this)
			{
				return sleepPeriod;
			}
		}

		/// <summary>Set the grace period and adjust the sleep period accordingly.</summary>
		internal virtual void SetGraceSleepPeriod(long gracePeriod)
		{
			lock (this)
			{
				UnsyncSetGraceSleepPeriod(gracePeriod);
			}
		}

		private void UnsyncSetGraceSleepPeriod(long gracePeriod)
		{
			if (gracePeriod < 100L)
			{
				throw new HadoopIllegalArgumentException(gracePeriod + " = gracePeriod < 100ms is too small."
					);
			}
			this.gracePeriod = gracePeriod;
			long half = gracePeriod / 2;
			this.sleepPeriod = half < LeaseRenewerSleepDefault ? half : LeaseRenewerSleepDefault;
		}

		/// <summary>Is the daemon running?</summary>
		internal virtual bool IsRunning()
		{
			lock (this)
			{
				return daemon != null && daemon.IsAlive();
			}
		}

		/// <summary>Does this renewer have nothing to renew?</summary>
		public virtual bool IsEmpty()
		{
			return dfsclients.IsEmpty();
		}

		/// <summary>Used only by tests</summary>
		internal virtual string GetDaemonName()
		{
			lock (this)
			{
				return daemon.GetName();
			}
		}

		/// <summary>Is the empty period longer than the grace period?</summary>
		private bool IsRenewerExpired()
		{
			lock (this)
			{
				return emptyTime != long.MaxValue && Time.MonotonicNow() - emptyTime > gracePeriod;
			}
		}

		internal virtual void Put(long inodeId, DFSOutputStream @out, DFSClient dfsc)
		{
			lock (this)
			{
				if (dfsc.IsClientRunning())
				{
					if (!IsRunning() || IsRenewerExpired())
					{
						//start a new deamon with a new id.
						int id = ++currentId;
						daemon = new Daemon(new _Runnable_296(this, id));
						daemon.Start();
					}
					dfsc.PutFileBeingWritten(inodeId, @out);
					emptyTime = long.MaxValue;
				}
			}
		}

		private sealed class _Runnable_296 : Runnable
		{
			public _Runnable_296(LeaseRenewer _enclosing, int id)
			{
				this._enclosing = _enclosing;
				this.id = id;
			}

			public void Run()
			{
				try
				{
					if (LeaseRenewer.Log.IsDebugEnabled())
					{
						LeaseRenewer.Log.Debug("Lease renewer daemon for " + this._enclosing.ClientsString
							() + " with renew id " + id + " started");
					}
					this._enclosing.Run(id);
				}
				catch (Exception e)
				{
					if (LeaseRenewer.Log.IsDebugEnabled())
					{
						LeaseRenewer.Log.Debug(this._enclosing.GetType().Name + " is interrupted.", e);
					}
				}
				finally
				{
					lock (this._enclosing)
					{
						LeaseRenewer.Factory.Instance.Remove(this._enclosing);
					}
					if (LeaseRenewer.Log.IsDebugEnabled())
					{
						LeaseRenewer.Log.Debug("Lease renewer daemon for " + this._enclosing.ClientsString
							() + " with renew id " + id + " exited");
					}
				}
			}

			public override string ToString()
			{
				return this._enclosing.ToString();
			}

			private readonly LeaseRenewer _enclosing;

			private readonly int id;
		}

		[VisibleForTesting]
		internal virtual void SetEmptyTime(long time)
		{
			lock (this)
			{
				emptyTime = time;
			}
		}

		/// <summary>Close a file.</summary>
		internal virtual void CloseFile(long inodeId, DFSClient dfsc)
		{
			dfsc.RemoveFileBeingWritten(inodeId);
			lock (this)
			{
				if (dfsc.IsFilesBeingWrittenEmpty())
				{
					dfsclients.Remove(dfsc);
				}
				//update emptyTime if necessary
				if (emptyTime == long.MaxValue)
				{
					foreach (DFSClient c in dfsclients)
					{
						if (!c.IsFilesBeingWrittenEmpty())
						{
							//found a non-empty file-being-written map
							return;
						}
					}
					//discover the first time that all file-being-written maps are empty.
					emptyTime = Time.MonotonicNow();
				}
			}
		}

		/// <summary>Close the given client.</summary>
		internal virtual void CloseClient(DFSClient dfsc)
		{
			lock (this)
			{
				dfsclients.Remove(dfsc);
				if (dfsclients.IsEmpty())
				{
					if (!IsRunning() || IsRenewerExpired())
					{
						LeaseRenewer.Factory.Instance.Remove(this);
						return;
					}
					if (emptyTime == long.MaxValue)
					{
						//discover the first time that the client list is empty.
						emptyTime = Time.MonotonicNow();
					}
				}
				//update renewal time
				if (renewal == dfsc.GetHdfsTimeout() / 2)
				{
					long min = HdfsConstants.LeaseSoftlimitPeriod;
					foreach (DFSClient c in dfsclients)
					{
						if (c.GetHdfsTimeout() > 0)
						{
							long timeout = c.GetHdfsTimeout();
							if (timeout < min)
							{
								min = timeout;
							}
						}
					}
					renewal = min / 2;
				}
			}
		}

		/// <exception cref="System.Exception"/>
		internal virtual void InterruptAndJoin()
		{
			Daemon daemonCopy = null;
			lock (this)
			{
				if (IsRunning())
				{
					daemon.Interrupt();
					daemonCopy = daemon;
				}
			}
			if (daemonCopy != null)
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Wait for lease checker to terminate");
				}
				daemonCopy.Join();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void Renew()
		{
			IList<DFSClient> copies;
			lock (this)
			{
				copies = new AList<DFSClient>(dfsclients);
			}
			//sort the client names for finding out repeated names.
			copies.Sort(new _IComparer_412());
			string previousName = string.Empty;
			for (int i = 0; i < copies.Count; i++)
			{
				DFSClient c = copies[i];
				//skip if current client name is the same as the previous name.
				if (!c.GetClientName().Equals(previousName))
				{
					if (!c.RenewLease())
					{
						if (Log.IsDebugEnabled())
						{
							Log.Debug("Did not renew lease for client " + c);
						}
						continue;
					}
					previousName = c.GetClientName();
					if (Log.IsDebugEnabled())
					{
						Log.Debug("Lease renewed for client " + previousName);
					}
				}
			}
		}

		private sealed class _IComparer_412 : IComparer<DFSClient>
		{
			public _IComparer_412()
			{
			}

			public int Compare(DFSClient left, DFSClient right)
			{
				return string.CompareOrdinal(left.GetClientName(), right.GetClientName());
			}
		}

		/// <summary>
		/// Periodically check in with the namenode and renew all the leases
		/// when the lease period is half over.
		/// </summary>
		/// <exception cref="System.Exception"/>
		private void Run(int id)
		{
			for (long lastRenewed = Time.MonotonicNow(); !Sharpen.Thread.Interrupted(); Sharpen.Thread
				.Sleep(GetSleepPeriod()))
			{
				long elapsed = Time.MonotonicNow() - lastRenewed;
				if (elapsed >= GetRenewalTime())
				{
					try
					{
						Renew();
						if (Log.IsDebugEnabled())
						{
							Log.Debug("Lease renewer daemon for " + ClientsString() + " with renew id " + id 
								+ " executed");
						}
						lastRenewed = Time.MonotonicNow();
					}
					catch (SocketTimeoutException ie)
					{
						Log.Warn("Failed to renew lease for " + ClientsString() + " for " + (elapsed / 1000
							) + " seconds.  Aborting ...", ie);
						lock (this)
						{
							while (!dfsclients.IsEmpty())
							{
								DFSClient dfsClient = dfsclients[0];
								dfsClient.CloseAllFilesBeingWritten(true);
								CloseClient(dfsClient);
							}
							//Expire the current LeaseRenewer thread.
							emptyTime = 0;
						}
						break;
					}
					catch (IOException ie)
					{
						Log.Warn("Failed to renew lease for " + ClientsString() + " for " + (elapsed / 1000
							) + " seconds.  Will retry shortly ...", ie);
					}
				}
				lock (this)
				{
					if (id != currentId || IsRenewerExpired())
					{
						if (Log.IsDebugEnabled())
						{
							if (id != currentId)
							{
								Log.Debug("Lease renewer daemon for " + ClientsString() + " with renew id " + id 
									+ " is not current");
							}
							else
							{
								Log.Debug("Lease renewer daemon for " + ClientsString() + " with renew id " + id 
									+ " expired");
							}
						}
						//no longer the current daemon or expired
						return;
					}
					// if no clients are in running state or there is no more clients
					// registered with this renewer, stop the daemon after the grace
					// period.
					if (!ClientsRunning() && emptyTime == long.MaxValue)
					{
						emptyTime = Time.MonotonicNow();
					}
				}
			}
		}

		public override string ToString()
		{
			string s = GetType().Name + ":" + factorykey;
			if (Log.IsTraceEnabled())
			{
				return s + ", clients=" + ClientsString() + ", created at " + instantiationTrace;
			}
			return s;
		}

		/// <summary>Get the names of all clients</summary>
		private string ClientsString()
		{
			lock (this)
			{
				if (dfsclients.IsEmpty())
				{
					return "[]";
				}
				else
				{
					StringBuilder b = new StringBuilder("[").Append(dfsclients[0].GetClientName());
					for (int i = 1; i < dfsclients.Count; i++)
					{
						b.Append(", ").Append(dfsclients[i].GetClientName());
					}
					return b.Append("]").ToString();
				}
			}
		}
	}
}
