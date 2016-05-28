using System;
using System.Collections.Generic;
using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Hdfs.Nfs.Conf;
using Org.Apache.Hadoop.Nfs.Nfs3;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Nfs.Nfs3
{
	/// <summary>A cache saves OpenFileCtx objects for different users.</summary>
	/// <remarks>
	/// A cache saves OpenFileCtx objects for different users. Each cache entry is
	/// used to maintain the writing context for a single file.
	/// </remarks>
	internal class OpenFileCtxCache
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.Nfs.Nfs3.OpenFileCtxCache
			));

		private readonly ConcurrentMap<FileHandle, OpenFileCtx> openFileMap = Maps.NewConcurrentMap
			();

		private readonly int maxStreams;

		private readonly long streamTimeout;

		private readonly OpenFileCtxCache.StreamMonitor streamMonitor;

		internal OpenFileCtxCache(NfsConfiguration config, long streamTimeout)
		{
			// Insert and delete with openFileMap are synced
			maxStreams = config.GetInt(NfsConfigKeys.DfsNfsMaxOpenFilesKey, NfsConfigKeys.DfsNfsMaxOpenFilesDefault
				);
			Log.Info("Maximum open streams is " + maxStreams);
			this.streamTimeout = streamTimeout;
			streamMonitor = new OpenFileCtxCache.StreamMonitor(this);
		}

		/// <summary>
		/// The entry to be evicted is based on the following rules:<br />
		/// 1.
		/// </summary>
		/// <remarks>
		/// The entry to be evicted is based on the following rules:<br />
		/// 1. if the OpenFileCtx has any pending task, it will not be chosen.<br />
		/// 2. if there is inactive OpenFileCtx, the first found one is to evict. <br />
		/// 3. For OpenFileCtx entries don't belong to group 1 or 2, the idlest one
		/// is select. If it's idle longer than OUTPUT_STREAM_TIMEOUT_MIN_DEFAULT, it
		/// will be evicted. Otherwise, the whole eviction request is failed.
		/// </remarks>
		[VisibleForTesting]
		internal virtual KeyValuePair<FileHandle, OpenFileCtx> GetEntryToEvict()
		{
			IEnumerator<KeyValuePair<FileHandle, OpenFileCtx>> it = openFileMap.GetEnumerator
				();
			if (Log.IsTraceEnabled())
			{
				Log.Trace("openFileMap size:" + openFileMap.Count);
			}
			KeyValuePair<FileHandle, OpenFileCtx> idlest = null;
			while (it.HasNext())
			{
				KeyValuePair<FileHandle, OpenFileCtx> pairs = it.Next();
				OpenFileCtx ctx = pairs.Value;
				if (!ctx.GetActiveState())
				{
					if (Log.IsDebugEnabled())
					{
						Log.Debug("Got one inactive stream: " + ctx);
					}
					return pairs;
				}
				if (ctx.HasPendingWork())
				{
					// Always skip files with pending work.
					continue;
				}
				if (idlest == null)
				{
					idlest = pairs;
				}
				else
				{
					if (ctx.GetLastAccessTime() < idlest.Value.GetLastAccessTime())
					{
						idlest = pairs;
					}
				}
			}
			if (idlest == null)
			{
				Log.Warn("No eviction candidate. All streams have pending work.");
				return null;
			}
			else
			{
				long idleTime = Time.MonotonicNow() - idlest.Value.GetLastAccessTime();
				if (idleTime < NfsConfigKeys.DfsNfsStreamTimeoutMinDefault)
				{
					if (Log.IsDebugEnabled())
					{
						Log.Debug("idlest stream's idle time:" + idleTime);
					}
					Log.Warn("All opened streams are busy, can't remove any from cache.");
					return null;
				}
				else
				{
					return idlest;
				}
			}
		}

		internal virtual bool Put(FileHandle h, OpenFileCtx context)
		{
			OpenFileCtx toEvict = null;
			lock (this)
			{
				Preconditions.CheckState(openFileMap.Count <= this.maxStreams, "stream cache size "
					 + openFileMap.Count + "  is larger than maximum" + this.maxStreams);
				if (openFileMap.Count == this.maxStreams)
				{
					KeyValuePair<FileHandle, OpenFileCtx> pairs = GetEntryToEvict();
					if (pairs == null)
					{
						return false;
					}
					else
					{
						if (Log.IsDebugEnabled())
						{
							Log.Debug("Evict stream ctx: " + pairs.Value);
						}
						toEvict = Sharpen.Collections.Remove(openFileMap, pairs.Key);
						Preconditions.CheckState(toEvict == pairs.Value, "The deleted entry is not the same as odlest found."
							);
					}
				}
				openFileMap[h] = context;
			}
			// Cleanup the old stream outside the lock
			if (toEvict != null)
			{
				toEvict.Cleanup();
			}
			return true;
		}

		[VisibleForTesting]
		internal virtual void Scan(long streamTimeout)
		{
			AList<OpenFileCtx> ctxToRemove = new AList<OpenFileCtx>();
			IEnumerator<KeyValuePair<FileHandle, OpenFileCtx>> it = openFileMap.GetEnumerator
				();
			if (Log.IsTraceEnabled())
			{
				Log.Trace("openFileMap size:" + openFileMap.Count);
			}
			while (it.HasNext())
			{
				KeyValuePair<FileHandle, OpenFileCtx> pairs = it.Next();
				FileHandle handle = pairs.Key;
				OpenFileCtx ctx = pairs.Value;
				if (!ctx.StreamCleanup(handle.GetFileId(), streamTimeout))
				{
					continue;
				}
				// Check it again inside lock before removing
				lock (this)
				{
					OpenFileCtx ctx2 = openFileMap[handle];
					if (ctx2 != null)
					{
						if (ctx2.StreamCleanup(handle.GetFileId(), streamTimeout))
						{
							Sharpen.Collections.Remove(openFileMap, handle);
							if (Log.IsDebugEnabled())
							{
								Log.Debug("After remove stream " + handle.GetFileId() + ", the stream number:" + 
									openFileMap.Count);
							}
							ctxToRemove.AddItem(ctx2);
						}
					}
				}
			}
			// Invoke the cleanup outside the lock
			foreach (OpenFileCtx ofc in ctxToRemove)
			{
				ofc.Cleanup();
			}
		}

		internal virtual OpenFileCtx Get(FileHandle key)
		{
			return openFileMap[key];
		}

		internal virtual int Size()
		{
			return openFileMap.Count;
		}

		internal virtual void Start()
		{
			streamMonitor.Start();
		}

		// Evict all entries
		internal virtual void CleanAll()
		{
			AList<OpenFileCtx> cleanedContext = new AList<OpenFileCtx>();
			lock (this)
			{
				IEnumerator<KeyValuePair<FileHandle, OpenFileCtx>> it = openFileMap.GetEnumerator
					();
				if (Log.IsTraceEnabled())
				{
					Log.Trace("openFileMap size:" + openFileMap.Count);
				}
				while (it.HasNext())
				{
					KeyValuePair<FileHandle, OpenFileCtx> pairs = it.Next();
					OpenFileCtx ctx = pairs.Value;
					it.Remove();
					cleanedContext.AddItem(ctx);
				}
			}
			// Invoke the cleanup outside the lock
			foreach (OpenFileCtx ofc in cleanedContext)
			{
				ofc.Cleanup();
			}
		}

		internal virtual void Shutdown()
		{
			// stop the dump thread
			if (streamMonitor.IsAlive())
			{
				streamMonitor.ShouldRun(false);
				streamMonitor.Interrupt();
				try
				{
					streamMonitor.Join(3000);
				}
				catch (Exception)
				{
				}
			}
			CleanAll();
		}

		/// <summary>StreamMonitor wakes up periodically to find and closes idle streams.</summary>
		internal class StreamMonitor : Daemon
		{
			private const int rotation = 5 * 1000;

			private long lastWakeupTime = 0;

			private bool shouldRun = true;

			// 5 seconds
			internal virtual void ShouldRun(bool shouldRun)
			{
				this.shouldRun = shouldRun;
			}

			public override void Run()
			{
				while (this.shouldRun)
				{
					this._enclosing.Scan(this._enclosing.streamTimeout);
					// Check if it can sleep
					try
					{
						long workedTime = Time.MonotonicNow() - this.lastWakeupTime;
						if (workedTime < OpenFileCtxCache.StreamMonitor.rotation)
						{
							if (OpenFileCtxCache.Log.IsTraceEnabled())
							{
								OpenFileCtxCache.Log.Trace("StreamMonitor can still have a sleep:" + ((OpenFileCtxCache.StreamMonitor
									.rotation - workedTime) / 1000));
							}
							Sharpen.Thread.Sleep(OpenFileCtxCache.StreamMonitor.rotation - workedTime);
						}
						this.lastWakeupTime = Time.MonotonicNow();
					}
					catch (Exception)
					{
						OpenFileCtxCache.Log.Info("StreamMonitor got interrupted");
						return;
					}
				}
			}

			internal StreamMonitor(OpenFileCtxCache _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly OpenFileCtxCache _enclosing;
		}
	}
}
