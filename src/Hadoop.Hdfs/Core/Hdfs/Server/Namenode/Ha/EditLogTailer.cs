using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.ProtocolPB;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.HA
{
	/// <summary>
	/// EditLogTailer represents a thread which periodically reads from edits
	/// journals and applies the transactions contained within to a given
	/// FSNamesystem.
	/// </summary>
	public class EditLogTailer
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.Server.Namenode.HA.EditLogTailer
			));

		private readonly EditLogTailer.EditLogTailerThread tailerThread;

		private readonly Configuration conf;

		private readonly FSNamesystem namesystem;

		private FSEditLog editLog;

		private IPEndPoint activeAddr;

		private NamenodeProtocol cachedActiveProxy = null;

		/// <summary>The last transaction ID at which an edit log roll was initiated.</summary>
		private long lastRollTriggerTxId = HdfsConstants.InvalidTxid;

		/// <summary>The highest transaction ID loaded by the Standby.</summary>
		private long lastLoadedTxnId = HdfsConstants.InvalidTxid;

		/// <summary>
		/// The last time we successfully loaded a non-zero number of edits from the
		/// shared directory.
		/// </summary>
		private long lastLoadTimeMs;

		/// <summary>How often the Standby should roll edit logs.</summary>
		/// <remarks>
		/// How often the Standby should roll edit logs. Since the Standby only reads
		/// from finalized log segments, the Standby will only be as up-to-date as how
		/// often the logs are rolled.
		/// </remarks>
		private readonly long logRollPeriodMs;

		/// <summary>
		/// How often the Standby should check if there are new finalized segment(s)
		/// available to be read from.
		/// </summary>
		private readonly long sleepTimeMs;

		public EditLogTailer(FSNamesystem namesystem, Configuration conf)
		{
			this.tailerThread = new EditLogTailer.EditLogTailerThread(this);
			this.conf = conf;
			this.namesystem = namesystem;
			this.editLog = namesystem.GetEditLog();
			lastLoadTimeMs = Time.MonotonicNow();
			logRollPeriodMs = conf.GetInt(DFSConfigKeys.DfsHaLogrollPeriodKey, DFSConfigKeys.
				DfsHaLogrollPeriodDefault) * 1000;
			if (logRollPeriodMs >= 0)
			{
				this.activeAddr = GetActiveNodeAddress();
				Preconditions.CheckArgument(activeAddr.Port > 0, "Active NameNode must have an IPC port configured. "
					 + "Got address '%s'", activeAddr);
				Log.Info("Will roll logs on active node at " + activeAddr + " every " + (logRollPeriodMs
					 / 1000) + " seconds.");
			}
			else
			{
				Log.Info("Not going to trigger log rolls on active node because " + DFSConfigKeys
					.DfsHaLogrollPeriodKey + " is negative.");
			}
			sleepTimeMs = conf.GetInt(DFSConfigKeys.DfsHaTaileditsPeriodKey, DFSConfigKeys.DfsHaTaileditsPeriodDefault
				) * 1000;
			Log.Debug("logRollPeriodMs=" + logRollPeriodMs + " sleepTime=" + sleepTimeMs);
		}

		private IPEndPoint GetActiveNodeAddress()
		{
			Configuration activeConf = HAUtil.GetConfForOtherNode(conf);
			return NameNode.GetServiceAddress(activeConf, true);
		}

		/// <exception cref="System.IO.IOException"/>
		private NamenodeProtocol GetActiveNodeProxy()
		{
			if (cachedActiveProxy == null)
			{
				int rpcTimeout = conf.GetInt(DFSConfigKeys.DfsHaLogrollRpcTimeoutKey, DFSConfigKeys
					.DfsHaLogrollRpcTimeoutDefault);
				NamenodeProtocolPB proxy = RPC.WaitForProxy<NamenodeProtocolPB>(RPC.GetProtocolVersion
					(typeof(NamenodeProtocolPB)), activeAddr, conf, rpcTimeout, long.MaxValue);
				cachedActiveProxy = new NamenodeProtocolTranslatorPB(proxy);
			}
			System.Diagnostics.Debug.Assert(cachedActiveProxy != null);
			return cachedActiveProxy;
		}

		public virtual void Start()
		{
			tailerThread.Start();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Stop()
		{
			tailerThread.SetShouldRun(false);
			tailerThread.Interrupt();
			try
			{
				tailerThread.Join();
			}
			catch (Exception e)
			{
				Log.Warn("Edit log tailer thread exited with an exception");
				throw new IOException(e);
			}
		}

		[VisibleForTesting]
		internal virtual FSEditLog GetEditLog()
		{
			return editLog;
		}

		[VisibleForTesting]
		public virtual void SetEditLog(FSEditLog editLog)
		{
			this.editLog = editLog;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void CatchupDuringFailover()
		{
			Preconditions.CheckState(tailerThread == null || !tailerThread.IsAlive(), "Tailer thread should not be running once failover starts"
				);
			// Important to do tailing as the login user, in case the shared
			// edits storage is implemented by a JournalManager that depends
			// on security credentials to access the logs (eg QuorumJournalManager).
			SecurityUtil.DoAsLoginUser(new _PrivilegedExceptionAction_182(this));
		}

		private sealed class _PrivilegedExceptionAction_182 : PrivilegedExceptionAction<Void
			>
		{
			public _PrivilegedExceptionAction_182(EditLogTailer _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.Exception"/>
			public Void Run()
			{
				try
				{
					// It is already under the full name system lock and the checkpointer
					// thread is already stopped. No need to acqure any other lock.
					this._enclosing.DoTailEdits();
				}
				catch (Exception e)
				{
					throw new IOException(e);
				}
				return null;
			}

			private readonly EditLogTailer _enclosing;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[VisibleForTesting]
		internal virtual void DoTailEdits()
		{
			// Write lock needs to be interruptible here because the 
			// transitionToActive RPC takes the write lock before calling
			// tailer.stop() -- so if we're not interruptible, it will
			// deadlock.
			namesystem.WriteLockInterruptibly();
			try
			{
				FSImage image = namesystem.GetFSImage();
				long lastTxnId = image.GetLastAppliedTxId();
				if (Log.IsDebugEnabled())
				{
					Log.Debug("lastTxnId: " + lastTxnId);
				}
				ICollection<EditLogInputStream> streams;
				try
				{
					streams = editLog.SelectInputStreams(lastTxnId + 1, 0, null, false);
				}
				catch (IOException ioe)
				{
					// This is acceptable. If we try to tail edits in the middle of an edits
					// log roll, i.e. the last one has been finalized but the new inprogress
					// edits file hasn't been started yet.
					Log.Warn("Edits tailer failed to find any streams. Will try again " + "later.", ioe
						);
					return;
				}
				if (Log.IsDebugEnabled())
				{
					Log.Debug("edit streams to load from: " + streams.Count);
				}
				// Once we have streams to load, errors encountered are legitimate cause
				// for concern, so we don't catch them here. Simple errors reading from
				// disk are ignored.
				long editsLoaded = 0;
				try
				{
					editsLoaded = image.LoadEdits(streams, namesystem);
				}
				catch (EditLogInputException elie)
				{
					editsLoaded = elie.GetNumEditsLoaded();
					throw;
				}
				finally
				{
					if (editsLoaded > 0 || Log.IsDebugEnabled())
					{
						Log.Info(string.Format("Loaded %d edits starting from txid %d ", editsLoaded, lastTxnId
							));
					}
				}
				if (editsLoaded > 0)
				{
					lastLoadTimeMs = Time.MonotonicNow();
				}
				lastLoadedTxnId = image.GetLastAppliedTxId();
			}
			finally
			{
				namesystem.WriteUnlock();
			}
		}

		/// <returns>time in msec of when we last loaded a non-zero number of edits.</returns>
		public virtual long GetLastLoadTimeMs()
		{
			return lastLoadTimeMs;
		}

		/// <returns>true if the configured log roll period has elapsed.</returns>
		private bool TooLongSinceLastLoad()
		{
			return logRollPeriodMs >= 0 && (Time.MonotonicNow() - lastLoadTimeMs) > logRollPeriodMs;
		}

		/// <summary>Trigger the active node to roll its logs.</summary>
		private void TriggerActiveLogRoll()
		{
			Log.Info("Triggering log roll on remote NameNode " + activeAddr);
			try
			{
				GetActiveNodeProxy().RollEditLog();
				lastRollTriggerTxId = lastLoadedTxnId;
			}
			catch (IOException ioe)
			{
				Log.Warn("Unable to trigger a roll of the active NN", ioe);
			}
		}

		/// <summary>
		/// The thread which does the actual work of tailing edits journals and
		/// applying the transactions to the FSNS.
		/// </summary>
		private class EditLogTailerThread : Sharpen.Thread
		{
			private volatile bool shouldRun = true;

			private EditLogTailerThread(EditLogTailer _enclosing)
				: base("Edit log tailer")
			{
				this._enclosing = _enclosing;
			}

			private void SetShouldRun(bool shouldRun)
			{
				this.shouldRun = shouldRun;
			}

			public override void Run()
			{
				SecurityUtil.DoAsLoginUserOrFatal(new _PrivilegedAction_298(this));
			}

			private sealed class _PrivilegedAction_298 : PrivilegedAction<object>
			{
				public _PrivilegedAction_298(EditLogTailerThread _enclosing)
				{
					this._enclosing = _enclosing;
				}

				public object Run()
				{
					this._enclosing.DoWork();
					return null;
				}

				private readonly EditLogTailerThread _enclosing;
			}

			private void DoWork()
			{
				while (this.shouldRun)
				{
					try
					{
						// There's no point in triggering a log roll if the Standby hasn't
						// read any more transactions since the last time a roll was
						// triggered. 
						if (this._enclosing.TooLongSinceLastLoad() && this._enclosing.lastRollTriggerTxId
							 < this._enclosing.lastLoadedTxnId)
						{
							this._enclosing.TriggerActiveLogRoll();
						}
						if (!this.shouldRun)
						{
							break;
						}
						// Prevent reading of name system while being modified. The full
						// name system lock will be acquired to further block even the block
						// state updates.
						this._enclosing.namesystem.CpLockInterruptibly();
						try
						{
							this._enclosing.DoTailEdits();
						}
						finally
						{
							this._enclosing.namesystem.CpUnlock();
						}
					}
					catch (EditLogInputException elie)
					{
						EditLogTailer.Log.Warn("Error while reading edits from disk. Will try again.", elie
							);
					}
					catch (Exception)
					{
						// interrupter should have already set shouldRun to false
						continue;
					}
					catch (Exception t)
					{
						EditLogTailer.Log.Fatal("Unknown error encountered while tailing edits. " + "Shutting down standby NN."
							, t);
						ExitUtil.Terminate(1, t);
					}
					try
					{
						Sharpen.Thread.Sleep(this._enclosing.sleepTimeMs);
					}
					catch (Exception e)
					{
						EditLogTailer.Log.Warn("Edit log tailer interrupted", e);
					}
				}
			}

			private readonly EditLogTailer _enclosing;
		}
	}
}
