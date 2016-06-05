using System;
using System.IO;
using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Com.Google.Common.Util.Concurrent;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Util;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.HA
{
	/// <summary>
	/// Thread which runs inside the NN when it's in Standby state,
	/// periodically waking up to take a checkpoint of the namespace.
	/// </summary>
	/// <remarks>
	/// Thread which runs inside the NN when it's in Standby state,
	/// periodically waking up to take a checkpoint of the namespace.
	/// When it takes a checkpoint, it saves it to its local
	/// storage and then uploads it to the remote NameNode.
	/// </remarks>
	public class StandbyCheckpointer
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.Server.Namenode.HA.StandbyCheckpointer
			));

		private const long PreventAfterCancelMs = 2 * 60 * 1000L;

		private readonly CheckpointConf checkpointConf;

		private readonly Configuration conf;

		private readonly FSNamesystem namesystem;

		private long lastCheckpointTime;

		private readonly StandbyCheckpointer.CheckpointerThread thread;

		private readonly ThreadFactory uploadThreadFactory;

		private Uri activeNNAddress;

		private Uri myNNAddress;

		private readonly object cancelLock = new object();

		private Canceler canceler;

		private static int canceledCount = 0;

		/// <exception cref="System.IO.IOException"/>
		public StandbyCheckpointer(Configuration conf, FSNamesystem ns)
		{
			// Keep track of how many checkpoints were canceled.
			// This is for use in tests.
			this.namesystem = ns;
			this.conf = conf;
			this.checkpointConf = new CheckpointConf(conf);
			this.thread = new StandbyCheckpointer.CheckpointerThread(this);
			this.uploadThreadFactory = new ThreadFactoryBuilder().SetDaemon(true).SetNameFormat
				("TransferFsImageUpload-%d").Build();
			SetNameNodeAddresses(conf);
		}

		/// <summary>
		/// Determine the address of the NN we are checkpointing
		/// as well as our own HTTP address from the configuration.
		/// </summary>
		/// <exception cref="System.IO.IOException"></exception>
		private void SetNameNodeAddresses(Configuration conf)
		{
			// Look up our own address.
			myNNAddress = GetHttpAddress(conf);
			// Look up the active node's address
			Configuration confForActive = HAUtil.GetConfForOtherNode(conf);
			activeNNAddress = GetHttpAddress(confForActive);
			// Sanity-check.
			Preconditions.CheckArgument(CheckAddress(activeNNAddress), "Bad address for active NN: %s"
				, activeNNAddress);
			Preconditions.CheckArgument(CheckAddress(myNNAddress), "Bad address for standby NN: %s"
				, myNNAddress);
		}

		/// <exception cref="System.IO.IOException"/>
		private Uri GetHttpAddress(Configuration conf)
		{
			string scheme = DFSUtil.GetHttpClientScheme(conf);
			string defaultHost = NameNode.GetServiceAddress(conf, true).GetHostName();
			URI addr = DFSUtil.GetInfoServerWithDefaultHost(defaultHost, conf, scheme);
			return addr.ToURL();
		}

		/// <summary>
		/// Ensure that the given address is valid and has a port
		/// specified.
		/// </summary>
		private static bool CheckAddress(Uri addr)
		{
			return addr.Port != 0;
		}

		public virtual void Start()
		{
			Log.Info("Starting standby checkpoint thread...\n" + "Checkpointing active NN at "
				 + activeNNAddress + "\n" + "Serving checkpoints at " + myNNAddress);
			thread.Start();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Stop()
		{
			CancelAndPreventCheckpoints("Stopping checkpointer");
			thread.SetShouldRun(false);
			thread.Interrupt();
			try
			{
				thread.Join();
			}
			catch (Exception e)
			{
				Log.Warn("Edit log tailer thread exited with an exception");
				throw new IOException(e);
			}
		}

		public virtual void TriggerRollbackCheckpoint()
		{
			thread.Interrupt();
		}

		/// <exception cref="System.Exception"/>
		/// <exception cref="System.IO.IOException"/>
		private void DoCheckpoint()
		{
			System.Diagnostics.Debug.Assert(canceler != null);
			long txid;
			NNStorage.NameNodeFile imageType;
			// Acquire cpLock to make sure no one is modifying the name system.
			// It does not need the full namesystem write lock, since the only thing
			// that modifies namesystem on standby node is edit log replaying.
			namesystem.CpLockInterruptibly();
			try
			{
				System.Diagnostics.Debug.Assert(namesystem.GetEditLog().IsOpenForRead(), "Standby Checkpointer should only attempt a checkpoint when "
					 + "NN is in standby mode, but the edit logs are in an unexpected state");
				FSImage img = namesystem.GetFSImage();
				long prevCheckpointTxId = img.GetStorage().GetMostRecentCheckpointTxId();
				long thisCheckpointTxId = img.GetLastAppliedOrWrittenTxId();
				System.Diagnostics.Debug.Assert(thisCheckpointTxId >= prevCheckpointTxId);
				if (thisCheckpointTxId == prevCheckpointTxId)
				{
					Log.Info("A checkpoint was triggered but the Standby Node has not " + "received any transactions since the last checkpoint at txid "
						 + thisCheckpointTxId + ". Skipping...");
					return;
				}
				if (namesystem.IsRollingUpgrade() && !namesystem.GetFSImage().HasRollbackFSImage(
					))
				{
					// if we will do rolling upgrade but have not created the rollback image
					// yet, name this checkpoint as fsimage_rollback
					imageType = NNStorage.NameNodeFile.ImageRollback;
				}
				else
				{
					imageType = NNStorage.NameNodeFile.Image;
				}
				img.SaveNamespace(namesystem, imageType, canceler);
				txid = img.GetStorage().GetMostRecentCheckpointTxId();
				System.Diagnostics.Debug.Assert(txid == thisCheckpointTxId, "expected to save checkpoint at txid="
					 + thisCheckpointTxId + " but instead saved at txid=" + txid);
				// Save the legacy OIV image, if the output dir is defined.
				string outputDir = checkpointConf.GetLegacyOivImageDir();
				if (outputDir != null && !outputDir.IsEmpty())
				{
					img.SaveLegacyOIVImage(namesystem, outputDir, canceler);
				}
			}
			finally
			{
				namesystem.CpUnlock();
			}
			// Upload the saved checkpoint back to the active
			// Do this in a separate thread to avoid blocking transition to active
			// See HDFS-4816
			ExecutorService executor = Executors.NewSingleThreadExecutor(uploadThreadFactory);
			Future<Void> upload = executor.Submit(new _Callable_204(this, imageType, txid));
			executor.Shutdown();
			try
			{
				upload.Get();
			}
			catch (Exception e)
			{
				// The background thread may be blocked waiting in the throttler, so
				// interrupt it.
				upload.Cancel(true);
				throw;
			}
			catch (ExecutionException e)
			{
				throw new IOException("Exception during image upload: " + e.Message, e.InnerException
					);
			}
		}

		private sealed class _Callable_204 : Callable<Void>
		{
			public _Callable_204(StandbyCheckpointer _enclosing, NNStorage.NameNodeFile imageType
				, long txid)
			{
				this._enclosing = _enclosing;
				this.imageType = imageType;
				this.txid = txid;
			}

			/// <exception cref="System.IO.IOException"/>
			public Void Call()
			{
				TransferFsImage.UploadImageFromStorage(this._enclosing.activeNNAddress, this._enclosing
					.conf, this._enclosing.namesystem.GetFSImage().GetStorage(), imageType, txid, this
					._enclosing.canceler);
				return null;
			}

			private readonly StandbyCheckpointer _enclosing;

			private readonly NNStorage.NameNodeFile imageType;

			private readonly long txid;
		}

		/// <summary>
		/// Cancel any checkpoint that's currently being made,
		/// and prevent any new checkpoints from starting for the next
		/// minute or so.
		/// </summary>
		/// <exception cref="Org.Apache.Hadoop.HA.ServiceFailedException"/>
		public virtual void CancelAndPreventCheckpoints(string msg)
		{
			lock (cancelLock)
			{
				// The checkpointer thread takes this lock and checks if checkpointing is
				// postponed. 
				thread.PreventCheckpointsFor(PreventAfterCancelMs);
				// Before beginning a checkpoint, the checkpointer thread
				// takes this lock, and creates a canceler object.
				// If the canceler is non-null, then a checkpoint is in
				// progress and we need to cancel it. If it's null, then
				// the operation has not started, meaning that the above
				// time-based prevention will take effect.
				if (canceler != null)
				{
					canceler.Cancel(msg);
				}
			}
		}

		[VisibleForTesting]
		internal static int GetCanceledCount()
		{
			return canceledCount;
		}

		private long CountUncheckpointedTxns()
		{
			FSImage img = namesystem.GetFSImage();
			return img.GetLastAppliedOrWrittenTxId() - img.GetStorage().GetMostRecentCheckpointTxId
				();
		}

		private class CheckpointerThread : Sharpen.Thread
		{
			private volatile bool shouldRun = true;

			private volatile long preventCheckpointsUntil = 0;

			private CheckpointerThread(StandbyCheckpointer _enclosing)
				: base("Standby State Checkpointer")
			{
				this._enclosing = _enclosing;
			}

			private void SetShouldRun(bool shouldRun)
			{
				this.shouldRun = shouldRun;
			}

			public override void Run()
			{
				// We have to make sure we're logged in as far as JAAS
				// is concerned, in order to use kerberized SSL properly.
				SecurityUtil.DoAsLoginUserOrFatal(new _PrivilegedAction_277(this));
			}

			private sealed class _PrivilegedAction_277 : PrivilegedAction<object>
			{
				public _PrivilegedAction_277(CheckpointerThread _enclosing)
				{
					this._enclosing = _enclosing;
				}

				public object Run()
				{
					this._enclosing.DoWork();
					return null;
				}

				private readonly CheckpointerThread _enclosing;
			}

			/// <summary>
			/// Prevent checkpoints from occurring for some time period
			/// in the future.
			/// </summary>
			/// <remarks>
			/// Prevent checkpoints from occurring for some time period
			/// in the future. This is used when preparing to enter active
			/// mode. We need to not only cancel any concurrent checkpoint,
			/// but also prevent any checkpoints from racing to start just
			/// after the cancel call.
			/// </remarks>
			/// <param name="delayMs">
			/// the number of MS for which checkpoints will be
			/// prevented
			/// </param>
			private void PreventCheckpointsFor(long delayMs)
			{
				this.preventCheckpointsUntil = Time.MonotonicNow() + delayMs;
			}

			private void DoWork()
			{
				long checkPeriod = 1000 * this._enclosing.checkpointConf.GetCheckPeriod();
				// Reset checkpoint time so that we don't always checkpoint
				// on startup.
				this._enclosing.lastCheckpointTime = Time.MonotonicNow();
				while (this.shouldRun)
				{
					bool needRollbackCheckpoint = this._enclosing.namesystem.IsNeedRollbackFsImage();
					if (!needRollbackCheckpoint)
					{
						try
						{
							Sharpen.Thread.Sleep(checkPeriod);
						}
						catch (Exception)
						{
						}
						if (!this.shouldRun)
						{
							break;
						}
					}
					try
					{
						// We may have lost our ticket since last checkpoint, log in again, just in case
						if (UserGroupInformation.IsSecurityEnabled())
						{
							UserGroupInformation.GetCurrentUser().CheckTGTAndReloginFromKeytab();
						}
						long now = Time.MonotonicNow();
						long uncheckpointed = this._enclosing.CountUncheckpointedTxns();
						long secsSinceLast = (now - this._enclosing.lastCheckpointTime) / 1000;
						bool needCheckpoint = needRollbackCheckpoint;
						if (needCheckpoint)
						{
							StandbyCheckpointer.Log.Info("Triggering a rollback fsimage for rolling upgrade."
								);
						}
						else
						{
							if (uncheckpointed >= this._enclosing.checkpointConf.GetTxnCount())
							{
								StandbyCheckpointer.Log.Info("Triggering checkpoint because there have been " + uncheckpointed
									 + " txns since the last checkpoint, which " + "exceeds the configured threshold "
									 + this._enclosing.checkpointConf.GetTxnCount());
								needCheckpoint = true;
							}
							else
							{
								if (secsSinceLast >= this._enclosing.checkpointConf.GetPeriod())
								{
									StandbyCheckpointer.Log.Info("Triggering checkpoint because it has been " + secsSinceLast
										 + " seconds since the last checkpoint, which " + "exceeds the configured interval "
										 + this._enclosing.checkpointConf.GetPeriod());
									needCheckpoint = true;
								}
							}
						}
						lock (this._enclosing.cancelLock)
						{
							if (now < this.preventCheckpointsUntil)
							{
								StandbyCheckpointer.Log.Info("But skipping this checkpoint since we are about to failover!"
									);
								StandbyCheckpointer.canceledCount++;
								continue;
							}
							System.Diagnostics.Debug.Assert(this._enclosing.canceler == null);
							this._enclosing.canceler = new Canceler();
						}
						if (needCheckpoint)
						{
							this._enclosing.DoCheckpoint();
							// reset needRollbackCheckpoint to false only when we finish a ckpt
							// for rollback image
							if (needRollbackCheckpoint && this._enclosing.namesystem.GetFSImage().HasRollbackFSImage
								())
							{
								this._enclosing.namesystem.SetCreatedRollbackImages(true);
								this._enclosing.namesystem.SetNeedRollbackFsImage(false);
							}
							this._enclosing.lastCheckpointTime = now;
						}
					}
					catch (SaveNamespaceCancelledException ce)
					{
						StandbyCheckpointer.Log.Info("Checkpoint was cancelled: " + ce.Message);
						StandbyCheckpointer.canceledCount++;
					}
					catch (Exception ie)
					{
						StandbyCheckpointer.Log.Info("Interrupted during checkpointing", ie);
						// Probably requested shutdown.
						continue;
					}
					catch (Exception t)
					{
						StandbyCheckpointer.Log.Error("Exception in doCheckpoint", t);
					}
					finally
					{
						lock (this._enclosing.cancelLock)
						{
							this._enclosing.canceler = null;
						}
					}
				}
			}

			private readonly StandbyCheckpointer _enclosing;
		}

		[VisibleForTesting]
		internal virtual Uri GetActiveNNAddress()
		{
			return activeNNAddress;
		}
	}
}
