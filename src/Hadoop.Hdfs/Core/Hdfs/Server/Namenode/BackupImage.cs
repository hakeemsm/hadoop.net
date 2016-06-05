using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>Extension of FSImage for the backup node.</summary>
	/// <remarks>
	/// Extension of FSImage for the backup node.
	/// This class handles the setup of the journaling
	/// spool on the backup namenode.
	/// </remarks>
	public class BackupImage : FSImage
	{
		/// <summary>Backup input stream for loading edits into memory</summary>
		private readonly EditLogBackupInputStream backupInputStream = new EditLogBackupInputStream
			("Data from remote NameNode");

		/// <summary>Current state of the BackupNode.</summary>
		/// <remarks>
		/// Current state of the BackupNode. The BackupNode's state
		/// transitions are as follows:
		/// Initial: DROP_UNTIL_NEXT_ROLL
		/// - Transitions to JOURNAL_ONLY the next time the log rolls
		/// - Transitions to IN_SYNC in convergeJournalSpool
		/// - Transitions back to JOURNAL_ONLY if the log rolls while
		/// stopApplyingOnNextRoll is true.
		/// </remarks>
		internal volatile BackupImage.BNState bnState;

		internal enum BNState
		{
			DropUntilNextRoll,
			JournalOnly,
			InSync
		}

		/// <summary>
		/// Flag to indicate that the next time the NN rolls, the BN
		/// should transition from to JOURNAL_ONLY state.
		/// </summary>
		/// <remarks>
		/// Flag to indicate that the next time the NN rolls, the BN
		/// should transition from to JOURNAL_ONLY state.
		/// <seealso>#freezeNamespaceAtNextRoll()</seealso>
		/// </remarks>
		private bool stopApplyingEditsOnNextRoll = false;

		private FSNamesystem namesystem;

		/// <summary>Construct a backup image.</summary>
		/// <param name="conf">Configuration</param>
		/// <exception cref="System.IO.IOException">if storage cannot be initialised.</exception>
		internal BackupImage(Configuration conf)
			: base(conf)
		{
			storage.SetDisablePreUpgradableLayoutCheck(true);
			bnState = BackupImage.BNState.DropUntilNextRoll;
		}

		internal virtual FSNamesystem GetNamesystem()
		{
			lock (this)
			{
				return namesystem;
			}
		}

		internal virtual void SetNamesystem(FSNamesystem fsn)
		{
			lock (this)
			{
				// Avoids overriding this.namesystem object
				if (namesystem == null)
				{
					this.namesystem = fsn;
				}
			}
		}

		/// <summary>
		/// Analyze backup storage directories for consistency.<br />
		/// Recover from incomplete checkpoints if required.<br />
		/// Read VERSION and fstime files if exist.<br />
		/// Do not load image or edits.
		/// </summary>
		/// <exception cref="System.IO.IOException">if the node should shutdown.</exception>
		internal virtual void RecoverCreateRead()
		{
			for (IEnumerator<Storage.StorageDirectory> it = storage.DirIterator(); it.HasNext
				(); )
			{
				Storage.StorageDirectory sd = it.Next();
				Storage.StorageState curState;
				try
				{
					curState = sd.AnalyzeStorage(HdfsServerConstants.StartupOption.Regular, storage);
					switch (curState)
					{
						case Storage.StorageState.NonExistent:
						{
							// sd is locked but not opened
							// fail if any of the configured storage dirs are inaccessible
							throw new InconsistentFSStateException(sd.GetRoot(), "checkpoint directory does not exist or is not accessible."
								);
						}

						case Storage.StorageState.NotFormatted:
						{
							// for backup node all directories may be unformatted initially
							Log.Info("Storage directory " + sd.GetRoot() + " is not formatted.");
							Log.Info("Formatting ...");
							sd.ClearDirectory();
							// create empty current
							break;
						}

						case Storage.StorageState.Normal:
						{
							break;
						}

						default:
						{
							// recovery is possible
							sd.DoRecover(curState);
							break;
						}
					}
					if (curState != Storage.StorageState.NotFormatted)
					{
						// read and verify consistency with other directories
						storage.ReadProperties(sd);
					}
				}
				catch (IOException ioe)
				{
					sd.Unlock();
					throw;
				}
			}
		}

		/// <summary>Receive a batch of edits from the NameNode.</summary>
		/// <remarks>
		/// Receive a batch of edits from the NameNode.
		/// Depending on bnState, different actions are taken. See
		/// <see cref="BNState"/>
		/// </remarks>
		/// <param name="firstTxId">first txid in batch</param>
		/// <param name="numTxns">number of transactions</param>
		/// <param name="data">serialized journal records.</param>
		/// <exception cref="System.IO.IOException"/>
		/// <seealso cref="ConvergeJournalSpool()"/>
		internal virtual void Journal(long firstTxId, int numTxns, byte[] data)
		{
			lock (this)
			{
				if (Log.IsTraceEnabled())
				{
					Log.Trace("Got journal, " + "state = " + bnState + "; firstTxId = " + firstTxId +
						 "; numTxns = " + numTxns);
				}
				switch (bnState)
				{
					case BackupImage.BNState.DropUntilNextRoll:
					{
						return;
					}

					case BackupImage.BNState.InSync:
					{
						// update NameSpace in memory
						ApplyEdits(firstTxId, numTxns, data);
						break;
					}

					case BackupImage.BNState.JournalOnly:
					{
						break;
					}

					default:
					{
						throw new Exception("Unhandled state: " + bnState);
					}
				}
				// write to BN's local edit log.
				LogEditsLocally(firstTxId, numTxns, data);
			}
		}

		/// <summary>Write the batch of edits to the local copy of the edit logs.</summary>
		private void LogEditsLocally(long firstTxId, int numTxns, byte[] data)
		{
			long expectedTxId = editLog.GetLastWrittenTxId() + 1;
			Preconditions.CheckState(firstTxId == expectedTxId, "received txid batch starting at %s but expected txn %s"
				, firstTxId, expectedTxId);
			editLog.SetNextTxId(firstTxId + numTxns - 1);
			editLog.LogEdit(data.Length, data);
			editLog.LogSync();
		}

		/// <summary>Apply the batch of edits to the local namespace.</summary>
		/// <exception cref="System.IO.IOException"/>
		private void ApplyEdits(long firstTxId, int numTxns, byte[] data)
		{
			lock (this)
			{
				Preconditions.CheckArgument(firstTxId == lastAppliedTxId + 1, "Received txn batch starting at %s but expected %s"
					, firstTxId, lastAppliedTxId + 1);
				System.Diagnostics.Debug.Assert(backupInputStream.Length() == 0, "backup input stream is not empty"
					);
				try
				{
					if (Log.IsTraceEnabled())
					{
						Log.Debug("data:" + StringUtils.ByteToHexString(data));
					}
					FSEditLogLoader logLoader = new FSEditLogLoader(GetNamesystem(), lastAppliedTxId);
					int logVersion = storage.GetLayoutVersion();
					backupInputStream.SetBytes(data, logVersion);
					long numTxnsAdvanced = logLoader.LoadEditRecords(backupInputStream, true, lastAppliedTxId
						 + 1, null, null);
					if (numTxnsAdvanced != numTxns)
					{
						throw new IOException("Batch of txns starting at txnid " + firstTxId + " was supposed to contain "
							 + numTxns + " transactions, but we were only able to advance by " + numTxnsAdvanced
							);
					}
					lastAppliedTxId = logLoader.GetLastAppliedTxId();
					FSImage.UpdateCountForQuota(GetNamesystem().dir.GetBlockStoragePolicySuite(), GetNamesystem
						().dir.rootDir);
				}
				finally
				{
					// inefficient!
					backupInputStream.Clear();
				}
			}
		}

		/// <summary>Transition the BackupNode from JOURNAL_ONLY state to IN_SYNC state.</summary>
		/// <remarks>
		/// Transition the BackupNode from JOURNAL_ONLY state to IN_SYNC state.
		/// This is done by repeated invocations of tryConvergeJournalSpool until
		/// we are caught up to the latest in-progress edits file.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void ConvergeJournalSpool()
		{
			Preconditions.CheckState(bnState == BackupImage.BNState.JournalOnly, "bad state: %s"
				, bnState);
			while (!TryConvergeJournalSpool())
			{
			}
			System.Diagnostics.Debug.Assert(bnState == BackupImage.BNState.InSync);
		}

		/// <exception cref="System.IO.IOException"/>
		private bool TryConvergeJournalSpool()
		{
			Preconditions.CheckState(bnState == BackupImage.BNState.JournalOnly, "bad state: %s"
				, bnState);
			// This section is unsynchronized so we can continue to apply
			// ahead of where we're reading, concurrently. Since the state
			// is JOURNAL_ONLY at this point, we know that lastAppliedTxId
			// doesn't change, and curSegmentTxId only increases
			while (lastAppliedTxId < editLog.GetCurSegmentTxId() - 1)
			{
				long target = editLog.GetCurSegmentTxId();
				Log.Info("Loading edits into backupnode to try to catch up from txid " + lastAppliedTxId
					 + " to " + target);
				FSImageTransactionalStorageInspector inspector = new FSImageTransactionalStorageInspector
					();
				storage.InspectStorageDirs(inspector);
				editLog.RecoverUnclosedStreams();
				IEnumerable<EditLogInputStream> editStreamsAll = editLog.SelectInputStreams(lastAppliedTxId
					, target - 1);
				// remove inprogress
				IList<EditLogInputStream> editStreams = Lists.NewArrayList();
				foreach (EditLogInputStream s in editStreamsAll)
				{
					if (s.GetFirstTxId() != editLog.GetCurSegmentTxId())
					{
						editStreams.AddItem(s);
					}
				}
				LoadEdits(editStreams, GetNamesystem());
			}
			// now, need to load the in-progress file
			lock (this)
			{
				if (lastAppliedTxId != editLog.GetCurSegmentTxId() - 1)
				{
					Log.Debug("Logs rolled while catching up to current segment");
					return false;
				}
				// drop lock and try again to load local logs
				EditLogInputStream stream = null;
				ICollection<EditLogInputStream> editStreams = GetEditLog().SelectInputStreams(GetEditLog
					().GetCurSegmentTxId(), GetEditLog().GetCurSegmentTxId());
				foreach (EditLogInputStream s in editStreams)
				{
					if (s.GetFirstTxId() == GetEditLog().GetCurSegmentTxId())
					{
						stream = s;
					}
					break;
				}
				if (stream == null)
				{
					Log.Warn("Unable to find stream starting with " + editLog.GetCurSegmentTxId() + ". This indicates that there is an error in synchronization in BackupImage"
						);
					return false;
				}
				try
				{
					long remainingTxns = GetEditLog().GetLastWrittenTxId() - lastAppliedTxId;
					Log.Info("Going to finish converging with remaining " + remainingTxns + " txns from in-progress stream "
						 + stream);
					FSEditLogLoader loader = new FSEditLogLoader(GetNamesystem(), lastAppliedTxId);
					loader.LoadFSEdits(stream, lastAppliedTxId + 1);
					lastAppliedTxId = loader.GetLastAppliedTxId();
					System.Diagnostics.Debug.Assert(lastAppliedTxId == GetEditLog().GetLastWrittenTxId
						());
				}
				finally
				{
					FSEditLog.CloseAllStreams(editStreams);
				}
				Log.Info("Successfully synced BackupNode with NameNode at txnid " + lastAppliedTxId
					);
				SetState(BackupImage.BNState.InSync);
			}
			return true;
		}

		/// <summary>Transition edit log to a new state, logging as necessary.</summary>
		private void SetState(BackupImage.BNState newState)
		{
			lock (this)
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug("State transition " + bnState + " -> " + newState);
				}
				bnState = newState;
			}
		}

		/// <summary>Receive a notification that the NameNode has begun a new edit log.</summary>
		/// <remarks>
		/// Receive a notification that the NameNode has begun a new edit log.
		/// This causes the BN to also start the new edit log in its local
		/// directories.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void NamenodeStartedLogSegment(long txid)
		{
			lock (this)
			{
				Log.Info("NameNode started a new log segment at txid " + txid);
				if (editLog.IsSegmentOpen())
				{
					if (editLog.GetLastWrittenTxId() == txid - 1)
					{
						// We are in sync with the NN, so end and finalize the current segment
						editLog.EndCurrentLogSegment(false);
					}
					else
					{
						// We appear to have missed some transactions -- the NN probably
						// lost contact with us temporarily. So, mark the current segment
						// as aborted.
						Log.Warn("NN started new log segment at txid " + txid + ", but BN had only written up to txid "
							 + editLog.GetLastWrittenTxId() + "in the log segment starting at " + editLog.GetCurSegmentTxId
							() + ". Aborting this " + "log segment.");
						editLog.AbortCurrentLogSegment();
					}
				}
				editLog.SetNextTxId(txid);
				editLog.StartLogSegment(txid, false);
				if (bnState == BackupImage.BNState.DropUntilNextRoll)
				{
					SetState(BackupImage.BNState.JournalOnly);
				}
				if (stopApplyingEditsOnNextRoll)
				{
					if (bnState == BackupImage.BNState.InSync)
					{
						Log.Info("Stopped applying edits to prepare for checkpoint.");
						SetState(BackupImage.BNState.JournalOnly);
					}
					stopApplyingEditsOnNextRoll = false;
					Sharpen.Runtime.NotifyAll(this);
				}
			}
		}

		/// <summary>
		/// Request that the next time the BN receives a log roll, it should
		/// stop applying the edits log to the local namespace.
		/// </summary>
		/// <remarks>
		/// Request that the next time the BN receives a log roll, it should
		/// stop applying the edits log to the local namespace. This is
		/// typically followed on by a call to
		/// <see cref="WaitUntilNamespaceFrozen()"/>
		/// </remarks>
		internal virtual void FreezeNamespaceAtNextRoll()
		{
			lock (this)
			{
				stopApplyingEditsOnNextRoll = true;
			}
		}

		/// <summary>
		/// After
		/// <see cref="FreezeNamespaceAtNextRoll()"/>
		/// has been called, wait until
		/// the BN receives notification of the next log roll.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void WaitUntilNamespaceFrozen()
		{
			lock (this)
			{
				if (bnState != BackupImage.BNState.InSync)
				{
					return;
				}
				Log.Info("Waiting until the NameNode rolls its edit logs in order " + "to freeze the BackupNode namespace."
					);
				while (bnState == BackupImage.BNState.InSync)
				{
					Preconditions.CheckState(stopApplyingEditsOnNextRoll, "If still in sync, we should still have the flag set to "
						 + "freeze at next roll");
					try
					{
						Sharpen.Runtime.Wait(this);
					}
					catch (Exception ie)
					{
						Log.Warn("Interrupted waiting for namespace to freeze", ie);
						throw new IOException(ie);
					}
				}
				Log.Info("BackupNode namespace frozen.");
			}
		}

		/// <summary>Override close() so that we don't finalize edit logs.</summary>
		/// <exception cref="System.IO.IOException"/>
		public override void Close()
		{
			lock (this)
			{
				editLog.AbortCurrentLogSegment();
				storage.Close();
			}
		}
	}
}
