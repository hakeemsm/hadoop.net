using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Security.Token.Delegation;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Namenode.Metrics;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security.Token.Delegation;
using Org.Apache.Hadoop.Util;
using Sharpen;
using Sharpen.Reflect;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>FSEditLog maintains a log of the namespace modifications.</summary>
	public class FSEditLog : LogsPurgeable
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.Server.Namenode.FSEditLog
			));

		/// <summary>State machine for edit log.</summary>
		/// <remarks>
		/// State machine for edit log.
		/// In a non-HA setup:
		/// The log starts in UNITIALIZED state upon construction. Once it's
		/// initialized, it is usually in IN_SEGMENT state, indicating that edits may
		/// be written. In the middle of a roll, or while saving the namespace, it
		/// briefly enters the BETWEEN_LOG_SEGMENTS state, indicating that the previous
		/// segment has been closed, but the new one has not yet been opened.
		/// In an HA setup:
		/// The log starts in UNINITIALIZED state upon construction. Once it's
		/// initialized, it sits in the OPEN_FOR_READING state the entire time that the
		/// NN is in standby. Upon the NN transition to active, the log will be CLOSED,
		/// and then move to being BETWEEN_LOG_SEGMENTS, much as if the NN had just
		/// started up, and then will move to IN_SEGMENT so it can begin writing to the
		/// log. The log states will then revert to behaving as they do in a non-HA
		/// setup.
		/// </remarks>
		private enum State
		{
			Uninitialized,
			BetweenLogSegments,
			InSegment,
			OpenForReading,
			Closed
		}

		private FSEditLog.State state = FSEditLog.State.Uninitialized;

		private JournalSet journalSet = null;

		private EditLogOutputStream editLogStream = null;

		private long txid = 0;

		private long synctxid = 0;

		private long curSegmentTxId = HdfsConstants.InvalidTxid;

		private long lastPrintTime;

		private volatile bool isSyncRunning;

		private volatile bool isAutoSyncScheduled = false;

		private long numTransactions;

		private long numTransactionsBatchedInSync;

		private long totalTimeTransactions;

		private NameNodeMetrics metrics;

		private readonly NNStorage storage;

		private readonly Configuration conf;

		private readonly IList<URI> editsDirs;

		private sealed class _ThreadLocal_186 : ThreadLocal<FSEditLogOp.OpInstanceCache>
		{
			public _ThreadLocal_186()
			{
			}

			//initialize
			// a monotonically increasing counter that represents transactionIds.
			// stores the last synced transactionId.
			// the first txid of the log that's currently open for writing.
			// If this value is N, we are currently writing to edits_inprogress_N
			// the time of printing the statistics to the log file.
			// is a sync currently running?
			// is an automatic sync scheduled?
			// these are statistics counters.
			// number of transactions
			// total time for all transactions
			protected override FSEditLogOp.OpInstanceCache InitialValue()
			{
				return new FSEditLogOp.OpInstanceCache();
			}
		}

		private readonly ThreadLocal<FSEditLogOp.OpInstanceCache> cache = new _ThreadLocal_186
			();

		/// <summary>The edit directories that are shared between primary and secondary.</summary>
		private readonly IList<URI> sharedEditsDirs;

		/// <summary>Take this lock when adding journals to or closing the JournalSet.</summary>
		/// <remarks>
		/// Take this lock when adding journals to or closing the JournalSet. Allows
		/// us to ensure that the JournalSet isn't closed or updated underneath us
		/// in selectInputStreams().
		/// </remarks>
		private readonly object journalSetLock = new object();

		private class TransactionId
		{
			public long txid;

			internal TransactionId(long value)
			{
				this.txid = value;
			}
		}

		private sealed class _ThreadLocal_214 : ThreadLocal<FSEditLog.TransactionId>
		{
			public _ThreadLocal_214()
			{
			}

			// stores the most current transactionId of this thread.
			protected override FSEditLog.TransactionId InitialValue()
			{
				lock (this)
				{
					return new FSEditLog.TransactionId(long.MaxValue);
				}
			}
		}

		private static readonly ThreadLocal<FSEditLog.TransactionId> myTransactionId = new 
			_ThreadLocal_214();

		/// <summary>Constructor for FSEditLog.</summary>
		/// <remarks>
		/// Constructor for FSEditLog. Underlying journals are constructed, but
		/// no streams are opened until open() is called.
		/// </remarks>
		/// <param name="conf">The namenode configuration</param>
		/// <param name="storage">Storage object used by namenode</param>
		/// <param name="editsDirs">List of journals to use</param>
		internal FSEditLog(Configuration conf, NNStorage storage, IList<URI> editsDirs)
		{
			isSyncRunning = false;
			this.conf = conf;
			this.storage = storage;
			metrics = NameNode.GetNameNodeMetrics();
			lastPrintTime = Time.MonotonicNow();
			// If this list is empty, an error will be thrown on first use
			// of the editlog, as no journals will exist
			this.editsDirs = Lists.NewArrayList(editsDirs);
			this.sharedEditsDirs = FSNamesystem.GetSharedEditsDirs(conf);
		}

		public virtual void InitJournalsForWrite()
		{
			lock (this)
			{
				Preconditions.CheckState(state == FSEditLog.State.Uninitialized || state == FSEditLog.State
					.Closed, "Unexpected state: %s", state);
				InitJournals(this.editsDirs);
				state = FSEditLog.State.BetweenLogSegments;
			}
		}

		public virtual void InitSharedJournalsForRead()
		{
			lock (this)
			{
				if (state == FSEditLog.State.OpenForReading)
				{
					Log.Warn("Initializing shared journals for READ, already open for READ", new Exception
						());
					return;
				}
				Preconditions.CheckState(state == FSEditLog.State.Uninitialized || state == FSEditLog.State
					.Closed);
				InitJournals(this.sharedEditsDirs);
				state = FSEditLog.State.OpenForReading;
			}
		}

		private void InitJournals(IList<URI> dirs)
		{
			lock (this)
			{
				int minimumRedundantJournals = conf.GetInt(DFSConfigKeys.DfsNamenodeEditsDirMinimumKey
					, DFSConfigKeys.DfsNamenodeEditsDirMinimumDefault);
				lock (journalSetLock)
				{
					journalSet = new JournalSet(minimumRedundantJournals);
					foreach (URI u in dirs)
					{
						bool required = FSNamesystem.GetRequiredNamespaceEditsDirs(conf).Contains(u);
						if (u.GetScheme().Equals(NNStorage.LocalUriScheme))
						{
							Storage.StorageDirectory sd = storage.GetStorageDirectory(u);
							if (sd != null)
							{
								journalSet.Add(new FileJournalManager(conf, sd, storage), required, sharedEditsDirs
									.Contains(u));
							}
						}
						else
						{
							journalSet.Add(CreateJournal(u), required, sharedEditsDirs.Contains(u));
						}
					}
				}
				if (journalSet.IsEmpty())
				{
					Log.Error("No edits directories configured!");
				}
			}
		}

		/// <summary>Get the list of URIs the editlog is using for storage</summary>
		/// <returns>collection of URIs in use by the edit log</returns>
		internal virtual ICollection<URI> GetEditURIs()
		{
			return editsDirs;
		}

		/// <summary>
		/// Initialize the output stream for logging, opening the first
		/// log segment.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void OpenForWrite()
		{
			lock (this)
			{
				Preconditions.CheckState(state == FSEditLog.State.BetweenLogSegments, "Bad state: %s"
					, state);
				long segmentTxId = GetLastWrittenTxId() + 1;
				// Safety check: we should never start a segment if there are
				// newer txids readable.
				IList<EditLogInputStream> streams = new AList<EditLogInputStream>();
				journalSet.SelectInputStreams(streams, segmentTxId, true);
				if (!streams.IsEmpty())
				{
					string error = string.Format("Cannot start writing at txid %s " + "when there is a stream available for read: %s"
						, segmentTxId, streams[0]);
					IOUtils.Cleanup(Log, Sharpen.Collections.ToArray(streams, new EditLogInputStream[
						0]));
					throw new InvalidOperationException(error);
				}
				StartLogSegment(segmentTxId, true);
				System.Diagnostics.Debug.Assert(state == FSEditLog.State.InSegment, "Bad state: "
					 + state);
			}
		}

		/// <returns>
		/// true if the log is currently open in write mode, regardless
		/// of whether it actually has an open segment.
		/// </returns>
		internal virtual bool IsOpenForWrite()
		{
			lock (this)
			{
				return state == FSEditLog.State.InSegment || state == FSEditLog.State.BetweenLogSegments;
			}
		}

		/// <returns>
		/// true if the log is open in write mode and has a segment open
		/// ready to take edits.
		/// </returns>
		internal virtual bool IsSegmentOpen()
		{
			lock (this)
			{
				return state == FSEditLog.State.InSegment;
			}
		}

		/// <returns>true if the log is open in read mode.</returns>
		public virtual bool IsOpenForRead()
		{
			lock (this)
			{
				return state == FSEditLog.State.OpenForReading;
			}
		}

		/// <summary>Shutdown the file store.</summary>
		internal virtual void Close()
		{
			lock (this)
			{
				if (state == FSEditLog.State.Closed)
				{
					Log.Debug("Closing log when already closed");
					return;
				}
				try
				{
					if (state == FSEditLog.State.InSegment)
					{
						System.Diagnostics.Debug.Assert(editLogStream != null);
						WaitForSyncToFinish();
						EndCurrentLogSegment(true);
					}
				}
				finally
				{
					if (journalSet != null && !journalSet.IsEmpty())
					{
						try
						{
							lock (journalSetLock)
							{
								journalSet.Close();
							}
						}
						catch (IOException ioe)
						{
							Log.Warn("Error closing journalSet", ioe);
						}
					}
					state = FSEditLog.State.Closed;
				}
			}
		}

		/// <summary>Format all configured journals which are not file-based.</summary>
		/// <remarks>
		/// Format all configured journals which are not file-based.
		/// File-based journals are skipped, since they are formatted by the
		/// Storage format code.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void FormatNonFileJournals(NamespaceInfo nsInfo)
		{
			lock (this)
			{
				Preconditions.CheckState(state == FSEditLog.State.BetweenLogSegments, "Bad state: %s"
					, state);
				foreach (JournalManager jm in journalSet.GetJournalManagers())
				{
					if (!(jm is FileJournalManager))
					{
						jm.Format(nsInfo);
					}
				}
			}
		}

		internal virtual IList<Storage.FormatConfirmable> GetFormatConfirmables()
		{
			lock (this)
			{
				Preconditions.CheckState(state == FSEditLog.State.BetweenLogSegments, "Bad state: %s"
					, state);
				IList<Storage.FormatConfirmable> ret = Lists.NewArrayList();
				foreach (JournalManager jm in journalSet.GetJournalManagers())
				{
					// The FJMs are confirmed separately since they are also
					// StorageDirectories
					if (!(jm is FileJournalManager))
					{
						ret.AddItem(jm);
					}
				}
				return ret;
			}
		}

		/// <summary>Write an operation to the edit log.</summary>
		/// <remarks>
		/// Write an operation to the edit log. Do not sync to persistent
		/// store yet.
		/// </remarks>
		internal virtual void LogEdit(FSEditLogOp op)
		{
			lock (this)
			{
				System.Diagnostics.Debug.Assert(IsOpenForWrite(), "bad state: " + state);
				// wait if an automatic sync is scheduled
				WaitIfAutoSyncScheduled();
				long start = BeginTransaction();
				op.SetTransactionId(txid);
				try
				{
					editLogStream.Write(op);
				}
				catch (IOException)
				{
				}
				finally
				{
					// All journals failed, it is handled in logSync.
					op.Reset();
				}
				EndTransaction(start);
				// check if it is time to schedule an automatic sync
				if (!ShouldForceSync())
				{
					return;
				}
				isAutoSyncScheduled = true;
			}
			// sync buffered edit log entries to persistent store
			LogSync();
		}

		/// <summary>Wait if an automatic sync is scheduled</summary>
		internal virtual void WaitIfAutoSyncScheduled()
		{
			lock (this)
			{
				try
				{
					while (isAutoSyncScheduled)
					{
						Sharpen.Runtime.Wait(this, 1000);
					}
				}
				catch (Exception)
				{
				}
			}
		}

		/// <summary>Signal that an automatic sync scheduling is done if it is scheduled</summary>
		internal virtual void DoneWithAutoSyncScheduling()
		{
			lock (this)
			{
				if (isAutoSyncScheduled)
				{
					isAutoSyncScheduled = false;
					Sharpen.Runtime.NotifyAll(this);
				}
			}
		}

		/// <summary>
		/// Check if should automatically sync buffered edits to
		/// persistent store
		/// </summary>
		/// <returns>true if any of the edit stream says that it should sync</returns>
		private bool ShouldForceSync()
		{
			return editLogStream.ShouldForceSync();
		}

		private long BeginTransaction()
		{
			System.Diagnostics.Debug.Assert(Sharpen.Thread.HoldsLock(this));
			// get a new transactionId
			txid++;
			//
			// record the transactionId when new data was written to the edits log
			//
			FSEditLog.TransactionId id = myTransactionId.Get();
			id.txid = txid;
			return Time.MonotonicNow();
		}

		private void EndTransaction(long start)
		{
			System.Diagnostics.Debug.Assert(Sharpen.Thread.HoldsLock(this));
			// update statistics
			long end = Time.MonotonicNow();
			numTransactions++;
			totalTimeTransactions += (end - start);
			if (metrics != null)
			{
				// Metrics is non-null only when used inside name node
				metrics.AddTransaction(end - start);
			}
		}

		/// <summary>Return the transaction ID of the last transaction written to the log.</summary>
		public virtual long GetLastWrittenTxId()
		{
			lock (this)
			{
				return txid;
			}
		}

		/// <returns>the first transaction ID in the current log segment</returns>
		internal virtual long GetCurSegmentTxId()
		{
			lock (this)
			{
				Preconditions.CheckState(IsSegmentOpen(), "Bad state: %s", state);
				return curSegmentTxId;
			}
		}

		/// <summary>Set the transaction ID to use for the next transaction written.</summary>
		internal virtual void SetNextTxId(long nextTxId)
		{
			lock (this)
			{
				Preconditions.CheckArgument(synctxid <= txid && nextTxId >= txid, "May not decrease txid."
					 + " synctxid=%s txid=%s nextTxId=%s", synctxid, txid, nextTxId);
				txid = nextTxId - 1;
			}
		}

		/// <summary>Blocks until all ongoing edits have been synced to disk.</summary>
		/// <remarks>
		/// Blocks until all ongoing edits have been synced to disk.
		/// This differs from logSync in that it waits for edits that have been
		/// written by other threads, not just edits from the calling thread.
		/// NOTE: this should be done while holding the FSNamesystem lock, or
		/// else more operations can start writing while this is in progress.
		/// </remarks>
		internal virtual void LogSyncAll()
		{
			// Record the most recent transaction ID as our own id
			lock (this)
			{
				FSEditLog.TransactionId id = myTransactionId.Get();
				id.txid = txid;
			}
			// Then make sure we're synced up to this point
			LogSync();
		}

		/// <summary>Sync all modifications done by this thread.</summary>
		/// <remarks>
		/// Sync all modifications done by this thread.
		/// The internal concurrency design of this class is as follows:
		/// - Log items are written synchronized into an in-memory buffer,
		/// and each assigned a transaction ID.
		/// - When a thread (client) would like to sync all of its edits, logSync()
		/// uses a ThreadLocal transaction ID to determine what edit number must
		/// be synced to.
		/// - The isSyncRunning volatile boolean tracks whether a sync is currently
		/// under progress.
		/// The data is double-buffered within each edit log implementation so that
		/// in-memory writing can occur in parallel with the on-disk writing.
		/// Each sync occurs in three steps:
		/// 1. synchronized, it swaps the double buffer and sets the isSyncRunning
		/// flag.
		/// 2. unsynchronized, it flushes the data to storage
		/// 3. synchronized, it resets the flag and notifies anyone waiting on the
		/// sync.
		/// The lack of synchronization on step 2 allows other threads to continue
		/// to write into the memory buffer while the sync is in progress.
		/// Because this step is unsynchronized, actions that need to avoid
		/// concurrency with sync() should be synchronized and also call
		/// waitForSyncToFinish() before assuming they are running alone.
		/// </remarks>
		public virtual void LogSync()
		{
			long syncStart = 0;
			// Fetch the transactionId of this thread. 
			long mytxid = myTransactionId.Get().txid;
			bool sync = false;
			try
			{
				EditLogOutputStream logStream = null;
				lock (this)
				{
					try
					{
						PrintStatistics(false);
						// if somebody is already syncing, then wait
						while (mytxid > synctxid && isSyncRunning)
						{
							try
							{
								Sharpen.Runtime.Wait(this, 1000);
							}
							catch (Exception)
							{
							}
						}
						//
						// If this transaction was already flushed, then nothing to do
						//
						if (mytxid <= synctxid)
						{
							numTransactionsBatchedInSync++;
							if (metrics != null)
							{
								// Metrics is non-null only when used inside name node
								metrics.IncrTransactionsBatchedInSync();
							}
							return;
						}
						// now, this thread will do the sync
						syncStart = txid;
						isSyncRunning = true;
						sync = true;
						// swap buffers
						try
						{
							if (journalSet.IsEmpty())
							{
								throw new IOException("No journals available to flush");
							}
							editLogStream.SetReadyToFlush();
						}
						catch (IOException e)
						{
							string msg = "Could not sync enough journals to persistent storage " + "due to " 
								+ e.Message + ". " + "Unsynced transactions: " + (txid - synctxid);
							Log.Fatal(msg, new Exception());
							lock (journalSetLock)
							{
								IOUtils.Cleanup(Log, journalSet);
							}
							ExitUtil.Terminate(1, msg);
						}
					}
					finally
					{
						// Prevent RuntimeException from blocking other log edit write 
						DoneWithAutoSyncScheduling();
					}
					//editLogStream may become null,
					//so store a local variable for flush.
					logStream = editLogStream;
				}
				// do the sync
				long start = Time.MonotonicNow();
				try
				{
					if (logStream != null)
					{
						logStream.Flush();
					}
				}
				catch (IOException)
				{
					lock (this)
					{
						string msg = "Could not sync enough journals to persistent storage. " + "Unsynced transactions: "
							 + (txid - synctxid);
						Log.Fatal(msg, new Exception());
						lock (journalSetLock)
						{
							IOUtils.Cleanup(Log, journalSet);
						}
						ExitUtil.Terminate(1, msg);
					}
				}
				long elapsed = Time.MonotonicNow() - start;
				if (metrics != null)
				{
					// Metrics non-null only when used inside name node
					metrics.AddSync(elapsed);
				}
			}
			finally
			{
				// Prevent RuntimeException from blocking other log edit sync 
				lock (this)
				{
					if (sync)
					{
						synctxid = syncStart;
						isSyncRunning = false;
					}
					Sharpen.Runtime.NotifyAll(this);
				}
			}
		}

		//
		// print statistics every 1 minute.
		//
		private void PrintStatistics(bool force)
		{
			long now = Time.MonotonicNow();
			if (lastPrintTime + 60000 > now && !force)
			{
				return;
			}
			lastPrintTime = now;
			StringBuilder buf = new StringBuilder();
			buf.Append("Number of transactions: ");
			buf.Append(numTransactions);
			buf.Append(" Total time for transactions(ms): ");
			buf.Append(totalTimeTransactions);
			buf.Append(" Number of transactions batched in Syncs: ");
			buf.Append(numTransactionsBatchedInSync);
			buf.Append(" Number of syncs: ");
			buf.Append(editLogStream.GetNumSync());
			buf.Append(" SyncTimes(ms): ");
			buf.Append(journalSet.GetSyncTimes());
			Log.Info(buf);
		}

		/// <summary>Record the RPC IDs if necessary</summary>
		private void LogRpcIds(FSEditLogOp op, bool toLogRpcIds)
		{
			if (toLogRpcIds)
			{
				op.SetRpcClientId(Org.Apache.Hadoop.Ipc.Server.GetClientId());
				op.SetRpcCallId(Org.Apache.Hadoop.Ipc.Server.GetCallId());
			}
		}

		public virtual void LogAppendFile(string path, INodeFile file, bool newBlock, bool
			 toLogRpcIds)
		{
			FileUnderConstructionFeature uc = file.GetFileUnderConstructionFeature();
			System.Diagnostics.Debug.Assert(uc != null);
			FSEditLogOp.AppendOp op = FSEditLogOp.AppendOp.GetInstance(cache.Get()).SetPath(path
				).SetClientName(uc.GetClientName()).SetClientMachine(uc.GetClientMachine()).SetNewBlock
				(newBlock);
			LogRpcIds(op, toLogRpcIds);
			LogEdit(op);
		}

		/// <summary>Add open lease record to edit log.</summary>
		/// <remarks>
		/// Add open lease record to edit log.
		/// Records the block locations of the last block.
		/// </remarks>
		public virtual void LogOpenFile(string path, INodeFile newNode, bool overwrite, bool
			 toLogRpcIds)
		{
			Preconditions.CheckArgument(newNode.IsUnderConstruction());
			PermissionStatus permissions = newNode.GetPermissionStatus();
			FSEditLogOp.AddOp op = FSEditLogOp.AddOp.GetInstance(cache.Get()).SetInodeId(newNode
				.GetId()).SetPath(path).SetReplication(newNode.GetFileReplication()).SetModificationTime
				(newNode.GetModificationTime()).SetAccessTime(newNode.GetAccessTime()).SetBlockSize
				(newNode.GetPreferredBlockSize()).SetBlocks(newNode.GetBlocks()).SetPermissionStatus
				(permissions).SetClientName(newNode.GetFileUnderConstructionFeature().GetClientName
				()).SetClientMachine(newNode.GetFileUnderConstructionFeature().GetClientMachine(
				)).SetOverwrite(overwrite).SetStoragePolicyId(newNode.GetLocalStoragePolicyID());
			AclFeature f = newNode.GetAclFeature();
			if (f != null)
			{
				op.SetAclEntries(AclStorage.ReadINodeLogicalAcl(newNode));
			}
			XAttrFeature x = newNode.GetXAttrFeature();
			if (x != null)
			{
				op.SetXAttrs(x.GetXAttrs());
			}
			LogRpcIds(op, toLogRpcIds);
			LogEdit(op);
		}

		/// <summary>Add close lease record to edit log.</summary>
		public virtual void LogCloseFile(string path, INodeFile newNode)
		{
			FSEditLogOp.CloseOp op = FSEditLogOp.CloseOp.GetInstance(cache.Get()).SetPath(path
				).SetReplication(newNode.GetFileReplication()).SetModificationTime(newNode.GetModificationTime
				()).SetAccessTime(newNode.GetAccessTime()).SetBlockSize(newNode.GetPreferredBlockSize
				()).SetBlocks(newNode.GetBlocks()).SetPermissionStatus(newNode.GetPermissionStatus
				());
			LogEdit(op);
		}

		public virtual void LogAddBlock(string path, INodeFile file)
		{
			Preconditions.CheckArgument(file.IsUnderConstruction());
			BlockInfoContiguous[] blocks = file.GetBlocks();
			Preconditions.CheckState(blocks != null && blocks.Length > 0);
			BlockInfoContiguous pBlock = blocks.Length > 1 ? blocks[blocks.Length - 2] : null;
			BlockInfoContiguous lastBlock = blocks[blocks.Length - 1];
			FSEditLogOp.AddBlockOp op = FSEditLogOp.AddBlockOp.GetInstance(cache.Get()).SetPath
				(path).SetPenultimateBlock(pBlock).SetLastBlock(lastBlock);
			LogEdit(op);
		}

		public virtual void LogUpdateBlocks(string path, INodeFile file, bool toLogRpcIds
			)
		{
			Preconditions.CheckArgument(file.IsUnderConstruction());
			FSEditLogOp.UpdateBlocksOp op = FSEditLogOp.UpdateBlocksOp.GetInstance(cache.Get(
				)).SetPath(path).SetBlocks(file.GetBlocks());
			LogRpcIds(op, toLogRpcIds);
			LogEdit(op);
		}

		/// <summary>Add create directory record to edit log</summary>
		public virtual void LogMkDir(string path, INode newNode)
		{
			PermissionStatus permissions = newNode.GetPermissionStatus();
			FSEditLogOp.MkdirOp op = FSEditLogOp.MkdirOp.GetInstance(cache.Get()).SetInodeId(
				newNode.GetId()).SetPath(path).SetTimestamp(newNode.GetModificationTime()).SetPermissionStatus
				(permissions);
			AclFeature f = newNode.GetAclFeature();
			if (f != null)
			{
				op.SetAclEntries(AclStorage.ReadINodeLogicalAcl(newNode));
			}
			XAttrFeature x = newNode.GetXAttrFeature();
			if (x != null)
			{
				op.SetXAttrs(x.GetXAttrs());
			}
			LogEdit(op);
		}

		/// <summary>Add rename record to edit log.</summary>
		/// <remarks>
		/// Add rename record to edit log.
		/// The destination should be the file name, not the destination directory.
		/// TODO: use String parameters until just before writing to disk
		/// </remarks>
		internal virtual void LogRename(string src, string dst, long timestamp, bool toLogRpcIds
			)
		{
			FSEditLogOp.RenameOldOp op = FSEditLogOp.RenameOldOp.GetInstance(cache.Get()).SetSource
				(src).SetDestination(dst).SetTimestamp(timestamp);
			LogRpcIds(op, toLogRpcIds);
			LogEdit(op);
		}

		/// <summary>Add rename record to edit log.</summary>
		/// <remarks>
		/// Add rename record to edit log.
		/// The destination should be the file name, not the destination directory.
		/// </remarks>
		internal virtual void LogRename(string src, string dst, long timestamp, bool toLogRpcIds
			, params Options.Rename[] options)
		{
			FSEditLogOp.RenameOp op = FSEditLogOp.RenameOp.GetInstance(cache.Get()).SetSource
				(src).SetDestination(dst).SetTimestamp(timestamp).SetOptions(options);
			LogRpcIds(op, toLogRpcIds);
			LogEdit(op);
		}

		/// <summary>Add set replication record to edit log</summary>
		internal virtual void LogSetReplication(string src, short replication)
		{
			FSEditLogOp.SetReplicationOp op = FSEditLogOp.SetReplicationOp.GetInstance(cache.
				Get()).SetPath(src).SetReplication(replication);
			LogEdit(op);
		}

		/// <summary>Add set storage policy id record to edit log</summary>
		internal virtual void LogSetStoragePolicy(string src, byte policyId)
		{
			FSEditLogOp.SetStoragePolicyOp op = FSEditLogOp.SetStoragePolicyOp.GetInstance(cache
				.Get()).SetPath(src).SetPolicyId(policyId);
			LogEdit(op);
		}

		/// <summary>Add set namespace quota record to edit log</summary>
		/// <param name="src">the string representation of the path to a directory</param>
		/// <param name="nsQuota">namespace quota</param>
		/// <param name="dsQuota">diskspace quota</param>
		internal virtual void LogSetQuota(string src, long nsQuota, long dsQuota)
		{
			FSEditLogOp.SetQuotaOp op = FSEditLogOp.SetQuotaOp.GetInstance(cache.Get()).SetSource
				(src).SetNSQuota(nsQuota).SetDSQuota(dsQuota);
			LogEdit(op);
		}

		/// <summary>Add set quota by storage type record to edit log</summary>
		internal virtual void LogSetQuotaByStorageType(string src, long dsQuota, StorageType
			 type)
		{
			FSEditLogOp.SetQuotaByStorageTypeOp op = FSEditLogOp.SetQuotaByStorageTypeOp.GetInstance
				(cache.Get()).SetSource(src).SetQuotaByStorageType(dsQuota, type);
			LogEdit(op);
		}

		/// <summary>Add set permissions record to edit log</summary>
		internal virtual void LogSetPermissions(string src, FsPermission permissions)
		{
			FSEditLogOp.SetPermissionsOp op = FSEditLogOp.SetPermissionsOp.GetInstance(cache.
				Get()).SetSource(src).SetPermissions(permissions);
			LogEdit(op);
		}

		/// <summary>Add set owner record to edit log</summary>
		internal virtual void LogSetOwner(string src, string username, string groupname)
		{
			FSEditLogOp.SetOwnerOp op = FSEditLogOp.SetOwnerOp.GetInstance(cache.Get()).SetSource
				(src).SetUser(username).SetGroup(groupname);
			LogEdit(op);
		}

		/// <summary>concat(trg,src..) log</summary>
		internal virtual void LogConcat(string trg, string[] srcs, long timestamp, bool toLogRpcIds
			)
		{
			FSEditLogOp.ConcatDeleteOp op = FSEditLogOp.ConcatDeleteOp.GetInstance(cache.Get(
				)).SetTarget(trg).SetSources(srcs).SetTimestamp(timestamp);
			LogRpcIds(op, toLogRpcIds);
			LogEdit(op);
		}

		/// <summary>Add delete file record to edit log</summary>
		internal virtual void LogDelete(string src, long timestamp, bool toLogRpcIds)
		{
			FSEditLogOp.DeleteOp op = FSEditLogOp.DeleteOp.GetInstance(cache.Get()).SetPath(src
				).SetTimestamp(timestamp);
			LogRpcIds(op, toLogRpcIds);
			LogEdit(op);
		}

		/// <summary>Add truncate file record to edit log</summary>
		internal virtual void LogTruncate(string src, string clientName, string clientMachine
			, long size, long timestamp, Block truncateBlock)
		{
			FSEditLogOp.TruncateOp op = FSEditLogOp.TruncateOp.GetInstance(cache.Get()).SetPath
				(src).SetClientName(clientName).SetClientMachine(clientMachine).SetNewLength(size
				).SetTimestamp(timestamp).SetTruncateBlock(truncateBlock);
			LogEdit(op);
		}

		/// <summary>Add legacy block generation stamp record to edit log</summary>
		internal virtual void LogGenerationStampV1(long genstamp)
		{
			FSEditLogOp.SetGenstampV1Op op = FSEditLogOp.SetGenstampV1Op.GetInstance(cache.Get
				()).SetGenerationStamp(genstamp);
			LogEdit(op);
		}

		/// <summary>Add generation stamp record to edit log</summary>
		internal virtual void LogGenerationStampV2(long genstamp)
		{
			FSEditLogOp.SetGenstampV2Op op = FSEditLogOp.SetGenstampV2Op.GetInstance(cache.Get
				()).SetGenerationStamp(genstamp);
			LogEdit(op);
		}

		/// <summary>Record a newly allocated block ID in the edit log</summary>
		internal virtual void LogAllocateBlockId(long blockId)
		{
			FSEditLogOp.AllocateBlockIdOp op = FSEditLogOp.AllocateBlockIdOp.GetInstance(cache
				.Get()).SetBlockId(blockId);
			LogEdit(op);
		}

		/// <summary>Add access time record to edit log</summary>
		internal virtual void LogTimes(string src, long mtime, long atime)
		{
			FSEditLogOp.TimesOp op = FSEditLogOp.TimesOp.GetInstance(cache.Get()).SetPath(src
				).SetModificationTime(mtime).SetAccessTime(atime);
			LogEdit(op);
		}

		/// <summary>Add a create symlink record.</summary>
		internal virtual void LogSymlink(string path, string value, long mtime, long atime
			, INodeSymlink node, bool toLogRpcIds)
		{
			FSEditLogOp.SymlinkOp op = FSEditLogOp.SymlinkOp.GetInstance(cache.Get()).SetId(node
				.GetId()).SetPath(path).SetValue(value).SetModificationTime(mtime).SetAccessTime
				(atime).SetPermissionStatus(node.GetPermissionStatus());
			LogRpcIds(op, toLogRpcIds);
			LogEdit(op);
		}

		/// <summary>log delegation token to edit log</summary>
		/// <param name="id">DelegationTokenIdentifier</param>
		/// <param name="expiryTime">of the token</param>
		internal virtual void LogGetDelegationToken(DelegationTokenIdentifier id, long expiryTime
			)
		{
			FSEditLogOp.GetDelegationTokenOp op = FSEditLogOp.GetDelegationTokenOp.GetInstance
				(cache.Get()).SetDelegationTokenIdentifier(id).SetExpiryTime(expiryTime);
			LogEdit(op);
		}

		internal virtual void LogRenewDelegationToken(DelegationTokenIdentifier id, long 
			expiryTime)
		{
			FSEditLogOp.RenewDelegationTokenOp op = FSEditLogOp.RenewDelegationTokenOp.GetInstance
				(cache.Get()).SetDelegationTokenIdentifier(id).SetExpiryTime(expiryTime);
			LogEdit(op);
		}

		internal virtual void LogCancelDelegationToken(DelegationTokenIdentifier id)
		{
			FSEditLogOp.CancelDelegationTokenOp op = FSEditLogOp.CancelDelegationTokenOp.GetInstance
				(cache.Get()).SetDelegationTokenIdentifier(id);
			LogEdit(op);
		}

		internal virtual void LogUpdateMasterKey(DelegationKey key)
		{
			FSEditLogOp.UpdateMasterKeyOp op = FSEditLogOp.UpdateMasterKeyOp.GetInstance(cache
				.Get()).SetDelegationKey(key);
			LogEdit(op);
		}

		internal virtual void LogReassignLease(string leaseHolder, string src, string newHolder
			)
		{
			FSEditLogOp.ReassignLeaseOp op = FSEditLogOp.ReassignLeaseOp.GetInstance(cache.Get
				()).SetLeaseHolder(leaseHolder).SetPath(src).SetNewHolder(newHolder);
			LogEdit(op);
		}

		internal virtual void LogCreateSnapshot(string snapRoot, string snapName, bool toLogRpcIds
			)
		{
			FSEditLogOp.CreateSnapshotOp op = FSEditLogOp.CreateSnapshotOp.GetInstance(cache.
				Get()).SetSnapshotRoot(snapRoot).SetSnapshotName(snapName);
			LogRpcIds(op, toLogRpcIds);
			LogEdit(op);
		}

		internal virtual void LogDeleteSnapshot(string snapRoot, string snapName, bool toLogRpcIds
			)
		{
			FSEditLogOp.DeleteSnapshotOp op = FSEditLogOp.DeleteSnapshotOp.GetInstance(cache.
				Get()).SetSnapshotRoot(snapRoot).SetSnapshotName(snapName);
			LogRpcIds(op, toLogRpcIds);
			LogEdit(op);
		}

		internal virtual void LogRenameSnapshot(string path, string snapOldName, string snapNewName
			, bool toLogRpcIds)
		{
			FSEditLogOp.RenameSnapshotOp op = FSEditLogOp.RenameSnapshotOp.GetInstance(cache.
				Get()).SetSnapshotRoot(path).SetSnapshotOldName(snapOldName).SetSnapshotNewName(
				snapNewName);
			LogRpcIds(op, toLogRpcIds);
			LogEdit(op);
		}

		internal virtual void LogAllowSnapshot(string path)
		{
			FSEditLogOp.AllowSnapshotOp op = FSEditLogOp.AllowSnapshotOp.GetInstance(cache.Get
				()).SetSnapshotRoot(path);
			LogEdit(op);
		}

		internal virtual void LogDisallowSnapshot(string path)
		{
			FSEditLogOp.DisallowSnapshotOp op = FSEditLogOp.DisallowSnapshotOp.GetInstance(cache
				.Get()).SetSnapshotRoot(path);
			LogEdit(op);
		}

		/// <summary>
		/// Log a CacheDirectiveInfo returned from
		/// <see cref="CacheManager.AddDirective(Org.Apache.Hadoop.Hdfs.Protocol.CacheDirectiveInfo, FSPermissionChecker, Sharpen.EnumSet{E})
		/// 	"/>
		/// </summary>
		internal virtual void LogAddCacheDirectiveInfo(CacheDirectiveInfo directive, bool
			 toLogRpcIds)
		{
			FSEditLogOp.AddCacheDirectiveInfoOp op = FSEditLogOp.AddCacheDirectiveInfoOp.GetInstance
				(cache.Get()).SetDirective(directive);
			LogRpcIds(op, toLogRpcIds);
			LogEdit(op);
		}

		internal virtual void LogModifyCacheDirectiveInfo(CacheDirectiveInfo directive, bool
			 toLogRpcIds)
		{
			FSEditLogOp.ModifyCacheDirectiveInfoOp op = FSEditLogOp.ModifyCacheDirectiveInfoOp
				.GetInstance(cache.Get()).SetDirective(directive);
			LogRpcIds(op, toLogRpcIds);
			LogEdit(op);
		}

		internal virtual void LogRemoveCacheDirectiveInfo(long id, bool toLogRpcIds)
		{
			FSEditLogOp.RemoveCacheDirectiveInfoOp op = FSEditLogOp.RemoveCacheDirectiveInfoOp
				.GetInstance(cache.Get()).SetId(id);
			LogRpcIds(op, toLogRpcIds);
			LogEdit(op);
		}

		internal virtual void LogAddCachePool(CachePoolInfo pool, bool toLogRpcIds)
		{
			FSEditLogOp.AddCachePoolOp op = FSEditLogOp.AddCachePoolOp.GetInstance(cache.Get(
				)).SetPool(pool);
			LogRpcIds(op, toLogRpcIds);
			LogEdit(op);
		}

		internal virtual void LogModifyCachePool(CachePoolInfo info, bool toLogRpcIds)
		{
			FSEditLogOp.ModifyCachePoolOp op = FSEditLogOp.ModifyCachePoolOp.GetInstance(cache
				.Get()).SetInfo(info);
			LogRpcIds(op, toLogRpcIds);
			LogEdit(op);
		}

		internal virtual void LogRemoveCachePool(string poolName, bool toLogRpcIds)
		{
			FSEditLogOp.RemoveCachePoolOp op = FSEditLogOp.RemoveCachePoolOp.GetInstance(cache
				.Get()).SetPoolName(poolName);
			LogRpcIds(op, toLogRpcIds);
			LogEdit(op);
		}

		internal virtual void LogStartRollingUpgrade(long startTime)
		{
			FSEditLogOp.RollingUpgradeOp op = FSEditLogOp.RollingUpgradeOp.GetStartInstance(cache
				.Get());
			op.SetTime(startTime);
			LogEdit(op);
		}

		internal virtual void LogFinalizeRollingUpgrade(long finalizeTime)
		{
			FSEditLogOp.RollingUpgradeOp op = FSEditLogOp.RollingUpgradeOp.GetFinalizeInstance
				(cache.Get());
			op.SetTime(finalizeTime);
			LogEdit(op);
		}

		internal virtual void LogSetAcl(string src, IList<AclEntry> entries)
		{
			FSEditLogOp.SetAclOp op = FSEditLogOp.SetAclOp.GetInstance();
			op.src = src;
			op.aclEntries = entries;
			LogEdit(op);
		}

		internal virtual void LogSetXAttrs(string src, IList<XAttr> xAttrs, bool toLogRpcIds
			)
		{
			FSEditLogOp.SetXAttrOp op = FSEditLogOp.SetXAttrOp.GetInstance();
			op.src = src;
			op.xAttrs = xAttrs;
			LogRpcIds(op, toLogRpcIds);
			LogEdit(op);
		}

		internal virtual void LogRemoveXAttrs(string src, IList<XAttr> xAttrs, bool toLogRpcIds
			)
		{
			FSEditLogOp.RemoveXAttrOp op = FSEditLogOp.RemoveXAttrOp.GetInstance();
			op.src = src;
			op.xAttrs = xAttrs;
			LogRpcIds(op, toLogRpcIds);
			LogEdit(op);
		}

		/// <summary>Get all the journals this edit log is currently operating on.</summary>
		internal virtual IList<JournalSet.JournalAndStream> GetJournals()
		{
			lock (this)
			{
				return journalSet.GetAllJournalStreams();
			}
		}

		/// <summary>Used only by tests.</summary>
		[VisibleForTesting]
		public virtual JournalSet GetJournalSet()
		{
			lock (this)
			{
				return journalSet;
			}
		}

		[VisibleForTesting]
		internal virtual void SetJournalSetForTesting(JournalSet js)
		{
			lock (this)
			{
				this.journalSet = js;
			}
		}

		/// <summary>Used only by tests.</summary>
		[VisibleForTesting]
		internal virtual void SetMetricsForTests(NameNodeMetrics metrics)
		{
			this.metrics = metrics;
		}

		/// <summary>Return a manifest of what finalized edit logs are available</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual RemoteEditLogManifest GetEditLogManifest(long fromTxId)
		{
			lock (this)
			{
				return journalSet.GetEditLogManifest(fromTxId);
			}
		}

		/// <summary>Finalizes the current edit log and opens a new log segment.</summary>
		/// <returns>
		/// the transaction id of the BEGIN_LOG_SEGMENT transaction
		/// in the new log.
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		internal virtual long RollEditLog()
		{
			lock (this)
			{
				Log.Info("Rolling edit logs");
				EndCurrentLogSegment(true);
				long nextTxId = GetLastWrittenTxId() + 1;
				StartLogSegment(nextTxId, true);
				System.Diagnostics.Debug.Assert(curSegmentTxId == nextTxId);
				return nextTxId;
			}
		}

		/// <summary>Start writing to the log segment with the given txid.</summary>
		/// <remarks>
		/// Start writing to the log segment with the given txid.
		/// Transitions from BETWEEN_LOG_SEGMENTS state to IN_LOG_SEGMENT state.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void StartLogSegment(long segmentTxId, bool writeHeaderTxn)
		{
			lock (this)
			{
				Log.Info("Starting log segment at " + segmentTxId);
				Preconditions.CheckArgument(segmentTxId > 0, "Bad txid: %s", segmentTxId);
				Preconditions.CheckState(state == FSEditLog.State.BetweenLogSegments, "Bad state: %s"
					, state);
				Preconditions.CheckState(segmentTxId > curSegmentTxId, "Cannot start writing to log segment "
					 + segmentTxId + " when previous log segment started at " + curSegmentTxId);
				Preconditions.CheckArgument(segmentTxId == txid + 1, "Cannot start log segment at txid %s when next expected "
					 + "txid is %s", segmentTxId, txid + 1);
				numTransactions = totalTimeTransactions = numTransactionsBatchedInSync = 0;
				// TODO no need to link this back to storage anymore!
				// See HDFS-2174.
				storage.AttemptRestoreRemovedStorage();
				try
				{
					editLogStream = journalSet.StartLogSegment(segmentTxId, NameNodeLayoutVersion.CurrentLayoutVersion
						);
				}
				catch (IOException ex)
				{
					throw new IOException("Unable to start log segment " + segmentTxId + ": too few journals successfully started."
						, ex);
				}
				curSegmentTxId = segmentTxId;
				state = FSEditLog.State.InSegment;
				if (writeHeaderTxn)
				{
					LogEdit(FSEditLogOp.LogSegmentOp.GetInstance(cache.Get(), FSEditLogOpCodes.OpStartLogSegment
						));
					LogSync();
				}
			}
		}

		/// <summary>Finalize the current log segment.</summary>
		/// <remarks>
		/// Finalize the current log segment.
		/// Transitions from IN_SEGMENT state to BETWEEN_LOG_SEGMENTS state.
		/// </remarks>
		public virtual void EndCurrentLogSegment(bool writeEndTxn)
		{
			lock (this)
			{
				Log.Info("Ending log segment " + curSegmentTxId);
				Preconditions.CheckState(IsSegmentOpen(), "Bad state: %s", state);
				if (writeEndTxn)
				{
					LogEdit(FSEditLogOp.LogSegmentOp.GetInstance(cache.Get(), FSEditLogOpCodes.OpEndLogSegment
						));
					LogSync();
				}
				PrintStatistics(true);
				long lastTxId = GetLastWrittenTxId();
				try
				{
					journalSet.FinalizeLogSegment(curSegmentTxId, lastTxId);
					editLogStream = null;
				}
				catch (IOException)
				{
				}
				//All journals have failed, it will be handled in logSync.
				state = FSEditLog.State.BetweenLogSegments;
			}
		}

		/// <summary>Abort all current logs.</summary>
		/// <remarks>Abort all current logs. Called from the backup node.</remarks>
		internal virtual void AbortCurrentLogSegment()
		{
			lock (this)
			{
				try
				{
					//Check for null, as abort can be called any time.
					if (editLogStream != null)
					{
						editLogStream.Abort();
						editLogStream = null;
						state = FSEditLog.State.BetweenLogSegments;
					}
				}
				catch (IOException e)
				{
					Log.Warn("All journals failed to abort", e);
				}
			}
		}

		/// <summary>Archive any log files that are older than the given txid.</summary>
		/// <remarks>
		/// Archive any log files that are older than the given txid.
		/// If the edit log is not open for write, then this call returns with no
		/// effect.
		/// </remarks>
		public virtual void PurgeLogsOlderThan(long minTxIdToKeep)
		{
			lock (this)
			{
				// Should not purge logs unless they are open for write.
				// This prevents the SBN from purging logs on shared storage, for example.
				if (!IsOpenForWrite())
				{
					return;
				}
				System.Diagnostics.Debug.Assert(curSegmentTxId == HdfsConstants.InvalidTxid || minTxIdToKeep
					 <= curSegmentTxId, "cannot purge logs older than txid " + minTxIdToKeep + " when current segment starts at "
					 + curSegmentTxId);
				// on format this is no-op
				if (minTxIdToKeep == 0)
				{
					return;
				}
				// This could be improved to not need synchronization. But currently,
				// journalSet is not threadsafe, so we need to synchronize this method.
				try
				{
					journalSet.PurgeLogsOlderThan(minTxIdToKeep);
				}
				catch (IOException)
				{
				}
			}
		}

		//All journals have failed, it will be handled in logSync.
		/// <summary>The actual sync activity happens while not synchronized on this object.</summary>
		/// <remarks>
		/// The actual sync activity happens while not synchronized on this object.
		/// Thus, synchronized activities that require that they are not concurrent
		/// with file operations should wait for any running sync to finish.
		/// </remarks>
		internal virtual void WaitForSyncToFinish()
		{
			lock (this)
			{
				while (isSyncRunning)
				{
					try
					{
						Sharpen.Runtime.Wait(this, 1000);
					}
					catch (Exception)
					{
					}
				}
			}
		}

		/// <summary>Return the txid of the last synced transaction.</summary>
		public virtual long GetSyncTxId()
		{
			lock (this)
			{
				return synctxid;
			}
		}

		// sets the initial capacity of the flush buffer.
		internal virtual void SetOutputBufferCapacity(int size)
		{
			lock (this)
			{
				journalSet.SetOutputBufferCapacity(size);
			}
		}

		/// <summary>
		/// Create (or find if already exists) an edit output stream, which
		/// streams journal records (edits) to the specified backup node.<br />
		/// The new BackupNode will start receiving edits the next time this
		/// NameNode's logs roll.
		/// </summary>
		/// <param name="bnReg">the backup node registration information.</param>
		/// <param name="nnReg">this (active) name-node registration.</param>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void RegisterBackupNode(NamenodeRegistration bnReg, NamenodeRegistration
			 nnReg)
		{
			lock (this)
			{
				// backup node
				// active name-node
				if (bnReg.IsRole(HdfsServerConstants.NamenodeRole.Checkpoint))
				{
					return;
				}
				// checkpoint node does not stream edits
				JournalManager jas = FindBackupJournal(bnReg);
				if (jas != null)
				{
					// already registered
					Log.Info("Backup node " + bnReg + " re-registers");
					return;
				}
				Log.Info("Registering new backup node: " + bnReg);
				BackupJournalManager bjm = new BackupJournalManager(bnReg, nnReg);
				lock (journalSetLock)
				{
					journalSet.Add(bjm, false);
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void ReleaseBackupStream(NamenodeRegistration registration)
		{
			lock (this)
			{
				BackupJournalManager bjm = this.FindBackupJournal(registration);
				if (bjm != null)
				{
					Log.Info("Removing backup journal " + bjm);
					lock (journalSetLock)
					{
						journalSet.Remove(bjm);
					}
				}
			}
		}

		/// <summary>Find the JournalAndStream associated with this BackupNode.</summary>
		/// <returns>null if it cannot be found</returns>
		private BackupJournalManager FindBackupJournal(NamenodeRegistration bnReg)
		{
			lock (this)
			{
				foreach (JournalManager bjm in journalSet.GetJournalManagers())
				{
					if ((bjm is BackupJournalManager) && ((BackupJournalManager)bjm).MatchesRegistration
						(bnReg))
					{
						return (BackupJournalManager)bjm;
					}
				}
				return null;
			}
		}

		/// <summary>Write an operation to the edit log.</summary>
		/// <remarks>
		/// Write an operation to the edit log. Do not sync to persistent
		/// store yet.
		/// </remarks>
		internal virtual void LogEdit(int length, byte[] data)
		{
			lock (this)
			{
				long start = BeginTransaction();
				try
				{
					editLogStream.WriteRaw(data, 0, length);
				}
				catch (IOException)
				{
				}
				// All journals have failed, it will be handled in logSync.
				EndTransaction(start);
			}
		}

		/// <summary>Run recovery on all journals to recover any unclosed segments</summary>
		internal virtual void RecoverUnclosedStreams()
		{
			lock (this)
			{
				Preconditions.CheckState(state == FSEditLog.State.BetweenLogSegments, "May not recover segments - wrong state: %s"
					, state);
				try
				{
					journalSet.RecoverUnfinalizedSegments();
				}
				catch (IOException)
				{
				}
			}
		}

		// All journals have failed, it is handled in logSync.
		// TODO: are we sure this is OK?
		/// <exception cref="System.IO.IOException"/>
		public virtual long GetSharedLogCTime()
		{
			foreach (JournalSet.JournalAndStream jas in journalSet.GetAllJournalStreams())
			{
				if (jas.IsShared())
				{
					return jas.GetManager().GetJournalCTime();
				}
			}
			throw new IOException("No shared log found.");
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void DoPreUpgradeOfSharedLog()
		{
			lock (this)
			{
				foreach (JournalSet.JournalAndStream jas in journalSet.GetAllJournalStreams())
				{
					if (jas.IsShared())
					{
						jas.GetManager().DoPreUpgrade();
					}
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void DoUpgradeOfSharedLog()
		{
			lock (this)
			{
				foreach (JournalSet.JournalAndStream jas in journalSet.GetAllJournalStreams())
				{
					if (jas.IsShared())
					{
						jas.GetManager().DoUpgrade(storage);
					}
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void DoFinalizeOfSharedLog()
		{
			lock (this)
			{
				foreach (JournalSet.JournalAndStream jas in journalSet.GetAllJournalStreams())
				{
					if (jas.IsShared())
					{
						jas.GetManager().DoFinalize();
					}
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual bool CanRollBackSharedLog(StorageInfo prevStorage, int targetLayoutVersion
			)
		{
			lock (this)
			{
				foreach (JournalSet.JournalAndStream jas in journalSet.GetAllJournalStreams())
				{
					if (jas.IsShared())
					{
						return jas.GetManager().CanRollBack(storage, prevStorage, targetLayoutVersion);
					}
				}
				throw new IOException("No shared log found.");
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void DoRollback()
		{
			lock (this)
			{
				foreach (JournalSet.JournalAndStream jas in journalSet.GetAllJournalStreams())
				{
					if (jas.IsShared())
					{
						jas.GetManager().DoRollback();
					}
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void DiscardSegments(long markerTxid)
		{
			lock (this)
			{
				foreach (JournalSet.JournalAndStream jas in journalSet.GetAllJournalStreams())
				{
					jas.GetManager().DiscardSegments(markerTxid);
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void SelectInputStreams(ICollection<EditLogInputStream> streams, long
			 fromTxId, bool inProgressOk)
		{
			journalSet.SelectInputStreams(streams, fromTxId, inProgressOk);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual ICollection<EditLogInputStream> SelectInputStreams(long fromTxId, 
			long toAtLeastTxId)
		{
			return SelectInputStreams(fromTxId, toAtLeastTxId, null, true);
		}

		/// <summary>Select a list of input streams.</summary>
		/// <param name="fromTxId">first transaction in the selected streams</param>
		/// <param name="toAtLeastTxId">the selected streams must contain this transaction</param>
		/// <param name="recovery">recovery context</param>
		/// <param name="inProgressOk">set to true if in-progress streams are OK</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual ICollection<EditLogInputStream> SelectInputStreams(long fromTxId, 
			long toAtLeastTxId, MetaRecoveryContext recovery, bool inProgressOk)
		{
			IList<EditLogInputStream> streams = new AList<EditLogInputStream>();
			lock (journalSetLock)
			{
				Preconditions.CheckState(journalSet.IsOpen(), "Cannot call " + "selectInputStreams() on closed FSEditLog"
					);
				SelectInputStreams(streams, fromTxId, inProgressOk);
			}
			try
			{
				CheckForGaps(streams, fromTxId, toAtLeastTxId, inProgressOk);
			}
			catch (IOException e)
			{
				if (recovery != null)
				{
					// If recovery mode is enabled, continue loading even if we know we
					// can't load up to toAtLeastTxId.
					Log.Error(e);
				}
				else
				{
					CloseAllStreams(streams);
					throw;
				}
			}
			return streams;
		}

		/// <summary>Check for gaps in the edit log input stream list.</summary>
		/// <remarks>
		/// Check for gaps in the edit log input stream list.
		/// Note: we're assuming that the list is sorted and that txid ranges don't
		/// overlap.  This could be done better and with more generality with an
		/// interval tree.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		private void CheckForGaps(IList<EditLogInputStream> streams, long fromTxId, long 
			toAtLeastTxId, bool inProgressOk)
		{
			IEnumerator<EditLogInputStream> iter = streams.GetEnumerator();
			long txId = fromTxId;
			while (true)
			{
				if (txId > toAtLeastTxId)
				{
					return;
				}
				if (!iter.HasNext())
				{
					break;
				}
				EditLogInputStream elis = iter.Next();
				if (elis.GetFirstTxId() > txId)
				{
					break;
				}
				long next = elis.GetLastTxId();
				if (next == HdfsConstants.InvalidTxid)
				{
					if (!inProgressOk)
					{
						throw new RuntimeException("inProgressOk = false, but " + "selectInputStreams returned an in-progress edit "
							 + "log input stream (" + elis + ")");
					}
					// We don't know where the in-progress stream ends.
					// It could certainly go all the way up to toAtLeastTxId.
					return;
				}
				txId = next + 1;
			}
			throw new IOException(string.Format("Gap in transactions. Expected to " + "be able to read up until at least txid %d but unable to find any "
				 + "edit logs containing txid %d", toAtLeastTxId, txId));
		}

		/// <summary>Close all the streams in a collection</summary>
		/// <param name="streams">The list of streams to close</param>
		internal static void CloseAllStreams(IEnumerable<EditLogInputStream> streams)
		{
			foreach (EditLogInputStream s in streams)
			{
				IOUtils.CloseStream(s);
			}
		}

		/// <summary>Retrieve the implementation class for a Journal scheme.</summary>
		/// <param name="conf">The configuration to retrieve the information from</param>
		/// <param name="uriScheme">The uri scheme to look up.</param>
		/// <returns>the class of the journal implementation</returns>
		/// <exception cref="System.ArgumentException">if no class is configured for uri</exception>
		internal static Type GetJournalClass(Configuration conf, string uriScheme)
		{
			string key = DFSConfigKeys.DfsNamenodeEditsPluginPrefix + "." + uriScheme;
			Type clazz = null;
			try
			{
				clazz = conf.GetClass<JournalManager>(key, null);
			}
			catch (RuntimeException re)
			{
				throw new ArgumentException("Invalid class specified for " + uriScheme, re);
			}
			if (clazz == null)
			{
				Log.Warn("No class configured for " + uriScheme + ", " + key + " is empty");
				throw new ArgumentException("No class configured for " + uriScheme);
			}
			return clazz;
		}

		/// <summary>Construct a custom journal manager.</summary>
		/// <remarks>
		/// Construct a custom journal manager.
		/// The class to construct is taken from the configuration.
		/// </remarks>
		/// <param name="uri">Uri to construct</param>
		/// <returns>The constructed journal manager</returns>
		/// <exception cref="System.ArgumentException">if no class is configured for uri</exception>
		private JournalManager CreateJournal(URI uri)
		{
			Type clazz = GetJournalClass(conf, uri.GetScheme());
			try
			{
				Constructor<JournalManager> cons = clazz.GetConstructor(typeof(Configuration), typeof(
					URI), typeof(NamespaceInfo));
				return cons.NewInstance(conf, uri, storage.GetNamespaceInfo());
			}
			catch (Exception e)
			{
				throw new ArgumentException("Unable to construct journal, " + uri, e);
			}
		}
	}
}
