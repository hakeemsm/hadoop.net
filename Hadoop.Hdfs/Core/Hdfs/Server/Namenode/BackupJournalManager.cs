using System;
using System.Collections.Generic;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>
	/// A JournalManager implementation that uses RPCs to log transactions
	/// to a BackupNode.
	/// </summary>
	internal class BackupJournalManager : JournalManager
	{
		private readonly NamenodeRegistration bnReg;

		private readonly JournalInfo journalInfo;

		internal BackupJournalManager(NamenodeRegistration bnReg, NamenodeRegistration nnReg
			)
		{
			journalInfo = new JournalInfo(nnReg.GetLayoutVersion(), nnReg.GetClusterID(), nnReg
				.GetNamespaceID());
			this.bnReg = bnReg;
		}

		public override void Format(NamespaceInfo nsInfo)
		{
			// format() should only get called at startup, before any BNs
			// can register with the NN.
			throw new NotSupportedException("BackupNode journal should never get formatted");
		}

		public virtual bool HasSomeData()
		{
			throw new NotSupportedException();
		}

		/// <exception cref="System.IO.IOException"/>
		public override EditLogOutputStream StartLogSegment(long txId, int layoutVersion)
		{
			EditLogBackupOutputStream stm = new EditLogBackupOutputStream(bnReg, journalInfo);
			stm.StartLogSegment(txId);
			return stm;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void FinalizeLogSegment(long firstTxId, long lastTxId)
		{
		}

		public override void SetOutputBufferCapacity(int size)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void PurgeLogsOlderThan(long minTxIdToKeep)
		{
		}

		public virtual void SelectInputStreams(ICollection<EditLogInputStream> streams, long
			 fromTxnId, bool inProgressOk)
		{
		}

		// This JournalManager is never used for input. Therefore it cannot
		// return any transactions
		/// <exception cref="System.IO.IOException"/>
		public override void RecoverUnfinalizedSegments()
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Close()
		{
		}

		public virtual bool MatchesRegistration(NamenodeRegistration bnReg)
		{
			return bnReg.GetAddress().Equals(this.bnReg.GetAddress());
		}

		public override string ToString()
		{
			return "BackupJournalManager";
		}

		/// <exception cref="System.IO.IOException"/>
		public override void DiscardSegments(long startTxId)
		{
			throw new NotSupportedException();
		}

		/// <exception cref="System.IO.IOException"/>
		public override void DoPreUpgrade()
		{
			throw new NotSupportedException();
		}

		/// <exception cref="System.IO.IOException"/>
		public override void DoUpgrade(Storage storage)
		{
			throw new NotSupportedException();
		}

		/// <exception cref="System.IO.IOException"/>
		public override void DoFinalize()
		{
			throw new NotSupportedException();
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool CanRollBack(StorageInfo storage, StorageInfo prevStorage, int
			 targetLayoutVersion)
		{
			throw new NotSupportedException();
		}

		/// <exception cref="System.IO.IOException"/>
		public override void DoRollback()
		{
			throw new NotSupportedException();
		}

		/// <exception cref="System.IO.IOException"/>
		public override long GetJournalCTime()
		{
			throw new NotSupportedException();
		}
	}
}
