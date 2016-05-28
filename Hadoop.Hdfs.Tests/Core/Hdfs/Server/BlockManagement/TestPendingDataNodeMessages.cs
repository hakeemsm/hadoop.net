using Com.Google.Common.Base;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Blockmanagement
{
	public class TestPendingDataNodeMessages
	{
		internal readonly PendingDataNodeMessages msgs = new PendingDataNodeMessages();

		private readonly Block block1Gs1 = new Block(1, 0, 1);

		private readonly Block block1Gs2 = new Block(1, 0, 2);

		private readonly Block block1Gs2DifferentInstance = new Block(1, 0, 2);

		private readonly Block block2Gs1 = new Block(2, 0, 1);

		[NUnit.Framework.Test]
		public virtual void TestQueues()
		{
			DatanodeDescriptor fakeDN = DFSTestUtil.GetLocalDatanodeDescriptor();
			DatanodeStorage storage = new DatanodeStorage("STORAGE_ID");
			DatanodeStorageInfo storageInfo = new DatanodeStorageInfo(fakeDN, storage);
			msgs.EnqueueReportedBlock(storageInfo, block1Gs1, HdfsServerConstants.ReplicaState
				.Finalized);
			msgs.EnqueueReportedBlock(storageInfo, block1Gs2, HdfsServerConstants.ReplicaState
				.Finalized);
			NUnit.Framework.Assert.AreEqual(2, msgs.Count());
			// Nothing queued yet for block 2
			NUnit.Framework.Assert.IsNull(msgs.TakeBlockQueue(block2Gs1));
			NUnit.Framework.Assert.AreEqual(2, msgs.Count());
			Queue<PendingDataNodeMessages.ReportedBlockInfo> q = msgs.TakeBlockQueue(block1Gs2DifferentInstance
				);
			NUnit.Framework.Assert.AreEqual("ReportedBlockInfo [block=blk_1_1, dn=127.0.0.1:50010, reportedState=FINALIZED],"
				 + "ReportedBlockInfo [block=blk_1_2, dn=127.0.0.1:50010, reportedState=FINALIZED]"
				, Joiner.On(",").Join(q));
			NUnit.Framework.Assert.AreEqual(0, msgs.Count());
			// Should be null if we pull again
			NUnit.Framework.Assert.IsNull(msgs.TakeBlockQueue(block1Gs1));
			NUnit.Framework.Assert.AreEqual(0, msgs.Count());
		}
	}
}
