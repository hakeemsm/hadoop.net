using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Blockmanagement
{
	/// <summary>This class provides tests for BlockInfoUnderConstruction class</summary>
	public class TestBlockInfoUnderConstruction
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestInitializeBlockRecovery()
		{
			DatanodeStorageInfo s1 = DFSTestUtil.CreateDatanodeStorageInfo("10.10.1.1", "s1");
			DatanodeDescriptor dd1 = s1.GetDatanodeDescriptor();
			DatanodeStorageInfo s2 = DFSTestUtil.CreateDatanodeStorageInfo("10.10.1.2", "s2");
			DatanodeDescriptor dd2 = s2.GetDatanodeDescriptor();
			DatanodeStorageInfo s3 = DFSTestUtil.CreateDatanodeStorageInfo("10.10.1.3", "s3");
			DatanodeDescriptor dd3 = s3.GetDatanodeDescriptor();
			dd1.isAlive = dd2.isAlive = dd3.isAlive = true;
			BlockInfoContiguousUnderConstruction blockInfo = new BlockInfoContiguousUnderConstruction
				(new Block(0, 0, GenerationStamp.LastReservedStamp), (short)3, HdfsServerConstants.BlockUCState
				.UnderConstruction, new DatanodeStorageInfo[] { s1, s2, s3 });
			// Recovery attempt #1.
			DFSTestUtil.ResetLastUpdatesWithOffset(dd1, -3 * 1000);
			DFSTestUtil.ResetLastUpdatesWithOffset(dd2, -1 * 1000);
			DFSTestUtil.ResetLastUpdatesWithOffset(dd3, -2 * 1000);
			blockInfo.InitializeBlockRecovery(1);
			BlockInfoContiguousUnderConstruction[] blockInfoRecovery = dd2.GetLeaseRecoveryCommand
				(1);
			NUnit.Framework.Assert.AreEqual(blockInfoRecovery[0], blockInfo);
			// Recovery attempt #2.
			DFSTestUtil.ResetLastUpdatesWithOffset(dd1, -2 * 1000);
			DFSTestUtil.ResetLastUpdatesWithOffset(dd2, -1 * 1000);
			DFSTestUtil.ResetLastUpdatesWithOffset(dd3, -3 * 1000);
			blockInfo.InitializeBlockRecovery(2);
			blockInfoRecovery = dd1.GetLeaseRecoveryCommand(1);
			NUnit.Framework.Assert.AreEqual(blockInfoRecovery[0], blockInfo);
			// Recovery attempt #3.
			DFSTestUtil.ResetLastUpdatesWithOffset(dd1, -2 * 1000);
			DFSTestUtil.ResetLastUpdatesWithOffset(dd2, -1 * 1000);
			DFSTestUtil.ResetLastUpdatesWithOffset(dd3, -3 * 1000);
			blockInfo.InitializeBlockRecovery(3);
			blockInfoRecovery = dd3.GetLeaseRecoveryCommand(1);
			NUnit.Framework.Assert.AreEqual(blockInfoRecovery[0], blockInfo);
			// Recovery attempt #4.
			// Reset everything. And again pick DN with most recent heart beat.
			DFSTestUtil.ResetLastUpdatesWithOffset(dd1, -2 * 1000);
			DFSTestUtil.ResetLastUpdatesWithOffset(dd2, -1 * 1000);
			DFSTestUtil.ResetLastUpdatesWithOffset(dd3, 0);
			blockInfo.InitializeBlockRecovery(3);
			blockInfoRecovery = dd3.GetLeaseRecoveryCommand(1);
			NUnit.Framework.Assert.AreEqual(blockInfoRecovery[0], blockInfo);
		}
	}
}
