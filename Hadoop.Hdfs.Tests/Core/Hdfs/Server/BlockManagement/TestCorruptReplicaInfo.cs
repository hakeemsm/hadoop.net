using System.Collections.Generic;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Blockmanagement
{
	/// <summary>
	/// This test makes sure that
	/// CorruptReplicasMap::numBlocksWithCorruptReplicas and
	/// CorruptReplicasMap::getCorruptReplicaBlockIds
	/// return the correct values
	/// </summary>
	public class TestCorruptReplicaInfo
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.Server.Blockmanagement.TestCorruptReplicaInfo
			));

		private readonly IDictionary<long, Block> block_map = new Dictionary<long, Block>
			();

		// Allow easy block creation by block id
		// Return existing block if one with same block id already exists
		private Block GetBlock(long block_id)
		{
			if (!block_map.Contains(block_id))
			{
				block_map[block_id] = new Block(block_id, 0, 0);
			}
			return block_map[block_id];
		}

		private Block GetBlock(int block_id)
		{
			return GetBlock((long)block_id);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCorruptReplicaInfo()
		{
			CorruptReplicasMap crm = new CorruptReplicasMap();
			// Make sure initial values are returned correctly
			NUnit.Framework.Assert.AreEqual("Number of corrupt blocks must initially be 0", 0
				, crm.Size());
			NUnit.Framework.Assert.IsNull("Param n cannot be less than 0", crm.GetCorruptReplicaBlockIds
				(-1, null));
			NUnit.Framework.Assert.IsNull("Param n cannot be greater than 100", crm.GetCorruptReplicaBlockIds
				(101, null));
			long[] l = crm.GetCorruptReplicaBlockIds(0, null);
			NUnit.Framework.Assert.IsNotNull("n = 0 must return non-null", l);
			NUnit.Framework.Assert.AreEqual("n = 0 must return an empty list", 0, l.Length);
			// create a list of block_ids. A list is used to allow easy validation of the
			// output of getCorruptReplicaBlockIds
			int NumBlockIds = 140;
			IList<long> block_ids = new List<long>();
			for (int i = 0; i < NumBlockIds; i++)
			{
				block_ids.AddItem((long)i);
			}
			DatanodeDescriptor dn1 = DFSTestUtil.GetLocalDatanodeDescriptor();
			DatanodeDescriptor dn2 = DFSTestUtil.GetLocalDatanodeDescriptor();
			AddToCorruptReplicasMap(crm, GetBlock(0), dn1);
			NUnit.Framework.Assert.AreEqual("Number of corrupt blocks not returning correctly"
				, 1, crm.Size());
			AddToCorruptReplicasMap(crm, GetBlock(1), dn1);
			NUnit.Framework.Assert.AreEqual("Number of corrupt blocks not returning correctly"
				, 2, crm.Size());
			AddToCorruptReplicasMap(crm, GetBlock(1), dn2);
			NUnit.Framework.Assert.AreEqual("Number of corrupt blocks not returning correctly"
				, 2, crm.Size());
			crm.RemoveFromCorruptReplicasMap(GetBlock(1));
			NUnit.Framework.Assert.AreEqual("Number of corrupt blocks not returning correctly"
				, 1, crm.Size());
			crm.RemoveFromCorruptReplicasMap(GetBlock(0));
			NUnit.Framework.Assert.AreEqual("Number of corrupt blocks not returning correctly"
				, 0, crm.Size());
			foreach (long block_id in block_ids)
			{
				AddToCorruptReplicasMap(crm, GetBlock(block_id), dn1);
			}
			NUnit.Framework.Assert.AreEqual("Number of corrupt blocks not returning correctly"
				, NumBlockIds, crm.Size());
			NUnit.Framework.Assert.IsTrue("First five block ids not returned correctly ", Arrays
				.Equals(new long[] { 0, 1, 2, 3, 4 }, crm.GetCorruptReplicaBlockIds(5, null)));
			Log.Info(crm.GetCorruptReplicaBlockIds(10, 7L));
			Log.Info(block_ids.SubList(7, 18));
			NUnit.Framework.Assert.IsTrue("10 blocks after 7 not returned correctly ", Arrays
				.Equals(new long[] { 8, 9, 10, 11, 12, 13, 14, 15, 16, 17 }, crm.GetCorruptReplicaBlockIds
				(10, 7L)));
		}

		private static void AddToCorruptReplicasMap(CorruptReplicasMap crm, Block blk, DatanodeDescriptor
			 dn)
		{
			crm.AddToCorruptReplicasMap(blk, dn, "TEST", CorruptReplicasMap.Reason.None);
		}
	}
}
