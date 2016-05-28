using System;
using NUnit.Framework;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset.Impl
{
	/// <summary>Unit test for ReplicasMap class</summary>
	public class TestReplicaMap
	{
		private readonly ReplicaMap map = new ReplicaMap(typeof(TestReplicaMap));

		private readonly string bpid = "BP-TEST";

		private readonly Block block = new Block(1234, 1234, 1234);

		[SetUp]
		public virtual void Setup()
		{
			map.Add(bpid, new FinalizedReplica(block, null, null));
		}

		/// <summary>Test for ReplicasMap.get(Block) and ReplicasMap.get(long) tests</summary>
		[NUnit.Framework.Test]
		public virtual void TestGet()
		{
			// Test 1: null argument throws invalid argument exception
			try
			{
				map.Get(bpid, null);
				NUnit.Framework.Assert.Fail("Expected exception not thrown");
			}
			catch (ArgumentException)
			{
			}
			// Test 2: successful lookup based on block
			NUnit.Framework.Assert.IsNotNull(map.Get(bpid, block));
			// Test 3: Lookup failure - generation stamp mismatch 
			Block b = new Block(block);
			b.SetGenerationStamp(0);
			NUnit.Framework.Assert.IsNull(map.Get(bpid, b));
			// Test 4: Lookup failure - blockID mismatch
			b.SetGenerationStamp(block.GetGenerationStamp());
			b.SetBlockId(0);
			NUnit.Framework.Assert.IsNull(map.Get(bpid, b));
			// Test 5: successful lookup based on block ID
			NUnit.Framework.Assert.IsNotNull(map.Get(bpid, block.GetBlockId()));
			// Test 6: failed lookup for invalid block ID
			NUnit.Framework.Assert.IsNull(map.Get(bpid, 0));
		}

		[NUnit.Framework.Test]
		public virtual void TestAdd()
		{
			// Test 1: null argument throws invalid argument exception
			try
			{
				map.Add(bpid, null);
				NUnit.Framework.Assert.Fail("Expected exception not thrown");
			}
			catch (ArgumentException)
			{
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestRemove()
		{
			// Test 1: null argument throws invalid argument exception
			try
			{
				map.Remove(bpid, null);
				NUnit.Framework.Assert.Fail("Expected exception not thrown");
			}
			catch (ArgumentException)
			{
			}
			// Test 2: remove failure - generation stamp mismatch 
			Block b = new Block(block);
			b.SetGenerationStamp(0);
			NUnit.Framework.Assert.IsNull(map.Remove(bpid, b));
			// Test 3: remove failure - blockID mismatch
			b.SetGenerationStamp(block.GetGenerationStamp());
			b.SetBlockId(0);
			NUnit.Framework.Assert.IsNull(map.Remove(bpid, b));
			// Test 4: remove success
			NUnit.Framework.Assert.IsNotNull(map.Remove(bpid, block));
			// Test 5: remove failure - invalid blockID
			NUnit.Framework.Assert.IsNull(map.Remove(bpid, 0));
			// Test 6: remove success
			map.Add(bpid, new FinalizedReplica(block, null, null));
			NUnit.Framework.Assert.IsNotNull(map.Remove(bpid, block.GetBlockId()));
		}
	}
}
