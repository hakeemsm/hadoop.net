using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Protocol
{
	public class TestExtendedBlock
	{
		internal const string PoolA = "blockpool-a";

		internal const string PoolB = "blockpool-b";

		internal static readonly Block Block1Gs1 = new Block(1L, 100L, 1L);

		internal static readonly Block Block1Gs2 = new Block(1L, 100L, 2L);

		internal static readonly Block Block2Gs1 = new Block(2L, 100L, 1L);

		[NUnit.Framework.Test]
		public virtual void TestEquals()
		{
			// Same block -> equal
			NUnit.Framework.Assert.AreEqual(new ExtendedBlock(PoolA, Block1Gs1), new ExtendedBlock
				(PoolA, Block1Gs1));
			// Different pools, same block id -> not equal
			AssertNotEquals(new ExtendedBlock(PoolA, Block1Gs1), new ExtendedBlock(PoolB, Block1Gs1
				));
			// Same pool, different block id -> not equal
			AssertNotEquals(new ExtendedBlock(PoolA, Block1Gs1), new ExtendedBlock(PoolA, Block2Gs1
				));
			// Same block, different genstamps -> equal
			NUnit.Framework.Assert.AreEqual(new ExtendedBlock(PoolA, Block1Gs1), new ExtendedBlock
				(PoolA, Block1Gs2));
		}

		[NUnit.Framework.Test]
		public virtual void TestHashcode()
		{
			// Different pools, same block id -> different hashcode
			AssertNotEquals(new ExtendedBlock(PoolA, Block1Gs1).GetHashCode(), new ExtendedBlock
				(PoolB, Block1Gs1).GetHashCode());
			// Same pool, different block id -> different hashcode
			AssertNotEquals(new ExtendedBlock(PoolA, Block1Gs1).GetHashCode(), new ExtendedBlock
				(PoolA, Block2Gs1).GetHashCode());
			// Same block -> same hashcode
			NUnit.Framework.Assert.AreEqual(new ExtendedBlock(PoolA, Block1Gs1).GetHashCode()
				, new ExtendedBlock(PoolA, Block1Gs1).GetHashCode());
		}

		private static void AssertNotEquals(object a, object b)
		{
			NUnit.Framework.Assert.IsFalse("expected not equal: '" + a + "' and '" + b + "'", 
				a.Equals(b));
		}
	}
}
