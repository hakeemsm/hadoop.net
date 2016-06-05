using System.Collections.Generic;
using Com.Google.Common.Collect;
using NUnit.Framework;


namespace Org.Apache.Hadoop.Util.Bloom
{
	public class TestBloomFilters
	{
		internal int numInsertions = 1000;

		internal int bitSize;

		internal int hashFunctionNumber = 5;

		private sealed class _AbstractCollection_45 : AbstractCollection<Key>
		{
			public _AbstractCollection_45()
			{
				this.falsePositive = ImmutableList.Of<Key>(new Key(Runtime.GetBytesForString
					("99")), new Key(Runtime.GetBytesForString("963")));
			}

			internal readonly ImmutableList<Key> falsePositive;

			public override IEnumerator<Key> GetEnumerator()
			{
				return this.falsePositive.GetEnumerator();
			}

			public override int Count
			{
				get
				{
					return this.falsePositive.Count;
				}
			}
		}

		private sealed class _AbstractCollection_58 : AbstractCollection<Key>
		{
			public _AbstractCollection_58()
			{
				this.falsePositive = ImmutableList.Of<Key>(new Key(Runtime.GetBytesForString
					("769")), new Key(Runtime.GetBytesForString("772")), new Key(Runtime.GetBytesForString
					("810")), new Key(Runtime.GetBytesForString("874")));
			}

			internal readonly ImmutableList<Key> falsePositive;

			public override IEnumerator<Key> GetEnumerator()
			{
				return this.falsePositive.GetEnumerator();
			}

			public override int Count
			{
				get
				{
					return this.falsePositive.Count;
				}
			}
		}

		private static readonly ImmutableMap<int, AbstractCollection<Key>> FalsePositiveUnder1000
			 = ImmutableMap.Of(Org.Apache.Hadoop.Util.Hash.Hash.JenkinsHash, new _AbstractCollection_45
			(), Org.Apache.Hadoop.Util.Hash.Hash.MurmurHash, new _AbstractCollection_58());

		[System.Serializable]
		private sealed class Digits
		{
			public static readonly TestBloomFilters.Digits Odd = new TestBloomFilters.Digits(
				1);

			public static readonly TestBloomFilters.Digits Even = new TestBloomFilters.Digits
				(0);

			internal int start;

			internal Digits(int start)
			{
				this.start = start;
			}

			internal int GetStart()
			{
				return TestBloomFilters.Digits.start;
			}
		}

		[Fact]
		public virtual void TestDynamicBloomFilter()
		{
			int hashId = Org.Apache.Hadoop.Util.Hash.Hash.JenkinsHash;
			Filter filter = new DynamicBloomFilter(bitSize, hashFunctionNumber, Org.Apache.Hadoop.Util.Hash.Hash
				.JenkinsHash, 3);
			BloomFilterCommonTester.Of(hashId, numInsertions).WithFilterInstance(filter).WithTestCases
				(ImmutableSet.Of(BloomFilterCommonTester.BloomFilterTestStrategy.KeyTestStrategy
				, BloomFilterCommonTester.BloomFilterTestStrategy.AddKeysStrategy, BloomFilterCommonTester.BloomFilterTestStrategy
				.ExceptionsCheckStrategy, BloomFilterCommonTester.BloomFilterTestStrategy.WriteReadStrategy
				, BloomFilterCommonTester.BloomFilterTestStrategy.OddEvenAbsentStrategy)).Test();
			NUnit.Framework.Assert.IsNotNull("testDynamicBloomFilter error ", filter.ToString
				());
		}

		[Fact]
		public virtual void TestCountingBloomFilter()
		{
			int hashId = Org.Apache.Hadoop.Util.Hash.Hash.JenkinsHash;
			CountingBloomFilter filter = new CountingBloomFilter(bitSize, hashFunctionNumber, 
				hashId);
			Key key = new Key(new byte[] { 48, 48 });
			filter.Add(key);
			Assert.True("CountingBloomFilter.membership error ", filter.MembershipTest
				(key));
			Assert.True("CountingBloomFilter.approximateCount error", filter
				.ApproximateCount(key) == 1);
			filter.Add(key);
			Assert.True("CountingBloomFilter.approximateCount error", filter
				.ApproximateCount(key) == 2);
			filter.Delete(key);
			Assert.True("CountingBloomFilter.membership error ", filter.MembershipTest
				(key));
			filter.Delete(key);
			NUnit.Framework.Assert.IsFalse("CountingBloomFilter.membership error ", filter.MembershipTest
				(key));
			Assert.True("CountingBloomFilter.approximateCount error", filter
				.ApproximateCount(key) == 0);
			BloomFilterCommonTester.Of(hashId, numInsertions).WithFilterInstance(filter).WithTestCases
				(ImmutableSet.Of(BloomFilterCommonTester.BloomFilterTestStrategy.KeyTestStrategy
				, BloomFilterCommonTester.BloomFilterTestStrategy.AddKeysStrategy, BloomFilterCommonTester.BloomFilterTestStrategy
				.ExceptionsCheckStrategy, BloomFilterCommonTester.BloomFilterTestStrategy.OddEvenAbsentStrategy
				, BloomFilterCommonTester.BloomFilterTestStrategy.WriteReadStrategy, BloomFilterCommonTester.BloomFilterTestStrategy
				.FilterOrStrategy, BloomFilterCommonTester.BloomFilterTestStrategy.FilterXorStrategy
				)).Test();
		}

		[Fact]
		public virtual void TestRetouchedBloomFilterSpecific()
		{
			int numInsertions = 1000;
			int hashFunctionNumber = 5;
			ImmutableSet<int> hashes = ImmutableSet.Of(Org.Apache.Hadoop.Util.Hash.Hash.MurmurHash
				, Org.Apache.Hadoop.Util.Hash.Hash.JenkinsHash);
			foreach (int hashId in hashes)
			{
				RetouchedBloomFilter filter = new RetouchedBloomFilter(bitSize, hashFunctionNumber
					, hashId);
				CheckOnAbsentFalsePositive(hashId, numInsertions, filter, TestBloomFilters.Digits
					.Odd, RemoveScheme.MaximumFp);
				filter.And(new RetouchedBloomFilter(bitSize, hashFunctionNumber, hashId));
				CheckOnAbsentFalsePositive(hashId, numInsertions, filter, TestBloomFilters.Digits
					.Even, RemoveScheme.MaximumFp);
				filter.And(new RetouchedBloomFilter(bitSize, hashFunctionNumber, hashId));
				CheckOnAbsentFalsePositive(hashId, numInsertions, filter, TestBloomFilters.Digits
					.Odd, RemoveScheme.MinimumFn);
				filter.And(new RetouchedBloomFilter(bitSize, hashFunctionNumber, hashId));
				CheckOnAbsentFalsePositive(hashId, numInsertions, filter, TestBloomFilters.Digits
					.Even, RemoveScheme.MinimumFn);
				filter.And(new RetouchedBloomFilter(bitSize, hashFunctionNumber, hashId));
				CheckOnAbsentFalsePositive(hashId, numInsertions, filter, TestBloomFilters.Digits
					.Odd, RemoveScheme.Ratio);
				filter.And(new RetouchedBloomFilter(bitSize, hashFunctionNumber, hashId));
				CheckOnAbsentFalsePositive(hashId, numInsertions, filter, TestBloomFilters.Digits
					.Even, RemoveScheme.Ratio);
				filter.And(new RetouchedBloomFilter(bitSize, hashFunctionNumber, hashId));
			}
		}

		private void CheckOnAbsentFalsePositive(int hashId, int numInsertions, RetouchedBloomFilter
			 filter, TestBloomFilters.Digits digits, short removeSchema)
		{
			AbstractCollection<Key> falsePositives = FalsePositiveUnder1000[hashId];
			if (falsePositives == null)
			{
				NUnit.Framework.Assert.Fail(string.Format("false positives for hash %d not founded"
					, hashId));
			}
			filter.AddFalsePositive(falsePositives);
			for (int i = digits.GetStart(); i < numInsertions; i += 2)
			{
				filter.Add(new Key(Runtime.GetBytesForString(Extensions.ToString(
					i))));
			}
			foreach (Key key in falsePositives)
			{
				filter.SelectiveClearing(key, removeSchema);
			}
			for (int i_1 = 1 - digits.GetStart(); i_1 < numInsertions; i_1 += 2)
			{
				NUnit.Framework.Assert.IsFalse(" testRetouchedBloomFilterAddFalsePositive error "
					 + i_1, filter.MembershipTest(new Key(Runtime.GetBytesForString(Extensions.ToString
					(i_1)))));
			}
		}

		[Fact]
		public virtual void TestFiltersWithJenkinsHash()
		{
			int hashId = Org.Apache.Hadoop.Util.Hash.Hash.JenkinsHash;
			BloomFilterCommonTester.Of(hashId, numInsertions).WithFilterInstance(new BloomFilter
				(bitSize, hashFunctionNumber, hashId)).WithFilterInstance(new RetouchedBloomFilter
				(bitSize, hashFunctionNumber, hashId)).WithTestCases(ImmutableSet.Of(BloomFilterCommonTester.BloomFilterTestStrategy
				.KeyTestStrategy, BloomFilterCommonTester.BloomFilterTestStrategy.AddKeysStrategy
				, BloomFilterCommonTester.BloomFilterTestStrategy.ExceptionsCheckStrategy, BloomFilterCommonTester.BloomFilterTestStrategy
				.OddEvenAbsentStrategy, BloomFilterCommonTester.BloomFilterTestStrategy.WriteReadStrategy
				, BloomFilterCommonTester.BloomFilterTestStrategy.FilterOrStrategy, BloomFilterCommonTester.BloomFilterTestStrategy
				.FilterAndStrategy, BloomFilterCommonTester.BloomFilterTestStrategy.FilterXorStrategy
				)).Test();
		}

		[Fact]
		public virtual void TestFiltersWithMurmurHash()
		{
			int hashId = Org.Apache.Hadoop.Util.Hash.Hash.MurmurHash;
			BloomFilterCommonTester.Of(hashId, numInsertions).WithFilterInstance(new BloomFilter
				(bitSize, hashFunctionNumber, hashId)).WithFilterInstance(new RetouchedBloomFilter
				(bitSize, hashFunctionNumber, hashId)).WithTestCases(ImmutableSet.Of(BloomFilterCommonTester.BloomFilterTestStrategy
				.KeyTestStrategy, BloomFilterCommonTester.BloomFilterTestStrategy.AddKeysStrategy
				, BloomFilterCommonTester.BloomFilterTestStrategy.ExceptionsCheckStrategy, BloomFilterCommonTester.BloomFilterTestStrategy
				.OddEvenAbsentStrategy, BloomFilterCommonTester.BloomFilterTestStrategy.WriteReadStrategy
				, BloomFilterCommonTester.BloomFilterTestStrategy.FilterOrStrategy, BloomFilterCommonTester.BloomFilterTestStrategy
				.FilterAndStrategy, BloomFilterCommonTester.BloomFilterTestStrategy.FilterXorStrategy
				)).Test();
		}

		[Fact]
		public virtual void TestNot()
		{
			BloomFilter bf = new BloomFilter(8, 1, Org.Apache.Hadoop.Util.Hash.Hash.JenkinsHash
				);
			bf.bits = BitSet.ValueOf(new byte[] { unchecked((byte)unchecked((int)(0x95))) });
			BitSet origBitSet = (BitSet)bf.bits.Clone();
			bf.Not();
			NUnit.Framework.Assert.IsFalse("BloomFilter#not should have inverted all bits", bf
				.bits.Intersects(origBitSet));
		}

		public TestBloomFilters()
		{
			bitSize = BloomFilterCommonTester.OptimalNumOfBits(numInsertions, 0.03);
		}
	}
}
