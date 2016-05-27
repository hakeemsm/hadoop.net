using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Collect;
using NUnit.Framework;
using Org.Apache.Hadoop.IO;
using Org.Apache.Log4j;
using Sharpen;

namespace Org.Apache.Hadoop.Util.Bloom
{
	public class BloomFilterCommonTester<T>
		where T : Filter
	{
		private static readonly double Ln2 = Math.Log(2);

		private static readonly double Ln2Squared = Ln2 * Ln2;

		private readonly int hashType;

		private readonly int numInsertions;

		private readonly ImmutableList.Builder<T> builder = ImmutableList.Builder();

		private ImmutableSet<BloomFilterCommonTester.BloomFilterTestStrategy> filterTestStrateges;

		private readonly BloomFilterCommonTester.PreAssertionHelper preAssertionHelper;

		internal static int OptimalNumOfBits(int n, double p)
		{
			return (int)(-n * Math.Log(p) / Ln2Squared);
		}

		public static Org.Apache.Hadoop.Util.Bloom.BloomFilterCommonTester<T> Of<T>(int hashId
			, int numInsertions)
			where T : Filter
		{
			return new Org.Apache.Hadoop.Util.Bloom.BloomFilterCommonTester<T>(hashId, numInsertions
				);
		}

		public virtual Org.Apache.Hadoop.Util.Bloom.BloomFilterCommonTester<T> WithFilterInstance
			(T filter)
		{
			builder.Add(filter);
			return this;
		}

		private BloomFilterCommonTester(int hashId, int numInsertions)
		{
			this.hashType = hashId;
			this.numInsertions = numInsertions;
			this.preAssertionHelper = new _PreAssertionHelper_71();
		}

		private sealed class _PreAssertionHelper_71 : BloomFilterCommonTester.PreAssertionHelper
		{
			public _PreAssertionHelper_71()
			{
			}

			public ImmutableSet<int> FalsePositives(int hashId)
			{
				switch (hashId)
				{
					case Org.Apache.Hadoop.Util.Hash.Hash.JenkinsHash:
					{
						// // false pos for odd and event under 1000
						return ImmutableSet.Of(99, 963);
					}

					case Org.Apache.Hadoop.Util.Hash.Hash.MurmurHash:
					{
						// false pos for odd and event under 1000
						return ImmutableSet.Of(769, 772, 810, 874);
					}

					default:
					{
						// fail fast with unknown hash error !!!
						NUnit.Framework.Assert.IsFalse("unknown hash error", true);
						return ImmutableSet.Of();
					}
				}
			}
		}

		public virtual Org.Apache.Hadoop.Util.Bloom.BloomFilterCommonTester<T> WithTestCases
			(ImmutableSet<BloomFilterCommonTester.BloomFilterTestStrategy> filterTestStrateges
			)
		{
			this.filterTestStrateges = ImmutableSet.CopyOf(filterTestStrateges);
			return this;
		}

		public virtual void Test()
		{
			ImmutableList<T> filtersList = ((ImmutableList<T>)builder.Build());
			ImmutableSet<int> falsePositives = preAssertionHelper.FalsePositives(hashType);
			foreach (T filter in filtersList)
			{
				foreach (BloomFilterCommonTester.BloomFilterTestStrategy strategy in filterTestStrateges)
				{
					strategy.GetStrategy().AssertWhat(filter, numInsertions, hashType, falsePositives
						);
					// create fresh instance for next test iteration 
					filter = (T)GetSymmetricFilter(filter.GetType(), numInsertions, hashType);
				}
			}
		}

		internal abstract class FilterTesterStrategy
		{
			public const Logger logger = Logger.GetLogger(typeof(BloomFilterCommonTester.FilterTesterStrategy
				));

			public abstract void AssertWhat(Filter filter, int numInsertions, int hashId, ImmutableSet
				<int> falsePositives);
		}

		internal static class FilterTesterStrategyConstants
		{
		}

		private static Filter GetSymmetricFilter(Type filterClass, int numInsertions, int
			 hashType)
		{
			int bitSetSize = OptimalNumOfBits(numInsertions, 0.03);
			int hashFunctionNumber = 5;
			if (filterClass == typeof(BloomFilter))
			{
				return new BloomFilter(bitSetSize, hashFunctionNumber, hashType);
			}
			else
			{
				if (filterClass == typeof(CountingBloomFilter))
				{
					return new CountingBloomFilter(bitSetSize, hashFunctionNumber, hashType);
				}
				else
				{
					if (filterClass == typeof(RetouchedBloomFilter))
					{
						return new RetouchedBloomFilter(bitSetSize, hashFunctionNumber, hashType);
					}
					else
					{
						if (filterClass == typeof(DynamicBloomFilter))
						{
							return new DynamicBloomFilter(bitSetSize, hashFunctionNumber, hashType, 3);
						}
						else
						{
							//fail fast
							NUnit.Framework.Assert.IsFalse("unexpected filterClass", true);
							return null;
						}
					}
				}
			}
		}

		[System.Serializable]
		public sealed class BloomFilterTestStrategy
		{
			private sealed class _FilterTesterStrategy_144 : BloomFilterCommonTester.FilterTesterStrategy
			{
				public _FilterTesterStrategy_144(BloomFilterTestStrategy _enclosing)
				{
					this._enclosing = _enclosing;
					this.keys = ImmutableList.Of(new Key(new byte[] { 49, 48, 48 }), new Key(new byte
						[] { 50, 48, 48 }));
				}

				private readonly ImmutableList<Key> keys;

				public override void AssertWhat(Filter filter, int numInsertions, int hashId, ImmutableSet
					<int> falsePositives)
				{
					filter.Add(this.keys);
					NUnit.Framework.Assert.IsTrue(" might contain key error ", filter.MembershipTest(
						new Key(Sharpen.Runtime.GetBytesForString("100"))));
					NUnit.Framework.Assert.IsTrue(" might contain key error ", filter.MembershipTest(
						new Key(Sharpen.Runtime.GetBytesForString("200"))));
					filter.Add(Sharpen.Collections.ToArray(this.keys, new Key[] {  }));
					NUnit.Framework.Assert.IsTrue(" might contain key error ", filter.MembershipTest(
						new Key(Sharpen.Runtime.GetBytesForString("100"))));
					NUnit.Framework.Assert.IsTrue(" might contain key error ", filter.MembershipTest(
						new Key(Sharpen.Runtime.GetBytesForString("200"))));
					filter.Add(new _AbstractCollection_167(this));
					NUnit.Framework.Assert.IsTrue(" might contain key error ", filter.MembershipTest(
						new Key(Sharpen.Runtime.GetBytesForString("100"))));
					NUnit.Framework.Assert.IsTrue(" might contain key error ", filter.MembershipTest(
						new Key(Sharpen.Runtime.GetBytesForString("200"))));
				}

				private sealed class _AbstractCollection_167 : AbstractCollection<Key>
				{
					public _AbstractCollection_167(_FilterTesterStrategy_144 _enclosing)
					{
						this._enclosing = _enclosing;
					}

					public override IEnumerator<Key> GetEnumerator()
					{
						return this._enclosing.keys.GetEnumerator();
					}

					public override int Count
					{
						get
						{
							return this._enclosing.keys.Count;
						}
					}

					private readonly _FilterTesterStrategy_144 _enclosing;
				}

				private readonly BloomFilterTestStrategy _enclosing;
			}

			public static readonly BloomFilterCommonTester.BloomFilterTestStrategy AddKeysStrategy;

			private sealed class _FilterTesterStrategy_188 : BloomFilterCommonTester.FilterTesterStrategy
			{
				public _FilterTesterStrategy_188()
				{
				}

				private void CheckOnKeyMethods()
				{
					string line = "werabsdbe";
					Key key = new Key(Sharpen.Runtime.GetBytesForString(line));
					NUnit.Framework.Assert.IsTrue("default key weight error ", key.GetWeight() == 1d);
					key.Set(Sharpen.Runtime.GetBytesForString(line), 2d);
					NUnit.Framework.Assert.IsTrue(" setted key weight error ", key.GetWeight() == 2d);
					Key sKey = new Key(Sharpen.Runtime.GetBytesForString(line), 2d);
					NUnit.Framework.Assert.IsTrue("equals error", key.Equals(sKey));
					NUnit.Framework.Assert.IsTrue("hashcode error", key.GetHashCode() == sKey.GetHashCode
						());
					sKey = new Key(Sharpen.Runtime.GetBytesForString(line.Concat("a")), 2d);
					NUnit.Framework.Assert.IsFalse("equals error", key.Equals(sKey));
					NUnit.Framework.Assert.IsFalse("hashcode error", key.GetHashCode() == sKey.GetHashCode
						());
					sKey = new Key(Sharpen.Runtime.GetBytesForString(line), 3d);
					NUnit.Framework.Assert.IsFalse("equals error", key.Equals(sKey));
					NUnit.Framework.Assert.IsFalse("hashcode error", key.GetHashCode() == sKey.GetHashCode
						());
					key.IncrementWeight();
					NUnit.Framework.Assert.IsTrue("weight error", key.GetWeight() == 3d);
					key.IncrementWeight(2d);
					NUnit.Framework.Assert.IsTrue("weight error", key.GetWeight() == 5d);
				}

				private void CheckOnReadWrite()
				{
					string line = "qryqeb354645rghdfvbaq23312fg";
					DataOutputBuffer @out = new DataOutputBuffer();
					DataInputBuffer @in = new DataInputBuffer();
					Key originKey = new Key(Sharpen.Runtime.GetBytesForString(line), 100d);
					try
					{
						originKey.Write(@out);
						@in.Reset(@out.GetData(), @out.GetData().Length);
						Key restoredKey = new Key(new byte[] { 0 });
						NUnit.Framework.Assert.IsFalse("checkOnReadWrite equals error", restoredKey.Equals
							(originKey));
						restoredKey.ReadFields(@in);
						NUnit.Framework.Assert.IsTrue("checkOnReadWrite equals error", restoredKey.Equals
							(originKey));
						@out.Reset();
					}
					catch (Exception)
					{
						NUnit.Framework.Assert.Fail("checkOnReadWrite ex error");
					}
				}

				private void CheckSetOnIAE()
				{
					Key key = new Key();
					try
					{
						key.Set(null, 0);
					}
					catch (ArgumentException)
					{
					}
					catch (Exception)
					{
						// expected
						NUnit.Framework.Assert.Fail("checkSetOnIAE ex error");
					}
				}

				public override void AssertWhat(Filter filter, int numInsertions, int hashId, ImmutableSet
					<int> falsePositives)
				{
					this.CheckOnKeyMethods();
					this.CheckOnReadWrite();
					this.CheckSetOnIAE();
				}
			}

			public static readonly BloomFilterCommonTester.BloomFilterTestStrategy KeyTestStrategy
				 = new BloomFilterCommonTester.BloomFilterTestStrategy(new _FilterTesterStrategy_188
				());

			private sealed class _FilterTesterStrategy_256 : BloomFilterCommonTester.FilterTesterStrategy
			{
				public _FilterTesterStrategy_256()
				{
				}

				public override void AssertWhat(Filter filter, int numInsertions, int hashId, ImmutableSet
					<int> falsePositives)
				{
					this.CheckAddOnNPE(filter);
					this.CheckTestMembershipOnNPE(filter);
					this.CheckAndOnIAE(filter);
				}

				private void CheckAndOnIAE(Filter filter)
				{
					Filter tfilter = null;
					try
					{
						ICollection<Key> keys = null;
						filter.Add(keys);
					}
					catch (ArgumentException)
					{
					}
					catch (Exception e)
					{
						//
						NUnit.Framework.Assert.Fail(string.Empty + e);
					}
					try
					{
						Key[] keys = null;
						filter.Add(keys);
					}
					catch (ArgumentException)
					{
					}
					catch (Exception e)
					{
						//
						NUnit.Framework.Assert.Fail(string.Empty + e);
					}
					try
					{
						ImmutableList<Key> keys = null;
						filter.Add(keys);
					}
					catch (ArgumentException)
					{
					}
					catch (Exception e)
					{
						//
						NUnit.Framework.Assert.Fail(string.Empty + e);
					}
					try
					{
						filter.And(tfilter);
					}
					catch (ArgumentException)
					{
					}
					catch (Exception e)
					{
						// expected
						NUnit.Framework.Assert.Fail(string.Empty + e);
					}
					try
					{
						filter.Or(tfilter);
					}
					catch (ArgumentException)
					{
					}
					catch (Exception e)
					{
						// expected
						NUnit.Framework.Assert.Fail(string.Empty + e);
					}
					try
					{
						filter.Xor(tfilter);
					}
					catch (ArgumentException)
					{
					}
					catch (NotSupportedException)
					{
					}
					catch (Exception e)
					{
						// expected
						//
						NUnit.Framework.Assert.Fail(string.Empty + e);
					}
				}

				private void CheckTestMembershipOnNPE(Filter filter)
				{
					try
					{
						Key nullKey = null;
						filter.MembershipTest(nullKey);
					}
					catch (ArgumentNullException)
					{
					}
					catch (Exception e)
					{
						// expected
						NUnit.Framework.Assert.Fail(string.Empty + e);
					}
				}

				private void CheckAddOnNPE(Filter filter)
				{
					try
					{
						Key nullKey = null;
						filter.Add(nullKey);
					}
					catch (ArgumentNullException)
					{
					}
					catch (Exception e)
					{
						// expected
						NUnit.Framework.Assert.Fail(string.Empty + e);
					}
				}
			}

			public static readonly BloomFilterCommonTester.BloomFilterTestStrategy ExceptionsCheckStrategy
				 = new BloomFilterCommonTester.BloomFilterTestStrategy(new _FilterTesterStrategy_256
				());

			private sealed class _FilterTesterStrategy_347 : BloomFilterCommonTester.FilterTesterStrategy
			{
				public _FilterTesterStrategy_347()
				{
				}

				public override void AssertWhat(Filter filter, int numInsertions, int hashId, ImmutableSet
					<int> falsePositives)
				{
					// add all even keys
					for (int i = 0; i < numInsertions; i += 2)
					{
						filter.Add(new Key(Sharpen.Runtime.GetBytesForString(Sharpen.Extensions.ToString(
							i))));
					}
					// check on present even key
					for (int i_1 = 0; i_1 < numInsertions; i_1 += 2)
					{
						NUnit.Framework.Assert.IsTrue(" filter might contains " + i_1, filter.MembershipTest
							(new Key(Sharpen.Runtime.GetBytesForString(Sharpen.Extensions.ToString(i_1)))));
					}
					// check on absent odd in event
					for (int i_2 = 1; i_2 < numInsertions; i_2 += 2)
					{
						if (!falsePositives.Contains(i_2))
						{
							NUnit.Framework.Assert.IsFalse(" filter should not contain " + i_2, filter.MembershipTest
								(new Key(Sharpen.Runtime.GetBytesForString(Sharpen.Extensions.ToString(i_2)))));
						}
					}
				}
			}

			public static readonly BloomFilterCommonTester.BloomFilterTestStrategy OddEvenAbsentStrategy
				 = new BloomFilterCommonTester.BloomFilterTestStrategy(new _FilterTesterStrategy_347
				());

			private sealed class _FilterTesterStrategy_374 : BloomFilterCommonTester.FilterTesterStrategy
			{
				public _FilterTesterStrategy_374()
				{
					this.slotSize = 10;
				}

				private int slotSize;

				public override void AssertWhat(Filter filter, int numInsertions, int hashId, ImmutableSet
					<int> falsePositives)
				{
					Random rnd = new Random();
					DataOutputBuffer @out = new DataOutputBuffer();
					DataInputBuffer @in = new DataInputBuffer();
					try
					{
						Filter tempFilter = BloomFilterCommonTester.GetSymmetricFilter(filter.GetType(), 
							numInsertions, hashId);
						ImmutableList.Builder<int> blist = ImmutableList.Builder();
						for (int i = 0; i < this.slotSize; i++)
						{
							blist.Add(rnd.Next(numInsertions * 2));
						}
						ImmutableList<int> list = ((ImmutableList<int>)blist.Build());
						// mark bits for later check
						foreach (int slot in list)
						{
							filter.Add(new Key(Sharpen.Runtime.GetBytesForString(slot.ToString())));
						}
						filter.Write(@out);
						@in.Reset(@out.GetData(), @out.GetLength());
						tempFilter.ReadFields(@in);
						foreach (int slot_1 in list)
						{
							NUnit.Framework.Assert.IsTrue("read/write mask check filter error on " + slot_1, 
								filter.MembershipTest(new Key(Sharpen.Runtime.GetBytesForString(slot_1.ToString(
								)))));
						}
					}
					catch (IOException ex)
					{
						NUnit.Framework.Assert.Fail("error ex !!!" + ex);
					}
				}
			}

			public static readonly BloomFilterCommonTester.BloomFilterTestStrategy WriteReadStrategy
				 = new BloomFilterCommonTester.BloomFilterTestStrategy(new _FilterTesterStrategy_374
				());

			private sealed class _FilterTesterStrategy_415 : BloomFilterCommonTester.FilterTesterStrategy
			{
				public _FilterTesterStrategy_415()
				{
				}

				public override void AssertWhat(Filter filter, int numInsertions, int hashId, ImmutableSet
					<int> falsePositives)
				{
					Filter symmetricFilter = BloomFilterCommonTester.GetSymmetricFilter(filter.GetType
						(), numInsertions, hashId);
					try
					{
						// 0 xor 0 -> 0
						filter.Xor(symmetricFilter);
						// check on present all key
						for (int i = 0; i < numInsertions; i++)
						{
							NUnit.Framework.Assert.IsFalse(" filter might contains " + i, filter.MembershipTest
								(new Key(Sharpen.Runtime.GetBytesForString(Sharpen.Extensions.ToString(i)))));
						}
						// add all even keys
						for (int i_1 = 0; i_1 < numInsertions; i_1 += 2)
						{
							filter.Add(new Key(Sharpen.Runtime.GetBytesForString(Sharpen.Extensions.ToString(
								i_1))));
						}
						// add all odd keys
						for (int i_2 = 0; i_2 < numInsertions; i_2 += 2)
						{
							symmetricFilter.Add(new Key(Sharpen.Runtime.GetBytesForString(Sharpen.Extensions.ToString
								(i_2))));
						}
						filter.Xor(symmetricFilter);
						// 1 xor 1 -> 0
						// check on absent all key
						for (int i_3 = 0; i_3 < numInsertions; i_3++)
						{
							NUnit.Framework.Assert.IsFalse(" filter might not contains " + i_3, filter.MembershipTest
								(new Key(Sharpen.Runtime.GetBytesForString(Sharpen.Extensions.ToString(i_3)))));
						}
					}
					catch (NotSupportedException)
					{
						// not all Filter's implements this method
						return;
					}
				}
			}

			public static readonly BloomFilterCommonTester.BloomFilterTestStrategy FilterXorStrategy
				 = new BloomFilterCommonTester.BloomFilterTestStrategy(new _FilterTesterStrategy_415
				());

			private sealed class _FilterTesterStrategy_456 : BloomFilterCommonTester.FilterTesterStrategy
			{
				public _FilterTesterStrategy_456()
				{
				}

				public override void AssertWhat(Filter filter, int numInsertions, int hashId, ImmutableSet
					<int> falsePositives)
				{
					int startIntersection = numInsertions - (numInsertions - 100);
					int endIntersection = numInsertions - 100;
					Filter partialFilter = BloomFilterCommonTester.GetSymmetricFilter(filter.GetType(
						), numInsertions, hashId);
					for (int i = 0; i < numInsertions; i++)
					{
						string digit = Sharpen.Extensions.ToString(i);
						filter.Add(new Key(Sharpen.Runtime.GetBytesForString(digit)));
						if (i >= startIntersection && i <= endIntersection)
						{
							partialFilter.Add(new Key(Sharpen.Runtime.GetBytesForString(digit)));
						}
					}
					// do logic AND
					filter.And(partialFilter);
					for (int i_1 = 0; i_1 < numInsertions; i_1++)
					{
						if (i_1 >= startIntersection && i_1 <= endIntersection)
						{
							NUnit.Framework.Assert.IsTrue(" filter might contains " + i_1, filter.MembershipTest
								(new Key(Sharpen.Runtime.GetBytesForString(Sharpen.Extensions.ToString(i_1)))));
						}
					}
				}
			}

			public static readonly BloomFilterCommonTester.BloomFilterTestStrategy FilterAndStrategy
				 = new BloomFilterCommonTester.BloomFilterTestStrategy(new _FilterTesterStrategy_456
				());

			private sealed class _FilterTesterStrategy_488 : BloomFilterCommonTester.FilterTesterStrategy
			{
				public _FilterTesterStrategy_488()
				{
				}

				public override void AssertWhat(Filter filter, int numInsertions, int hashId, ImmutableSet
					<int> falsePositives)
				{
					Filter evenFilter = BloomFilterCommonTester.GetSymmetricFilter(filter.GetType(), 
						numInsertions, hashId);
					// add all even
					for (int i = 0; i < numInsertions; i += 2)
					{
						evenFilter.Add(new Key(Sharpen.Runtime.GetBytesForString(Sharpen.Extensions.ToString
							(i))));
					}
					// add all odd
					for (int i_1 = 1; i_1 < numInsertions; i_1 += 2)
					{
						filter.Add(new Key(Sharpen.Runtime.GetBytesForString(Sharpen.Extensions.ToString(
							i_1))));
					}
					// union odd with even
					filter.Or(evenFilter);
					// check on present all key
					for (int i_2 = 0; i_2 < numInsertions; i_2++)
					{
						NUnit.Framework.Assert.IsTrue(" filter might contains " + i_2, filter.MembershipTest
							(new Key(Sharpen.Runtime.GetBytesForString(Sharpen.Extensions.ToString(i_2)))));
					}
				}
			}

			public static readonly BloomFilterCommonTester.BloomFilterTestStrategy FilterOrStrategy
				 = new BloomFilterCommonTester.BloomFilterTestStrategy(new _FilterTesterStrategy_488
				());

			private readonly BloomFilterCommonTester.FilterTesterStrategy testerStrategy;

			internal BloomFilterTestStrategy(BloomFilterCommonTester.FilterTesterStrategy testerStrategy
				)
			{
				AddKeysStrategy = new BloomFilterCommonTester.BloomFilterTestStrategy(new _FilterTesterStrategy_144
					(this));
				this.testerStrategy = testerStrategy;
			}

			public BloomFilterCommonTester.FilterTesterStrategy GetStrategy()
			{
				return BloomFilterCommonTester.BloomFilterTestStrategy.testerStrategy;
			}
		}

		internal interface PreAssertionHelper
		{
			ImmutableSet<int> FalsePositives(int hashId);
		}
	}
}
