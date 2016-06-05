using System;
using System.Collections.Generic;
using NUnit.Framework;
using Sharpen;

namespace Org.Apache.Hadoop.Util
{
	/// <summary>
	/// Testing
	/// <see cref="LightWeightCache{K, E}"/>
	/// 
	/// </summary>
	public class TestLightWeightCache
	{
		private static readonly long starttime = Time.Now();

		private static readonly long seed = starttime;

		private static readonly Random ran = new Random(seed);

		static TestLightWeightCache()
		{
			Println("Start time = " + Sharpen.Extensions.CreateDate(starttime) + ", seed=" + 
				seed);
		}

		private static void Print(object s)
		{
			System.Console.Out.Write(s);
			System.Console.Out.Flush();
		}

		private static void Println(object s)
		{
			System.Console.Out.WriteLine(s);
		}

		[Fact]
		public virtual void TestLightWeightCache()
		{
			{
				// test randomized creation expiration with zero access expiration 
				long creationExpiration = ran.Next(1024) + 1;
				Check(1, creationExpiration, 0L, 1 << 10, 65537);
				Check(17, creationExpiration, 0L, 1 << 16, 17);
				Check(255, creationExpiration, 0L, 1 << 16, 65537);
			}
			// test randomized creation/access expiration periods
			for (int i = 0; i < 3; i++)
			{
				long creationExpiration = ran.Next(1024) + 1;
				long accessExpiration = ran.Next(1024) + 1;
				Check(1, creationExpiration, accessExpiration, 1 << 10, 65537);
				Check(17, creationExpiration, accessExpiration, 1 << 16, 17);
				Check(255, creationExpiration, accessExpiration, 1 << 16, 65537);
			}
			// test size limit
			int dataSize = 1 << 16;
			for (int i_1 = 0; i_1 < 10; i_1++)
			{
				int modulus = ran.Next(1024) + 1;
				int sizeLimit = ran.Next(modulus) + 1;
				CheckSizeLimit(sizeLimit, dataSize, modulus);
			}
		}

		private static void CheckSizeLimit(int sizeLimit, int datasize, int modulus)
		{
			TestLightWeightCache.LightWeightCacheTestCase test = new TestLightWeightCache.LightWeightCacheTestCase
				(sizeLimit, sizeLimit, 1L << 32, 1L << 32, datasize, modulus);
			// keep putting entries and check size limit
			Print("  check size ................. ");
			for (int i = 0; i < test.data.Size(); i++)
			{
				test.cache.Put(test.data.Get(i));
				Assert.True(test.cache.Size() <= sizeLimit);
			}
			Println("DONE " + test.Stat());
		}

		/// <summary>Test various createionExpirationPeriod and accessExpirationPeriod.</summary>
		/// <remarks>
		/// Test various createionExpirationPeriod and accessExpirationPeriod.
		/// It runs ~2 minutes. If you are changing the implementation,
		/// please un-comment the following line in order to run the test.
		/// </remarks>
		public virtual void TestExpirationPeriods()
		{
			//  @Test
			for (int k = -4; k < 10; k += 4)
			{
				long accessExpirationPeriod = k < 0 ? 0L : (1L << k);
				for (int j = 0; j < 10; j += 4)
				{
					long creationExpirationPeriod = 1L << j;
					RunTests(1, creationExpirationPeriod, accessExpirationPeriod);
					for (int i = 1; i < int.Size - 1; i += 8)
					{
						RunTests((1 << i) + 1, creationExpirationPeriod, accessExpirationPeriod);
					}
				}
			}
		}

		/// <summary>Run tests with various table lengths.</summary>
		private static void RunTests(int modulus, long creationExpirationPeriod, long accessExpirationPeriod
			)
		{
			Println("\n\n\n*** runTest: modulus=" + modulus + ", creationExpirationPeriod=" +
				 creationExpirationPeriod + ", accessExpirationPeriod=" + accessExpirationPeriod
				);
			for (int i = 0; i <= 16; i += 4)
			{
				int tablelength = (1 << i);
				int upper = i + 2;
				int steps = Math.Max(1, upper / 3);
				for (int j = upper; j > 0; j -= steps)
				{
					int datasize = 1 << j;
					Check(tablelength, creationExpirationPeriod, accessExpirationPeriod, datasize, modulus
						);
				}
			}
		}

		private static void Check(int tablelength, long creationExpirationPeriod, long accessExpirationPeriod
			, int datasize, int modulus)
		{
			Check(new TestLightWeightCache.LightWeightCacheTestCase(tablelength, -1, creationExpirationPeriod
				, accessExpirationPeriod, datasize, modulus));
		}

		/// <summary>
		/// check the following operations
		/// (1) put
		/// (2) remove & put
		/// (3) remove
		/// (4) remove & put again
		/// </summary>
		private static void Check(TestLightWeightCache.LightWeightCacheTestCase test)
		{
			//check put
			Print("  check put .................. ");
			for (int i = 0; i < test.data.Size() / 2; i++)
			{
				test.Put(test.data.Get(i));
			}
			for (int i_1 = 0; i_1 < test.data.Size(); i_1++)
			{
				test.Put(test.data.Get(i_1));
			}
			Println("DONE " + test.Stat());
			//check remove and put
			Print("  check remove & put ......... ");
			for (int j = 0; j < 10; j++)
			{
				for (int i_2 = 0; i_2 < test.data.Size() / 2; i_2++)
				{
					int r = ran.Next(test.data.Size());
					test.Remove(test.data.Get(r));
				}
				for (int i_3 = 0; i_3 < test.data.Size() / 2; i_3++)
				{
					int r = ran.Next(test.data.Size());
					test.Put(test.data.Get(r));
				}
			}
			Println("DONE " + test.Stat());
			//check remove
			Print("  check remove ............... ");
			for (int i_4 = 0; i_4 < test.data.Size(); i_4++)
			{
				test.Remove(test.data.Get(i_4));
			}
			Assert.Equal(0, test.cache.Size());
			Println("DONE " + test.Stat());
			//check remove and put again
			Print("  check remove & put again ... ");
			for (int j_1 = 0; j_1 < 10; j_1++)
			{
				for (int i_2 = 0; i_2 < test.data.Size() / 2; i_2++)
				{
					int r = ran.Next(test.data.Size());
					test.Remove(test.data.Get(r));
				}
				for (int i_3 = 0; i_3 < test.data.Size() / 2; i_3++)
				{
					int r = ran.Next(test.data.Size());
					test.Put(test.data.Get(r));
				}
			}
			Println("DONE " + test.Stat());
			long s = (Time.Now() - starttime) / 1000L;
			Println("total time elapsed=" + s + "s\n");
		}

		/// <summary>The test case contains two data structures, a cache and a hashMap.</summary>
		/// <remarks>
		/// The test case contains two data structures, a cache and a hashMap.
		/// The hashMap is used to verify the correctness of the cache.  Note that
		/// no automatic eviction is performed in the hashMap.  Thus, we have
		/// (1) If an entry exists in cache, it MUST exist in the hashMap.
		/// (2) If an entry does not exist in the cache, it may or may not exist in the
		/// hashMap.  If it exists, it must be expired.
		/// </remarks>
		private class LightWeightCacheTestCase : GSet<TestLightWeightCache.IntEntry, TestLightWeightCache.IntEntry
			>
		{
			/// <summary>hashMap will not evict entries automatically.</summary>
			internal readonly GSet<TestLightWeightCache.IntEntry, TestLightWeightCache.IntEntry
				> hashMap = new GSetByHashMap<TestLightWeightCache.IntEntry, TestLightWeightCache.IntEntry
				>(1024, 0.75f);

			internal readonly LightWeightCache<TestLightWeightCache.IntEntry, TestLightWeightCache.IntEntry
				> cache;

			internal readonly TestLightWeightCache.IntData data;

			internal readonly string info;

			internal readonly long starttime = Time.Now();

			/// <summary>
			/// Determine the probability in
			/// <see cref="Check()"/>
			/// .
			/// </summary>
			internal readonly int denominator;

			internal int iterate_count = 0;

			internal int contain_count = 0;

			private long currentTestTime = ran.Next();

			internal LightWeightCacheTestCase(int tablelength, int sizeLimit, long creationExpirationPeriod
				, long accessExpirationPeriod, int datasize, int modulus)
			{
				denominator = Math.Min((datasize >> 7) + 1, 1 << 16);
				info = GetType().Name + "(" + Sharpen.Extensions.CreateDate(starttime) + "): tablelength="
					 + tablelength + ", creationExpirationPeriod=" + creationExpirationPeriod + ", accessExpirationPeriod="
					 + accessExpirationPeriod + ", datasize=" + datasize + ", modulus=" + modulus + 
					", denominator=" + denominator;
				Println(info);
				data = new TestLightWeightCache.IntData(datasize, modulus);
				cache = new LightWeightCache<TestLightWeightCache.IntEntry, TestLightWeightCache.IntEntry
					>(tablelength, sizeLimit, creationExpirationPeriod, 0, new _Clock_232(this));
				Assert.Equal(0, cache.Size());
			}

			private sealed class _Clock_232 : LightWeightCache.Clock
			{
				public _Clock_232(LightWeightCacheTestCase _enclosing)
				{
					this._enclosing = _enclosing;
				}

				internal override long CurrentTime()
				{
					return this._enclosing.currentTestTime;
				}

				private readonly LightWeightCacheTestCase _enclosing;
			}

			private bool ContainsTest(TestLightWeightCache.IntEntry key)
			{
				bool c = cache.Contains(key);
				if (c)
				{
					Assert.True(hashMap.Contains(key));
				}
				else
				{
					TestLightWeightCache.IntEntry h = hashMap.Remove(key);
					if (h != null)
					{
						Assert.True(cache.IsExpired(h, currentTestTime));
					}
				}
				return c;
			}

			public override bool Contains(TestLightWeightCache.IntEntry key)
			{
				bool e = ContainsTest(key);
				Check();
				return e;
			}

			private TestLightWeightCache.IntEntry GetTest(TestLightWeightCache.IntEntry key)
			{
				TestLightWeightCache.IntEntry c = cache.Get(key);
				if (c != null)
				{
					Assert.Equal(hashMap.Get(key).id, c.id);
				}
				else
				{
					TestLightWeightCache.IntEntry h = hashMap.Remove(key);
					if (h != null)
					{
						Assert.True(cache.IsExpired(h, currentTestTime));
					}
				}
				return c;
			}

			public override TestLightWeightCache.IntEntry Get(TestLightWeightCache.IntEntry key
				)
			{
				TestLightWeightCache.IntEntry e = GetTest(key);
				Check();
				return e;
			}

			private TestLightWeightCache.IntEntry PutTest(TestLightWeightCache.IntEntry entry
				)
			{
				TestLightWeightCache.IntEntry c = cache.Put(entry);
				if (c != null)
				{
					Assert.Equal(hashMap.Put(entry).id, c.id);
				}
				else
				{
					TestLightWeightCache.IntEntry h = hashMap.Put(entry);
					if (h != null && h != entry)
					{
						// if h == entry, its expiration time is already updated
						Assert.True(cache.IsExpired(h, currentTestTime));
					}
				}
				return c;
			}

			public override TestLightWeightCache.IntEntry Put(TestLightWeightCache.IntEntry entry
				)
			{
				TestLightWeightCache.IntEntry e = PutTest(entry);
				Check();
				return e;
			}

			private TestLightWeightCache.IntEntry RemoveTest(TestLightWeightCache.IntEntry key
				)
			{
				TestLightWeightCache.IntEntry c = cache.Remove(key);
				if (c != null)
				{
					Assert.Equal(c.id, hashMap.Remove(key).id);
				}
				else
				{
					TestLightWeightCache.IntEntry h = hashMap.Remove(key);
					if (h != null)
					{
						Assert.True(cache.IsExpired(h, currentTestTime));
					}
				}
				return c;
			}

			public override TestLightWeightCache.IntEntry Remove(TestLightWeightCache.IntEntry
				 key)
			{
				TestLightWeightCache.IntEntry e = RemoveTest(key);
				Check();
				return e;
			}

			private int SizeTest()
			{
				int c = cache.Size();
				Assert.True(hashMap.Size() >= c);
				return c;
			}

			public override int Size()
			{
				int s = SizeTest();
				Check();
				return s;
			}

			public virtual IEnumerator<TestLightWeightCache.IntEntry> GetEnumerator()
			{
				throw new NotSupportedException();
			}

			internal virtual bool TossCoin()
			{
				return ran.Next(denominator) == 0;
			}

			internal virtual void Check()
			{
				currentTestTime += ran.Next() & unchecked((int)(0x3));
				//test size
				SizeTest();
				if (TossCoin())
				{
					//test get(..), check content and test iterator
					iterate_count++;
					foreach (TestLightWeightCache.IntEntry i in cache)
					{
						GetTest(i);
					}
				}
				if (TossCoin())
				{
					//test contains(..)
					contain_count++;
					int count = Math.Min(data.Size(), 1000);
					if (count == data.Size())
					{
						foreach (TestLightWeightCache.IntEntry i in data.integers)
						{
							ContainsTest(i);
						}
					}
					else
					{
						for (int j = 0; j < count; j++)
						{
							ContainsTest(data.Get(ran.Next(data.Size())));
						}
					}
				}
			}

			internal virtual string Stat()
			{
				long t = Time.Now() - starttime;
				return string.Format(" iterate=%5d, contain=%5d, time elapsed=%5d.%03ds", iterate_count
					, contain_count, t / 1000, t % 1000);
			}

			public override void Clear()
			{
				hashMap.Clear();
				cache.Clear();
				Assert.Equal(0, Size());
			}
		}

		private class IntData
		{
			internal readonly TestLightWeightCache.IntEntry[] integers;

			internal IntData(int size, int modulus)
			{
				integers = new TestLightWeightCache.IntEntry[size];
				for (int i = 0; i < integers.Length; i++)
				{
					integers[i] = new TestLightWeightCache.IntEntry(i, ran.Next(modulus));
				}
			}

			internal virtual TestLightWeightCache.IntEntry Get(int i)
			{
				return integers[i];
			}

			internal virtual int Size()
			{
				return integers.Length;
			}
		}

		/// <summary>
		/// Entries of
		/// <see cref="LightWeightCache{K, E}"/>
		/// in this test
		/// </summary>
		private class IntEntry : LightWeightCache.Entry, Comparable<TestLightWeightCache.IntEntry
			>
		{
			private LightWeightGSet.LinkedElement next;

			internal readonly int id;

			internal readonly int value;

			private long expirationTime = 0;

			internal IntEntry(int id, int value)
			{
				this.id = id;
				this.value = value;
			}

			public override bool Equals(object obj)
			{
				return obj != null && obj is TestLightWeightCache.IntEntry && value == ((TestLightWeightCache.IntEntry
					)obj).value;
			}

			public override int GetHashCode()
			{
				return value;
			}

			public virtual int CompareTo(TestLightWeightCache.IntEntry that)
			{
				return value - that.value;
			}

			public override string ToString()
			{
				return id + "#" + value + ",expirationTime=" + expirationTime;
			}

			public virtual LightWeightGSet.LinkedElement GetNext()
			{
				return next;
			}

			public virtual void SetNext(LightWeightGSet.LinkedElement e)
			{
				next = e;
			}

			public virtual void SetExpirationTime(long timeNano)
			{
				this.expirationTime = timeNano;
			}

			public virtual long GetExpirationTime()
			{
				return expirationTime;
			}
		}
	}
}
