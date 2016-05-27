using System;
using System.Collections.Generic;
using NUnit.Framework;
using Sharpen;

namespace Org.Apache.Hadoop.Util
{
	public class TestGSet
	{
		private static readonly Random ran = new Random();

		private static readonly long starttime = Time.Now();

		private static void Print(object s)
		{
			System.Console.Out.Write(s);
			System.Console.Out.Flush();
		}

		private static void Println(object s)
		{
			System.Console.Out.WriteLine(s);
		}

		[NUnit.Framework.Test]
		public virtual void TestExceptionCases()
		{
			{
				//test contains
				LightWeightGSet<int, int> gset = new LightWeightGSet<int, int>(16);
				try
				{
					//test contains with a null element
					gset.Contains(null);
					NUnit.Framework.Assert.Fail();
				}
				catch (ArgumentNullException e)
				{
					LightWeightGSet.Log.Info("GOOD: getting " + e, e);
				}
			}
			{
				//test get
				LightWeightGSet<int, int> gset = new LightWeightGSet<int, int>(16);
				try
				{
					//test get with a null element
					gset.Get(null);
					NUnit.Framework.Assert.Fail();
				}
				catch (ArgumentNullException e)
				{
					LightWeightGSet.Log.Info("GOOD: getting " + e, e);
				}
			}
			{
				//test put
				LightWeightGSet<int, int> gset = new LightWeightGSet<int, int>(16);
				try
				{
					//test put with a null element
					gset.Put(null);
					NUnit.Framework.Assert.Fail();
				}
				catch (ArgumentNullException e)
				{
					LightWeightGSet.Log.Info("GOOD: getting " + e, e);
				}
				try
				{
					//test putting an element which is not implementing LinkedElement
					gset.Put(1);
					NUnit.Framework.Assert.Fail();
				}
				catch (ArgumentException e)
				{
					LightWeightGSet.Log.Info("GOOD: getting " + e, e);
				}
			}
			{
				//test iterator
				TestGSet.IntElement[] data = new TestGSet.IntElement[5];
				for (int i = 0; i < data.Length; i++)
				{
					data[i] = new TestGSet.IntElement(i, i);
				}
				for (int v = 1; v < data.Length - 1; v++)
				{
					{
						//test remove while iterating
						GSet<TestGSet.IntElement, TestGSet.IntElement> gset = CreateGSet(data);
						foreach (TestGSet.IntElement i_1 in gset)
						{
							if (i_1.value == v)
							{
								//okay because data[0] is not in gset
								gset.Remove(data[0]);
							}
						}
						try
						{
							//exception because data[1] is in gset
							foreach (TestGSet.IntElement i_2 in gset)
							{
								if (i_2.value == v)
								{
									gset.Remove(data[1]);
								}
							}
							NUnit.Framework.Assert.Fail();
						}
						catch (ConcurrentModificationException e)
						{
							LightWeightGSet.Log.Info("GOOD: getting " + e, e);
						}
					}
					{
						//test put new element while iterating
						GSet<TestGSet.IntElement, TestGSet.IntElement> gset = CreateGSet(data);
						try
						{
							foreach (TestGSet.IntElement i_1 in gset)
							{
								if (i_1.value == v)
								{
									gset.Put(data[0]);
								}
							}
							NUnit.Framework.Assert.Fail();
						}
						catch (ConcurrentModificationException e)
						{
							LightWeightGSet.Log.Info("GOOD: getting " + e, e);
						}
					}
					{
						//test put existing element while iterating
						GSet<TestGSet.IntElement, TestGSet.IntElement> gset = CreateGSet(data);
						try
						{
							foreach (TestGSet.IntElement i_1 in gset)
							{
								if (i_1.value == v)
								{
									gset.Put(data[3]);
								}
							}
							NUnit.Framework.Assert.Fail();
						}
						catch (ConcurrentModificationException e)
						{
							LightWeightGSet.Log.Info("GOOD: getting " + e, e);
						}
					}
				}
			}
		}

		private static GSet<TestGSet.IntElement, TestGSet.IntElement> CreateGSet(TestGSet.IntElement
			[] data)
		{
			GSet<TestGSet.IntElement, TestGSet.IntElement> gset = new LightWeightGSet<TestGSet.IntElement
				, TestGSet.IntElement>(8);
			for (int i = 1; i < data.Length; i++)
			{
				gset.Put(data[i]);
			}
			return gset;
		}

		[NUnit.Framework.Test]
		public virtual void TestGSet()
		{
			//The parameters are: table length, data size, modulus.
			Check(new TestGSet.GSetTestCase(1, 1 << 4, 65537));
			Check(new TestGSet.GSetTestCase(17, 1 << 16, 17));
			Check(new TestGSet.GSetTestCase(255, 1 << 10, 65537));
		}

		/// <summary>A long running test with various data sets and parameters.</summary>
		/// <remarks>
		/// A long running test with various data sets and parameters.
		/// It may take ~5 hours,
		/// If you are changing the implementation,
		/// please un-comment the following line in order to run the test.
		/// </remarks>
		public virtual void RunMultipleTestGSet()
		{
			//@Test
			for (int offset = -2; offset <= 2; offset++)
			{
				RunTestGSet(1, offset);
				for (int i = 1; i < int.Size - 1; i++)
				{
					RunTestGSet((1 << i) + 1, offset);
				}
			}
		}

		private static void RunTestGSet(int modulus, int offset)
		{
			Println("\n\nmodulus=" + modulus + ", offset=" + offset);
			for (int i = 0; i <= 16; i += 4)
			{
				int tablelength = (1 << i) + offset;
				int upper = i + 2;
				int steps = Math.Max(1, upper / 3);
				for (int j = 0; j <= upper; j += steps)
				{
					int datasize = 1 << j;
					Check(new TestGSet.GSetTestCase(tablelength, datasize, modulus));
				}
			}
		}

		private static void Check(TestGSet.GSetTestCase test)
		{
			//check add
			Print("  check add .................. ");
			for (int i = 0; i < test.data.Size() / 2; i++)
			{
				test.Put(test.data.Get(i));
			}
			for (int i_1 = 0; i_1 < test.data.Size(); i_1++)
			{
				test.Put(test.data.Get(i_1));
			}
			Println("DONE " + test.Stat());
			//check remove and add
			Print("  check remove & add ......... ");
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
			NUnit.Framework.Assert.AreEqual(0, test.gset.Size());
			Println("DONE " + test.Stat());
			//check remove and add again
			Print("  check remove & add again ... ");
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

		/// <summary>Test cases</summary>
		private class GSetTestCase : GSet<TestGSet.IntElement, TestGSet.IntElement>
		{
			internal readonly GSet<TestGSet.IntElement, TestGSet.IntElement> expected = new GSetByHashMap
				<TestGSet.IntElement, TestGSet.IntElement>(1024, 0.75f);

			internal readonly GSet<TestGSet.IntElement, TestGSet.IntElement> gset;

			internal readonly TestGSet.IntData data;

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

			internal GSetTestCase(int tablelength, int datasize, int modulus)
			{
				denominator = Math.Min((datasize >> 7) + 1, 1 << 16);
				info = GetType().Name + ": tablelength=" + tablelength + ", datasize=" + datasize
					 + ", modulus=" + modulus + ", denominator=" + denominator;
				Println(info);
				data = new TestGSet.IntData(datasize, modulus);
				gset = new LightWeightGSet<TestGSet.IntElement, TestGSet.IntElement>(tablelength);
				NUnit.Framework.Assert.AreEqual(0, gset.Size());
			}

			private bool ContainsTest(TestGSet.IntElement key)
			{
				bool e = expected.Contains(key);
				NUnit.Framework.Assert.AreEqual(e, gset.Contains(key));
				return e;
			}

			public override bool Contains(TestGSet.IntElement key)
			{
				bool e = ContainsTest(key);
				Check();
				return e;
			}

			private TestGSet.IntElement GetTest(TestGSet.IntElement key)
			{
				TestGSet.IntElement e = expected.Get(key);
				NUnit.Framework.Assert.AreEqual(e.id, gset.Get(key).id);
				return e;
			}

			public override TestGSet.IntElement Get(TestGSet.IntElement key)
			{
				TestGSet.IntElement e = GetTest(key);
				Check();
				return e;
			}

			private TestGSet.IntElement PutTest(TestGSet.IntElement element)
			{
				TestGSet.IntElement e = expected.Put(element);
				if (e == null)
				{
					NUnit.Framework.Assert.AreEqual(null, gset.Put(element));
				}
				else
				{
					NUnit.Framework.Assert.AreEqual(e.id, gset.Put(element).id);
				}
				return e;
			}

			public override TestGSet.IntElement Put(TestGSet.IntElement element)
			{
				TestGSet.IntElement e = PutTest(element);
				Check();
				return e;
			}

			private TestGSet.IntElement RemoveTest(TestGSet.IntElement key)
			{
				TestGSet.IntElement e = expected.Remove(key);
				if (e == null)
				{
					NUnit.Framework.Assert.AreEqual(null, gset.Remove(key));
				}
				else
				{
					NUnit.Framework.Assert.AreEqual(e.id, gset.Remove(key).id);
				}
				return e;
			}

			public override TestGSet.IntElement Remove(TestGSet.IntElement key)
			{
				TestGSet.IntElement e = RemoveTest(key);
				Check();
				return e;
			}

			private int SizeTest()
			{
				int s = expected.Size();
				NUnit.Framework.Assert.AreEqual(s, gset.Size());
				return s;
			}

			public override int Size()
			{
				int s = SizeTest();
				Check();
				return s;
			}

			public virtual IEnumerator<TestGSet.IntElement> GetEnumerator()
			{
				throw new NotSupportedException();
			}

			internal virtual void Check()
			{
				//test size
				SizeTest();
				if (ran.Next(denominator) == 0)
				{
					//test get(..), check content and test iterator
					iterate_count++;
					foreach (TestGSet.IntElement i in gset)
					{
						GetTest(i);
					}
				}
				if (ran.Next(denominator) == 0)
				{
					//test contains(..)
					contain_count++;
					int count = Math.Min(data.Size(), 1000);
					if (count == data.Size())
					{
						foreach (TestGSet.IntElement i in data.integers)
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
				expected.Clear();
				gset.Clear();
				NUnit.Framework.Assert.AreEqual(0, Size());
			}
		}

		/// <summary>Test data set</summary>
		private class IntData
		{
			internal readonly TestGSet.IntElement[] integers;

			internal IntData(int size, int modulus)
			{
				integers = new TestGSet.IntElement[size];
				for (int i = 0; i < integers.Length; i++)
				{
					integers[i] = new TestGSet.IntElement(i, ran.Next(modulus));
				}
			}

			internal virtual TestGSet.IntElement Get(int i)
			{
				return integers[i];
			}

			internal virtual int Size()
			{
				return integers.Length;
			}
		}

		/// <summary>
		/// Elements of
		/// <see cref="LightWeightGSet{K, E}"/>
		/// in this test
		/// </summary>
		private class IntElement : LightWeightGSet.LinkedElement, Comparable<TestGSet.IntElement
			>
		{
			private LightWeightGSet.LinkedElement next;

			internal readonly int id;

			internal readonly int value;

			internal IntElement(int id, int value)
			{
				this.id = id;
				this.value = value;
			}

			public override bool Equals(object obj)
			{
				return obj != null && obj is TestGSet.IntElement && value == ((TestGSet.IntElement
					)obj).value;
			}

			public override int GetHashCode()
			{
				return value;
			}

			public virtual int CompareTo(TestGSet.IntElement that)
			{
				return value - that.value;
			}

			public override string ToString()
			{
				return id + "#" + value;
			}

			public virtual LightWeightGSet.LinkedElement GetNext()
			{
				return next;
			}

			public virtual void SetNext(LightWeightGSet.LinkedElement e)
			{
				next = e;
			}
		}

		/// <summary>
		/// Test for
		/// <see cref="LightWeightGSet{K, E}.ComputeCapacity(double, string)"/>
		/// with invalid percent less than 0.
		/// </summary>
		public virtual void TestComputeCapacityNegativePercent()
		{
			LightWeightGSet.ComputeCapacity(1024, -1.0, "testMap");
		}

		/// <summary>
		/// Test for
		/// <see cref="LightWeightGSet{K, E}.ComputeCapacity(double, string)"/>
		/// with invalid percent greater than 100.
		/// </summary>
		public virtual void TestComputeCapacityInvalidPercent()
		{
			LightWeightGSet.ComputeCapacity(1024, 101.0, "testMap");
		}

		/// <summary>
		/// Test for
		/// <see cref="LightWeightGSet{K, E}.ComputeCapacity(double, string)"/>
		/// with invalid negative max memory
		/// </summary>
		public virtual void TestComputeCapacityInvalidMemory()
		{
			LightWeightGSet.ComputeCapacity(-1, 50.0, "testMap");
		}

		private static bool IsPowerOfTwo(int num)
		{
			return num == 0 || (num > 0 && Sharpen.Extensions.BitCount(num) == 1);
		}

		/// <summary>Return capacity as percentage of total memory</summary>
		private static int GetPercent(long total, int capacity)
		{
			// Reference size in bytes
			double referenceSize = Runtime.GetProperty("sun.arch.data.model").Equals("32") ? 
				4.0 : 8.0;
			return (int)(((capacity * referenceSize) / total) * 100.0);
		}

		/// <summary>Return capacity as percentage of total memory</summary>
		private static void TestCapacity(long maxMemory, double percent)
		{
			int capacity = LightWeightGSet.ComputeCapacity(maxMemory, percent, "map");
			LightWeightGSet.Log.Info("Validating - total memory " + maxMemory + " percent " +
				 percent + " returned capacity " + capacity);
			// Returned capacity is zero or power of two
			NUnit.Framework.Assert.IsTrue(IsPowerOfTwo(capacity));
			// Ensure the capacity returned is the nearest to the asked perecentage
			int capacityPercent = GetPercent(maxMemory, capacity);
			if (capacityPercent == percent)
			{
				return;
			}
			else
			{
				if (capacityPercent > percent)
				{
					NUnit.Framework.Assert.IsTrue(GetPercent(maxMemory, capacity * 2) > percent);
				}
				else
				{
					NUnit.Framework.Assert.IsTrue(GetPercent(maxMemory, capacity / 2) < percent);
				}
			}
		}

		/// <summary>
		/// Test for
		/// <see cref="LightWeightGSet{K, E}.ComputeCapacity(double, string)"/>
		/// </summary>
		[NUnit.Framework.Test]
		public virtual void TestComputeCapacity()
		{
			// Tests for boundary conditions where percent or memory are zero
			TestCapacity(0, 0.0);
			TestCapacity(100, 0.0);
			TestCapacity(0, 100.0);
			// Compute capacity for some 100 random max memory and percentage
			Random r = new Random();
			for (int i = 0; i < 100; i++)
			{
				long maxMemory = r.Next(int.MaxValue);
				double percent = r.Next(101);
				TestCapacity(maxMemory, percent);
			}
		}
	}
}
