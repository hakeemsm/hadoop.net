using System;
using System.Collections.Generic;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Util
{
	public class TestLightWeightHashSet
	{
		private static readonly Log Log = LogFactory.GetLog("org.apache.hadoop.hdfs.TestLightWeightHashSet"
			);

		private readonly AList<int> list = new AList<int>();

		private readonly int Num = 100;

		private LightWeightHashSet<int> set;

		private Random rand;

		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			float maxF = LightWeightHashSet.DefaultMaxLoadFactor;
			float minF = LightWeightHashSet.DefautMinLoadFactor;
			int initCapacity = LightWeightHashSet.MinimumCapacity;
			rand = new Random(Time.Now());
			list.Clear();
			for (int i = 0; i < Num; i++)
			{
				list.AddItem(rand.Next());
			}
			set = new LightWeightHashSet<int>(initCapacity, maxF, minF);
		}

		[NUnit.Framework.Test]
		public virtual void TestEmptyBasic()
		{
			Log.Info("Test empty basic");
			IEnumerator<int> iter = set.GetEnumerator();
			// iterator should not have next
			NUnit.Framework.Assert.IsFalse(iter.HasNext());
			NUnit.Framework.Assert.AreEqual(0, set.Count);
			NUnit.Framework.Assert.IsTrue(set.IsEmpty());
			Log.Info("Test empty - DONE");
		}

		[NUnit.Framework.Test]
		public virtual void TestOneElementBasic()
		{
			Log.Info("Test one element basic");
			set.AddItem(list[0]);
			// set should be non-empty
			NUnit.Framework.Assert.AreEqual(1, set.Count);
			NUnit.Framework.Assert.IsFalse(set.IsEmpty());
			// iterator should have next
			IEnumerator<int> iter = set.GetEnumerator();
			NUnit.Framework.Assert.IsTrue(iter.HasNext());
			// iterator should not have next
			NUnit.Framework.Assert.AreEqual(list[0], iter.Next());
			NUnit.Framework.Assert.IsFalse(iter.HasNext());
			Log.Info("Test one element basic - DONE");
		}

		[NUnit.Framework.Test]
		public virtual void TestMultiBasic()
		{
			Log.Info("Test multi element basic");
			// add once
			foreach (int i in list)
			{
				NUnit.Framework.Assert.IsTrue(set.AddItem(i));
			}
			NUnit.Framework.Assert.AreEqual(list.Count, set.Count);
			// check if the elements are in the set
			foreach (int i_1 in list)
			{
				NUnit.Framework.Assert.IsTrue(set.Contains(i_1));
			}
			// add again - should return false each time
			foreach (int i_2 in list)
			{
				NUnit.Framework.Assert.IsFalse(set.AddItem(i_2));
			}
			// check again if the elements are there
			foreach (int i_3 in list)
			{
				NUnit.Framework.Assert.IsTrue(set.Contains(i_3));
			}
			IEnumerator<int> iter = set.GetEnumerator();
			int num = 0;
			while (iter.HasNext())
			{
				int next = iter.Next();
				NUnit.Framework.Assert.IsNotNull(next);
				NUnit.Framework.Assert.IsTrue(list.Contains(next));
				num++;
			}
			// check the number of element from the iterator
			NUnit.Framework.Assert.AreEqual(list.Count, num);
			Log.Info("Test multi element basic - DONE");
		}

		[NUnit.Framework.Test]
		public virtual void TestRemoveOne()
		{
			Log.Info("Test remove one");
			NUnit.Framework.Assert.IsTrue(set.AddItem(list[0]));
			NUnit.Framework.Assert.AreEqual(1, set.Count);
			// remove from the head/tail
			NUnit.Framework.Assert.IsTrue(set.Remove(list[0]));
			NUnit.Framework.Assert.AreEqual(0, set.Count);
			// check the iterator
			IEnumerator<int> iter = set.GetEnumerator();
			NUnit.Framework.Assert.IsFalse(iter.HasNext());
			// add the element back to the set
			NUnit.Framework.Assert.IsTrue(set.AddItem(list[0]));
			NUnit.Framework.Assert.AreEqual(1, set.Count);
			iter = set.GetEnumerator();
			NUnit.Framework.Assert.IsTrue(iter.HasNext());
			Log.Info("Test remove one - DONE");
		}

		[NUnit.Framework.Test]
		public virtual void TestRemoveMulti()
		{
			Log.Info("Test remove multi");
			foreach (int i in list)
			{
				NUnit.Framework.Assert.IsTrue(set.AddItem(i));
			}
			for (int i_1 = 0; i_1 < Num / 2; i_1++)
			{
				NUnit.Framework.Assert.IsTrue(set.Remove(list[i_1]));
			}
			// the deleted elements should not be there
			for (int i_2 = 0; i_2 < Num / 2; i_2++)
			{
				NUnit.Framework.Assert.IsFalse(set.Contains(list[i_2]));
			}
			// the rest should be there
			for (int i_3 = Num / 2; i_3 < Num; i_3++)
			{
				NUnit.Framework.Assert.IsTrue(set.Contains(list[i_3]));
			}
			Log.Info("Test remove multi - DONE");
		}

		[NUnit.Framework.Test]
		public virtual void TestRemoveAll()
		{
			Log.Info("Test remove all");
			foreach (int i in list)
			{
				NUnit.Framework.Assert.IsTrue(set.AddItem(i));
			}
			for (int i_1 = 0; i_1 < Num; i_1++)
			{
				NUnit.Framework.Assert.IsTrue(set.Remove(list[i_1]));
			}
			// the deleted elements should not be there
			for (int i_2 = 0; i_2 < Num; i_2++)
			{
				NUnit.Framework.Assert.IsFalse(set.Contains(list[i_2]));
			}
			// iterator should not have next
			IEnumerator<int> iter = set.GetEnumerator();
			NUnit.Framework.Assert.IsFalse(iter.HasNext());
			NUnit.Framework.Assert.IsTrue(set.IsEmpty());
			Log.Info("Test remove all - DONE");
		}

		[NUnit.Framework.Test]
		public virtual void TestPollAll()
		{
			Log.Info("Test poll all");
			foreach (int i in list)
			{
				NUnit.Framework.Assert.IsTrue(set.AddItem(i));
			}
			// remove all elements by polling
			IList<int> poll = set.PollAll();
			NUnit.Framework.Assert.AreEqual(0, set.Count);
			NUnit.Framework.Assert.IsTrue(set.IsEmpty());
			// the deleted elements should not be there
			for (int i_1 = 0; i_1 < Num; i_1++)
			{
				NUnit.Framework.Assert.IsFalse(set.Contains(list[i_1]));
			}
			// we should get all original items
			foreach (int i_2 in poll)
			{
				NUnit.Framework.Assert.IsTrue(list.Contains(i_2));
			}
			IEnumerator<int> iter = set.GetEnumerator();
			NUnit.Framework.Assert.IsFalse(iter.HasNext());
			Log.Info("Test poll all - DONE");
		}

		[NUnit.Framework.Test]
		public virtual void TestPollNMulti()
		{
			Log.Info("Test pollN multi");
			// use addAll
			Sharpen.Collections.AddAll(set, list);
			// poll zero
			IList<int> poll = set.PollN(0);
			NUnit.Framework.Assert.AreEqual(0, poll.Count);
			foreach (int i in list)
			{
				NUnit.Framework.Assert.IsTrue(set.Contains(i));
			}
			// poll existing elements (less than size)
			poll = set.PollN(10);
			NUnit.Framework.Assert.AreEqual(10, poll.Count);
			foreach (int i_1 in poll)
			{
				// should be in original items
				NUnit.Framework.Assert.IsTrue(list.Contains(i_1));
				// should not be in the set anymore
				NUnit.Framework.Assert.IsFalse(set.Contains(i_1));
			}
			// poll more elements than present
			poll = set.PollN(1000);
			NUnit.Framework.Assert.AreEqual(Num - 10, poll.Count);
			foreach (int i_2 in poll)
			{
				// should be in original items
				NUnit.Framework.Assert.IsTrue(list.Contains(i_2));
			}
			// set is empty
			NUnit.Framework.Assert.IsTrue(set.IsEmpty());
			NUnit.Framework.Assert.AreEqual(0, set.Count);
			Log.Info("Test pollN multi - DONE");
		}

		[NUnit.Framework.Test]
		public virtual void TestPollNMultiArray()
		{
			Log.Info("Test pollN multi array");
			// use addAll
			Sharpen.Collections.AddAll(set, list);
			// poll existing elements (less than size)
			int[] poll = new int[10];
			poll = set.PollToArray(poll);
			NUnit.Framework.Assert.AreEqual(10, poll.Length);
			foreach (int i in poll)
			{
				// should be in original items
				NUnit.Framework.Assert.IsTrue(list.Contains(i));
				// should not be in the set anymore
				NUnit.Framework.Assert.IsFalse(set.Contains(i));
			}
			// poll other elements (more than size)
			poll = new int[Num];
			poll = set.PollToArray(poll);
			NUnit.Framework.Assert.AreEqual(Num - 10, poll.Length);
			for (int i_1 = 0; i_1 < Num - 10; i_1++)
			{
				NUnit.Framework.Assert.IsTrue(list.Contains(poll[i_1]));
			}
			// set is empty
			NUnit.Framework.Assert.IsTrue(set.IsEmpty());
			NUnit.Framework.Assert.AreEqual(0, set.Count);
			// //////
			Sharpen.Collections.AddAll(set, list);
			// poll existing elements (exactly the size)
			poll = new int[Num];
			poll = set.PollToArray(poll);
			NUnit.Framework.Assert.IsTrue(set.IsEmpty());
			NUnit.Framework.Assert.AreEqual(0, set.Count);
			NUnit.Framework.Assert.AreEqual(Num, poll.Length);
			for (int i_2 = 0; i_2 < Num; i_2++)
			{
				NUnit.Framework.Assert.IsTrue(list.Contains(poll[i_2]));
			}
			// //////
			// //////
			Sharpen.Collections.AddAll(set, list);
			// poll existing elements (exactly the size)
			poll = new int[0];
			poll = set.PollToArray(poll);
			for (int i_3 = 0; i_3 < Num; i_3++)
			{
				NUnit.Framework.Assert.IsTrue(set.Contains(list[i_3]));
			}
			NUnit.Framework.Assert.AreEqual(0, poll.Length);
			// //////
			Log.Info("Test pollN multi array- DONE");
		}

		[NUnit.Framework.Test]
		public virtual void TestClear()
		{
			Log.Info("Test clear");
			// use addAll
			Sharpen.Collections.AddAll(set, list);
			NUnit.Framework.Assert.AreEqual(Num, set.Count);
			NUnit.Framework.Assert.IsFalse(set.IsEmpty());
			// clear the set
			set.Clear();
			NUnit.Framework.Assert.AreEqual(0, set.Count);
			NUnit.Framework.Assert.IsTrue(set.IsEmpty());
			// iterator should be empty
			IEnumerator<int> iter = set.GetEnumerator();
			NUnit.Framework.Assert.IsFalse(iter.HasNext());
			Log.Info("Test clear - DONE");
		}

		[NUnit.Framework.Test]
		public virtual void TestCapacity()
		{
			Log.Info("Test capacity");
			float maxF = LightWeightHashSet.DefaultMaxLoadFactor;
			float minF = LightWeightHashSet.DefautMinLoadFactor;
			// capacity lower than min_capacity
			set = new LightWeightHashSet<int>(1, maxF, minF);
			NUnit.Framework.Assert.AreEqual(LightWeightHashSet.MinimumCapacity, set.GetCapacity
				());
			// capacity not a power of two
			set = new LightWeightHashSet<int>(30, maxF, minF);
			NUnit.Framework.Assert.AreEqual(Math.Max(LightWeightHashSet.MinimumCapacity, 32), 
				set.GetCapacity());
			// capacity valid
			set = new LightWeightHashSet<int>(64, maxF, minF);
			NUnit.Framework.Assert.AreEqual(Math.Max(LightWeightHashSet.MinimumCapacity, 64), 
				set.GetCapacity());
			// add NUM elements
			Sharpen.Collections.AddAll(set, list);
			int expCap = LightWeightHashSet.MinimumCapacity;
			while (expCap < Num && maxF * expCap < Num)
			{
				expCap <<= 1;
			}
			NUnit.Framework.Assert.AreEqual(expCap, set.GetCapacity());
			// see if the set shrinks if we remove elements by removing
			set.Clear();
			Sharpen.Collections.AddAll(set, list);
			int toRemove = set.Count - (int)(set.GetCapacity() * minF) + 1;
			for (int i = 0; i < toRemove; i++)
			{
				set.Remove(list[i]);
			}
			NUnit.Framework.Assert.AreEqual(Math.Max(LightWeightHashSet.MinimumCapacity, expCap
				 / 2), set.GetCapacity());
			Log.Info("Test capacity - DONE");
		}

		[NUnit.Framework.Test]
		public virtual void TestOther()
		{
			Log.Info("Test other");
			// remove all
			NUnit.Framework.Assert.IsTrue(Sharpen.Collections.AddAll(set, list));
			NUnit.Framework.Assert.IsTrue(set.RemoveAll(list));
			NUnit.Framework.Assert.IsTrue(set.IsEmpty());
			// remove sublist
			IList<int> sub = new List<int>();
			for (int i = 0; i < 10; i++)
			{
				sub.AddItem(list[i]);
			}
			NUnit.Framework.Assert.IsTrue(Sharpen.Collections.AddAll(set, list));
			NUnit.Framework.Assert.IsTrue(set.RemoveAll(sub));
			NUnit.Framework.Assert.IsFalse(set.IsEmpty());
			NUnit.Framework.Assert.AreEqual(Num - 10, set.Count);
			foreach (int i_1 in sub)
			{
				NUnit.Framework.Assert.IsFalse(set.Contains(i_1));
			}
			NUnit.Framework.Assert.IsFalse(set.ContainsAll(sub));
			// the rest of the elements should be there
			IList<int> sub2 = new List<int>();
			for (int i_2 = 10; i_2 < Num; i_2++)
			{
				sub2.AddItem(list[i_2]);
			}
			NUnit.Framework.Assert.IsTrue(set.ContainsAll(sub2));
			// to array
			int[] array = Sharpen.Collections.ToArray(set, new int[0]);
			NUnit.Framework.Assert.AreEqual(Num - 10, array.Length);
			for (int i_3 = 0; i_3 < array.Length; i_3++)
			{
				NUnit.Framework.Assert.IsTrue(sub2.Contains(array[i_3]));
			}
			NUnit.Framework.Assert.AreEqual(Num - 10, set.Count);
			// to array
			object[] array2 = Sharpen.Collections.ToArray(set);
			NUnit.Framework.Assert.AreEqual(Num - 10, array2.Length);
			for (int i_4 = 0; i_4 < array2.Length; i_4++)
			{
				NUnit.Framework.Assert.IsTrue(sub2.Contains(array2[i_4]));
			}
			Log.Info("Test other - DONE");
		}

		[NUnit.Framework.Test]
		public virtual void TestGetElement()
		{
			LightWeightHashSet<TestLightWeightHashSet.TestObject> objSet = new LightWeightHashSet
				<TestLightWeightHashSet.TestObject>();
			TestLightWeightHashSet.TestObject objA = new TestLightWeightHashSet.TestObject("object A"
				);
			TestLightWeightHashSet.TestObject equalToObjA = new TestLightWeightHashSet.TestObject
				("object A");
			TestLightWeightHashSet.TestObject objB = new TestLightWeightHashSet.TestObject("object B"
				);
			objSet.AddItem(objA);
			objSet.AddItem(objB);
			NUnit.Framework.Assert.AreSame(objA, objSet.GetElement(objA));
			NUnit.Framework.Assert.AreSame(objA, objSet.GetElement(equalToObjA));
			NUnit.Framework.Assert.AreSame(objB, objSet.GetElement(objB));
			NUnit.Framework.Assert.IsNull(objSet.GetElement(new TestLightWeightHashSet.TestObject
				("not in set")));
		}

		/// <summary>
		/// Wrapper class which is used in
		/// <see cref="TestLightWeightHashSet.TestGetElement()"/>
		/// </summary>
		private class TestObject
		{
			private readonly string value;

			public TestObject(string value)
				: base()
			{
				this.value = value;
			}

			public override int GetHashCode()
			{
				return value.GetHashCode();
			}

			public override bool Equals(object obj)
			{
				if (this == obj)
				{
					return true;
				}
				if (obj == null)
				{
					return false;
				}
				if (GetType() != obj.GetType())
				{
					return false;
				}
				TestLightWeightHashSet.TestObject other = (TestLightWeightHashSet.TestObject)obj;
				return this.value.Equals(other.value);
			}
		}
	}
}
