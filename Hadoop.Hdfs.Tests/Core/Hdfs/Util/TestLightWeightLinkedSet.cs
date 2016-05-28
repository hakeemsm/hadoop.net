using System.Collections.Generic;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Util
{
	public class TestLightWeightLinkedSet
	{
		private static readonly Log Log = LogFactory.GetLog("org.apache.hadoop.hdfs.TestLightWeightLinkedSet"
			);

		private readonly AList<int> list = new AList<int>();

		private readonly int Num = 100;

		private LightWeightLinkedSet<int> set;

		private Random rand;

		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			float maxF = LightWeightLinkedSet.DefaultMaxLoadFactor;
			float minF = LightWeightLinkedSet.DefautMinLoadFactor;
			int initCapacity = LightWeightLinkedSet.MinimumCapacity;
			rand = new Random(Time.Now());
			list.Clear();
			for (int i = 0; i < Num; i++)
			{
				list.AddItem(rand.Next());
			}
			set = new LightWeightLinkedSet<int>(initCapacity, maxF, minF);
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
			// poll should return nothing
			NUnit.Framework.Assert.IsNull(set.PollFirst());
			NUnit.Framework.Assert.AreEqual(0, set.PollAll().Count);
			NUnit.Framework.Assert.AreEqual(0, set.PollN(10).Count);
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
				NUnit.Framework.Assert.AreEqual(list[num++], iter.Next());
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
			// poll should return nothing
			NUnit.Framework.Assert.IsNull(set.PollFirst());
			NUnit.Framework.Assert.AreEqual(0, set.PollAll().Count);
			NUnit.Framework.Assert.AreEqual(0, set.PollN(10).Count);
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
			IEnumerator<int> iter = set.GetEnumerator();
			// the remaining elements should be in order
			int num = Num / 2;
			while (iter.HasNext())
			{
				NUnit.Framework.Assert.AreEqual(list[num++], iter.Next());
			}
			NUnit.Framework.Assert.AreEqual(num, Num);
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
		public virtual void TestPollOneElement()
		{
			Log.Info("Test poll one element");
			set.AddItem(list[0]);
			NUnit.Framework.Assert.AreEqual(list[0], set.PollFirst());
			NUnit.Framework.Assert.IsNull(set.PollFirst());
			Log.Info("Test poll one element - DONE");
		}

		[NUnit.Framework.Test]
		public virtual void TestPollMulti()
		{
			Log.Info("Test poll multi");
			foreach (int i in list)
			{
				NUnit.Framework.Assert.IsTrue(set.AddItem(i));
			}
			// remove half of the elements by polling
			for (int i_1 = 0; i_1 < Num / 2; i_1++)
			{
				NUnit.Framework.Assert.AreEqual(list[i_1], set.PollFirst());
			}
			NUnit.Framework.Assert.AreEqual(Num / 2, set.Count);
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
			IEnumerator<int> iter = set.GetEnumerator();
			// the remaining elements should be in order
			int num = Num / 2;
			while (iter.HasNext())
			{
				NUnit.Framework.Assert.AreEqual(list[num++], iter.Next());
			}
			NUnit.Framework.Assert.AreEqual(num, Num);
			// add elements back
			for (int i_4 = 0; i_4 < Num / 2; i_4++)
			{
				NUnit.Framework.Assert.IsTrue(set.AddItem(list[i_4]));
			}
			// order should be switched
			NUnit.Framework.Assert.AreEqual(Num, set.Count);
			for (int i_5 = Num / 2; i_5 < Num; i_5++)
			{
				NUnit.Framework.Assert.AreEqual(list[i_5], set.PollFirst());
			}
			for (int i_6 = 0; i_6 < Num / 2; i_6++)
			{
				NUnit.Framework.Assert.AreEqual(list[i_6], set.PollFirst());
			}
			NUnit.Framework.Assert.AreEqual(0, set.Count);
			NUnit.Framework.Assert.IsTrue(set.IsEmpty());
			Log.Info("Test poll multi - DONE");
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
			while (set.PollFirst() != null)
			{
			}
			NUnit.Framework.Assert.AreEqual(0, set.Count);
			NUnit.Framework.Assert.IsTrue(set.IsEmpty());
			// the deleted elements should not be there
			for (int i_1 = 0; i_1 < Num; i_1++)
			{
				NUnit.Framework.Assert.IsFalse(set.Contains(list[i_1]));
			}
			IEnumerator<int> iter = set.GetEnumerator();
			NUnit.Framework.Assert.IsFalse(iter.HasNext());
			Log.Info("Test poll all - DONE");
		}

		[NUnit.Framework.Test]
		public virtual void TestPollNOne()
		{
			Log.Info("Test pollN one");
			set.AddItem(list[0]);
			IList<int> l = set.PollN(10);
			NUnit.Framework.Assert.AreEqual(1, l.Count);
			NUnit.Framework.Assert.AreEqual(list[0], l[0]);
			Log.Info("Test pollN one - DONE");
		}

		[NUnit.Framework.Test]
		public virtual void TestPollNMulti()
		{
			Log.Info("Test pollN multi");
			// use addAll
			Sharpen.Collections.AddAll(set, list);
			// poll existing elements
			IList<int> l = set.PollN(10);
			NUnit.Framework.Assert.AreEqual(10, l.Count);
			for (int i = 0; i < 10; i++)
			{
				NUnit.Framework.Assert.AreEqual(list[i], l[i]);
			}
			// poll more elements than present
			l = set.PollN(1000);
			NUnit.Framework.Assert.AreEqual(Num - 10, l.Count);
			// check the order
			for (int i_1 = 10; i_1 < Num; i_1++)
			{
				NUnit.Framework.Assert.AreEqual(list[i_1], l[i_1 - 10]);
			}
			// set is empty
			NUnit.Framework.Assert.IsTrue(set.IsEmpty());
			NUnit.Framework.Assert.AreEqual(0, set.Count);
			Log.Info("Test pollN multi - DONE");
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
			// poll should return an empty list
			NUnit.Framework.Assert.AreEqual(0, set.PollAll().Count);
			NUnit.Framework.Assert.AreEqual(0, set.PollN(10).Count);
			NUnit.Framework.Assert.IsNull(set.PollFirst());
			// iterator should be empty
			IEnumerator<int> iter = set.GetEnumerator();
			NUnit.Framework.Assert.IsFalse(iter.HasNext());
			Log.Info("Test clear - DONE");
		}

		[NUnit.Framework.Test]
		public virtual void TestOther()
		{
			Log.Info("Test other");
			NUnit.Framework.Assert.IsTrue(Sharpen.Collections.AddAll(set, list));
			// to array
			int[] array = Sharpen.Collections.ToArray(set, new int[0]);
			NUnit.Framework.Assert.AreEqual(Num, array.Length);
			for (int i = 0; i < array.Length; i++)
			{
				NUnit.Framework.Assert.IsTrue(list.Contains(array[i]));
			}
			NUnit.Framework.Assert.AreEqual(Num, set.Count);
			// to array
			object[] array2 = Sharpen.Collections.ToArray(set);
			NUnit.Framework.Assert.AreEqual(Num, array2.Length);
			for (int i_1 = 0; i_1 < array2.Length; i_1++)
			{
				NUnit.Framework.Assert.IsTrue(list.Contains(array2[i_1]));
			}
			Log.Info("Test capacity - DONE");
		}
	}
}
