using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Sharpen;

namespace Org.Apache.Hadoop.Util
{
	/// <summary>
	/// Testing
	/// <see cref="LightWeightGSet{K, E}"/>
	/// 
	/// </summary>
	public class TestLightWeightGSet
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(TestLightWeightGSet));

		private static AList<int> GetRandomList(int length, int randomSeed)
		{
			Random random = new Random(randomSeed);
			AList<int> list = new AList<int>(length);
			for (int i = 0; i < length; i++)
			{
				list.AddItem(random.Next());
			}
			return list;
		}

		private class TestElement : LightWeightGSet.LinkedElement
		{
			private readonly int val;

			private LightWeightGSet.LinkedElement next;

			internal TestElement(int val)
			{
				this.val = val;
				this.next = null;
			}

			public virtual int GetVal()
			{
				return val;
			}

			public virtual void SetNext(LightWeightGSet.LinkedElement next)
			{
				this.next = next;
			}

			public virtual LightWeightGSet.LinkedElement GetNext()
			{
				return next;
			}
		}

		public virtual void TestRemoveAllViaIterator()
		{
			AList<int> list = GetRandomList(100, 123);
			LightWeightGSet<TestLightWeightGSet.TestElement, TestLightWeightGSet.TestElement>
				 set = new LightWeightGSet<TestLightWeightGSet.TestElement, TestLightWeightGSet.TestElement
				>(16);
			foreach (int i in list)
			{
				set.Put(new TestLightWeightGSet.TestElement(i));
			}
			for (IEnumerator<TestLightWeightGSet.TestElement> iter = set.GetEnumerator(); iter
				.HasNext(); )
			{
				iter.Next();
				iter.Remove();
			}
			Assert.Equal(0, set.Size());
		}

		public virtual void TestRemoveSomeViaIterator()
		{
			AList<int> list = GetRandomList(100, 123);
			LightWeightGSet<TestLightWeightGSet.TestElement, TestLightWeightGSet.TestElement>
				 set = new LightWeightGSet<TestLightWeightGSet.TestElement, TestLightWeightGSet.TestElement
				>(16);
			foreach (int i in list)
			{
				set.Put(new TestLightWeightGSet.TestElement(i));
			}
			long sum = 0;
			for (IEnumerator<TestLightWeightGSet.TestElement> iter = set.GetEnumerator(); iter
				.HasNext(); )
			{
				sum += iter.Next().GetVal();
			}
			long mode = sum / set.Size();
			Log.Info("Removing all elements above " + mode);
			for (IEnumerator<TestLightWeightGSet.TestElement> iter_1 = set.GetEnumerator(); iter_1
				.HasNext(); )
			{
				int item = iter_1.Next().GetVal();
				if (item > mode)
				{
					iter_1.Remove();
				}
			}
			for (IEnumerator<TestLightWeightGSet.TestElement> iter_2 = set.GetEnumerator(); iter_2
				.HasNext(); )
			{
				Assert.True(iter_2.Next().GetVal() <= mode);
			}
		}
	}
}
