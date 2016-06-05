using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public class TestSortedRanges : TestCase
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestSortedRanges));

		public virtual void TestAdd()
		{
			SortedRanges sr = new SortedRanges();
			sr.Add(new SortedRanges.Range(2, 9));
			NUnit.Framework.Assert.AreEqual(9, sr.GetIndicesCount());
			sr.Add(new SortedRanges.Range(3, 5));
			NUnit.Framework.Assert.AreEqual(9, sr.GetIndicesCount());
			sr.Add(new SortedRanges.Range(7, 1));
			NUnit.Framework.Assert.AreEqual(9, sr.GetIndicesCount());
			sr.Add(new SortedRanges.Range(1, 12));
			NUnit.Framework.Assert.AreEqual(12, sr.GetIndicesCount());
			sr.Add(new SortedRanges.Range(7, 9));
			NUnit.Framework.Assert.AreEqual(15, sr.GetIndicesCount());
			sr.Add(new SortedRanges.Range(31, 10));
			sr.Add(new SortedRanges.Range(51, 10));
			sr.Add(new SortedRanges.Range(66, 10));
			NUnit.Framework.Assert.AreEqual(45, sr.GetIndicesCount());
			sr.Add(new SortedRanges.Range(21, 50));
			NUnit.Framework.Assert.AreEqual(70, sr.GetIndicesCount());
			Log.Debug(sr);
			IEnumerator<long> it = sr.SkipRangeIterator();
			int i = 0;
			NUnit.Framework.Assert.AreEqual(i, it.Next());
			for (i = 16; i < 21; i++)
			{
				NUnit.Framework.Assert.AreEqual(i, it.Next());
			}
			NUnit.Framework.Assert.AreEqual(76, it.Next());
			NUnit.Framework.Assert.AreEqual(77, it.Next());
		}

		public virtual void TestRemove()
		{
			SortedRanges sr = new SortedRanges();
			sr.Add(new SortedRanges.Range(2, 19));
			NUnit.Framework.Assert.AreEqual(19, sr.GetIndicesCount());
			sr.Remove(new SortedRanges.Range(15, 8));
			NUnit.Framework.Assert.AreEqual(13, sr.GetIndicesCount());
			sr.Remove(new SortedRanges.Range(6, 5));
			NUnit.Framework.Assert.AreEqual(8, sr.GetIndicesCount());
			sr.Remove(new SortedRanges.Range(8, 4));
			NUnit.Framework.Assert.AreEqual(7, sr.GetIndicesCount());
			sr.Add(new SortedRanges.Range(18, 5));
			NUnit.Framework.Assert.AreEqual(12, sr.GetIndicesCount());
			sr.Add(new SortedRanges.Range(25, 1));
			NUnit.Framework.Assert.AreEqual(13, sr.GetIndicesCount());
			sr.Remove(new SortedRanges.Range(7, 24));
			NUnit.Framework.Assert.AreEqual(4, sr.GetIndicesCount());
			sr.Remove(new SortedRanges.Range(5, 1));
			NUnit.Framework.Assert.AreEqual(3, sr.GetIndicesCount());
			Log.Debug(sr);
		}
	}
}
