using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Nfs.Nfs3
{
	public class TestOffsetRange
	{
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestConstructor1()
		{
			new OffsetRange(0, 0);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestConstructor2()
		{
			new OffsetRange(-1, 0);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestConstructor3()
		{
			new OffsetRange(-3, -1);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestConstructor4()
		{
			new OffsetRange(-3, 100);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestCompare()
		{
			OffsetRange r1 = new OffsetRange(0, 1);
			OffsetRange r2 = new OffsetRange(1, 3);
			OffsetRange r3 = new OffsetRange(1, 3);
			OffsetRange r4 = new OffsetRange(3, 4);
			NUnit.Framework.Assert.AreEqual(0, OffsetRange.ReverseComparatorOnMin.Compare(r2, 
				r3));
			NUnit.Framework.Assert.AreEqual(0, OffsetRange.ReverseComparatorOnMin.Compare(r2, 
				r2));
			NUnit.Framework.Assert.IsTrue(OffsetRange.ReverseComparatorOnMin.Compare(r2, r1) 
				< 0);
			NUnit.Framework.Assert.IsTrue(OffsetRange.ReverseComparatorOnMin.Compare(r2, r4) 
				> 0);
		}
	}
}
