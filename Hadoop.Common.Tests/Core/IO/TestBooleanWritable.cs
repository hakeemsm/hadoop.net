using Sharpen;

namespace Org.Apache.Hadoop.IO
{
	public class TestBooleanWritable
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCompareUnequalWritables()
		{
			DataOutputBuffer bTrue = WriteWritable(new BooleanWritable(true));
			DataOutputBuffer bFalse = WriteWritable(new BooleanWritable(false));
			WritableComparator writableComparator = WritableComparator.Get(typeof(BooleanWritable
				));
			NUnit.Framework.Assert.AreEqual(0, Compare(writableComparator, bTrue, bTrue));
			NUnit.Framework.Assert.AreEqual(0, Compare(writableComparator, bFalse, bFalse));
			NUnit.Framework.Assert.AreEqual(1, Compare(writableComparator, bTrue, bFalse));
			NUnit.Framework.Assert.AreEqual(-1, Compare(writableComparator, bFalse, bTrue));
		}

		private int Compare(WritableComparator writableComparator, DataOutputBuffer buf1, 
			DataOutputBuffer buf2)
		{
			return writableComparator.Compare(buf1.GetData(), 0, buf1.Size(), buf2.GetData(), 
				0, buf2.Size());
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual DataOutputBuffer WriteWritable(Writable writable)
		{
			DataOutputBuffer @out = new DataOutputBuffer(1024);
			writable.Write(@out);
			@out.Flush();
			return @out;
		}

		/// <summary>
		/// test
		/// <see cref="BooleanWritable"/>
		/// methods hashCode(), equals(), compareTo()
		/// </summary>
		[NUnit.Framework.Test]
		public virtual void TestCommonMethods()
		{
			NUnit.Framework.Assert.IsTrue("testCommonMethods1 error !!!", NewInstance(true).Equals
				(NewInstance(true)));
			NUnit.Framework.Assert.IsTrue("testCommonMethods2 error  !!!", NewInstance(false)
				.Equals(NewInstance(false)));
			NUnit.Framework.Assert.IsFalse("testCommonMethods3 error !!!", NewInstance(false)
				.Equals(NewInstance(true)));
			NUnit.Framework.Assert.IsTrue("testCommonMethods4 error !!!", CheckHashCode(NewInstance
				(true), NewInstance(true)));
			NUnit.Framework.Assert.IsFalse("testCommonMethods5 error !!! ", CheckHashCode(NewInstance
				(true), NewInstance(false)));
			NUnit.Framework.Assert.IsTrue("testCommonMethods6 error !!!", NewInstance(true).CompareTo
				(NewInstance(false)) > 0);
			NUnit.Framework.Assert.IsTrue("testCommonMethods7 error !!!", NewInstance(false).
				CompareTo(NewInstance(true)) < 0);
			NUnit.Framework.Assert.IsTrue("testCommonMethods8 error !!!", NewInstance(false).
				CompareTo(NewInstance(false)) == 0);
			NUnit.Framework.Assert.AreEqual("testCommonMethods9 error !!!", "true", NewInstance
				(true).ToString());
		}

		private bool CheckHashCode(BooleanWritable f, BooleanWritable s)
		{
			return f.GetHashCode() == s.GetHashCode();
		}

		private static BooleanWritable NewInstance(bool flag)
		{
			return new BooleanWritable(flag);
		}
	}
}
