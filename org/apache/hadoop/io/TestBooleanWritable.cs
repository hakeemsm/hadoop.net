using Sharpen;

namespace org.apache.hadoop.io
{
	public class TestBooleanWritable
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testCompareUnequalWritables()
		{
			org.apache.hadoop.io.DataOutputBuffer bTrue = writeWritable(new org.apache.hadoop.io.BooleanWritable
				(true));
			org.apache.hadoop.io.DataOutputBuffer bFalse = writeWritable(new org.apache.hadoop.io.BooleanWritable
				(false));
			org.apache.hadoop.io.WritableComparator writableComparator = org.apache.hadoop.io.WritableComparator
				.get(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.BooleanWritable
				)));
			NUnit.Framework.Assert.AreEqual(0, compare(writableComparator, bTrue, bTrue));
			NUnit.Framework.Assert.AreEqual(0, compare(writableComparator, bFalse, bFalse));
			NUnit.Framework.Assert.AreEqual(1, compare(writableComparator, bTrue, bFalse));
			NUnit.Framework.Assert.AreEqual(-1, compare(writableComparator, bFalse, bTrue));
		}

		private int compare(org.apache.hadoop.io.WritableComparator writableComparator, org.apache.hadoop.io.DataOutputBuffer
			 buf1, org.apache.hadoop.io.DataOutputBuffer buf2)
		{
			return writableComparator.compare(buf1.getData(), 0, buf1.size(), buf2.getData(), 
				0, buf2.size());
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual org.apache.hadoop.io.DataOutputBuffer writeWritable(org.apache.hadoop.io.Writable
			 writable)
		{
			org.apache.hadoop.io.DataOutputBuffer @out = new org.apache.hadoop.io.DataOutputBuffer
				(1024);
			writable.write(@out);
			@out.flush();
			return @out;
		}

		/// <summary>
		/// test
		/// <see cref="BooleanWritable"/>
		/// methods hashCode(), equals(), compareTo()
		/// </summary>
		[NUnit.Framework.Test]
		public virtual void testCommonMethods()
		{
			NUnit.Framework.Assert.IsTrue("testCommonMethods1 error !!!", newInstance(true).Equals
				(newInstance(true)));
			NUnit.Framework.Assert.IsTrue("testCommonMethods2 error  !!!", newInstance(false)
				.Equals(newInstance(false)));
			NUnit.Framework.Assert.IsFalse("testCommonMethods3 error !!!", newInstance(false)
				.Equals(newInstance(true)));
			NUnit.Framework.Assert.IsTrue("testCommonMethods4 error !!!", checkHashCode(newInstance
				(true), newInstance(true)));
			NUnit.Framework.Assert.IsFalse("testCommonMethods5 error !!! ", checkHashCode(newInstance
				(true), newInstance(false)));
			NUnit.Framework.Assert.IsTrue("testCommonMethods6 error !!!", newInstance(true).compareTo
				(newInstance(false)) > 0);
			NUnit.Framework.Assert.IsTrue("testCommonMethods7 error !!!", newInstance(false).
				compareTo(newInstance(true)) < 0);
			NUnit.Framework.Assert.IsTrue("testCommonMethods8 error !!!", newInstance(false).
				compareTo(newInstance(false)) == 0);
			NUnit.Framework.Assert.AreEqual("testCommonMethods9 error !!!", "true", newInstance
				(true).ToString());
		}

		private bool checkHashCode(org.apache.hadoop.io.BooleanWritable f, org.apache.hadoop.io.BooleanWritable
			 s)
		{
			return f.GetHashCode() == s.GetHashCode();
		}

		private static org.apache.hadoop.io.BooleanWritable newInstance(bool flag)
		{
			return new org.apache.hadoop.io.BooleanWritable(flag);
		}
	}
}
