using System.IO;
using Hadoop.Common.Core.IO;
using Org.Apache.Hadoop.IO;
using Should;
using Xunit;

namespace Hadoop.Common.Tests.Core.IO
{
	public class TestBooleanWritable
	{
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestCompareUnequalWritables()
		{
			var bTrue = WriteWritable(new BooleanWritable(true));
			var bFalse = WriteWritable(new BooleanWritable(false));
			WritableComparator writableComparator = WritableComparator.Get(typeof(BooleanWritable));
            0.ShouldEqual(Compare(writableComparator, bTrue, bTrue));
            0.ShouldEqual(Compare(writableComparator, bTrue, bTrue));
            0.ShouldEqual(Compare(writableComparator, bFalse, bFalse));
			1.ShouldEqual(Compare(writableComparator, bTrue, bFalse));
		    (-1).ShouldEqual(Compare(writableComparator, bFalse, bTrue));
            
		}

		private int Compare(WritableComparator writableComparator, MemoryStream buf1, MemoryStream buf2)
		{
		    return writableComparator.Compare(buf1.GetBuffer(), 0, buf1.Capacity, buf2.GetBuffer(), 0, buf2.Capacity);
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual MemoryStream WriteWritable(IWritable writable)
		{
		    var memoryStream = new MemoryStream() { Capacity = 1024 };
		    var writer = new BinaryWriter(memoryStream);
		    writable.Write(writer);
		    writer.Flush();
			return memoryStream;
		}

		/// <summary>
		/// test
		/// <see cref="BooleanWritable"/>
		/// methods hashCode(), equals(), compareTo()
		/// </summary>
		[Fact]
		public virtual void TestCommonMethods()
		{
            NewInstance(true).ShouldEqual(NewInstance(true), "testCommonMethods1 error !!!");
            NewInstance(false).ShouldEqual(NewInstance(false), "testCommonMethods2 error  !!!");
            NewInstance(false).ShouldEqual(NewInstance(true), "testCommonMethods3 error  !!!");
			CheckHashCode(NewInstance(true), NewInstance(true)).ShouldBeTrue("testCommonMethods4 error !!!");
			CheckHashCode(NewInstance(true), NewInstance(false)).ShouldBeTrue("testCommonMethods5 error !!!");
            NewInstance(true).CompareTo(NewInstance(false)).ShouldBeGreaterThan(0);
		    NewInstance(false).CompareTo(NewInstance(true)).ShouldBeLessThan(0);
		    NewInstance(false).CompareTo(NewInstance(false)).ShouldEqual(0);
            "true".ShouldEqual(NewInstance(true).ToString(), "testCommonMethods9 error !!!");
			
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
