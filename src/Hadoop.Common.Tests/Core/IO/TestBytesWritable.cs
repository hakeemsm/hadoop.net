using System;
using Hadoop.Common.Core.IO;
using Org.Apache.Hadoop.IO;
using Should;
using Should.Core.Assertions;
using Xunit;

namespace Hadoop.Common.Tests.Core.IO
{
	/// <summary>This is the unit test for BytesWritable.</summary>
	public class TestBytesWritable
	{
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestSizeChange()
		{
            var hadoop = GetBytesForString("hadoop");

		    BytesWritable buf = new BytesWritable(hadoop);
			int size = buf.Length;
			int orig_capacity = buf.Capacity;
			buf.Size = (size * 2);
			int newCapacity = buf.Capacity;
			Array.Copy(buf.Bytes, 0, buf.Bytes, size, size);
			newCapacity.ShouldBeGreaterThanOrEqualTo(size * 2);
			(size * 2).ShouldEqual(buf.Length);
            
			Assert.True(newCapacity != orig_capacity);
			buf.Size = (size * 4);
			Assert.True(newCapacity != buf.Capacity);
			for (int i = 0; i < size * 2; ++i)
			{
				Assert.Equal(hadoop[i % size], buf.Bytes[i]);
			}
            // ensure that copyBytes is exactly the right length
            
            Assert.Equal(size * 4, buf.CopyBytes().Length);
			// shrink the buffer
			buf.Capacity = (1);
			// make sure the size has been cut down too
			Assert.Equal(1, buf.Length);
			// but that the data is still there
			Assert.Equal(hadoop[0], buf.Bytes[0]);
		}

	    private static byte[] GetBytesForString(string str)
	    {
	        byte[] bytes = new byte[str.Length*sizeof (char)];
	        Buffer.BlockCopy(str.ToCharArray(), 0, bytes, 0, bytes.Length);
	        return bytes;
	    }

	    /// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestHash()
		{
			byte[] owen = GetBytesForString("owen");
			BytesWritable buf = new BytesWritable(owen);
			Assert.Equal(4347922, buf.GetHashCode());
			buf.Capacity = (10000);
			Assert.Equal(4347922, buf.GetHashCode());
			buf.Size = (0);
			Assert.Equal(1, buf.GetHashCode());
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestCompare()
		{
			byte[][] values = { GetBytesForString("abc"), GetBytesForString
				("ad"), GetBytesForString("abcd"), GetBytesForString
				(string.Empty), GetBytesForString("b") };
			BytesWritable[] buf = new BytesWritable[values.Length];
			for (int i = 0; i < values.Length; ++i)
			{
				buf[i] = new BytesWritable(values[i]);
			}
			// check to make sure the compare function is symetric and reflexive
			for (int i_1 = 0; i_1 < values.Length; ++i_1)
			{
				for (int j = 0; j < values.Length; ++j)
				{
					Assert.True(buf[i_1].CompareTo(buf[j]) == -buf[j].CompareTo(buf
						[i_1]));
					Assert.True((i_1 == j) == (buf[i_1].CompareTo(buf[j]) == 0));
				}
			}
			Assert.True(buf[0].CompareTo(buf[1]) < 0);
			Assert.True(buf[1].CompareTo(buf[2]) > 0);
			Assert.True(buf[2].CompareTo(buf[3]) > 0);
			Assert.True(buf[3].CompareTo(buf[4]) < 0);
		}

		private void CheckToString(byte[] input, string expected)
		{
			string actual = new BytesWritable(input).ToString();
			Assert.Equal(expected, actual);
		}

		[Fact]
		public virtual void TestToString()
		{
			CheckToString(new byte[] { 0, 1, 2, 0x10 }, "00 01 02 10");
			CheckToString(new byte[] { unchecked((byte)(-0x80)), unchecked(
				(byte)(-0x7f)), unchecked((byte)(-0x1)), unchecked(
				(byte)(-0x2)), 1, 0 }, "80 81 ff fe 01 00");
		}

		/// <summary>
		/// This test was written as result of adding the new zero
		/// copy constructor and set method to BytesWritable.
		/// </summary>
		/// <remarks>
		/// This test was written as result of adding the new zero
		/// copy constructor and set method to BytesWritable. These
		/// methods allow users to specify the backing buffer of the
		/// BytesWritable instance and a length.
		/// </remarks>
		[Fact]
		public virtual void TestZeroCopy()
		{
			byte[] bytes = GetBytesForString("brock");
			BytesWritable zeroBuf = new BytesWritable(bytes, bytes.Length);
			// new
			BytesWritable copyBuf = new BytesWritable(bytes);
			// old
			// using zero copy constructor shouldn't result in a copy
			Assert.True(bytes == zeroBuf.Bytes, "copy took place, backing array != array passed to constructor");
			Assert.True(zeroBuf.Length == bytes.Length, "length of BW should backing byte array");
			Assert.Equal(zeroBuf, copyBuf, "objects with same backing array should be equal");
		    Assert.Equal(zeroBuf.ToString(), copyBuf.ToString(),
		        "string repr of objects with same backing array should be equal");
		    Assert.True(zeroBuf.CompareTo(copyBuf) == 0, "compare order objects with same backing array should be equal");
		    Assert.True(zeroBuf.GetHashCode() == copyBuf.GetHashCode(),
		        "hash of objects with same backing array should be equal");
				
			// ensure expanding buffer is handled correctly
			// for buffers created with zero copy api
			byte[] buffer = new byte[bytes.Length * 5];
			zeroBuf.Set(buffer, 0, buffer.Length);
			// expand internal buffer
			zeroBuf.Set(bytes, 0, bytes.Length);
			// set back to normal contents
		    Assert.Equal(zeroBuf, copyBuf, "buffer created with (array, len) has bad contents");
		    Assert.True(zeroBuf.Length == copyBuf.Length, "buffer created with (array, len) has bad length");

		}

		/// <summary>
		/// test
		/// <see cref="ByteWritable"/>
		/// 
		/// methods compareTo(), toString(), equals()
		/// </summary>
		[Fact]
		public virtual void TestObjectCommonMethods()
		{
			byte b = 0x9;
			ByteWritable bw = new ByteWritable();
			bw.Set(b);
			Assert.True(bw.Get() == b, "testSetByteWritable error");
			Assert.True(bw.CompareTo(new ByteWritable(0xA)) < 0, "testSetByteWritable error < 0");
			Assert.True(bw.CompareTo(new ByteWritable(0x8)) > 0, "testSetByteWritable error > 0");
			Assert.True(bw.CompareTo(new ByteWritable(0x9)) == 0, "testSetByteWritable error == 0");
			Assert.True(bw.Equals(new ByteWritable(0x9)), "testSetByteWritable equals error !!!");
			Assert.True(!bw.Equals(new ByteWritable(0xA)), "testSetByteWritable equals error !!!");
			Assert.True(!bw.Equals(new IntWritable(1)), "testSetByteWritable equals error !!!");
			Assert.Equal("testSetByteWritable error ", "9", bw.ToString());
		}
	}
}
