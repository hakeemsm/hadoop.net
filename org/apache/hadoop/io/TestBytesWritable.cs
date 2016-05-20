using Sharpen;

namespace org.apache.hadoop.io
{
	/// <summary>This is the unit test for BytesWritable.</summary>
	public class TestBytesWritable
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testSizeChange()
		{
			byte[] hadoop = Sharpen.Runtime.getBytesForString("hadoop");
			org.apache.hadoop.io.BytesWritable buf = new org.apache.hadoop.io.BytesWritable(hadoop
				);
			int size = buf.getLength();
			int orig_capacity = buf.getCapacity();
			buf.setSize(size * 2);
			int new_capacity = buf.getCapacity();
			System.Array.Copy(buf.getBytes(), 0, buf.getBytes(), size, size);
			NUnit.Framework.Assert.IsTrue(new_capacity >= size * 2);
			NUnit.Framework.Assert.AreEqual(size * 2, buf.getLength());
			NUnit.Framework.Assert.IsTrue(new_capacity != orig_capacity);
			buf.setSize(size * 4);
			NUnit.Framework.Assert.IsTrue(new_capacity != buf.getCapacity());
			for (int i = 0; i < size * 2; ++i)
			{
				NUnit.Framework.Assert.AreEqual(hadoop[i % size], buf.getBytes()[i]);
			}
			// ensure that copyBytes is exactly the right length
			NUnit.Framework.Assert.AreEqual(size * 4, buf.copyBytes().Length);
			// shrink the buffer
			buf.setCapacity(1);
			// make sure the size has been cut down too
			NUnit.Framework.Assert.AreEqual(1, buf.getLength());
			// but that the data is still there
			NUnit.Framework.Assert.AreEqual(hadoop[0], buf.getBytes()[0]);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testHash()
		{
			byte[] owen = Sharpen.Runtime.getBytesForString("owen");
			org.apache.hadoop.io.BytesWritable buf = new org.apache.hadoop.io.BytesWritable(owen
				);
			NUnit.Framework.Assert.AreEqual(4347922, buf.GetHashCode());
			buf.setCapacity(10000);
			NUnit.Framework.Assert.AreEqual(4347922, buf.GetHashCode());
			buf.setSize(0);
			NUnit.Framework.Assert.AreEqual(1, buf.GetHashCode());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testCompare()
		{
			byte[][] values = new byte[][] { Sharpen.Runtime.getBytesForString("abc"), Sharpen.Runtime.getBytesForString
				("ad"), Sharpen.Runtime.getBytesForString("abcd"), Sharpen.Runtime.getBytesForString
				(string.Empty), Sharpen.Runtime.getBytesForString("b") };
			org.apache.hadoop.io.BytesWritable[] buf = new org.apache.hadoop.io.BytesWritable
				[values.Length];
			for (int i = 0; i < values.Length; ++i)
			{
				buf[i] = new org.apache.hadoop.io.BytesWritable(values[i]);
			}
			// check to make sure the compare function is symetric and reflexive
			for (int i_1 = 0; i_1 < values.Length; ++i_1)
			{
				for (int j = 0; j < values.Length; ++j)
				{
					NUnit.Framework.Assert.IsTrue(buf[i_1].compareTo(buf[j]) == -buf[j].compareTo(buf
						[i_1]));
					NUnit.Framework.Assert.IsTrue((i_1 == j) == (buf[i_1].compareTo(buf[j]) == 0));
				}
			}
			NUnit.Framework.Assert.IsTrue(buf[0].compareTo(buf[1]) < 0);
			NUnit.Framework.Assert.IsTrue(buf[1].compareTo(buf[2]) > 0);
			NUnit.Framework.Assert.IsTrue(buf[2].compareTo(buf[3]) > 0);
			NUnit.Framework.Assert.IsTrue(buf[3].compareTo(buf[4]) < 0);
		}

		private void checkToString(byte[] input, string expected)
		{
			string actual = new org.apache.hadoop.io.BytesWritable(input).ToString();
			NUnit.Framework.Assert.AreEqual(expected, actual);
		}

		[NUnit.Framework.Test]
		public virtual void testToString()
		{
			checkToString(new byte[] { 0, 1, 2, unchecked((int)(0x10)) }, "00 01 02 10");
			checkToString(new byte[] { unchecked((byte)(-unchecked((int)(0x80)))), unchecked(
				(byte)(-unchecked((int)(0x7f)))), unchecked((byte)(-unchecked((int)(0x1)))), unchecked(
				(byte)(-unchecked((int)(0x2)))), 1, 0 }, "80 81 ff fe 01 00");
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
		[NUnit.Framework.Test]
		public virtual void testZeroCopy()
		{
			byte[] bytes = Sharpen.Runtime.getBytesForString("brock");
			org.apache.hadoop.io.BytesWritable zeroBuf = new org.apache.hadoop.io.BytesWritable
				(bytes, bytes.Length);
			// new
			org.apache.hadoop.io.BytesWritable copyBuf = new org.apache.hadoop.io.BytesWritable
				(bytes);
			// old
			// using zero copy constructor shouldn't result in a copy
			NUnit.Framework.Assert.IsTrue("copy took place, backing array != array passed to constructor"
				, bytes == zeroBuf.getBytes());
			NUnit.Framework.Assert.IsTrue("length of BW should backing byte array", zeroBuf.getLength
				() == bytes.Length);
			NUnit.Framework.Assert.AreEqual("objects with same backing array should be equal"
				, zeroBuf, copyBuf);
			NUnit.Framework.Assert.AreEqual("string repr of objects with same backing array should be equal"
				, zeroBuf.ToString(), copyBuf.ToString());
			NUnit.Framework.Assert.IsTrue("compare order objects with same backing array should be equal"
				, zeroBuf.compareTo(copyBuf) == 0);
			NUnit.Framework.Assert.IsTrue("hash of objects with same backing array should be equal"
				, zeroBuf.GetHashCode() == copyBuf.GetHashCode());
			// ensure expanding buffer is handled correctly
			// for buffers created with zero copy api
			byte[] buffer = new byte[bytes.Length * 5];
			zeroBuf.set(buffer, 0, buffer.Length);
			// expand internal buffer
			zeroBuf.set(bytes, 0, bytes.Length);
			// set back to normal contents
			NUnit.Framework.Assert.AreEqual("buffer created with (array, len) has bad contents"
				, zeroBuf, copyBuf);
			NUnit.Framework.Assert.IsTrue("buffer created with (array, len) has bad length", 
				zeroBuf.getLength() == copyBuf.getLength());
		}

		/// <summary>
		/// test
		/// <see cref="ByteWritable"/>
		/// 
		/// methods compareTo(), toString(), equals()
		/// </summary>
		[NUnit.Framework.Test]
		public virtual void testObjectCommonMethods()
		{
			byte b = unchecked((int)(0x9));
			org.apache.hadoop.io.ByteWritable bw = new org.apache.hadoop.io.ByteWritable();
			bw.set(b);
			NUnit.Framework.Assert.IsTrue("testSetByteWritable error", bw.get() == b);
			NUnit.Framework.Assert.IsTrue("testSetByteWritable error < 0", bw.compareTo(new org.apache.hadoop.io.ByteWritable
				(unchecked((byte)unchecked((int)(0xA))))) < 0);
			NUnit.Framework.Assert.IsTrue("testSetByteWritable error > 0", bw.compareTo(new org.apache.hadoop.io.ByteWritable
				(unchecked((byte)unchecked((int)(0x8))))) > 0);
			NUnit.Framework.Assert.IsTrue("testSetByteWritable error == 0", bw.compareTo(new 
				org.apache.hadoop.io.ByteWritable(unchecked((byte)unchecked((int)(0x9))))) == 0);
			NUnit.Framework.Assert.IsTrue("testSetByteWritable equals error !!!", bw.Equals(new 
				org.apache.hadoop.io.ByteWritable(unchecked((byte)unchecked((int)(0x9))))));
			NUnit.Framework.Assert.IsTrue("testSetByteWritable equals error !!!", !bw.Equals(
				new org.apache.hadoop.io.ByteWritable(unchecked((byte)unchecked((int)(0xA))))));
			NUnit.Framework.Assert.IsTrue("testSetByteWritable equals error !!!", !bw.Equals(
				new org.apache.hadoop.io.IntWritable(1)));
			NUnit.Framework.Assert.AreEqual("testSetByteWritable error ", "9", bw.ToString());
		}
	}
}
