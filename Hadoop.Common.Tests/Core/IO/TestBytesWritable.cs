using Sharpen;

namespace Org.Apache.Hadoop.IO
{
	/// <summary>This is the unit test for BytesWritable.</summary>
	public class TestBytesWritable
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSizeChange()
		{
			byte[] hadoop = Sharpen.Runtime.GetBytesForString("hadoop");
			BytesWritable buf = new BytesWritable(hadoop);
			int size = buf.GetLength();
			int orig_capacity = buf.GetCapacity();
			buf.SetSize(size * 2);
			int new_capacity = buf.GetCapacity();
			System.Array.Copy(buf.GetBytes(), 0, buf.GetBytes(), size, size);
			NUnit.Framework.Assert.IsTrue(new_capacity >= size * 2);
			NUnit.Framework.Assert.AreEqual(size * 2, buf.GetLength());
			NUnit.Framework.Assert.IsTrue(new_capacity != orig_capacity);
			buf.SetSize(size * 4);
			NUnit.Framework.Assert.IsTrue(new_capacity != buf.GetCapacity());
			for (int i = 0; i < size * 2; ++i)
			{
				NUnit.Framework.Assert.AreEqual(hadoop[i % size], buf.GetBytes()[i]);
			}
			// ensure that copyBytes is exactly the right length
			NUnit.Framework.Assert.AreEqual(size * 4, buf.CopyBytes().Length);
			// shrink the buffer
			buf.SetCapacity(1);
			// make sure the size has been cut down too
			NUnit.Framework.Assert.AreEqual(1, buf.GetLength());
			// but that the data is still there
			NUnit.Framework.Assert.AreEqual(hadoop[0], buf.GetBytes()[0]);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestHash()
		{
			byte[] owen = Sharpen.Runtime.GetBytesForString("owen");
			BytesWritable buf = new BytesWritable(owen);
			NUnit.Framework.Assert.AreEqual(4347922, buf.GetHashCode());
			buf.SetCapacity(10000);
			NUnit.Framework.Assert.AreEqual(4347922, buf.GetHashCode());
			buf.SetSize(0);
			NUnit.Framework.Assert.AreEqual(1, buf.GetHashCode());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCompare()
		{
			byte[][] values = new byte[][] { Sharpen.Runtime.GetBytesForString("abc"), Sharpen.Runtime.GetBytesForString
				("ad"), Sharpen.Runtime.GetBytesForString("abcd"), Sharpen.Runtime.GetBytesForString
				(string.Empty), Sharpen.Runtime.GetBytesForString("b") };
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
					NUnit.Framework.Assert.IsTrue(buf[i_1].CompareTo(buf[j]) == -buf[j].CompareTo(buf
						[i_1]));
					NUnit.Framework.Assert.IsTrue((i_1 == j) == (buf[i_1].CompareTo(buf[j]) == 0));
				}
			}
			NUnit.Framework.Assert.IsTrue(buf[0].CompareTo(buf[1]) < 0);
			NUnit.Framework.Assert.IsTrue(buf[1].CompareTo(buf[2]) > 0);
			NUnit.Framework.Assert.IsTrue(buf[2].CompareTo(buf[3]) > 0);
			NUnit.Framework.Assert.IsTrue(buf[3].CompareTo(buf[4]) < 0);
		}

		private void CheckToString(byte[] input, string expected)
		{
			string actual = new BytesWritable(input).ToString();
			NUnit.Framework.Assert.AreEqual(expected, actual);
		}

		[NUnit.Framework.Test]
		public virtual void TestToString()
		{
			CheckToString(new byte[] { 0, 1, 2, unchecked((int)(0x10)) }, "00 01 02 10");
			CheckToString(new byte[] { unchecked((byte)(-unchecked((int)(0x80)))), unchecked(
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
		public virtual void TestZeroCopy()
		{
			byte[] bytes = Sharpen.Runtime.GetBytesForString("brock");
			BytesWritable zeroBuf = new BytesWritable(bytes, bytes.Length);
			// new
			BytesWritable copyBuf = new BytesWritable(bytes);
			// old
			// using zero copy constructor shouldn't result in a copy
			NUnit.Framework.Assert.IsTrue("copy took place, backing array != array passed to constructor"
				, bytes == zeroBuf.GetBytes());
			NUnit.Framework.Assert.IsTrue("length of BW should backing byte array", zeroBuf.GetLength
				() == bytes.Length);
			NUnit.Framework.Assert.AreEqual("objects with same backing array should be equal"
				, zeroBuf, copyBuf);
			NUnit.Framework.Assert.AreEqual("string repr of objects with same backing array should be equal"
				, zeroBuf.ToString(), copyBuf.ToString());
			NUnit.Framework.Assert.IsTrue("compare order objects with same backing array should be equal"
				, zeroBuf.CompareTo(copyBuf) == 0);
			NUnit.Framework.Assert.IsTrue("hash of objects with same backing array should be equal"
				, zeroBuf.GetHashCode() == copyBuf.GetHashCode());
			// ensure expanding buffer is handled correctly
			// for buffers created with zero copy api
			byte[] buffer = new byte[bytes.Length * 5];
			zeroBuf.Set(buffer, 0, buffer.Length);
			// expand internal buffer
			zeroBuf.Set(bytes, 0, bytes.Length);
			// set back to normal contents
			NUnit.Framework.Assert.AreEqual("buffer created with (array, len) has bad contents"
				, zeroBuf, copyBuf);
			NUnit.Framework.Assert.IsTrue("buffer created with (array, len) has bad length", 
				zeroBuf.GetLength() == copyBuf.GetLength());
		}

		/// <summary>
		/// test
		/// <see cref="ByteWritable"/>
		/// 
		/// methods compareTo(), toString(), equals()
		/// </summary>
		[NUnit.Framework.Test]
		public virtual void TestObjectCommonMethods()
		{
			byte b = unchecked((int)(0x9));
			ByteWritable bw = new ByteWritable();
			bw.Set(b);
			NUnit.Framework.Assert.IsTrue("testSetByteWritable error", bw.Get() == b);
			NUnit.Framework.Assert.IsTrue("testSetByteWritable error < 0", bw.CompareTo(new ByteWritable
				(unchecked((byte)unchecked((int)(0xA))))) < 0);
			NUnit.Framework.Assert.IsTrue("testSetByteWritable error > 0", bw.CompareTo(new ByteWritable
				(unchecked((byte)unchecked((int)(0x8))))) > 0);
			NUnit.Framework.Assert.IsTrue("testSetByteWritable error == 0", bw.CompareTo(new 
				ByteWritable(unchecked((byte)unchecked((int)(0x9))))) == 0);
			NUnit.Framework.Assert.IsTrue("testSetByteWritable equals error !!!", bw.Equals(new 
				ByteWritable(unchecked((byte)unchecked((int)(0x9))))));
			NUnit.Framework.Assert.IsTrue("testSetByteWritable equals error !!!", !bw.Equals(
				new ByteWritable(unchecked((byte)unchecked((int)(0xA))))));
			NUnit.Framework.Assert.IsTrue("testSetByteWritable equals error !!!", !bw.Equals(
				new IntWritable(1)));
			NUnit.Framework.Assert.AreEqual("testSetByteWritable error ", "9", bw.ToString());
		}
	}
}
