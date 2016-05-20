using Sharpen;

namespace org.apache.hadoop.io
{
	/// <summary>Unit tests for UTF8.</summary>
	public class TestUTF8 : NUnit.Framework.TestCase
	{
		public TestUTF8(string name)
			: base(name)
		{
		}

		private static readonly java.util.Random RANDOM = new java.util.Random();

		/// <exception cref="System.Exception"/>
		public static string getTestString()
		{
			java.lang.StringBuilder buffer = new java.lang.StringBuilder();
			int length = RANDOM.nextInt(100);
			for (int i = 0; i < length; i++)
			{
				buffer.Append((char)(RANDOM.nextInt(char.MaxValue)));
			}
			return buffer.ToString();
		}

		/// <exception cref="System.Exception"/>
		public virtual void testWritable()
		{
			for (int i = 0; i < 10000; i++)
			{
				org.apache.hadoop.io.TestWritable.testWritable(new org.apache.hadoop.io.UTF8(getTestString
					()));
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void testGetBytes()
		{
			for (int i = 0; i < 10000; i++)
			{
				// generate a random string
				string before = getTestString();
				// Check that the bytes are stored correctly in Modified-UTF8 format.
				// Note that the DataInput and DataOutput interfaces convert between
				// bytes and Strings using the Modified-UTF8 format.
				NUnit.Framework.Assert.AreEqual(before, readModifiedUTF(org.apache.hadoop.io.UTF8
					.getBytes(before)));
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private string readModifiedUTF(byte[] bytes)
		{
			short lengthBytes = (short)2;
			java.nio.ByteBuffer bb = java.nio.ByteBuffer.allocate(bytes.Length + lengthBytes);
			bb.putShort((short)bytes.Length).put(bytes);
			java.io.ByteArrayInputStream bis = new java.io.ByteArrayInputStream(((byte[])bb.array
				()));
			java.io.DataInputStream dis = new java.io.DataInputStream(bis);
			return dis.readUTF();
		}

		/// <exception cref="System.Exception"/>
		public virtual void testIO()
		{
			org.apache.hadoop.io.DataOutputBuffer @out = new org.apache.hadoop.io.DataOutputBuffer
				();
			org.apache.hadoop.io.DataInputBuffer @in = new org.apache.hadoop.io.DataInputBuffer
				();
			for (int i = 0; i < 10000; i++)
			{
				// generate a random string
				string before = getTestString();
				// write it
				@out.reset();
				org.apache.hadoop.io.UTF8.writeString(@out, before);
				// test that it reads correctly
				@in.reset(@out.getData(), @out.getLength());
				string after = org.apache.hadoop.io.UTF8.readString(@in);
				NUnit.Framework.Assert.AreEqual(before, after);
				// test that it reads correctly with DataInput
				@in.reset(@out.getData(), @out.getLength());
				string after2 = @in.readUTF();
				NUnit.Framework.Assert.AreEqual(before, after2);
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void testNullEncoding()
		{
			string s = new string(new char[] { 0 });
			org.apache.hadoop.io.DataOutputBuffer dob = new org.apache.hadoop.io.DataOutputBuffer
				();
			new org.apache.hadoop.io.UTF8(s).write(dob);
			NUnit.Framework.Assert.AreEqual(s, Sharpen.Runtime.getStringForBytes(dob.getData(
				), 2, dob.getLength() - 2, "UTF-8"));
		}

		/// <summary>Test encoding and decoding of UTF8 outside the basic multilingual plane.
		/// 	</summary>
		/// <remarks>
		/// Test encoding and decoding of UTF8 outside the basic multilingual plane.
		/// This is a regression test for HADOOP-9103.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void testNonBasicMultilingualPlane()
		{
			// Test using the "CAT FACE" character (U+1F431)
			// See http://www.fileformat.info/info/unicode/char/1f431/index.htm
			string catFace = "\uD83D\uDC31";
			// This encodes to 4 bytes in UTF-8:
			byte[] encoded = Sharpen.Runtime.getBytesForString(catFace, "UTF-8");
			NUnit.Framework.Assert.AreEqual(4, encoded.Length);
			NUnit.Framework.Assert.AreEqual("f09f90b1", org.apache.hadoop.util.StringUtils.byteToHexString
				(encoded));
			// Decode back to String using our own decoder
			string roundTrip = org.apache.hadoop.io.UTF8.fromBytes(encoded);
			NUnit.Framework.Assert.AreEqual(catFace, roundTrip);
		}

		/// <summary>Test that decoding invalid UTF8 throws an appropriate error message.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void testInvalidUTF8()
		{
			byte[] invalid = new byte[] { unchecked((int)(0x01)), unchecked((int)(0x02)), unchecked(
				(byte)unchecked((int)(0xff))), unchecked((byte)unchecked((int)(0xff))), unchecked(
				(int)(0x01)), unchecked((int)(0x02)), unchecked((int)(0x03)), unchecked((int)(0x04
				)), unchecked((int)(0x05)) };
			try
			{
				org.apache.hadoop.io.UTF8.fromBytes(invalid);
				fail("did not throw an exception");
			}
			catch (java.io.UTFDataFormatException utfde)
			{
				org.apache.hadoop.test.GenericTestUtils.assertExceptionContains("Invalid UTF8 at ffff01020304"
					, utfde);
			}
		}

		/// <summary>Test for a 5-byte UTF8 sequence, which is now considered illegal.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void test5ByteUtf8Sequence()
		{
			byte[] invalid = new byte[] { unchecked((int)(0x01)), unchecked((int)(0x02)), unchecked(
				(byte)unchecked((int)(0xf8))), unchecked((byte)unchecked((int)(0x88))), unchecked(
				(byte)unchecked((int)(0x80))), unchecked((byte)unchecked((int)(0x80))), unchecked(
				(byte)unchecked((int)(0x80))), unchecked((int)(0x04)), unchecked((int)(0x05)) };
			try
			{
				org.apache.hadoop.io.UTF8.fromBytes(invalid);
				fail("did not throw an exception");
			}
			catch (java.io.UTFDataFormatException utfde)
			{
				org.apache.hadoop.test.GenericTestUtils.assertExceptionContains("Invalid UTF8 at f88880808004"
					, utfde);
			}
		}

		/// <summary>
		/// Test that decoding invalid UTF8 due to truncation yields the correct
		/// exception type.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void testInvalidUTF8Truncated()
		{
			// Truncated CAT FACE character -- this is a 4-byte sequence, but we
			// only have the first three bytes.
			byte[] truncated = new byte[] { unchecked((byte)unchecked((int)(0xF0))), unchecked(
				(byte)unchecked((int)(0x9F))), unchecked((byte)unchecked((int)(0x90))) };
			try
			{
				org.apache.hadoop.io.UTF8.fromBytes(truncated);
				fail("did not throw an exception");
			}
			catch (java.io.UTFDataFormatException utfde)
			{
				org.apache.hadoop.test.GenericTestUtils.assertExceptionContains("Truncated UTF8 at f09f90"
					, utfde);
			}
		}
	}
}
