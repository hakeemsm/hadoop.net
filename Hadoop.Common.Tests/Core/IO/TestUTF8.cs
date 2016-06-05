using System.IO;
using System.Text;
using NUnit.Framework;
using Org.Apache.Hadoop.Test;
using Org.Apache.Hadoop.Util;


namespace Org.Apache.Hadoop.IO
{
	/// <summary>Unit tests for UTF8.</summary>
	public class TestUTF8 : TestCase
	{
		public TestUTF8(string name)
			: base(name)
		{
		}

		private static readonly Random Random = new Random();

		/// <exception cref="System.Exception"/>
		public static string GetTestString()
		{
			StringBuilder buffer = new StringBuilder();
			int length = Random.Next(100);
			for (int i = 0; i < length; i++)
			{
				buffer.Append((char)(Random.Next(char.MaxValue)));
			}
			return buffer.ToString();
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestWritable()
		{
			for (int i = 0; i < 10000; i++)
			{
				Org.Apache.Hadoop.IO.TestWritable.TestWritable(new UTF8(GetTestString()));
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestGetBytes()
		{
			for (int i = 0; i < 10000; i++)
			{
				// generate a random string
				string before = GetTestString();
				// Check that the bytes are stored correctly in Modified-UTF8 format.
				// Note that the BinaryReader and BinaryWriter interfaces convert between
				// bytes and Strings using the Modified-UTF8 format.
				Assert.Equal(before, ReadModifiedUTF(UTF8.GetBytes(before)));
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private string ReadModifiedUTF(byte[] bytes)
		{
			short lengthBytes = (short)2;
			ByteBuffer bb = ByteBuffer.Allocate(bytes.Length + lengthBytes);
			bb.PutShort((short)bytes.Length).Put(bytes);
			ByteArrayInputStream bis = new ByteArrayInputStream(((byte[])bb.Array()));
			DataInputStream dis = new DataInputStream(bis);
			return dis.ReadUTF();
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestIO()
		{
			DataOutputBuffer @out = new DataOutputBuffer();
			DataInputBuffer @in = new DataInputBuffer();
			for (int i = 0; i < 10000; i++)
			{
				// generate a random string
				string before = GetTestString();
				// write it
				@out.Reset();
				UTF8.WriteString(@out, before);
				// test that it reads correctly
				@in.Reset(@out.GetData(), @out.GetLength());
				string after = UTF8.ReadString(@in);
				Assert.Equal(before, after);
				// test that it reads correctly with BinaryReader
				@in.Reset(@out.GetData(), @out.GetLength());
				string after2 = @in.ReadUTF();
				Assert.Equal(before, after2);
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestNullEncoding()
		{
			string s = new string(new char[] { 0 });
			DataOutputBuffer dob = new DataOutputBuffer();
			new UTF8(s).Write(dob);
			Assert.Equal(s, Runtime.GetStringForBytes(dob.GetData(
				), 2, dob.GetLength() - 2, "UTF-8"));
		}

		/// <summary>Test encoding and decoding of UTF8 outside the basic multilingual plane.
		/// 	</summary>
		/// <remarks>
		/// Test encoding and decoding of UTF8 outside the basic multilingual plane.
		/// This is a regression test for HADOOP-9103.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestNonBasicMultilingualPlane()
		{
			// Test using the "CAT FACE" character (U+1F431)
			// See http://www.fileformat.info/info/unicode/char/1f431/index.htm
			string catFace = "\uD83D\uDC31";
			// This encodes to 4 bytes in UTF-8:
			byte[] encoded = Runtime.GetBytesForString(catFace, "UTF-8");
			Assert.Equal(4, encoded.Length);
			Assert.Equal("f09f90b1", StringUtils.ByteToHexString(encoded));
			// Decode back to String using our own decoder
			string roundTrip = UTF8.FromBytes(encoded);
			Assert.Equal(catFace, roundTrip);
		}

		/// <summary>Test that decoding invalid UTF8 throws an appropriate error message.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestInvalidUTF8()
		{
			byte[] invalid = new byte[] { unchecked((int)(0x01)), unchecked((int)(0x02)), unchecked(
				(byte)unchecked((int)(0xff))), unchecked((byte)unchecked((int)(0xff))), unchecked(
				(int)(0x01)), unchecked((int)(0x02)), unchecked((int)(0x03)), unchecked((int)(0x04
				)), unchecked((int)(0x05)) };
			try
			{
				UTF8.FromBytes(invalid);
				Fail("did not throw an exception");
			}
			catch (UTFDataFormatException utfde)
			{
				GenericTestUtils.AssertExceptionContains("Invalid UTF8 at ffff01020304", utfde);
			}
		}

		/// <summary>Test for a 5-byte UTF8 sequence, which is now considered illegal.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void Test5ByteUtf8Sequence()
		{
			byte[] invalid = new byte[] { unchecked((int)(0x01)), unchecked((int)(0x02)), unchecked(
				(byte)unchecked((int)(0xf8))), unchecked((byte)unchecked((int)(0x88))), unchecked(
				(byte)unchecked((int)(0x80))), unchecked((byte)unchecked((int)(0x80))), unchecked(
				(byte)unchecked((int)(0x80))), unchecked((int)(0x04)), unchecked((int)(0x05)) };
			try
			{
				UTF8.FromBytes(invalid);
				Fail("did not throw an exception");
			}
			catch (UTFDataFormatException utfde)
			{
				GenericTestUtils.AssertExceptionContains("Invalid UTF8 at f88880808004", utfde);
			}
		}

		/// <summary>
		/// Test that decoding invalid UTF8 due to truncation yields the correct
		/// exception type.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestInvalidUTF8Truncated()
		{
			// Truncated CAT FACE character -- this is a 4-byte sequence, but we
			// only have the first three bytes.
			byte[] truncated = new byte[] { unchecked((byte)unchecked((int)(0xF0))), unchecked(
				(byte)unchecked((int)(0x9F))), unchecked((byte)unchecked((int)(0x90))) };
			try
			{
				UTF8.FromBytes(truncated);
				Fail("did not throw an exception");
			}
			catch (UTFDataFormatException utfde)
			{
				GenericTestUtils.AssertExceptionContains("Truncated UTF8 at f09f90", utfde);
			}
		}
	}
}
