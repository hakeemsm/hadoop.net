using System;
using System.IO;
using System.Text;
using Com.Google.Common.Base;
using Com.Google.Common.Primitives;
using Hadoop.Common.Core.IO;
using NUnit.Framework;


namespace Org.Apache.Hadoop.IO
{
	/// <summary>Unit tests for LargeUTF8.</summary>
	public class TestText : TestCase
	{
		private const int NumIterations = 100;

		public TestText(string name)
			: base(name)
		{
		}

		private static readonly Random Random = new Random(1);

		private const int RandLen = -1;

		// generate a valid java String
		/// <exception cref="System.Exception"/>
		private static string GetTestString(int len)
		{
			StringBuilder buffer = new StringBuilder();
			int length = (len == RandLen) ? Random.Next(1000) : len;
			while (buffer.Length < length)
			{
				int codePoint = Random.Next(char.MaxCodePoint);
				char[] tmpStr = new char[2];
				if (char.IsDefined(codePoint))
				{
					//unpaired surrogate
					if (codePoint < char.MinSupplementaryCodePoint && !char.IsHighSurrogate((char)codePoint
						) && !char.IsLowSurrogate((char)codePoint))
					{
						char.ToChars(codePoint, tmpStr, 0);
						buffer.Append(tmpStr);
					}
				}
			}
			return buffer.ToString();
		}

		/// <exception cref="System.Exception"/>
		public static string GetTestString()
		{
			return GetTestString(RandLen);
		}

		/// <exception cref="System.Exception"/>
		public static string GetLongString()
		{
			string str = GetTestString();
			int length = short.MaxValue + str.Length;
			StringBuilder buffer = new StringBuilder();
			while (buffer.Length < length)
			{
				buffer.Append(str);
			}
			return buffer.ToString();
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestWritable()
		{
			for (int i = 0; i < NumIterations; i++)
			{
				string str;
				if (i == 0)
				{
					str = GetLongString();
				}
				else
				{
					str = GetTestString();
				}
				Org.Apache.Hadoop.IO.TestWritable.TestWritable(new Org.Apache.Hadoop.IO.Text(str)
					);
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestCoding()
		{
			string before = "Bad \t encoding \t testcase";
			Org.Apache.Hadoop.IO.Text text = new Org.Apache.Hadoop.IO.Text(before);
			string after = text.ToString();
			Assert.True(before.Equals(after));
			for (int i = 0; i < NumIterations; i++)
			{
				// generate a random string
				if (i == 0)
				{
					before = GetLongString();
				}
				else
				{
					before = GetTestString();
				}
				// test string to utf8
				ByteBuffer bb = Org.Apache.Hadoop.IO.Text.Encode(before);
				byte[] utf8Text = ((byte[])bb.Array());
				byte[] utf8Java = Runtime.GetBytesForString(before, "UTF-8");
				Assert.Equal(0, WritableComparator.CompareBytes(utf8Text, 0, bb
					.Limit(), utf8Java, 0, utf8Java.Length));
				// test utf8 to string
				after = Org.Apache.Hadoop.IO.Text.Decode(utf8Java);
				Assert.True(before.Equals(after));
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestIO()
		{
			DataOutputBuffer @out = new DataOutputBuffer();
			DataInputBuffer @in = new DataInputBuffer();
			for (int i = 0; i < NumIterations; i++)
			{
				// generate a random string
				string before;
				if (i == 0)
				{
					before = GetLongString();
				}
				else
				{
					before = GetTestString();
				}
				// write it
				@out.Reset();
				Org.Apache.Hadoop.IO.Text.WriteString(@out, before);
				// test that it reads correctly
				@in.Reset(@out.GetData(), @out.GetLength());
				string after = Org.Apache.Hadoop.IO.Text.ReadString(@in);
				Assert.True(before.Equals(after));
				// Test compatibility with Java's other decoder 
				int strLenSize = WritableUtils.GetVIntSize(Org.Apache.Hadoop.IO.Text.Utf8Length(before
					));
				string after2 = Runtime.GetStringForBytes(@out.GetData(), strLenSize, @out
					.GetLength() - strLenSize, "UTF-8");
				Assert.True(before.Equals(after2));
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void DoTestLimitedIO(string str, int len)
		{
			DataOutputBuffer @out = new DataOutputBuffer();
			DataInputBuffer @in = new DataInputBuffer();
			@out.Reset();
			try
			{
				Org.Apache.Hadoop.IO.Text.WriteString(@out, str, len);
				Fail("expected writeString to fail when told to write a string " + "that was too long!  The string was '"
					 + str + "'");
			}
			catch (IOException)
			{
			}
			Org.Apache.Hadoop.IO.Text.WriteString(@out, str, len + 1);
			// test that it reads correctly
			@in.Reset(@out.GetData(), @out.GetLength());
			@in.Mark(len);
			string after;
			try
			{
				after = Org.Apache.Hadoop.IO.Text.ReadString(@in, len);
				Fail("expected readString to fail when told to read a string " + "that was too long!  The string was '"
					 + str + "'");
			}
			catch (IOException)
			{
			}
			@in.Reset();
			after = Org.Apache.Hadoop.IO.Text.ReadString(@in, len + 1);
			Assert.True(str.Equals(after));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestLimitedIO()
		{
			DoTestLimitedIO("abcd", 3);
			DoTestLimitedIO("foo bar baz", 10);
			DoTestLimitedIO("1", 0);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestCompare()
		{
			DataOutputBuffer out1 = new DataOutputBuffer();
			DataOutputBuffer out2 = new DataOutputBuffer();
			DataOutputBuffer out3 = new DataOutputBuffer();
			Text.Comparator comparator = new Text.Comparator();
			for (int i = 0; i < NumIterations; i++)
			{
				// reset output buffer
				out1.Reset();
				out2.Reset();
				out3.Reset();
				// generate two random strings
				string str1 = GetTestString();
				string str2 = GetTestString();
				if (i == 0)
				{
					str1 = GetLongString();
					str2 = GetLongString();
				}
				else
				{
					str1 = GetTestString();
					str2 = GetTestString();
				}
				// convert to texts
				Org.Apache.Hadoop.IO.Text txt1 = new Org.Apache.Hadoop.IO.Text(str1);
				Org.Apache.Hadoop.IO.Text txt2 = new Org.Apache.Hadoop.IO.Text(str2);
				Org.Apache.Hadoop.IO.Text txt3 = new Org.Apache.Hadoop.IO.Text(str1);
				// serialize them
				txt1.Write(out1);
				txt2.Write(out2);
				txt3.Write(out3);
				// compare two strings by looking at their binary formats
				int ret1 = comparator.Compare(out1.GetData(), 0, out1.GetLength(), out2.GetData()
					, 0, out2.GetLength());
				// compare two strings
				int ret2 = txt1.CompareTo(txt2);
				Assert.Equal(ret1, ret2);
				Assert.Equal("Equivalence of different txt objects, same content"
					, 0, txt1.CompareTo(txt3));
				Assert.Equal("Equvalence of data output buffers", 0, comparator
					.Compare(out1.GetData(), 0, out3.GetLength(), out3.GetData(), 0, out3.GetLength(
					)));
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestFind()
		{
			Org.Apache.Hadoop.IO.Text text = new Org.Apache.Hadoop.IO.Text("abcd\u20acbdcd\u20ac"
				);
			Assert.True(text.Find("abd") == -1);
			Assert.True(text.Find("ac") == -1);
			Assert.True(text.Find("\u20ac") == 4);
			Assert.True(text.Find("\u20ac", 5) == 11);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestFindAfterUpdatingContents()
		{
			Org.Apache.Hadoop.IO.Text text = new Org.Apache.Hadoop.IO.Text("abcd");
			text.Set(Runtime.GetBytesForString("a"));
			Assert.Equal(text.GetLength(), 1);
			Assert.Equal(text.Find("a"), 0);
			Assert.Equal(text.Find("b"), -1);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestValidate()
		{
			Org.Apache.Hadoop.IO.Text text = new Org.Apache.Hadoop.IO.Text("abcd\u20acbdcd\u20ac"
				);
			byte[] utf8 = text.GetBytes();
			int length = text.GetLength();
			Org.Apache.Hadoop.IO.Text.ValidateUTF8(utf8, 0, length);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestClear()
		{
			// Test lengths on an empty text object
			Org.Apache.Hadoop.IO.Text text = new Org.Apache.Hadoop.IO.Text();
			Assert.Equal("Actual string on an empty text object must be an empty string"
				, string.Empty, text.ToString());
			Assert.Equal("Underlying byte array length must be zero", 0, text
				.GetBytes().Length);
			Assert.Equal("String's length must be zero", 0, text.GetLength
				());
			// Test if clear works as intended
			text = new Org.Apache.Hadoop.IO.Text("abcd\u20acbdcd\u20ac");
			int len = text.GetLength();
			text.Clear();
			Assert.Equal("String must be empty after clear()", string.Empty
				, text.ToString());
			Assert.True("Length of the byte array must not decrease after clear()"
				, text.GetBytes().Length >= len);
			Assert.Equal("Length of the string must be reset to 0 after clear()"
				, 0, text.GetLength());
		}

		/// <exception cref="CharacterCodingException"/>
		public virtual void TestTextText()
		{
			Org.Apache.Hadoop.IO.Text a = new Org.Apache.Hadoop.IO.Text("abc");
			Org.Apache.Hadoop.IO.Text b = new Org.Apache.Hadoop.IO.Text("a");
			b.Set(a);
			Assert.Equal("abc", b.ToString());
			a.Append(Runtime.GetBytesForString("xdefgxxx"), 1, 4);
			Assert.Equal("modified aliased string", "abc", b.ToString());
			Assert.Equal("appended string incorrectly", "abcdefg", a.ToString
				());
			// add an extra byte so that capacity = 14 and length = 8
			a.Append(new byte[] { (byte)('d') }, 0, 1);
			Assert.Equal(14, a.GetBytes().Length);
			Assert.Equal(8, a.CopyBytes().Length);
		}

		private class ConcurrentEncodeDecodeThread : Thread
		{
			public ConcurrentEncodeDecodeThread(TestText _enclosing, string name)
				: base(name)
			{
				this._enclosing = _enclosing;
			}

			public override void Run()
			{
				string name = this.GetName();
				DataOutputBuffer @out = new DataOutputBuffer();
				DataInputBuffer @in = new DataInputBuffer();
				for (int i = 0; i < 1000; ++i)
				{
					try
					{
						@out.Reset();
						WritableUtils.WriteString(@out, name);
						@in.Reset(@out.GetData(), @out.GetLength());
						string s = WritableUtils.ReadString(@in);
						Assert.Equal("input buffer reset contents = " + name, name, s);
					}
					catch (Exception ioe)
					{
						throw new RuntimeException(ioe);
					}
				}
			}

			private readonly TestText _enclosing;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestConcurrentEncodeDecode()
		{
			Thread thread1 = new TestText.ConcurrentEncodeDecodeThread(this, "apache"
				);
			Thread thread2 = new TestText.ConcurrentEncodeDecodeThread(this, "hadoop"
				);
			thread1.Start();
			thread2.Start();
			thread2.Join();
			thread2.Join();
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestAvroReflect()
		{
			AvroTestUtil.TestReflect(new Org.Apache.Hadoop.IO.Text("foo"), "{\"type\":\"string\",\"java-class\":\"org.apache.hadoop.io.Text\"}"
				);
		}

		public virtual void TestCharAt()
		{
			string line = "adsawseeeeegqewgasddga";
			Org.Apache.Hadoop.IO.Text text = new Org.Apache.Hadoop.IO.Text(line);
			for (int i = 0; i < line.Length; i++)
			{
				Assert.True("testCharAt error1 !!!", text.CharAt(i) == line[i]);
			}
			Assert.Equal("testCharAt error2 !!!", -1, text.CharAt(-1));
			Assert.Equal("testCharAt error3 !!!", -1, text.CharAt(100));
		}

		/// <summary>
		/// test
		/// <c>Text</c>
		/// readFields/write operations
		/// </summary>
		public virtual void TestReadWriteOperations()
		{
			string line = "adsawseeeeegqewgasddga";
			byte[] inputBytes = Runtime.GetBytesForString(line);
			inputBytes = Bytes.Concat(new byte[] { unchecked((byte)22) }, inputBytes);
			DataInputBuffer @in = new DataInputBuffer();
			DataOutputBuffer @out = new DataOutputBuffer();
			Org.Apache.Hadoop.IO.Text text = new Org.Apache.Hadoop.IO.Text(line);
			try
			{
				@in.Reset(inputBytes, inputBytes.Length);
				text.ReadFields(@in);
			}
			catch (Exception)
			{
				Fail("testReadFields error !!!");
			}
			try
			{
				text.Write(@out);
			}
			catch (IOException)
			{
			}
			catch (Exception)
			{
				Fail("testReadWriteOperations error !!!");
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestReadWithKnownLength()
		{
			string line = "hello world";
			byte[] inputBytes = Runtime.GetBytesForString(line, Charsets.Utf8);
			DataInputBuffer @in = new DataInputBuffer();
			Org.Apache.Hadoop.IO.Text text = new Org.Apache.Hadoop.IO.Text();
			@in.Reset(inputBytes, inputBytes.Length);
			text.ReadWithKnownLength(@in, 5);
			Assert.Equal("hello", text.ToString());
			// Read longer length, make sure it lengthens
			@in.Reset(inputBytes, inputBytes.Length);
			text.ReadWithKnownLength(@in, 7);
			Assert.Equal("hello w", text.ToString());
			// Read shorter length, make sure it shortens
			@in.Reset(inputBytes, inputBytes.Length);
			text.ReadWithKnownLength(@in, 2);
			Assert.Equal("he", text.ToString());
		}

		/// <summary>
		/// test
		/// <c>Text.bytesToCodePoint(bytes)</c>
		/// 
		/// with
		/// <c>BufferUnderflowException</c>
		/// </summary>
		public virtual void TestBytesToCodePoint()
		{
			try
			{
				ByteBuffer bytes = ByteBuffer.Wrap(new byte[] { unchecked((byte)(-2)), 45, 23, 12
					, 76, 89 });
				Org.Apache.Hadoop.IO.Text.BytesToCodePoint(bytes);
				Assert.True("testBytesToCodePoint error !!!", bytes.Position() 
					== 6);
			}
			catch (BufferUnderflowException)
			{
				Fail("testBytesToCodePoint unexp exception");
			}
			catch (Exception)
			{
				Fail("testBytesToCodePoint unexp exception");
			}
		}

		public virtual void TestbytesToCodePointWithInvalidUTF()
		{
			try
			{
				Org.Apache.Hadoop.IO.Text.BytesToCodePoint(ByteBuffer.Wrap(new byte[] { unchecked(
					(byte)(-2)) }));
				Fail("testbytesToCodePointWithInvalidUTF error unexp exception !!!");
			}
			catch (BufferUnderflowException)
			{
			}
			catch (Exception)
			{
				Fail("testbytesToCodePointWithInvalidUTF error unexp exception !!!");
			}
		}

		public virtual void TestUtf8Length()
		{
			Assert.Equal("testUtf8Length1 error   !!!", 1, Org.Apache.Hadoop.IO.Text
				.Utf8Length(new string(new char[] { (char)1 })));
			Assert.Equal("testUtf8Length127 error !!!", 1, Org.Apache.Hadoop.IO.Text
				.Utf8Length(new string(new char[] { (char)127 })));
			Assert.Equal("testUtf8Length128 error !!!", 2, Org.Apache.Hadoop.IO.Text
				.Utf8Length(new string(new char[] { (char)128 })));
			Assert.Equal("testUtf8Length193 error !!!", 2, Org.Apache.Hadoop.IO.Text
				.Utf8Length(new string(new char[] { (char)193 })));
			Assert.Equal("testUtf8Length225 error !!!", 2, Org.Apache.Hadoop.IO.Text
				.Utf8Length(new string(new char[] { (char)225 })));
			Assert.Equal("testUtf8Length254 error !!!", 2, Org.Apache.Hadoop.IO.Text
				.Utf8Length(new string(new char[] { (char)254 })));
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			TestText test = new TestText("main");
			test.TestIO();
			test.TestCompare();
			test.TestCoding();
			test.TestWritable();
			test.TestFind();
			test.TestValidate();
		}
	}
}
