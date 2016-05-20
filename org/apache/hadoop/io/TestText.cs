using Sharpen;

namespace org.apache.hadoop.io
{
	/// <summary>Unit tests for LargeUTF8.</summary>
	public class TestText : NUnit.Framework.TestCase
	{
		private const int NUM_ITERATIONS = 100;

		public TestText(string name)
			: base(name)
		{
		}

		private static readonly java.util.Random RANDOM = new java.util.Random(1);

		private const int RAND_LEN = -1;

		// generate a valid java String
		/// <exception cref="System.Exception"/>
		private static string getTestString(int len)
		{
			java.lang.StringBuilder buffer = new java.lang.StringBuilder();
			int length = (len == RAND_LEN) ? RANDOM.nextInt(1000) : len;
			while (buffer.Length < length)
			{
				int codePoint = RANDOM.nextInt(char.MAX_CODE_POINT);
				char[] tmpStr = new char[2];
				if (char.isDefined(codePoint))
				{
					//unpaired surrogate
					if (codePoint < char.MIN_SUPPLEMENTARY_CODE_POINT && !char.isHighSurrogate((char)
						codePoint) && !char.isLowSurrogate((char)codePoint))
					{
						char.toChars(codePoint, tmpStr, 0);
						buffer.Append(tmpStr);
					}
				}
			}
			return buffer.ToString();
		}

		/// <exception cref="System.Exception"/>
		public static string getTestString()
		{
			return getTestString(RAND_LEN);
		}

		/// <exception cref="System.Exception"/>
		public static string getLongString()
		{
			string str = getTestString();
			int length = short.MaxValue + str.Length;
			java.lang.StringBuilder buffer = new java.lang.StringBuilder();
			while (buffer.Length < length)
			{
				buffer.Append(str);
			}
			return buffer.ToString();
		}

		/// <exception cref="System.Exception"/>
		public virtual void testWritable()
		{
			for (int i = 0; i < NUM_ITERATIONS; i++)
			{
				string str;
				if (i == 0)
				{
					str = getLongString();
				}
				else
				{
					str = getTestString();
				}
				org.apache.hadoop.io.TestWritable.testWritable(new org.apache.hadoop.io.Text(str)
					);
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void testCoding()
		{
			string before = "Bad \t encoding \t testcase";
			org.apache.hadoop.io.Text text = new org.apache.hadoop.io.Text(before);
			string after = text.ToString();
			NUnit.Framework.Assert.IsTrue(before.Equals(after));
			for (int i = 0; i < NUM_ITERATIONS; i++)
			{
				// generate a random string
				if (i == 0)
				{
					before = getLongString();
				}
				else
				{
					before = getTestString();
				}
				// test string to utf8
				java.nio.ByteBuffer bb = org.apache.hadoop.io.Text.encode(before);
				byte[] utf8Text = ((byte[])bb.array());
				byte[] utf8Java = Sharpen.Runtime.getBytesForString(before, "UTF-8");
				NUnit.Framework.Assert.AreEqual(0, org.apache.hadoop.io.WritableComparator.compareBytes
					(utf8Text, 0, bb.limit(), utf8Java, 0, utf8Java.Length));
				// test utf8 to string
				after = org.apache.hadoop.io.Text.decode(utf8Java);
				NUnit.Framework.Assert.IsTrue(before.Equals(after));
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void testIO()
		{
			org.apache.hadoop.io.DataOutputBuffer @out = new org.apache.hadoop.io.DataOutputBuffer
				();
			org.apache.hadoop.io.DataInputBuffer @in = new org.apache.hadoop.io.DataInputBuffer
				();
			for (int i = 0; i < NUM_ITERATIONS; i++)
			{
				// generate a random string
				string before;
				if (i == 0)
				{
					before = getLongString();
				}
				else
				{
					before = getTestString();
				}
				// write it
				@out.reset();
				org.apache.hadoop.io.Text.writeString(@out, before);
				// test that it reads correctly
				@in.reset(@out.getData(), @out.getLength());
				string after = org.apache.hadoop.io.Text.readString(@in);
				NUnit.Framework.Assert.IsTrue(before.Equals(after));
				// Test compatibility with Java's other decoder 
				int strLenSize = org.apache.hadoop.io.WritableUtils.getVIntSize(org.apache.hadoop.io.Text
					.utf8Length(before));
				string after2 = Sharpen.Runtime.getStringForBytes(@out.getData(), strLenSize, @out
					.getLength() - strLenSize, "UTF-8");
				NUnit.Framework.Assert.IsTrue(before.Equals(after2));
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void doTestLimitedIO(string str, int len)
		{
			org.apache.hadoop.io.DataOutputBuffer @out = new org.apache.hadoop.io.DataOutputBuffer
				();
			org.apache.hadoop.io.DataInputBuffer @in = new org.apache.hadoop.io.DataInputBuffer
				();
			@out.reset();
			try
			{
				org.apache.hadoop.io.Text.writeString(@out, str, len);
				fail("expected writeString to fail when told to write a string " + "that was too long!  The string was '"
					 + str + "'");
			}
			catch (System.IO.IOException)
			{
			}
			org.apache.hadoop.io.Text.writeString(@out, str, len + 1);
			// test that it reads correctly
			@in.reset(@out.getData(), @out.getLength());
			@in.mark(len);
			string after;
			try
			{
				after = org.apache.hadoop.io.Text.readString(@in, len);
				fail("expected readString to fail when told to read a string " + "that was too long!  The string was '"
					 + str + "'");
			}
			catch (System.IO.IOException)
			{
			}
			@in.reset();
			after = org.apache.hadoop.io.Text.readString(@in, len + 1);
			NUnit.Framework.Assert.IsTrue(str.Equals(after));
		}

		/// <exception cref="System.Exception"/>
		public virtual void testLimitedIO()
		{
			doTestLimitedIO("abcd", 3);
			doTestLimitedIO("foo bar baz", 10);
			doTestLimitedIO("1", 0);
		}

		/// <exception cref="System.Exception"/>
		public virtual void testCompare()
		{
			org.apache.hadoop.io.DataOutputBuffer out1 = new org.apache.hadoop.io.DataOutputBuffer
				();
			org.apache.hadoop.io.DataOutputBuffer out2 = new org.apache.hadoop.io.DataOutputBuffer
				();
			org.apache.hadoop.io.DataOutputBuffer out3 = new org.apache.hadoop.io.DataOutputBuffer
				();
			org.apache.hadoop.io.Text.Comparator comparator = new org.apache.hadoop.io.Text.Comparator
				();
			for (int i = 0; i < NUM_ITERATIONS; i++)
			{
				// reset output buffer
				out1.reset();
				out2.reset();
				out3.reset();
				// generate two random strings
				string str1 = getTestString();
				string str2 = getTestString();
				if (i == 0)
				{
					str1 = getLongString();
					str2 = getLongString();
				}
				else
				{
					str1 = getTestString();
					str2 = getTestString();
				}
				// convert to texts
				org.apache.hadoop.io.Text txt1 = new org.apache.hadoop.io.Text(str1);
				org.apache.hadoop.io.Text txt2 = new org.apache.hadoop.io.Text(str2);
				org.apache.hadoop.io.Text txt3 = new org.apache.hadoop.io.Text(str1);
				// serialize them
				txt1.write(out1);
				txt2.write(out2);
				txt3.write(out3);
				// compare two strings by looking at their binary formats
				int ret1 = comparator.compare(out1.getData(), 0, out1.getLength(), out2.getData()
					, 0, out2.getLength());
				// compare two strings
				int ret2 = txt1.compareTo(txt2);
				NUnit.Framework.Assert.AreEqual(ret1, ret2);
				NUnit.Framework.Assert.AreEqual("Equivalence of different txt objects, same content"
					, 0, txt1.compareTo(txt3));
				NUnit.Framework.Assert.AreEqual("Equvalence of data output buffers", 0, comparator
					.compare(out1.getData(), 0, out3.getLength(), out3.getData(), 0, out3.getLength(
					)));
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void testFind()
		{
			org.apache.hadoop.io.Text text = new org.apache.hadoop.io.Text("abcd\u20acbdcd\u20ac"
				);
			NUnit.Framework.Assert.IsTrue(text.find("abd") == -1);
			NUnit.Framework.Assert.IsTrue(text.find("ac") == -1);
			NUnit.Framework.Assert.IsTrue(text.find("\u20ac") == 4);
			NUnit.Framework.Assert.IsTrue(text.find("\u20ac", 5) == 11);
		}

		/// <exception cref="System.Exception"/>
		public virtual void testFindAfterUpdatingContents()
		{
			org.apache.hadoop.io.Text text = new org.apache.hadoop.io.Text("abcd");
			text.set(Sharpen.Runtime.getBytesForString("a"));
			NUnit.Framework.Assert.AreEqual(text.getLength(), 1);
			NUnit.Framework.Assert.AreEqual(text.find("a"), 0);
			NUnit.Framework.Assert.AreEqual(text.find("b"), -1);
		}

		/// <exception cref="System.Exception"/>
		public virtual void testValidate()
		{
			org.apache.hadoop.io.Text text = new org.apache.hadoop.io.Text("abcd\u20acbdcd\u20ac"
				);
			byte[] utf8 = text.getBytes();
			int length = text.getLength();
			org.apache.hadoop.io.Text.validateUTF8(utf8, 0, length);
		}

		/// <exception cref="System.Exception"/>
		public virtual void testClear()
		{
			// Test lengths on an empty text object
			org.apache.hadoop.io.Text text = new org.apache.hadoop.io.Text();
			NUnit.Framework.Assert.AreEqual("Actual string on an empty text object must be an empty string"
				, string.Empty, text.ToString());
			NUnit.Framework.Assert.AreEqual("Underlying byte array length must be zero", 0, text
				.getBytes().Length);
			NUnit.Framework.Assert.AreEqual("String's length must be zero", 0, text.getLength
				());
			// Test if clear works as intended
			text = new org.apache.hadoop.io.Text("abcd\u20acbdcd\u20ac");
			int len = text.getLength();
			text.clear();
			NUnit.Framework.Assert.AreEqual("String must be empty after clear()", string.Empty
				, text.ToString());
			NUnit.Framework.Assert.IsTrue("Length of the byte array must not decrease after clear()"
				, text.getBytes().Length >= len);
			NUnit.Framework.Assert.AreEqual("Length of the string must be reset to 0 after clear()"
				, 0, text.getLength());
		}

		/// <exception cref="java.nio.charset.CharacterCodingException"/>
		public virtual void testTextText()
		{
			org.apache.hadoop.io.Text a = new org.apache.hadoop.io.Text("abc");
			org.apache.hadoop.io.Text b = new org.apache.hadoop.io.Text("a");
			b.set(a);
			NUnit.Framework.Assert.AreEqual("abc", b.ToString());
			a.append(Sharpen.Runtime.getBytesForString("xdefgxxx"), 1, 4);
			NUnit.Framework.Assert.AreEqual("modified aliased string", "abc", b.ToString());
			NUnit.Framework.Assert.AreEqual("appended string incorrectly", "abcdefg", a.ToString
				());
			// add an extra byte so that capacity = 14 and length = 8
			a.append(new byte[] { (byte)('d') }, 0, 1);
			NUnit.Framework.Assert.AreEqual(14, a.getBytes().Length);
			NUnit.Framework.Assert.AreEqual(8, a.copyBytes().Length);
		}

		private class ConcurrentEncodeDecodeThread : java.lang.Thread
		{
			public ConcurrentEncodeDecodeThread(TestText _enclosing, string name)
				: base(name)
			{
				this._enclosing = _enclosing;
			}

			public override void run()
			{
				string name = this.getName();
				org.apache.hadoop.io.DataOutputBuffer @out = new org.apache.hadoop.io.DataOutputBuffer
					();
				org.apache.hadoop.io.DataInputBuffer @in = new org.apache.hadoop.io.DataInputBuffer
					();
				for (int i = 0; i < 1000; ++i)
				{
					try
					{
						@out.reset();
						org.apache.hadoop.io.WritableUtils.writeString(@out, name);
						@in.reset(@out.getData(), @out.getLength());
						string s = org.apache.hadoop.io.WritableUtils.readString(@in);
						NUnit.Framework.Assert.AreEqual("input buffer reset contents = " + name, name, s);
					}
					catch (System.Exception ioe)
					{
						throw new System.Exception(ioe);
					}
				}
			}

			private readonly TestText _enclosing;
		}

		/// <exception cref="System.Exception"/>
		public virtual void testConcurrentEncodeDecode()
		{
			java.lang.Thread thread1 = new org.apache.hadoop.io.TestText.ConcurrentEncodeDecodeThread
				(this, "apache");
			java.lang.Thread thread2 = new org.apache.hadoop.io.TestText.ConcurrentEncodeDecodeThread
				(this, "hadoop");
			thread1.start();
			thread2.start();
			thread2.join();
			thread2.join();
		}

		/// <exception cref="System.Exception"/>
		public virtual void testAvroReflect()
		{
			org.apache.hadoop.io.AvroTestUtil.testReflect(new org.apache.hadoop.io.Text("foo"
				), "{\"type\":\"string\",\"java-class\":\"org.apache.hadoop.io.Text\"}");
		}

		public virtual void testCharAt()
		{
			string line = "adsawseeeeegqewgasddga";
			org.apache.hadoop.io.Text text = new org.apache.hadoop.io.Text(line);
			for (int i = 0; i < line.Length; i++)
			{
				NUnit.Framework.Assert.IsTrue("testCharAt error1 !!!", text.charAt(i) == line[i]);
			}
			NUnit.Framework.Assert.AreEqual("testCharAt error2 !!!", -1, text.charAt(-1));
			NUnit.Framework.Assert.AreEqual("testCharAt error3 !!!", -1, text.charAt(100));
		}

		/// <summary>
		/// test
		/// <c>Text</c>
		/// readFields/write operations
		/// </summary>
		public virtual void testReadWriteOperations()
		{
			string line = "adsawseeeeegqewgasddga";
			byte[] inputBytes = Sharpen.Runtime.getBytesForString(line);
			inputBytes = com.google.common.primitives.Bytes.concat(new byte[] { unchecked((byte
				)22) }, inputBytes);
			org.apache.hadoop.io.DataInputBuffer @in = new org.apache.hadoop.io.DataInputBuffer
				();
			org.apache.hadoop.io.DataOutputBuffer @out = new org.apache.hadoop.io.DataOutputBuffer
				();
			org.apache.hadoop.io.Text text = new org.apache.hadoop.io.Text(line);
			try
			{
				@in.reset(inputBytes, inputBytes.Length);
				text.readFields(@in);
			}
			catch (System.Exception)
			{
				fail("testReadFields error !!!");
			}
			try
			{
				text.write(@out);
			}
			catch (System.IO.IOException)
			{
			}
			catch (System.Exception)
			{
				fail("testReadWriteOperations error !!!");
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testReadWithKnownLength()
		{
			string line = "hello world";
			byte[] inputBytes = Sharpen.Runtime.getBytesForString(line, com.google.common.@base.Charsets
				.UTF_8);
			org.apache.hadoop.io.DataInputBuffer @in = new org.apache.hadoop.io.DataInputBuffer
				();
			org.apache.hadoop.io.Text text = new org.apache.hadoop.io.Text();
			@in.reset(inputBytes, inputBytes.Length);
			text.readWithKnownLength(@in, 5);
			NUnit.Framework.Assert.AreEqual("hello", text.ToString());
			// Read longer length, make sure it lengthens
			@in.reset(inputBytes, inputBytes.Length);
			text.readWithKnownLength(@in, 7);
			NUnit.Framework.Assert.AreEqual("hello w", text.ToString());
			// Read shorter length, make sure it shortens
			@in.reset(inputBytes, inputBytes.Length);
			text.readWithKnownLength(@in, 2);
			NUnit.Framework.Assert.AreEqual("he", text.ToString());
		}

		/// <summary>
		/// test
		/// <c>Text.bytesToCodePoint(bytes)</c>
		/// 
		/// with
		/// <c>BufferUnderflowException</c>
		/// </summary>
		public virtual void testBytesToCodePoint()
		{
			try
			{
				java.nio.ByteBuffer bytes = java.nio.ByteBuffer.wrap(new byte[] { unchecked((byte
					)(-2)), 45, 23, 12, 76, 89 });
				org.apache.hadoop.io.Text.bytesToCodePoint(bytes);
				NUnit.Framework.Assert.IsTrue("testBytesToCodePoint error !!!", bytes.position() 
					== 6);
			}
			catch (java.nio.BufferUnderflowException)
			{
				fail("testBytesToCodePoint unexp exception");
			}
			catch (System.Exception)
			{
				fail("testBytesToCodePoint unexp exception");
			}
		}

		public virtual void testbytesToCodePointWithInvalidUTF()
		{
			try
			{
				org.apache.hadoop.io.Text.bytesToCodePoint(java.nio.ByteBuffer.wrap(new byte[] { 
					unchecked((byte)(-2)) }));
				fail("testbytesToCodePointWithInvalidUTF error unexp exception !!!");
			}
			catch (java.nio.BufferUnderflowException)
			{
			}
			catch (System.Exception)
			{
				fail("testbytesToCodePointWithInvalidUTF error unexp exception !!!");
			}
		}

		public virtual void testUtf8Length()
		{
			NUnit.Framework.Assert.AreEqual("testUtf8Length1 error   !!!", 1, org.apache.hadoop.io.Text
				.utf8Length(new string(new char[] { (char)1 })));
			NUnit.Framework.Assert.AreEqual("testUtf8Length127 error !!!", 1, org.apache.hadoop.io.Text
				.utf8Length(new string(new char[] { (char)127 })));
			NUnit.Framework.Assert.AreEqual("testUtf8Length128 error !!!", 2, org.apache.hadoop.io.Text
				.utf8Length(new string(new char[] { (char)128 })));
			NUnit.Framework.Assert.AreEqual("testUtf8Length193 error !!!", 2, org.apache.hadoop.io.Text
				.utf8Length(new string(new char[] { (char)193 })));
			NUnit.Framework.Assert.AreEqual("testUtf8Length225 error !!!", 2, org.apache.hadoop.io.Text
				.utf8Length(new string(new char[] { (char)225 })));
			NUnit.Framework.Assert.AreEqual("testUtf8Length254 error !!!", 2, org.apache.hadoop.io.Text
				.utf8Length(new string(new char[] { (char)254 })));
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			org.apache.hadoop.io.TestText test = new org.apache.hadoop.io.TestText("main");
			test.testIO();
			test.testCompare();
			test.testCoding();
			test.testWritable();
			test.testFind();
			test.testValidate();
		}
	}
}
