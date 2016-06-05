using System;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.IO.File.Tfile
{
	/// <summary>
	/// Streaming interfaces test case class using GZ compression codec, base class
	/// of none and LZO compression classes.
	/// </summary>
	public class TestTFileStreams : TestCase
	{
		private static string Root = Runtime.GetProperty("test.build.data", "/tmp/tfile-test"
			);

		private const int BlockSize = 512;

		private const int K = 1024;

		private const int M = K * K;

		protected internal bool skip = false;

		private FileSystem fs;

		private Configuration conf;

		private Path path;

		private FSDataOutputStream @out;

		internal TFile.Writer writer;

		private string compression = Compression.Algorithm.Gz.GetName();

		private string comparator = "memcmp";

		private readonly string outputFile = GetType().Name;

		public virtual void Init(string compression, string comparator)
		{
			this.compression = compression;
			this.comparator = comparator;
		}

		/// <exception cref="System.IO.IOException"/>
		protected override void SetUp()
		{
			conf = new Configuration();
			path = new Path(Root, outputFile);
			fs = path.GetFileSystem(conf);
			@out = fs.Create(path);
			writer = new TFile.Writer(@out, BlockSize, compression, comparator, conf);
		}

		/// <exception cref="System.IO.IOException"/>
		protected override void TearDown()
		{
			if (!skip)
			{
				try
				{
					CloseOutput();
				}
				catch (Exception)
				{
				}
				// no-op
				fs.Delete(path, true);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestNoEntry()
		{
			if (skip)
			{
				return;
			}
			CloseOutput();
			TestTFileByteArrays.ReadRecords(fs, path, 0, conf);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestOneEntryKnownLength()
		{
			if (skip)
			{
				return;
			}
			WriteRecords(1, true, true);
			TestTFileByteArrays.ReadRecords(fs, path, 1, conf);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestOneEntryUnknownLength()
		{
			if (skip)
			{
				return;
			}
			WriteRecords(1, false, false);
			// TODO: will throw exception at getValueLength, it's inconsistent though;
			// getKeyLength returns a value correctly, though initial length is -1
			TestTFileByteArrays.ReadRecords(fs, path, 1, conf);
		}

		// known key length, unknown value length
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestOneEntryMixedLengths1()
		{
			if (skip)
			{
				return;
			}
			WriteRecords(1, true, false);
			TestTFileByteArrays.ReadRecords(fs, path, 1, conf);
		}

		// unknown key length, known value length
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestOneEntryMixedLengths2()
		{
			if (skip)
			{
				return;
			}
			WriteRecords(1, false, true);
			TestTFileByteArrays.ReadRecords(fs, path, 1, conf);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestTwoEntriesKnownLength()
		{
			if (skip)
			{
				return;
			}
			WriteRecords(2, true, true);
			TestTFileByteArrays.ReadRecords(fs, path, 2, conf);
		}

		// Negative test
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestFailureAddKeyWithoutValue()
		{
			if (skip)
			{
				return;
			}
			DataOutputStream dos = writer.PrepareAppendKey(-1);
			dos.Write(Sharpen.Runtime.GetBytesForString("key0"));
			try
			{
				CloseOutput();
				Fail("Cannot add only a key without a value. ");
			}
			catch (InvalidOperationException)
			{
			}
		}

		// noop, expecting an exception
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestFailureAddValueWithoutKey()
		{
			if (skip)
			{
				return;
			}
			DataOutputStream outValue = null;
			try
			{
				outValue = writer.PrepareAppendValue(6);
				outValue.Write(Sharpen.Runtime.GetBytesForString("value0"));
				Fail("Cannot add a value without adding key first. ");
			}
			catch (Exception)
			{
			}
			finally
			{
				// noop, expecting an exception
				if (outValue != null)
				{
					outValue.Close();
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestFailureOneEntryKnownLength()
		{
			if (skip)
			{
				return;
			}
			DataOutputStream outKey = writer.PrepareAppendKey(2);
			try
			{
				outKey.Write(Sharpen.Runtime.GetBytesForString("key0"));
				Fail("Specified key length mismatched the actual key length.");
			}
			catch (IOException)
			{
			}
			// noop, expecting an exception
			DataOutputStream outValue = null;
			try
			{
				outValue = writer.PrepareAppendValue(6);
				outValue.Write(Sharpen.Runtime.GetBytesForString("value0"));
			}
			catch (Exception)
			{
			}
		}

		// noop, expecting an exception
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestFailureKeyTooLong()
		{
			if (skip)
			{
				return;
			}
			DataOutputStream outKey = writer.PrepareAppendKey(2);
			try
			{
				outKey.Write(Sharpen.Runtime.GetBytesForString("key0"));
				outKey.Close();
				NUnit.Framework.Assert.Fail("Key is longer than requested.");
			}
			catch (Exception)
			{
			}
			finally
			{
			}
		}

		// noop, expecting an exception
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestFailureKeyTooShort()
		{
			if (skip)
			{
				return;
			}
			DataOutputStream outKey = writer.PrepareAppendKey(4);
			outKey.Write(Sharpen.Runtime.GetBytesForString("key0"));
			outKey.Close();
			DataOutputStream outValue = writer.PrepareAppendValue(15);
			try
			{
				outValue.Write(Sharpen.Runtime.GetBytesForString("value0"));
				outValue.Close();
				NUnit.Framework.Assert.Fail("Value is shorter than expected.");
			}
			catch (Exception)
			{
			}
			finally
			{
			}
		}

		// noop, expecting an exception
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestFailureValueTooLong()
		{
			if (skip)
			{
				return;
			}
			DataOutputStream outKey = writer.PrepareAppendKey(4);
			outKey.Write(Sharpen.Runtime.GetBytesForString("key0"));
			outKey.Close();
			DataOutputStream outValue = writer.PrepareAppendValue(3);
			try
			{
				outValue.Write(Sharpen.Runtime.GetBytesForString("value0"));
				outValue.Close();
				NUnit.Framework.Assert.Fail("Value is longer than expected.");
			}
			catch (Exception)
			{
			}
			// noop, expecting an exception
			try
			{
				outKey.Close();
				outKey.Close();
			}
			catch (Exception)
			{
				NUnit.Framework.Assert.Fail("Second or more close() should have no effect.");
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestFailureValueTooShort()
		{
			if (skip)
			{
				return;
			}
			DataOutputStream outKey = writer.PrepareAppendKey(8);
			try
			{
				outKey.Write(Sharpen.Runtime.GetBytesForString("key0"));
				outKey.Close();
				NUnit.Framework.Assert.Fail("Key is shorter than expected.");
			}
			catch (Exception)
			{
			}
			finally
			{
			}
		}

		// noop, expecting an exception
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestFailureCloseKeyStreamManyTimesInWriter()
		{
			if (skip)
			{
				return;
			}
			DataOutputStream outKey = writer.PrepareAppendKey(4);
			try
			{
				outKey.Write(Sharpen.Runtime.GetBytesForString("key0"));
				outKey.Close();
			}
			catch (Exception)
			{
			}
			finally
			{
				// noop, expecting an exception
				try
				{
					outKey.Close();
				}
				catch (Exception)
				{
				}
			}
			// no-op
			outKey.Close();
			outKey.Close();
			Assert.True("Multiple close should have no effect.", true);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestFailureKeyLongerThan64K()
		{
			if (skip)
			{
				return;
			}
			try
			{
				DataOutputStream outKey = writer.PrepareAppendKey(64 * K + 1);
				NUnit.Framework.Assert.Fail("Failed to handle key longer than 64K.");
			}
			catch (IndexOutOfRangeException)
			{
			}
			// noop, expecting exceptions
			CloseOutput();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestFailureKeyLongerThan64K_2()
		{
			if (skip)
			{
				return;
			}
			DataOutputStream outKey = writer.PrepareAppendKey(-1);
			try
			{
				byte[] buf = new byte[K];
				Random rand = new Random();
				for (int nx = 0; nx < K + 2; nx++)
				{
					rand.NextBytes(buf);
					outKey.Write(buf);
				}
				outKey.Close();
				NUnit.Framework.Assert.Fail("Failed to handle key longer than 64K.");
			}
			catch (EOFException)
			{
			}
			finally
			{
				// noop, expecting exceptions
				try
				{
					CloseOutput();
				}
				catch (Exception)
				{
				}
			}
		}

		// no-op
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestFailureNegativeOffset()
		{
			if (skip)
			{
				return;
			}
			WriteRecords(2, true, true);
			TFile.Reader reader = new TFile.Reader(fs.Open(path), fs.GetFileStatus(path).GetLen
				(), conf);
			TFile.Reader.Scanner scanner = reader.CreateScanner();
			byte[] buf = new byte[K];
			try
			{
				scanner.Entry().GetKey(buf, -1);
				NUnit.Framework.Assert.Fail("Failed to handle key negative offset.");
			}
			catch (Exception)
			{
			}
			finally
			{
			}
			// noop, expecting exceptions
			scanner.Close();
			reader.Close();
		}

		/// <summary>Verify that the compressed data size is less than raw data size.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestFailureCompressionNotWorking()
		{
			if (skip)
			{
				return;
			}
			long rawDataSize = WriteRecords(10000, false, false, false);
			if (!Sharpen.Runtime.EqualsIgnoreCase(compression, Compression.Algorithm.None.GetName
				()))
			{
				Assert.True(@out.GetPos() < rawDataSize);
			}
			CloseOutput();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestFailureCompressionNotWorking2()
		{
			if (skip)
			{
				return;
			}
			long rawDataSize = WriteRecords(10000, true, true, false);
			if (!Sharpen.Runtime.EqualsIgnoreCase(compression, Compression.Algorithm.None.GetName
				()))
			{
				Assert.True(@out.GetPos() < rawDataSize);
			}
			CloseOutput();
		}

		/// <exception cref="System.IO.IOException"/>
		private long WriteRecords(int count, bool knownKeyLength, bool knownValueLength, 
			bool close)
		{
			long rawDataSize = 0;
			for (int nx = 0; nx < count; nx++)
			{
				string key = TestTFileByteArrays.ComposeSortedKey("key", nx);
				DataOutputStream outKey = writer.PrepareAppendKey(knownKeyLength ? key.Length : -
					1);
				outKey.Write(Sharpen.Runtime.GetBytesForString(key));
				outKey.Close();
				string value = "value" + nx;
				DataOutputStream outValue = writer.PrepareAppendValue(knownValueLength ? value.Length
					 : -1);
				outValue.Write(Sharpen.Runtime.GetBytesForString(value));
				outValue.Close();
				rawDataSize += WritableUtils.GetVIntSize(Sharpen.Runtime.GetBytesForString(key).Length
					) + Sharpen.Runtime.GetBytesForString(key).Length + WritableUtils.GetVIntSize(Sharpen.Runtime.GetBytesForString
					(value).Length) + Sharpen.Runtime.GetBytesForString(value).Length;
			}
			if (close)
			{
				CloseOutput();
			}
			return rawDataSize;
		}

		/// <exception cref="System.IO.IOException"/>
		private long WriteRecords(int count, bool knownKeyLength, bool knownValueLength)
		{
			return WriteRecords(count, knownKeyLength, knownValueLength, true);
		}

		/// <exception cref="System.IO.IOException"/>
		private void CloseOutput()
		{
			if (writer != null)
			{
				writer.Close();
				writer = null;
			}
			if (@out != null)
			{
				@out.Close();
				@out = null;
			}
		}
	}
}
