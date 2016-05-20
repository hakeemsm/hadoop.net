using Sharpen;

namespace org.apache.hadoop.io.file.tfile
{
	/// <summary>
	/// Streaming interfaces test case class using GZ compression codec, base class
	/// of none and LZO compression classes.
	/// </summary>
	public class TestTFileStreams : NUnit.Framework.TestCase
	{
		private static string ROOT = Sharpen.Runtime.getProperty("test.build.data", "/tmp/tfile-test"
			);

		private const int BLOCK_SIZE = 512;

		private const int K = 1024;

		private const int M = K * K;

		protected internal bool skip = false;

		private org.apache.hadoop.fs.FileSystem fs;

		private org.apache.hadoop.conf.Configuration conf;

		private org.apache.hadoop.fs.Path path;

		private org.apache.hadoop.fs.FSDataOutputStream @out;

		internal org.apache.hadoop.io.file.tfile.TFile.Writer writer;

		private string compression = org.apache.hadoop.io.file.tfile.Compression.Algorithm
			.GZ.getName();

		private string comparator = "memcmp";

		private readonly string outputFile = Sharpen.Runtime.getClassForObject(this).getSimpleName
			();

		public virtual void init(string compression, string comparator)
		{
			this.compression = compression;
			this.comparator = comparator;
		}

		/// <exception cref="System.IO.IOException"/>
		protected override void setUp()
		{
			conf = new org.apache.hadoop.conf.Configuration();
			path = new org.apache.hadoop.fs.Path(ROOT, outputFile);
			fs = path.getFileSystem(conf);
			@out = fs.create(path);
			writer = new org.apache.hadoop.io.file.tfile.TFile.Writer(@out, BLOCK_SIZE, compression
				, comparator, conf);
		}

		/// <exception cref="System.IO.IOException"/>
		protected override void tearDown()
		{
			if (!skip)
			{
				try
				{
					closeOutput();
				}
				catch (System.Exception)
				{
				}
				// no-op
				fs.delete(path, true);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testNoEntry()
		{
			if (skip)
			{
				return;
			}
			closeOutput();
			org.apache.hadoop.io.file.tfile.TestTFileByteArrays.readRecords(fs, path, 0, conf
				);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testOneEntryKnownLength()
		{
			if (skip)
			{
				return;
			}
			writeRecords(1, true, true);
			org.apache.hadoop.io.file.tfile.TestTFileByteArrays.readRecords(fs, path, 1, conf
				);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testOneEntryUnknownLength()
		{
			if (skip)
			{
				return;
			}
			writeRecords(1, false, false);
			// TODO: will throw exception at getValueLength, it's inconsistent though;
			// getKeyLength returns a value correctly, though initial length is -1
			org.apache.hadoop.io.file.tfile.TestTFileByteArrays.readRecords(fs, path, 1, conf
				);
		}

		// known key length, unknown value length
		/// <exception cref="System.IO.IOException"/>
		public virtual void testOneEntryMixedLengths1()
		{
			if (skip)
			{
				return;
			}
			writeRecords(1, true, false);
			org.apache.hadoop.io.file.tfile.TestTFileByteArrays.readRecords(fs, path, 1, conf
				);
		}

		// unknown key length, known value length
		/// <exception cref="System.IO.IOException"/>
		public virtual void testOneEntryMixedLengths2()
		{
			if (skip)
			{
				return;
			}
			writeRecords(1, false, true);
			org.apache.hadoop.io.file.tfile.TestTFileByteArrays.readRecords(fs, path, 1, conf
				);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testTwoEntriesKnownLength()
		{
			if (skip)
			{
				return;
			}
			writeRecords(2, true, true);
			org.apache.hadoop.io.file.tfile.TestTFileByteArrays.readRecords(fs, path, 2, conf
				);
		}

		// Negative test
		/// <exception cref="System.IO.IOException"/>
		public virtual void testFailureAddKeyWithoutValue()
		{
			if (skip)
			{
				return;
			}
			java.io.DataOutputStream dos = writer.prepareAppendKey(-1);
			dos.write(Sharpen.Runtime.getBytesForString("key0"));
			try
			{
				closeOutput();
				fail("Cannot add only a key without a value. ");
			}
			catch (System.InvalidOperationException)
			{
			}
		}

		// noop, expecting an exception
		/// <exception cref="System.IO.IOException"/>
		public virtual void testFailureAddValueWithoutKey()
		{
			if (skip)
			{
				return;
			}
			java.io.DataOutputStream outValue = null;
			try
			{
				outValue = writer.prepareAppendValue(6);
				outValue.write(Sharpen.Runtime.getBytesForString("value0"));
				fail("Cannot add a value without adding key first. ");
			}
			catch (System.Exception)
			{
			}
			finally
			{
				// noop, expecting an exception
				if (outValue != null)
				{
					outValue.close();
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testFailureOneEntryKnownLength()
		{
			if (skip)
			{
				return;
			}
			java.io.DataOutputStream outKey = writer.prepareAppendKey(2);
			try
			{
				outKey.write(Sharpen.Runtime.getBytesForString("key0"));
				fail("Specified key length mismatched the actual key length.");
			}
			catch (System.IO.IOException)
			{
			}
			// noop, expecting an exception
			java.io.DataOutputStream outValue = null;
			try
			{
				outValue = writer.prepareAppendValue(6);
				outValue.write(Sharpen.Runtime.getBytesForString("value0"));
			}
			catch (System.Exception)
			{
			}
		}

		// noop, expecting an exception
		/// <exception cref="System.IO.IOException"/>
		public virtual void testFailureKeyTooLong()
		{
			if (skip)
			{
				return;
			}
			java.io.DataOutputStream outKey = writer.prepareAppendKey(2);
			try
			{
				outKey.write(Sharpen.Runtime.getBytesForString("key0"));
				outKey.close();
				NUnit.Framework.Assert.Fail("Key is longer than requested.");
			}
			catch (System.Exception)
			{
			}
			finally
			{
			}
		}

		// noop, expecting an exception
		/// <exception cref="System.IO.IOException"/>
		public virtual void testFailureKeyTooShort()
		{
			if (skip)
			{
				return;
			}
			java.io.DataOutputStream outKey = writer.prepareAppendKey(4);
			outKey.write(Sharpen.Runtime.getBytesForString("key0"));
			outKey.close();
			java.io.DataOutputStream outValue = writer.prepareAppendValue(15);
			try
			{
				outValue.write(Sharpen.Runtime.getBytesForString("value0"));
				outValue.close();
				NUnit.Framework.Assert.Fail("Value is shorter than expected.");
			}
			catch (System.Exception)
			{
			}
			finally
			{
			}
		}

		// noop, expecting an exception
		/// <exception cref="System.IO.IOException"/>
		public virtual void testFailureValueTooLong()
		{
			if (skip)
			{
				return;
			}
			java.io.DataOutputStream outKey = writer.prepareAppendKey(4);
			outKey.write(Sharpen.Runtime.getBytesForString("key0"));
			outKey.close();
			java.io.DataOutputStream outValue = writer.prepareAppendValue(3);
			try
			{
				outValue.write(Sharpen.Runtime.getBytesForString("value0"));
				outValue.close();
				NUnit.Framework.Assert.Fail("Value is longer than expected.");
			}
			catch (System.Exception)
			{
			}
			// noop, expecting an exception
			try
			{
				outKey.close();
				outKey.close();
			}
			catch (System.Exception)
			{
				NUnit.Framework.Assert.Fail("Second or more close() should have no effect.");
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testFailureValueTooShort()
		{
			if (skip)
			{
				return;
			}
			java.io.DataOutputStream outKey = writer.prepareAppendKey(8);
			try
			{
				outKey.write(Sharpen.Runtime.getBytesForString("key0"));
				outKey.close();
				NUnit.Framework.Assert.Fail("Key is shorter than expected.");
			}
			catch (System.Exception)
			{
			}
			finally
			{
			}
		}

		// noop, expecting an exception
		/// <exception cref="System.IO.IOException"/>
		public virtual void testFailureCloseKeyStreamManyTimesInWriter()
		{
			if (skip)
			{
				return;
			}
			java.io.DataOutputStream outKey = writer.prepareAppendKey(4);
			try
			{
				outKey.write(Sharpen.Runtime.getBytesForString("key0"));
				outKey.close();
			}
			catch (System.Exception)
			{
			}
			finally
			{
				// noop, expecting an exception
				try
				{
					outKey.close();
				}
				catch (System.Exception)
				{
				}
			}
			// no-op
			outKey.close();
			outKey.close();
			NUnit.Framework.Assert.IsTrue("Multiple close should have no effect.", true);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testFailureKeyLongerThan64K()
		{
			if (skip)
			{
				return;
			}
			try
			{
				java.io.DataOutputStream outKey = writer.prepareAppendKey(64 * K + 1);
				NUnit.Framework.Assert.Fail("Failed to handle key longer than 64K.");
			}
			catch (System.IndexOutOfRangeException)
			{
			}
			// noop, expecting exceptions
			closeOutput();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testFailureKeyLongerThan64K_2()
		{
			if (skip)
			{
				return;
			}
			java.io.DataOutputStream outKey = writer.prepareAppendKey(-1);
			try
			{
				byte[] buf = new byte[K];
				java.util.Random rand = new java.util.Random();
				for (int nx = 0; nx < K + 2; nx++)
				{
					rand.nextBytes(buf);
					outKey.write(buf);
				}
				outKey.close();
				NUnit.Framework.Assert.Fail("Failed to handle key longer than 64K.");
			}
			catch (java.io.EOFException)
			{
			}
			finally
			{
				// noop, expecting exceptions
				try
				{
					closeOutput();
				}
				catch (System.Exception)
				{
				}
			}
		}

		// no-op
		/// <exception cref="System.IO.IOException"/>
		public virtual void testFailureNegativeOffset()
		{
			if (skip)
			{
				return;
			}
			writeRecords(2, true, true);
			org.apache.hadoop.io.file.tfile.TFile.Reader reader = new org.apache.hadoop.io.file.tfile.TFile.Reader
				(fs.open(path), fs.getFileStatus(path).getLen(), conf);
			org.apache.hadoop.io.file.tfile.TFile.Reader.Scanner scanner = reader.createScanner
				();
			byte[] buf = new byte[K];
			try
			{
				scanner.entry().getKey(buf, -1);
				NUnit.Framework.Assert.Fail("Failed to handle key negative offset.");
			}
			catch (System.Exception)
			{
			}
			finally
			{
			}
			// noop, expecting exceptions
			scanner.close();
			reader.close();
		}

		/// <summary>Verify that the compressed data size is less than raw data size.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void testFailureCompressionNotWorking()
		{
			if (skip)
			{
				return;
			}
			long rawDataSize = writeRecords(10000, false, false, false);
			if (!Sharpen.Runtime.equalsIgnoreCase(compression, org.apache.hadoop.io.file.tfile.Compression.Algorithm
				.NONE.getName()))
			{
				NUnit.Framework.Assert.IsTrue(@out.getPos() < rawDataSize);
			}
			closeOutput();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testFailureCompressionNotWorking2()
		{
			if (skip)
			{
				return;
			}
			long rawDataSize = writeRecords(10000, true, true, false);
			if (!Sharpen.Runtime.equalsIgnoreCase(compression, org.apache.hadoop.io.file.tfile.Compression.Algorithm
				.NONE.getName()))
			{
				NUnit.Framework.Assert.IsTrue(@out.getPos() < rawDataSize);
			}
			closeOutput();
		}

		/// <exception cref="System.IO.IOException"/>
		private long writeRecords(int count, bool knownKeyLength, bool knownValueLength, 
			bool close)
		{
			long rawDataSize = 0;
			for (int nx = 0; nx < count; nx++)
			{
				string key = org.apache.hadoop.io.file.tfile.TestTFileByteArrays.composeSortedKey
					("key", nx);
				java.io.DataOutputStream outKey = writer.prepareAppendKey(knownKeyLength ? key.Length
					 : -1);
				outKey.write(Sharpen.Runtime.getBytesForString(key));
				outKey.close();
				string value = "value" + nx;
				java.io.DataOutputStream outValue = writer.prepareAppendValue(knownValueLength ? 
					value.Length : -1);
				outValue.write(Sharpen.Runtime.getBytesForString(value));
				outValue.close();
				rawDataSize += org.apache.hadoop.io.WritableUtils.getVIntSize(Sharpen.Runtime.getBytesForString
					(key).Length) + Sharpen.Runtime.getBytesForString(key).Length + org.apache.hadoop.io.WritableUtils
					.getVIntSize(Sharpen.Runtime.getBytesForString(value).Length) + Sharpen.Runtime.getBytesForString
					(value).Length;
			}
			if (close)
			{
				closeOutput();
			}
			return rawDataSize;
		}

		/// <exception cref="System.IO.IOException"/>
		private long writeRecords(int count, bool knownKeyLength, bool knownValueLength)
		{
			return writeRecords(count, knownKeyLength, knownValueLength, true);
		}

		/// <exception cref="System.IO.IOException"/>
		private void closeOutput()
		{
			if (writer != null)
			{
				writer.close();
				writer = null;
			}
			if (@out != null)
			{
				@out.close();
				@out = null;
			}
		}
	}
}
