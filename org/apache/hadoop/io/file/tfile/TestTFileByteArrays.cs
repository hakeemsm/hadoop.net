using Sharpen;

namespace org.apache.hadoop.io.file.tfile
{
	/// <summary>
	/// Byte arrays test case class using GZ compression codec, base class of none
	/// and LZO compression classes.
	/// </summary>
	public class TestTFileByteArrays
	{
		private static string ROOT = Sharpen.Runtime.getProperty("test.build.data", "/tmp/tfile-test"
			);

		private const int BLOCK_SIZE = 512;

		private const int BUF_SIZE = 64;

		private const int K = 1024;

		protected internal bool skip = false;

		private const string KEY = "key";

		private const string VALUE = "value";

		private org.apache.hadoop.fs.FileSystem fs;

		private org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
			();

		private org.apache.hadoop.fs.Path path;

		private org.apache.hadoop.fs.FSDataOutputStream @out;

		private org.apache.hadoop.io.file.tfile.TFile.Writer writer;

		private string compression = org.apache.hadoop.io.file.tfile.Compression.Algorithm
			.GZ.getName();

		private string comparator = "memcmp";

		private readonly string outputFile = Sharpen.Runtime.getClassForObject(this).getSimpleName
			();

		private bool usingNative;

		private int records1stBlock = usingNative ? 5674 : 4480;

		private int records2ndBlock = usingNative ? 5574 : 4263;

		/*
		* pre-sampled numbers of records in one block, based on the given the
		* generated key and value strings. This is slightly different based on
		* whether or not the native libs are present.
		*/
		public virtual void init(string compression, string comparator, int numRecords1stBlock
			, int numRecords2ndBlock)
		{
			init(compression, comparator);
			this.records1stBlock = numRecords1stBlock;
			this.records2ndBlock = numRecords2ndBlock;
		}

		public virtual void init(string compression, string comparator)
		{
			this.compression = compression;
			this.comparator = comparator;
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.SetUp]
		public virtual void setUp()
		{
			path = new org.apache.hadoop.fs.Path(ROOT, outputFile);
			fs = path.getFileSystem(conf);
			@out = fs.create(path);
			writer = new org.apache.hadoop.io.file.tfile.TFile.Writer(@out, BLOCK_SIZE, compression
				, comparator, conf);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.TearDown]
		public virtual void tearDown()
		{
			if (!skip)
			{
				fs.delete(path, true);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testNoDataEntry()
		{
			if (skip)
			{
				return;
			}
			closeOutput();
			org.apache.hadoop.io.file.tfile.TFile.Reader reader = new org.apache.hadoop.io.file.tfile.TFile.Reader
				(fs.open(path), fs.getFileStatus(path).getLen(), conf);
			NUnit.Framework.Assert.IsTrue(reader.isSorted());
			org.apache.hadoop.io.file.tfile.TFile.Reader.Scanner scanner = reader.createScanner
				();
			NUnit.Framework.Assert.IsTrue(scanner.atEnd());
			scanner.close();
			reader.close();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testOneDataEntry()
		{
			if (skip)
			{
				return;
			}
			writeRecords(1);
			readRecords(1);
			checkBlockIndex(0, 0);
			readValueBeforeKey(0);
			readKeyWithoutValue(0);
			readValueWithoutKey(0);
			readKeyManyTimes(0);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testTwoDataEntries()
		{
			if (skip)
			{
				return;
			}
			writeRecords(2);
			readRecords(2);
		}

		/// <summary>Fill up exactly one block.</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testOneBlock()
		{
			if (skip)
			{
				return;
			}
			// just under one block
			writeRecords(records1stBlock);
			readRecords(records1stBlock);
			// last key should be in the first block (block 0)
			checkBlockIndex(records1stBlock - 1, 0);
		}

		/// <summary>One block plus one record.</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testOneBlockPlusOneEntry()
		{
			if (skip)
			{
				return;
			}
			writeRecords(records1stBlock + 1);
			readRecords(records1stBlock + 1);
			checkBlockIndex(records1stBlock - 1, 0);
			checkBlockIndex(records1stBlock, 1);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testTwoBlocks()
		{
			if (skip)
			{
				return;
			}
			writeRecords(records1stBlock + 5);
			readRecords(records1stBlock + 5);
			checkBlockIndex(records1stBlock + 4, 1);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testThreeBlocks()
		{
			if (skip)
			{
				return;
			}
			writeRecords(2 * records1stBlock + 5);
			readRecords(2 * records1stBlock + 5);
			checkBlockIndex(2 * records1stBlock + 4, 2);
			// 1st key in file
			readValueBeforeKey(0);
			readKeyWithoutValue(0);
			readValueWithoutKey(0);
			readKeyManyTimes(0);
			// last key in file
			readValueBeforeKey(2 * records1stBlock + 4);
			readKeyWithoutValue(2 * records1stBlock + 4);
			readValueWithoutKey(2 * records1stBlock + 4);
			readKeyManyTimes(2 * records1stBlock + 4);
			// 1st key in mid block, verify block indexes then read
			checkBlockIndex(records1stBlock - 1, 0);
			checkBlockIndex(records1stBlock, 1);
			readValueBeforeKey(records1stBlock);
			readKeyWithoutValue(records1stBlock);
			readValueWithoutKey(records1stBlock);
			readKeyManyTimes(records1stBlock);
			// last key in mid block, verify block indexes then read
			checkBlockIndex(records1stBlock + records2ndBlock - 1, 1);
			checkBlockIndex(records1stBlock + records2ndBlock, 2);
			readValueBeforeKey(records1stBlock + records2ndBlock - 1);
			readKeyWithoutValue(records1stBlock + records2ndBlock - 1);
			readValueWithoutKey(records1stBlock + records2ndBlock - 1);
			readKeyManyTimes(records1stBlock + records2ndBlock - 1);
			// mid in mid block
			readValueBeforeKey(records1stBlock + 10);
			readKeyWithoutValue(records1stBlock + 10);
			readValueWithoutKey(records1stBlock + 10);
			readKeyManyTimes(records1stBlock + 10);
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual org.apache.hadoop.io.file.tfile.TFile.Reader.Location locate(org.apache.hadoop.io.file.tfile.TFile.Reader.Scanner
			 scanner, byte[] key)
		{
			if (scanner.seekTo(key) == true)
			{
				return scanner.currentLocation;
			}
			return scanner.endLocation;
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testLocate()
		{
			if (skip)
			{
				return;
			}
			writeRecords(3 * records1stBlock);
			org.apache.hadoop.io.file.tfile.TFile.Reader reader = new org.apache.hadoop.io.file.tfile.TFile.Reader
				(fs.open(path), fs.getFileStatus(path).getLen(), conf);
			org.apache.hadoop.io.file.tfile.TFile.Reader.Scanner scanner = reader.createScanner
				();
			locate(scanner, Sharpen.Runtime.getBytesForString(composeSortedKey(KEY, 2)));
			locate(scanner, Sharpen.Runtime.getBytesForString(composeSortedKey(KEY, records1stBlock
				 - 1)));
			locate(scanner, Sharpen.Runtime.getBytesForString(composeSortedKey(KEY, records1stBlock
				)));
			org.apache.hadoop.io.file.tfile.TFile.Reader.Location locX = locate(scanner, Sharpen.Runtime.getBytesForString
				("keyX"));
			NUnit.Framework.Assert.AreEqual(scanner.endLocation, locX);
			scanner.close();
			reader.close();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testFailureWriterNotClosed()
		{
			if (skip)
			{
				return;
			}
			org.apache.hadoop.io.file.tfile.TFile.Reader reader = null;
			try
			{
				reader = new org.apache.hadoop.io.file.tfile.TFile.Reader(fs.open(path), fs.getFileStatus
					(path).getLen(), conf);
				NUnit.Framework.Assert.Fail("Cannot read before closing the writer.");
			}
			catch (System.IO.IOException)
			{
			}
			finally
			{
				// noop, expecting exceptions
				if (reader != null)
				{
					reader.close();
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testFailureWriteMetaBlocksWithSameName()
		{
			if (skip)
			{
				return;
			}
			writer.append(Sharpen.Runtime.getBytesForString("keyX"), Sharpen.Runtime.getBytesForString
				("valueX"));
			// create a new metablock
			java.io.DataOutputStream outMeta = writer.prepareMetaBlock("testX", org.apache.hadoop.io.file.tfile.Compression.Algorithm
				.GZ.getName());
			outMeta.write(123);
			outMeta.write(Sharpen.Runtime.getBytesForString("foo"));
			outMeta.close();
			// add the same metablock
			try
			{
				writer.prepareMetaBlock("testX", org.apache.hadoop.io.file.tfile.Compression.Algorithm
					.GZ.getName());
				NUnit.Framework.Assert.Fail("Cannot create metablocks with the same name.");
			}
			catch (System.Exception)
			{
			}
			// noop, expecting exceptions
			closeOutput();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testFailureGetNonExistentMetaBlock()
		{
			if (skip)
			{
				return;
			}
			writer.append(Sharpen.Runtime.getBytesForString("keyX"), Sharpen.Runtime.getBytesForString
				("valueX"));
			// create a new metablock
			java.io.DataOutputStream outMeta = writer.prepareMetaBlock("testX", org.apache.hadoop.io.file.tfile.Compression.Algorithm
				.GZ.getName());
			outMeta.write(123);
			outMeta.write(Sharpen.Runtime.getBytesForString("foo"));
			outMeta.close();
			closeOutput();
			org.apache.hadoop.io.file.tfile.TFile.Reader reader = new org.apache.hadoop.io.file.tfile.TFile.Reader
				(fs.open(path), fs.getFileStatus(path).getLen(), conf);
			java.io.DataInputStream mb = reader.getMetaBlock("testX");
			NUnit.Framework.Assert.IsNotNull(mb);
			mb.close();
			try
			{
				java.io.DataInputStream mbBad = reader.getMetaBlock("testY");
				NUnit.Framework.Assert.Fail("Error on handling non-existent metablocks.");
			}
			catch (System.Exception)
			{
			}
			// noop, expecting exceptions
			reader.close();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testFailureWriteRecordAfterMetaBlock()
		{
			if (skip)
			{
				return;
			}
			// write a key/value first
			writer.append(Sharpen.Runtime.getBytesForString("keyX"), Sharpen.Runtime.getBytesForString
				("valueX"));
			// create a new metablock
			java.io.DataOutputStream outMeta = writer.prepareMetaBlock("testX", org.apache.hadoop.io.file.tfile.Compression.Algorithm
				.GZ.getName());
			outMeta.write(123);
			outMeta.write(Sharpen.Runtime.getBytesForString("dummy"));
			outMeta.close();
			// add more key/value
			try
			{
				writer.append(Sharpen.Runtime.getBytesForString("keyY"), Sharpen.Runtime.getBytesForString
					("valueY"));
				NUnit.Framework.Assert.Fail("Cannot add key/value after start adding meta blocks."
					);
			}
			catch (System.Exception)
			{
			}
			// noop, expecting exceptions
			closeOutput();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testFailureReadValueManyTimes()
		{
			if (skip)
			{
				return;
			}
			writeRecords(5);
			org.apache.hadoop.io.file.tfile.TFile.Reader reader = new org.apache.hadoop.io.file.tfile.TFile.Reader
				(fs.open(path), fs.getFileStatus(path).getLen(), conf);
			org.apache.hadoop.io.file.tfile.TFile.Reader.Scanner scanner = reader.createScanner
				();
			byte[] vbuf = new byte[BUF_SIZE];
			int vlen = scanner.entry().getValueLength();
			scanner.entry().getValue(vbuf);
			NUnit.Framework.Assert.AreEqual(Sharpen.Runtime.getStringForBytes(vbuf, 0, vlen), 
				VALUE + 0);
			try
			{
				scanner.entry().getValue(vbuf);
				NUnit.Framework.Assert.Fail("Cannot get the value mlutiple times.");
			}
			catch (System.Exception)
			{
			}
			// noop, expecting exceptions
			scanner.close();
			reader.close();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testFailureBadCompressionCodec()
		{
			if (skip)
			{
				return;
			}
			closeOutput();
			@out = fs.create(path);
			try
			{
				writer = new org.apache.hadoop.io.file.tfile.TFile.Writer(@out, BLOCK_SIZE, "BAD"
					, comparator, conf);
				NUnit.Framework.Assert.Fail("Error on handling invalid compression codecs.");
			}
			catch (System.Exception)
			{
			}
		}

		// noop, expecting exceptions
		// e.printStackTrace();
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testFailureOpenEmptyFile()
		{
			if (skip)
			{
				return;
			}
			closeOutput();
			// create an absolutely empty file
			path = new org.apache.hadoop.fs.Path(fs.getWorkingDirectory(), outputFile);
			@out = fs.create(path);
			@out.close();
			try
			{
				new org.apache.hadoop.io.file.tfile.TFile.Reader(fs.open(path), fs.getFileStatus(
					path).getLen(), conf);
				NUnit.Framework.Assert.Fail("Error on handling empty files.");
			}
			catch (java.io.EOFException)
			{
			}
		}

		// noop, expecting exceptions
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testFailureOpenRandomFile()
		{
			if (skip)
			{
				return;
			}
			closeOutput();
			// create an random file
			path = new org.apache.hadoop.fs.Path(fs.getWorkingDirectory(), outputFile);
			@out = fs.create(path);
			java.util.Random rand = new java.util.Random();
			byte[] buf = new byte[K];
			// fill with > 1MB data
			for (int nx = 0; nx < K + 2; nx++)
			{
				rand.nextBytes(buf);
				@out.write(buf);
			}
			@out.close();
			try
			{
				new org.apache.hadoop.io.file.tfile.TFile.Reader(fs.open(path), fs.getFileStatus(
					path).getLen(), conf);
				NUnit.Framework.Assert.Fail("Error on handling random files.");
			}
			catch (System.IO.IOException)
			{
			}
		}

		// noop, expecting exceptions
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testFailureKeyLongerThan64K()
		{
			if (skip)
			{
				return;
			}
			byte[] buf = new byte[64 * K + 1];
			java.util.Random rand = new java.util.Random();
			rand.nextBytes(buf);
			try
			{
				writer.append(buf, Sharpen.Runtime.getBytesForString("valueX"));
			}
			catch (System.IndexOutOfRangeException)
			{
			}
			// noop, expecting exceptions
			closeOutput();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testFailureOutOfOrderKeys()
		{
			if (skip)
			{
				return;
			}
			try
			{
				writer.append(Sharpen.Runtime.getBytesForString("keyM"), Sharpen.Runtime.getBytesForString
					("valueM"));
				writer.append(Sharpen.Runtime.getBytesForString("keyA"), Sharpen.Runtime.getBytesForString
					("valueA"));
				NUnit.Framework.Assert.Fail("Error on handling out of order keys.");
			}
			catch (System.Exception)
			{
			}
			// noop, expecting exceptions
			// e.printStackTrace();
			closeOutput();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testFailureNegativeOffset()
		{
			if (skip)
			{
				return;
			}
			try
			{
				writer.append(Sharpen.Runtime.getBytesForString("keyX"), -1, 4, Sharpen.Runtime.getBytesForString
					("valueX"), 0, 6);
				NUnit.Framework.Assert.Fail("Error on handling negative offset.");
			}
			catch (System.Exception)
			{
			}
			// noop, expecting exceptions
			closeOutput();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testFailureNegativeOffset_2()
		{
			if (skip)
			{
				return;
			}
			closeOutput();
			org.apache.hadoop.io.file.tfile.TFile.Reader reader = new org.apache.hadoop.io.file.tfile.TFile.Reader
				(fs.open(path), fs.getFileStatus(path).getLen(), conf);
			org.apache.hadoop.io.file.tfile.TFile.Reader.Scanner scanner = reader.createScanner
				();
			try
			{
				scanner.lowerBound(Sharpen.Runtime.getBytesForString("keyX"), -1, 4);
				NUnit.Framework.Assert.Fail("Error on handling negative offset.");
			}
			catch (System.Exception)
			{
			}
			finally
			{
				// noop, expecting exceptions
				reader.close();
				scanner.close();
			}
			closeOutput();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testFailureNegativeLength()
		{
			if (skip)
			{
				return;
			}
			try
			{
				writer.append(Sharpen.Runtime.getBytesForString("keyX"), 0, -1, Sharpen.Runtime.getBytesForString
					("valueX"), 0, 6);
				NUnit.Framework.Assert.Fail("Error on handling negative length.");
			}
			catch (System.Exception)
			{
			}
			// noop, expecting exceptions
			closeOutput();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testFailureNegativeLength_2()
		{
			if (skip)
			{
				return;
			}
			closeOutput();
			org.apache.hadoop.io.file.tfile.TFile.Reader reader = new org.apache.hadoop.io.file.tfile.TFile.Reader
				(fs.open(path), fs.getFileStatus(path).getLen(), conf);
			org.apache.hadoop.io.file.tfile.TFile.Reader.Scanner scanner = reader.createScanner
				();
			try
			{
				scanner.lowerBound(Sharpen.Runtime.getBytesForString("keyX"), 0, -1);
				NUnit.Framework.Assert.Fail("Error on handling negative length.");
			}
			catch (System.Exception)
			{
			}
			finally
			{
				// noop, expecting exceptions
				scanner.close();
				reader.close();
			}
			closeOutput();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testFailureNegativeLength_3()
		{
			if (skip)
			{
				return;
			}
			writeRecords(3);
			org.apache.hadoop.io.file.tfile.TFile.Reader reader = new org.apache.hadoop.io.file.tfile.TFile.Reader
				(fs.open(path), fs.getFileStatus(path).getLen(), conf);
			org.apache.hadoop.io.file.tfile.TFile.Reader.Scanner scanner = reader.createScanner
				();
			try
			{
				// test negative array offset
				try
				{
					scanner.seekTo(Sharpen.Runtime.getBytesForString("keyY"), -1, 4);
					NUnit.Framework.Assert.Fail("Failed to handle negative offset.");
				}
				catch (System.Exception)
				{
				}
				// noop, expecting exceptions
				// test negative array length
				try
				{
					scanner.seekTo(Sharpen.Runtime.getBytesForString("keyY"), 0, -2);
					NUnit.Framework.Assert.Fail("Failed to handle negative key length.");
				}
				catch (System.Exception)
				{
				}
			}
			finally
			{
				// noop, expecting exceptions
				reader.close();
				scanner.close();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testFailureCompressionNotWorking()
		{
			if (skip)
			{
				return;
			}
			long rawDataSize = writeRecords(10 * records1stBlock, false);
			if (!Sharpen.Runtime.equalsIgnoreCase(compression, org.apache.hadoop.io.file.tfile.Compression.Algorithm
				.NONE.getName()))
			{
				NUnit.Framework.Assert.IsTrue(@out.getPos() < rawDataSize);
			}
			closeOutput();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testFailureFileWriteNotAt0Position()
		{
			if (skip)
			{
				return;
			}
			closeOutput();
			@out = fs.create(path);
			@out.write(123);
			try
			{
				writer = new org.apache.hadoop.io.file.tfile.TFile.Writer(@out, BLOCK_SIZE, compression
					, comparator, conf);
				NUnit.Framework.Assert.Fail("Failed to catch file write not at position 0.");
			}
			catch (System.Exception)
			{
			}
			// noop, expecting exceptions
			closeOutput();
		}

		/// <exception cref="System.IO.IOException"/>
		private long writeRecords(int count)
		{
			return writeRecords(count, true);
		}

		/// <exception cref="System.IO.IOException"/>
		private long writeRecords(int count, bool close)
		{
			long rawDataSize = writeRecords(writer, count);
			if (close)
			{
				closeOutput();
			}
			return rawDataSize;
		}

		/// <exception cref="System.IO.IOException"/>
		internal static long writeRecords(org.apache.hadoop.io.file.tfile.TFile.Writer writer
			, int count)
		{
			long rawDataSize = 0;
			int nx;
			for (nx = 0; nx < count; nx++)
			{
				byte[] key = Sharpen.Runtime.getBytesForString(composeSortedKey(KEY, nx));
				byte[] value = Sharpen.Runtime.getBytesForString((VALUE + nx));
				writer.append(key, value);
				rawDataSize += org.apache.hadoop.io.WritableUtils.getVIntSize(key.Length) + key.Length
					 + org.apache.hadoop.io.WritableUtils.getVIntSize(value.Length) + value.Length;
			}
			return rawDataSize;
		}

		/// <summary>Insert some leading 0's in front of the value, to make the keys sorted.</summary>
		/// <param name="prefix"/>
		/// <param name="value"/>
		/// <returns/>
		internal static string composeSortedKey(string prefix, int value)
		{
			return string.format("%s%010d", prefix, value);
		}

		/// <exception cref="System.IO.IOException"/>
		private void readRecords(int count)
		{
			readRecords(fs, path, count, conf);
		}

		/// <exception cref="System.IO.IOException"/>
		internal static void readRecords(org.apache.hadoop.fs.FileSystem fs, org.apache.hadoop.fs.Path
			 path, int count, org.apache.hadoop.conf.Configuration conf)
		{
			org.apache.hadoop.io.file.tfile.TFile.Reader reader = new org.apache.hadoop.io.file.tfile.TFile.Reader
				(fs.open(path), fs.getFileStatus(path).getLen(), conf);
			org.apache.hadoop.io.file.tfile.TFile.Reader.Scanner scanner = reader.createScanner
				();
			try
			{
				for (int nx = 0; nx < count; nx++, scanner.advance())
				{
					NUnit.Framework.Assert.IsFalse(scanner.atEnd());
					// Assert.assertTrue(scanner.next());
					byte[] kbuf = new byte[BUF_SIZE];
					int klen = scanner.entry().getKeyLength();
					scanner.entry().getKey(kbuf);
					NUnit.Framework.Assert.AreEqual(Sharpen.Runtime.getStringForBytes(kbuf, 0, klen), 
						composeSortedKey(KEY, nx));
					byte[] vbuf = new byte[BUF_SIZE];
					int vlen = scanner.entry().getValueLength();
					scanner.entry().getValue(vbuf);
					NUnit.Framework.Assert.AreEqual(Sharpen.Runtime.getStringForBytes(vbuf, 0, vlen), 
						VALUE + nx);
				}
				NUnit.Framework.Assert.IsTrue(scanner.atEnd());
				NUnit.Framework.Assert.IsFalse(scanner.advance());
			}
			finally
			{
				scanner.close();
				reader.close();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void checkBlockIndex(int recordIndex, int blockIndexExpected)
		{
			org.apache.hadoop.io.file.tfile.TFile.Reader reader = new org.apache.hadoop.io.file.tfile.TFile.Reader
				(fs.open(path), fs.getFileStatus(path).getLen(), conf);
			org.apache.hadoop.io.file.tfile.TFile.Reader.Scanner scanner = reader.createScanner
				();
			scanner.seekTo(Sharpen.Runtime.getBytesForString(composeSortedKey(KEY, recordIndex
				)));
			NUnit.Framework.Assert.AreEqual(blockIndexExpected, scanner.currentLocation.getBlockIndex
				());
			scanner.close();
			reader.close();
		}

		/// <exception cref="System.IO.IOException"/>
		private void readValueBeforeKey(int recordIndex)
		{
			org.apache.hadoop.io.file.tfile.TFile.Reader reader = new org.apache.hadoop.io.file.tfile.TFile.Reader
				(fs.open(path), fs.getFileStatus(path).getLen(), conf);
			org.apache.hadoop.io.file.tfile.TFile.Reader.Scanner scanner = reader.createScannerByKey
				(Sharpen.Runtime.getBytesForString(composeSortedKey(KEY, recordIndex)), null);
			try
			{
				byte[] vbuf = new byte[BUF_SIZE];
				int vlen = scanner.entry().getValueLength();
				scanner.entry().getValue(vbuf);
				NUnit.Framework.Assert.AreEqual(Sharpen.Runtime.getStringForBytes(vbuf, 0, vlen), 
					VALUE + recordIndex);
				byte[] kbuf = new byte[BUF_SIZE];
				int klen = scanner.entry().getKeyLength();
				scanner.entry().getKey(kbuf);
				NUnit.Framework.Assert.AreEqual(Sharpen.Runtime.getStringForBytes(kbuf, 0, klen), 
					composeSortedKey(KEY, recordIndex));
			}
			finally
			{
				scanner.close();
				reader.close();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void readKeyWithoutValue(int recordIndex)
		{
			org.apache.hadoop.io.file.tfile.TFile.Reader reader = new org.apache.hadoop.io.file.tfile.TFile.Reader
				(fs.open(path), fs.getFileStatus(path).getLen(), conf);
			org.apache.hadoop.io.file.tfile.TFile.Reader.Scanner scanner = reader.createScannerByKey
				(Sharpen.Runtime.getBytesForString(composeSortedKey(KEY, recordIndex)), null);
			try
			{
				// read the indexed key
				byte[] kbuf1 = new byte[BUF_SIZE];
				int klen1 = scanner.entry().getKeyLength();
				scanner.entry().getKey(kbuf1);
				NUnit.Framework.Assert.AreEqual(Sharpen.Runtime.getStringForBytes(kbuf1, 0, klen1
					), composeSortedKey(KEY, recordIndex));
				if (scanner.advance() && !scanner.atEnd())
				{
					// read the next key following the indexed
					byte[] kbuf2 = new byte[BUF_SIZE];
					int klen2 = scanner.entry().getKeyLength();
					scanner.entry().getKey(kbuf2);
					NUnit.Framework.Assert.AreEqual(Sharpen.Runtime.getStringForBytes(kbuf2, 0, klen2
						), composeSortedKey(KEY, recordIndex + 1));
				}
			}
			finally
			{
				scanner.close();
				reader.close();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void readValueWithoutKey(int recordIndex)
		{
			org.apache.hadoop.io.file.tfile.TFile.Reader reader = new org.apache.hadoop.io.file.tfile.TFile.Reader
				(fs.open(path), fs.getFileStatus(path).getLen(), conf);
			org.apache.hadoop.io.file.tfile.TFile.Reader.Scanner scanner = reader.createScannerByKey
				(Sharpen.Runtime.getBytesForString(composeSortedKey(KEY, recordIndex)), null);
			byte[] vbuf1 = new byte[BUF_SIZE];
			int vlen1 = scanner.entry().getValueLength();
			scanner.entry().getValue(vbuf1);
			NUnit.Framework.Assert.AreEqual(Sharpen.Runtime.getStringForBytes(vbuf1, 0, vlen1
				), VALUE + recordIndex);
			if (scanner.advance() && !scanner.atEnd())
			{
				byte[] vbuf2 = new byte[BUF_SIZE];
				int vlen2 = scanner.entry().getValueLength();
				scanner.entry().getValue(vbuf2);
				NUnit.Framework.Assert.AreEqual(Sharpen.Runtime.getStringForBytes(vbuf2, 0, vlen2
					), VALUE + (recordIndex + 1));
			}
			scanner.close();
			reader.close();
		}

		/// <exception cref="System.IO.IOException"/>
		private void readKeyManyTimes(int recordIndex)
		{
			org.apache.hadoop.io.file.tfile.TFile.Reader reader = new org.apache.hadoop.io.file.tfile.TFile.Reader
				(fs.open(path), fs.getFileStatus(path).getLen(), conf);
			org.apache.hadoop.io.file.tfile.TFile.Reader.Scanner scanner = reader.createScannerByKey
				(Sharpen.Runtime.getBytesForString(composeSortedKey(KEY, recordIndex)), null);
			// read the indexed key
			byte[] kbuf1 = new byte[BUF_SIZE];
			int klen1 = scanner.entry().getKeyLength();
			scanner.entry().getKey(kbuf1);
			NUnit.Framework.Assert.AreEqual(Sharpen.Runtime.getStringForBytes(kbuf1, 0, klen1
				), composeSortedKey(KEY, recordIndex));
			klen1 = scanner.entry().getKeyLength();
			scanner.entry().getKey(kbuf1);
			NUnit.Framework.Assert.AreEqual(Sharpen.Runtime.getStringForBytes(kbuf1, 0, klen1
				), composeSortedKey(KEY, recordIndex));
			klen1 = scanner.entry().getKeyLength();
			scanner.entry().getKey(kbuf1);
			NUnit.Framework.Assert.AreEqual(Sharpen.Runtime.getStringForBytes(kbuf1, 0, klen1
				), composeSortedKey(KEY, recordIndex));
			scanner.close();
			reader.close();
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

		public TestTFileByteArrays()
		{
			usingNative = org.apache.hadoop.io.compress.zlib.ZlibFactory.isNativeZlibLoaded(conf
				);
		}
	}
}
