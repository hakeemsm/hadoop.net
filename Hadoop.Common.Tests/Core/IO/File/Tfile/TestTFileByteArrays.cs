using System;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.IO.Compress.Zlib;
using Sharpen;

namespace Org.Apache.Hadoop.IO.File.Tfile
{
	/// <summary>
	/// Byte arrays test case class using GZ compression codec, base class of none
	/// and LZO compression classes.
	/// </summary>
	public class TestTFileByteArrays
	{
		private static string Root = Runtime.GetProperty("test.build.data", "/tmp/tfile-test"
			);

		private const int BlockSize = 512;

		private const int BufSize = 64;

		private const int K = 1024;

		protected internal bool skip = false;

		private const string Key = "key";

		private const string Value = "value";

		private FileSystem fs;

		private Configuration conf = new Configuration();

		private Path path;

		private FSDataOutputStream @out;

		private TFile.Writer writer;

		private string compression = Compression.Algorithm.Gz.GetName();

		private string comparator = "memcmp";

		private readonly string outputFile = GetType().Name;

		private bool usingNative;

		private int records1stBlock = usingNative ? 5674 : 4480;

		private int records2ndBlock = usingNative ? 5574 : 4263;

		/*
		* pre-sampled numbers of records in one block, based on the given the
		* generated key and value strings. This is slightly different based on
		* whether or not the native libs are present.
		*/
		public virtual void Init(string compression, string comparator, int numRecords1stBlock
			, int numRecords2ndBlock)
		{
			Init(compression, comparator);
			this.records1stBlock = numRecords1stBlock;
			this.records2ndBlock = numRecords2ndBlock;
		}

		public virtual void Init(string compression, string comparator)
		{
			this.compression = compression;
			this.comparator = comparator;
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			path = new Path(Root, outputFile);
			fs = path.GetFileSystem(conf);
			@out = fs.Create(path);
			writer = new TFile.Writer(@out, BlockSize, compression, comparator, conf);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			if (!skip)
			{
				fs.Delete(path, true);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestNoDataEntry()
		{
			if (skip)
			{
				return;
			}
			CloseOutput();
			TFile.Reader reader = new TFile.Reader(fs.Open(path), fs.GetFileStatus(path).GetLen
				(), conf);
			NUnit.Framework.Assert.IsTrue(reader.IsSorted());
			TFile.Reader.Scanner scanner = reader.CreateScanner();
			NUnit.Framework.Assert.IsTrue(scanner.AtEnd());
			scanner.Close();
			reader.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestOneDataEntry()
		{
			if (skip)
			{
				return;
			}
			WriteRecords(1);
			ReadRecords(1);
			CheckBlockIndex(0, 0);
			ReadValueBeforeKey(0);
			ReadKeyWithoutValue(0);
			ReadValueWithoutKey(0);
			ReadKeyManyTimes(0);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestTwoDataEntries()
		{
			if (skip)
			{
				return;
			}
			WriteRecords(2);
			ReadRecords(2);
		}

		/// <summary>Fill up exactly one block.</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestOneBlock()
		{
			if (skip)
			{
				return;
			}
			// just under one block
			WriteRecords(records1stBlock);
			ReadRecords(records1stBlock);
			// last key should be in the first block (block 0)
			CheckBlockIndex(records1stBlock - 1, 0);
		}

		/// <summary>One block plus one record.</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestOneBlockPlusOneEntry()
		{
			if (skip)
			{
				return;
			}
			WriteRecords(records1stBlock + 1);
			ReadRecords(records1stBlock + 1);
			CheckBlockIndex(records1stBlock - 1, 0);
			CheckBlockIndex(records1stBlock, 1);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestTwoBlocks()
		{
			if (skip)
			{
				return;
			}
			WriteRecords(records1stBlock + 5);
			ReadRecords(records1stBlock + 5);
			CheckBlockIndex(records1stBlock + 4, 1);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestThreeBlocks()
		{
			if (skip)
			{
				return;
			}
			WriteRecords(2 * records1stBlock + 5);
			ReadRecords(2 * records1stBlock + 5);
			CheckBlockIndex(2 * records1stBlock + 4, 2);
			// 1st key in file
			ReadValueBeforeKey(0);
			ReadKeyWithoutValue(0);
			ReadValueWithoutKey(0);
			ReadKeyManyTimes(0);
			// last key in file
			ReadValueBeforeKey(2 * records1stBlock + 4);
			ReadKeyWithoutValue(2 * records1stBlock + 4);
			ReadValueWithoutKey(2 * records1stBlock + 4);
			ReadKeyManyTimes(2 * records1stBlock + 4);
			// 1st key in mid block, verify block indexes then read
			CheckBlockIndex(records1stBlock - 1, 0);
			CheckBlockIndex(records1stBlock, 1);
			ReadValueBeforeKey(records1stBlock);
			ReadKeyWithoutValue(records1stBlock);
			ReadValueWithoutKey(records1stBlock);
			ReadKeyManyTimes(records1stBlock);
			// last key in mid block, verify block indexes then read
			CheckBlockIndex(records1stBlock + records2ndBlock - 1, 1);
			CheckBlockIndex(records1stBlock + records2ndBlock, 2);
			ReadValueBeforeKey(records1stBlock + records2ndBlock - 1);
			ReadKeyWithoutValue(records1stBlock + records2ndBlock - 1);
			ReadValueWithoutKey(records1stBlock + records2ndBlock - 1);
			ReadKeyManyTimes(records1stBlock + records2ndBlock - 1);
			// mid in mid block
			ReadValueBeforeKey(records1stBlock + 10);
			ReadKeyWithoutValue(records1stBlock + 10);
			ReadValueWithoutKey(records1stBlock + 10);
			ReadKeyManyTimes(records1stBlock + 10);
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual TFile.Reader.Location Locate(TFile.Reader.Scanner scanner, byte[]
			 key)
		{
			if (scanner.SeekTo(key) == true)
			{
				return scanner.currentLocation;
			}
			return scanner.endLocation;
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestLocate()
		{
			if (skip)
			{
				return;
			}
			WriteRecords(3 * records1stBlock);
			TFile.Reader reader = new TFile.Reader(fs.Open(path), fs.GetFileStatus(path).GetLen
				(), conf);
			TFile.Reader.Scanner scanner = reader.CreateScanner();
			Locate(scanner, Sharpen.Runtime.GetBytesForString(ComposeSortedKey(Key, 2)));
			Locate(scanner, Sharpen.Runtime.GetBytesForString(ComposeSortedKey(Key, records1stBlock
				 - 1)));
			Locate(scanner, Sharpen.Runtime.GetBytesForString(ComposeSortedKey(Key, records1stBlock
				)));
			TFile.Reader.Location locX = Locate(scanner, Sharpen.Runtime.GetBytesForString("keyX"
				));
			NUnit.Framework.Assert.AreEqual(scanner.endLocation, locX);
			scanner.Close();
			reader.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestFailureWriterNotClosed()
		{
			if (skip)
			{
				return;
			}
			TFile.Reader reader = null;
			try
			{
				reader = new TFile.Reader(fs.Open(path), fs.GetFileStatus(path).GetLen(), conf);
				NUnit.Framework.Assert.Fail("Cannot read before closing the writer.");
			}
			catch (IOException)
			{
			}
			finally
			{
				// noop, expecting exceptions
				if (reader != null)
				{
					reader.Close();
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestFailureWriteMetaBlocksWithSameName()
		{
			if (skip)
			{
				return;
			}
			writer.Append(Sharpen.Runtime.GetBytesForString("keyX"), Sharpen.Runtime.GetBytesForString
				("valueX"));
			// create a new metablock
			DataOutputStream outMeta = writer.PrepareMetaBlock("testX", Compression.Algorithm
				.Gz.GetName());
			outMeta.Write(123);
			outMeta.Write(Sharpen.Runtime.GetBytesForString("foo"));
			outMeta.Close();
			// add the same metablock
			try
			{
				writer.PrepareMetaBlock("testX", Compression.Algorithm.Gz.GetName());
				NUnit.Framework.Assert.Fail("Cannot create metablocks with the same name.");
			}
			catch (Exception)
			{
			}
			// noop, expecting exceptions
			CloseOutput();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestFailureGetNonExistentMetaBlock()
		{
			if (skip)
			{
				return;
			}
			writer.Append(Sharpen.Runtime.GetBytesForString("keyX"), Sharpen.Runtime.GetBytesForString
				("valueX"));
			// create a new metablock
			DataOutputStream outMeta = writer.PrepareMetaBlock("testX", Compression.Algorithm
				.Gz.GetName());
			outMeta.Write(123);
			outMeta.Write(Sharpen.Runtime.GetBytesForString("foo"));
			outMeta.Close();
			CloseOutput();
			TFile.Reader reader = new TFile.Reader(fs.Open(path), fs.GetFileStatus(path).GetLen
				(), conf);
			DataInputStream mb = reader.GetMetaBlock("testX");
			NUnit.Framework.Assert.IsNotNull(mb);
			mb.Close();
			try
			{
				DataInputStream mbBad = reader.GetMetaBlock("testY");
				NUnit.Framework.Assert.Fail("Error on handling non-existent metablocks.");
			}
			catch (Exception)
			{
			}
			// noop, expecting exceptions
			reader.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestFailureWriteRecordAfterMetaBlock()
		{
			if (skip)
			{
				return;
			}
			// write a key/value first
			writer.Append(Sharpen.Runtime.GetBytesForString("keyX"), Sharpen.Runtime.GetBytesForString
				("valueX"));
			// create a new metablock
			DataOutputStream outMeta = writer.PrepareMetaBlock("testX", Compression.Algorithm
				.Gz.GetName());
			outMeta.Write(123);
			outMeta.Write(Sharpen.Runtime.GetBytesForString("dummy"));
			outMeta.Close();
			// add more key/value
			try
			{
				writer.Append(Sharpen.Runtime.GetBytesForString("keyY"), Sharpen.Runtime.GetBytesForString
					("valueY"));
				NUnit.Framework.Assert.Fail("Cannot add key/value after start adding meta blocks."
					);
			}
			catch (Exception)
			{
			}
			// noop, expecting exceptions
			CloseOutput();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestFailureReadValueManyTimes()
		{
			if (skip)
			{
				return;
			}
			WriteRecords(5);
			TFile.Reader reader = new TFile.Reader(fs.Open(path), fs.GetFileStatus(path).GetLen
				(), conf);
			TFile.Reader.Scanner scanner = reader.CreateScanner();
			byte[] vbuf = new byte[BufSize];
			int vlen = scanner.Entry().GetValueLength();
			scanner.Entry().GetValue(vbuf);
			NUnit.Framework.Assert.AreEqual(Sharpen.Runtime.GetStringForBytes(vbuf, 0, vlen), 
				Value + 0);
			try
			{
				scanner.Entry().GetValue(vbuf);
				NUnit.Framework.Assert.Fail("Cannot get the value mlutiple times.");
			}
			catch (Exception)
			{
			}
			// noop, expecting exceptions
			scanner.Close();
			reader.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestFailureBadCompressionCodec()
		{
			if (skip)
			{
				return;
			}
			CloseOutput();
			@out = fs.Create(path);
			try
			{
				writer = new TFile.Writer(@out, BlockSize, "BAD", comparator, conf);
				NUnit.Framework.Assert.Fail("Error on handling invalid compression codecs.");
			}
			catch (Exception)
			{
			}
		}

		// noop, expecting exceptions
		// e.printStackTrace();
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestFailureOpenEmptyFile()
		{
			if (skip)
			{
				return;
			}
			CloseOutput();
			// create an absolutely empty file
			path = new Path(fs.GetWorkingDirectory(), outputFile);
			@out = fs.Create(path);
			@out.Close();
			try
			{
				new TFile.Reader(fs.Open(path), fs.GetFileStatus(path).GetLen(), conf);
				NUnit.Framework.Assert.Fail("Error on handling empty files.");
			}
			catch (EOFException)
			{
			}
		}

		// noop, expecting exceptions
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestFailureOpenRandomFile()
		{
			if (skip)
			{
				return;
			}
			CloseOutput();
			// create an random file
			path = new Path(fs.GetWorkingDirectory(), outputFile);
			@out = fs.Create(path);
			Random rand = new Random();
			byte[] buf = new byte[K];
			// fill with > 1MB data
			for (int nx = 0; nx < K + 2; nx++)
			{
				rand.NextBytes(buf);
				@out.Write(buf);
			}
			@out.Close();
			try
			{
				new TFile.Reader(fs.Open(path), fs.GetFileStatus(path).GetLen(), conf);
				NUnit.Framework.Assert.Fail("Error on handling random files.");
			}
			catch (IOException)
			{
			}
		}

		// noop, expecting exceptions
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestFailureKeyLongerThan64K()
		{
			if (skip)
			{
				return;
			}
			byte[] buf = new byte[64 * K + 1];
			Random rand = new Random();
			rand.NextBytes(buf);
			try
			{
				writer.Append(buf, Sharpen.Runtime.GetBytesForString("valueX"));
			}
			catch (IndexOutOfRangeException)
			{
			}
			// noop, expecting exceptions
			CloseOutput();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestFailureOutOfOrderKeys()
		{
			if (skip)
			{
				return;
			}
			try
			{
				writer.Append(Sharpen.Runtime.GetBytesForString("keyM"), Sharpen.Runtime.GetBytesForString
					("valueM"));
				writer.Append(Sharpen.Runtime.GetBytesForString("keyA"), Sharpen.Runtime.GetBytesForString
					("valueA"));
				NUnit.Framework.Assert.Fail("Error on handling out of order keys.");
			}
			catch (Exception)
			{
			}
			// noop, expecting exceptions
			// e.printStackTrace();
			CloseOutput();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestFailureNegativeOffset()
		{
			if (skip)
			{
				return;
			}
			try
			{
				writer.Append(Sharpen.Runtime.GetBytesForString("keyX"), -1, 4, Sharpen.Runtime.GetBytesForString
					("valueX"), 0, 6);
				NUnit.Framework.Assert.Fail("Error on handling negative offset.");
			}
			catch (Exception)
			{
			}
			// noop, expecting exceptions
			CloseOutput();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestFailureNegativeOffset_2()
		{
			if (skip)
			{
				return;
			}
			CloseOutput();
			TFile.Reader reader = new TFile.Reader(fs.Open(path), fs.GetFileStatus(path).GetLen
				(), conf);
			TFile.Reader.Scanner scanner = reader.CreateScanner();
			try
			{
				scanner.LowerBound(Sharpen.Runtime.GetBytesForString("keyX"), -1, 4);
				NUnit.Framework.Assert.Fail("Error on handling negative offset.");
			}
			catch (Exception)
			{
			}
			finally
			{
				// noop, expecting exceptions
				reader.Close();
				scanner.Close();
			}
			CloseOutput();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestFailureNegativeLength()
		{
			if (skip)
			{
				return;
			}
			try
			{
				writer.Append(Sharpen.Runtime.GetBytesForString("keyX"), 0, -1, Sharpen.Runtime.GetBytesForString
					("valueX"), 0, 6);
				NUnit.Framework.Assert.Fail("Error on handling negative length.");
			}
			catch (Exception)
			{
			}
			// noop, expecting exceptions
			CloseOutput();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestFailureNegativeLength_2()
		{
			if (skip)
			{
				return;
			}
			CloseOutput();
			TFile.Reader reader = new TFile.Reader(fs.Open(path), fs.GetFileStatus(path).GetLen
				(), conf);
			TFile.Reader.Scanner scanner = reader.CreateScanner();
			try
			{
				scanner.LowerBound(Sharpen.Runtime.GetBytesForString("keyX"), 0, -1);
				NUnit.Framework.Assert.Fail("Error on handling negative length.");
			}
			catch (Exception)
			{
			}
			finally
			{
				// noop, expecting exceptions
				scanner.Close();
				reader.Close();
			}
			CloseOutput();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestFailureNegativeLength_3()
		{
			if (skip)
			{
				return;
			}
			WriteRecords(3);
			TFile.Reader reader = new TFile.Reader(fs.Open(path), fs.GetFileStatus(path).GetLen
				(), conf);
			TFile.Reader.Scanner scanner = reader.CreateScanner();
			try
			{
				// test negative array offset
				try
				{
					scanner.SeekTo(Sharpen.Runtime.GetBytesForString("keyY"), -1, 4);
					NUnit.Framework.Assert.Fail("Failed to handle negative offset.");
				}
				catch (Exception)
				{
				}
				// noop, expecting exceptions
				// test negative array length
				try
				{
					scanner.SeekTo(Sharpen.Runtime.GetBytesForString("keyY"), 0, -2);
					NUnit.Framework.Assert.Fail("Failed to handle negative key length.");
				}
				catch (Exception)
				{
				}
			}
			finally
			{
				// noop, expecting exceptions
				reader.Close();
				scanner.Close();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestFailureCompressionNotWorking()
		{
			if (skip)
			{
				return;
			}
			long rawDataSize = WriteRecords(10 * records1stBlock, false);
			if (!Sharpen.Runtime.EqualsIgnoreCase(compression, Compression.Algorithm.None.GetName
				()))
			{
				NUnit.Framework.Assert.IsTrue(@out.GetPos() < rawDataSize);
			}
			CloseOutput();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestFailureFileWriteNotAt0Position()
		{
			if (skip)
			{
				return;
			}
			CloseOutput();
			@out = fs.Create(path);
			@out.Write(123);
			try
			{
				writer = new TFile.Writer(@out, BlockSize, compression, comparator, conf);
				NUnit.Framework.Assert.Fail("Failed to catch file write not at position 0.");
			}
			catch (Exception)
			{
			}
			// noop, expecting exceptions
			CloseOutput();
		}

		/// <exception cref="System.IO.IOException"/>
		private long WriteRecords(int count)
		{
			return WriteRecords(count, true);
		}

		/// <exception cref="System.IO.IOException"/>
		private long WriteRecords(int count, bool close)
		{
			long rawDataSize = WriteRecords(writer, count);
			if (close)
			{
				CloseOutput();
			}
			return rawDataSize;
		}

		/// <exception cref="System.IO.IOException"/>
		internal static long WriteRecords(TFile.Writer writer, int count)
		{
			long rawDataSize = 0;
			int nx;
			for (nx = 0; nx < count; nx++)
			{
				byte[] key = Sharpen.Runtime.GetBytesForString(ComposeSortedKey(Key, nx));
				byte[] value = Sharpen.Runtime.GetBytesForString((Value + nx));
				writer.Append(key, value);
				rawDataSize += WritableUtils.GetVIntSize(key.Length) + key.Length + WritableUtils
					.GetVIntSize(value.Length) + value.Length;
			}
			return rawDataSize;
		}

		/// <summary>Insert some leading 0's in front of the value, to make the keys sorted.</summary>
		/// <param name="prefix"/>
		/// <param name="value"/>
		/// <returns/>
		internal static string ComposeSortedKey(string prefix, int value)
		{
			return string.Format("%s%010d", prefix, value);
		}

		/// <exception cref="System.IO.IOException"/>
		private void ReadRecords(int count)
		{
			ReadRecords(fs, path, count, conf);
		}

		/// <exception cref="System.IO.IOException"/>
		internal static void ReadRecords(FileSystem fs, Path path, int count, Configuration
			 conf)
		{
			TFile.Reader reader = new TFile.Reader(fs.Open(path), fs.GetFileStatus(path).GetLen
				(), conf);
			TFile.Reader.Scanner scanner = reader.CreateScanner();
			try
			{
				for (int nx = 0; nx < count; nx++, scanner.Advance())
				{
					NUnit.Framework.Assert.IsFalse(scanner.AtEnd());
					// Assert.assertTrue(scanner.next());
					byte[] kbuf = new byte[BufSize];
					int klen = scanner.Entry().GetKeyLength();
					scanner.Entry().GetKey(kbuf);
					NUnit.Framework.Assert.AreEqual(Sharpen.Runtime.GetStringForBytes(kbuf, 0, klen), 
						ComposeSortedKey(Key, nx));
					byte[] vbuf = new byte[BufSize];
					int vlen = scanner.Entry().GetValueLength();
					scanner.Entry().GetValue(vbuf);
					NUnit.Framework.Assert.AreEqual(Sharpen.Runtime.GetStringForBytes(vbuf, 0, vlen), 
						Value + nx);
				}
				NUnit.Framework.Assert.IsTrue(scanner.AtEnd());
				NUnit.Framework.Assert.IsFalse(scanner.Advance());
			}
			finally
			{
				scanner.Close();
				reader.Close();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void CheckBlockIndex(int recordIndex, int blockIndexExpected)
		{
			TFile.Reader reader = new TFile.Reader(fs.Open(path), fs.GetFileStatus(path).GetLen
				(), conf);
			TFile.Reader.Scanner scanner = reader.CreateScanner();
			scanner.SeekTo(Sharpen.Runtime.GetBytesForString(ComposeSortedKey(Key, recordIndex
				)));
			NUnit.Framework.Assert.AreEqual(blockIndexExpected, scanner.currentLocation.GetBlockIndex
				());
			scanner.Close();
			reader.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		private void ReadValueBeforeKey(int recordIndex)
		{
			TFile.Reader reader = new TFile.Reader(fs.Open(path), fs.GetFileStatus(path).GetLen
				(), conf);
			TFile.Reader.Scanner scanner = reader.CreateScannerByKey(Sharpen.Runtime.GetBytesForString
				(ComposeSortedKey(Key, recordIndex)), null);
			try
			{
				byte[] vbuf = new byte[BufSize];
				int vlen = scanner.Entry().GetValueLength();
				scanner.Entry().GetValue(vbuf);
				NUnit.Framework.Assert.AreEqual(Sharpen.Runtime.GetStringForBytes(vbuf, 0, vlen), 
					Value + recordIndex);
				byte[] kbuf = new byte[BufSize];
				int klen = scanner.Entry().GetKeyLength();
				scanner.Entry().GetKey(kbuf);
				NUnit.Framework.Assert.AreEqual(Sharpen.Runtime.GetStringForBytes(kbuf, 0, klen), 
					ComposeSortedKey(Key, recordIndex));
			}
			finally
			{
				scanner.Close();
				reader.Close();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void ReadKeyWithoutValue(int recordIndex)
		{
			TFile.Reader reader = new TFile.Reader(fs.Open(path), fs.GetFileStatus(path).GetLen
				(), conf);
			TFile.Reader.Scanner scanner = reader.CreateScannerByKey(Sharpen.Runtime.GetBytesForString
				(ComposeSortedKey(Key, recordIndex)), null);
			try
			{
				// read the indexed key
				byte[] kbuf1 = new byte[BufSize];
				int klen1 = scanner.Entry().GetKeyLength();
				scanner.Entry().GetKey(kbuf1);
				NUnit.Framework.Assert.AreEqual(Sharpen.Runtime.GetStringForBytes(kbuf1, 0, klen1
					), ComposeSortedKey(Key, recordIndex));
				if (scanner.Advance() && !scanner.AtEnd())
				{
					// read the next key following the indexed
					byte[] kbuf2 = new byte[BufSize];
					int klen2 = scanner.Entry().GetKeyLength();
					scanner.Entry().GetKey(kbuf2);
					NUnit.Framework.Assert.AreEqual(Sharpen.Runtime.GetStringForBytes(kbuf2, 0, klen2
						), ComposeSortedKey(Key, recordIndex + 1));
				}
			}
			finally
			{
				scanner.Close();
				reader.Close();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void ReadValueWithoutKey(int recordIndex)
		{
			TFile.Reader reader = new TFile.Reader(fs.Open(path), fs.GetFileStatus(path).GetLen
				(), conf);
			TFile.Reader.Scanner scanner = reader.CreateScannerByKey(Sharpen.Runtime.GetBytesForString
				(ComposeSortedKey(Key, recordIndex)), null);
			byte[] vbuf1 = new byte[BufSize];
			int vlen1 = scanner.Entry().GetValueLength();
			scanner.Entry().GetValue(vbuf1);
			NUnit.Framework.Assert.AreEqual(Sharpen.Runtime.GetStringForBytes(vbuf1, 0, vlen1
				), Value + recordIndex);
			if (scanner.Advance() && !scanner.AtEnd())
			{
				byte[] vbuf2 = new byte[BufSize];
				int vlen2 = scanner.Entry().GetValueLength();
				scanner.Entry().GetValue(vbuf2);
				NUnit.Framework.Assert.AreEqual(Sharpen.Runtime.GetStringForBytes(vbuf2, 0, vlen2
					), Value + (recordIndex + 1));
			}
			scanner.Close();
			reader.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		private void ReadKeyManyTimes(int recordIndex)
		{
			TFile.Reader reader = new TFile.Reader(fs.Open(path), fs.GetFileStatus(path).GetLen
				(), conf);
			TFile.Reader.Scanner scanner = reader.CreateScannerByKey(Sharpen.Runtime.GetBytesForString
				(ComposeSortedKey(Key, recordIndex)), null);
			// read the indexed key
			byte[] kbuf1 = new byte[BufSize];
			int klen1 = scanner.Entry().GetKeyLength();
			scanner.Entry().GetKey(kbuf1);
			NUnit.Framework.Assert.AreEqual(Sharpen.Runtime.GetStringForBytes(kbuf1, 0, klen1
				), ComposeSortedKey(Key, recordIndex));
			klen1 = scanner.Entry().GetKeyLength();
			scanner.Entry().GetKey(kbuf1);
			NUnit.Framework.Assert.AreEqual(Sharpen.Runtime.GetStringForBytes(kbuf1, 0, klen1
				), ComposeSortedKey(Key, recordIndex));
			klen1 = scanner.Entry().GetKeyLength();
			scanner.Entry().GetKey(kbuf1);
			NUnit.Framework.Assert.AreEqual(Sharpen.Runtime.GetStringForBytes(kbuf1, 0, klen1
				), ComposeSortedKey(Key, recordIndex));
			scanner.Close();
			reader.Close();
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

		public TestTFileByteArrays()
		{
			usingNative = ZlibFactory.IsNativeZlibLoaded(conf);
		}
	}
}
