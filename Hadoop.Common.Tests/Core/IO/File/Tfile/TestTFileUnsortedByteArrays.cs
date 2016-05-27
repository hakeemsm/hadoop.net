using System;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.IO.File.Tfile
{
	public class TestTFileUnsortedByteArrays : TestCase
	{
		private static string Root = Runtime.GetProperty("test.build.data", "/tmp/tfile-test"
			);

		private const int BlockSize = 512;

		private const int BufSize = 64;

		private FileSystem fs;

		private Configuration conf;

		private Path path;

		private FSDataOutputStream @out;

		private TFile.Writer writer;

		private string compression = Compression.Algorithm.Gz.GetName();

		private string outputFile = "TFileTestUnsorted";

		private int records1stBlock = 4314;

		private int records2ndBlock = 4108;

		/*
		* pre-sampled numbers of records in one block, based on the given the
		* generated key and value strings
		*/
		public virtual void Init(string compression, string outputFile, int numRecords1stBlock
			, int numRecords2ndBlock)
		{
			this.compression = compression;
			this.outputFile = outputFile;
			this.records1stBlock = numRecords1stBlock;
			this.records2ndBlock = numRecords2ndBlock;
		}

		/// <exception cref="System.IO.IOException"/>
		protected override void SetUp()
		{
			conf = new Configuration();
			path = new Path(Root, outputFile);
			fs = path.GetFileSystem(conf);
			@out = fs.Create(path);
			writer = new TFile.Writer(@out, BlockSize, compression, null, conf);
			writer.Append(Sharpen.Runtime.GetBytesForString("keyZ"), Sharpen.Runtime.GetBytesForString
				("valueZ"));
			writer.Append(Sharpen.Runtime.GetBytesForString("keyM"), Sharpen.Runtime.GetBytesForString
				("valueM"));
			writer.Append(Sharpen.Runtime.GetBytesForString("keyN"), Sharpen.Runtime.GetBytesForString
				("valueN"));
			writer.Append(Sharpen.Runtime.GetBytesForString("keyA"), Sharpen.Runtime.GetBytesForString
				("valueA"));
			CloseOutput();
		}

		/// <exception cref="System.IO.IOException"/>
		protected override void TearDown()
		{
			fs.Delete(path, true);
		}

		// we still can scan records in an unsorted TFile
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestFailureScannerWithKeys()
		{
			TFile.Reader reader = new TFile.Reader(fs.Open(path), fs.GetFileStatus(path).GetLen
				(), conf);
			NUnit.Framework.Assert.IsFalse(reader.IsSorted());
			NUnit.Framework.Assert.AreEqual((int)reader.GetEntryCount(), 4);
			try
			{
				TFile.Reader.Scanner scanner = reader.CreateScannerByKey(Sharpen.Runtime.GetBytesForString
					("aaa"), Sharpen.Runtime.GetBytesForString("zzz"));
				NUnit.Framework.Assert.Fail("Failed to catch creating scanner with keys on unsorted file."
					);
			}
			catch (RuntimeException)
			{
			}
			finally
			{
				reader.Close();
			}
		}

		// we still can scan records in an unsorted TFile
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestScan()
		{
			TFile.Reader reader = new TFile.Reader(fs.Open(path), fs.GetFileStatus(path).GetLen
				(), conf);
			NUnit.Framework.Assert.IsFalse(reader.IsSorted());
			NUnit.Framework.Assert.AreEqual((int)reader.GetEntryCount(), 4);
			TFile.Reader.Scanner scanner = reader.CreateScanner();
			try
			{
				// read key and value
				byte[] kbuf = new byte[BufSize];
				int klen = scanner.Entry().GetKeyLength();
				scanner.Entry().GetKey(kbuf);
				NUnit.Framework.Assert.AreEqual(Sharpen.Runtime.GetStringForBytes(kbuf, 0, klen), 
					"keyZ");
				byte[] vbuf = new byte[BufSize];
				int vlen = scanner.Entry().GetValueLength();
				scanner.Entry().GetValue(vbuf);
				NUnit.Framework.Assert.AreEqual(Sharpen.Runtime.GetStringForBytes(vbuf, 0, vlen), 
					"valueZ");
				scanner.Advance();
				// now try get value first
				vbuf = new byte[BufSize];
				vlen = scanner.Entry().GetValueLength();
				scanner.Entry().GetValue(vbuf);
				NUnit.Framework.Assert.AreEqual(Sharpen.Runtime.GetStringForBytes(vbuf, 0, vlen), 
					"valueM");
				kbuf = new byte[BufSize];
				klen = scanner.Entry().GetKeyLength();
				scanner.Entry().GetKey(kbuf);
				NUnit.Framework.Assert.AreEqual(Sharpen.Runtime.GetStringForBytes(kbuf, 0, klen), 
					"keyM");
			}
			finally
			{
				scanner.Close();
				reader.Close();
			}
		}

		// we still can scan records in an unsorted TFile
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestScanRange()
		{
			TFile.Reader reader = new TFile.Reader(fs.Open(path), fs.GetFileStatus(path).GetLen
				(), conf);
			NUnit.Framework.Assert.IsFalse(reader.IsSorted());
			NUnit.Framework.Assert.AreEqual((int)reader.GetEntryCount(), 4);
			TFile.Reader.Scanner scanner = reader.CreateScanner();
			try
			{
				// read key and value
				byte[] kbuf = new byte[BufSize];
				int klen = scanner.Entry().GetKeyLength();
				scanner.Entry().GetKey(kbuf);
				NUnit.Framework.Assert.AreEqual(Sharpen.Runtime.GetStringForBytes(kbuf, 0, klen), 
					"keyZ");
				byte[] vbuf = new byte[BufSize];
				int vlen = scanner.Entry().GetValueLength();
				scanner.Entry().GetValue(vbuf);
				NUnit.Framework.Assert.AreEqual(Sharpen.Runtime.GetStringForBytes(vbuf, 0, vlen), 
					"valueZ");
				scanner.Advance();
				// now try get value first
				vbuf = new byte[BufSize];
				vlen = scanner.Entry().GetValueLength();
				scanner.Entry().GetValue(vbuf);
				NUnit.Framework.Assert.AreEqual(Sharpen.Runtime.GetStringForBytes(vbuf, 0, vlen), 
					"valueM");
				kbuf = new byte[BufSize];
				klen = scanner.Entry().GetKeyLength();
				scanner.Entry().GetKey(kbuf);
				NUnit.Framework.Assert.AreEqual(Sharpen.Runtime.GetStringForBytes(kbuf, 0, klen), 
					"keyM");
			}
			finally
			{
				scanner.Close();
				reader.Close();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestFailureSeek()
		{
			TFile.Reader reader = new TFile.Reader(fs.Open(path), fs.GetFileStatus(path).GetLen
				(), conf);
			TFile.Reader.Scanner scanner = reader.CreateScanner();
			try
			{
				// can't find ceil
				try
				{
					scanner.LowerBound(Sharpen.Runtime.GetBytesForString("keyN"));
					NUnit.Framework.Assert.Fail("Cannot search in a unsorted TFile!");
				}
				catch (Exception)
				{
				}
				finally
				{
				}
				// noop, expecting excetions
				// can't find higher
				try
				{
					scanner.UpperBound(Sharpen.Runtime.GetBytesForString("keyA"));
					NUnit.Framework.Assert.Fail("Cannot search higher in a unsorted TFile!");
				}
				catch (Exception)
				{
				}
				finally
				{
				}
				// noop, expecting excetions
				// can't seek
				try
				{
					scanner.SeekTo(Sharpen.Runtime.GetBytesForString("keyM"));
					NUnit.Framework.Assert.Fail("Cannot search a unsorted TFile!");
				}
				catch (Exception)
				{
				}
				finally
				{
				}
			}
			finally
			{
				// noop, expecting excetions
				scanner.Close();
				reader.Close();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void CloseOutput()
		{
			if (writer != null)
			{
				writer.Close();
				writer = null;
				@out.Close();
				@out = null;
			}
		}
	}
}
