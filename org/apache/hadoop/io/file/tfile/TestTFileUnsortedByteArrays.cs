using Sharpen;

namespace org.apache.hadoop.io.file.tfile
{
	public class TestTFileUnsortedByteArrays : NUnit.Framework.TestCase
	{
		private static string ROOT = Sharpen.Runtime.getProperty("test.build.data", "/tmp/tfile-test"
			);

		private const int BLOCK_SIZE = 512;

		private const int BUF_SIZE = 64;

		private org.apache.hadoop.fs.FileSystem fs;

		private org.apache.hadoop.conf.Configuration conf;

		private org.apache.hadoop.fs.Path path;

		private org.apache.hadoop.fs.FSDataOutputStream @out;

		private org.apache.hadoop.io.file.tfile.TFile.Writer writer;

		private string compression = org.apache.hadoop.io.file.tfile.Compression.Algorithm
			.GZ.getName();

		private string outputFile = "TFileTestUnsorted";

		private int records1stBlock = 4314;

		private int records2ndBlock = 4108;

		/*
		* pre-sampled numbers of records in one block, based on the given the
		* generated key and value strings
		*/
		public virtual void init(string compression, string outputFile, int numRecords1stBlock
			, int numRecords2ndBlock)
		{
			this.compression = compression;
			this.outputFile = outputFile;
			this.records1stBlock = numRecords1stBlock;
			this.records2ndBlock = numRecords2ndBlock;
		}

		/// <exception cref="System.IO.IOException"/>
		protected override void setUp()
		{
			conf = new org.apache.hadoop.conf.Configuration();
			path = new org.apache.hadoop.fs.Path(ROOT, outputFile);
			fs = path.getFileSystem(conf);
			@out = fs.create(path);
			writer = new org.apache.hadoop.io.file.tfile.TFile.Writer(@out, BLOCK_SIZE, compression
				, null, conf);
			writer.append(Sharpen.Runtime.getBytesForString("keyZ"), Sharpen.Runtime.getBytesForString
				("valueZ"));
			writer.append(Sharpen.Runtime.getBytesForString("keyM"), Sharpen.Runtime.getBytesForString
				("valueM"));
			writer.append(Sharpen.Runtime.getBytesForString("keyN"), Sharpen.Runtime.getBytesForString
				("valueN"));
			writer.append(Sharpen.Runtime.getBytesForString("keyA"), Sharpen.Runtime.getBytesForString
				("valueA"));
			closeOutput();
		}

		/// <exception cref="System.IO.IOException"/>
		protected override void tearDown()
		{
			fs.delete(path, true);
		}

		// we still can scan records in an unsorted TFile
		/// <exception cref="System.IO.IOException"/>
		public virtual void testFailureScannerWithKeys()
		{
			org.apache.hadoop.io.file.tfile.TFile.Reader reader = new org.apache.hadoop.io.file.tfile.TFile.Reader
				(fs.open(path), fs.getFileStatus(path).getLen(), conf);
			NUnit.Framework.Assert.IsFalse(reader.isSorted());
			NUnit.Framework.Assert.AreEqual((int)reader.getEntryCount(), 4);
			try
			{
				org.apache.hadoop.io.file.tfile.TFile.Reader.Scanner scanner = reader.createScannerByKey
					(Sharpen.Runtime.getBytesForString("aaa"), Sharpen.Runtime.getBytesForString("zzz"
					));
				NUnit.Framework.Assert.Fail("Failed to catch creating scanner with keys on unsorted file."
					);
			}
			catch (System.Exception)
			{
			}
			finally
			{
				reader.close();
			}
		}

		// we still can scan records in an unsorted TFile
		/// <exception cref="System.IO.IOException"/>
		public virtual void testScan()
		{
			org.apache.hadoop.io.file.tfile.TFile.Reader reader = new org.apache.hadoop.io.file.tfile.TFile.Reader
				(fs.open(path), fs.getFileStatus(path).getLen(), conf);
			NUnit.Framework.Assert.IsFalse(reader.isSorted());
			NUnit.Framework.Assert.AreEqual((int)reader.getEntryCount(), 4);
			org.apache.hadoop.io.file.tfile.TFile.Reader.Scanner scanner = reader.createScanner
				();
			try
			{
				// read key and value
				byte[] kbuf = new byte[BUF_SIZE];
				int klen = scanner.entry().getKeyLength();
				scanner.entry().getKey(kbuf);
				NUnit.Framework.Assert.AreEqual(Sharpen.Runtime.getStringForBytes(kbuf, 0, klen), 
					"keyZ");
				byte[] vbuf = new byte[BUF_SIZE];
				int vlen = scanner.entry().getValueLength();
				scanner.entry().getValue(vbuf);
				NUnit.Framework.Assert.AreEqual(Sharpen.Runtime.getStringForBytes(vbuf, 0, vlen), 
					"valueZ");
				scanner.advance();
				// now try get value first
				vbuf = new byte[BUF_SIZE];
				vlen = scanner.entry().getValueLength();
				scanner.entry().getValue(vbuf);
				NUnit.Framework.Assert.AreEqual(Sharpen.Runtime.getStringForBytes(vbuf, 0, vlen), 
					"valueM");
				kbuf = new byte[BUF_SIZE];
				klen = scanner.entry().getKeyLength();
				scanner.entry().getKey(kbuf);
				NUnit.Framework.Assert.AreEqual(Sharpen.Runtime.getStringForBytes(kbuf, 0, klen), 
					"keyM");
			}
			finally
			{
				scanner.close();
				reader.close();
			}
		}

		// we still can scan records in an unsorted TFile
		/// <exception cref="System.IO.IOException"/>
		public virtual void testScanRange()
		{
			org.apache.hadoop.io.file.tfile.TFile.Reader reader = new org.apache.hadoop.io.file.tfile.TFile.Reader
				(fs.open(path), fs.getFileStatus(path).getLen(), conf);
			NUnit.Framework.Assert.IsFalse(reader.isSorted());
			NUnit.Framework.Assert.AreEqual((int)reader.getEntryCount(), 4);
			org.apache.hadoop.io.file.tfile.TFile.Reader.Scanner scanner = reader.createScanner
				();
			try
			{
				// read key and value
				byte[] kbuf = new byte[BUF_SIZE];
				int klen = scanner.entry().getKeyLength();
				scanner.entry().getKey(kbuf);
				NUnit.Framework.Assert.AreEqual(Sharpen.Runtime.getStringForBytes(kbuf, 0, klen), 
					"keyZ");
				byte[] vbuf = new byte[BUF_SIZE];
				int vlen = scanner.entry().getValueLength();
				scanner.entry().getValue(vbuf);
				NUnit.Framework.Assert.AreEqual(Sharpen.Runtime.getStringForBytes(vbuf, 0, vlen), 
					"valueZ");
				scanner.advance();
				// now try get value first
				vbuf = new byte[BUF_SIZE];
				vlen = scanner.entry().getValueLength();
				scanner.entry().getValue(vbuf);
				NUnit.Framework.Assert.AreEqual(Sharpen.Runtime.getStringForBytes(vbuf, 0, vlen), 
					"valueM");
				kbuf = new byte[BUF_SIZE];
				klen = scanner.entry().getKeyLength();
				scanner.entry().getKey(kbuf);
				NUnit.Framework.Assert.AreEqual(Sharpen.Runtime.getStringForBytes(kbuf, 0, klen), 
					"keyM");
			}
			finally
			{
				scanner.close();
				reader.close();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testFailureSeek()
		{
			org.apache.hadoop.io.file.tfile.TFile.Reader reader = new org.apache.hadoop.io.file.tfile.TFile.Reader
				(fs.open(path), fs.getFileStatus(path).getLen(), conf);
			org.apache.hadoop.io.file.tfile.TFile.Reader.Scanner scanner = reader.createScanner
				();
			try
			{
				// can't find ceil
				try
				{
					scanner.lowerBound(Sharpen.Runtime.getBytesForString("keyN"));
					NUnit.Framework.Assert.Fail("Cannot search in a unsorted TFile!");
				}
				catch (System.Exception)
				{
				}
				finally
				{
				}
				// noop, expecting excetions
				// can't find higher
				try
				{
					scanner.upperBound(Sharpen.Runtime.getBytesForString("keyA"));
					NUnit.Framework.Assert.Fail("Cannot search higher in a unsorted TFile!");
				}
				catch (System.Exception)
				{
				}
				finally
				{
				}
				// noop, expecting excetions
				// can't seek
				try
				{
					scanner.seekTo(Sharpen.Runtime.getBytesForString("keyM"));
					NUnit.Framework.Assert.Fail("Cannot search a unsorted TFile!");
				}
				catch (System.Exception)
				{
				}
				finally
				{
				}
			}
			finally
			{
				// noop, expecting excetions
				scanner.close();
				reader.close();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void closeOutput()
		{
			if (writer != null)
			{
				writer.close();
				writer = null;
				@out.close();
				@out = null;
			}
		}
	}
}
