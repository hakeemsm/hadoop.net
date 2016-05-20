using Sharpen;

namespace org.apache.hadoop.io.file.tfile
{
	public class TestTFileSplit : NUnit.Framework.TestCase
	{
		private static string ROOT = Sharpen.Runtime.getProperty("test.build.data", "/tmp/tfile-test"
			);

		private const int BLOCK_SIZE = 64 * 1024;

		private const string KEY = "key";

		private const string VALUE = "value";

		private org.apache.hadoop.fs.FileSystem fs;

		private org.apache.hadoop.conf.Configuration conf;

		private org.apache.hadoop.fs.Path path;

		private java.util.Random random = new java.util.Random();

		private string comparator = "memcmp";

		private string outputFile = "TestTFileSplit";

		/// <exception cref="System.IO.IOException"/>
		internal virtual void createFile(int count, string compress)
		{
			conf = new org.apache.hadoop.conf.Configuration();
			path = new org.apache.hadoop.fs.Path(ROOT, outputFile + "." + compress);
			fs = path.getFileSystem(conf);
			org.apache.hadoop.fs.FSDataOutputStream @out = fs.create(path);
			org.apache.hadoop.io.file.tfile.TFile.Writer writer = new org.apache.hadoop.io.file.tfile.TFile.Writer
				(@out, BLOCK_SIZE, compress, comparator, conf);
			int nx;
			for (nx = 0; nx < count; nx++)
			{
				byte[] key = Sharpen.Runtime.getBytesForString(composeSortedKey(KEY, count, nx));
				byte[] value = Sharpen.Runtime.getBytesForString((VALUE + nx));
				writer.append(key, value);
			}
			writer.close();
			@out.close();
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void readFile()
		{
			long fileLength = fs.getFileStatus(path).getLen();
			int numSplit = 10;
			long splitSize = fileLength / numSplit + 1;
			org.apache.hadoop.io.file.tfile.TFile.Reader reader = new org.apache.hadoop.io.file.tfile.TFile.Reader
				(fs.open(path), fs.getFileStatus(path).getLen(), conf);
			long offset = 0;
			long rowCount = 0;
			org.apache.hadoop.io.BytesWritable key;
			org.apache.hadoop.io.BytesWritable value;
			for (int i = 0; i < numSplit; ++i, offset += splitSize)
			{
				org.apache.hadoop.io.file.tfile.TFile.Reader.Scanner scanner = reader.createScannerByByteRange
					(offset, splitSize);
				int count = 0;
				key = new org.apache.hadoop.io.BytesWritable();
				value = new org.apache.hadoop.io.BytesWritable();
				while (!scanner.atEnd())
				{
					scanner.entry().get(key, value);
					++count;
					scanner.advance();
				}
				scanner.close();
				NUnit.Framework.Assert.IsTrue(count > 0);
				rowCount += count;
			}
			NUnit.Framework.Assert.AreEqual(rowCount, reader.getEntryCount());
			reader.close();
		}

		/* Similar to readFile(), tests the scanner created
		* by record numbers rather than the offsets.
		*/
		/// <exception cref="System.IO.IOException"/>
		internal virtual void readRowSplits(int numSplits)
		{
			org.apache.hadoop.io.file.tfile.TFile.Reader reader = new org.apache.hadoop.io.file.tfile.TFile.Reader
				(fs.open(path), fs.getFileStatus(path).getLen(), conf);
			long totalRecords = reader.getEntryCount();
			for (int i = 0; i < numSplits; i++)
			{
				long startRec = i * totalRecords / numSplits;
				long endRec = (i + 1) * totalRecords / numSplits;
				if (i == numSplits - 1)
				{
					endRec = totalRecords;
				}
				org.apache.hadoop.io.file.tfile.TFile.Reader.Scanner scanner = reader.createScannerByRecordNum
					(startRec, endRec);
				int count = 0;
				org.apache.hadoop.io.BytesWritable key = new org.apache.hadoop.io.BytesWritable();
				org.apache.hadoop.io.BytesWritable value = new org.apache.hadoop.io.BytesWritable
					();
				long x = startRec;
				while (!scanner.atEnd())
				{
					NUnit.Framework.Assert.AreEqual("Incorrect RecNum returned by scanner", scanner.getRecordNum
						(), x);
					scanner.entry().get(key, value);
					++count;
					NUnit.Framework.Assert.AreEqual("Incorrect RecNum returned by scanner", scanner.getRecordNum
						(), x);
					scanner.advance();
					++x;
				}
				scanner.close();
				NUnit.Framework.Assert.IsTrue(count == (endRec - startRec));
			}
			// make sure specifying range at the end gives zero records.
			org.apache.hadoop.io.file.tfile.TFile.Reader.Scanner scanner_1 = reader.createScannerByRecordNum
				(totalRecords, -1);
			NUnit.Framework.Assert.IsTrue(scanner_1.atEnd());
		}

		internal static string composeSortedKey(string prefix, int total, int value)
		{
			return string.format("%s%010d", prefix, value);
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void checkRecNums()
		{
			long fileLen = fs.getFileStatus(path).getLen();
			org.apache.hadoop.io.file.tfile.TFile.Reader reader = new org.apache.hadoop.io.file.tfile.TFile.Reader
				(fs.open(path), fileLen, conf);
			long totalRecs = reader.getEntryCount();
			long begin = random.nextLong() % (totalRecs / 2);
			if (begin < 0)
			{
				begin += (totalRecs / 2);
			}
			long end = random.nextLong() % (totalRecs / 2);
			if (end < 0)
			{
				end += (totalRecs / 2);
			}
			end += (totalRecs / 2) + 1;
			NUnit.Framework.Assert.AreEqual("RecNum for offset=0 should be 0", 0, reader.getRecordNumNear
				(0));
			foreach (long x in new long[] { fileLen, fileLen + 1, 2 * fileLen })
			{
				NUnit.Framework.Assert.AreEqual("RecNum for offset>=fileLen should be total entries"
					, totalRecs, reader.getRecordNumNear(x));
			}
			for (long i = 0; i < 100; ++i)
			{
				NUnit.Framework.Assert.AreEqual("Locaton to RecNum conversion not symmetric", i, 
					reader.getRecordNumByLocation(reader.getLocationByRecordNum(i)));
			}
			for (long i_1 = 1; i_1 < 100; ++i_1)
			{
				long x_1 = totalRecs - i_1;
				NUnit.Framework.Assert.AreEqual("Locaton to RecNum conversion not symmetric", x_1
					, reader.getRecordNumByLocation(reader.getLocationByRecordNum(x_1)));
			}
			for (long i_2 = begin; i_2 < end; ++i_2)
			{
				NUnit.Framework.Assert.AreEqual("Locaton to RecNum conversion not symmetric", i_2
					, reader.getRecordNumByLocation(reader.getLocationByRecordNum(i_2)));
			}
			for (int i_3 = 0; i_3 < 1000; ++i_3)
			{
				long x_1 = random.nextLong() % totalRecs;
				if (x_1 < 0)
				{
					x_1 += totalRecs;
				}
				NUnit.Framework.Assert.AreEqual("Locaton to RecNum conversion not symmetric", x_1
					, reader.getRecordNumByLocation(reader.getLocationByRecordNum(x_1)));
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testSplit()
		{
			System.Console.Out.WriteLine("testSplit");
			createFile(100000, org.apache.hadoop.io.file.tfile.Compression.Algorithm.NONE.getName
				());
			checkRecNums();
			readFile();
			readRowSplits(10);
			fs.delete(path, true);
			createFile(500000, org.apache.hadoop.io.file.tfile.Compression.Algorithm.GZ.getName
				());
			checkRecNums();
			readFile();
			readRowSplits(83);
			fs.delete(path, true);
		}
	}
}
