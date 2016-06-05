using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;


namespace Org.Apache.Hadoop.IO.File.Tfile
{
	public class TestTFileSplit : TestCase
	{
		private static string Root = Runtime.GetProperty("test.build.data", "/tmp/tfile-test"
			);

		private const int BlockSize = 64 * 1024;

		private const string Key = "key";

		private const string Value = "value";

		private FileSystem fs;

		private Configuration conf;

		private Path path;

		private Random random = new Random();

		private string comparator = "memcmp";

		private string outputFile = "TestTFileSplit";

		/// <exception cref="System.IO.IOException"/>
		internal virtual void CreateFile(int count, string compress)
		{
			conf = new Configuration();
			path = new Path(Root, outputFile + "." + compress);
			fs = path.GetFileSystem(conf);
			FSDataOutputStream @out = fs.Create(path);
			TFile.Writer writer = new TFile.Writer(@out, BlockSize, compress, comparator, conf
				);
			int nx;
			for (nx = 0; nx < count; nx++)
			{
				byte[] key = Runtime.GetBytesForString(ComposeSortedKey(Key, count, nx));
				byte[] value = Runtime.GetBytesForString((Value + nx));
				writer.Append(key, value);
			}
			writer.Close();
			@out.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void ReadFile()
		{
			long fileLength = fs.GetFileStatus(path).GetLen();
			int numSplit = 10;
			long splitSize = fileLength / numSplit + 1;
			TFile.Reader reader = new TFile.Reader(fs.Open(path), fs.GetFileStatus(path).GetLen
				(), conf);
			long offset = 0;
			long rowCount = 0;
			BytesWritable key;
			BytesWritable value;
			for (int i = 0; i < numSplit; ++i, offset += splitSize)
			{
				TFile.Reader.Scanner scanner = reader.CreateScannerByByteRange(offset, splitSize);
				int count = 0;
				key = new BytesWritable();
				value = new BytesWritable();
				while (!scanner.AtEnd())
				{
					scanner.Entry().Get(key, value);
					++count;
					scanner.Advance();
				}
				scanner.Close();
				Assert.True(count > 0);
				rowCount += count;
			}
			Assert.Equal(rowCount, reader.GetEntryCount());
			reader.Close();
		}

		/* Similar to readFile(), tests the scanner created
		* by record numbers rather than the offsets.
		*/
		/// <exception cref="System.IO.IOException"/>
		internal virtual void ReadRowSplits(int numSplits)
		{
			TFile.Reader reader = new TFile.Reader(fs.Open(path), fs.GetFileStatus(path).GetLen
				(), conf);
			long totalRecords = reader.GetEntryCount();
			for (int i = 0; i < numSplits; i++)
			{
				long startRec = i * totalRecords / numSplits;
				long endRec = (i + 1) * totalRecords / numSplits;
				if (i == numSplits - 1)
				{
					endRec = totalRecords;
				}
				TFile.Reader.Scanner scanner = reader.CreateScannerByRecordNum(startRec, endRec);
				int count = 0;
				BytesWritable key = new BytesWritable();
				BytesWritable value = new BytesWritable();
				long x = startRec;
				while (!scanner.AtEnd())
				{
					Assert.Equal("Incorrect RecNum returned by scanner", scanner.GetRecordNum
						(), x);
					scanner.Entry().Get(key, value);
					++count;
					Assert.Equal("Incorrect RecNum returned by scanner", scanner.GetRecordNum
						(), x);
					scanner.Advance();
					++x;
				}
				scanner.Close();
				Assert.True(count == (endRec - startRec));
			}
			// make sure specifying range at the end gives zero records.
			TFile.Reader.Scanner scanner_1 = reader.CreateScannerByRecordNum(totalRecords, -1
				);
			Assert.True(scanner_1.AtEnd());
		}

		internal static string ComposeSortedKey(string prefix, int total, int value)
		{
			return string.Format("%s%010d", prefix, value);
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void CheckRecNums()
		{
			long fileLen = fs.GetFileStatus(path).GetLen();
			TFile.Reader reader = new TFile.Reader(fs.Open(path), fileLen, conf);
			long totalRecs = reader.GetEntryCount();
			long begin = random.NextLong() % (totalRecs / 2);
			if (begin < 0)
			{
				begin += (totalRecs / 2);
			}
			long end = random.NextLong() % (totalRecs / 2);
			if (end < 0)
			{
				end += (totalRecs / 2);
			}
			end += (totalRecs / 2) + 1;
			Assert.Equal("RecNum for offset=0 should be 0", 0, reader.GetRecordNumNear
				(0));
			foreach (long x in new long[] { fileLen, fileLen + 1, 2 * fileLen })
			{
				Assert.Equal("RecNum for offset>=fileLen should be total entries"
					, totalRecs, reader.GetRecordNumNear(x));
			}
			for (long i = 0; i < 100; ++i)
			{
				Assert.Equal("Locaton to RecNum conversion not symmetric", i, 
					reader.GetRecordNumByLocation(reader.GetLocationByRecordNum(i)));
			}
			for (long i_1 = 1; i_1 < 100; ++i_1)
			{
				long x_1 = totalRecs - i_1;
				Assert.Equal("Locaton to RecNum conversion not symmetric", x_1
					, reader.GetRecordNumByLocation(reader.GetLocationByRecordNum(x_1)));
			}
			for (long i_2 = begin; i_2 < end; ++i_2)
			{
				Assert.Equal("Locaton to RecNum conversion not symmetric", i_2
					, reader.GetRecordNumByLocation(reader.GetLocationByRecordNum(i_2)));
			}
			for (int i_3 = 0; i_3 < 1000; ++i_3)
			{
				long x_1 = random.NextLong() % totalRecs;
				if (x_1 < 0)
				{
					x_1 += totalRecs;
				}
				Assert.Equal("Locaton to RecNum conversion not symmetric", x_1
					, reader.GetRecordNumByLocation(reader.GetLocationByRecordNum(x_1)));
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestSplit()
		{
			System.Console.Out.WriteLine("testSplit");
			CreateFile(100000, Compression.Algorithm.None.GetName());
			CheckRecNums();
			ReadFile();
			ReadRowSplits(10);
			fs.Delete(path, true);
			CreateFile(500000, Compression.Algorithm.Gz.GetName());
			CheckRecNums();
			ReadFile();
			ReadRowSplits(83);
			fs.Delete(path, true);
		}
	}
}
