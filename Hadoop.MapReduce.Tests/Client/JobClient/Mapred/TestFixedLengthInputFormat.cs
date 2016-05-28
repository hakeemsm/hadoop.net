using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.IO.Compress;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public class TestFixedLengthInputFormat
	{
		private static Log Log;

		private static Configuration defaultConf;

		private static FileSystem localFs;

		private static Path workDir;

		private static Reporter voidReporter;

		private static char[] chars;

		private static Random charRand;

		// some chars for the record data
		[BeforeClass]
		public static void OnlyOnce()
		{
			try
			{
				Log = LogFactory.GetLog(typeof(TestFixedLengthInputFormat).FullName);
				defaultConf = new Configuration();
				defaultConf.Set("fs.defaultFS", "file:///");
				localFs = FileSystem.GetLocal(defaultConf);
				voidReporter = Reporter.Null;
				// our set of chars
				chars = ("abcdefghijklmnopqrstuvABCDEFGHIJKLMN OPQRSTUVWXYZ1234567890)" + "(*&^%$#@!-=><?:\"{}][';/.,']"
					).ToCharArray();
				workDir = new Path(new Path(Runtime.GetProperty("test.build.data", "."), "data"), 
					"TestKeyValueFixedLengthInputFormat");
				charRand = new Random();
			}
			catch (IOException e)
			{
				throw new RuntimeException("init failure", e);
			}
		}

		/// <summary>20 random tests of various record, file, and split sizes.</summary>
		/// <remarks>
		/// 20 random tests of various record, file, and split sizes.  All tests have
		/// uncompressed file as input.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestFormat()
		{
			RunRandomTests(null);
		}

		/// <summary>20 random tests of various record, file, and split sizes.</summary>
		/// <remarks>
		/// 20 random tests of various record, file, and split sizes.  All tests have
		/// compressed file as input.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestFormatCompressedIn()
		{
			RunRandomTests(new GzipCodec());
		}

		/// <summary>Test with no record length set.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestNoRecordLength()
		{
			localFs.Delete(workDir, true);
			Path file = new Path(workDir, new string("testFormat.txt"));
			CreateFile(file, null, 10, 10);
			// Set the fixed length record length config property 
			JobConf job = new JobConf(defaultConf);
			FileInputFormat.SetInputPaths(job, workDir);
			FixedLengthInputFormat format = new FixedLengthInputFormat();
			format.Configure(job);
			InputSplit[] splits = format.GetSplits(job, 1);
			bool exceptionThrown = false;
			foreach (InputSplit split in splits)
			{
				try
				{
					RecordReader<LongWritable, BytesWritable> reader = format.GetRecordReader(split, 
						job, voidReporter);
				}
				catch (IOException ioe)
				{
					exceptionThrown = true;
					Log.Info("Exception message:" + ioe.Message);
				}
			}
			NUnit.Framework.Assert.IsTrue("Exception for not setting record length:", exceptionThrown
				);
		}

		/// <summary>Test with record length set to 0</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestZeroRecordLength()
		{
			localFs.Delete(workDir, true);
			Path file = new Path(workDir, new string("testFormat.txt"));
			CreateFile(file, null, 10, 10);
			// Set the fixed length record length config property 
			JobConf job = new JobConf(defaultConf);
			FileInputFormat.SetInputPaths(job, workDir);
			FixedLengthInputFormat format = new FixedLengthInputFormat();
			FixedLengthInputFormat.SetRecordLength(job, 0);
			format.Configure(job);
			InputSplit[] splits = format.GetSplits(job, 1);
			bool exceptionThrown = false;
			foreach (InputSplit split in splits)
			{
				try
				{
					RecordReader<LongWritable, BytesWritable> reader = format.GetRecordReader(split, 
						job, voidReporter);
				}
				catch (IOException ioe)
				{
					exceptionThrown = true;
					Log.Info("Exception message:" + ioe.Message);
				}
			}
			NUnit.Framework.Assert.IsTrue("Exception for zero record length:", exceptionThrown
				);
		}

		/// <summary>Test with record length set to a negative value</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestNegativeRecordLength()
		{
			localFs.Delete(workDir, true);
			Path file = new Path(workDir, new string("testFormat.txt"));
			CreateFile(file, null, 10, 10);
			// Set the fixed length record length config property 
			JobConf job = new JobConf(defaultConf);
			FileInputFormat.SetInputPaths(job, workDir);
			FixedLengthInputFormat format = new FixedLengthInputFormat();
			FixedLengthInputFormat.SetRecordLength(job, -10);
			format.Configure(job);
			InputSplit[] splits = format.GetSplits(job, 1);
			bool exceptionThrown = false;
			foreach (InputSplit split in splits)
			{
				try
				{
					RecordReader<LongWritable, BytesWritable> reader = format.GetRecordReader(split, 
						job, voidReporter);
				}
				catch (IOException ioe)
				{
					exceptionThrown = true;
					Log.Info("Exception message:" + ioe.Message);
				}
			}
			NUnit.Framework.Assert.IsTrue("Exception for negative record length:", exceptionThrown
				);
		}

		/// <summary>Test with partial record at the end of a compressed input file.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestPartialRecordCompressedIn()
		{
			CompressionCodec gzip = new GzipCodec();
			RunPartialRecordTest(gzip);
		}

		/// <summary>Test with partial record at the end of an uncompressed input file.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestPartialRecordUncompressedIn()
		{
			RunPartialRecordTest(null);
		}

		/// <summary>Test using the gzip codec with two input files.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestGzipWithTwoInputs()
		{
			CompressionCodec gzip = new GzipCodec();
			localFs.Delete(workDir, true);
			FixedLengthInputFormat format = new FixedLengthInputFormat();
			JobConf job = new JobConf(defaultConf);
			FixedLengthInputFormat.SetRecordLength(job, 5);
			FileInputFormat.SetInputPaths(job, workDir);
			ReflectionUtils.SetConf(gzip, job);
			format.Configure(job);
			// Create files with fixed length records with 5 byte long records.
			WriteFile(localFs, new Path(workDir, "part1.txt.gz"), gzip, "one  two  threefour five six  seveneightnine ten  "
				);
			WriteFile(localFs, new Path(workDir, "part2.txt.gz"), gzip, "ten  nine eightsevensix  five four threetwo  one  "
				);
			InputSplit[] splits = format.GetSplits(job, 100);
			NUnit.Framework.Assert.AreEqual("compressed splits == 2", 2, splits.Length);
			FileSplit tmp = (FileSplit)splits[0];
			if (tmp.GetPath().GetName().Equals("part2.txt.gz"))
			{
				splits[0] = splits[1];
				splits[1] = tmp;
			}
			IList<string> results = ReadSplit(format, splits[0], job);
			NUnit.Framework.Assert.AreEqual("splits[0] length", 10, results.Count);
			NUnit.Framework.Assert.AreEqual("splits[0][5]", "six  ", results[5]);
			results = ReadSplit(format, splits[1], job);
			NUnit.Framework.Assert.AreEqual("splits[1] length", 10, results.Count);
			NUnit.Framework.Assert.AreEqual("splits[1][0]", "ten  ", results[0]);
			NUnit.Framework.Assert.AreEqual("splits[1][1]", "nine ", results[1]);
		}

		// Create a file containing fixed length records with random data
		/// <exception cref="System.IO.IOException"/>
		private AList<string> CreateFile(Path targetFile, CompressionCodec codec, int recordLen
			, int numRecords)
		{
			AList<string> recordList = new AList<string>(numRecords);
			OutputStream ostream = localFs.Create(targetFile);
			if (codec != null)
			{
				ostream = codec.CreateOutputStream(ostream);
			}
			TextWriter writer = new OutputStreamWriter(ostream);
			try
			{
				StringBuilder sb = new StringBuilder();
				for (int i = 0; i < numRecords; i++)
				{
					for (int j = 0; j < recordLen; j++)
					{
						sb.Append(chars[charRand.Next(chars.Length)]);
					}
					string recordData = sb.ToString();
					recordList.AddItem(recordData);
					writer.Write(recordData);
					sb.Length = 0;
				}
			}
			finally
			{
				writer.Close();
			}
			return recordList;
		}

		/// <exception cref="System.IO.IOException"/>
		private void RunRandomTests(CompressionCodec codec)
		{
			StringBuilder fileName = new StringBuilder("testFormat.txt");
			if (codec != null)
			{
				fileName.Append(".gz");
			}
			localFs.Delete(workDir, true);
			Path file = new Path(workDir, fileName.ToString());
			int seed = new Random().Next();
			Log.Info("Seed = " + seed);
			Random random = new Random(seed);
			int MaxTests = 20;
			LongWritable key = new LongWritable();
			BytesWritable value = new BytesWritable();
			for (int i = 0; i < MaxTests; i++)
			{
				Log.Info("----------------------------------------------------------");
				// Maximum total records of 999
				int totalRecords = random.Next(999) + 1;
				// Test an empty file
				if (i == 8)
				{
					totalRecords = 0;
				}
				// Maximum bytes in a record of 100K
				int recordLength = random.Next(1024 * 100) + 1;
				// For the 11th test, force a record length of 1
				if (i == 10)
				{
					recordLength = 1;
				}
				// The total bytes in the test file
				int fileSize = (totalRecords * recordLength);
				Log.Info("totalRecords=" + totalRecords + " recordLength=" + recordLength);
				// Create the job 
				JobConf job = new JobConf(defaultConf);
				if (codec != null)
				{
					ReflectionUtils.SetConf(codec, job);
				}
				// Create the test file
				AList<string> recordList = CreateFile(file, codec, recordLength, totalRecords);
				NUnit.Framework.Assert.IsTrue(localFs.Exists(file));
				//set the fixed length record length config property for the job
				FixedLengthInputFormat.SetRecordLength(job, recordLength);
				int numSplits = 1;
				// Arbitrarily set number of splits.
				if (i > 0)
				{
					if (i == (MaxTests - 1))
					{
						// Test a split size that is less than record len
						numSplits = (int)(fileSize / Math.Floor(recordLength / 2));
					}
					else
					{
						if (MaxTests % i == 0)
						{
							// Let us create a split size that is forced to be 
							// smaller than the end file itself, (ensures 1+ splits)
							numSplits = fileSize / (fileSize - random.Next(fileSize));
						}
						else
						{
							// Just pick a random split size with no upper bound 
							numSplits = Math.Max(1, fileSize / random.Next(int.MaxValue));
						}
					}
					Log.Info("Number of splits set to: " + numSplits);
				}
				// Setup the input path
				FileInputFormat.SetInputPaths(job, workDir);
				// Try splitting the file in a variety of sizes
				FixedLengthInputFormat format = new FixedLengthInputFormat();
				format.Configure(job);
				InputSplit[] splits = format.GetSplits(job, numSplits);
				Log.Info("Actual number of splits = " + splits.Length);
				// Test combined split lengths = total file size
				long recordOffset = 0;
				int recordNumber = 0;
				foreach (InputSplit split in splits)
				{
					RecordReader<LongWritable, BytesWritable> reader = format.GetRecordReader(split, 
						job, voidReporter);
					Type clazz = reader.GetType();
					NUnit.Framework.Assert.AreEqual("RecordReader class should be FixedLengthRecordReader:"
						, typeof(FixedLengthRecordReader), clazz);
					// Plow through the records in this split
					while (reader.Next(key, value))
					{
						NUnit.Framework.Assert.AreEqual("Checking key", (long)(recordNumber * recordLength
							), key.Get());
						string valueString = Sharpen.Runtime.GetStringForBytes(value.GetBytes(), 0, value
							.GetLength());
						NUnit.Framework.Assert.AreEqual("Checking record length:", recordLength, value.GetLength
							());
						NUnit.Framework.Assert.IsTrue("Checking for more records than expected:", recordNumber
							 < totalRecords);
						string origRecord = recordList[recordNumber];
						NUnit.Framework.Assert.AreEqual("Checking record content:", origRecord, valueString
							);
						recordNumber++;
					}
					reader.Close();
				}
				NUnit.Framework.Assert.AreEqual("Total original records should be total read records:"
					, recordList.Count, recordNumber);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static void WriteFile(FileSystem fs, Path name, CompressionCodec codec, string
			 contents)
		{
			OutputStream stm;
			if (codec == null)
			{
				stm = fs.Create(name);
			}
			else
			{
				stm = codec.CreateOutputStream(fs.Create(name));
			}
			stm.Write(Sharpen.Runtime.GetBytesForString(contents));
			stm.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		private static IList<string> ReadSplit(FixedLengthInputFormat format, InputSplit 
			split, JobConf job)
		{
			IList<string> result = new AList<string>();
			RecordReader<LongWritable, BytesWritable> reader = format.GetRecordReader(split, 
				job, voidReporter);
			LongWritable key = reader.CreateKey();
			BytesWritable value = reader.CreateValue();
			try
			{
				while (reader.Next(key, value))
				{
					result.AddItem(Sharpen.Runtime.GetStringForBytes(value.GetBytes(), 0, value.GetLength
						()));
				}
			}
			finally
			{
				reader.Close();
			}
			return result;
		}

		/// <exception cref="System.IO.IOException"/>
		private void RunPartialRecordTest(CompressionCodec codec)
		{
			localFs.Delete(workDir, true);
			// Create a file with fixed length records with 5 byte long
			// records with a partial record at the end.
			StringBuilder fileName = new StringBuilder("testFormat.txt");
			if (codec != null)
			{
				fileName.Append(".gz");
			}
			FixedLengthInputFormat format = new FixedLengthInputFormat();
			JobConf job = new JobConf(defaultConf);
			FixedLengthInputFormat.SetRecordLength(job, 5);
			FileInputFormat.SetInputPaths(job, workDir);
			if (codec != null)
			{
				ReflectionUtils.SetConf(codec, job);
			}
			format.Configure(job);
			WriteFile(localFs, new Path(workDir, fileName.ToString()), codec, "one  two  threefour five six  seveneightnine ten"
				);
			InputSplit[] splits = format.GetSplits(job, 100);
			if (codec != null)
			{
				NUnit.Framework.Assert.AreEqual("compressed splits == 1", 1, splits.Length);
			}
			bool exceptionThrown = false;
			foreach (InputSplit split in splits)
			{
				try
				{
					IList<string> results = ReadSplit(format, split, job);
				}
				catch (IOException ioe)
				{
					exceptionThrown = true;
					Log.Info("Exception message:" + ioe.Message);
				}
			}
			NUnit.Framework.Assert.IsTrue("Exception for partial record:", exceptionThrown);
		}
	}
}
