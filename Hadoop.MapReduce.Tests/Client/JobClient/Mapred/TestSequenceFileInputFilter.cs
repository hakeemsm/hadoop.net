using System;
using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public class TestSequenceFileInputFilter : TestCase
	{
		private static readonly Log Log = FileInputFormat.Log;

		private const int MaxLength = 15000;

		private static readonly Configuration conf = new Configuration();

		private static readonly JobConf job = new JobConf(conf);

		private static readonly FileSystem fs;

		private static readonly Path inDir = new Path(Runtime.GetProperty("test.build.data"
			, ".") + "/mapred");

		private static readonly Path inFile = new Path(inDir, "test.seq");

		private static readonly Random random = new Random(1);

		private static readonly Reporter reporter = Reporter.Null;

		static TestSequenceFileInputFilter()
		{
			FileInputFormat.SetInputPaths(job, inDir);
			try
			{
				fs = FileSystem.GetLocal(conf);
			}
			catch (IOException e)
			{
				Sharpen.Runtime.PrintStackTrace(e);
				throw new RuntimeException(e);
			}
		}

		/// <exception cref="System.Exception"/>
		private static void CreateSequenceFile(int numRecords)
		{
			// create a file with length entries
			SequenceFile.Writer writer = SequenceFile.CreateWriter(fs, conf, inFile, typeof(Text
				), typeof(BytesWritable));
			try
			{
				for (int i = 1; i <= numRecords; i++)
				{
					Text key = new Text(Sharpen.Extensions.ToString(i));
					byte[] data = new byte[random.Next(10)];
					random.NextBytes(data);
					BytesWritable value = new BytesWritable(data);
					writer.Append(key, value);
				}
			}
			finally
			{
				writer.Close();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private int CountRecords(int numSplits)
		{
			InputFormat<Text, BytesWritable> format = new SequenceFileInputFilter<Text, BytesWritable
				>();
			Text key = new Text();
			BytesWritable value = new BytesWritable();
			if (numSplits == 0)
			{
				numSplits = random.Next(MaxLength / (SequenceFile.SyncInterval / 20)) + 1;
			}
			InputSplit[] splits = format.GetSplits(job, numSplits);
			// check each split
			int count = 0;
			Log.Info("Generated " + splits.Length + " splits.");
			for (int j = 0; j < splits.Length; j++)
			{
				RecordReader<Text, BytesWritable> reader = format.GetRecordReader(splits[j], job, 
					reporter);
				try
				{
					while (reader.Next(key, value))
					{
						Log.Info("Accept record " + key.ToString());
						count++;
					}
				}
				finally
				{
					reader.Close();
				}
			}
			return count;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRegexFilter()
		{
			// set the filter class
			Log.Info("Testing Regex Filter with patter: \\A10*");
			SequenceFileInputFilter.SetFilterClass(job, typeof(SequenceFileInputFilter.RegexFilter
				));
			SequenceFileInputFilter.RegexFilter.SetPattern(job, "\\A10*");
			// clean input dir
			fs.Delete(inDir, true);
			// for a variety of lengths
			for (int length = 1; length < MaxLength; length += random.Next(MaxLength / 10) + 
				1)
			{
				Log.Info("******Number of records: " + length);
				CreateSequenceFile(length);
				int count = CountRecords(0);
				NUnit.Framework.Assert.AreEqual(count, length == 0 ? 0 : (int)Math.Log10(length) 
					+ 1);
			}
			// clean up
			fs.Delete(inDir, true);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestPercentFilter()
		{
			Log.Info("Testing Percent Filter with frequency: 1000");
			// set the filter class
			SequenceFileInputFilter.SetFilterClass(job, typeof(SequenceFileInputFilter.PercentFilter
				));
			SequenceFileInputFilter.PercentFilter.SetFrequency(job, 1000);
			// clean input dir
			fs.Delete(inDir, true);
			// for a variety of lengths
			for (int length = 0; length < MaxLength; length += random.Next(MaxLength / 10) + 
				1)
			{
				Log.Info("******Number of records: " + length);
				CreateSequenceFile(length);
				int count = CountRecords(1);
				Log.Info("Accepted " + count + " records");
				int expectedCount = length / 1000;
				if (expectedCount * 1000 != length)
				{
					expectedCount++;
				}
				NUnit.Framework.Assert.AreEqual(count, expectedCount);
			}
			// clean up
			fs.Delete(inDir, true);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestMD5Filter()
		{
			// set the filter class
			Log.Info("Testing MD5 Filter with frequency: 1000");
			SequenceFileInputFilter.SetFilterClass(job, typeof(SequenceFileInputFilter.MD5Filter
				));
			SequenceFileInputFilter.MD5Filter.SetFrequency(job, 1000);
			// clean input dir
			fs.Delete(inDir, true);
			// for a variety of lengths
			for (int length = 0; length < MaxLength; length += random.Next(MaxLength / 10) + 
				1)
			{
				Log.Info("******Number of records: " + length);
				CreateSequenceFile(length);
				Log.Info("Accepted " + CountRecords(0) + " records");
			}
			// clean up
			fs.Delete(inDir, true);
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			TestSequenceFileInputFilter filter = new TestSequenceFileInputFilter();
			filter.TestRegexFilter();
		}
	}
}
