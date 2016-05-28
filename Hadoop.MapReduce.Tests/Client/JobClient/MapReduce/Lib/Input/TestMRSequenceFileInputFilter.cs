using System;
using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Task;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Input
{
	public class TestMRSequenceFileInputFilter : TestCase
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestMRSequenceFileInputFilter
			).FullName);

		private const int MaxLength = 15000;

		private static readonly Configuration conf = new Configuration();

		private static readonly Job job;

		private static readonly FileSystem fs;

		private static readonly Path inDir = new Path(Runtime.GetProperty("test.build.data"
			, ".") + "/mapred");

		private static readonly Path inFile = new Path(inDir, "test.seq");

		private static readonly Random random = new Random(1);

		static TestMRSequenceFileInputFilter()
		{
			try
			{
				job = Job.GetInstance(conf);
				FileInputFormat.SetInputPaths(job, inDir);
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
		/// <exception cref="System.Exception"/>
		private int CountRecords(int numSplits)
		{
			InputFormat<Text, BytesWritable> format = new SequenceFileInputFilter<Text, BytesWritable
				>();
			if (numSplits == 0)
			{
				numSplits = random.Next(MaxLength / (SequenceFile.SyncInterval / 20)) + 1;
			}
			FileInputFormat.SetMaxInputSplitSize(job, fs.GetFileStatus(inFile).GetLen() / numSplits
				);
			TaskAttemptContext context = MapReduceTestUtil.CreateDummyMapTaskAttemptContext(job
				.GetConfiguration());
			// check each split
			int count = 0;
			foreach (InputSplit split in format.GetSplits(job))
			{
				RecordReader<Text, BytesWritable> reader = format.CreateRecordReader(split, context
					);
				MapContext<Text, BytesWritable, Text, BytesWritable> mcontext = new MapContextImpl
					<Text, BytesWritable, Text, BytesWritable>(job.GetConfiguration(), context.GetTaskAttemptID
					(), reader, null, null, MapReduceTestUtil.CreateDummyReporter(), split);
				reader.Initialize(split, mcontext);
				try
				{
					while (reader.NextKeyValue())
					{
						Log.Info("Accept record " + reader.GetCurrentKey().ToString());
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
			SequenceFileInputFilter.RegexFilter.SetPattern(job.GetConfiguration(), "\\A10*");
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
			SequenceFileInputFilter.PercentFilter.SetFrequency(job.GetConfiguration(), 1000);
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
			SequenceFileInputFilter.MD5Filter.SetFrequency(job.GetConfiguration(), 1000);
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
			TestMRSequenceFileInputFilter filter = new TestMRSequenceFileInputFilter();
			filter.TestRegexFilter();
		}
	}
}
