using System.Collections.Generic;
using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Task;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Input
{
	public class TestCombineSequenceFileInputFormat
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestCombineSequenceFileInputFormat
			));

		private static Configuration conf = new Configuration();

		private static FileSystem localFs = null;

		static TestCombineSequenceFileInputFormat()
		{
			try
			{
				conf.Set("fs.defaultFS", "file:///");
				localFs = FileSystem.GetLocal(conf);
			}
			catch (IOException e)
			{
				throw new RuntimeException("init failure", e);
			}
		}

		private static Path workDir = new Path(new Path(Runtime.GetProperty("test.build.data"
			, "."), "data"), "TestCombineSequenceFileInputFormat");

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void TestFormat()
		{
			Job job = Job.GetInstance(conf);
			Random random = new Random();
			long seed = random.NextLong();
			random.SetSeed(seed);
			localFs.Delete(workDir, true);
			FileInputFormat.SetInputPaths(job, workDir);
			int length = 10000;
			int numFiles = 10;
			// create files with a variety of lengths
			CreateFiles(length, numFiles, random, job);
			TaskAttemptContext context = MapReduceTestUtil.CreateDummyMapTaskAttemptContext(job
				.GetConfiguration());
			// create a combine split for the files
			InputFormat<IntWritable, BytesWritable> format = new CombineSequenceFileInputFormat
				<IntWritable, BytesWritable>();
			for (int i = 0; i < 3; i++)
			{
				int numSplits = random.Next(length / (SequenceFile.SyncInterval / 20)) + 1;
				Log.Info("splitting: requesting = " + numSplits);
				IList<InputSplit> splits = format.GetSplits(job);
				Log.Info("splitting: got =        " + splits.Count);
				// we should have a single split as the length is comfortably smaller than
				// the block size
				NUnit.Framework.Assert.AreEqual("We got more than one splits!", 1, splits.Count);
				InputSplit split = splits[0];
				NUnit.Framework.Assert.AreEqual("It should be CombineFileSplit", typeof(CombineFileSplit
					), split.GetType());
				// check the split
				BitSet bits = new BitSet(length);
				RecordReader<IntWritable, BytesWritable> reader = format.CreateRecordReader(split
					, context);
				MapContext<IntWritable, BytesWritable, IntWritable, BytesWritable> mcontext = new 
					MapContextImpl<IntWritable, BytesWritable, IntWritable, BytesWritable>(job.GetConfiguration
					(), context.GetTaskAttemptID(), reader, null, null, MapReduceTestUtil.CreateDummyReporter
					(), split);
				reader.Initialize(split, mcontext);
				NUnit.Framework.Assert.AreEqual("reader class is CombineFileRecordReader.", typeof(
					CombineFileRecordReader), reader.GetType());
				try
				{
					while (reader.NextKeyValue())
					{
						IntWritable key = reader.GetCurrentKey();
						BytesWritable value = reader.GetCurrentValue();
						NUnit.Framework.Assert.IsNotNull("Value should not be null.", value);
						int k = key.Get();
						Log.Debug("read " + k);
						NUnit.Framework.Assert.IsFalse("Key in multiple partitions.", bits.Get(k));
						bits.Set(k);
					}
				}
				finally
				{
					reader.Close();
				}
				NUnit.Framework.Assert.AreEqual("Some keys in no partition.", length, bits.Cardinality
					());
			}
		}

		private class Range
		{
			private readonly int start;

			private readonly int end;

			internal Range(int start, int end)
			{
				this.start = start;
				this.end = end;
			}

			public override string ToString()
			{
				return "(" + start + ", " + end + ")";
			}
		}

		private static TestCombineSequenceFileInputFormat.Range[] CreateRanges(int length
			, int numFiles, Random random)
		{
			// generate a number of files with various lengths
			TestCombineSequenceFileInputFormat.Range[] ranges = new TestCombineSequenceFileInputFormat.Range
				[numFiles];
			for (int i = 0; i < numFiles; i++)
			{
				int start = i == 0 ? 0 : ranges[i - 1].end;
				int end = i == numFiles - 1 ? length : (length / numFiles) * (2 * i + 1) / 2 + random
					.Next(length / numFiles) + 1;
				ranges[i] = new TestCombineSequenceFileInputFormat.Range(start, end);
			}
			return ranges;
		}

		/// <exception cref="System.IO.IOException"/>
		private static void CreateFiles(int length, int numFiles, Random random, Job job)
		{
			TestCombineSequenceFileInputFormat.Range[] ranges = CreateRanges(length, numFiles
				, random);
			for (int i = 0; i < numFiles; i++)
			{
				Path file = new Path(workDir, "test_" + i + ".seq");
				// create a file with length entries
				SequenceFile.Writer writer = SequenceFile.CreateWriter(localFs, job.GetConfiguration
					(), file, typeof(IntWritable), typeof(BytesWritable));
				TestCombineSequenceFileInputFormat.Range range = ranges[i];
				try
				{
					for (int j = range.start; j < range.end; j++)
					{
						IntWritable key = new IntWritable(j);
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
		}
	}
}
