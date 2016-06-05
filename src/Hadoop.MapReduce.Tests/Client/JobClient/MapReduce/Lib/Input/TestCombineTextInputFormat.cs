using System.Collections.Generic;
using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.IO.Compress;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Task;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Input
{
	public class TestCombineTextInputFormat
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestCombineTextInputFormat
			));

		private static Configuration defaultConf = new Configuration();

		private static FileSystem localFs = null;

		static TestCombineTextInputFormat()
		{
			try
			{
				defaultConf.Set("fs.defaultFS", "file:///");
				localFs = FileSystem.GetLocal(defaultConf);
			}
			catch (IOException e)
			{
				throw new RuntimeException("init failure", e);
			}
		}

		private static Path workDir = new Path(new Path(Runtime.GetProperty("test.build.data"
			, "."), "data"), "TestCombineTextInputFormat");

		/// <exception cref="System.Exception"/>
		public virtual void TestFormat()
		{
			Job job = Job.GetInstance(new Configuration(defaultConf));
			Random random = new Random();
			long seed = random.NextLong();
			Log.Info("seed = " + seed);
			random.SetSeed(seed);
			localFs.Delete(workDir, true);
			FileInputFormat.SetInputPaths(job, workDir);
			int length = 10000;
			int numFiles = 10;
			// create files with various lengths
			CreateFiles(length, numFiles, random);
			// create a combined split for the files
			CombineTextInputFormat format = new CombineTextInputFormat();
			for (int i = 0; i < 3; i++)
			{
				int numSplits = random.Next(length / 20) + 1;
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
				Log.Debug("split= " + split);
				TaskAttemptContext context = MapReduceTestUtil.CreateDummyMapTaskAttemptContext(job
					.GetConfiguration());
				RecordReader<LongWritable, Text> reader = format.CreateRecordReader(split, context
					);
				NUnit.Framework.Assert.AreEqual("reader class is CombineFileRecordReader.", typeof(
					CombineFileRecordReader), reader.GetType());
				MapContext<LongWritable, Text, LongWritable, Text> mcontext = new MapContextImpl<
					LongWritable, Text, LongWritable, Text>(job.GetConfiguration(), context.GetTaskAttemptID
					(), reader, null, null, MapReduceTestUtil.CreateDummyReporter(), split);
				reader.Initialize(split, mcontext);
				try
				{
					int count = 0;
					while (reader.NextKeyValue())
					{
						LongWritable key = reader.GetCurrentKey();
						NUnit.Framework.Assert.IsNotNull("Key should not be null.", key);
						Text value = reader.GetCurrentValue();
						int v = System.Convert.ToInt32(value.ToString());
						Log.Debug("read " + v);
						NUnit.Framework.Assert.IsFalse("Key in multiple partitions.", bits.Get(v));
						bits.Set(v);
						count++;
					}
					Log.Debug("split=" + split + " count=" + count);
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

		private static TestCombineTextInputFormat.Range[] CreateRanges(int length, int numFiles
			, Random random)
		{
			// generate a number of files with various lengths
			TestCombineTextInputFormat.Range[] ranges = new TestCombineTextInputFormat.Range[
				numFiles];
			for (int i = 0; i < numFiles; i++)
			{
				int start = i == 0 ? 0 : ranges[i - 1].end;
				int end = i == numFiles - 1 ? length : (length / numFiles) * (2 * i + 1) / 2 + random
					.Next(length / numFiles) + 1;
				ranges[i] = new TestCombineTextInputFormat.Range(start, end);
			}
			return ranges;
		}

		/// <exception cref="System.IO.IOException"/>
		private static void CreateFiles(int length, int numFiles, Random random)
		{
			TestCombineTextInputFormat.Range[] ranges = CreateRanges(length, numFiles, random
				);
			for (int i = 0; i < numFiles; i++)
			{
				Path file = new Path(workDir, "test_" + i + ".txt");
				TextWriter writer = new OutputStreamWriter(localFs.Create(file));
				TestCombineTextInputFormat.Range range = ranges[i];
				try
				{
					for (int j = range.start; j < range.end; j++)
					{
						writer.Write(Sharpen.Extensions.ToString(j));
						writer.Write("\n");
					}
				}
				finally
				{
					writer.Close();
				}
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
		/// <exception cref="System.Exception"/>
		private static IList<Text> ReadSplit(InputFormat<LongWritable, Text> format, InputSplit
			 split, Job job)
		{
			IList<Text> result = new AList<Text>();
			Configuration conf = job.GetConfiguration();
			TaskAttemptContext context = MapReduceTestUtil.CreateDummyMapTaskAttemptContext(conf
				);
			RecordReader<LongWritable, Text> reader = format.CreateRecordReader(split, MapReduceTestUtil
				.CreateDummyMapTaskAttemptContext(conf));
			MapContext<LongWritable, Text, LongWritable, Text> mcontext = new MapContextImpl<
				LongWritable, Text, LongWritable, Text>(conf, context.GetTaskAttemptID(), reader
				, null, null, MapReduceTestUtil.CreateDummyReporter(), split);
			reader.Initialize(split, mcontext);
			while (reader.NextKeyValue())
			{
				result.AddItem(new Text(reader.GetCurrentValue()));
			}
			return result;
		}

		/// <summary>Test using the gzip codec for reading</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void TestGzip()
		{
			Configuration conf = new Configuration(defaultConf);
			CompressionCodec gzip = new GzipCodec();
			ReflectionUtils.SetConf(gzip, conf);
			localFs.Delete(workDir, true);
			WriteFile(localFs, new Path(workDir, "part1.txt.gz"), gzip, "the quick\nbrown\nfox jumped\nover\n the lazy\n dog\n"
				);
			WriteFile(localFs, new Path(workDir, "part2.txt.gz"), gzip, "this is a test\nof gzip\n"
				);
			Job job = Job.GetInstance(conf);
			FileInputFormat.SetInputPaths(job, workDir);
			CombineTextInputFormat format = new CombineTextInputFormat();
			IList<InputSplit> splits = format.GetSplits(job);
			NUnit.Framework.Assert.AreEqual("compressed splits == 1", 1, splits.Count);
			IList<Text> results = ReadSplit(format, splits[0], job);
			NUnit.Framework.Assert.AreEqual("splits[0] length", 8, results.Count);
			string[] firstList = new string[] { "the quick", "brown", "fox jumped", "over", " the lazy"
				, " dog" };
			string[] secondList = new string[] { "this is a test", "of gzip" };
			string first = results[0].ToString();
			if (first.Equals(firstList[0]))
			{
				TestResults(results, firstList, secondList);
			}
			else
			{
				if (first.Equals(secondList[0]))
				{
					TestResults(results, secondList, firstList);
				}
				else
				{
					NUnit.Framework.Assert.Fail("unexpected first token!");
				}
			}
		}

		private static void TestResults(IList<Text> results, string[] first, string[] second
			)
		{
			for (int i = 0; i < first.Length; i++)
			{
				NUnit.Framework.Assert.AreEqual("splits[0][" + i + "]", first[i], results[i].ToString
					());
			}
			for (int i_1 = 0; i_1 < second.Length; i_1++)
			{
				int j = i_1 + first.Length;
				NUnit.Framework.Assert.AreEqual("splits[0][" + j + "]", second[i_1], results[j].ToString
					());
			}
		}
	}
}
