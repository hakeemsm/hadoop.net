using System.Collections.Generic;
using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.IO.Compress;
using Org.Apache.Hadoop.Mapred.Lib;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public class TestCombineTextInputFormat
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestCombineTextInputFormat
			));

		private static JobConf defaultConf = new JobConf();

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
			, "/tmp")), "TestCombineTextInputFormat").MakeQualified(localFs);

		private static readonly Reporter voidReporter = Reporter.Null;

		// A reporter that does nothing
		/// <exception cref="System.Exception"/>
		public virtual void TestFormat()
		{
			JobConf job = new JobConf(defaultConf);
			Random random = new Random();
			long seed = random.NextLong();
			Log.Info("seed = " + seed);
			random.SetSeed(seed);
			localFs.Delete(workDir, true);
			FileInputFormat.SetInputPaths(job, workDir);
			int length = 10000;
			int numFiles = 10;
			CreateFiles(length, numFiles, random);
			// create a combined split for the files
			CombineTextInputFormat format = new CombineTextInputFormat();
			LongWritable key = new LongWritable();
			Text value = new Text();
			for (int i = 0; i < 3; i++)
			{
				int numSplits = random.Next(length / 20) + 1;
				Log.Info("splitting: requesting = " + numSplits);
				InputSplit[] splits = format.GetSplits(job, numSplits);
				Log.Info("splitting: got =        " + splits.Length);
				// we should have a single split as the length is comfortably smaller than
				// the block size
				NUnit.Framework.Assert.AreEqual("We got more than one splits!", 1, splits.Length);
				InputSplit split = splits[0];
				NUnit.Framework.Assert.AreEqual("It should be CombineFileSplit", typeof(CombineFileSplit
					), split.GetType());
				// check the split
				BitSet bits = new BitSet(length);
				Log.Debug("split= " + split);
				RecordReader<LongWritable, Text> reader = format.GetRecordReader(split, job, voidReporter
					);
				try
				{
					int count = 0;
					while (reader.Next(key, value))
					{
						int v = System.Convert.ToInt32(value.ToString());
						Log.Debug("read " + v);
						if (bits.Get(v))
						{
							Log.Warn("conflict with " + v + " at position " + reader.GetPos());
						}
						NUnit.Framework.Assert.IsFalse("Key in multiple partitions.", bits.Get(v));
						bits.Set(v);
						count++;
					}
					Log.Info("splits=" + split + " count=" + count);
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
		private static IList<Text> ReadSplit(InputFormat<LongWritable, Text> format, InputSplit
			 split, JobConf job)
		{
			IList<Text> result = new AList<Text>();
			RecordReader<LongWritable, Text> reader = format.GetRecordReader(split, job, voidReporter
				);
			LongWritable key = reader.CreateKey();
			Text value = reader.CreateValue();
			while (reader.Next(key, value))
			{
				result.AddItem(value);
				value = reader.CreateValue();
			}
			reader.Close();
			return result;
		}

		/// <summary>Test using the gzip codec for reading</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestGzip()
		{
			JobConf job = new JobConf(defaultConf);
			CompressionCodec gzip = new GzipCodec();
			ReflectionUtils.SetConf(gzip, job);
			localFs.Delete(workDir, true);
			WriteFile(localFs, new Path(workDir, "part1.txt.gz"), gzip, "the quick\nbrown\nfox jumped\nover\n the lazy\n dog\n"
				);
			WriteFile(localFs, new Path(workDir, "part2.txt.gz"), gzip, "this is a test\nof gzip\n"
				);
			FileInputFormat.SetInputPaths(job, workDir);
			CombineTextInputFormat format = new CombineTextInputFormat();
			InputSplit[] splits = format.GetSplits(job, 100);
			NUnit.Framework.Assert.AreEqual("compressed splits == 1", 1, splits.Length);
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
