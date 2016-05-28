using System;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Lib
{
	public class TestLineInputFormat : TestCase
	{
		private static int MaxLength = 200;

		private static JobConf defaultConf = new JobConf();

		private static FileSystem localFs = null;

		static TestLineInputFormat()
		{
			try
			{
				localFs = FileSystem.GetLocal(defaultConf);
			}
			catch (IOException e)
			{
				throw new RuntimeException("init failure", e);
			}
		}

		private static Path workDir = new Path(new Path(Runtime.GetProperty("test.build.data"
			, "."), "data"), "TestLineInputFormat");

		/// <exception cref="System.Exception"/>
		public virtual void TestFormat()
		{
			JobConf job = new JobConf();
			Path file = new Path(workDir, "test.txt");
			int seed = new Random().Next();
			Random random = new Random(seed);
			localFs.Delete(workDir, true);
			FileInputFormat.SetInputPaths(job, workDir);
			int numLinesPerMap = 5;
			job.SetInt("mapreduce.input.lineinputformat.linespermap", numLinesPerMap);
			// for a variety of lengths
			for (int length = 0; length < MaxLength; length += random.Next(MaxLength / 10) + 
				1)
			{
				// create a file with length entries
				TextWriter writer = new OutputStreamWriter(localFs.Create(file));
				try
				{
					for (int i = 0; i < length; i++)
					{
						writer.Write(Sharpen.Extensions.ToString(i));
						writer.Write("\n");
					}
				}
				finally
				{
					writer.Close();
				}
				CheckFormat(job, numLinesPerMap);
			}
		}

		private static readonly Reporter voidReporter = Reporter.Null;

		// A reporter that does nothing
		/// <exception cref="System.IO.IOException"/>
		internal virtual void CheckFormat(JobConf job, int expectedN)
		{
			NLineInputFormat format = new NLineInputFormat();
			format.Configure(job);
			int ignoredNumSplits = 1;
			InputSplit[] splits = format.GetSplits(job, ignoredNumSplits);
			// check all splits except last one
			int count = 0;
			for (int j = 0; j < splits.Length - 1; j++)
			{
				NUnit.Framework.Assert.AreEqual("There are no split locations", 0, splits[j].GetLocations
					().Length);
				RecordReader<LongWritable, Text> reader = format.GetRecordReader(splits[j], job, 
					voidReporter);
				Type readerClass = reader.GetType();
				NUnit.Framework.Assert.AreEqual("reader class is LineRecordReader.", typeof(LineRecordReader
					), readerClass);
				LongWritable key = reader.CreateKey();
				Type keyClass = key.GetType();
				NUnit.Framework.Assert.AreEqual("Key class is LongWritable.", typeof(LongWritable
					), keyClass);
				Text value = reader.CreateValue();
				Type valueClass = value.GetType();
				NUnit.Framework.Assert.AreEqual("Value class is Text.", typeof(Text), valueClass);
				try
				{
					count = 0;
					while (reader.Next(key, value))
					{
						count++;
					}
				}
				finally
				{
					reader.Close();
				}
				NUnit.Framework.Assert.AreEqual("number of lines in split is " + expectedN, expectedN
					, count);
			}
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			new TestLineInputFormat().TestFormat();
		}
	}
}
