using System;
using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Task;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Input
{
	public class TestNLineInputFormat : TestCase
	{
		private static int MaxLength = 200;

		private static Configuration conf = new Configuration();

		private static FileSystem localFs = null;

		static TestNLineInputFormat()
		{
			try
			{
				localFs = FileSystem.GetLocal(conf);
			}
			catch (IOException e)
			{
				throw new RuntimeException("init failure", e);
			}
		}

		private static Path workDir = new Path(new Path(Runtime.GetProperty("test.build.data"
			, "."), "data"), "TestNLineInputFormat");

		/// <exception cref="System.Exception"/>
		public virtual void TestFormat()
		{
			Job job = Job.GetInstance(conf);
			Path file = new Path(workDir, "test.txt");
			localFs.Delete(workDir, true);
			FileInputFormat.SetInputPaths(job, workDir);
			int numLinesPerMap = 5;
			NLineInputFormat.SetNumLinesPerSplit(job, numLinesPerMap);
			for (int length = 0; length < MaxLength; length += 1)
			{
				// create a file with length entries
				TextWriter writer = new OutputStreamWriter(localFs.Create(file));
				try
				{
					for (int i = 0; i < length; i++)
					{
						writer.Write(Sharpen.Extensions.ToString(i) + " some more text");
						writer.Write("\n");
					}
				}
				finally
				{
					writer.Close();
				}
				int lastN = 0;
				if (length != 0)
				{
					lastN = length % 5;
					if (lastN == 0)
					{
						lastN = 5;
					}
				}
				CheckFormat(job, numLinesPerMap, lastN);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		internal virtual void CheckFormat(Job job, int expectedN, int lastN)
		{
			NLineInputFormat format = new NLineInputFormat();
			IList<InputSplit> splits = format.GetSplits(job);
			int count = 0;
			for (int i = 0; i < splits.Count; i++)
			{
				NUnit.Framework.Assert.AreEqual("There are no split locations", 0, splits[i].GetLocations
					().Length);
				TaskAttemptContext context = MapReduceTestUtil.CreateDummyMapTaskAttemptContext(job
					.GetConfiguration());
				RecordReader<LongWritable, Text> reader = format.CreateRecordReader(splits[i], context
					);
				Type clazz = reader.GetType();
				NUnit.Framework.Assert.AreEqual("reader class is LineRecordReader.", typeof(LineRecordReader
					), clazz);
				MapContext<LongWritable, Text, LongWritable, Text> mcontext = new MapContextImpl<
					LongWritable, Text, LongWritable, Text>(job.GetConfiguration(), context.GetTaskAttemptID
					(), reader, null, null, MapReduceTestUtil.CreateDummyReporter(), splits[i]);
				reader.Initialize(splits[i], mcontext);
				try
				{
					count = 0;
					while (reader.NextKeyValue())
					{
						count++;
					}
				}
				finally
				{
					reader.Close();
				}
				if (i == splits.Count - 1)
				{
					NUnit.Framework.Assert.AreEqual("number of lines in split(" + i + ") is wrong", lastN
						, count);
				}
				else
				{
					NUnit.Framework.Assert.AreEqual("number of lines in split(" + i + ") is wrong", expectedN
						, count);
				}
			}
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			new TestNLineInputFormat().TestFormat();
		}
	}
}
