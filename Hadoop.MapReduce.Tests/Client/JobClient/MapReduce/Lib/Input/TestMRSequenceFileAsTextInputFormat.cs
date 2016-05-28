using System;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Task;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Input
{
	public class TestMRSequenceFileAsTextInputFormat : TestCase
	{
		private static int MaxLength = 10000;

		private static Configuration conf = new Configuration();

		/// <exception cref="System.Exception"/>
		public virtual void TestFormat()
		{
			Job job = Job.GetInstance(conf);
			FileSystem fs = FileSystem.GetLocal(conf);
			Path dir = new Path(Runtime.GetProperty("test.build.data", ".") + "/mapred");
			Path file = new Path(dir, "test.seq");
			int seed = new Random().Next();
			Random random = new Random(seed);
			fs.Delete(dir, true);
			FileInputFormat.SetInputPaths(job, dir);
			// for a variety of lengths
			for (int length = 0; length < MaxLength; length += random.Next(MaxLength / 10) + 
				1)
			{
				// create a file with length entries
				SequenceFile.Writer writer = SequenceFile.CreateWriter(fs, conf, file, typeof(IntWritable
					), typeof(LongWritable));
				try
				{
					for (int i = 0; i < length; i++)
					{
						IntWritable key = new IntWritable(i);
						LongWritable value = new LongWritable(10 * i);
						writer.Append(key, value);
					}
				}
				finally
				{
					writer.Close();
				}
				TaskAttemptContext context = MapReduceTestUtil.CreateDummyMapTaskAttemptContext(job
					.GetConfiguration());
				// try splitting the file in a variety of sizes
				InputFormat<Text, Text> format = new SequenceFileAsTextInputFormat();
				for (int i_1 = 0; i_1 < 3; i_1++)
				{
					// check each split
					BitSet bits = new BitSet(length);
					int numSplits = random.Next(MaxLength / (SequenceFile.SyncInterval / 20)) + 1;
					FileInputFormat.SetMaxInputSplitSize(job, fs.GetFileStatus(file).GetLen() / numSplits
						);
					foreach (InputSplit split in format.GetSplits(job))
					{
						RecordReader<Text, Text> reader = format.CreateRecordReader(split, context);
						MapContext<Text, Text, Text, Text> mcontext = new MapContextImpl<Text, Text, Text
							, Text>(job.GetConfiguration(), context.GetTaskAttemptID(), reader, null, null, 
							MapReduceTestUtil.CreateDummyReporter(), split);
						reader.Initialize(split, mcontext);
						Type readerClass = reader.GetType();
						NUnit.Framework.Assert.AreEqual("reader class is SequenceFileAsTextRecordReader."
							, typeof(SequenceFileAsTextRecordReader), readerClass);
						Text key;
						try
						{
							int count = 0;
							while (reader.NextKeyValue())
							{
								key = reader.GetCurrentKey();
								int keyInt = System.Convert.ToInt32(key.ToString());
								NUnit.Framework.Assert.IsFalse("Key in multiple partitions.", bits.Get(keyInt));
								bits.Set(keyInt);
								count++;
							}
						}
						finally
						{
							reader.Close();
						}
					}
					NUnit.Framework.Assert.AreEqual("Some keys in no partition.", length, bits.Cardinality
						());
				}
			}
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			new TestMRSequenceFileAsTextInputFormat().TestFormat();
		}
	}
}
