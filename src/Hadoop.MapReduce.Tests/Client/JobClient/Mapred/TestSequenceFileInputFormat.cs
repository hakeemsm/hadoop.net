using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public class TestSequenceFileInputFormat : TestCase
	{
		private static readonly Log Log = FileInputFormat.Log;

		private static int MaxLength = 10000;

		private static Configuration conf = new Configuration();

		/// <exception cref="System.Exception"/>
		public virtual void TestFormat()
		{
			JobConf job = new JobConf(conf);
			FileSystem fs = FileSystem.GetLocal(conf);
			Path dir = new Path(Runtime.GetProperty("test.build.data", ".") + "/mapred");
			Path file = new Path(dir, "test.seq");
			Reporter reporter = Reporter.Null;
			int seed = new Random().Next();
			//LOG.info("seed = "+seed);
			Random random = new Random(seed);
			fs.Delete(dir, true);
			FileInputFormat.SetInputPaths(job, dir);
			// for a variety of lengths
			for (int length = 0; length < MaxLength; length += random.Next(MaxLength / 10) + 
				1)
			{
				//LOG.info("creating; entries = " + length);
				// create a file with length entries
				SequenceFile.Writer writer = SequenceFile.CreateWriter(fs, conf, file, typeof(IntWritable
					), typeof(BytesWritable));
				try
				{
					for (int i = 0; i < length; i++)
					{
						IntWritable key = new IntWritable(i);
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
				// try splitting the file in a variety of sizes
				InputFormat<IntWritable, BytesWritable> format = new SequenceFileInputFormat<IntWritable
					, BytesWritable>();
				IntWritable key_1 = new IntWritable();
				BytesWritable value_1 = new BytesWritable();
				for (int i_1 = 0; i_1 < 3; i_1++)
				{
					int numSplits = random.Next(MaxLength / (SequenceFile.SyncInterval / 20)) + 1;
					//LOG.info("splitting: requesting = " + numSplits);
					InputSplit[] splits = format.GetSplits(job, numSplits);
					//LOG.info("splitting: got =        " + splits.length);
					// check each split
					BitSet bits = new BitSet(length);
					for (int j = 0; j < splits.Length; j++)
					{
						RecordReader<IntWritable, BytesWritable> reader = format.GetRecordReader(splits[j
							], job, reporter);
						try
						{
							int count = 0;
							while (reader.Next(key_1, value_1))
							{
								// if (bits.get(key.get())) {
								// LOG.info("splits["+j+"]="+splits[j]+" : " + key.get());
								// LOG.info("@"+reader.getPos());
								// }
								NUnit.Framework.Assert.IsFalse("Key in multiple partitions.", bits.Get(key_1.Get(
									)));
								bits.Set(key_1.Get());
								count++;
							}
						}
						finally
						{
							//LOG.info("splits["+j+"]="+splits[j]+" count=" + count);
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
			new TestSequenceFileInputFormat().TestFormat();
		}
	}
}
