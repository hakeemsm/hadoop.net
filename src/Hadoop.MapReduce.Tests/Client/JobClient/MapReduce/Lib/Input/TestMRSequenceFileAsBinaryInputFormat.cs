using NUnit.Framework;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Task;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Input
{
	public class TestMRSequenceFileAsBinaryInputFormat : TestCase
	{
		private const int Records = 10000;

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void TestBinary()
		{
			Job job = Job.GetInstance();
			FileSystem fs = FileSystem.GetLocal(job.GetConfiguration());
			Path dir = new Path(Runtime.GetProperty("test.build.data", ".") + "/mapred");
			Path file = new Path(dir, "testbinary.seq");
			Random r = new Random();
			long seed = r.NextLong();
			r.SetSeed(seed);
			fs.Delete(dir, true);
			FileInputFormat.SetInputPaths(job, dir);
			Text tkey = new Text();
			Text tval = new Text();
			SequenceFile.Writer writer = new SequenceFile.Writer(fs, job.GetConfiguration(), 
				file, typeof(Text), typeof(Text));
			try
			{
				for (int i = 0; i < Records; ++i)
				{
					tkey.Set(Sharpen.Extensions.ToString(r.Next(), 36));
					tval.Set(System.Convert.ToString(r.NextLong(), 36));
					writer.Append(tkey, tval);
				}
			}
			finally
			{
				writer.Close();
			}
			TaskAttemptContext context = MapReduceTestUtil.CreateDummyMapTaskAttemptContext(job
				.GetConfiguration());
			InputFormat<BytesWritable, BytesWritable> bformat = new SequenceFileAsBinaryInputFormat
				();
			int count = 0;
			r.SetSeed(seed);
			BytesWritable bkey = new BytesWritable();
			BytesWritable bval = new BytesWritable();
			Text cmpkey = new Text();
			Text cmpval = new Text();
			DataInputBuffer buf = new DataInputBuffer();
			FileInputFormat.SetInputPaths(job, file);
			foreach (InputSplit split in bformat.GetSplits(job))
			{
				RecordReader<BytesWritable, BytesWritable> reader = bformat.CreateRecordReader(split
					, context);
				MapContext<BytesWritable, BytesWritable, BytesWritable, BytesWritable> mcontext = 
					new MapContextImpl<BytesWritable, BytesWritable, BytesWritable, BytesWritable>(job
					.GetConfiguration(), context.GetTaskAttemptID(), reader, null, null, MapReduceTestUtil
					.CreateDummyReporter(), split);
				reader.Initialize(split, mcontext);
				try
				{
					while (reader.NextKeyValue())
					{
						bkey = reader.GetCurrentKey();
						bval = reader.GetCurrentValue();
						tkey.Set(Sharpen.Extensions.ToString(r.Next(), 36));
						tval.Set(System.Convert.ToString(r.NextLong(), 36));
						buf.Reset(bkey.GetBytes(), bkey.GetLength());
						cmpkey.ReadFields(buf);
						buf.Reset(bval.GetBytes(), bval.GetLength());
						cmpval.ReadFields(buf);
						NUnit.Framework.Assert.IsTrue("Keys don't match: " + "*" + cmpkey.ToString() + ":"
							 + tkey.ToString() + "*", cmpkey.ToString().Equals(tkey.ToString()));
						NUnit.Framework.Assert.IsTrue("Vals don't match: " + "*" + cmpval.ToString() + ":"
							 + tval.ToString() + "*", cmpval.ToString().Equals(tval.ToString()));
						++count;
					}
				}
				finally
				{
					reader.Close();
				}
			}
			NUnit.Framework.Assert.AreEqual("Some records not found", Records, count);
		}
	}
}
