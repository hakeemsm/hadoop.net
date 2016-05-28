using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public class TestSequenceFileAsBinaryInputFormat : TestCase
	{
		private static readonly Log Log = FileInputFormat.Log;

		private const int Records = 10000;

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestBinary()
		{
			JobConf job = new JobConf();
			FileSystem fs = FileSystem.GetLocal(job);
			Path dir = new Path(Runtime.GetProperty("test.build.data", ".") + "/mapred");
			Path file = new Path(dir, "testbinary.seq");
			Random r = new Random();
			long seed = r.NextLong();
			r.SetSeed(seed);
			fs.Delete(dir, true);
			FileInputFormat.SetInputPaths(job, dir);
			Text tkey = new Text();
			Text tval = new Text();
			SequenceFile.Writer writer = new SequenceFile.Writer(fs, job, file, typeof(Text), 
				typeof(Text));
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
			InputFormat<BytesWritable, BytesWritable> bformat = new SequenceFileAsBinaryInputFormat
				();
			int count = 0;
			r.SetSeed(seed);
			BytesWritable bkey = new BytesWritable();
			BytesWritable bval = new BytesWritable();
			Text cmpkey = new Text();
			Text cmpval = new Text();
			DataInputBuffer buf = new DataInputBuffer();
			int NumSplits = 3;
			FileInputFormat.SetInputPaths(job, file);
			foreach (InputSplit split in bformat.GetSplits(job, NumSplits))
			{
				RecordReader<BytesWritable, BytesWritable> reader = bformat.GetRecordReader(split
					, job, Reporter.Null);
				try
				{
					while (reader.Next(bkey, bval))
					{
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
