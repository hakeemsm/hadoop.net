using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO.Compress;
using Org.Apache.Hadoop.Mapred;
using Sharpen;

namespace Org.Apache.Hadoop.IO
{
	public class TestSequenceFileMergeProgress : TestCase
	{
		private static readonly Log Log = FileInputFormat.Log;

		private const int Records = 10000;

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestMergeProgressWithNoCompression()
		{
			RunTest(SequenceFile.CompressionType.None);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestMergeProgressWithRecordCompression()
		{
			RunTest(SequenceFile.CompressionType.Record);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestMergeProgressWithBlockCompression()
		{
			RunTest(SequenceFile.CompressionType.Block);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void RunTest(SequenceFile.CompressionType compressionType)
		{
			JobConf job = new JobConf();
			FileSystem fs = FileSystem.GetLocal(job);
			Path dir = new Path(Runtime.GetProperty("test.build.data", ".") + "/mapred");
			Path file = new Path(dir, "test.seq");
			Path tempDir = new Path(dir, "tmp");
			fs.Delete(dir, true);
			FileInputFormat.SetInputPaths(job, dir);
			fs.Mkdirs(tempDir);
			LongWritable tkey = new LongWritable();
			Text tval = new Text();
			SequenceFile.Writer writer = SequenceFile.CreateWriter(fs, job, file, typeof(LongWritable
				), typeof(Text), compressionType, new DefaultCodec());
			try
			{
				for (int i = 0; i < Records; ++i)
				{
					tkey.Set(1234);
					tval.Set("valuevaluevaluevaluevaluevaluevaluevaluevaluevaluevalue");
					writer.Append(tkey, tval);
				}
			}
			finally
			{
				writer.Close();
			}
			long fileLength = fs.GetFileStatus(file).GetLen();
			Log.Info("With compression = " + compressionType + ": " + "compressed length = " 
				+ fileLength);
			SequenceFile.Sorter sorter = new SequenceFile.Sorter(fs, job.GetOutputKeyComparator
				(), job.GetMapOutputKeyClass(), job.GetMapOutputValueClass(), job);
			Path[] paths = new Path[] { file };
			SequenceFile.Sorter.RawKeyValueIterator rIter = sorter.Merge(paths, tempDir, false
				);
			int count = 0;
			while (rIter.Next())
			{
				count++;
			}
			NUnit.Framework.Assert.AreEqual(Records, count);
			NUnit.Framework.Assert.AreEqual(1.0f, rIter.GetProgress().Get());
		}
	}
}
