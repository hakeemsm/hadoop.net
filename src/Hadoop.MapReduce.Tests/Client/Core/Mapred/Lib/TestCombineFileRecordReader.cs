using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Lib
{
	public class TestCombineFileRecordReader
	{
		private static Path outDir = new Path(Runtime.GetProperty("test.build.data", "/tmp"
			), typeof(TestCombineFileRecordReader).FullName);

		private class TextRecordReaderWrapper : CombineFileRecordReaderWrapper<LongWritable
			, Text>
		{
			/// <exception cref="System.IO.IOException"/>
			public TextRecordReaderWrapper(CombineFileSplit split, Configuration conf, Reporter
				 reporter, int idx)
				: base(new TextInputFormat(), split, conf, reporter, idx)
			{
			}
			// this constructor signature is required by CombineFileRecordReader
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestInitNextRecordReader()
		{
			JobConf conf = new JobConf();
			Path[] paths = new Path[3];
			long[] fileLength = new long[3];
			FilePath[] files = new FilePath[3];
			LongWritable key = new LongWritable(1);
			Text value = new Text();
			try
			{
				for (int i = 0; i < 3; i++)
				{
					fileLength[i] = i;
					FilePath dir = new FilePath(outDir.ToString());
					dir.Mkdir();
					files[i] = new FilePath(dir, "testfile" + i);
					FileWriter fileWriter = new FileWriter(files[i]);
					fileWriter.Close();
					paths[i] = new Path(outDir + "/testfile" + i);
				}
				CombineFileSplit combineFileSplit = new CombineFileSplit(conf, paths, fileLength);
				Reporter reporter = Org.Mockito.Mockito.Mock<Reporter>();
				CombineFileRecordReader cfrr = new CombineFileRecordReader(conf, combineFileSplit
					, reporter, typeof(TestCombineFileRecordReader.TextRecordReaderWrapper));
				Org.Mockito.Mockito.Verify(reporter).Progress();
				NUnit.Framework.Assert.IsFalse(cfrr.Next(key, value));
				Org.Mockito.Mockito.Verify(reporter, Org.Mockito.Mockito.Times(3)).Progress();
			}
			finally
			{
				FileUtil.FullyDelete(new FilePath(outDir.ToString()));
			}
		}
	}
}
