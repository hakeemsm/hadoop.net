using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Task;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Input
{
	public class TestCombineFileRecordReader
	{
		private static Path outDir = new Path(Runtime.GetProperty("test.build.data", "/tmp"
			), typeof(TestCombineFileRecordReader).FullName);

		private class TextRecordReaderWrapper : CombineFileRecordReaderWrapper<LongWritable
			, Text>
		{
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public TextRecordReaderWrapper(CombineFileSplit split, TaskAttemptContext context
				, int idx)
				: base(new TextInputFormat(), split, context, idx)
			{
			}
			// this constructor signature is required by CombineFileRecordReader
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestProgressIsReportedIfInputASeriesOfEmptyFiles()
		{
			JobConf conf = new JobConf();
			Path[] paths = new Path[3];
			FilePath[] files = new FilePath[3];
			long[] fileLength = new long[3];
			try
			{
				for (int i = 0; i < 3; i++)
				{
					FilePath dir = new FilePath(outDir.ToString());
					dir.Mkdir();
					files[i] = new FilePath(dir, "testfile" + i);
					FileWriter fileWriter = new FileWriter(files[i]);
					fileWriter.Flush();
					fileWriter.Close();
					fileLength[i] = i;
					paths[i] = new Path(outDir + "/testfile" + i);
				}
				CombineFileSplit combineFileSplit = new CombineFileSplit(paths, fileLength);
				TaskAttemptID taskAttemptID = Org.Mockito.Mockito.Mock<TaskAttemptID>();
				Task.TaskReporter reporter = Org.Mockito.Mockito.Mock<Task.TaskReporter>();
				TaskAttemptContextImpl taskAttemptContext = new TaskAttemptContextImpl(conf, taskAttemptID
					, reporter);
				CombineFileRecordReader cfrr = new CombineFileRecordReader(combineFileSplit, taskAttemptContext
					, typeof(TestCombineFileRecordReader.TextRecordReaderWrapper));
				cfrr.Initialize(combineFileSplit, taskAttemptContext);
				Org.Mockito.Mockito.Verify(reporter).Progress();
				NUnit.Framework.Assert.IsFalse(cfrr.NextKeyValue());
				Org.Mockito.Mockito.Verify(reporter, Org.Mockito.Mockito.Times(3)).Progress();
			}
			finally
			{
				FileUtil.FullyDelete(new FilePath(outDir.ToString()));
			}
		}
	}
}
