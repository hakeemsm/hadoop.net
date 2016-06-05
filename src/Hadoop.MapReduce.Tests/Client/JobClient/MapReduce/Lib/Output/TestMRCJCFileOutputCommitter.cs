using System;
using System.IO;
using System.Text;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Task;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Output
{
	public class TestMRCJCFileOutputCommitter : TestCase
	{
		private static Path outDir = new Path(Runtime.GetProperty("test.build.data", "/tmp"
			), "output");

		private static string attempt = "attempt_200707121733_0001_m_000000_0";

		private static string partFile = "part-m-00000";

		private static TaskAttemptID taskID = TaskAttemptID.ForName(attempt);

		private Text key1 = new Text("key1");

		private Text key2 = new Text("key2");

		private Text val1 = new Text("val1");

		private Text val2 = new Text("val2");

		// A random task attempt id for testing.
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		private void WriteOutput(RecordWriter theRecordWriter, TaskAttemptContext context
			)
		{
			NullWritable nullWritable = NullWritable.Get();
			try
			{
				theRecordWriter.Write(key1, val1);
				theRecordWriter.Write(null, nullWritable);
				theRecordWriter.Write(null, val1);
				theRecordWriter.Write(nullWritable, val2);
				theRecordWriter.Write(key2, nullWritable);
				theRecordWriter.Write(key1, null);
				theRecordWriter.Write(null, null);
				theRecordWriter.Write(key2, val2);
			}
			finally
			{
				theRecordWriter.Close(context);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static void Cleanup()
		{
			Configuration conf = new Configuration();
			FileSystem fs = outDir.GetFileSystem(conf);
			fs.Delete(outDir, true);
		}

		/// <exception cref="System.IO.IOException"/>
		protected override void SetUp()
		{
			Cleanup();
		}

		/// <exception cref="System.IO.IOException"/>
		protected override void TearDown()
		{
			Cleanup();
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestCommitter()
		{
			Job job = Job.GetInstance();
			FileOutputFormat.SetOutputPath(job, outDir);
			Configuration conf = job.GetConfiguration();
			conf.Set(MRJobConfig.TaskAttemptId, attempt);
			JobContext jContext = new JobContextImpl(conf, taskID.GetJobID());
			TaskAttemptContext tContext = new TaskAttemptContextImpl(conf, taskID);
			FileOutputCommitter committer = new FileOutputCommitter(outDir, tContext);
			// setup
			committer.SetupJob(jContext);
			committer.SetupTask(tContext);
			// write output
			TextOutputFormat theOutputFormat = new TextOutputFormat();
			RecordWriter theRecordWriter = theOutputFormat.GetRecordWriter(tContext);
			WriteOutput(theRecordWriter, tContext);
			// do commit
			committer.CommitTask(tContext);
			committer.CommitJob(jContext);
			// validate output
			FilePath expectedFile = new FilePath(new Path(outDir, partFile).ToString());
			StringBuilder expectedOutput = new StringBuilder();
			expectedOutput.Append(key1).Append('\t').Append(val1).Append("\n");
			expectedOutput.Append(val1).Append("\n");
			expectedOutput.Append(val2).Append("\n");
			expectedOutput.Append(key2).Append("\n");
			expectedOutput.Append(key1).Append("\n");
			expectedOutput.Append(key2).Append('\t').Append(val2).Append("\n");
			string output = UtilsForTests.Slurp(expectedFile);
			NUnit.Framework.Assert.AreEqual(output, expectedOutput.ToString());
			FileUtil.FullyDelete(new FilePath(outDir.ToString()));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestEmptyOutput()
		{
			Job job = Job.GetInstance();
			FileOutputFormat.SetOutputPath(job, outDir);
			Configuration conf = job.GetConfiguration();
			conf.Set(MRJobConfig.TaskAttemptId, attempt);
			JobContext jContext = new JobContextImpl(conf, taskID.GetJobID());
			TaskAttemptContext tContext = new TaskAttemptContextImpl(conf, taskID);
			FileOutputCommitter committer = new FileOutputCommitter(outDir, tContext);
			// setup
			committer.SetupJob(jContext);
			committer.SetupTask(tContext);
			// Do not write any output
			// do commit
			committer.CommitTask(tContext);
			committer.CommitJob(jContext);
			FileUtil.FullyDelete(new FilePath(outDir.ToString()));
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void TestAbort()
		{
			Job job = Job.GetInstance();
			FileOutputFormat.SetOutputPath(job, outDir);
			Configuration conf = job.GetConfiguration();
			conf.Set(MRJobConfig.TaskAttemptId, attempt);
			JobContext jContext = new JobContextImpl(conf, taskID.GetJobID());
			TaskAttemptContext tContext = new TaskAttemptContextImpl(conf, taskID);
			FileOutputCommitter committer = new FileOutputCommitter(outDir, tContext);
			// do setup
			committer.SetupJob(jContext);
			committer.SetupTask(tContext);
			// write output
			TextOutputFormat theOutputFormat = new TextOutputFormat();
			RecordWriter theRecordWriter = theOutputFormat.GetRecordWriter(tContext);
			WriteOutput(theRecordWriter, tContext);
			// do abort
			committer.AbortTask(tContext);
			FilePath expectedFile = new FilePath(new Path(committer.GetWorkPath(), partFile).
				ToString());
			NUnit.Framework.Assert.IsFalse("task temp dir still exists", expectedFile.Exists(
				));
			committer.AbortJob(jContext, JobStatus.State.Failed);
			expectedFile = new FilePath(new Path(outDir, FileOutputCommitter.PendingDirName).
				ToString());
			NUnit.Framework.Assert.IsFalse("job temp dir still exists", expectedFile.Exists()
				);
			NUnit.Framework.Assert.AreEqual("Output directory not empty", 0, new FilePath(outDir
				.ToString()).ListFiles().Length);
			FileUtil.FullyDelete(new FilePath(outDir.ToString()));
		}

		public class FakeFileSystem : RawLocalFileSystem
		{
			public FakeFileSystem()
				: base()
			{
			}

			public override URI GetUri()
			{
				return URI.Create("faildel:///");
			}

			/// <exception cref="System.IO.IOException"/>
			public override bool Delete(Path p, bool recursive)
			{
				throw new IOException("fake delete failed");
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void TestFailAbort()
		{
			Job job = Job.GetInstance();
			Configuration conf = job.GetConfiguration();
			conf.Set(FileSystem.FsDefaultNameKey, "faildel:///");
			conf.SetClass("fs.faildel.impl", typeof(TestMRCJCFileOutputCommitter.FakeFileSystem
				), typeof(FileSystem));
			conf.Set(MRJobConfig.TaskAttemptId, attempt);
			FileOutputFormat.SetOutputPath(job, outDir);
			JobContext jContext = new JobContextImpl(conf, taskID.GetJobID());
			TaskAttemptContext tContext = new TaskAttemptContextImpl(conf, taskID);
			FileOutputCommitter committer = new FileOutputCommitter(outDir, tContext);
			// do setup
			committer.SetupJob(jContext);
			committer.SetupTask(tContext);
			// write output
			TextOutputFormat<object, object> theOutputFormat = new TextOutputFormat();
			RecordWriter<object, object> theRecordWriter = theOutputFormat.GetRecordWriter(tContext
				);
			WriteOutput(theRecordWriter, tContext);
			// do abort
			Exception th = null;
			try
			{
				committer.AbortTask(tContext);
			}
			catch (IOException ie)
			{
				th = ie;
			}
			NUnit.Framework.Assert.IsNotNull(th);
			NUnit.Framework.Assert.IsTrue(th is IOException);
			NUnit.Framework.Assert.IsTrue(th.Message.Contains("fake delete failed"));
			//Path taskBaseDirName = committer.getTaskAttemptBaseDirName(tContext);
			FilePath jobTmpDir = new FilePath(committer.GetJobAttemptPath(jContext).ToUri().GetPath
				());
			FilePath taskTmpDir = new FilePath(committer.GetTaskAttemptPath(tContext).ToUri()
				.GetPath());
			FilePath expectedFile = new FilePath(taskTmpDir, partFile);
			NUnit.Framework.Assert.IsTrue(expectedFile + " does not exists", expectedFile.Exists
				());
			th = null;
			try
			{
				committer.AbortJob(jContext, JobStatus.State.Failed);
			}
			catch (IOException ie)
			{
				th = ie;
			}
			NUnit.Framework.Assert.IsNotNull(th);
			NUnit.Framework.Assert.IsTrue(th is IOException);
			NUnit.Framework.Assert.IsTrue(th.Message.Contains("fake delete failed"));
			NUnit.Framework.Assert.IsTrue("job temp dir does not exists", jobTmpDir.Exists());
			FileUtil.FullyDelete(new FilePath(outDir.ToString()));
		}
	}
}
