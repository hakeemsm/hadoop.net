using System;
using System.IO;
using System.Text;
using NUnit.Framework;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public class TestMRCJCFileOutputCommitter : TestCase
	{
		private static Path outDir = new Path(Runtime.GetProperty("test.build.data", "/tmp"
			), "output");

		private static string attempt = "attempt_200707121733_0001_m_000000_0";

		private static TaskAttemptID taskID = ((TaskAttemptID)TaskAttemptID.ForName(attempt
			));

		private Text key1 = new Text("key1");

		private Text key2 = new Text("key2");

		private Text val1 = new Text("val1");

		private Text val2 = new Text("val2");

		// A random task attempt id for testing.
		/// <exception cref="System.IO.IOException"/>
		private void WriteOutput(RecordWriter theRecordWriter, Reporter reporter)
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
				theRecordWriter.Close(reporter);
			}
		}

		private void SetConfForFileOutputCommitter(JobConf job)
		{
			job.Set(JobContext.TaskAttemptId, attempt);
			job.SetOutputCommitter(typeof(FileOutputCommitter));
			FileOutputFormat.SetOutputPath(job, outDir);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestCommitter()
		{
			JobConf job = new JobConf();
			SetConfForFileOutputCommitter(job);
			JobContext jContext = new JobContextImpl(job, ((JobID)taskID.GetJobID()));
			TaskAttemptContext tContext = new TaskAttemptContextImpl(job, taskID);
			FileOutputCommitter committer = new FileOutputCommitter();
			FileOutputFormat.SetWorkOutputPath(job, committer.GetTaskAttemptPath(tContext));
			committer.SetupJob(jContext);
			committer.SetupTask(tContext);
			string file = "test.txt";
			// A reporter that does nothing
			Reporter reporter = Reporter.Null;
			// write output
			FileSystem localFs = FileSystem.GetLocal(job);
			TextOutputFormat theOutputFormat = new TextOutputFormat();
			RecordWriter theRecordWriter = theOutputFormat.GetRecordWriter(localFs, job, file
				, reporter);
			WriteOutput(theRecordWriter, reporter);
			// do commit
			committer.CommitTask(tContext);
			committer.CommitJob(jContext);
			// validate output
			FilePath expectedFile = new FilePath(new Path(outDir, file).ToString());
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

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestAbort()
		{
			JobConf job = new JobConf();
			SetConfForFileOutputCommitter(job);
			JobContext jContext = new JobContextImpl(job, ((JobID)taskID.GetJobID()));
			TaskAttemptContext tContext = new TaskAttemptContextImpl(job, taskID);
			FileOutputCommitter committer = new FileOutputCommitter();
			FileOutputFormat.SetWorkOutputPath(job, committer.GetTaskAttemptPath(tContext));
			// do setup
			committer.SetupJob(jContext);
			committer.SetupTask(tContext);
			string file = "test.txt";
			// A reporter that does nothing
			Reporter reporter = Reporter.Null;
			// write output
			FileSystem localFs = FileSystem.GetLocal(job);
			TextOutputFormat theOutputFormat = new TextOutputFormat();
			RecordWriter theRecordWriter = theOutputFormat.GetRecordWriter(localFs, job, file
				, reporter);
			WriteOutput(theRecordWriter, reporter);
			// do abort
			committer.AbortTask(tContext);
			FilePath expectedFile = new FilePath(new Path(committer.GetTaskAttemptPath(tContext
				), file).ToString());
			NUnit.Framework.Assert.IsFalse("task temp dir still exists", expectedFile.Exists(
				));
			committer.AbortJob(jContext, JobStatus.State.Failed);
			expectedFile = new FilePath(new Path(outDir, FileOutputCommitter.TempDirName).ToString
				());
			NUnit.Framework.Assert.IsFalse("job temp dir " + expectedFile + " still exists", 
				expectedFile.Exists());
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
		public virtual void TestFailAbort()
		{
			JobConf job = new JobConf();
			job.Set(FileSystem.FsDefaultNameKey, "faildel:///");
			job.SetClass("fs.faildel.impl", typeof(TestMRCJCFileOutputCommitter.FakeFileSystem
				), typeof(FileSystem));
			SetConfForFileOutputCommitter(job);
			JobContext jContext = new JobContextImpl(job, ((JobID)taskID.GetJobID()));
			TaskAttemptContext tContext = new TaskAttemptContextImpl(job, taskID);
			FileOutputCommitter committer = new FileOutputCommitter();
			FileOutputFormat.SetWorkOutputPath(job, committer.GetTaskAttemptPath(tContext));
			// do setup
			committer.SetupJob(jContext);
			committer.SetupTask(tContext);
			string file = "test.txt";
			FilePath jobTmpDir = new FilePath(committer.GetJobAttemptPath(jContext).ToUri().GetPath
				());
			FilePath taskTmpDir = new FilePath(committer.GetTaskAttemptPath(tContext).ToUri()
				.GetPath());
			FilePath expectedFile = new FilePath(taskTmpDir, file);
			// A reporter that does nothing
			Reporter reporter = Reporter.Null;
			// write output
			FileSystem localFs = new TestMRCJCFileOutputCommitter.FakeFileSystem();
			TextOutputFormat theOutputFormat = new TextOutputFormat();
			RecordWriter theRecordWriter = theOutputFormat.GetRecordWriter(localFs, job, expectedFile
				.GetAbsolutePath(), reporter);
			WriteOutput(theRecordWriter, reporter);
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
		}
	}
}
