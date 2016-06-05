using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public class TestTaskCommit : HadoopTestCase
	{
		internal Path rootDir = new Path(Runtime.GetProperty("test.build.data", "/tmp"), 
			"test");

		internal class CommitterWithCommitFail : FileOutputCommitter
		{
			/// <exception cref="System.IO.IOException"/>
			public override void CommitTask(TaskAttemptContext context)
			{
				Path taskOutputPath = GetTaskAttemptPath(context);
				TaskAttemptID attemptId = context.GetTaskAttemptID();
				JobConf job = context.GetJobConf();
				if (taskOutputPath != null)
				{
					FileSystem fs = taskOutputPath.GetFileSystem(job);
					if (fs.Exists(taskOutputPath))
					{
						throw new IOException();
					}
				}
			}
		}

		/// <summary>
		/// Special Committer that does not cleanup temporary files in
		/// abortTask
		/// The framework's FileOutputCommitter cleans up any temporary
		/// files left behind in abortTask.
		/// </summary>
		/// <remarks>
		/// Special Committer that does not cleanup temporary files in
		/// abortTask
		/// The framework's FileOutputCommitter cleans up any temporary
		/// files left behind in abortTask. We want the test case to
		/// find these files and hence short-circuit abortTask.
		/// </remarks>
		internal class CommitterWithoutCleanup : FileOutputCommitter
		{
			/// <exception cref="System.IO.IOException"/>
			public override void AbortTask(TaskAttemptContext context)
			{
			}
			// does nothing
		}

		/// <summary>Special committer that always requires commit.</summary>
		internal class CommitterThatAlwaysRequiresCommit : FileOutputCommitter
		{
			/// <exception cref="System.IO.IOException"/>
			public override bool NeedsTaskCommit(TaskAttemptContext context)
			{
				return true;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public TestTaskCommit()
			: base(LocalMr, LocalFs, 1, 1)
		{
		}

		/// <exception cref="System.Exception"/>
		protected override void TearDown()
		{
			base.TearDown();
			FileUtil.FullyDelete(new FilePath(rootDir.ToString()));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestCommitFail()
		{
			Path inDir = new Path(rootDir, "./input");
			Path outDir = new Path(rootDir, "./output");
			JobConf jobConf = CreateJobConf();
			jobConf.SetMaxMapAttempts(1);
			jobConf.SetOutputCommitter(typeof(TestTaskCommit.CommitterWithCommitFail));
			RunningJob rJob = UtilsForTests.RunJob(jobConf, inDir, outDir, 1, 0);
			rJob.WaitForCompletion();
			NUnit.Framework.Assert.AreEqual(JobStatus.Failed, rJob.GetJobState());
		}

		private class MyUmbilical : TaskUmbilicalProtocol
		{
			internal bool taskDone = false;

			/// <exception cref="System.IO.IOException"/>
			public override bool CanCommit(TaskAttemptID taskid)
			{
				return false;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public override void CommitPending(TaskAttemptID taskId, TaskStatus taskStatus)
			{
				TestCase.Fail("Task should not go to commit-pending");
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Done(TaskAttemptID taskid)
			{
				this.taskDone = true;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void FatalError(TaskAttemptID taskId, string message)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override void FsError(TaskAttemptID taskId, string message)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override MapTaskCompletionEventsUpdate GetMapCompletionEvents(JobID jobId, 
				int fromIndex, int maxLocs, TaskAttemptID id)
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public override JvmTask GetTask(JvmContext context)
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public override bool Ping(TaskAttemptID taskid)
			{
				return true;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void ReportDiagnosticInfo(TaskAttemptID taskid, string trace)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override void ReportNextRecordRange(TaskAttemptID taskid, SortedRanges.Range
				 range)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override void ShuffleError(TaskAttemptID taskId, string message)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public override bool StatusUpdate(TaskAttemptID taskId, TaskStatus taskStatus)
			{
				return true;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual long GetProtocolVersion(string protocol, long clientVersion)
			{
				return 0;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual ProtocolSignature GetProtocolSignature(string protocol, long clientVersion
				, int clientMethodsHash)
			{
				return null;
			}

			internal MyUmbilical(TestTaskCommit _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestTaskCommit _enclosing;
		}

		/// <summary>
		/// A test that mimics a failed task to ensure that it does
		/// not get into the COMMIT_PENDING state, by using a fake
		/// UmbilicalProtocol's implementation that fails if the commit.
		/// </summary>
		/// <remarks>
		/// A test that mimics a failed task to ensure that it does
		/// not get into the COMMIT_PENDING state, by using a fake
		/// UmbilicalProtocol's implementation that fails if the commit.
		/// protocol is played.
		/// The test mocks the various steps in a failed task's
		/// life-cycle using a special OutputCommitter and UmbilicalProtocol
		/// implementation.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestTaskCleanupDoesNotCommit()
		{
			// Mimic a job with a special committer that does not cleanup
			// files when a task fails.
			JobConf job = new JobConf();
			job.SetOutputCommitter(typeof(TestTaskCommit.CommitterWithoutCleanup));
			Path outDir = new Path(rootDir, "output");
			FileOutputFormat.SetOutputPath(job, outDir);
			// Mimic job setup
			string dummyAttemptID = "attempt_200707121733_0001_m_000000_0";
			TaskAttemptID attemptID = ((TaskAttemptID)TaskAttemptID.ForName(dummyAttemptID));
			OutputCommitter committer = new TestTaskCommit.CommitterWithoutCleanup();
			JobContext jContext = new JobContextImpl(job, ((JobID)attemptID.GetJobID()));
			committer.SetupJob(jContext);
			// Mimic a map task
			dummyAttemptID = "attempt_200707121733_0001_m_000001_0";
			attemptID = ((TaskAttemptID)TaskAttemptID.ForName(dummyAttemptID));
			Task task = new MapTask(null, attemptID, 0, null, 1);
			task.SetConf(job);
			task.LocalizeConfiguration(job);
			task.Initialize(job, ((JobID)attemptID.GetJobID()), Reporter.Null, false);
			// Mimic the map task writing some output.
			string file = "test.txt";
			FileSystem localFs = FileSystem.GetLocal(job);
			TextOutputFormat<Text, Text> theOutputFormat = new TextOutputFormat<Text, Text>();
			RecordWriter<Text, Text> theRecordWriter = theOutputFormat.GetRecordWriter(localFs
				, job, file, Reporter.Null);
			theRecordWriter.Write(new Text("key"), new Text("value"));
			theRecordWriter.Close(Reporter.Null);
			// Mimic a task failure; setting up the task for cleanup simulates
			// the abort protocol to be played.
			// Without checks in the framework, this will fail
			// as the committer will cause a COMMIT to happen for
			// the cleanup task.
			task.SetTaskCleanupTask();
			TestTaskCommit.MyUmbilical umbilical = new TestTaskCommit.MyUmbilical(this);
			task.Run(job, umbilical);
			NUnit.Framework.Assert.IsTrue("Task did not succeed", umbilical.taskDone);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestCommitRequiredForMapTask()
		{
			Task testTask = CreateDummyTask(TaskType.Map);
			NUnit.Framework.Assert.IsTrue("MapTask should need commit", testTask.IsCommitRequired
				());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestCommitRequiredForReduceTask()
		{
			Task testTask = CreateDummyTask(TaskType.Reduce);
			NUnit.Framework.Assert.IsTrue("ReduceTask should need commit", testTask.IsCommitRequired
				());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestCommitNotRequiredForJobSetup()
		{
			Task testTask = CreateDummyTask(TaskType.Map);
			testTask.SetJobSetupTask();
			NUnit.Framework.Assert.IsFalse("Job setup task should not need commit", testTask.
				IsCommitRequired());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestCommitNotRequiredForJobCleanup()
		{
			Task testTask = CreateDummyTask(TaskType.Map);
			testTask.SetJobCleanupTask();
			NUnit.Framework.Assert.IsFalse("Job cleanup task should not need commit", testTask
				.IsCommitRequired());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestCommitNotRequiredForTaskCleanup()
		{
			Task testTask = CreateDummyTask(TaskType.Reduce);
			testTask.SetTaskCleanupTask();
			NUnit.Framework.Assert.IsFalse("Task cleanup task should not need commit", testTask
				.IsCommitRequired());
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.TypeLoadException"/>
		/// <exception cref="System.Exception"/>
		private Task CreateDummyTask(TaskType type)
		{
			JobConf conf = new JobConf();
			conf.SetOutputCommitter(typeof(TestTaskCommit.CommitterThatAlwaysRequiresCommit));
			Path outDir = new Path(rootDir, "output");
			FileOutputFormat.SetOutputPath(conf, outDir);
			JobID jobId = ((JobID)JobID.ForName("job_201002121132_0001"));
			Task testTask;
			if (type == TaskType.Map)
			{
				testTask = new MapTask();
			}
			else
			{
				testTask = new ReduceTask();
			}
			testTask.SetConf(conf);
			testTask.Initialize(conf, jobId, Reporter.Null, false);
			return testTask;
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] argv)
		{
			TestTaskCommit td = new TestTaskCommit();
			td.TestCommitFail();
		}
	}
}
