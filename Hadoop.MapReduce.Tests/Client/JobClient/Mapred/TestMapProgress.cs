using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Lib.Input;
using Org.Apache.Hadoop.Mapreduce.Lib.Output;
using Org.Apache.Hadoop.Mapreduce.Server.Jobtracker;
using Org.Apache.Hadoop.Mapreduce.Split;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>Validates map phase progress.</summary>
	/// <remarks>
	/// Validates map phase progress.
	/// Testcase uses newApi.
	/// We extend Task.TaskReporter class and override setProgress()
	/// to validate the map phase progress being set.
	/// We extend MapTask and override startReporter() method that creates
	/// TestTaskReporter instead of TaskReporter and call mapTask.run().
	/// Similar to LocalJobRunner, we set up splits and call mapTask.run()
	/// directly. No job is run, only map task is run.
	/// As the reporter's setProgress() validates progress after
	/// every record is read, we are done with the validation of map phase progress
	/// once mapTask.run() is finished. Sort phase progress in map task is not
	/// validated here.
	/// </remarks>
	public class TestMapProgress : TestCase
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapred.TestMapProgress
			));

		private static string TestRootDir;

		static TestMapProgress()
		{
			string root = new FilePath(Runtime.GetProperty("test.build.data", "/tmp")).GetAbsolutePath
				();
			TestRootDir = new Path(root, "mapPhaseprogress").ToString();
		}

		internal class FakeUmbilical : TaskUmbilicalProtocol
		{
			public virtual long GetProtocolVersion(string protocol, long clientVersion)
			{
				return TaskUmbilicalProtocol.versionID;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual ProtocolSignature GetProtocolSignature(string protocol, long clientVersion
				, int clientMethodsHash)
			{
				return ProtocolSignature.GetProtocolSignature(this, protocol, clientVersion, clientMethodsHash
					);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Done(TaskAttemptID taskid)
			{
				Log.Info("Task " + taskid + " reporting done.");
			}

			/// <exception cref="System.IO.IOException"/>
			public override void FsError(TaskAttemptID taskId, string message)
			{
				Log.Info("Task " + taskId + " reporting file system error: " + message);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void ShuffleError(TaskAttemptID taskId, string message)
			{
				Log.Info("Task " + taskId + " reporting shuffle error: " + message);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void FatalError(TaskAttemptID taskId, string msg)
			{
				Log.Info("Task " + taskId + " reporting fatal error: " + msg);
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
			/// <exception cref="System.Exception"/>
			public override void CommitPending(TaskAttemptID taskId, TaskStatus taskStatus)
			{
				StatusUpdate(taskId, taskStatus);
			}

			/// <exception cref="System.IO.IOException"/>
			public override bool CanCommit(TaskAttemptID taskid)
			{
				return true;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public override bool StatusUpdate(TaskAttemptID taskId, TaskStatus taskStatus)
			{
				StringBuilder buf = new StringBuilder("Task ");
				buf.Append(taskId);
				if (taskStatus != null)
				{
					buf.Append(" making progress to ");
					buf.Append(taskStatus.GetProgress());
					string state = taskStatus.GetStateString();
					if (state != null)
					{
						buf.Append(" and state of ");
						buf.Append(state);
					}
				}
				Log.Info(buf.ToString());
				// ignore phase
				// ignore counters
				return true;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void ReportDiagnosticInfo(TaskAttemptID taskid, string trace)
			{
				Log.Info("Task " + taskid + " has problem " + trace);
			}

			/// <exception cref="System.IO.IOException"/>
			public override MapTaskCompletionEventsUpdate GetMapCompletionEvents(JobID jobId, 
				int fromEventId, int maxLocs, TaskAttemptID id)
			{
				return new MapTaskCompletionEventsUpdate(TaskCompletionEvent.EmptyArray, false);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void ReportNextRecordRange(TaskAttemptID taskid, SortedRanges.Range
				 range)
			{
				Log.Info("Task " + taskid + " reportedNextRecordRange " + range);
			}
		}

		private FileSystem fs = null;

		private TestMapProgress.TestMapTask map = null;

		private JobID jobId = null;

		private TestMapProgress.FakeUmbilical fakeUmbilical = new TestMapProgress.FakeUmbilical
			();

		/// <summary>
		/// Task Reporter that validates map phase progress after each record is
		/// processed by map task
		/// </summary>
		public class TestTaskReporter : Task.TaskReporter
		{
			private int recordNum = 0;

			internal TestTaskReporter(TestMapProgress _enclosing, Task task)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			// number of records processed
			public override void SetProgress(float progress)
			{
				base.SetProgress(progress);
				float mapTaskProgress = this._enclosing.map.GetProgress().GetProgress();
				TestMapProgress.Log.Info("Map task progress is " + mapTaskProgress);
				if (this.recordNum < 3)
				{
					// only 3 records are there; Ignore validating progress after 3 times
					this.recordNum++;
				}
				else
				{
					return;
				}
				// validate map task progress when the map task is in map phase
				NUnit.Framework.Assert.IsTrue("Map progress is not the expected value.", Math.Abs
					(mapTaskProgress - ((float)this.recordNum / 3)) < 0.001);
			}

			private readonly TestMapProgress _enclosing;
		}

		/// <summary>
		/// Map Task that overrides run method and uses TestTaskReporter instead of
		/// TaskReporter and uses FakeUmbilical.
		/// </summary>
		internal class TestMapTask : MapTask
		{
			public TestMapTask(TestMapProgress _enclosing, string jobFile, TaskAttemptID taskId
				, int partition, JobSplit.TaskSplitIndex splitIndex, int numSlotsRequired)
				: base(jobFile, taskId, partition, splitIndex, numSlotsRequired)
			{
				this._enclosing = _enclosing;
			}

			/// <summary>Create a TestTaskReporter and use it for validating map phase progress</summary>
			internal override Task.TaskReporter StartReporter(TaskUmbilicalProtocol umbilical
				)
			{
				// start thread that will handle communication with parent
				Task.TaskReporter reporter = new TestMapProgress.TestTaskReporter(this, this._enclosing
					.map);
				return reporter;
			}

			private readonly TestMapProgress _enclosing;
		}

		// In the given dir, creates part-0 file with 3 records of same size
		/// <exception cref="System.IO.IOException"/>
		private void CreateInputFile(Path rootDir)
		{
			if (fs.Exists(rootDir))
			{
				fs.Delete(rootDir, true);
			}
			string str = "The quick brown fox\n" + "The brown quick fox\n" + "The fox brown quick\n";
			DataOutputStream inpFile = fs.Create(new Path(rootDir, "part-0"));
			inpFile.WriteBytes(str);
			inpFile.Close();
		}

		/// <summary>
		/// Validates map phase progress after each record is processed by map task
		/// using custom task reporter.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestMapProgress()
		{
			JobConf job = new JobConf();
			fs = FileSystem.GetLocal(job);
			Path rootDir = new Path(TestRootDir);
			CreateInputFile(rootDir);
			job.SetNumReduceTasks(0);
			TaskAttemptID taskId = ((TaskAttemptID)TaskAttemptID.ForName("attempt_200907082313_0424_m_000000_0"
				));
			job.SetClass("mapreduce.job.outputformat.class", typeof(NullOutputFormat), typeof(
				OutputFormat));
			job.Set(FileInputFormat.InputDir, TestRootDir);
			jobId = ((JobID)taskId.GetJobID());
			JobContext jContext = new JobContextImpl(job, jobId);
			InputFormat<object, object> input = ReflectionUtils.NewInstance(jContext.GetInputFormatClass
				(), job);
			IList<InputSplit> splits = input.GetSplits(jContext);
			JobSplitWriter.CreateSplitFiles(new Path(TestRootDir), job, new Path(TestRootDir)
				.GetFileSystem(job), splits);
			JobSplit.TaskSplitMetaInfo[] splitMetaInfo = SplitMetaInfoReader.ReadSplitMetaInfo
				(jobId, fs, job, new Path(TestRootDir));
			job.SetUseNewMapper(true);
			// use new api    
			for (int i = 0; i < splitMetaInfo.Length; i++)
			{
				// rawSplits.length is 1
				map = new TestMapProgress.TestMapTask(this, job.Get(JTConfig.JtSystemDir, "/tmp/hadoop/mapred/system"
					) + jobId + "job.xml", taskId, i, splitMetaInfo[i].GetSplitIndex(), 1);
				JobConf localConf = new JobConf(job);
				map.LocalizeConfiguration(localConf);
				map.SetConf(localConf);
				map.Run(localConf, fakeUmbilical);
			}
			// clean up
			fs.Delete(rootDir, true);
		}
	}
}
