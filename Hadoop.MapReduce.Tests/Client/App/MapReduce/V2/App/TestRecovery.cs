using System.Collections.Generic;
using System.IO;
using System.Text;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Jobhistory;
using Org.Apache.Hadoop.Mapreduce.Lib.Output;
using Org.Apache.Hadoop.Mapreduce.Security.Token;
using Org.Apache.Hadoop.Mapreduce.Split;
using Org.Apache.Hadoop.Mapreduce.Task;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job.Event;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job.Impl;
using Org.Apache.Hadoop.Mapreduce.V2.App.Launcher;
using Org.Apache.Hadoop.Mapreduce.V2.App.Metrics;
using Org.Apache.Hadoop.Mapreduce.V2.Util;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Mockito;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App
{
	public class TestRecovery
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestRecovery));

		private static Path outputDir = new Path(new FilePath("target", typeof(TestRecovery
			).FullName).GetAbsolutePath() + Path.Separator + "out");

		private static string partFile = "part-r-00000";

		private Text key1 = new Text("key1");

		private Text key2 = new Text("key2");

		private Text val1 = new Text("val1");

		private Text val2 = new Text("val2");

		/// <summary>AM with 2 maps and 1 reduce.</summary>
		/// <remarks>
		/// AM with 2 maps and 1 reduce. For 1st map, one attempt fails, one attempt
		/// completely disappears because of failed launch, one attempt gets killed and
		/// one attempt succeeds. AM crashes after the first tasks finishes and
		/// recovers completely and succeeds in the second generation.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCrashed()
		{
			int runCount = 0;
			long am1StartTimeEst = Runtime.CurrentTimeMillis();
			MRApp app = new TestRecovery.MRAppWithHistory(2, 1, false, this.GetType().FullName
				, true, ++runCount);
			Configuration conf = new Configuration();
			conf.SetBoolean("mapred.mapper.new-api", true);
			conf.SetBoolean("mapred.reducer.new-api", true);
			conf.SetBoolean(MRJobConfig.JobUbertaskEnable, false);
			conf.Set(FileOutputFormat.Outdir, outputDir.ToString());
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = app.Submit(conf);
			app.WaitForState(job, JobState.Running);
			long jobStartTime = job.GetReport().GetStartTime();
			//all maps would be running
			NUnit.Framework.Assert.AreEqual("No of tasks not correct", 3, job.GetTasks().Count
				);
			IEnumerator<Task> it = job.GetTasks().Values.GetEnumerator();
			Task mapTask1 = it.Next();
			Task mapTask2 = it.Next();
			Task reduceTask = it.Next();
			// all maps must be running
			app.WaitForState(mapTask1, TaskState.Running);
			app.WaitForState(mapTask2, TaskState.Running);
			TaskAttempt task1Attempt1 = mapTask1.GetAttempts().Values.GetEnumerator().Next();
			TaskAttempt task2Attempt = mapTask2.GetAttempts().Values.GetEnumerator().Next();
			//before sending the TA_DONE, event make sure attempt has come to 
			//RUNNING state
			app.WaitForState(task1Attempt1, TaskAttemptState.Running);
			app.WaitForState(task2Attempt, TaskAttemptState.Running);
			// reduces must be in NEW state
			NUnit.Framework.Assert.AreEqual("Reduce Task state not correct", TaskState.Running
				, reduceTask.GetReport().GetTaskState());
			/////////// Play some games with the TaskAttempts of the first task //////
			//send the fail signal to the 1st map task attempt
			app.GetContext().GetEventHandler().Handle(new TaskAttemptEvent(task1Attempt1.GetID
				(), TaskAttemptEventType.TaFailmsg));
			app.WaitForState(task1Attempt1, TaskAttemptState.Failed);
			int timeOut = 0;
			while (mapTask1.GetAttempts().Count != 2 && timeOut++ < 10)
			{
				Sharpen.Thread.Sleep(2000);
				Log.Info("Waiting for next attempt to start");
			}
			NUnit.Framework.Assert.AreEqual(2, mapTask1.GetAttempts().Count);
			IEnumerator<TaskAttempt> itr = mapTask1.GetAttempts().Values.GetEnumerator();
			itr.Next();
			TaskAttempt task1Attempt2 = itr.Next();
			// This attempt will automatically fail because of the way ContainerLauncher
			// is setup
			// This attempt 'disappears' from JobHistory and so causes MAPREDUCE-3846
			app.GetContext().GetEventHandler().Handle(new TaskAttemptEvent(task1Attempt2.GetID
				(), TaskAttemptEventType.TaContainerLaunchFailed));
			app.WaitForState(task1Attempt2, TaskAttemptState.Failed);
			timeOut = 0;
			while (mapTask1.GetAttempts().Count != 3 && timeOut++ < 10)
			{
				Sharpen.Thread.Sleep(2000);
				Log.Info("Waiting for next attempt to start");
			}
			NUnit.Framework.Assert.AreEqual(3, mapTask1.GetAttempts().Count);
			itr = mapTask1.GetAttempts().Values.GetEnumerator();
			itr.Next();
			itr.Next();
			TaskAttempt task1Attempt3 = itr.Next();
			app.WaitForState(task1Attempt3, TaskAttemptState.Running);
			//send the kill signal to the 1st map 3rd attempt
			app.GetContext().GetEventHandler().Handle(new TaskAttemptEvent(task1Attempt3.GetID
				(), TaskAttemptEventType.TaKill));
			app.WaitForState(task1Attempt3, TaskAttemptState.Killed);
			timeOut = 0;
			while (mapTask1.GetAttempts().Count != 4 && timeOut++ < 10)
			{
				Sharpen.Thread.Sleep(2000);
				Log.Info("Waiting for next attempt to start");
			}
			NUnit.Framework.Assert.AreEqual(4, mapTask1.GetAttempts().Count);
			itr = mapTask1.GetAttempts().Values.GetEnumerator();
			itr.Next();
			itr.Next();
			itr.Next();
			TaskAttempt task1Attempt4 = itr.Next();
			app.WaitForState(task1Attempt4, TaskAttemptState.Running);
			//send the done signal to the 1st map 4th attempt
			app.GetContext().GetEventHandler().Handle(new TaskAttemptEvent(task1Attempt4.GetID
				(), TaskAttemptEventType.TaDone));
			/////////// End of games with the TaskAttempts of the first task //////
			//wait for first map task to complete
			app.WaitForState(mapTask1, TaskState.Succeeded);
			long task1StartTime = mapTask1.GetReport().GetStartTime();
			long task1FinishTime = mapTask1.GetReport().GetFinishTime();
			//stop the app
			app.Stop();
			//rerun
			//in rerun the 1st map will be recovered from previous run
			long am2StartTimeEst = Runtime.CurrentTimeMillis();
			app = new TestRecovery.MRAppWithHistory(2, 1, false, this.GetType().FullName, false
				, ++runCount);
			conf = new Configuration();
			conf.SetBoolean(MRJobConfig.MrAmJobRecoveryEnable, true);
			conf.SetBoolean("mapred.mapper.new-api", true);
			conf.SetBoolean("mapred.reducer.new-api", true);
			conf.Set(FileOutputFormat.Outdir, outputDir.ToString());
			conf.SetBoolean(MRJobConfig.JobUbertaskEnable, false);
			job = app.Submit(conf);
			app.WaitForState(job, JobState.Running);
			//all maps would be running
			NUnit.Framework.Assert.AreEqual("No of tasks not correct", 3, job.GetTasks().Count
				);
			it = job.GetTasks().Values.GetEnumerator();
			mapTask1 = it.Next();
			mapTask2 = it.Next();
			reduceTask = it.Next();
			// first map will be recovered, no need to send done
			app.WaitForState(mapTask1, TaskState.Succeeded);
			app.WaitForState(mapTask2, TaskState.Running);
			task2Attempt = mapTask2.GetAttempts().Values.GetEnumerator().Next();
			//before sending the TA_DONE, event make sure attempt has come to 
			//RUNNING state
			app.WaitForState(task2Attempt, TaskAttemptState.Running);
			//send the done signal to the 2nd map task
			app.GetContext().GetEventHandler().Handle(new TaskAttemptEvent(mapTask2.GetAttempts
				().Values.GetEnumerator().Next().GetID(), TaskAttemptEventType.TaDone));
			//wait to get it completed
			app.WaitForState(mapTask2, TaskState.Succeeded);
			//wait for reduce to be running before sending done
			app.WaitForState(reduceTask, TaskState.Running);
			//send the done signal to the reduce
			app.GetContext().GetEventHandler().Handle(new TaskAttemptEvent(reduceTask.GetAttempts
				().Values.GetEnumerator().Next().GetID(), TaskAttemptEventType.TaDone));
			app.WaitForState(job, JobState.Succeeded);
			app.VerifyCompleted();
			NUnit.Framework.Assert.AreEqual("Job Start time not correct", jobStartTime, job.GetReport
				().GetStartTime());
			NUnit.Framework.Assert.AreEqual("Task Start time not correct", task1StartTime, mapTask1
				.GetReport().GetStartTime());
			NUnit.Framework.Assert.AreEqual("Task Finish time not correct", task1FinishTime, 
				mapTask1.GetReport().GetFinishTime());
			NUnit.Framework.Assert.AreEqual(2, job.GetAMInfos().Count);
			int attemptNum = 1;
			// Verify AMInfo
			foreach (AMInfo amInfo in job.GetAMInfos())
			{
				NUnit.Framework.Assert.AreEqual(attemptNum++, amInfo.GetAppAttemptId().GetAttemptId
					());
				NUnit.Framework.Assert.AreEqual(amInfo.GetAppAttemptId(), amInfo.GetContainerId()
					.GetApplicationAttemptId());
				NUnit.Framework.Assert.AreEqual(MRApp.NmHost, amInfo.GetNodeManagerHost());
				NUnit.Framework.Assert.AreEqual(MRApp.NmPort, amInfo.GetNodeManagerPort());
				NUnit.Framework.Assert.AreEqual(MRApp.NmHttpPort, amInfo.GetNodeManagerHttpPort()
					);
			}
			long am1StartTimeReal = job.GetAMInfos()[0].GetStartTime();
			long am2StartTimeReal = job.GetAMInfos()[1].GetStartTime();
			NUnit.Framework.Assert.IsTrue(am1StartTimeReal >= am1StartTimeEst && am1StartTimeReal
				 <= am2StartTimeEst);
			NUnit.Framework.Assert.IsTrue(am2StartTimeReal >= am2StartTimeEst && am2StartTimeReal
				 <= Runtime.CurrentTimeMillis());
		}

		// TODO Add verification of additional data from jobHistory - whatever was
		// available in the failed attempt should be available here
		/// <summary>AM with 3 maps and 0 reduce.</summary>
		/// <remarks>
		/// AM with 3 maps and 0 reduce. AM crashes after the first two tasks finishes
		/// and recovers completely and succeeds in the second generation.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCrashOfMapsOnlyJob()
		{
			int runCount = 0;
			MRApp app = new TestRecovery.MRAppWithHistory(3, 0, false, this.GetType().FullName
				, true, ++runCount);
			Configuration conf = new Configuration();
			conf.SetBoolean("mapred.mapper.new-api", true);
			conf.SetBoolean(MRJobConfig.JobUbertaskEnable, false);
			conf.Set(FileOutputFormat.Outdir, outputDir.ToString());
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = app.Submit(conf);
			app.WaitForState(job, JobState.Running);
			// all maps would be running
			NUnit.Framework.Assert.AreEqual("No of tasks not correct", 3, job.GetTasks().Count
				);
			IEnumerator<Task> it = job.GetTasks().Values.GetEnumerator();
			Task mapTask1 = it.Next();
			Task mapTask2 = it.Next();
			Task mapTask3 = it.Next();
			// all maps must be running
			app.WaitForState(mapTask1, TaskState.Running);
			app.WaitForState(mapTask2, TaskState.Running);
			app.WaitForState(mapTask3, TaskState.Running);
			TaskAttempt task1Attempt = mapTask1.GetAttempts().Values.GetEnumerator().Next();
			TaskAttempt task2Attempt = mapTask2.GetAttempts().Values.GetEnumerator().Next();
			TaskAttempt task3Attempt = mapTask3.GetAttempts().Values.GetEnumerator().Next();
			// before sending the TA_DONE, event make sure attempt has come to
			// RUNNING state
			app.WaitForState(task1Attempt, TaskAttemptState.Running);
			app.WaitForState(task2Attempt, TaskAttemptState.Running);
			app.WaitForState(task3Attempt, TaskAttemptState.Running);
			// send the done signal to the 1st two maps
			app.GetContext().GetEventHandler().Handle(new TaskAttemptEvent(task1Attempt.GetID
				(), TaskAttemptEventType.TaDone));
			app.GetContext().GetEventHandler().Handle(new TaskAttemptEvent(task2Attempt.GetID
				(), TaskAttemptEventType.TaDone));
			// wait for first two map task to complete
			app.WaitForState(mapTask1, TaskState.Succeeded);
			app.WaitForState(mapTask2, TaskState.Succeeded);
			// stop the app
			app.Stop();
			// rerun
			// in rerun the 1st two map will be recovered from previous run
			app = new TestRecovery.MRAppWithHistory(2, 1, false, this.GetType().FullName, false
				, ++runCount);
			conf = new Configuration();
			conf.SetBoolean(MRJobConfig.MrAmJobRecoveryEnable, true);
			conf.SetBoolean("mapred.mapper.new-api", true);
			conf.Set(FileOutputFormat.Outdir, outputDir.ToString());
			// Set num-reduces explicitly in conf as recovery logic depends on it.
			conf.SetInt(MRJobConfig.NumReduces, 0);
			conf.SetBoolean(MRJobConfig.JobUbertaskEnable, false);
			job = app.Submit(conf);
			app.WaitForState(job, JobState.Running);
			NUnit.Framework.Assert.AreEqual("No of tasks not correct", 3, job.GetTasks().Count
				);
			it = job.GetTasks().Values.GetEnumerator();
			mapTask1 = it.Next();
			mapTask2 = it.Next();
			mapTask3 = it.Next();
			// first two maps will be recovered, no need to send done
			app.WaitForState(mapTask1, TaskState.Succeeded);
			app.WaitForState(mapTask2, TaskState.Succeeded);
			app.WaitForState(mapTask3, TaskState.Running);
			task3Attempt = mapTask3.GetAttempts().Values.GetEnumerator().Next();
			// before sending the TA_DONE, event make sure attempt has come to
			// RUNNING state
			app.WaitForState(task3Attempt, TaskAttemptState.Running);
			// send the done signal to the 3rd map task
			app.GetContext().GetEventHandler().Handle(new TaskAttemptEvent(mapTask3.GetAttempts
				().Values.GetEnumerator().Next().GetID(), TaskAttemptEventType.TaDone));
			// wait to get it completed
			app.WaitForState(mapTask3, TaskState.Succeeded);
			app.WaitForState(job, JobState.Succeeded);
			app.VerifyCompleted();
		}

		/// <summary>
		/// The class provides a custom implementation of output committer setupTask
		/// and isRecoverySupported methods, which determines if recovery supported
		/// based on config property.
		/// </summary>
		public class TestFileOutputCommitter : FileOutputCommitter
		{
			public override bool IsRecoverySupported(JobContext jobContext)
			{
				bool isRecoverySupported = false;
				if (jobContext != null && jobContext.GetConfiguration() != null)
				{
					isRecoverySupported = jobContext.GetConfiguration().GetBoolean("want.am.recovery"
						, false);
				}
				return isRecoverySupported;
			}
		}

		/// <summary>
		/// This test case primarily verifies if the recovery is controlled through config
		/// property.
		/// </summary>
		/// <remarks>
		/// This test case primarily verifies if the recovery is controlled through config
		/// property. In this case, recover is turned ON. AM with 3 maps and 0 reduce.
		/// AM crashes after the first two tasks finishes and recovers completely and
		/// succeeds in the second generation.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRecoverySuccessUsingCustomOutputCommitter()
		{
			int runCount = 0;
			MRApp app = new TestRecovery.MRAppWithHistory(3, 0, false, this.GetType().FullName
				, true, ++runCount);
			Configuration conf = new Configuration();
			conf.SetClass("mapred.output.committer.class", typeof(TestRecovery.TestFileOutputCommitter
				), typeof(OutputCommitter));
			conf.Set(FileOutputFormat.Outdir, outputDir.ToString());
			conf.SetBoolean("want.am.recovery", true);
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = app.Submit(conf);
			app.WaitForState(job, JobState.Running);
			// all maps would be running
			NUnit.Framework.Assert.AreEqual("No of tasks not correct", 3, job.GetTasks().Count
				);
			IEnumerator<Task> it = job.GetTasks().Values.GetEnumerator();
			Task mapTask1 = it.Next();
			Task mapTask2 = it.Next();
			Task mapTask3 = it.Next();
			// all maps must be running
			app.WaitForState(mapTask1, TaskState.Running);
			app.WaitForState(mapTask2, TaskState.Running);
			app.WaitForState(mapTask3, TaskState.Running);
			TaskAttempt task1Attempt = mapTask1.GetAttempts().Values.GetEnumerator().Next();
			TaskAttempt task2Attempt = mapTask2.GetAttempts().Values.GetEnumerator().Next();
			TaskAttempt task3Attempt = mapTask3.GetAttempts().Values.GetEnumerator().Next();
			// before sending the TA_DONE, event make sure attempt has come to
			// RUNNING state
			app.WaitForState(task1Attempt, TaskAttemptState.Running);
			app.WaitForState(task2Attempt, TaskAttemptState.Running);
			app.WaitForState(task3Attempt, TaskAttemptState.Running);
			// send the done signal to the 1st two maps
			app.GetContext().GetEventHandler().Handle(new TaskAttemptEvent(task1Attempt.GetID
				(), TaskAttemptEventType.TaDone));
			app.GetContext().GetEventHandler().Handle(new TaskAttemptEvent(task2Attempt.GetID
				(), TaskAttemptEventType.TaDone));
			// wait for first two map task to complete
			app.WaitForState(mapTask1, TaskState.Succeeded);
			app.WaitForState(mapTask2, TaskState.Succeeded);
			// stop the app
			app.Stop();
			// rerun
			// in rerun the 1st two map will be recovered from previous run
			app = new TestRecovery.MRAppWithHistory(2, 1, false, this.GetType().FullName, false
				, ++runCount);
			conf = new Configuration();
			conf.SetClass("mapred.output.committer.class", typeof(TestRecovery.TestFileOutputCommitter
				), typeof(OutputCommitter));
			conf.SetBoolean("want.am.recovery", true);
			conf.Set(FileOutputFormat.Outdir, outputDir.ToString());
			// Set num-reduces explicitly in conf as recovery logic depends on it.
			conf.SetInt(MRJobConfig.NumReduces, 0);
			conf.SetBoolean(MRJobConfig.JobUbertaskEnable, false);
			job = app.Submit(conf);
			app.WaitForState(job, JobState.Running);
			NUnit.Framework.Assert.AreEqual("No of tasks not correct", 3, job.GetTasks().Count
				);
			it = job.GetTasks().Values.GetEnumerator();
			mapTask1 = it.Next();
			mapTask2 = it.Next();
			mapTask3 = it.Next();
			// first two maps will be recovered, no need to send done
			app.WaitForState(mapTask1, TaskState.Succeeded);
			app.WaitForState(mapTask2, TaskState.Succeeded);
			app.WaitForState(mapTask3, TaskState.Running);
			task3Attempt = mapTask3.GetAttempts().Values.GetEnumerator().Next();
			// before sending the TA_DONE, event make sure attempt has come to
			// RUNNING state
			app.WaitForState(task3Attempt, TaskAttemptState.Running);
			// send the done signal to the 3rd map task
			app.GetContext().GetEventHandler().Handle(new TaskAttemptEvent(mapTask3.GetAttempts
				().Values.GetEnumerator().Next().GetID(), TaskAttemptEventType.TaDone));
			// wait to get it completed
			app.WaitForState(mapTask3, TaskState.Succeeded);
			app.WaitForState(job, JobState.Succeeded);
			app.VerifyCompleted();
		}

		/// <summary>
		/// This test case primarily verifies if the recovery is controlled through config
		/// property.
		/// </summary>
		/// <remarks>
		/// This test case primarily verifies if the recovery is controlled through config
		/// property. In this case, recover is turned OFF. AM with 3 maps and 0 reduce.
		/// AM crashes after the first two tasks finishes and recovery fails and have
		/// to rerun fully in the second generation and succeeds.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRecoveryFailsUsingCustomOutputCommitter()
		{
			int runCount = 0;
			MRApp app = new TestRecovery.MRAppWithHistory(3, 0, false, this.GetType().FullName
				, true, ++runCount);
			Configuration conf = new Configuration();
			conf.SetClass("mapred.output.committer.class", typeof(TestRecovery.TestFileOutputCommitter
				), typeof(OutputCommitter));
			conf.Set(FileOutputFormat.Outdir, outputDir.ToString());
			conf.SetBoolean("want.am.recovery", false);
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = app.Submit(conf);
			app.WaitForState(job, JobState.Running);
			// all maps would be running
			NUnit.Framework.Assert.AreEqual("No of tasks not correct", 3, job.GetTasks().Count
				);
			IEnumerator<Task> it = job.GetTasks().Values.GetEnumerator();
			Task mapTask1 = it.Next();
			Task mapTask2 = it.Next();
			Task mapTask3 = it.Next();
			// all maps must be running
			app.WaitForState(mapTask1, TaskState.Running);
			app.WaitForState(mapTask2, TaskState.Running);
			app.WaitForState(mapTask3, TaskState.Running);
			TaskAttempt task1Attempt = mapTask1.GetAttempts().Values.GetEnumerator().Next();
			TaskAttempt task2Attempt = mapTask2.GetAttempts().Values.GetEnumerator().Next();
			TaskAttempt task3Attempt = mapTask3.GetAttempts().Values.GetEnumerator().Next();
			// before sending the TA_DONE, event make sure attempt has come to
			// RUNNING state
			app.WaitForState(task1Attempt, TaskAttemptState.Running);
			app.WaitForState(task2Attempt, TaskAttemptState.Running);
			app.WaitForState(task3Attempt, TaskAttemptState.Running);
			// send the done signal to the 1st two maps
			app.GetContext().GetEventHandler().Handle(new TaskAttemptEvent(task1Attempt.GetID
				(), TaskAttemptEventType.TaDone));
			app.GetContext().GetEventHandler().Handle(new TaskAttemptEvent(task2Attempt.GetID
				(), TaskAttemptEventType.TaDone));
			// wait for first two map task to complete
			app.WaitForState(mapTask1, TaskState.Succeeded);
			app.WaitForState(mapTask2, TaskState.Succeeded);
			// stop the app
			app.Stop();
			// rerun
			// in rerun the 1st two map will be recovered from previous run
			app = new TestRecovery.MRAppWithHistory(2, 1, false, this.GetType().FullName, false
				, ++runCount);
			conf = new Configuration();
			conf.SetClass("mapred.output.committer.class", typeof(TestRecovery.TestFileOutputCommitter
				), typeof(OutputCommitter));
			conf.SetBoolean("want.am.recovery", false);
			conf.Set(FileOutputFormat.Outdir, outputDir.ToString());
			// Set num-reduces explicitly in conf as recovery logic depends on it.
			conf.SetInt(MRJobConfig.NumReduces, 0);
			conf.SetBoolean(MRJobConfig.JobUbertaskEnable, false);
			job = app.Submit(conf);
			app.WaitForState(job, JobState.Running);
			NUnit.Framework.Assert.AreEqual("No of tasks not correct", 3, job.GetTasks().Count
				);
			it = job.GetTasks().Values.GetEnumerator();
			mapTask1 = it.Next();
			mapTask2 = it.Next();
			mapTask3 = it.Next();
			// first two maps will NOT  be recovered, need to send done from them
			app.WaitForState(mapTask1, TaskState.Running);
			app.WaitForState(mapTask2, TaskState.Running);
			app.WaitForState(mapTask3, TaskState.Running);
			task3Attempt = mapTask3.GetAttempts().Values.GetEnumerator().Next();
			// before sending the TA_DONE, event make sure attempt has come to
			// RUNNING state
			app.WaitForState(task3Attempt, TaskAttemptState.Running);
			// send the done signal to all 3 tasks map task
			app.GetContext().GetEventHandler().Handle(new TaskAttemptEvent(mapTask1.GetAttempts
				().Values.GetEnumerator().Next().GetID(), TaskAttemptEventType.TaDone));
			app.GetContext().GetEventHandler().Handle(new TaskAttemptEvent(mapTask2.GetAttempts
				().Values.GetEnumerator().Next().GetID(), TaskAttemptEventType.TaDone));
			app.GetContext().GetEventHandler().Handle(new TaskAttemptEvent(mapTask3.GetAttempts
				().Values.GetEnumerator().Next().GetID(), TaskAttemptEventType.TaDone));
			// wait to get it completed
			app.WaitForState(mapTask3, TaskState.Succeeded);
			app.WaitForState(job, JobState.Succeeded);
			app.VerifyCompleted();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMultipleCrashes()
		{
			int runCount = 0;
			MRApp app = new TestRecovery.MRAppWithHistory(2, 1, false, this.GetType().FullName
				, true, ++runCount);
			Configuration conf = new Configuration();
			conf.SetBoolean("mapred.mapper.new-api", true);
			conf.SetBoolean("mapred.reducer.new-api", true);
			conf.SetBoolean(MRJobConfig.JobUbertaskEnable, false);
			conf.Set(FileOutputFormat.Outdir, outputDir.ToString());
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = app.Submit(conf);
			app.WaitForState(job, JobState.Running);
			//all maps would be running
			NUnit.Framework.Assert.AreEqual("No of tasks not correct", 3, job.GetTasks().Count
				);
			IEnumerator<Task> it = job.GetTasks().Values.GetEnumerator();
			Task mapTask1 = it.Next();
			Task mapTask2 = it.Next();
			Task reduceTask = it.Next();
			// all maps must be running
			app.WaitForState(mapTask1, TaskState.Running);
			app.WaitForState(mapTask2, TaskState.Running);
			TaskAttempt task1Attempt1 = mapTask1.GetAttempts().Values.GetEnumerator().Next();
			TaskAttempt task2Attempt = mapTask2.GetAttempts().Values.GetEnumerator().Next();
			//before sending the TA_DONE, event make sure attempt has come to 
			//RUNNING state
			app.WaitForState(task1Attempt1, TaskAttemptState.Running);
			app.WaitForState(task2Attempt, TaskAttemptState.Running);
			// reduces must be in NEW state
			NUnit.Framework.Assert.AreEqual("Reduce Task state not correct", TaskState.Running
				, reduceTask.GetReport().GetTaskState());
			//send the done signal to the 1st map
			app.GetContext().GetEventHandler().Handle(new TaskAttemptEvent(task1Attempt1.GetID
				(), TaskAttemptEventType.TaDone));
			//wait for first map task to complete
			app.WaitForState(mapTask1, TaskState.Succeeded);
			// Crash the app
			app.Stop();
			//rerun
			//in rerun the 1st map will be recovered from previous run
			app = new TestRecovery.MRAppWithHistory(2, 1, false, this.GetType().FullName, false
				, ++runCount);
			conf = new Configuration();
			conf.SetBoolean(MRJobConfig.MrAmJobRecoveryEnable, true);
			conf.SetBoolean("mapred.mapper.new-api", true);
			conf.SetBoolean("mapred.reducer.new-api", true);
			conf.Set(FileOutputFormat.Outdir, outputDir.ToString());
			conf.SetBoolean(MRJobConfig.JobUbertaskEnable, false);
			job = app.Submit(conf);
			app.WaitForState(job, JobState.Running);
			//all maps would be running
			NUnit.Framework.Assert.AreEqual("No of tasks not correct", 3, job.GetTasks().Count
				);
			it = job.GetTasks().Values.GetEnumerator();
			mapTask1 = it.Next();
			mapTask2 = it.Next();
			reduceTask = it.Next();
			// first map will be recovered, no need to send done
			app.WaitForState(mapTask1, TaskState.Succeeded);
			app.WaitForState(mapTask2, TaskState.Running);
			task2Attempt = mapTask2.GetAttempts().Values.GetEnumerator().Next();
			//before sending the TA_DONE, event make sure attempt has come to 
			//RUNNING state
			app.WaitForState(task2Attempt, TaskAttemptState.Running);
			//send the done signal to the 2nd map task
			app.GetContext().GetEventHandler().Handle(new TaskAttemptEvent(mapTask2.GetAttempts
				().Values.GetEnumerator().Next().GetID(), TaskAttemptEventType.TaDone));
			//wait to get it completed
			app.WaitForState(mapTask2, TaskState.Succeeded);
			// Crash the app again.
			app.Stop();
			//rerun
			//in rerun the 1st and 2nd map will be recovered from previous run
			app = new TestRecovery.MRAppWithHistory(2, 1, false, this.GetType().FullName, false
				, ++runCount);
			conf = new Configuration();
			conf.SetBoolean(MRJobConfig.MrAmJobRecoveryEnable, true);
			conf.SetBoolean("mapred.mapper.new-api", true);
			conf.SetBoolean("mapred.reducer.new-api", true);
			conf.Set(FileOutputFormat.Outdir, outputDir.ToString());
			conf.SetBoolean(MRJobConfig.JobUbertaskEnable, false);
			job = app.Submit(conf);
			app.WaitForState(job, JobState.Running);
			//all maps would be running
			NUnit.Framework.Assert.AreEqual("No of tasks not correct", 3, job.GetTasks().Count
				);
			it = job.GetTasks().Values.GetEnumerator();
			mapTask1 = it.Next();
			mapTask2 = it.Next();
			reduceTask = it.Next();
			// The maps will be recovered, no need to send done
			app.WaitForState(mapTask1, TaskState.Succeeded);
			app.WaitForState(mapTask2, TaskState.Succeeded);
			//wait for reduce to be running before sending done
			app.WaitForState(reduceTask, TaskState.Running);
			//send the done signal to the reduce
			app.GetContext().GetEventHandler().Handle(new TaskAttemptEvent(reduceTask.GetAttempts
				().Values.GetEnumerator().Next().GetID(), TaskAttemptEventType.TaDone));
			app.WaitForState(job, JobState.Succeeded);
			app.VerifyCompleted();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestOutputRecovery()
		{
			int runCount = 0;
			MRApp app = new TestRecovery.MRAppWithHistory(1, 2, false, this.GetType().FullName
				, true, ++runCount);
			Configuration conf = new Configuration();
			conf.SetBoolean("mapred.mapper.new-api", true);
			conf.SetBoolean("mapred.reducer.new-api", true);
			conf.SetBoolean(MRJobConfig.JobUbertaskEnable, false);
			conf.Set(FileOutputFormat.Outdir, outputDir.ToString());
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = app.Submit(conf);
			app.WaitForState(job, JobState.Running);
			NUnit.Framework.Assert.AreEqual("No of tasks not correct", 3, job.GetTasks().Count
				);
			IEnumerator<Task> it = job.GetTasks().Values.GetEnumerator();
			Task mapTask1 = it.Next();
			Task reduceTask1 = it.Next();
			// all maps must be running
			app.WaitForState(mapTask1, TaskState.Running);
			TaskAttempt task1Attempt1 = mapTask1.GetAttempts().Values.GetEnumerator().Next();
			//before sending the TA_DONE, event make sure attempt has come to 
			//RUNNING state
			app.WaitForState(task1Attempt1, TaskAttemptState.Running);
			//send the done signal to the map
			app.GetContext().GetEventHandler().Handle(new TaskAttemptEvent(task1Attempt1.GetID
				(), TaskAttemptEventType.TaDone));
			//wait for map task to complete
			app.WaitForState(mapTask1, TaskState.Succeeded);
			// Verify the shuffle-port
			NUnit.Framework.Assert.AreEqual(5467, task1Attempt1.GetShufflePort());
			app.WaitForState(reduceTask1, TaskState.Running);
			TaskAttempt reduce1Attempt1 = reduceTask1.GetAttempts().Values.GetEnumerator().Next
				();
			// write output corresponding to reduce1
			WriteOutput(reduce1Attempt1, conf);
			//send the done signal to the 1st reduce
			app.GetContext().GetEventHandler().Handle(new TaskAttemptEvent(reduce1Attempt1.GetID
				(), TaskAttemptEventType.TaDone));
			//wait for first reduce task to complete
			app.WaitForState(reduceTask1, TaskState.Succeeded);
			//stop the app before the job completes.
			app.Stop();
			//rerun
			//in rerun the map will be recovered from previous run
			app = new TestRecovery.MRAppWithHistory(1, 2, false, this.GetType().FullName, false
				, ++runCount);
			conf = new Configuration();
			conf.SetBoolean(MRJobConfig.MrAmJobRecoveryEnable, true);
			conf.SetBoolean("mapred.mapper.new-api", true);
			conf.SetBoolean("mapred.reducer.new-api", true);
			conf.Set(FileOutputFormat.Outdir, outputDir.ToString());
			conf.SetBoolean(MRJobConfig.JobUbertaskEnable, false);
			job = app.Submit(conf);
			app.WaitForState(job, JobState.Running);
			NUnit.Framework.Assert.AreEqual("No of tasks not correct", 3, job.GetTasks().Count
				);
			it = job.GetTasks().Values.GetEnumerator();
			mapTask1 = it.Next();
			reduceTask1 = it.Next();
			Task reduceTask2 = it.Next();
			// map will be recovered, no need to send done
			app.WaitForState(mapTask1, TaskState.Succeeded);
			// Verify the shuffle-port after recovery
			task1Attempt1 = mapTask1.GetAttempts().Values.GetEnumerator().Next();
			NUnit.Framework.Assert.AreEqual(5467, task1Attempt1.GetShufflePort());
			// first reduce will be recovered, no need to send done
			app.WaitForState(reduceTask1, TaskState.Succeeded);
			app.WaitForState(reduceTask2, TaskState.Running);
			TaskAttempt reduce2Attempt = reduceTask2.GetAttempts().Values.GetEnumerator().Next
				();
			//before sending the TA_DONE, event make sure attempt has come to 
			//RUNNING state
			app.WaitForState(reduce2Attempt, TaskAttemptState.Running);
			//send the done signal to the 2nd reduce task
			app.GetContext().GetEventHandler().Handle(new TaskAttemptEvent(reduce2Attempt.GetID
				(), TaskAttemptEventType.TaDone));
			//wait to get it completed
			app.WaitForState(reduceTask2, TaskState.Succeeded);
			app.WaitForState(job, JobState.Succeeded);
			app.VerifyCompleted();
			ValidateOutput();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestOutputRecoveryMapsOnly()
		{
			int runCount = 0;
			MRApp app = new TestRecovery.MRAppWithHistory(2, 1, false, this.GetType().FullName
				, true, ++runCount);
			Configuration conf = new Configuration();
			conf.SetBoolean("mapred.mapper.new-api", true);
			conf.SetBoolean("mapred.reducer.new-api", true);
			conf.SetBoolean(MRJobConfig.JobUbertaskEnable, false);
			conf.Set(FileOutputFormat.Outdir, outputDir.ToString());
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = app.Submit(conf);
			app.WaitForState(job, JobState.Running);
			NUnit.Framework.Assert.AreEqual("No of tasks not correct", 3, job.GetTasks().Count
				);
			IEnumerator<Task> it = job.GetTasks().Values.GetEnumerator();
			Task mapTask1 = it.Next();
			Task mapTask2 = it.Next();
			Task reduceTask1 = it.Next();
			// all maps must be running
			app.WaitForState(mapTask1, TaskState.Running);
			TaskAttempt task1Attempt1 = mapTask1.GetAttempts().Values.GetEnumerator().Next();
			//before sending the TA_DONE, event make sure attempt has come to 
			//RUNNING state
			app.WaitForState(task1Attempt1, TaskAttemptState.Running);
			// write output corresponding to map1 (This is just to validate that it is
			//no included in the output)
			WriteBadOutput(task1Attempt1, conf);
			//send the done signal to the map
			app.GetContext().GetEventHandler().Handle(new TaskAttemptEvent(task1Attempt1.GetID
				(), TaskAttemptEventType.TaDone));
			//wait for map task to complete
			app.WaitForState(mapTask1, TaskState.Succeeded);
			// Verify the shuffle-port
			NUnit.Framework.Assert.AreEqual(5467, task1Attempt1.GetShufflePort());
			//stop the app before the job completes.
			app.Stop();
			//rerun
			//in rerun the map will be recovered from previous run
			app = new TestRecovery.MRAppWithHistory(2, 1, false, this.GetType().FullName, false
				, ++runCount);
			conf = new Configuration();
			conf.SetBoolean(MRJobConfig.MrAmJobRecoveryEnable, true);
			conf.SetBoolean("mapred.mapper.new-api", true);
			conf.SetBoolean("mapred.reducer.new-api", true);
			conf.Set(FileOutputFormat.Outdir, outputDir.ToString());
			conf.SetBoolean(MRJobConfig.JobUbertaskEnable, false);
			job = app.Submit(conf);
			app.WaitForState(job, JobState.Running);
			NUnit.Framework.Assert.AreEqual("No of tasks not correct", 3, job.GetTasks().Count
				);
			it = job.GetTasks().Values.GetEnumerator();
			mapTask1 = it.Next();
			mapTask2 = it.Next();
			reduceTask1 = it.Next();
			// map will be recovered, no need to send done
			app.WaitForState(mapTask1, TaskState.Succeeded);
			// Verify the shuffle-port after recovery
			task1Attempt1 = mapTask1.GetAttempts().Values.GetEnumerator().Next();
			NUnit.Framework.Assert.AreEqual(5467, task1Attempt1.GetShufflePort());
			app.WaitForState(mapTask2, TaskState.Running);
			TaskAttempt task2Attempt1 = mapTask2.GetAttempts().Values.GetEnumerator().Next();
			//before sending the TA_DONE, event make sure attempt has come to 
			//RUNNING state
			app.WaitForState(task2Attempt1, TaskAttemptState.Running);
			//send the done signal to the map
			app.GetContext().GetEventHandler().Handle(new TaskAttemptEvent(task2Attempt1.GetID
				(), TaskAttemptEventType.TaDone));
			//wait for map task to complete
			app.WaitForState(mapTask2, TaskState.Succeeded);
			// Verify the shuffle-port
			NUnit.Framework.Assert.AreEqual(5467, task2Attempt1.GetShufflePort());
			app.WaitForState(reduceTask1, TaskState.Running);
			TaskAttempt reduce1Attempt1 = reduceTask1.GetAttempts().Values.GetEnumerator().Next
				();
			// write output corresponding to reduce1
			WriteOutput(reduce1Attempt1, conf);
			//send the done signal to the 1st reduce
			app.GetContext().GetEventHandler().Handle(new TaskAttemptEvent(reduce1Attempt1.GetID
				(), TaskAttemptEventType.TaDone));
			//wait for first reduce task to complete
			app.WaitForState(reduceTask1, TaskState.Succeeded);
			app.WaitForState(job, JobState.Succeeded);
			app.VerifyCompleted();
			ValidateOutput();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRecoveryWithOldCommiter()
		{
			int runCount = 0;
			MRApp app = new TestRecovery.MRAppWithHistory(1, 2, false, this.GetType().FullName
				, true, ++runCount);
			Configuration conf = new Configuration();
			conf.SetBoolean("mapred.mapper.new-api", false);
			conf.SetBoolean("mapred.reducer.new-api", false);
			conf.SetBoolean(MRJobConfig.JobUbertaskEnable, false);
			conf.Set(FileOutputFormat.Outdir, outputDir.ToString());
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = app.Submit(conf);
			app.WaitForState(job, JobState.Running);
			NUnit.Framework.Assert.AreEqual("No of tasks not correct", 3, job.GetTasks().Count
				);
			IEnumerator<Task> it = job.GetTasks().Values.GetEnumerator();
			Task mapTask1 = it.Next();
			Task reduceTask1 = it.Next();
			// all maps must be running
			app.WaitForState(mapTask1, TaskState.Running);
			TaskAttempt task1Attempt1 = mapTask1.GetAttempts().Values.GetEnumerator().Next();
			//before sending the TA_DONE, event make sure attempt has come to 
			//RUNNING state
			app.WaitForState(task1Attempt1, TaskAttemptState.Running);
			//send the done signal to the map
			app.GetContext().GetEventHandler().Handle(new TaskAttemptEvent(task1Attempt1.GetID
				(), TaskAttemptEventType.TaDone));
			//wait for map task to complete
			app.WaitForState(mapTask1, TaskState.Succeeded);
			// Verify the shuffle-port
			NUnit.Framework.Assert.AreEqual(5467, task1Attempt1.GetShufflePort());
			app.WaitForState(reduceTask1, TaskState.Running);
			TaskAttempt reduce1Attempt1 = reduceTask1.GetAttempts().Values.GetEnumerator().Next
				();
			// write output corresponding to reduce1
			WriteOutput(reduce1Attempt1, conf);
			//send the done signal to the 1st reduce
			app.GetContext().GetEventHandler().Handle(new TaskAttemptEvent(reduce1Attempt1.GetID
				(), TaskAttemptEventType.TaDone));
			//wait for first reduce task to complete
			app.WaitForState(reduceTask1, TaskState.Succeeded);
			//stop the app before the job completes.
			app.Stop();
			//rerun
			//in rerun the map will be recovered from previous run
			app = new TestRecovery.MRAppWithHistory(1, 2, false, this.GetType().FullName, false
				, ++runCount);
			conf = new Configuration();
			conf.SetBoolean(MRJobConfig.MrAmJobRecoveryEnable, true);
			conf.SetBoolean("mapred.mapper.new-api", false);
			conf.SetBoolean("mapred.reducer.new-api", false);
			conf.Set(FileOutputFormat.Outdir, outputDir.ToString());
			conf.SetBoolean(MRJobConfig.JobUbertaskEnable, false);
			job = app.Submit(conf);
			app.WaitForState(job, JobState.Running);
			NUnit.Framework.Assert.AreEqual("No of tasks not correct", 3, job.GetTasks().Count
				);
			it = job.GetTasks().Values.GetEnumerator();
			mapTask1 = it.Next();
			reduceTask1 = it.Next();
			Task reduceTask2 = it.Next();
			// map will be recovered, no need to send done
			app.WaitForState(mapTask1, TaskState.Succeeded);
			// Verify the shuffle-port after recovery
			task1Attempt1 = mapTask1.GetAttempts().Values.GetEnumerator().Next();
			NUnit.Framework.Assert.AreEqual(5467, task1Attempt1.GetShufflePort());
			// first reduce will be recovered, no need to send done
			app.WaitForState(reduceTask1, TaskState.Succeeded);
			app.WaitForState(reduceTask2, TaskState.Running);
			TaskAttempt reduce2Attempt = reduceTask2.GetAttempts().Values.GetEnumerator().Next
				();
			//before sending the TA_DONE, event make sure attempt has come to 
			//RUNNING state
			app.WaitForState(reduce2Attempt, TaskAttemptState.Running);
			//send the done signal to the 2nd reduce task
			app.GetContext().GetEventHandler().Handle(new TaskAttemptEvent(reduce2Attempt.GetID
				(), TaskAttemptEventType.TaDone));
			//wait to get it completed
			app.WaitForState(reduceTask2, TaskState.Succeeded);
			app.WaitForState(job, JobState.Succeeded);
			app.VerifyCompleted();
			ValidateOutput();
		}

		/// <summary>AM with 2 maps and 1 reduce.</summary>
		/// <remarks>
		/// AM with 2 maps and 1 reduce. For 1st map, one attempt fails, one attempt
		/// completely disappears because of failed launch, one attempt gets killed and
		/// one attempt succeeds. AM crashes after the first tasks finishes and
		/// recovers completely and succeeds in the second generation.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSpeculative()
		{
			int runCount = 0;
			long am1StartTimeEst = Runtime.CurrentTimeMillis();
			MRApp app = new TestRecovery.MRAppWithHistory(2, 1, false, this.GetType().FullName
				, true, ++runCount);
			Configuration conf = new Configuration();
			conf.SetBoolean("mapred.mapper.new-api", true);
			conf.SetBoolean("mapred.reducer.new-api", true);
			conf.SetBoolean(MRJobConfig.JobUbertaskEnable, false);
			conf.Set(FileOutputFormat.Outdir, outputDir.ToString());
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = app.Submit(conf);
			app.WaitForState(job, JobState.Running);
			long jobStartTime = job.GetReport().GetStartTime();
			//all maps would be running
			NUnit.Framework.Assert.AreEqual("No of tasks not correct", 3, job.GetTasks().Count
				);
			IEnumerator<Task> it = job.GetTasks().Values.GetEnumerator();
			Task mapTask1 = it.Next();
			Task mapTask2 = it.Next();
			Task reduceTask = it.Next();
			// all maps must be running
			app.WaitForState(mapTask1, TaskState.Running);
			app.WaitForState(mapTask2, TaskState.Running);
			// Launch a Speculative Task for the first Task
			app.GetContext().GetEventHandler().Handle(new TaskEvent(mapTask1.GetID(), TaskEventType
				.TAddSpecAttempt));
			int timeOut = 0;
			while (mapTask1.GetAttempts().Count != 2 && timeOut++ < 10)
			{
				Sharpen.Thread.Sleep(1000);
				Log.Info("Waiting for next attempt to start");
			}
			IEnumerator<TaskAttempt> t1it = mapTask1.GetAttempts().Values.GetEnumerator();
			TaskAttempt task1Attempt1 = t1it.Next();
			TaskAttempt task1Attempt2 = t1it.Next();
			TaskAttempt task2Attempt = mapTask2.GetAttempts().Values.GetEnumerator().Next();
			ContainerId t1a2contId = task1Attempt2.GetAssignedContainerID();
			Log.Info(t1a2contId.ToString());
			Log.Info(task1Attempt1.GetID().ToString());
			Log.Info(task1Attempt2.GetID().ToString());
			// Launch container for speculative attempt
			app.GetContext().GetEventHandler().Handle(new TaskAttemptContainerLaunchedEvent(task1Attempt2
				.GetID(), runCount));
			//before sending the TA_DONE, event make sure attempt has come to 
			//RUNNING state
			app.WaitForState(task1Attempt1, TaskAttemptState.Running);
			app.WaitForState(task1Attempt2, TaskAttemptState.Running);
			app.WaitForState(task2Attempt, TaskAttemptState.Running);
			// reduces must be in NEW state
			NUnit.Framework.Assert.AreEqual("Reduce Task state not correct", TaskState.Running
				, reduceTask.GetReport().GetTaskState());
			//send the done signal to the map 1 attempt 1
			app.GetContext().GetEventHandler().Handle(new TaskAttemptEvent(task1Attempt1.GetID
				(), TaskAttemptEventType.TaDone));
			app.WaitForState(task1Attempt1, TaskAttemptState.Succeeded);
			//wait for first map task to complete
			app.WaitForState(mapTask1, TaskState.Succeeded);
			long task1StartTime = mapTask1.GetReport().GetStartTime();
			long task1FinishTime = mapTask1.GetReport().GetFinishTime();
			//stop the app
			app.Stop();
			//rerun
			//in rerun the 1st map will be recovered from previous run
			long am2StartTimeEst = Runtime.CurrentTimeMillis();
			app = new TestRecovery.MRAppWithHistory(2, 1, false, this.GetType().FullName, false
				, ++runCount);
			conf = new Configuration();
			conf.SetBoolean(MRJobConfig.MrAmJobRecoveryEnable, true);
			conf.SetBoolean("mapred.mapper.new-api", true);
			conf.SetBoolean("mapred.reducer.new-api", true);
			conf.Set(FileOutputFormat.Outdir, outputDir.ToString());
			conf.SetBoolean(MRJobConfig.JobUbertaskEnable, false);
			job = app.Submit(conf);
			app.WaitForState(job, JobState.Running);
			//all maps would be running
			NUnit.Framework.Assert.AreEqual("No of tasks not correct", 3, job.GetTasks().Count
				);
			it = job.GetTasks().Values.GetEnumerator();
			mapTask1 = it.Next();
			mapTask2 = it.Next();
			reduceTask = it.Next();
			// first map will be recovered, no need to send done
			app.WaitForState(mapTask1, TaskState.Succeeded);
			app.WaitForState(mapTask2, TaskState.Running);
			task2Attempt = mapTask2.GetAttempts().Values.GetEnumerator().Next();
			//before sending the TA_DONE, event make sure attempt has come to 
			//RUNNING state
			app.WaitForState(task2Attempt, TaskAttemptState.Running);
			//send the done signal to the 2nd map task
			app.GetContext().GetEventHandler().Handle(new TaskAttemptEvent(mapTask2.GetAttempts
				().Values.GetEnumerator().Next().GetID(), TaskAttemptEventType.TaDone));
			//wait to get it completed
			app.WaitForState(mapTask2, TaskState.Succeeded);
			//wait for reduce to be running before sending done
			app.WaitForState(reduceTask, TaskState.Running);
			//send the done signal to the reduce
			app.GetContext().GetEventHandler().Handle(new TaskAttemptEvent(reduceTask.GetAttempts
				().Values.GetEnumerator().Next().GetID(), TaskAttemptEventType.TaDone));
			app.WaitForState(job, JobState.Succeeded);
			app.VerifyCompleted();
			NUnit.Framework.Assert.AreEqual("Job Start time not correct", jobStartTime, job.GetReport
				().GetStartTime());
			NUnit.Framework.Assert.AreEqual("Task Start time not correct", task1StartTime, mapTask1
				.GetReport().GetStartTime());
			NUnit.Framework.Assert.AreEqual("Task Finish time not correct", task1FinishTime, 
				mapTask1.GetReport().GetFinishTime());
			NUnit.Framework.Assert.AreEqual(2, job.GetAMInfos().Count);
			int attemptNum = 1;
			// Verify AMInfo
			foreach (AMInfo amInfo in job.GetAMInfos())
			{
				NUnit.Framework.Assert.AreEqual(attemptNum++, amInfo.GetAppAttemptId().GetAttemptId
					());
				NUnit.Framework.Assert.AreEqual(amInfo.GetAppAttemptId(), amInfo.GetContainerId()
					.GetApplicationAttemptId());
				NUnit.Framework.Assert.AreEqual(MRApp.NmHost, amInfo.GetNodeManagerHost());
				NUnit.Framework.Assert.AreEqual(MRApp.NmPort, amInfo.GetNodeManagerPort());
				NUnit.Framework.Assert.AreEqual(MRApp.NmHttpPort, amInfo.GetNodeManagerHttpPort()
					);
			}
			long am1StartTimeReal = job.GetAMInfos()[0].GetStartTime();
			long am2StartTimeReal = job.GetAMInfos()[1].GetStartTime();
			NUnit.Framework.Assert.IsTrue(am1StartTimeReal >= am1StartTimeEst && am1StartTimeReal
				 <= am2StartTimeEst);
			NUnit.Framework.Assert.IsTrue(am2StartTimeReal >= am2StartTimeEst && am2StartTimeReal
				 <= Runtime.CurrentTimeMillis());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRecoveryWithoutShuffleSecret()
		{
			int runCount = 0;
			MRApp app = new TestRecovery.MRAppNoShuffleSecret(2, 1, false, this.GetType().FullName
				, true, ++runCount);
			Configuration conf = new Configuration();
			conf.SetBoolean("mapred.mapper.new-api", true);
			conf.SetBoolean("mapred.reducer.new-api", true);
			conf.SetBoolean(MRJobConfig.JobUbertaskEnable, false);
			conf.Set(FileOutputFormat.Outdir, outputDir.ToString());
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = app.Submit(conf);
			app.WaitForState(job, JobState.Running);
			//all maps would be running
			NUnit.Framework.Assert.AreEqual("No of tasks not correct", 3, job.GetTasks().Count
				);
			IEnumerator<Task> it = job.GetTasks().Values.GetEnumerator();
			Task mapTask1 = it.Next();
			Task mapTask2 = it.Next();
			Task reduceTask = it.Next();
			// all maps must be running
			app.WaitForState(mapTask1, TaskState.Running);
			app.WaitForState(mapTask2, TaskState.Running);
			TaskAttempt task1Attempt = mapTask1.GetAttempts().Values.GetEnumerator().Next();
			TaskAttempt task2Attempt = mapTask2.GetAttempts().Values.GetEnumerator().Next();
			//before sending the TA_DONE, event make sure attempt has come to
			//RUNNING state
			app.WaitForState(task1Attempt, TaskAttemptState.Running);
			app.WaitForState(task2Attempt, TaskAttemptState.Running);
			// reduces must be in NEW state
			NUnit.Framework.Assert.AreEqual("Reduce Task state not correct", TaskState.Running
				, reduceTask.GetReport().GetTaskState());
			//send the done signal to the 1st map attempt
			app.GetContext().GetEventHandler().Handle(new TaskAttemptEvent(task1Attempt.GetID
				(), TaskAttemptEventType.TaDone));
			//wait for first map task to complete
			app.WaitForState(mapTask1, TaskState.Succeeded);
			//stop the app
			app.Stop();
			//in recovery the 1st map should NOT be recovered from previous run
			//since the shuffle secret was not provided with the job credentials
			//and had to be rolled per app attempt
			app = new TestRecovery.MRAppNoShuffleSecret(2, 1, false, this.GetType().FullName, 
				false, ++runCount);
			conf = new Configuration();
			conf.SetBoolean(MRJobConfig.MrAmJobRecoveryEnable, true);
			conf.SetBoolean("mapred.mapper.new-api", true);
			conf.SetBoolean("mapred.reducer.new-api", true);
			conf.Set(FileOutputFormat.Outdir, outputDir.ToString());
			conf.SetBoolean(MRJobConfig.JobUbertaskEnable, false);
			job = app.Submit(conf);
			app.WaitForState(job, JobState.Running);
			//all maps would be running
			NUnit.Framework.Assert.AreEqual("No of tasks not correct", 3, job.GetTasks().Count
				);
			it = job.GetTasks().Values.GetEnumerator();
			mapTask1 = it.Next();
			mapTask2 = it.Next();
			reduceTask = it.Next();
			app.WaitForState(mapTask1, TaskState.Running);
			app.WaitForState(mapTask2, TaskState.Running);
			task2Attempt = mapTask2.GetAttempts().Values.GetEnumerator().Next();
			//before sending the TA_DONE, event make sure attempt has come to
			//RUNNING state
			app.WaitForState(task2Attempt, TaskAttemptState.Running);
			//send the done signal to the 2nd map task
			app.GetContext().GetEventHandler().Handle(new TaskAttemptEvent(mapTask2.GetAttempts
				().Values.GetEnumerator().Next().GetID(), TaskAttemptEventType.TaDone));
			//wait to get it completed
			app.WaitForState(mapTask2, TaskState.Succeeded);
			//verify first map task is still running
			app.WaitForState(mapTask1, TaskState.Running);
			//send the done signal to the 2nd map task
			app.GetContext().GetEventHandler().Handle(new TaskAttemptEvent(mapTask1.GetAttempts
				().Values.GetEnumerator().Next().GetID(), TaskAttemptEventType.TaDone));
			//wait to get it completed
			app.WaitForState(mapTask1, TaskState.Succeeded);
			//wait for reduce to be running before sending done
			app.WaitForState(reduceTask, TaskState.Running);
			//send the done signal to the reduce
			app.GetContext().GetEventHandler().Handle(new TaskAttemptEvent(reduceTask.GetAttempts
				().Values.GetEnumerator().Next().GetID(), TaskAttemptEventType.TaDone));
			app.WaitForState(job, JobState.Succeeded);
			app.VerifyCompleted();
		}

		[NUnit.Framework.Test]
		public virtual void TestRecoverySuccessAttempt()
		{
			Log.Info("--- START: testRecoverySuccessAttempt ---");
			long clusterTimestamp = Runtime.CurrentTimeMillis();
			EventHandler mockEventHandler = Org.Mockito.Mockito.Mock<EventHandler>();
			MapTaskImpl recoverMapTask = GetMockMapTask(clusterTimestamp, mockEventHandler);
			TaskId taskId = recoverMapTask.GetID();
			JobID jobID = new JobID(System.Convert.ToString(clusterTimestamp), 1);
			TaskID taskID = new TaskID(jobID, TaskType.Map, taskId.GetId());
			//Mock up the TaskAttempts
			IDictionary<TaskAttemptID, JobHistoryParser.TaskAttemptInfo> mockTaskAttempts = new 
				Dictionary<TaskAttemptID, JobHistoryParser.TaskAttemptInfo>();
			TaskAttemptID taId1 = new TaskAttemptID(taskID, 2);
			JobHistoryParser.TaskAttemptInfo mockTAinfo1 = GetMockTaskAttemptInfo(taId1, TaskAttemptState
				.Succeeded);
			mockTaskAttempts[taId1] = mockTAinfo1;
			TaskAttemptID taId2 = new TaskAttemptID(taskID, 1);
			JobHistoryParser.TaskAttemptInfo mockTAinfo2 = GetMockTaskAttemptInfo(taId2, TaskAttemptState
				.Failed);
			mockTaskAttempts[taId2] = mockTAinfo2;
			OutputCommitter mockCommitter = Org.Mockito.Mockito.Mock<OutputCommitter>();
			JobHistoryParser.TaskInfo mockTaskInfo = Org.Mockito.Mockito.Mock<JobHistoryParser.TaskInfo
				>();
			Org.Mockito.Mockito.When(mockTaskInfo.GetTaskStatus()).ThenReturn("SUCCEEDED");
			Org.Mockito.Mockito.When(mockTaskInfo.GetTaskId()).ThenReturn(taskID);
			Org.Mockito.Mockito.When(mockTaskInfo.GetAllTaskAttempts()).ThenReturn(mockTaskAttempts
				);
			recoverMapTask.Handle(new TaskRecoverEvent(taskId, mockTaskInfo, mockCommitter, true
				));
			ArgumentCaptor<Org.Apache.Hadoop.Mapreduce.Jobhistory.Event> arg = ArgumentCaptor
				.ForClass<Org.Apache.Hadoop.Mapreduce.Jobhistory.Event>();
			Org.Mockito.Mockito.Verify(mockEventHandler, Org.Mockito.Mockito.AtLeast(1)).Handle
				((Org.Apache.Hadoop.Yarn.Event.Event)arg.Capture());
			IDictionary<TaskAttemptID, TaskAttemptState> finalAttemptStates = new Dictionary<
				TaskAttemptID, TaskAttemptState>();
			finalAttemptStates[taId1] = TaskAttemptState.Succeeded;
			finalAttemptStates[taId2] = TaskAttemptState.Failed;
			IList<EventType> jobHistoryEvents = new AList<EventType>();
			jobHistoryEvents.AddItem(EventType.TaskStarted);
			jobHistoryEvents.AddItem(EventType.MapAttemptStarted);
			jobHistoryEvents.AddItem(EventType.MapAttemptFinished);
			jobHistoryEvents.AddItem(EventType.MapAttemptStarted);
			jobHistoryEvents.AddItem(EventType.MapAttemptFailed);
			jobHistoryEvents.AddItem(EventType.TaskFinished);
			RecoveryChecker(recoverMapTask, TaskState.Succeeded, finalAttemptStates, arg, jobHistoryEvents
				, 2L, 1L);
		}

		[NUnit.Framework.Test]
		public virtual void TestRecoveryAllFailAttempts()
		{
			Log.Info("--- START: testRecoveryAllFailAttempts ---");
			long clusterTimestamp = Runtime.CurrentTimeMillis();
			EventHandler mockEventHandler = Org.Mockito.Mockito.Mock<EventHandler>();
			MapTaskImpl recoverMapTask = GetMockMapTask(clusterTimestamp, mockEventHandler);
			TaskId taskId = recoverMapTask.GetID();
			JobID jobID = new JobID(System.Convert.ToString(clusterTimestamp), 1);
			TaskID taskID = new TaskID(jobID, TaskType.Map, taskId.GetId());
			//Mock up the TaskAttempts
			IDictionary<TaskAttemptID, JobHistoryParser.TaskAttemptInfo> mockTaskAttempts = new 
				Dictionary<TaskAttemptID, JobHistoryParser.TaskAttemptInfo>();
			TaskAttemptID taId1 = new TaskAttemptID(taskID, 2);
			JobHistoryParser.TaskAttemptInfo mockTAinfo1 = GetMockTaskAttemptInfo(taId1, TaskAttemptState
				.Failed);
			mockTaskAttempts[taId1] = mockTAinfo1;
			TaskAttemptID taId2 = new TaskAttemptID(taskID, 1);
			JobHistoryParser.TaskAttemptInfo mockTAinfo2 = GetMockTaskAttemptInfo(taId2, TaskAttemptState
				.Failed);
			mockTaskAttempts[taId2] = mockTAinfo2;
			OutputCommitter mockCommitter = Org.Mockito.Mockito.Mock<OutputCommitter>();
			JobHistoryParser.TaskInfo mockTaskInfo = Org.Mockito.Mockito.Mock<JobHistoryParser.TaskInfo
				>();
			Org.Mockito.Mockito.When(mockTaskInfo.GetTaskStatus()).ThenReturn("FAILED");
			Org.Mockito.Mockito.When(mockTaskInfo.GetTaskId()).ThenReturn(taskID);
			Org.Mockito.Mockito.When(mockTaskInfo.GetAllTaskAttempts()).ThenReturn(mockTaskAttempts
				);
			recoverMapTask.Handle(new TaskRecoverEvent(taskId, mockTaskInfo, mockCommitter, true
				));
			ArgumentCaptor<Org.Apache.Hadoop.Mapreduce.Jobhistory.Event> arg = ArgumentCaptor
				.ForClass<Org.Apache.Hadoop.Mapreduce.Jobhistory.Event>();
			Org.Mockito.Mockito.Verify(mockEventHandler, Org.Mockito.Mockito.AtLeast(1)).Handle
				((Org.Apache.Hadoop.Yarn.Event.Event)arg.Capture());
			IDictionary<TaskAttemptID, TaskAttemptState> finalAttemptStates = new Dictionary<
				TaskAttemptID, TaskAttemptState>();
			finalAttemptStates[taId1] = TaskAttemptState.Failed;
			finalAttemptStates[taId2] = TaskAttemptState.Failed;
			IList<EventType> jobHistoryEvents = new AList<EventType>();
			jobHistoryEvents.AddItem(EventType.TaskStarted);
			jobHistoryEvents.AddItem(EventType.MapAttemptStarted);
			jobHistoryEvents.AddItem(EventType.MapAttemptFailed);
			jobHistoryEvents.AddItem(EventType.MapAttemptStarted);
			jobHistoryEvents.AddItem(EventType.MapAttemptFailed);
			jobHistoryEvents.AddItem(EventType.TaskFailed);
			RecoveryChecker(recoverMapTask, TaskState.Failed, finalAttemptStates, arg, jobHistoryEvents
				, 2L, 2L);
		}

		[NUnit.Framework.Test]
		public virtual void TestRecoveryTaskSuccessAllAttemptsFail()
		{
			Log.Info("--- START:  testRecoveryTaskSuccessAllAttemptsFail ---");
			long clusterTimestamp = Runtime.CurrentTimeMillis();
			EventHandler mockEventHandler = Org.Mockito.Mockito.Mock<EventHandler>();
			MapTaskImpl recoverMapTask = GetMockMapTask(clusterTimestamp, mockEventHandler);
			TaskId taskId = recoverMapTask.GetID();
			JobID jobID = new JobID(System.Convert.ToString(clusterTimestamp), 1);
			TaskID taskID = new TaskID(jobID, TaskType.Map, taskId.GetId());
			//Mock up the TaskAttempts
			IDictionary<TaskAttemptID, JobHistoryParser.TaskAttemptInfo> mockTaskAttempts = new 
				Dictionary<TaskAttemptID, JobHistoryParser.TaskAttemptInfo>();
			TaskAttemptID taId1 = new TaskAttemptID(taskID, 2);
			JobHistoryParser.TaskAttemptInfo mockTAinfo1 = GetMockTaskAttemptInfo(taId1, TaskAttemptState
				.Failed);
			mockTaskAttempts[taId1] = mockTAinfo1;
			TaskAttemptID taId2 = new TaskAttemptID(taskID, 1);
			JobHistoryParser.TaskAttemptInfo mockTAinfo2 = GetMockTaskAttemptInfo(taId2, TaskAttemptState
				.Failed);
			mockTaskAttempts[taId2] = mockTAinfo2;
			OutputCommitter mockCommitter = Org.Mockito.Mockito.Mock<OutputCommitter>();
			JobHistoryParser.TaskInfo mockTaskInfo = Org.Mockito.Mockito.Mock<JobHistoryParser.TaskInfo
				>();
			Org.Mockito.Mockito.When(mockTaskInfo.GetTaskStatus()).ThenReturn("SUCCEEDED");
			Org.Mockito.Mockito.When(mockTaskInfo.GetTaskId()).ThenReturn(taskID);
			Org.Mockito.Mockito.When(mockTaskInfo.GetAllTaskAttempts()).ThenReturn(mockTaskAttempts
				);
			recoverMapTask.Handle(new TaskRecoverEvent(taskId, mockTaskInfo, mockCommitter, true
				));
			ArgumentCaptor<Org.Apache.Hadoop.Mapreduce.Jobhistory.Event> arg = ArgumentCaptor
				.ForClass<Org.Apache.Hadoop.Mapreduce.Jobhistory.Event>();
			Org.Mockito.Mockito.Verify(mockEventHandler, Org.Mockito.Mockito.AtLeast(1)).Handle
				((Org.Apache.Hadoop.Yarn.Event.Event)arg.Capture());
			IDictionary<TaskAttemptID, TaskAttemptState> finalAttemptStates = new Dictionary<
				TaskAttemptID, TaskAttemptState>();
			finalAttemptStates[taId1] = TaskAttemptState.Failed;
			finalAttemptStates[taId2] = TaskAttemptState.Failed;
			// check for one new attempt launched since successful attempt not found
			TaskAttemptID taId3 = new TaskAttemptID(taskID, 2000);
			finalAttemptStates[taId3] = TaskAttemptState.New;
			IList<EventType> jobHistoryEvents = new AList<EventType>();
			jobHistoryEvents.AddItem(EventType.TaskStarted);
			jobHistoryEvents.AddItem(EventType.MapAttemptStarted);
			jobHistoryEvents.AddItem(EventType.MapAttemptFailed);
			jobHistoryEvents.AddItem(EventType.MapAttemptStarted);
			jobHistoryEvents.AddItem(EventType.MapAttemptFailed);
			RecoveryChecker(recoverMapTask, TaskState.Running, finalAttemptStates, arg, jobHistoryEvents
				, 2L, 2L);
		}

		[NUnit.Framework.Test]
		public virtual void TestRecoveryTaskSuccessAllAttemptsSucceed()
		{
			Log.Info("--- START:  testRecoveryTaskSuccessAllAttemptsFail ---");
			long clusterTimestamp = Runtime.CurrentTimeMillis();
			EventHandler mockEventHandler = Org.Mockito.Mockito.Mock<EventHandler>();
			MapTaskImpl recoverMapTask = GetMockMapTask(clusterTimestamp, mockEventHandler);
			TaskId taskId = recoverMapTask.GetID();
			JobID jobID = new JobID(System.Convert.ToString(clusterTimestamp), 1);
			TaskID taskID = new TaskID(jobID, TaskType.Map, taskId.GetId());
			//Mock up the TaskAttempts
			IDictionary<TaskAttemptID, JobHistoryParser.TaskAttemptInfo> mockTaskAttempts = new 
				Dictionary<TaskAttemptID, JobHistoryParser.TaskAttemptInfo>();
			TaskAttemptID taId1 = new TaskAttemptID(taskID, 2);
			JobHistoryParser.TaskAttemptInfo mockTAinfo1 = GetMockTaskAttemptInfo(taId1, TaskAttemptState
				.Succeeded);
			mockTaskAttempts[taId1] = mockTAinfo1;
			TaskAttemptID taId2 = new TaskAttemptID(taskID, 1);
			JobHistoryParser.TaskAttemptInfo mockTAinfo2 = GetMockTaskAttemptInfo(taId2, TaskAttemptState
				.Succeeded);
			mockTaskAttempts[taId2] = mockTAinfo2;
			OutputCommitter mockCommitter = Org.Mockito.Mockito.Mock<OutputCommitter>();
			JobHistoryParser.TaskInfo mockTaskInfo = Org.Mockito.Mockito.Mock<JobHistoryParser.TaskInfo
				>();
			Org.Mockito.Mockito.When(mockTaskInfo.GetTaskStatus()).ThenReturn("SUCCEEDED");
			Org.Mockito.Mockito.When(mockTaskInfo.GetTaskId()).ThenReturn(taskID);
			Org.Mockito.Mockito.When(mockTaskInfo.GetAllTaskAttempts()).ThenReturn(mockTaskAttempts
				);
			recoverMapTask.Handle(new TaskRecoverEvent(taskId, mockTaskInfo, mockCommitter, true
				));
			ArgumentCaptor<Org.Apache.Hadoop.Mapreduce.Jobhistory.Event> arg = ArgumentCaptor
				.ForClass<Org.Apache.Hadoop.Mapreduce.Jobhistory.Event>();
			Org.Mockito.Mockito.Verify(mockEventHandler, Org.Mockito.Mockito.AtLeast(1)).Handle
				((Org.Apache.Hadoop.Yarn.Event.Event)arg.Capture());
			IDictionary<TaskAttemptID, TaskAttemptState> finalAttemptStates = new Dictionary<
				TaskAttemptID, TaskAttemptState>();
			finalAttemptStates[taId1] = TaskAttemptState.Succeeded;
			finalAttemptStates[taId2] = TaskAttemptState.Succeeded;
			IList<EventType> jobHistoryEvents = new AList<EventType>();
			jobHistoryEvents.AddItem(EventType.TaskStarted);
			jobHistoryEvents.AddItem(EventType.MapAttemptStarted);
			jobHistoryEvents.AddItem(EventType.MapAttemptFinished);
			jobHistoryEvents.AddItem(EventType.MapAttemptStarted);
			jobHistoryEvents.AddItem(EventType.MapAttemptFinished);
			jobHistoryEvents.AddItem(EventType.TaskFinished);
			RecoveryChecker(recoverMapTask, TaskState.Succeeded, finalAttemptStates, arg, jobHistoryEvents
				, 2L, 0L);
		}

		[NUnit.Framework.Test]
		public virtual void TestRecoveryAllAttemptsKilled()
		{
			Log.Info("--- START:  testRecoveryAllAttemptsKilled ---");
			long clusterTimestamp = Runtime.CurrentTimeMillis();
			EventHandler mockEventHandler = Org.Mockito.Mockito.Mock<EventHandler>();
			MapTaskImpl recoverMapTask = GetMockMapTask(clusterTimestamp, mockEventHandler);
			TaskId taskId = recoverMapTask.GetID();
			JobID jobID = new JobID(System.Convert.ToString(clusterTimestamp), 1);
			TaskID taskID = new TaskID(jobID, TaskType.Map, taskId.GetId());
			//Mock up the TaskAttempts
			IDictionary<TaskAttemptID, JobHistoryParser.TaskAttemptInfo> mockTaskAttempts = new 
				Dictionary<TaskAttemptID, JobHistoryParser.TaskAttemptInfo>();
			TaskAttemptID taId1 = new TaskAttemptID(taskID, 2);
			JobHistoryParser.TaskAttemptInfo mockTAinfo1 = GetMockTaskAttemptInfo(taId1, TaskAttemptState
				.Killed);
			mockTaskAttempts[taId1] = mockTAinfo1;
			TaskAttemptID taId2 = new TaskAttemptID(taskID, 1);
			JobHistoryParser.TaskAttemptInfo mockTAinfo2 = GetMockTaskAttemptInfo(taId2, TaskAttemptState
				.Killed);
			mockTaskAttempts[taId2] = mockTAinfo2;
			OutputCommitter mockCommitter = Org.Mockito.Mockito.Mock<OutputCommitter>();
			JobHistoryParser.TaskInfo mockTaskInfo = Org.Mockito.Mockito.Mock<JobHistoryParser.TaskInfo
				>();
			Org.Mockito.Mockito.When(mockTaskInfo.GetTaskStatus()).ThenReturn("KILLED");
			Org.Mockito.Mockito.When(mockTaskInfo.GetTaskId()).ThenReturn(taskID);
			Org.Mockito.Mockito.When(mockTaskInfo.GetAllTaskAttempts()).ThenReturn(mockTaskAttempts
				);
			recoverMapTask.Handle(new TaskRecoverEvent(taskId, mockTaskInfo, mockCommitter, true
				));
			ArgumentCaptor<Org.Apache.Hadoop.Mapreduce.Jobhistory.Event> arg = ArgumentCaptor
				.ForClass<Org.Apache.Hadoop.Mapreduce.Jobhistory.Event>();
			Org.Mockito.Mockito.Verify(mockEventHandler, Org.Mockito.Mockito.AtLeast(1)).Handle
				((Org.Apache.Hadoop.Yarn.Event.Event)arg.Capture());
			IDictionary<TaskAttemptID, TaskAttemptState> finalAttemptStates = new Dictionary<
				TaskAttemptID, TaskAttemptState>();
			finalAttemptStates[taId1] = TaskAttemptState.Killed;
			finalAttemptStates[taId2] = TaskAttemptState.Killed;
			IList<EventType> jobHistoryEvents = new AList<EventType>();
			jobHistoryEvents.AddItem(EventType.TaskStarted);
			jobHistoryEvents.AddItem(EventType.MapAttemptStarted);
			jobHistoryEvents.AddItem(EventType.MapAttemptKilled);
			jobHistoryEvents.AddItem(EventType.MapAttemptStarted);
			jobHistoryEvents.AddItem(EventType.MapAttemptKilled);
			jobHistoryEvents.AddItem(EventType.TaskFailed);
			RecoveryChecker(recoverMapTask, TaskState.Killed, finalAttemptStates, arg, jobHistoryEvents
				, 2L, 0L);
		}

		private void RecoveryChecker(MapTaskImpl checkTask, TaskState finalState, IDictionary
			<TaskAttemptID, TaskAttemptState> finalAttemptStates, ArgumentCaptor<Org.Apache.Hadoop.Mapreduce.Jobhistory.Event
			> arg, IList<EventType> expectedJobHistoryEvents, long expectedMapLaunches, long
			 expectedFailedMaps)
		{
			NUnit.Framework.Assert.AreEqual("Final State of Task", finalState, checkTask.GetState
				());
			IDictionary<TaskAttemptId, TaskAttempt> recoveredAttempts = checkTask.GetAttempts
				();
			NUnit.Framework.Assert.AreEqual("Expected Number of Task Attempts", finalAttemptStates
				.Count, recoveredAttempts.Count);
			foreach (TaskAttemptID taID in finalAttemptStates.Keys)
			{
				NUnit.Framework.Assert.AreEqual("Expected Task Attempt State", finalAttemptStates
					[taID], recoveredAttempts[TypeConverter.ToYarn(taID)].GetState());
			}
			IEnumerator<Org.Apache.Hadoop.Mapreduce.Jobhistory.Event> ie = arg.GetAllValues()
				.GetEnumerator();
			int eventNum = 0;
			long totalLaunchedMaps = 0;
			long totalFailedMaps = 0;
			bool jobTaskEventReceived = false;
			while (ie.HasNext())
			{
				object current = ie.Next();
				++eventNum;
				Log.Info(eventNum + " " + current.GetType().FullName);
				if (current is JobHistoryEvent)
				{
					JobHistoryEvent jhe = (JobHistoryEvent)current;
					Log.Info(expectedJobHistoryEvents[0].ToString() + " " + jhe.GetHistoryEvent().GetEventType
						().ToString() + " " + jhe.GetJobID());
					NUnit.Framework.Assert.AreEqual(expectedJobHistoryEvents[0], jhe.GetHistoryEvent(
						).GetEventType());
					expectedJobHistoryEvents.Remove(0);
				}
				else
				{
					if (current is JobCounterUpdateEvent)
					{
						JobCounterUpdateEvent jcue = (JobCounterUpdateEvent)current;
						Log.Info("JobCounterUpdateEvent " + jcue.GetCounterUpdates()[0].GetCounterKey() +
							 " " + jcue.GetCounterUpdates()[0].GetIncrementValue());
						if (jcue.GetCounterUpdates()[0].GetCounterKey() == JobCounter.NumFailedMaps)
						{
							totalFailedMaps += jcue.GetCounterUpdates()[0].GetIncrementValue();
						}
						else
						{
							if (jcue.GetCounterUpdates()[0].GetCounterKey() == JobCounter.TotalLaunchedMaps)
							{
								totalLaunchedMaps += jcue.GetCounterUpdates()[0].GetIncrementValue();
							}
						}
					}
					else
					{
						if (current is JobTaskEvent)
						{
							JobTaskEvent jte = (JobTaskEvent)current;
							NUnit.Framework.Assert.AreEqual(jte.GetState(), finalState);
							jobTaskEventReceived = true;
						}
					}
				}
			}
			NUnit.Framework.Assert.IsTrue(jobTaskEventReceived || (finalState == TaskState.Running
				));
			NUnit.Framework.Assert.AreEqual("Did not process all expected JobHistoryEvents", 
				0, expectedJobHistoryEvents.Count);
			NUnit.Framework.Assert.AreEqual("Expected Map Launches", expectedMapLaunches, totalLaunchedMaps
				);
			NUnit.Framework.Assert.AreEqual("Expected Failed Maps", expectedFailedMaps, totalFailedMaps
				);
		}

		private MapTaskImpl GetMockMapTask(long clusterTimestamp, EventHandler eh)
		{
			ApplicationId appId = ApplicationId.NewInstance(clusterTimestamp, 1);
			JobId jobId = MRBuilderUtils.NewJobId(appId, 1);
			int partitions = 2;
			Path remoteJobConfFile = Org.Mockito.Mockito.Mock<Path>();
			JobConf conf = new JobConf();
			TaskAttemptListener taskAttemptListener = Org.Mockito.Mockito.Mock<TaskAttemptListener
				>();
			Org.Apache.Hadoop.Security.Token.Token<JobTokenIdentifier> jobToken = (Org.Apache.Hadoop.Security.Token.Token
				<JobTokenIdentifier>)Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Security.Token.Token
				>();
			Credentials credentials = null;
			Clock clock = new SystemClock();
			int appAttemptId = 3;
			MRAppMetrics metrics = Org.Mockito.Mockito.Mock<MRAppMetrics>();
			Resource minContainerRequirements = Org.Mockito.Mockito.Mock<Resource>();
			Org.Mockito.Mockito.When(minContainerRequirements.GetMemory()).ThenReturn(1000);
			ClusterInfo clusterInfo = Org.Mockito.Mockito.Mock<ClusterInfo>();
			AppContext appContext = Org.Mockito.Mockito.Mock<AppContext>();
			Org.Mockito.Mockito.When(appContext.GetClusterInfo()).ThenReturn(clusterInfo);
			JobSplit.TaskSplitMetaInfo taskSplitMetaInfo = Org.Mockito.Mockito.Mock<JobSplit.TaskSplitMetaInfo
				>();
			MapTaskImpl mapTask = new MapTaskImpl(jobId, partitions, eh, remoteJobConfFile, conf
				, taskSplitMetaInfo, taskAttemptListener, jobToken, credentials, clock, appAttemptId
				, metrics, appContext);
			return mapTask;
		}

		private JobHistoryParser.TaskAttemptInfo GetMockTaskAttemptInfo(TaskAttemptID tai
			, TaskAttemptState tas)
		{
			ContainerId ci = Org.Mockito.Mockito.Mock<ContainerId>();
			Counters counters = Org.Mockito.Mockito.Mock<Counters>();
			TaskType tt = TaskType.Map;
			long finishTime = Runtime.CurrentTimeMillis();
			JobHistoryParser.TaskAttemptInfo mockTAinfo = Org.Mockito.Mockito.Mock<JobHistoryParser.TaskAttemptInfo
				>();
			Org.Mockito.Mockito.When(mockTAinfo.GetAttemptId()).ThenReturn(tai);
			Org.Mockito.Mockito.When(mockTAinfo.GetContainerId()).ThenReturn(ci);
			Org.Mockito.Mockito.When(mockTAinfo.GetCounters()).ThenReturn(counters);
			Org.Mockito.Mockito.When(mockTAinfo.GetError()).ThenReturn(string.Empty);
			Org.Mockito.Mockito.When(mockTAinfo.GetFinishTime()).ThenReturn(finishTime);
			Org.Mockito.Mockito.When(mockTAinfo.GetHostname()).ThenReturn("localhost");
			Org.Mockito.Mockito.When(mockTAinfo.GetHttpPort()).ThenReturn(23);
			Org.Mockito.Mockito.When(mockTAinfo.GetMapFinishTime()).ThenReturn(finishTime - 1000L
				);
			Org.Mockito.Mockito.When(mockTAinfo.GetPort()).ThenReturn(24);
			Org.Mockito.Mockito.When(mockTAinfo.GetRackname()).ThenReturn("defaultRack");
			Org.Mockito.Mockito.When(mockTAinfo.GetShuffleFinishTime()).ThenReturn(finishTime
				 - 2000L);
			Org.Mockito.Mockito.When(mockTAinfo.GetShufflePort()).ThenReturn(25);
			Org.Mockito.Mockito.When(mockTAinfo.GetSortFinishTime()).ThenReturn(finishTime - 
				3000L);
			Org.Mockito.Mockito.When(mockTAinfo.GetStartTime()).ThenReturn(finishTime - 10000
				);
			Org.Mockito.Mockito.When(mockTAinfo.GetState()).ThenReturn("task in progress");
			Org.Mockito.Mockito.When(mockTAinfo.GetTaskStatus()).ThenReturn(tas.ToString());
			Org.Mockito.Mockito.When(mockTAinfo.GetTaskType()).ThenReturn(tt);
			Org.Mockito.Mockito.When(mockTAinfo.GetTrackerName()).ThenReturn("TrackerName");
			return mockTAinfo;
		}

		/// <exception cref="System.Exception"/>
		private void WriteBadOutput(TaskAttempt attempt, Configuration conf)
		{
			TaskAttemptContext tContext = new TaskAttemptContextImpl(conf, TypeConverter.FromYarn
				(attempt.GetID()));
			TextOutputFormat<object, object> theOutputFormat = new TextOutputFormat();
			RecordWriter theRecordWriter = theOutputFormat.GetRecordWriter(tContext);
			NullWritable nullWritable = NullWritable.Get();
			try
			{
				theRecordWriter.Write(key2, val2);
				theRecordWriter.Write(null, nullWritable);
				theRecordWriter.Write(null, val2);
				theRecordWriter.Write(nullWritable, val1);
				theRecordWriter.Write(key1, nullWritable);
				theRecordWriter.Write(key2, null);
				theRecordWriter.Write(null, null);
				theRecordWriter.Write(key1, val1);
			}
			finally
			{
				theRecordWriter.Close(tContext);
			}
			OutputFormat outputFormat = ReflectionUtils.NewInstance(tContext.GetOutputFormatClass
				(), conf);
			OutputCommitter committer = outputFormat.GetOutputCommitter(tContext);
			committer.CommitTask(tContext);
		}

		/// <exception cref="System.Exception"/>
		private void WriteOutput(TaskAttempt attempt, Configuration conf)
		{
			TaskAttemptContext tContext = new TaskAttemptContextImpl(conf, TypeConverter.FromYarn
				(attempt.GetID()));
			TextOutputFormat<object, object> theOutputFormat = new TextOutputFormat();
			RecordWriter theRecordWriter = theOutputFormat.GetRecordWriter(tContext);
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
				theRecordWriter.Close(tContext);
			}
			OutputFormat outputFormat = ReflectionUtils.NewInstance(tContext.GetOutputFormatClass
				(), conf);
			OutputCommitter committer = outputFormat.GetOutputCommitter(tContext);
			committer.CommitTask(tContext);
		}

		/// <exception cref="System.IO.IOException"/>
		private void ValidateOutput()
		{
			FilePath expectedFile = new FilePath(new Path(outputDir, partFile).ToString());
			StringBuilder expectedOutput = new StringBuilder();
			expectedOutput.Append(key1).Append('\t').Append(val1).Append("\n");
			expectedOutput.Append(val1).Append("\n");
			expectedOutput.Append(val2).Append("\n");
			expectedOutput.Append(key2).Append("\n");
			expectedOutput.Append(key1).Append("\n");
			expectedOutput.Append(key2).Append('\t').Append(val2).Append("\n");
			string output = Slurp(expectedFile);
			NUnit.Framework.Assert.AreEqual(output, expectedOutput.ToString());
		}

		/// <exception cref="System.IO.IOException"/>
		public static string Slurp(FilePath f)
		{
			int len = (int)f.Length();
			byte[] buf = new byte[len];
			FileInputStream @in = new FileInputStream(f);
			string contents = null;
			try
			{
				@in.Read(buf, 0, len);
				contents = Sharpen.Runtime.GetStringForBytes(buf, "UTF-8");
			}
			finally
			{
				@in.Close();
			}
			return contents;
		}

		internal class MRAppWithHistory : MRApp
		{
			public MRAppWithHistory(int maps, int reduces, bool autoComplete, string testName
				, bool cleanOnStart, int startCount)
				: base(maps, reduces, autoComplete, testName, cleanOnStart, startCount)
			{
			}

			protected internal override ContainerLauncher CreateContainerLauncher(AppContext 
				context)
			{
				MRApp.MockContainerLauncher launcher = new _MockContainerLauncher_1933();
				// Pass everything except the 2nd attempt of the first task.
				launcher.shufflePort = 5467;
				return launcher;
			}

			private sealed class _MockContainerLauncher_1933 : MRApp.MockContainerLauncher
			{
				public _MockContainerLauncher_1933()
				{
				}

				public override void Handle(ContainerLauncherEvent @event)
				{
					TaskAttemptId taskAttemptID = @event.GetTaskAttemptID();
					if (taskAttemptID.GetId() != 1 || taskAttemptID.GetTaskId().GetId() != 0)
					{
						base.Handle(@event);
					}
				}
			}

			protected internal override EventHandler<JobHistoryEvent> CreateJobHistoryHandler
				(AppContext context)
			{
				JobHistoryEventHandler eventHandler = new JobHistoryEventHandler(context, GetStartCount
					());
				return eventHandler;
			}
		}

		internal class MRAppNoShuffleSecret : TestRecovery.MRAppWithHistory
		{
			public MRAppNoShuffleSecret(int maps, int reduces, bool autoComplete, string testName
				, bool cleanOnStart, int startCount)
				: base(maps, reduces, autoComplete, testName, cleanOnStart, startCount)
			{
			}

			protected internal override void InitJobCredentialsAndUGI(Configuration conf)
			{
			}
			// do NOT put a shuffle secret in the job credentials
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] arg)
		{
			TestRecovery test = new TestRecovery();
			test.TestCrashed();
			test.TestMultipleCrashes();
			test.TestOutputRecovery();
			test.TestOutputRecoveryMapsOnly();
			test.TestRecoveryWithOldCommiter();
			test.TestSpeculative();
			test.TestRecoveryWithoutShuffleSecret();
			test.TestRecoverySuccessAttempt();
			test.TestRecoveryAllFailAttempts();
			test.TestRecoveryTaskSuccessAllAttemptsFail();
			test.TestRecoveryTaskSuccessAllAttemptsSucceed();
			test.TestRecoveryAllAttemptsKilled();
		}
	}
}
