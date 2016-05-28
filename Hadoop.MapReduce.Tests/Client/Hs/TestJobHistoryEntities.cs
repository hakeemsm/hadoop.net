using System.Collections.Generic;
using NUnit.Framework.Runners;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job;
using Org.Apache.Hadoop.Mapreduce.V2.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.HS
{
	public class TestJobHistoryEntities
	{
		private readonly string historyFileName = "job_1329348432655_0001-1329348443227-user-Sleep+job-1329348468601-10-1-SUCCEEDED-default.jhist";

		private readonly string historyFileNameZeroReduceTasks = "job_1416424547277_0002-1416424775281-root-TeraGen-1416424785433-2-0-SUCCEEDED-default-1416424779349.jhist";

		private readonly string confFileName = "job_1329348432655_0001_conf.xml";

		private readonly Configuration conf = new Configuration();

		private readonly JobACLsManager jobAclsManager;

		private bool loadTasks;

		private JobId jobId = MRBuilderUtils.NewJobId(1329348432655l, 1, 1);

		internal Path fullHistoryPath = new Path(this.GetType().GetClassLoader().GetResource
			(historyFileName).GetFile());

		internal Path fullHistoryPathZeroReduces = new Path(this.GetType().GetClassLoader
			().GetResource(historyFileNameZeroReduceTasks).GetFile());

		internal Path fullConfPath = new Path(this.GetType().GetClassLoader().GetResource
			(confFileName).GetFile());

		private CompletedJob completedJob;

		/// <exception cref="System.Exception"/>
		public TestJobHistoryEntities(bool loadTasks)
		{
			jobAclsManager = new JobACLsManager(conf);
			this.loadTasks = loadTasks;
		}

		[Parameterized.Parameters]
		public static ICollection<object[]> Data()
		{
			IList<object[]> list = new AList<object[]>(2);
			list.AddItem(new object[] { true });
			list.AddItem(new object[] { false });
			return list;
		}

		/* Verify some expected values based on the history file */
		/// <exception cref="System.Exception"/>
		public virtual void TestCompletedJob()
		{
			HistoryFileManager.HistoryFileInfo info = Org.Mockito.Mockito.Mock<HistoryFileManager.HistoryFileInfo
				>();
			Org.Mockito.Mockito.When(info.GetConfFile()).ThenReturn(fullConfPath);
			//Re-initialize to verify the delayed load.
			completedJob = new CompletedJob(conf, jobId, fullHistoryPath, loadTasks, "user", 
				info, jobAclsManager);
			//Verify tasks loaded based on loadTask parameter.
			NUnit.Framework.Assert.AreEqual(loadTasks, completedJob.tasksLoaded.Get());
			NUnit.Framework.Assert.AreEqual(1, completedJob.GetAMInfos().Count);
			NUnit.Framework.Assert.AreEqual(10, completedJob.GetCompletedMaps());
			NUnit.Framework.Assert.AreEqual(1, completedJob.GetCompletedReduces());
			NUnit.Framework.Assert.AreEqual(12, completedJob.GetTasks().Count);
			//Verify tasks loaded at this point.
			NUnit.Framework.Assert.AreEqual(true, completedJob.tasksLoaded.Get());
			NUnit.Framework.Assert.AreEqual(10, completedJob.GetTasks(TaskType.Map).Count);
			NUnit.Framework.Assert.AreEqual(2, completedJob.GetTasks(TaskType.Reduce).Count);
			NUnit.Framework.Assert.AreEqual("user", completedJob.GetUserName());
			NUnit.Framework.Assert.AreEqual(JobState.Succeeded, completedJob.GetState());
			JobReport jobReport = completedJob.GetReport();
			NUnit.Framework.Assert.AreEqual("user", jobReport.GetUser());
			NUnit.Framework.Assert.AreEqual(JobState.Succeeded, jobReport.GetJobState());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestCopmletedJobReportWithZeroTasks()
		{
			HistoryFileManager.HistoryFileInfo info = Org.Mockito.Mockito.Mock<HistoryFileManager.HistoryFileInfo
				>();
			Org.Mockito.Mockito.When(info.GetConfFile()).ThenReturn(fullConfPath);
			completedJob = new CompletedJob(conf, jobId, fullHistoryPathZeroReduces, loadTasks
				, "user", info, jobAclsManager);
			JobReport jobReport = completedJob.GetReport();
			// Make sure that the number reduces (completed and total) are equal to zero.
			NUnit.Framework.Assert.AreEqual(0, completedJob.GetTotalReduces());
			NUnit.Framework.Assert.AreEqual(0, completedJob.GetCompletedReduces());
			// Verify that the reduce progress is 1.0 (not NaN)
			NUnit.Framework.Assert.AreEqual(1.0, jobReport.GetReduceProgress(), 0.001);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestCompletedTask()
		{
			HistoryFileManager.HistoryFileInfo info = Org.Mockito.Mockito.Mock<HistoryFileManager.HistoryFileInfo
				>();
			Org.Mockito.Mockito.When(info.GetConfFile()).ThenReturn(fullConfPath);
			completedJob = new CompletedJob(conf, jobId, fullHistoryPath, loadTasks, "user", 
				info, jobAclsManager);
			TaskId mt1Id = MRBuilderUtils.NewTaskId(jobId, 0, TaskType.Map);
			TaskId rt1Id = MRBuilderUtils.NewTaskId(jobId, 0, TaskType.Reduce);
			IDictionary<TaskId, Task> mapTasks = completedJob.GetTasks(TaskType.Map);
			IDictionary<TaskId, Task> reduceTasks = completedJob.GetTasks(TaskType.Reduce);
			NUnit.Framework.Assert.AreEqual(10, mapTasks.Count);
			NUnit.Framework.Assert.AreEqual(2, reduceTasks.Count);
			Task mt1 = mapTasks[mt1Id];
			NUnit.Framework.Assert.AreEqual(1, mt1.GetAttempts().Count);
			NUnit.Framework.Assert.AreEqual(TaskState.Succeeded, mt1.GetState());
			TaskReport mt1Report = mt1.GetReport();
			NUnit.Framework.Assert.AreEqual(TaskState.Succeeded, mt1Report.GetTaskState());
			NUnit.Framework.Assert.AreEqual(mt1Id, mt1Report.GetTaskId());
			Task rt1 = reduceTasks[rt1Id];
			NUnit.Framework.Assert.AreEqual(1, rt1.GetAttempts().Count);
			NUnit.Framework.Assert.AreEqual(TaskState.Succeeded, rt1.GetState());
			TaskReport rt1Report = rt1.GetReport();
			NUnit.Framework.Assert.AreEqual(TaskState.Succeeded, rt1Report.GetTaskState());
			NUnit.Framework.Assert.AreEqual(rt1Id, rt1Report.GetTaskId());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestCompletedTaskAttempt()
		{
			HistoryFileManager.HistoryFileInfo info = Org.Mockito.Mockito.Mock<HistoryFileManager.HistoryFileInfo
				>();
			Org.Mockito.Mockito.When(info.GetConfFile()).ThenReturn(fullConfPath);
			completedJob = new CompletedJob(conf, jobId, fullHistoryPath, loadTasks, "user", 
				info, jobAclsManager);
			TaskId mt1Id = MRBuilderUtils.NewTaskId(jobId, 0, TaskType.Map);
			TaskId rt1Id = MRBuilderUtils.NewTaskId(jobId, 0, TaskType.Reduce);
			TaskAttemptId mta1Id = MRBuilderUtils.NewTaskAttemptId(mt1Id, 0);
			TaskAttemptId rta1Id = MRBuilderUtils.NewTaskAttemptId(rt1Id, 0);
			Task mt1 = completedJob.GetTask(mt1Id);
			Task rt1 = completedJob.GetTask(rt1Id);
			TaskAttempt mta1 = mt1.GetAttempt(mta1Id);
			NUnit.Framework.Assert.AreEqual(TaskAttemptState.Succeeded, mta1.GetState());
			NUnit.Framework.Assert.AreEqual("localhost:45454", mta1.GetAssignedContainerMgrAddress
				());
			NUnit.Framework.Assert.AreEqual("localhost:9999", mta1.GetNodeHttpAddress());
			TaskAttemptReport mta1Report = mta1.GetReport();
			NUnit.Framework.Assert.AreEqual(TaskAttemptState.Succeeded, mta1Report.GetTaskAttemptState
				());
			NUnit.Framework.Assert.AreEqual("localhost", mta1Report.GetNodeManagerHost());
			NUnit.Framework.Assert.AreEqual(45454, mta1Report.GetNodeManagerPort());
			NUnit.Framework.Assert.AreEqual(9999, mta1Report.GetNodeManagerHttpPort());
			TaskAttempt rta1 = rt1.GetAttempt(rta1Id);
			NUnit.Framework.Assert.AreEqual(TaskAttemptState.Succeeded, rta1.GetState());
			NUnit.Framework.Assert.AreEqual("localhost:45454", rta1.GetAssignedContainerMgrAddress
				());
			NUnit.Framework.Assert.AreEqual("localhost:9999", rta1.GetNodeHttpAddress());
			TaskAttemptReport rta1Report = rta1.GetReport();
			NUnit.Framework.Assert.AreEqual(TaskAttemptState.Succeeded, rta1Report.GetTaskAttemptState
				());
			NUnit.Framework.Assert.AreEqual("localhost", rta1Report.GetNodeManagerHost());
			NUnit.Framework.Assert.AreEqual(45454, rta1Report.GetNodeManagerPort());
			NUnit.Framework.Assert.AreEqual(9999, rta1Report.GetNodeManagerHttpPort());
		}

		/// <summary>Simple test of some methods of CompletedJob</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestGetTaskAttemptCompletionEvent()
		{
			HistoryFileManager.HistoryFileInfo info = Org.Mockito.Mockito.Mock<HistoryFileManager.HistoryFileInfo
				>();
			Org.Mockito.Mockito.When(info.GetConfFile()).ThenReturn(fullConfPath);
			completedJob = new CompletedJob(conf, jobId, fullHistoryPath, loadTasks, "user", 
				info, jobAclsManager);
			TaskCompletionEvent[] events = completedJob.GetMapAttemptCompletionEvents(0, 1000
				);
			NUnit.Framework.Assert.AreEqual(10, completedJob.GetMapAttemptCompletionEvents(0, 
				10).Length);
			int currentEventId = 0;
			foreach (TaskCompletionEvent taskAttemptCompletionEvent in events)
			{
				int eventId = taskAttemptCompletionEvent.GetEventId();
				NUnit.Framework.Assert.IsTrue(eventId >= currentEventId);
				currentEventId = eventId;
			}
			NUnit.Framework.Assert.IsNull(completedJob.LoadConfFile());
			// job name
			NUnit.Framework.Assert.AreEqual("Sleep job", completedJob.GetName());
			// queue name
			NUnit.Framework.Assert.AreEqual("default", completedJob.GetQueueName());
			// progress
			NUnit.Framework.Assert.AreEqual(1.0, completedJob.GetProgress(), 0.001);
			// 12 rows in answer
			NUnit.Framework.Assert.AreEqual(12, completedJob.GetTaskAttemptCompletionEvents(0
				, 1000).Length);
			// select first 10 rows
			NUnit.Framework.Assert.AreEqual(10, completedJob.GetTaskAttemptCompletionEvents(0
				, 10).Length);
			// select 5-10 rows include 5th
			NUnit.Framework.Assert.AreEqual(7, completedJob.GetTaskAttemptCompletionEvents(5, 
				10).Length);
			// without errors
			NUnit.Framework.Assert.AreEqual(1, completedJob.GetDiagnostics().Count);
			NUnit.Framework.Assert.AreEqual(string.Empty, completedJob.GetDiagnostics()[0]);
			NUnit.Framework.Assert.AreEqual(0, completedJob.GetJobACLs().Count);
		}
	}
}
