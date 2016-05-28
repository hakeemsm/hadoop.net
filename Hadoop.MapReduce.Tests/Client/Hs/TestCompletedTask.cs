using System.Collections.Generic;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Jobhistory;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.HS
{
	public class TestCompletedTask
	{
		public virtual void TestTaskStartTimes()
		{
			TaskId taskId = Org.Mockito.Mockito.Mock<TaskId>();
			JobHistoryParser.TaskInfo taskInfo = Org.Mockito.Mockito.Mock<JobHistoryParser.TaskInfo
				>();
			IDictionary<TaskAttemptID, JobHistoryParser.TaskAttemptInfo> taskAttempts = new SortedDictionary
				<TaskAttemptID, JobHistoryParser.TaskAttemptInfo>();
			TaskAttemptID id = new TaskAttemptID("0", 0, TaskType.Map, 0, 0);
			JobHistoryParser.TaskAttemptInfo info = Org.Mockito.Mockito.Mock<JobHistoryParser.TaskAttemptInfo
				>();
			Org.Mockito.Mockito.When(info.GetAttemptId()).ThenReturn(id);
			Org.Mockito.Mockito.When(info.GetStartTime()).ThenReturn(10l);
			taskAttempts[id] = info;
			id = new TaskAttemptID("1", 0, TaskType.Map, 1, 1);
			info = Org.Mockito.Mockito.Mock<JobHistoryParser.TaskAttemptInfo>();
			Org.Mockito.Mockito.When(info.GetAttemptId()).ThenReturn(id);
			Org.Mockito.Mockito.When(info.GetStartTime()).ThenReturn(20l);
			taskAttempts[id] = info;
			Org.Mockito.Mockito.When(taskInfo.GetAllTaskAttempts()).ThenReturn(taskAttempts);
			CompletedTask task = new CompletedTask(taskId, taskInfo);
			TaskReport report = task.GetReport();
			// Make sure the startTime returned by report is the lesser of the 
			// attempy launch times
			NUnit.Framework.Assert.IsTrue(report.GetStartTime() == 10);
		}

		/// <summary>test some methods of CompletedTaskAttempt</summary>
		public virtual void TestCompletedTaskAttempt()
		{
			JobHistoryParser.TaskAttemptInfo attemptInfo = Org.Mockito.Mockito.Mock<JobHistoryParser.TaskAttemptInfo
				>();
			Org.Mockito.Mockito.When(attemptInfo.GetRackname()).ThenReturn("Rackname");
			Org.Mockito.Mockito.When(attemptInfo.GetShuffleFinishTime()).ThenReturn(11L);
			Org.Mockito.Mockito.When(attemptInfo.GetSortFinishTime()).ThenReturn(12L);
			Org.Mockito.Mockito.When(attemptInfo.GetShufflePort()).ThenReturn(10);
			JobID jobId = new JobID("12345", 0);
			TaskID taskId = new TaskID(jobId, TaskType.Reduce, 0);
			TaskAttemptID taskAttemptId = new TaskAttemptID(taskId, 0);
			Org.Mockito.Mockito.When(attemptInfo.GetAttemptId()).ThenReturn(taskAttemptId);
			CompletedTaskAttempt taskAttemt = new CompletedTaskAttempt(null, attemptInfo);
			NUnit.Framework.Assert.AreEqual("Rackname", taskAttemt.GetNodeRackName());
			NUnit.Framework.Assert.AreEqual(Phase.Cleanup, taskAttemt.GetPhase());
			NUnit.Framework.Assert.IsTrue(taskAttemt.IsFinished());
			NUnit.Framework.Assert.AreEqual(11L, taskAttemt.GetShuffleFinishTime());
			NUnit.Framework.Assert.AreEqual(12L, taskAttemt.GetSortFinishTime());
			NUnit.Framework.Assert.AreEqual(10, taskAttemt.GetShufflePort());
		}
	}
}
