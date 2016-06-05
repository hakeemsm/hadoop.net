using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App
{
	public class TestAMInfos
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAMInfosWithoutRecoveryEnabled()
		{
			int runCount = 0;
			MRApp app = new TestRecovery.MRAppWithHistory(1, 0, false, this.GetType().FullName
				, true, ++runCount);
			Configuration conf = new Configuration();
			conf.SetBoolean(MRJobConfig.JobUbertaskEnable, false);
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = app.Submit(conf);
			app.WaitForState(job, JobState.Running);
			long am1StartTime = app.GetAllAMInfos()[0].GetStartTime();
			NUnit.Framework.Assert.AreEqual("No of tasks not correct", 1, job.GetTasks().Count
				);
			IEnumerator<Task> it = job.GetTasks().Values.GetEnumerator();
			Task mapTask = it.Next();
			app.WaitForState(mapTask, TaskState.Running);
			TaskAttempt taskAttempt = mapTask.GetAttempts().Values.GetEnumerator().Next();
			app.WaitForState(taskAttempt, TaskAttemptState.Running);
			// stop the app
			app.Stop();
			// rerun
			app = new TestRecovery.MRAppWithHistory(1, 0, false, this.GetType().FullName, false
				, ++runCount);
			conf = new Configuration();
			// in rerun the AMInfo will be recovered from previous run even if recovery
			// is not enabled.
			conf.SetBoolean(MRJobConfig.MrAmJobRecoveryEnable, false);
			conf.SetBoolean(MRJobConfig.JobUbertaskEnable, false);
			job = app.Submit(conf);
			app.WaitForState(job, JobState.Running);
			NUnit.Framework.Assert.AreEqual("No of tasks not correct", 1, job.GetTasks().Count
				);
			it = job.GetTasks().Values.GetEnumerator();
			mapTask = it.Next();
			// There should be two AMInfos
			IList<AMInfo> amInfos = app.GetAllAMInfos();
			NUnit.Framework.Assert.AreEqual(2, amInfos.Count);
			AMInfo amInfoOne = amInfos[0];
			NUnit.Framework.Assert.AreEqual(am1StartTime, amInfoOne.GetStartTime());
			app.Stop();
		}
	}
}
