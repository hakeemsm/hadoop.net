using System;
using System.Collections.Generic;
using System.IO;
using Javax.Servlet.Http;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Mapreduce.V2.App;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job;
using Org.Apache.Hadoop.Mapreduce.V2.App.Webapp;
using Org.Apache.Hadoop.Mapreduce.V2.Util;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Webapp;
using Org.Apache.Hadoop.Yarn.Webapp.Log;
using Org.Apache.Hadoop.Yarn.Webapp.View;
using Org.Mockito;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.HS.Webapp
{
	/// <summary>Test some HtmlBlock classes</summary>
	public class TestBlocks
	{
		private ByteArrayOutputStream data = new ByteArrayOutputStream();

		/// <summary>test HsTasksBlock's rendering.</summary>
		[NUnit.Framework.Test]
		public virtual void TestHsTasksBlock()
		{
			Task task = GetTask(0);
			IDictionary<TaskId, Task> tasks = new Dictionary<TaskId, Task>();
			tasks[task.GetID()] = task;
			AppContext ctx = Org.Mockito.Mockito.Mock<AppContext>();
			AppForTest app = new AppForTest(ctx);
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job
				>();
			Org.Mockito.Mockito.When(job.GetTasks()).ThenReturn(tasks);
			app.SetJob(job);
			TestBlocks.HsTasksBlockForTest block = new TestBlocks.HsTasksBlockForTest(this, app
				);
			block.AddParameter(AMParams.TaskType, "r");
			PrintWriter pWriter = new PrintWriter(data);
			HtmlBlock.Block html = new BlockForTest(new TestBlocks.HtmlBlockForTest(this), pWriter
				, 0, false);
			block.Render(html);
			pWriter.Flush();
			// should be printed information about task
			NUnit.Framework.Assert.IsTrue(data.ToString().Contains("task_0_0001_r_000000"));
			NUnit.Framework.Assert.IsTrue(data.ToString().Contains("SUCCEEDED"));
			NUnit.Framework.Assert.IsTrue(data.ToString().Contains("100001"));
			NUnit.Framework.Assert.IsTrue(data.ToString().Contains("100011"));
			NUnit.Framework.Assert.IsTrue(data.ToString().Contains(string.Empty));
		}

		/// <summary>test AttemptsBlock's rendering.</summary>
		[NUnit.Framework.Test]
		public virtual void TestAttemptsBlock()
		{
			AppContext ctx = Org.Mockito.Mockito.Mock<AppContext>();
			AppForTest app = new AppForTest(ctx);
			Task task = GetTask(0);
			IDictionary<TaskAttemptId, TaskAttempt> attempts = new Dictionary<TaskAttemptId, 
				TaskAttempt>();
			TaskAttempt attempt = Org.Mockito.Mockito.Mock<TaskAttempt>();
			TaskAttemptId taId = new TaskAttemptIdPBImpl();
			taId.SetId(0);
			taId.SetTaskId(task.GetID());
			Org.Mockito.Mockito.When(attempt.GetID()).ThenReturn(taId);
			Org.Mockito.Mockito.When(attempt.GetNodeHttpAddress()).ThenReturn("Node address");
			ApplicationId appId = ApplicationIdPBImpl.NewInstance(0, 5);
			ApplicationAttemptId appAttemptId = ApplicationAttemptIdPBImpl.NewInstance(appId, 
				1);
			ContainerId containerId = ContainerIdPBImpl.NewContainerId(appAttemptId, 1);
			Org.Mockito.Mockito.When(attempt.GetAssignedContainerID()).ThenReturn(containerId
				);
			Org.Mockito.Mockito.When(attempt.GetAssignedContainerMgrAddress()).ThenReturn("assignedContainerMgrAddress"
				);
			Org.Mockito.Mockito.When(attempt.GetNodeRackName()).ThenReturn("nodeRackName");
			long taStartTime = 100002L;
			long taFinishTime = 100012L;
			long taShuffleFinishTime = 100010L;
			long taSortFinishTime = 100011L;
			TaskAttemptState taState = TaskAttemptState.Succeeded;
			Org.Mockito.Mockito.When(attempt.GetLaunchTime()).ThenReturn(taStartTime);
			Org.Mockito.Mockito.When(attempt.GetFinishTime()).ThenReturn(taFinishTime);
			Org.Mockito.Mockito.When(attempt.GetShuffleFinishTime()).ThenReturn(taShuffleFinishTime
				);
			Org.Mockito.Mockito.When(attempt.GetSortFinishTime()).ThenReturn(taSortFinishTime
				);
			Org.Mockito.Mockito.When(attempt.GetState()).ThenReturn(taState);
			TaskAttemptReport taReport = Org.Mockito.Mockito.Mock<TaskAttemptReport>();
			Org.Mockito.Mockito.When(taReport.GetStartTime()).ThenReturn(taStartTime);
			Org.Mockito.Mockito.When(taReport.GetFinishTime()).ThenReturn(taFinishTime);
			Org.Mockito.Mockito.When(taReport.GetShuffleFinishTime()).ThenReturn(taShuffleFinishTime
				);
			Org.Mockito.Mockito.When(taReport.GetSortFinishTime()).ThenReturn(taSortFinishTime
				);
			Org.Mockito.Mockito.When(taReport.GetContainerId()).ThenReturn(containerId);
			Org.Mockito.Mockito.When(taReport.GetProgress()).ThenReturn(1.0f);
			Org.Mockito.Mockito.When(taReport.GetStateString()).ThenReturn("Processed 128/128 records <p> \n"
				);
			Org.Mockito.Mockito.When(taReport.GetTaskAttemptState()).ThenReturn(taState);
			Org.Mockito.Mockito.When(taReport.GetDiagnosticInfo()).ThenReturn(string.Empty);
			Org.Mockito.Mockito.When(attempt.GetReport()).ThenReturn(taReport);
			attempts[taId] = attempt;
			Org.Mockito.Mockito.When(task.GetAttempts()).ThenReturn(attempts);
			app.SetTask(task);
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job
				>();
			Org.Mockito.Mockito.When(job.GetUserName()).ThenReturn("User");
			app.SetJob(job);
			TestBlocks.AttemptsBlockForTest block = new TestBlocks.AttemptsBlockForTest(this, 
				app);
			block.AddParameter(AMParams.TaskType, "r");
			PrintWriter pWriter = new PrintWriter(data);
			HtmlBlock.Block html = new BlockForTest(new TestBlocks.HtmlBlockForTest(this), pWriter
				, 0, false);
			block.Render(html);
			pWriter.Flush();
			// should be printed information about attempts
			NUnit.Framework.Assert.IsTrue(data.ToString().Contains("0 attempt_0_0001_r_000000_0"
				));
			NUnit.Framework.Assert.IsTrue(data.ToString().Contains("SUCCEEDED"));
			NUnit.Framework.Assert.IsFalse(data.ToString().Contains("Processed 128/128 records <p> \n"
				));
			NUnit.Framework.Assert.IsTrue(data.ToString().Contains("Processed 128\\/128 records &lt;p&gt; \\n"
				));
			NUnit.Framework.Assert.IsTrue(data.ToString().Contains("_0005_01_000001:attempt_0_0001_r_000000_0:User:"
				));
			NUnit.Framework.Assert.IsTrue(data.ToString().Contains("100002"));
			NUnit.Framework.Assert.IsTrue(data.ToString().Contains("100010"));
			NUnit.Framework.Assert.IsTrue(data.ToString().Contains("100011"));
			NUnit.Framework.Assert.IsTrue(data.ToString().Contains("100012"));
		}

		/// <summary>test HsJobsBlock's rendering.</summary>
		[NUnit.Framework.Test]
		public virtual void TestHsJobsBlock()
		{
			AppContext ctx = Org.Mockito.Mockito.Mock<AppContext>();
			IDictionary<JobId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job> jobs = new Dictionary
				<JobId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job>();
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = GetJob();
			jobs[job.GetID()] = job;
			Org.Mockito.Mockito.When(ctx.GetAllJobs()).ThenReturn(jobs);
			HsJobsBlock block = new TestBlocks.HsJobsBlockForTest(this, ctx);
			PrintWriter pWriter = new PrintWriter(data);
			HtmlBlock.Block html = new BlockForTest(new TestBlocks.HtmlBlockForTest(this), pWriter
				, 0, false);
			block.Render(html);
			pWriter.Flush();
			NUnit.Framework.Assert.IsTrue(data.ToString().Contains("JobName"));
			NUnit.Framework.Assert.IsTrue(data.ToString().Contains("UserName"));
			NUnit.Framework.Assert.IsTrue(data.ToString().Contains("QueueName"));
			NUnit.Framework.Assert.IsTrue(data.ToString().Contains("SUCCEEDED"));
		}

		/// <summary>test HsController</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestHsController()
		{
			AppContext ctx = Org.Mockito.Mockito.Mock<AppContext>();
			ApplicationId appId = ApplicationIdPBImpl.NewInstance(0, 5);
			Org.Mockito.Mockito.When(ctx.GetApplicationID()).ThenReturn(appId);
			AppForTest app = new AppForTest(ctx);
			Configuration config = new Configuration();
			Controller.RequestContext requestCtx = Org.Mockito.Mockito.Mock<Controller.RequestContext
				>();
			TestBlocks.HsControllerForTest controller = new TestBlocks.HsControllerForTest(app
				, config, requestCtx);
			controller.Index();
			NUnit.Framework.Assert.AreEqual("JobHistory", controller.Get(Params.Title, string.Empty
				));
			NUnit.Framework.Assert.AreEqual(typeof(HsJobPage), controller.JobPage());
			NUnit.Framework.Assert.AreEqual(typeof(HsCountersPage), controller.CountersPage()
				);
			NUnit.Framework.Assert.AreEqual(typeof(HsTasksPage), controller.TasksPage());
			NUnit.Framework.Assert.AreEqual(typeof(HsTaskPage), controller.TaskPage());
			NUnit.Framework.Assert.AreEqual(typeof(HsAttemptsPage), controller.AttemptsPage()
				);
			controller.Set(AMParams.JobId, "job_01_01");
			controller.Set(AMParams.TaskId, "task_01_01_m01_01");
			controller.Set(AMParams.TaskType, "m");
			controller.Set(AMParams.AttemptState, "State");
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job
				>();
			Task task = Org.Mockito.Mockito.Mock<Task>();
			Org.Mockito.Mockito.When(job.GetTask(Matchers.Any<TaskId>())).ThenReturn(task);
			JobId jobID = MRApps.ToJobID("job_01_01");
			Org.Mockito.Mockito.When(ctx.GetJob(jobID)).ThenReturn(job);
			Org.Mockito.Mockito.When(job.CheckAccess(Matchers.Any<UserGroupInformation>(), Matchers.Any
				<JobACL>())).ThenReturn(true);
			controller.Job();
			NUnit.Framework.Assert.AreEqual(typeof(HsJobPage), controller.GetClazz());
			controller.JobCounters();
			NUnit.Framework.Assert.AreEqual(typeof(HsCountersPage), controller.GetClazz());
			controller.TaskCounters();
			NUnit.Framework.Assert.AreEqual(typeof(HsCountersPage), controller.GetClazz());
			controller.Tasks();
			NUnit.Framework.Assert.AreEqual(typeof(HsTasksPage), controller.GetClazz());
			controller.Task();
			NUnit.Framework.Assert.AreEqual(typeof(HsTaskPage), controller.GetClazz());
			controller.Attempts();
			NUnit.Framework.Assert.AreEqual(typeof(HsAttemptsPage), controller.GetClazz());
			NUnit.Framework.Assert.AreEqual(typeof(HsConfPage), controller.ConfPage());
			NUnit.Framework.Assert.AreEqual(typeof(HsAboutPage), controller.AboutPage());
			controller.About();
			NUnit.Framework.Assert.AreEqual(typeof(HsAboutPage), controller.GetClazz());
			controller.Logs();
			NUnit.Framework.Assert.AreEqual(typeof(HsLogsPage), controller.GetClazz());
			controller.Nmlogs();
			NUnit.Framework.Assert.AreEqual(typeof(AggregatedLogsPage), controller.GetClazz()
				);
			NUnit.Framework.Assert.AreEqual(typeof(HsSingleCounterPage), controller.SingleCounterPage
				());
			controller.SingleJobCounter();
			NUnit.Framework.Assert.AreEqual(typeof(HsSingleCounterPage), controller.GetClazz(
				));
			controller.SingleTaskCounter();
			NUnit.Framework.Assert.AreEqual(typeof(HsSingleCounterPage), controller.GetClazz(
				));
		}

		private class HsControllerForTest : HsController
		{
			private static IDictionary<string, string> @params = new Dictionary<string, string
				>();

			private Type clazz;

			internal ByteArrayOutputStream data = new ByteArrayOutputStream();

			public override void Set(string name, string value)
			{
				@params[name] = value;
			}

			public override string Get(string key, string defaultValue)
			{
				string value = @params[key];
				return value == null ? defaultValue : value;
			}

			internal HsControllerForTest(Org.Apache.Hadoop.Mapreduce.V2.App.Webapp.App app, Configuration
				 configuration, Controller.RequestContext ctx)
				: base(app, configuration, ctx)
			{
			}

			public override HttpServletRequest Request()
			{
				HttpServletRequest result = Org.Mockito.Mockito.Mock<HttpServletRequest>();
				Org.Mockito.Mockito.When(result.GetRemoteUser()).ThenReturn("User");
				return result;
			}

			public override HttpServletResponse Response()
			{
				HttpServletResponse result = Org.Mockito.Mockito.Mock<HttpServletResponse>();
				try
				{
					Org.Mockito.Mockito.When(result.GetWriter()).ThenReturn(new PrintWriter(data));
				}
				catch (IOException)
				{
				}
				return result;
			}

			protected override void Render(Type cls)
			{
				clazz = cls;
			}

			public virtual Type GetClazz()
			{
				return clazz;
			}
		}

		private Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job GetJob()
		{
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job
				>();
			JobId jobId = new JobIdPBImpl();
			ApplicationId appId = ApplicationIdPBImpl.NewInstance(Runtime.CurrentTimeMillis()
				, 4);
			jobId.SetAppId(appId);
			jobId.SetId(1);
			Org.Mockito.Mockito.When(job.GetID()).ThenReturn(jobId);
			JobReport report = Org.Mockito.Mockito.Mock<JobReport>();
			Org.Mockito.Mockito.When(report.GetStartTime()).ThenReturn(100010L);
			Org.Mockito.Mockito.When(report.GetFinishTime()).ThenReturn(100015L);
			Org.Mockito.Mockito.When(job.GetReport()).ThenReturn(report);
			Org.Mockito.Mockito.When(job.GetName()).ThenReturn("JobName");
			Org.Mockito.Mockito.When(job.GetUserName()).ThenReturn("UserName");
			Org.Mockito.Mockito.When(job.GetQueueName()).ThenReturn("QueueName");
			Org.Mockito.Mockito.When(job.GetState()).ThenReturn(JobState.Succeeded);
			Org.Mockito.Mockito.When(job.GetTotalMaps()).ThenReturn(3);
			Org.Mockito.Mockito.When(job.GetCompletedMaps()).ThenReturn(2);
			Org.Mockito.Mockito.When(job.GetTotalReduces()).ThenReturn(2);
			Org.Mockito.Mockito.When(job.GetCompletedReduces()).ThenReturn(1);
			Org.Mockito.Mockito.When(job.GetCompletedReduces()).ThenReturn(1);
			return job;
		}

		private Task GetTask(long timestamp)
		{
			JobId jobId = new JobIdPBImpl();
			jobId.SetId(0);
			jobId.SetAppId(ApplicationIdPBImpl.NewInstance(timestamp, 1));
			TaskId taskId = new TaskIdPBImpl();
			taskId.SetId(0);
			taskId.SetTaskType(TaskType.Reduce);
			taskId.SetJobId(jobId);
			Task task = Org.Mockito.Mockito.Mock<Task>();
			Org.Mockito.Mockito.When(task.GetID()).ThenReturn(taskId);
			TaskReport report = Org.Mockito.Mockito.Mock<TaskReport>();
			Org.Mockito.Mockito.When(report.GetProgress()).ThenReturn(0.7f);
			Org.Mockito.Mockito.When(report.GetTaskState()).ThenReturn(TaskState.Succeeded);
			Org.Mockito.Mockito.When(report.GetStartTime()).ThenReturn(100001L);
			Org.Mockito.Mockito.When(report.GetFinishTime()).ThenReturn(100011L);
			Org.Mockito.Mockito.When(task.GetReport()).ThenReturn(report);
			Org.Mockito.Mockito.When(task.GetType()).ThenReturn(TaskType.Reduce);
			return task;
		}

		private class HsJobsBlockForTest : HsJobsBlock
		{
			internal HsJobsBlockForTest(TestBlocks _enclosing, AppContext appCtx)
				: base(appCtx)
			{
				this._enclosing = _enclosing;
			}

			public override string Url(params string[] parts)
			{
				string result = "url://";
				foreach (string @string in parts)
				{
					result += @string + ":";
				}
				return result;
			}

			private readonly TestBlocks _enclosing;
		}

		private class AttemptsBlockForTest : HsTaskPage.AttemptsBlock
		{
			private readonly IDictionary<string, string> @params = new Dictionary<string, string
				>();

			public virtual void AddParameter(string name, string value)
			{
				this.@params[name] = value;
			}

			public override string $(string key, string defaultValue)
			{
				string value = this.@params[key];
				return value == null ? defaultValue : value;
			}

			public AttemptsBlockForTest(TestBlocks _enclosing, Org.Apache.Hadoop.Mapreduce.V2.App.Webapp.App
				 ctx)
				: base(ctx)
			{
				this._enclosing = _enclosing;
			}

			public override string Url(params string[] parts)
			{
				string result = "url://";
				foreach (string @string in parts)
				{
					result += @string + ":";
				}
				return result;
			}

			private readonly TestBlocks _enclosing;
		}

		private class HsTasksBlockForTest : HsTasksBlock
		{
			private readonly IDictionary<string, string> @params = new Dictionary<string, string
				>();

			public virtual void AddParameter(string name, string value)
			{
				this.@params[name] = value;
			}

			public override string $(string key, string defaultValue)
			{
				string value = this.@params[key];
				return value == null ? defaultValue : value;
			}

			public override string Url(params string[] parts)
			{
				string result = "url://";
				foreach (string @string in parts)
				{
					result += @string + ":";
				}
				return result;
			}

			public HsTasksBlockForTest(TestBlocks _enclosing, Org.Apache.Hadoop.Mapreduce.V2.App.Webapp.App
				 app)
				: base(app)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestBlocks _enclosing;
		}

		private class HtmlBlockForTest : HtmlBlock
		{
			protected override void Render(HtmlBlock.Block html)
			{
			}

			internal HtmlBlockForTest(TestBlocks _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestBlocks _enclosing;
		}
	}
}
