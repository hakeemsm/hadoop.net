using System.Collections.Generic;
using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Mapreduce.V2.App;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Webapp.View;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Webapp
{
	public class TestBlocks
	{
		private ByteArrayOutputStream data = new ByteArrayOutputStream();

		/// <summary>Test rendering for ConfBlock</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestConfigurationBlock()
		{
			AppContext ctx = Org.Mockito.Mockito.Mock<AppContext>();
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job
				>();
			Path path = new Path("conf");
			Configuration configuration = new Configuration();
			configuration.Set("Key for test", "Value for test");
			Org.Mockito.Mockito.When(job.GetConfFile()).ThenReturn(path);
			Org.Mockito.Mockito.When(job.LoadConfFile()).ThenReturn(configuration);
			Org.Mockito.Mockito.When(ctx.GetJob(Any<JobId>())).ThenReturn(job);
			TestBlocks.ConfBlockForTest configurationBlock = new TestBlocks.ConfBlockForTest(
				this, ctx);
			PrintWriter pWriter = new PrintWriter(data);
			HtmlBlock.Block html = new BlockForTest(new TestBlocks.HtmlBlockForTest(this), pWriter
				, 0, false);
			configurationBlock.Render(html);
			pWriter.Flush();
			NUnit.Framework.Assert.IsTrue(data.ToString().Contains("Sorry, can't do anything without a JobID"
				));
			configurationBlock.AddParameter(AMParams.JobId, "job_01_01");
			data.Reset();
			configurationBlock.Render(html);
			pWriter.Flush();
			NUnit.Framework.Assert.IsTrue(data.ToString().Contains("Key for test"));
			NUnit.Framework.Assert.IsTrue(data.ToString().Contains("Value for test"));
		}

		/// <summary>Test rendering for TasksBlock</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestTasksBlock()
		{
			ApplicationId appId = ApplicationIdPBImpl.NewInstance(0, 1);
			JobId jobId = new JobIdPBImpl();
			jobId.SetId(0);
			jobId.SetAppId(appId);
			TaskId taskId = new TaskIdPBImpl();
			taskId.SetId(0);
			taskId.SetTaskType(TaskType.Map);
			taskId.SetJobId(jobId);
			Task task = Org.Mockito.Mockito.Mock<Task>();
			Org.Mockito.Mockito.When(task.GetID()).ThenReturn(taskId);
			TaskReport report = Org.Mockito.Mockito.Mock<TaskReport>();
			Org.Mockito.Mockito.When(report.GetProgress()).ThenReturn(0.7f);
			Org.Mockito.Mockito.When(report.GetTaskState()).ThenReturn(TaskState.Succeeded);
			Org.Mockito.Mockito.When(report.GetStartTime()).ThenReturn(100001L);
			Org.Mockito.Mockito.When(report.GetFinishTime()).ThenReturn(100011L);
			Org.Mockito.Mockito.When(report.GetStatus()).ThenReturn("Dummy Status \n*");
			Org.Mockito.Mockito.When(task.GetReport()).ThenReturn(report);
			Org.Mockito.Mockito.When(task.GetType()).ThenReturn(TaskType.Map);
			IDictionary<TaskId, Task> tasks = new Dictionary<TaskId, Task>();
			tasks[taskId] = task;
			AppContext ctx = Org.Mockito.Mockito.Mock<AppContext>();
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job
				>();
			Org.Mockito.Mockito.When(job.GetTasks()).ThenReturn(tasks);
			Org.Apache.Hadoop.Mapreduce.V2.App.Webapp.App app = new Org.Apache.Hadoop.Mapreduce.V2.App.Webapp.App
				(ctx);
			app.SetJob(job);
			TasksBlockForTest taskBlock = new TasksBlockForTest(app);
			taskBlock.AddParameter(AMParams.TaskType, "m");
			PrintWriter pWriter = new PrintWriter(data);
			HtmlBlock.Block html = new BlockForTest(new TestBlocks.HtmlBlockForTest(this), pWriter
				, 0, false);
			taskBlock.Render(html);
			pWriter.Flush();
			NUnit.Framework.Assert.IsTrue(data.ToString().Contains("task_0_0001_m_000000"));
			NUnit.Framework.Assert.IsTrue(data.ToString().Contains("70.00"));
			NUnit.Framework.Assert.IsTrue(data.ToString().Contains("SUCCEEDED"));
			NUnit.Framework.Assert.IsTrue(data.ToString().Contains("100001"));
			NUnit.Framework.Assert.IsTrue(data.ToString().Contains("100011"));
			NUnit.Framework.Assert.IsFalse(data.ToString().Contains("Dummy Status \n*"));
			NUnit.Framework.Assert.IsTrue(data.ToString().Contains("Dummy Status \\n*"));
		}

		private class ConfBlockForTest : ConfBlock
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

			internal ConfBlockForTest(TestBlocks _enclosing, AppContext appCtx)
				: base(appCtx)
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
