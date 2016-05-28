using System.Collections.Generic;
using Org.Apache.Commons.Lang;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job;
using Org.Apache.Hadoop.Mapreduce.V2.Util;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Webapp;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Webapp
{
	public class TestAppController
	{
		private AppControllerForTest appController;

		private Controller.RequestContext ctx;

		private Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job;

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			AppContext context = Org.Mockito.Mockito.Mock<AppContext>();
			Org.Mockito.Mockito.When(context.GetApplicationID()).ThenReturn(ApplicationId.NewInstance
				(0, 0));
			Org.Mockito.Mockito.When(context.GetApplicationName()).ThenReturn("AppName");
			Org.Mockito.Mockito.When(context.GetUser()).ThenReturn("User");
			Org.Mockito.Mockito.When(context.GetStartTime()).ThenReturn(Runtime.CurrentTimeMillis
				());
			job = Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job>();
			Task task = Org.Mockito.Mockito.Mock<Task>();
			Org.Mockito.Mockito.When(job.GetTask(Any<TaskId>())).ThenReturn(task);
			JobId jobID = MRApps.ToJobID("job_01_01");
			Org.Mockito.Mockito.When(context.GetJob(jobID)).ThenReturn(job);
			Org.Mockito.Mockito.When(job.CheckAccess(Any<UserGroupInformation>(), Any<JobACL>
				())).ThenReturn(true);
			Org.Apache.Hadoop.Mapreduce.V2.App.Webapp.App app = new Org.Apache.Hadoop.Mapreduce.V2.App.Webapp.App
				(context);
			Configuration configuration = new Configuration();
			ctx = Org.Mockito.Mockito.Mock<Controller.RequestContext>();
			appController = new AppControllerForTest(app, configuration, ctx);
			appController.GetProperty()[AMParams.JobId] = "job_01_01";
			appController.GetProperty()[AMParams.TaskId] = "task_01_01_m01_01";
		}

		/// <summary>test bad request should be status 400...</summary>
		[NUnit.Framework.Test]
		public virtual void TestBadRequest()
		{
			string message = "test string";
			appController.BadRequest(message);
			VerifyExpectations(message);
		}

		[NUnit.Framework.Test]
		public virtual void TestBadRequestWithNullMessage()
		{
			// It should not throw NullPointerException
			appController.BadRequest(null);
			VerifyExpectations(StringUtils.Empty);
		}

		private void VerifyExpectations(string message)
		{
			Org.Mockito.Mockito.Verify(ctx).SetStatus(400);
			NUnit.Framework.Assert.AreEqual("application_0_0000", appController.GetProperty()
				["app.id"]);
			NUnit.Framework.Assert.IsNotNull(appController.GetProperty()["rm.web"]);
			NUnit.Framework.Assert.AreEqual("Bad request: " + message, appController.GetProperty
				()["title"]);
		}

		/// <summary>Test the method 'info'.</summary>
		[NUnit.Framework.Test]
		public virtual void TestInfo()
		{
			appController.Info();
			IEnumerator<ResponseInfo.Item> iterator = appController.GetResponseInfo().GetEnumerator
				();
			ResponseInfo.Item item = iterator.Next();
			NUnit.Framework.Assert.AreEqual("Application ID:", item.key);
			NUnit.Framework.Assert.AreEqual("application_0_0000", item.value);
			item = iterator.Next();
			NUnit.Framework.Assert.AreEqual("Application Name:", item.key);
			NUnit.Framework.Assert.AreEqual("AppName", item.value);
			item = iterator.Next();
			NUnit.Framework.Assert.AreEqual("User:", item.key);
			NUnit.Framework.Assert.AreEqual("User", item.value);
			item = iterator.Next();
			NUnit.Framework.Assert.AreEqual("Started on:", item.key);
			item = iterator.Next();
			NUnit.Framework.Assert.AreEqual("Elasped: ", item.key);
		}

		/// <summary>Test method 'job'.</summary>
		/// <remarks>Test method 'job'. Should print message about error or set JobPage class for rendering
		/// 	</remarks>
		[NUnit.Framework.Test]
		public virtual void TestGetJob()
		{
			Org.Mockito.Mockito.When(job.CheckAccess(Any<UserGroupInformation>(), Any<JobACL>
				())).ThenReturn(false);
			appController.Job();
			Org.Mockito.Mockito.Verify(appController.Response()).SetContentType(MimeType.Text
				);
			NUnit.Framework.Assert.AreEqual("Access denied: User user does not have permission to view job job_01_01"
				, appController.GetData());
			Org.Mockito.Mockito.When(job.CheckAccess(Any<UserGroupInformation>(), Any<JobACL>
				())).ThenReturn(true);
			Sharpen.Collections.Remove(appController.GetProperty(), AMParams.JobId);
			appController.Job();
			NUnit.Framework.Assert.AreEqual("Access denied: User user does not have permission to view job job_01_01Bad Request: Missing job ID"
				, appController.GetData());
			appController.GetProperty()[AMParams.JobId] = "job_01_01";
			appController.Job();
			NUnit.Framework.Assert.AreEqual(typeof(JobPage), appController.GetClazz());
		}

		/// <summary>Test method 'jobCounters'.</summary>
		/// <remarks>Test method 'jobCounters'. Should print message about error or set CountersPage class for rendering
		/// 	</remarks>
		[NUnit.Framework.Test]
		public virtual void TestGetJobCounters()
		{
			Org.Mockito.Mockito.When(job.CheckAccess(Any<UserGroupInformation>(), Any<JobACL>
				())).ThenReturn(false);
			appController.JobCounters();
			Org.Mockito.Mockito.Verify(appController.Response()).SetContentType(MimeType.Text
				);
			NUnit.Framework.Assert.AreEqual("Access denied: User user does not have permission to view job job_01_01"
				, appController.GetData());
			Org.Mockito.Mockito.When(job.CheckAccess(Any<UserGroupInformation>(), Any<JobACL>
				())).ThenReturn(true);
			Sharpen.Collections.Remove(appController.GetProperty(), AMParams.JobId);
			appController.JobCounters();
			NUnit.Framework.Assert.AreEqual("Access denied: User user does not have permission to view job job_01_01Bad Request: Missing job ID"
				, appController.GetData());
			appController.GetProperty()[AMParams.JobId] = "job_01_01";
			appController.JobCounters();
			NUnit.Framework.Assert.AreEqual(typeof(CountersPage), appController.GetClazz());
		}

		/// <summary>Test method 'taskCounters'.</summary>
		/// <remarks>Test method 'taskCounters'. Should print message about error or set CountersPage class for rendering
		/// 	</remarks>
		[NUnit.Framework.Test]
		public virtual void TestGetTaskCounters()
		{
			Org.Mockito.Mockito.When(job.CheckAccess(Any<UserGroupInformation>(), Any<JobACL>
				())).ThenReturn(false);
			appController.TaskCounters();
			Org.Mockito.Mockito.Verify(appController.Response()).SetContentType(MimeType.Text
				);
			NUnit.Framework.Assert.AreEqual("Access denied: User user does not have permission to view job job_01_01"
				, appController.GetData());
			Org.Mockito.Mockito.When(job.CheckAccess(Any<UserGroupInformation>(), Any<JobACL>
				())).ThenReturn(true);
			Sharpen.Collections.Remove(appController.GetProperty(), AMParams.TaskId);
			appController.TaskCounters();
			NUnit.Framework.Assert.AreEqual("Access denied: User user does not have permission to view job job_01_01missing task ID"
				, appController.GetData());
			appController.GetProperty()[AMParams.TaskId] = "task_01_01_m01_01";
			appController.TaskCounters();
			NUnit.Framework.Assert.AreEqual(typeof(CountersPage), appController.GetClazz());
		}

		/// <summary>Test method 'singleJobCounter'.</summary>
		/// <remarks>Test method 'singleJobCounter'. Should set SingleCounterPage class for rendering
		/// 	</remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestGetSingleJobCounter()
		{
			appController.SingleJobCounter();
			NUnit.Framework.Assert.AreEqual(typeof(SingleCounterPage), appController.GetClazz
				());
		}

		/// <summary>Test method 'singleTaskCounter'.</summary>
		/// <remarks>Test method 'singleTaskCounter'. Should set SingleCounterPage class for rendering
		/// 	</remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestGetSingleTaskCounter()
		{
			appController.SingleTaskCounter();
			NUnit.Framework.Assert.AreEqual(typeof(SingleCounterPage), appController.GetClazz
				());
			NUnit.Framework.Assert.IsNotNull(appController.GetProperty()[AppController.CounterGroup
				]);
			NUnit.Framework.Assert.IsNotNull(appController.GetProperty()[AppController.CounterName
				]);
		}

		/// <summary>Test method 'tasks'.</summary>
		/// <remarks>Test method 'tasks'. Should set TasksPage class for rendering</remarks>
		[NUnit.Framework.Test]
		public virtual void TestTasks()
		{
			appController.Tasks();
			NUnit.Framework.Assert.AreEqual(typeof(TasksPage), appController.GetClazz());
		}

		/// <summary>Test method 'task'.</summary>
		/// <remarks>Test method 'task'. Should set TaskPage class for rendering and information for title
		/// 	</remarks>
		[NUnit.Framework.Test]
		public virtual void TestTask()
		{
			appController.Task();
			NUnit.Framework.Assert.AreEqual("Attempts for task_01_01_m01_01", appController.GetProperty
				()["title"]);
			NUnit.Framework.Assert.AreEqual(typeof(TaskPage), appController.GetClazz());
		}

		/// <summary>Test method 'conf'.</summary>
		/// <remarks>Test method 'conf'. Should set JobConfPage class for rendering</remarks>
		[NUnit.Framework.Test]
		public virtual void TestConfiguration()
		{
			appController.Conf();
			NUnit.Framework.Assert.AreEqual(typeof(JobConfPage), appController.GetClazz());
		}

		/// <summary>Test method 'conf'.</summary>
		/// <remarks>Test method 'conf'. Should set AttemptsPage class for rendering or print information about error
		/// 	</remarks>
		[NUnit.Framework.Test]
		public virtual void TestAttempts()
		{
			Sharpen.Collections.Remove(appController.GetProperty(), AMParams.TaskType);
			Org.Mockito.Mockito.When(job.CheckAccess(Any<UserGroupInformation>(), Any<JobACL>
				())).ThenReturn(false);
			appController.Attempts();
			Org.Mockito.Mockito.Verify(appController.Response()).SetContentType(MimeType.Text
				);
			NUnit.Framework.Assert.AreEqual("Access denied: User user does not have permission to view job job_01_01"
				, appController.GetData());
			Org.Mockito.Mockito.When(job.CheckAccess(Any<UserGroupInformation>(), Any<JobACL>
				())).ThenReturn(true);
			Sharpen.Collections.Remove(appController.GetProperty(), AMParams.TaskId);
			appController.Attempts();
			NUnit.Framework.Assert.AreEqual("Access denied: User user does not have permission to view job job_01_01"
				, appController.GetData());
			appController.GetProperty()[AMParams.TaskId] = "task_01_01_m01_01";
			appController.Attempts();
			NUnit.Framework.Assert.AreEqual("Bad request: missing task-type.", appController.
				GetProperty()["title"]);
			appController.GetProperty()[AMParams.TaskType] = "m";
			appController.Attempts();
			NUnit.Framework.Assert.AreEqual("Bad request: missing attempt-state.", appController
				.GetProperty()["title"]);
			appController.GetProperty()[AMParams.AttemptState] = "State";
			appController.Attempts();
			NUnit.Framework.Assert.AreEqual(typeof(AttemptsPage), appController.GetClazz());
		}
	}
}
