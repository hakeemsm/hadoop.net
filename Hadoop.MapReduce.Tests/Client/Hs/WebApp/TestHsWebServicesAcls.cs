using System.Collections.Generic;
using Javax.Servlet.Http;
using Javax.WS.RS;
using Javax.WS.RS.Core;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job;
using Org.Apache.Hadoop.Mapreduce.V2.HS;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authorize;
using Org.Apache.Hadoop.Yarn.Webapp;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.HS.Webapp
{
	public class TestHsWebServicesAcls
	{
		private static string FriendlyUser = "friendly";

		private static string EnemyUser = "enemy";

		private JobConf conf;

		private HistoryContext ctx;

		private string jobIdStr;

		private string taskIdStr;

		private string taskAttemptIdStr;

		private HsWebServices hsWebServices;

		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public virtual void Setup()
		{
			this.conf = new JobConf();
			this.conf.Set(CommonConfigurationKeys.HadoopSecurityGroupMapping, typeof(TestHsWebServicesAcls.NullGroupsProvider
				).FullName);
			this.conf.SetBoolean(MRConfig.MrAclsEnabled, true);
			Groups.GetUserToGroupsMappingService(conf);
			this.ctx = BuildHistoryContext(this.conf);
			WebApp webApp = Org.Mockito.Mockito.Mock<HsWebApp>();
			Org.Mockito.Mockito.When(webApp.Name()).ThenReturn("hsmockwebapp");
			this.hsWebServices = new HsWebServices(ctx, conf, webApp);
			this.hsWebServices.SetResponse(Org.Mockito.Mockito.Mock<HttpServletResponse>());
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = ctx.GetAllJobs().Values.GetEnumerator
				().Next();
			this.jobIdStr = job.GetID().ToString();
			Task task = job.GetTasks().Values.GetEnumerator().Next();
			this.taskIdStr = task.GetID().ToString();
			this.taskAttemptIdStr = task.GetAttempts().Keys.GetEnumerator().Next().ToString();
		}

		[NUnit.Framework.Test]
		public virtual void TestGetJobAcls()
		{
			HttpServletRequest hsr = Org.Mockito.Mockito.Mock<HttpServletRequest>();
			Org.Mockito.Mockito.When(hsr.GetRemoteUser()).ThenReturn(EnemyUser);
			try
			{
				hsWebServices.GetJob(hsr, jobIdStr);
				NUnit.Framework.Assert.Fail("enemy can access job");
			}
			catch (WebApplicationException e)
			{
				NUnit.Framework.Assert.AreEqual(Response.Status.Unauthorized, Response.Status.FromStatusCode
					(e.GetResponse().GetStatus()));
			}
			Org.Mockito.Mockito.When(hsr.GetRemoteUser()).ThenReturn(FriendlyUser);
			hsWebServices.GetJob(hsr, jobIdStr);
		}

		[NUnit.Framework.Test]
		public virtual void TestGetJobCountersAcls()
		{
			HttpServletRequest hsr = Org.Mockito.Mockito.Mock<HttpServletRequest>();
			Org.Mockito.Mockito.When(hsr.GetRemoteUser()).ThenReturn(EnemyUser);
			try
			{
				hsWebServices.GetJobCounters(hsr, jobIdStr);
				NUnit.Framework.Assert.Fail("enemy can access job");
			}
			catch (WebApplicationException e)
			{
				NUnit.Framework.Assert.AreEqual(Response.Status.Unauthorized, Response.Status.FromStatusCode
					(e.GetResponse().GetStatus()));
			}
			Org.Mockito.Mockito.When(hsr.GetRemoteUser()).ThenReturn(FriendlyUser);
			hsWebServices.GetJobCounters(hsr, jobIdStr);
		}

		[NUnit.Framework.Test]
		public virtual void TestGetJobConfAcls()
		{
			HttpServletRequest hsr = Org.Mockito.Mockito.Mock<HttpServletRequest>();
			Org.Mockito.Mockito.When(hsr.GetRemoteUser()).ThenReturn(EnemyUser);
			try
			{
				hsWebServices.GetJobConf(hsr, jobIdStr);
				NUnit.Framework.Assert.Fail("enemy can access job");
			}
			catch (WebApplicationException e)
			{
				NUnit.Framework.Assert.AreEqual(Response.Status.Unauthorized, Response.Status.FromStatusCode
					(e.GetResponse().GetStatus()));
			}
			Org.Mockito.Mockito.When(hsr.GetRemoteUser()).ThenReturn(FriendlyUser);
			hsWebServices.GetJobConf(hsr, jobIdStr);
		}

		[NUnit.Framework.Test]
		public virtual void TestGetJobTasksAcls()
		{
			HttpServletRequest hsr = Org.Mockito.Mockito.Mock<HttpServletRequest>();
			Org.Mockito.Mockito.When(hsr.GetRemoteUser()).ThenReturn(EnemyUser);
			try
			{
				hsWebServices.GetJobTasks(hsr, jobIdStr, "m");
				NUnit.Framework.Assert.Fail("enemy can access job");
			}
			catch (WebApplicationException e)
			{
				NUnit.Framework.Assert.AreEqual(Response.Status.Unauthorized, Response.Status.FromStatusCode
					(e.GetResponse().GetStatus()));
			}
			Org.Mockito.Mockito.When(hsr.GetRemoteUser()).ThenReturn(FriendlyUser);
			hsWebServices.GetJobTasks(hsr, jobIdStr, "m");
		}

		[NUnit.Framework.Test]
		public virtual void TestGetJobTaskAcls()
		{
			HttpServletRequest hsr = Org.Mockito.Mockito.Mock<HttpServletRequest>();
			Org.Mockito.Mockito.When(hsr.GetRemoteUser()).ThenReturn(EnemyUser);
			try
			{
				hsWebServices.GetJobTask(hsr, jobIdStr, this.taskIdStr);
				NUnit.Framework.Assert.Fail("enemy can access job");
			}
			catch (WebApplicationException e)
			{
				NUnit.Framework.Assert.AreEqual(Response.Status.Unauthorized, Response.Status.FromStatusCode
					(e.GetResponse().GetStatus()));
			}
			Org.Mockito.Mockito.When(hsr.GetRemoteUser()).ThenReturn(FriendlyUser);
			hsWebServices.GetJobTask(hsr, this.jobIdStr, this.taskIdStr);
		}

		[NUnit.Framework.Test]
		public virtual void TestGetSingleTaskCountersAcls()
		{
			HttpServletRequest hsr = Org.Mockito.Mockito.Mock<HttpServletRequest>();
			Org.Mockito.Mockito.When(hsr.GetRemoteUser()).ThenReturn(EnemyUser);
			try
			{
				hsWebServices.GetSingleTaskCounters(hsr, this.jobIdStr, this.taskIdStr);
				NUnit.Framework.Assert.Fail("enemy can access job");
			}
			catch (WebApplicationException e)
			{
				NUnit.Framework.Assert.AreEqual(Response.Status.Unauthorized, Response.Status.FromStatusCode
					(e.GetResponse().GetStatus()));
			}
			Org.Mockito.Mockito.When(hsr.GetRemoteUser()).ThenReturn(FriendlyUser);
			hsWebServices.GetSingleTaskCounters(hsr, this.jobIdStr, this.taskIdStr);
		}

		[NUnit.Framework.Test]
		public virtual void TestGetJobTaskAttemptsAcls()
		{
			HttpServletRequest hsr = Org.Mockito.Mockito.Mock<HttpServletRequest>();
			Org.Mockito.Mockito.When(hsr.GetRemoteUser()).ThenReturn(EnemyUser);
			try
			{
				hsWebServices.GetJobTaskAttempts(hsr, this.jobIdStr, this.taskIdStr);
				NUnit.Framework.Assert.Fail("enemy can access job");
			}
			catch (WebApplicationException e)
			{
				NUnit.Framework.Assert.AreEqual(Response.Status.Unauthorized, Response.Status.FromStatusCode
					(e.GetResponse().GetStatus()));
			}
			Org.Mockito.Mockito.When(hsr.GetRemoteUser()).ThenReturn(FriendlyUser);
			hsWebServices.GetJobTaskAttempts(hsr, this.jobIdStr, this.taskIdStr);
		}

		[NUnit.Framework.Test]
		public virtual void TestGetJobTaskAttemptIdAcls()
		{
			HttpServletRequest hsr = Org.Mockito.Mockito.Mock<HttpServletRequest>();
			Org.Mockito.Mockito.When(hsr.GetRemoteUser()).ThenReturn(EnemyUser);
			try
			{
				hsWebServices.GetJobTaskAttemptId(hsr, this.jobIdStr, this.taskIdStr, this.taskAttemptIdStr
					);
				NUnit.Framework.Assert.Fail("enemy can access job");
			}
			catch (WebApplicationException e)
			{
				NUnit.Framework.Assert.AreEqual(Response.Status.Unauthorized, Response.Status.FromStatusCode
					(e.GetResponse().GetStatus()));
			}
			Org.Mockito.Mockito.When(hsr.GetRemoteUser()).ThenReturn(FriendlyUser);
			hsWebServices.GetJobTaskAttemptId(hsr, this.jobIdStr, this.taskIdStr, this.taskAttemptIdStr
				);
		}

		[NUnit.Framework.Test]
		public virtual void TestGetJobTaskAttemptIdCountersAcls()
		{
			HttpServletRequest hsr = Org.Mockito.Mockito.Mock<HttpServletRequest>();
			Org.Mockito.Mockito.When(hsr.GetRemoteUser()).ThenReturn(EnemyUser);
			try
			{
				hsWebServices.GetJobTaskAttemptIdCounters(hsr, this.jobIdStr, this.taskIdStr, this
					.taskAttemptIdStr);
				NUnit.Framework.Assert.Fail("enemy can access job");
			}
			catch (WebApplicationException e)
			{
				NUnit.Framework.Assert.AreEqual(Response.Status.Unauthorized, Response.Status.FromStatusCode
					(e.GetResponse().GetStatus()));
			}
			Org.Mockito.Mockito.When(hsr.GetRemoteUser()).ThenReturn(FriendlyUser);
			hsWebServices.GetJobTaskAttemptIdCounters(hsr, this.jobIdStr, this.taskIdStr, this
				.taskAttemptIdStr);
		}

		/// <exception cref="System.IO.IOException"/>
		private static HistoryContext BuildHistoryContext(Configuration conf)
		{
			HistoryContext ctx = new MockHistoryContext(1, 1, 1);
			IDictionary<JobId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job> jobs = ctx.GetAllJobs
				();
			JobId jobId = jobs.Keys.GetEnumerator().Next();
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job mockJob = new TestHsWebServicesAcls.MockJobForAcls
				(jobs[jobId], conf);
			jobs[jobId] = mockJob;
			return ctx;
		}

		private class NullGroupsProvider : GroupMappingServiceProvider
		{
			/// <exception cref="System.IO.IOException"/>
			public override IList<string> GetGroups(string user)
			{
				return Sharpen.Collections.EmptyList();
			}

			/// <exception cref="System.IO.IOException"/>
			public override void CacheGroupsRefresh()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override void CacheGroupsAdd(IList<string> groups)
			{
			}
		}

		private class MockJobForAcls : Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job
		{
			private Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job mockJob;

			private Configuration conf;

			private IDictionary<JobACL, AccessControlList> jobAcls;

			private JobACLsManager aclsMgr;

			public MockJobForAcls(Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job mockJob, Configuration
				 conf)
			{
				this.mockJob = mockJob;
				this.conf = conf;
				AccessControlList viewAcl = new AccessControlList(FriendlyUser);
				this.jobAcls = new Dictionary<JobACL, AccessControlList>();
				this.jobAcls[JobACL.ViewJob] = viewAcl;
				this.aclsMgr = new JobACLsManager(conf);
			}

			public virtual JobId GetID()
			{
				return mockJob.GetID();
			}

			public virtual string GetName()
			{
				return mockJob.GetName();
			}

			public virtual JobState GetState()
			{
				return mockJob.GetState();
			}

			public virtual JobReport GetReport()
			{
				return mockJob.GetReport();
			}

			public virtual Counters GetAllCounters()
			{
				return mockJob.GetAllCounters();
			}

			public virtual IDictionary<TaskId, Task> GetTasks()
			{
				return mockJob.GetTasks();
			}

			public virtual IDictionary<TaskId, Task> GetTasks(TaskType taskType)
			{
				return mockJob.GetTasks(taskType);
			}

			public virtual Task GetTask(TaskId taskID)
			{
				return mockJob.GetTask(taskID);
			}

			public virtual IList<string> GetDiagnostics()
			{
				return mockJob.GetDiagnostics();
			}

			public virtual int GetTotalMaps()
			{
				return mockJob.GetTotalMaps();
			}

			public virtual int GetTotalReduces()
			{
				return mockJob.GetTotalReduces();
			}

			public virtual int GetCompletedMaps()
			{
				return mockJob.GetCompletedMaps();
			}

			public virtual int GetCompletedReduces()
			{
				return mockJob.GetCompletedReduces();
			}

			public virtual float GetProgress()
			{
				return mockJob.GetProgress();
			}

			public virtual bool IsUber()
			{
				return mockJob.IsUber();
			}

			public virtual string GetUserName()
			{
				return mockJob.GetUserName();
			}

			public virtual string GetQueueName()
			{
				return mockJob.GetQueueName();
			}

			public virtual Path GetConfFile()
			{
				return new Path("/some/path/to/conf");
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual Configuration LoadConfFile()
			{
				return conf;
			}

			public virtual IDictionary<JobACL, AccessControlList> GetJobACLs()
			{
				return jobAcls;
			}

			public virtual TaskAttemptCompletionEvent[] GetTaskAttemptCompletionEvents(int fromEventId
				, int maxEvents)
			{
				return mockJob.GetTaskAttemptCompletionEvents(fromEventId, maxEvents);
			}

			public virtual TaskCompletionEvent[] GetMapAttemptCompletionEvents(int startIndex
				, int maxEvents)
			{
				return mockJob.GetMapAttemptCompletionEvents(startIndex, maxEvents);
			}

			public virtual IList<AMInfo> GetAMInfos()
			{
				return mockJob.GetAMInfos();
			}

			public virtual bool CheckAccess(UserGroupInformation callerUGI, JobACL jobOperation
				)
			{
				return aclsMgr.CheckAccess(callerUGI, jobOperation, this.GetUserName(), jobAcls[jobOperation
					]);
			}

			public virtual void SetQueueName(string queueName)
			{
			}
		}
	}
}
