using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Net;
using Com.Google.Inject;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Http;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App;
using Org.Apache.Hadoop.Mapreduce.V2.App.Client;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job;
using Org.Apache.Hadoop.Mapreduce.V2.Util;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Server.Webproxy;
using Org.Apache.Hadoop.Yarn.Server.Webproxy.Amfilter;
using Org.Apache.Hadoop.Yarn.Webapp;
using Org.Apache.Hadoop.Yarn.Webapp.Test;
using Org.Apache.Hadoop.Yarn.Webapp.Util;
using Org.Apache.Http;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Webapp
{
	public class TestAMWebApp
	{
		[NUnit.Framework.Test]
		public virtual void TestAppControllerIndex()
		{
			AppContext ctx = new MockAppContext(0, 1, 1, 1);
			Injector injector = WebAppTests.CreateMockInjector<AppContext>(ctx);
			AppController controller = injector.GetInstance<AppController>();
			controller.Index();
			NUnit.Framework.Assert.AreEqual(ctx.GetApplicationID().ToString(), controller.Get
				(AMParams.AppId, string.Empty));
		}

		[NUnit.Framework.Test]
		public virtual void TestAppView()
		{
			WebAppTests.TestPage<AppContext>(typeof(AppView), new MockAppContext(0, 1, 1, 1));
		}

		[NUnit.Framework.Test]
		public virtual void TestJobView()
		{
			AppContext appContext = new MockAppContext(0, 1, 1, 1);
			IDictionary<string, string> @params = GetJobParams(appContext);
			WebAppTests.TestPage<AppContext>(typeof(JobPage), appContext, @params);
		}

		[NUnit.Framework.Test]
		public virtual void TestTasksView()
		{
			AppContext appContext = new MockAppContext(0, 1, 1, 1);
			IDictionary<string, string> @params = GetTaskParams(appContext);
			WebAppTests.TestPage<AppContext>(typeof(TasksPage), appContext, @params);
		}

		[NUnit.Framework.Test]
		public virtual void TestTaskView()
		{
			AppContext appContext = new MockAppContext(0, 1, 1, 1);
			IDictionary<string, string> @params = GetTaskParams(appContext);
			Org.Apache.Hadoop.Mapreduce.V2.App.Webapp.App app = new Org.Apache.Hadoop.Mapreduce.V2.App.Webapp.App
				(appContext);
			app.SetJob(appContext.GetAllJobs().Values.GetEnumerator().Next());
			app.SetTask(app.GetJob().GetTasks().Values.GetEnumerator().Next());
			WebAppTests.TestPage<Org.Apache.Hadoop.Mapreduce.V2.App.Webapp.App>(typeof(TaskPage
				), app, @params);
		}

		public static IDictionary<string, string> GetJobParams(AppContext appContext)
		{
			JobId jobId = appContext.GetAllJobs().GetEnumerator().Next().Key;
			IDictionary<string, string> @params = new Dictionary<string, string>();
			@params[AMParams.JobId] = MRApps.ToString(jobId);
			return @params;
		}

		public static IDictionary<string, string> GetTaskParams(AppContext appContext)
		{
			JobId jobId = appContext.GetAllJobs().GetEnumerator().Next().Key;
			KeyValuePair<TaskId, Task> e = appContext.GetJob(jobId).GetTasks().GetEnumerator(
				).Next();
			e.Value.GetType();
			IDictionary<string, string> @params = new Dictionary<string, string>();
			@params[AMParams.JobId] = MRApps.ToString(jobId);
			@params[AMParams.TaskId] = MRApps.ToString(e.Key);
			@params[AMParams.TaskType] = MRApps.TaskSymbol(e.Value.GetType());
			return @params;
		}

		[NUnit.Framework.Test]
		public virtual void TestConfView()
		{
			WebAppTests.TestPage<AppContext>(typeof(JobConfPage), new MockAppContext(0, 1, 1, 
				1));
		}

		[NUnit.Framework.Test]
		public virtual void TestCountersView()
		{
			AppContext appContext = new MockAppContext(0, 1, 1, 1);
			IDictionary<string, string> @params = GetJobParams(appContext);
			WebAppTests.TestPage<AppContext>(typeof(CountersPage), appContext, @params);
		}

		[NUnit.Framework.Test]
		public virtual void TestSingleCounterView()
		{
			AppContext appContext = new MockAppContext(0, 1, 1, 1);
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = appContext.GetAllJobs().Values.GetEnumerator
				().Next();
			// add a failed task to the job without any counters
			Task failedTask = MockJobs.NewTask(job.GetID(), 2, 1, true);
			IDictionary<TaskId, Task> tasks = job.GetTasks();
			tasks[failedTask.GetID()] = failedTask;
			IDictionary<string, string> @params = GetJobParams(appContext);
			@params[AMParams.CounterGroup] = "org.apache.hadoop.mapreduce.FileSystemCounter";
			@params[AMParams.CounterName] = "HDFS_WRITE_OPS";
			WebAppTests.TestPage<AppContext>(typeof(SingleCounterPage), appContext, @params);
		}

		[NUnit.Framework.Test]
		public virtual void TestTaskCountersView()
		{
			AppContext appContext = new MockAppContext(0, 1, 1, 1);
			IDictionary<string, string> @params = GetTaskParams(appContext);
			WebAppTests.TestPage<AppContext>(typeof(CountersPage), appContext, @params);
		}

		[NUnit.Framework.Test]
		public virtual void TestSingleTaskCounterView()
		{
			AppContext appContext = new MockAppContext(0, 1, 1, 2);
			IDictionary<string, string> @params = GetTaskParams(appContext);
			@params[AMParams.CounterGroup] = "org.apache.hadoop.mapreduce.FileSystemCounter";
			@params[AMParams.CounterName] = "HDFS_WRITE_OPS";
			// remove counters from one task attempt
			// to test handling of missing counters
			TaskId taskID = MRApps.ToTaskID(@params[AMParams.TaskId]);
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = appContext.GetJob(taskID.GetJobId
				());
			Task task = job.GetTask(taskID);
			TaskAttempt attempt = task.GetAttempts().Values.GetEnumerator().Next();
			attempt.GetReport().SetCounters(null);
			WebAppTests.TestPage<AppContext>(typeof(SingleCounterPage), appContext, @params);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMRWebAppSSLDisabled()
		{
			MRApp app = new _MRApp_175(2, 2, true, this.GetType().FullName, true);
			Configuration conf = new Configuration();
			// MR is explicitly disabling SSL, even though setting as HTTPS_ONLY
			conf.Set(YarnConfiguration.YarnHttpPolicyKey, HttpConfig.Policy.HttpsOnly.ToString
				());
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = app.Submit(conf);
			string hostPort = NetUtils.GetHostPortString(((MRClientService)app.GetClientService
				()).GetWebApp().GetListenerAddress());
			// http:// should be accessible
			Uri httpUrl = new Uri("http://" + hostPort);
			HttpURLConnection conn = (HttpURLConnection)httpUrl.OpenConnection();
			InputStream @in = conn.GetInputStream();
			ByteArrayOutputStream @out = new ByteArrayOutputStream();
			IOUtils.CopyBytes(@in, @out, 1024);
			NUnit.Framework.Assert.IsTrue(@out.ToString().Contains("MapReduce Application"));
			// https:// is not accessible.
			Uri httpsUrl = new Uri("https://" + hostPort);
			try
			{
				HttpURLConnection httpsConn = (HttpURLConnection)httpsUrl.OpenConnection();
				httpsConn.GetInputStream();
				NUnit.Framework.Assert.Fail("https:// is not accessible, expected to fail");
			}
			catch (Exception e)
			{
				NUnit.Framework.Assert.IsTrue(e is SSLException);
			}
			app.WaitForState(job, JobState.Succeeded);
			app.VerifyCompleted();
		}

		private sealed class _MRApp_175 : MRApp
		{
			public _MRApp_175(int baseArg1, int baseArg2, bool baseArg3, string baseArg4, bool
				 baseArg5)
				: base(baseArg1, baseArg2, baseArg3, baseArg4, baseArg5)
			{
			}

			protected internal override ClientService CreateClientService(AppContext context)
			{
				return new MRClientService(context);
			}
		}

		internal static string webProxyBase = null;

		public class TestAMFilterInitializer : AmFilterInitializer
		{
			protected override string GetApplicationWebProxyBase()
			{
				return webProxyBase;
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMRWebAppRedirection()
		{
			string[] schemePrefix = new string[] { WebAppUtils.HttpPrefix, WebAppUtils.HttpsPrefix
				 };
			foreach (string scheme in schemePrefix)
			{
				MRApp app = new _MRApp_227(2, 2, true, this.GetType().FullName, true);
				Configuration conf = new Configuration();
				conf.Set(YarnConfiguration.ProxyAddress, "9.9.9.9");
				conf.Set(YarnConfiguration.YarnHttpPolicyKey, scheme.Equals(WebAppUtils.HttpsPrefix
					) ? HttpConfig.Policy.HttpsOnly.ToString() : HttpConfig.Policy.HttpOnly.ToString
					());
				webProxyBase = "/proxy/" + app.GetAppID();
				conf.Set("hadoop.http.filter.initializers", typeof(TestAMWebApp.TestAMFilterInitializer
					).FullName);
				Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = app.Submit(conf);
				string hostPort = NetUtils.GetHostPortString(((MRClientService)app.GetClientService
					()).GetWebApp().GetListenerAddress());
				Uri httpUrl = new Uri("http://" + hostPort + "/mapreduce");
				HttpURLConnection conn = (HttpURLConnection)httpUrl.OpenConnection();
				conn.SetInstanceFollowRedirects(false);
				conn.Connect();
				string expectedURL = scheme + conf.Get(YarnConfiguration.ProxyAddress) + ProxyUriUtils
					.GetPath(app.GetAppID(), "/mapreduce");
				NUnit.Framework.Assert.AreEqual(expectedURL, conn.GetHeaderField(HttpHeaders.Location
					));
				NUnit.Framework.Assert.AreEqual(HttpStatus.ScMovedTemporarily, conn.GetResponseCode
					());
				app.WaitForState(job, JobState.Succeeded);
				app.VerifyCompleted();
			}
		}

		private sealed class _MRApp_227 : MRApp
		{
			public _MRApp_227(int baseArg1, int baseArg2, bool baseArg3, string baseArg4, bool
				 baseArg5)
				: base(baseArg1, baseArg2, baseArg3, baseArg4, baseArg5)
			{
			}

			protected internal override ClientService CreateClientService(AppContext context)
			{
				return new MRClientService(context);
			}
		}

		public static void Main(string[] args)
		{
			WebApps.$for<AppContext>("yarn", new MockAppContext(0, 8, 88, 4)).At(58888).InDevMode
				().Start(new AMWebApp()).JoinThread();
		}
	}
}
