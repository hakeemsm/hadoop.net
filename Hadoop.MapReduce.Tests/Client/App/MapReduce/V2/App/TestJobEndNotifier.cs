using System.IO;
using Javax.Servlet.Http;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Http;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App.Client;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job.Event;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job.Impl;
using Org.Apache.Hadoop.Mapreduce.V2.App.RM;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App
{
	/// <summary>Tests job end notification</summary>
	public class TestJobEndNotifier : JobEndNotifier
	{
		//Test maximum retries is capped by MR_JOB_END_NOTIFICATION_MAX_ATTEMPTS
		private void TestNumRetries(Configuration conf)
		{
			conf.Set(MRJobConfig.MrJobEndNotificationMaxAttempts, "0");
			conf.Set(MRJobConfig.MrJobEndRetryAttempts, "10");
			SetConf(conf);
			NUnit.Framework.Assert.IsTrue("Expected numTries to be 0, but was " + numTries, numTries
				 == 0);
			conf.Set(MRJobConfig.MrJobEndNotificationMaxAttempts, "1");
			SetConf(conf);
			NUnit.Framework.Assert.IsTrue("Expected numTries to be 1, but was " + numTries, numTries
				 == 1);
			conf.Set(MRJobConfig.MrJobEndNotificationMaxAttempts, "20");
			SetConf(conf);
			NUnit.Framework.Assert.IsTrue("Expected numTries to be 11, but was " + numTries, 
				numTries == 11);
		}

		//11 because number of _retries_ is 10
		//Test maximum retry interval is capped by
		//MR_JOB_END_NOTIFICATION_MAX_RETRY_INTERVAL
		private void TestWaitInterval(Configuration conf)
		{
			conf.Set(MRJobConfig.MrJobEndNotificationMaxRetryInterval, "5000");
			conf.Set(MRJobConfig.MrJobEndRetryInterval, "1000");
			SetConf(conf);
			NUnit.Framework.Assert.IsTrue("Expected waitInterval to be 1000, but was " + waitInterval
				, waitInterval == 1000);
			conf.Set(MRJobConfig.MrJobEndRetryInterval, "10000");
			SetConf(conf);
			NUnit.Framework.Assert.IsTrue("Expected waitInterval to be 5000, but was " + waitInterval
				, waitInterval == 5000);
			//Test negative numbers are set to default
			conf.Set(MRJobConfig.MrJobEndRetryInterval, "-10");
			SetConf(conf);
			NUnit.Framework.Assert.IsTrue("Expected waitInterval to be 5000, but was " + waitInterval
				, waitInterval == 5000);
		}

		private void TestTimeout(Configuration conf)
		{
			conf.Set(MRJobConfig.MrJobEndNotificationTimeout, "1000");
			SetConf(conf);
			NUnit.Framework.Assert.IsTrue("Expected timeout to be 1000, but was " + timeout, 
				timeout == 1000);
		}

		private void TestProxyConfiguration(Configuration conf)
		{
			conf.Set(MRJobConfig.MrJobEndNotificationProxy, "somehost");
			SetConf(conf);
			NUnit.Framework.Assert.IsTrue("Proxy shouldn't be set because port wasn't specified"
				, proxyToUse.Type() == Proxy.Type.Direct);
			conf.Set(MRJobConfig.MrJobEndNotificationProxy, "somehost:someport");
			SetConf(conf);
			NUnit.Framework.Assert.IsTrue("Proxy shouldn't be set because port wasn't numeric"
				, proxyToUse.Type() == Proxy.Type.Direct);
			conf.Set(MRJobConfig.MrJobEndNotificationProxy, "somehost:1000");
			SetConf(conf);
			NUnit.Framework.Assert.IsTrue("Proxy should have been set but wasn't ", proxyToUse
				.ToString().Equals("HTTP @ somehost:1000"));
			conf.Set(MRJobConfig.MrJobEndNotificationProxy, "socks@somehost:1000");
			SetConf(conf);
			NUnit.Framework.Assert.IsTrue("Proxy should have been socks but wasn't ", proxyToUse
				.ToString().Equals("SOCKS @ somehost:1000"));
			conf.Set(MRJobConfig.MrJobEndNotificationProxy, "SOCKS@somehost:1000");
			SetConf(conf);
			NUnit.Framework.Assert.IsTrue("Proxy should have been socks but wasn't ", proxyToUse
				.ToString().Equals("SOCKS @ somehost:1000"));
			conf.Set(MRJobConfig.MrJobEndNotificationProxy, "sfafn@somehost:1000");
			SetConf(conf);
			NUnit.Framework.Assert.IsTrue("Proxy should have been http but wasn't ", proxyToUse
				.ToString().Equals("HTTP @ somehost:1000"));
		}

		/// <summary>Test that setting parameters has the desired effect</summary>
		[NUnit.Framework.Test]
		public virtual void CheckConfiguration()
		{
			Configuration conf = new Configuration();
			TestNumRetries(conf);
			TestWaitInterval(conf);
			TestTimeout(conf);
			TestProxyConfiguration(conf);
		}

		protected internal int notificationCount = 0;

		protected internal override bool NotifyURLOnce()
		{
			bool success = base.NotifyURLOnce();
			notificationCount++;
			return success;
		}

		//Check retries happen as intended
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNotifyRetries()
		{
			JobConf conf = new JobConf();
			conf.Set(MRJobConfig.MrJobEndRetryAttempts, "0");
			conf.Set(MRJobConfig.MrJobEndNotificationMaxAttempts, "1");
			conf.Set(MRJobConfig.MrJobEndNotificationUrl, "http://nonexistent");
			conf.Set(MRJobConfig.MrJobEndRetryInterval, "5000");
			conf.Set(MRJobConfig.MrJobEndNotificationMaxRetryInterval, "5000");
			JobReport jobReport = Org.Mockito.Mockito.Mock<JobReport>();
			long startTime = Runtime.CurrentTimeMillis();
			this.notificationCount = 0;
			this.SetConf(conf);
			this.Notify(jobReport);
			long endTime = Runtime.CurrentTimeMillis();
			NUnit.Framework.Assert.AreEqual("Only 1 try was expected but was : " + this.notificationCount
				, 1, this.notificationCount);
			NUnit.Framework.Assert.IsTrue("Should have taken more than 5 seconds it took " + 
				(endTime - startTime), endTime - startTime > 5000);
			conf.Set(MRJobConfig.MrJobEndNotificationMaxAttempts, "3");
			conf.Set(MRJobConfig.MrJobEndRetryAttempts, "3");
			conf.Set(MRJobConfig.MrJobEndRetryInterval, "3000");
			conf.Set(MRJobConfig.MrJobEndNotificationMaxRetryInterval, "3000");
			startTime = Runtime.CurrentTimeMillis();
			this.notificationCount = 0;
			this.SetConf(conf);
			this.Notify(jobReport);
			endTime = Runtime.CurrentTimeMillis();
			NUnit.Framework.Assert.AreEqual("Only 3 retries were expected but was : " + this.
				notificationCount, 3, this.notificationCount);
			NUnit.Framework.Assert.IsTrue("Should have taken more than 9 seconds it took " + 
				(endTime - startTime), endTime - startTime > 9000);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNotificationOnLastRetryNormalShutdown()
		{
			HttpServer2 server = StartHttpServer();
			// Act like it is the second attempt. Default max attempts is 2
			MRApp app = Org.Mockito.Mockito.Spy(new TestJobEndNotifier.MRAppWithCustomContainerAllocator
				(this, 2, 2, true, this.GetType().FullName, true, 2, true));
			Org.Mockito.Mockito.DoNothing().When(app).Sysexit();
			JobConf conf = new JobConf();
			conf.Set(JobContext.MrJobEndNotificationUrl, TestJobEndNotifier.JobEndServlet.baseUrl
				 + "jobend?jobid=$jobId&status=$jobStatus");
			JobImpl job = (JobImpl)app.Submit(conf);
			app.WaitForInternalState(job, JobStateInternal.Succeeded);
			// Unregistration succeeds: successfullyUnregistered is set
			app.ShutDownJob();
			NUnit.Framework.Assert.IsTrue(app.IsLastAMRetry());
			NUnit.Framework.Assert.AreEqual(1, TestJobEndNotifier.JobEndServlet.calledTimes);
			NUnit.Framework.Assert.AreEqual("jobid=" + job.GetID() + "&status=SUCCEEDED", TestJobEndNotifier.JobEndServlet
				.requestUri.GetQuery());
			NUnit.Framework.Assert.AreEqual(JobState.Succeeded.ToString(), TestJobEndNotifier.JobEndServlet
				.foundJobState);
			server.Stop();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAbsentNotificationOnNotLastRetryUnregistrationFailure()
		{
			HttpServer2 server = StartHttpServer();
			MRApp app = Org.Mockito.Mockito.Spy(new TestJobEndNotifier.MRAppWithCustomContainerAllocator
				(this, 2, 2, false, this.GetType().FullName, true, 1, false));
			Org.Mockito.Mockito.DoNothing().When(app).Sysexit();
			JobConf conf = new JobConf();
			conf.Set(JobContext.MrJobEndNotificationUrl, TestJobEndNotifier.JobEndServlet.baseUrl
				 + "jobend?jobid=$jobId&status=$jobStatus");
			JobImpl job = (JobImpl)app.Submit(conf);
			app.WaitForState(job, JobState.Running);
			app.GetContext().GetEventHandler().Handle(new JobEvent(app.GetJobId(), JobEventType
				.JobAmReboot));
			app.WaitForInternalState(job, JobStateInternal.Reboot);
			// Now shutdown.
			// Unregistration fails: isLastAMRetry is recalculated, this is not
			app.ShutDownJob();
			// Not the last AM attempt. So user should that the job is still running.
			app.WaitForState(job, JobState.Running);
			NUnit.Framework.Assert.IsFalse(app.IsLastAMRetry());
			NUnit.Framework.Assert.AreEqual(0, TestJobEndNotifier.JobEndServlet.calledTimes);
			NUnit.Framework.Assert.IsNull(TestJobEndNotifier.JobEndServlet.requestUri);
			NUnit.Framework.Assert.IsNull(TestJobEndNotifier.JobEndServlet.foundJobState);
			server.Stop();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNotificationOnLastRetryUnregistrationFailure()
		{
			HttpServer2 server = StartHttpServer();
			MRApp app = Org.Mockito.Mockito.Spy(new TestJobEndNotifier.MRAppWithCustomContainerAllocator
				(this, 2, 2, false, this.GetType().FullName, true, 2, false));
			// Currently, we will have isLastRetry always equals to false at beginning
			// of MRAppMaster, except staging area exists or commit already started at 
			// the beginning.
			// Now manually set isLastRetry to true and this should reset to false when
			// unregister failed.
			app.isLastAMRetry = true;
			Org.Mockito.Mockito.DoNothing().When(app).Sysexit();
			JobConf conf = new JobConf();
			conf.Set(JobContext.MrJobEndNotificationUrl, TestJobEndNotifier.JobEndServlet.baseUrl
				 + "jobend?jobid=$jobId&status=$jobStatus");
			JobImpl job = (JobImpl)app.Submit(conf);
			app.WaitForState(job, JobState.Running);
			app.GetContext().GetEventHandler().Handle(new JobEvent(app.GetJobId(), JobEventType
				.JobAmReboot));
			app.WaitForInternalState(job, JobStateInternal.Reboot);
			// Now shutdown. User should see FAILED state.
			// Unregistration fails: isLastAMRetry is recalculated, this is
			///reboot will stop service internally, we don't need to shutdown twice
			app.WaitForServiceToStop(10000);
			NUnit.Framework.Assert.IsFalse(app.IsLastAMRetry());
			// Since it's not last retry, JobEndServlet didn't called
			NUnit.Framework.Assert.AreEqual(0, TestJobEndNotifier.JobEndServlet.calledTimes);
			NUnit.Framework.Assert.IsNull(TestJobEndNotifier.JobEndServlet.requestUri);
			NUnit.Framework.Assert.IsNull(TestJobEndNotifier.JobEndServlet.foundJobState);
			server.Stop();
		}

		/// <exception cref="System.Exception"/>
		private static HttpServer2 StartHttpServer()
		{
			new FilePath(Runtime.GetProperty("build.webapps", "build/webapps") + "/test").Mkdirs
				();
			HttpServer2 server = new HttpServer2.Builder().SetName("test").AddEndpoint(URI.Create
				("http://localhost:0")).SetFindPort(true).Build();
			server.AddServlet("jobend", "/jobend", typeof(TestJobEndNotifier.JobEndServlet));
			server.Start();
			TestJobEndNotifier.JobEndServlet.calledTimes = 0;
			TestJobEndNotifier.JobEndServlet.requestUri = null;
			TestJobEndNotifier.JobEndServlet.baseUrl = "http://localhost:" + server.GetConnectorAddress
				(0).Port + "/";
			TestJobEndNotifier.JobEndServlet.foundJobState = null;
			return server;
		}

		[System.Serializable]
		public class JobEndServlet : HttpServlet
		{
			public static volatile int calledTimes = 0;

			public static URI requestUri;

			public static string baseUrl;

			public static string foundJobState;

			/// <exception cref="Javax.Servlet.ServletException"/>
			/// <exception cref="System.IO.IOException"/>
			protected override void DoGet(HttpServletRequest request, HttpServletResponse response
				)
			{
				InputStreamReader @in = new InputStreamReader(request.GetInputStream());
				TextWriter @out = new TextWriter(response.GetOutputStream());
				calledTimes++;
				try
				{
					requestUri = new URI(null, null, request.GetRequestURI(), request.GetQueryString(
						), null);
					foundJobState = request.GetParameter("status");
				}
				catch (URISyntaxException)
				{
				}
				@in.Close();
				@out.Close();
			}
		}

		private class MRAppWithCustomContainerAllocator : MRApp
		{
			private bool crushUnregistration;

			public MRAppWithCustomContainerAllocator(TestJobEndNotifier _enclosing, int maps, 
				int reduces, bool autoComplete, string testName, bool cleanOnStart, int startCount
				, bool crushUnregistration)
				: base(maps, reduces, autoComplete, testName, cleanOnStart, startCount, false)
			{
				this._enclosing = _enclosing;
				this.crushUnregistration = crushUnregistration;
			}

			protected internal override ContainerAllocator CreateContainerAllocator(ClientService
				 clientService, AppContext context)
			{
				context = Org.Mockito.Mockito.Spy(context);
				Org.Mockito.Mockito.When(context.GetEventHandler()).ThenReturn(null);
				Org.Mockito.Mockito.When(context.GetApplicationID()).ThenReturn(null);
				return new TestJobEndNotifier.MRAppWithCustomContainerAllocator.CustomContainerAllocator
					(this, this, context);
			}

			private class CustomContainerAllocator : RMCommunicator, ContainerAllocator, RMHeartbeatHandler
			{
				private TestJobEndNotifier.MRAppWithCustomContainerAllocator app;

				private MRApp.MRAppContainerAllocator allocator;

				public CustomContainerAllocator(MRAppWithCustomContainerAllocator _enclosing, TestJobEndNotifier.MRAppWithCustomContainerAllocator
					 app, AppContext context)
					: base(null, context)
				{
					this._enclosing = _enclosing;
					allocator = new MRApp.MRAppContainerAllocator(this);
					this.app = app;
				}

				protected override void ServiceInit(Configuration conf)
				{
				}

				protected override void ServiceStart()
				{
				}

				protected override void ServiceStop()
				{
					this.Unregister();
				}

				/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
				/// <exception cref="System.IO.IOException"/>
				/// <exception cref="System.Exception"/>
				protected internal override void DoUnregistration()
				{
					if (this._enclosing.crushUnregistration)
					{
						this.app.successfullyUnregistered.Set(true);
					}
					else
					{
						throw new YarnException("test exception");
					}
				}

				public virtual void Handle(ContainerAllocatorEvent @event)
				{
					this.allocator.Handle(@event);
				}

				public override long GetLastHeartbeatTime()
				{
					return this.allocator.GetLastHeartbeatTime();
				}

				public override void RunOnNextHeartbeat(Runnable callback)
				{
					this.allocator.RunOnNextHeartbeat(callback);
				}

				/// <exception cref="System.Exception"/>
				protected internal override void Heartbeat()
				{
				}

				private readonly MRAppWithCustomContainerAllocator _enclosing;
			}

			private readonly TestJobEndNotifier _enclosing;
		}
	}
}
