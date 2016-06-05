using System;
using System.IO;
using Javax.Servlet.Http;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Http;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public class TestJobEndNotifier : TestCase
	{
		internal HttpServer2 server;

		internal Uri baseUrl;

		[System.Serializable]
		public class JobEndServlet : HttpServlet
		{
			public static volatile int calledTimes = 0;

			public static URI requestUri;

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
				}
				catch (URISyntaxException)
				{
				}
				@in.Close();
				@out.Close();
			}
		}

		[System.Serializable]
		public class DelayServlet : HttpServlet
		{
			public static volatile int calledTimes = 0;

			// Servlet that delays requests for a long time
			/// <exception cref="Javax.Servlet.ServletException"/>
			/// <exception cref="System.IO.IOException"/>
			protected override void DoGet(HttpServletRequest request, HttpServletResponse response
				)
			{
				bool timedOut = false;
				calledTimes++;
				try
				{
					// Sleep for a long time
					Sharpen.Thread.Sleep(1000000);
				}
				catch (Exception)
				{
					timedOut = true;
				}
				NUnit.Framework.Assert.IsTrue("DelayServlet should be interrupted", timedOut);
			}
		}

		[System.Serializable]
		public class FailServlet : HttpServlet
		{
			public static volatile int calledTimes = 0;

			// Servlet that fails all requests into it
			/// <exception cref="Javax.Servlet.ServletException"/>
			/// <exception cref="System.IO.IOException"/>
			protected override void DoGet(HttpServletRequest request, HttpServletResponse response
				)
			{
				calledTimes++;
				throw new IOException("I am failing!");
			}
		}

		/// <exception cref="System.Exception"/>
		protected override void SetUp()
		{
			new FilePath(Runtime.GetProperty("build.webapps", "build/webapps") + "/test").Mkdirs
				();
			server = new HttpServer2.Builder().SetName("test").AddEndpoint(URI.Create("http://localhost:0"
				)).SetFindPort(true).Build();
			server.AddServlet("delay", "/delay", typeof(TestJobEndNotifier.DelayServlet));
			server.AddServlet("jobend", "/jobend", typeof(TestJobEndNotifier.JobEndServlet));
			server.AddServlet("fail", "/fail", typeof(TestJobEndNotifier.FailServlet));
			server.Start();
			int port = server.GetConnectorAddress(0).Port;
			baseUrl = new Uri("http://localhost:" + port + "/");
			TestJobEndNotifier.JobEndServlet.calledTimes = 0;
			TestJobEndNotifier.JobEndServlet.requestUri = null;
			TestJobEndNotifier.DelayServlet.calledTimes = 0;
			TestJobEndNotifier.FailServlet.calledTimes = 0;
		}

		/// <exception cref="System.Exception"/>
		protected override void TearDown()
		{
			server.Stop();
		}

		/// <summary>Basic validation for localRunnerNotification.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestLocalJobRunnerUriSubstitution()
		{
			JobStatus jobStatus = CreateTestJobStatus("job_20130313155005308_0001", JobStatus
				.Succeeded);
			JobConf jobConf = CreateTestJobConf(new Configuration(), 0, baseUrl + "jobend?jobid=$jobId&status=$jobStatus"
				);
			JobEndNotifier.LocalRunnerNotification(jobConf, jobStatus);
			// No need to wait for the notification to go thru since calls are
			// synchronous
			// Validate params
			NUnit.Framework.Assert.AreEqual(1, TestJobEndNotifier.JobEndServlet.calledTimes);
			NUnit.Framework.Assert.AreEqual("jobid=job_20130313155005308_0001&status=SUCCEEDED"
				, TestJobEndNotifier.JobEndServlet.requestUri.GetQuery());
		}

		/// <summary>Validate job.end.retry.attempts for the localJobRunner.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestLocalJobRunnerRetryCount()
		{
			int retryAttempts = 3;
			JobStatus jobStatus = CreateTestJobStatus("job_20130313155005308_0001", JobStatus
				.Succeeded);
			JobConf jobConf = CreateTestJobConf(new Configuration(), retryAttempts, baseUrl +
				 "fail");
			JobEndNotifier.LocalRunnerNotification(jobConf, jobStatus);
			// Validate params
			NUnit.Framework.Assert.AreEqual(retryAttempts + 1, TestJobEndNotifier.FailServlet
				.calledTimes);
		}

		/// <summary>
		/// Validate that the notification times out after reaching
		/// mapreduce.job.end-notification.timeout.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestNotificationTimeout()
		{
			Configuration conf = new Configuration();
			// Reduce the timeout to 1 second
			conf.SetInt("mapreduce.job.end-notification.timeout", 1000);
			JobStatus jobStatus = CreateTestJobStatus("job_20130313155005308_0001", JobStatus
				.Succeeded);
			JobConf jobConf = CreateTestJobConf(conf, 0, baseUrl + "delay");
			long startTime = Runtime.CurrentTimeMillis();
			JobEndNotifier.LocalRunnerNotification(jobConf, jobStatus);
			long elapsedTime = Runtime.CurrentTimeMillis() - startTime;
			// Validate params
			NUnit.Framework.Assert.AreEqual(1, TestJobEndNotifier.DelayServlet.calledTimes);
			// Make sure we timed out with time slightly above 1 second
			// (default timeout is in terms of minutes, so we'll catch the problem)
			NUnit.Framework.Assert.IsTrue(elapsedTime < 2000);
		}

		private static JobStatus CreateTestJobStatus(string jobId, int state)
		{
			return new JobStatus(((JobID)JobID.ForName(jobId)), 0.5f, 0.0f, state, "root", "TestJobEndNotifier"
				, null, null);
		}

		private static JobConf CreateTestJobConf(Configuration conf, int retryAttempts, string
			 notificationUri)
		{
			JobConf jobConf = new JobConf(conf);
			jobConf.SetInt("job.end.retry.attempts", retryAttempts);
			jobConf.Set("job.end.retry.interval", "0");
			jobConf.SetJobEndNotificationURI(notificationUri);
			return jobConf;
		}
	}
}
