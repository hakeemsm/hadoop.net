using System.Collections.Generic;
using System.IO;
using System.Net;
using NUnit.Framework;
using NUnit.Framework.Runners;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.V2.Api;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.Util;
using Org.Apache.Hadoop.Security.Authorize;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Mockito;
using Sharpen;
using Sharpen.Reflect;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>Tests for ClientServiceDelegate.java</summary>
	public class TestClientServiceDelegate
	{
		private JobID oldJobId = JobID.ForName("job_1315895242400_2");

		private JobId jobId;

		private bool isAMReachableFromClient;

		public TestClientServiceDelegate(bool isAMReachableFromClient)
		{
			jobId = TypeConverter.ToYarn(oldJobId);
			this.isAMReachableFromClient = isAMReachableFromClient;
		}

		[Parameterized.Parameters]
		public static ICollection<object[]> Data()
		{
			object[][] data = new object[][] { new object[] { true }, new object[] { false } };
			return Arrays.AsList(data);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestUnknownAppInRM()
		{
			MRClientProtocol historyServerProxy = Org.Mockito.Mockito.Mock<MRClientProtocol>(
				);
			Org.Mockito.Mockito.When(historyServerProxy.GetJobReport(GetJobReportRequest())).
				ThenReturn(GetJobReportResponse());
			ClientServiceDelegate clientServiceDelegate = GetClientServiceDelegate(historyServerProxy
				, GetRMDelegate());
			JobStatus jobStatus = clientServiceDelegate.GetJobStatus(oldJobId);
			NUnit.Framework.Assert.IsNotNull(jobStatus);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRemoteExceptionFromHistoryServer()
		{
			MRClientProtocol historyServerProxy = Org.Mockito.Mockito.Mock<MRClientProtocol>(
				);
			Org.Mockito.Mockito.When(historyServerProxy.GetJobReport(GetJobReportRequest())).
				ThenThrow(new IOException("Job ID doesnot Exist"));
			ResourceMgrDelegate rm = Org.Mockito.Mockito.Mock<ResourceMgrDelegate>();
			Org.Mockito.Mockito.When(rm.GetApplicationReport(TypeConverter.ToYarn(oldJobId).GetAppId
				())).ThenReturn(null);
			ClientServiceDelegate clientServiceDelegate = GetClientServiceDelegate(historyServerProxy
				, rm);
			try
			{
				clientServiceDelegate.GetJobStatus(oldJobId);
				NUnit.Framework.Assert.Fail("Invoke should throw exception after retries.");
			}
			catch (IOException e)
			{
				NUnit.Framework.Assert.IsTrue(e.Message.Contains("Job ID doesnot Exist"));
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRetriesOnConnectionFailure()
		{
			MRClientProtocol historyServerProxy = Org.Mockito.Mockito.Mock<MRClientProtocol>(
				);
			Org.Mockito.Mockito.When(historyServerProxy.GetJobReport(GetJobReportRequest())).
				ThenThrow(new RuntimeException("1")).ThenThrow(new RuntimeException("2")).ThenReturn
				(GetJobReportResponse());
			ResourceMgrDelegate rm = Org.Mockito.Mockito.Mock<ResourceMgrDelegate>();
			Org.Mockito.Mockito.When(rm.GetApplicationReport(TypeConverter.ToYarn(oldJobId).GetAppId
				())).ThenReturn(null);
			ClientServiceDelegate clientServiceDelegate = GetClientServiceDelegate(historyServerProxy
				, rm);
			JobStatus jobStatus = clientServiceDelegate.GetJobStatus(oldJobId);
			NUnit.Framework.Assert.IsNotNull(jobStatus);
			Org.Mockito.Mockito.Verify(historyServerProxy, Org.Mockito.Mockito.Times(3)).GetJobReport
				(Matchers.Any<Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords.GetJobReportRequest
				>());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRetriesOnAMConnectionFailures()
		{
			if (!isAMReachableFromClient)
			{
				return;
			}
			ResourceMgrDelegate rm = Org.Mockito.Mockito.Mock<ResourceMgrDelegate>();
			Org.Mockito.Mockito.When(rm.GetApplicationReport(TypeConverter.ToYarn(oldJobId).GetAppId
				())).ThenReturn(GetRunningApplicationReport("am1", 78));
			// throw exception in 1st, 2nd, 3rd and 4th call of getJobReport, and
			// succeed in the 5th call.
			MRClientProtocol amProxy = Org.Mockito.Mockito.Mock<MRClientProtocol>();
			Org.Mockito.Mockito.When(amProxy.GetJobReport(Matchers.Any<Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords.GetJobReportRequest
				>())).ThenThrow(new RuntimeException("11")).ThenThrow(new RuntimeException("22")
				).ThenThrow(new RuntimeException("33")).ThenThrow(new RuntimeException("44")).ThenReturn
				(GetJobReportResponse());
			Configuration conf = new YarnConfiguration();
			conf.Set(MRConfig.FrameworkName, MRConfig.YarnFrameworkName);
			conf.SetBoolean(MRJobConfig.JobAmAccessDisabled, !isAMReachableFromClient);
			ClientServiceDelegate clientServiceDelegate = new _ClientServiceDelegate_167(amProxy
				, conf, rm, oldJobId, null);
			JobStatus jobStatus = clientServiceDelegate.GetJobStatus(oldJobId);
			NUnit.Framework.Assert.IsNotNull(jobStatus);
			// assert maxClientRetry is not decremented.
			NUnit.Framework.Assert.AreEqual(conf.GetInt(MRJobConfig.MrClientMaxRetries, MRJobConfig
				.DefaultMrClientMaxRetries), clientServiceDelegate.GetMaxClientRetry());
			Org.Mockito.Mockito.Verify(amProxy, Org.Mockito.Mockito.Times(5)).GetJobReport(Matchers.Any
				<Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords.GetJobReportRequest>());
		}

		private sealed class _ClientServiceDelegate_167 : ClientServiceDelegate
		{
			public _ClientServiceDelegate_167(MRClientProtocol amProxy, Configuration baseArg1
				, ResourceMgrDelegate baseArg2, JobID baseArg3, MRClientProtocol baseArg4)
				: base(baseArg1, baseArg2, baseArg3, baseArg4)
			{
				this.amProxy = amProxy;
			}

			/// <exception cref="System.IO.IOException"/>
			internal override MRClientProtocol InstantiateAMProxy(IPEndPoint serviceAddr)
			{
				base.InstantiateAMProxy(serviceAddr);
				return amProxy;
			}

			private readonly MRClientProtocol amProxy;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNoRetryOnAMAuthorizationException()
		{
			if (!isAMReachableFromClient)
			{
				return;
			}
			ResourceMgrDelegate rm = Org.Mockito.Mockito.Mock<ResourceMgrDelegate>();
			Org.Mockito.Mockito.When(rm.GetApplicationReport(TypeConverter.ToYarn(oldJobId).GetAppId
				())).ThenReturn(GetRunningApplicationReport("am1", 78));
			// throw authorization exception on first invocation
			MRClientProtocol amProxy = Org.Mockito.Mockito.Mock<MRClientProtocol>();
			Org.Mockito.Mockito.When(amProxy.GetJobReport(Matchers.Any<Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords.GetJobReportRequest
				>())).ThenThrow(new AuthorizationException("Denied"));
			Configuration conf = new YarnConfiguration();
			conf.Set(MRConfig.FrameworkName, MRConfig.YarnFrameworkName);
			conf.SetBoolean(MRJobConfig.JobAmAccessDisabled, !isAMReachableFromClient);
			ClientServiceDelegate clientServiceDelegate = new _ClientServiceDelegate_205(amProxy
				, conf, rm, oldJobId, null);
			try
			{
				clientServiceDelegate.GetJobStatus(oldJobId);
				NUnit.Framework.Assert.Fail("Exception should be thrown upon AuthorizationException"
					);
			}
			catch (IOException e)
			{
				NUnit.Framework.Assert.AreEqual(typeof(AuthorizationException).FullName + ": Denied"
					, e.Message);
			}
			// assert maxClientRetry is not decremented.
			NUnit.Framework.Assert.AreEqual(conf.GetInt(MRJobConfig.MrClientMaxRetries, MRJobConfig
				.DefaultMrClientMaxRetries), clientServiceDelegate.GetMaxClientRetry());
			Org.Mockito.Mockito.Verify(amProxy, Org.Mockito.Mockito.Times(1)).GetJobReport(Matchers.Any
				<Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords.GetJobReportRequest>());
		}

		private sealed class _ClientServiceDelegate_205 : ClientServiceDelegate
		{
			public _ClientServiceDelegate_205(MRClientProtocol amProxy, Configuration baseArg1
				, ResourceMgrDelegate baseArg2, JobID baseArg3, MRClientProtocol baseArg4)
				: base(baseArg1, baseArg2, baseArg3, baseArg4)
			{
				this.amProxy = amProxy;
			}

			/// <exception cref="System.IO.IOException"/>
			internal override MRClientProtocol InstantiateAMProxy(IPEndPoint serviceAddr)
			{
				base.InstantiateAMProxy(serviceAddr);
				return amProxy;
			}

			private readonly MRClientProtocol amProxy;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestHistoryServerNotConfigured()
		{
			//RM doesn't have app report and job History Server is not configured
			ClientServiceDelegate clientServiceDelegate = GetClientServiceDelegate(null, GetRMDelegate
				());
			JobStatus jobStatus = clientServiceDelegate.GetJobStatus(oldJobId);
			NUnit.Framework.Assert.AreEqual("N/A", jobStatus.GetUsername());
			NUnit.Framework.Assert.AreEqual(JobStatus.State.Prep, jobStatus.GetState());
			//RM has app report and job History Server is not configured
			ResourceMgrDelegate rm = Org.Mockito.Mockito.Mock<ResourceMgrDelegate>();
			ApplicationReport applicationReport = GetFinishedApplicationReport();
			Org.Mockito.Mockito.When(rm.GetApplicationReport(jobId.GetAppId())).ThenReturn(applicationReport
				);
			clientServiceDelegate = GetClientServiceDelegate(null, rm);
			jobStatus = clientServiceDelegate.GetJobStatus(oldJobId);
			NUnit.Framework.Assert.AreEqual(applicationReport.GetUser(), jobStatus.GetUsername
				());
			NUnit.Framework.Assert.AreEqual(JobStatus.State.Succeeded, jobStatus.GetState());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestJobReportFromHistoryServer()
		{
			MRClientProtocol historyServerProxy = Org.Mockito.Mockito.Mock<MRClientProtocol>(
				);
			Org.Mockito.Mockito.When(historyServerProxy.GetJobReport(GetJobReportRequest())).
				ThenReturn(GetJobReportResponseFromHistoryServer());
			ResourceMgrDelegate rm = Org.Mockito.Mockito.Mock<ResourceMgrDelegate>();
			Org.Mockito.Mockito.When(rm.GetApplicationReport(TypeConverter.ToYarn(oldJobId).GetAppId
				())).ThenReturn(null);
			ClientServiceDelegate clientServiceDelegate = GetClientServiceDelegate(historyServerProxy
				, rm);
			JobStatus jobStatus = clientServiceDelegate.GetJobStatus(oldJobId);
			NUnit.Framework.Assert.IsNotNull(jobStatus);
			NUnit.Framework.Assert.AreEqual("TestJobFilePath", jobStatus.GetJobFile());
			NUnit.Framework.Assert.AreEqual("http://TestTrackingUrl", jobStatus.GetTrackingUrl
				());
			NUnit.Framework.Assert.AreEqual(1.0f, jobStatus.GetMapProgress(), 0.0f);
			NUnit.Framework.Assert.AreEqual(1.0f, jobStatus.GetReduceProgress(), 0.0f);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCountersFromHistoryServer()
		{
			MRClientProtocol historyServerProxy = Org.Mockito.Mockito.Mock<MRClientProtocol>(
				);
			Org.Mockito.Mockito.When(historyServerProxy.GetCounters(GetCountersRequest())).ThenReturn
				(GetCountersResponseFromHistoryServer());
			ResourceMgrDelegate rm = Org.Mockito.Mockito.Mock<ResourceMgrDelegate>();
			Org.Mockito.Mockito.When(rm.GetApplicationReport(TypeConverter.ToYarn(oldJobId).GetAppId
				())).ThenReturn(null);
			ClientServiceDelegate clientServiceDelegate = GetClientServiceDelegate(historyServerProxy
				, rm);
			Counters counters = TypeConverter.ToYarn(clientServiceDelegate.GetJobCounters(oldJobId
				));
			NUnit.Framework.Assert.IsNotNull(counters);
			NUnit.Framework.Assert.AreEqual(1001, counters.GetCounterGroup("dummyCounters").GetCounter
				("dummyCounter").GetValue());
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestReconnectOnAMRestart()
		{
			//test not applicable when AM not reachable
			//as instantiateAMProxy is not called at all
			if (!isAMReachableFromClient)
			{
				return;
			}
			MRClientProtocol historyServerProxy = Org.Mockito.Mockito.Mock<MRClientProtocol>(
				);
			// RM returns AM1 url, null, null and AM2 url on invocations.
			// Nulls simulate the time when AM2 is in the process of restarting.
			ResourceMgrDelegate rmDelegate = Org.Mockito.Mockito.Mock<ResourceMgrDelegate>();
			try
			{
				Org.Mockito.Mockito.When(rmDelegate.GetApplicationReport(jobId.GetAppId())).ThenReturn
					(GetRunningApplicationReport("am1", 78)).ThenReturn(GetRunningApplicationReport(
					null, 0)).ThenReturn(GetRunningApplicationReport(null, 0)).ThenReturn(GetRunningApplicationReport
					("am2", 90));
			}
			catch (YarnException e)
			{
				throw new IOException(e);
			}
			Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords.GetJobReportResponse jobReportResponse1
				 = Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords.GetJobReportResponse
				>();
			Org.Mockito.Mockito.When(jobReportResponse1.GetJobReport()).ThenReturn(MRBuilderUtils
				.NewJobReport(jobId, "jobName-firstGen", "user", JobState.Running, 0, 0, 0, 0, 0
				, 0, 0, "anything", null, false, string.Empty));
			// First AM returns a report with jobName firstGen and simulates AM shutdown
			// on second invocation.
			MRClientProtocol firstGenAMProxy = Org.Mockito.Mockito.Mock<MRClientProtocol>();
			Org.Mockito.Mockito.When(firstGenAMProxy.GetJobReport(Matchers.Any<Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords.GetJobReportRequest
				>())).ThenReturn(jobReportResponse1).ThenThrow(new RuntimeException("AM is down!"
				));
			Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords.GetJobReportResponse jobReportResponse2
				 = Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords.GetJobReportResponse
				>();
			Org.Mockito.Mockito.When(jobReportResponse2.GetJobReport()).ThenReturn(MRBuilderUtils
				.NewJobReport(jobId, "jobName-secondGen", "user", JobState.Running, 0, 0, 0, 0, 
				0, 0, 0, "anything", null, false, string.Empty));
			// Second AM generation returns a report with jobName secondGen
			MRClientProtocol secondGenAMProxy = Org.Mockito.Mockito.Mock<MRClientProtocol>();
			Org.Mockito.Mockito.When(secondGenAMProxy.GetJobReport(Matchers.Any<Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords.GetJobReportRequest
				>())).ThenReturn(jobReportResponse2);
			ClientServiceDelegate clientServiceDelegate = Org.Mockito.Mockito.Spy(GetClientServiceDelegate
				(historyServerProxy, rmDelegate));
			// First time, connection should be to AM1, then to AM2. Further requests
			// should use the same proxy to AM2 and so instantiateProxy shouldn't be
			// called.
			Org.Mockito.Mockito.DoReturn(firstGenAMProxy).DoReturn(secondGenAMProxy).When(clientServiceDelegate
				).InstantiateAMProxy(Matchers.Any<IPEndPoint>());
			JobStatus jobStatus = clientServiceDelegate.GetJobStatus(oldJobId);
			NUnit.Framework.Assert.IsNotNull(jobStatus);
			NUnit.Framework.Assert.AreEqual("jobName-firstGen", jobStatus.GetJobName());
			jobStatus = clientServiceDelegate.GetJobStatus(oldJobId);
			NUnit.Framework.Assert.IsNotNull(jobStatus);
			NUnit.Framework.Assert.AreEqual("jobName-secondGen", jobStatus.GetJobName());
			jobStatus = clientServiceDelegate.GetJobStatus(oldJobId);
			NUnit.Framework.Assert.IsNotNull(jobStatus);
			NUnit.Framework.Assert.AreEqual("jobName-secondGen", jobStatus.GetJobName());
			Org.Mockito.Mockito.Verify(clientServiceDelegate, Org.Mockito.Mockito.Times(2)).InstantiateAMProxy
				(Matchers.Any<IPEndPoint>());
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestAMAccessDisabled()
		{
			//test only applicable when AM not reachable
			if (isAMReachableFromClient)
			{
				return;
			}
			MRClientProtocol historyServerProxy = Org.Mockito.Mockito.Mock<MRClientProtocol>(
				);
			Org.Mockito.Mockito.When(historyServerProxy.GetJobReport(GetJobReportRequest())).
				ThenReturn(GetJobReportResponseFromHistoryServer());
			ResourceMgrDelegate rmDelegate = Org.Mockito.Mockito.Mock<ResourceMgrDelegate>();
			try
			{
				Org.Mockito.Mockito.When(rmDelegate.GetApplicationReport(jobId.GetAppId())).ThenReturn
					(GetRunningApplicationReport("am1", 78)).ThenReturn(GetRunningApplicationReport(
					"am1", 78)).ThenReturn(GetRunningApplicationReport("am1", 78)).ThenReturn(GetFinishedApplicationReport
					());
			}
			catch (YarnException e)
			{
				throw new IOException(e);
			}
			ClientServiceDelegate clientServiceDelegate = Org.Mockito.Mockito.Spy(GetClientServiceDelegate
				(historyServerProxy, rmDelegate));
			JobStatus jobStatus = clientServiceDelegate.GetJobStatus(oldJobId);
			NUnit.Framework.Assert.IsNotNull(jobStatus);
			NUnit.Framework.Assert.AreEqual("N/A", jobStatus.GetJobName());
			Org.Mockito.Mockito.Verify(clientServiceDelegate, Org.Mockito.Mockito.Times(0)).InstantiateAMProxy
				(Matchers.Any<IPEndPoint>());
			// Should not reach AM even for second and third times too.
			jobStatus = clientServiceDelegate.GetJobStatus(oldJobId);
			NUnit.Framework.Assert.IsNotNull(jobStatus);
			NUnit.Framework.Assert.AreEqual("N/A", jobStatus.GetJobName());
			Org.Mockito.Mockito.Verify(clientServiceDelegate, Org.Mockito.Mockito.Times(0)).InstantiateAMProxy
				(Matchers.Any<IPEndPoint>());
			jobStatus = clientServiceDelegate.GetJobStatus(oldJobId);
			NUnit.Framework.Assert.IsNotNull(jobStatus);
			NUnit.Framework.Assert.AreEqual("N/A", jobStatus.GetJobName());
			Org.Mockito.Mockito.Verify(clientServiceDelegate, Org.Mockito.Mockito.Times(0)).InstantiateAMProxy
				(Matchers.Any<IPEndPoint>());
			// The third time around, app is completed, so should go to JHS
			JobStatus jobStatus1 = clientServiceDelegate.GetJobStatus(oldJobId);
			NUnit.Framework.Assert.IsNotNull(jobStatus1);
			NUnit.Framework.Assert.AreEqual("TestJobFilePath", jobStatus1.GetJobFile());
			NUnit.Framework.Assert.AreEqual("http://TestTrackingUrl", jobStatus1.GetTrackingUrl
				());
			NUnit.Framework.Assert.AreEqual(1.0f, jobStatus1.GetMapProgress(), 0.0f);
			NUnit.Framework.Assert.AreEqual(1.0f, jobStatus1.GetReduceProgress(), 0.0f);
			Org.Mockito.Mockito.Verify(clientServiceDelegate, Org.Mockito.Mockito.Times(0)).InstantiateAMProxy
				(Matchers.Any<IPEndPoint>());
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestRMDownForJobStatusBeforeGetAMReport()
		{
			Configuration conf = new YarnConfiguration();
			TestRMDownForJobStatusBeforeGetAMReport(conf, MRJobConfig.DefaultMrClientMaxRetries
				);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestRMDownForJobStatusBeforeGetAMReportWithRetryTimes()
		{
			Configuration conf = new YarnConfiguration();
			conf.SetInt(MRJobConfig.MrClientMaxRetries, 2);
			TestRMDownForJobStatusBeforeGetAMReport(conf, conf.GetInt(MRJobConfig.MrClientMaxRetries
				, MRJobConfig.DefaultMrClientMaxRetries));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestRMDownRestoreForJobStatusBeforeGetAMReport()
		{
			Configuration conf = new YarnConfiguration();
			conf.SetInt(MRJobConfig.MrClientMaxRetries, 3);
			conf.Set(MRConfig.FrameworkName, MRConfig.YarnFrameworkName);
			conf.SetBoolean(MRJobConfig.JobAmAccessDisabled, !isAMReachableFromClient);
			MRClientProtocol historyServerProxy = Org.Mockito.Mockito.Mock<MRClientProtocol>(
				);
			Org.Mockito.Mockito.When(historyServerProxy.GetJobReport(Matchers.Any<Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords.GetJobReportRequest
				>())).ThenReturn(GetJobReportResponse());
			ResourceMgrDelegate rmDelegate = Org.Mockito.Mockito.Mock<ResourceMgrDelegate>();
			try
			{
				Org.Mockito.Mockito.When(rmDelegate.GetApplicationReport(jobId.GetAppId())).ThenThrow
					(new UndeclaredThrowableException(new IOException("Connection refuced1"))).ThenThrow
					(new UndeclaredThrowableException(new IOException("Connection refuced2"))).ThenReturn
					(GetFinishedApplicationReport());
				ClientServiceDelegate clientServiceDelegate = new ClientServiceDelegate(conf, rmDelegate
					, oldJobId, historyServerProxy);
				JobStatus jobStatus = clientServiceDelegate.GetJobStatus(oldJobId);
				Org.Mockito.Mockito.Verify(rmDelegate, Org.Mockito.Mockito.Times(3)).GetApplicationReport
					(Matchers.Any<ApplicationId>());
				NUnit.Framework.Assert.IsNotNull(jobStatus);
			}
			catch (YarnException e)
			{
				throw new IOException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void TestRMDownForJobStatusBeforeGetAMReport(Configuration conf, int noOfRetries
			)
		{
			conf.Set(MRConfig.FrameworkName, MRConfig.YarnFrameworkName);
			conf.SetBoolean(MRJobConfig.JobAmAccessDisabled, !isAMReachableFromClient);
			MRClientProtocol historyServerProxy = Org.Mockito.Mockito.Mock<MRClientProtocol>(
				);
			ResourceMgrDelegate rmDelegate = Org.Mockito.Mockito.Mock<ResourceMgrDelegate>();
			try
			{
				Org.Mockito.Mockito.When(rmDelegate.GetApplicationReport(jobId.GetAppId())).ThenThrow
					(new UndeclaredThrowableException(new IOException("Connection refuced1"))).ThenThrow
					(new UndeclaredThrowableException(new IOException("Connection refuced2"))).ThenThrow
					(new UndeclaredThrowableException(new IOException("Connection refuced3")));
				ClientServiceDelegate clientServiceDelegate = new ClientServiceDelegate(conf, rmDelegate
					, oldJobId, historyServerProxy);
				try
				{
					clientServiceDelegate.GetJobStatus(oldJobId);
					NUnit.Framework.Assert.Fail("It should throw exception after retries");
				}
				catch (IOException e)
				{
					System.Console.Out.WriteLine("fail to get job status,and e=" + e.ToString());
				}
				Org.Mockito.Mockito.Verify(rmDelegate, Org.Mockito.Mockito.Times(noOfRetries)).GetApplicationReport
					(Matchers.Any<ApplicationId>());
			}
			catch (YarnException e)
			{
				throw new IOException(e);
			}
		}

		private Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords.GetJobReportRequest GetJobReportRequest
			()
		{
			Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords.GetJobReportRequest request = 
				Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords.GetJobReportRequest
				>();
			request.SetJobId(jobId);
			return request;
		}

		private Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords.GetJobReportResponse GetJobReportResponse
			()
		{
			Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords.GetJobReportResponse jobReportResponse
				 = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords.GetJobReportResponse
				>();
			JobReport jobReport = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<JobReport>();
			jobReport.SetJobId(jobId);
			jobReport.SetJobState(JobState.Succeeded);
			jobReportResponse.SetJobReport(jobReport);
			return jobReportResponse;
		}

		private Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords.GetCountersRequest GetCountersRequest
			()
		{
			Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords.GetCountersRequest request = Org.Apache.Hadoop.Yarn.Util.Records
				.NewRecord<Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords.GetCountersRequest
				>();
			request.SetJobId(jobId);
			return request;
		}

		private ApplicationReport GetFinishedApplicationReport()
		{
			ApplicationId appId = ApplicationId.NewInstance(1234, 5);
			ApplicationAttemptId attemptId = ApplicationAttemptId.NewInstance(appId, 0);
			return ApplicationReport.NewInstance(appId, attemptId, "user", "queue", "appname"
				, "host", 124, null, YarnApplicationState.Finished, "diagnostics", "url", 0, 0, 
				FinalApplicationStatus.Succeeded, null, "N/A", 0.0f, YarnConfiguration.DefaultApplicationType
				, null);
		}

		private ApplicationReport GetRunningApplicationReport(string host, int port)
		{
			ApplicationId appId = ApplicationId.NewInstance(1234, 5);
			ApplicationAttemptId attemptId = ApplicationAttemptId.NewInstance(appId, 0);
			return ApplicationReport.NewInstance(appId, attemptId, "user", "queue", "appname"
				, host, port, null, YarnApplicationState.Running, "diagnostics", "url", 0, 0, FinalApplicationStatus
				.Undefined, null, "N/A", 0.0f, YarnConfiguration.DefaultApplicationType, null);
		}

		/// <exception cref="System.IO.IOException"/>
		private ResourceMgrDelegate GetRMDelegate()
		{
			ResourceMgrDelegate rm = Org.Mockito.Mockito.Mock<ResourceMgrDelegate>();
			try
			{
				ApplicationId appId = jobId.GetAppId();
				Org.Mockito.Mockito.When(rm.GetApplicationReport(appId)).ThenThrow(new ApplicationNotFoundException
					(appId + " not found"));
			}
			catch (YarnException e)
			{
				throw new IOException(e);
			}
			return rm;
		}

		private ClientServiceDelegate GetClientServiceDelegate(MRClientProtocol historyServerProxy
			, ResourceMgrDelegate rm)
		{
			Configuration conf = new YarnConfiguration();
			conf.Set(MRConfig.FrameworkName, MRConfig.YarnFrameworkName);
			conf.SetBoolean(MRJobConfig.JobAmAccessDisabled, !isAMReachableFromClient);
			ClientServiceDelegate clientServiceDelegate = new ClientServiceDelegate(conf, rm, 
				oldJobId, historyServerProxy);
			return clientServiceDelegate;
		}

		private Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords.GetJobReportResponse GetJobReportResponseFromHistoryServer
			()
		{
			Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords.GetJobReportResponse jobReportResponse
				 = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords.GetJobReportResponse
				>();
			JobReport jobReport = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<JobReport>();
			jobReport.SetJobId(jobId);
			jobReport.SetJobState(JobState.Succeeded);
			jobReport.SetMapProgress(1.0f);
			jobReport.SetReduceProgress(1.0f);
			jobReport.SetJobFile("TestJobFilePath");
			jobReport.SetTrackingUrl("http://TestTrackingUrl");
			jobReportResponse.SetJobReport(jobReport);
			return jobReportResponse;
		}

		private GetCountersResponse GetCountersResponseFromHistoryServer()
		{
			GetCountersResponse countersResponse = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord
				<GetCountersResponse>();
			Counter counter = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<Counter>();
			CounterGroup counterGroup = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<CounterGroup
				>();
			Counters counters = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<Counters>();
			counter.SetDisplayName("dummyCounter");
			counter.SetName("dummyCounter");
			counter.SetValue(1001);
			counterGroup.SetName("dummyCounters");
			counterGroup.SetDisplayName("dummyCounters");
			counterGroup.SetCounter("dummyCounter", counter);
			counters.SetCounterGroup("dummyCounters", counterGroup);
			countersResponse.SetCounters(counters);
			return countersResponse;
		}
	}
}
