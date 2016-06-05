using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.V2.Api;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.Jobhistory;
using Org.Apache.Hadoop.Metrics2.Lib;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Org.Apache.Hadoop.Yarn.Ipc;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public class TestClientRedirect
	{
		static TestClientRedirect()
		{
			DefaultMetricsSystem.SetMiniClusterMode(true);
		}

		private static readonly Log Log = LogFactory.GetLog(typeof(TestClientRedirect));

		private const string Rmaddress = "0.0.0.0:8054";

		private static readonly RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory
			(null);

		private const string Amhostaddress = "0.0.0.0:10020";

		private const string Hshostaddress = "0.0.0.0:10021";

		private volatile bool amContact = false;

		private volatile bool hsContact = false;

		private volatile bool amRunning = false;

		private volatile bool amRestarting = false;

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRedirect()
		{
			Configuration conf = new YarnConfiguration();
			conf.Set(MRConfig.FrameworkName, MRConfig.YarnFrameworkName);
			conf.Set(YarnConfiguration.RmAddress, Rmaddress);
			conf.Set(JHAdminConfig.MrHistoryAddress, Hshostaddress);
			// Start the RM.
			TestClientRedirect.RMService rmService = new TestClientRedirect.RMService(this, "test"
				);
			rmService.Init(conf);
			rmService.Start();
			// Start the AM.
			TestClientRedirect.AMService amService = new TestClientRedirect.AMService(this);
			amService.Init(conf);
			amService.Start(conf);
			// Start the HS.
			TestClientRedirect.HistoryService historyService = new TestClientRedirect.HistoryService
				(this);
			historyService.Init(conf);
			historyService.Start(conf);
			Log.Info("services started");
			Cluster cluster = new Cluster(conf);
			JobID jobID = new JobID("201103121733", 1);
			Counters counters = cluster.GetJob(jobID).GetCounters();
			ValidateCounters(counters);
			NUnit.Framework.Assert.IsTrue(amContact);
			Log.Info("Sleeping for 5 seconds before stop for" + " the client socket to not get EOF immediately.."
				);
			Sharpen.Thread.Sleep(5000);
			//bring down the AM service
			amService.Stop();
			Log.Info("Sleeping for 5 seconds after stop for" + " the server to exit cleanly.."
				);
			Sharpen.Thread.Sleep(5000);
			amRestarting = true;
			// Same client
			//results are returned from fake (not started job)
			counters = cluster.GetJob(jobID).GetCounters();
			NUnit.Framework.Assert.AreEqual(0, counters.CountCounters());
			Job job = cluster.GetJob(jobID);
			TaskID taskId = new TaskID(jobID, TaskType.Map, 0);
			TaskAttemptID tId = new TaskAttemptID(taskId, 0);
			//invoke all methods to check that no exception is thrown
			job.KillJob();
			job.KillTask(tId);
			job.FailTask(tId);
			job.GetTaskCompletionEvents(0, 100);
			job.GetStatus();
			job.GetTaskDiagnostics(tId);
			job.GetTaskReports(TaskType.Map);
			job.GetTrackingURL();
			amRestarting = false;
			amService = new TestClientRedirect.AMService(this);
			amService.Init(conf);
			amService.Start(conf);
			amContact = false;
			//reset
			counters = cluster.GetJob(jobID).GetCounters();
			ValidateCounters(counters);
			NUnit.Framework.Assert.IsTrue(amContact);
			// Stop the AM. It is not even restarting. So it should be treated as
			// completed.
			amService.Stop();
			// Same client
			counters = cluster.GetJob(jobID).GetCounters();
			ValidateCounters(counters);
			NUnit.Framework.Assert.IsTrue(hsContact);
			rmService.Stop();
			historyService.Stop();
		}

		private void ValidateCounters(Counters counters)
		{
			IEnumerator<CounterGroup> it = counters.GetEnumerator();
			while (it.HasNext())
			{
				CounterGroup group = it.Next();
				Log.Info("Group " + group.GetDisplayName());
				IEnumerator<Counter> itc = group.GetEnumerator();
				while (itc.HasNext())
				{
					Log.Info("Counter is " + itc.Next().GetDisplayName());
				}
			}
			NUnit.Framework.Assert.AreEqual(1, counters.CountCounters());
		}

		internal class RMService : AbstractService, ApplicationClientProtocol
		{
			private string clientServiceBindAddress;

			internal IPEndPoint clientBindAddress;

			private Server server;

			public RMService(TestClientRedirect _enclosing, string name)
				: base(name)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.Exception"/>
			protected override void ServiceInit(Configuration conf)
			{
				this.clientServiceBindAddress = TestClientRedirect.Rmaddress;
				/*
				clientServiceBindAddress = conf.get(
				YarnConfiguration.APPSMANAGER_ADDRESS,
				YarnConfiguration.DEFAULT_APPSMANAGER_BIND_ADDRESS);
				*/
				this.clientBindAddress = NetUtils.CreateSocketAddr(this.clientServiceBindAddress);
				base.ServiceInit(conf);
			}

			/// <exception cref="System.Exception"/>
			protected override void ServiceStart()
			{
				// All the clients to appsManager are supposed to be authenticated via
				// Kerberos if security is enabled, so no secretManager.
				YarnRPC rpc = YarnRPC.Create(this.GetConfig());
				Configuration clientServerConf = new Configuration(this.GetConfig());
				this.server = rpc.GetServer(typeof(ApplicationClientProtocol), this, this.clientBindAddress
					, clientServerConf, null, 1);
				this.server.Start();
				base.ServiceStart();
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual GetNewApplicationResponse GetNewApplication(GetNewApplicationRequest
				 request)
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual GetApplicationReportResponse GetApplicationReport(GetApplicationReportRequest
				 request)
			{
				ApplicationId applicationId = request.GetApplicationId();
				ApplicationReport application = TestClientRedirect.recordFactory.NewRecordInstance
					<ApplicationReport>();
				application.SetApplicationId(applicationId);
				application.SetFinalApplicationStatus(FinalApplicationStatus.Undefined);
				if (this._enclosing.amRunning)
				{
					application.SetYarnApplicationState(YarnApplicationState.Running);
				}
				else
				{
					if (this._enclosing.amRestarting)
					{
						application.SetYarnApplicationState(YarnApplicationState.Submitted);
					}
					else
					{
						application.SetYarnApplicationState(YarnApplicationState.Finished);
						application.SetFinalApplicationStatus(FinalApplicationStatus.Succeeded);
					}
				}
				string[] split = TestClientRedirect.Amhostaddress.Split(":");
				application.SetHost(split[0]);
				application.SetRpcPort(System.Convert.ToInt32(split[1]));
				application.SetUser("TestClientRedirect-user");
				application.SetName("N/A");
				application.SetQueue("N/A");
				application.SetStartTime(0);
				application.SetFinishTime(0);
				application.SetTrackingUrl("N/A");
				application.SetDiagnostics("N/A");
				GetApplicationReportResponse response = TestClientRedirect.recordFactory.NewRecordInstance
					<GetApplicationReportResponse>();
				response.SetApplicationReport(application);
				return response;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual SubmitApplicationResponse SubmitApplication(SubmitApplicationRequest
				 request)
			{
				throw new IOException("Test");
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual KillApplicationResponse ForceKillApplication(KillApplicationRequest
				 request)
			{
				return KillApplicationResponse.NewInstance(true);
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual GetClusterMetricsResponse GetClusterMetrics(GetClusterMetricsRequest
				 request)
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual GetApplicationsResponse GetApplications(GetApplicationsRequest request
				)
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual GetClusterNodesResponse GetClusterNodes(GetClusterNodesRequest request
				)
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual GetQueueInfoResponse GetQueueInfo(GetQueueInfoRequest request)
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual GetQueueUserAclsInfoResponse GetQueueUserAcls(GetQueueUserAclsInfoRequest
				 request)
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual GetDelegationTokenResponse GetDelegationToken(GetDelegationTokenRequest
				 request)
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual RenewDelegationTokenResponse RenewDelegationToken(RenewDelegationTokenRequest
				 request)
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual CancelDelegationTokenResponse CancelDelegationToken(CancelDelegationTokenRequest
				 request)
			{
				return null;
			}

			/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
			/// <exception cref="System.IO.IOException"/>
			public virtual MoveApplicationAcrossQueuesResponse MoveApplicationAcrossQueues(MoveApplicationAcrossQueuesRequest
				 request)
			{
				return null;
			}

			/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
			/// <exception cref="System.IO.IOException"/>
			public virtual GetApplicationAttemptReportResponse GetApplicationAttemptReport(GetApplicationAttemptReportRequest
				 request)
			{
				return null;
			}

			/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
			/// <exception cref="System.IO.IOException"/>
			public virtual GetApplicationAttemptsResponse GetApplicationAttempts(GetApplicationAttemptsRequest
				 request)
			{
				return null;
			}

			/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
			/// <exception cref="System.IO.IOException"/>
			public virtual GetContainerReportResponse GetContainerReport(GetContainerReportRequest
				 request)
			{
				return null;
			}

			/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
			/// <exception cref="System.IO.IOException"/>
			public virtual GetContainersResponse GetContainers(GetContainersRequest request)
			{
				return null;
			}

			/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
			/// <exception cref="System.IO.IOException"/>
			public virtual ReservationSubmissionResponse SubmitReservation(ReservationSubmissionRequest
				 request)
			{
				return null;
			}

			/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
			/// <exception cref="System.IO.IOException"/>
			public virtual ReservationUpdateResponse UpdateReservation(ReservationUpdateRequest
				 request)
			{
				return null;
			}

			/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
			/// <exception cref="System.IO.IOException"/>
			public virtual ReservationDeleteResponse DeleteReservation(ReservationDeleteRequest
				 request)
			{
				return null;
			}

			/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
			/// <exception cref="System.IO.IOException"/>
			public virtual GetNodesToLabelsResponse GetNodeToLabels(GetNodesToLabelsRequest request
				)
			{
				return null;
			}

			/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
			/// <exception cref="System.IO.IOException"/>
			public virtual GetClusterNodeLabelsResponse GetClusterNodeLabels(GetClusterNodeLabelsRequest
				 request)
			{
				return null;
			}

			/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
			/// <exception cref="System.IO.IOException"/>
			public virtual GetLabelsToNodesResponse GetLabelsToNodes(GetLabelsToNodesRequest 
				request)
			{
				return null;
			}

			private readonly TestClientRedirect _enclosing;
		}

		internal class HistoryService : TestClientRedirect.AMService, HSClientProtocol
		{
			public HistoryService(TestClientRedirect _enclosing)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
				this.protocol = typeof(HSClientProtocol);
			}

			/// <exception cref="System.IO.IOException"/>
			public override GetCountersResponse GetCounters(GetCountersRequest request)
			{
				this._enclosing.hsContact = true;
				Counters counters = TestClientRedirect.GetMyCounters();
				GetCountersResponse response = TestClientRedirect.recordFactory.NewRecordInstance
					<GetCountersResponse>();
				response.SetCounters(counters);
				return response;
			}

			private readonly TestClientRedirect _enclosing;
		}

		internal class AMService : AbstractService, MRClientProtocol
		{
			protected internal Type protocol;

			private IPEndPoint bindAddress;

			private Server server;

			private readonly string hostAddress;

			public AMService(TestClientRedirect _enclosing)
				: this(TestClientRedirect.Amhostaddress)
			{
				this._enclosing = _enclosing;
			}

			public virtual IPEndPoint GetConnectAddress()
			{
				return this.bindAddress;
			}

			public AMService(TestClientRedirect _enclosing, string hostAddress)
				: base("AMService")
			{
				this._enclosing = _enclosing;
				this.protocol = typeof(MRClientProtocol);
				this.hostAddress = hostAddress;
			}

			public virtual void Start(Configuration conf)
			{
				YarnRPC rpc = YarnRPC.Create(conf);
				//TODO : use fixed port ??
				IPEndPoint address = NetUtils.CreateSocketAddr(this.hostAddress);
				IPAddress hostNameResolved = null;
				try
				{
					address.Address;
					hostNameResolved = Sharpen.Runtime.GetLocalHost();
				}
				catch (UnknownHostException e)
				{
					throw new YarnRuntimeException(e);
				}
				this.server = rpc.GetServer(this.protocol, this, address, conf, null, 1);
				this.server.Start();
				this.bindAddress = NetUtils.GetConnectAddress(this.server);
				base.Start();
				this._enclosing.amRunning = true;
			}

			/// <exception cref="System.Exception"/>
			protected override void ServiceStop()
			{
				if (this.server != null)
				{
					this.server.Stop();
				}
				base.ServiceStop();
				this._enclosing.amRunning = false;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual GetCountersResponse GetCounters(GetCountersRequest request)
			{
				JobId jobID = request.GetJobId();
				this._enclosing.amContact = true;
				Counters counters = TestClientRedirect.GetMyCounters();
				GetCountersResponse response = TestClientRedirect.recordFactory.NewRecordInstance
					<GetCountersResponse>();
				response.SetCounters(counters);
				return response;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual GetJobReportResponse GetJobReport(GetJobReportRequest request)
			{
				this._enclosing.amContact = true;
				JobReport jobReport = TestClientRedirect.recordFactory.NewRecordInstance<JobReport
					>();
				jobReport.SetJobId(request.GetJobId());
				jobReport.SetJobState(JobState.Running);
				jobReport.SetJobName("TestClientRedirect-jobname");
				jobReport.SetUser("TestClientRedirect-user");
				jobReport.SetStartTime(0L);
				jobReport.SetFinishTime(1L);
				GetJobReportResponse response = TestClientRedirect.recordFactory.NewRecordInstance
					<GetJobReportResponse>();
				response.SetJobReport(jobReport);
				return response;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual GetTaskReportResponse GetTaskReport(GetTaskReportRequest request)
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual GetTaskAttemptReportResponse GetTaskAttemptReport(GetTaskAttemptReportRequest
				 request)
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual GetTaskAttemptCompletionEventsResponse GetTaskAttemptCompletionEvents
				(GetTaskAttemptCompletionEventsRequest request)
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual GetTaskReportsResponse GetTaskReports(GetTaskReportsRequest request
				)
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual GetDiagnosticsResponse GetDiagnostics(GetDiagnosticsRequest request
				)
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual KillJobResponse KillJob(KillJobRequest request)
			{
				return TestClientRedirect.recordFactory.NewRecordInstance<KillJobResponse>();
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual KillTaskResponse KillTask(KillTaskRequest request)
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual KillTaskAttemptResponse KillTaskAttempt(KillTaskAttemptRequest request
				)
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual FailTaskAttemptResponse FailTaskAttempt(FailTaskAttemptRequest request
				)
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual GetDelegationTokenResponse GetDelegationToken(GetDelegationTokenRequest
				 request)
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual RenewDelegationTokenResponse RenewDelegationToken(RenewDelegationTokenRequest
				 request)
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual CancelDelegationTokenResponse CancelDelegationToken(CancelDelegationTokenRequest
				 request)
			{
				return null;
			}

			private readonly TestClientRedirect _enclosing;
		}

		internal static Counters GetMyCounters()
		{
			Counter counter = recordFactory.NewRecordInstance<Counter>();
			counter.SetName("Mycounter");
			counter.SetDisplayName("My counter display name");
			counter.SetValue(12345);
			CounterGroup group = recordFactory.NewRecordInstance<CounterGroup>();
			group.SetName("MyGroup");
			group.SetDisplayName("My groupd display name");
			group.SetCounter("myCounter", counter);
			Counters counters = recordFactory.NewRecordInstance<Counters>();
			counters.SetCounterGroup("myGroupd", group);
			return counters;
		}
	}
}
