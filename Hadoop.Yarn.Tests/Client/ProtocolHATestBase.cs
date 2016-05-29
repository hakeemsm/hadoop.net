using System;
using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.HA;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Client.Api;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Server;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Security;
using Org.Apache.Hadoop.Yarn.Server.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Client
{
	/// <summary>Test Base for ResourceManager's Protocol on HA.</summary>
	/// <remarks>
	/// Test Base for ResourceManager's Protocol on HA.
	/// Limited scope:
	/// For all the test cases, we only test whether the method will be re-entered
	/// when failover happens. Does not cover the entire logic of test.
	/// Test strategy:
	/// Create a separate failover thread with a trigger flag,
	/// override all APIs that are added trigger flag.
	/// When the APIs are called, we will set trigger flag as true to kick off
	/// the failover. So We can make sure the failover happens during process
	/// of the method. If this API is marked as @Idempotent or @AtMostOnce,
	/// the test cases will pass; otherwise, they will throw the exception.
	/// </remarks>
	public abstract class ProtocolHATestBase : ClientBaseWithFixes
	{
		protected internal static readonly HAServiceProtocol.StateChangeRequestInfo req = 
			new HAServiceProtocol.StateChangeRequestInfo(HAServiceProtocol.RequestSource.RequestByUser
			);

		protected internal const string Rm1NodeId = "rm1";

		protected internal const int Rm1PortBase = 10000;

		protected internal const string Rm2NodeId = "rm2";

		protected internal const int Rm2PortBase = 20000;

		protected internal Configuration conf;

		protected internal ProtocolHATestBase.MiniYARNClusterForHATesting cluster;

		protected internal Sharpen.Thread failoverThread = null;

		private volatile bool keepRunning;

		private void SetConfForRM(string rmId, string prefix, string value)
		{
			conf.Set(HAUtil.AddSuffix(prefix, rmId), value);
		}

		private void SetRpcAddressForRM(string rmId, int @base)
		{
			SetConfForRM(rmId, YarnConfiguration.RmAddress, "0.0.0.0:" + (@base + YarnConfiguration
				.DefaultRmPort));
			SetConfForRM(rmId, YarnConfiguration.RmSchedulerAddress, "0.0.0.0:" + (@base + YarnConfiguration
				.DefaultRmSchedulerPort));
			SetConfForRM(rmId, YarnConfiguration.RmAdminAddress, "0.0.0.0:" + (@base + YarnConfiguration
				.DefaultRmAdminPort));
			SetConfForRM(rmId, YarnConfiguration.RmResourceTrackerAddress, "0.0.0.0:" + (@base
				 + YarnConfiguration.DefaultRmResourceTrackerPort));
			SetConfForRM(rmId, YarnConfiguration.RmWebappAddress, "0.0.0.0:" + (@base + YarnConfiguration
				.DefaultRmWebappPort));
			SetConfForRM(rmId, YarnConfiguration.RmWebappHttpsAddress, "0.0.0.0:" + (@base + 
				YarnConfiguration.DefaultRmWebappHttpsPort));
		}

		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public virtual void Setup()
		{
			failoverThread = null;
			keepRunning = true;
			conf = new YarnConfiguration();
			conf.SetBoolean(YarnConfiguration.RmHaEnabled, true);
			conf.SetInt(YarnConfiguration.ClientFailoverMaxAttempts, 5);
			conf.Set(YarnConfiguration.RmHaIds, Rm1NodeId + "," + Rm2NodeId);
			SetRpcAddressForRM(Rm1NodeId, Rm1PortBase);
			SetRpcAddressForRM(Rm2NodeId, Rm2PortBase);
			conf.SetLong(YarnConfiguration.ClientFailoverSleeptimeBaseMs, 100L);
			conf.SetBoolean(YarnConfiguration.YarnMiniclusterFixedPorts, true);
			conf.SetBoolean(YarnConfiguration.YarnMiniclusterUseRpc, true);
		}

		/// <exception cref="System.Exception"/>
		[TearDown]
		public virtual void Teardown()
		{
			keepRunning = false;
			if (failoverThread != null)
			{
				failoverThread.Interrupt();
				try
				{
					failoverThread.Join();
				}
				catch (Exception ex)
				{
					Log.Error("Error joining with failover thread", ex);
				}
			}
			cluster.Stop();
		}

		protected internal virtual AdminService GetAdminService(int index)
		{
			return cluster.GetResourceManager(index).GetRMContext().GetRMAdminService();
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void ExplicitFailover()
		{
			int activeRMIndex = cluster.GetActiveRMIndex();
			int newActiveRMIndex = (activeRMIndex + 1) % 2;
			GetAdminService(activeRMIndex).TransitionToStandby(req);
			GetAdminService(newActiveRMIndex).TransitionToActive(req);
			NUnit.Framework.Assert.AreEqual("Failover failed", newActiveRMIndex, cluster.GetActiveRMIndex
				());
		}

		protected internal virtual YarnClient CreateAndStartYarnClient(Configuration conf
			)
		{
			Configuration configuration = new YarnConfiguration(conf);
			YarnClient client = YarnClient.CreateYarnClient();
			client.Init(configuration);
			client.Start();
			return client;
		}

		/// <exception cref="System.Exception"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		protected internal virtual void VerifyConnections()
		{
			NUnit.Framework.Assert.IsTrue("NMs failed to connect to the RM", cluster.WaitForNodeManagersToConnect
				(20000));
			VerifyClientConnection();
		}

		protected internal virtual void VerifyClientConnection()
		{
			int numRetries = 3;
			while (numRetries-- > 0)
			{
				Configuration conf = new YarnConfiguration(this.conf);
				YarnClient client = CreateAndStartYarnClient(conf);
				try
				{
					Sharpen.Thread.Sleep(100);
					client.GetApplications();
					return;
				}
				catch (Exception e)
				{
					Log.Error(e.Message);
				}
				finally
				{
					client.Stop();
				}
			}
			NUnit.Framework.Assert.Fail("Client couldn't connect to the Active RM");
		}

		protected internal virtual Sharpen.Thread CreateAndStartFailoverThread()
		{
			Sharpen.Thread failoverThread = new _Thread_263(this);
			// Do Nothing
			// DO NOTHING
			failoverThread.Start();
			return failoverThread;
		}

		private sealed class _Thread_263 : Sharpen.Thread
		{
			public _Thread_263(ProtocolHATestBase _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public override void Run()
			{
				this._enclosing.keepRunning = true;
				while (this._enclosing.keepRunning)
				{
					if (this._enclosing.cluster.GetStartFailoverFlag())
					{
						try
						{
							this._enclosing.ExplicitFailover();
							this._enclosing.keepRunning = false;
							this._enclosing.cluster.ResetFailoverTriggeredFlag(true);
						}
						catch (Exception)
						{
						}
						finally
						{
							this._enclosing.keepRunning = false;
						}
					}
					try
					{
						Sharpen.Thread.Sleep(50);
					}
					catch (Exception)
					{
					}
				}
			}

			private readonly ProtocolHATestBase _enclosing;
		}

		/// <exception cref="System.Exception"/>
		protected internal virtual void StartHACluster(int numOfNMs, bool overrideClientRMService
			, bool overrideRTS, bool overrideApplicationMasterService)
		{
			conf.SetBoolean(YarnConfiguration.RecoveryEnabled, true);
			conf.SetBoolean(YarnConfiguration.AutoFailoverEnabled, false);
			cluster = new ProtocolHATestBase.MiniYARNClusterForHATesting(this, typeof(TestRMFailover
				).FullName, 2, numOfNMs, 1, 1, false, overrideClientRMService, overrideRTS, overrideApplicationMasterService
				);
			cluster.ResetStartFailoverFlag(false);
			cluster.Init(conf);
			cluster.Start();
			GetAdminService(0).TransitionToActive(req);
			NUnit.Framework.Assert.IsFalse("RM never turned active", -1 == cluster.GetActiveRMIndex
				());
			VerifyConnections();
			// Do the failover
			ExplicitFailover();
			VerifyConnections();
			failoverThread = CreateAndStartFailoverThread();
		}

		protected internal virtual ResourceManager GetActiveRM()
		{
			return cluster.GetResourceManager(cluster.GetActiveRMIndex());
		}

		public class MiniYARNClusterForHATesting : MiniYARNCluster
		{
			private bool overrideClientRMService;

			private bool overrideRTS;

			private bool overrideApplicationMasterService;

			private readonly AtomicBoolean startFailover = new AtomicBoolean(false);

			private readonly AtomicBoolean failoverTriggered = new AtomicBoolean(false);

			public MiniYARNClusterForHATesting(ProtocolHATestBase _enclosing, string testName
				, int numResourceManagers, int numNodeManagers, int numLocalDirs, int numLogDirs
				, bool enableAHS, bool overrideClientRMService, bool overrideRTS, bool overrideApplicationMasterService
				)
				: base(testName, numResourceManagers, numNodeManagers, numLocalDirs, numLogDirs, 
					enableAHS)
			{
				this._enclosing = _enclosing;
				this.overrideClientRMService = overrideClientRMService;
				this.overrideRTS = overrideRTS;
				this.overrideApplicationMasterService = overrideApplicationMasterService;
			}

			public virtual bool GetStartFailoverFlag()
			{
				return this.startFailover.Get();
			}

			public virtual void ResetStartFailoverFlag(bool flag)
			{
				this.startFailover.Set(flag);
			}

			public virtual void ResetFailoverTriggeredFlag(bool flag)
			{
				this.failoverTriggered.Set(flag);
			}

			private bool WaittingForFailOver()
			{
				int maximumWaittingTime = 50;
				int count = 0;
				while (!this.failoverTriggered.Get() && count >= maximumWaittingTime)
				{
					try
					{
						Sharpen.Thread.Sleep(100);
					}
					catch (Exception)
					{
					}
					// DO NOTHING
					count++;
				}
				if (count >= maximumWaittingTime)
				{
					return false;
				}
				try
				{
					Sharpen.Thread.Sleep(100);
				}
				catch (Exception)
				{
				}
				// DO NOTHING
				return true;
			}

			protected override ResourceManager CreateResourceManager()
			{
				return new _ResourceManager_373(this);
			}

			private sealed class _ResourceManager_373 : ResourceManager
			{
				public _ResourceManager_373(MiniYARNClusterForHATesting _enclosing)
				{
					this._enclosing = _enclosing;
				}

				/// <exception cref="System.IO.IOException"/>
				protected override void DoSecureLogin()
				{
				}

				// Don't try to login using keytab in the testcases.
				protected override ClientRMService CreateClientRMService()
				{
					if (this._enclosing.overrideClientRMService)
					{
						return new ProtocolHATestBase.MiniYARNClusterForHATesting.CustomedClientRMService
							(this, this.rmContext, this.scheduler, this.rmAppManager, this.applicationACLsManager
							, this.queueACLsManager, this.rmContext.GetRMDelegationTokenSecretManager());
					}
					return base.CreateClientRMService();
				}

				protected override ResourceTrackerService CreateResourceTrackerService()
				{
					if (this._enclosing.overrideRTS)
					{
						return new ProtocolHATestBase.MiniYARNClusterForHATesting.CustomedResourceTrackerService
							(this, this.rmContext, this.nodesListManager, this.nmLivelinessMonitor, this.rmContext
							.GetContainerTokenSecretManager(), this.rmContext.GetNMTokenSecretManager());
					}
					return base.CreateResourceTrackerService();
				}

				protected override ApplicationMasterService CreateApplicationMasterService()
				{
					if (this._enclosing.overrideApplicationMasterService)
					{
						return new ProtocolHATestBase.MiniYARNClusterForHATesting.CustomedApplicationMasterService
							(this, this.rmContext, this.scheduler);
					}
					return base.CreateApplicationMasterService();
				}

				private readonly MiniYARNClusterForHATesting _enclosing;
			}

			private class CustomedClientRMService : ClientRMService
			{
				public CustomedClientRMService(MiniYARNClusterForHATesting _enclosing, RMContext 
					rmContext, YarnScheduler scheduler, RMAppManager rmAppManager, ApplicationACLsManager
					 applicationACLsManager, QueueACLsManager queueACLsManager, RMDelegationTokenSecretManager
					 rmDTSecretManager)
					: base(rmContext, scheduler, rmAppManager, applicationACLsManager, queueACLsManager
						, rmDTSecretManager)
				{
					this._enclosing = _enclosing;
				}

				/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
				public override GetNewApplicationResponse GetNewApplication(GetNewApplicationRequest
					 request)
				{
					this._enclosing.ResetStartFailoverFlag(true);
					// make sure failover has been triggered
					NUnit.Framework.Assert.IsTrue(this._enclosing.WaittingForFailOver());
					// create the GetNewApplicationResponse with fake applicationId
					GetNewApplicationResponse response = GetNewApplicationResponse.NewInstance(this._enclosing
						.CreateFakeAppId(), null, null);
					return response;
				}

				/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
				public override GetApplicationReportResponse GetApplicationReport(GetApplicationReportRequest
					 request)
				{
					this._enclosing.ResetStartFailoverFlag(true);
					// make sure failover has been triggered
					NUnit.Framework.Assert.IsTrue(this._enclosing.WaittingForFailOver());
					// create a fake application report
					ApplicationReport report = this._enclosing.CreateFakeAppReport();
					GetApplicationReportResponse response = GetApplicationReportResponse.NewInstance(
						report);
					return response;
				}

				/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
				public override GetClusterMetricsResponse GetClusterMetrics(GetClusterMetricsRequest
					 request)
				{
					this._enclosing.ResetStartFailoverFlag(true);
					// make sure failover has been triggered
					NUnit.Framework.Assert.IsTrue(this._enclosing.WaittingForFailOver());
					// create GetClusterMetricsResponse with fake YarnClusterMetrics
					GetClusterMetricsResponse response = GetClusterMetricsResponse.NewInstance(this._enclosing
						.CreateFakeYarnClusterMetrics());
					return response;
				}

				/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
				public override GetApplicationsResponse GetApplications(GetApplicationsRequest request
					)
				{
					this._enclosing.ResetStartFailoverFlag(true);
					// make sure failover has been triggered
					NUnit.Framework.Assert.IsTrue(this._enclosing.WaittingForFailOver());
					// create GetApplicationsResponse with fake applicationList
					GetApplicationsResponse response = GetApplicationsResponse.NewInstance(this._enclosing
						.CreateFakeAppReports());
					return response;
				}

				/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
				public override GetClusterNodesResponse GetClusterNodes(GetClusterNodesRequest request
					)
				{
					this._enclosing.ResetStartFailoverFlag(true);
					// make sure failover has been triggered
					NUnit.Framework.Assert.IsTrue(this._enclosing.WaittingForFailOver());
					// create GetClusterNodesResponse with fake ClusterNodeLists
					GetClusterNodesResponse response = GetClusterNodesResponse.NewInstance(this._enclosing
						.CreateFakeNodeReports());
					return response;
				}

				/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
				public override GetQueueInfoResponse GetQueueInfo(GetQueueInfoRequest request)
				{
					this._enclosing.ResetStartFailoverFlag(true);
					// make sure failover has been triggered
					NUnit.Framework.Assert.IsTrue(this._enclosing.WaittingForFailOver());
					// return fake QueueInfo
					return GetQueueInfoResponse.NewInstance(this._enclosing.CreateFakeQueueInfo());
				}

				/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
				public override GetQueueUserAclsInfoResponse GetQueueUserAcls(GetQueueUserAclsInfoRequest
					 request)
				{
					this._enclosing.ResetStartFailoverFlag(true);
					// make sure failover has been triggered
					NUnit.Framework.Assert.IsTrue(this._enclosing.WaittingForFailOver());
					// return fake queueUserAcls
					return GetQueueUserAclsInfoResponse.NewInstance(this._enclosing.CreateFakeQueueUserACLInfoList
						());
				}

				/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
				/// <exception cref="System.IO.IOException"/>
				public override GetApplicationAttemptReportResponse GetApplicationAttemptReport(GetApplicationAttemptReportRequest
					 request)
				{
					this._enclosing.ResetStartFailoverFlag(true);
					// make sure failover has been triggered
					NUnit.Framework.Assert.IsTrue(this._enclosing.WaittingForFailOver());
					// return fake ApplicationAttemptReport
					return GetApplicationAttemptReportResponse.NewInstance(this._enclosing.CreateFakeApplicationAttemptReport
						());
				}

				/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
				/// <exception cref="System.IO.IOException"/>
				public override GetApplicationAttemptsResponse GetApplicationAttempts(GetApplicationAttemptsRequest
					 request)
				{
					this._enclosing.ResetStartFailoverFlag(true);
					// make sure failover has been triggered
					NUnit.Framework.Assert.IsTrue(this._enclosing.WaittingForFailOver());
					// return fake ApplicationAttemptReports
					return GetApplicationAttemptsResponse.NewInstance(this._enclosing.CreateFakeApplicationAttemptReports
						());
				}

				/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
				/// <exception cref="System.IO.IOException"/>
				public override GetContainerReportResponse GetContainerReport(GetContainerReportRequest
					 request)
				{
					this._enclosing.ResetStartFailoverFlag(true);
					// make sure failover has been triggered
					NUnit.Framework.Assert.IsTrue(this._enclosing.WaittingForFailOver());
					// return fake containerReport
					return GetContainerReportResponse.NewInstance(this._enclosing.CreateFakeContainerReport
						());
				}

				/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
				/// <exception cref="System.IO.IOException"/>
				public override GetContainersResponse GetContainers(GetContainersRequest request)
				{
					this._enclosing.ResetStartFailoverFlag(true);
					// make sure failover has been triggered
					NUnit.Framework.Assert.IsTrue(this._enclosing.WaittingForFailOver());
					// return fake ContainerReports
					return GetContainersResponse.NewInstance(this._enclosing.CreateFakeContainerReports
						());
				}

				/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
				public override SubmitApplicationResponse SubmitApplication(SubmitApplicationRequest
					 request)
				{
					this._enclosing.ResetStartFailoverFlag(true);
					// make sure failover has been triggered
					NUnit.Framework.Assert.IsTrue(this._enclosing.WaittingForFailOver());
					return base.SubmitApplication(request);
				}

				/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
				public override KillApplicationResponse ForceKillApplication(KillApplicationRequest
					 request)
				{
					this._enclosing.ResetStartFailoverFlag(true);
					// make sure failover has been triggered
					NUnit.Framework.Assert.IsTrue(this._enclosing.WaittingForFailOver());
					return KillApplicationResponse.NewInstance(true);
				}

				/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
				public override MoveApplicationAcrossQueuesResponse MoveApplicationAcrossQueues(MoveApplicationAcrossQueuesRequest
					 request)
				{
					this._enclosing.ResetStartFailoverFlag(true);
					// make sure failover has been triggered
					NUnit.Framework.Assert.IsTrue(this._enclosing.WaittingForFailOver());
					return Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<MoveApplicationAcrossQueuesResponse
						>();
				}

				/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
				public override GetDelegationTokenResponse GetDelegationToken(GetDelegationTokenRequest
					 request)
				{
					this._enclosing.ResetStartFailoverFlag(true);
					// make sure failover has been triggered
					NUnit.Framework.Assert.IsTrue(this._enclosing.WaittingForFailOver());
					return GetDelegationTokenResponse.NewInstance(this._enclosing.CreateFakeToken());
				}

				/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
				public override RenewDelegationTokenResponse RenewDelegationToken(RenewDelegationTokenRequest
					 request)
				{
					this._enclosing.ResetStartFailoverFlag(true);
					// make sure failover has been triggered
					NUnit.Framework.Assert.IsTrue(this._enclosing.WaittingForFailOver());
					return RenewDelegationTokenResponse.NewInstance(this._enclosing.CreateNextExpirationTime
						());
				}

				/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
				public override CancelDelegationTokenResponse CancelDelegationToken(CancelDelegationTokenRequest
					 request)
				{
					this._enclosing.ResetStartFailoverFlag(true);
					// make sure failover has been triggered
					NUnit.Framework.Assert.IsTrue(this._enclosing.WaittingForFailOver());
					return CancelDelegationTokenResponse.NewInstance();
				}

				private readonly MiniYARNClusterForHATesting _enclosing;
			}

			public virtual ApplicationReport CreateFakeAppReport()
			{
				ApplicationId appId = ApplicationId.NewInstance(1000l, 1);
				ApplicationAttemptId attemptId = ApplicationAttemptId.NewInstance(appId, 1);
				// create a fake application report
				ApplicationReport report = ApplicationReport.NewInstance(appId, attemptId, "fakeUser"
					, "fakeQueue", "fakeApplicationName", "localhost", 0, null, YarnApplicationState
					.Finished, "fake an application report", string.Empty, 1000l, 1200l, FinalApplicationStatus
					.Failed, null, string.Empty, 50f, "fakeApplicationType", null);
				return report;
			}

			public virtual IList<ApplicationReport> CreateFakeAppReports()
			{
				IList<ApplicationReport> reports = new AList<ApplicationReport>();
				reports.AddItem(this.CreateFakeAppReport());
				return reports;
			}

			public virtual ApplicationId CreateFakeAppId()
			{
				return ApplicationId.NewInstance(1000l, 1);
			}

			public virtual ApplicationAttemptId CreateFakeApplicationAttemptId()
			{
				return ApplicationAttemptId.NewInstance(this.CreateFakeAppId(), 0);
			}

			public virtual ContainerId CreateFakeContainerId()
			{
				return ContainerId.NewContainerId(this.CreateFakeApplicationAttemptId(), 0);
			}

			public virtual YarnClusterMetrics CreateFakeYarnClusterMetrics()
			{
				return YarnClusterMetrics.NewInstance(1);
			}

			public virtual IList<NodeReport> CreateFakeNodeReports()
			{
				NodeId nodeId = NodeId.NewInstance("localhost", 0);
				NodeReport report = NodeReport.NewInstance(nodeId, NodeState.Running, "localhost"
					, "rack1", null, null, 4, null, 1000l, null);
				IList<NodeReport> reports = new AList<NodeReport>();
				reports.AddItem(report);
				return reports;
			}

			public virtual QueueInfo CreateFakeQueueInfo()
			{
				return QueueInfo.NewInstance("root", 100f, 100f, 50f, null, this.CreateFakeAppReports
					(), QueueState.Running, null, null);
			}

			public virtual IList<QueueUserACLInfo> CreateFakeQueueUserACLInfoList()
			{
				IList<QueueACL> queueACL = new AList<QueueACL>();
				queueACL.AddItem(QueueACL.SubmitApplications);
				QueueUserACLInfo info = QueueUserACLInfo.NewInstance("root", queueACL);
				IList<QueueUserACLInfo> infos = new AList<QueueUserACLInfo>();
				infos.AddItem(info);
				return infos;
			}

			public virtual ApplicationAttemptReport CreateFakeApplicationAttemptReport()
			{
				return ApplicationAttemptReport.NewInstance(this.CreateFakeApplicationAttemptId()
					, "localhost", 0, string.Empty, string.Empty, string.Empty, YarnApplicationAttemptState
					.Running, this.CreateFakeContainerId());
			}

			public virtual IList<ApplicationAttemptReport> CreateFakeApplicationAttemptReports
				()
			{
				IList<ApplicationAttemptReport> reports = new AList<ApplicationAttemptReport>();
				reports.AddItem(this.CreateFakeApplicationAttemptReport());
				return reports;
			}

			public virtual ContainerReport CreateFakeContainerReport()
			{
				return ContainerReport.NewInstance(this.CreateFakeContainerId(), null, NodeId.NewInstance
					("localhost", 0), null, 1000l, 1200l, string.Empty, string.Empty, 0, ContainerState
					.Complete, "http://" + NodeId.NewInstance("localhost", 0).ToString());
			}

			public virtual IList<ContainerReport> CreateFakeContainerReports()
			{
				IList<ContainerReport> reports = new AList<ContainerReport>();
				reports.AddItem(this.CreateFakeContainerReport());
				return reports;
			}

			public virtual Token CreateFakeToken()
			{
				string identifier = "fake Token";
				string password = "fake token passwd";
				Token token = Token.NewInstance(Sharpen.Runtime.GetBytesForString(identifier), " "
					, Sharpen.Runtime.GetBytesForString(password), " ");
				return token;
			}

			public virtual long CreateNextExpirationTime()
			{
				return Sharpen.Runtime.GetBytesForString("fake Token").Length;
			}

			private class CustomedResourceTrackerService : ResourceTrackerService
			{
				public CustomedResourceTrackerService(MiniYARNClusterForHATesting _enclosing, RMContext
					 rmContext, NodesListManager nodesListManager, NMLivelinessMonitor nmLivelinessMonitor
					, RMContainerTokenSecretManager containerTokenSecretManager, NMTokenSecretManagerInRM
					 nmTokenSecretManager)
					: base(rmContext, nodesListManager, nmLivelinessMonitor, containerTokenSecretManager
						, nmTokenSecretManager)
				{
					this._enclosing = _enclosing;
				}

				/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
				/// <exception cref="System.IO.IOException"/>
				public override RegisterNodeManagerResponse RegisterNodeManager(RegisterNodeManagerRequest
					 request)
				{
					this._enclosing.ResetStartFailoverFlag(true);
					// make sure failover has been triggered
					NUnit.Framework.Assert.IsTrue(this._enclosing.WaittingForFailOver());
					return base.RegisterNodeManager(request);
				}

				/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
				/// <exception cref="System.IO.IOException"/>
				public override NodeHeartbeatResponse NodeHeartbeat(NodeHeartbeatRequest request)
				{
					this._enclosing.ResetStartFailoverFlag(true);
					// make sure failover has been triggered
					NUnit.Framework.Assert.IsTrue(this._enclosing.WaittingForFailOver());
					return base.NodeHeartbeat(request);
				}

				private readonly MiniYARNClusterForHATesting _enclosing;
			}

			private class CustomedApplicationMasterService : ApplicationMasterService
			{
				public CustomedApplicationMasterService(MiniYARNClusterForHATesting _enclosing, RMContext
					 rmContext, YarnScheduler scheduler)
					: base(rmContext, scheduler)
				{
					this._enclosing = _enclosing;
				}

				/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
				/// <exception cref="System.IO.IOException"/>
				public override AllocateResponse Allocate(AllocateRequest request)
				{
					this._enclosing.ResetStartFailoverFlag(true);
					// make sure failover has been triggered
					NUnit.Framework.Assert.IsTrue(this._enclosing.WaittingForFailOver());
					return this._enclosing.CreateFakeAllocateResponse();
				}

				/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
				/// <exception cref="System.IO.IOException"/>
				public override RegisterApplicationMasterResponse RegisterApplicationMaster(RegisterApplicationMasterRequest
					 request)
				{
					this._enclosing.ResetStartFailoverFlag(true);
					// make sure failover has been triggered
					NUnit.Framework.Assert.IsTrue(this._enclosing.WaittingForFailOver());
					return this._enclosing.CreateFakeRegisterApplicationMasterResponse();
				}

				/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
				/// <exception cref="System.IO.IOException"/>
				public override FinishApplicationMasterResponse FinishApplicationMaster(FinishApplicationMasterRequest
					 request)
				{
					this._enclosing.ResetStartFailoverFlag(true);
					// make sure failover has been triggered
					NUnit.Framework.Assert.IsTrue(this._enclosing.WaittingForFailOver());
					return this._enclosing.CreateFakeFinishApplicationMasterResponse();
				}

				private readonly MiniYARNClusterForHATesting _enclosing;
			}

			public virtual RegisterApplicationMasterResponse CreateFakeRegisterApplicationMasterResponse
				()
			{
				Resource minCapability = Resource.NewInstance(2048, 2);
				Resource maxCapability = Resource.NewInstance(4096, 4);
				IDictionary<ApplicationAccessType, string> acls = new Dictionary<ApplicationAccessType
					, string>();
				acls[ApplicationAccessType.ModifyApp] = "*";
				ByteBuffer key = ByteBuffer.Wrap(Sharpen.Runtime.GetBytesForString("fake_key"));
				return RegisterApplicationMasterResponse.NewInstance(minCapability, maxCapability
					, acls, key, new AList<Container>(), "root", new AList<NMToken>());
			}

			public virtual FinishApplicationMasterResponse CreateFakeFinishApplicationMasterResponse
				()
			{
				return FinishApplicationMasterResponse.NewInstance(true);
			}

			public virtual AllocateResponse CreateFakeAllocateResponse()
			{
				return AllocateResponse.NewInstance(-1, new AList<ContainerStatus>(), new AList<Container
					>(), new AList<NodeReport>(), Resource.NewInstance(1024, 2), null, 1, null, new 
					AList<NMToken>());
			}

			private readonly ProtocolHATestBase _enclosing;
		}
	}
}
