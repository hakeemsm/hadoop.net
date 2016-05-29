using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Client.Api;
using Org.Apache.Hadoop.Yarn.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Client
{
	public class TestApplicationClientProtocolOnHA : ProtocolHATestBase
	{
		private YarnClient client = null;

		/// <exception cref="System.Exception"/>
		[SetUp]
		public virtual void Initiate()
		{
			StartHACluster(1, true, false, false);
			Configuration conf = new YarnConfiguration(this.conf);
			client = CreateAndStartYarnClient(conf);
		}

		[TearDown]
		public virtual void ShutDown()
		{
			if (client != null)
			{
				client.Stop();
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestGetApplicationReportOnHA()
		{
			ApplicationReport report = client.GetApplicationReport(cluster.CreateFakeAppId());
			NUnit.Framework.Assert.IsTrue(report != null);
			NUnit.Framework.Assert.AreEqual(cluster.CreateFakeAppReport(), report);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestGetNewApplicationOnHA()
		{
			ApplicationId appId = client.CreateApplication().GetApplicationSubmissionContext(
				).GetApplicationId();
			NUnit.Framework.Assert.IsTrue(appId != null);
			NUnit.Framework.Assert.AreEqual(cluster.CreateFakeAppId(), appId);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestGetClusterMetricsOnHA()
		{
			YarnClusterMetrics clusterMetrics = client.GetYarnClusterMetrics();
			NUnit.Framework.Assert.IsTrue(clusterMetrics != null);
			NUnit.Framework.Assert.AreEqual(cluster.CreateFakeYarnClusterMetrics(), clusterMetrics
				);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestGetApplicationsOnHA()
		{
			IList<ApplicationReport> reports = client.GetApplications();
			NUnit.Framework.Assert.IsTrue(reports != null && !reports.IsEmpty());
			NUnit.Framework.Assert.AreEqual(cluster.CreateFakeAppReports(), reports);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestGetClusterNodesOnHA()
		{
			IList<NodeReport> reports = client.GetNodeReports(NodeState.Running);
			NUnit.Framework.Assert.IsTrue(reports != null && !reports.IsEmpty());
			NUnit.Framework.Assert.AreEqual(cluster.CreateFakeNodeReports(), reports);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestGetQueueInfoOnHA()
		{
			QueueInfo queueInfo = client.GetQueueInfo("root");
			NUnit.Framework.Assert.IsTrue(queueInfo != null);
			NUnit.Framework.Assert.AreEqual(cluster.CreateFakeQueueInfo(), queueInfo);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestGetQueueUserAclsOnHA()
		{
			IList<QueueUserACLInfo> queueUserAclsList = client.GetQueueAclsInfo();
			NUnit.Framework.Assert.IsTrue(queueUserAclsList != null && !queueUserAclsList.IsEmpty
				());
			NUnit.Framework.Assert.AreEqual(cluster.CreateFakeQueueUserACLInfoList(), queueUserAclsList
				);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestGetApplicationAttemptReportOnHA()
		{
			ApplicationAttemptReport report = client.GetApplicationAttemptReport(cluster.CreateFakeApplicationAttemptId
				());
			NUnit.Framework.Assert.IsTrue(report != null);
			NUnit.Framework.Assert.AreEqual(cluster.CreateFakeApplicationAttemptReport(), report
				);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestGetApplicationAttemptsOnHA()
		{
			IList<ApplicationAttemptReport> reports = client.GetApplicationAttempts(cluster.CreateFakeAppId
				());
			NUnit.Framework.Assert.IsTrue(reports != null && !reports.IsEmpty());
			NUnit.Framework.Assert.AreEqual(cluster.CreateFakeApplicationAttemptReports(), reports
				);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestGetContainerReportOnHA()
		{
			ContainerReport report = client.GetContainerReport(cluster.CreateFakeContainerId(
				));
			NUnit.Framework.Assert.IsTrue(report != null);
			NUnit.Framework.Assert.AreEqual(cluster.CreateFakeContainerReport(), report);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestGetContainersOnHA()
		{
			IList<ContainerReport> reports = client.GetContainers(cluster.CreateFakeApplicationAttemptId
				());
			NUnit.Framework.Assert.IsTrue(reports != null && !reports.IsEmpty());
			NUnit.Framework.Assert.AreEqual(cluster.CreateFakeContainerReports(), reports);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestSubmitApplicationOnHA()
		{
			ApplicationSubmissionContext appContext = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord
				<ApplicationSubmissionContext>();
			appContext.SetApplicationId(cluster.CreateFakeAppId());
			ContainerLaunchContext amContainer = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord
				<ContainerLaunchContext>();
			appContext.SetAMContainerSpec(amContainer);
			Resource capability = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<Resource>();
			capability.SetMemory(10);
			capability.SetVirtualCores(1);
			appContext.SetResource(capability);
			ApplicationId appId = client.SubmitApplication(appContext);
			NUnit.Framework.Assert.IsTrue(GetActiveRM().GetRMContext().GetRMApps().Contains(appId
				));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestMoveApplicationAcrossQueuesOnHA()
		{
			client.MoveApplicationAcrossQueues(cluster.CreateFakeAppId(), "root");
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestForceKillApplicationOnHA()
		{
			client.KillApplication(cluster.CreateFakeAppId());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestGetDelegationTokenOnHA()
		{
			Token token = client.GetRMDelegationToken(new Text(" "));
			NUnit.Framework.Assert.AreEqual(token, cluster.CreateFakeToken());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRenewDelegationTokenOnHA()
		{
			RenewDelegationTokenRequest request = RenewDelegationTokenRequest.NewInstance(cluster
				.CreateFakeToken());
			long newExpirationTime = ClientRMProxy.CreateRMProxy<ApplicationClientProtocol>(this
				.conf).RenewDelegationToken(request).GetNextExpirationTime();
			NUnit.Framework.Assert.AreEqual(newExpirationTime, cluster.CreateNextExpirationTime
				());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestCancelDelegationTokenOnHA()
		{
			CancelDelegationTokenRequest request = CancelDelegationTokenRequest.NewInstance(cluster
				.CreateFakeToken());
			ClientRMProxy.CreateRMProxy<ApplicationClientProtocol>(this.conf).CancelDelegationToken
				(request);
		}
	}
}
