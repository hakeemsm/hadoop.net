using System;
using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Server.Security;
using Org.Apache.Hadoop.Yarn.Server.Timeline;
using Org.Apache.Hadoop.Yarn.Server.Timeline.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice
{
	public class TestApplicationHistoryClientService
	{
		private static ApplicationHistoryClientService clientService;

		private const int MaxApps = 2;

		private static TimelineDataManager dataManager;

		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void Setup()
		{
			Configuration conf = new YarnConfiguration();
			TimelineStore store = TestApplicationHistoryManagerOnTimelineStore.CreateStore(MaxApps
				);
			TimelineACLsManager aclsManager = new TimelineACLsManager(conf);
			dataManager = new TimelineDataManager(store, aclsManager);
			ApplicationACLsManager appAclsManager = new ApplicationACLsManager(conf);
			ApplicationHistoryManagerOnTimelineStore historyManager = new ApplicationHistoryManagerOnTimelineStore
				(dataManager, appAclsManager);
			historyManager.Init(conf);
			historyManager.Start();
			clientService = new ApplicationHistoryClientService(historyManager);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		[NUnit.Framework.Test]
		public virtual void TestApplicationNotFound()
		{
			ApplicationId appId = null;
			appId = ApplicationId.NewInstance(0, MaxApps + 1);
			GetApplicationReportRequest request = GetApplicationReportRequest.NewInstance(appId
				);
			try
			{
				GetApplicationReportResponse response = clientService.GetApplicationReport(request
					);
				NUnit.Framework.Assert.Fail("Exception should have been thrown before we reach here."
					);
			}
			catch (ApplicationNotFoundException e)
			{
				//This exception is expected.
				NUnit.Framework.Assert.IsTrue(e.Message.Contains("doesn't exist in the timeline store"
					));
			}
			catch (Exception)
			{
				NUnit.Framework.Assert.Fail("Undesired exception caught");
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		[NUnit.Framework.Test]
		public virtual void TestApplicationAttemptNotFound()
		{
			ApplicationId appId = ApplicationId.NewInstance(0, 1);
			ApplicationAttemptId appAttemptId = ApplicationAttemptId.NewInstance(appId, MaxApps
				 + 1);
			GetApplicationAttemptReportRequest request = GetApplicationAttemptReportRequest.NewInstance
				(appAttemptId);
			try
			{
				GetApplicationAttemptReportResponse response = clientService.GetApplicationAttemptReport
					(request);
				NUnit.Framework.Assert.Fail("Exception should have been thrown before we reach here."
					);
			}
			catch (ApplicationAttemptNotFoundException e)
			{
				//This Exception is expected
				System.Console.Out.WriteLine(e.Message);
				NUnit.Framework.Assert.IsTrue(e.Message.Contains("doesn't exist in the timeline store"
					));
			}
			catch (Exception)
			{
				NUnit.Framework.Assert.Fail("Undesired exception caught");
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		[NUnit.Framework.Test]
		public virtual void TestContainerNotFound()
		{
			ApplicationId appId = ApplicationId.NewInstance(0, 1);
			ApplicationAttemptId appAttemptId = ApplicationAttemptId.NewInstance(appId, 1);
			ContainerId containerId = ContainerId.NewContainerId(appAttemptId, MaxApps + 1);
			GetContainerReportRequest request = GetContainerReportRequest.NewInstance(containerId
				);
			try
			{
				GetContainerReportResponse response = clientService.GetContainerReport(request);
			}
			catch (ContainerNotFoundException e)
			{
				//This exception is expected
				NUnit.Framework.Assert.IsTrue(e.Message.Contains("doesn't exist in the timeline store"
					));
			}
			catch (Exception)
			{
				NUnit.Framework.Assert.Fail("Undesired exception caught");
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		[NUnit.Framework.Test]
		public virtual void TestApplicationReport()
		{
			ApplicationId appId = null;
			appId = ApplicationId.NewInstance(0, 1);
			GetApplicationReportRequest request = GetApplicationReportRequest.NewInstance(appId
				);
			GetApplicationReportResponse response = clientService.GetApplicationReport(request
				);
			ApplicationReport appReport = response.GetApplicationReport();
			NUnit.Framework.Assert.IsNotNull(appReport);
			NUnit.Framework.Assert.AreEqual(123, appReport.GetApplicationResourceUsageReport(
				).GetMemorySeconds());
			NUnit.Framework.Assert.AreEqual(345, appReport.GetApplicationResourceUsageReport(
				).GetVcoreSeconds());
			NUnit.Framework.Assert.AreEqual("application_0_0001", appReport.GetApplicationId(
				).ToString());
			NUnit.Framework.Assert.AreEqual("test app type", appReport.GetApplicationType().ToString
				());
			NUnit.Framework.Assert.AreEqual("test queue", appReport.GetQueue().ToString());
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		[NUnit.Framework.Test]
		public virtual void TestApplications()
		{
			ApplicationId appId = null;
			appId = ApplicationId.NewInstance(0, 1);
			ApplicationId appId1 = ApplicationId.NewInstance(0, 2);
			GetApplicationsRequest request = GetApplicationsRequest.NewInstance();
			GetApplicationsResponse response = clientService.GetApplications(request);
			IList<ApplicationReport> appReport = response.GetApplicationList();
			NUnit.Framework.Assert.IsNotNull(appReport);
			NUnit.Framework.Assert.AreEqual(appId, appReport[1].GetApplicationId());
			NUnit.Framework.Assert.AreEqual(appId1, appReport[0].GetApplicationId());
			// Create a historyManager, and set the max_apps can be loaded
			// as 1.
			Configuration conf = new YarnConfiguration();
			conf.SetLong(YarnConfiguration.ApplicationHistoryMaxApps, 1);
			ApplicationHistoryManagerOnTimelineStore historyManager2 = new ApplicationHistoryManagerOnTimelineStore
				(dataManager, new ApplicationACLsManager(conf));
			historyManager2.Init(conf);
			historyManager2.Start();
			ApplicationHistoryClientService clientService2 = new ApplicationHistoryClientService
				(historyManager2);
			response = clientService2.GetApplications(request);
			appReport = response.GetApplicationList();
			NUnit.Framework.Assert.IsNotNull(appReport);
			NUnit.Framework.Assert.IsTrue(appReport.Count == 1);
			// Expected to get the appReport for application with appId1
			NUnit.Framework.Assert.AreEqual(appId1, appReport[0].GetApplicationId());
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		[NUnit.Framework.Test]
		public virtual void TestApplicationAttemptReport()
		{
			ApplicationId appId = ApplicationId.NewInstance(0, 1);
			ApplicationAttemptId appAttemptId = ApplicationAttemptId.NewInstance(appId, 1);
			GetApplicationAttemptReportRequest request = GetApplicationAttemptReportRequest.NewInstance
				(appAttemptId);
			GetApplicationAttemptReportResponse response = clientService.GetApplicationAttemptReport
				(request);
			ApplicationAttemptReport attemptReport = response.GetApplicationAttemptReport();
			NUnit.Framework.Assert.IsNotNull(attemptReport);
			NUnit.Framework.Assert.AreEqual("appattempt_0_0001_000001", attemptReport.GetApplicationAttemptId
				().ToString());
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		[NUnit.Framework.Test]
		public virtual void TestApplicationAttempts()
		{
			ApplicationId appId = ApplicationId.NewInstance(0, 1);
			ApplicationAttemptId appAttemptId = ApplicationAttemptId.NewInstance(appId, 1);
			ApplicationAttemptId appAttemptId1 = ApplicationAttemptId.NewInstance(appId, 2);
			GetApplicationAttemptsRequest request = GetApplicationAttemptsRequest.NewInstance
				(appId);
			GetApplicationAttemptsResponse response = clientService.GetApplicationAttempts(request
				);
			IList<ApplicationAttemptReport> attemptReports = response.GetApplicationAttemptList
				();
			NUnit.Framework.Assert.IsNotNull(attemptReports);
			NUnit.Framework.Assert.AreEqual(appAttemptId, attemptReports[0].GetApplicationAttemptId
				());
			NUnit.Framework.Assert.AreEqual(appAttemptId1, attemptReports[1].GetApplicationAttemptId
				());
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		[NUnit.Framework.Test]
		public virtual void TestContainerReport()
		{
			ApplicationId appId = ApplicationId.NewInstance(0, 1);
			ApplicationAttemptId appAttemptId = ApplicationAttemptId.NewInstance(appId, 1);
			ContainerId containerId = ContainerId.NewContainerId(appAttemptId, 1);
			GetContainerReportRequest request = GetContainerReportRequest.NewInstance(containerId
				);
			GetContainerReportResponse response = clientService.GetContainerReport(request);
			ContainerReport container = response.GetContainerReport();
			NUnit.Framework.Assert.IsNotNull(container);
			NUnit.Framework.Assert.AreEqual(containerId, container.GetContainerId());
			NUnit.Framework.Assert.AreEqual("http://0.0.0.0:8188/applicationhistory/logs/" + 
				"test host:100/container_0_0001_01_000001/" + "container_0_0001_01_000001/user1"
				, container.GetLogUrl());
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		[NUnit.Framework.Test]
		public virtual void TestContainers()
		{
			ApplicationId appId = ApplicationId.NewInstance(0, 1);
			ApplicationAttemptId appAttemptId = ApplicationAttemptId.NewInstance(appId, 1);
			ContainerId containerId = ContainerId.NewContainerId(appAttemptId, 1);
			ContainerId containerId1 = ContainerId.NewContainerId(appAttemptId, 2);
			GetContainersRequest request = GetContainersRequest.NewInstance(appAttemptId);
			GetContainersResponse response = clientService.GetContainers(request);
			IList<ContainerReport> containers = response.GetContainerList();
			NUnit.Framework.Assert.IsNotNull(containers);
			NUnit.Framework.Assert.AreEqual(containerId, containers[0].GetContainerId());
			NUnit.Framework.Assert.AreEqual(containerId1, containers[1].GetContainerId());
		}
	}
}
