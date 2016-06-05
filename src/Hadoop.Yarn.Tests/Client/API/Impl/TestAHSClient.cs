using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Client.Api;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Mockito;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Client.Api.Impl
{
	public class TestAHSClient
	{
		[NUnit.Framework.Test]
		public virtual void TestClientStop()
		{
			Configuration conf = new Configuration();
			AHSClient client = AHSClient.CreateAHSClient();
			client.Init(conf);
			client.Start();
			client.Stop();
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestGetApplications()
		{
			Configuration conf = new Configuration();
			AHSClient client = new TestAHSClient.MockAHSClient();
			client.Init(conf);
			client.Start();
			IList<ApplicationReport> expectedReports = ((TestAHSClient.MockAHSClient)client).
				GetReports();
			IList<ApplicationReport> reports = client.GetApplications();
			NUnit.Framework.Assert.AreEqual(reports, expectedReports);
			reports = client.GetApplications();
			NUnit.Framework.Assert.AreEqual(reports.Count, 4);
			client.Stop();
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestGetApplicationReport()
		{
			Configuration conf = new Configuration();
			AHSClient client = new TestAHSClient.MockAHSClient();
			client.Init(conf);
			client.Start();
			IList<ApplicationReport> expectedReports = ((TestAHSClient.MockAHSClient)client).
				GetReports();
			ApplicationId applicationId = ApplicationId.NewInstance(1234, 5);
			ApplicationReport report = client.GetApplicationReport(applicationId);
			NUnit.Framework.Assert.AreEqual(report, expectedReports[0]);
			NUnit.Framework.Assert.AreEqual(report.GetApplicationId().ToString(), expectedReports
				[0].GetApplicationId().ToString());
			client.Stop();
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestGetApplicationAttempts()
		{
			Configuration conf = new Configuration();
			AHSClient client = new TestAHSClient.MockAHSClient();
			client.Init(conf);
			client.Start();
			ApplicationId applicationId = ApplicationId.NewInstance(1234, 5);
			IList<ApplicationAttemptReport> reports = client.GetApplicationAttempts(applicationId
				);
			NUnit.Framework.Assert.IsNotNull(reports);
			NUnit.Framework.Assert.AreEqual(reports[0].GetApplicationAttemptId(), ApplicationAttemptId
				.NewInstance(applicationId, 1));
			NUnit.Framework.Assert.AreEqual(reports[1].GetApplicationAttemptId(), ApplicationAttemptId
				.NewInstance(applicationId, 2));
			client.Stop();
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestGetApplicationAttempt()
		{
			Configuration conf = new Configuration();
			AHSClient client = new TestAHSClient.MockAHSClient();
			client.Init(conf);
			client.Start();
			IList<ApplicationReport> expectedReports = ((TestAHSClient.MockAHSClient)client).
				GetReports();
			ApplicationId applicationId = ApplicationId.NewInstance(1234, 5);
			ApplicationAttemptId appAttemptId = ApplicationAttemptId.NewInstance(applicationId
				, 1);
			ApplicationAttemptReport report = client.GetApplicationAttemptReport(appAttemptId
				);
			NUnit.Framework.Assert.IsNotNull(report);
			NUnit.Framework.Assert.AreEqual(report.GetApplicationAttemptId().ToString(), expectedReports
				[0].GetCurrentApplicationAttemptId().ToString());
			client.Stop();
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestGetContainers()
		{
			Configuration conf = new Configuration();
			AHSClient client = new TestAHSClient.MockAHSClient();
			client.Init(conf);
			client.Start();
			ApplicationId applicationId = ApplicationId.NewInstance(1234, 5);
			ApplicationAttemptId appAttemptId = ApplicationAttemptId.NewInstance(applicationId
				, 1);
			IList<ContainerReport> reports = client.GetContainers(appAttemptId);
			NUnit.Framework.Assert.IsNotNull(reports);
			NUnit.Framework.Assert.AreEqual(reports[0].GetContainerId(), (ContainerId.NewContainerId
				(appAttemptId, 1)));
			NUnit.Framework.Assert.AreEqual(reports[1].GetContainerId(), (ContainerId.NewContainerId
				(appAttemptId, 2)));
			client.Stop();
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestGetContainerReport()
		{
			Configuration conf = new Configuration();
			AHSClient client = new TestAHSClient.MockAHSClient();
			client.Init(conf);
			client.Start();
			IList<ApplicationReport> expectedReports = ((TestAHSClient.MockAHSClient)client).
				GetReports();
			ApplicationId applicationId = ApplicationId.NewInstance(1234, 5);
			ApplicationAttemptId appAttemptId = ApplicationAttemptId.NewInstance(applicationId
				, 1);
			ContainerId containerId = ContainerId.NewContainerId(appAttemptId, 1);
			ContainerReport report = client.GetContainerReport(containerId);
			NUnit.Framework.Assert.IsNotNull(report);
			NUnit.Framework.Assert.AreEqual(report.GetContainerId().ToString(), (ContainerId.
				NewContainerId(expectedReports[0].GetCurrentApplicationAttemptId(), 1)).ToString
				());
			client.Stop();
		}

		private class MockAHSClient : AHSClientImpl
		{
			private IList<ApplicationReport> reports = new AList<ApplicationReport>();

			private Dictionary<ApplicationId, IList<ApplicationAttemptReport>> attempts = new 
				Dictionary<ApplicationId, IList<ApplicationAttemptReport>>();

			private Dictionary<ApplicationAttemptId, IList<ContainerReport>> containers = new 
				Dictionary<ApplicationAttemptId, IList<ContainerReport>>();

			internal GetApplicationsResponse mockAppResponse = Org.Mockito.Mockito.Mock<GetApplicationsResponse
				>();

			internal GetApplicationReportResponse mockResponse = Org.Mockito.Mockito.Mock<GetApplicationReportResponse
				>();

			internal GetApplicationAttemptsResponse mockAppAttemptsResponse = Org.Mockito.Mockito.Mock
				<GetApplicationAttemptsResponse>();

			internal GetApplicationAttemptReportResponse mockAttemptResponse = Org.Mockito.Mockito.Mock
				<GetApplicationAttemptReportResponse>();

			internal GetContainersResponse mockContainersResponse = Org.Mockito.Mockito.Mock<
				GetContainersResponse>();

			internal GetContainerReportResponse mockContainerResponse = Org.Mockito.Mockito.Mock
				<GetContainerReportResponse>();

			public MockAHSClient()
				: base()
			{
				// private ApplicationReport mockReport;
				CreateAppReports();
			}

			public override void Start()
			{
				ahsClient = Org.Mockito.Mockito.Mock<ApplicationHistoryProtocol>();
				try
				{
					Org.Mockito.Mockito.When(ahsClient.GetApplicationReport(Matchers.Any<GetApplicationReportRequest
						>())).ThenReturn(mockResponse);
					Org.Mockito.Mockito.When(ahsClient.GetApplications(Matchers.Any<GetApplicationsRequest
						>())).ThenReturn(mockAppResponse);
					Org.Mockito.Mockito.When(ahsClient.GetApplicationAttemptReport(Matchers.Any<GetApplicationAttemptReportRequest
						>())).ThenReturn(mockAttemptResponse);
					Org.Mockito.Mockito.When(ahsClient.GetApplicationAttempts(Matchers.Any<GetApplicationAttemptsRequest
						>())).ThenReturn(mockAppAttemptsResponse);
					Org.Mockito.Mockito.When(ahsClient.GetContainers(Matchers.Any<GetContainersRequest
						>())).ThenReturn(mockContainersResponse);
					Org.Mockito.Mockito.When(ahsClient.GetContainerReport(Matchers.Any<GetContainerReportRequest
						>())).ThenReturn(mockContainerResponse);
				}
				catch (YarnException)
				{
					NUnit.Framework.Assert.Fail("Exception is not expected.");
				}
				catch (IOException)
				{
					NUnit.Framework.Assert.Fail("Exception is not expected.");
				}
			}

			/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
			/// <exception cref="System.IO.IOException"/>
			public override IList<ApplicationReport> GetApplications()
			{
				Org.Mockito.Mockito.When(mockAppResponse.GetApplicationList()).ThenReturn(reports
					);
				return base.GetApplications();
			}

			/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
			/// <exception cref="System.IO.IOException"/>
			public override ApplicationReport GetApplicationReport(ApplicationId appId)
			{
				Org.Mockito.Mockito.When(mockResponse.GetApplicationReport()).ThenReturn(GetReport
					(appId));
				return base.GetApplicationReport(appId);
			}

			/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
			/// <exception cref="System.IO.IOException"/>
			public override IList<ApplicationAttemptReport> GetApplicationAttempts(ApplicationId
				 appId)
			{
				Org.Mockito.Mockito.When(mockAppAttemptsResponse.GetApplicationAttemptList()).ThenReturn
					(GetAttempts(appId));
				return base.GetApplicationAttempts(appId);
			}

			/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
			/// <exception cref="System.IO.IOException"/>
			public override ApplicationAttemptReport GetApplicationAttemptReport(ApplicationAttemptId
				 appAttemptId)
			{
				Org.Mockito.Mockito.When(mockAttemptResponse.GetApplicationAttemptReport()).ThenReturn
					(GetAttempt(appAttemptId));
				return base.GetApplicationAttemptReport(appAttemptId);
			}

			/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
			/// <exception cref="System.IO.IOException"/>
			public override IList<ContainerReport> GetContainers(ApplicationAttemptId appAttemptId
				)
			{
				Org.Mockito.Mockito.When(mockContainersResponse.GetContainerList()).ThenReturn(GetContainersReport
					(appAttemptId));
				return base.GetContainers(appAttemptId);
			}

			/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
			/// <exception cref="System.IO.IOException"/>
			public override ContainerReport GetContainerReport(ContainerId containerId)
			{
				Org.Mockito.Mockito.When(mockContainerResponse.GetContainerReport()).ThenReturn(GetContainer
					(containerId));
				return base.GetContainerReport(containerId);
			}

			public override void Stop()
			{
			}

			public virtual ApplicationReport GetReport(ApplicationId appId)
			{
				for (int i = 0; i < reports.Count; ++i)
				{
					if (Sharpen.Runtime.EqualsIgnoreCase(appId.ToString(), reports[i].GetApplicationId
						().ToString()))
					{
						return reports[i];
					}
				}
				return null;
			}

			public virtual IList<ApplicationAttemptReport> GetAttempts(ApplicationId appId)
			{
				return attempts[appId];
			}

			public virtual ApplicationAttemptReport GetAttempt(ApplicationAttemptId appAttemptId
				)
			{
				return attempts[appAttemptId.GetApplicationId()][0];
			}

			public virtual IList<ContainerReport> GetContainersReport(ApplicationAttemptId appAttemptId
				)
			{
				return containers[appAttemptId];
			}

			public virtual ContainerReport GetContainer(ContainerId containerId)
			{
				return containers[containerId.GetApplicationAttemptId()][0];
			}

			public virtual IList<ApplicationReport> GetReports()
			{
				return this.reports;
			}

			private void CreateAppReports()
			{
				ApplicationId applicationId = ApplicationId.NewInstance(1234, 5);
				ApplicationReport newApplicationReport = ApplicationReport.NewInstance(applicationId
					, ApplicationAttemptId.NewInstance(applicationId, 1), "user", "queue", "appname"
					, "host", 124, null, YarnApplicationState.Running, "diagnostics", "url", 0, 0, FinalApplicationStatus
					.Succeeded, null, "N/A", 0.53789f, "YARN", null);
				IList<ApplicationReport> applicationReports = new AList<ApplicationReport>();
				applicationReports.AddItem(newApplicationReport);
				IList<ApplicationAttemptReport> appAttempts = new AList<ApplicationAttemptReport>
					();
				ApplicationAttemptReport attempt = ApplicationAttemptReport.NewInstance(ApplicationAttemptId
					.NewInstance(applicationId, 1), "host", 124, "url", "oUrl", "diagnostics", YarnApplicationAttemptState
					.Finished, ContainerId.NewContainerId(newApplicationReport.GetCurrentApplicationAttemptId
					(), 1));
				appAttempts.AddItem(attempt);
				ApplicationAttemptReport attempt1 = ApplicationAttemptReport.NewInstance(ApplicationAttemptId
					.NewInstance(applicationId, 2), "host", 124, "url", "oUrl", "diagnostics", YarnApplicationAttemptState
					.Finished, ContainerId.NewContainerId(newApplicationReport.GetCurrentApplicationAttemptId
					(), 2));
				appAttempts.AddItem(attempt1);
				attempts[applicationId] = appAttempts;
				IList<ContainerReport> containerReports = new AList<ContainerReport>();
				ContainerReport container = ContainerReport.NewInstance(ContainerId.NewContainerId
					(attempt.GetApplicationAttemptId(), 1), null, NodeId.NewInstance("host", 1234), 
					Priority.Undefined, 1234, 5678, "diagnosticInfo", "logURL", 0, ContainerState.Complete
					, "http://" + NodeId.NewInstance("host", 2345).ToString());
				containerReports.AddItem(container);
				ContainerReport container1 = ContainerReport.NewInstance(ContainerId.NewContainerId
					(attempt.GetApplicationAttemptId(), 2), null, NodeId.NewInstance("host", 1234), 
					Priority.Undefined, 1234, 5678, "diagnosticInfo", "logURL", 0, ContainerState.Complete
					, "http://" + NodeId.NewInstance("host", 2345).ToString());
				containerReports.AddItem(container1);
				containers[attempt.GetApplicationAttemptId()] = containerReports;
				ApplicationId applicationId2 = ApplicationId.NewInstance(1234, 6);
				ApplicationReport newApplicationReport2 = ApplicationReport.NewInstance(applicationId2
					, ApplicationAttemptId.NewInstance(applicationId2, 2), "user2", "queue2", "appname2"
					, "host2", 125, null, YarnApplicationState.Finished, "diagnostics2", "url2", 2, 
					2, FinalApplicationStatus.Succeeded, null, "N/A", 0.63789f, "NON-YARN", null);
				applicationReports.AddItem(newApplicationReport2);
				ApplicationId applicationId3 = ApplicationId.NewInstance(1234, 7);
				ApplicationReport newApplicationReport3 = ApplicationReport.NewInstance(applicationId3
					, ApplicationAttemptId.NewInstance(applicationId3, 3), "user3", "queue3", "appname3"
					, "host3", 126, null, YarnApplicationState.Running, "diagnostics3", "url3", 3, 3
					, FinalApplicationStatus.Succeeded, null, "N/A", 0.73789f, "MAPREDUCE", null);
				applicationReports.AddItem(newApplicationReport3);
				ApplicationId applicationId4 = ApplicationId.NewInstance(1234, 8);
				ApplicationReport newApplicationReport4 = ApplicationReport.NewInstance(applicationId4
					, ApplicationAttemptId.NewInstance(applicationId4, 4), "user4", "queue4", "appname4"
					, "host4", 127, null, YarnApplicationState.Failed, "diagnostics4", "url4", 4, 4, 
					FinalApplicationStatus.Succeeded, null, "N/A", 0.83789f, "NON-MAPREDUCE", null);
				applicationReports.AddItem(newApplicationReport4);
				reports = applicationReports;
			}
		}
	}
}
