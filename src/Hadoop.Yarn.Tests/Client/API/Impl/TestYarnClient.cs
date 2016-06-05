using System;
using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Client.Api;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Security.Client;
using Org.Apache.Hadoop.Yarn.Server;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Log4j;
using Org.Mockito;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Client.Api.Impl
{
	public class TestYarnClient
	{
		[NUnit.Framework.Test]
		public virtual void Test()
		{
		}

		// More to come later.
		[NUnit.Framework.Test]
		public virtual void TestClientStop()
		{
			Configuration conf = new Configuration();
			ResourceManager rm = new ResourceManager();
			rm.Init(conf);
			rm.Start();
			YarnClient client = YarnClient.CreateYarnClient();
			client.Init(conf);
			client.Start();
			client.Stop();
			rm.Stop();
		}

		public virtual void TestSubmitApplication()
		{
			Configuration conf = new Configuration();
			conf.SetLong(YarnConfiguration.YarnClientAppSubmissionPollIntervalMs, 100);
			// speed up tests
			YarnClient client = new TestYarnClient.MockYarnClient();
			client.Init(conf);
			client.Start();
			YarnApplicationState[] exitStates = new YarnApplicationState[] { YarnApplicationState
				.Accepted, YarnApplicationState.Running, YarnApplicationState.Finished };
			// Submit an application without ApplicationId provided
			// Should get ApplicationIdNotProvidedException
			ApplicationSubmissionContext contextWithoutApplicationId = Org.Mockito.Mockito.Mock
				<ApplicationSubmissionContext>();
			try
			{
				client.SubmitApplication(contextWithoutApplicationId);
				NUnit.Framework.Assert.Fail("Should throw the ApplicationIdNotProvidedException");
			}
			catch (YarnException e)
			{
				NUnit.Framework.Assert.IsTrue(e is ApplicationIdNotProvidedException);
				NUnit.Framework.Assert.IsTrue(e.Message.Contains("ApplicationId is not provided in ApplicationSubmissionContext"
					));
			}
			catch (IOException)
			{
				NUnit.Framework.Assert.Fail("IOException is not expected.");
			}
			// Submit the application with applicationId provided
			// Should be successful
			for (int i = 0; i < exitStates.Length; ++i)
			{
				ApplicationSubmissionContext context = Org.Mockito.Mockito.Mock<ApplicationSubmissionContext
					>();
				ApplicationId applicationId = ApplicationId.NewInstance(Runtime.CurrentTimeMillis
					(), i);
				Org.Mockito.Mockito.When(context.GetApplicationId()).ThenReturn(applicationId);
				((TestYarnClient.MockYarnClient)client).SetYarnApplicationState(exitStates[i]);
				try
				{
					client.SubmitApplication(context);
				}
				catch (YarnException)
				{
					NUnit.Framework.Assert.Fail("Exception is not expected.");
				}
				catch (IOException)
				{
					NUnit.Framework.Assert.Fail("Exception is not expected.");
				}
				Org.Mockito.Mockito.Verify(((TestYarnClient.MockYarnClient)client).mockReport, Org.Mockito.Mockito.Times
					(4 * i + 4)).GetYarnApplicationState();
			}
			client.Stop();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestSubmitIncorrectQueue()
		{
			MiniYARNCluster cluster = new MiniYARNCluster("testMRAMTokens", 1, 1, 1);
			YarnClient rmClient = null;
			try
			{
				cluster.Init(new YarnConfiguration());
				cluster.Start();
				Configuration yarnConf = cluster.GetConfig();
				rmClient = YarnClient.CreateYarnClient();
				rmClient.Init(yarnConf);
				rmClient.Start();
				YarnClientApplication newApp = rmClient.CreateApplication();
				ApplicationId appId = newApp.GetNewApplicationResponse().GetApplicationId();
				// Create launch context for app master
				ApplicationSubmissionContext appContext = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord
					<ApplicationSubmissionContext>();
				// set the application id
				appContext.SetApplicationId(appId);
				// set the application name
				appContext.SetApplicationName("test");
				// Set the queue to which this application is to be submitted in the RM
				appContext.SetQueue("nonexist");
				// Set up the container launch context for the application master
				ContainerLaunchContext amContainer = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord
					<ContainerLaunchContext>();
				appContext.SetAMContainerSpec(amContainer);
				appContext.SetResource(Resource.NewInstance(1024, 1));
				// appContext.setUnmanagedAM(unmanaged);
				// Submit the application to the applications manager
				rmClient.SubmitApplication(appContext);
				NUnit.Framework.Assert.Fail("Job submission should have thrown an exception");
			}
			catch (YarnException e)
			{
				NUnit.Framework.Assert.IsTrue(e.Message.Contains("Failed to submit"));
			}
			finally
			{
				if (rmClient != null)
				{
					rmClient.Stop();
				}
				cluster.Stop();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestKillApplication()
		{
			MockRM rm = new MockRM();
			rm.Start();
			RMApp app = rm.SubmitApp(2000);
			Configuration conf = new Configuration();
			YarnClient client = new TestYarnClient.MockYarnClient();
			client.Init(conf);
			client.Start();
			client.KillApplication(app.GetApplicationId());
			Org.Mockito.Mockito.Verify(((TestYarnClient.MockYarnClient)client).GetRMClient(), 
				Org.Mockito.Mockito.Times(2)).ForceKillApplication(Matchers.Any<KillApplicationRequest
				>());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestApplicationType()
		{
			Logger rootLogger = LogManager.GetRootLogger();
			rootLogger.SetLevel(Level.Debug);
			MockRM rm = new MockRM();
			rm.Start();
			RMApp app = rm.SubmitApp(2000);
			RMApp app1 = rm.SubmitApp(200, "name", "user", new Dictionary<ApplicationAccessType
				, string>(), false, "default", -1, null, "MAPREDUCE");
			NUnit.Framework.Assert.AreEqual("YARN", app.GetApplicationType());
			NUnit.Framework.Assert.AreEqual("MAPREDUCE", app1.GetApplicationType());
			rm.Stop();
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestApplicationTypeLimit()
		{
			Logger rootLogger = LogManager.GetRootLogger();
			rootLogger.SetLevel(Level.Debug);
			MockRM rm = new MockRM();
			rm.Start();
			RMApp app1 = rm.SubmitApp(200, "name", "user", new Dictionary<ApplicationAccessType
				, string>(), false, "default", -1, null, "MAPREDUCE-LENGTH-IS-20");
			NUnit.Framework.Assert.AreEqual("MAPREDUCE-LENGTH-IS-", app1.GetApplicationType()
				);
			rm.Stop();
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestGetApplications()
		{
			Configuration conf = new Configuration();
			YarnClient client = new TestYarnClient.MockYarnClient();
			client.Init(conf);
			client.Start();
			IList<ApplicationReport> expectedReports = ((TestYarnClient.MockYarnClient)client
				).GetReports();
			IList<ApplicationReport> reports = client.GetApplications();
			NUnit.Framework.Assert.AreEqual(reports, expectedReports);
			ICollection<string> appTypes = new HashSet<string>();
			appTypes.AddItem("YARN");
			appTypes.AddItem("NON-YARN");
			reports = client.GetApplications(appTypes, null);
			NUnit.Framework.Assert.AreEqual(reports.Count, 2);
			NUnit.Framework.Assert.IsTrue((reports[0].GetApplicationType().Equals("YARN") && 
				reports[1].GetApplicationType().Equals("NON-YARN")) || (reports[1].GetApplicationType
				().Equals("YARN") && reports[0].GetApplicationType().Equals("NON-YARN")));
			foreach (ApplicationReport report in reports)
			{
				NUnit.Framework.Assert.IsTrue(expectedReports.Contains(report));
			}
			EnumSet<YarnApplicationState> appStates = EnumSet.NoneOf<YarnApplicationState>();
			appStates.AddItem(YarnApplicationState.Finished);
			appStates.AddItem(YarnApplicationState.Failed);
			reports = client.GetApplications(null, appStates);
			NUnit.Framework.Assert.AreEqual(reports.Count, 2);
			NUnit.Framework.Assert.IsTrue((reports[0].GetApplicationType().Equals("NON-YARN")
				 && reports[1].GetApplicationType().Equals("NON-MAPREDUCE")) || (reports[1].GetApplicationType
				().Equals("NON-YARN") && reports[0].GetApplicationType().Equals("NON-MAPREDUCE")
				));
			foreach (ApplicationReport report_1 in reports)
			{
				NUnit.Framework.Assert.IsTrue(expectedReports.Contains(report_1));
			}
			reports = client.GetApplications(appTypes, appStates);
			NUnit.Framework.Assert.AreEqual(reports.Count, 1);
			NUnit.Framework.Assert.IsTrue((reports[0].GetApplicationType().Equals("NON-YARN")
				));
			foreach (ApplicationReport report_2 in reports)
			{
				NUnit.Framework.Assert.IsTrue(expectedReports.Contains(report_2));
			}
			client.Stop();
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestGetApplicationAttempts()
		{
			Configuration conf = new Configuration();
			YarnClient client = new TestYarnClient.MockYarnClient();
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
			YarnClient client = new TestYarnClient.MockYarnClient();
			client.Init(conf);
			client.Start();
			IList<ApplicationReport> expectedReports = ((TestYarnClient.MockYarnClient)client
				).GetReports();
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
			conf.SetBoolean(YarnConfiguration.ApplicationHistoryEnabled, true);
			YarnClient client = new TestYarnClient.MockYarnClient();
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
			NUnit.Framework.Assert.AreEqual(reports[2].GetContainerId(), (ContainerId.NewContainerId
				(appAttemptId, 3)));
			//First2 containers should come from RM with updated state information and 
			// 3rd container is not there in RM and should
			NUnit.Framework.Assert.AreEqual(ContainerState.Running, (reports[0].GetContainerState
				()));
			NUnit.Framework.Assert.AreEqual(ContainerState.Running, (reports[1].GetContainerState
				()));
			NUnit.Framework.Assert.AreEqual(ContainerState.Complete, (reports[2].GetContainerState
				()));
			client.Stop();
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestGetContainerReport()
		{
			Configuration conf = new Configuration();
			conf.SetBoolean(YarnConfiguration.ApplicationHistoryEnabled, true);
			YarnClient client = new TestYarnClient.MockYarnClient();
			client.Init(conf);
			client.Start();
			IList<ApplicationReport> expectedReports = ((TestYarnClient.MockYarnClient)client
				).GetReports();
			ApplicationId applicationId = ApplicationId.NewInstance(1234, 5);
			ApplicationAttemptId appAttemptId = ApplicationAttemptId.NewInstance(applicationId
				, 1);
			ContainerId containerId = ContainerId.NewContainerId(appAttemptId, 1);
			ContainerReport report = client.GetContainerReport(containerId);
			NUnit.Framework.Assert.IsNotNull(report);
			NUnit.Framework.Assert.AreEqual(report.GetContainerId().ToString(), (ContainerId.
				NewContainerId(expectedReports[0].GetCurrentApplicationAttemptId(), 1)).ToString
				());
			containerId = ContainerId.NewContainerId(appAttemptId, 3);
			report = client.GetContainerReport(containerId);
			NUnit.Framework.Assert.IsNotNull(report);
			NUnit.Framework.Assert.AreEqual(report.GetContainerId().ToString(), (ContainerId.
				NewContainerId(expectedReports[0].GetCurrentApplicationAttemptId(), 3)).ToString
				());
			client.Stop();
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestGetLabelsToNodes()
		{
			Configuration conf = new Configuration();
			YarnClient client = new TestYarnClient.MockYarnClient();
			client.Init(conf);
			client.Start();
			// Get labels to nodes mapping
			IDictionary<string, ICollection<NodeId>> expectedLabelsToNodes = ((TestYarnClient.MockYarnClient
				)client).GetLabelsToNodesMap();
			IDictionary<string, ICollection<NodeId>> labelsToNodes = client.GetLabelsToNodes(
				);
			NUnit.Framework.Assert.AreEqual(labelsToNodes, expectedLabelsToNodes);
			NUnit.Framework.Assert.AreEqual(labelsToNodes.Count, 3);
			// Get labels to nodes for selected labels
			ICollection<string> setLabels = new HashSet<string>(Arrays.AsList("x", "z"));
			expectedLabelsToNodes = ((TestYarnClient.MockYarnClient)client).GetLabelsToNodesMap
				(setLabels);
			labelsToNodes = client.GetLabelsToNodes(setLabels);
			NUnit.Framework.Assert.AreEqual(labelsToNodes, expectedLabelsToNodes);
			NUnit.Framework.Assert.AreEqual(labelsToNodes.Count, 2);
			client.Stop();
			client.Close();
		}

		private class MockYarnClient : YarnClientImpl
		{
			private ApplicationReport mockReport;

			private IList<ApplicationReport> reports;

			private Dictionary<ApplicationId, IList<ApplicationAttemptReport>> attempts = new 
				Dictionary<ApplicationId, IList<ApplicationAttemptReport>>();

			private Dictionary<ApplicationAttemptId, IList<ContainerReport>> containers = new 
				Dictionary<ApplicationAttemptId, IList<ContainerReport>>();

			private Dictionary<ApplicationAttemptId, IList<ContainerReport>> containersFromAHS
				 = new Dictionary<ApplicationAttemptId, IList<ContainerReport>>();

			internal GetApplicationsResponse mockAppResponse = Org.Mockito.Mockito.Mock<GetApplicationsResponse
				>();

			internal GetApplicationAttemptsResponse mockAppAttemptsResponse = Org.Mockito.Mockito.Mock
				<GetApplicationAttemptsResponse>();

			internal GetApplicationAttemptReportResponse mockAttemptResponse = Org.Mockito.Mockito.Mock
				<GetApplicationAttemptReportResponse>();

			internal GetContainersResponse mockContainersResponse = Org.Mockito.Mockito.Mock<
				GetContainersResponse>();

			internal GetContainerReportResponse mockContainerResponse = Org.Mockito.Mockito.Mock
				<GetContainerReportResponse>();

			internal GetLabelsToNodesResponse mockLabelsToNodesResponse = Org.Mockito.Mockito.Mock
				<GetLabelsToNodesResponse>();

			public MockYarnClient()
				: base()
			{
				reports = CreateAppReports();
			}

			public override void Start()
			{
				rmClient = Org.Mockito.Mockito.Mock<ApplicationClientProtocol>();
				GetApplicationReportResponse mockResponse = Org.Mockito.Mockito.Mock<GetApplicationReportResponse
					>();
				mockReport = Org.Mockito.Mockito.Mock<ApplicationReport>();
				try
				{
					Org.Mockito.Mockito.When(rmClient.GetApplicationReport(Matchers.Any<GetApplicationReportRequest
						>())).ThenReturn(mockResponse);
					Org.Mockito.Mockito.When(rmClient.GetApplications(Matchers.Any<GetApplicationsRequest
						>())).ThenReturn(mockAppResponse);
					// return false for 1st kill request, and true for the 2nd.
					Org.Mockito.Mockito.When(rmClient.ForceKillApplication(Matchers.Any<KillApplicationRequest
						>())).ThenReturn(KillApplicationResponse.NewInstance(false)).ThenReturn(KillApplicationResponse
						.NewInstance(true));
					Org.Mockito.Mockito.When(rmClient.GetApplicationAttemptReport(Matchers.Any<GetApplicationAttemptReportRequest
						>())).ThenReturn(mockAttemptResponse);
					Org.Mockito.Mockito.When(rmClient.GetApplicationAttempts(Matchers.Any<GetApplicationAttemptsRequest
						>())).ThenReturn(mockAppAttemptsResponse);
					Org.Mockito.Mockito.When(rmClient.GetContainers(Matchers.Any<GetContainersRequest
						>())).ThenReturn(mockContainersResponse);
					Org.Mockito.Mockito.When(rmClient.GetContainerReport(Matchers.Any<GetContainerReportRequest
						>())).ThenReturn(mockContainerResponse);
					Org.Mockito.Mockito.When(rmClient.GetLabelsToNodes(Matchers.Any<GetLabelsToNodesRequest
						>())).ThenReturn(mockLabelsToNodesResponse);
					historyClient = Org.Mockito.Mockito.Mock<AHSClient>();
				}
				catch (YarnException)
				{
					NUnit.Framework.Assert.Fail("Exception is not expected.");
				}
				catch (IOException)
				{
					NUnit.Framework.Assert.Fail("Exception is not expected.");
				}
				Org.Mockito.Mockito.When(mockResponse.GetApplicationReport()).ThenReturn(mockReport
					);
			}

			public virtual ApplicationClientProtocol GetRMClient()
			{
				return rmClient;
			}

			/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
			/// <exception cref="System.IO.IOException"/>
			public override IList<ApplicationReport> GetApplications(ICollection<string> applicationTypes
				, EnumSet<YarnApplicationState> applicationStates)
			{
				Org.Mockito.Mockito.When(mockAppResponse.GetApplicationList()).ThenReturn(GetApplicationReports
					(reports, applicationTypes, applicationStates));
				return base.GetApplications(applicationTypes, applicationStates);
			}

			public override void Stop()
			{
			}

			public virtual void SetYarnApplicationState(YarnApplicationState state)
			{
				Org.Mockito.Mockito.When(mockReport.GetYarnApplicationState()).ThenReturn(YarnApplicationState
					.New, YarnApplicationState.NewSaving, YarnApplicationState.NewSaving, state);
			}

			public virtual IList<ApplicationReport> GetReports()
			{
				return this.reports;
			}

			private IList<ApplicationReport> CreateAppReports()
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
					Priority.Undefined, 1234, 5678, "diagnosticInfo", "logURL", 0, ContainerState.Running
					, "http://" + NodeId.NewInstance("host", 2345).ToString());
				containerReports.AddItem(container);
				ContainerReport container1 = ContainerReport.NewInstance(ContainerId.NewContainerId
					(attempt.GetApplicationAttemptId(), 2), null, NodeId.NewInstance("host", 1234), 
					Priority.Undefined, 1234, 5678, "diagnosticInfo", "logURL", 0, ContainerState.Running
					, "http://" + NodeId.NewInstance("host", 2345).ToString());
				containerReports.AddItem(container1);
				containers[attempt.GetApplicationAttemptId()] = containerReports;
				//add containers to be sent from AHS
				IList<ContainerReport> containerReportsForAHS = new AList<ContainerReport>();
				container = ContainerReport.NewInstance(ContainerId.NewContainerId(attempt.GetApplicationAttemptId
					(), 1), null, NodeId.NewInstance("host", 1234), Priority.Undefined, 1234, 5678, 
					"diagnosticInfo", "logURL", 0, null, "http://" + NodeId.NewInstance("host", 2345
					).ToString());
				containerReportsForAHS.AddItem(container);
				container1 = ContainerReport.NewInstance(ContainerId.NewContainerId(attempt.GetApplicationAttemptId
					(), 2), null, NodeId.NewInstance("host", 1234), Priority.Undefined, 1234, 5678, 
					"diagnosticInfo", "HSlogURL", 0, null, "http://" + NodeId.NewInstance("host", 2345
					).ToString());
				containerReportsForAHS.AddItem(container1);
				ContainerReport container2 = ContainerReport.NewInstance(ContainerId.NewContainerId
					(attempt.GetApplicationAttemptId(), 3), null, NodeId.NewInstance("host", 1234), 
					Priority.Undefined, 1234, 5678, "diagnosticInfo", "HSlogURL", 0, ContainerState.
					Complete, "http://" + NodeId.NewInstance("host", 2345).ToString());
				containerReportsForAHS.AddItem(container2);
				containersFromAHS[attempt.GetApplicationAttemptId()] = containerReportsForAHS;
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
				return applicationReports;
			}

			private IList<ApplicationReport> GetApplicationReports(IList<ApplicationReport> applicationReports
				, ICollection<string> applicationTypes, EnumSet<YarnApplicationState> applicationStates
				)
			{
				IList<ApplicationReport> appReports = new AList<ApplicationReport>();
				foreach (ApplicationReport appReport in applicationReports)
				{
					if (applicationTypes != null && !applicationTypes.IsEmpty())
					{
						if (!applicationTypes.Contains(appReport.GetApplicationType()))
						{
							continue;
						}
					}
					if (applicationStates != null && !applicationStates.IsEmpty())
					{
						if (!applicationStates.Contains(appReport.GetYarnApplicationState()))
						{
							continue;
						}
					}
					appReports.AddItem(appReport);
				}
				return appReports;
			}

			/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
			/// <exception cref="System.IO.IOException"/>
			public override IDictionary<string, ICollection<NodeId>> GetLabelsToNodes()
			{
				Org.Mockito.Mockito.When(mockLabelsToNodesResponse.GetLabelsToNodes()).ThenReturn
					(GetLabelsToNodesMap());
				return base.GetLabelsToNodes();
			}

			/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
			/// <exception cref="System.IO.IOException"/>
			public override IDictionary<string, ICollection<NodeId>> GetLabelsToNodes(ICollection
				<string> labels)
			{
				Org.Mockito.Mockito.When(mockLabelsToNodesResponse.GetLabelsToNodes()).ThenReturn
					(GetLabelsToNodesMap(labels));
				return base.GetLabelsToNodes(labels);
			}

			public virtual IDictionary<string, ICollection<NodeId>> GetLabelsToNodesMap()
			{
				IDictionary<string, ICollection<NodeId>> map = new Dictionary<string, ICollection
					<NodeId>>();
				ICollection<NodeId> setNodeIds = new HashSet<NodeId>(Arrays.AsList(NodeId.NewInstance
					("host1", 0), NodeId.NewInstance("host2", 0)));
				map["x"] = setNodeIds;
				map["y"] = setNodeIds;
				map["z"] = setNodeIds;
				return map;
			}

			public virtual IDictionary<string, ICollection<NodeId>> GetLabelsToNodesMap(ICollection
				<string> labels)
			{
				IDictionary<string, ICollection<NodeId>> map = new Dictionary<string, ICollection
					<NodeId>>();
				ICollection<NodeId> setNodeIds = new HashSet<NodeId>(Arrays.AsList(NodeId.NewInstance
					("host1", 0), NodeId.NewInstance("host2", 0)));
				foreach (string label in labels)
				{
					map[label] = setNodeIds;
				}
				return map;
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
				Org.Mockito.Mockito.When(historyClient.GetContainers(Matchers.Any<ApplicationAttemptId
					>())).ThenReturn(GetContainersFromAHS(appAttemptId));
				return base.GetContainers(appAttemptId);
			}

			private IList<ContainerReport> GetContainersFromAHS(ApplicationAttemptId appAttemptId
				)
			{
				return containersFromAHS[appAttemptId];
			}

			/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
			/// <exception cref="System.IO.IOException"/>
			public override ContainerReport GetContainerReport(ContainerId containerId)
			{
				try
				{
					ContainerReport container = GetContainer(containerId, containers);
					Org.Mockito.Mockito.When(mockContainerResponse.GetContainerReport()).ThenReturn(container
						);
				}
				catch (YarnException e)
				{
					Org.Mockito.Mockito.When(rmClient.GetContainerReport(Matchers.Any<GetContainerReportRequest
						>())).ThenThrow(e).ThenReturn(mockContainerResponse);
				}
				try
				{
					ContainerReport container = GetContainer(containerId, containersFromAHS);
					Org.Mockito.Mockito.When(historyClient.GetContainerReport(Matchers.Any<ContainerId
						>())).ThenReturn(container);
				}
				catch (YarnException e)
				{
					Org.Mockito.Mockito.When(historyClient.GetContainerReport(Matchers.Any<ContainerId
						>())).ThenThrow(e);
				}
				return base.GetContainerReport(containerId);
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

			/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
			/// <exception cref="System.IO.IOException"/>
			private ContainerReport GetContainer(ContainerId containerId, Dictionary<ApplicationAttemptId
				, IList<ContainerReport>> containersToAppAttemptMapping)
			{
				IList<ContainerReport> containersForAppAttempt = containersToAppAttemptMapping[containerId
					.GetApplicationAttemptId()];
				if (containersForAppAttempt == null)
				{
					throw new ApplicationNotFoundException(containerId.GetApplicationAttemptId().GetApplicationId
						() + " is not found ");
				}
				IEnumerator<ContainerReport> iterator = containersForAppAttempt.GetEnumerator();
				while (iterator.HasNext())
				{
					ContainerReport next = iterator.Next();
					if (next.GetContainerId().Equals(containerId))
					{
						return next;
					}
				}
				throw new ContainerNotFoundException(containerId + " is not found ");
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestAMMRTokens()
		{
			MiniYARNCluster cluster = new MiniYARNCluster("testMRAMTokens", 1, 1, 1);
			YarnClient rmClient = null;
			try
			{
				cluster.Init(new YarnConfiguration());
				cluster.Start();
				Configuration yarnConf = cluster.GetConfig();
				rmClient = YarnClient.CreateYarnClient();
				rmClient.Init(yarnConf);
				rmClient.Start();
				ApplicationId appId = CreateApp(rmClient, false);
				WaitTillAccepted(rmClient, appId);
				//managed AMs don't return AMRM token
				NUnit.Framework.Assert.IsNull(rmClient.GetAMRMToken(appId));
				appId = CreateApp(rmClient, true);
				WaitTillAccepted(rmClient, appId);
				long start = Runtime.CurrentTimeMillis();
				while (rmClient.GetAMRMToken(appId) == null)
				{
					if (Runtime.CurrentTimeMillis() - start > 20 * 1000)
					{
						NUnit.Framework.Assert.Fail("AMRM token is null");
					}
					Sharpen.Thread.Sleep(100);
				}
				//unmanaged AMs do return AMRM token
				NUnit.Framework.Assert.IsNotNull(rmClient.GetAMRMToken(appId));
				UserGroupInformation other = UserGroupInformation.CreateUserForTesting("foo", new 
					string[] {  });
				appId = other.DoAs(new _PrivilegedExceptionAction_866(this, yarnConf));
				//unmanaged AMs do return AMRM token
				//other users don't get AMRM token
				NUnit.Framework.Assert.IsNull(rmClient.GetAMRMToken(appId));
			}
			finally
			{
				if (rmClient != null)
				{
					rmClient.Stop();
				}
				cluster.Stop();
			}
		}

		private sealed class _PrivilegedExceptionAction_866 : PrivilegedExceptionAction<ApplicationId
			>
		{
			public _PrivilegedExceptionAction_866(TestYarnClient _enclosing, Configuration yarnConf
				)
			{
				this._enclosing = _enclosing;
				this.yarnConf = yarnConf;
			}

			/// <exception cref="System.Exception"/>
			public ApplicationId Run()
			{
				YarnClient rmClient = YarnClient.CreateYarnClient();
				rmClient.Init(yarnConf);
				rmClient.Start();
				ApplicationId appId = this._enclosing.CreateApp(rmClient, true);
				this._enclosing.WaitTillAccepted(rmClient, appId);
				long start = Runtime.CurrentTimeMillis();
				while (rmClient.GetAMRMToken(appId) == null)
				{
					if (Runtime.CurrentTimeMillis() - start > 20 * 1000)
					{
						NUnit.Framework.Assert.Fail("AMRM token is null");
					}
					Sharpen.Thread.Sleep(100);
				}
				NUnit.Framework.Assert.IsNotNull(rmClient.GetAMRMToken(appId));
				return appId;
			}

			private readonly TestYarnClient _enclosing;

			private readonly Configuration yarnConf;
		}

		/// <exception cref="System.Exception"/>
		private ApplicationId CreateApp(YarnClient rmClient, bool unmanaged)
		{
			YarnClientApplication newApp = rmClient.CreateApplication();
			ApplicationId appId = newApp.GetNewApplicationResponse().GetApplicationId();
			// Create launch context for app master
			ApplicationSubmissionContext appContext = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord
				<ApplicationSubmissionContext>();
			// set the application id
			appContext.SetApplicationId(appId);
			// set the application name
			appContext.SetApplicationName("test");
			// Set the priority for the application master
			Priority pri = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<Priority>();
			pri.SetPriority(1);
			appContext.SetPriority(pri);
			// Set the queue to which this application is to be submitted in the RM
			appContext.SetQueue("default");
			// Set up the container launch context for the application master
			ContainerLaunchContext amContainer = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord
				<ContainerLaunchContext>();
			appContext.SetAMContainerSpec(amContainer);
			appContext.SetResource(Resource.NewInstance(1024, 1));
			appContext.SetUnmanagedAM(unmanaged);
			// Submit the application to the applications manager
			rmClient.SubmitApplication(appContext);
			return appId;
		}

		/// <exception cref="System.Exception"/>
		private void WaitTillAccepted(YarnClient rmClient, ApplicationId appId)
		{
			try
			{
				long start = Runtime.CurrentTimeMillis();
				ApplicationReport report = rmClient.GetApplicationReport(appId);
				while (YarnApplicationState.Accepted != report.GetYarnApplicationState())
				{
					if (Runtime.CurrentTimeMillis() - start > 20 * 1000)
					{
						throw new Exception("App '" + appId + "' time out, failed to reach ACCEPTED state"
							);
					}
					Sharpen.Thread.Sleep(200);
					report = rmClient.GetApplicationReport(appId);
				}
			}
			catch (Exception ex)
			{
				throw new Exception(ex);
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestAsyncAPIPollTimeout()
		{
			TestAsyncAPIPollTimeoutHelper(null, false);
			TestAsyncAPIPollTimeoutHelper(0L, true);
			TestAsyncAPIPollTimeoutHelper(1L, true);
		}

		private void TestAsyncAPIPollTimeoutHelper(long valueForTimeout, bool expectedTimeoutEnforcement
			)
		{
			YarnClientImpl client = new YarnClientImpl();
			try
			{
				Configuration conf = new Configuration();
				if (valueForTimeout != null)
				{
					conf.SetLong(YarnConfiguration.YarnClientApplicationClientProtocolPollTimeoutMs, 
						valueForTimeout);
				}
				client.Init(conf);
				NUnit.Framework.Assert.AreEqual(expectedTimeoutEnforcement, client.EnforceAsyncAPITimeout
					());
			}
			finally
			{
				IOUtils.CloseQuietly(client);
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestBestEffortTimelineDelegationToken()
		{
			Configuration conf = new YarnConfiguration();
			conf.SetBoolean(YarnConfiguration.TimelineServiceEnabled, true);
			SecurityUtil.SetAuthenticationMethod(UserGroupInformation.AuthenticationMethod.Kerberos
				, conf);
			YarnClientImpl client = Org.Mockito.Mockito.Spy(new _YarnClientImpl_985());
			client.Init(conf);
			try
			{
				conf.SetBoolean(YarnConfiguration.TimelineServiceClientBestEffort, true);
				client.ServiceInit(conf);
				client.GetTimelineDelegationToken();
			}
			catch (Exception)
			{
				NUnit.Framework.Assert.Fail("Should not have thrown an exception");
			}
			try
			{
				conf.SetBoolean(YarnConfiguration.TimelineServiceClientBestEffort, false);
				client.ServiceInit(conf);
				client.GetTimelineDelegationToken();
				NUnit.Framework.Assert.Fail("Get delegation token should have thrown an exception"
					);
			}
			catch (Exception)
			{
			}
		}

		private sealed class _YarnClientImpl_985 : YarnClientImpl
		{
			public _YarnClientImpl_985()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
			internal override TimelineClient CreateTimelineClient()
			{
				this.timelineClient = Org.Mockito.Mockito.Mock<TimelineClient>();
				Org.Mockito.Mockito.When(this.timelineClient.GetDelegationToken(Matchers.Any<string
					>())).ThenThrow(new IOException("Best effort test exception"));
				return this.timelineClient;
			}
		}

		// Success
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAutomaticTimelineDelegationTokenLoading()
		{
			Configuration conf = new YarnConfiguration();
			conf.SetBoolean(YarnConfiguration.TimelineServiceEnabled, true);
			SecurityUtil.SetAuthenticationMethod(UserGroupInformation.AuthenticationMethod.Kerberos
				, conf);
			TimelineDelegationTokenIdentifier timelineDT = new TimelineDelegationTokenIdentifier
				();
			Org.Apache.Hadoop.Security.Token.Token<TimelineDelegationTokenIdentifier> dToken = 
				new Org.Apache.Hadoop.Security.Token.Token<TimelineDelegationTokenIdentifier>(timelineDT
				.GetBytes(), new byte[0], timelineDT.GetKind(), new Text());
			// create a mock client
			YarnClientImpl client = Org.Mockito.Mockito.Spy(new _YarnClientImpl_1026(dToken));
			client.Init(conf);
			client.Start();
			try
			{
				// when i == 0, timeline DT already exists, no need to get one more
				// when i == 1, timeline DT doesn't exist, need to get one more
				for (int i = 0; i < 2; ++i)
				{
					ApplicationSubmissionContext context = Org.Mockito.Mockito.Mock<ApplicationSubmissionContext
						>();
					ApplicationId applicationId = ApplicationId.NewInstance(0, i + 1);
					Org.Mockito.Mockito.When(context.GetApplicationId()).ThenReturn(applicationId);
					DataOutputBuffer dob = new DataOutputBuffer();
					Credentials credentials = new Credentials();
					if (i == 0)
					{
						credentials.AddToken(client.timelineService, dToken);
					}
					credentials.WriteTokenStorageToStream(dob);
					ByteBuffer tokens = ByteBuffer.Wrap(dob.GetData(), 0, dob.GetLength());
					ContainerLaunchContext clc = ContainerLaunchContext.NewInstance(null, null, null, 
						null, tokens, null);
					Org.Mockito.Mockito.When(context.GetAMContainerSpec()).ThenReturn(clc);
					client.SubmitApplication(context);
					if (i == 0)
					{
						// GetTimelineDelegationToken shouldn't be called
						Org.Mockito.Mockito.Verify(client, Org.Mockito.Mockito.Never()).GetTimelineDelegationToken
							();
					}
					// In either way, token should be there
					credentials = new Credentials();
					DataInputByteBuffer dibb = new DataInputByteBuffer();
					tokens = clc.GetTokens();
					if (tokens != null)
					{
						dibb.Reset(tokens);
						credentials.ReadTokenStorageStream(dibb);
						tokens.Rewind();
					}
					ICollection<Org.Apache.Hadoop.Security.Token.Token<TokenIdentifier>> dTokens = credentials
						.GetAllTokens();
					NUnit.Framework.Assert.AreEqual(1, dTokens.Count);
					NUnit.Framework.Assert.AreEqual(dToken, dTokens.GetEnumerator().Next());
				}
			}
			finally
			{
				client.Stop();
			}
		}

		private sealed class _YarnClientImpl_1026 : YarnClientImpl
		{
			public _YarnClientImpl_1026(Org.Apache.Hadoop.Security.Token.Token<TimelineDelegationTokenIdentifier
				> dToken)
			{
				this.dToken = dToken;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
			internal override TimelineClient CreateTimelineClient()
			{
				this.timelineClient = Org.Mockito.Mockito.Mock<TimelineClient>();
				Org.Mockito.Mockito.When(this.timelineClient.GetDelegationToken(Matchers.Any<string
					>())).ThenReturn(dToken);
				return this.timelineClient;
			}

			/// <exception cref="System.Exception"/>
			protected override void ServiceStart()
			{
				this.rmClient = Org.Mockito.Mockito.Mock<ApplicationClientProtocol>();
			}

			/// <exception cref="System.Exception"/>
			protected override void ServiceStop()
			{
			}

			public override ApplicationReport GetApplicationReport(ApplicationId appId)
			{
				ApplicationReport report = Org.Mockito.Mockito.Mock<ApplicationReport>();
				Org.Mockito.Mockito.When(report.GetYarnApplicationState()).ThenReturn(YarnApplicationState
					.Running);
				return report;
			}

			protected internal override bool IsSecurityEnabled()
			{
				return true;
			}

			private readonly Org.Apache.Hadoop.Security.Token.Token<TimelineDelegationTokenIdentifier
				> dToken;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestParseTimelineDelegationTokenRenewer()
		{
			// Client side
			YarnClientImpl client = (YarnClientImpl)YarnClient.CreateYarnClient();
			Configuration conf = new YarnConfiguration();
			conf.SetBoolean(YarnConfiguration.TimelineServiceEnabled, true);
			conf.Set(YarnConfiguration.RmPrincipal, "rm/_HOST@EXAMPLE.COM");
			conf.Set(YarnConfiguration.RmAddress, "localhost:8188");
			try
			{
				client.Init(conf);
				client.Start();
				NUnit.Framework.Assert.AreEqual("rm/localhost@EXAMPLE.COM", client.timelineDTRenewer
					);
			}
			finally
			{
				client.Stop();
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestReservationAPIs()
		{
			// initialize
			CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
			ReservationSystemTestUtil.SetupQueueConfiguration(conf);
			conf.SetClass(YarnConfiguration.RmScheduler, typeof(CapacityScheduler), typeof(ResourceScheduler
				));
			conf.SetBoolean(YarnConfiguration.RmReservationSystemEnable, true);
			MiniYARNCluster cluster = new MiniYARNCluster("testReservationAPIs", 2, 1, 1);
			YarnClient client = null;
			try
			{
				cluster.Init(conf);
				cluster.Start();
				Configuration yarnConf = cluster.GetConfig();
				client = YarnClient.CreateYarnClient();
				client.Init(yarnConf);
				client.Start();
				// create a reservation
				Clock clock = new UTCClock();
				long arrival = clock.GetTime();
				long duration = 60000;
				long deadline = (long)(arrival + 1.05 * duration);
				ReservationSubmissionRequest sRequest = CreateSimpleReservationRequest(4, arrival
					, deadline, duration);
				ReservationSubmissionResponse sResponse = null;
				try
				{
					sResponse = client.SubmitReservation(sRequest);
				}
				catch (Exception e)
				{
					NUnit.Framework.Assert.Fail(e.Message);
				}
				NUnit.Framework.Assert.IsNotNull(sResponse);
				ReservationId reservationID = sResponse.GetReservationId();
				NUnit.Framework.Assert.IsNotNull(reservationID);
				System.Console.Out.WriteLine("Submit reservation response: " + reservationID);
				// Update the reservation
				ReservationDefinition rDef = sRequest.GetReservationDefinition();
				ReservationRequest rr = rDef.GetReservationRequests().GetReservationResources()[0
					];
				rr.SetNumContainers(5);
				arrival = clock.GetTime();
				duration = 30000;
				deadline = (long)(arrival + 1.05 * duration);
				rr.SetDuration(duration);
				rDef.SetArrival(arrival);
				rDef.SetDeadline(deadline);
				ReservationUpdateRequest uRequest = ReservationUpdateRequest.NewInstance(rDef, reservationID
					);
				ReservationUpdateResponse uResponse = null;
				try
				{
					uResponse = client.UpdateReservation(uRequest);
				}
				catch (Exception e)
				{
					NUnit.Framework.Assert.Fail(e.Message);
				}
				NUnit.Framework.Assert.IsNotNull(sResponse);
				System.Console.Out.WriteLine("Update reservation response: " + uResponse);
				// Delete the reservation
				ReservationDeleteRequest dRequest = ReservationDeleteRequest.NewInstance(reservationID
					);
				ReservationDeleteResponse dResponse = null;
				try
				{
					dResponse = client.DeleteReservation(dRequest);
				}
				catch (Exception e)
				{
					NUnit.Framework.Assert.Fail(e.Message);
				}
				NUnit.Framework.Assert.IsNotNull(sResponse);
				System.Console.Out.WriteLine("Delete reservation response: " + dResponse);
			}
			finally
			{
				// clean-up
				if (client != null)
				{
					client.Stop();
				}
				cluster.Stop();
			}
		}

		private ReservationSubmissionRequest CreateSimpleReservationRequest(int numContainers
			, long arrival, long deadline, long duration)
		{
			// create a request with a single atomic ask
			ReservationRequest r = ReservationRequest.NewInstance(Resource.NewInstance(1024, 
				1), numContainers, 1, duration);
			ReservationRequests reqs = ReservationRequests.NewInstance(Sharpen.Collections.SingletonList
				(r), ReservationRequestInterpreter.RAll);
			ReservationDefinition rDef = ReservationDefinition.NewInstance(arrival, deadline, 
				reqs, "testYarnClient#reservation");
			ReservationSubmissionRequest request = ReservationSubmissionRequest.NewInstance(rDef
				, ReservationSystemTestUtil.reservationQ);
			return request;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestShouldNotRetryForeverForNonNetworkExceptions()
		{
			YarnConfiguration conf = new YarnConfiguration();
			conf.SetInt(YarnConfiguration.ResourcemanagerConnectMaxWaitMs, -1);
			ResourceManager rm = null;
			YarnClient yarnClient = null;
			try
			{
				// start rm
				rm = new ResourceManager();
				rm.Init(conf);
				rm.Start();
				yarnClient = YarnClient.CreateYarnClient();
				yarnClient.Init(conf);
				yarnClient.Start();
				// create invalid application id
				ApplicationId appId = ApplicationId.NewInstance(1430126768L, 10645);
				// RM should throw ApplicationNotFoundException exception
				yarnClient.GetApplicationReport(appId);
			}
			finally
			{
				if (yarnClient != null)
				{
					yarnClient.Stop();
				}
				if (rm != null)
				{
					rm.Stop();
				}
			}
		}
	}
}
