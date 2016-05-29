using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Com.Google.Common.Collect;
using NUnit.Framework;
using Org.Apache.Commons.Cli;
using Org.Apache.Commons.Lang.Time;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Client.Api;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Mockito;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Client.Cli
{
	public class TestYarnCLI
	{
		private YarnClient client = Org.Mockito.Mockito.Mock<YarnClient>();

		internal ByteArrayOutputStream sysOutStream;

		private TextWriter sysOut;

		internal ByteArrayOutputStream sysErrStream;

		private TextWriter sysErr;

		[SetUp]
		public virtual void Setup()
		{
			sysOutStream = new ByteArrayOutputStream();
			sysOut = Org.Mockito.Mockito.Spy(new TextWriter(sysOutStream));
			sysErrStream = new ByteArrayOutputStream();
			sysErr = Org.Mockito.Mockito.Spy(new TextWriter(sysErrStream));
			Runtime.SetOut(sysOut);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetApplicationReport()
		{
			for (int i = 0; i < 2; ++i)
			{
				ApplicationCLI cli = CreateAndGetAppCLI();
				ApplicationId applicationId = ApplicationId.NewInstance(1234, 5);
				ApplicationResourceUsageReport usageReport = i == 0 ? null : ApplicationResourceUsageReport
					.NewInstance(2, 0, null, null, null, 123456, 4567);
				ApplicationReport newApplicationReport = ApplicationReport.NewInstance(applicationId
					, ApplicationAttemptId.NewInstance(applicationId, 1), "user", "queue", "appname"
					, "host", 124, null, YarnApplicationState.Finished, "diagnostics", "url", 0, 0, 
					FinalApplicationStatus.Succeeded, usageReport, "N/A", 0.53789f, "YARN", null);
				Org.Mockito.Mockito.When(client.GetApplicationReport(Matchers.Any<ApplicationId>(
					))).ThenReturn(newApplicationReport);
				int result = cli.Run(new string[] { "application", "-status", applicationId.ToString
					() });
				NUnit.Framework.Assert.AreEqual(0, result);
				Org.Mockito.Mockito.Verify(client, Org.Mockito.Mockito.Times(1 + i)).GetApplicationReport
					(applicationId);
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				PrintWriter pw = new PrintWriter(baos);
				pw.WriteLine("Application Report : ");
				pw.WriteLine("\tApplication-Id : application_1234_0005");
				pw.WriteLine("\tApplication-Name : appname");
				pw.WriteLine("\tApplication-Type : YARN");
				pw.WriteLine("\tUser : user");
				pw.WriteLine("\tQueue : queue");
				pw.WriteLine("\tStart-Time : 0");
				pw.WriteLine("\tFinish-Time : 0");
				pw.WriteLine("\tProgress : 53.79%");
				pw.WriteLine("\tState : FINISHED");
				pw.WriteLine("\tFinal-State : SUCCEEDED");
				pw.WriteLine("\tTracking-URL : N/A");
				pw.WriteLine("\tRPC Port : 124");
				pw.WriteLine("\tAM Host : host");
				pw.WriteLine("\tAggregate Resource Allocation : " + (i == 0 ? "N/A" : "123456 MB-seconds, 4567 vcore-seconds"
					));
				pw.WriteLine("\tDiagnostics : diagnostics");
				pw.Close();
				string appReportStr = baos.ToString("UTF-8");
				NUnit.Framework.Assert.AreEqual(appReportStr, sysOutStream.ToString());
				sysOutStream.Reset();
				Org.Mockito.Mockito.Verify(sysOut, Org.Mockito.Mockito.Times(1 + i)).WriteLine(Matchers.IsA
					<string>());
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetApplicationAttemptReport()
		{
			ApplicationCLI cli = CreateAndGetAppCLI();
			ApplicationId applicationId = ApplicationId.NewInstance(1234, 5);
			ApplicationAttemptId attemptId = ApplicationAttemptId.NewInstance(applicationId, 
				1);
			ApplicationAttemptReport attemptReport = ApplicationAttemptReport.NewInstance(attemptId
				, "host", 124, "url", "oUrl", "diagnostics", YarnApplicationAttemptState.Finished
				, ContainerId.NewContainerId(attemptId, 1));
			Org.Mockito.Mockito.When(client.GetApplicationAttemptReport(Matchers.Any<ApplicationAttemptId
				>())).ThenReturn(attemptReport);
			int result = cli.Run(new string[] { "applicationattempt", "-status", attemptId.ToString
				() });
			NUnit.Framework.Assert.AreEqual(0, result);
			Org.Mockito.Mockito.Verify(client).GetApplicationAttemptReport(attemptId);
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			PrintWriter pw = new PrintWriter(baos);
			pw.WriteLine("Application Attempt Report : ");
			pw.WriteLine("\tApplicationAttempt-Id : appattempt_1234_0005_000001");
			pw.WriteLine("\tState : FINISHED");
			pw.WriteLine("\tAMContainer : container_1234_0005_01_000001");
			pw.WriteLine("\tTracking-URL : url");
			pw.WriteLine("\tRPC Port : 124");
			pw.WriteLine("\tAM Host : host");
			pw.WriteLine("\tDiagnostics : diagnostics");
			pw.Close();
			string appReportStr = baos.ToString("UTF-8");
			NUnit.Framework.Assert.AreEqual(appReportStr, sysOutStream.ToString());
			Org.Mockito.Mockito.Verify(sysOut, Org.Mockito.Mockito.Times(1)).WriteLine(Matchers.IsA
				<string>());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetApplicationAttempts()
		{
			ApplicationCLI cli = CreateAndGetAppCLI();
			ApplicationId applicationId = ApplicationId.NewInstance(1234, 5);
			ApplicationAttemptId attemptId = ApplicationAttemptId.NewInstance(applicationId, 
				1);
			ApplicationAttemptId attemptId1 = ApplicationAttemptId.NewInstance(applicationId, 
				2);
			ApplicationAttemptReport attemptReport = ApplicationAttemptReport.NewInstance(attemptId
				, "host", 124, "url", "oUrl", "diagnostics", YarnApplicationAttemptState.Finished
				, ContainerId.NewContainerId(attemptId, 1));
			ApplicationAttemptReport attemptReport1 = ApplicationAttemptReport.NewInstance(attemptId1
				, "host", 124, "url", "oUrl", "diagnostics", YarnApplicationAttemptState.Finished
				, ContainerId.NewContainerId(attemptId1, 1));
			IList<ApplicationAttemptReport> reports = new AList<ApplicationAttemptReport>();
			reports.AddItem(attemptReport);
			reports.AddItem(attemptReport1);
			Org.Mockito.Mockito.When(client.GetApplicationAttempts(Matchers.Any<ApplicationId
				>())).ThenReturn(reports);
			int result = cli.Run(new string[] { "applicationattempt", "-list", applicationId.
				ToString() });
			NUnit.Framework.Assert.AreEqual(0, result);
			Org.Mockito.Mockito.Verify(client).GetApplicationAttempts(applicationId);
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			PrintWriter pw = new PrintWriter(baos);
			pw.WriteLine("Total number of application attempts :2");
			pw.Write("         ApplicationAttempt-Id");
			pw.Write("\t               State");
			pw.Write("\t                    AM-Container-Id");
			pw.WriteLine("\t                       Tracking-URL");
			pw.Write("   appattempt_1234_0005_000001");
			pw.Write("\t            FINISHED");
			pw.Write("\t      container_1234_0005_01_000001");
			pw.WriteLine("\t                                url");
			pw.Write("   appattempt_1234_0005_000002");
			pw.Write("\t            FINISHED");
			pw.Write("\t      container_1234_0005_02_000001");
			pw.WriteLine("\t                                url");
			pw.Close();
			string appReportStr = baos.ToString("UTF-8");
			NUnit.Framework.Assert.AreEqual(appReportStr, sysOutStream.ToString());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetContainerReport()
		{
			ApplicationCLI cli = CreateAndGetAppCLI();
			ApplicationId applicationId = ApplicationId.NewInstance(1234, 5);
			ApplicationAttemptId attemptId = ApplicationAttemptId.NewInstance(applicationId, 
				1);
			ContainerId containerId = ContainerId.NewContainerId(attemptId, 1);
			ContainerReport container = ContainerReport.NewInstance(containerId, null, NodeId
				.NewInstance("host", 1234), Priority.Undefined, 1234, 5678, "diagnosticInfo", "logURL"
				, 0, ContainerState.Complete, "http://" + NodeId.NewInstance("host", 2345).ToString
				());
			Org.Mockito.Mockito.When(client.GetContainerReport(Matchers.Any<ContainerId>())).
				ThenReturn(container);
			int result = cli.Run(new string[] { "container", "-status", containerId.ToString(
				) });
			NUnit.Framework.Assert.AreEqual(0, result);
			Org.Mockito.Mockito.Verify(client).GetContainerReport(containerId);
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			PrintWriter pw = new PrintWriter(baos);
			pw.WriteLine("Container Report : ");
			pw.WriteLine("\tContainer-Id : container_1234_0005_01_000001");
			pw.WriteLine("\tStart-Time : 1234");
			pw.WriteLine("\tFinish-Time : 5678");
			pw.WriteLine("\tState : COMPLETE");
			pw.WriteLine("\tLOG-URL : logURL");
			pw.WriteLine("\tHost : host:1234");
			pw.WriteLine("\tNodeHttpAddress : http://host:2345");
			pw.WriteLine("\tDiagnostics : diagnosticInfo");
			pw.Close();
			string appReportStr = baos.ToString("UTF-8");
			NUnit.Framework.Assert.AreEqual(appReportStr, sysOutStream.ToString());
			Org.Mockito.Mockito.Verify(sysOut, Org.Mockito.Mockito.Times(1)).WriteLine(Matchers.IsA
				<string>());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetContainers()
		{
			ApplicationCLI cli = CreateAndGetAppCLI();
			ApplicationId applicationId = ApplicationId.NewInstance(1234, 5);
			ApplicationAttemptId attemptId = ApplicationAttemptId.NewInstance(applicationId, 
				1);
			ContainerId containerId = ContainerId.NewContainerId(attemptId, 1);
			ContainerId containerId1 = ContainerId.NewContainerId(attemptId, 2);
			ContainerId containerId2 = ContainerId.NewContainerId(attemptId, 3);
			long time1 = 1234;
			long time2 = 5678;
			ContainerReport container = ContainerReport.NewInstance(containerId, null, NodeId
				.NewInstance("host", 1234), Priority.Undefined, time1, time2, "diagnosticInfo", 
				"logURL", 0, ContainerState.Complete, "http://" + NodeId.NewInstance("host", 2345
				).ToString());
			ContainerReport container1 = ContainerReport.NewInstance(containerId1, null, NodeId
				.NewInstance("host", 1234), Priority.Undefined, time1, time2, "diagnosticInfo", 
				"logURL", 0, ContainerState.Complete, "http://" + NodeId.NewInstance("host", 2345
				).ToString());
			ContainerReport container2 = ContainerReport.NewInstance(containerId2, null, NodeId
				.NewInstance("host", 1234), Priority.Undefined, time1, 0, "diagnosticInfo", string.Empty
				, 0, ContainerState.Running, "http://" + NodeId.NewInstance("host", 2345).ToString
				());
			IList<ContainerReport> reports = new AList<ContainerReport>();
			reports.AddItem(container);
			reports.AddItem(container1);
			reports.AddItem(container2);
			DateFormat dateFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy");
			Org.Mockito.Mockito.When(client.GetContainers(Matchers.Any<ApplicationAttemptId>(
				))).ThenReturn(reports);
			sysOutStream.Reset();
			int result = cli.Run(new string[] { "container", "-list", attemptId.ToString() });
			NUnit.Framework.Assert.AreEqual(0, result);
			Org.Mockito.Mockito.Verify(client).GetContainers(attemptId);
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			PrintWriter pw = new PrintWriter(baos);
			pw.WriteLine("Total number of containers :3");
			pw.Write("                  Container-Id");
			pw.Write("\t          Start Time");
			pw.Write("\t         Finish Time");
			pw.Write("\t               State");
			pw.Write("\t                Host");
			pw.Write("\t   Node Http Address");
			pw.WriteLine("\t                            LOG-URL");
			pw.Write(" container_1234_0005_01_000001");
			pw.Write("\t" + dateFormat.Format(Sharpen.Extensions.CreateDate(time1)));
			pw.Write("\t" + dateFormat.Format(Sharpen.Extensions.CreateDate(time2)));
			pw.Write("\t            COMPLETE");
			pw.Write("\t           host:1234");
			pw.Write("\t    http://host:2345");
			pw.WriteLine("\t                             logURL");
			pw.Write(" container_1234_0005_01_000002");
			pw.Write("\t" + dateFormat.Format(Sharpen.Extensions.CreateDate(time1)));
			pw.Write("\t" + dateFormat.Format(Sharpen.Extensions.CreateDate(time2)));
			pw.Write("\t            COMPLETE");
			pw.Write("\t           host:1234");
			pw.Write("\t    http://host:2345");
			pw.WriteLine("\t                             logURL");
			pw.Write(" container_1234_0005_01_000003");
			pw.Write("\t" + dateFormat.Format(Sharpen.Extensions.CreateDate(time1)));
			pw.Write("\t                 N/A");
			pw.Write("\t             RUNNING");
			pw.Write("\t           host:1234");
			pw.Write("\t    http://host:2345");
			pw.WriteLine("\t                                   ");
			pw.Close();
			string appReportStr = baos.ToString("UTF-8");
			Org.Mortbay.Log.Log.Info("ExpectedOutput");
			Org.Mortbay.Log.Log.Info("[" + appReportStr + "]");
			Org.Mortbay.Log.Log.Info("OutputFrom command");
			string actualOutput = sysOutStream.ToString();
			Org.Mortbay.Log.Log.Info("[" + actualOutput + "]");
			NUnit.Framework.Assert.AreEqual(appReportStr, sysOutStream.ToString());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetApplicationReportException()
		{
			ApplicationCLI cli = CreateAndGetAppCLI();
			ApplicationId applicationId = ApplicationId.NewInstance(1234, 5);
			Org.Mockito.Mockito.When(client.GetApplicationReport(Matchers.Any<ApplicationId>(
				))).ThenThrow(new ApplicationNotFoundException("History file for application" + 
				applicationId + " is not found"));
			int exitCode = cli.Run(new string[] { "application", "-status", applicationId.ToString
				() });
			Org.Mockito.Mockito.Verify(sysOut).WriteLine("Application with id '" + applicationId
				 + "' doesn't exist in RM or Timeline Server.");
			NUnit.Framework.Assert.AreNotSame("should return non-zero exit code.", 0, exitCode
				);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetApplications()
		{
			ApplicationCLI cli = CreateAndGetAppCLI();
			ApplicationId applicationId = ApplicationId.NewInstance(1234, 5);
			ApplicationReport newApplicationReport = ApplicationReport.NewInstance(applicationId
				, ApplicationAttemptId.NewInstance(applicationId, 1), "user", "queue", "appname"
				, "host", 124, null, YarnApplicationState.Running, "diagnostics", "url", 0, 0, FinalApplicationStatus
				.Succeeded, null, "N/A", 0.53789f, "YARN", null);
			IList<ApplicationReport> applicationReports = new AList<ApplicationReport>();
			applicationReports.AddItem(newApplicationReport);
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
			ApplicationId applicationId5 = ApplicationId.NewInstance(1234, 9);
			ApplicationReport newApplicationReport5 = ApplicationReport.NewInstance(applicationId5
				, ApplicationAttemptId.NewInstance(applicationId5, 5), "user5", "queue5", "appname5"
				, "host5", 128, null, YarnApplicationState.Accepted, "diagnostics5", "url5", 5, 
				5, FinalApplicationStatus.Killed, null, "N/A", 0.93789f, "HIVE", null);
			applicationReports.AddItem(newApplicationReport5);
			ApplicationId applicationId6 = ApplicationId.NewInstance(1234, 10);
			ApplicationReport newApplicationReport6 = ApplicationReport.NewInstance(applicationId6
				, ApplicationAttemptId.NewInstance(applicationId6, 6), "user6", "queue6", "appname6"
				, "host6", 129, null, YarnApplicationState.Submitted, "diagnostics6", "url6", 6, 
				6, FinalApplicationStatus.Killed, null, "N/A", 0.99789f, "PIG", null);
			applicationReports.AddItem(newApplicationReport6);
			// Test command yarn application -list
			// if the set appStates is empty, RUNNING state will be automatically added
			// to the appStates list
			// the output of yarn application -list should be the same as
			// equals to yarn application -list --appStates RUNNING,ACCEPTED,SUBMITTED
			ICollection<string> appType1 = new HashSet<string>();
			EnumSet<YarnApplicationState> appState1 = EnumSet.NoneOf<YarnApplicationState>();
			appState1.AddItem(YarnApplicationState.Running);
			appState1.AddItem(YarnApplicationState.Accepted);
			appState1.AddItem(YarnApplicationState.Submitted);
			Org.Mockito.Mockito.When(client.GetApplications(appType1, appState1)).ThenReturn(
				GetApplicationReports(applicationReports, appType1, appState1, false));
			int result = cli.Run(new string[] { "application", "-list" });
			NUnit.Framework.Assert.AreEqual(0, result);
			Org.Mockito.Mockito.Verify(client).GetApplications(appType1, appState1);
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			PrintWriter pw = new PrintWriter(baos);
			pw.WriteLine("Total number of applications (application-types: " + appType1 + " and states: "
				 + appState1 + ")" + ":" + 4);
			pw.Write("                Application-Id\t    Application-Name");
			pw.Write("\t    Application-Type");
			pw.Write("\t      User\t     Queue\t             State\t       ");
			pw.Write("Final-State\t       Progress");
			pw.WriteLine("\t                       Tracking-URL");
			pw.Write("         application_1234_0005\t             ");
			pw.Write("appname\t                YARN\t      user\t     ");
			pw.Write("queue\t           RUNNING\t         ");
			pw.Write("SUCCEEDED\t         53.79%");
			pw.WriteLine("\t                                N/A");
			pw.Write("         application_1234_0007\t            ");
			pw.Write("appname3\t           MAPREDUCE\t     user3\t    ");
			pw.Write("queue3\t           RUNNING\t         ");
			pw.Write("SUCCEEDED\t         73.79%");
			pw.WriteLine("\t                                N/A");
			pw.Write("         application_1234_0009\t            ");
			pw.Write("appname5\t                HIVE\t     user5\t    ");
			pw.Write("queue5\t          ACCEPTED\t            ");
			pw.Write("KILLED\t         93.79%");
			pw.WriteLine("\t                                N/A");
			pw.Write("         application_1234_0010\t            ");
			pw.Write("appname6\t                 PIG\t     user6\t    ");
			pw.Write("queue6\t         SUBMITTED\t            ");
			pw.Write("KILLED\t         99.79%");
			pw.WriteLine("\t                                N/A");
			pw.Close();
			string appsReportStr = baos.ToString("UTF-8");
			NUnit.Framework.Assert.AreEqual(appsReportStr, sysOutStream.ToString());
			Org.Mockito.Mockito.Verify(sysOut, Org.Mockito.Mockito.Times(1)).Write(Matchers.Any
				<byte[]>(), Matchers.AnyInt(), Matchers.AnyInt());
			//Test command yarn application -list --appTypes apptype1,apptype2
			//the output should be the same as
			// yarn application -list --appTypes apptyp1, apptype2 --appStates
			// RUNNING,ACCEPTED,SUBMITTED
			sysOutStream.Reset();
			ICollection<string> appType2 = new HashSet<string>();
			appType2.AddItem("YARN");
			appType2.AddItem("NON-YARN");
			EnumSet<YarnApplicationState> appState2 = EnumSet.NoneOf<YarnApplicationState>();
			appState2.AddItem(YarnApplicationState.Running);
			appState2.AddItem(YarnApplicationState.Accepted);
			appState2.AddItem(YarnApplicationState.Submitted);
			Org.Mockito.Mockito.When(client.GetApplications(appType2, appState2)).ThenReturn(
				GetApplicationReports(applicationReports, appType2, appState2, false));
			result = cli.Run(new string[] { "application", "-list", "-appTypes", "YARN, ,,  NON-YARN"
				, "   ,, ,," });
			NUnit.Framework.Assert.AreEqual(0, result);
			Org.Mockito.Mockito.Verify(client).GetApplications(appType2, appState2);
			baos = new ByteArrayOutputStream();
			pw = new PrintWriter(baos);
			pw.WriteLine("Total number of applications (application-types: " + appType2 + " and states: "
				 + appState2 + ")" + ":" + 1);
			pw.Write("                Application-Id\t    Application-Name");
			pw.Write("\t    Application-Type");
			pw.Write("\t      User\t     Queue\t             State\t       ");
			pw.Write("Final-State\t       Progress");
			pw.WriteLine("\t                       Tracking-URL");
			pw.Write("         application_1234_0005\t             ");
			pw.Write("appname\t                YARN\t      user\t     ");
			pw.Write("queue\t           RUNNING\t         ");
			pw.Write("SUCCEEDED\t         53.79%");
			pw.WriteLine("\t                                N/A");
			pw.Close();
			appsReportStr = baos.ToString("UTF-8");
			NUnit.Framework.Assert.AreEqual(appsReportStr, sysOutStream.ToString());
			Org.Mockito.Mockito.Verify(sysOut, Org.Mockito.Mockito.Times(2)).Write(Matchers.Any
				<byte[]>(), Matchers.AnyInt(), Matchers.AnyInt());
			//Test command yarn application -list --appStates appState1,appState2
			sysOutStream.Reset();
			ICollection<string> appType3 = new HashSet<string>();
			EnumSet<YarnApplicationState> appState3 = EnumSet.NoneOf<YarnApplicationState>();
			appState3.AddItem(YarnApplicationState.Finished);
			appState3.AddItem(YarnApplicationState.Failed);
			Org.Mockito.Mockito.When(client.GetApplications(appType3, appState3)).ThenReturn(
				GetApplicationReports(applicationReports, appType3, appState3, false));
			result = cli.Run(new string[] { "application", "-list", "--appStates", "FINISHED ,, , FAILED"
				, ",,FINISHED" });
			NUnit.Framework.Assert.AreEqual(0, result);
			Org.Mockito.Mockito.Verify(client).GetApplications(appType3, appState3);
			baos = new ByteArrayOutputStream();
			pw = new PrintWriter(baos);
			pw.WriteLine("Total number of applications (application-types: " + appType3 + " and states: "
				 + appState3 + ")" + ":" + 2);
			pw.Write("                Application-Id\t    Application-Name");
			pw.Write("\t    Application-Type");
			pw.Write("\t      User\t     Queue\t             State\t       ");
			pw.Write("Final-State\t       Progress");
			pw.WriteLine("\t                       Tracking-URL");
			pw.Write("         application_1234_0006\t            ");
			pw.Write("appname2\t            NON-YARN\t     user2\t    ");
			pw.Write("queue2\t          FINISHED\t         ");
			pw.Write("SUCCEEDED\t         63.79%");
			pw.WriteLine("\t                                N/A");
			pw.Write("         application_1234_0008\t            ");
			pw.Write("appname4\t       NON-MAPREDUCE\t     user4\t    ");
			pw.Write("queue4\t            FAILED\t         ");
			pw.Write("SUCCEEDED\t         83.79%");
			pw.WriteLine("\t                                N/A");
			pw.Close();
			appsReportStr = baos.ToString("UTF-8");
			NUnit.Framework.Assert.AreEqual(appsReportStr, sysOutStream.ToString());
			Org.Mockito.Mockito.Verify(sysOut, Org.Mockito.Mockito.Times(3)).Write(Matchers.Any
				<byte[]>(), Matchers.AnyInt(), Matchers.AnyInt());
			// Test command yarn application -list --appTypes apptype1,apptype2
			// --appStates appstate1,appstate2
			sysOutStream.Reset();
			ICollection<string> appType4 = new HashSet<string>();
			appType4.AddItem("YARN");
			appType4.AddItem("NON-YARN");
			EnumSet<YarnApplicationState> appState4 = EnumSet.NoneOf<YarnApplicationState>();
			appState4.AddItem(YarnApplicationState.Finished);
			appState4.AddItem(YarnApplicationState.Failed);
			Org.Mockito.Mockito.When(client.GetApplications(appType4, appState4)).ThenReturn(
				GetApplicationReports(applicationReports, appType4, appState4, false));
			result = cli.Run(new string[] { "application", "-list", "--appTypes", "YARN,NON-YARN"
				, "--appStates", "FINISHED ,, , FAILED" });
			NUnit.Framework.Assert.AreEqual(0, result);
			Org.Mockito.Mockito.Verify(client).GetApplications(appType2, appState2);
			baos = new ByteArrayOutputStream();
			pw = new PrintWriter(baos);
			pw.WriteLine("Total number of applications (application-types: " + appType4 + " and states: "
				 + appState4 + ")" + ":" + 1);
			pw.Write("                Application-Id\t    Application-Name");
			pw.Write("\t    Application-Type");
			pw.Write("\t      User\t     Queue\t             State\t       ");
			pw.Write("Final-State\t       Progress");
			pw.WriteLine("\t                       Tracking-URL");
			pw.Write("         application_1234_0006\t            ");
			pw.Write("appname2\t            NON-YARN\t     user2\t    ");
			pw.Write("queue2\t          FINISHED\t         ");
			pw.Write("SUCCEEDED\t         63.79%");
			pw.WriteLine("\t                                N/A");
			pw.Close();
			appsReportStr = baos.ToString("UTF-8");
			NUnit.Framework.Assert.AreEqual(appsReportStr, sysOutStream.ToString());
			Org.Mockito.Mockito.Verify(sysOut, Org.Mockito.Mockito.Times(4)).Write(Matchers.Any
				<byte[]>(), Matchers.AnyInt(), Matchers.AnyInt());
			//Test command yarn application -list --appStates with invalid appStates
			sysOutStream.Reset();
			result = cli.Run(new string[] { "application", "-list", "--appStates", "FINISHED ,, , INVALID"
				 });
			NUnit.Framework.Assert.AreEqual(-1, result);
			baos = new ByteArrayOutputStream();
			pw = new PrintWriter(baos);
			pw.WriteLine("The application state  INVALID is invalid.");
			pw.Write("The valid application state can be one of the following: ");
			StringBuilder sb = new StringBuilder();
			sb.Append("ALL,");
			foreach (YarnApplicationState state in YarnApplicationState.Values())
			{
				sb.Append(state + ",");
			}
			string output = sb.ToString();
			pw.WriteLine(Sharpen.Runtime.Substring(output, 0, output.Length - 1));
			pw.Close();
			appsReportStr = baos.ToString("UTF-8");
			NUnit.Framework.Assert.AreEqual(appsReportStr, sysOutStream.ToString());
			Org.Mockito.Mockito.Verify(sysOut, Org.Mockito.Mockito.Times(4)).Write(Matchers.Any
				<byte[]>(), Matchers.AnyInt(), Matchers.AnyInt());
			//Test command yarn application -list --appStates all
			sysOutStream.Reset();
			ICollection<string> appType5 = new HashSet<string>();
			EnumSet<YarnApplicationState> appState5 = EnumSet.NoneOf<YarnApplicationState>();
			appState5.AddItem(YarnApplicationState.Finished);
			Org.Mockito.Mockito.When(client.GetApplications(appType5, appState5)).ThenReturn(
				GetApplicationReports(applicationReports, appType5, appState5, true));
			result = cli.Run(new string[] { "application", "-list", "--appStates", "FINISHED ,, , ALL"
				 });
			NUnit.Framework.Assert.AreEqual(0, result);
			Org.Mockito.Mockito.Verify(client).GetApplications(appType5, appState5);
			baos = new ByteArrayOutputStream();
			pw = new PrintWriter(baos);
			pw.WriteLine("Total number of applications (application-types: " + appType5 + " and states: "
				 + appState5 + ")" + ":" + 6);
			pw.Write("                Application-Id\t    Application-Name");
			pw.Write("\t    Application-Type");
			pw.Write("\t      User\t     Queue\t             State\t       ");
			pw.Write("Final-State\t       Progress");
			pw.WriteLine("\t                       Tracking-URL");
			pw.Write("         application_1234_0005\t             ");
			pw.Write("appname\t                YARN\t      user\t     ");
			pw.Write("queue\t           RUNNING\t         ");
			pw.Write("SUCCEEDED\t         53.79%");
			pw.WriteLine("\t                                N/A");
			pw.Write("         application_1234_0006\t            ");
			pw.Write("appname2\t            NON-YARN\t     user2\t    ");
			pw.Write("queue2\t          FINISHED\t         ");
			pw.Write("SUCCEEDED\t         63.79%");
			pw.WriteLine("\t                                N/A");
			pw.Write("         application_1234_0007\t            ");
			pw.Write("appname3\t           MAPREDUCE\t     user3\t    ");
			pw.Write("queue3\t           RUNNING\t         ");
			pw.Write("SUCCEEDED\t         73.79%");
			pw.WriteLine("\t                                N/A");
			pw.Write("         application_1234_0008\t            ");
			pw.Write("appname4\t       NON-MAPREDUCE\t     user4\t    ");
			pw.Write("queue4\t            FAILED\t         ");
			pw.Write("SUCCEEDED\t         83.79%");
			pw.WriteLine("\t                                N/A");
			pw.Write("         application_1234_0009\t            ");
			pw.Write("appname5\t                HIVE\t     user5\t    ");
			pw.Write("queue5\t          ACCEPTED\t            ");
			pw.Write("KILLED\t         93.79%");
			pw.WriteLine("\t                                N/A");
			pw.Write("         application_1234_0010\t            ");
			pw.Write("appname6\t                 PIG\t     user6\t    ");
			pw.Write("queue6\t         SUBMITTED\t            ");
			pw.Write("KILLED\t         99.79%");
			pw.WriteLine("\t                                N/A");
			pw.Close();
			appsReportStr = baos.ToString("UTF-8");
			NUnit.Framework.Assert.AreEqual(appsReportStr, sysOutStream.ToString());
			Org.Mockito.Mockito.Verify(sysOut, Org.Mockito.Mockito.Times(5)).Write(Matchers.Any
				<byte[]>(), Matchers.AnyInt(), Matchers.AnyInt());
			// Test command yarn application user case insensitive
			sysOutStream.Reset();
			ICollection<string> appType6 = new HashSet<string>();
			appType6.AddItem("YARN");
			appType6.AddItem("NON-YARN");
			EnumSet<YarnApplicationState> appState6 = EnumSet.NoneOf<YarnApplicationState>();
			appState6.AddItem(YarnApplicationState.Finished);
			Org.Mockito.Mockito.When(client.GetApplications(appType6, appState6)).ThenReturn(
				GetApplicationReports(applicationReports, appType6, appState6, false));
			result = cli.Run(new string[] { "application", "-list", "-appTypes", "YARN, ,,  NON-YARN"
				, "--appStates", "finished" });
			NUnit.Framework.Assert.AreEqual(0, result);
			Org.Mockito.Mockito.Verify(client).GetApplications(appType6, appState6);
			baos = new ByteArrayOutputStream();
			pw = new PrintWriter(baos);
			pw.WriteLine("Total number of applications (application-types: " + appType6 + " and states: "
				 + appState6 + ")" + ":" + 1);
			pw.Write("                Application-Id\t    Application-Name");
			pw.Write("\t    Application-Type");
			pw.Write("\t      User\t     Queue\t             State\t       ");
			pw.Write("Final-State\t       Progress");
			pw.WriteLine("\t                       Tracking-URL");
			pw.Write("         application_1234_0006\t            ");
			pw.Write("appname2\t            NON-YARN\t     user2\t    ");
			pw.Write("queue2\t          FINISHED\t         ");
			pw.Write("SUCCEEDED\t         63.79%");
			pw.WriteLine("\t                                N/A");
			pw.Close();
			appsReportStr = baos.ToString("UTF-8");
			NUnit.Framework.Assert.AreEqual(appsReportStr, sysOutStream.ToString());
			Org.Mockito.Mockito.Verify(sysOut, Org.Mockito.Mockito.Times(6)).Write(Matchers.Any
				<byte[]>(), Matchers.AnyInt(), Matchers.AnyInt());
		}

		private IList<ApplicationReport> GetApplicationReports(IList<ApplicationReport> applicationReports
			, ICollection<string> appTypes, EnumSet<YarnApplicationState> appStates, bool allStates
			)
		{
			IList<ApplicationReport> appReports = new AList<ApplicationReport>();
			if (allStates)
			{
				foreach (YarnApplicationState state in YarnApplicationState.Values())
				{
					appStates.AddItem(state);
				}
			}
			foreach (ApplicationReport appReport in applicationReports)
			{
				if (appTypes != null && !appTypes.IsEmpty())
				{
					if (!appTypes.Contains(appReport.GetApplicationType()))
					{
						continue;
					}
				}
				if (appStates != null && !appStates.IsEmpty())
				{
					if (!appStates.Contains(appReport.GetYarnApplicationState()))
					{
						continue;
					}
				}
				appReports.AddItem(appReport);
			}
			return appReports;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestAppsHelpCommand()
		{
			ApplicationCLI cli = CreateAndGetAppCLI();
			ApplicationCLI spyCli = Org.Mockito.Mockito.Spy(cli);
			int result = spyCli.Run(new string[] { "application", "-help" });
			NUnit.Framework.Assert.IsTrue(result == 0);
			Org.Mockito.Mockito.Verify(spyCli).PrintUsage(Matchers.Any<string>(), Matchers.Any
				<Options>());
			NUnit.Framework.Assert.AreEqual(CreateApplicationCLIHelpMessage(), sysOutStream.ToString
				());
			sysOutStream.Reset();
			ApplicationId applicationId = ApplicationId.NewInstance(1234, 5);
			result = cli.Run(new string[] { "application", "-kill", applicationId.ToString(), 
				"args" });
			Org.Mockito.Mockito.Verify(spyCli).PrintUsage(Matchers.Any<string>(), Matchers.Any
				<Options>());
			NUnit.Framework.Assert.AreEqual(CreateApplicationCLIHelpMessage(), sysOutStream.ToString
				());
			sysOutStream.Reset();
			NodeId nodeId = NodeId.NewInstance("host0", 0);
			result = cli.Run(new string[] { "application", "-status", nodeId.ToString(), "args"
				 });
			Org.Mockito.Mockito.Verify(spyCli).PrintUsage(Matchers.Any<string>(), Matchers.Any
				<Options>());
			NUnit.Framework.Assert.AreEqual(CreateApplicationCLIHelpMessage(), sysOutStream.ToString
				());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestAppAttemptsHelpCommand()
		{
			ApplicationCLI cli = CreateAndGetAppCLI();
			ApplicationCLI spyCli = Org.Mockito.Mockito.Spy(cli);
			int result = spyCli.Run(new string[] { "applicationattempt", "-help" });
			NUnit.Framework.Assert.IsTrue(result == 0);
			Org.Mockito.Mockito.Verify(spyCli).PrintUsage(Matchers.Any<string>(), Matchers.Any
				<Options>());
			NUnit.Framework.Assert.AreEqual(CreateApplicationAttemptCLIHelpMessage(), sysOutStream
				.ToString());
			sysOutStream.Reset();
			ApplicationId applicationId = ApplicationId.NewInstance(1234, 5);
			result = cli.Run(new string[] { "applicationattempt", "-list", applicationId.ToString
				(), "args" });
			Org.Mockito.Mockito.Verify(spyCli).PrintUsage(Matchers.Any<string>(), Matchers.Any
				<Options>());
			NUnit.Framework.Assert.AreEqual(CreateApplicationAttemptCLIHelpMessage(), sysOutStream
				.ToString());
			sysOutStream.Reset();
			ApplicationAttemptId appAttemptId = ApplicationAttemptId.NewInstance(applicationId
				, 6);
			result = cli.Run(new string[] { "applicationattempt", "-status", appAttemptId.ToString
				(), "args" });
			Org.Mockito.Mockito.Verify(spyCli).PrintUsage(Matchers.Any<string>(), Matchers.Any
				<Options>());
			NUnit.Framework.Assert.AreEqual(CreateApplicationAttemptCLIHelpMessage(), sysOutStream
				.ToString());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestContainersHelpCommand()
		{
			ApplicationCLI cli = CreateAndGetAppCLI();
			ApplicationCLI spyCli = Org.Mockito.Mockito.Spy(cli);
			int result = spyCli.Run(new string[] { "container", "-help" });
			NUnit.Framework.Assert.IsTrue(result == 0);
			Org.Mockito.Mockito.Verify(spyCli).PrintUsage(Matchers.Any<string>(), Matchers.Any
				<Options>());
			NUnit.Framework.Assert.AreEqual(CreateContainerCLIHelpMessage(), sysOutStream.ToString
				());
			sysOutStream.Reset();
			ApplicationId applicationId = ApplicationId.NewInstance(1234, 5);
			ApplicationAttemptId appAttemptId = ApplicationAttemptId.NewInstance(applicationId
				, 6);
			result = cli.Run(new string[] { "container", "-list", appAttemptId.ToString(), "args"
				 });
			Org.Mockito.Mockito.Verify(spyCli).PrintUsage(Matchers.Any<string>(), Matchers.Any
				<Options>());
			NUnit.Framework.Assert.AreEqual(CreateContainerCLIHelpMessage(), sysOutStream.ToString
				());
			sysOutStream.Reset();
			ContainerId containerId = ContainerId.NewContainerId(appAttemptId, 7);
			result = cli.Run(new string[] { "container", "-status", containerId.ToString(), "args"
				 });
			Org.Mockito.Mockito.Verify(spyCli).PrintUsage(Matchers.Any<string>(), Matchers.Any
				<Options>());
			NUnit.Framework.Assert.AreEqual(CreateContainerCLIHelpMessage(), sysOutStream.ToString
				());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestNodesHelpCommand()
		{
			NodeCLI nodeCLI = new NodeCLI();
			nodeCLI.SetClient(client);
			nodeCLI.SetSysOutPrintStream(sysOut);
			nodeCLI.SetSysErrPrintStream(sysErr);
			nodeCLI.Run(new string[] {  });
			NUnit.Framework.Assert.AreEqual(CreateNodeCLIHelpMessage(), sysOutStream.ToString
				());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestKillApplication()
		{
			ApplicationCLI cli = CreateAndGetAppCLI();
			ApplicationId applicationId = ApplicationId.NewInstance(1234, 5);
			ApplicationReport newApplicationReport2 = ApplicationReport.NewInstance(applicationId
				, ApplicationAttemptId.NewInstance(applicationId, 1), "user", "queue", "appname"
				, "host", 124, null, YarnApplicationState.Finished, "diagnostics", "url", 0, 0, 
				FinalApplicationStatus.Succeeded, null, "N/A", 0.53789f, "YARN", null);
			Org.Mockito.Mockito.When(client.GetApplicationReport(Matchers.Any<ApplicationId>(
				))).ThenReturn(newApplicationReport2);
			int result = cli.Run(new string[] { "application", "-kill", applicationId.ToString
				() });
			NUnit.Framework.Assert.AreEqual(0, result);
			Org.Mockito.Mockito.Verify(client, Org.Mockito.Mockito.Times(0)).KillApplication(
				Matchers.Any<ApplicationId>());
			Org.Mockito.Mockito.Verify(sysOut).WriteLine("Application " + applicationId + " has already finished "
				);
			ApplicationReport newApplicationReport = ApplicationReport.NewInstance(applicationId
				, ApplicationAttemptId.NewInstance(applicationId, 1), "user", "queue", "appname"
				, "host", 124, null, YarnApplicationState.Running, "diagnostics", "url", 0, 0, FinalApplicationStatus
				.Succeeded, null, "N/A", 0.53789f, "YARN", null);
			Org.Mockito.Mockito.When(client.GetApplicationReport(Matchers.Any<ApplicationId>(
				))).ThenReturn(newApplicationReport);
			result = cli.Run(new string[] { "application", "-kill", applicationId.ToString() }
				);
			NUnit.Framework.Assert.AreEqual(0, result);
			Org.Mockito.Mockito.Verify(client).KillApplication(Matchers.Any<ApplicationId>());
			Org.Mockito.Mockito.Verify(sysOut).WriteLine("Killing application application_1234_0005"
				);
			Org.Mockito.Mockito.DoThrow(new ApplicationNotFoundException("Application with id '"
				 + applicationId + "' doesn't exist in RM.")).When(client).GetApplicationReport(
				applicationId);
			cli = CreateAndGetAppCLI();
			try
			{
				int exitCode = cli.Run(new string[] { "application", "-kill", applicationId.ToString
					() });
				Org.Mockito.Mockito.Verify(sysOut).WriteLine("Application with id '" + applicationId
					 + "' doesn't exist in RM.");
				NUnit.Framework.Assert.AreNotSame("should return non-zero exit code.", 0, exitCode
					);
			}
			catch (ApplicationNotFoundException appEx)
			{
				NUnit.Framework.Assert.Fail("application -kill should not throw" + "ApplicationNotFoundException. "
					 + appEx);
			}
			catch (Exception e)
			{
				NUnit.Framework.Assert.Fail("Unexpected exception: " + e);
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMoveApplicationAcrossQueues()
		{
			ApplicationCLI cli = CreateAndGetAppCLI();
			ApplicationId applicationId = ApplicationId.NewInstance(1234, 5);
			ApplicationReport newApplicationReport2 = ApplicationReport.NewInstance(applicationId
				, ApplicationAttemptId.NewInstance(applicationId, 1), "user", "queue", "appname"
				, "host", 124, null, YarnApplicationState.Finished, "diagnostics", "url", 0, 0, 
				FinalApplicationStatus.Succeeded, null, "N/A", 0.53789f, "YARN", null);
			Org.Mockito.Mockito.When(client.GetApplicationReport(Matchers.Any<ApplicationId>(
				))).ThenReturn(newApplicationReport2);
			int result = cli.Run(new string[] { "application", "-movetoqueue", applicationId.
				ToString(), "-queue", "targetqueue" });
			NUnit.Framework.Assert.AreEqual(0, result);
			Org.Mockito.Mockito.Verify(client, Org.Mockito.Mockito.Times(0)).MoveApplicationAcrossQueues
				(Matchers.Any<ApplicationId>(), Matchers.Any<string>());
			Org.Mockito.Mockito.Verify(sysOut).WriteLine("Application " + applicationId + " has already finished "
				);
			ApplicationReport newApplicationReport = ApplicationReport.NewInstance(applicationId
				, ApplicationAttemptId.NewInstance(applicationId, 1), "user", "queue", "appname"
				, "host", 124, null, YarnApplicationState.Running, "diagnostics", "url", 0, 0, FinalApplicationStatus
				.Succeeded, null, "N/A", 0.53789f, "YARN", null);
			Org.Mockito.Mockito.When(client.GetApplicationReport(Matchers.Any<ApplicationId>(
				))).ThenReturn(newApplicationReport);
			result = cli.Run(new string[] { "application", "-movetoqueue", applicationId.ToString
				(), "-queue", "targetqueue" });
			NUnit.Framework.Assert.AreEqual(0, result);
			Org.Mockito.Mockito.Verify(client).MoveApplicationAcrossQueues(Matchers.Any<ApplicationId
				>(), Matchers.Any<string>());
			Org.Mockito.Mockito.Verify(sysOut).WriteLine("Moving application application_1234_0005 to queue targetqueue"
				);
			Org.Mockito.Mockito.Verify(sysOut).WriteLine("Successfully completed move.");
			Org.Mockito.Mockito.DoThrow(new ApplicationNotFoundException("Application with id '"
				 + applicationId + "' doesn't exist in RM.")).When(client).MoveApplicationAcrossQueues
				(applicationId, "targetqueue");
			cli = CreateAndGetAppCLI();
			try
			{
				result = cli.Run(new string[] { "application", "-movetoqueue", applicationId.ToString
					(), "-queue", "targetqueue" });
				NUnit.Framework.Assert.Fail();
			}
			catch (Exception ex)
			{
				NUnit.Framework.Assert.IsTrue(ex is ApplicationNotFoundException);
				NUnit.Framework.Assert.AreEqual("Application with id '" + applicationId + "' doesn't exist in RM."
					, ex.Message);
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestListClusterNodes()
		{
			IList<NodeReport> nodeReports = new AList<NodeReport>();
			Sharpen.Collections.AddAll(nodeReports, GetNodeReports(1, NodeState.New));
			Sharpen.Collections.AddAll(nodeReports, GetNodeReports(2, NodeState.Running));
			Sharpen.Collections.AddAll(nodeReports, GetNodeReports(1, NodeState.Unhealthy));
			Sharpen.Collections.AddAll(nodeReports, GetNodeReports(1, NodeState.Decommissioned
				));
			Sharpen.Collections.AddAll(nodeReports, GetNodeReports(1, NodeState.Rebooted));
			Sharpen.Collections.AddAll(nodeReports, GetNodeReports(1, NodeState.Lost));
			NodeCLI cli = new NodeCLI();
			cli.SetClient(client);
			cli.SetSysOutPrintStream(sysOut);
			ICollection<NodeState> nodeStates = new HashSet<NodeState>();
			nodeStates.AddItem(NodeState.New);
			NodeState[] states = Sharpen.Collections.ToArray(nodeStates, new NodeState[0]);
			Org.Mockito.Mockito.When(client.GetNodeReports(states)).ThenReturn(GetNodeReports
				(nodeReports, nodeStates));
			int result = cli.Run(new string[] { "-list", "--states", "NEW" });
			NUnit.Framework.Assert.AreEqual(0, result);
			Org.Mockito.Mockito.Verify(client).GetNodeReports(states);
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			PrintWriter pw = new PrintWriter(baos);
			pw.WriteLine("Total Nodes:1");
			pw.Write("         Node-Id\t     Node-State\tNode-Http-Address\t");
			pw.WriteLine("Number-of-Running-Containers");
			pw.Write("         host0:0\t            NEW\t       host1:8888\t");
			pw.WriteLine("                           0");
			pw.Close();
			string nodesReportStr = baos.ToString("UTF-8");
			NUnit.Framework.Assert.AreEqual(nodesReportStr, sysOutStream.ToString());
			Org.Mockito.Mockito.Verify(sysOut, Org.Mockito.Mockito.Times(1)).Write(Matchers.Any
				<byte[]>(), Matchers.AnyInt(), Matchers.AnyInt());
			sysOutStream.Reset();
			nodeStates.Clear();
			nodeStates.AddItem(NodeState.Running);
			states = Sharpen.Collections.ToArray(nodeStates, new NodeState[0]);
			Org.Mockito.Mockito.When(client.GetNodeReports(states)).ThenReturn(GetNodeReports
				(nodeReports, nodeStates));
			result = cli.Run(new string[] { "-list", "--states", "RUNNING" });
			NUnit.Framework.Assert.AreEqual(0, result);
			Org.Mockito.Mockito.Verify(client).GetNodeReports(states);
			baos = new ByteArrayOutputStream();
			pw = new PrintWriter(baos);
			pw.WriteLine("Total Nodes:2");
			pw.Write("         Node-Id\t     Node-State\tNode-Http-Address\t");
			pw.WriteLine("Number-of-Running-Containers");
			pw.Write("         host0:0\t        RUNNING\t       host1:8888\t");
			pw.WriteLine("                           0");
			pw.Write("         host1:0\t        RUNNING\t       host1:8888\t");
			pw.WriteLine("                           0");
			pw.Close();
			nodesReportStr = baos.ToString("UTF-8");
			NUnit.Framework.Assert.AreEqual(nodesReportStr, sysOutStream.ToString());
			Org.Mockito.Mockito.Verify(sysOut, Org.Mockito.Mockito.Times(2)).Write(Matchers.Any
				<byte[]>(), Matchers.AnyInt(), Matchers.AnyInt());
			sysOutStream.Reset();
			result = cli.Run(new string[] { "-list" });
			NUnit.Framework.Assert.AreEqual(0, result);
			NUnit.Framework.Assert.AreEqual(nodesReportStr, sysOutStream.ToString());
			Org.Mockito.Mockito.Verify(sysOut, Org.Mockito.Mockito.Times(3)).Write(Matchers.Any
				<byte[]>(), Matchers.AnyInt(), Matchers.AnyInt());
			sysOutStream.Reset();
			nodeStates.Clear();
			nodeStates.AddItem(NodeState.Unhealthy);
			states = Sharpen.Collections.ToArray(nodeStates, new NodeState[0]);
			Org.Mockito.Mockito.When(client.GetNodeReports(states)).ThenReturn(GetNodeReports
				(nodeReports, nodeStates));
			result = cli.Run(new string[] { "-list", "--states", "UNHEALTHY" });
			NUnit.Framework.Assert.AreEqual(0, result);
			Org.Mockito.Mockito.Verify(client).GetNodeReports(states);
			baos = new ByteArrayOutputStream();
			pw = new PrintWriter(baos);
			pw.WriteLine("Total Nodes:1");
			pw.Write("         Node-Id\t     Node-State\tNode-Http-Address\t");
			pw.WriteLine("Number-of-Running-Containers");
			pw.Write("         host0:0\t      UNHEALTHY\t       host1:8888\t");
			pw.WriteLine("                           0");
			pw.Close();
			nodesReportStr = baos.ToString("UTF-8");
			NUnit.Framework.Assert.AreEqual(nodesReportStr, sysOutStream.ToString());
			Org.Mockito.Mockito.Verify(sysOut, Org.Mockito.Mockito.Times(4)).Write(Matchers.Any
				<byte[]>(), Matchers.AnyInt(), Matchers.AnyInt());
			sysOutStream.Reset();
			nodeStates.Clear();
			nodeStates.AddItem(NodeState.Decommissioned);
			states = Sharpen.Collections.ToArray(nodeStates, new NodeState[0]);
			Org.Mockito.Mockito.When(client.GetNodeReports(states)).ThenReturn(GetNodeReports
				(nodeReports, nodeStates));
			result = cli.Run(new string[] { "-list", "--states", "DECOMMISSIONED" });
			NUnit.Framework.Assert.AreEqual(0, result);
			Org.Mockito.Mockito.Verify(client).GetNodeReports(states);
			baos = new ByteArrayOutputStream();
			pw = new PrintWriter(baos);
			pw.WriteLine("Total Nodes:1");
			pw.Write("         Node-Id\t     Node-State\tNode-Http-Address\t");
			pw.WriteLine("Number-of-Running-Containers");
			pw.Write("         host0:0\t DECOMMISSIONED\t       host1:8888\t");
			pw.WriteLine("                           0");
			pw.Close();
			nodesReportStr = baos.ToString("UTF-8");
			NUnit.Framework.Assert.AreEqual(nodesReportStr, sysOutStream.ToString());
			Org.Mockito.Mockito.Verify(sysOut, Org.Mockito.Mockito.Times(5)).Write(Matchers.Any
				<byte[]>(), Matchers.AnyInt(), Matchers.AnyInt());
			sysOutStream.Reset();
			nodeStates.Clear();
			nodeStates.AddItem(NodeState.Rebooted);
			states = Sharpen.Collections.ToArray(nodeStates, new NodeState[0]);
			Org.Mockito.Mockito.When(client.GetNodeReports(states)).ThenReturn(GetNodeReports
				(nodeReports, nodeStates));
			result = cli.Run(new string[] { "-list", "--states", "REBOOTED" });
			NUnit.Framework.Assert.AreEqual(0, result);
			Org.Mockito.Mockito.Verify(client).GetNodeReports(states);
			baos = new ByteArrayOutputStream();
			pw = new PrintWriter(baos);
			pw.WriteLine("Total Nodes:1");
			pw.Write("         Node-Id\t     Node-State\tNode-Http-Address\t");
			pw.WriteLine("Number-of-Running-Containers");
			pw.Write("         host0:0\t       REBOOTED\t       host1:8888\t");
			pw.WriteLine("                           0");
			pw.Close();
			nodesReportStr = baos.ToString("UTF-8");
			NUnit.Framework.Assert.AreEqual(nodesReportStr, sysOutStream.ToString());
			Org.Mockito.Mockito.Verify(sysOut, Org.Mockito.Mockito.Times(6)).Write(Matchers.Any
				<byte[]>(), Matchers.AnyInt(), Matchers.AnyInt());
			sysOutStream.Reset();
			nodeStates.Clear();
			nodeStates.AddItem(NodeState.Lost);
			states = Sharpen.Collections.ToArray(nodeStates, new NodeState[0]);
			Org.Mockito.Mockito.When(client.GetNodeReports(states)).ThenReturn(GetNodeReports
				(nodeReports, nodeStates));
			result = cli.Run(new string[] { "-list", "--states", "LOST" });
			NUnit.Framework.Assert.AreEqual(0, result);
			Org.Mockito.Mockito.Verify(client).GetNodeReports(states);
			baos = new ByteArrayOutputStream();
			pw = new PrintWriter(baos);
			pw.WriteLine("Total Nodes:1");
			pw.Write("         Node-Id\t     Node-State\tNode-Http-Address\t");
			pw.WriteLine("Number-of-Running-Containers");
			pw.Write("         host0:0\t           LOST\t       host1:8888\t");
			pw.WriteLine("                           0");
			pw.Close();
			nodesReportStr = baos.ToString("UTF-8");
			NUnit.Framework.Assert.AreEqual(nodesReportStr, sysOutStream.ToString());
			Org.Mockito.Mockito.Verify(sysOut, Org.Mockito.Mockito.Times(7)).Write(Matchers.Any
				<byte[]>(), Matchers.AnyInt(), Matchers.AnyInt());
			sysOutStream.Reset();
			nodeStates.Clear();
			nodeStates.AddItem(NodeState.New);
			nodeStates.AddItem(NodeState.Running);
			nodeStates.AddItem(NodeState.Lost);
			nodeStates.AddItem(NodeState.Rebooted);
			states = Sharpen.Collections.ToArray(nodeStates, new NodeState[0]);
			Org.Mockito.Mockito.When(client.GetNodeReports(states)).ThenReturn(GetNodeReports
				(nodeReports, nodeStates));
			result = cli.Run(new string[] { "-list", "--states", "NEW,RUNNING,LOST,REBOOTED" }
				);
			NUnit.Framework.Assert.AreEqual(0, result);
			Org.Mockito.Mockito.Verify(client).GetNodeReports(states);
			baos = new ByteArrayOutputStream();
			pw = new PrintWriter(baos);
			pw.WriteLine("Total Nodes:5");
			pw.Write("         Node-Id\t     Node-State\tNode-Http-Address\t");
			pw.WriteLine("Number-of-Running-Containers");
			pw.Write("         host0:0\t            NEW\t       host1:8888\t");
			pw.WriteLine("                           0");
			pw.Write("         host0:0\t        RUNNING\t       host1:8888\t");
			pw.WriteLine("                           0");
			pw.Write("         host1:0\t        RUNNING\t       host1:8888\t");
			pw.WriteLine("                           0");
			pw.Write("         host0:0\t       REBOOTED\t       host1:8888\t");
			pw.WriteLine("                           0");
			pw.Write("         host0:0\t           LOST\t       host1:8888\t");
			pw.WriteLine("                           0");
			pw.Close();
			nodesReportStr = baos.ToString("UTF-8");
			NUnit.Framework.Assert.AreEqual(nodesReportStr, sysOutStream.ToString());
			Org.Mockito.Mockito.Verify(sysOut, Org.Mockito.Mockito.Times(8)).Write(Matchers.Any
				<byte[]>(), Matchers.AnyInt(), Matchers.AnyInt());
			sysOutStream.Reset();
			nodeStates.Clear();
			foreach (NodeState s in NodeState.Values())
			{
				nodeStates.AddItem(s);
			}
			states = Sharpen.Collections.ToArray(nodeStates, new NodeState[0]);
			Org.Mockito.Mockito.When(client.GetNodeReports(states)).ThenReturn(GetNodeReports
				(nodeReports, nodeStates));
			result = cli.Run(new string[] { "-list", "--all" });
			NUnit.Framework.Assert.AreEqual(0, result);
			Org.Mockito.Mockito.Verify(client).GetNodeReports(states);
			baos = new ByteArrayOutputStream();
			pw = new PrintWriter(baos);
			pw.WriteLine("Total Nodes:7");
			pw.Write("         Node-Id\t     Node-State\tNode-Http-Address\t");
			pw.WriteLine("Number-of-Running-Containers");
			pw.Write("         host0:0\t            NEW\t       host1:8888\t");
			pw.WriteLine("                           0");
			pw.Write("         host0:0\t        RUNNING\t       host1:8888\t");
			pw.WriteLine("                           0");
			pw.Write("         host1:0\t        RUNNING\t       host1:8888\t");
			pw.WriteLine("                           0");
			pw.Write("         host0:0\t      UNHEALTHY\t       host1:8888\t");
			pw.WriteLine("                           0");
			pw.Write("         host0:0\t DECOMMISSIONED\t       host1:8888\t");
			pw.WriteLine("                           0");
			pw.Write("         host0:0\t       REBOOTED\t       host1:8888\t");
			pw.WriteLine("                           0");
			pw.Write("         host0:0\t           LOST\t       host1:8888\t");
			pw.WriteLine("                           0");
			pw.Close();
			nodesReportStr = baos.ToString("UTF-8");
			NUnit.Framework.Assert.AreEqual(nodesReportStr, sysOutStream.ToString());
			Org.Mockito.Mockito.Verify(sysOut, Org.Mockito.Mockito.Times(9)).Write(Matchers.Any
				<byte[]>(), Matchers.AnyInt(), Matchers.AnyInt());
		}

		private IList<NodeReport> GetNodeReports(IList<NodeReport> nodeReports, ICollection
			<NodeState> nodeStates)
		{
			IList<NodeReport> reports = new AList<NodeReport>();
			foreach (NodeReport nodeReport in nodeReports)
			{
				if (nodeStates.Contains(nodeReport.GetNodeState()))
				{
					reports.AddItem(nodeReport);
				}
			}
			return reports;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNodeStatus()
		{
			NodeId nodeId = NodeId.NewInstance("host0", 0);
			NodeCLI cli = new NodeCLI();
			Org.Mockito.Mockito.When(client.GetNodeReports()).ThenReturn(GetNodeReports(3, NodeState
				.Running, false));
			cli.SetClient(client);
			cli.SetSysOutPrintStream(sysOut);
			cli.SetSysErrPrintStream(sysErr);
			int result = cli.Run(new string[] { "-status", nodeId.ToString() });
			NUnit.Framework.Assert.AreEqual(0, result);
			Org.Mockito.Mockito.Verify(client).GetNodeReports();
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			PrintWriter pw = new PrintWriter(baos);
			pw.WriteLine("Node Report : ");
			pw.WriteLine("\tNode-Id : host0:0");
			pw.WriteLine("\tRack : rack1");
			pw.WriteLine("\tNode-State : RUNNING");
			pw.WriteLine("\tNode-Http-Address : host1:8888");
			pw.WriteLine("\tLast-Health-Update : " + DateFormatUtils.Format(Sharpen.Extensions.CreateDate
				(0), "E dd/MMM/yy hh:mm:ss:SSzz"));
			pw.WriteLine("\tHealth-Report : ");
			pw.WriteLine("\tContainers : 0");
			pw.WriteLine("\tMemory-Used : 0MB");
			pw.WriteLine("\tMemory-Capacity : 0MB");
			pw.WriteLine("\tCPU-Used : 0 vcores");
			pw.WriteLine("\tCPU-Capacity : 0 vcores");
			pw.WriteLine("\tNode-Labels : a,b,c,x,y,z");
			pw.Close();
			string nodeStatusStr = baos.ToString("UTF-8");
			Org.Mockito.Mockito.Verify(sysOut, Org.Mockito.Mockito.Times(1)).WriteLine(Matchers.IsA
				<string>());
			Org.Mockito.Mockito.Verify(sysOut).WriteLine(nodeStatusStr);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNodeStatusWithEmptyNodeLabels()
		{
			NodeId nodeId = NodeId.NewInstance("host0", 0);
			NodeCLI cli = new NodeCLI();
			Org.Mockito.Mockito.When(client.GetNodeReports()).ThenReturn(GetNodeReports(3, NodeState
				.Running));
			cli.SetClient(client);
			cli.SetSysOutPrintStream(sysOut);
			cli.SetSysErrPrintStream(sysErr);
			int result = cli.Run(new string[] { "-status", nodeId.ToString() });
			NUnit.Framework.Assert.AreEqual(0, result);
			Org.Mockito.Mockito.Verify(client).GetNodeReports();
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			PrintWriter pw = new PrintWriter(baos);
			pw.WriteLine("Node Report : ");
			pw.WriteLine("\tNode-Id : host0:0");
			pw.WriteLine("\tRack : rack1");
			pw.WriteLine("\tNode-State : RUNNING");
			pw.WriteLine("\tNode-Http-Address : host1:8888");
			pw.WriteLine("\tLast-Health-Update : " + DateFormatUtils.Format(Sharpen.Extensions.CreateDate
				(0), "E dd/MMM/yy hh:mm:ss:SSzz"));
			pw.WriteLine("\tHealth-Report : ");
			pw.WriteLine("\tContainers : 0");
			pw.WriteLine("\tMemory-Used : 0MB");
			pw.WriteLine("\tMemory-Capacity : 0MB");
			pw.WriteLine("\tCPU-Used : 0 vcores");
			pw.WriteLine("\tCPU-Capacity : 0 vcores");
			pw.WriteLine("\tNode-Labels : ");
			pw.Close();
			string nodeStatusStr = baos.ToString("UTF-8");
			Org.Mockito.Mockito.Verify(sysOut, Org.Mockito.Mockito.Times(1)).WriteLine(Matchers.IsA
				<string>());
			Org.Mockito.Mockito.Verify(sysOut).WriteLine(nodeStatusStr);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAbsentNodeStatus()
		{
			NodeId nodeId = NodeId.NewInstance("Absenthost0", 0);
			NodeCLI cli = new NodeCLI();
			Org.Mockito.Mockito.When(client.GetNodeReports()).ThenReturn(GetNodeReports(0, NodeState
				.Running));
			cli.SetClient(client);
			cli.SetSysOutPrintStream(sysOut);
			cli.SetSysErrPrintStream(sysErr);
			int result = cli.Run(new string[] { "-status", nodeId.ToString() });
			NUnit.Framework.Assert.AreEqual(0, result);
			Org.Mockito.Mockito.Verify(client).GetNodeReports();
			Org.Mockito.Mockito.Verify(sysOut, Org.Mockito.Mockito.Times(1)).WriteLine(Matchers.IsA
				<string>());
			Org.Mockito.Mockito.Verify(sysOut).WriteLine("Could not find the node report for node id : "
				 + nodeId.ToString());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAppCLIUsageInfo()
		{
			VerifyUsageInfo(new ApplicationCLI());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNodeCLIUsageInfo()
		{
			VerifyUsageInfo(new NodeCLI());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMissingArguments()
		{
			ApplicationCLI cli = CreateAndGetAppCLI();
			int result = cli.Run(new string[] { "application", "-status" });
			NUnit.Framework.Assert.AreEqual(result, -1);
			NUnit.Framework.Assert.AreEqual(string.Format("Missing argument for options%n%1s"
				, CreateApplicationCLIHelpMessage()), sysOutStream.ToString());
			sysOutStream.Reset();
			result = cli.Run(new string[] { "applicationattempt", "-status" });
			NUnit.Framework.Assert.AreEqual(result, -1);
			NUnit.Framework.Assert.AreEqual(string.Format("Missing argument for options%n%1s"
				, CreateApplicationAttemptCLIHelpMessage()), sysOutStream.ToString());
			sysOutStream.Reset();
			result = cli.Run(new string[] { "container", "-status" });
			NUnit.Framework.Assert.AreEqual(result, -1);
			NUnit.Framework.Assert.AreEqual(string.Format("Missing argument for options%n%1s"
				, CreateContainerCLIHelpMessage()), sysOutStream.ToString());
			sysOutStream.Reset();
			NodeCLI nodeCLI = new NodeCLI();
			nodeCLI.SetClient(client);
			nodeCLI.SetSysOutPrintStream(sysOut);
			nodeCLI.SetSysErrPrintStream(sysErr);
			result = nodeCLI.Run(new string[] { "-status" });
			NUnit.Framework.Assert.AreEqual(result, -1);
			NUnit.Framework.Assert.AreEqual(string.Format("Missing argument for options%n%1s"
				, CreateNodeCLIHelpMessage()), sysOutStream.ToString());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetQueueInfo()
		{
			QueueCLI cli = CreateAndGetQueueCLI();
			ICollection<string> nodeLabels = new HashSet<string>();
			nodeLabels.AddItem("GPU");
			nodeLabels.AddItem("JDK_7");
			QueueInfo queueInfo = QueueInfo.NewInstance("queueA", 0.4f, 0.8f, 0.5f, null, null
				, QueueState.Running, nodeLabels, "GPU");
			Org.Mockito.Mockito.When(client.GetQueueInfo(Matchers.Any<string>())).ThenReturn(
				queueInfo);
			int result = cli.Run(new string[] { "-status", "queueA" });
			NUnit.Framework.Assert.AreEqual(0, result);
			Org.Mockito.Mockito.Verify(client).GetQueueInfo("queueA");
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			PrintWriter pw = new PrintWriter(baos);
			pw.WriteLine("Queue Information : ");
			pw.WriteLine("Queue Name : " + "queueA");
			pw.WriteLine("\tState : " + "RUNNING");
			pw.WriteLine("\tCapacity : " + "40.0%");
			pw.WriteLine("\tCurrent Capacity : " + "50.0%");
			pw.WriteLine("\tMaximum Capacity : " + "80.0%");
			pw.WriteLine("\tDefault Node Label expression : " + "GPU");
			pw.WriteLine("\tAccessible Node Labels : " + "JDK_7,GPU");
			pw.Close();
			string queueInfoStr = baos.ToString("UTF-8");
			NUnit.Framework.Assert.AreEqual(queueInfoStr, sysOutStream.ToString());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetQueueInfoWithEmptyNodeLabel()
		{
			QueueCLI cli = CreateAndGetQueueCLI();
			QueueInfo queueInfo = QueueInfo.NewInstance("queueA", 0.4f, 0.8f, 0.5f, null, null
				, QueueState.Running, null, null);
			Org.Mockito.Mockito.When(client.GetQueueInfo(Matchers.Any<string>())).ThenReturn(
				queueInfo);
			int result = cli.Run(new string[] { "-status", "queueA" });
			NUnit.Framework.Assert.AreEqual(0, result);
			Org.Mockito.Mockito.Verify(client).GetQueueInfo("queueA");
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			PrintWriter pw = new PrintWriter(baos);
			pw.WriteLine("Queue Information : ");
			pw.WriteLine("Queue Name : " + "queueA");
			pw.WriteLine("\tState : " + "RUNNING");
			pw.WriteLine("\tCapacity : " + "40.0%");
			pw.WriteLine("\tCurrent Capacity : " + "50.0%");
			pw.WriteLine("\tMaximum Capacity : " + "80.0%");
			pw.WriteLine("\tDefault Node Label expression : ");
			pw.WriteLine("\tAccessible Node Labels : ");
			pw.Close();
			string queueInfoStr = baos.ToString("UTF-8");
			NUnit.Framework.Assert.AreEqual(queueInfoStr, sysOutStream.ToString());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetQueueInfoWithNonExistedQueue()
		{
			string queueName = "non-existed-queue";
			QueueCLI cli = CreateAndGetQueueCLI();
			Org.Mockito.Mockito.When(client.GetQueueInfo(Matchers.Any<string>())).ThenReturn(
				null);
			int result = cli.Run(new string[] { "-status", queueName });
			NUnit.Framework.Assert.AreEqual(-1, result);
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			PrintWriter pw = new PrintWriter(baos);
			pw.WriteLine("Cannot get queue from RM by queueName = " + queueName + ", please check."
				);
			pw.Close();
			string queueInfoStr = baos.ToString("UTF-8");
			NUnit.Framework.Assert.AreEqual(queueInfoStr, sysOutStream.ToString());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetApplicationAttemptReportException()
		{
			ApplicationCLI cli = CreateAndGetAppCLI();
			ApplicationId applicationId = ApplicationId.NewInstance(1234, 5);
			ApplicationAttemptId attemptId1 = ApplicationAttemptId.NewInstance(applicationId, 
				1);
			Org.Mockito.Mockito.When(client.GetApplicationAttemptReport(attemptId1)).ThenThrow
				(new ApplicationNotFoundException("History file for application" + applicationId
				 + " is not found"));
			int exitCode = cli.Run(new string[] { "applicationattempt", "-status", attemptId1
				.ToString() });
			Org.Mockito.Mockito.Verify(sysOut).WriteLine("Application for AppAttempt with id '"
				 + attemptId1 + "' doesn't exist in RM or Timeline Server.");
			NUnit.Framework.Assert.AreNotSame("should return non-zero exit code.", 0, exitCode
				);
			ApplicationAttemptId attemptId2 = ApplicationAttemptId.NewInstance(applicationId, 
				2);
			Org.Mockito.Mockito.When(client.GetApplicationAttemptReport(attemptId2)).ThenThrow
				(new ApplicationAttemptNotFoundException("History file for application attempt" 
				+ attemptId2 + " is not found"));
			exitCode = cli.Run(new string[] { "applicationattempt", "-status", attemptId2.ToString
				() });
			Org.Mockito.Mockito.Verify(sysOut).WriteLine("Application Attempt with id '" + attemptId2
				 + "' doesn't exist in RM or Timeline Server.");
			NUnit.Framework.Assert.AreNotSame("should return non-zero exit code.", 0, exitCode
				);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetContainerReportException()
		{
			ApplicationCLI cli = CreateAndGetAppCLI();
			ApplicationId applicationId = ApplicationId.NewInstance(1234, 5);
			ApplicationAttemptId attemptId = ApplicationAttemptId.NewInstance(applicationId, 
				1);
			long cntId = 1;
			ContainerId containerId1 = ContainerId.NewContainerId(attemptId, cntId++);
			Org.Mockito.Mockito.When(client.GetContainerReport(containerId1)).ThenThrow(new ApplicationNotFoundException
				("History file for application" + applicationId + " is not found"));
			int exitCode = cli.Run(new string[] { "container", "-status", containerId1.ToString
				() });
			Org.Mockito.Mockito.Verify(sysOut).WriteLine("Application for Container with id '"
				 + containerId1 + "' doesn't exist in RM or Timeline Server.");
			NUnit.Framework.Assert.AreNotSame("should return non-zero exit code.", 0, exitCode
				);
			ContainerId containerId2 = ContainerId.NewContainerId(attemptId, cntId++);
			Org.Mockito.Mockito.When(client.GetContainerReport(containerId2)).ThenThrow(new ApplicationAttemptNotFoundException
				("History file for application attempt" + attemptId + " is not found"));
			exitCode = cli.Run(new string[] { "container", "-status", containerId2.ToString()
				 });
			Org.Mockito.Mockito.Verify(sysOut).WriteLine("Application Attempt for Container with id '"
				 + containerId2 + "' doesn't exist in RM or Timeline Server.");
			NUnit.Framework.Assert.AreNotSame("should return non-zero exit code.", 0, exitCode
				);
			ContainerId containerId3 = ContainerId.NewContainerId(attemptId, cntId++);
			Org.Mockito.Mockito.When(client.GetContainerReport(containerId3)).ThenThrow(new ContainerNotFoundException
				("History file for container" + containerId3 + " is not found"));
			exitCode = cli.Run(new string[] { "container", "-status", containerId3.ToString()
				 });
			Org.Mockito.Mockito.Verify(sysOut).WriteLine("Container with id '" + containerId3
				 + "' doesn't exist in RM or Timeline Server.");
			NUnit.Framework.Assert.AreNotSame("should return non-zero exit code.", 0, exitCode
				);
		}

		/// <exception cref="System.Exception"/>
		private void VerifyUsageInfo(YarnCLI cli)
		{
			cli.SetSysErrPrintStream(sysErr);
			cli.Run(new string[] { "application" });
			Org.Mockito.Mockito.Verify(sysErr).WriteLine("Invalid Command Usage : ");
		}

		private IList<NodeReport> GetNodeReports(int noOfNodes, NodeState state)
		{
			return GetNodeReports(noOfNodes, state, true);
		}

		private IList<NodeReport> GetNodeReports(int noOfNodes, NodeState state, bool emptyNodeLabel
			)
		{
			IList<NodeReport> nodeReports = new AList<NodeReport>();
			for (int i = 0; i < noOfNodes; i++)
			{
				ICollection<string> nodeLabels = null;
				if (!emptyNodeLabel)
				{
					// node labels is not ordered, but when we output it, it should be
					// ordered
					nodeLabels = ImmutableSet.Of("c", "b", "a", "x", "z", "y");
				}
				NodeReport nodeReport = NodeReport.NewInstance(NodeId.NewInstance("host" + i, 0), 
					state, "host" + 1 + ":8888", "rack1", Org.Apache.Hadoop.Yarn.Util.Records.NewRecord
					<Resource>(), Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<Resource>(), 0, string.Empty
					, 0, nodeLabels);
				nodeReports.AddItem(nodeReport);
			}
			return nodeReports;
		}

		private ApplicationCLI CreateAndGetAppCLI()
		{
			ApplicationCLI cli = new ApplicationCLI();
			cli.SetClient(client);
			cli.SetSysOutPrintStream(sysOut);
			return cli;
		}

		private QueueCLI CreateAndGetQueueCLI()
		{
			QueueCLI cli = new QueueCLI();
			cli.SetClient(client);
			cli.SetSysOutPrintStream(sysOut);
			cli.SetSysErrPrintStream(sysErr);
			return cli;
		}

		/// <exception cref="System.IO.IOException"/>
		private string CreateApplicationCLIHelpMessage()
		{
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			PrintWriter pw = new PrintWriter(baos);
			pw.WriteLine("usage: application");
			pw.WriteLine(" -appStates <States>             Works with -list to filter applications"
				);
			pw.WriteLine("                                 based on input comma-separated list of"
				);
			pw.WriteLine("                                 application states. The valid application"
				);
			pw.WriteLine("                                 state can be one of the following:"
				);
			pw.WriteLine("                                 ALL,NEW,NEW_SAVING,SUBMITTED,ACCEPTED,RUN"
				);
			pw.WriteLine("                                 NING,FINISHED,FAILED,KILLED");
			pw.WriteLine(" -appTypes <Types>               Works with -list to filter applications"
				);
			pw.WriteLine("                                 based on input comma-separated list of"
				);
			pw.WriteLine("                                 application types.");
			pw.WriteLine(" -help                           Displays help for all commands.");
			pw.WriteLine(" -kill <Application ID>          Kills the application.");
			pw.WriteLine(" -list                           List applications. Supports optional use"
				);
			pw.WriteLine("                                 of -appTypes to filter applications based"
				);
			pw.WriteLine("                                 on application type, and -appStates to"
				);
			pw.WriteLine("                                 filter applications based on application"
				);
			pw.WriteLine("                                 state.");
			pw.WriteLine(" -movetoqueue <Application ID>   Moves the application to a different"
				);
			pw.WriteLine("                                 queue.");
			pw.WriteLine(" -queue <Queue Name>             Works with the movetoqueue command to"
				);
			pw.WriteLine("                                 specify which queue to move an");
			pw.WriteLine("                                 application to.");
			pw.WriteLine(" -status <Application ID>        Prints the status of the application."
				);
			pw.Close();
			string appsHelpStr = baos.ToString("UTF-8");
			return appsHelpStr;
		}

		/// <exception cref="System.IO.IOException"/>
		private string CreateApplicationAttemptCLIHelpMessage()
		{
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			PrintWriter pw = new PrintWriter(baos);
			pw.WriteLine("usage: applicationattempt");
			pw.WriteLine(" -help                              Displays help for all commands."
				);
			pw.WriteLine(" -list <Application ID>             List application attempts for");
			pw.WriteLine("                                    aplication.");
			pw.WriteLine(" -status <Application Attempt ID>   Prints the status of the application"
				);
			pw.WriteLine("                                    attempt.");
			pw.Close();
			string appsHelpStr = baos.ToString("UTF-8");
			return appsHelpStr;
		}

		/// <exception cref="System.IO.IOException"/>
		private string CreateContainerCLIHelpMessage()
		{
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			PrintWriter pw = new PrintWriter(baos);
			pw.WriteLine("usage: container");
			pw.WriteLine(" -help                            Displays help for all commands.");
			pw.WriteLine(" -list <Application Attempt ID>   List containers for application attempt."
				);
			pw.WriteLine(" -status <Container ID>           Prints the status of the container."
				);
			pw.Close();
			string appsHelpStr = baos.ToString("UTF-8");
			return appsHelpStr;
		}

		/// <exception cref="System.IO.IOException"/>
		private string CreateNodeCLIHelpMessage()
		{
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			PrintWriter pw = new PrintWriter(baos);
			pw.WriteLine("usage: node");
			pw.WriteLine(" -all               Works with -list to list all nodes.");
			pw.WriteLine(" -help              Displays help for all commands.");
			pw.WriteLine(" -list              List all running nodes. Supports optional use of"
				);
			pw.WriteLine("                    -states to filter nodes based on node state, all -all"
				);
			pw.WriteLine("                    to list all nodes.");
			pw.WriteLine(" -states <States>   Works with -list to filter nodes based on input"
				);
			pw.WriteLine("                    comma-separated list of node states.");
			pw.WriteLine(" -status <NodeId>   Prints the status report of the node.");
			pw.Close();
			string nodesHelpStr = baos.ToString("UTF-8");
			return nodesHelpStr;
		}
	}
}
