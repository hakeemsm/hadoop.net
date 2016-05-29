using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Client.Api;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Logaggregation;
using Org.Mockito;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Client.Cli
{
	public class TestLogsCLI
	{
		internal ByteArrayOutputStream sysOutStream;

		private TextWriter sysOut;

		internal ByteArrayOutputStream sysErrStream;

		private TextWriter sysErr;

		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			sysOutStream = new ByteArrayOutputStream();
			sysOut = new TextWriter(sysOutStream);
			Runtime.SetOut(sysOut);
			sysErrStream = new ByteArrayOutputStream();
			sysErr = new TextWriter(sysErrStream);
			Runtime.SetErr(sysErr);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestFailResultCodes()
		{
			Configuration conf = new YarnConfiguration();
			conf.SetClass("fs.file.impl", typeof(LocalFileSystem), typeof(FileSystem));
			LogCLIHelpers cliHelper = new LogCLIHelpers();
			cliHelper.SetConf(conf);
			YarnClient mockYarnClient = CreateMockYarnClient(YarnApplicationState.Finished);
			LogsCLI dumper = new TestLogsCLI.LogsCLIForTest(mockYarnClient);
			dumper.SetConf(conf);
			// verify dumping a non-existent application's logs returns a failure code
			int exitCode = dumper.Run(new string[] { "-applicationId", "application_0_0" });
			NUnit.Framework.Assert.IsTrue("Should return an error code", exitCode != 0);
			// verify dumping a non-existent container log is a failure code 
			exitCode = cliHelper.DumpAContainersLogs("application_0_0", "container_0_0", "nonexistentnode:1234"
				, "nobody");
			NUnit.Framework.Assert.IsTrue("Should return an error code", exitCode != 0);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestInvalidApplicationId()
		{
			Configuration conf = new YarnConfiguration();
			YarnClient mockYarnClient = CreateMockYarnClient(YarnApplicationState.Finished);
			LogsCLI cli = new TestLogsCLI.LogsCLIForTest(mockYarnClient);
			cli.SetConf(conf);
			int exitCode = cli.Run(new string[] { "-applicationId", "not_an_app_id" });
			NUnit.Framework.Assert.IsTrue(exitCode == -1);
			NUnit.Framework.Assert.IsTrue(sysErrStream.ToString().StartsWith("Invalid ApplicationId specified"
				));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestUnknownApplicationId()
		{
			Configuration conf = new YarnConfiguration();
			YarnClient mockYarnClient = CreateMockYarnClientUnknownApp();
			LogsCLI cli = new TestLogsCLI.LogsCLIForTest(mockYarnClient);
			cli.SetConf(conf);
			int exitCode = cli.Run(new string[] { "-applicationId", ApplicationId.NewInstance
				(1, 1).ToString() });
			// Error since no logs present for the app.
			NUnit.Framework.Assert.IsTrue(exitCode != 0);
			NUnit.Framework.Assert.IsTrue(sysErrStream.ToString().StartsWith("Unable to get ApplicationState"
				));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestHelpMessage()
		{
			Configuration conf = new YarnConfiguration();
			YarnClient mockYarnClient = CreateMockYarnClient(YarnApplicationState.Finished);
			LogsCLI dumper = new TestLogsCLI.LogsCLIForTest(mockYarnClient);
			dumper.SetConf(conf);
			int exitCode = dumper.Run(new string[] {  });
			NUnit.Framework.Assert.IsTrue(exitCode == -1);
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			PrintWriter pw = new PrintWriter(baos);
			pw.WriteLine("Retrieve logs for completed YARN applications.");
			pw.WriteLine("usage: yarn logs -applicationId <application ID> [OPTIONS]");
			pw.WriteLine();
			pw.WriteLine("general options are:");
			pw.WriteLine(" -appOwner <Application Owner>   AppOwner (assumed to be current user if"
				);
			pw.WriteLine("                                 not specified)");
			pw.WriteLine(" -containerId <Container ID>     ContainerId (must be specified if node"
				);
			pw.WriteLine("                                 address is specified)");
			pw.WriteLine(" -help                           Displays help for all commands.");
			pw.WriteLine(" -nodeAddress <Node Address>     NodeAddress in the format nodename:port"
				);
			pw.WriteLine("                                 (must be specified if container id is"
				);
			pw.WriteLine("                                 specified)");
			pw.Close();
			string appReportStr = baos.ToString("UTF-8");
			NUnit.Framework.Assert.AreEqual(appReportStr, sysOutStream.ToString());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestFetchApplictionLogs()
		{
			string remoteLogRootDir = "target/logs/";
			Configuration configuration = new Configuration();
			configuration.SetBoolean(YarnConfiguration.LogAggregationEnabled, true);
			configuration.Set(YarnConfiguration.NmRemoteAppLogDir, remoteLogRootDir);
			configuration.SetBoolean(YarnConfiguration.YarnAclEnable, true);
			configuration.Set(YarnConfiguration.YarnAdminAcl, "admin");
			FileSystem fs = FileSystem.Get(configuration);
			UserGroupInformation ugi = UserGroupInformation.GetCurrentUser();
			ApplicationId appId = ApplicationIdPBImpl.NewInstance(0, 1);
			ApplicationAttemptId appAttemptId = ApplicationAttemptIdPBImpl.NewInstance(appId, 
				1);
			ContainerId containerId0 = ContainerIdPBImpl.NewContainerId(appAttemptId, 0);
			ContainerId containerId1 = ContainerIdPBImpl.NewContainerId(appAttemptId, 1);
			ContainerId containerId2 = ContainerIdPBImpl.NewContainerId(appAttemptId, 2);
			NodeId nodeId = NodeId.NewInstance("localhost", 1234);
			// create local logs
			string rootLogDir = "target/LocalLogs";
			Path rootLogDirPath = new Path(rootLogDir);
			if (fs.Exists(rootLogDirPath))
			{
				fs.Delete(rootLogDirPath, true);
			}
			NUnit.Framework.Assert.IsTrue(fs.Mkdirs(rootLogDirPath));
			Path appLogsDir = new Path(rootLogDirPath, appId.ToString());
			if (fs.Exists(appLogsDir))
			{
				fs.Delete(appLogsDir, true);
			}
			NUnit.Framework.Assert.IsTrue(fs.Mkdirs(appLogsDir));
			IList<string> rootLogDirs = Arrays.AsList(rootLogDir);
			// create container logs in localLogDir
			CreateContainerLogInLocalDir(appLogsDir, containerId1, fs);
			CreateContainerLogInLocalDir(appLogsDir, containerId2, fs);
			Path path = new Path(remoteLogRootDir + ugi.GetShortUserName() + "/logs/application_0_0001"
				);
			if (fs.Exists(path))
			{
				fs.Delete(path, true);
			}
			NUnit.Framework.Assert.IsTrue(fs.Mkdirs(path));
			// upload container logs into remote directory
			// the first two logs is empty. When we try to read first two logs,
			// we will meet EOF exception, but it will not impact other logs.
			// Other logs should be read successfully.
			UploadEmptyContainerLogIntoRemoteDir(ugi, configuration, rootLogDirs, nodeId, containerId0
				, path, fs);
			UploadEmptyContainerLogIntoRemoteDir(ugi, configuration, rootLogDirs, nodeId, containerId1
				, path, fs);
			UploadContainerLogIntoRemoteDir(ugi, configuration, rootLogDirs, nodeId, containerId1
				, path, fs);
			UploadContainerLogIntoRemoteDir(ugi, configuration, rootLogDirs, nodeId, containerId2
				, path, fs);
			YarnClient mockYarnClient = CreateMockYarnClient(YarnApplicationState.Finished);
			LogsCLI cli = new TestLogsCLI.LogsCLIForTest(mockYarnClient);
			cli.SetConf(configuration);
			int exitCode = cli.Run(new string[] { "-applicationId", appId.ToString() });
			NUnit.Framework.Assert.IsTrue(exitCode == 0);
			NUnit.Framework.Assert.IsTrue(sysOutStream.ToString().Contains("Hello container_0_0001_01_000001!"
				));
			NUnit.Framework.Assert.IsTrue(sysOutStream.ToString().Contains("Hello container_0_0001_01_000002!"
				));
			sysOutStream.Reset();
			// uploaded two logs for container1. The first log is empty.
			// The second one is not empty.
			// We can still successfully read logs for container1.
			exitCode = cli.Run(new string[] { "-applicationId", appId.ToString(), "-nodeAddress"
				, nodeId.ToString(), "-containerId", containerId1.ToString() });
			NUnit.Framework.Assert.IsTrue(exitCode == 0);
			NUnit.Framework.Assert.IsTrue(sysOutStream.ToString().Contains("Hello container_0_0001_01_000001!"
				));
			NUnit.Framework.Assert.IsTrue(sysOutStream.ToString().Contains("Log Upload Time")
				);
			NUnit.Framework.Assert.IsTrue(!sysOutStream.ToString().Contains("Logs for container "
				 + containerId1.ToString() + " are not present in this log-file."));
			sysOutStream.Reset();
			// Uploaded the empty log for container0.
			// We should see the message showing the log for container0
			// are not present.
			exitCode = cli.Run(new string[] { "-applicationId", appId.ToString(), "-nodeAddress"
				, nodeId.ToString(), "-containerId", containerId0.ToString() });
			NUnit.Framework.Assert.IsTrue(exitCode == -1);
			NUnit.Framework.Assert.IsTrue(sysOutStream.ToString().Contains("Logs for container "
				 + containerId0.ToString() + " are not present in this log-file."));
			fs.Delete(new Path(remoteLogRootDir), true);
			fs.Delete(new Path(rootLogDir), true);
		}

		/// <exception cref="System.Exception"/>
		private static void CreateContainerLogInLocalDir(Path appLogsDir, ContainerId containerId
			, FileSystem fs)
		{
			Path containerLogsDir = new Path(appLogsDir, containerId.ToString());
			if (fs.Exists(containerLogsDir))
			{
				fs.Delete(containerLogsDir, true);
			}
			NUnit.Framework.Assert.IsTrue(fs.Mkdirs(containerLogsDir));
			TextWriter writer = new FileWriter(new FilePath(containerLogsDir.ToString(), "sysout"
				));
			writer.Write("Hello " + containerId + "!");
			writer.Close();
		}

		/// <exception cref="System.Exception"/>
		private static void UploadContainerLogIntoRemoteDir(UserGroupInformation ugi, Configuration
			 configuration, IList<string> rootLogDirs, NodeId nodeId, ContainerId containerId
			, Path appDir, FileSystem fs)
		{
			Path path = new Path(appDir, LogAggregationUtils.GetNodeString(nodeId) + Runtime.
				CurrentTimeMillis());
			AggregatedLogFormat.LogWriter writer = new AggregatedLogFormat.LogWriter(configuration
				, path, ugi);
			writer.WriteApplicationOwner(ugi.GetUserName());
			IDictionary<ApplicationAccessType, string> appAcls = new Dictionary<ApplicationAccessType
				, string>();
			appAcls[ApplicationAccessType.ViewApp] = ugi.GetUserName();
			writer.WriteApplicationACLs(appAcls);
			writer.Append(new AggregatedLogFormat.LogKey(containerId), new AggregatedLogFormat.LogValue
				(rootLogDirs, containerId, UserGroupInformation.GetCurrentUser().GetShortUserName
				()));
			writer.Close();
		}

		/// <exception cref="System.Exception"/>
		private static void UploadEmptyContainerLogIntoRemoteDir(UserGroupInformation ugi
			, Configuration configuration, IList<string> rootLogDirs, NodeId nodeId, ContainerId
			 containerId, Path appDir, FileSystem fs)
		{
			Path path = new Path(appDir, LogAggregationUtils.GetNodeString(nodeId) + Runtime.
				CurrentTimeMillis());
			AggregatedLogFormat.LogWriter writer = new AggregatedLogFormat.LogWriter(configuration
				, path, ugi);
			writer.WriteApplicationOwner(ugi.GetUserName());
			IDictionary<ApplicationAccessType, string> appAcls = new Dictionary<ApplicationAccessType
				, string>();
			appAcls[ApplicationAccessType.ViewApp] = ugi.GetUserName();
			writer.WriteApplicationACLs(appAcls);
			DataOutputStream @out = writer.GetWriter().PrepareAppendKey(-1);
			new AggregatedLogFormat.LogKey(containerId).Write(@out);
			@out.Close();
			@out = writer.GetWriter().PrepareAppendValue(-1);
			new AggregatedLogFormat.LogValue(rootLogDirs, containerId, UserGroupInformation.GetCurrentUser
				().GetShortUserName()).Write(@out, new HashSet<FilePath>());
			@out.Close();
			writer.Close();
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		private YarnClient CreateMockYarnClient(YarnApplicationState appState)
		{
			YarnClient mockClient = Org.Mockito.Mockito.Mock<YarnClient>();
			ApplicationReport mockAppReport = Org.Mockito.Mockito.Mock<ApplicationReport>();
			Org.Mockito.Mockito.DoReturn(appState).When(mockAppReport).GetYarnApplicationState
				();
			Org.Mockito.Mockito.DoReturn(mockAppReport).When(mockClient).GetApplicationReport
				(Matchers.Any<ApplicationId>());
			return mockClient;
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		private YarnClient CreateMockYarnClientUnknownApp()
		{
			YarnClient mockClient = Org.Mockito.Mockito.Mock<YarnClient>();
			Org.Mockito.Mockito.DoThrow(new YarnException("Unknown AppId")).When(mockClient).
				GetApplicationReport(Matchers.Any<ApplicationId>());
			return mockClient;
		}

		private class LogsCLIForTest : LogsCLI
		{
			private YarnClient yarnClient;

			public LogsCLIForTest(YarnClient yarnClient)
				: base()
			{
				this.yarnClient = yarnClient;
			}

			protected internal override YarnClient CreateYarnClient()
			{
				return yarnClient;
			}
		}
	}
}
