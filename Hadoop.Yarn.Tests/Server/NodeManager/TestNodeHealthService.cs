using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Org.Apache.Hadoop.Yarn.Server.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager
{
	public class TestNodeHealthService
	{
		private static volatile Log Log = LogFactory.GetLog(typeof(TestNodeHealthService)
			);

		protected internal static FilePath testRootDir = new FilePath("target", typeof(TestNodeHealthService
			).FullName + "-localDir").GetAbsoluteFile();

		internal static readonly FilePath nodeHealthConfigFile = new FilePath(testRootDir
			, "modified-mapred-site.xml");

		private FilePath nodeHealthscriptFile = new FilePath(testRootDir, Shell.AppendScriptExtension
			("failingscript"));

		[SetUp]
		public virtual void Setup()
		{
			testRootDir.Mkdirs();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			if (testRootDir.Exists())
			{
				FileContext.GetLocalFSFileContext().Delete(new Path(testRootDir.GetAbsolutePath()
					), true);
			}
		}

		private Configuration GetConfForNodeHealthScript()
		{
			Configuration conf = new Configuration();
			conf.Set(YarnConfiguration.NmHealthCheckScriptPath, nodeHealthscriptFile.GetAbsolutePath
				());
			conf.SetLong(YarnConfiguration.NmHealthCheckIntervalMs, 500);
			conf.SetLong(YarnConfiguration.NmHealthCheckScriptTimeoutMs, 1000);
			return conf;
		}

		/// <exception cref="System.IO.IOException"/>
		private void WriteNodeHealthScriptFile(string scriptStr, bool setExecutable)
		{
			PrintWriter pw = null;
			try
			{
				FileUtil.SetWritable(nodeHealthscriptFile, true);
				FileUtil.SetReadable(nodeHealthscriptFile, true);
				pw = new PrintWriter(new FileOutputStream(nodeHealthscriptFile));
				pw.WriteLine(scriptStr);
				pw.Flush();
			}
			finally
			{
				pw.Close();
			}
			FileUtil.SetExecutable(nodeHealthscriptFile, setExecutable);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestNodeHealthScriptShouldRun()
		{
			// Node health script should not start if there is no property called
			// node health script path.
			NUnit.Framework.Assert.IsFalse("By default Health script should not have started"
				, NodeHealthScriptRunner.ShouldRun(new Configuration()));
			Configuration conf = GetConfForNodeHealthScript();
			// Node health script should not start if the node health script does not
			// exists
			NUnit.Framework.Assert.IsFalse("Node health script should start", NodeHealthScriptRunner
				.ShouldRun(conf));
			// Create script path.
			conf.WriteXml(new FileOutputStream(nodeHealthConfigFile));
			conf.AddResource(nodeHealthConfigFile.GetName());
			WriteNodeHealthScriptFile(string.Empty, false);
			// Node health script should not start if the node health script is not
			// executable.
			NUnit.Framework.Assert.IsFalse("Node health script should start", NodeHealthScriptRunner
				.ShouldRun(conf));
			WriteNodeHealthScriptFile(string.Empty, true);
			NUnit.Framework.Assert.IsTrue("Node health script should start", NodeHealthScriptRunner
				.ShouldRun(conf));
		}

		private void SetHealthStatus(NodeHealthStatus healthStatus, bool isHealthy, string
			 healthReport, long lastHealthReportTime)
		{
			healthStatus.SetHealthReport(healthReport);
			healthStatus.SetIsNodeHealthy(isHealthy);
			healthStatus.SetLastHealthReportTime(lastHealthReportTime);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNodeHealthScript()
		{
			RecordFactory factory = RecordFactoryProvider.GetRecordFactory(null);
			NodeHealthStatus healthStatus = factory.NewRecordInstance<NodeHealthStatus>();
			string errorScript = "echo ERROR\n echo \"Tracker not healthy\"";
			string normalScript = "echo \"I am all fine\"";
			string timeOutScript = Shell.Windows ? "@echo off\nping -n 4 127.0.0.1 >nul\necho \"I am fine\""
				 : "sleep 4\necho \"I am fine\"";
			Configuration conf = GetConfForNodeHealthScript();
			conf.WriteXml(new FileOutputStream(nodeHealthConfigFile));
			conf.AddResource(nodeHealthConfigFile.GetName());
			WriteNodeHealthScriptFile(normalScript, true);
			NodeHealthCheckerService nodeHealthChecker = new NodeHealthCheckerService();
			nodeHealthChecker.Init(conf);
			NodeHealthScriptRunner nodeHealthScriptRunner = nodeHealthChecker.GetNodeHealthScriptRunner
				();
			TimerTask timerTask = nodeHealthScriptRunner.GetTimerTask();
			timerTask.Run();
			SetHealthStatus(healthStatus, nodeHealthChecker.IsHealthy(), nodeHealthChecker.GetHealthReport
				(), nodeHealthChecker.GetLastHealthReportTime());
			Log.Info("Checking initial healthy condition");
			// Check proper report conditions.
			NUnit.Framework.Assert.IsTrue("Node health status reported unhealthy", healthStatus
				.GetIsNodeHealthy());
			NUnit.Framework.Assert.IsTrue("Node health status reported unhealthy", healthStatus
				.GetHealthReport().Equals(nodeHealthChecker.GetHealthReport()));
			// write out error file.
			// Healthy to unhealthy transition
			WriteNodeHealthScriptFile(errorScript, true);
			// Run timer
			timerTask.Run();
			// update health status
			SetHealthStatus(healthStatus, nodeHealthChecker.IsHealthy(), nodeHealthChecker.GetHealthReport
				(), nodeHealthChecker.GetLastHealthReportTime());
			Log.Info("Checking Healthy--->Unhealthy");
			NUnit.Framework.Assert.IsFalse("Node health status reported healthy", healthStatus
				.GetIsNodeHealthy());
			NUnit.Framework.Assert.IsTrue("Node health status reported healthy", healthStatus
				.GetHealthReport().Equals(nodeHealthChecker.GetHealthReport()));
			// Check unhealthy to healthy transitions.
			WriteNodeHealthScriptFile(normalScript, true);
			timerTask.Run();
			SetHealthStatus(healthStatus, nodeHealthChecker.IsHealthy(), nodeHealthChecker.GetHealthReport
				(), nodeHealthChecker.GetLastHealthReportTime());
			Log.Info("Checking UnHealthy--->healthy");
			// Check proper report conditions.
			NUnit.Framework.Assert.IsTrue("Node health status reported unhealthy", healthStatus
				.GetIsNodeHealthy());
			NUnit.Framework.Assert.IsTrue("Node health status reported unhealthy", healthStatus
				.GetHealthReport().Equals(nodeHealthChecker.GetHealthReport()));
			// Healthy to timeout transition.
			WriteNodeHealthScriptFile(timeOutScript, true);
			timerTask.Run();
			SetHealthStatus(healthStatus, nodeHealthChecker.IsHealthy(), nodeHealthChecker.GetHealthReport
				(), nodeHealthChecker.GetLastHealthReportTime());
			Log.Info("Checking Healthy--->timeout");
			NUnit.Framework.Assert.IsFalse("Node health status reported healthy even after timeout"
				, healthStatus.GetIsNodeHealthy());
			NUnit.Framework.Assert.IsTrue("Node script time out message not propogated", healthStatus
				.GetHealthReport().Equals(NodeHealthScriptRunner.NodeHealthScriptTimedOutMsg + NodeHealthCheckerService
				.Separator + nodeHealthChecker.GetDiskHandler().GetDisksHealthReport(false)));
		}
	}
}
