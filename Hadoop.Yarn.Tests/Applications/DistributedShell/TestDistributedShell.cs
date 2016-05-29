using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Timeline;
using Org.Apache.Hadoop.Yarn.Client.Api;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Server;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Applications.Distributedshell
{
	public class TestDistributedShell
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestDistributedShell));

		protected internal MiniYARNCluster yarnCluster = null;

		protected internal YarnConfiguration conf = null;

		private const int NumNms = 1;

		protected internal static readonly string AppmasterJar = JarFinder.GetJar(typeof(
			ApplicationMaster));

		/// <exception cref="System.Exception"/>
		[SetUp]
		public virtual void Setup()
		{
			SetupInternal(NumNms);
		}

		/// <exception cref="System.Exception"/>
		protected internal virtual void SetupInternal(int numNodeManager)
		{
			Log.Info("Starting up YARN cluster");
			conf = new YarnConfiguration();
			conf.SetInt(YarnConfiguration.RmSchedulerMinimumAllocationMb, 128);
			conf.Set("yarn.log.dir", "target");
			conf.SetBoolean(YarnConfiguration.TimelineServiceEnabled, true);
			conf.Set(YarnConfiguration.RmScheduler, typeof(CapacityScheduler).FullName);
			conf.SetBoolean(YarnConfiguration.NodeLabelsEnabled, true);
			if (yarnCluster == null)
			{
				yarnCluster = new MiniYARNCluster(typeof(TestDistributedShell).Name, 1, numNodeManager
					, 1, 1);
				yarnCluster.Init(conf);
				yarnCluster.Start();
				conf.Set(YarnConfiguration.TimelineServiceWebappAddress, MiniYARNCluster.GetHostname
					() + ":" + yarnCluster.GetApplicationHistoryServer().GetPort());
				WaitForNMsToRegister();
				Uri url = Sharpen.Thread.CurrentThread().GetContextClassLoader().GetResource("yarn-site.xml"
					);
				if (url == null)
				{
					throw new RuntimeException("Could not find 'yarn-site.xml' dummy file in classpath"
						);
				}
				Configuration yarnClusterConfig = yarnCluster.GetConfig();
				yarnClusterConfig.Set("yarn.application.classpath", new FilePath(url.AbsolutePath
					).GetParent());
				//write the document to a buffer (not directly to the file, as that
				//can cause the file being written to get read -which will then fail.
				ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
				yarnClusterConfig.WriteXml(bytesOut);
				bytesOut.Close();
				//write the bytes to the file in the classpath
				OutputStream os = new FileOutputStream(new FilePath(url.AbsolutePath));
				os.Write(bytesOut.ToByteArray());
				os.Close();
			}
			FileContext fsContext = FileContext.GetLocalFSFileContext();
			fsContext.Delete(new Path(conf.Get("yarn.timeline-service.leveldb-timeline-store.path"
				)), true);
			try
			{
				Sharpen.Thread.Sleep(2000);
			}
			catch (Exception e)
			{
				Log.Info("setup thread sleep interrupted. message=" + e.Message);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			if (yarnCluster != null)
			{
				try
				{
					yarnCluster.Stop();
				}
				finally
				{
					yarnCluster = null;
				}
			}
			FileContext fsContext = FileContext.GetLocalFSFileContext();
			fsContext.Delete(new Path(conf.Get("yarn.timeline-service.leveldb-timeline-store.path"
				)), true);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestDSShellWithDomain()
		{
			TestDSShell(true);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestDSShellWithoutDomain()
		{
			TestDSShell(false);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestDSShell(bool haveDomain)
		{
			string[] args = new string[] { "--jar", AppmasterJar, "--num_containers", "2", "--shell_command"
				, Shell.Windows ? "dir" : "ls", "--master_memory", "512", "--master_vcores", "2"
				, "--container_memory", "128", "--container_vcores", "1" };
			if (haveDomain)
			{
				string[] domainArgs = new string[] { "--domain", "TEST_DOMAIN", "--view_acls", "reader_user reader_group"
					, "--modify_acls", "writer_user writer_group", "--create" };
				IList<string> argsList = new AList<string>(Arrays.AsList(args));
				Sharpen.Collections.AddAll(argsList, Arrays.AsList(domainArgs));
				args = Sharpen.Collections.ToArray(argsList, new string[argsList.Count]);
			}
			Log.Info("Initializing DS Client");
			Client client = new Client(new Configuration(yarnCluster.GetConfig()));
			bool initSuccess = client.Init(args);
			NUnit.Framework.Assert.IsTrue(initSuccess);
			Log.Info("Running DS Client");
			AtomicBoolean result = new AtomicBoolean(false);
			Sharpen.Thread t = new _Thread_194(result, client);
			t.Start();
			YarnClient yarnClient = YarnClient.CreateYarnClient();
			yarnClient.Init(new Configuration(yarnCluster.GetConfig()));
			yarnClient.Start();
			string hostName = NetUtils.GetHostname();
			bool verified = false;
			string errorMessage = string.Empty;
			while (!verified)
			{
				IList<ApplicationReport> apps = yarnClient.GetApplications();
				if (apps.Count == 0)
				{
					Sharpen.Thread.Sleep(10);
					continue;
				}
				ApplicationReport appReport = apps[0];
				if (appReport.GetHost().Equals("N/A"))
				{
					Sharpen.Thread.Sleep(10);
					continue;
				}
				errorMessage = "Expected host name to start with '" + hostName + "', was '" + appReport
					.GetHost() + "'. Expected rpc port to be '-1', was '" + appReport.GetRpcPort() +
					 "'.";
				if (CheckHostname(appReport.GetHost()) && appReport.GetRpcPort() == -1)
				{
					verified = true;
				}
				if (appReport.GetYarnApplicationState() == YarnApplicationState.Finished)
				{
					break;
				}
			}
			NUnit.Framework.Assert.IsTrue(errorMessage, verified);
			t.Join();
			Log.Info("Client run completed. Result=" + result);
			NUnit.Framework.Assert.IsTrue(result.Get());
			TimelineDomain domain = null;
			if (haveDomain)
			{
				domain = yarnCluster.GetApplicationHistoryServer().GetTimelineStore().GetDomain("TEST_DOMAIN"
					);
				NUnit.Framework.Assert.IsNotNull(domain);
				NUnit.Framework.Assert.AreEqual("reader_user reader_group", domain.GetReaders());
				NUnit.Framework.Assert.AreEqual("writer_user writer_group", domain.GetWriters());
			}
			TimelineEntities entitiesAttempts = yarnCluster.GetApplicationHistoryServer().GetTimelineStore
				().GetEntities(ApplicationMaster.DSEntity.DsAppAttempt.ToString(), null, null, null
				, null, null, null, null, null, null);
			NUnit.Framework.Assert.IsNotNull(entitiesAttempts);
			NUnit.Framework.Assert.AreEqual(1, entitiesAttempts.GetEntities().Count);
			NUnit.Framework.Assert.AreEqual(2, entitiesAttempts.GetEntities()[0].GetEvents().
				Count);
			NUnit.Framework.Assert.AreEqual(entitiesAttempts.GetEntities()[0].GetEntityType()
				.ToString(), ApplicationMaster.DSEntity.DsAppAttempt.ToString());
			if (haveDomain)
			{
				NUnit.Framework.Assert.AreEqual(domain.GetId(), entitiesAttempts.GetEntities()[0]
					.GetDomainId());
			}
			else
			{
				NUnit.Framework.Assert.AreEqual("DEFAULT", entitiesAttempts.GetEntities()[0].GetDomainId
					());
			}
			TimelineEntities entities = yarnCluster.GetApplicationHistoryServer().GetTimelineStore
				().GetEntities(ApplicationMaster.DSEntity.DsContainer.ToString(), null, null, null
				, null, null, null, null, null, null);
			NUnit.Framework.Assert.IsNotNull(entities);
			NUnit.Framework.Assert.AreEqual(2, entities.GetEntities().Count);
			NUnit.Framework.Assert.AreEqual(entities.GetEntities()[0].GetEntityType().ToString
				(), ApplicationMaster.DSEntity.DsContainer.ToString());
			if (haveDomain)
			{
				NUnit.Framework.Assert.AreEqual(domain.GetId(), entities.GetEntities()[0].GetDomainId
					());
			}
			else
			{
				NUnit.Framework.Assert.AreEqual("DEFAULT", entities.GetEntities()[0].GetDomainId(
					));
			}
		}

		private sealed class _Thread_194 : Sharpen.Thread
		{
			public _Thread_194(AtomicBoolean result, Client client)
			{
				this.result = result;
				this.client = client;
			}

			public override void Run()
			{
				try
				{
					result.Set(client.Run());
				}
				catch (Exception e)
				{
					throw new RuntimeException(e);
				}
			}

			private readonly AtomicBoolean result;

			private readonly Client client;
		}

		/*
		* NetUtils.getHostname() returns a string in the form "hostname/ip".
		* Sometimes the hostname we get is the FQDN and sometimes the short name. In
		* addition, on machines with multiple network interfaces, it runs any one of
		* the ips. The function below compares the returns values for
		* NetUtils.getHostname() accounting for the conditions mentioned.
		*/
		/// <exception cref="System.Exception"/>
		private bool CheckHostname(string appHostname)
		{
			string hostname = NetUtils.GetHostname();
			if (hostname.Equals(appHostname))
			{
				return true;
			}
			NUnit.Framework.Assert.IsTrue("Unknown format for hostname " + appHostname, appHostname
				.Contains("/"));
			NUnit.Framework.Assert.IsTrue("Unknown format for hostname " + hostname, hostname
				.Contains("/"));
			string[] appHostnameParts = appHostname.Split("/");
			string[] hostnameParts = hostname.Split("/");
			return (CompareFQDNs(appHostnameParts[0], hostnameParts[0]) && CheckIPs(hostnameParts
				[0], hostnameParts[1], appHostnameParts[1]));
		}

		/// <exception cref="System.Exception"/>
		private bool CompareFQDNs(string appHostname, string hostname)
		{
			if (appHostname.Equals(hostname))
			{
				return true;
			}
			string appFQDN = Sharpen.Extensions.GetAddressByName(appHostname).ToString();
			string localFQDN = Sharpen.Extensions.GetAddressByName(hostname).ToString();
			return appFQDN.Equals(localFQDN);
		}

		/// <exception cref="System.Exception"/>
		private bool CheckIPs(string hostname, string localIP, string appIP)
		{
			if (localIP.Equals(appIP))
			{
				return true;
			}
			bool appIPCheck = false;
			bool localIPCheck = false;
			IPAddress[] addresses = IPAddress.GetAllByName(hostname);
			foreach (IPAddress ia in addresses)
			{
				if (ia.GetHostAddress().Equals(appIP))
				{
					appIPCheck = true;
					continue;
				}
				if (ia.GetHostAddress().Equals(localIP))
				{
					localIPCheck = true;
				}
			}
			return (appIPCheck && localIPCheck);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestDSRestartWithPreviousRunningContainers()
		{
			string[] args = new string[] { "--jar", AppmasterJar, "--num_containers", "1", "--shell_command"
				, "sleep 8", "--master_memory", "512", "--container_memory", "128", "--keep_containers_across_application_attempts"
				 };
			Log.Info("Initializing DS Client");
			Org.Apache.Hadoop.Yarn.Applications.Distributedshell.Client client = new Org.Apache.Hadoop.Yarn.Applications.Distributedshell.Client
				(typeof(TestDSFailedAppMaster).FullName, new Configuration(yarnCluster.GetConfig
				()));
			client.Init(args);
			Log.Info("Running DS Client");
			bool result = client.Run();
			Log.Info("Client run completed. Result=" + result);
			// application should succeed
			NUnit.Framework.Assert.IsTrue(result);
		}

		/*
		* The sleeping period in TestDSSleepingAppMaster is set as 5 seconds.
		* Set attempt_failures_validity_interval as 2.5 seconds. It will check
		* how many attempt failures for previous 2.5 seconds.
		* The application is expected to be successful.
		*/
		/// <exception cref="System.Exception"/>
		public virtual void TestDSAttemptFailuresValidityIntervalSucess()
		{
			string[] args = new string[] { "--jar", AppmasterJar, "--num_containers", "1", "--shell_command"
				, "sleep 8", "--master_memory", "512", "--container_memory", "128", "--attempt_failures_validity_interval"
				, "2500" };
			Log.Info("Initializing DS Client");
			Configuration conf = yarnCluster.GetConfig();
			conf.SetInt(YarnConfiguration.RmAmMaxAttempts, 2);
			Org.Apache.Hadoop.Yarn.Applications.Distributedshell.Client client = new Org.Apache.Hadoop.Yarn.Applications.Distributedshell.Client
				(typeof(TestDSSleepingAppMaster).FullName, new Configuration(conf));
			client.Init(args);
			Log.Info("Running DS Client");
			bool result = client.Run();
			Log.Info("Client run completed. Result=" + result);
			// application should succeed
			NUnit.Framework.Assert.IsTrue(result);
		}

		/*
		* The sleeping period in TestDSSleepingAppMaster is set as 5 seconds.
		* Set attempt_failures_validity_interval as 15 seconds. It will check
		* how many attempt failure for previous 15 seconds.
		* The application is expected to be fail.
		*/
		/// <exception cref="System.Exception"/>
		public virtual void TestDSAttemptFailuresValidityIntervalFailed()
		{
			string[] args = new string[] { "--jar", AppmasterJar, "--num_containers", "1", "--shell_command"
				, "sleep 8", "--master_memory", "512", "--container_memory", "128", "--attempt_failures_validity_interval"
				, "15000" };
			Log.Info("Initializing DS Client");
			Configuration conf = yarnCluster.GetConfig();
			conf.SetInt(YarnConfiguration.RmAmMaxAttempts, 2);
			Org.Apache.Hadoop.Yarn.Applications.Distributedshell.Client client = new Org.Apache.Hadoop.Yarn.Applications.Distributedshell.Client
				(typeof(TestDSSleepingAppMaster).FullName, new Configuration(conf));
			client.Init(args);
			Log.Info("Running DS Client");
			bool result = client.Run();
			Log.Info("Client run completed. Result=" + result);
			// application should be failed
			NUnit.Framework.Assert.IsFalse(result);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestDSShellWithCustomLogPropertyFile()
		{
			FilePath basedir = new FilePath("target", typeof(TestDistributedShell).FullName);
			FilePath tmpDir = new FilePath(basedir, "tmpDir");
			tmpDir.Mkdirs();
			FilePath customLogProperty = new FilePath(tmpDir, "custom_log4j.properties");
			if (customLogProperty.Exists())
			{
				customLogProperty.Delete();
			}
			if (!customLogProperty.CreateNewFile())
			{
				NUnit.Framework.Assert.Fail("Can not create custom log4j property file.");
			}
			PrintWriter fileWriter = new PrintWriter(customLogProperty);
			// set the output to DEBUG level
			fileWriter.Write("log4j.rootLogger=debug,stdout");
			fileWriter.Close();
			string[] args = new string[] { "--jar", AppmasterJar, "--num_containers", "3", "--shell_command"
				, "echo", "--shell_args", "HADOOP", "--log_properties", customLogProperty.GetAbsolutePath
				(), "--master_memory", "512", "--master_vcores", "2", "--container_memory", "128"
				, "--container_vcores", "1" };
			//Before run the DS, the default the log level is INFO
			Log LOG_Client = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Applications.Distributedshell.Client
				));
			NUnit.Framework.Assert.IsTrue(LOG_Client.IsInfoEnabled());
			NUnit.Framework.Assert.IsFalse(LOG_Client.IsDebugEnabled());
			Log LogAm = LogFactory.GetLog(typeof(ApplicationMaster));
			NUnit.Framework.Assert.IsTrue(LogAm.IsInfoEnabled());
			NUnit.Framework.Assert.IsFalse(LogAm.IsDebugEnabled());
			Log.Info("Initializing DS Client");
			Org.Apache.Hadoop.Yarn.Applications.Distributedshell.Client client = new Org.Apache.Hadoop.Yarn.Applications.Distributedshell.Client
				(new Configuration(yarnCluster.GetConfig()));
			bool initSuccess = client.Init(args);
			NUnit.Framework.Assert.IsTrue(initSuccess);
			Log.Info("Running DS Client");
			bool result = client.Run();
			Log.Info("Client run completed. Result=" + result);
			NUnit.Framework.Assert.IsTrue(VerifyContainerLog(3, null, true, "DEBUG") > 10);
			//After DS is finished, the log level should be DEBUG
			NUnit.Framework.Assert.IsTrue(LOG_Client.IsInfoEnabled());
			NUnit.Framework.Assert.IsTrue(LOG_Client.IsDebugEnabled());
			NUnit.Framework.Assert.IsTrue(LogAm.IsInfoEnabled());
			NUnit.Framework.Assert.IsTrue(LogAm.IsDebugEnabled());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestDSShellWithCommands()
		{
			string[] args = new string[] { "--jar", AppmasterJar, "--num_containers", "2", "--shell_command"
				, "\"echo output_ignored;echo output_expected\"", "--master_memory", "512", "--master_vcores"
				, "2", "--container_memory", "128", "--container_vcores", "1" };
			Log.Info("Initializing DS Client");
			Org.Apache.Hadoop.Yarn.Applications.Distributedshell.Client client = new Org.Apache.Hadoop.Yarn.Applications.Distributedshell.Client
				(new Configuration(yarnCluster.GetConfig()));
			bool initSuccess = client.Init(args);
			NUnit.Framework.Assert.IsTrue(initSuccess);
			Log.Info("Running DS Client");
			bool result = client.Run();
			Log.Info("Client run completed. Result=" + result);
			IList<string> expectedContent = new AList<string>();
			expectedContent.AddItem("output_expected");
			VerifyContainerLog(2, expectedContent, false, string.Empty);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestDSShellWithMultipleArgs()
		{
			string[] args = new string[] { "--jar", AppmasterJar, "--num_containers", "4", "--shell_command"
				, "echo", "--shell_args", "HADOOP YARN MAPREDUCE HDFS", "--master_memory", "512"
				, "--master_vcores", "2", "--container_memory", "128", "--container_vcores", "1"
				 };
			Log.Info("Initializing DS Client");
			Org.Apache.Hadoop.Yarn.Applications.Distributedshell.Client client = new Org.Apache.Hadoop.Yarn.Applications.Distributedshell.Client
				(new Configuration(yarnCluster.GetConfig()));
			bool initSuccess = client.Init(args);
			NUnit.Framework.Assert.IsTrue(initSuccess);
			Log.Info("Running DS Client");
			bool result = client.Run();
			Log.Info("Client run completed. Result=" + result);
			IList<string> expectedContent = new AList<string>();
			expectedContent.AddItem("HADOOP YARN MAPREDUCE HDFS");
			VerifyContainerLog(4, expectedContent, false, string.Empty);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestDSShellWithShellScript()
		{
			FilePath basedir = new FilePath("target", typeof(TestDistributedShell).FullName);
			FilePath tmpDir = new FilePath(basedir, "tmpDir");
			tmpDir.Mkdirs();
			FilePath customShellScript = new FilePath(tmpDir, "custom_script.sh");
			if (customShellScript.Exists())
			{
				customShellScript.Delete();
			}
			if (!customShellScript.CreateNewFile())
			{
				NUnit.Framework.Assert.Fail("Can not create custom shell script file.");
			}
			PrintWriter fileWriter = new PrintWriter(customShellScript);
			// set the output to DEBUG level
			fileWriter.Write("echo testDSShellWithShellScript");
			fileWriter.Close();
			System.Console.Out.WriteLine(customShellScript.GetAbsolutePath());
			string[] args = new string[] { "--jar", AppmasterJar, "--num_containers", "1", "--shell_script"
				, customShellScript.GetAbsolutePath(), "--master_memory", "512", "--master_vcores"
				, "2", "--container_memory", "128", "--container_vcores", "1" };
			Log.Info("Initializing DS Client");
			Org.Apache.Hadoop.Yarn.Applications.Distributedshell.Client client = new Org.Apache.Hadoop.Yarn.Applications.Distributedshell.Client
				(new Configuration(yarnCluster.GetConfig()));
			bool initSuccess = client.Init(args);
			NUnit.Framework.Assert.IsTrue(initSuccess);
			Log.Info("Running DS Client");
			bool result = client.Run();
			Log.Info("Client run completed. Result=" + result);
			IList<string> expectedContent = new AList<string>();
			expectedContent.AddItem("testDSShellWithShellScript");
			VerifyContainerLog(1, expectedContent, false, string.Empty);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestDSShellWithInvalidArgs()
		{
			Org.Apache.Hadoop.Yarn.Applications.Distributedshell.Client client = new Org.Apache.Hadoop.Yarn.Applications.Distributedshell.Client
				(new Configuration(yarnCluster.GetConfig()));
			Log.Info("Initializing DS Client with no args");
			try
			{
				client.Init(new string[] {  });
				NUnit.Framework.Assert.Fail("Exception is expected");
			}
			catch (ArgumentException e)
			{
				NUnit.Framework.Assert.IsTrue("The throw exception is not expected", e.Message.Contains
					("No args"));
			}
			Log.Info("Initializing DS Client with no jar file");
			try
			{
				string[] args = new string[] { "--num_containers", "2", "--shell_command", Shell.
					Windows ? "dir" : "ls", "--master_memory", "512", "--container_memory", "128" };
				client.Init(args);
				NUnit.Framework.Assert.Fail("Exception is expected");
			}
			catch (ArgumentException e)
			{
				NUnit.Framework.Assert.IsTrue("The throw exception is not expected", e.Message.Contains
					("No jar"));
			}
			Log.Info("Initializing DS Client with no shell command");
			try
			{
				string[] args = new string[] { "--jar", AppmasterJar, "--num_containers", "2", "--master_memory"
					, "512", "--container_memory", "128" };
				client.Init(args);
				NUnit.Framework.Assert.Fail("Exception is expected");
			}
			catch (ArgumentException e)
			{
				NUnit.Framework.Assert.IsTrue("The throw exception is not expected", e.Message.Contains
					("No shell command"));
			}
			Log.Info("Initializing DS Client with invalid no. of containers");
			try
			{
				string[] args = new string[] { "--jar", AppmasterJar, "--num_containers", "-1", "--shell_command"
					, Shell.Windows ? "dir" : "ls", "--master_memory", "512", "--container_memory", 
					"128" };
				client.Init(args);
				NUnit.Framework.Assert.Fail("Exception is expected");
			}
			catch (ArgumentException e)
			{
				NUnit.Framework.Assert.IsTrue("The throw exception is not expected", e.Message.Contains
					("Invalid no. of containers"));
			}
			Log.Info("Initializing DS Client with invalid no. of vcores");
			try
			{
				string[] args = new string[] { "--jar", AppmasterJar, "--num_containers", "2", "--shell_command"
					, Shell.Windows ? "dir" : "ls", "--master_memory", "512", "--master_vcores", "-2"
					, "--container_memory", "128", "--container_vcores", "1" };
				client.Init(args);
				NUnit.Framework.Assert.Fail("Exception is expected");
			}
			catch (ArgumentException e)
			{
				NUnit.Framework.Assert.IsTrue("The throw exception is not expected", e.Message.Contains
					("Invalid virtual cores specified"));
			}
			Log.Info("Initializing DS Client with --shell_command and --shell_script");
			try
			{
				string[] args = new string[] { "--jar", AppmasterJar, "--num_containers", "2", "--shell_command"
					, Shell.Windows ? "dir" : "ls", "--master_memory", "512", "--master_vcores", "2"
					, "--container_memory", "128", "--container_vcores", "1", "--shell_script", "test.sh"
					 };
				client.Init(args);
				NUnit.Framework.Assert.Fail("Exception is expected");
			}
			catch (ArgumentException e)
			{
				NUnit.Framework.Assert.IsTrue("The throw exception is not expected", e.Message.Contains
					("Can not specify shell_command option " + "and shell_script option at the same time"
					));
			}
			Log.Info("Initializing DS Client without --shell_command and --shell_script");
			try
			{
				string[] args = new string[] { "--jar", AppmasterJar, "--num_containers", "2", "--master_memory"
					, "512", "--master_vcores", "2", "--container_memory", "128", "--container_vcores"
					, "1" };
				client.Init(args);
				NUnit.Framework.Assert.Fail("Exception is expected");
			}
			catch (ArgumentException e)
			{
				NUnit.Framework.Assert.IsTrue("The throw exception is not expected", e.Message.Contains
					("No shell command or shell script specified " + "to be executed by application master"
					));
			}
		}

		/// <exception cref="System.Exception"/>
		protected internal virtual void WaitForNMsToRegister()
		{
			int sec = 60;
			while (sec >= 0)
			{
				if (yarnCluster.GetResourceManager().GetRMContext().GetRMNodes().Count >= NumNms)
				{
					break;
				}
				Sharpen.Thread.Sleep(1000);
				sec--;
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestContainerLaunchFailureHandling()
		{
			string[] args = new string[] { "--jar", AppmasterJar, "--num_containers", "2", "--shell_command"
				, Shell.Windows ? "dir" : "ls", "--master_memory", "512", "--container_memory", 
				"128" };
			Log.Info("Initializing DS Client");
			Org.Apache.Hadoop.Yarn.Applications.Distributedshell.Client client = new Org.Apache.Hadoop.Yarn.Applications.Distributedshell.Client
				(typeof(ContainerLaunchFailAppMaster).FullName, new Configuration(yarnCluster.GetConfig
				()));
			bool initSuccess = client.Init(args);
			NUnit.Framework.Assert.IsTrue(initSuccess);
			Log.Info("Running DS Client");
			bool result = client.Run();
			Log.Info("Client run completed. Result=" + result);
			NUnit.Framework.Assert.IsFalse(result);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestDebugFlag()
		{
			string[] args = new string[] { "--jar", AppmasterJar, "--num_containers", "2", "--shell_command"
				, Shell.Windows ? "dir" : "ls", "--master_memory", "512", "--master_vcores", "2"
				, "--container_memory", "128", "--container_vcores", "1", "--debug" };
			Log.Info("Initializing DS Client");
			Org.Apache.Hadoop.Yarn.Applications.Distributedshell.Client client = new Org.Apache.Hadoop.Yarn.Applications.Distributedshell.Client
				(new Configuration(yarnCluster.GetConfig()));
			NUnit.Framework.Assert.IsTrue(client.Init(args));
			Log.Info("Running DS Client");
			NUnit.Framework.Assert.IsTrue(client.Run());
		}

		private int VerifyContainerLog(int containerNum, IList<string> expectedContent, bool
			 count, string expectedWord)
		{
			FilePath logFolder = new FilePath(yarnCluster.GetNodeManager(0).GetConfig().Get(YarnConfiguration
				.NmLogDirs, YarnConfiguration.DefaultNmLogDirs));
			FilePath[] listOfFiles = logFolder.ListFiles();
			int currentContainerLogFileIndex = -1;
			for (int i = listOfFiles.Length - 1; i >= 0; i--)
			{
				if (listOfFiles[i].ListFiles().Length == containerNum + 1)
				{
					currentContainerLogFileIndex = i;
					break;
				}
			}
			NUnit.Framework.Assert.IsTrue(currentContainerLogFileIndex != -1);
			FilePath[] containerFiles = listOfFiles[currentContainerLogFileIndex].ListFiles();
			int numOfWords = 0;
			for (int i_1 = 0; i_1 < containerFiles.Length; i_1++)
			{
				foreach (FilePath output in containerFiles[i_1].ListFiles())
				{
					if (output.GetName().Trim().Contains("stdout"))
					{
						BufferedReader br = null;
						IList<string> stdOutContent = new AList<string>();
						try
						{
							string sCurrentLine;
							br = new BufferedReader(new FileReader(output));
							int numOfline = 0;
							while ((sCurrentLine = br.ReadLine()) != null)
							{
								if (count)
								{
									if (sCurrentLine.Contains(expectedWord))
									{
										numOfWords++;
									}
								}
								else
								{
									if (output.GetName().Trim().Equals("stdout"))
									{
										if (!Shell.Windows)
										{
											NUnit.Framework.Assert.AreEqual("The current is" + sCurrentLine, expectedContent[
												numOfline], sCurrentLine.Trim());
											numOfline++;
										}
										else
										{
											stdOutContent.AddItem(sCurrentLine.Trim());
										}
									}
								}
							}
							/* By executing bat script using cmd /c,
							* it will output all contents from bat script first
							* It is hard for us to do check line by line
							* Simply check whether output from bat file contains
							* all the expected messages
							*/
							if (Shell.Windows && !count && output.GetName().Trim().Equals("stdout"))
							{
								NUnit.Framework.Assert.IsTrue(stdOutContent.ContainsAll(expectedContent));
							}
						}
						catch (IOException e)
						{
							Sharpen.Runtime.PrintStackTrace(e);
						}
						finally
						{
							try
							{
								if (br != null)
								{
									br.Close();
								}
							}
							catch (IOException ex)
							{
								Sharpen.Runtime.PrintStackTrace(ex);
							}
						}
					}
				}
			}
			return numOfWords;
		}
	}
}
