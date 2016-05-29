using System;
using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Client;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Server;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Applications.Unmanagedamlauncher
{
	public class TestUnmanagedAMLauncher
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestUnmanagedAMLauncher
			));

		protected internal static MiniYARNCluster yarnCluster = null;

		protected internal static Configuration conf = new YarnConfiguration();

		/// <exception cref="System.Exception"/>
		/// <exception cref="System.IO.IOException"/>
		[BeforeClass]
		public static void Setup()
		{
			Log.Info("Starting up YARN cluster");
			conf.SetInt(YarnConfiguration.RmSchedulerMinimumAllocationMb, 128);
			if (yarnCluster == null)
			{
				yarnCluster = new MiniYARNCluster(typeof(TestUnmanagedAMLauncher).Name, 1, 1, 1);
				yarnCluster.Init(conf);
				yarnCluster.Start();
				//get the address
				Configuration yarnClusterConfig = yarnCluster.GetConfig();
				Log.Info("MiniYARN ResourceManager published address: " + yarnClusterConfig.Get(YarnConfiguration
					.RmAddress));
				Log.Info("MiniYARN ResourceManager published web address: " + yarnClusterConfig.Get
					(YarnConfiguration.RmWebappAddress));
				string webapp = yarnClusterConfig.Get(YarnConfiguration.RmWebappAddress);
				NUnit.Framework.Assert.IsTrue("Web app address still unbound to a host at " + webapp
					, !webapp.StartsWith("0.0.0.0"));
				Log.Info("Yarn webapp is at " + webapp);
				Uri url = Sharpen.Thread.CurrentThread().GetContextClassLoader().GetResource("yarn-site.xml"
					);
				if (url == null)
				{
					throw new RuntimeException("Could not find 'yarn-site.xml' dummy file in classpath"
						);
				}
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
		[AfterClass]
		public static void TearDown()
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
		}

		private static string GetTestRuntimeClasspath()
		{
			Log.Info("Trying to generate classpath for app master from current thread's classpath"
				);
			string envClassPath = string.Empty;
			string cp = Runtime.GetProperty("java.class.path");
			if (cp != null)
			{
				envClassPath += cp.Trim() + FilePath.pathSeparator;
			}
			// yarn-site.xml at this location contains proper config for mini cluster
			ClassLoader thisClassLoader = Sharpen.Thread.CurrentThread().GetContextClassLoader
				();
			Uri url = thisClassLoader.GetResource("yarn-site.xml");
			envClassPath += new FilePath(url.GetFile()).GetParent();
			return envClassPath;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestUMALauncher()
		{
			string classpath = GetTestRuntimeClasspath();
			string javaHome = Runtime.Getenv("JAVA_HOME");
			if (javaHome == null)
			{
				Log.Fatal("JAVA_HOME not defined. Test not running.");
				return;
			}
			string[] args = new string[] { "--classpath", classpath, "--queue", "default", "--cmd"
				, javaHome + "/bin/java -Xmx512m " + typeof(TestUnmanagedAMLauncher).GetCanonicalName
				() + " success" };
			Log.Info("Initializing Launcher");
			UnmanagedAMLauncher launcher = new _UnmanagedAMLauncher_147(new Configuration(yarnCluster
				.GetConfig()));
			bool initSuccess = launcher.Init(args);
			NUnit.Framework.Assert.IsTrue(initSuccess);
			Log.Info("Running Launcher");
			bool result = launcher.Run();
			Log.Info("Launcher run completed. Result=" + result);
			NUnit.Framework.Assert.IsTrue(result);
		}

		private sealed class _UnmanagedAMLauncher_147 : UnmanagedAMLauncher
		{
			public _UnmanagedAMLauncher_147(Configuration baseArg1)
				: base(baseArg1)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
			public override void LaunchAM(ApplicationAttemptId attemptId)
			{
				YarnApplicationAttemptState attemptState = this.rmClient.GetApplicationAttemptReport
					(attemptId).GetYarnApplicationAttemptState();
				NUnit.Framework.Assert.IsTrue(attemptState.Equals(YarnApplicationAttemptState.Launched
					));
				base.LaunchAM(attemptId);
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestUMALauncherError()
		{
			string classpath = GetTestRuntimeClasspath();
			string javaHome = Runtime.Getenv("JAVA_HOME");
			if (javaHome == null)
			{
				Log.Fatal("JAVA_HOME not defined. Test not running.");
				return;
			}
			string[] args = new string[] { "--classpath", classpath, "--queue", "default", "--cmd"
				, javaHome + "/bin/java -Xmx512m " + typeof(TestUnmanagedAMLauncher).GetCanonicalName
				() + " failure" };
			Log.Info("Initializing Launcher");
			UnmanagedAMLauncher launcher = new UnmanagedAMLauncher(new Configuration(yarnCluster
				.GetConfig()));
			bool initSuccess = launcher.Init(args);
			NUnit.Framework.Assert.IsTrue(initSuccess);
			Log.Info("Running Launcher");
			try
			{
				launcher.Run();
				NUnit.Framework.Assert.Fail("Expected an exception to occur as launch should have failed"
					);
			}
			catch (RuntimeException)
			{
			}
		}

		// Expected
		// provide main method so this class can act as AM
		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			if (args[0].Equals("success"))
			{
				ApplicationMasterProtocol client = ClientRMProxy.CreateRMProxy<ApplicationMasterProtocol
					>(conf);
				client.RegisterApplicationMaster(RegisterApplicationMasterRequest.NewInstance(NetUtils
					.GetHostname(), -1, string.Empty));
				Sharpen.Thread.Sleep(1000);
				FinishApplicationMasterResponse resp = client.FinishApplicationMaster(FinishApplicationMasterRequest
					.NewInstance(FinalApplicationStatus.Succeeded, "success", null));
				NUnit.Framework.Assert.IsTrue(resp.GetIsUnregistered());
				System.Environment.Exit(0);
			}
			else
			{
				System.Environment.Exit(1);
			}
		}
	}
}
