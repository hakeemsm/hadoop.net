using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Org.Apache.Hadoop.Yarn.Security;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Metrics;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Recovery;
using Org.Apache.Hadoop.Yarn.Server.Security;
using Org.Apache.Hadoop.Yarn.Server.Utils;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp
{
	public class TestNMWebServer
	{
		private static readonly FilePath testRootDir = new FilePath("target", typeof(TestNMWebServer
			).Name);

		private static FilePath testLogDir = new FilePath("target", typeof(TestNMWebServer
			).Name + "LogDir");

		[SetUp]
		public virtual void Setup()
		{
			testRootDir.Mkdirs();
			testLogDir.Mkdir();
		}

		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			FileUtil.FullyDelete(testRootDir);
			FileUtil.FullyDelete(testLogDir);
		}

		private int StartNMWebAppServer(string webAddr)
		{
			Context nmContext = new NodeManager.NMContext(null, null, null, null, null);
			ResourceView resourceView = new _ResourceView_84();
			Configuration conf = new Configuration();
			conf.Set(YarnConfiguration.NmLocalDirs, testRootDir.GetAbsolutePath());
			conf.Set(YarnConfiguration.NmLogDirs, testLogDir.GetAbsolutePath());
			NodeHealthCheckerService healthChecker = new NodeHealthCheckerService();
			healthChecker.Init(conf);
			LocalDirsHandlerService dirsHandler = healthChecker.GetDiskHandler();
			conf.Set(YarnConfiguration.NmWebappAddress, webAddr);
			WebServer server = new WebServer(nmContext, resourceView, new ApplicationACLsManager
				(conf), dirsHandler);
			try
			{
				server.Init(conf);
				server.Start();
				return server.GetPort();
			}
			finally
			{
				server.Stop();
				healthChecker.Stop();
			}
		}

		private sealed class _ResourceView_84 : ResourceView
		{
			public _ResourceView_84()
			{
			}

			public long GetVmemAllocatedForContainers()
			{
				return 0;
			}

			public long GetPmemAllocatedForContainers()
			{
				return 0;
			}

			public long GetVCoresAllocatedForContainers()
			{
				return 0;
			}

			public bool IsVmemCheckEnabled()
			{
				return true;
			}

			public bool IsPmemCheckEnabled()
			{
				return true;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestNMWebAppWithOutPort()
		{
			int port = StartNMWebAppServer("0.0.0.0");
			ValidatePortVal(port);
		}

		private void ValidatePortVal(int portVal)
		{
			NUnit.Framework.Assert.IsTrue("Port is not updated", portVal > 0);
			NUnit.Framework.Assert.IsTrue("Port is default " + YarnConfiguration.DefaultNmPort
				, portVal != YarnConfiguration.DefaultNmPort);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestNMWebAppWithEphemeralPort()
		{
			int port = StartNMWebAppServer("0.0.0.0:0");
			ValidatePortVal(port);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		[NUnit.Framework.Test]
		public virtual void TestNMWebApp()
		{
			Context nmContext = new NodeManager.NMContext(null, null, null, null, null);
			ResourceView resourceView = new _ResourceView_147();
			Configuration conf = new Configuration();
			conf.Set(YarnConfiguration.NmLocalDirs, testRootDir.GetAbsolutePath());
			conf.Set(YarnConfiguration.NmLogDirs, testLogDir.GetAbsolutePath());
			NodeHealthCheckerService healthChecker = new NodeHealthCheckerService();
			healthChecker.Init(conf);
			LocalDirsHandlerService dirsHandler = healthChecker.GetDiskHandler();
			WebServer server = new WebServer(nmContext, resourceView, new ApplicationACLsManager
				(conf), dirsHandler);
			server.Init(conf);
			server.Start();
			// Add an application and the corresponding containers
			RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory(conf);
			Dispatcher dispatcher = new AsyncDispatcher();
			string user = "nobody";
			long clusterTimeStamp = 1234;
			ApplicationId appId = BuilderUtils.NewApplicationId(recordFactory, clusterTimeStamp
				, 1);
			Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
				 app = Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
				>();
			Org.Mockito.Mockito.When(app.GetUser()).ThenReturn(user);
			Org.Mockito.Mockito.When(app.GetAppId()).ThenReturn(appId);
			nmContext.GetApplications()[appId] = app;
			ApplicationAttemptId appAttemptId = BuilderUtils.NewApplicationAttemptId(appId, 1
				);
			ContainerId container1 = BuilderUtils.NewContainerId(recordFactory, appId, appAttemptId
				, 0);
			ContainerId container2 = BuilderUtils.NewContainerId(recordFactory, appId, appAttemptId
				, 1);
			NodeManagerMetrics metrics = Org.Mockito.Mockito.Mock<NodeManagerMetrics>();
			NMStateStoreService stateStore = new NMNullStateStoreService();
			foreach (ContainerId containerId in new ContainerId[] { container1, container2 })
			{
				// TODO: Use builder utils
				ContainerLaunchContext launchContext = recordFactory.NewRecordInstance<ContainerLaunchContext
					>();
				long currentTime = Runtime.CurrentTimeMillis();
				Token containerToken = BuilderUtils.NewContainerToken(containerId, "127.0.0.1", 1234
					, user, BuilderUtils.NewResource(1024, 1), currentTime + 10000L, 123, Sharpen.Runtime.GetBytesForString
					("password"), currentTime);
				Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container container
					 = new _ContainerImpl_214(conf, dispatcher, stateStore, launchContext, null, metrics
					, BuilderUtils.NewContainerTokenIdentifier(containerToken));
				nmContext.GetContainers()[containerId] = container;
				//TODO: Gross hack. Fix in code.
				ApplicationId applicationId = containerId.GetApplicationAttemptId().GetApplicationId
					();
				nmContext.GetApplications()[applicationId].GetContainers()[containerId] = container;
				WriteContainerLogs(nmContext, containerId, dirsHandler);
			}
		}

		private sealed class _ResourceView_147 : ResourceView
		{
			public _ResourceView_147()
			{
			}

			public long GetVmemAllocatedForContainers()
			{
				return 0;
			}

			public long GetPmemAllocatedForContainers()
			{
				return 0;
			}

			public long GetVCoresAllocatedForContainers()
			{
				return 0;
			}

			public bool IsVmemCheckEnabled()
			{
				return true;
			}

			public bool IsPmemCheckEnabled()
			{
				return true;
			}
		}

		private sealed class _ContainerImpl_214 : ContainerImpl
		{
			public _ContainerImpl_214(Configuration baseArg1, Dispatcher baseArg2, NMStateStoreService
				 baseArg3, ContainerLaunchContext baseArg4, Credentials baseArg5, NodeManagerMetrics
				 baseArg6, ContainerTokenIdentifier baseArg7)
				: base(baseArg1, baseArg2, baseArg3, baseArg4, baseArg5, baseArg6, baseArg7)
			{
			}

			public override ContainerState GetContainerState()
			{
				return ContainerState.Running;
			}
		}

		// TODO: Pull logs and test contents.
		//    Thread.sleep(1000000);
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		private void WriteContainerLogs(Context nmContext, ContainerId containerId, LocalDirsHandlerService
			 dirsHandler)
		{
			// ContainerLogDir should be created
			FilePath containerLogDir = ContainerLogsUtils.GetContainerLogDirs(containerId, dirsHandler
				)[0];
			containerLogDir.Mkdirs();
			foreach (string fileType in new string[] { "stdout", "stderr", "syslog" })
			{
				TextWriter writer = new FileWriter(new FilePath(containerLogDir, fileType));
				writer.Write(ConverterUtils.ToString(containerId) + "\n Hello " + fileType + "!");
				writer.Close();
			}
		}
	}
}
