using System.Collections.Generic;
using System.IO;
using Com.Google.Inject;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO.Nativeio;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Launcher;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Recovery;
using Org.Apache.Hadoop.Yarn.Server.Security;
using Org.Apache.Hadoop.Yarn.Server.Utils;
using Org.Apache.Hadoop.Yarn.Webapp;
using Org.Apache.Hadoop.Yarn.Webapp.Test;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp
{
	public class TestContainerLogsPage
	{
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		public virtual void TestContainerLogDirs()
		{
			FilePath absLogDir = new FilePath("target", typeof(TestNMWebServer).Name + "LogDir"
				).GetAbsoluteFile();
			string logdirwithFile = absLogDir.ToURI().ToString();
			Configuration conf = new Configuration();
			conf.Set(YarnConfiguration.NmLogDirs, logdirwithFile);
			NodeHealthCheckerService healthChecker = new NodeHealthCheckerService();
			healthChecker.Init(conf);
			LocalDirsHandlerService dirsHandler = healthChecker.GetDiskHandler();
			NodeManager.NMContext nmContext = new NodeManager.NMContext(null, null, dirsHandler
				, new ApplicationACLsManager(conf), new NMNullStateStoreService());
			// Add an application and the corresponding containers
			RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory(conf);
			string user = "nobody";
			long clusterTimeStamp = 1234;
			ApplicationId appId = BuilderUtils.NewApplicationId(recordFactory, clusterTimeStamp
				, 1);
			Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
				 app = Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
				>();
			Org.Mockito.Mockito.When(app.GetUser()).ThenReturn(user);
			Org.Mockito.Mockito.When(app.GetAppId()).ThenReturn(appId);
			ApplicationAttemptId appAttemptId = BuilderUtils.NewApplicationAttemptId(appId, 1
				);
			ContainerId container1 = BuilderUtils.NewContainerId(recordFactory, appId, appAttemptId
				, 0);
			nmContext.GetApplications()[appId] = app;
			MockContainer container = new MockContainer(appAttemptId, new AsyncDispatcher(), 
				conf, user, appId, 1);
			container.SetState(ContainerState.Running);
			nmContext.GetContainers()[container1] = container;
			IList<FilePath> files = null;
			files = ContainerLogsUtils.GetContainerLogDirs(container1, user, nmContext);
			NUnit.Framework.Assert.IsTrue(!(files[0].ToString().Contains("file:")));
			// After container is completed, it is removed from nmContext
			Sharpen.Collections.Remove(nmContext.GetContainers(), container1);
			NUnit.Framework.Assert.IsNull(nmContext.GetContainers()[container1]);
			files = ContainerLogsUtils.GetContainerLogDirs(container1, user, nmContext);
			NUnit.Framework.Assert.IsTrue(!(files[0].ToString().Contains("file:")));
			// Create a new context to check if correct container log dirs are fetched
			// on full disk.
			LocalDirsHandlerService dirsHandlerForFullDisk = Org.Mockito.Mockito.Spy(dirsHandler
				);
			// good log dirs are empty and nm log dir is in the full log dir list.
			Org.Mockito.Mockito.When(dirsHandlerForFullDisk.GetLogDirs()).ThenReturn(new AList
				<string>());
			Org.Mockito.Mockito.When(dirsHandlerForFullDisk.GetLogDirsForRead()).ThenReturn(Arrays
				.AsList(new string[] { absLogDir.GetAbsolutePath() }));
			nmContext = new NodeManager.NMContext(null, null, dirsHandlerForFullDisk, new ApplicationACLsManager
				(conf), new NMNullStateStoreService());
			nmContext.GetApplications()[appId] = app;
			container.SetState(ContainerState.Running);
			nmContext.GetContainers()[container1] = container;
			IList<FilePath> dirs = ContainerLogsUtils.GetContainerLogDirs(container1, user, nmContext
				);
			FilePath containerLogDir = new FilePath(absLogDir, appId + "/" + container1);
			NUnit.Framework.Assert.IsTrue(dirs.Contains(containerLogDir));
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		public virtual void TestContainerLogFile()
		{
			FilePath absLogDir = new FilePath("target", typeof(TestNMWebServer).Name + "LogDir"
				).GetAbsoluteFile();
			string logdirwithFile = absLogDir.ToURI().ToString();
			Configuration conf = new Configuration();
			conf.Set(YarnConfiguration.NmLogDirs, logdirwithFile);
			conf.SetFloat(YarnConfiguration.NmMaxPerDiskUtilizationPercentage, 0.0f);
			LocalDirsHandlerService dirsHandler = new LocalDirsHandlerService();
			dirsHandler.Init(conf);
			NodeManager.NMContext nmContext = new NodeManager.NMContext(null, null, dirsHandler
				, new ApplicationACLsManager(conf), new NMNullStateStoreService());
			// Add an application and the corresponding containers
			string user = "nobody";
			long clusterTimeStamp = 1234;
			ApplicationId appId = BuilderUtils.NewApplicationId(clusterTimeStamp, 1);
			Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
				 app = Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
				>();
			Org.Mockito.Mockito.When(app.GetUser()).ThenReturn(user);
			Org.Mockito.Mockito.When(app.GetAppId()).ThenReturn(appId);
			ApplicationAttemptId appAttemptId = BuilderUtils.NewApplicationAttemptId(appId, 1
				);
			ContainerId containerId = BuilderUtils.NewContainerId(appAttemptId, 1);
			nmContext.GetApplications()[appId] = app;
			MockContainer container = new MockContainer(appAttemptId, new AsyncDispatcher(), 
				conf, user, appId, 1);
			container.SetState(ContainerState.Running);
			nmContext.GetContainers()[containerId] = container;
			FilePath containerLogDir = new FilePath(absLogDir, ContainerLaunch.GetRelativeContainerLogDir
				(appId.ToString(), containerId.ToString()));
			containerLogDir.Mkdirs();
			string fileName = "fileName";
			FilePath containerLogFile = new FilePath(containerLogDir, fileName);
			containerLogFile.CreateNewFile();
			FilePath file = ContainerLogsUtils.GetContainerLogFile(containerId, fileName, user
				, nmContext);
			NUnit.Framework.Assert.AreEqual(containerLogFile.ToURI().ToString(), file.ToURI()
				.ToString());
			FileUtil.FullyDelete(absLogDir);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestContainerLogPageAccess()
		{
			// SecureIOUtils require Native IO to be enabled. This test will run
			// only if it is enabled.
			Assume.AssumeTrue(NativeIO.IsAvailable());
			string user = "randomUser" + Runtime.CurrentTimeMillis();
			FilePath absLogDir = null;
			FilePath appDir = null;
			FilePath containerDir = null;
			FilePath syslog = null;
			try
			{
				// target log directory
				absLogDir = new FilePath("target", typeof(TestContainerLogsPage).Name + "LogDir")
					.GetAbsoluteFile();
				absLogDir.Mkdir();
				Configuration conf = new Configuration();
				conf.Set(YarnConfiguration.NmLogDirs, absLogDir.ToURI().ToString());
				conf.Set(CommonConfigurationKeysPublic.HadoopSecurityAuthentication, "kerberos");
				UserGroupInformation.SetConfiguration(conf);
				NodeHealthCheckerService healthChecker = new NodeHealthCheckerService();
				healthChecker.Init(conf);
				LocalDirsHandlerService dirsHandler = healthChecker.GetDiskHandler();
				// Add an application and the corresponding containers
				RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory(conf);
				long clusterTimeStamp = 1234;
				ApplicationId appId = BuilderUtils.NewApplicationId(recordFactory, clusterTimeStamp
					, 1);
				Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
					 app = Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
					>();
				Org.Mockito.Mockito.When(app.GetAppId()).ThenReturn(appId);
				// Making sure that application returns a random user. This is required
				// for SecureIOUtils' file owner check.
				Org.Mockito.Mockito.When(app.GetUser()).ThenReturn(user);
				ApplicationAttemptId appAttemptId = BuilderUtils.NewApplicationAttemptId(appId, 1
					);
				ContainerId container1 = BuilderUtils.NewContainerId(recordFactory, appId, appAttemptId
					, 0);
				// Testing secure read access for log files
				// Creating application and container directory and syslog file.
				appDir = new FilePath(absLogDir, appId.ToString());
				appDir.Mkdir();
				containerDir = new FilePath(appDir, container1.ToString());
				containerDir.Mkdir();
				syslog = new FilePath(containerDir, "syslog");
				syslog.CreateNewFile();
				BufferedOutputStream @out = new BufferedOutputStream(new FileOutputStream(syslog)
					);
				@out.Write(Sharpen.Runtime.GetBytesForString("Log file Content"));
				@out.Close();
				Context context = Org.Mockito.Mockito.Mock<Context>();
				ConcurrentMap<ApplicationId, Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
					> appMap = new ConcurrentHashMap<ApplicationId, Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
					>();
				appMap[appId] = app;
				Org.Mockito.Mockito.When(context.GetApplications()).ThenReturn(appMap);
				ConcurrentHashMap<ContainerId, Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
					> containers = new ConcurrentHashMap<ContainerId, Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
					>();
				Org.Mockito.Mockito.When(context.GetContainers()).ThenReturn(containers);
				Org.Mockito.Mockito.When(context.GetLocalDirsHandler()).ThenReturn(dirsHandler);
				MockContainer container = new MockContainer(appAttemptId, new AsyncDispatcher(), 
					conf, user, appId, 1);
				container.SetState(ContainerState.Running);
				context.GetContainers()[container1] = container;
				ContainerLogsPage.ContainersLogsBlock cLogsBlock = new ContainerLogsPage.ContainersLogsBlock
					(context);
				IDictionary<string, string> @params = new Dictionary<string, string>();
				@params[YarnWebParams.ContainerId] = container1.ToString();
				@params[YarnWebParams.ContainerLogType] = "syslog";
				Injector injector = WebAppTests.TestPage<ContainerLogsPage.ContainersLogsBlock>(typeof(
					ContainerLogsPage), cLogsBlock, @params, (Module[])null);
				PrintWriter spyPw = WebAppTests.GetPrintWriter(injector);
				Org.Mockito.Mockito.Verify(spyPw).Write("Exception reading log file. Application submitted by '"
					 + user + "' doesn't own requested log file : syslog");
			}
			finally
			{
				if (syslog != null)
				{
					syslog.Delete();
				}
				if (containerDir != null)
				{
					containerDir.Delete();
				}
				if (appDir != null)
				{
					appDir.Delete();
				}
				if (absLogDir != null)
				{
					absLogDir.Delete();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestLogDirWithDriveLetter()
		{
			//To verify that logs paths which include drive letters (Windows)
			//do not lose their drive letter specification
			LocalDirsHandlerService localDirs = Org.Mockito.Mockito.Mock<LocalDirsHandlerService
				>();
			IList<string> logDirs = new AList<string>();
			logDirs.AddItem("F:/nmlogs");
			Org.Mockito.Mockito.When(localDirs.GetLogDirsForRead()).ThenReturn(logDirs);
			ApplicationIdPBImpl appId = Org.Mockito.Mockito.Mock<ApplicationIdPBImpl>();
			Org.Mockito.Mockito.When(appId.ToString()).ThenReturn("app_id_1");
			ApplicationAttemptIdPBImpl appAttemptId = Org.Mockito.Mockito.Mock<ApplicationAttemptIdPBImpl
				>();
			Org.Mockito.Mockito.When(appAttemptId.GetApplicationId()).ThenReturn(appId);
			ContainerId containerId = Org.Mockito.Mockito.Mock<ContainerIdPBImpl>();
			Org.Mockito.Mockito.When(containerId.GetApplicationAttemptId()).ThenReturn(appAttemptId
				);
			IList<FilePath> logDirFiles = ContainerLogsUtils.GetContainerLogDirs(containerId, 
				localDirs);
			NUnit.Framework.Assert.IsTrue("logDir lost drive letter " + logDirFiles[0], logDirFiles
				[0].ToString().IndexOf("F:" + FilePath.separator + "nmlogs") > -1);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestLogFileWithDriveLetter()
		{
			ContainerImpl container = Org.Mockito.Mockito.Mock<ContainerImpl>();
			ApplicationIdPBImpl appId = Org.Mockito.Mockito.Mock<ApplicationIdPBImpl>();
			Org.Mockito.Mockito.When(appId.ToString()).ThenReturn("appId");
			Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
				 app = Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
				>();
			Org.Mockito.Mockito.When(app.GetAppId()).ThenReturn(appId);
			ApplicationAttemptIdPBImpl appAttemptId = Org.Mockito.Mockito.Mock<ApplicationAttemptIdPBImpl
				>();
			Org.Mockito.Mockito.When(appAttemptId.GetApplicationId()).ThenReturn(appId);
			ConcurrentMap<ApplicationId, Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
				> applications = new ConcurrentHashMap<ApplicationId, Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
				>();
			applications[appId] = app;
			ContainerId containerId = Org.Mockito.Mockito.Mock<ContainerIdPBImpl>();
			Org.Mockito.Mockito.When(containerId.ToString()).ThenReturn("containerId");
			Org.Mockito.Mockito.When(containerId.GetApplicationAttemptId()).ThenReturn(appAttemptId
				);
			ConcurrentMap<ContainerId, Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
				> containers = new ConcurrentHashMap<ContainerId, Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
				>();
			containers[containerId] = container;
			LocalDirsHandlerService localDirs = Org.Mockito.Mockito.Mock<LocalDirsHandlerService
				>();
			Org.Mockito.Mockito.When(localDirs.GetLogPathToRead("appId" + Path.Separator + "containerId"
				 + Path.Separator + "fileName")).ThenReturn(new Path("F:/nmlogs/appId/containerId/fileName"
				));
			NodeManager.NMContext context = Org.Mockito.Mockito.Mock<NodeManager.NMContext>();
			Org.Mockito.Mockito.When(context.GetLocalDirsHandler()).ThenReturn(localDirs);
			Org.Mockito.Mockito.When(context.GetApplications()).ThenReturn(applications);
			Org.Mockito.Mockito.When(context.GetContainers()).ThenReturn(containers);
			FilePath logFile = ContainerLogsUtils.GetContainerLogFile(containerId, "fileName"
				, null, context);
			NUnit.Framework.Assert.IsTrue("logFile lost drive letter " + logFile, logFile.ToString
				().IndexOf("F:" + FilePath.separator + "nmlogs") > -1);
		}
	}
}
