using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Text;
using Com.Google.Common.Base;
using NUnit.Framework;
using Org.Apache.Commons.Lang;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Test;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Org.Apache.Hadoop.Yarn.Logaggregation;
using Org.Apache.Hadoop.Yarn.Security.Client;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Loghandler;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Loghandler.Event;
using Org.Apache.Hadoop.Yarn.Server.Utils;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Mockito;
using Org.Mortbay.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Logaggregation
{
	public class TestLogAggregationService : BaseContainerManagerTest
	{
		private IDictionary<ApplicationAccessType, string> acls;

		static TestLogAggregationService()
		{
			acls = CreateAppAcls();
			//@Ignore
			Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Logaggregation.TestLogAggregationService
				));
		}

		private static RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory
			(null);

		private FilePath remoteRootLogDir = new FilePath("target", this.GetType().FullName
			 + "-remoteLogDir");

		/// <exception cref="Org.Apache.Hadoop.FS.UnsupportedFileSystemException"/>
		public TestLogAggregationService()
			: base()
		{
			acls = CreateAppAcls();
			this.remoteRootLogDir.Mkdir();
		}

		internal DrainDispatcher dispatcher;

		internal EventHandler<ApplicationEvent> appEventHandler;

		/// <exception cref="System.IO.IOException"/>
		public override void Setup()
		{
			base.Setup();
			NodeId nodeId = NodeId.NewInstance("0.0.0.0", 5555);
			((NodeManager.NMContext)context).SetNodeId(nodeId);
			dispatcher = CreateDispatcher();
			appEventHandler = Org.Mockito.Mockito.Mock<EventHandler>();
			dispatcher.Register(typeof(ApplicationEventType), appEventHandler);
			UserGroupInformation.SetConfiguration(conf);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override void TearDown()
		{
			base.TearDown();
			CreateContainerExecutor().DeleteAsUser(user, new Path(remoteRootLogDir.GetAbsolutePath
				()), new Path[] {  });
			dispatcher.Await();
			dispatcher.Stop();
			dispatcher.Close();
		}

		/// <exception cref="System.Exception"/>
		private void VerifyLocalFileDeletion(LogAggregationService logAggregationService)
		{
			logAggregationService.Init(this.conf);
			logAggregationService.Start();
			ApplicationId application1 = BuilderUtils.NewApplicationId(1234, 1);
			// AppLogDir should be created
			FilePath app1LogDir = new FilePath(localLogDir, ConverterUtils.ToString(application1
				));
			app1LogDir.Mkdir();
			logAggregationService.Handle(new LogHandlerAppStartedEvent(application1, this.user
				, null, ContainerLogsRetentionPolicy.AllContainers, this.acls));
			ApplicationAttemptId appAttemptId = BuilderUtils.NewApplicationAttemptId(application1
				, 1);
			ContainerId container11 = BuilderUtils.NewContainerId(appAttemptId, 1);
			// Simulate log-file creation
			WriteContainerLogs(app1LogDir, container11, new string[] { "stdout", "stderr", "syslog"
				 });
			logAggregationService.Handle(new LogHandlerContainerFinishedEvent(container11, 0)
				);
			logAggregationService.Handle(new LogHandlerAppFinishedEvent(application1));
			logAggregationService.Stop();
			NUnit.Framework.Assert.AreEqual(0, logAggregationService.GetNumAggregators());
			// ensure filesystems were closed
			Org.Mockito.Mockito.Verify(logAggregationService).CloseFileSystems(Matchers.Any<UserGroupInformation
				>());
			Org.Mockito.Mockito.Verify(delSrvc).Delete(Matchers.Eq(user), Matchers.Eq((Path)null
				), Matchers.Eq(new Path(app1LogDir.GetAbsolutePath())));
			delSrvc.Stop();
			string containerIdStr = ConverterUtils.ToString(container11);
			FilePath containerLogDir = new FilePath(app1LogDir, containerIdStr);
			foreach (string fileType in new string[] { "stdout", "stderr", "syslog" })
			{
				FilePath f = new FilePath(containerLogDir, fileType);
				NUnit.Framework.Assert.IsFalse("check " + f, f.Exists());
			}
			NUnit.Framework.Assert.IsFalse(app1LogDir.Exists());
			Path logFilePath = logAggregationService.GetRemoteNodeLogFileForApp(application1, 
				this.user);
			NUnit.Framework.Assert.IsTrue("Log file [" + logFilePath + "] not found", new FilePath
				(logFilePath.ToUri().GetPath()).Exists());
			dispatcher.Await();
			ApplicationEvent[] expectedEvents = new ApplicationEvent[] { new ApplicationEvent
				(appAttemptId.GetApplicationId(), ApplicationEventType.ApplicationLogHandlingInited
				), new ApplicationEvent(appAttemptId.GetApplicationId(), ApplicationEventType.ApplicationLogHandlingFinished
				) };
			CheckEvents(appEventHandler, expectedEvents, true, "getType", "getApplicationID");
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestLocalFileDeletionAfterUpload()
		{
			this.delSrvc = new DeletionService(CreateContainerExecutor());
			delSrvc = Org.Mockito.Mockito.Spy(delSrvc);
			this.delSrvc.Init(conf);
			this.conf.Set(YarnConfiguration.NmLogDirs, localLogDir.GetAbsolutePath());
			this.conf.Set(YarnConfiguration.NmRemoteAppLogDir, this.remoteRootLogDir.GetAbsolutePath
				());
			LogAggregationService logAggregationService = Org.Mockito.Mockito.Spy(new LogAggregationService
				(dispatcher, this.context, this.delSrvc, base.dirsHandler));
			VerifyLocalFileDeletion(logAggregationService);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestLocalFileDeletionOnDiskFull()
		{
			this.delSrvc = new DeletionService(CreateContainerExecutor());
			delSrvc = Org.Mockito.Mockito.Spy(delSrvc);
			this.delSrvc.Init(conf);
			this.conf.Set(YarnConfiguration.NmLogDirs, localLogDir.GetAbsolutePath());
			this.conf.Set(YarnConfiguration.NmRemoteAppLogDir, this.remoteRootLogDir.GetAbsolutePath
				());
			IList<string> logDirs = base.dirsHandler.GetLogDirs();
			LocalDirsHandlerService dirsHandler = Org.Mockito.Mockito.Spy(base.dirsHandler);
			// Simulate disk being full by returning no good log dirs but having a
			// directory in full log dirs.
			Org.Mockito.Mockito.When(dirsHandler.GetLogDirs()).ThenReturn(new AList<string>()
				);
			Org.Mockito.Mockito.When(dirsHandler.GetLogDirsForRead()).ThenReturn(logDirs);
			LogAggregationService logAggregationService = Org.Mockito.Mockito.Spy(new LogAggregationService
				(dispatcher, this.context, this.delSrvc, dirsHandler));
			VerifyLocalFileDeletion(logAggregationService);
		}

		/* Test to verify fix for YARN-3793 */
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNoLogsUploadedOnAppFinish()
		{
			this.delSrvc = new DeletionService(CreateContainerExecutor());
			delSrvc = Org.Mockito.Mockito.Spy(delSrvc);
			this.delSrvc.Init(conf);
			this.conf.Set(YarnConfiguration.NmLogDirs, localLogDir.GetAbsolutePath());
			this.conf.Set(YarnConfiguration.NmRemoteAppLogDir, this.remoteRootLogDir.GetAbsolutePath
				());
			LogAggregationService logAggregationService = new LogAggregationService(dispatcher
				, this.context, this.delSrvc, base.dirsHandler);
			logAggregationService.Init(this.conf);
			logAggregationService.Start();
			ApplicationId app = BuilderUtils.NewApplicationId(1234, 1);
			FilePath appLogDir = new FilePath(localLogDir, ConverterUtils.ToString(app));
			appLogDir.Mkdir();
			LogAggregationContext context = LogAggregationContext.NewInstance("HOST*", "sys*"
				);
			logAggregationService.Handle(new LogHandlerAppStartedEvent(app, this.user, null, 
				ContainerLogsRetentionPolicy.AllContainers, this.acls, context));
			ApplicationAttemptId appAttemptId = BuilderUtils.NewApplicationAttemptId(app, 1);
			ContainerId cont = BuilderUtils.NewContainerId(appAttemptId, 1);
			WriteContainerLogs(appLogDir, cont, new string[] { "stdout", "stderr", "syslog" }
				);
			logAggregationService.Handle(new LogHandlerContainerFinishedEvent(cont, 0));
			logAggregationService.Handle(new LogHandlerAppFinishedEvent(app));
			logAggregationService.Stop();
			delSrvc.Stop();
			// Aggregated logs should not be deleted if not uploaded.
			Org.Mockito.Mockito.Verify(delSrvc, Org.Mockito.Mockito.Times(0)).Delete(user, null
				);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNoContainerOnNode()
		{
			this.conf.Set(YarnConfiguration.NmLogDirs, localLogDir.GetAbsolutePath());
			this.conf.Set(YarnConfiguration.NmRemoteAppLogDir, this.remoteRootLogDir.GetAbsolutePath
				());
			LogAggregationService logAggregationService = new LogAggregationService(dispatcher
				, this.context, this.delSrvc, base.dirsHandler);
			logAggregationService.Init(this.conf);
			logAggregationService.Start();
			ApplicationId application1 = BuilderUtils.NewApplicationId(1234, 1);
			// AppLogDir should be created
			FilePath app1LogDir = new FilePath(localLogDir, ConverterUtils.ToString(application1
				));
			app1LogDir.Mkdir();
			logAggregationService.Handle(new LogHandlerAppStartedEvent(application1, this.user
				, null, ContainerLogsRetentionPolicy.AllContainers, this.acls));
			logAggregationService.Handle(new LogHandlerAppFinishedEvent(application1));
			logAggregationService.Stop();
			NUnit.Framework.Assert.AreEqual(0, logAggregationService.GetNumAggregators());
			NUnit.Framework.Assert.IsFalse(new FilePath(logAggregationService.GetRemoteNodeLogFileForApp
				(application1, this.user).ToUri().GetPath()).Exists());
			dispatcher.Await();
			ApplicationEvent[] expectedEvents = new ApplicationEvent[] { new ApplicationEvent
				(application1, ApplicationEventType.ApplicationLogHandlingInited), new ApplicationEvent
				(application1, ApplicationEventType.ApplicationLogHandlingFinished) };
			CheckEvents(appEventHandler, expectedEvents, true, "getType", "getApplicationID");
			logAggregationService.Close();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMultipleAppsLogAggregation()
		{
			this.conf.Set(YarnConfiguration.NmLogDirs, localLogDir.GetAbsolutePath());
			this.conf.Set(YarnConfiguration.NmRemoteAppLogDir, this.remoteRootLogDir.GetAbsolutePath
				());
			string[] fileNames = new string[] { "stdout", "stderr", "syslog" };
			LogAggregationService logAggregationService = new LogAggregationService(dispatcher
				, this.context, this.delSrvc, base.dirsHandler);
			logAggregationService.Init(this.conf);
			logAggregationService.Start();
			ApplicationId application1 = BuilderUtils.NewApplicationId(1234, 1);
			// AppLogDir should be created
			FilePath app1LogDir = new FilePath(localLogDir, ConverterUtils.ToString(application1
				));
			app1LogDir.Mkdir();
			logAggregationService.Handle(new LogHandlerAppStartedEvent(application1, this.user
				, null, ContainerLogsRetentionPolicy.AllContainers, this.acls));
			ApplicationAttemptId appAttemptId1 = BuilderUtils.NewApplicationAttemptId(application1
				, 1);
			ContainerId container11 = BuilderUtils.NewContainerId(appAttemptId1, 1);
			// Simulate log-file creation
			WriteContainerLogs(app1LogDir, container11, fileNames);
			logAggregationService.Handle(new LogHandlerContainerFinishedEvent(container11, 0)
				);
			ApplicationId application2 = BuilderUtils.NewApplicationId(1234, 2);
			ApplicationAttemptId appAttemptId2 = BuilderUtils.NewApplicationAttemptId(application2
				, 1);
			FilePath app2LogDir = new FilePath(localLogDir, ConverterUtils.ToString(application2
				));
			app2LogDir.Mkdir();
			logAggregationService.Handle(new LogHandlerAppStartedEvent(application2, this.user
				, null, ContainerLogsRetentionPolicy.ApplicationMasterOnly, this.acls));
			ContainerId container21 = BuilderUtils.NewContainerId(appAttemptId2, 1);
			WriteContainerLogs(app2LogDir, container21, fileNames);
			logAggregationService.Handle(new LogHandlerContainerFinishedEvent(container21, 0)
				);
			ContainerId container12 = BuilderUtils.NewContainerId(appAttemptId1, 2);
			WriteContainerLogs(app1LogDir, container12, fileNames);
			logAggregationService.Handle(new LogHandlerContainerFinishedEvent(container12, 0)
				);
			ApplicationId application3 = BuilderUtils.NewApplicationId(1234, 3);
			ApplicationAttemptId appAttemptId3 = BuilderUtils.NewApplicationAttemptId(application3
				, 1);
			FilePath app3LogDir = new FilePath(localLogDir, ConverterUtils.ToString(application3
				));
			app3LogDir.Mkdir();
			logAggregationService.Handle(new LogHandlerAppStartedEvent(application3, this.user
				, null, ContainerLogsRetentionPolicy.AmAndFailedContainersOnly, this.acls));
			dispatcher.Await();
			ApplicationEvent[] expectedInitEvents = new ApplicationEvent[] { new ApplicationEvent
				(application1, ApplicationEventType.ApplicationLogHandlingInited), new ApplicationEvent
				(application2, ApplicationEventType.ApplicationLogHandlingInited), new ApplicationEvent
				(application3, ApplicationEventType.ApplicationLogHandlingInited) };
			CheckEvents(appEventHandler, expectedInitEvents, false, "getType", "getApplicationID"
				);
			Org.Mockito.Mockito.Reset(appEventHandler);
			ContainerId container31 = BuilderUtils.NewContainerId(appAttemptId3, 1);
			WriteContainerLogs(app3LogDir, container31, fileNames);
			logAggregationService.Handle(new LogHandlerContainerFinishedEvent(container31, 0)
				);
			ContainerId container32 = BuilderUtils.NewContainerId(appAttemptId3, 2);
			WriteContainerLogs(app3LogDir, container32, fileNames);
			logAggregationService.Handle(new LogHandlerContainerFinishedEvent(container32, 1)
				);
			// Failed 
			ContainerId container22 = BuilderUtils.NewContainerId(appAttemptId2, 2);
			WriteContainerLogs(app2LogDir, container22, fileNames);
			logAggregationService.Handle(new LogHandlerContainerFinishedEvent(container22, 0)
				);
			ContainerId container33 = BuilderUtils.NewContainerId(appAttemptId3, 3);
			WriteContainerLogs(app3LogDir, container33, fileNames);
			logAggregationService.Handle(new LogHandlerContainerFinishedEvent(container33, 0)
				);
			logAggregationService.Handle(new LogHandlerAppFinishedEvent(application2));
			logAggregationService.Handle(new LogHandlerAppFinishedEvent(application3));
			logAggregationService.Handle(new LogHandlerAppFinishedEvent(application1));
			logAggregationService.Stop();
			NUnit.Framework.Assert.AreEqual(0, logAggregationService.GetNumAggregators());
			VerifyContainerLogs(logAggregationService, application1, new ContainerId[] { container11
				, container12 }, fileNames, 3, false);
			VerifyContainerLogs(logAggregationService, application2, new ContainerId[] { container21
				 }, fileNames, 3, false);
			VerifyContainerLogs(logAggregationService, application3, new ContainerId[] { container31
				, container32 }, fileNames, 3, false);
			dispatcher.Await();
			ApplicationEvent[] expectedFinishedEvents = new ApplicationEvent[] { new ApplicationEvent
				(application1, ApplicationEventType.ApplicationLogHandlingFinished), new ApplicationEvent
				(application2, ApplicationEventType.ApplicationLogHandlingFinished), new ApplicationEvent
				(application3, ApplicationEventType.ApplicationLogHandlingFinished) };
			CheckEvents(appEventHandler, expectedFinishedEvents, false, "getType", "getApplicationID"
				);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestVerifyAndCreateRemoteDirsFailure()
		{
			this.conf.Set(YarnConfiguration.NmLogDirs, localLogDir.GetAbsolutePath());
			this.conf.Set(YarnConfiguration.NmRemoteAppLogDir, this.remoteRootLogDir.GetAbsolutePath
				());
			LogAggregationService logAggregationService = Org.Mockito.Mockito.Spy(new LogAggregationService
				(dispatcher, this.context, this.delSrvc, base.dirsHandler));
			logAggregationService.Init(this.conf);
			YarnRuntimeException e = new YarnRuntimeException("KABOOM!");
			Org.Mockito.Mockito.DoThrow(e).When(logAggregationService).VerifyAndCreateRemoteLogDir
				(Matchers.Any<Configuration>());
			logAggregationService.Start();
			// Now try to start an application
			ApplicationId appId = BuilderUtils.NewApplicationId(Runtime.CurrentTimeMillis(), 
				(int)(Math.Random() * 1000));
			logAggregationService.Handle(new LogHandlerAppStartedEvent(appId, this.user, null
				, ContainerLogsRetentionPolicy.AmAndFailedContainersOnly, this.acls));
			dispatcher.Await();
			// Verify that it failed
			ApplicationEvent[] expectedEvents = new ApplicationEvent[] { new ApplicationEvent
				(appId, ApplicationEventType.ApplicationLogHandlingFailed) };
			CheckEvents(appEventHandler, expectedEvents, false, "getType", "getApplicationID"
				, "getDiagnostic");
			Org.Mockito.Mockito.Reset(logAggregationService);
			// Now try to start another one
			ApplicationId appId2 = BuilderUtils.NewApplicationId(Runtime.CurrentTimeMillis(), 
				(int)(Math.Random() * 1000));
			FilePath appLogDir = new FilePath(localLogDir, ConverterUtils.ToString(appId2));
			appLogDir.Mkdir();
			logAggregationService.Handle(new LogHandlerAppStartedEvent(appId2, this.user, null
				, ContainerLogsRetentionPolicy.AmAndFailedContainersOnly, this.acls));
			dispatcher.Await();
			// Verify that it worked
			expectedEvents = new ApplicationEvent[] { new ApplicationEvent(appId, ApplicationEventType
				.ApplicationLogHandlingFailed), new ApplicationEvent(appId2, ApplicationEventType
				.ApplicationLogHandlingInited) };
			// original failure
			// success
			CheckEvents(appEventHandler, expectedEvents, false, "getType", "getApplicationID"
				, "getDiagnostic");
			logAggregationService.Stop();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestVerifyAndCreateRemoteDirNonExistence()
		{
			this.conf.Set(YarnConfiguration.NmLogDirs, localLogDir.GetAbsolutePath());
			FilePath aNewFile = new FilePath(("tmp" + Runtime.CurrentTimeMillis()).ToString()
				);
			this.conf.Set(YarnConfiguration.NmRemoteAppLogDir, aNewFile.GetAbsolutePath());
			LogAggregationService logAggregationService = Org.Mockito.Mockito.Spy(new LogAggregationService
				(dispatcher, this.context, this.delSrvc, base.dirsHandler));
			logAggregationService.Init(this.conf);
			bool existsBefore = aNewFile.Exists();
			NUnit.Framework.Assert.IsTrue("The new file already exists!", !existsBefore);
			logAggregationService.VerifyAndCreateRemoteLogDir(this.conf);
			bool existsAfter = aNewFile.Exists();
			NUnit.Framework.Assert.IsTrue("The new aggregate file is not successfully created"
				, existsAfter);
			aNewFile.Delete();
		}

		//housekeeping
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAppLogDirCreation()
		{
			string logSuffix = "logs";
			this.conf.Set(YarnConfiguration.NmLogDirs, localLogDir.GetAbsolutePath());
			this.conf.Set(YarnConfiguration.NmRemoteAppLogDir, this.remoteRootLogDir.GetAbsolutePath
				());
			this.conf.Set(YarnConfiguration.NmRemoteAppLogDirSuffix, logSuffix);
			InlineDispatcher dispatcher = new InlineDispatcher();
			dispatcher.Init(this.conf);
			dispatcher.Start();
			FileSystem fs = FileSystem.Get(this.conf);
			FileSystem spyFs = Org.Mockito.Mockito.Spy(FileSystem.Get(this.conf));
			LogAggregationService aggSvc = new _LogAggregationService_609(spyFs, dispatcher, 
				this.context, this.delSrvc, base.dirsHandler);
			aggSvc.Init(this.conf);
			aggSvc.Start();
			// start an application and verify user, suffix, and app dirs created
			ApplicationId appId = BuilderUtils.NewApplicationId(1, 1);
			Path userDir = fs.MakeQualified(new Path(remoteRootLogDir.GetAbsolutePath(), this
				.user));
			Path suffixDir = new Path(userDir, logSuffix);
			Path appDir = new Path(suffixDir, appId.ToString());
			aggSvc.Handle(new LogHandlerAppStartedEvent(appId, this.user, null, ContainerLogsRetentionPolicy
				.AllContainers, this.acls));
			Org.Mockito.Mockito.Verify(spyFs).Mkdirs(Matchers.Eq(userDir), Matchers.IsA<FsPermission
				>());
			Org.Mockito.Mockito.Verify(spyFs).Mkdirs(Matchers.Eq(suffixDir), Matchers.IsA<FsPermission
				>());
			Org.Mockito.Mockito.Verify(spyFs).Mkdirs(Matchers.Eq(appDir), Matchers.IsA<FsPermission
				>());
			// start another application and verify only app dir created
			ApplicationId appId2 = BuilderUtils.NewApplicationId(1, 2);
			Path appDir2 = new Path(suffixDir, appId2.ToString());
			aggSvc.Handle(new LogHandlerAppStartedEvent(appId2, this.user, null, ContainerLogsRetentionPolicy
				.AllContainers, this.acls));
			Org.Mockito.Mockito.Verify(spyFs).Mkdirs(Matchers.Eq(appDir2), Matchers.IsA<FsPermission
				>());
			// start another application with the app dir already created and verify
			// we do not try to create it again
			ApplicationId appId3 = BuilderUtils.NewApplicationId(1, 3);
			Path appDir3 = new Path(suffixDir, appId3.ToString());
			new FilePath(appDir3.ToUri().GetPath()).Mkdir();
			aggSvc.Handle(new LogHandlerAppStartedEvent(appId3, this.user, null, ContainerLogsRetentionPolicy
				.AllContainers, this.acls));
			Org.Mockito.Mockito.Verify(spyFs, Org.Mockito.Mockito.Never()).Mkdirs(Matchers.Eq
				(appDir3), Matchers.IsA<FsPermission>());
			aggSvc.Stop();
			aggSvc.Close();
			dispatcher.Stop();
		}

		private sealed class _LogAggregationService_609 : LogAggregationService
		{
			public _LogAggregationService_609(FileSystem spyFs, Dispatcher baseArg1, Context 
				baseArg2, DeletionService baseArg3, LocalDirsHandlerService baseArg4)
				: base(baseArg1, baseArg2, baseArg3, baseArg4)
			{
				this.spyFs = spyFs;
			}

			protected internal override FileSystem GetFileSystem(Configuration conf)
			{
				return spyFs;
			}

			private readonly FileSystem spyFs;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestLogAggregationInitAppFailsWithoutKillingNM()
		{
			this.conf.Set(YarnConfiguration.NmLogDirs, localLogDir.GetAbsolutePath());
			this.conf.Set(YarnConfiguration.NmRemoteAppLogDir, this.remoteRootLogDir.GetAbsolutePath
				());
			LogAggregationService logAggregationService = Org.Mockito.Mockito.Spy(new LogAggregationService
				(dispatcher, this.context, this.delSrvc, base.dirsHandler));
			logAggregationService.Init(this.conf);
			logAggregationService.Start();
			ApplicationId appId = BuilderUtils.NewApplicationId(Runtime.CurrentTimeMillis(), 
				(int)(Math.Random() * 1000));
			Org.Mockito.Mockito.DoThrow(new YarnRuntimeException("KABOOM!")).When(logAggregationService
				).InitAppAggregator(Matchers.Eq(appId), Matchers.Eq(user), Matchers.Any<Credentials
				>(), Matchers.Any<ContainerLogsRetentionPolicy>(), Matchers.AnyMap(), Matchers.Any
				<LogAggregationContext>());
			logAggregationService.Handle(new LogHandlerAppStartedEvent(appId, this.user, null
				, ContainerLogsRetentionPolicy.AmAndFailedContainersOnly, this.acls));
			dispatcher.Await();
			ApplicationEvent[] expectedEvents = new ApplicationEvent[] { new ApplicationEvent
				(appId, ApplicationEventType.ApplicationLogHandlingFailed) };
			CheckEvents(appEventHandler, expectedEvents, false, "getType", "getApplicationID"
				, "getDiagnostic");
			// no filesystems instantiated yet
			Org.Mockito.Mockito.Verify(logAggregationService, Org.Mockito.Mockito.Never()).CloseFileSystems
				(Matchers.Any<UserGroupInformation>());
			// verify trying to collect logs for containers/apps we don't know about
			// doesn't blow up and tear down the NM
			logAggregationService.Handle(new LogHandlerContainerFinishedEvent(BuilderUtils.NewContainerId
				(4, 1, 1, 1), 0));
			dispatcher.Await();
			logAggregationService.Handle(new LogHandlerAppFinishedEvent(BuilderUtils.NewApplicationId
				(1, 5)));
			dispatcher.Await();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestLogAggregationCreateDirsFailsWithoutKillingNM()
		{
			this.conf.Set(YarnConfiguration.NmLogDirs, localLogDir.GetAbsolutePath());
			this.conf.Set(YarnConfiguration.NmRemoteAppLogDir, this.remoteRootLogDir.GetAbsolutePath
				());
			DeletionService spyDelSrvc = Org.Mockito.Mockito.Spy(this.delSrvc);
			LogAggregationService logAggregationService = Org.Mockito.Mockito.Spy(new LogAggregationService
				(dispatcher, this.context, spyDelSrvc, base.dirsHandler));
			logAggregationService.Init(this.conf);
			logAggregationService.Start();
			ApplicationId appId = BuilderUtils.NewApplicationId(Runtime.CurrentTimeMillis(), 
				(int)(Math.Random() * 1000));
			FilePath appLogDir = new FilePath(localLogDir, ConverterUtils.ToString(appId));
			appLogDir.Mkdir();
			Exception e = new RuntimeException("KABOOM!");
			Org.Mockito.Mockito.DoThrow(e).When(logAggregationService).CreateAppDir(Matchers.Any
				<string>(), Matchers.Any<ApplicationId>(), Matchers.Any<UserGroupInformation>());
			logAggregationService.Handle(new LogHandlerAppStartedEvent(appId, this.user, null
				, ContainerLogsRetentionPolicy.AmAndFailedContainersOnly, this.acls));
			dispatcher.Await();
			ApplicationEvent[] expectedEvents = new ApplicationEvent[] { new ApplicationEvent
				(appId, ApplicationEventType.ApplicationLogHandlingFailed) };
			CheckEvents(appEventHandler, expectedEvents, false, "getType", "getApplicationID"
				, "getDiagnostic");
			// verify trying to collect logs for containers/apps we don't know about
			// doesn't blow up and tear down the NM
			logAggregationService.Handle(new LogHandlerContainerFinishedEvent(BuilderUtils.NewContainerId
				(4, 1, 1, 1), 0));
			dispatcher.Await();
			logAggregationService.Handle(new LogHandlerAppFinishedEvent(BuilderUtils.NewApplicationId
				(1, 5)));
			dispatcher.Await();
			logAggregationService.Stop();
			NUnit.Framework.Assert.AreEqual(0, logAggregationService.GetNumAggregators());
			Org.Mockito.Mockito.Verify(spyDelSrvc).Delete(Matchers.Eq(user), Matchers.Any<Path
				>(), Org.Mockito.Mockito.AnyVararg<Path>());
			Org.Mockito.Mockito.Verify(logAggregationService).CloseFileSystems(Matchers.Any<UserGroupInformation
				>());
		}

		/// <exception cref="System.IO.IOException"/>
		private void WriteContainerLogs(FilePath appLogDir, ContainerId containerId, string
			[] fileName)
		{
			// ContainerLogDir should be created
			string containerStr = ConverterUtils.ToString(containerId);
			FilePath containerLogDir = new FilePath(appLogDir, containerStr);
			containerLogDir.Mkdir();
			foreach (string fileType in fileName)
			{
				TextWriter writer11 = new FileWriter(new FilePath(containerLogDir, fileType));
				writer11.Write(containerStr + " Hello " + fileType + "!");
				writer11.Close();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private TestLogAggregationService.LogFileStatusInLastCycle VerifyContainerLogs(LogAggregationService
			 logAggregationService, ApplicationId appId, ContainerId[] expectedContainerIds, 
			string[] logFiles, int numOfContainerLogs, bool multiLogs)
		{
			Path appLogDir = logAggregationService.GetRemoteAppLogDir(appId, this.user);
			RemoteIterator<FileStatus> nodeFiles = null;
			try
			{
				Path qualifiedLogDir = FileContext.GetFileContext(this.conf).MakeQualified(appLogDir
					);
				nodeFiles = FileContext.GetFileContext(qualifiedLogDir.ToUri(), this.conf).ListStatus
					(appLogDir);
			}
			catch (FileNotFoundException)
			{
				NUnit.Framework.Assert.Fail("Should have log files");
			}
			NUnit.Framework.Assert.IsTrue(nodeFiles.HasNext());
			FileStatus targetNodeFile = null;
			if (!multiLogs)
			{
				targetNodeFile = nodeFiles.Next();
				NUnit.Framework.Assert.IsTrue(targetNodeFile.GetPath().GetName().Equals(LogAggregationUtils
					.GetNodeString(logAggregationService.GetNodeId())));
			}
			else
			{
				long fileCreateTime = 0;
				while (nodeFiles.HasNext())
				{
					FileStatus nodeFile = nodeFiles.Next();
					if (!nodeFile.GetPath().GetName().Contains(LogAggregationUtils.TmpFileSuffix))
					{
						long time = long.Parse(nodeFile.GetPath().GetName().Split("_")[2]);
						if (time > fileCreateTime)
						{
							targetNodeFile = nodeFile;
							fileCreateTime = time;
						}
					}
				}
				string[] fileName = targetNodeFile.GetPath().GetName().Split("_");
				NUnit.Framework.Assert.IsTrue(fileName.Length == 3);
				NUnit.Framework.Assert.AreEqual(fileName[0] + ":" + fileName[1], logAggregationService
					.GetNodeId().ToString());
			}
			AggregatedLogFormat.LogReader reader = new AggregatedLogFormat.LogReader(this.conf
				, targetNodeFile.GetPath());
			NUnit.Framework.Assert.AreEqual(this.user, reader.GetApplicationOwner());
			VerifyAcls(reader.GetApplicationAcls());
			IList<string> fileTypes = new AList<string>();
			try
			{
				IDictionary<string, IDictionary<string, string>> logMap = new Dictionary<string, 
					IDictionary<string, string>>();
				DataInputStream valueStream;
				AggregatedLogFormat.LogKey key = new AggregatedLogFormat.LogKey();
				valueStream = reader.Next(key);
				while (valueStream != null)
				{
					Log.Info("Found container " + key.ToString());
					IDictionary<string, string> perContainerMap = new Dictionary<string, string>();
					logMap[key.ToString()] = perContainerMap;
					while (true)
					{
						try
						{
							ByteArrayOutputStream baos = new ByteArrayOutputStream();
							TextWriter ps = new TextWriter(baos);
							AggregatedLogFormat.LogReader.ReadAContainerLogsForALogType(valueStream, ps);
							string[] writtenLines = baos.ToString().Split(Runtime.GetProperty("line.separator"
								));
							NUnit.Framework.Assert.AreEqual("LogType:", Sharpen.Runtime.Substring(writtenLines
								[0], 0, 8));
							string fileType = Sharpen.Runtime.Substring(writtenLines[0], 8);
							fileTypes.AddItem(fileType);
							NUnit.Framework.Assert.AreEqual("LogLength:", Sharpen.Runtime.Substring(writtenLines
								[1], 0, 10));
							string fileLengthStr = Sharpen.Runtime.Substring(writtenLines[1], 10);
							long fileLength = long.Parse(fileLengthStr);
							NUnit.Framework.Assert.AreEqual("Log Contents:", Sharpen.Runtime.Substring(writtenLines
								[2], 0, 13));
							string logContents = StringUtils.Join(Arrays.CopyOfRange(writtenLines, 3, writtenLines
								.Length), "\n");
							perContainerMap[fileType] = logContents;
							Log.Info("LogType:" + fileType);
							Log.Info("LogLength:" + fileLength);
							Log.Info("Log Contents:\n" + perContainerMap[fileType]);
						}
						catch (EOFException)
						{
							break;
						}
					}
					// Next container
					key = new AggregatedLogFormat.LogKey();
					valueStream = reader.Next(key);
				}
				// 1 for each container
				NUnit.Framework.Assert.AreEqual(expectedContainerIds.Length, logMap.Count);
				foreach (ContainerId cId in expectedContainerIds)
				{
					string containerStr = ConverterUtils.ToString(cId);
					IDictionary<string, string> thisContainerMap = Sharpen.Collections.Remove(logMap, 
						containerStr);
					NUnit.Framework.Assert.AreEqual(numOfContainerLogs, thisContainerMap.Count);
					foreach (string fileType in logFiles)
					{
						string expectedValue = containerStr + " Hello " + fileType + "!End of LogType:" +
							 fileType;
						Log.Info("Expected log-content : " + new string(expectedValue));
						string foundValue = Sharpen.Collections.Remove(thisContainerMap, fileType);
						NUnit.Framework.Assert.IsNotNull(cId + " " + fileType + " not present in aggregated log-file!"
							, foundValue);
						NUnit.Framework.Assert.AreEqual(expectedValue, foundValue);
					}
					NUnit.Framework.Assert.AreEqual(0, thisContainerMap.Count);
				}
				NUnit.Framework.Assert.AreEqual(0, logMap.Count);
				return new TestLogAggregationService.LogFileStatusInLastCycle(targetNodeFile.GetPath
					().GetName(), fileTypes);
			}
			finally
			{
				reader.Close();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		[NUnit.Framework.Test]
		public virtual void TestLogAggregationForRealContainerLaunch()
		{
			this.containerManager.Start();
			FilePath scriptFile = new FilePath(tmpDir, "scriptFile.sh");
			PrintWriter fileWriter = new PrintWriter(scriptFile);
			fileWriter.Write("\necho Hello World! Stdout! > " + new FilePath(localLogDir, "stdout"
				));
			fileWriter.Write("\necho Hello World! Stderr! > " + new FilePath(localLogDir, "stderr"
				));
			fileWriter.Write("\necho Hello World! Syslog! > " + new FilePath(localLogDir, "syslog"
				));
			fileWriter.Close();
			ContainerLaunchContext containerLaunchContext = recordFactory.NewRecordInstance<ContainerLaunchContext
				>();
			// ////// Construct the Container-id
			ApplicationId appId = ApplicationId.NewInstance(0, 0);
			ApplicationAttemptId appAttemptId = BuilderUtils.NewApplicationAttemptId(appId, 1
				);
			ContainerId cId = BuilderUtils.NewContainerId(appAttemptId, 0);
			URL resource_alpha = ConverterUtils.GetYarnUrlFromPath(localFS.MakeQualified(new 
				Path(scriptFile.GetAbsolutePath())));
			LocalResource rsrc_alpha = recordFactory.NewRecordInstance<LocalResource>();
			rsrc_alpha.SetResource(resource_alpha);
			rsrc_alpha.SetSize(-1);
			rsrc_alpha.SetVisibility(LocalResourceVisibility.Application);
			rsrc_alpha.SetType(LocalResourceType.File);
			rsrc_alpha.SetTimestamp(scriptFile.LastModified());
			string destinationFile = "dest_file";
			IDictionary<string, LocalResource> localResources = new Dictionary<string, LocalResource
				>();
			localResources[destinationFile] = rsrc_alpha;
			containerLaunchContext.SetLocalResources(localResources);
			IList<string> commands = new AList<string>();
			commands.AddItem("/bin/bash");
			commands.AddItem(scriptFile.GetAbsolutePath());
			containerLaunchContext.SetCommands(commands);
			StartContainerRequest scRequest = StartContainerRequest.NewInstance(containerLaunchContext
				, TestContainerManager.CreateContainerToken(cId, DummyRmIdentifier, context.GetNodeId
				(), user, context.GetContainerTokenSecretManager()));
			IList<StartContainerRequest> list = new AList<StartContainerRequest>();
			list.AddItem(scRequest);
			StartContainersRequest allRequests = StartContainersRequest.NewInstance(list);
			this.containerManager.StartContainers(allRequests);
			BaseContainerManagerTest.WaitForContainerState(this.containerManager, cId, ContainerState
				.Complete);
			this.containerManager.Handle(new CMgrCompletedAppsEvent(Arrays.AsList(appId), CMgrCompletedAppsEvent.Reason
				.OnShutdown));
			this.containerManager.Stop();
		}

		private void VerifyAcls(IDictionary<ApplicationAccessType, string> logAcls)
		{
			NUnit.Framework.Assert.AreEqual(this.acls.Count, logAcls.Count);
			foreach (ApplicationAccessType appAccessType in this.acls.Keys)
			{
				NUnit.Framework.Assert.AreEqual(this.acls[appAccessType], logAcls[appAccessType]);
			}
		}

		private DrainDispatcher CreateDispatcher()
		{
			DrainDispatcher dispatcher = new DrainDispatcher();
			dispatcher.Init(this.conf);
			dispatcher.Start();
			return dispatcher;
		}

		private IDictionary<ApplicationAccessType, string> CreateAppAcls()
		{
			IDictionary<ApplicationAccessType, string> appAcls = new Dictionary<ApplicationAccessType
				, string>();
			appAcls[ApplicationAccessType.ModifyApp] = "user group";
			appAcls[ApplicationAccessType.ViewApp] = "*";
			return appAcls;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestStopAfterError()
		{
			DeletionService delSrvc = Org.Mockito.Mockito.Mock<DeletionService>();
			// get the AppLogAggregationImpl thread to crash
			LocalDirsHandlerService mockedDirSvc = Org.Mockito.Mockito.Mock<LocalDirsHandlerService
				>();
			Org.Mockito.Mockito.When(mockedDirSvc.GetLogDirs()).ThenThrow(new RuntimeException
				());
			LogAggregationService logAggregationService = new LogAggregationService(dispatcher
				, this.context, delSrvc, mockedDirSvc);
			logAggregationService.Init(this.conf);
			logAggregationService.Start();
			ApplicationId application1 = BuilderUtils.NewApplicationId(1234, 1);
			logAggregationService.Handle(new LogHandlerAppStartedEvent(application1, this.user
				, null, ContainerLogsRetentionPolicy.AllContainers, this.acls));
			logAggregationService.Stop();
			NUnit.Framework.Assert.AreEqual(0, logAggregationService.GetNumAggregators());
			logAggregationService.Close();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestLogAggregatorCleanup()
		{
			DeletionService delSrvc = Org.Mockito.Mockito.Mock<DeletionService>();
			// get the AppLogAggregationImpl thread to crash
			LocalDirsHandlerService mockedDirSvc = Org.Mockito.Mockito.Mock<LocalDirsHandlerService
				>();
			LogAggregationService logAggregationService = new LogAggregationService(dispatcher
				, this.context, delSrvc, mockedDirSvc);
			logAggregationService.Init(this.conf);
			logAggregationService.Start();
			ApplicationId application1 = BuilderUtils.NewApplicationId(1234, 1);
			logAggregationService.Handle(new LogHandlerAppStartedEvent(application1, this.user
				, null, ContainerLogsRetentionPolicy.AllContainers, this.acls));
			logAggregationService.Handle(new LogHandlerAppFinishedEvent(application1));
			dispatcher.Await();
			int timeToWait = 20 * 1000;
			while (timeToWait > 0 && logAggregationService.GetNumAggregators() > 0)
			{
				Sharpen.Thread.Sleep(100);
				timeToWait -= 100;
			}
			NUnit.Framework.Assert.AreEqual("Log aggregator failed to cleanup!", 0, logAggregationService
				.GetNumAggregators());
			logAggregationService.Stop();
			logAggregationService.Close();
		}

		/// <exception cref="System.Exception"/>
		private static void CheckEvents<T>(EventHandler<T> eventHandler, T[] expectedEvents
			, bool inOrder, params string[] methods)
			where T : Org.Apache.Hadoop.Yarn.Event.Event<object>
		{
			Type genericClass = (Type)expectedEvents.GetType().GetElementType();
			ArgumentCaptor<T> eventCaptor = ArgumentCaptor.ForClass(genericClass);
			// captor work work unless used via a verify
			Org.Mockito.Mockito.Verify(eventHandler, Org.Mockito.Mockito.AtLeast(0)).Handle(eventCaptor
				.Capture());
			IList<T> actualEvents = eventCaptor.GetAllValues();
			// batch up exceptions so junit presents them as one
			MultiException failures = new MultiException();
			try
			{
				NUnit.Framework.Assert.AreEqual("expected events", expectedEvents.Length, actualEvents
					.Count);
			}
			catch (Exception e)
			{
				failures.Add(e);
			}
			if (inOrder)
			{
				// sequentially verify the events
				int len = Math.Max(expectedEvents.Length, actualEvents.Count);
				for (int n = 0; n < len; n++)
				{
					try
					{
						string expect = (n < expectedEvents.Length) ? EventToString(expectedEvents[n], methods
							) : null;
						string actual = (n < actualEvents.Count) ? EventToString(actualEvents[n], methods
							) : null;
						NUnit.Framework.Assert.AreEqual("event#" + n, expect, actual);
					}
					catch (Exception e)
					{
						failures.Add(e);
					}
				}
			}
			else
			{
				// verify the actual events were expected
				// verify no expected events were not seen
				ICollection<string> expectedSet = new HashSet<string>();
				foreach (T expectedEvent in expectedEvents)
				{
					expectedSet.AddItem(EventToString(expectedEvent, methods));
				}
				foreach (T actualEvent in actualEvents)
				{
					try
					{
						string actual = EventToString(actualEvent, methods);
						NUnit.Framework.Assert.IsTrue("unexpected event: " + actual, expectedSet.Remove(actual
							));
					}
					catch (Exception e)
					{
						failures.Add(e);
					}
				}
				foreach (string expected in expectedSet)
				{
					try
					{
						NUnit.Framework.Assert.Fail("missing event: " + expected);
					}
					catch (Exception e)
					{
						failures.Add(e);
					}
				}
			}
			failures.IfExceptionThrow();
		}

		/// <exception cref="System.Exception"/>
		private static string EventToString<_T0>(Org.Apache.Hadoop.Yarn.Event.Event<_T0> 
			@event, string[] methods)
			where _T0 : Enum<TYPE>
		{
			StringBuilder sb = new StringBuilder("[ ");
			foreach (string m in methods)
			{
				try
				{
					MethodInfo method = @event.GetType().GetMethod(m);
					string value = method.Invoke(@event).ToString();
					sb.Append(method.Name).Append("=").Append(value).Append(" ");
				}
				catch (Exception)
				{
				}
			}
			// ignore, actual event may not implement the method...
			sb.Append("]");
			return sb.ToString();
		}

		/*
		* Test to make sure we handle cases where the directories we get back from
		* the LocalDirsHandler may have issues including the log dir not being
		* present as well as other issues. The test uses helper functions from
		* TestNonAggregatingLogHandler.
		*/
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFailedDirsLocalFileDeletionAfterUpload()
		{
			// setup conf and services
			DeletionService mockDelService = Org.Mockito.Mockito.Mock<DeletionService>();
			FilePath[] localLogDirs = TestNonAggregatingLogHandler.GetLocalLogDirFiles(this.GetType
				().FullName, 7);
			IList<string> localLogDirPaths = new AList<string>(localLogDirs.Length);
			for (int i = 0; i < localLogDirs.Length; i++)
			{
				localLogDirPaths.AddItem(localLogDirs[i].GetAbsolutePath());
			}
			string localLogDirsString = StringUtils.Join(localLogDirPaths, ",");
			this.conf.Set(YarnConfiguration.NmLogDirs, localLogDirsString);
			this.conf.Set(YarnConfiguration.NmRemoteAppLogDir, this.remoteRootLogDir.GetAbsolutePath
				());
			this.conf.SetLong(YarnConfiguration.NmDiskHealthCheckIntervalMs, 500);
			ApplicationId application1 = BuilderUtils.NewApplicationId(1234, 1);
			ApplicationAttemptId appAttemptId = BuilderUtils.NewApplicationAttemptId(application1
				, 1);
			this.dirsHandler = new LocalDirsHandlerService();
			LocalDirsHandlerService mockDirsHandler = Org.Mockito.Mockito.Mock<LocalDirsHandlerService
				>();
			LogAggregationService logAggregationService = Org.Mockito.Mockito.Spy(new LogAggregationService
				(dispatcher, this.context, mockDelService, mockDirsHandler));
			AbstractFileSystem spylfs = Org.Mockito.Mockito.Spy(FileContext.GetLocalFSFileContext
				().GetDefaultFileSystem());
			FileContext lfs = FileContext.GetFileContext(spylfs, conf);
			Org.Mockito.Mockito.DoReturn(lfs).When(logAggregationService).GetLocalFileContext
				(Matchers.IsA<Configuration>());
			logAggregationService.Init(this.conf);
			logAggregationService.Start();
			TestNonAggregatingLogHandler.RunMockedFailedDirs(logAggregationService, application1
				, user, mockDelService, mockDirsHandler, conf, spylfs, lfs, localLogDirs);
			logAggregationService.Stop();
			NUnit.Framework.Assert.AreEqual(0, logAggregationService.GetNumAggregators());
			Org.Mockito.Mockito.Verify(logAggregationService).CloseFileSystems(Matchers.Any<UserGroupInformation
				>());
			ApplicationEvent[] expectedEvents = new ApplicationEvent[] { new ApplicationEvent
				(appAttemptId.GetApplicationId(), ApplicationEventType.ApplicationLogHandlingInited
				), new ApplicationEvent(appAttemptId.GetApplicationId(), ApplicationEventType.ApplicationLogHandlingFinished
				) };
			CheckEvents(appEventHandler, expectedEvents, true, "getType", "getApplicationID");
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestLogAggregationServiceWithPatterns()
		{
			LogAggregationContext logAggregationContextWithIncludePatterns = Org.Apache.Hadoop.Yarn.Util.Records
				.NewRecord<LogAggregationContext>();
			string includePattern = "stdout|syslog";
			logAggregationContextWithIncludePatterns.SetIncludePattern(includePattern);
			LogAggregationContext LogAggregationContextWithExcludePatterns = Org.Apache.Hadoop.Yarn.Util.Records
				.NewRecord<LogAggregationContext>();
			string excludePattern = "stdout|syslog";
			LogAggregationContextWithExcludePatterns.SetExcludePattern(excludePattern);
			this.conf.Set(YarnConfiguration.NmLogDirs, localLogDir.GetAbsolutePath());
			this.conf.Set(YarnConfiguration.NmRemoteAppLogDir, this.remoteRootLogDir.GetAbsolutePath
				());
			ApplicationId application1 = BuilderUtils.NewApplicationId(1234, 1);
			ApplicationId application2 = BuilderUtils.NewApplicationId(1234, 2);
			ApplicationId application3 = BuilderUtils.NewApplicationId(1234, 3);
			ApplicationId application4 = BuilderUtils.NewApplicationId(1234, 4);
			Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
				 mockApp = Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
				>();
			Org.Mockito.Mockito.When(mockApp.GetContainers()).ThenReturn(new Dictionary<ContainerId
				, Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
				>());
			this.context.GetApplications()[application1] = mockApp;
			this.context.GetApplications()[application2] = mockApp;
			this.context.GetApplications()[application3] = mockApp;
			this.context.GetApplications()[application4] = mockApp;
			LogAggregationService logAggregationService = new LogAggregationService(dispatcher
				, this.context, this.delSrvc, base.dirsHandler);
			logAggregationService.Init(this.conf);
			logAggregationService.Start();
			// LogContext for application1 has includePatten which includes
			// stdout and syslog.
			// After logAggregation is finished, we expect the logs for application1
			// has only logs from stdout and syslog
			// AppLogDir should be created
			FilePath appLogDir1 = new FilePath(localLogDir, ConverterUtils.ToString(application1
				));
			appLogDir1.Mkdir();
			logAggregationService.Handle(new LogHandlerAppStartedEvent(application1, this.user
				, null, ContainerLogsRetentionPolicy.AllContainers, this.acls, logAggregationContextWithIncludePatterns
				));
			ApplicationAttemptId appAttemptId1 = BuilderUtils.NewApplicationAttemptId(application1
				, 1);
			ContainerId container1 = BuilderUtils.NewContainerId(appAttemptId1, 1);
			// Simulate log-file creation
			WriteContainerLogs(appLogDir1, container1, new string[] { "stdout", "stderr", "syslog"
				 });
			logAggregationService.Handle(new LogHandlerContainerFinishedEvent(container1, 0));
			// LogContext for application2 has excludePatten which includes
			// stdout and syslog.
			// After logAggregation is finished, we expect the logs for application2
			// has only logs from stderr
			ApplicationAttemptId appAttemptId2 = BuilderUtils.NewApplicationAttemptId(application2
				, 1);
			FilePath app2LogDir = new FilePath(localLogDir, ConverterUtils.ToString(application2
				));
			app2LogDir.Mkdir();
			logAggregationService.Handle(new LogHandlerAppStartedEvent(application2, this.user
				, null, ContainerLogsRetentionPolicy.ApplicationMasterOnly, this.acls, LogAggregationContextWithExcludePatterns
				));
			ContainerId container2 = BuilderUtils.NewContainerId(appAttemptId2, 1);
			WriteContainerLogs(app2LogDir, container2, new string[] { "stdout", "stderr", "syslog"
				 });
			logAggregationService.Handle(new LogHandlerContainerFinishedEvent(container2, 0));
			// LogContext for application3 has includePattern which is *.log and
			// excludePatten which includes std.log and sys.log.
			// After logAggregation is finished, we expect the logs for application3
			// has all logs whose suffix is .log but excluding sys.log and std.log
			LogAggregationContext context1 = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<LogAggregationContext
				>();
			context1.SetIncludePattern(".*.log");
			context1.SetExcludePattern("sys.log|std.log");
			ApplicationAttemptId appAttemptId3 = BuilderUtils.NewApplicationAttemptId(application3
				, 1);
			FilePath app3LogDir = new FilePath(localLogDir, ConverterUtils.ToString(application3
				));
			app3LogDir.Mkdir();
			logAggregationService.Handle(new LogHandlerAppStartedEvent(application3, this.user
				, null, ContainerLogsRetentionPolicy.ApplicationMasterOnly, this.acls, context1)
				);
			ContainerId container3 = BuilderUtils.NewContainerId(appAttemptId3, 1);
			WriteContainerLogs(app3LogDir, container3, new string[] { "stdout", "sys.log", "std.log"
				, "out.log", "err.log", "log" });
			logAggregationService.Handle(new LogHandlerContainerFinishedEvent(container3, 0));
			// LogContext for application4 has includePattern
			// which includes std.log and sys.log and
			// excludePatten which includes std.log.
			// After logAggregation is finished, we expect the logs for application4
			// only has sys.log
			LogAggregationContext context2 = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<LogAggregationContext
				>();
			context2.SetIncludePattern("sys.log|std.log");
			context2.SetExcludePattern("std.log");
			ApplicationAttemptId appAttemptId4 = BuilderUtils.NewApplicationAttemptId(application4
				, 1);
			FilePath app4LogDir = new FilePath(localLogDir, ConverterUtils.ToString(application4
				));
			app4LogDir.Mkdir();
			logAggregationService.Handle(new LogHandlerAppStartedEvent(application4, this.user
				, null, ContainerLogsRetentionPolicy.ApplicationMasterOnly, this.acls, context2)
				);
			ContainerId container4 = BuilderUtils.NewContainerId(appAttemptId4, 1);
			WriteContainerLogs(app4LogDir, container4, new string[] { "stdout", "sys.log", "std.log"
				, "out.log", "err.log", "log" });
			logAggregationService.Handle(new LogHandlerContainerFinishedEvent(container4, 0));
			dispatcher.Await();
			ApplicationEvent[] expectedInitEvents = new ApplicationEvent[] { new ApplicationEvent
				(application1, ApplicationEventType.ApplicationLogHandlingInited), new ApplicationEvent
				(application2, ApplicationEventType.ApplicationLogHandlingInited), new ApplicationEvent
				(application3, ApplicationEventType.ApplicationLogHandlingInited), new ApplicationEvent
				(application4, ApplicationEventType.ApplicationLogHandlingInited) };
			CheckEvents(appEventHandler, expectedInitEvents, false, "getType", "getApplicationID"
				);
			Org.Mockito.Mockito.Reset(appEventHandler);
			logAggregationService.Handle(new LogHandlerAppFinishedEvent(application1));
			logAggregationService.Handle(new LogHandlerAppFinishedEvent(application2));
			logAggregationService.Handle(new LogHandlerAppFinishedEvent(application3));
			logAggregationService.Handle(new LogHandlerAppFinishedEvent(application4));
			logAggregationService.Stop();
			NUnit.Framework.Assert.AreEqual(0, logAggregationService.GetNumAggregators());
			string[] logFiles = new string[] { "stdout", "syslog" };
			VerifyContainerLogs(logAggregationService, application1, new ContainerId[] { container1
				 }, logFiles, 2, false);
			logFiles = new string[] { "stderr" };
			VerifyContainerLogs(logAggregationService, application2, new ContainerId[] { container2
				 }, logFiles, 1, false);
			logFiles = new string[] { "out.log", "err.log" };
			VerifyContainerLogs(logAggregationService, application3, new ContainerId[] { container3
				 }, logFiles, 2, false);
			logFiles = new string[] { "sys.log" };
			VerifyContainerLogs(logAggregationService, application4, new ContainerId[] { container4
				 }, logFiles, 1, false);
			dispatcher.Await();
			ApplicationEvent[] expectedFinishedEvents = new ApplicationEvent[] { new ApplicationEvent
				(application1, ApplicationEventType.ApplicationLogHandlingFinished), new ApplicationEvent
				(application2, ApplicationEventType.ApplicationLogHandlingFinished), new ApplicationEvent
				(application3, ApplicationEventType.ApplicationLogHandlingFinished), new ApplicationEvent
				(application4, ApplicationEventType.ApplicationLogHandlingFinished) };
			CheckEvents(appEventHandler, expectedFinishedEvents, false, "getType", "getApplicationID"
				);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestLogAggregationServiceWithInterval()
		{
			TestLogAggregationService(false);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestLogAggregationServiceWithRetention()
		{
			TestLogAggregationService(true);
		}

		/// <exception cref="System.Exception"/>
		private void TestLogAggregationService(bool retentionSizeLimitation)
		{
			LogAggregationContext logAggregationContextWithInterval = Org.Apache.Hadoop.Yarn.Util.Records
				.NewRecord<LogAggregationContext>();
			// set IncludePattern/excludePattern in rolling fashion
			// we expect all the logs except std_final will be uploaded
			// when app is running. The std_final will be uploaded when
			// the app finishes.
			logAggregationContextWithInterval.SetRolledLogsIncludePattern(".*");
			logAggregationContextWithInterval.SetRolledLogsExcludePattern("std_final");
			this.conf.Set(YarnConfiguration.NmLogDirs, localLogDir.GetAbsolutePath());
			this.conf.Set(YarnConfiguration.NmRemoteAppLogDir, this.remoteRootLogDir.GetAbsolutePath
				());
			this.conf.SetLong(YarnConfiguration.NmLogAggregationRollMonitoringIntervalSeconds
				, 3600);
			if (retentionSizeLimitation)
			{
				// set the retention size as 1. The number of logs for one application
				// in one NM should be 1.
				this.conf.SetInt(YarnConfiguration.NmPrefix + "log-aggregation.num-log-files-per-app"
					, 1);
			}
			// by setting this configuration, the log files will not be deleted immediately after
			// they are aggregated to remote directory.
			// We could use it to test whether the previous aggregated log files will be aggregated
			// again in next cycle.
			this.conf.SetLong(YarnConfiguration.DebugNmDeleteDelaySec, 3600);
			ApplicationId application = BuilderUtils.NewApplicationId(123456, 1);
			ApplicationAttemptId appAttemptId = BuilderUtils.NewApplicationAttemptId(application
				, 1);
			ContainerId container = BuilderUtils.NewContainerId(appAttemptId, 1);
			Context context = Org.Mockito.Mockito.Spy(this.context);
			ConcurrentMap<ApplicationId, Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
				> maps = new ConcurrentHashMap<ApplicationId, Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
				>();
			Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
				 app = Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
				>();
			IDictionary<ContainerId, Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
				> containers = new Dictionary<ContainerId, Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
				>();
			containers[container] = Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
				>();
			maps[application] = app;
			Org.Mockito.Mockito.When(app.GetContainers()).ThenReturn(containers);
			Org.Mockito.Mockito.When(context.GetApplications()).ThenReturn(maps);
			LogAggregationService logAggregationService = new LogAggregationService(dispatcher
				, context, this.delSrvc, base.dirsHandler);
			logAggregationService.Init(this.conf);
			logAggregationService.Start();
			// AppLogDir should be created
			FilePath appLogDir = new FilePath(localLogDir, ConverterUtils.ToString(application
				));
			appLogDir.Mkdir();
			logAggregationService.Handle(new LogHandlerAppStartedEvent(application, this.user
				, null, ContainerLogsRetentionPolicy.AllContainers, this.acls, logAggregationContextWithInterval
				));
			TestLogAggregationService.LogFileStatusInLastCycle logFileStatusInLastCycle = null;
			// Simulate log-file creation
			// create std_final in log directory which will not be aggregated
			// until the app finishes.
			string[] logFiles1WithFinalLog = new string[] { "stdout", "stderr", "syslog", "std_final"
				 };
			string[] logFiles1 = new string[] { "stdout", "stderr", "syslog" };
			WriteContainerLogs(appLogDir, container, logFiles1WithFinalLog);
			// Do log aggregation
			AppLogAggregatorImpl aggregator = (AppLogAggregatorImpl)logAggregationService.GetAppLogAggregators
				()[application];
			aggregator.DoLogAggregationOutOfBand();
			if (retentionSizeLimitation)
			{
				NUnit.Framework.Assert.IsTrue(WaitAndCheckLogNum(logAggregationService, application
					, 50, 1, true, null));
			}
			else
			{
				NUnit.Framework.Assert.IsTrue(WaitAndCheckLogNum(logAggregationService, application
					, 50, 1, false, null));
			}
			// Container logs should be uploaded
			logFileStatusInLastCycle = VerifyContainerLogs(logAggregationService, application
				, new ContainerId[] { container }, logFiles1, 3, true);
			foreach (string logFile in logFiles1)
			{
				NUnit.Framework.Assert.IsTrue(logFileStatusInLastCycle.GetLogFileTypesInLastCycle
					().Contains(logFile));
			}
			// Make sure the std_final is not uploaded.
			NUnit.Framework.Assert.IsFalse(logFileStatusInLastCycle.GetLogFileTypesInLastCycle
				().Contains("std_final"));
			Sharpen.Thread.Sleep(2000);
			// There is no log generated at this time. Do the log aggregation again.
			aggregator.DoLogAggregationOutOfBand();
			// Same logs will not be aggregated again.
			// Only one aggregated log file in Remote file directory.
			NUnit.Framework.Assert.AreEqual(NumOfLogsAvailable(logAggregationService, application
				, true, null), 1);
			Sharpen.Thread.Sleep(2000);
			// Do log aggregation
			string[] logFiles2 = new string[] { "stdout_1", "stderr_1", "syslog_1" };
			WriteContainerLogs(appLogDir, container, logFiles2);
			aggregator.DoLogAggregationOutOfBand();
			if (retentionSizeLimitation)
			{
				NUnit.Framework.Assert.IsTrue(WaitAndCheckLogNum(logAggregationService, application
					, 50, 1, true, logFileStatusInLastCycle.GetLogFilePathInLastCycle()));
			}
			else
			{
				NUnit.Framework.Assert.IsTrue(WaitAndCheckLogNum(logAggregationService, application
					, 50, 2, false, null));
			}
			// Container logs should be uploaded
			logFileStatusInLastCycle = VerifyContainerLogs(logAggregationService, application
				, new ContainerId[] { container }, logFiles2, 3, true);
			foreach (string logFile_1 in logFiles2)
			{
				NUnit.Framework.Assert.IsTrue(logFileStatusInLastCycle.GetLogFileTypesInLastCycle
					().Contains(logFile_1));
			}
			// Make sure the std_final is not uploaded.
			NUnit.Framework.Assert.IsFalse(logFileStatusInLastCycle.GetLogFileTypesInLastCycle
				().Contains("std_final"));
			Sharpen.Thread.Sleep(2000);
			// create another logs
			string[] logFiles3 = new string[] { "stdout_2", "stderr_2", "syslog_2" };
			WriteContainerLogs(appLogDir, container, logFiles3);
			logAggregationService.Handle(new LogHandlerContainerFinishedEvent(container, 0));
			dispatcher.Await();
			logAggregationService.Handle(new LogHandlerAppFinishedEvent(application));
			if (retentionSizeLimitation)
			{
				NUnit.Framework.Assert.IsTrue(WaitAndCheckLogNum(logAggregationService, application
					, 50, 1, true, logFileStatusInLastCycle.GetLogFilePathInLastCycle()));
			}
			else
			{
				NUnit.Framework.Assert.IsTrue(WaitAndCheckLogNum(logAggregationService, application
					, 50, 3, false, null));
			}
			// the app is finished. The log "std_final" should be aggregated this time.
			string[] logFiles3WithFinalLog = new string[] { "stdout_2", "stderr_2", "syslog_2"
				, "std_final" };
			VerifyContainerLogs(logAggregationService, application, new ContainerId[] { container
				 }, logFiles3WithFinalLog, 4, true);
			logAggregationService.Stop();
			NUnit.Framework.Assert.AreEqual(0, logAggregationService.GetNumAggregators());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestAddNewTokenSentFromRMForLogAggregation()
		{
			Configuration conf = new YarnConfiguration();
			conf.Set(CommonConfigurationKeysPublic.HadoopSecurityAuthentication, "kerberos");
			UserGroupInformation.SetConfiguration(conf);
			ApplicationId application1 = BuilderUtils.NewApplicationId(1234, 1);
			Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
				 mockApp = Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
				>();
			Org.Mockito.Mockito.When(mockApp.GetContainers()).ThenReturn(new Dictionary<ContainerId
				, Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
				>());
			this.context.GetApplications()[application1] = mockApp;
			LogAggregationService logAggregationService = new LogAggregationService(dispatcher
				, this.context, this.delSrvc, base.dirsHandler);
			logAggregationService.Init(this.conf);
			logAggregationService.Start();
			logAggregationService.Handle(new LogHandlerAppStartedEvent(application1, this.user
				, null, ContainerLogsRetentionPolicy.AllContainers, this.acls, Org.Apache.Hadoop.Yarn.Util.Records
				.NewRecord<LogAggregationContext>()));
			// Inject new token for log-aggregation after app log-aggregator init
			Org.Apache.Hadoop.IO.Text userText1 = new Org.Apache.Hadoop.IO.Text("user1");
			RMDelegationTokenIdentifier dtId1 = new RMDelegationTokenIdentifier(userText1, new 
				Org.Apache.Hadoop.IO.Text("renewer1"), userText1);
			Org.Apache.Hadoop.Security.Token.Token<RMDelegationTokenIdentifier> token1 = new 
				Org.Apache.Hadoop.Security.Token.Token<RMDelegationTokenIdentifier>(dtId1.GetBytes
				(), Sharpen.Runtime.GetBytesForString("password1"), dtId1.GetKind(), new Org.Apache.Hadoop.IO.Text
				("service1"));
			Credentials credentials = new Credentials();
			credentials.AddToken(userText1, token1);
			this.context.GetSystemCredentialsForApps()[application1] = credentials;
			logAggregationService.Handle(new LogHandlerAppFinishedEvent(application1));
			UserGroupInformation ugi = ((AppLogAggregatorImpl)logAggregationService.GetAppLogAggregators
				()[application1]).GetUgi();
			GenericTestUtils.WaitFor(new _Supplier_1560(ugi, token1), 1000, 20000);
			logAggregationService.Stop();
		}

		private sealed class _Supplier_1560 : Supplier<bool>
		{
			public _Supplier_1560(UserGroupInformation ugi, Org.Apache.Hadoop.Security.Token.Token
				<RMDelegationTokenIdentifier> token1)
			{
				this.ugi = ugi;
				this.token1 = token1;
			}

			public bool Get()
			{
				bool hasNewToken = false;
				foreach (Org.Apache.Hadoop.Security.Token.Token<object> token in ugi.GetCredentials
					().GetAllTokens())
				{
					if (token.Equals(token1))
					{
						hasNewToken = true;
					}
				}
				return hasNewToken;
			}

			private readonly UserGroupInformation ugi;

			private readonly Org.Apache.Hadoop.Security.Token.Token<RMDelegationTokenIdentifier
				> token1;
		}

		/// <exception cref="System.IO.IOException"/>
		private int NumOfLogsAvailable(LogAggregationService logAggregationService, ApplicationId
			 appId, bool sizeLimited, string lastLogFile)
		{
			Path appLogDir = logAggregationService.GetRemoteAppLogDir(appId, this.user);
			RemoteIterator<FileStatus> nodeFiles = null;
			try
			{
				Path qualifiedLogDir = FileContext.GetFileContext(this.conf).MakeQualified(appLogDir
					);
				nodeFiles = FileContext.GetFileContext(qualifiedLogDir.ToUri(), this.conf).ListStatus
					(appLogDir);
			}
			catch (FileNotFoundException)
			{
				return -1;
			}
			int count = 0;
			while (nodeFiles.HasNext())
			{
				FileStatus status = nodeFiles.Next();
				string filename = status.GetPath().GetName();
				if (filename.Contains(LogAggregationUtils.TmpFileSuffix) || (lastLogFile != null 
					&& filename.Contains(lastLogFile) && sizeLimited))
				{
					return -1;
				}
				if (filename.Contains(LogAggregationUtils.GetNodeString(logAggregationService.GetNodeId
					())))
				{
					count++;
				}
			}
			return count;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		private bool WaitAndCheckLogNum(LogAggregationService logAggregationService, ApplicationId
			 application, int maxAttempts, int expectNum, bool sizeLimited, string lastLogFile
			)
		{
			int count = 0;
			while (NumOfLogsAvailable(logAggregationService, application, sizeLimited, lastLogFile
				) != expectNum && count <= maxAttempts)
			{
				Sharpen.Thread.Sleep(500);
				count++;
			}
			return NumOfLogsAvailable(logAggregationService, application, sizeLimited, lastLogFile
				) == expectNum;
		}

		private class LogFileStatusInLastCycle
		{
			private string logFilePathInLastCycle;

			private IList<string> logFileTypesInLastCycle;

			public LogFileStatusInLastCycle(string logFilePathInLastCycle, IList<string> logFileTypesInLastCycle
				)
			{
				this.logFilePathInLastCycle = logFilePathInLastCycle;
				this.logFileTypesInLastCycle = logFileTypesInLastCycle;
			}

			public virtual string GetLogFilePathInLastCycle()
			{
				return this.logFilePathInLastCycle;
			}

			public virtual IList<string> GetLogFileTypesInLastCycle()
			{
				return this.logFileTypesInLastCycle;
			}
		}
	}
}
