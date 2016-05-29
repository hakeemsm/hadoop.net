using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using Com.Google.Common.Collect;
using NUnit.Framework;
using Org.Apache.Commons.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Api;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Api.Protocolrecords.Impl.PB;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer.Event;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Recovery;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Security;
using Org.Apache.Hadoop.Yarn.Server.Security;
using Org.Apache.Hadoop.Yarn.Server.Utils;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Mockito;
using Org.Mockito.Internal.Matchers;
using Org.Mockito.Invocation;
using Org.Mockito.Stubbing;
using Sharpen;
using Sharpen.Reflect;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer
{
	public class TestResourceLocalizationService
	{
		internal static readonly Path basedir = new Path("target", typeof(TestResourceLocalizationService
			).FullName);

		internal static Org.Apache.Hadoop.Ipc.Server mockServer;

		private Configuration conf;

		private AbstractFileSystem spylfs;

		private FileContext lfs;

		private NodeManager.NMContext nmContext;

		[BeforeClass]
		public static void SetupClass()
		{
			mockServer = Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Ipc.Server>();
			Org.Mockito.Mockito.DoReturn(new IPEndPoint(123)).When(mockServer).GetListenerAddress
				();
		}

		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public virtual void Setup()
		{
			conf = new Configuration();
			spylfs = Org.Mockito.Mockito.Spy(FileContext.GetLocalFSFileContext().GetDefaultFileSystem
				());
			lfs = FileContext.GetFileContext(spylfs, conf);
			string logDir = lfs.MakeQualified(new Path(basedir, "logdir ")).ToString();
			conf.Set(YarnConfiguration.NmLogDirs, logDir);
			nmContext = new NodeManager.NMContext(new NMContainerTokenSecretManager(conf), new 
				NMTokenSecretManagerInNM(), null, new ApplicationACLsManager(conf), new NMNullStateStoreService
				());
		}

		/// <exception cref="System.IO.IOException"/>
		[TearDown]
		public virtual void Cleanup()
		{
			conf = null;
			FileUtils.DeleteDirectory(new FilePath(basedir.ToString()));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestLocalizationInit()
		{
			conf.Set(CommonConfigurationKeys.FsPermissionsUmaskKey, "077");
			AsyncDispatcher dispatcher = new AsyncDispatcher();
			dispatcher.Init(new Configuration());
			ContainerExecutor exec = Org.Mockito.Mockito.Mock<ContainerExecutor>();
			DeletionService delService = Org.Mockito.Mockito.Spy(new DeletionService(exec));
			delService.Init(conf);
			delService.Start();
			IList<Path> localDirs = new AList<Path>();
			string[] sDirs = new string[4];
			for (int i = 0; i < 4; ++i)
			{
				localDirs.AddItem(lfs.MakeQualified(new Path(basedir, i + string.Empty)));
				sDirs[i] = localDirs[i].ToString();
			}
			conf.SetStrings(YarnConfiguration.NmLocalDirs, sDirs);
			LocalDirsHandlerService diskhandler = new LocalDirsHandlerService();
			diskhandler.Init(conf);
			ResourceLocalizationService locService = Org.Mockito.Mockito.Spy(new ResourceLocalizationService
				(dispatcher, exec, delService, diskhandler, nmContext));
			Org.Mockito.Mockito.DoReturn(lfs).When(locService).GetLocalFileContext(Matchers.IsA
				<Configuration>());
			try
			{
				dispatcher.Start();
				// initialize ResourceLocalizationService
				locService.Init(conf);
				FsPermission defaultPerm = new FsPermission((short)0x1ed);
				// verify directory creation
				foreach (Path p in localDirs)
				{
					p = new Path((new URI(p.ToString())).GetPath());
					Path usercache = new Path(p, ContainerLocalizer.Usercache);
					Org.Mockito.Mockito.Verify(spylfs).Mkdir(Matchers.Eq(usercache), Matchers.Eq(defaultPerm
						), Matchers.Eq(true));
					Path publicCache = new Path(p, ContainerLocalizer.Filecache);
					Org.Mockito.Mockito.Verify(spylfs).Mkdir(Matchers.Eq(publicCache), Matchers.Eq(defaultPerm
						), Matchers.Eq(true));
					Path nmPriv = new Path(p, ResourceLocalizationService.NmPrivateDir);
					Org.Mockito.Mockito.Verify(spylfs).Mkdir(Matchers.Eq(nmPriv), Matchers.Eq(ResourceLocalizationService
						.NmPrivatePerm), Matchers.Eq(true));
				}
			}
			finally
			{
				dispatcher.Stop();
				delService.Stop();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.URISyntaxException"/>
		[NUnit.Framework.Test]
		public virtual void TestDirectoryCleanupOnNewlyCreatedStateStore()
		{
			conf.Set(CommonConfigurationKeys.FsPermissionsUmaskKey, "077");
			AsyncDispatcher dispatcher = new AsyncDispatcher();
			dispatcher.Init(new Configuration());
			ContainerExecutor exec = Org.Mockito.Mockito.Mock<ContainerExecutor>();
			DeletionService delService = Org.Mockito.Mockito.Spy(new DeletionService(exec));
			delService.Init(conf);
			delService.Start();
			IList<Path> localDirs = new AList<Path>();
			string[] sDirs = new string[4];
			for (int i = 0; i < 4; ++i)
			{
				localDirs.AddItem(lfs.MakeQualified(new Path(basedir, i + string.Empty)));
				sDirs[i] = localDirs[i].ToString();
			}
			conf.SetStrings(YarnConfiguration.NmLocalDirs, sDirs);
			LocalDirsHandlerService diskhandler = new LocalDirsHandlerService();
			diskhandler.Init(conf);
			NMStateStoreService nmStateStoreService = Org.Mockito.Mockito.Mock<NMStateStoreService
				>();
			Org.Mockito.Mockito.When(nmStateStoreService.CanRecover()).ThenReturn(true);
			Org.Mockito.Mockito.When(nmStateStoreService.IsNewlyCreated()).ThenReturn(true);
			ResourceLocalizationService locService = Org.Mockito.Mockito.Spy(new ResourceLocalizationService
				(dispatcher, exec, delService, diskhandler, nmContext));
			Org.Mockito.Mockito.DoReturn(lfs).When(locService).GetLocalFileContext(Matchers.IsA
				<Configuration>());
			try
			{
				dispatcher.Start();
				// initialize ResourceLocalizationService
				locService.Init(conf);
				FsPermission defaultPerm = new FsPermission((short)0x1ed);
				// verify directory creation
				foreach (Path p in localDirs)
				{
					p = new Path((new URI(p.ToString())).GetPath());
					Path usercache = new Path(p, ContainerLocalizer.Usercache);
					Org.Mockito.Mockito.Verify(spylfs).Rename(Matchers.Eq(usercache), Matchers.Any<Path
						>(), Matchers.Any<Options.Rename>());
					Org.Mockito.Mockito.Verify(spylfs).Mkdir(Matchers.Eq(usercache), Matchers.Eq(defaultPerm
						), Matchers.Eq(true));
					Path publicCache = new Path(p, ContainerLocalizer.Filecache);
					Org.Mockito.Mockito.Verify(spylfs).Rename(Matchers.Eq(usercache), Matchers.Any<Path
						>(), Matchers.Any<Options.Rename>());
					Org.Mockito.Mockito.Verify(spylfs).Mkdir(Matchers.Eq(publicCache), Matchers.Eq(defaultPerm
						), Matchers.Eq(true));
					Path nmPriv = new Path(p, ResourceLocalizationService.NmPrivateDir);
					Org.Mockito.Mockito.Verify(spylfs).Rename(Matchers.Eq(usercache), Matchers.Any<Path
						>(), Matchers.Any<Options.Rename>());
					Org.Mockito.Mockito.Verify(spylfs).Mkdir(Matchers.Eq(nmPriv), Matchers.Eq(ResourceLocalizationService
						.NmPrivatePerm), Matchers.Eq(true));
				}
			}
			finally
			{
				dispatcher.Stop();
				delService.Stop();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestResourceRelease()
		{
			// mocked generics
			IList<Path> localDirs = new AList<Path>();
			string[] sDirs = new string[4];
			for (int i = 0; i < 4; ++i)
			{
				localDirs.AddItem(lfs.MakeQualified(new Path(basedir, i + string.Empty)));
				sDirs[i] = localDirs[i].ToString();
			}
			conf.SetStrings(YarnConfiguration.NmLocalDirs, sDirs);
			ResourceLocalizationService.LocalizerTracker mockLocallilzerTracker = Org.Mockito.Mockito.Mock
				<ResourceLocalizationService.LocalizerTracker>();
			DrainDispatcher dispatcher = new DrainDispatcher();
			dispatcher.Init(conf);
			dispatcher.Start();
			EventHandler<ApplicationEvent> applicationBus = Org.Mockito.Mockito.Mock<EventHandler
				>();
			dispatcher.Register(typeof(ApplicationEventType), applicationBus);
			EventHandler<ContainerEvent> containerBus = Org.Mockito.Mockito.Mock<EventHandler
				>();
			dispatcher.Register(typeof(ContainerEventType), containerBus);
			//Ignore actual localization
			EventHandler<LocalizerEvent> localizerBus = Org.Mockito.Mockito.Mock<EventHandler
				>();
			dispatcher.Register(typeof(LocalizerEventType), localizerBus);
			ContainerExecutor exec = Org.Mockito.Mockito.Mock<ContainerExecutor>();
			LocalDirsHandlerService dirsHandler = new LocalDirsHandlerService();
			dirsHandler.Init(conf);
			DeletionService delService = new DeletionService(exec);
			delService.Init(new Configuration());
			delService.Start();
			ResourceLocalizationService rawService = new ResourceLocalizationService(dispatcher
				, exec, delService, dirsHandler, nmContext);
			ResourceLocalizationService spyService = Org.Mockito.Mockito.Spy(rawService);
			Org.Mockito.Mockito.DoReturn(mockServer).When(spyService).CreateServer();
			Org.Mockito.Mockito.DoReturn(mockLocallilzerTracker).When(spyService).CreateLocalizerTracker
				(Matchers.IsA<Configuration>());
			Org.Mockito.Mockito.DoReturn(lfs).When(spyService).GetLocalFileContext(Matchers.IsA
				<Configuration>());
			try
			{
				spyService.Init(conf);
				spyService.Start();
				string user = "user0";
				// init application
				Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
					 app = Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
					>();
				ApplicationId appId = BuilderUtils.NewApplicationId(314159265358979L, 3);
				Org.Mockito.Mockito.When(app.GetUser()).ThenReturn(user);
				Org.Mockito.Mockito.When(app.GetAppId()).ThenReturn(appId);
				spyService.Handle(new ApplicationLocalizationEvent(LocalizationEventType.InitApplicationResources
					, app));
				dispatcher.Await();
				//Get a handle on the trackers after they're setup with INIT_APP_RESOURCES
				LocalResourcesTracker appTracker = spyService.GetLocalResourcesTracker(LocalResourceVisibility
					.Application, user, appId);
				LocalResourcesTracker privTracker = spyService.GetLocalResourcesTracker(LocalResourceVisibility
					.Private, user, appId);
				LocalResourcesTracker pubTracker = spyService.GetLocalResourcesTracker(LocalResourceVisibility
					.Public, user, appId);
				// init container.
				Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container c = 
					GetMockContainer(appId, 42, user);
				// init resources
				Random r = new Random();
				long seed = r.NextLong();
				System.Console.Out.WriteLine("SEED: " + seed);
				r.SetSeed(seed);
				// Send localization requests for one resource of each type.
				LocalResource privResource = GetPrivateMockedResource(r);
				LocalResourceRequest privReq = new LocalResourceRequest(privResource);
				LocalResource pubResource = GetPublicMockedResource(r);
				LocalResourceRequest pubReq = new LocalResourceRequest(pubResource);
				LocalResource pubResource2 = GetPublicMockedResource(r);
				LocalResourceRequest pubReq2 = new LocalResourceRequest(pubResource2);
				LocalResource appResource = GetAppMockedResource(r);
				LocalResourceRequest appReq = new LocalResourceRequest(appResource);
				IDictionary<LocalResourceVisibility, ICollection<LocalResourceRequest>> req = new 
					Dictionary<LocalResourceVisibility, ICollection<LocalResourceRequest>>();
				req[LocalResourceVisibility.Private] = Sharpen.Collections.SingletonList(privReq);
				req[LocalResourceVisibility.Public] = Sharpen.Collections.SingletonList(pubReq);
				req[LocalResourceVisibility.Application] = Sharpen.Collections.SingletonList(appReq
					);
				IDictionary<LocalResourceVisibility, ICollection<LocalResourceRequest>> req2 = new 
					Dictionary<LocalResourceVisibility, ICollection<LocalResourceRequest>>();
				req2[LocalResourceVisibility.Private] = Sharpen.Collections.SingletonList(privReq
					);
				req2[LocalResourceVisibility.Public] = Sharpen.Collections.SingletonList(pubReq2);
				ICollection<LocalResourceRequest> pubRsrcs = new HashSet<LocalResourceRequest>();
				pubRsrcs.AddItem(pubReq);
				pubRsrcs.AddItem(pubReq2);
				// Send Request event
				spyService.Handle(new ContainerLocalizationRequestEvent(c, req));
				spyService.Handle(new ContainerLocalizationRequestEvent(c, req2));
				dispatcher.Await();
				int privRsrcCount = 0;
				foreach (LocalizedResource lr in privTracker)
				{
					privRsrcCount++;
					NUnit.Framework.Assert.AreEqual("Incorrect reference count", 2, lr.GetRefCount());
					NUnit.Framework.Assert.AreEqual(privReq, lr.GetRequest());
				}
				NUnit.Framework.Assert.AreEqual(1, privRsrcCount);
				int pubRsrcCount = 0;
				foreach (LocalizedResource lr_1 in pubTracker)
				{
					pubRsrcCount++;
					NUnit.Framework.Assert.AreEqual("Incorrect reference count", 1, lr_1.GetRefCount(
						));
					pubRsrcs.Remove(lr_1.GetRequest());
				}
				NUnit.Framework.Assert.AreEqual(0, pubRsrcs.Count);
				NUnit.Framework.Assert.AreEqual(2, pubRsrcCount);
				int appRsrcCount = 0;
				foreach (LocalizedResource lr_2 in appTracker)
				{
					appRsrcCount++;
					NUnit.Framework.Assert.AreEqual("Incorrect reference count", 1, lr_2.GetRefCount(
						));
					NUnit.Framework.Assert.AreEqual(appReq, lr_2.GetRequest());
				}
				NUnit.Framework.Assert.AreEqual(1, appRsrcCount);
				//Send Cleanup Event
				spyService.Handle(new ContainerLocalizationCleanupEvent(c, req));
				Org.Mockito.Mockito.Verify(mockLocallilzerTracker).CleanupPrivLocalizers("container_314159265358979_0003_01_000042"
					);
				Sharpen.Collections.Remove(req2, LocalResourceVisibility.Private);
				spyService.Handle(new ContainerLocalizationCleanupEvent(c, req2));
				dispatcher.Await();
				pubRsrcs.AddItem(pubReq);
				pubRsrcs.AddItem(pubReq2);
				privRsrcCount = 0;
				foreach (LocalizedResource lr_3 in privTracker)
				{
					privRsrcCount++;
					NUnit.Framework.Assert.AreEqual("Incorrect reference count", 1, lr_3.GetRefCount(
						));
					NUnit.Framework.Assert.AreEqual(privReq, lr_3.GetRequest());
				}
				NUnit.Framework.Assert.AreEqual(1, privRsrcCount);
				pubRsrcCount = 0;
				foreach (LocalizedResource lr_4 in pubTracker)
				{
					pubRsrcCount++;
					NUnit.Framework.Assert.AreEqual("Incorrect reference count", 0, lr_4.GetRefCount(
						));
					pubRsrcs.Remove(lr_4.GetRequest());
				}
				NUnit.Framework.Assert.AreEqual(0, pubRsrcs.Count);
				NUnit.Framework.Assert.AreEqual(2, pubRsrcCount);
				appRsrcCount = 0;
				foreach (LocalizedResource lr_5 in appTracker)
				{
					appRsrcCount++;
				}
				NUnit.Framework.Assert.AreEqual(0, appRsrcCount);
			}
			finally
			{
				dispatcher.Stop();
				delService.Stop();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRecovery()
		{
			// mocked generics
			string user1 = "user1";
			string user2 = "user2";
			ApplicationId appId1 = ApplicationId.NewInstance(1, 1);
			ApplicationId appId2 = ApplicationId.NewInstance(1, 2);
			IList<Path> localDirs = new AList<Path>();
			string[] sDirs = new string[4];
			for (int i = 0; i < 4; ++i)
			{
				localDirs.AddItem(lfs.MakeQualified(new Path(basedir, i + string.Empty)));
				sDirs[i] = localDirs[i].ToString();
			}
			conf.SetStrings(YarnConfiguration.NmLocalDirs, sDirs);
			conf.SetBoolean(YarnConfiguration.NmRecoveryEnabled, true);
			NMMemoryStateStoreService stateStore = new NMMemoryStateStoreService();
			stateStore.Init(conf);
			stateStore.Start();
			DrainDispatcher dispatcher = new DrainDispatcher();
			dispatcher.Init(conf);
			dispatcher.Start();
			EventHandler<ApplicationEvent> applicationBus = Org.Mockito.Mockito.Mock<EventHandler
				>();
			dispatcher.Register(typeof(ApplicationEventType), applicationBus);
			EventHandler<ContainerEvent> containerBus = Org.Mockito.Mockito.Mock<EventHandler
				>();
			dispatcher.Register(typeof(ContainerEventType), containerBus);
			//Ignore actual localization
			EventHandler<LocalizerEvent> localizerBus = Org.Mockito.Mockito.Mock<EventHandler
				>();
			dispatcher.Register(typeof(LocalizerEventType), localizerBus);
			LocalDirsHandlerService dirsHandler = new LocalDirsHandlerService();
			dirsHandler.Init(conf);
			ResourceLocalizationService spyService = CreateSpyService(dispatcher, dirsHandler
				, stateStore);
			try
			{
				spyService.Init(conf);
				spyService.Start();
				Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
					 app1 = Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
					>();
				Org.Mockito.Mockito.When(app1.GetUser()).ThenReturn(user1);
				Org.Mockito.Mockito.When(app1.GetAppId()).ThenReturn(appId1);
				Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
					 app2 = Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
					>();
				Org.Mockito.Mockito.When(app2.GetUser()).ThenReturn(user2);
				Org.Mockito.Mockito.When(app2.GetAppId()).ThenReturn(appId2);
				spyService.Handle(new ApplicationLocalizationEvent(LocalizationEventType.InitApplicationResources
					, app1));
				spyService.Handle(new ApplicationLocalizationEvent(LocalizationEventType.InitApplicationResources
					, app2));
				dispatcher.Await();
				//Get a handle on the trackers after they're setup with INIT_APP_RESOURCES
				LocalResourcesTracker appTracker1 = spyService.GetLocalResourcesTracker(LocalResourceVisibility
					.Application, user1, appId1);
				LocalResourcesTracker privTracker1 = spyService.GetLocalResourcesTracker(LocalResourceVisibility
					.Private, user1, null);
				LocalResourcesTracker appTracker2 = spyService.GetLocalResourcesTracker(LocalResourceVisibility
					.Application, user2, appId2);
				LocalResourcesTracker pubTracker = spyService.GetLocalResourcesTracker(LocalResourceVisibility
					.Public, null, null);
				// init containers
				Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container c1
					 = GetMockContainer(appId1, 1, user1);
				Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container c2
					 = GetMockContainer(appId2, 2, user2);
				// init resources
				Random r = new Random();
				long seed = r.NextLong();
				System.Console.Out.WriteLine("SEED: " + seed);
				r.SetSeed(seed);
				// Send localization requests of each type.
				LocalResource privResource1 = GetPrivateMockedResource(r);
				LocalResourceRequest privReq1 = new LocalResourceRequest(privResource1);
				LocalResource privResource2 = GetPrivateMockedResource(r);
				LocalResourceRequest privReq2 = new LocalResourceRequest(privResource2);
				LocalResource pubResource1 = GetPublicMockedResource(r);
				LocalResourceRequest pubReq1 = new LocalResourceRequest(pubResource1);
				LocalResource pubResource2 = GetPublicMockedResource(r);
				LocalResourceRequest pubReq2 = new LocalResourceRequest(pubResource2);
				LocalResource appResource1 = GetAppMockedResource(r);
				LocalResourceRequest appReq1 = new LocalResourceRequest(appResource1);
				LocalResource appResource2 = GetAppMockedResource(r);
				LocalResourceRequest appReq2 = new LocalResourceRequest(appResource2);
				LocalResource appResource3 = GetAppMockedResource(r);
				LocalResourceRequest appReq3 = new LocalResourceRequest(appResource3);
				IDictionary<LocalResourceVisibility, ICollection<LocalResourceRequest>> req1 = new 
					Dictionary<LocalResourceVisibility, ICollection<LocalResourceRequest>>();
				req1[LocalResourceVisibility.Private] = Arrays.AsList(new LocalResourceRequest[] 
					{ privReq1, privReq2 });
				req1[LocalResourceVisibility.Public] = Sharpen.Collections.SingletonList(pubReq1);
				req1[LocalResourceVisibility.Application] = Sharpen.Collections.SingletonList(appReq1
					);
				IDictionary<LocalResourceVisibility, ICollection<LocalResourceRequest>> req2 = new 
					Dictionary<LocalResourceVisibility, ICollection<LocalResourceRequest>>();
				req2[LocalResourceVisibility.Application] = Arrays.AsList(new LocalResourceRequest
					[] { appReq2, appReq3 });
				req2[LocalResourceVisibility.Public] = Sharpen.Collections.SingletonList(pubReq2);
				// Send Request event
				spyService.Handle(new ContainerLocalizationRequestEvent(c1, req1));
				spyService.Handle(new ContainerLocalizationRequestEvent(c2, req2));
				dispatcher.Await();
				// Simulate start of localization for all resources
				privTracker1.GetPathForLocalization(privReq1, dirsHandler.GetLocalPathForWrite(ContainerLocalizer
					.Usercache + user1), null);
				privTracker1.GetPathForLocalization(privReq2, dirsHandler.GetLocalPathForWrite(ContainerLocalizer
					.Usercache + user1), null);
				LocalizedResource privLr1 = privTracker1.GetLocalizedResource(privReq1);
				LocalizedResource privLr2 = privTracker1.GetLocalizedResource(privReq2);
				appTracker1.GetPathForLocalization(appReq1, dirsHandler.GetLocalPathForWrite(ContainerLocalizer
					.Appcache + appId1), null);
				LocalizedResource appLr1 = appTracker1.GetLocalizedResource(appReq1);
				appTracker2.GetPathForLocalization(appReq2, dirsHandler.GetLocalPathForWrite(ContainerLocalizer
					.Appcache + appId2), null);
				LocalizedResource appLr2 = appTracker2.GetLocalizedResource(appReq2);
				appTracker2.GetPathForLocalization(appReq3, dirsHandler.GetLocalPathForWrite(ContainerLocalizer
					.Appcache + appId2), null);
				LocalizedResource appLr3 = appTracker2.GetLocalizedResource(appReq3);
				pubTracker.GetPathForLocalization(pubReq1, dirsHandler.GetLocalPathForWrite(ContainerLocalizer
					.Filecache), null);
				LocalizedResource pubLr1 = pubTracker.GetLocalizedResource(pubReq1);
				pubTracker.GetPathForLocalization(pubReq2, dirsHandler.GetLocalPathForWrite(ContainerLocalizer
					.Filecache), null);
				LocalizedResource pubLr2 = pubTracker.GetLocalizedResource(pubReq2);
				// Simulate completion of localization for most resources with
				// possibly different sizes than in the request
				NUnit.Framework.Assert.IsNotNull("Localization not started", privLr1.GetLocalPath
					());
				privTracker1.Handle(new ResourceLocalizedEvent(privReq1, privLr1.GetLocalPath(), 
					privLr1.GetSize() + 5));
				NUnit.Framework.Assert.IsNotNull("Localization not started", privLr2.GetLocalPath
					());
				privTracker1.Handle(new ResourceLocalizedEvent(privReq2, privLr2.GetLocalPath(), 
					privLr2.GetSize() + 10));
				NUnit.Framework.Assert.IsNotNull("Localization not started", appLr1.GetLocalPath(
					));
				appTracker1.Handle(new ResourceLocalizedEvent(appReq1, appLr1.GetLocalPath(), appLr1
					.GetSize()));
				NUnit.Framework.Assert.IsNotNull("Localization not started", appLr3.GetLocalPath(
					));
				appTracker2.Handle(new ResourceLocalizedEvent(appReq3, appLr3.GetLocalPath(), appLr3
					.GetSize() + 7));
				NUnit.Framework.Assert.IsNotNull("Localization not started", pubLr1.GetLocalPath(
					));
				pubTracker.Handle(new ResourceLocalizedEvent(pubReq1, pubLr1.GetLocalPath(), pubLr1
					.GetSize() + 1000));
				NUnit.Framework.Assert.IsNotNull("Localization not started", pubLr2.GetLocalPath(
					));
				pubTracker.Handle(new ResourceLocalizedEvent(pubReq2, pubLr2.GetLocalPath(), pubLr2
					.GetSize() + 99999));
				dispatcher.Await();
				NUnit.Framework.Assert.AreEqual(ResourceState.Localized, privLr1.GetState());
				NUnit.Framework.Assert.AreEqual(ResourceState.Localized, privLr2.GetState());
				NUnit.Framework.Assert.AreEqual(ResourceState.Localized, appLr1.GetState());
				NUnit.Framework.Assert.AreEqual(ResourceState.Downloading, appLr2.GetState());
				NUnit.Framework.Assert.AreEqual(ResourceState.Localized, appLr3.GetState());
				NUnit.Framework.Assert.AreEqual(ResourceState.Localized, pubLr1.GetState());
				NUnit.Framework.Assert.AreEqual(ResourceState.Localized, pubLr2.GetState());
				// restart and recover
				spyService = CreateSpyService(dispatcher, dirsHandler, stateStore);
				spyService.Init(conf);
				spyService.RecoverLocalizedResources(stateStore.LoadLocalizationState());
				dispatcher.Await();
				appTracker1 = spyService.GetLocalResourcesTracker(LocalResourceVisibility.Application
					, user1, appId1);
				privTracker1 = spyService.GetLocalResourcesTracker(LocalResourceVisibility.Private
					, user1, null);
				appTracker2 = spyService.GetLocalResourcesTracker(LocalResourceVisibility.Application
					, user2, appId2);
				pubTracker = spyService.GetLocalResourcesTracker(LocalResourceVisibility.Public, 
					null, null);
				LocalizedResource recoveredRsrc = privTracker1.GetLocalizedResource(privReq1);
				NUnit.Framework.Assert.AreEqual(privReq1, recoveredRsrc.GetRequest());
				NUnit.Framework.Assert.AreEqual(privLr1.GetLocalPath(), recoveredRsrc.GetLocalPath
					());
				NUnit.Framework.Assert.AreEqual(privLr1.GetSize(), recoveredRsrc.GetSize());
				NUnit.Framework.Assert.AreEqual(ResourceState.Localized, recoveredRsrc.GetState()
					);
				recoveredRsrc = privTracker1.GetLocalizedResource(privReq2);
				NUnit.Framework.Assert.AreEqual(privReq2, recoveredRsrc.GetRequest());
				NUnit.Framework.Assert.AreEqual(privLr2.GetLocalPath(), recoveredRsrc.GetLocalPath
					());
				NUnit.Framework.Assert.AreEqual(privLr2.GetSize(), recoveredRsrc.GetSize());
				NUnit.Framework.Assert.AreEqual(ResourceState.Localized, recoveredRsrc.GetState()
					);
				recoveredRsrc = appTracker1.GetLocalizedResource(appReq1);
				NUnit.Framework.Assert.AreEqual(appReq1, recoveredRsrc.GetRequest());
				NUnit.Framework.Assert.AreEqual(appLr1.GetLocalPath(), recoveredRsrc.GetLocalPath
					());
				NUnit.Framework.Assert.AreEqual(appLr1.GetSize(), recoveredRsrc.GetSize());
				NUnit.Framework.Assert.AreEqual(ResourceState.Localized, recoveredRsrc.GetState()
					);
				recoveredRsrc = appTracker2.GetLocalizedResource(appReq2);
				NUnit.Framework.Assert.IsNull("in-progress resource should not be present", recoveredRsrc
					);
				recoveredRsrc = appTracker2.GetLocalizedResource(appReq3);
				NUnit.Framework.Assert.AreEqual(appReq3, recoveredRsrc.GetRequest());
				NUnit.Framework.Assert.AreEqual(appLr3.GetLocalPath(), recoveredRsrc.GetLocalPath
					());
				NUnit.Framework.Assert.AreEqual(appLr3.GetSize(), recoveredRsrc.GetSize());
				NUnit.Framework.Assert.AreEqual(ResourceState.Localized, recoveredRsrc.GetState()
					);
			}
			finally
			{
				dispatcher.Stop();
				stateStore.Close();
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestLocalizerRunnerException()
		{
			// mocked generics
			DrainDispatcher dispatcher = new DrainDispatcher();
			dispatcher.Init(conf);
			dispatcher.Start();
			EventHandler<ApplicationEvent> applicationBus = Org.Mockito.Mockito.Mock<EventHandler
				>();
			dispatcher.Register(typeof(ApplicationEventType), applicationBus);
			EventHandler<ContainerEvent> containerBus = Org.Mockito.Mockito.Mock<EventHandler
				>();
			dispatcher.Register(typeof(ContainerEventType), containerBus);
			ContainerExecutor exec = Org.Mockito.Mockito.Mock<ContainerExecutor>();
			LocalDirsHandlerService dirsHandler = new LocalDirsHandlerService();
			LocalDirsHandlerService dirsHandlerSpy = Org.Mockito.Mockito.Spy(dirsHandler);
			dirsHandlerSpy.Init(conf);
			DeletionService delServiceReal = new DeletionService(exec);
			DeletionService delService = Org.Mockito.Mockito.Spy(delServiceReal);
			delService.Init(new Configuration());
			delService.Start();
			ResourceLocalizationService rawService = new ResourceLocalizationService(dispatcher
				, exec, delService, dirsHandlerSpy, nmContext);
			ResourceLocalizationService spyService = Org.Mockito.Mockito.Spy(rawService);
			Org.Mockito.Mockito.DoReturn(mockServer).When(spyService).CreateServer();
			try
			{
				spyService.Init(conf);
				spyService.Start();
				// init application
				Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
					 app = Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
					>();
				ApplicationId appId = BuilderUtils.NewApplicationId(314159265358979L, 3);
				Org.Mockito.Mockito.When(app.GetUser()).ThenReturn("user0");
				Org.Mockito.Mockito.When(app.GetAppId()).ThenReturn(appId);
				spyService.Handle(new ApplicationLocalizationEvent(LocalizationEventType.InitApplicationResources
					, app));
				dispatcher.Await();
				Random r = new Random();
				long seed = r.NextLong();
				System.Console.Out.WriteLine("SEED: " + seed);
				r.SetSeed(seed);
				Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container c = 
					GetMockContainer(appId, 42, "user0");
				LocalResource resource1 = GetPrivateMockedResource(r);
				System.Console.Out.WriteLine("Here 4");
				LocalResourceRequest req1 = new LocalResourceRequest(resource1);
				IDictionary<LocalResourceVisibility, ICollection<LocalResourceRequest>> rsrcs = new 
					Dictionary<LocalResourceVisibility, ICollection<LocalResourceRequest>>();
				IList<LocalResourceRequest> privateResourceList = new AList<LocalResourceRequest>
					();
				privateResourceList.AddItem(req1);
				rsrcs[LocalResourceVisibility.Private] = privateResourceList;
				Constructor<object>[] constructors = typeof(FSError).GetDeclaredConstructors();
				FSError fsError = (FSError)constructors[0].NewInstance(new IOException("Disk Error"
					));
				Org.Mockito.Mockito.DoThrow(fsError).When(dirsHandlerSpy).GetLocalPathForWrite(Matchers.IsA
					<string>());
				spyService.Handle(new ContainerLocalizationRequestEvent(c, rsrcs));
				Sharpen.Thread.Sleep(1000);
				dispatcher.Await();
				// Verify if ContainerResourceFailedEvent is invoked on FSError
				Org.Mockito.Mockito.Verify(containerBus).Handle(Matchers.IsA<ContainerResourceFailedEvent
					>());
			}
			finally
			{
				spyService.Stop();
				dispatcher.Stop();
				delService.Stop();
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestLocalizationHeartbeat()
		{
			// mocked generics
			IList<Path> localDirs = new AList<Path>();
			string[] sDirs = new string[1];
			// Making sure that we have only one local disk so that it will only be
			// selected for consecutive resource localization calls.  This is required
			// to test LocalCacheDirectoryManager.
			localDirs.AddItem(lfs.MakeQualified(new Path(basedir, 0 + string.Empty)));
			sDirs[0] = localDirs[0].ToString();
			conf.SetStrings(YarnConfiguration.NmLocalDirs, sDirs);
			// Adding configuration to make sure there is only one file per
			// directory
			conf.Set(YarnConfiguration.NmLocalCacheMaxFilesPerDirectory, "37");
			DrainDispatcher dispatcher = new DrainDispatcher();
			dispatcher.Init(conf);
			dispatcher.Start();
			EventHandler<ApplicationEvent> applicationBus = Org.Mockito.Mockito.Mock<EventHandler
				>();
			dispatcher.Register(typeof(ApplicationEventType), applicationBus);
			EventHandler<ContainerEvent> containerBus = Org.Mockito.Mockito.Mock<EventHandler
				>();
			dispatcher.Register(typeof(ContainerEventType), containerBus);
			ContainerExecutor exec = Org.Mockito.Mockito.Mock<ContainerExecutor>();
			LocalDirsHandlerService dirsHandler = new LocalDirsHandlerService();
			dirsHandler.Init(conf);
			DeletionService delServiceReal = new DeletionService(exec);
			DeletionService delService = Org.Mockito.Mockito.Spy(delServiceReal);
			delService.Init(new Configuration());
			delService.Start();
			ResourceLocalizationService rawService = new ResourceLocalizationService(dispatcher
				, exec, delService, dirsHandler, nmContext);
			ResourceLocalizationService spyService = Org.Mockito.Mockito.Spy(rawService);
			Org.Mockito.Mockito.DoReturn(mockServer).When(spyService).CreateServer();
			Org.Mockito.Mockito.DoReturn(lfs).When(spyService).GetLocalFileContext(Matchers.IsA
				<Configuration>());
			FsPermission defaultPermission = FsPermission.GetDirDefault().ApplyUMask(lfs.GetUMask
				());
			FsPermission nmPermission = ResourceLocalizationService.NmPrivatePerm.ApplyUMask(
				lfs.GetUMask());
			Path userDir = new Path(Sharpen.Runtime.Substring(sDirs[0], "file:".Length), ContainerLocalizer
				.Usercache);
			Path fileDir = new Path(Sharpen.Runtime.Substring(sDirs[0], "file:".Length), ContainerLocalizer
				.Filecache);
			Path sysDir = new Path(Sharpen.Runtime.Substring(sDirs[0], "file:".Length), ResourceLocalizationService
				.NmPrivateDir);
			FileStatus fs = new FileStatus(0, true, 1, 0, Runtime.CurrentTimeMillis(), 0, defaultPermission
				, string.Empty, string.Empty, new Path(sDirs[0]));
			FileStatus nmFs = new FileStatus(0, true, 1, 0, Runtime.CurrentTimeMillis(), 0, nmPermission
				, string.Empty, string.Empty, sysDir);
			Org.Mockito.Mockito.DoAnswer(new _Answer_866(userDir, fileDir, fs, nmFs)).When(spylfs
				).GetFileStatus(Matchers.IsA<Path>());
			try
			{
				spyService.Init(conf);
				spyService.Start();
				// init application
				Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
					 app = Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
					>();
				ApplicationId appId = BuilderUtils.NewApplicationId(314159265358979L, 3);
				Org.Mockito.Mockito.When(app.GetUser()).ThenReturn("user0");
				Org.Mockito.Mockito.When(app.GetAppId()).ThenReturn(appId);
				spyService.Handle(new ApplicationLocalizationEvent(LocalizationEventType.InitApplicationResources
					, app));
				ArgumentMatcher<ApplicationEvent> matchesAppInit = new _ArgumentMatcher_892(appId
					);
				dispatcher.Await();
				Org.Mockito.Mockito.Verify(applicationBus).Handle(Matchers.ArgThat(matchesAppInit
					));
				// init container rsrc, localizer
				Random r = new Random();
				long seed = r.NextLong();
				System.Console.Out.WriteLine("SEED: " + seed);
				r.SetSeed(seed);
				Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container c = 
					GetMockContainer(appId, 42, "user0");
				FSDataOutputStream @out = new FSDataOutputStream(new DataOutputBuffer(), null);
				Org.Mockito.Mockito.DoReturn(@out).When(spylfs).CreateInternal(Matchers.IsA<Path>
					(), Matchers.IsA<EnumSet>(), Matchers.IsA<FsPermission>(), Matchers.AnyInt(), Matchers.AnyShort
					(), Matchers.AnyLong(), Matchers.IsA<Progressable>(), Matchers.IsA<Options.ChecksumOpt
					>(), Matchers.AnyBoolean());
				LocalResource resource1 = GetPrivateMockedResource(r);
				LocalResource resource2 = null;
				do
				{
					resource2 = GetPrivateMockedResource(r);
				}
				while (resource2 == null || resource2.Equals(resource1));
				LocalResource resource3 = null;
				do
				{
					resource3 = GetPrivateMockedResource(r);
				}
				while (resource3 == null || resource3.Equals(resource1) || resource3.Equals(resource2
					));
				// above call to make sure we don't get identical resources.
				LocalResourceRequest req1 = new LocalResourceRequest(resource1);
				LocalResourceRequest req2 = new LocalResourceRequest(resource2);
				LocalResourceRequest req3 = new LocalResourceRequest(resource3);
				IDictionary<LocalResourceVisibility, ICollection<LocalResourceRequest>> rsrcs = new 
					Dictionary<LocalResourceVisibility, ICollection<LocalResourceRequest>>();
				IList<LocalResourceRequest> privateResourceList = new AList<LocalResourceRequest>
					();
				privateResourceList.AddItem(req1);
				privateResourceList.AddItem(req2);
				privateResourceList.AddItem(req3);
				rsrcs[LocalResourceVisibility.Private] = privateResourceList;
				spyService.Handle(new ContainerLocalizationRequestEvent(c, rsrcs));
				// Sigh. Thread init of private localizer not accessible
				Sharpen.Thread.Sleep(1000);
				dispatcher.Await();
				string appStr = ConverterUtils.ToString(appId);
				string ctnrStr = c.GetContainerId().ToString();
				ArgumentCaptor<Path> tokenPathCaptor = ArgumentCaptor.ForClass<Path>();
				Org.Mockito.Mockito.Verify(exec).StartLocalizer(tokenPathCaptor.Capture(), Matchers.IsA
					<IPEndPoint>(), Matchers.Eq("user0"), Matchers.Eq(appStr), Matchers.Eq(ctnrStr), 
					Matchers.IsA<LocalDirsHandlerService>());
				Path localizationTokenPath = tokenPathCaptor.GetValue();
				// heartbeat from localizer
				LocalResourceStatus rsrc1success = Org.Mockito.Mockito.Mock<LocalResourceStatus>(
					);
				LocalResourceStatus rsrc2pending = Org.Mockito.Mockito.Mock<LocalResourceStatus>(
					);
				LocalResourceStatus rsrc2success = Org.Mockito.Mockito.Mock<LocalResourceStatus>(
					);
				LocalResourceStatus rsrc3success = Org.Mockito.Mockito.Mock<LocalResourceStatus>(
					);
				LocalizerStatus stat = Org.Mockito.Mockito.Mock<LocalizerStatus>();
				Org.Mockito.Mockito.When(stat.GetLocalizerId()).ThenReturn(ctnrStr);
				Org.Mockito.Mockito.When(rsrc1success.GetResource()).ThenReturn(resource1);
				Org.Mockito.Mockito.When(rsrc2pending.GetResource()).ThenReturn(resource2);
				Org.Mockito.Mockito.When(rsrc2success.GetResource()).ThenReturn(resource2);
				Org.Mockito.Mockito.When(rsrc3success.GetResource()).ThenReturn(resource3);
				Org.Mockito.Mockito.When(rsrc1success.GetLocalSize()).ThenReturn(4344L);
				Org.Mockito.Mockito.When(rsrc2success.GetLocalSize()).ThenReturn(2342L);
				Org.Mockito.Mockito.When(rsrc3success.GetLocalSize()).ThenReturn(5345L);
				URL locPath = GetPath("/cache/private/blah");
				Org.Mockito.Mockito.When(rsrc1success.GetLocalPath()).ThenReturn(locPath);
				Org.Mockito.Mockito.When(rsrc2success.GetLocalPath()).ThenReturn(locPath);
				Org.Mockito.Mockito.When(rsrc3success.GetLocalPath()).ThenReturn(locPath);
				Org.Mockito.Mockito.When(rsrc1success.GetStatus()).ThenReturn(ResourceStatusType.
					FetchSuccess);
				Org.Mockito.Mockito.When(rsrc2pending.GetStatus()).ThenReturn(ResourceStatusType.
					FetchPending);
				Org.Mockito.Mockito.When(rsrc2success.GetStatus()).ThenReturn(ResourceStatusType.
					FetchSuccess);
				Org.Mockito.Mockito.When(rsrc3success.GetStatus()).ThenReturn(ResourceStatusType.
					FetchSuccess);
				// Four heartbeats with sending:
				// 1 - empty
				// 2 - resource1 FETCH_SUCCESS
				// 3 - resource2 FETCH_PENDING
				// 4 - resource2 FETCH_SUCCESS, resource3 FETCH_SUCCESS
				IList<LocalResourceStatus> rsrcs4 = new AList<LocalResourceStatus>();
				rsrcs4.AddItem(rsrc2success);
				rsrcs4.AddItem(rsrc3success);
				Org.Mockito.Mockito.When(stat.GetResources()).ThenReturn(Sharpen.Collections.EmptyList
					<LocalResourceStatus>()).ThenReturn(Sharpen.Collections.SingletonList(rsrc1success
					)).ThenReturn(Sharpen.Collections.SingletonList(rsrc2pending)).ThenReturn(rsrcs4
					).ThenReturn(Sharpen.Collections.EmptyList<LocalResourceStatus>());
				string localPath = Path.Separator + ContainerLocalizer.Usercache + Path.Separator
					 + "user0" + Path.Separator + ContainerLocalizer.Filecache;
				// First heartbeat
				LocalizerHeartbeatResponse response = spyService.Heartbeat(stat);
				NUnit.Framework.Assert.AreEqual(LocalizerAction.Live, response.GetLocalizerAction
					());
				NUnit.Framework.Assert.AreEqual(1, response.GetResourceSpecs().Count);
				NUnit.Framework.Assert.AreEqual(req1, new LocalResourceRequest(response.GetResourceSpecs
					()[0].GetResource()));
				URL localizedPath = response.GetResourceSpecs()[0].GetDestinationDirectory();
				// Appending to local path unique number(10) generated as a part of
				// LocalResourcesTracker
				NUnit.Framework.Assert.IsTrue(localizedPath.GetFile().EndsWith(localPath + Path.Separator
					 + "10"));
				// Second heartbeat
				response = spyService.Heartbeat(stat);
				NUnit.Framework.Assert.AreEqual(LocalizerAction.Live, response.GetLocalizerAction
					());
				NUnit.Framework.Assert.AreEqual(1, response.GetResourceSpecs().Count);
				NUnit.Framework.Assert.AreEqual(req2, new LocalResourceRequest(response.GetResourceSpecs
					()[0].GetResource()));
				localizedPath = response.GetResourceSpecs()[0].GetDestinationDirectory();
				// Resource's destination path should be now inside sub directory 0 as
				// LocalCacheDirectoryManager will be used and we have restricted number
				// of files per directory to 1.
				NUnit.Framework.Assert.IsTrue(localizedPath.GetFile().EndsWith(localPath + Path.Separator
					 + "0" + Path.Separator + "11"));
				// Third heartbeat
				response = spyService.Heartbeat(stat);
				NUnit.Framework.Assert.AreEqual(LocalizerAction.Live, response.GetLocalizerAction
					());
				NUnit.Framework.Assert.AreEqual(1, response.GetResourceSpecs().Count);
				NUnit.Framework.Assert.AreEqual(req3, new LocalResourceRequest(response.GetResourceSpecs
					()[0].GetResource()));
				localizedPath = response.GetResourceSpecs()[0].GetDestinationDirectory();
				NUnit.Framework.Assert.IsTrue(localizedPath.GetFile().EndsWith(localPath + Path.Separator
					 + "1" + Path.Separator + "12"));
				response = spyService.Heartbeat(stat);
				NUnit.Framework.Assert.AreEqual(LocalizerAction.Live, response.GetLocalizerAction
					());
				spyService.Handle(new ContainerLocalizationEvent(LocalizationEventType.ContainerResourcesLocalized
					, c));
				// get shutdown after receive CONTAINER_RESOURCES_LOCALIZED event
				response = spyService.Heartbeat(stat);
				NUnit.Framework.Assert.AreEqual(LocalizerAction.Die, response.GetLocalizerAction(
					));
				dispatcher.Await();
				// verify container notification
				ArgumentMatcher<ContainerEvent> matchesContainerLoc = new _ArgumentMatcher_1043(c
					);
				// total 3 resource localzation calls. one for each resource.
				Org.Mockito.Mockito.Verify(containerBus, Org.Mockito.Mockito.Times(3)).Handle(Matchers.ArgThat
					(matchesContainerLoc));
				// Verify deletion of localization token.
				Org.Mockito.Mockito.Verify(delService).Delete((string)Matchers.IsNull(), Matchers.Eq
					(localizationTokenPath));
			}
			finally
			{
				spyService.Stop();
				dispatcher.Stop();
				delService.Stop();
			}
		}

		private sealed class _Answer_866 : Answer<FileStatus>
		{
			public _Answer_866(Path userDir, Path fileDir, FileStatus fs, FileStatus nmFs)
			{
				this.userDir = userDir;
				this.fileDir = fileDir;
				this.fs = fs;
				this.nmFs = nmFs;
			}

			/// <exception cref="System.Exception"/>
			public FileStatus Answer(InvocationOnMock invocation)
			{
				object[] args = invocation.GetArguments();
				if (args.Length > 0)
				{
					if (args[0].Equals(userDir) || args[0].Equals(fileDir))
					{
						return fs;
					}
				}
				return nmFs;
			}

			private readonly Path userDir;

			private readonly Path fileDir;

			private readonly FileStatus fs;

			private readonly FileStatus nmFs;
		}

		private sealed class _ArgumentMatcher_892 : ArgumentMatcher<ApplicationEvent>
		{
			public _ArgumentMatcher_892(ApplicationId appId)
			{
				this.appId = appId;
			}

			public override bool Matches(object o)
			{
				ApplicationEvent evt = (ApplicationEvent)o;
				return evt.GetType() == ApplicationEventType.ApplicationInited && appId == evt.GetApplicationID
					();
			}

			private readonly ApplicationId appId;
		}

		private sealed class _ArgumentMatcher_1043 : ArgumentMatcher<ContainerEvent>
		{
			public _ArgumentMatcher_1043(Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
				 c)
			{
				this.c = c;
			}

			public override bool Matches(object o)
			{
				ContainerEvent evt = (ContainerEvent)o;
				return evt.GetType() == ContainerEventType.ResourceLocalized && c.GetContainerId(
					) == evt.GetContainerID();
			}

			private readonly Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
				 c;
		}

		[System.Serializable]
		private class DownloadingPathsMatcher : ArgumentMatcher<Path[]>, VarargMatcher
		{
			internal const long serialVersionUID = 0;

			[System.NonSerialized]
			private ICollection<Path> matchPaths;

			internal DownloadingPathsMatcher(ICollection<Path> matchPaths)
			{
				this.matchPaths = matchPaths;
			}

			public override bool Matches(object varargs)
			{
				Path[] downloadingPaths = (Path[])varargs;
				if (matchPaths.Count != downloadingPaths.Length)
				{
					return false;
				}
				foreach (Path downloadingPath in downloadingPaths)
				{
					if (!matchPaths.Contains(downloadingPath))
					{
						return false;
					}
				}
				return true;
			}

			/// <exception cref="System.IO.NotSerializableException"/>
			private void ReadObject(ObjectInputStream os)
			{
				throw new NotSerializableException(this.GetType().FullName);
			}
		}

		private class DummyExecutor : DefaultContainerExecutor
		{
			private volatile bool stopLocalization = false;

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public override void StartLocalizer(Path nmPrivateContainerTokensPath, IPEndPoint
				 nmAddr, string user, string appId, string locId, LocalDirsHandlerService dirsHandler
				)
			{
				while (!stopLocalization)
				{
					Sharpen.Thread.Yield();
				}
			}

			internal virtual void SetStopLocalization()
			{
				stopLocalization = true;
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestDownloadingResourcesOnContainerKill()
		{
			IList<Path> localDirs = new AList<Path>();
			string[] sDirs = new string[1];
			localDirs.AddItem(lfs.MakeQualified(new Path(basedir, 0 + string.Empty)));
			sDirs[0] = localDirs[0].ToString();
			conf.SetStrings(YarnConfiguration.NmLocalDirs, sDirs);
			DrainDispatcher dispatcher = new DrainDispatcher();
			dispatcher.Init(conf);
			dispatcher.Start();
			EventHandler<ApplicationEvent> applicationBus = Org.Mockito.Mockito.Mock<EventHandler
				>();
			dispatcher.Register(typeof(ApplicationEventType), applicationBus);
			EventHandler<ContainerEvent> containerBus = Org.Mockito.Mockito.Mock<EventHandler
				>();
			dispatcher.Register(typeof(ContainerEventType), containerBus);
			TestResourceLocalizationService.DummyExecutor exec = new TestResourceLocalizationService.DummyExecutor
				();
			LocalDirsHandlerService dirsHandler = new LocalDirsHandlerService();
			dirsHandler.Init(conf);
			DeletionService delServiceReal = new DeletionService(exec);
			DeletionService delService = Org.Mockito.Mockito.Spy(delServiceReal);
			delService.Init(new Configuration());
			delService.Start();
			ResourceLocalizationService rawService = new ResourceLocalizationService(dispatcher
				, exec, delService, dirsHandler, nmContext);
			ResourceLocalizationService spyService = Org.Mockito.Mockito.Spy(rawService);
			Org.Mockito.Mockito.DoReturn(mockServer).When(spyService).CreateServer();
			Org.Mockito.Mockito.DoReturn(lfs).When(spyService).GetLocalFileContext(Org.Mockito.Matchers.IsA
				<Configuration>());
			FsPermission defaultPermission = FsPermission.GetDirDefault().ApplyUMask(lfs.GetUMask
				());
			FsPermission nmPermission = ResourceLocalizationService.NmPrivatePerm.ApplyUMask(
				lfs.GetUMask());
			Path userDir = new Path(Sharpen.Runtime.Substring(sDirs[0], "file:".Length), ContainerLocalizer
				.Usercache);
			Path fileDir = new Path(Sharpen.Runtime.Substring(sDirs[0], "file:".Length), ContainerLocalizer
				.Filecache);
			Path sysDir = new Path(Sharpen.Runtime.Substring(sDirs[0], "file:".Length), ResourceLocalizationService
				.NmPrivateDir);
			FileStatus fs = new FileStatus(0, true, 1, 0, Runtime.CurrentTimeMillis(), 0, defaultPermission
				, string.Empty, string.Empty, new Path(sDirs[0]));
			FileStatus nmFs = new FileStatus(0, true, 1, 0, Runtime.CurrentTimeMillis(), 0, nmPermission
				, string.Empty, string.Empty, sysDir);
			Org.Mockito.Mockito.DoAnswer(new _Answer_1159(userDir, fileDir, fs, nmFs)).When(spylfs
				).GetFileStatus(Org.Mockito.Matchers.IsA<Path>());
			try
			{
				spyService.Init(conf);
				spyService.Start();
				Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
					 app = Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
					>();
				ApplicationId appId = BuilderUtils.NewApplicationId(314159265358979L, 3);
				string user = "user0";
				Org.Mockito.Mockito.When(app.GetUser()).ThenReturn(user);
				Org.Mockito.Mockito.When(app.GetAppId()).ThenReturn(appId);
				spyService.Handle(new ApplicationLocalizationEvent(LocalizationEventType.InitApplicationResources
					, app));
				ArgumentMatcher<ApplicationEvent> matchesAppInit = new _ArgumentMatcher_1185(appId
					);
				dispatcher.Await();
				Org.Mockito.Mockito.Verify(applicationBus).Handle(Org.Mockito.Matchers.ArgThat(matchesAppInit
					));
				// Initialize localizer.
				Random r = new Random();
				long seed = r.NextLong();
				System.Console.Out.WriteLine("SEED: " + seed);
				r.SetSeed(seed);
				Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container c1
					 = GetMockContainer(appId, 42, "user0");
				Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container c2
					 = GetMockContainer(appId, 43, "user0");
				FSDataOutputStream @out = new FSDataOutputStream(new DataOutputBuffer(), null);
				Org.Mockito.Mockito.DoReturn(@out).When(spylfs).CreateInternal(Org.Mockito.Matchers.IsA
					<Path>(), Org.Mockito.Matchers.IsA<EnumSet>(), Org.Mockito.Matchers.IsA<FsPermission
					>(), Org.Mockito.Matchers.AnyInt(), Org.Mockito.Matchers.AnyShort(), Org.Mockito.Matchers.AnyLong
					(), Org.Mockito.Matchers.IsA<Progressable>(), Org.Mockito.Matchers.IsA<Options.ChecksumOpt
					>(), Org.Mockito.Matchers.AnyBoolean());
				LocalResource resource1 = GetPrivateMockedResource(r);
				LocalResource resource2 = null;
				do
				{
					resource2 = GetPrivateMockedResource(r);
				}
				while (resource2 == null || resource2.Equals(resource1));
				LocalResource resource3 = null;
				do
				{
					resource3 = GetPrivateMockedResource(r);
				}
				while (resource3 == null || resource3.Equals(resource1) || resource3.Equals(resource2
					));
				// Send localization requests for container c1 and c2.
				LocalResourceRequest req1 = new LocalResourceRequest(resource1);
				LocalResourceRequest req2 = new LocalResourceRequest(resource2);
				LocalResourceRequest req3 = new LocalResourceRequest(resource3);
				IDictionary<LocalResourceVisibility, ICollection<LocalResourceRequest>> rsrcs = new 
					Dictionary<LocalResourceVisibility, ICollection<LocalResourceRequest>>();
				IList<LocalResourceRequest> privateResourceList = new AList<LocalResourceRequest>
					();
				privateResourceList.AddItem(req1);
				privateResourceList.AddItem(req2);
				privateResourceList.AddItem(req3);
				rsrcs[LocalResourceVisibility.Private] = privateResourceList;
				spyService.Handle(new ContainerLocalizationRequestEvent(c1, rsrcs));
				LocalResourceRequest req1_1 = new LocalResourceRequest(resource2);
				IDictionary<LocalResourceVisibility, ICollection<LocalResourceRequest>> rsrcs1 = 
					new Dictionary<LocalResourceVisibility, ICollection<LocalResourceRequest>>();
				IList<LocalResourceRequest> privateResourceList1 = new AList<LocalResourceRequest
					>();
				privateResourceList1.AddItem(req1_1);
				rsrcs1[LocalResourceVisibility.Private] = privateResourceList1;
				spyService.Handle(new ContainerLocalizationRequestEvent(c2, rsrcs1));
				dispatcher.Await();
				string containerIdStr = c1.GetContainerId().ToString();
				// Heartbeats from container localizer
				LocalResourceStatus rsrc1success = Org.Mockito.Mockito.Mock<LocalResourceStatus>(
					);
				LocalResourceStatus rsrc2pending = Org.Mockito.Mockito.Mock<LocalResourceStatus>(
					);
				LocalizerStatus stat = Org.Mockito.Mockito.Mock<LocalizerStatus>();
				Org.Mockito.Mockito.When(stat.GetLocalizerId()).ThenReturn(containerIdStr);
				Org.Mockito.Mockito.When(rsrc1success.GetResource()).ThenReturn(resource1);
				Org.Mockito.Mockito.When(rsrc2pending.GetResource()).ThenReturn(resource2);
				Org.Mockito.Mockito.When(rsrc1success.GetLocalSize()).ThenReturn(4344L);
				URL locPath = GetPath("/some/path");
				Org.Mockito.Mockito.When(rsrc1success.GetLocalPath()).ThenReturn(locPath);
				Org.Mockito.Mockito.When(rsrc1success.GetStatus()).ThenReturn(ResourceStatusType.
					FetchSuccess);
				Org.Mockito.Mockito.When(rsrc2pending.GetStatus()).ThenReturn(ResourceStatusType.
					FetchPending);
				Org.Mockito.Mockito.When(stat.GetResources()).ThenReturn(Sharpen.Collections.EmptyList
					<LocalResourceStatus>()).ThenReturn(Sharpen.Collections.SingletonList(rsrc1success
					)).ThenReturn(Sharpen.Collections.SingletonList(rsrc2pending)).ThenReturn(Sharpen.Collections
					.SingletonList(rsrc2pending)).ThenReturn(Sharpen.Collections.EmptyList<LocalResourceStatus
					>());
				// First heartbeat which schedules first resource.
				LocalizerHeartbeatResponse response = spyService.Heartbeat(stat);
				NUnit.Framework.Assert.AreEqual(LocalizerAction.Live, response.GetLocalizerAction
					());
				// Second heartbeat which reports first resource as success.
				// Second resource is scheduled.
				response = spyService.Heartbeat(stat);
				NUnit.Framework.Assert.AreEqual(LocalizerAction.Live, response.GetLocalizerAction
					());
				string locPath1 = response.GetResourceSpecs()[0].GetDestinationDirectory().GetFile
					();
				// Third heartbeat which reports second resource as pending.
				// Third resource is scheduled.
				response = spyService.Heartbeat(stat);
				NUnit.Framework.Assert.AreEqual(LocalizerAction.Live, response.GetLocalizerAction
					());
				string locPath2 = response.GetResourceSpecs()[0].GetDestinationDirectory().GetFile
					();
				// Container c1 is killed which leads to cleanup
				spyService.Handle(new ContainerLocalizationCleanupEvent(c1, rsrcs));
				// This heartbeat will indicate to container localizer to die as localizer
				// runner has stopped.
				response = spyService.Heartbeat(stat);
				NUnit.Framework.Assert.AreEqual(LocalizerAction.Die, response.GetLocalizerAction(
					));
				exec.SetStopLocalization();
				dispatcher.Await();
				// verify container notification
				ArgumentMatcher<ContainerEvent> successContainerLoc = new _ArgumentMatcher_1299(c1
					);
				// Only one resource gets localized for container c1.
				Org.Mockito.Mockito.Verify(containerBus).Handle(Org.Mockito.Matchers.ArgThat(successContainerLoc
					));
				ICollection<Path> paths = Sets.NewHashSet(new Path(locPath1), new Path(locPath1 +
					 "_tmp"), new Path(locPath2), new Path(locPath2 + "_tmp"));
				// Verify if downloading resources were submitted for deletion.
				Org.Mockito.Mockito.Verify(delService).Delete(Org.Mockito.Matchers.Eq(user), (Path
					)Org.Mockito.Matchers.Eq(null), Org.Mockito.Matchers.ArgThat(new TestResourceLocalizationService.DownloadingPathsMatcher
					(paths)));
				LocalResourcesTracker tracker = spyService.GetLocalResourcesTracker(LocalResourceVisibility
					.Private, "user0", appId);
				// Container c1 was killed but this resource was localized before kill
				// hence its not removed despite ref cnt being 0.
				LocalizedResource rsrc1 = tracker.GetLocalizedResource(req1);
				NUnit.Framework.Assert.IsNotNull(rsrc1);
				NUnit.Framework.Assert.AreEqual(rsrc1.GetState(), ResourceState.Localized);
				NUnit.Framework.Assert.AreEqual(rsrc1.GetRefCount(), 0);
				// Container c1 was killed but this resource is referenced by container c2
				// as well hence its ref cnt is 1.
				LocalizedResource rsrc2 = tracker.GetLocalizedResource(req2);
				NUnit.Framework.Assert.IsNotNull(rsrc2);
				NUnit.Framework.Assert.AreEqual(rsrc2.GetState(), ResourceState.Downloading);
				NUnit.Framework.Assert.AreEqual(rsrc2.GetRefCount(), 1);
				// As container c1 was killed and this resource was not referenced by any
				// other container, hence its removed.
				LocalizedResource rsrc3 = tracker.GetLocalizedResource(req3);
				NUnit.Framework.Assert.IsNull(rsrc3);
			}
			finally
			{
				spyService.Stop();
				dispatcher.Stop();
				delService.Stop();
			}
		}

		private sealed class _Answer_1159 : Answer<FileStatus>
		{
			public _Answer_1159(Path userDir, Path fileDir, FileStatus fs, FileStatus nmFs)
			{
				this.userDir = userDir;
				this.fileDir = fileDir;
				this.fs = fs;
				this.nmFs = nmFs;
			}

			/// <exception cref="System.Exception"/>
			public FileStatus Answer(InvocationOnMock invocation)
			{
				object[] args = invocation.GetArguments();
				if (args.Length > 0)
				{
					if (args[0].Equals(userDir) || args[0].Equals(fileDir))
					{
						return fs;
					}
				}
				return nmFs;
			}

			private readonly Path userDir;

			private readonly Path fileDir;

			private readonly FileStatus fs;

			private readonly FileStatus nmFs;
		}

		private sealed class _ArgumentMatcher_1185 : ArgumentMatcher<ApplicationEvent>
		{
			public _ArgumentMatcher_1185(ApplicationId appId)
			{
				this.appId = appId;
			}

			public override bool Matches(object o)
			{
				ApplicationEvent evt = (ApplicationEvent)o;
				return evt.GetType() == ApplicationEventType.ApplicationInited && appId == evt.GetApplicationID
					();
			}

			private readonly ApplicationId appId;
		}

		private sealed class _ArgumentMatcher_1299 : ArgumentMatcher<ContainerEvent>
		{
			public _ArgumentMatcher_1299(Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
				 c1)
			{
				this.c1 = c1;
			}

			public override bool Matches(object o)
			{
				ContainerEvent evt = (ContainerEvent)o;
				return evt.GetType() == ContainerEventType.ResourceLocalized && c1.GetContainerId
					() == evt.GetContainerID();
			}

			private readonly Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
				 c1;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestPublicResourceInitializesLocalDir()
		{
			// Setup state to simulate restart NM with existing state meaning no
			// directory creation during initialization
			NMStateStoreService spyStateStore = Org.Mockito.Mockito.Spy(nmContext.GetNMStateStore
				());
			Org.Mockito.Mockito.When(spyStateStore.CanRecover()).ThenReturn(true);
			NodeManager.NMContext spyContext = Org.Mockito.Mockito.Spy(nmContext);
			Org.Mockito.Mockito.When(spyContext.GetNMStateStore()).ThenReturn(spyStateStore);
			IList<Path> localDirs = new AList<Path>();
			string[] sDirs = new string[4];
			for (int i = 0; i < 4; ++i)
			{
				localDirs.AddItem(lfs.MakeQualified(new Path(basedir, i + string.Empty)));
				sDirs[i] = localDirs[i].ToString();
			}
			conf.SetStrings(YarnConfiguration.NmLocalDirs, sDirs);
			DrainDispatcher dispatcher = new DrainDispatcher();
			EventHandler<ApplicationEvent> applicationBus = Org.Mockito.Mockito.Mock<EventHandler
				>();
			dispatcher.Register(typeof(ApplicationEventType), applicationBus);
			EventHandler<ContainerEvent> containerBus = Org.Mockito.Mockito.Mock<EventHandler
				>();
			dispatcher.Register(typeof(ContainerEventType), containerBus);
			ContainerExecutor exec = Org.Mockito.Mockito.Mock<ContainerExecutor>();
			DeletionService delService = Org.Mockito.Mockito.Mock<DeletionService>();
			LocalDirsHandlerService dirsHandler = new LocalDirsHandlerService();
			dirsHandler.Init(conf);
			dispatcher.Init(conf);
			dispatcher.Start();
			try
			{
				ResourceLocalizationService rawService = new ResourceLocalizationService(dispatcher
					, exec, delService, dirsHandler, spyContext);
				ResourceLocalizationService spyService = Org.Mockito.Mockito.Spy(rawService);
				Org.Mockito.Mockito.DoReturn(mockServer).When(spyService).CreateServer();
				Org.Mockito.Mockito.DoReturn(lfs).When(spyService).GetLocalFileContext(Org.Mockito.Matchers.IsA
					<Configuration>());
				spyService.Init(conf);
				spyService.Start();
				FsPermission defaultPerm = new FsPermission((short)0x1ed);
				// verify directory is not created at initialization
				foreach (Path p in localDirs)
				{
					p = new Path((new URI(p.ToString())).GetPath());
					Path publicCache = new Path(p, ContainerLocalizer.Filecache);
					Org.Mockito.Mockito.Verify(spylfs, Org.Mockito.Mockito.Never()).Mkdir(Org.Mockito.Matchers.Eq
						(publicCache), Org.Mockito.Matchers.Eq(defaultPerm), Org.Mockito.Matchers.Eq(true
						));
				}
				string user = "user0";
				// init application
				Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
					 app = Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
					>();
				ApplicationId appId = BuilderUtils.NewApplicationId(314159265358979L, 3);
				Org.Mockito.Mockito.When(app.GetUser()).ThenReturn(user);
				Org.Mockito.Mockito.When(app.GetAppId()).ThenReturn(appId);
				spyService.Handle(new ApplicationLocalizationEvent(LocalizationEventType.InitApplicationResources
					, app));
				dispatcher.Await();
				// init container.
				Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container c = 
					GetMockContainer(appId, 42, user);
				// init resources
				Random r = new Random();
				long seed = r.NextLong();
				System.Console.Out.WriteLine("SEED: " + seed);
				r.SetSeed(seed);
				// Queue up public resource localization
				LocalResource pubResource = GetPublicMockedResource(r);
				LocalResourceRequest pubReq = new LocalResourceRequest(pubResource);
				IDictionary<LocalResourceVisibility, ICollection<LocalResourceRequest>> req = new 
					Dictionary<LocalResourceVisibility, ICollection<LocalResourceRequest>>();
				req[LocalResourceVisibility.Public] = Sharpen.Collections.SingletonList(pubReq);
				ICollection<LocalResourceRequest> pubRsrcs = new HashSet<LocalResourceRequest>();
				pubRsrcs.AddItem(pubReq);
				spyService.Handle(new ContainerLocalizationRequestEvent(c, req));
				dispatcher.Await();
				// verify directory creation
				foreach (Path p_1 in localDirs)
				{
					p_1 = new Path((new URI(p_1.ToString())).GetPath());
					Path publicCache = new Path(p_1, ContainerLocalizer.Filecache);
					Org.Mockito.Mockito.Verify(spylfs).Mkdir(Org.Mockito.Matchers.Eq(publicCache), Org.Mockito.Matchers.Eq
						(defaultPerm), Org.Mockito.Matchers.Eq(true));
				}
			}
			finally
			{
				dispatcher.Stop();
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestFailedPublicResource()
		{
			// mocked generics
			IList<Path> localDirs = new AList<Path>();
			string[] sDirs = new string[4];
			for (int i = 0; i < 4; ++i)
			{
				localDirs.AddItem(lfs.MakeQualified(new Path(basedir, i + string.Empty)));
				sDirs[i] = localDirs[i].ToString();
			}
			conf.SetStrings(YarnConfiguration.NmLocalDirs, sDirs);
			DrainDispatcher dispatcher = new DrainDispatcher();
			EventHandler<ApplicationEvent> applicationBus = Org.Mockito.Mockito.Mock<EventHandler
				>();
			dispatcher.Register(typeof(ApplicationEventType), applicationBus);
			EventHandler<ContainerEvent> containerBus = Org.Mockito.Mockito.Mock<EventHandler
				>();
			dispatcher.Register(typeof(ContainerEventType), containerBus);
			ContainerExecutor exec = Org.Mockito.Mockito.Mock<ContainerExecutor>();
			DeletionService delService = Org.Mockito.Mockito.Mock<DeletionService>();
			LocalDirsHandlerService dirsHandler = new LocalDirsHandlerService();
			dirsHandler.Init(conf);
			dispatcher.Init(conf);
			dispatcher.Start();
			try
			{
				ResourceLocalizationService rawService = new ResourceLocalizationService(dispatcher
					, exec, delService, dirsHandler, nmContext);
				ResourceLocalizationService spyService = Org.Mockito.Mockito.Spy(rawService);
				Org.Mockito.Mockito.DoReturn(mockServer).When(spyService).CreateServer();
				Org.Mockito.Mockito.DoReturn(lfs).When(spyService).GetLocalFileContext(Org.Mockito.Matchers.IsA
					<Configuration>());
				spyService.Init(conf);
				spyService.Start();
				string user = "user0";
				// init application
				Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
					 app = Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
					>();
				ApplicationId appId = BuilderUtils.NewApplicationId(314159265358979L, 3);
				Org.Mockito.Mockito.When(app.GetUser()).ThenReturn(user);
				Org.Mockito.Mockito.When(app.GetAppId()).ThenReturn(appId);
				spyService.Handle(new ApplicationLocalizationEvent(LocalizationEventType.InitApplicationResources
					, app));
				dispatcher.Await();
				// init container.
				Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container c = 
					GetMockContainer(appId, 42, user);
				// init resources
				Random r = new Random();
				long seed = r.NextLong();
				System.Console.Out.WriteLine("SEED: " + seed);
				r.SetSeed(seed);
				// cause chmod to fail after a delay
				CyclicBarrier barrier = new CyclicBarrier(2);
				Org.Mockito.Mockito.DoAnswer(new _Answer_1506(barrier)).When(spylfs).SetPermission
					(Org.Mockito.Matchers.IsA<Path>(), Org.Mockito.Matchers.IsA<FsPermission>());
				// Queue up two localization requests for the same public resource
				LocalResource pubResource = GetPublicMockedResource(r);
				LocalResourceRequest pubReq = new LocalResourceRequest(pubResource);
				IDictionary<LocalResourceVisibility, ICollection<LocalResourceRequest>> req = new 
					Dictionary<LocalResourceVisibility, ICollection<LocalResourceRequest>>();
				req[LocalResourceVisibility.Public] = Sharpen.Collections.SingletonList(pubReq);
				ICollection<LocalResourceRequest> pubRsrcs = new HashSet<LocalResourceRequest>();
				pubRsrcs.AddItem(pubReq);
				spyService.Handle(new ContainerLocalizationRequestEvent(c, req));
				spyService.Handle(new ContainerLocalizationRequestEvent(c, req));
				dispatcher.Await();
				// allow the chmod to fail now that both requests have been queued
				barrier.Await();
				Org.Mockito.Mockito.Verify(containerBus, Org.Mockito.Mockito.Timeout(5000).Times(
					2)).Handle(Org.Mockito.Matchers.IsA<ContainerResourceFailedEvent>());
			}
			finally
			{
				dispatcher.Stop();
			}
		}

		private sealed class _Answer_1506 : Answer<Void>
		{
			public _Answer_1506(CyclicBarrier barrier)
			{
				this.barrier = barrier;
			}

			/// <exception cref="System.IO.IOException"/>
			public Void Answer(InvocationOnMock invocation)
			{
				try
				{
					barrier.Await();
				}
				catch (Exception)
				{
				}
				catch (BrokenBarrierException)
				{
				}
				throw new IOException("forced failure");
			}

			private readonly CyclicBarrier barrier;
		}

		/*
		* Test case for handling RejectedExecutionException and IOException which can
		* be thrown when adding public resources to the pending queue.
		* RejectedExecutionException can be thrown either due to the incoming queue
		* being full or if the ExecutorCompletionService threadpool is shutdown.
		* Since it's hard to simulate the queue being full, this test just shuts down
		* the threadpool and makes sure the exception is handled. If anything is
		* messed up the async dispatcher thread will cause a system exit causing the
		* test to fail.
		*/
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestPublicResourceAddResourceExceptions()
		{
			IList<Path> localDirs = new AList<Path>();
			string[] sDirs = new string[4];
			for (int i = 0; i < 4; ++i)
			{
				localDirs.AddItem(lfs.MakeQualified(new Path(basedir, i + string.Empty)));
				sDirs[i] = localDirs[i].ToString();
			}
			conf.SetStrings(YarnConfiguration.NmLocalDirs, sDirs);
			conf.SetBoolean(Dispatcher.DispatcherExitOnErrorKey, true);
			DrainDispatcher dispatcher = new DrainDispatcher();
			EventHandler<ApplicationEvent> applicationBus = Org.Mockito.Mockito.Mock<EventHandler
				>();
			dispatcher.Register(typeof(ApplicationEventType), applicationBus);
			EventHandler<ContainerEvent> containerBus = Org.Mockito.Mockito.Mock<EventHandler
				>();
			dispatcher.Register(typeof(ContainerEventType), containerBus);
			ContainerExecutor exec = Org.Mockito.Mockito.Mock<ContainerExecutor>();
			DeletionService delService = Org.Mockito.Mockito.Mock<DeletionService>();
			LocalDirsHandlerService dirsHandler = new LocalDirsHandlerService();
			LocalDirsHandlerService dirsHandlerSpy = Org.Mockito.Mockito.Spy(dirsHandler);
			dirsHandlerSpy.Init(conf);
			dispatcher.Init(conf);
			dispatcher.Start();
			try
			{
				ResourceLocalizationService rawService = new ResourceLocalizationService(dispatcher
					, exec, delService, dirsHandlerSpy, nmContext);
				ResourceLocalizationService spyService = Org.Mockito.Mockito.Spy(rawService);
				Org.Mockito.Mockito.DoReturn(mockServer).When(spyService).CreateServer();
				Org.Mockito.Mockito.DoReturn(lfs).When(spyService).GetLocalFileContext(Org.Mockito.Matchers.IsA
					<Configuration>());
				spyService.Init(conf);
				spyService.Start();
				string user = "user0";
				// init application
				Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
					 app = Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
					>();
				ApplicationId appId = BuilderUtils.NewApplicationId(314159265358979L, 3);
				Org.Mockito.Mockito.When(app.GetUser()).ThenReturn(user);
				Org.Mockito.Mockito.When(app.GetAppId()).ThenReturn(appId);
				spyService.Handle(new ApplicationLocalizationEvent(LocalizationEventType.InitApplicationResources
					, app));
				dispatcher.Await();
				// init resources
				Random r = new Random();
				r.SetSeed(r.NextLong());
				// Queue localization request for the public resource
				LocalResource pubResource = GetPublicMockedResource(r);
				LocalResourceRequest pubReq = new LocalResourceRequest(pubResource);
				IDictionary<LocalResourceVisibility, ICollection<LocalResourceRequest>> req = new 
					Dictionary<LocalResourceVisibility, ICollection<LocalResourceRequest>>();
				req[LocalResourceVisibility.Public] = Sharpen.Collections.SingletonList(pubReq);
				// init container.
				Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container c = 
					GetMockContainer(appId, 42, user);
				// first test ioexception
				Org.Mockito.Mockito.DoThrow(new IOException()).When(dirsHandlerSpy).GetLocalPathForWrite
					(Org.Mockito.Matchers.IsA<string>(), Org.Mockito.Mockito.AnyLong(), Org.Mockito.Mockito
					.AnyBoolean());
				// send request
				spyService.Handle(new ContainerLocalizationRequestEvent(c, req));
				dispatcher.Await();
				LocalResourcesTracker tracker = spyService.GetLocalResourcesTracker(LocalResourceVisibility
					.Public, user, appId);
				NUnit.Framework.Assert.IsNull(tracker.GetLocalizedResource(pubReq));
				// test IllegalArgumentException
				string name = long.ToHexString(r.NextLong());
				URL url = GetPath("/local/PRIVATE/" + name + "/");
				LocalResource rsrc = BuilderUtils.NewLocalResource(url, LocalResourceType.File, LocalResourceVisibility
					.Public, r.Next(1024) + 1024L, r.Next(1024) + 2048L, false);
				LocalResourceRequest pubReq1 = new LocalResourceRequest(rsrc);
				IDictionary<LocalResourceVisibility, ICollection<LocalResourceRequest>> req1 = new 
					Dictionary<LocalResourceVisibility, ICollection<LocalResourceRequest>>();
				req1[LocalResourceVisibility.Public] = Sharpen.Collections.SingletonList(pubReq1);
				Org.Mockito.Mockito.DoCallRealMethod().When(dirsHandlerSpy).GetLocalPathForWrite(
					Org.Mockito.Matchers.IsA<string>(), Org.Mockito.Mockito.AnyLong(), Org.Mockito.Mockito
					.AnyBoolean());
				// send request
				spyService.Handle(new ContainerLocalizationRequestEvent(c, req1));
				dispatcher.Await();
				tracker = spyService.GetLocalResourcesTracker(LocalResourceVisibility.Public, user
					, appId);
				NUnit.Framework.Assert.IsNull(tracker.GetLocalizedResource(pubReq));
				// test RejectedExecutionException by shutting down the thread pool
				ResourceLocalizationService.PublicLocalizer publicLocalizer = spyService.GetPublicLocalizer
					();
				publicLocalizer.threadPool.Shutdown();
				spyService.Handle(new ContainerLocalizationRequestEvent(c, req));
				dispatcher.Await();
				tracker = spyService.GetLocalResourcesTracker(LocalResourceVisibility.Public, user
					, appId);
				NUnit.Framework.Assert.IsNull(tracker.GetLocalizedResource(pubReq));
			}
			finally
			{
				// if we call stop with events in the queue, an InterruptedException gets
				// thrown resulting in the dispatcher thread causing a system exit
				dispatcher.Await();
				dispatcher.Stop();
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestParallelDownloadAttemptsForPrivateResource()
		{
			DrainDispatcher dispatcher1 = null;
			try
			{
				dispatcher1 = new DrainDispatcher();
				string user = "testuser";
				ApplicationId appId = BuilderUtils.NewApplicationId(1, 1);
				// creating one local directory
				IList<Path> localDirs = new AList<Path>();
				string[] sDirs = new string[1];
				for (int i = 0; i < 1; ++i)
				{
					localDirs.AddItem(lfs.MakeQualified(new Path(basedir, i + string.Empty)));
					sDirs[i] = localDirs[i].ToString();
				}
				conf.SetStrings(YarnConfiguration.NmLocalDirs, sDirs);
				LocalDirsHandlerService localDirHandler = new LocalDirsHandlerService();
				localDirHandler.Init(conf);
				// Registering event handlers
				EventHandler<ApplicationEvent> applicationBus = Org.Mockito.Mockito.Mock<EventHandler
					>();
				dispatcher1.Register(typeof(ApplicationEventType), applicationBus);
				EventHandler<ContainerEvent> containerBus = Org.Mockito.Mockito.Mock<EventHandler
					>();
				dispatcher1.Register(typeof(ContainerEventType), containerBus);
				ContainerExecutor exec = Org.Mockito.Mockito.Mock<ContainerExecutor>();
				DeletionService delService = Org.Mockito.Mockito.Mock<DeletionService>();
				LocalDirsHandlerService dirsHandler = new LocalDirsHandlerService();
				// initializing directory handler.
				dirsHandler.Init(conf);
				dispatcher1.Init(conf);
				dispatcher1.Start();
				ResourceLocalizationService rls = new ResourceLocalizationService(dispatcher1, exec
					, delService, localDirHandler, nmContext);
				dispatcher1.Register(typeof(LocalizationEventType), rls);
				rls.Init(conf);
				rls.Handle(CreateApplicationLocalizationEvent(user, appId));
				LocalResourceRequest req = new LocalResourceRequest(new Path("file:///tmp"), 123L
					, LocalResourceType.File, LocalResourceVisibility.Private, string.Empty);
				// We need to pre-populate the LocalizerRunner as the
				// Resource Localization Service code internally starts them which
				// definitely we don't want.
				// creating new containers and populating corresponding localizer runners
				// Container - 1
				ContainerImpl container1 = CreateMockContainer(user, 1);
				string localizerId1 = container1.GetContainerId().ToString();
				rls.GetPrivateLocalizers()[localizerId1] = new ResourceLocalizationService.LocalizerRunner
					(this, new LocalizerContext(user, container1.GetContainerId(), null), localizerId1
					);
				ResourceLocalizationService.LocalizerRunner localizerRunner1 = rls.GetLocalizerRunner
					(localizerId1);
				dispatcher1.GetEventHandler().Handle(CreateContainerLocalizationEvent(container1, 
					LocalResourceVisibility.Private, req));
				NUnit.Framework.Assert.IsTrue(WaitForPrivateDownloadToStart(rls, localizerId1, 1, 
					200));
				// Container - 2 now makes the request.
				ContainerImpl container2 = CreateMockContainer(user, 2);
				string localizerId2 = container2.GetContainerId().ToString();
				rls.GetPrivateLocalizers()[localizerId2] = new ResourceLocalizationService.LocalizerRunner
					(this, new LocalizerContext(user, container2.GetContainerId(), null), localizerId2
					);
				ResourceLocalizationService.LocalizerRunner localizerRunner2 = rls.GetLocalizerRunner
					(localizerId2);
				dispatcher1.GetEventHandler().Handle(CreateContainerLocalizationEvent(container2, 
					LocalResourceVisibility.Private, req));
				NUnit.Framework.Assert.IsTrue(WaitForPrivateDownloadToStart(rls, localizerId2, 1, 
					200));
				// Retrieving localized resource.
				LocalResourcesTracker tracker = rls.GetLocalResourcesTracker(LocalResourceVisibility
					.Private, user, appId);
				LocalizedResource lr = tracker.GetLocalizedResource(req);
				// Resource would now have moved into DOWNLOADING state
				NUnit.Framework.Assert.AreEqual(ResourceState.Downloading, lr.GetState());
				// Resource should have one permit
				NUnit.Framework.Assert.AreEqual(1, lr.sem.AvailablePermits());
				// Resource Localization Service receives first heart beat from
				// ContainerLocalizer for container1
				LocalizerHeartbeatResponse response1 = rls.Heartbeat(CreateLocalizerStatus(localizerId1
					));
				// Resource must have been added to scheduled map
				NUnit.Framework.Assert.AreEqual(1, localizerRunner1.scheduled.Count);
				// Checking resource in the response and also available permits for it.
				NUnit.Framework.Assert.AreEqual(req.GetResource(), response1.GetResourceSpecs()[0
					].GetResource().GetResource());
				NUnit.Framework.Assert.AreEqual(0, lr.sem.AvailablePermits());
				// Resource Localization Service now receives first heart beat from
				// ContainerLocalizer for container2
				LocalizerHeartbeatResponse response2 = rls.Heartbeat(CreateLocalizerStatus(localizerId2
					));
				// Resource must not have been added to scheduled map
				NUnit.Framework.Assert.AreEqual(0, localizerRunner2.scheduled.Count);
				// No resource is returned in response
				NUnit.Framework.Assert.AreEqual(0, response2.GetResourceSpecs().Count);
				// ContainerLocalizer - 1 now sends failed resource heartbeat.
				rls.Heartbeat(CreateLocalizerStatusForFailedResource(localizerId1, req));
				// Resource Localization should fail and state is modified accordingly.
				// Also Local should be release on the LocalizedResource.
				NUnit.Framework.Assert.IsTrue(WaitForResourceState(lr, rls, req, LocalResourceVisibility
					.Private, user, appId, ResourceState.Failed, 200));
				NUnit.Framework.Assert.IsTrue(lr.GetState().Equals(ResourceState.Failed));
				NUnit.Framework.Assert.AreEqual(0, localizerRunner1.scheduled.Count);
				// Now Container-2 once again sends heart beat to resource localization
				// service
				// Now container-2 again try to download the resource it should still
				// not get the resource as the resource is now not in DOWNLOADING state.
				response2 = rls.Heartbeat(CreateLocalizerStatus(localizerId2));
				// Resource must not have been added to scheduled map.
				// Also as the resource has failed download it will be removed from
				// pending list.
				NUnit.Framework.Assert.AreEqual(0, localizerRunner2.scheduled.Count);
				NUnit.Framework.Assert.AreEqual(0, localizerRunner2.pending.Count);
				NUnit.Framework.Assert.AreEqual(0, response2.GetResourceSpecs().Count);
			}
			finally
			{
				if (dispatcher1 != null)
				{
					dispatcher1.Stop();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestLocalResourcePath()
		{
			// test the local path where application and user cache files will be
			// localized.
			DrainDispatcher dispatcher1 = null;
			try
			{
				dispatcher1 = new DrainDispatcher();
				string user = "testuser";
				ApplicationId appId = BuilderUtils.NewApplicationId(1, 1);
				// creating one local directory
				IList<Path> localDirs = new AList<Path>();
				string[] sDirs = new string[1];
				for (int i = 0; i < 1; ++i)
				{
					localDirs.AddItem(lfs.MakeQualified(new Path(basedir, i + string.Empty)));
					sDirs[i] = localDirs[i].ToString();
				}
				conf.SetStrings(YarnConfiguration.NmLocalDirs, sDirs);
				LocalDirsHandlerService localDirHandler = new LocalDirsHandlerService();
				localDirHandler.Init(conf);
				// Registering event handlers
				EventHandler<ApplicationEvent> applicationBus = Org.Mockito.Mockito.Mock<EventHandler
					>();
				dispatcher1.Register(typeof(ApplicationEventType), applicationBus);
				EventHandler<ContainerEvent> containerBus = Org.Mockito.Mockito.Mock<EventHandler
					>();
				dispatcher1.Register(typeof(ContainerEventType), containerBus);
				ContainerExecutor exec = Org.Mockito.Mockito.Mock<ContainerExecutor>();
				DeletionService delService = Org.Mockito.Mockito.Mock<DeletionService>();
				LocalDirsHandlerService dirsHandler = new LocalDirsHandlerService();
				// initializing directory handler.
				dirsHandler.Init(conf);
				dispatcher1.Init(conf);
				dispatcher1.Start();
				ResourceLocalizationService rls = new ResourceLocalizationService(dispatcher1, exec
					, delService, localDirHandler, nmContext);
				dispatcher1.Register(typeof(LocalizationEventType), rls);
				rls.Init(conf);
				rls.Handle(CreateApplicationLocalizationEvent(user, appId));
				// We need to pre-populate the LocalizerRunner as the
				// Resource Localization Service code internally starts them which
				// definitely we don't want.
				// creating new container and populating corresponding localizer runner
				// Container - 1
				Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container container1
					 = CreateMockContainer(user, 1);
				string localizerId1 = container1.GetContainerId().ToString();
				rls.GetPrivateLocalizers()[localizerId1] = new ResourceLocalizationService.LocalizerRunner
					(this, new LocalizerContext(user, container1.GetContainerId(), null), localizerId1
					);
				// Creating two requests for container
				// 1) Private resource
				// 2) Application resource
				LocalResourceRequest reqPriv = new LocalResourceRequest(new Path("file:///tmp1"), 
					123L, LocalResourceType.File, LocalResourceVisibility.Private, string.Empty);
				IList<LocalResourceRequest> privList = new AList<LocalResourceRequest>();
				privList.AddItem(reqPriv);
				LocalResourceRequest reqApp = new LocalResourceRequest(new Path("file:///tmp2"), 
					123L, LocalResourceType.File, LocalResourceVisibility.Application, string.Empty);
				IList<LocalResourceRequest> appList = new AList<LocalResourceRequest>();
				appList.AddItem(reqApp);
				IDictionary<LocalResourceVisibility, ICollection<LocalResourceRequest>> rsrcs = new 
					Dictionary<LocalResourceVisibility, ICollection<LocalResourceRequest>>();
				rsrcs[LocalResourceVisibility.Application] = appList;
				rsrcs[LocalResourceVisibility.Private] = privList;
				dispatcher1.GetEventHandler().Handle(new ContainerLocalizationRequestEvent(container1
					, rsrcs));
				// Now waiting for resource download to start. Here actual will not start
				// Only the resources will be populated into pending list.
				NUnit.Framework.Assert.IsTrue(WaitForPrivateDownloadToStart(rls, localizerId1, 2, 
					500));
				// Validating user and application cache paths
				string userCachePath = StringUtils.Join(Path.Separator, Arrays.AsList(localDirs[0
					].ToUri().GetRawPath(), ContainerLocalizer.Usercache, user, ContainerLocalizer.Filecache
					));
				string userAppCachePath = StringUtils.Join(Path.Separator, Arrays.AsList(localDirs
					[0].ToUri().GetRawPath(), ContainerLocalizer.Usercache, user, ContainerLocalizer
					.Appcache, appId.ToString(), ContainerLocalizer.Filecache));
				// Now the Application and private resources may come in any order
				// for download.
				// For User cahce :
				// returned destinationPath = user cache path + random number
				// For App cache :
				// returned destinationPath = user app cache path + random number
				int returnedResources = 0;
				bool appRsrc = false;
				bool privRsrc = false;
				while (returnedResources < 2)
				{
					LocalizerHeartbeatResponse response = rls.Heartbeat(CreateLocalizerStatus(localizerId1
						));
					foreach (ResourceLocalizationSpec resourceSpec in response.GetResourceSpecs())
					{
						returnedResources++;
						Path destinationDirectory = new Path(resourceSpec.GetDestinationDirectory().GetFile
							());
						if (resourceSpec.GetResource().GetVisibility() == LocalResourceVisibility.Application)
						{
							appRsrc = true;
							NUnit.Framework.Assert.AreEqual(userAppCachePath, destinationDirectory.GetParent(
								).ToUri().ToString());
						}
						else
						{
							if (resourceSpec.GetResource().GetVisibility() == LocalResourceVisibility.Private)
							{
								privRsrc = true;
								NUnit.Framework.Assert.AreEqual(userCachePath, destinationDirectory.GetParent().ToUri
									().ToString());
							}
							else
							{
								throw new Exception("Unexpected resource recevied.");
							}
						}
					}
				}
				// We should receive both the resources (Application and Private)
				NUnit.Framework.Assert.IsTrue(appRsrc && privRsrc);
			}
			finally
			{
				if (dispatcher1 != null)
				{
					dispatcher1.Stop();
				}
			}
		}

		private LocalizerStatus CreateLocalizerStatusForFailedResource(string localizerId
			, LocalResourceRequest req)
		{
			LocalizerStatus status = CreateLocalizerStatus(localizerId);
			LocalResourceStatus resourceStatus = new LocalResourceStatusPBImpl();
			resourceStatus.SetException(SerializedException.NewInstance(new YarnException("test"
				)));
			resourceStatus.SetStatus(ResourceStatusType.FetchFailure);
			resourceStatus.SetResource(req);
			status.AddResourceStatus(resourceStatus);
			return status;
		}

		private LocalizerStatus CreateLocalizerStatus(string localizerId1)
		{
			LocalizerStatus status = new LocalizerStatusPBImpl();
			status.SetLocalizerId(localizerId1);
			return status;
		}

		private LocalizationEvent CreateApplicationLocalizationEvent(string user, ApplicationId
			 appId)
		{
			Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
				 app = Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
				>();
			Org.Mockito.Mockito.When(app.GetUser()).ThenReturn(user);
			Org.Mockito.Mockito.When(app.GetAppId()).ThenReturn(appId);
			return new ApplicationLocalizationEvent(LocalizationEventType.InitApplicationResources
				, app);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestParallelDownloadAttemptsForPublicResource()
		{
			DrainDispatcher dispatcher1 = null;
			string user = "testuser";
			try
			{
				// creating one local directory
				IList<Path> localDirs = new AList<Path>();
				string[] sDirs = new string[1];
				for (int i = 0; i < 1; ++i)
				{
					localDirs.AddItem(lfs.MakeQualified(new Path(basedir, i + string.Empty)));
					sDirs[i] = localDirs[i].ToString();
				}
				conf.SetStrings(YarnConfiguration.NmLocalDirs, sDirs);
				// Registering event handlers
				EventHandler<ApplicationEvent> applicationBus = Org.Mockito.Mockito.Mock<EventHandler
					>();
				dispatcher1 = new DrainDispatcher();
				dispatcher1.Register(typeof(ApplicationEventType), applicationBus);
				EventHandler<ContainerEvent> containerBus = Org.Mockito.Mockito.Mock<EventHandler
					>();
				dispatcher1.Register(typeof(ContainerEventType), containerBus);
				ContainerExecutor exec = Org.Mockito.Mockito.Mock<ContainerExecutor>();
				DeletionService delService = Org.Mockito.Mockito.Mock<DeletionService>();
				LocalDirsHandlerService dirsHandler = new LocalDirsHandlerService();
				// initializing directory handler.
				dirsHandler.Init(conf);
				dispatcher1.Init(conf);
				dispatcher1.Start();
				// Creating and initializing ResourceLocalizationService but not starting
				// it as otherwise it will remove requests from pending queue.
				ResourceLocalizationService rawService = new ResourceLocalizationService(dispatcher1
					, exec, delService, dirsHandler, nmContext);
				ResourceLocalizationService spyService = Org.Mockito.Mockito.Spy(rawService);
				dispatcher1.Register(typeof(LocalizationEventType), spyService);
				spyService.Init(conf);
				// Initially pending map should be empty for public localizer
				NUnit.Framework.Assert.AreEqual(0, spyService.GetPublicLocalizer().pending.Count);
				LocalResourceRequest req = new LocalResourceRequest(new Path("/tmp"), 123L, LocalResourceType
					.File, LocalResourceVisibility.Public, string.Empty);
				// Initializing application
				ApplicationImpl app = Org.Mockito.Mockito.Mock<ApplicationImpl>();
				ApplicationId appId = BuilderUtils.NewApplicationId(1, 1);
				Org.Mockito.Mockito.When(app.GetAppId()).ThenReturn(appId);
				Org.Mockito.Mockito.When(app.GetUser()).ThenReturn(user);
				dispatcher1.GetEventHandler().Handle(new ApplicationLocalizationEvent(LocalizationEventType
					.InitApplicationResources, app));
				// Container - 1
				// container requesting the resource
				ContainerImpl container1 = CreateMockContainer(user, 1);
				dispatcher1.GetEventHandler().Handle(CreateContainerLocalizationEvent(container1, 
					LocalResourceVisibility.Public, req));
				// Waiting for resource to change into DOWNLOADING state.
				NUnit.Framework.Assert.IsTrue(WaitForResourceState(null, spyService, req, LocalResourceVisibility
					.Public, user, null, ResourceState.Downloading, 200));
				// Waiting for download to start.
				NUnit.Framework.Assert.IsTrue(WaitForPublicDownloadToStart(spyService, 1, 200));
				LocalizedResource lr = GetLocalizedResource(spyService, req, LocalResourceVisibility
					.Public, user, null);
				// Resource would now have moved into DOWNLOADING state
				NUnit.Framework.Assert.AreEqual(ResourceState.Downloading, lr.GetState());
				// pending should have this resource now.
				NUnit.Framework.Assert.AreEqual(1, spyService.GetPublicLocalizer().pending.Count);
				// Now resource should have 0 permit.
				NUnit.Framework.Assert.AreEqual(0, lr.sem.AvailablePermits());
				// Container - 2
				// Container requesting the same resource.
				ContainerImpl container2 = CreateMockContainer(user, 2);
				dispatcher1.GetEventHandler().Handle(CreateContainerLocalizationEvent(container2, 
					LocalResourceVisibility.Public, req));
				// Waiting for download to start. This should return false as new download
				// will not start
				NUnit.Framework.Assert.IsFalse(WaitForPublicDownloadToStart(spyService, 2, 100));
				// Now Failing the resource download. As a part of it
				// resource state is changed and then lock is released.
				ResourceFailedLocalizationEvent locFailedEvent = new ResourceFailedLocalizationEvent
					(req, new Exception("test").ToString());
				spyService.GetLocalResourcesTracker(LocalResourceVisibility.Public, user, null).Handle
					(locFailedEvent);
				// Waiting for resource to change into FAILED state.
				NUnit.Framework.Assert.IsTrue(WaitForResourceState(lr, spyService, req, LocalResourceVisibility
					.Public, user, null, ResourceState.Failed, 200));
				// releasing lock as a part of download failed process.
				lr.Unlock();
				// removing pending download request.
				spyService.GetPublicLocalizer().pending.Clear();
				// Now I need to simulate a race condition wherein Event is added to
				// dispatcher before resource state changes to either FAILED or LOCALIZED
				// Hence sending event directly to dispatcher.
				LocalizerResourceRequestEvent localizerEvent = new LocalizerResourceRequestEvent(
					lr, null, Org.Mockito.Mockito.Mock<LocalizerContext>(), null);
				dispatcher1.GetEventHandler().Handle(localizerEvent);
				// Waiting for download to start. This should return false as new download
				// will not start
				NUnit.Framework.Assert.IsFalse(WaitForPublicDownloadToStart(spyService, 1, 100));
				// Checking available permits now.
				NUnit.Framework.Assert.AreEqual(1, lr.sem.AvailablePermits());
			}
			finally
			{
				if (dispatcher1 != null)
				{
					dispatcher1.Stop();
				}
			}
		}

		private bool WaitForPrivateDownloadToStart(ResourceLocalizationService service, string
			 localizerId, int size, int maxWaitTime)
		{
			IList<LocalizerResourceRequestEvent> pending = null;
			do
			{
				// Waiting for localizer to be created.
				if (service.GetPrivateLocalizers()[localizerId] != null)
				{
					pending = service.GetPrivateLocalizers()[localizerId].pending;
				}
				if (pending == null)
				{
					try
					{
						maxWaitTime -= 20;
						Sharpen.Thread.Sleep(20);
					}
					catch (Exception)
					{
					}
				}
				else
				{
					break;
				}
			}
			while (maxWaitTime > 0);
			if (pending == null)
			{
				return false;
			}
			do
			{
				if (pending.Count == size)
				{
					return true;
				}
				else
				{
					try
					{
						maxWaitTime -= 20;
						Sharpen.Thread.Sleep(20);
					}
					catch (Exception)
					{
					}
				}
			}
			while (maxWaitTime > 0);
			return pending.Count == size;
		}

		private bool WaitForPublicDownloadToStart(ResourceLocalizationService service, int
			 size, int maxWaitTime)
		{
			IDictionary<Future<Path>, LocalizerResourceRequestEvent> pending = null;
			do
			{
				// Waiting for localizer to be created.
				if (service.GetPublicLocalizer() != null)
				{
					pending = service.GetPublicLocalizer().pending;
				}
				if (pending == null)
				{
					try
					{
						maxWaitTime -= 20;
						Sharpen.Thread.Sleep(20);
					}
					catch (Exception)
					{
					}
				}
				else
				{
					break;
				}
			}
			while (maxWaitTime > 0);
			if (pending == null)
			{
				return false;
			}
			do
			{
				if (pending.Count == size)
				{
					return true;
				}
				else
				{
					try
					{
						maxWaitTime -= 20;
						Sharpen.Thread.Sleep(20);
					}
					catch (Exception)
					{
					}
				}
			}
			while (maxWaitTime > 0);
			return pending.Count == size;
		}

		private LocalizedResource GetLocalizedResource(ResourceLocalizationService service
			, LocalResourceRequest req, LocalResourceVisibility vis, string user, ApplicationId
			 appId)
		{
			return service.GetLocalResourcesTracker(vis, user, appId).GetLocalizedResource(req
				);
		}

		private bool WaitForResourceState(LocalizedResource lr, ResourceLocalizationService
			 service, LocalResourceRequest req, LocalResourceVisibility vis, string user, ApplicationId
			 appId, ResourceState resourceState, long maxWaitTime)
		{
			LocalResourcesTracker tracker = null;
			do
			{
				// checking tracker is created
				if (tracker == null)
				{
					tracker = service.GetLocalResourcesTracker(vis, user, appId);
				}
				if (tracker != null && lr == null)
				{
					lr = tracker.GetLocalizedResource(req);
				}
				if (lr != null)
				{
					break;
				}
				else
				{
					try
					{
						maxWaitTime -= 20;
						Sharpen.Thread.Sleep(20);
					}
					catch (Exception)
					{
					}
				}
			}
			while (maxWaitTime > 0);
			// this will wait till resource state is changed to (resourceState).
			if (lr == null)
			{
				return false;
			}
			do
			{
				if (!lr.GetState().Equals(resourceState))
				{
					try
					{
						maxWaitTime -= 50;
						Sharpen.Thread.Sleep(50);
					}
					catch (Exception)
					{
					}
				}
				else
				{
					break;
				}
			}
			while (maxWaitTime > 0);
			return lr.GetState().Equals(resourceState);
		}

		private ContainerLocalizationRequestEvent CreateContainerLocalizationEvent(ContainerImpl
			 container, LocalResourceVisibility vis, LocalResourceRequest req)
		{
			IDictionary<LocalResourceVisibility, ICollection<LocalResourceRequest>> reqs = new 
				Dictionary<LocalResourceVisibility, ICollection<LocalResourceRequest>>();
			IList<LocalResourceRequest> resourceList = new AList<LocalResourceRequest>();
			resourceList.AddItem(req);
			reqs[vis] = resourceList;
			return new ContainerLocalizationRequestEvent(container, reqs);
		}

		private ContainerImpl CreateMockContainer(string user, int containerId)
		{
			ContainerImpl container = Org.Mockito.Mockito.Mock<ContainerImpl>();
			Org.Mockito.Mockito.When(container.GetContainerId()).ThenReturn(BuilderUtils.NewContainerId
				(1, 1, 1, containerId));
			Org.Mockito.Mockito.When(container.GetUser()).ThenReturn(user);
			Credentials mockCredentials = Org.Mockito.Mockito.Mock<Credentials>();
			Org.Mockito.Mockito.When(container.GetCredentials()).ThenReturn(mockCredentials);
			return container;
		}

		private static URL GetPath(string path)
		{
			URL url = BuilderUtils.NewURL("file", null, 0, path);
			return url;
		}

		private static LocalResource GetMockedResource(Random r, LocalResourceVisibility 
			vis)
		{
			string name = long.ToHexString(r.NextLong());
			URL url = GetPath("/local/PRIVATE/" + name);
			LocalResource rsrc = BuilderUtils.NewLocalResource(url, LocalResourceType.File, vis
				, r.Next(1024) + 1024L, r.Next(1024) + 2048L, false);
			return rsrc;
		}

		private static LocalResource GetAppMockedResource(Random r)
		{
			return GetMockedResource(r, LocalResourceVisibility.Application);
		}

		private static LocalResource GetPublicMockedResource(Random r)
		{
			return GetMockedResource(r, LocalResourceVisibility.Public);
		}

		private static LocalResource GetPrivateMockedResource(Random r)
		{
			return GetMockedResource(r, LocalResourceVisibility.Private);
		}

		private static Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
			 GetMockContainer(ApplicationId appId, int id, string user)
		{
			Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container c = 
				Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
				>();
			ApplicationAttemptId appAttemptId = BuilderUtils.NewApplicationAttemptId(appId, 1
				);
			ContainerId cId = BuilderUtils.NewContainerId(appAttemptId, id);
			Org.Mockito.Mockito.When(c.GetUser()).ThenReturn(user);
			Org.Mockito.Mockito.When(c.GetContainerId()).ThenReturn(cId);
			Credentials creds = new Credentials();
			creds.AddToken(new Text("tok" + id), GetToken(id));
			Org.Mockito.Mockito.When(c.GetCredentials()).ThenReturn(creds);
			Org.Mockito.Mockito.When(c.ToString()).ThenReturn(cId.ToString());
			return c;
		}

		private ResourceLocalizationService CreateSpyService(DrainDispatcher dispatcher, 
			LocalDirsHandlerService dirsHandler, NMStateStoreService stateStore)
		{
			ContainerExecutor exec = Org.Mockito.Mockito.Mock<ContainerExecutor>();
			ResourceLocalizationService.LocalizerTracker mockLocalizerTracker = Org.Mockito.Mockito.Mock
				<ResourceLocalizationService.LocalizerTracker>();
			DeletionService delService = Org.Mockito.Mockito.Mock<DeletionService>();
			NodeManager.NMContext nmContext = new NodeManager.NMContext(new NMContainerTokenSecretManager
				(conf), new NMTokenSecretManagerInNM(), null, new ApplicationACLsManager(conf), 
				stateStore);
			ResourceLocalizationService rawService = new ResourceLocalizationService(dispatcher
				, exec, delService, dirsHandler, nmContext);
			ResourceLocalizationService spyService = Org.Mockito.Mockito.Spy(rawService);
			Org.Mockito.Mockito.DoReturn(mockServer).When(spyService).CreateServer();
			Org.Mockito.Mockito.DoReturn(mockLocalizerTracker).When(spyService).CreateLocalizerTracker
				(Org.Mockito.Matchers.IsA<Configuration>());
			Org.Mockito.Mockito.DoReturn(lfs).When(spyService).GetLocalFileContext(Org.Mockito.Matchers.IsA
				<Configuration>());
			return spyService;
		}

		internal static Org.Apache.Hadoop.Security.Token.Token<TokenIdentifier> GetToken(
			int id)
		{
			return new Org.Apache.Hadoop.Security.Token.Token(Sharpen.Runtime.GetBytesForString
				(("ident" + id)), Sharpen.Runtime.GetBytesForString(("passwd" + id)), new Text("kind"
				 + id), new Text("service" + id));
		}

		/*
		* Test to ensure ResourceLocalizationService can handle local dirs going bad.
		* Test first sets up all the components required, then sends events to fetch
		* a private, app and public resource. It then sends events to clean up the
		* container and the app and ensures the right delete calls were made.
		*/
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFailedDirsResourceRelease()
		{
			// mocked generics
			// setup components
			FilePath f = new FilePath(basedir.ToString());
			string[] sDirs = new string[4];
			IList<Path> localDirs = new AList<Path>(sDirs.Length);
			for (int i = 0; i < 4; ++i)
			{
				sDirs[i] = f.GetAbsolutePath() + i;
				localDirs.AddItem(new Path(sDirs[i]));
			}
			IList<Path> containerLocalDirs = new AList<Path>(localDirs.Count);
			IList<Path> appLocalDirs = new AList<Path>(localDirs.Count);
			IList<Path> nmLocalContainerDirs = new AList<Path>(localDirs.Count);
			IList<Path> nmLocalAppDirs = new AList<Path>(localDirs.Count);
			conf.SetStrings(YarnConfiguration.NmLocalDirs, sDirs);
			conf.SetLong(YarnConfiguration.NmDiskHealthCheckIntervalMs, 500);
			ResourceLocalizationService.LocalizerTracker mockLocallilzerTracker = Org.Mockito.Mockito.Mock
				<ResourceLocalizationService.LocalizerTracker>();
			DrainDispatcher dispatcher = new DrainDispatcher();
			dispatcher.Init(conf);
			dispatcher.Start();
			EventHandler<ApplicationEvent> applicationBus = Org.Mockito.Mockito.Mock<EventHandler
				>();
			dispatcher.Register(typeof(ApplicationEventType), applicationBus);
			EventHandler<ContainerEvent> containerBus = Org.Mockito.Mockito.Mock<EventHandler
				>();
			dispatcher.Register(typeof(ContainerEventType), containerBus);
			// Ignore actual localization
			EventHandler<LocalizerEvent> localizerBus = Org.Mockito.Mockito.Mock<EventHandler
				>();
			dispatcher.Register(typeof(LocalizerEventType), localizerBus);
			ContainerExecutor exec = Org.Mockito.Mockito.Mock<ContainerExecutor>();
			LocalDirsHandlerService mockDirsHandler = Org.Mockito.Mockito.Mock<LocalDirsHandlerService
				>();
			Org.Mockito.Mockito.DoReturn(new AList<string>(Arrays.AsList(sDirs))).When(mockDirsHandler
				).GetLocalDirsForCleanup();
			DeletionService delService = Org.Mockito.Mockito.Mock<DeletionService>();
			// setup mocks
			ResourceLocalizationService rawService = new ResourceLocalizationService(dispatcher
				, exec, delService, mockDirsHandler, nmContext);
			ResourceLocalizationService spyService = Org.Mockito.Mockito.Spy(rawService);
			Org.Mockito.Mockito.DoReturn(mockServer).When(spyService).CreateServer();
			Org.Mockito.Mockito.DoReturn(mockLocallilzerTracker).When(spyService).CreateLocalizerTracker
				(Org.Mockito.Matchers.IsA<Configuration>());
			Org.Mockito.Mockito.DoReturn(lfs).When(spyService).GetLocalFileContext(Org.Mockito.Matchers.IsA
				<Configuration>());
			FsPermission defaultPermission = FsPermission.GetDirDefault().ApplyUMask(lfs.GetUMask
				());
			FsPermission nmPermission = ResourceLocalizationService.NmPrivatePerm.ApplyUMask(
				lfs.GetUMask());
			FileStatus fs = new FileStatus(0, true, 1, 0, Runtime.CurrentTimeMillis(), 0, defaultPermission
				, string.Empty, string.Empty, localDirs[0]);
			FileStatus nmFs = new FileStatus(0, true, 1, 0, Runtime.CurrentTimeMillis(), 0, nmPermission
				, string.Empty, string.Empty, localDirs[0]);
			string user = "user0";
			// init application
			Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
				 app = Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
				>();
			ApplicationId appId = BuilderUtils.NewApplicationId(314159265358979L, 3);
			Org.Mockito.Mockito.When(app.GetUser()).ThenReturn(user);
			Org.Mockito.Mockito.When(app.GetAppId()).ThenReturn(appId);
			Org.Mockito.Mockito.When(app.ToString()).ThenReturn(ConverterUtils.ToString(appId
				));
			// init container.
			Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container c = 
				GetMockContainer(appId, 42, user);
			// setup local app dirs
			IList<string> tmpDirs = mockDirsHandler.GetLocalDirs();
			for (int i_1 = 0; i_1 < tmpDirs.Count; ++i_1)
			{
				Path usersdir = new Path(tmpDirs[i_1], ContainerLocalizer.Usercache);
				Path userdir = new Path(usersdir, user);
				Path allAppsdir = new Path(userdir, ContainerLocalizer.Appcache);
				Path appDir = new Path(allAppsdir, ConverterUtils.ToString(appId));
				Path containerDir = new Path(appDir, ConverterUtils.ToString(c.GetContainerId()));
				containerLocalDirs.AddItem(containerDir);
				appLocalDirs.AddItem(appDir);
				Path sysDir = new Path(tmpDirs[i_1], ResourceLocalizationService.NmPrivateDir);
				Path appSysDir = new Path(sysDir, ConverterUtils.ToString(appId));
				Path containerSysDir = new Path(appSysDir, ConverterUtils.ToString(c.GetContainerId
					()));
				nmLocalContainerDirs.AddItem(containerSysDir);
				nmLocalAppDirs.AddItem(appSysDir);
			}
			try
			{
				spyService.Init(conf);
				spyService.Start();
				spyService.Handle(new ApplicationLocalizationEvent(LocalizationEventType.InitApplicationResources
					, app));
				dispatcher.Await();
				// Get a handle on the trackers after they're setup with
				// INIT_APP_RESOURCES
				LocalResourcesTracker appTracker = spyService.GetLocalResourcesTracker(LocalResourceVisibility
					.Application, user, appId);
				LocalResourcesTracker privTracker = spyService.GetLocalResourcesTracker(LocalResourceVisibility
					.Private, user, appId);
				LocalResourcesTracker pubTracker = spyService.GetLocalResourcesTracker(LocalResourceVisibility
					.Public, user, appId);
				// init resources
				Random r = new Random();
				long seed = r.NextLong();
				r.SetSeed(seed);
				// Send localization requests, one for each type of resource
				LocalResource privResource = GetPrivateMockedResource(r);
				LocalResourceRequest privReq = new LocalResourceRequest(privResource);
				LocalResource appResource = GetAppMockedResource(r);
				LocalResourceRequest appReq = new LocalResourceRequest(appResource);
				LocalResource pubResource = GetPublicMockedResource(r);
				LocalResourceRequest pubReq = new LocalResourceRequest(pubResource);
				IDictionary<LocalResourceVisibility, ICollection<LocalResourceRequest>> req = new 
					Dictionary<LocalResourceVisibility, ICollection<LocalResourceRequest>>();
				req[LocalResourceVisibility.Private] = Sharpen.Collections.SingletonList(privReq);
				req[LocalResourceVisibility.Application] = Sharpen.Collections.SingletonList(appReq
					);
				req[LocalResourceVisibility.Public] = Sharpen.Collections.SingletonList(pubReq);
				IDictionary<LocalResourceVisibility, ICollection<LocalResourceRequest>> req2 = new 
					Dictionary<LocalResourceVisibility, ICollection<LocalResourceRequest>>();
				req2[LocalResourceVisibility.Private] = Sharpen.Collections.SingletonList(privReq
					);
				// Send Request event
				spyService.Handle(new ContainerLocalizationRequestEvent(c, req));
				spyService.Handle(new ContainerLocalizationRequestEvent(c, req2));
				dispatcher.Await();
				int privRsrcCount = 0;
				foreach (LocalizedResource lr in privTracker)
				{
					privRsrcCount++;
					NUnit.Framework.Assert.AreEqual("Incorrect reference count", 2, lr.GetRefCount());
					NUnit.Framework.Assert.AreEqual(privReq, lr.GetRequest());
				}
				NUnit.Framework.Assert.AreEqual(1, privRsrcCount);
				int appRsrcCount = 0;
				foreach (LocalizedResource lr_1 in appTracker)
				{
					appRsrcCount++;
					NUnit.Framework.Assert.AreEqual("Incorrect reference count", 1, lr_1.GetRefCount(
						));
					NUnit.Framework.Assert.AreEqual(appReq, lr_1.GetRequest());
				}
				NUnit.Framework.Assert.AreEqual(1, appRsrcCount);
				int pubRsrcCount = 0;
				foreach (LocalizedResource lr_2 in pubTracker)
				{
					pubRsrcCount++;
					NUnit.Framework.Assert.AreEqual("Incorrect reference count", 1, lr_2.GetRefCount(
						));
					NUnit.Framework.Assert.AreEqual(pubReq, lr_2.GetRequest());
				}
				NUnit.Framework.Assert.AreEqual(1, pubRsrcCount);
				// setup mocks for test, a set of dirs with IOExceptions and let the rest
				// go through
				for (int i_2 = 0; i_2 < containerLocalDirs.Count; ++i_2)
				{
					if (i_2 == 2)
					{
						Org.Mockito.Mockito.DoThrow(new IOException()).When(spylfs).GetFileStatus(Org.Mockito.Matchers.Eq
							(containerLocalDirs[i_2]));
						Org.Mockito.Mockito.DoThrow(new IOException()).When(spylfs).GetFileStatus(Org.Mockito.Matchers.Eq
							(nmLocalContainerDirs[i_2]));
					}
					else
					{
						Org.Mockito.Mockito.DoReturn(fs).When(spylfs).GetFileStatus(Org.Mockito.Matchers.Eq
							(containerLocalDirs[i_2]));
						Org.Mockito.Mockito.DoReturn(nmFs).When(spylfs).GetFileStatus(Org.Mockito.Matchers.Eq
							(nmLocalContainerDirs[i_2]));
					}
				}
				// Send Cleanup Event
				spyService.Handle(new ContainerLocalizationCleanupEvent(c, req));
				Org.Mockito.Mockito.Verify(mockLocallilzerTracker).CleanupPrivLocalizers("container_314159265358979_0003_01_000042"
					);
				// match cleanup events with the mocks we setup earlier
				for (int i_3 = 0; i_3 < containerLocalDirs.Count; ++i_3)
				{
					if (i_3 == 2)
					{
						try
						{
							Org.Mockito.Mockito.Verify(delService).Delete(user, containerLocalDirs[i_3]);
							Org.Mockito.Mockito.Verify(delService).Delete(null, nmLocalContainerDirs[i_3]);
							NUnit.Framework.Assert.Fail("deletion attempts for invalid dirs");
						}
						catch
						{
							continue;
						}
					}
					else
					{
						Org.Mockito.Mockito.Verify(delService).Delete(user, containerLocalDirs[i_3]);
						Org.Mockito.Mockito.Verify(delService).Delete(null, nmLocalContainerDirs[i_3]);
					}
				}
				ArgumentMatcher<ApplicationEvent> matchesAppDestroy = new _ArgumentMatcher_2562(appId
					);
				dispatcher.Await();
				// setup mocks again, this time throw UnsupportedFileSystemException and
				// IOExceptions
				for (int i_4 = 0; i_4 < containerLocalDirs.Count; ++i_4)
				{
					if (i_4 == 3)
					{
						Org.Mockito.Mockito.DoThrow(new IOException()).When(spylfs).GetFileStatus(Org.Mockito.Matchers.Eq
							(appLocalDirs[i_4]));
						Org.Mockito.Mockito.DoThrow(new UnsupportedFileSystemException("test")).When(spylfs
							).GetFileStatus(Org.Mockito.Matchers.Eq(nmLocalAppDirs[i_4]));
					}
					else
					{
						Org.Mockito.Mockito.DoReturn(fs).When(spylfs).GetFileStatus(Org.Mockito.Matchers.Eq
							(appLocalDirs[i_4]));
						Org.Mockito.Mockito.DoReturn(nmFs).When(spylfs).GetFileStatus(Org.Mockito.Matchers.Eq
							(nmLocalAppDirs[i_4]));
					}
				}
				LocalizationEvent destroyApp = new ApplicationLocalizationEvent(LocalizationEventType
					.DestroyApplicationResources, app);
				spyService.Handle(destroyApp);
				Org.Mockito.Mockito.Verify(applicationBus).Handle(Org.Mockito.Matchers.ArgThat(matchesAppDestroy
					));
				// verify we got the right delete calls
				for (int i_5 = 0; i_5 < containerLocalDirs.Count; ++i_5)
				{
					if (i_5 == 3)
					{
						try
						{
							Org.Mockito.Mockito.Verify(delService).Delete(user, containerLocalDirs[i_5]);
							Org.Mockito.Mockito.Verify(delService).Delete(null, nmLocalContainerDirs[i_5]);
							NUnit.Framework.Assert.Fail("deletion attempts for invalid dirs");
						}
						catch
						{
							continue;
						}
					}
					else
					{
						Org.Mockito.Mockito.Verify(delService).Delete(user, appLocalDirs[i_5]);
						Org.Mockito.Mockito.Verify(delService).Delete(null, nmLocalAppDirs[i_5]);
					}
				}
			}
			finally
			{
				dispatcher.Stop();
				delService.Stop();
			}
		}

		private sealed class _ArgumentMatcher_2562 : ArgumentMatcher<ApplicationEvent>
		{
			public _ArgumentMatcher_2562(ApplicationId appId)
			{
				this.appId = appId;
			}

			public override bool Matches(object o)
			{
				ApplicationEvent evt = (ApplicationEvent)o;
				return (evt.GetType() == ApplicationEventType.ApplicationResourcesCleanedup) && appId
					 == evt.GetApplicationID();
			}

			private readonly ApplicationId appId;
		}
	}
}
