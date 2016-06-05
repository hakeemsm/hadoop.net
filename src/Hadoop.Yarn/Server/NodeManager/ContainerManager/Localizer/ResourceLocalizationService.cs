using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using Com.Google.Common.Annotations;
using Com.Google.Common.Cache;
using Com.Google.Common.Util.Concurrent;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Org.Apache.Hadoop.Yarn.Ipc;
using Org.Apache.Hadoop.Yarn.Proto;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Api;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer.Event;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer.Security;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Recovery;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Security.Authorize;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Util;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer
{
	public class ResourceLocalizationService : CompositeService, EventHandler<LocalizationEvent
		>, LocalizationProtocol
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer.ResourceLocalizationService
			));

		public const string NmPrivateDir = "nmPrivate";

		public static readonly FsPermission NmPrivatePerm = new FsPermission((short)0x1c0
			);

		private Org.Apache.Hadoop.Ipc.Server server;

		private IPEndPoint localizationServerAddress;

		private long cacheTargetSize;

		private long cacheCleanupPeriod;

		private readonly ContainerExecutor exec;

		protected internal readonly Dispatcher dispatcher;

		private readonly DeletionService delService;

		private ResourceLocalizationService.LocalizerTracker localizerTracker;

		private RecordFactory recordFactory;

		private readonly ScheduledExecutorService cacheCleanup;

		private LocalizerTokenSecretManager secretManager;

		private NMStateStoreService stateStore;

		private LocalResourcesTracker publicRsrc;

		private LocalDirsHandlerService dirsHandler;

		private Context nmContext;

		/// <summary>
		/// Map of LocalResourceTrackers keyed by username, for private
		/// resources.
		/// </summary>
		private readonly ConcurrentMap<string, LocalResourcesTracker> privateRsrc = new ConcurrentHashMap
			<string, LocalResourcesTracker>();

		/// <summary>
		/// Map of LocalResourceTrackers keyed by appid, for application
		/// resources.
		/// </summary>
		private readonly ConcurrentMap<string, LocalResourcesTracker> appRsrc = new ConcurrentHashMap
			<string, LocalResourcesTracker>();

		internal FileContext lfs;

		public ResourceLocalizationService(Dispatcher dispatcher, ContainerExecutor exec, 
			DeletionService delService, LocalDirsHandlerService dirsHandler, Context context
			)
			: base(typeof(Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer.ResourceLocalizationService
				).FullName)
		{
			this.exec = exec;
			this.dispatcher = dispatcher;
			this.delService = delService;
			this.dirsHandler = dirsHandler;
			this.cacheCleanup = new ScheduledThreadPoolExecutor(1, new ThreadFactoryBuilder()
				.SetNameFormat("ResourceLocalizationService Cache Cleanup").Build());
			this.stateStore = context.GetNMStateStore();
			this.nmContext = context;
		}

		internal virtual FileContext GetLocalFileContext(Configuration conf)
		{
			try
			{
				return FileContext.GetLocalFSFileContext(conf);
			}
			catch (IOException)
			{
				throw new YarnRuntimeException("Failed to access local fs");
			}
		}

		private void ValidateConf(Configuration conf)
		{
			int perDirFileLimit = conf.GetInt(YarnConfiguration.NmLocalCacheMaxFilesPerDirectory
				, YarnConfiguration.DefaultNmLocalCacheMaxFilesPerDirectory);
			if (perDirFileLimit <= 36)
			{
				Log.Error(YarnConfiguration.NmLocalCacheMaxFilesPerDirectory + " parameter is configured with very low value."
					);
				throw new YarnRuntimeException(YarnConfiguration.NmLocalCacheMaxFilesPerDirectory
					 + " parameter is configured with a value less than 37.");
			}
			else
			{
				Log.Info("per directory file limit = " + perDirFileLimit);
			}
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceInit(Configuration conf)
		{
			this.ValidateConf(conf);
			this.publicRsrc = new LocalResourcesTrackerImpl(null, null, dispatcher, true, conf
				, stateStore);
			this.recordFactory = RecordFactoryProvider.GetRecordFactory(conf);
			try
			{
				lfs = GetLocalFileContext(conf);
				lfs.SetUMask(new FsPermission((short)FsPermission.DefaultUmask));
				if (!stateStore.CanRecover() || stateStore.IsNewlyCreated())
				{
					CleanUpLocalDirs(lfs, delService);
					InitializeLocalDirs(lfs);
					InitializeLogDirs(lfs);
				}
			}
			catch (Exception e)
			{
				throw new YarnRuntimeException("Failed to initialize LocalizationService", e);
			}
			cacheTargetSize = conf.GetLong(YarnConfiguration.NmLocalizerCacheTargetSizeMb, YarnConfiguration
				.DefaultNmLocalizerCacheTargetSizeMb) << 20;
			cacheCleanupPeriod = conf.GetLong(YarnConfiguration.NmLocalizerCacheCleanupIntervalMs
				, YarnConfiguration.DefaultNmLocalizerCacheCleanupIntervalMs);
			localizationServerAddress = conf.GetSocketAddr(YarnConfiguration.NmBindHost, YarnConfiguration
				.NmLocalizerAddress, YarnConfiguration.DefaultNmLocalizerAddress, YarnConfiguration
				.DefaultNmLocalizerPort);
			localizerTracker = CreateLocalizerTracker(conf);
			AddService(localizerTracker);
			dispatcher.Register(typeof(LocalizerEventType), localizerTracker);
			base.ServiceInit(conf);
		}

		//Recover localized resources after an NM restart
		/// <exception cref="Sharpen.URISyntaxException"/>
		public virtual void RecoverLocalizedResources(NMStateStoreService.RecoveredLocalizationState
			 state)
		{
			NMStateStoreService.LocalResourceTrackerState trackerState = state.GetPublicTrackerState
				();
			RecoverTrackerResources(publicRsrc, trackerState);
			foreach (KeyValuePair<string, NMStateStoreService.RecoveredUserResources> userEntry
				 in state.GetUserResources())
			{
				string user = userEntry.Key;
				NMStateStoreService.RecoveredUserResources userResources = userEntry.Value;
				trackerState = userResources.GetPrivateTrackerState();
				if (!trackerState.IsEmpty())
				{
					LocalResourcesTracker tracker = new LocalResourcesTrackerImpl(user, null, dispatcher
						, true, base.GetConfig(), stateStore);
					LocalResourcesTracker oldTracker = privateRsrc.PutIfAbsent(user, tracker);
					if (oldTracker != null)
					{
						tracker = oldTracker;
					}
					RecoverTrackerResources(tracker, trackerState);
				}
				foreach (KeyValuePair<ApplicationId, NMStateStoreService.LocalResourceTrackerState
					> appEntry in userResources.GetAppTrackerStates())
				{
					trackerState = appEntry.Value;
					if (!trackerState.IsEmpty())
					{
						ApplicationId appId = appEntry.Key;
						string appIdStr = ConverterUtils.ToString(appId);
						LocalResourcesTracker tracker = new LocalResourcesTrackerImpl(user, appId, dispatcher
							, false, base.GetConfig(), stateStore);
						LocalResourcesTracker oldTracker = appRsrc.PutIfAbsent(appIdStr, tracker);
						if (oldTracker != null)
						{
							tracker = oldTracker;
						}
						RecoverTrackerResources(tracker, trackerState);
					}
				}
			}
		}

		/// <exception cref="Sharpen.URISyntaxException"/>
		private void RecoverTrackerResources(LocalResourcesTracker tracker, NMStateStoreService.LocalResourceTrackerState
			 state)
		{
			foreach (YarnServerNodemanagerRecoveryProtos.LocalizedResourceProto proto in state
				.GetLocalizedResources())
			{
				LocalResource rsrc = new LocalResourcePBImpl(proto.GetResource());
				LocalResourceRequest req = new LocalResourceRequest(rsrc);
				Log.Info("Recovering localized resource " + req + " at " + proto.GetLocalPath());
				tracker.Handle(new ResourceRecoveredEvent(req, new Path(proto.GetLocalPath()), proto
					.GetSize()));
			}
			foreach (KeyValuePair<YarnProtos.LocalResourceProto, Path> entry in state.GetInProgressResources
				())
			{
				LocalResource rsrc = new LocalResourcePBImpl(entry.Key);
				LocalResourceRequest req = new LocalResourceRequest(rsrc);
				Path localPath = entry.Value;
				tracker.Handle(new ResourceRecoveredEvent(req, localPath, 0));
				// delete any in-progress localizations, containers will request again
				Log.Info("Deleting in-progress localization for " + req + " at " + localPath);
				tracker.Remove(tracker.GetLocalizedResource(req), delService);
			}
		}

		// TODO: remove untracked directories in local filesystem
		public virtual LocalizerHeartbeatResponse Heartbeat(LocalizerStatus status)
		{
			return localizerTracker.ProcessHeartbeat(status);
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStart()
		{
			cacheCleanup.ScheduleWithFixedDelay(new ResourceLocalizationService.CacheCleanup(
				dispatcher), cacheCleanupPeriod, cacheCleanupPeriod, TimeUnit.Milliseconds);
			server = CreateServer();
			server.Start();
			localizationServerAddress = GetConfig().UpdateConnectAddr(YarnConfiguration.NmBindHost
				, YarnConfiguration.NmLocalizerAddress, YarnConfiguration.DefaultNmLocalizerAddress
				, server.GetListenerAddress());
			Log.Info("Localizer started on port " + server.GetPort());
			base.ServiceStart();
		}

		internal virtual ResourceLocalizationService.LocalizerTracker CreateLocalizerTracker
			(Configuration conf)
		{
			return new ResourceLocalizationService.LocalizerTracker(this, conf);
		}

		internal virtual Org.Apache.Hadoop.Ipc.Server CreateServer()
		{
			Configuration conf = GetConfig();
			YarnRPC rpc = YarnRPC.Create(conf);
			if (UserGroupInformation.IsSecurityEnabled())
			{
				secretManager = new LocalizerTokenSecretManager();
			}
			Org.Apache.Hadoop.Ipc.Server server = rpc.GetServer(typeof(LocalizationProtocol), 
				this, localizationServerAddress, conf, secretManager, conf.GetInt(YarnConfiguration
				.NmLocalizerClientThreadCount, YarnConfiguration.DefaultNmLocalizerClientThreadCount
				));
			// Enable service authorization?
			if (conf.GetBoolean(CommonConfigurationKeysPublic.HadoopSecurityAuthorization, false
				))
			{
				server.RefreshServiceAcl(conf, new NMPolicyProvider());
			}
			return server;
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStop()
		{
			if (server != null)
			{
				server.Stop();
			}
			cacheCleanup.Shutdown();
			base.ServiceStop();
		}

		public virtual void Handle(LocalizationEvent @event)
		{
			switch (@event.GetType())
			{
				case LocalizationEventType.InitApplicationResources:
				{
					// TODO: create log dir as $logdir/$user/$appId
					HandleInitApplicationResources(((ApplicationLocalizationEvent)@event).GetApplication
						());
					break;
				}

				case LocalizationEventType.InitContainerResources:
				{
					HandleInitContainerResources((ContainerLocalizationRequestEvent)@event);
					break;
				}

				case LocalizationEventType.ContainerResourcesLocalized:
				{
					HandleContainerResourcesLocalized((ContainerLocalizationEvent)@event);
					break;
				}

				case LocalizationEventType.CacheCleanup:
				{
					HandleCacheCleanup(@event);
					break;
				}

				case LocalizationEventType.CleanupContainerResources:
				{
					HandleCleanupContainerResources((ContainerLocalizationCleanupEvent)@event);
					break;
				}

				case LocalizationEventType.DestroyApplicationResources:
				{
					HandleDestroyApplicationResources(((ApplicationLocalizationEvent)@event).GetApplication
						());
					break;
				}

				default:
				{
					throw new YarnRuntimeException("Unknown localization event: " + @event);
				}
			}
		}

		/// <summary>
		/// Handle event received the first time any container is scheduled
		/// by a given application.
		/// </summary>
		private void HandleInitApplicationResources(Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
			 app)
		{
			// 0) Create application tracking structs
			string userName = app.GetUser();
			privateRsrc.PutIfAbsent(userName, new LocalResourcesTrackerImpl(userName, null, dispatcher
				, true, base.GetConfig(), stateStore));
			string appIdStr = ConverterUtils.ToString(app.GetAppId());
			appRsrc.PutIfAbsent(appIdStr, new LocalResourcesTrackerImpl(app.GetUser(), app.GetAppId
				(), dispatcher, false, base.GetConfig(), stateStore));
			// 1) Signal container init
			//
			// This is handled by the ApplicationImpl state machine and allows
			// containers to proceed with launching.
			dispatcher.GetEventHandler().Handle(new ApplicationInitedEvent(app.GetAppId()));
		}

		/// <summary>
		/// For each of the requested resources for a container, determines the
		/// appropriate
		/// <see cref="LocalResourcesTracker"/>
		/// and forwards a
		/// <see cref="LocalResourceRequest"/>
		/// to that tracker.
		/// </summary>
		private void HandleInitContainerResources(ContainerLocalizationRequestEvent rsrcReqs
			)
		{
			Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container c = 
				rsrcReqs.GetContainer();
			// create a loading cache for the file statuses
			LoadingCache<Path, Future<FileStatus>> statCache = CacheBuilder.NewBuilder().Build
				(FSDownload.CreateStatusCacheLoader(GetConfig()));
			LocalizerContext ctxt = new LocalizerContext(c.GetUser(), c.GetContainerId(), c.GetCredentials
				(), statCache);
			IDictionary<LocalResourceVisibility, ICollection<LocalResourceRequest>> rsrcs = rsrcReqs
				.GetRequestedResources();
			foreach (KeyValuePair<LocalResourceVisibility, ICollection<LocalResourceRequest>>
				 e in rsrcs)
			{
				LocalResourcesTracker tracker = GetLocalResourcesTracker(e.Key, c.GetUser(), c.GetContainerId
					().GetApplicationAttemptId().GetApplicationId());
				foreach (LocalResourceRequest req in e.Value)
				{
					tracker.Handle(new ResourceRequestEvent(req, e.Key, ctxt));
				}
			}
		}

		/// <summary>
		/// Once a container's resources are localized, kill the corresponding
		/// <see cref="ContainerLocalizer"/>
		/// </summary>
		private void HandleContainerResourcesLocalized(ContainerLocalizationEvent @event)
		{
			Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container c = 
				@event.GetContainer();
			string locId = ConverterUtils.ToString(c.GetContainerId());
			localizerTracker.EndContainerLocalization(locId);
		}

		private void HandleCacheCleanup(LocalizationEvent @event)
		{
			ResourceRetentionSet retain = new ResourceRetentionSet(delService, cacheTargetSize
				);
			retain.AddResources(publicRsrc);
			Log.Debug("Resource cleanup (public) " + retain);
			foreach (LocalResourcesTracker t in privateRsrc.Values)
			{
				retain.AddResources(t);
				Log.Debug("Resource cleanup " + t.GetUser() + ":" + retain);
			}
		}

		//TODO Check if appRsrcs should also be added to the retention set.
		private void HandleCleanupContainerResources(ContainerLocalizationCleanupEvent rsrcCleanup
			)
		{
			Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container c = 
				rsrcCleanup.GetContainer();
			IDictionary<LocalResourceVisibility, ICollection<LocalResourceRequest>> rsrcs = rsrcCleanup
				.GetResources();
			foreach (KeyValuePair<LocalResourceVisibility, ICollection<LocalResourceRequest>>
				 e in rsrcs)
			{
				LocalResourcesTracker tracker = GetLocalResourcesTracker(e.Key, c.GetUser(), c.GetContainerId
					().GetApplicationAttemptId().GetApplicationId());
				foreach (LocalResourceRequest req in e.Value)
				{
					tracker.Handle(new ResourceReleaseEvent(req, c.GetContainerId()));
				}
			}
			string locId = ConverterUtils.ToString(c.GetContainerId());
			localizerTracker.CleanupPrivLocalizers(locId);
			// Delete the container directories
			string userName = c.GetUser();
			string containerIDStr = c.ToString();
			string appIDStr = ConverterUtils.ToString(c.GetContainerId().GetApplicationAttemptId
				().GetApplicationId());
			// Try deleting from good local dirs and full local dirs because a dir might
			// have gone bad while the app was running(disk full). In addition
			// a dir might have become good while the app was running.
			// Check if the container dir exists and if it does, try to delete it
			foreach (string localDir in dirsHandler.GetLocalDirsForCleanup())
			{
				// Delete the user-owned container-dir
				Path usersdir = new Path(localDir, ContainerLocalizer.Usercache);
				Path userdir = new Path(usersdir, userName);
				Path allAppsdir = new Path(userdir, ContainerLocalizer.Appcache);
				Path appDir = new Path(allAppsdir, appIDStr);
				Path containerDir = new Path(appDir, containerIDStr);
				SubmitDirForDeletion(userName, containerDir);
				// Delete the nmPrivate container-dir
				Path sysDir = new Path(localDir, NmPrivateDir);
				Path appSysDir = new Path(sysDir, appIDStr);
				Path containerSysDir = new Path(appSysDir, containerIDStr);
				SubmitDirForDeletion(null, containerSysDir);
			}
			dispatcher.GetEventHandler().Handle(new ContainerEvent(c.GetContainerId(), ContainerEventType
				.ContainerResourcesCleanedup));
		}

		private void SubmitDirForDeletion(string userName, Path dir)
		{
			try
			{
				lfs.GetFileStatus(dir);
				delService.Delete(userName, dir, new Path[] {  });
			}
			catch (UnsupportedFileSystemException ue)
			{
				Log.Warn("Local dir " + dir + " is an unsupported filesystem", ue);
			}
			catch (IOException)
			{
				// ignore
				return;
			}
		}

		private void HandleDestroyApplicationResources(Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
			 application)
		{
			string userName = application.GetUser();
			ApplicationId appId = application.GetAppId();
			string appIDStr = application.ToString();
			LocalResourcesTracker appLocalRsrcsTracker = Sharpen.Collections.Remove(appRsrc, 
				ConverterUtils.ToString(appId));
			if (appLocalRsrcsTracker != null)
			{
				foreach (LocalizedResource rsrc in appLocalRsrcsTracker)
				{
					Path localPath = rsrc.GetLocalPath();
					if (localPath != null)
					{
						try
						{
							stateStore.RemoveLocalizedResource(userName, appId, localPath);
						}
						catch (IOException e)
						{
							Log.Error("Unable to remove resource " + rsrc + " for " + appIDStr + " from state store"
								, e);
						}
					}
				}
			}
			else
			{
				Log.Warn("Removing uninitialized application " + application);
			}
			// Delete the application directories
			userName = application.GetUser();
			appIDStr = application.ToString();
			foreach (string localDir in dirsHandler.GetLocalDirsForCleanup())
			{
				// Delete the user-owned app-dir
				Path usersdir = new Path(localDir, ContainerLocalizer.Usercache);
				Path userdir = new Path(usersdir, userName);
				Path allAppsdir = new Path(userdir, ContainerLocalizer.Appcache);
				Path appDir = new Path(allAppsdir, appIDStr);
				SubmitDirForDeletion(userName, appDir);
				// Delete the nmPrivate app-dir
				Path sysDir = new Path(localDir, NmPrivateDir);
				Path appSysDir = new Path(sysDir, appIDStr);
				SubmitDirForDeletion(null, appSysDir);
			}
			// TODO: decrement reference counts of all resources associated with this
			// app
			dispatcher.GetEventHandler().Handle(new ApplicationEvent(application.GetAppId(), 
				ApplicationEventType.ApplicationResourcesCleanedup));
		}

		internal virtual LocalResourcesTracker GetLocalResourcesTracker(LocalResourceVisibility
			 visibility, string user, ApplicationId appId)
		{
			switch (visibility)
			{
				case LocalResourceVisibility.Public:
				default:
				{
					return publicRsrc;
				}

				case LocalResourceVisibility.Private:
				{
					return privateRsrc[user];
				}

				case LocalResourceVisibility.Application:
				{
					return appRsrc[ConverterUtils.ToString(appId)];
				}
			}
		}

		private string GetUserFileCachePath(string user)
		{
			return StringUtils.Join(Path.Separator, Arrays.AsList(".", ContainerLocalizer.Usercache
				, user, ContainerLocalizer.Filecache));
		}

		private string GetAppFileCachePath(string user, string appId)
		{
			return StringUtils.Join(Path.Separator, Arrays.AsList(".", ContainerLocalizer.Usercache
				, user, ContainerLocalizer.Appcache, appId, ContainerLocalizer.Filecache));
		}

		[VisibleForTesting]
		[InterfaceAudience.Private]
		public virtual ResourceLocalizationService.PublicLocalizer GetPublicLocalizer()
		{
			return localizerTracker.publicLocalizer;
		}

		[VisibleForTesting]
		[InterfaceAudience.Private]
		public virtual ResourceLocalizationService.LocalizerRunner GetLocalizerRunner(string
			 locId)
		{
			return localizerTracker.privLocalizers[locId];
		}

		[VisibleForTesting]
		[InterfaceAudience.Private]
		public virtual IDictionary<string, ResourceLocalizationService.LocalizerRunner> GetPrivateLocalizers
			()
		{
			return localizerTracker.privLocalizers;
		}

		/// <summary>
		/// Sub-component handling the spawning of
		/// <see cref="ContainerLocalizer"/>
		/// s
		/// </summary>
		internal class LocalizerTracker : AbstractService, EventHandler<LocalizerEvent>
		{
			private readonly ResourceLocalizationService.PublicLocalizer publicLocalizer;

			private readonly IDictionary<string, ResourceLocalizationService.LocalizerRunner>
				 privLocalizers;

			internal LocalizerTracker(ResourceLocalizationService _enclosing, Configuration conf
				)
				: this(conf, new Dictionary<string, ResourceLocalizationService.LocalizerRunner>(
					))
			{
				this._enclosing = _enclosing;
			}

			internal LocalizerTracker(ResourceLocalizationService _enclosing, Configuration conf
				, IDictionary<string, ResourceLocalizationService.LocalizerRunner> privLocalizers
				)
				: base(typeof(ResourceLocalizationService.LocalizerTracker).FullName)
			{
				this._enclosing = _enclosing;
				this.publicLocalizer = new ResourceLocalizationService.PublicLocalizer(this, conf
					);
				this.privLocalizers = privLocalizers;
			}

			/// <exception cref="System.Exception"/>
			protected override void ServiceStart()
			{
				lock (this)
				{
					this.publicLocalizer.Start();
					base.ServiceStart();
				}
			}

			public virtual LocalizerHeartbeatResponse ProcessHeartbeat(LocalizerStatus status
				)
			{
				string locId = status.GetLocalizerId();
				lock (this.privLocalizers)
				{
					ResourceLocalizationService.LocalizerRunner localizer = this.privLocalizers[locId
						];
					if (null == localizer)
					{
						// TODO process resources anyway
						ResourceLocalizationService.Log.Info("Unknown localizer with localizerId " + locId
							 + " is sending heartbeat. Ordering it to DIE");
						LocalizerHeartbeatResponse response = this._enclosing.recordFactory.NewRecordInstance
							<LocalizerHeartbeatResponse>();
						response.SetLocalizerAction(LocalizerAction.Die);
						return response;
					}
					return localizer.ProcessHeartbeat(status.GetResources());
				}
			}

			/// <exception cref="System.Exception"/>
			protected override void ServiceStop()
			{
				foreach (ResourceLocalizationService.LocalizerRunner localizer in this.privLocalizers
					.Values)
				{
					localizer.Interrupt();
				}
				this.publicLocalizer.Interrupt();
				base.ServiceStop();
			}

			public virtual void Handle(LocalizerEvent @event)
			{
				string locId = @event.GetLocalizerId();
				switch (@event.GetType())
				{
					case LocalizerEventType.RequestResourceLocalization:
					{
						// 0) find running localizer or start new thread
						LocalizerResourceRequestEvent req = (LocalizerResourceRequestEvent)@event;
						switch (req.GetVisibility())
						{
							case LocalResourceVisibility.Public:
							{
								this.publicLocalizer.AddResource(req);
								break;
							}

							case LocalResourceVisibility.Private:
							case LocalResourceVisibility.Application:
							{
								lock (this.privLocalizers)
								{
									ResourceLocalizationService.LocalizerRunner localizer = this.privLocalizers[locId
										];
									if (null == localizer)
									{
										ResourceLocalizationService.Log.Info("Created localizer for " + locId);
										localizer = new ResourceLocalizationService.LocalizerRunner(this, req.GetContext(
											), locId);
										this.privLocalizers[locId] = localizer;
										localizer.Start();
									}
									// 1) propagate event
									localizer.AddResource(req);
								}
								break;
							}
						}
						break;
					}
				}
			}

			public virtual void CleanupPrivLocalizers(string locId)
			{
				lock (this.privLocalizers)
				{
					ResourceLocalizationService.LocalizerRunner localizer = this.privLocalizers[locId
						];
					if (null == localizer)
					{
						return;
					}
					// ignore; already gone
					Sharpen.Collections.Remove(this.privLocalizers, locId);
					localizer.Interrupt();
				}
			}

			public virtual void EndContainerLocalization(string locId)
			{
				ResourceLocalizationService.LocalizerRunner localizer;
				lock (this.privLocalizers)
				{
					localizer = this.privLocalizers[locId];
					if (null == localizer)
					{
						return;
					}
				}
				// ignore
				localizer.EndContainerLocalization();
			}

			private readonly ResourceLocalizationService _enclosing;
		}

		private static ExecutorService CreateLocalizerExecutor(Configuration conf)
		{
			int nThreads = conf.GetInt(YarnConfiguration.NmLocalizerFetchThreadCount, YarnConfiguration
				.DefaultNmLocalizerFetchThreadCount);
			ThreadFactory tf = new ThreadFactoryBuilder().SetNameFormat("PublicLocalizer #%d"
				).Build();
			return Executors.NewFixedThreadPool(nThreads, tf);
		}

		internal class PublicLocalizer : Sharpen.Thread
		{
			internal readonly FileContext lfs;

			internal readonly Configuration conf;

			internal readonly ExecutorService threadPool;

			internal readonly CompletionService<Path> queue;

			internal readonly IDictionary<Future<Path>, LocalizerResourceRequestEvent> pending;

			internal PublicLocalizer(ResourceLocalizationService _enclosing, Configuration conf
				)
				: base("Public Localizer")
			{
				this._enclosing = _enclosing;
				// Its shared between public localizer and dispatcher thread.
				this.lfs = this._enclosing.GetLocalFileContext(conf);
				this.conf = conf;
				this.pending = Sharpen.Collections.SynchronizedMap(new Dictionary<Future<Path>, LocalizerResourceRequestEvent
					>());
				this.threadPool = ResourceLocalizationService.CreateLocalizerExecutor(conf);
				this.queue = new ExecutorCompletionService<Path>(this.threadPool);
			}

			public virtual void AddResource(LocalizerResourceRequestEvent request)
			{
				// TODO handle failures, cancellation, requests by other containers
				LocalizedResource rsrc = request.GetResource();
				LocalResourceRequest key = rsrc.GetRequest();
				ResourceLocalizationService.Log.Info("Downloading public rsrc:" + key);
				/*
				* Here multiple containers may request the same resource. So we need
				* to start downloading only when
				* 1) ResourceState == DOWNLOADING
				* 2) We are able to acquire non blocking semaphore lock.
				* If not we will skip this resource as either it is getting downloaded
				* or it FAILED / LOCALIZED.
				*/
				if (rsrc.TryAcquire())
				{
					if (rsrc.GetState() == ResourceState.Downloading)
					{
						LocalResource resource = request.GetResource().GetRequest();
						try
						{
							Path publicRootPath = this._enclosing.dirsHandler.GetLocalPathForWrite("." + Path
								.Separator + ContainerLocalizer.Filecache, ContainerLocalizer.GetEstimatedSize(resource
								), true);
							Path publicDirDestPath = this._enclosing.publicRsrc.GetPathForLocalization(key, publicRootPath
								, this._enclosing.delService);
							if (!publicDirDestPath.GetParent().Equals(publicRootPath))
							{
								DiskChecker.CheckDir(new FilePath(publicDirDestPath.ToUri().GetPath()));
							}
							// In case this is not a newly initialized nm state, ensure
							// initialized local/log dirs similar to LocalizerRunner
							this._enclosing.GetInitializedLocalDirs();
							this._enclosing.GetInitializedLogDirs();
							// explicitly synchronize pending here to avoid future task
							// completing and being dequeued before pending updated
							lock (this.pending)
							{
								this.pending[this.queue.Submit(new FSDownload(this.lfs, null, this.conf, publicDirDestPath
									, resource, request.GetContext().GetStatCache()))] = request;
							}
						}
						catch (IOException e)
						{
							rsrc.Unlock();
							this._enclosing.publicRsrc.Handle(new ResourceFailedLocalizationEvent(request.GetResource
								().GetRequest(), e.Message));
							ResourceLocalizationService.Log.Error("Local path for public localization is not found. "
								 + " May be disks failed.", e);
						}
						catch (ArgumentException ie)
						{
							rsrc.Unlock();
							this._enclosing.publicRsrc.Handle(new ResourceFailedLocalizationEvent(request.GetResource
								().GetRequest(), ie.Message));
							ResourceLocalizationService.Log.Error("Local path for public localization is not found. "
								 + " Incorrect path. " + request.GetResource().GetRequest().GetPath(), ie);
						}
						catch (RejectedExecutionException re)
						{
							rsrc.Unlock();
							this._enclosing.publicRsrc.Handle(new ResourceFailedLocalizationEvent(request.GetResource
								().GetRequest(), re.Message));
							ResourceLocalizationService.Log.Error("Failed to submit rsrc " + rsrc + " for download."
								 + " Either queue is full or threadpool is shutdown.", re);
						}
					}
					else
					{
						rsrc.Unlock();
					}
				}
			}

			public override void Run()
			{
				try
				{
					// TODO shutdown, better error handling esp. DU
					while (!Sharpen.Thread.CurrentThread().IsInterrupted())
					{
						try
						{
							Future<Path> completed = this.queue.Take();
							LocalizerResourceRequestEvent assoc = Sharpen.Collections.Remove(this.pending, completed
								);
							try
							{
								Path local = completed.Get();
								if (null == assoc)
								{
									ResourceLocalizationService.Log.Error("Localized unknown resource to " + completed
										);
									// TODO delete
									return;
								}
								LocalResourceRequest key = assoc.GetResource().GetRequest();
								this._enclosing.publicRsrc.Handle(new ResourceLocalizedEvent(key, local, FileUtil
									.GetDU(new FilePath(local.ToUri()))));
								assoc.GetResource().Unlock();
							}
							catch (ExecutionException e)
							{
								ResourceLocalizationService.Log.Info("Failed to download resource " + assoc.GetResource
									(), e.InnerException);
								LocalResourceRequest req = assoc.GetResource().GetRequest();
								this._enclosing.publicRsrc.Handle(new ResourceFailedLocalizationEvent(req, e.Message
									));
								assoc.GetResource().Unlock();
							}
							catch (CancellationException)
							{
							}
						}
						catch (Exception)
						{
							// ignore; shutting down
							return;
						}
					}
				}
				catch (Exception t)
				{
					ResourceLocalizationService.Log.Fatal("Error: Shutting down", t);
				}
				finally
				{
					ResourceLocalizationService.Log.Info("Public cache exiting");
					this.threadPool.ShutdownNow();
				}
			}

			private readonly ResourceLocalizationService _enclosing;
		}

		/// <summary>
		/// Runs the
		/// <see cref="ContainerLocalizer"/>
		/// itself in a separate process with
		/// access to user's credentials. One
		/// <see cref="LocalizerRunner"/>
		/// per localizerId.
		/// </summary>
		internal class LocalizerRunner : Sharpen.Thread
		{
			internal readonly LocalizerContext context;

			internal readonly string localizerId;

			internal readonly IDictionary<LocalResourceRequest, LocalizerResourceRequestEvent
				> scheduled;

			internal readonly IList<LocalizerResourceRequestEvent> pending;

			private AtomicBoolean killContainerLocalizer = new AtomicBoolean(false);

			private readonly RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory
				(this._enclosing.GetConfig());

			internal LocalizerRunner(ResourceLocalizationService _enclosing, LocalizerContext
				 context, string localizerId)
				: base("LocalizerRunner for " + localizerId)
			{
				this._enclosing = _enclosing;
				// Its a shared list between Private Localizer and dispatcher thread.
				// TODO: threadsafe, use outer?
				this.context = context;
				this.localizerId = localizerId;
				this.pending = Sharpen.Collections.SynchronizedList(new AList<LocalizerResourceRequestEvent
					>());
				this.scheduled = new Dictionary<LocalResourceRequest, LocalizerResourceRequestEvent
					>();
			}

			public virtual void AddResource(LocalizerResourceRequestEvent request)
			{
				this.pending.AddItem(request);
			}

			public virtual void EndContainerLocalization()
			{
				this.killContainerLocalizer.Set(true);
			}

			/// <summary>Find next resource to be given to a spawned localizer.</summary>
			/// <returns>the next resource to be localized</returns>
			private LocalResource FindNextResource()
			{
				lock (this.pending)
				{
					for (IEnumerator<LocalizerResourceRequestEvent> i = this.pending.GetEnumerator(); 
						i.HasNext(); )
					{
						LocalizerResourceRequestEvent evt = i.Next();
						LocalizedResource nRsrc = evt.GetResource();
						// Resource download should take place ONLY if resource is in
						// Downloading state
						if (nRsrc.GetState() != ResourceState.Downloading)
						{
							i.Remove();
							continue;
						}
						/*
						* Multiple containers will try to download the same resource. So the
						* resource download should start only if
						* 1) We can acquire a non blocking semaphore lock on resource
						* 2) Resource is still in DOWNLOADING state
						*/
						if (nRsrc.TryAcquire())
						{
							if (nRsrc.GetState() == ResourceState.Downloading)
							{
								LocalResourceRequest nextRsrc = nRsrc.GetRequest();
								LocalResource next = this.recordFactory.NewRecordInstance<LocalResource>();
								next.SetResource(ConverterUtils.GetYarnUrlFromPath(nextRsrc.GetPath()));
								next.SetTimestamp(nextRsrc.GetTimestamp());
								next.SetType(nextRsrc.GetType());
								next.SetVisibility(evt.GetVisibility());
								next.SetPattern(evt.GetPattern());
								this.scheduled[nextRsrc] = evt;
								return next;
							}
							else
							{
								// Need to release acquired lock
								nRsrc.Unlock();
							}
						}
					}
					return null;
				}
			}

			internal virtual LocalizerHeartbeatResponse ProcessHeartbeat(IList<LocalResourceStatus
				> remoteResourceStatuses)
			{
				LocalizerHeartbeatResponse response = this.recordFactory.NewRecordInstance<LocalizerHeartbeatResponse
					>();
				string user = this.context.GetUser();
				ApplicationId applicationId = this.context.GetContainerId().GetApplicationAttemptId
					().GetApplicationId();
				bool fetchFailed = false;
				// Update resource statuses.
				foreach (LocalResourceStatus stat in remoteResourceStatuses)
				{
					LocalResource rsrc = stat.GetResource();
					LocalResourceRequest req = null;
					try
					{
						req = new LocalResourceRequest(rsrc);
					}
					catch (URISyntaxException)
					{
					}
					// TODO fail? Already translated several times...
					LocalizerResourceRequestEvent assoc = this.scheduled[req];
					if (assoc == null)
					{
						// internal error
						ResourceLocalizationService.Log.Error("Unknown resource reported: " + req);
						continue;
					}
					switch (stat.GetStatus())
					{
						case ResourceStatusType.FetchSuccess:
						{
							// notify resource
							try
							{
								this._enclosing.GetLocalResourcesTracker(req.GetVisibility(), user, applicationId
									).Handle(new ResourceLocalizedEvent(req, ConverterUtils.GetPathFromYarnURL(stat.
									GetLocalPath()), stat.GetLocalSize()));
							}
							catch (URISyntaxException)
							{
							}
							// unlocking the resource and removing it from scheduled resource
							// list
							assoc.GetResource().Unlock();
							Sharpen.Collections.Remove(this.scheduled, req);
							break;
						}

						case ResourceStatusType.FetchPending:
						{
							break;
						}

						case ResourceStatusType.FetchFailure:
						{
							string diagnostics = stat.GetException().ToString();
							ResourceLocalizationService.Log.Warn(req + " failed: " + diagnostics);
							fetchFailed = true;
							this._enclosing.GetLocalResourcesTracker(req.GetVisibility(), user, applicationId
								).Handle(new ResourceFailedLocalizationEvent(req, diagnostics));
							// unlocking the resource and removing it from scheduled resource
							// list
							assoc.GetResource().Unlock();
							Sharpen.Collections.Remove(this.scheduled, req);
							break;
						}

						default:
						{
							ResourceLocalizationService.Log.Info("Unknown status: " + stat.GetStatus());
							fetchFailed = true;
							this._enclosing.GetLocalResourcesTracker(req.GetVisibility(), user, applicationId
								).Handle(new ResourceFailedLocalizationEvent(req, stat.GetException().GetMessage
								()));
							break;
						}
					}
				}
				if (fetchFailed || this.killContainerLocalizer.Get())
				{
					response.SetLocalizerAction(LocalizerAction.Die);
					return response;
				}
				// Give the localizer resources for remote-fetching.
				IList<ResourceLocalizationSpec> rsrcs = new AList<ResourceLocalizationSpec>();
				/*
				* TODO : It doesn't support multiple downloads per ContainerLocalizer
				* at the same time. We need to think whether we should support this.
				*/
				LocalResource next = this.FindNextResource();
				if (next != null)
				{
					try
					{
						ResourceLocalizationSpec resource = NodeManagerBuilderUtils.NewResourceLocalizationSpec
							(next, this.GetPathForLocalization(next));
						rsrcs.AddItem(resource);
					}
					catch (IOException e)
					{
						ResourceLocalizationService.Log.Error("local path for PRIVATE localization could not be "
							 + "found. Disks might have failed.", e);
					}
					catch (ArgumentException e)
					{
						ResourceLocalizationService.Log.Error("Inorrect path for PRIVATE localization." +
							 next.GetResource().GetFile(), e);
					}
					catch (URISyntaxException)
					{
					}
				}
				//TODO fail? Already translated several times...
				response.SetLocalizerAction(LocalizerAction.Live);
				response.SetResourceSpecs(rsrcs);
				return response;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Sharpen.URISyntaxException"/>
			private Path GetPathForLocalization(LocalResource rsrc)
			{
				string user = this.context.GetUser();
				ApplicationId appId = this.context.GetContainerId().GetApplicationAttemptId().GetApplicationId
					();
				LocalResourceVisibility vis = rsrc.GetVisibility();
				LocalResourcesTracker tracker = this._enclosing.GetLocalResourcesTracker(vis, user
					, appId);
				string cacheDirectory = null;
				if (vis == LocalResourceVisibility.Private)
				{
					// PRIVATE Only
					cacheDirectory = this._enclosing.GetUserFileCachePath(user);
				}
				else
				{
					// APPLICATION ONLY
					cacheDirectory = this._enclosing.GetAppFileCachePath(user, appId.ToString());
				}
				Path dirPath = this._enclosing.dirsHandler.GetLocalPathForWrite(cacheDirectory, ContainerLocalizer
					.GetEstimatedSize(rsrc), false);
				return tracker.GetPathForLocalization(new LocalResourceRequest(rsrc), dirPath, this
					._enclosing.delService);
			}

			public override void Run()
			{
				// dispatcher not typed
				Path nmPrivateCTokensPath = null;
				Exception exception = null;
				try
				{
					// Get nmPrivateDir
					nmPrivateCTokensPath = this._enclosing.dirsHandler.GetLocalPathForWrite(ResourceLocalizationService
						.NmPrivateDir + Path.Separator + string.Format(ContainerLocalizer.TokenFileNameFmt
						, this.localizerId));
					// 0) init queue, etc.
					// 1) write credentials to private dir
					this.WriteCredentials(nmPrivateCTokensPath);
					// 2) exec initApplication and wait
					IList<string> localDirs = this._enclosing.GetInitializedLocalDirs();
					IList<string> logDirs = this._enclosing.GetInitializedLogDirs();
					if (this._enclosing.dirsHandler.AreDisksHealthy())
					{
						this._enclosing.exec.StartLocalizer(nmPrivateCTokensPath, this._enclosing.localizationServerAddress
							, this.context.GetUser(), ConverterUtils.ToString(this.context.GetContainerId().
							GetApplicationAttemptId().GetApplicationId()), this.localizerId, this._enclosing
							.dirsHandler);
					}
					else
					{
						throw new IOException("All disks failed. " + this._enclosing.dirsHandler.GetDisksHealthReport
							(false));
					}
				}
				catch (FSError fe)
				{
					// TODO handle ExitCodeException separately?
					exception = fe;
				}
				catch (Exception e)
				{
					exception = e;
				}
				finally
				{
					if (exception != null)
					{
						ResourceLocalizationService.Log.Info("Localizer failed", exception);
						// On error, report failure to Container and signal ABORT
						// Notify resource of failed localization
						ContainerId cId = this.context.GetContainerId();
						this._enclosing.dispatcher.GetEventHandler().Handle(new ContainerResourceFailedEvent
							(cId, null, exception.Message));
					}
					IList<Path> paths = new AList<Path>();
					foreach (LocalizerResourceRequestEvent @event in this.scheduled.Values)
					{
						// This means some resources were in downloading state. Schedule
						// deletion task for localization dir and tmp dir used for downloading
						Path locRsrcPath = @event.GetResource().GetLocalPath();
						if (locRsrcPath != null)
						{
							Path locRsrcDirPath = locRsrcPath.GetParent();
							paths.AddItem(locRsrcDirPath);
							paths.AddItem(new Path(locRsrcDirPath + "_tmp"));
						}
						@event.GetResource().Unlock();
					}
					if (!paths.IsEmpty())
					{
						this._enclosing.delService.Delete(this.context.GetUser(), null, Sharpen.Collections.ToArray
							(paths, new Path[paths.Count]));
					}
					this._enclosing.delService.Delete(null, nmPrivateCTokensPath, new Path[] {  });
				}
			}

			/// <exception cref="System.IO.IOException"/>
			private Credentials GetSystemCredentialsSentFromRM(LocalizerContext localizerContext
				)
			{
				ApplicationId appId = localizerContext.GetContainerId().GetApplicationAttemptId()
					.GetApplicationId();
				Credentials systemCredentials = this._enclosing.nmContext.GetSystemCredentialsForApps
					()[appId];
				if (systemCredentials == null)
				{
					return null;
				}
				if (ResourceLocalizationService.Log.IsDebugEnabled())
				{
					ResourceLocalizationService.Log.Debug("Adding new framework-token for " + appId +
						 " for localization: " + systemCredentials.GetAllTokens());
				}
				return systemCredentials;
			}

			/// <exception cref="System.IO.IOException"/>
			private void WriteCredentials(Path nmPrivateCTokensPath)
			{
				DataOutputStream tokenOut = null;
				try
				{
					Credentials credentials = this.context.GetCredentials();
					if (UserGroupInformation.IsSecurityEnabled())
					{
						Credentials systemCredentials = this.GetSystemCredentialsSentFromRM(this.context);
						if (systemCredentials != null)
						{
							credentials = systemCredentials;
						}
					}
					FileContext lfs = this._enclosing.GetLocalFileContext(this._enclosing.GetConfig()
						);
					tokenOut = lfs.Create(nmPrivateCTokensPath, EnumSet.Of(CreateFlag.Create, CreateFlag
						.Overwrite));
					ResourceLocalizationService.Log.Info("Writing credentials to the nmPrivate file "
						 + nmPrivateCTokensPath.ToString() + ". Credentials list: ");
					if (ResourceLocalizationService.Log.IsDebugEnabled())
					{
						foreach (Org.Apache.Hadoop.Security.Token.Token<TokenIdentifier> tk in credentials
							.GetAllTokens())
						{
							ResourceLocalizationService.Log.Debug(tk.GetService() + " : " + tk.EncodeToUrlString
								());
						}
					}
					if (UserGroupInformation.IsSecurityEnabled())
					{
						credentials = new Credentials(credentials);
						LocalizerTokenIdentifier id = this._enclosing.secretManager.CreateIdentifier();
						Org.Apache.Hadoop.Security.Token.Token<LocalizerTokenIdentifier> localizerToken = 
							new Org.Apache.Hadoop.Security.Token.Token<LocalizerTokenIdentifier>(id, this._enclosing
							.secretManager);
						credentials.AddToken(id.GetKind(), localizerToken);
					}
					credentials.WriteTokenStorageToStream(tokenOut);
				}
				finally
				{
					if (tokenOut != null)
					{
						tokenOut.Close();
					}
				}
			}

			private readonly ResourceLocalizationService _enclosing;
		}

		internal class CacheCleanup : Sharpen.Thread
		{
			private readonly Dispatcher dispatcher;

			public CacheCleanup(Dispatcher dispatcher)
				: base("CacheCleanup")
			{
				this.dispatcher = dispatcher;
			}

			public override void Run()
			{
				// dispatcher not typed
				dispatcher.GetEventHandler().Handle(new LocalizationEvent(LocalizationEventType.CacheCleanup
					));
			}
		}

		private void InitializeLocalDirs(FileContext lfs)
		{
			IList<string> localDirs = dirsHandler.GetLocalDirs();
			foreach (string localDir in localDirs)
			{
				InitializeLocalDir(lfs, localDir);
			}
		}

		private void InitializeLocalDir(FileContext lfs, string localDir)
		{
			IDictionary<Path, FsPermission> pathPermissionMap = GetLocalDirsPathPermissionsMap
				(localDir);
			foreach (KeyValuePair<Path, FsPermission> entry in pathPermissionMap)
			{
				FileStatus status;
				try
				{
					status = lfs.GetFileStatus(entry.Key);
				}
				catch (FileNotFoundException)
				{
					status = null;
				}
				catch (IOException ie)
				{
					string msg = "Could not get file status for local dir " + entry.Key;
					Log.Warn(msg, ie);
					throw new YarnRuntimeException(msg, ie);
				}
				if (status == null)
				{
					try
					{
						lfs.Mkdir(entry.Key, entry.Value, true);
						status = lfs.GetFileStatus(entry.Key);
					}
					catch (IOException e)
					{
						string msg = "Could not initialize local dir " + entry.Key;
						Log.Warn(msg, e);
						throw new YarnRuntimeException(msg, e);
					}
				}
				FsPermission perms = status.GetPermission();
				if (!perms.Equals(entry.Value))
				{
					try
					{
						lfs.SetPermission(entry.Key, entry.Value);
					}
					catch (IOException ie)
					{
						string msg = "Could not set permissions for local dir " + entry.Key;
						Log.Warn(msg, ie);
						throw new YarnRuntimeException(msg, ie);
					}
				}
			}
		}

		private void InitializeLogDirs(FileContext lfs)
		{
			IList<string> logDirs = dirsHandler.GetLogDirs();
			foreach (string logDir in logDirs)
			{
				InitializeLogDir(lfs, logDir);
			}
		}

		private void InitializeLogDir(FileContext lfs, string logDir)
		{
			try
			{
				lfs.Mkdir(new Path(logDir), null, true);
			}
			catch (FileAlreadyExistsException)
			{
			}
			catch (IOException e)
			{
				// do nothing
				string msg = "Could not initialize log dir " + logDir;
				Log.Warn(msg, e);
				throw new YarnRuntimeException(msg, e);
			}
		}

		private void CleanUpLocalDirs(FileContext lfs, DeletionService del)
		{
			foreach (string localDir in dirsHandler.GetLocalDirsForCleanup())
			{
				CleanUpLocalDir(lfs, del, localDir);
			}
		}

		private void CleanUpLocalDir(FileContext lfs, DeletionService del, string localDir
			)
		{
			long currentTimeStamp = Runtime.CurrentTimeMillis();
			RenameLocalDir(lfs, localDir, ContainerLocalizer.Usercache, currentTimeStamp);
			RenameLocalDir(lfs, localDir, ContainerLocalizer.Filecache, currentTimeStamp);
			RenameLocalDir(lfs, localDir, ResourceLocalizationService.NmPrivateDir, currentTimeStamp
				);
			try
			{
				DeleteLocalDir(lfs, del, localDir);
			}
			catch (IOException)
			{
				// Do nothing, just give the warning
				Log.Warn("Failed to delete localDir: " + localDir);
			}
		}

		private void RenameLocalDir(FileContext lfs, string localDir, string localSubDir, 
			long currentTimeStamp)
		{
			try
			{
				lfs.Rename(new Path(localDir, localSubDir), new Path(localDir, localSubDir + "_DEL_"
					 + currentTimeStamp));
			}
			catch (FileNotFoundException)
			{
			}
			catch (Exception)
			{
				// No need to handle this exception
				// localSubDir may not be exist
				// Do nothing, just give the warning
				Log.Warn("Failed to rename the local file under " + localDir + "/" + localSubDir);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void DeleteLocalDir(FileContext lfs, DeletionService del, string localDir
			)
		{
			RemoteIterator<FileStatus> fileStatus = lfs.ListStatus(new Path(localDir));
			if (fileStatus != null)
			{
				while (fileStatus.HasNext())
				{
					FileStatus status = fileStatus.Next();
					try
					{
						if (status.GetPath().GetName().Matches(".*" + ContainerLocalizer.Usercache + "_DEL_.*"
							))
						{
							Log.Info("usercache path : " + status.GetPath().ToString());
							CleanUpFilesPerUserDir(lfs, del, status.GetPath());
						}
						else
						{
							if (status.GetPath().GetName().Matches(".*" + NmPrivateDir + "_DEL_.*") || status
								.GetPath().GetName().Matches(".*" + ContainerLocalizer.Filecache + "_DEL_.*"))
							{
								del.Delete(null, status.GetPath(), new Path[] {  });
							}
						}
					}
					catch (IOException)
					{
						// Do nothing, just give the warning
						Log.Warn("Failed to delete this local Directory: " + status.GetPath().GetName());
					}
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void CleanUpFilesPerUserDir(FileContext lfs, DeletionService del, Path userDirPath
			)
		{
			RemoteIterator<FileStatus> userDirStatus = lfs.ListStatus(userDirPath);
			DeletionService.FileDeletionTask dependentDeletionTask = del.CreateFileDeletionTask
				(null, userDirPath, new Path[] {  });
			if (userDirStatus != null && userDirStatus.HasNext())
			{
				IList<DeletionService.FileDeletionTask> deletionTasks = new AList<DeletionService.FileDeletionTask
					>();
				while (userDirStatus.HasNext())
				{
					FileStatus status = userDirStatus.Next();
					string owner = status.GetOwner();
					DeletionService.FileDeletionTask deletionTask = del.CreateFileDeletionTask(owner, 
						null, new Path[] { status.GetPath() });
					deletionTask.AddFileDeletionTaskDependency(dependentDeletionTask);
					deletionTasks.AddItem(deletionTask);
				}
				foreach (DeletionService.FileDeletionTask task in deletionTasks)
				{
					del.ScheduleFileDeletionTask(task);
				}
			}
			else
			{
				del.ScheduleFileDeletionTask(dependentDeletionTask);
			}
		}

		/// <summary>Synchronized method to get a list of initialized local dirs.</summary>
		/// <remarks>
		/// Synchronized method to get a list of initialized local dirs. Method will
		/// check each local dir to ensure it has been setup correctly and will attempt
		/// to fix any issues it finds.
		/// </remarks>
		/// <returns>list of initialized local dirs</returns>
		private IList<string> GetInitializedLocalDirs()
		{
			lock (this)
			{
				IList<string> dirs = dirsHandler.GetLocalDirs();
				IList<string> checkFailedDirs = new AList<string>();
				foreach (string dir in dirs)
				{
					try
					{
						CheckLocalDir(dir);
					}
					catch (YarnRuntimeException)
					{
						checkFailedDirs.AddItem(dir);
					}
				}
				foreach (string dir_1 in checkFailedDirs)
				{
					Log.Info("Attempting to initialize " + dir_1);
					InitializeLocalDir(lfs, dir_1);
					try
					{
						CheckLocalDir(dir_1);
					}
					catch (YarnRuntimeException e)
					{
						string msg = "Failed to setup local dir " + dir_1 + ", which was marked as good.";
						Log.Warn(msg, e);
						throw new YarnRuntimeException(msg, e);
					}
				}
				return dirs;
			}
		}

		private bool CheckLocalDir(string localDir)
		{
			IDictionary<Path, FsPermission> pathPermissionMap = GetLocalDirsPathPermissionsMap
				(localDir);
			foreach (KeyValuePair<Path, FsPermission> entry in pathPermissionMap)
			{
				FileStatus status;
				try
				{
					status = lfs.GetFileStatus(entry.Key);
				}
				catch (Exception e)
				{
					string msg = "Could not carry out resource dir checks for " + localDir + ", which was marked as good";
					Log.Warn(msg, e);
					throw new YarnRuntimeException(msg, e);
				}
				if (!status.GetPermission().Equals(entry.Value))
				{
					string msg = "Permissions incorrectly set for dir " + entry.Key + ", should be " 
						+ entry.Value + ", actual value = " + status.GetPermission();
					Log.Warn(msg);
					throw new YarnRuntimeException(msg);
				}
			}
			return true;
		}

		private IDictionary<Path, FsPermission> GetLocalDirsPathPermissionsMap(string localDir
			)
		{
			IDictionary<Path, FsPermission> localDirPathFsPermissionsMap = new Dictionary<Path
				, FsPermission>();
			FsPermission defaultPermission = FsPermission.GetDirDefault().ApplyUMask(lfs.GetUMask
				());
			FsPermission nmPrivatePermission = NmPrivatePerm.ApplyUMask(lfs.GetUMask());
			Path userDir = new Path(localDir, ContainerLocalizer.Usercache);
			Path fileDir = new Path(localDir, ContainerLocalizer.Filecache);
			Path sysDir = new Path(localDir, NmPrivateDir);
			localDirPathFsPermissionsMap[userDir] = defaultPermission;
			localDirPathFsPermissionsMap[fileDir] = defaultPermission;
			localDirPathFsPermissionsMap[sysDir] = nmPrivatePermission;
			return localDirPathFsPermissionsMap;
		}

		/// <summary>Synchronized method to get a list of initialized log dirs.</summary>
		/// <remarks>
		/// Synchronized method to get a list of initialized log dirs. Method will
		/// check each local dir to ensure it has been setup correctly and will attempt
		/// to fix any issues it finds.
		/// </remarks>
		/// <returns>list of initialized log dirs</returns>
		private IList<string> GetInitializedLogDirs()
		{
			lock (this)
			{
				IList<string> dirs = dirsHandler.GetLogDirs();
				InitializeLogDirs(lfs);
				return dirs;
			}
		}
	}
}
