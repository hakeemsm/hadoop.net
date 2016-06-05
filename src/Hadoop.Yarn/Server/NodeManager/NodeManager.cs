using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Metrics2.Lib;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Org.Apache.Hadoop.Yarn.Server.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Metrics;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Recovery;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Security;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp;
using Org.Apache.Hadoop.Yarn.Server.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager
{
	public class NodeManager : CompositeService, EventHandler<NodeManagerEvent>
	{
		/// <summary>Priority of the NodeManager shutdown hook.</summary>
		public const int ShutdownHookPriority = 30;

		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Nodemanager.NodeManager
			));

		protected internal readonly NodeManagerMetrics metrics = NodeManagerMetrics.Create
			();

		private ApplicationACLsManager aclsManager;

		private NodeHealthCheckerService nodeHealthChecker;

		private LocalDirsHandlerService dirsHandler;

		private Context context;

		private AsyncDispatcher dispatcher;

		private ContainerManagerImpl containerManager;

		private NodeStatusUpdater nodeStatusUpdater;

		private static CompositeService.CompositeServiceShutdownHook nodeManagerShutdownHook;

		private NMStateStoreService nmStore = null;

		private AtomicBoolean isStopping = new AtomicBoolean(false);

		private bool rmWorkPreservingRestartEnabled;

		private bool shouldExitOnShutdownEvent = false;

		public NodeManager()
			: base(typeof(Org.Apache.Hadoop.Yarn.Server.Nodemanager.NodeManager).FullName)
		{
		}

		protected internal virtual NodeStatusUpdater CreateNodeStatusUpdater(Context context
			, Dispatcher dispatcher, NodeHealthCheckerService healthChecker)
		{
			return new NodeStatusUpdaterImpl(context, dispatcher, healthChecker, metrics);
		}

		protected internal virtual NodeResourceMonitor CreateNodeResourceMonitor()
		{
			return new NodeResourceMonitorImpl();
		}

		protected internal virtual ContainerManagerImpl CreateContainerManager(Context context
			, ContainerExecutor exec, DeletionService del, NodeStatusUpdater nodeStatusUpdater
			, ApplicationACLsManager aclsManager, LocalDirsHandlerService dirsHandler)
		{
			return new ContainerManagerImpl(context, exec, del, nodeStatusUpdater, metrics, aclsManager
				, dirsHandler);
		}

		protected internal virtual WebServer CreateWebServer(Context nmContext, ResourceView
			 resourceView, ApplicationACLsManager aclsManager, LocalDirsHandlerService dirsHandler
			)
		{
			return new WebServer(nmContext, resourceView, aclsManager, dirsHandler);
		}

		protected internal virtual DeletionService CreateDeletionService(ContainerExecutor
			 exec)
		{
			return new DeletionService(exec, nmStore);
		}

		protected internal virtual NodeManager.NMContext CreateNMContext(NMContainerTokenSecretManager
			 containerTokenSecretManager, NMTokenSecretManagerInNM nmTokenSecretManager, NMStateStoreService
			 stateStore)
		{
			return new NodeManager.NMContext(containerTokenSecretManager, nmTokenSecretManager
				, dirsHandler, aclsManager, stateStore);
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void DoSecureLogin()
		{
			SecurityUtil.Login(GetConfig(), YarnConfiguration.NmKeytab, YarnConfiguration.NmPrincipal
				);
		}

		/// <exception cref="System.IO.IOException"/>
		private void InitAndStartRecoveryStore(Configuration conf)
		{
			bool recoveryEnabled = conf.GetBoolean(YarnConfiguration.NmRecoveryEnabled, YarnConfiguration
				.DefaultNmRecoveryEnabled);
			if (recoveryEnabled)
			{
				FileSystem recoveryFs = FileSystem.GetLocal(conf);
				string recoveryDirName = conf.Get(YarnConfiguration.NmRecoveryDir);
				if (recoveryDirName == null)
				{
					throw new ArgumentException("Recovery is enabled but " + YarnConfiguration.NmRecoveryDir
						 + " is not set.");
				}
				Path recoveryRoot = new Path(recoveryDirName);
				recoveryFs.Mkdirs(recoveryRoot, new FsPermission((short)0x1c0));
				nmStore = new NMLeveldbStateStoreService();
			}
			else
			{
				nmStore = new NMNullStateStoreService();
			}
			nmStore.Init(conf);
			nmStore.Start();
		}

		/// <exception cref="System.IO.IOException"/>
		private void StopRecoveryStore()
		{
			if (null != nmStore)
			{
				nmStore.Stop();
				if (null != context)
				{
					if (context.GetDecommissioned() && nmStore.CanRecover())
					{
						Log.Info("Removing state store due to decommission");
						Configuration conf = GetConfig();
						Path recoveryRoot = new Path(conf.Get(YarnConfiguration.NmRecoveryDir));
						Log.Info("Removing state store at " + recoveryRoot + " due to decommission");
						FileSystem recoveryFs = FileSystem.GetLocal(conf);
						if (!recoveryFs.Delete(recoveryRoot, true))
						{
							Log.Warn("Unable to delete " + recoveryRoot);
						}
					}
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void RecoverTokens(NMTokenSecretManagerInNM nmTokenSecretManager, NMContainerTokenSecretManager
			 containerTokenSecretManager)
		{
			if (nmStore.CanRecover())
			{
				nmTokenSecretManager.Recover();
				containerTokenSecretManager.Recover();
			}
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceInit(Configuration conf)
		{
			conf.SetBoolean(Dispatcher.DispatcherExitOnErrorKey, true);
			rmWorkPreservingRestartEnabled = conf.GetBoolean(YarnConfiguration.RmWorkPreservingRecoveryEnabled
				, YarnConfiguration.DefaultRmWorkPreservingRecoveryEnabled);
			InitAndStartRecoveryStore(conf);
			NMContainerTokenSecretManager containerTokenSecretManager = new NMContainerTokenSecretManager
				(conf, nmStore);
			NMTokenSecretManagerInNM nmTokenSecretManager = new NMTokenSecretManagerInNM(nmStore
				);
			RecoverTokens(nmTokenSecretManager, containerTokenSecretManager);
			this.aclsManager = new ApplicationACLsManager(conf);
			ContainerExecutor exec = ReflectionUtils.NewInstance(conf.GetClass<ContainerExecutor
				>(YarnConfiguration.NmContainerExecutor, typeof(DefaultContainerExecutor)), conf
				);
			try
			{
				exec.Init();
			}
			catch (IOException e)
			{
				throw new YarnRuntimeException("Failed to initialize container executor", e);
			}
			DeletionService del = CreateDeletionService(exec);
			AddService(del);
			// NodeManager level dispatcher
			this.dispatcher = new AsyncDispatcher();
			nodeHealthChecker = new NodeHealthCheckerService();
			AddService(nodeHealthChecker);
			dirsHandler = nodeHealthChecker.GetDiskHandler();
			this.context = CreateNMContext(containerTokenSecretManager, nmTokenSecretManager, 
				nmStore);
			nodeStatusUpdater = CreateNodeStatusUpdater(context, dispatcher, nodeHealthChecker
				);
			NodeResourceMonitor nodeResourceMonitor = CreateNodeResourceMonitor();
			AddService(nodeResourceMonitor);
			containerManager = CreateContainerManager(context, exec, del, nodeStatusUpdater, 
				this.aclsManager, dirsHandler);
			AddService(containerManager);
			((NodeManager.NMContext)context).SetContainerManager(containerManager);
			WebServer webServer = CreateWebServer(context, containerManager.GetContainersMonitor
				(), this.aclsManager, dirsHandler);
			AddService(webServer);
			((NodeManager.NMContext)context).SetWebServer(webServer);
			dispatcher.Register(typeof(ContainerManagerEventType), containerManager);
			dispatcher.Register(typeof(NodeManagerEventType), this);
			AddService(dispatcher);
			DefaultMetricsSystem.Initialize("NodeManager");
			// StatusUpdater should be added last so that it get started last 
			// so that we make sure everything is up before registering with RM. 
			AddService(nodeStatusUpdater);
			base.ServiceInit(conf);
		}

		// TODO add local dirs to del
		/// <exception cref="System.Exception"/>
		protected override void ServiceStart()
		{
			try
			{
				DoSecureLogin();
			}
			catch (IOException e)
			{
				throw new YarnRuntimeException("Failed NodeManager login", e);
			}
			base.ServiceStart();
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStop()
		{
			if (isStopping.GetAndSet(true))
			{
				return;
			}
			try
			{
				base.ServiceStop();
				DefaultMetricsSystem.Shutdown();
			}
			finally
			{
				// YARN-3641: NM's services stop get failed shouldn't block the
				// release of NMLevelDBStore.
				StopRecoveryStore();
			}
		}

		public override string GetName()
		{
			return "NodeManager";
		}

		protected internal virtual void ShutDown()
		{
			new _Thread_294(this).Start();
		}

		private sealed class _Thread_294 : Sharpen.Thread
		{
			public _Thread_294(NodeManager _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public override void Run()
			{
				try
				{
					this._enclosing.Stop();
				}
				catch (Exception t)
				{
					Org.Apache.Hadoop.Yarn.Server.Nodemanager.NodeManager.Log.Error("Error while shutting down NodeManager"
						, t);
				}
				finally
				{
					if (this._enclosing.shouldExitOnShutdownEvent && !ShutdownHookManager.Get().IsShutdownInProgress
						())
					{
						ExitUtil.Terminate(-1);
					}
				}
			}

			private readonly NodeManager _enclosing;
		}

		protected internal virtual void ResyncWithRM()
		{
			//we do not want to block dispatcher thread here
			new _Thread_313(this).Start();
		}

		private sealed class _Thread_313 : Sharpen.Thread
		{
			public _Thread_313(NodeManager _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public override void Run()
			{
				try
				{
					Org.Apache.Hadoop.Yarn.Server.Nodemanager.NodeManager.Log.Info("Notifying ContainerManager to block new container-requests"
						);
					this._enclosing.containerManager.SetBlockNewContainerRequests(true);
					if (!this._enclosing.rmWorkPreservingRestartEnabled)
					{
						Org.Apache.Hadoop.Yarn.Server.Nodemanager.NodeManager.Log.Info("Cleaning up running containers on resync"
							);
						this._enclosing.containerManager.CleanupContainersOnNMResync();
					}
					else
					{
						Org.Apache.Hadoop.Yarn.Server.Nodemanager.NodeManager.Log.Info("Preserving containers on resync"
							);
					}
					((NodeStatusUpdaterImpl)this._enclosing.nodeStatusUpdater).RebootNodeStatusUpdaterAndRegisterWithRM
						();
				}
				catch (YarnRuntimeException e)
				{
					Org.Apache.Hadoop.Yarn.Server.Nodemanager.NodeManager.Log.Fatal("Error while rebooting NodeStatusUpdater."
						, e);
					this._enclosing.ShutDown();
				}
			}

			private readonly NodeManager _enclosing;
		}

		public class NMContext : Context
		{
			private NodeId nodeId = null;

			protected internal readonly ConcurrentMap<ApplicationId, Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
				> applications = new ConcurrentHashMap<ApplicationId, Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
				>();

			private volatile IDictionary<ApplicationId, Credentials> systemCredentials = new 
				Dictionary<ApplicationId, Credentials>();

			protected internal readonly ConcurrentMap<ContainerId, Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
				> containers = new ConcurrentSkipListMap<ContainerId, Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
				>();

			private readonly NMContainerTokenSecretManager containerTokenSecretManager;

			private readonly NMTokenSecretManagerInNM nmTokenSecretManager;

			private ContainerManagementProtocol containerManager;

			private readonly LocalDirsHandlerService dirsHandler;

			private readonly ApplicationACLsManager aclsManager;

			private WebServer webServer;

			private readonly NodeHealthStatus nodeHealthStatus = RecordFactoryProvider.GetRecordFactory
				(null).NewRecordInstance<NodeHealthStatus>();

			private readonly NMStateStoreService stateStore;

			private bool isDecommissioned = false;

			public NMContext(NMContainerTokenSecretManager containerTokenSecretManager, NMTokenSecretManagerInNM
				 nmTokenSecretManager, LocalDirsHandlerService dirsHandler, ApplicationACLsManager
				 aclsManager, NMStateStoreService stateStore)
			{
				this.containerTokenSecretManager = containerTokenSecretManager;
				this.nmTokenSecretManager = nmTokenSecretManager;
				this.dirsHandler = dirsHandler;
				this.aclsManager = aclsManager;
				this.nodeHealthStatus.SetIsNodeHealthy(true);
				this.nodeHealthStatus.SetHealthReport("Healthy");
				this.nodeHealthStatus.SetLastHealthReportTime(Runtime.CurrentTimeMillis());
				this.stateStore = stateStore;
			}

			/// <summary>Usable only after ContainerManager is started.</summary>
			public virtual NodeId GetNodeId()
			{
				return this.nodeId;
			}

			public virtual int GetHttpPort()
			{
				return this.webServer.GetPort();
			}

			public virtual ConcurrentMap<ApplicationId, Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
				> GetApplications()
			{
				return this.applications;
			}

			public virtual ConcurrentMap<ContainerId, Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
				> GetContainers()
			{
				return this.containers;
			}

			public virtual NMContainerTokenSecretManager GetContainerTokenSecretManager()
			{
				return this.containerTokenSecretManager;
			}

			public virtual NMTokenSecretManagerInNM GetNMTokenSecretManager()
			{
				return this.nmTokenSecretManager;
			}

			public virtual NodeHealthStatus GetNodeHealthStatus()
			{
				return this.nodeHealthStatus;
			}

			public virtual ContainerManagementProtocol GetContainerManager()
			{
				return this.containerManager;
			}

			public virtual void SetContainerManager(ContainerManagementProtocol containerManager
				)
			{
				this.containerManager = containerManager;
			}

			public virtual void SetWebServer(WebServer webServer)
			{
				this.webServer = webServer;
			}

			public virtual void SetNodeId(NodeId nodeId)
			{
				this.nodeId = nodeId;
			}

			public virtual LocalDirsHandlerService GetLocalDirsHandler()
			{
				return dirsHandler;
			}

			public virtual ApplicationACLsManager GetApplicationACLsManager()
			{
				return aclsManager;
			}

			public virtual NMStateStoreService GetNMStateStore()
			{
				return stateStore;
			}

			public virtual bool GetDecommissioned()
			{
				return isDecommissioned;
			}

			public virtual void SetDecommissioned(bool isDecommissioned)
			{
				this.isDecommissioned = isDecommissioned;
			}

			public virtual IDictionary<ApplicationId, Credentials> GetSystemCredentialsForApps
				()
			{
				return systemCredentials;
			}

			public virtual void SetSystemCrendentialsForApps(IDictionary<ApplicationId, Credentials
				> systemCredentials)
			{
				this.systemCredentials = systemCredentials;
			}
		}

		/// <returns>the node health checker</returns>
		public virtual NodeHealthCheckerService GetNodeHealthChecker()
		{
			return nodeHealthChecker;
		}

		private void InitAndStartNodeManager(Configuration conf, bool hasToReboot)
		{
			try
			{
				// Remove the old hook if we are rebooting.
				if (hasToReboot && null != nodeManagerShutdownHook)
				{
					ShutdownHookManager.Get().RemoveShutdownHook(nodeManagerShutdownHook);
				}
				nodeManagerShutdownHook = new CompositeService.CompositeServiceShutdownHook(this);
				ShutdownHookManager.Get().AddShutdownHook(nodeManagerShutdownHook, ShutdownHookPriority
					);
				// System exit should be called only when NodeManager is instantiated from
				// main() funtion
				this.shouldExitOnShutdownEvent = true;
				this.Init(conf);
				this.Start();
			}
			catch (Exception t)
			{
				Log.Fatal("Error starting NodeManager", t);
				System.Environment.Exit(-1);
			}
		}

		public virtual void Handle(NodeManagerEvent @event)
		{
			switch (@event.GetType())
			{
				case NodeManagerEventType.Shutdown:
				{
					ShutDown();
					break;
				}

				case NodeManagerEventType.Resync:
				{
					ResyncWithRM();
					break;
				}

				default:
				{
					Log.Warn("Invalid shutdown event " + @event.GetType() + ". Ignoring.");
					break;
				}
			}
		}

		// For testing
		internal virtual NodeManager CreateNewNodeManager()
		{
			return new NodeManager();
		}

		// For testing
		internal virtual ContainerManagerImpl GetContainerManager()
		{
			return containerManager;
		}

		//For testing
		internal virtual Dispatcher GetNMDispatcher()
		{
			return dispatcher;
		}

		[VisibleForTesting]
		public virtual Context GetNMContext()
		{
			return this.context;
		}

		/// <exception cref="System.IO.IOException"/>
		public static void Main(string[] args)
		{
			Sharpen.Thread.SetDefaultUncaughtExceptionHandler(new YarnUncaughtExceptionHandler
				());
			StringUtils.StartupShutdownMessage(typeof(NodeManager), args, Log);
			NodeManager nodeManager = new NodeManager();
			Configuration conf = new YarnConfiguration();
			new GenericOptionsParser(conf, args);
			nodeManager.InitAndStartNodeManager(conf, false);
		}

		[VisibleForTesting]
		[InterfaceAudience.Private]
		public virtual NodeStatusUpdater GetNodeStatusUpdater()
		{
			return nodeStatusUpdater;
		}
	}
}
