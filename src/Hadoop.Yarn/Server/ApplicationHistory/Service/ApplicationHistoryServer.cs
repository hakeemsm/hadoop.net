using System;
using System.IO;
using System.Net;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Lang;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Http;
using Org.Apache.Hadoop.Metrics2.Lib;
using Org.Apache.Hadoop.Metrics2.Source;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice.Webapp;
using Org.Apache.Hadoop.Yarn.Server.Security;
using Org.Apache.Hadoop.Yarn.Server.Timeline;
using Org.Apache.Hadoop.Yarn.Server.Timeline.Security;
using Org.Apache.Hadoop.Yarn.Server.Timeline.Webapp;
using Org.Apache.Hadoop.Yarn.Webapp;
using Org.Apache.Hadoop.Yarn.Webapp.Util;
using Org.Mortbay.Jetty.Servlet;
using Org.Mortbay.Jetty.Webapp;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice
{
	/// <summary>History server that keeps track of all types of history in the cluster.</summary>
	/// <remarks>
	/// History server that keeps track of all types of history in the cluster.
	/// Application specific history to start with.
	/// </remarks>
	public class ApplicationHistoryServer : CompositeService
	{
		public const int ShutdownHookPriority = 30;

		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice.ApplicationHistoryServer
			));

		private ApplicationHistoryClientService ahsClientService;

		private ApplicationACLsManager aclsManager;

		private ApplicationHistoryManager historyManager;

		private TimelineStore timelineStore;

		private TimelineDelegationTokenSecretManagerService secretManagerService;

		private TimelineDataManager timelineDataManager;

		private WebApp webApp;

		public ApplicationHistoryServer()
			: base(typeof(Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice.ApplicationHistoryServer
				).FullName)
		{
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceInit(Configuration conf)
		{
			// init timeline services first
			timelineStore = CreateTimelineStore(conf);
			AddIfService(timelineStore);
			secretManagerService = CreateTimelineDelegationTokenSecretManagerService(conf);
			AddService(secretManagerService);
			timelineDataManager = CreateTimelineDataManager(conf);
			AddService(timelineDataManager);
			// init generic history service afterwards
			aclsManager = CreateApplicationACLsManager(conf);
			historyManager = CreateApplicationHistoryManager(conf);
			ahsClientService = CreateApplicationHistoryClientService(historyManager);
			AddService(ahsClientService);
			AddService((Org.Apache.Hadoop.Service.Service)historyManager);
			DefaultMetricsSystem.Initialize("ApplicationHistoryServer");
			JvmMetrics.InitSingleton("ApplicationHistoryServer", null);
			base.ServiceInit(conf);
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStart()
		{
			try
			{
				DoSecureLogin(GetConfig());
			}
			catch (IOException ie)
			{
				throw new YarnRuntimeException("Failed to login", ie);
			}
			base.ServiceStart();
			StartWebApp();
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStop()
		{
			if (webApp != null)
			{
				webApp.Stop();
			}
			DefaultMetricsSystem.Shutdown();
			base.ServiceStop();
		}

		[InterfaceAudience.Private]
		[VisibleForTesting]
		internal virtual ApplicationHistoryClientService GetClientService()
		{
			return this.ahsClientService;
		}

		private IPEndPoint GetListenerAddress()
		{
			return this.webApp.HttpServer().GetConnectorAddress(0);
		}

		[InterfaceAudience.Private]
		[VisibleForTesting]
		public virtual int GetPort()
		{
			return this.GetListenerAddress().Port;
		}

		/// <returns>ApplicationTimelineStore</returns>
		[InterfaceAudience.Private]
		[VisibleForTesting]
		public virtual TimelineStore GetTimelineStore()
		{
			return timelineStore;
		}

		[InterfaceAudience.Private]
		[VisibleForTesting]
		internal virtual ApplicationHistoryManager GetApplicationHistoryManager()
		{
			return this.historyManager;
		}

		internal static Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice.ApplicationHistoryServer
			 LaunchAppHistoryServer(string[] args)
		{
			Sharpen.Thread.SetDefaultUncaughtExceptionHandler(new YarnUncaughtExceptionHandler
				());
			StringUtils.StartupShutdownMessage(typeof(Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice.ApplicationHistoryServer
				), args, Log);
			Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice.ApplicationHistoryServer 
				appHistoryServer = null;
			try
			{
				appHistoryServer = new Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice.ApplicationHistoryServer
					();
				ShutdownHookManager.Get().AddShutdownHook(new CompositeService.CompositeServiceShutdownHook
					(appHistoryServer), ShutdownHookPriority);
				YarnConfiguration conf = new YarnConfiguration();
				new GenericOptionsParser(conf, args);
				appHistoryServer.Init(conf);
				appHistoryServer.Start();
			}
			catch (Exception t)
			{
				Log.Fatal("Error starting ApplicationHistoryServer", t);
				ExitUtil.Terminate(-1, "Error starting ApplicationHistoryServer");
			}
			return appHistoryServer;
		}

		public static void Main(string[] args)
		{
			LaunchAppHistoryServer(args);
		}

		private ApplicationHistoryClientService CreateApplicationHistoryClientService(ApplicationHistoryManager
			 historyManager)
		{
			return new ApplicationHistoryClientService(historyManager);
		}

		private ApplicationACLsManager CreateApplicationACLsManager(Configuration conf)
		{
			return new ApplicationACLsManager(conf);
		}

		private ApplicationHistoryManager CreateApplicationHistoryManager(Configuration conf
			)
		{
			// Backward compatibility:
			// APPLICATION_HISTORY_STORE is neither null nor empty, it means that the
			// user has enabled it explicitly.
			if (conf.Get(YarnConfiguration.ApplicationHistoryStore) == null || conf.Get(YarnConfiguration
				.ApplicationHistoryStore).Length == 0 || conf.Get(YarnConfiguration.ApplicationHistoryStore
				).Equals(typeof(NullApplicationHistoryStore).FullName))
			{
				return new ApplicationHistoryManagerOnTimelineStore(timelineDataManager, aclsManager
					);
			}
			else
			{
				Log.Warn("The filesystem based application history store is deprecated.");
				return new ApplicationHistoryManagerImpl();
			}
		}

		private TimelineStore CreateTimelineStore(Configuration conf)
		{
			return ReflectionUtils.NewInstance(conf.GetClass<TimelineStore>(YarnConfiguration
				.TimelineServiceStore, typeof(LeveldbTimelineStore)), conf);
		}

		private TimelineDelegationTokenSecretManagerService CreateTimelineDelegationTokenSecretManagerService
			(Configuration conf)
		{
			return new TimelineDelegationTokenSecretManagerService();
		}

		private TimelineDataManager CreateTimelineDataManager(Configuration conf)
		{
			return new TimelineDataManager(timelineStore, new TimelineACLsManager(conf));
		}

		private void StartWebApp()
		{
			Configuration conf = GetConfig();
			TimelineAuthenticationFilter.SetTimelineDelegationTokenSecretManager(secretManagerService
				.GetTimelineDelegationTokenSecretManager());
			// Always load pseudo authentication filter to parse "user.name" in an URL
			// to identify a HTTP request's user in insecure mode.
			// When Kerberos authentication type is set (i.e., secure mode is turned on),
			// the customized filter will be loaded by the timeline server to do Kerberos
			// + DT authentication.
			string initializers = conf.Get("hadoop.http.filter.initializers");
			bool modifiedInitializers = false;
			initializers = initializers == null || initializers.Length == 0 ? string.Empty : 
				initializers;
			if (!initializers.Contains(typeof(CrossOriginFilterInitializer).FullName))
			{
				if (conf.GetBoolean(YarnConfiguration.TimelineServiceHttpCrossOriginEnabled, YarnConfiguration
					.TimelineServiceHttpCrossOriginEnabledDefault))
				{
					if (initializers.Contains(typeof(HttpCrossOriginFilterInitializer).FullName))
					{
						initializers = initializers.ReplaceAll(typeof(HttpCrossOriginFilterInitializer).FullName
							, typeof(CrossOriginFilterInitializer).FullName);
					}
					else
					{
						if (initializers.Length != 0)
						{
							initializers += ",";
						}
						initializers += typeof(CrossOriginFilterInitializer).FullName;
					}
					modifiedInitializers = true;
				}
			}
			if (!initializers.Contains(typeof(TimelineAuthenticationFilterInitializer).FullName
				))
			{
				if (initializers.Length != 0)
				{
					initializers += ",";
				}
				initializers += typeof(TimelineAuthenticationFilterInitializer).FullName;
				modifiedInitializers = true;
			}
			string[] parts = initializers.Split(",");
			AList<string> target = new AList<string>();
			foreach (string filterInitializer in parts)
			{
				filterInitializer = filterInitializer.Trim();
				if (filterInitializer.Equals(typeof(AuthenticationFilterInitializer).FullName))
				{
					modifiedInitializers = true;
					continue;
				}
				target.AddItem(filterInitializer);
			}
			string actualInitializers = StringUtils.Join(target, ",");
			if (modifiedInitializers)
			{
				conf.Set("hadoop.http.filter.initializers", actualInitializers);
			}
			string bindAddress = WebAppUtils.GetWebAppBindURL(conf, YarnConfiguration.TimelineServiceBindHost
				, WebAppUtils.GetAHSWebAppURLWithoutScheme(conf));
			try
			{
				AHSWebApp ahsWebApp = new AHSWebApp(timelineDataManager, ahsClientService);
				webApp = WebApps.$for<ApplicationHistoryClientService>("applicationhistory", ahsClientService
					, "ws").With(conf).WithAttribute(YarnConfiguration.TimelineServiceWebappAddress, 
					conf.Get(YarnConfiguration.TimelineServiceWebappAddress)).At(bindAddress).Build(
					ahsWebApp);
				HttpServer2 httpServer = webApp.HttpServer();
				string[] names = conf.GetTrimmedStrings(YarnConfiguration.TimelineServiceUiNames);
				WebAppContext webAppContext = httpServer.GetWebAppContext();
				foreach (string name in names)
				{
					string webPath = conf.Get(YarnConfiguration.TimelineServiceUiWebPathPrefix + name
						);
					string onDiskPath = conf.Get(YarnConfiguration.TimelineServiceUiOnDiskPathPrefix 
						+ name);
					WebAppContext uiWebAppContext = new WebAppContext();
					uiWebAppContext.SetContextPath(webPath);
					uiWebAppContext.SetWar(onDiskPath);
					string[] AllUrls = new string[] { "/*" };
					FilterHolder[] filterHolders = webAppContext.GetServletHandler().GetFilters();
					foreach (FilterHolder filterHolder in filterHolders)
					{
						if (!"guice".Equals(filterHolder.GetName()))
						{
							HttpServer2.DefineFilter(uiWebAppContext, filterHolder.GetName(), filterHolder.GetClassName
								(), filterHolder.GetInitParameters(), AllUrls);
						}
					}
					Log.Info("Hosting " + name + " from " + onDiskPath + " at " + webPath);
					httpServer.AddContext(uiWebAppContext, true);
				}
				httpServer.Start();
				conf.UpdateConnectAddr(YarnConfiguration.TimelineServiceBindHost, YarnConfiguration
					.TimelineServiceWebappAddress, YarnConfiguration.DefaultTimelineServiceWebappAddress
					, this.GetListenerAddress());
				Log.Info("Instantiating AHSWebApp at " + GetPort());
			}
			catch (Exception e)
			{
				string msg = "AHSWebApp failed to start.";
				Log.Error(msg, e);
				throw new YarnRuntimeException(msg, e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void DoSecureLogin(Configuration conf)
		{
			IPEndPoint socAddr = GetBindAddress(conf);
			SecurityUtil.Login(conf, YarnConfiguration.TimelineServiceKeytab, YarnConfiguration
				.TimelineServicePrincipal, socAddr.GetHostName());
		}

		/// <summary>Retrieve the timeline server bind address from configuration</summary>
		/// <param name="conf"/>
		/// <returns>InetSocketAddress</returns>
		private static IPEndPoint GetBindAddress(Configuration conf)
		{
			return conf.GetSocketAddr(YarnConfiguration.TimelineServiceAddress, YarnConfiguration
				.DefaultTimelineServiceAddress, YarnConfiguration.DefaultTimelineServicePort);
		}
	}
}
