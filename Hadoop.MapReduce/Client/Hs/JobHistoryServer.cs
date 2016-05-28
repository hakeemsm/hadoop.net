using System;
using System.IO;
using System.Net;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.V2.HS.Server;
using Org.Apache.Hadoop.Mapreduce.V2.Jobhistory;
using Org.Apache.Hadoop.Mapreduce.V2.Util;
using Org.Apache.Hadoop.Metrics2.Lib;
using Org.Apache.Hadoop.Metrics2.Source;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Logaggregation;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.HS
{
	/// <summary>
	/// <see cref="JobHistoryServer"/>
	/// is responsible for servicing all job history
	/// related requests from client.
	/// </summary>
	public class JobHistoryServer : CompositeService
	{
		/// <summary>Priority of the JobHistoryServer shutdown hook.</summary>
		public const int ShutdownHookPriority = 30;

		public static readonly long historyServerTimeStamp = Runtime.CurrentTimeMillis();

		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapreduce.V2.HS.JobHistoryServer
			));

		protected internal HistoryContext historyContext;

		private HistoryClientService clientService;

		private JobHistory jobHistoryService;

		protected internal JHSDelegationTokenSecretManager jhsDTSecretManager;

		private AggregatedLogDeletionService aggLogDelService;

		private HSAdminServer hsAdminServer;

		private HistoryServerStateStoreService stateStore;

		private class HistoryServerSecretManagerService : AbstractService
		{
			public HistoryServerSecretManagerService(JobHistoryServer _enclosing)
				: base(typeof(JobHistoryServer.HistoryServerSecretManagerService).FullName)
			{
				this._enclosing = _enclosing;
			}

			// utility class to start and stop secret manager as part of service
			// framework and implement state recovery for secret manager on startup
			/// <exception cref="System.Exception"/>
			protected override void ServiceStart()
			{
				bool recoveryEnabled = this.GetConfig().GetBoolean(JHAdminConfig.MrHsRecoveryEnable
					, JHAdminConfig.DefaultMrHsRecoveryEnable);
				if (recoveryEnabled)
				{
					System.Diagnostics.Debug.Assert(this._enclosing.stateStore.IsInState(Service.STATE
						.Started));
					HistoryServerStateStoreService.HistoryServerState state = this._enclosing.stateStore
						.LoadState();
					this._enclosing.jhsDTSecretManager.Recover(state);
				}
				try
				{
					this._enclosing.jhsDTSecretManager.StartThreads();
				}
				catch (IOException io)
				{
					JobHistoryServer.Log.Error("Error while starting the Secret Manager threads", io);
					throw;
				}
				base.ServiceStart();
			}

			/// <exception cref="System.Exception"/>
			protected override void ServiceStop()
			{
				if (this._enclosing.jhsDTSecretManager != null)
				{
					this._enclosing.jhsDTSecretManager.StopThreads();
				}
				base.ServiceStop();
			}

			private readonly JobHistoryServer _enclosing;
		}

		public JobHistoryServer()
			: base(typeof(JobHistoryServer).FullName)
		{
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceInit(Configuration conf)
		{
			Configuration config = new YarnConfiguration(conf);
			config.SetBoolean(Dispatcher.DispatcherExitOnErrorKey, true);
			// This is required for WebApps to use https if enabled.
			MRWebAppUtil.Initialize(GetConfig());
			try
			{
				DoSecureLogin(conf);
			}
			catch (IOException ie)
			{
				throw new YarnRuntimeException("History Server Failed to login", ie);
			}
			jobHistoryService = new JobHistory();
			historyContext = (HistoryContext)jobHistoryService;
			stateStore = CreateStateStore(conf);
			this.jhsDTSecretManager = CreateJHSSecretManager(conf, stateStore);
			clientService = CreateHistoryClientService();
			aggLogDelService = new AggregatedLogDeletionService();
			hsAdminServer = new HSAdminServer(aggLogDelService, jobHistoryService);
			AddService(stateStore);
			AddService(new JobHistoryServer.HistoryServerSecretManagerService(this));
			AddService(jobHistoryService);
			AddService(clientService);
			AddService(aggLogDelService);
			AddService(hsAdminServer);
			base.ServiceInit(config);
		}

		[VisibleForTesting]
		protected internal virtual HistoryClientService CreateHistoryClientService()
		{
			return new HistoryClientService(historyContext, this.jhsDTSecretManager);
		}

		protected internal virtual JHSDelegationTokenSecretManager CreateJHSSecretManager
			(Configuration conf, HistoryServerStateStoreService store)
		{
			long secretKeyInterval = conf.GetLong(MRConfig.DelegationKeyUpdateIntervalKey, MRConfig
				.DelegationKeyUpdateIntervalDefault);
			long tokenMaxLifetime = conf.GetLong(MRConfig.DelegationTokenMaxLifetimeKey, MRConfig
				.DelegationTokenMaxLifetimeDefault);
			long tokenRenewInterval = conf.GetLong(MRConfig.DelegationTokenRenewIntervalKey, 
				MRConfig.DelegationTokenRenewIntervalDefault);
			return new JHSDelegationTokenSecretManager(secretKeyInterval, tokenMaxLifetime, tokenRenewInterval
				, 3600000, store);
		}

		protected internal virtual HistoryServerStateStoreService CreateStateStore(Configuration
			 conf)
		{
			return HistoryServerStateStoreServiceFactory.GetStore(conf);
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void DoSecureLogin(Configuration conf)
		{
			IPEndPoint socAddr = GetBindAddress(conf);
			SecurityUtil.Login(conf, JHAdminConfig.MrHistoryKeytab, JHAdminConfig.MrHistoryPrincipal
				, socAddr.GetHostName());
		}

		/// <summary>Retrieve JHS bind address from configuration</summary>
		/// <param name="conf"/>
		/// <returns>InetSocketAddress</returns>
		public static IPEndPoint GetBindAddress(Configuration conf)
		{
			return conf.GetSocketAddr(JHAdminConfig.MrHistoryAddress, JHAdminConfig.DefaultMrHistoryAddress
				, JHAdminConfig.DefaultMrHistoryPort);
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStart()
		{
			DefaultMetricsSystem.Initialize("JobHistoryServer");
			JvmMetrics.InitSingleton("JobHistoryServer", null);
			base.ServiceStart();
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStop()
		{
			DefaultMetricsSystem.Shutdown();
			base.ServiceStop();
		}

		[InterfaceAudience.Private]
		public virtual HistoryClientService GetClientService()
		{
			return this.clientService;
		}

		internal static JobHistoryServer LaunchJobHistoryServer(string[] args)
		{
			Sharpen.Thread.SetDefaultUncaughtExceptionHandler(new YarnUncaughtExceptionHandler
				());
			StringUtils.StartupShutdownMessage(typeof(JobHistoryServer), args, Log);
			JobHistoryServer jobHistoryServer = null;
			try
			{
				jobHistoryServer = new JobHistoryServer();
				ShutdownHookManager.Get().AddShutdownHook(new CompositeService.CompositeServiceShutdownHook
					(jobHistoryServer), ShutdownHookPriority);
				YarnConfiguration conf = new YarnConfiguration(new JobConf());
				new GenericOptionsParser(conf, args);
				jobHistoryServer.Init(conf);
				jobHistoryServer.Start();
			}
			catch (Exception t)
			{
				Log.Fatal("Error starting JobHistoryServer", t);
				ExitUtil.Terminate(-1, "Error starting JobHistoryServer");
			}
			return jobHistoryServer;
		}

		public static void Main(string[] args)
		{
			LaunchJobHistoryServer(args);
		}
	}
}
