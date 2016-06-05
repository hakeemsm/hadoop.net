using System;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Metrics2.Lib;
using Org.Apache.Hadoop.Metrics2.Source;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Server.Sharedcachemanager.Store;
using Org.Apache.Hadoop.Yarn.Server.Sharedcachemanager.Webapp;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Sharedcachemanager
{
	/// <summary>This service maintains the shared cache meta data.</summary>
	/// <remarks>
	/// This service maintains the shared cache meta data. It handles claiming and
	/// releasing of resources, all rpc calls from the client to the shared cache
	/// manager, and administrative commands. It also persists the shared cache meta
	/// data to a backend store, and cleans up stale entries on a regular basis.
	/// </remarks>
	public class SharedCacheManager : CompositeService
	{
		/// <summary>Priority of the SharedCacheManager shutdown hook.</summary>
		public const int ShutdownHookPriority = 30;

		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Sharedcachemanager.SharedCacheManager
			));

		private SCMStore store;

		public SharedCacheManager()
			: base("SharedCacheManager")
		{
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceInit(Configuration conf)
		{
			this.store = CreateSCMStoreService(conf);
			AddService(store);
			CleanerService cs = CreateCleanerService(store);
			AddService(cs);
			SharedCacheUploaderService nms = CreateNMCacheUploaderSCMProtocolService(store);
			AddService(nms);
			ClientProtocolService cps = CreateClientProtocolService(store);
			AddService(cps);
			SCMAdminProtocolService saps = CreateSCMAdminProtocolService(cs);
			AddService(saps);
			SCMWebServer webUI = CreateSCMWebServer(this);
			AddService(webUI);
			// init metrics
			DefaultMetricsSystem.Initialize("SharedCacheManager");
			JvmMetrics.InitSingleton("SharedCacheManager", null);
			base.ServiceInit(conf);
		}

		private static SCMStore CreateSCMStoreService(Configuration conf)
		{
			Type defaultStoreClass;
			try
			{
				defaultStoreClass = (Type)Sharpen.Runtime.GetType(YarnConfiguration.DefaultScmStoreClass
					);
			}
			catch (Exception e)
			{
				throw new YarnRuntimeException("Invalid default scm store class" + YarnConfiguration
					.DefaultScmStoreClass, e);
			}
			SCMStore store = ReflectionUtils.NewInstance(conf.GetClass<SCMStore>(YarnConfiguration
				.ScmStoreClass, defaultStoreClass), conf);
			return store;
		}

		private CleanerService CreateCleanerService(SCMStore store)
		{
			return new CleanerService(store);
		}

		private SharedCacheUploaderService CreateNMCacheUploaderSCMProtocolService(SCMStore
			 store)
		{
			return new SharedCacheUploaderService(store);
		}

		private ClientProtocolService CreateClientProtocolService(SCMStore store)
		{
			return new ClientProtocolService(store);
		}

		private SCMAdminProtocolService CreateSCMAdminProtocolService(CleanerService cleanerService
			)
		{
			return new SCMAdminProtocolService(cleanerService);
		}

		private SCMWebServer CreateSCMWebServer(Org.Apache.Hadoop.Yarn.Server.Sharedcachemanager.SharedCacheManager
			 scm)
		{
			return new SCMWebServer(scm);
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStop()
		{
			DefaultMetricsSystem.Shutdown();
			base.ServiceStop();
		}

		/// <summary>For testing purposes only.</summary>
		[VisibleForTesting]
		internal virtual SCMStore GetSCMStore()
		{
			return this.store;
		}

		public static void Main(string[] args)
		{
			Sharpen.Thread.SetDefaultUncaughtExceptionHandler(new YarnUncaughtExceptionHandler
				());
			StringUtils.StartupShutdownMessage(typeof(Org.Apache.Hadoop.Yarn.Server.Sharedcachemanager.SharedCacheManager
				), args, Log);
			try
			{
				Configuration conf = new YarnConfiguration();
				Org.Apache.Hadoop.Yarn.Server.Sharedcachemanager.SharedCacheManager sharedCacheManager
					 = new Org.Apache.Hadoop.Yarn.Server.Sharedcachemanager.SharedCacheManager();
				ShutdownHookManager.Get().AddShutdownHook(new CompositeService.CompositeServiceShutdownHook
					(sharedCacheManager), ShutdownHookPriority);
				sharedCacheManager.Init(conf);
				sharedCacheManager.Start();
			}
			catch (Exception t)
			{
				Log.Fatal("Error starting SharedCacheManager", t);
				System.Environment.Exit(-1);
			}
		}
	}
}
