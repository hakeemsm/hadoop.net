using System;
using Com.Google.Common.Util.Concurrent;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Amlauncher
{
	public class ApplicationMasterLauncher : AbstractService, EventHandler<AMLauncherEvent
		>
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Amlauncher.ApplicationMasterLauncher
			));

		private ThreadPoolExecutor launcherPool;

		private ApplicationMasterLauncher.LauncherThread launcherHandlingThread;

		private readonly BlockingQueue<Runnable> masterEvents = new LinkedBlockingQueue<Runnable
			>();

		protected internal readonly RMContext context;

		public ApplicationMasterLauncher(RMContext context)
			: base(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Amlauncher.ApplicationMasterLauncher
				).FullName)
		{
			this.context = context;
			this.launcherHandlingThread = new ApplicationMasterLauncher.LauncherThread(this);
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceInit(Configuration conf)
		{
			int threadCount = conf.GetInt(YarnConfiguration.RmAmlauncherThreadCount, YarnConfiguration
				.DefaultRmAmlauncherThreadCount);
			ThreadFactory tf = new ThreadFactoryBuilder().SetNameFormat("ApplicationMasterLauncher #%d"
				).Build();
			launcherPool = new ThreadPoolExecutor(threadCount, threadCount, 1, TimeUnit.Hours
				, new LinkedBlockingQueue<Runnable>());
			launcherPool.SetThreadFactory(tf);
			Configuration newConf = new YarnConfiguration(conf);
			newConf.SetInt(CommonConfigurationKeysPublic.IpcClientConnectMaxRetriesOnSocketTimeoutsKey
				, conf.GetInt(YarnConfiguration.RmNodemanagerConnectRetires, YarnConfiguration.DefaultRmNodemanagerConnectRetires
				));
			SetConfig(newConf);
			base.ServiceInit(newConf);
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStart()
		{
			launcherHandlingThread.Start();
			base.ServiceStart();
		}

		protected internal virtual Runnable CreateRunnableLauncher(RMAppAttempt application
			, AMLauncherEventType @event)
		{
			Runnable launcher = new AMLauncher(context, application, @event, GetConfig());
			return launcher;
		}

		private void Launch(RMAppAttempt application)
		{
			Runnable launcher = CreateRunnableLauncher(application, AMLauncherEventType.Launch
				);
			masterEvents.AddItem(launcher);
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStop()
		{
			launcherHandlingThread.Interrupt();
			try
			{
				launcherHandlingThread.Join();
			}
			catch (Exception ie)
			{
				Log.Info(launcherHandlingThread.GetName() + " interrupted during join ", ie);
			}
			launcherPool.Shutdown();
		}

		private class LauncherThread : Sharpen.Thread
		{
			public LauncherThread(ApplicationMasterLauncher _enclosing)
				: base("ApplicationMaster Launcher")
			{
				this._enclosing = _enclosing;
			}

			public override void Run()
			{
				while (!this.IsInterrupted())
				{
					Runnable toLaunch;
					try
					{
						toLaunch = this._enclosing.masterEvents.Take();
						this._enclosing.launcherPool.Execute(toLaunch);
					}
					catch (Exception)
					{
						ApplicationMasterLauncher.Log.Warn(this.GetType().FullName + " interrupted. Returning."
							);
						return;
					}
				}
			}

			private readonly ApplicationMasterLauncher _enclosing;
		}

		private void Cleanup(RMAppAttempt application)
		{
			Runnable launcher = CreateRunnableLauncher(application, AMLauncherEventType.Cleanup
				);
			masterEvents.AddItem(launcher);
		}

		public virtual void Handle(AMLauncherEvent appEvent)
		{
			lock (this)
			{
				AMLauncherEventType @event = appEvent.GetType();
				RMAppAttempt application = appEvent.GetAppAttempt();
				switch (@event)
				{
					case AMLauncherEventType.Launch:
					{
						Launch(application);
						break;
					}

					case AMLauncherEventType.Cleanup:
					{
						Cleanup(application);
						break;
					}

					default:
					{
						break;
					}
				}
			}
		}
	}
}
