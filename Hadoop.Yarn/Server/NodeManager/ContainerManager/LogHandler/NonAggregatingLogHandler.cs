using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Util.Concurrent;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Proto;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Loghandler.Event;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Recovery;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Loghandler
{
	/// <summary>
	/// Log Handler which schedules deletion of log files based on the configured log
	/// retention time.
	/// </summary>
	public class NonAggregatingLogHandler : AbstractService, LogHandler
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Loghandler.NonAggregatingLogHandler
			));

		private readonly Dispatcher dispatcher;

		private readonly DeletionService delService;

		private readonly IDictionary<ApplicationId, string> appOwners;

		private readonly LocalDirsHandlerService dirsHandler;

		private readonly NMStateStoreService stateStore;

		private long deleteDelaySeconds;

		private ScheduledThreadPoolExecutor sched;

		public NonAggregatingLogHandler(Dispatcher dispatcher, DeletionService delService
			, LocalDirsHandlerService dirsHandler, NMStateStoreService stateStore)
			: base(typeof(Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Loghandler.NonAggregatingLogHandler
				).FullName)
		{
			this.dispatcher = dispatcher;
			this.delService = delService;
			this.dirsHandler = dirsHandler;
			this.stateStore = stateStore;
			this.appOwners = new ConcurrentHashMap<ApplicationId, string>();
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceInit(Configuration conf)
		{
			// Default 3 hours.
			this.deleteDelaySeconds = conf.GetLong(YarnConfiguration.NmLogRetainSeconds, YarnConfiguration
				.DefaultNmLogRetainSeconds);
			sched = CreateScheduledThreadPoolExecutor(conf);
			base.ServiceInit(conf);
			Recover();
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStop()
		{
			if (sched != null)
			{
				sched.Shutdown();
				bool isShutdown = false;
				try
				{
					isShutdown = sched.AwaitTermination(10, TimeUnit.Seconds);
				}
				catch (Exception)
				{
					sched.ShutdownNow();
					isShutdown = true;
				}
				if (!isShutdown)
				{
					sched.ShutdownNow();
				}
			}
			base.ServiceStop();
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

		/// <exception cref="System.IO.IOException"/>
		private void Recover()
		{
			if (stateStore.CanRecover())
			{
				NMStateStoreService.RecoveredLogDeleterState state = stateStore.LoadLogDeleterState
					();
				long now = Runtime.CurrentTimeMillis();
				foreach (KeyValuePair<ApplicationId, YarnServerNodemanagerRecoveryProtos.LogDeleterProto
					> entry in state.GetLogDeleterMap())
				{
					ApplicationId appId = entry.Key;
					YarnServerNodemanagerRecoveryProtos.LogDeleterProto proto = entry.Value;
					long deleteDelayMsec = proto.GetDeletionTime() - now;
					if (Log.IsDebugEnabled())
					{
						Log.Debug("Scheduling deletion of " + appId + " logs in " + deleteDelayMsec + " msec"
							);
					}
					NonAggregatingLogHandler.LogDeleterRunnable logDeleter = new NonAggregatingLogHandler.LogDeleterRunnable
						(this, proto.GetUser(), appId);
					try
					{
						sched.Schedule(logDeleter, deleteDelayMsec, TimeUnit.Milliseconds);
					}
					catch (RejectedExecutionException)
					{
						// Handling this event in local thread before starting threads
						// or after calling sched.shutdownNow().
						logDeleter.Run();
					}
				}
			}
		}

		public virtual void Handle(LogHandlerEvent @event)
		{
			switch (@event.GetType())
			{
				case LogHandlerEventType.ApplicationStarted:
				{
					LogHandlerAppStartedEvent appStartedEvent = (LogHandlerAppStartedEvent)@event;
					this.appOwners[appStartedEvent.GetApplicationId()] = appStartedEvent.GetUser();
					this.dispatcher.GetEventHandler().Handle(new ApplicationEvent(appStartedEvent.GetApplicationId
						(), ApplicationEventType.ApplicationLogHandlingInited));
					break;
				}

				case LogHandlerEventType.ContainerFinished:
				{
					// Ignore
					break;
				}

				case LogHandlerEventType.ApplicationFinished:
				{
					LogHandlerAppFinishedEvent appFinishedEvent = (LogHandlerAppFinishedEvent)@event;
					ApplicationId appId = appFinishedEvent.GetApplicationId();
					// Schedule - so that logs are available on the UI till they're deleted.
					Log.Info("Scheduling Log Deletion for application: " + appId + ", with delay of "
						 + this.deleteDelaySeconds + " seconds");
					string user = Sharpen.Collections.Remove(appOwners, appId);
					if (user == null)
					{
						Log.Error("Unable to locate user for " + appId);
						break;
					}
					NonAggregatingLogHandler.LogDeleterRunnable logDeleter = new NonAggregatingLogHandler.LogDeleterRunnable
						(this, user, appId);
					long deletionTimestamp = Runtime.CurrentTimeMillis() + this.deleteDelaySeconds * 
						1000;
					YarnServerNodemanagerRecoveryProtos.LogDeleterProto deleterProto = ((YarnServerNodemanagerRecoveryProtos.LogDeleterProto
						)YarnServerNodemanagerRecoveryProtos.LogDeleterProto.NewBuilder().SetUser(user).
						SetDeletionTime(deletionTimestamp).Build());
					try
					{
						stateStore.StoreLogDeleter(appId, deleterProto);
					}
					catch (IOException e)
					{
						Log.Error("Unable to record log deleter state", e);
					}
					try
					{
						sched.Schedule(logDeleter, this.deleteDelaySeconds, TimeUnit.Seconds);
					}
					catch (RejectedExecutionException)
					{
						// Handling this event in local thread before starting threads
						// or after calling sched.shutdownNow().
						logDeleter.Run();
					}
					break;
				}

				default:
				{
					break;
				}
			}
		}

		// Ignore
		internal virtual ScheduledThreadPoolExecutor CreateScheduledThreadPoolExecutor(Configuration
			 conf)
		{
			ThreadFactory tf = new ThreadFactoryBuilder().SetNameFormat("LogDeleter #%d").Build
				();
			sched = new ScheduledThreadPoolExecutor(conf.GetInt(YarnConfiguration.NmLogDeletionThreadsCount
				, YarnConfiguration.DefaultNmLogDeleteThreadCount), tf);
			return sched;
		}

		internal class LogDeleterRunnable : Runnable
		{
			private string user;

			private ApplicationId applicationId;

			public LogDeleterRunnable(NonAggregatingLogHandler _enclosing, string user, ApplicationId
				 applicationId)
			{
				this._enclosing = _enclosing;
				this.user = user;
				this.applicationId = applicationId;
			}

			public virtual void Run()
			{
				IList<Path> localAppLogDirs = new AList<Path>();
				FileContext lfs = this._enclosing.GetLocalFileContext(this._enclosing.GetConfig()
					);
				foreach (string rootLogDir in this._enclosing.dirsHandler.GetLogDirsForCleanup())
				{
					Path logDir = new Path(rootLogDir, this.applicationId.ToString());
					try
					{
						lfs.GetFileStatus(logDir);
						localAppLogDirs.AddItem(logDir);
					}
					catch (UnsupportedFileSystemException ue)
					{
						NonAggregatingLogHandler.Log.Warn("Unsupported file system used for log dir " + logDir
							, ue);
						continue;
					}
					catch (IOException)
					{
						continue;
					}
				}
				// Inform the application before the actual delete itself, so that links
				// to logs will no longer be there on NM web-UI.
				this._enclosing.dispatcher.GetEventHandler().Handle(new ApplicationEvent(this.applicationId
					, ApplicationEventType.ApplicationLogHandlingFinished));
				if (localAppLogDirs.Count > 0)
				{
					this._enclosing.delService.Delete(this.user, null, (Path[])Sharpen.Collections.ToArray
						(localAppLogDirs, new Path[localAppLogDirs.Count]));
				}
				try
				{
					this._enclosing.stateStore.RemoveLogDeleter(this.applicationId);
				}
				catch (IOException e)
				{
					NonAggregatingLogHandler.Log.Error("Error removing log deletion state", e);
				}
			}

			public override string ToString()
			{
				return "LogDeleter for AppId " + this.applicationId.ToString() + ", owned by " + 
					this.user;
			}

			private readonly NonAggregatingLogHandler _enclosing;
		}
	}
}
