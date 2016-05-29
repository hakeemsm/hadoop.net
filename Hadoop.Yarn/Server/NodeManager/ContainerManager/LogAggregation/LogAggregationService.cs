using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Annotations;
using Com.Google.Common.Util.Concurrent;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Logaggregation;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Loghandler;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Loghandler.Event;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Logaggregation
{
	public class LogAggregationService : AbstractService, LogHandler
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Logaggregation.LogAggregationService
			));

		/// <summary>
		/// Permissions for the top level directory under which app directories will be
		/// created.
		/// </summary>
		private static readonly FsPermission TldirPermissions = FsPermission.CreateImmutable
			((short)0x3ff);

		/// <summary>Permissions for the Application directory.</summary>
		private static readonly FsPermission AppDirPermissions = FsPermission.CreateImmutable
			((short)0x1f8);

		private readonly Context context;

		private readonly DeletionService deletionService;

		private readonly Dispatcher dispatcher;

		private LocalDirsHandlerService dirsHandler;

		internal Path remoteRootLogDir;

		internal string remoteRootLogDirSuffix;

		private NodeId nodeId;

		private readonly ConcurrentMap<ApplicationId, AppLogAggregator> appLogAggregators;

		private readonly ExecutorService threadPool;

		public LogAggregationService(Dispatcher dispatcher, Context context, DeletionService
			 deletionService, LocalDirsHandlerService dirsHandler)
			: base(typeof(Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Logaggregation.LogAggregationService
				).FullName)
		{
			/*
			* Expected deployment TLD will be 1777, owner=<NMOwner>, group=<NMGroup -
			* Group to which NMOwner belongs> App dirs will be created as 770,
			* owner=<AppOwner>, group=<NMGroup>: so that the owner and <NMOwner> can
			* access / modify the files.
			* <NMGroup> should obviously be a limited access group.
			*/
			this.dispatcher = dispatcher;
			this.context = context;
			this.deletionService = deletionService;
			this.dirsHandler = dirsHandler;
			this.appLogAggregators = new ConcurrentHashMap<ApplicationId, AppLogAggregator>();
			this.threadPool = Executors.NewCachedThreadPool(new ThreadFactoryBuilder().SetNameFormat
				("LogAggregationService #%d").Build());
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceInit(Configuration conf)
		{
			this.remoteRootLogDir = new Path(conf.Get(YarnConfiguration.NmRemoteAppLogDir, YarnConfiguration
				.DefaultNmRemoteAppLogDir));
			this.remoteRootLogDirSuffix = conf.Get(YarnConfiguration.NmRemoteAppLogDirSuffix, 
				YarnConfiguration.DefaultNmRemoteAppLogDirSuffix);
			base.ServiceInit(conf);
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStart()
		{
			// NodeId is only available during start, the following cannot be moved
			// anywhere else.
			this.nodeId = this.context.GetNodeId();
			base.ServiceStart();
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStop()
		{
			Log.Info(this.GetName() + " waiting for pending aggregation during exit");
			StopAggregators();
			base.ServiceStop();
		}

		private void StopAggregators()
		{
			threadPool.Shutdown();
			// if recovery on restart is supported then leave outstanding aggregations
			// to the next restart
			bool shouldAbort = context.GetNMStateStore().CanRecover() && !context.GetDecommissioned
				();
			// politely ask to finish
			foreach (AppLogAggregator aggregator in appLogAggregators.Values)
			{
				if (shouldAbort)
				{
					aggregator.AbortLogAggregation();
				}
				else
				{
					aggregator.FinishLogAggregation();
				}
			}
			while (!threadPool.IsTerminated())
			{
				// wait for all threads to finish
				foreach (ApplicationId appId in appLogAggregators.Keys)
				{
					Log.Info("Waiting for aggregation to complete for " + appId);
				}
				try
				{
					if (!threadPool.AwaitTermination(30, TimeUnit.Seconds))
					{
						threadPool.ShutdownNow();
					}
				}
				catch (Exception)
				{
					// send interrupt to hurry them along
					Log.Warn("Aggregation stop interrupted!");
					break;
				}
			}
			foreach (ApplicationId appId_1 in appLogAggregators.Keys)
			{
				Log.Warn("Some logs may not have been aggregated for " + appId_1);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual FileSystem GetFileSystem(Configuration conf)
		{
			return FileSystem.Get(conf);
		}

		internal virtual void VerifyAndCreateRemoteLogDir(Configuration conf)
		{
			// Checking the existence of the TLD
			FileSystem remoteFS = null;
			try
			{
				remoteFS = GetFileSystem(conf);
			}
			catch (IOException e)
			{
				throw new YarnRuntimeException("Unable to get Remote FileSystem instance", e);
			}
			bool remoteExists = true;
			try
			{
				FsPermission perms = remoteFS.GetFileStatus(this.remoteRootLogDir).GetPermission(
					);
				if (!perms.Equals(TldirPermissions))
				{
					Log.Warn("Remote Root Log Dir [" + this.remoteRootLogDir + "] already exist, but with incorrect permissions. "
						 + "Expected: [" + TldirPermissions + "], Found: [" + perms + "]." + " The cluster may have problems with multiple users."
						);
				}
			}
			catch (FileNotFoundException)
			{
				remoteExists = false;
			}
			catch (IOException e)
			{
				throw new YarnRuntimeException("Failed to check permissions for dir [" + this.remoteRootLogDir
					 + "]", e);
			}
			if (!remoteExists)
			{
				Log.Warn("Remote Root Log Dir [" + this.remoteRootLogDir + "] does not exist. Attempting to create it."
					);
				try
				{
					Path qualified = this.remoteRootLogDir.MakeQualified(remoteFS.GetUri(), remoteFS.
						GetWorkingDirectory());
					remoteFS.Mkdirs(qualified, new FsPermission(TldirPermissions));
					remoteFS.SetPermission(qualified, new FsPermission(TldirPermissions));
				}
				catch (IOException e)
				{
					throw new YarnRuntimeException("Failed to create remoteLogDir [" + this.remoteRootLogDir
						 + "]", e);
				}
			}
		}

		internal virtual Path GetRemoteNodeLogFileForApp(ApplicationId appId, string user
			)
		{
			return LogAggregationUtils.GetRemoteNodeLogFileForApp(this.remoteRootLogDir, appId
				, user, this.nodeId, this.remoteRootLogDirSuffix);
		}

		internal virtual Path GetRemoteAppLogDir(ApplicationId appId, string user)
		{
			return LogAggregationUtils.GetRemoteAppLogDir(this.remoteRootLogDir, appId, user, 
				this.remoteRootLogDirSuffix);
		}

		/// <exception cref="System.IO.IOException"/>
		private void CreateDir(FileSystem fs, Path path, FsPermission fsPerm)
		{
			FsPermission dirPerm = new FsPermission(fsPerm);
			fs.Mkdirs(path, dirPerm);
			FsPermission umask = FsPermission.GetUMask(fs.GetConf());
			if (!dirPerm.Equals(dirPerm.ApplyUMask(umask)))
			{
				fs.SetPermission(path, new FsPermission(fsPerm));
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private bool CheckExists(FileSystem fs, Path path, FsPermission fsPerm)
		{
			bool exists = true;
			try
			{
				FileStatus appDirStatus = fs.GetFileStatus(path);
				if (!AppDirPermissions.Equals(appDirStatus.GetPermission()))
				{
					fs.SetPermission(path, AppDirPermissions);
				}
			}
			catch (FileNotFoundException)
			{
				exists = false;
			}
			return exists;
		}

		protected internal virtual void CreateAppDir(string user, ApplicationId appId, UserGroupInformation
			 userUgi)
		{
			try
			{
				userUgi.DoAs(new _PrivilegedExceptionAction_261(this, appId, user));
			}
			catch (Exception e)
			{
				// TODO: Reuse FS for user?
				// Only creating directories if they are missing to avoid
				// unnecessary load on the filesystem from all of the nodes
				throw new YarnRuntimeException(e);
			}
		}

		private sealed class _PrivilegedExceptionAction_261 : PrivilegedExceptionAction<object
			>
		{
			public _PrivilegedExceptionAction_261(LogAggregationService _enclosing, ApplicationId
				 appId, string user)
			{
				this._enclosing = _enclosing;
				this.appId = appId;
				this.user = user;
			}

			/// <exception cref="System.Exception"/>
			public object Run()
			{
				try
				{
					FileSystem remoteFS = this._enclosing.GetFileSystem(this._enclosing.GetConfig());
					Path appDir = LogAggregationUtils.GetRemoteAppLogDir(this._enclosing.remoteRootLogDir
						, appId, user, this._enclosing.remoteRootLogDirSuffix);
					appDir = appDir.MakeQualified(remoteFS.GetUri(), remoteFS.GetWorkingDirectory());
					if (!this._enclosing.CheckExists(remoteFS, appDir, Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Logaggregation.LogAggregationService
						.AppDirPermissions))
					{
						Path suffixDir = LogAggregationUtils.GetRemoteLogSuffixedDir(this._enclosing.remoteRootLogDir
							, user, this._enclosing.remoteRootLogDirSuffix);
						suffixDir = suffixDir.MakeQualified(remoteFS.GetUri(), remoteFS.GetWorkingDirectory
							());
						if (!this._enclosing.CheckExists(remoteFS, suffixDir, Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Logaggregation.LogAggregationService
							.AppDirPermissions))
						{
							Path userDir = LogAggregationUtils.GetRemoteLogUserDir(this._enclosing.remoteRootLogDir
								, user);
							userDir = userDir.MakeQualified(remoteFS.GetUri(), remoteFS.GetWorkingDirectory()
								);
							if (!this._enclosing.CheckExists(remoteFS, userDir, Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Logaggregation.LogAggregationService
								.AppDirPermissions))
							{
								this._enclosing.CreateDir(remoteFS, userDir, Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Logaggregation.LogAggregationService
									.AppDirPermissions);
							}
							this._enclosing.CreateDir(remoteFS, suffixDir, Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Logaggregation.LogAggregationService
								.AppDirPermissions);
						}
						this._enclosing.CreateDir(remoteFS, appDir, Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Logaggregation.LogAggregationService
							.AppDirPermissions);
					}
				}
				catch (IOException e)
				{
					Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Logaggregation.LogAggregationService
						.Log.Error("Failed to setup application log directory for " + appId, e);
					throw;
				}
				return null;
			}

			private readonly LogAggregationService _enclosing;

			private readonly ApplicationId appId;

			private readonly string user;
		}

		private void InitApp(ApplicationId appId, string user, Credentials credentials, ContainerLogsRetentionPolicy
			 logRetentionPolicy, IDictionary<ApplicationAccessType, string> appAcls, LogAggregationContext
			 logAggregationContext)
		{
			ApplicationEvent eventResponse;
			try
			{
				VerifyAndCreateRemoteLogDir(GetConfig());
				InitAppAggregator(appId, user, credentials, logRetentionPolicy, appAcls, logAggregationContext
					);
				eventResponse = new ApplicationEvent(appId, ApplicationEventType.ApplicationLogHandlingInited
					);
			}
			catch (YarnRuntimeException e)
			{
				Log.Warn("Application failed to init aggregation", e);
				eventResponse = new ApplicationEvent(appId, ApplicationEventType.ApplicationLogHandlingFailed
					);
			}
			this.dispatcher.GetEventHandler().Handle(eventResponse);
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

		protected internal virtual void InitAppAggregator(ApplicationId appId, string user
			, Credentials credentials, ContainerLogsRetentionPolicy logRetentionPolicy, IDictionary
			<ApplicationAccessType, string> appAcls, LogAggregationContext logAggregationContext
			)
		{
			// Get user's FileSystem credentials
			UserGroupInformation userUgi = UserGroupInformation.CreateRemoteUser(user);
			if (credentials != null)
			{
				userUgi.AddCredentials(credentials);
			}
			// New application
			AppLogAggregator appLogAggregator = new AppLogAggregatorImpl(this.dispatcher, this
				.deletionService, GetConfig(), appId, userUgi, this.nodeId, dirsHandler, GetRemoteNodeLogFileForApp
				(appId, user), logRetentionPolicy, appAcls, logAggregationContext, this.context, 
				GetLocalFileContext(GetConfig()));
			if (this.appLogAggregators.PutIfAbsent(appId, appLogAggregator) != null)
			{
				throw new YarnRuntimeException("Duplicate initApp for " + appId);
			}
			// wait until check for existing aggregator to create dirs
			YarnRuntimeException appDirException = null;
			try
			{
				// Create the app dir
				CreateAppDir(user, appId, userUgi);
			}
			catch (Exception e)
			{
				appLogAggregator.DisableLogAggregation();
				if (!(e is YarnRuntimeException))
				{
					appDirException = new YarnRuntimeException(e);
				}
				else
				{
					appDirException = (YarnRuntimeException)e;
				}
			}
			// TODO Get the user configuration for the list of containers that need log
			// aggregation.
			// Schedule the aggregator.
			Runnable aggregatorWrapper = new _Runnable_381(this, appLogAggregator, appId, userUgi
				);
			this.threadPool.Execute(aggregatorWrapper);
			if (appDirException != null)
			{
				throw appDirException;
			}
		}

		private sealed class _Runnable_381 : Runnable
		{
			public _Runnable_381(LogAggregationService _enclosing, AppLogAggregator appLogAggregator
				, ApplicationId appId, UserGroupInformation userUgi)
			{
				this._enclosing = _enclosing;
				this.appLogAggregator = appLogAggregator;
				this.appId = appId;
				this.userUgi = userUgi;
			}

			public void Run()
			{
				try
				{
					appLogAggregator.Run();
				}
				finally
				{
					Sharpen.Collections.Remove(this._enclosing.appLogAggregators, appId);
					this._enclosing.CloseFileSystems(userUgi);
				}
			}

			private readonly LogAggregationService _enclosing;

			private readonly AppLogAggregator appLogAggregator;

			private readonly ApplicationId appId;

			private readonly UserGroupInformation userUgi;
		}

		protected internal virtual void CloseFileSystems(UserGroupInformation userUgi)
		{
			try
			{
				FileSystem.CloseAllForUGI(userUgi);
			}
			catch (IOException e)
			{
				Log.Warn("Failed to close filesystems: ", e);
			}
		}

		// for testing only
		[InterfaceAudience.Private]
		internal virtual int GetNumAggregators()
		{
			return this.appLogAggregators.Count;
		}

		private void StopContainer(ContainerId containerId, int exitCode)
		{
			// A container is complete. Put this containers' logs up for aggregation if
			// this containers' logs are needed.
			AppLogAggregator aggregator = this.appLogAggregators[containerId.GetApplicationAttemptId
				().GetApplicationId()];
			if (aggregator == null)
			{
				Log.Warn("Log aggregation is not initialized for " + containerId + ", did it fail to start?"
					);
				return;
			}
			aggregator.StartContainerLogAggregation(containerId, exitCode == 0);
		}

		private void StopApp(ApplicationId appId)
		{
			// App is complete. Finish up any containers' pending log aggregation and
			// close the application specific logFile.
			AppLogAggregator aggregator = this.appLogAggregators[appId];
			if (aggregator == null)
			{
				Log.Warn("Log aggregation is not initialized for " + appId + ", did it fail to start?"
					);
				return;
			}
			aggregator.FinishLogAggregation();
		}

		public virtual void Handle(LogHandlerEvent @event)
		{
			switch (@event.GetType())
			{
				case LogHandlerEventType.ApplicationStarted:
				{
					LogHandlerAppStartedEvent appStartEvent = (LogHandlerAppStartedEvent)@event;
					InitApp(appStartEvent.GetApplicationId(), appStartEvent.GetUser(), appStartEvent.
						GetCredentials(), appStartEvent.GetLogRetentionPolicy(), appStartEvent.GetApplicationAcls
						(), appStartEvent.GetLogAggregationContext());
					break;
				}

				case LogHandlerEventType.ContainerFinished:
				{
					LogHandlerContainerFinishedEvent containerFinishEvent = (LogHandlerContainerFinishedEvent
						)@event;
					StopContainer(containerFinishEvent.GetContainerId(), containerFinishEvent.GetExitCode
						());
					break;
				}

				case LogHandlerEventType.ApplicationFinished:
				{
					LogHandlerAppFinishedEvent appFinishedEvent = (LogHandlerAppFinishedEvent)@event;
					StopApp(appFinishedEvent.GetApplicationId());
					break;
				}

				default:
				{
					break;
				}
			}
		}

		// Ignore
		[VisibleForTesting]
		public virtual ConcurrentMap<ApplicationId, AppLogAggregator> GetAppLogAggregators
			()
		{
			return this.appLogAggregators;
		}

		[VisibleForTesting]
		public virtual NodeId GetNodeId()
		{
			return this.nodeId;
		}
	}
}
