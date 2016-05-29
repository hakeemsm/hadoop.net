using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Logaggregation;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Logaggregation
{
	public class AppLogAggregatorImpl : AppLogAggregator
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Logaggregation.AppLogAggregatorImpl
			));

		private const int ThreadSleepTime = 1000;

		private const string NmLogAggregationNumLogFilesSizePerApp = YarnConfiguration.NmPrefix
			 + "log-aggregation.num-log-files-per-app";

		private const int DefaultNmLogAggregationNumLogFilesSizePerApp = 30;

		private const string NmLogAggregationDebugEnabled = YarnConfiguration.NmPrefix + 
			"log-aggregation.debug-enabled";

		private const bool DefaultNmLogAggregationDebugEnabled = false;

		private const long NmLogAggregationMinRollMonitoringIntervalSeconds = 3600;

		private readonly LocalDirsHandlerService dirsHandler;

		private readonly Dispatcher dispatcher;

		private readonly ApplicationId appId;

		private readonly string applicationId;

		private bool logAggregationDisabled = false;

		private readonly Configuration conf;

		private readonly DeletionService delService;

		private readonly UserGroupInformation userUgi;

		private readonly Path remoteNodeLogFileForApp;

		private readonly Path remoteNodeTmpLogFileForApp;

		private readonly ContainerLogsRetentionPolicy retentionPolicy;

		private readonly BlockingQueue<ContainerId> pendingContainers;

		private readonly AtomicBoolean appFinishing = new AtomicBoolean();

		private readonly AtomicBoolean appAggregationFinished = new AtomicBoolean();

		private readonly AtomicBoolean aborted = new AtomicBoolean();

		private readonly IDictionary<ApplicationAccessType, string> appAcls;

		private readonly FileContext lfs;

		private readonly LogAggregationContext logAggregationContext;

		private readonly Context context;

		private readonly int retentionSize;

		private readonly long rollingMonitorInterval;

		private readonly bool logAggregationInRolling;

		private readonly NodeId nodeId;

		private readonly AtomicBoolean waiting = new AtomicBoolean(false);

		private readonly IDictionary<ContainerId, AppLogAggregatorImpl.ContainerLogAggregator
			> containerLogAggregators = new Dictionary<ContainerId, AppLogAggregatorImpl.ContainerLogAggregator
			>();

		public AppLogAggregatorImpl(Dispatcher dispatcher, DeletionService deletionService
			, Configuration conf, ApplicationId appId, UserGroupInformation userUgi, NodeId 
			nodeId, LocalDirsHandlerService dirsHandler, Path remoteNodeLogFileForApp, ContainerLogsRetentionPolicy
			 retentionPolicy, IDictionary<ApplicationAccessType, string> appAcls, LogAggregationContext
			 logAggregationContext, Context context, FileContext lfs)
		{
			// This is temporary solution. The configuration will be deleted once
			// we find a more scalable method to only write a single log file per LRS.
			// This configuration is for debug and test purpose. By setting
			// this configuration as true. We can break the lower bound of
			// NM_LOG_AGGREGATION_ROLL_MONITORING_INTERVAL_SECONDS.
			// This variable is only for testing
			this.dispatcher = dispatcher;
			this.conf = conf;
			this.delService = deletionService;
			this.appId = appId;
			this.applicationId = ConverterUtils.ToString(appId);
			this.userUgi = userUgi;
			this.dirsHandler = dirsHandler;
			this.remoteNodeLogFileForApp = remoteNodeLogFileForApp;
			this.remoteNodeTmpLogFileForApp = GetRemoteNodeTmpLogFileForApp();
			this.retentionPolicy = retentionPolicy;
			this.pendingContainers = new LinkedBlockingQueue<ContainerId>();
			this.appAcls = appAcls;
			this.lfs = lfs;
			this.logAggregationContext = logAggregationContext;
			this.context = context;
			this.nodeId = nodeId;
			int configuredRentionSize = conf.GetInt(NmLogAggregationNumLogFilesSizePerApp, DefaultNmLogAggregationNumLogFilesSizePerApp
				);
			if (configuredRentionSize <= 0)
			{
				this.retentionSize = DefaultNmLogAggregationNumLogFilesSizePerApp;
			}
			else
			{
				this.retentionSize = configuredRentionSize;
			}
			long configuredRollingMonitorInterval = conf.GetLong(YarnConfiguration.NmLogAggregationRollMonitoringIntervalSeconds
				, YarnConfiguration.DefaultNmLogAggregationRollMonitoringIntervalSeconds);
			bool debug_mode = conf.GetBoolean(NmLogAggregationDebugEnabled, DefaultNmLogAggregationDebugEnabled
				);
			if (configuredRollingMonitorInterval > 0 && configuredRollingMonitorInterval < NmLogAggregationMinRollMonitoringIntervalSeconds)
			{
				if (debug_mode)
				{
					this.rollingMonitorInterval = configuredRollingMonitorInterval;
				}
				else
				{
					Log.Warn("rollingMonitorIntervall should be more than or equal to " + NmLogAggregationMinRollMonitoringIntervalSeconds
						 + " seconds. Using " + NmLogAggregationMinRollMonitoringIntervalSeconds + " seconds instead."
						);
					this.rollingMonitorInterval = NmLogAggregationMinRollMonitoringIntervalSeconds;
				}
			}
			else
			{
				if (configuredRollingMonitorInterval <= 0)
				{
					Log.Warn("rollingMonitorInterval is set as " + configuredRollingMonitorInterval +
						 ". " + "The log rolling mornitoring interval is disabled. " + "The logs will be aggregated after this application is finished."
						);
				}
				else
				{
					Log.Warn("rollingMonitorInterval is set as " + configuredRollingMonitorInterval +
						 ". " + "The logs will be aggregated every " + configuredRollingMonitorInterval 
						+ " seconds");
				}
				this.rollingMonitorInterval = configuredRollingMonitorInterval;
			}
			this.logAggregationInRolling = this.rollingMonitorInterval <= 0 || this.logAggregationContext
				 == null || this.logAggregationContext.GetRolledLogsIncludePattern() == null || 
				this.logAggregationContext.GetRolledLogsIncludePattern().IsEmpty() ? false : true;
		}

		private void UploadLogsForContainers(bool appFinished)
		{
			if (this.logAggregationDisabled)
			{
				return;
			}
			if (UserGroupInformation.IsSecurityEnabled())
			{
				Credentials systemCredentials = context.GetSystemCredentialsForApps()[appId];
				if (systemCredentials != null)
				{
					if (Log.IsDebugEnabled())
					{
						Log.Debug("Adding new framework-token for " + appId + " for log-aggregation: " + 
							systemCredentials.GetAllTokens() + "; userUgi=" + userUgi);
					}
					// this will replace old token
					userUgi.AddCredentials(systemCredentials);
				}
			}
			// Create a set of Containers whose logs will be uploaded in this cycle.
			// It includes:
			// a) all containers in pendingContainers: those containers are finished
			//    and satisfy the retentionPolicy.
			// b) some set of running containers: For all the Running containers,
			// we have ContainerLogsRetentionPolicy.AM_AND_FAILED_CONTAINERS_ONLY,
			// so simply set wasContainerSuccessful as true to
			// bypass FAILED_CONTAINERS check and find the running containers 
			// which satisfy the retentionPolicy.
			ICollection<ContainerId> pendingContainerInThisCycle = new HashSet<ContainerId>();
			this.pendingContainers.DrainTo(pendingContainerInThisCycle);
			ICollection<ContainerId> finishedContainers = new HashSet<ContainerId>(pendingContainerInThisCycle
				);
			if (this.context.GetApplications()[this.appId] != null)
			{
				foreach (ContainerId container in this.context.GetApplications()[this.appId].GetContainers
					().Keys)
				{
					if (ShouldUploadLogs(container, true))
					{
						pendingContainerInThisCycle.AddItem(container);
					}
				}
			}
			AggregatedLogFormat.LogWriter writer = null;
			try
			{
				try
				{
					writer = new AggregatedLogFormat.LogWriter(this.conf, this.remoteNodeTmpLogFileForApp
						, this.userUgi);
					// Write ACLs once when the writer is created.
					writer.WriteApplicationACLs(appAcls);
					writer.WriteApplicationOwner(this.userUgi.GetShortUserName());
				}
				catch (IOException e1)
				{
					Log.Error("Cannot create writer for app " + this.applicationId + ". Skip log upload this time. "
						, e1);
					return;
				}
				bool uploadedLogsInThisCycle = false;
				foreach (ContainerId container in pendingContainerInThisCycle)
				{
					AppLogAggregatorImpl.ContainerLogAggregator aggregator = null;
					if (containerLogAggregators.Contains(container))
					{
						aggregator = containerLogAggregators[container];
					}
					else
					{
						aggregator = new AppLogAggregatorImpl.ContainerLogAggregator(this, container);
						containerLogAggregators[container] = aggregator;
					}
					ICollection<Path> uploadedFilePathsInThisCycle = aggregator.DoContainerLogAggregation
						(writer, appFinished);
					if (uploadedFilePathsInThisCycle.Count > 0)
					{
						uploadedLogsInThisCycle = true;
						this.delService.Delete(this.userUgi.GetShortUserName(), null, Sharpen.Collections.ToArray
							(uploadedFilePathsInThisCycle, new Path[uploadedFilePathsInThisCycle.Count]));
					}
					// This container is finished, and all its logs have been uploaded,
					// remove it from containerLogAggregators.
					if (finishedContainers.Contains(container))
					{
						Sharpen.Collections.Remove(containerLogAggregators, container);
					}
				}
				// Before upload logs, make sure the number of existing logs
				// is smaller than the configured NM log aggregation retention size.
				if (uploadedLogsInThisCycle)
				{
					CleanOldLogs();
				}
				if (writer != null)
				{
					writer.Close();
					writer = null;
				}
				Path renamedPath = this.rollingMonitorInterval <= 0 ? remoteNodeLogFileForApp : new 
					Path(remoteNodeLogFileForApp.GetParent(), remoteNodeLogFileForApp.GetName() + "_"
					 + Runtime.CurrentTimeMillis());
				bool rename = uploadedLogsInThisCycle;
				try
				{
					userUgi.DoAs(new _PrivilegedExceptionAction_304(this, rename, renamedPath));
				}
				catch (Exception e)
				{
					Log.Error("Failed to move temporary log file to final location: [" + remoteNodeTmpLogFileForApp
						 + "] to [" + renamedPath + "]", e);
				}
			}
			finally
			{
				if (writer != null)
				{
					writer.Close();
				}
			}
		}

		private sealed class _PrivilegedExceptionAction_304 : PrivilegedExceptionAction<object
			>
		{
			public _PrivilegedExceptionAction_304(AppLogAggregatorImpl _enclosing, bool rename
				, Path renamedPath)
			{
				this._enclosing = _enclosing;
				this.rename = rename;
				this.renamedPath = renamedPath;
			}

			/// <exception cref="System.Exception"/>
			public object Run()
			{
				FileSystem remoteFS = FileSystem.Get(this._enclosing.conf);
				if (remoteFS.Exists(this._enclosing.remoteNodeTmpLogFileForApp))
				{
					if (rename)
					{
						remoteFS.Rename(this._enclosing.remoteNodeTmpLogFileForApp, renamedPath);
					}
					else
					{
						remoteFS.Delete(this._enclosing.remoteNodeTmpLogFileForApp, false);
					}
				}
				return null;
			}

			private readonly AppLogAggregatorImpl _enclosing;

			private readonly bool rename;

			private readonly Path renamedPath;
		}

		private void CleanOldLogs()
		{
			try
			{
				FileSystem remoteFS = this.remoteNodeLogFileForApp.GetFileSystem(conf);
				Path appDir = this.remoteNodeLogFileForApp.GetParent().MakeQualified(remoteFS.GetUri
					(), remoteFS.GetWorkingDirectory());
				ICollection<FileStatus> status = new HashSet<FileStatus>(Arrays.AsList(remoteFS.ListStatus
					(appDir)));
				IEnumerable<FileStatus> mask = Iterables.Filter(status, new _Predicate_342(this));
				status = Sets.NewHashSet(mask);
				// Normally, we just need to delete one oldest log
				// before we upload a new log.
				// If we can not delete the older logs in this cycle,
				// we will delete them in next cycle.
				if (status.Count >= this.retentionSize)
				{
					// sort by the lastModificationTime ascending
					IList<FileStatus> statusList = new AList<FileStatus>(status);
					statusList.Sort(new _IComparer_359());
					for (int i = 0; i <= statusList.Count - this.retentionSize; i++)
					{
						FileStatus remove = statusList[i];
						try
						{
							userUgi.DoAs(new _PrivilegedExceptionAction_368(remoteFS, remove));
						}
						catch (Exception e)
						{
							Log.Error("Failed to delete " + remove.GetPath(), e);
						}
					}
				}
			}
			catch (Exception e)
			{
				Log.Error("Failed to clean old logs", e);
			}
		}

		private sealed class _Predicate_342 : Predicate<FileStatus>
		{
			public _Predicate_342(AppLogAggregatorImpl _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public bool Apply(FileStatus next)
			{
				return next.GetPath().GetName().Contains(LogAggregationUtils.GetNodeString(this._enclosing
					.nodeId)) && !next.GetPath().GetName().EndsWith(LogAggregationUtils.TmpFileSuffix
					);
			}

			private readonly AppLogAggregatorImpl _enclosing;
		}

		private sealed class _IComparer_359 : IComparer<FileStatus>
		{
			public _IComparer_359()
			{
			}

			public int Compare(FileStatus s1, FileStatus s2)
			{
				return s1.GetModificationTime() < s2.GetModificationTime() ? -1 : s1.GetModificationTime
					() > s2.GetModificationTime() ? 1 : 0;
			}
		}

		private sealed class _PrivilegedExceptionAction_368 : PrivilegedExceptionAction<object
			>
		{
			public _PrivilegedExceptionAction_368(FileSystem remoteFS, FileStatus remove)
			{
				this.remoteFS = remoteFS;
				this.remove = remove;
			}

			/// <exception cref="System.Exception"/>
			public object Run()
			{
				remoteFS.Delete(remove.GetPath(), false);
				return null;
			}

			private readonly FileSystem remoteFS;

			private readonly FileStatus remove;
		}

		public virtual void Run()
		{
			try
			{
				DoAppLogAggregation();
			}
			catch (Exception e)
			{
				// do post clean up of log directories on any exception
				Log.Error("Error occured while aggregating the log for the application " + appId, 
					e);
				DoAppLogAggregationPostCleanUp();
			}
			finally
			{
				if (!this.appAggregationFinished.Get())
				{
					Log.Warn("Aggregation did not complete for application " + appId);
				}
				this.appAggregationFinished.Set(true);
			}
		}

		private void DoAppLogAggregation()
		{
			while (!this.appFinishing.Get() && !this.aborted.Get())
			{
				lock (this)
				{
					try
					{
						waiting.Set(true);
						if (logAggregationInRolling)
						{
							Sharpen.Runtime.Wait(this, this.rollingMonitorInterval * 1000);
							if (this.appFinishing.Get() || this.aborted.Get())
							{
								break;
							}
							UploadLogsForContainers(false);
						}
						else
						{
							Sharpen.Runtime.Wait(this, ThreadSleepTime);
						}
					}
					catch (Exception)
					{
						Log.Warn("PendingContainers queue is interrupted");
						this.appFinishing.Set(true);
					}
				}
			}
			if (this.aborted.Get())
			{
				return;
			}
			// App is finished, upload the container logs.
			UploadLogsForContainers(true);
			DoAppLogAggregationPostCleanUp();
			this.dispatcher.GetEventHandler().Handle(new ApplicationEvent(this.appId, ApplicationEventType
				.ApplicationLogHandlingFinished));
			this.appAggregationFinished.Set(true);
		}

		private void DoAppLogAggregationPostCleanUp()
		{
			// Remove the local app-log-dirs
			IList<Path> localAppLogDirs = new AList<Path>();
			foreach (string rootLogDir in dirsHandler.GetLogDirsForCleanup())
			{
				Path logPath = new Path(rootLogDir, applicationId);
				try
				{
					// check if log dir exists
					lfs.GetFileStatus(logPath);
					localAppLogDirs.AddItem(logPath);
				}
				catch (UnsupportedFileSystemException ue)
				{
					Log.Warn("Log dir " + rootLogDir + "is an unsupported file system", ue);
					continue;
				}
				catch (IOException)
				{
					continue;
				}
			}
			if (localAppLogDirs.Count > 0)
			{
				this.delService.Delete(this.userUgi.GetShortUserName(), null, Sharpen.Collections.ToArray
					(localAppLogDirs, new Path[localAppLogDirs.Count]));
			}
		}

		private Path GetRemoteNodeTmpLogFileForApp()
		{
			return new Path(remoteNodeLogFileForApp.GetParent(), (remoteNodeLogFileForApp.GetName
				() + LogAggregationUtils.TmpFileSuffix));
		}

		// TODO: The condition: containerId.getId() == 1 to determine an AM container
		// is not always true.
		private bool ShouldUploadLogs(ContainerId containerId, bool wasContainerSuccessful
			)
		{
			// All containers
			if (this.retentionPolicy.Equals(ContainerLogsRetentionPolicy.AllContainers))
			{
				return true;
			}
			// AM Container only
			if (this.retentionPolicy.Equals(ContainerLogsRetentionPolicy.ApplicationMasterOnly
				))
			{
				if ((containerId.GetContainerId() & ContainerId.ContainerIdBitmask) == 1)
				{
					return true;
				}
				return false;
			}
			// AM + Failing containers
			if (this.retentionPolicy.Equals(ContainerLogsRetentionPolicy.AmAndFailedContainersOnly
				))
			{
				if ((containerId.GetContainerId() & ContainerId.ContainerIdBitmask) == 1)
				{
					return true;
				}
				else
				{
					if (!wasContainerSuccessful)
					{
						return true;
					}
				}
				return false;
			}
			return false;
		}

		public virtual void StartContainerLogAggregation(ContainerId containerId, bool wasContainerSuccessful
			)
		{
			if (ShouldUploadLogs(containerId, wasContainerSuccessful))
			{
				Log.Info("Considering container " + containerId + " for log-aggregation");
				this.pendingContainers.AddItem(containerId);
			}
		}

		public virtual void FinishLogAggregation()
		{
			lock (this)
			{
				Log.Info("Application just finished : " + this.applicationId);
				this.appFinishing.Set(true);
				Sharpen.Runtime.NotifyAll(this);
			}
		}

		public virtual void AbortLogAggregation()
		{
			lock (this)
			{
				Log.Info("Aborting log aggregation for " + this.applicationId);
				this.aborted.Set(true);
				Sharpen.Runtime.NotifyAll(this);
			}
		}

		public virtual void DisableLogAggregation()
		{
			this.logAggregationDisabled = true;
		}

		[InterfaceAudience.Private]
		[VisibleForTesting]
		public virtual void DoLogAggregationOutOfBand()
		{
			lock (this)
			{
				// This is only used for testing.
				// This will wake the log aggregation thread that is waiting for
				// rollingMonitorInterval.
				// To use this method, make sure the log aggregation thread is running
				// and waiting for rollingMonitorInterval.
				while (!waiting.Get())
				{
					try
					{
						Sharpen.Runtime.Wait(this, 200);
					}
					catch (Exception)
					{
					}
				}
				// Do Nothing
				Log.Info("Do OutOfBand log aggregation");
				Sharpen.Runtime.NotifyAll(this);
			}
		}

		private class ContainerLogAggregator
		{
			private readonly ContainerId containerId;

			private ICollection<string> uploadedFileMeta = new HashSet<string>();

			public ContainerLogAggregator(AppLogAggregatorImpl _enclosing, ContainerId containerId
				)
			{
				this._enclosing = _enclosing;
				this.containerId = containerId;
			}

			public virtual ICollection<Path> DoContainerLogAggregation(AggregatedLogFormat.LogWriter
				 writer, bool appFinished)
			{
				AppLogAggregatorImpl.Log.Info("Uploading logs for container " + this.containerId 
					+ ". Current good log dirs are " + StringUtils.Join(",", this._enclosing.dirsHandler
					.GetLogDirsForRead()));
				AggregatedLogFormat.LogKey logKey = new AggregatedLogFormat.LogKey(this.containerId
					);
				AggregatedLogFormat.LogValue logValue = new AggregatedLogFormat.LogValue(this._enclosing
					.dirsHandler.GetLogDirsForRead(), this.containerId, this._enclosing.userUgi.GetShortUserName
					(), this._enclosing.logAggregationContext, this.uploadedFileMeta, appFinished);
				try
				{
					writer.Append(logKey, logValue);
				}
				catch (Exception e)
				{
					AppLogAggregatorImpl.Log.Error("Couldn't upload logs for " + this.containerId + ". Skipping this container."
						, e);
					return new HashSet<Path>();
				}
				Sharpen.Collections.AddAll(this.uploadedFileMeta, logValue.GetCurrentUpLoadedFileMeta
					());
				// if any of the previous uploaded logs have been deleted,
				// we need to remove them from alreadyUploadedLogs
				IEnumerable<string> mask = Iterables.Filter(this.uploadedFileMeta, new _Predicate_581
					(logValue));
				this.uploadedFileMeta = Sets.NewHashSet(mask);
				return logValue.GetCurrentUpLoadedFilesPath();
			}

			private sealed class _Predicate_581 : Predicate<string>
			{
				public _Predicate_581(AggregatedLogFormat.LogValue logValue)
				{
					this.logValue = logValue;
				}

				public bool Apply(string next)
				{
					return logValue.GetAllExistingFilesMeta().Contains(next);
				}

				private readonly AggregatedLogFormat.LogValue logValue;
			}

			private readonly AppLogAggregatorImpl _enclosing;
		}

		// only for test
		[VisibleForTesting]
		public virtual UserGroupInformation GetUgi()
		{
			return this.userUgi;
		}
	}
}
