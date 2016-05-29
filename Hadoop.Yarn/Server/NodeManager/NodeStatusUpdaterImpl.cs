using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Server.Api;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Server.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Metrics;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager
{
	public class NodeStatusUpdaterImpl : AbstractService, NodeStatusUpdater
	{
		public const string YarnNodemanagerDurationToTrackStoppedContainers = YarnConfiguration
			.NmPrefix + "duration-to-track-stopped-containers";

		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Nodemanager.NodeStatusUpdaterImpl
			));

		private readonly object heartbeatMonitor = new object();

		private readonly Context context;

		private readonly Dispatcher dispatcher;

		private NodeId nodeId;

		private long nextHeartBeatInterval;

		private ResourceTracker resourceTracker;

		private Resource totalResource;

		private int httpPort;

		private string nodeManagerVersionId;

		private string minimumResourceManagerVersion;

		private volatile bool isStopped;

		private bool tokenKeepAliveEnabled;

		private long tokenRemovalDelayMs;

		/// <summary>Keeps track of when the next keep alive request should be sent for an app
		/// 	</summary>
		private IDictionary<ApplicationId, long> appTokenKeepAliveMap = new Dictionary<ApplicationId
			, long>();

		private Random keepAliveDelayRandom = new Random();

		private readonly IDictionary<ContainerId, long> recentlyStoppedContainers;

		private readonly IDictionary<ContainerId, ContainerStatus> pendingCompletedContainers;

		private long durationToTrackStoppedContainers;

		private readonly NodeHealthCheckerService healthChecker;

		private readonly NodeManagerMetrics metrics;

		private Runnable statusUpdaterRunnable;

		private Sharpen.Thread statusUpdater;

		private long rmIdentifier = ResourceManagerConstants.RmInvalidIdentifier;

		internal ICollection<ContainerId> pendingContainersToRemove = new HashSet<ContainerId
			>();

		public NodeStatusUpdaterImpl(Context context, Dispatcher dispatcher, NodeHealthCheckerService
			 healthChecker, NodeManagerMetrics metrics)
			: base(typeof(Org.Apache.Hadoop.Yarn.Server.Nodemanager.NodeStatusUpdaterImpl).FullName
				)
		{
			// It will be used to track recently stopped containers on node manager, this
			// is to avoid the misleading no-such-container exception messages on NM, when
			// the AM finishes it informs the RM to stop the may-be-already-completed
			// containers.
			// Save the reported completed containers in case of lost heartbeat responses.
			// These completed containers will be sent again till a successful response.
			// Duration for which to track recently stopped container.
			this.healthChecker = healthChecker;
			this.context = context;
			this.dispatcher = dispatcher;
			this.metrics = metrics;
			this.recentlyStoppedContainers = new LinkedHashMap<ContainerId, long>();
			this.pendingCompletedContainers = new Dictionary<ContainerId, ContainerStatus>();
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceInit(Configuration conf)
		{
			int memoryMb = conf.GetInt(YarnConfiguration.NmPmemMb, YarnConfiguration.DefaultNmPmemMb
				);
			float vMemToPMem = conf.GetFloat(YarnConfiguration.NmVmemPmemRatio, YarnConfiguration
				.DefaultNmVmemPmemRatio);
			int virtualMemoryMb = (int)Math.Ceil(memoryMb * vMemToPMem);
			int virtualCores = conf.GetInt(YarnConfiguration.NmVcores, YarnConfiguration.DefaultNmVcores
				);
			this.totalResource = Resource.NewInstance(memoryMb, virtualCores);
			metrics.AddResource(totalResource);
			this.tokenKeepAliveEnabled = IsTokenKeepAliveEnabled(conf);
			this.tokenRemovalDelayMs = conf.GetInt(YarnConfiguration.RmNmExpiryIntervalMs, YarnConfiguration
				.DefaultRmNmExpiryIntervalMs);
			this.minimumResourceManagerVersion = conf.Get(YarnConfiguration.NmResourcemanagerMinimumVersion
				, YarnConfiguration.DefaultNmResourcemanagerMinimumVersion);
			// Default duration to track stopped containers on nodemanager is 10Min.
			// This should not be assigned very large value as it will remember all the
			// containers stopped during that time.
			durationToTrackStoppedContainers = conf.GetLong(YarnNodemanagerDurationToTrackStoppedContainers
				, 600000);
			if (durationToTrackStoppedContainers < 0)
			{
				string message = "Invalid configuration for " + YarnNodemanagerDurationToTrackStoppedContainers
					 + " default " + "value is 10Min(600000).";
				Log.Error(message);
				throw new YarnException(message);
			}
			if (Log.IsDebugEnabled())
			{
				Log.Debug(YarnNodemanagerDurationToTrackStoppedContainers + " :" + durationToTrackStoppedContainers
					);
			}
			base.ServiceInit(conf);
			Log.Info("Initialized nodemanager for " + nodeId + ":" + " physical-memory=" + memoryMb
				 + " virtual-memory=" + virtualMemoryMb + " virtual-cores=" + virtualCores);
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStart()
		{
			// NodeManager is the last service to start, so NodeId is available.
			this.nodeId = this.context.GetNodeId();
			this.httpPort = this.context.GetHttpPort();
			this.nodeManagerVersionId = YarnVersionInfo.GetVersion();
			try
			{
				// Registration has to be in start so that ContainerManager can get the
				// perNM tokens needed to authenticate ContainerTokens.
				this.resourceTracker = GetRMClient();
				RegisterWithRM();
				base.ServiceStart();
				StartStatusUpdater();
			}
			catch (Exception e)
			{
				string errorMessage = "Unexpected error starting NodeStatusUpdater";
				Log.Error(errorMessage, e);
				throw new YarnRuntimeException(e);
			}
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStop()
		{
			// Interrupt the updater.
			this.isStopped = true;
			StopRMProxy();
			base.ServiceStop();
		}

		protected internal virtual void RebootNodeStatusUpdaterAndRegisterWithRM()
		{
			// Interrupt the updater.
			this.isStopped = true;
			try
			{
				statusUpdater.Join();
				RegisterWithRM();
				statusUpdater = new Sharpen.Thread(statusUpdaterRunnable, "Node Status Updater");
				this.isStopped = false;
				statusUpdater.Start();
				Log.Info("NodeStatusUpdater thread is reRegistered and restarted");
			}
			catch (Exception e)
			{
				string errorMessage = "Unexpected error rebooting NodeStatusUpdater";
				Log.Error(errorMessage, e);
				throw new YarnRuntimeException(e);
			}
		}

		[VisibleForTesting]
		protected internal virtual void StopRMProxy()
		{
			if (this.resourceTracker != null)
			{
				RPC.StopProxy(this.resourceTracker);
			}
		}

		[InterfaceAudience.Private]
		protected internal virtual bool IsTokenKeepAliveEnabled(Configuration conf)
		{
			return conf.GetBoolean(YarnConfiguration.LogAggregationEnabled, YarnConfiguration
				.DefaultLogAggregationEnabled) && UserGroupInformation.IsSecurityEnabled();
		}

		/// <exception cref="System.IO.IOException"/>
		[VisibleForTesting]
		protected internal virtual ResourceTracker GetRMClient()
		{
			Configuration conf = GetConfig();
			return ServerRMProxy.CreateRMProxy<ResourceTracker>(conf);
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		[VisibleForTesting]
		protected internal virtual void RegisterWithRM()
		{
			IList<NMContainerStatus> containerReports = GetNMContainerStatuses();
			RegisterNodeManagerRequest request = RegisterNodeManagerRequest.NewInstance(nodeId
				, httpPort, totalResource, nodeManagerVersionId, containerReports, GetRunningApplications
				());
			if (containerReports != null)
			{
				Log.Info("Registering with RM using containers :" + containerReports);
			}
			RegisterNodeManagerResponse regNMResponse = resourceTracker.RegisterNodeManager(request
				);
			this.rmIdentifier = regNMResponse.GetRMIdentifier();
			// if the Resourcemanager instructs NM to shutdown.
			if (NodeAction.Shutdown.Equals(regNMResponse.GetNodeAction()))
			{
				string message = "Message from ResourceManager: " + regNMResponse.GetDiagnosticsMessage
					();
				throw new YarnRuntimeException("Recieved SHUTDOWN signal from Resourcemanager ,Registration of NodeManager failed, "
					 + message);
			}
			// if ResourceManager version is too old then shutdown
			if (!minimumResourceManagerVersion.Equals("NONE"))
			{
				if (minimumResourceManagerVersion.Equals("EqualToNM"))
				{
					minimumResourceManagerVersion = nodeManagerVersionId;
				}
				string rmVersion = regNMResponse.GetRMVersion();
				if (rmVersion == null)
				{
					string message = "The Resource Manager's did not return a version. " + "Valid version cannot be checked.";
					throw new YarnRuntimeException("Shutting down the Node Manager. " + message);
				}
				if (VersionUtil.CompareVersions(rmVersion, minimumResourceManagerVersion) < 0)
				{
					string message = "The Resource Manager's version (" + rmVersion + ") is less than the minimum "
						 + "allowed version " + minimumResourceManagerVersion;
					throw new YarnRuntimeException("Shutting down the Node Manager on RM " + "version error, "
						 + message);
				}
			}
			MasterKey masterKey = regNMResponse.GetContainerTokenMasterKey();
			// do this now so that its set before we start heartbeating to RM
			// It is expected that status updater is started by this point and
			// RM gives the shared secret in registration during
			// StatusUpdater#start().
			if (masterKey != null)
			{
				this.context.GetContainerTokenSecretManager().SetMasterKey(masterKey);
			}
			masterKey = regNMResponse.GetNMTokenMasterKey();
			if (masterKey != null)
			{
				this.context.GetNMTokenSecretManager().SetMasterKey(masterKey);
			}
			Log.Info("Registered with ResourceManager as " + this.nodeId + " with total resource of "
				 + this.totalResource);
			Log.Info("Notifying ContainerManager to unblock new container-requests");
			((ContainerManagerImpl)this.context.GetContainerManager()).SetBlockNewContainerRequests
				(false);
		}

		private IList<ApplicationId> CreateKeepAliveApplicationList()
		{
			if (!tokenKeepAliveEnabled)
			{
				return Sharpen.Collections.EmptyList();
			}
			IList<ApplicationId> appList = new AList<ApplicationId>();
			for (IEnumerator<KeyValuePair<ApplicationId, long>> i = this.appTokenKeepAliveMap
				.GetEnumerator(); i.HasNext(); )
			{
				KeyValuePair<ApplicationId, long> e = i.Next();
				ApplicationId appId = e.Key;
				long nextKeepAlive = e.Value;
				if (!this.context.GetApplications().Contains(appId))
				{
					// Remove if the application has finished.
					i.Remove();
				}
				else
				{
					if (Runtime.CurrentTimeMillis() > nextKeepAlive)
					{
						// KeepAlive list for the next hearbeat.
						appList.AddItem(appId);
						TrackAppForKeepAlive(appId);
					}
				}
			}
			return appList;
		}

		/// <exception cref="System.IO.IOException"/>
		private NodeStatus GetNodeStatus(int responseId)
		{
			NodeHealthStatus nodeHealthStatus = this.context.GetNodeHealthStatus();
			nodeHealthStatus.SetHealthReport(healthChecker.GetHealthReport());
			nodeHealthStatus.SetIsNodeHealthy(healthChecker.IsHealthy());
			nodeHealthStatus.SetLastHealthReportTime(healthChecker.GetLastHealthReportTime());
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Node's health-status : " + nodeHealthStatus.GetIsNodeHealthy() + ", " 
					+ nodeHealthStatus.GetHealthReport());
			}
			IList<ContainerStatus> containersStatuses = GetContainerStatuses();
			NodeStatus nodeStatus = NodeStatus.NewInstance(nodeId, responseId, containersStatuses
				, CreateKeepAliveApplicationList(), nodeHealthStatus);
			return nodeStatus;
		}

		// Iterate through the NMContext and clone and get all the containers'
		// statuses. If it's a completed container, add into the
		// recentlyStoppedContainers collections.
		/// <exception cref="System.IO.IOException"/>
		[VisibleForTesting]
		protected internal virtual IList<ContainerStatus> GetContainerStatuses()
		{
			IList<ContainerStatus> containerStatuses = new AList<ContainerStatus>();
			foreach (Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
				 container in this.context.GetContainers().Values)
			{
				ContainerId containerId = container.GetContainerId();
				ApplicationId applicationId = containerId.GetApplicationAttemptId().GetApplicationId
					();
				ContainerStatus containerStatus = container.CloneAndGetContainerStatus();
				if (containerStatus.GetState() == ContainerState.Complete)
				{
					if (IsApplicationStopped(applicationId))
					{
						if (Log.IsDebugEnabled())
						{
							Log.Debug(applicationId + " is completing, " + " remove " + containerId + " from NM context."
								);
						}
						Sharpen.Collections.Remove(context.GetContainers(), containerId);
						pendingCompletedContainers[containerId] = containerStatus;
					}
					else
					{
						if (!IsContainerRecentlyStopped(containerId))
						{
							pendingCompletedContainers[containerId] = containerStatus;
						}
					}
					// Adding to finished containers cache. Cache will keep it around at
					// least for #durationToTrackStoppedContainers duration. In the
					// subsequent call to stop container it will get removed from cache.
					AddCompletedContainer(containerId);
				}
				else
				{
					containerStatuses.AddItem(containerStatus);
				}
			}
			Sharpen.Collections.AddAll(containerStatuses, pendingCompletedContainers.Values);
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Sending out " + containerStatuses.Count + " container statuses: " + containerStatuses
					);
			}
			return containerStatuses;
		}

		private IList<ApplicationId> GetRunningApplications()
		{
			IList<ApplicationId> runningApplications = new AList<ApplicationId>();
			Sharpen.Collections.AddAll(runningApplications, this.context.GetApplications().Keys
				);
			return runningApplications;
		}

		// These NMContainerStatus are sent on NM registration and used by YARN only.
		/// <exception cref="System.IO.IOException"/>
		private IList<NMContainerStatus> GetNMContainerStatuses()
		{
			IList<NMContainerStatus> containerStatuses = new AList<NMContainerStatus>();
			foreach (Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
				 container in this.context.GetContainers().Values)
			{
				ContainerId containerId = container.GetContainerId();
				ApplicationId applicationId = containerId.GetApplicationAttemptId().GetApplicationId
					();
				if (!this.context.GetApplications().Contains(applicationId))
				{
					Sharpen.Collections.Remove(context.GetContainers(), containerId);
					continue;
				}
				NMContainerStatus status = container.GetNMContainerStatus();
				containerStatuses.AddItem(status);
				if (status.GetContainerState() == ContainerState.Complete)
				{
					// Adding to finished containers cache. Cache will keep it around at
					// least for #durationToTrackStoppedContainers duration. In the
					// subsequent call to stop container it will get removed from cache.
					AddCompletedContainer(containerId);
				}
			}
			Log.Info("Sending out " + containerStatuses.Count + " NM container statuses: " + 
				containerStatuses);
			return containerStatuses;
		}

		private bool IsApplicationStopped(ApplicationId applicationId)
		{
			if (!this.context.GetApplications().Contains(applicationId))
			{
				return true;
			}
			ApplicationState applicationState = this.context.GetApplications()[applicationId]
				.GetApplicationState();
			if (applicationState == ApplicationState.FinishingContainersWait || applicationState
				 == ApplicationState.ApplicationResourcesCleaningup || applicationState == ApplicationState
				.Finished)
			{
				return true;
			}
			else
			{
				return false;
			}
		}

		public virtual void AddCompletedContainer(ContainerId containerId)
		{
			lock (recentlyStoppedContainers)
			{
				RemoveVeryOldStoppedContainersFromCache();
				if (!recentlyStoppedContainers.Contains(containerId))
				{
					recentlyStoppedContainers[containerId] = Runtime.CurrentTimeMillis() + durationToTrackStoppedContainers;
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[VisibleForTesting]
		[InterfaceAudience.Private]
		public virtual void RemoveOrTrackCompletedContainersFromContext(IList<ContainerId
			> containerIds)
		{
			ICollection<ContainerId> removedContainers = new HashSet<ContainerId>();
			Sharpen.Collections.AddAll(pendingContainersToRemove, containerIds);
			IEnumerator<ContainerId> iter = pendingContainersToRemove.GetEnumerator();
			while (iter.HasNext())
			{
				ContainerId containerId = iter.Next();
				// remove the container only if the container is at DONE state
				Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container nmContainer
					 = context.GetContainers()[containerId];
				if (nmContainer == null)
				{
					iter.Remove();
				}
				else
				{
					if (nmContainer.GetContainerState().Equals(ContainerState.Done))
					{
						Sharpen.Collections.Remove(context.GetContainers(), containerId);
						removedContainers.AddItem(containerId);
						iter.Remove();
					}
				}
			}
			if (!removedContainers.IsEmpty())
			{
				Log.Info("Removed completed containers from NM context: " + removedContainers);
			}
			pendingCompletedContainers.Clear();
		}

		private void TrackAppsForKeepAlive(IList<ApplicationId> appIds)
		{
			if (tokenKeepAliveEnabled && appIds != null && appIds.Count > 0)
			{
				foreach (ApplicationId appId in appIds)
				{
					TrackAppForKeepAlive(appId);
				}
			}
		}

		private void TrackAppForKeepAlive(ApplicationId appId)
		{
			// Next keepAlive request for app between 0.7 & 0.9 of when the token will
			// likely expire.
			long nextTime = Runtime.CurrentTimeMillis() + (long)(0.7 * tokenRemovalDelayMs + 
				(0.2 * tokenRemovalDelayMs * keepAliveDelayRandom.Next(100)) / 100);
			appTokenKeepAliveMap[appId] = nextTime;
		}

		public virtual void SendOutofBandHeartBeat()
		{
			lock (this.heartbeatMonitor)
			{
				Sharpen.Runtime.Notify(this.heartbeatMonitor);
			}
		}

		public virtual bool IsContainerRecentlyStopped(ContainerId containerId)
		{
			lock (recentlyStoppedContainers)
			{
				return recentlyStoppedContainers.Contains(containerId);
			}
		}

		public virtual void ClearFinishedContainersFromCache()
		{
			lock (recentlyStoppedContainers)
			{
				recentlyStoppedContainers.Clear();
			}
		}

		[InterfaceAudience.Private]
		[VisibleForTesting]
		public virtual void RemoveVeryOldStoppedContainersFromCache()
		{
			lock (recentlyStoppedContainers)
			{
				long currentTime = Runtime.CurrentTimeMillis();
				IEnumerator<ContainerId> i = recentlyStoppedContainers.Keys.GetEnumerator();
				while (i.HasNext())
				{
					ContainerId cid = i.Next();
					if (recentlyStoppedContainers[cid] < currentTime)
					{
						if (!context.GetContainers().Contains(cid))
						{
							i.Remove();
							try
							{
								context.GetNMStateStore().RemoveContainer(cid);
							}
							catch (IOException e)
							{
								Log.Error("Unable to remove container " + cid + " in store", e);
							}
						}
					}
					else
					{
						break;
					}
				}
			}
		}

		public virtual long GetRMIdentifier()
		{
			return this.rmIdentifier;
		}

		/// <exception cref="System.IO.IOException"/>
		private static IDictionary<ApplicationId, Credentials> ParseCredentials(IDictionary
			<ApplicationId, ByteBuffer> systemCredentials)
		{
			IDictionary<ApplicationId, Credentials> map = new Dictionary<ApplicationId, Credentials
				>();
			foreach (KeyValuePair<ApplicationId, ByteBuffer> entry in systemCredentials)
			{
				Credentials credentials = new Credentials();
				DataInputByteBuffer buf = new DataInputByteBuffer();
				ByteBuffer buffer = entry.Value;
				buffer.Rewind();
				buf.Reset(buffer);
				credentials.ReadTokenStorageStream(buf);
				map[entry.Key] = credentials;
			}
			if (Log.IsDebugEnabled())
			{
				foreach (KeyValuePair<ApplicationId, Credentials> entry_1 in map)
				{
					Log.Debug("Retrieved credentials form RM for " + entry_1.Key + ": " + entry_1.Value
						.GetAllTokens());
				}
			}
			return map;
		}

		protected internal virtual void StartStatusUpdater()
		{
			statusUpdaterRunnable = new _Runnable_579(this);
			// Send heartbeat
			//get next heartbeat interval from response
			// Invalidate the RMIdentifier while resync
			// Explicitly put this method after checking the resync response. We
			// don't want to remove the completed containers before resync
			// because these completed containers will be reported back to RM
			// when NM re-registers with RM.
			// Only remove the cleanedup containers that are acked
			//Only start tracking for keepAlive on FINISH_APP
			//catch and throw the exception if tried MAX wait time to connect RM
			// TODO Better error handling. Thread can die with the rest of the
			// NM still running.
			// Do Nothing
			// See if the master-key has rolled over
			// Will be non-null only on roll-over on RM side
			statusUpdater = new Sharpen.Thread(statusUpdaterRunnable, "Node Status Updater");
			statusUpdater.Start();
		}

		private sealed class _Runnable_579 : Runnable
		{
			public _Runnable_579(NodeStatusUpdaterImpl _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public void Run()
			{
				int lastHeartBeatID = 0;
				while (!this._enclosing.isStopped)
				{
					try
					{
						NodeHeartbeatResponse response = null;
						NodeStatus nodeStatus = this._enclosing.GetNodeStatus(lastHeartBeatID);
						NodeHeartbeatRequest request = NodeHeartbeatRequest.NewInstance(nodeStatus, this.
							_enclosing.context.GetContainerTokenSecretManager().GetCurrentKey(), this._enclosing
							.context.GetNMTokenSecretManager().GetCurrentKey());
						response = this._enclosing.resourceTracker.NodeHeartbeat(request);
						this._enclosing.nextHeartBeatInterval = response.GetNextHeartBeatInterval();
						this.UpdateMasterKeys(response);
						if (response.GetNodeAction() == NodeAction.Shutdown)
						{
							Org.Apache.Hadoop.Yarn.Server.Nodemanager.NodeStatusUpdaterImpl.Log.Warn("Recieved SHUTDOWN signal from Resourcemanager as part of heartbeat,"
								 + " hence shutting down.");
							Org.Apache.Hadoop.Yarn.Server.Nodemanager.NodeStatusUpdaterImpl.Log.Warn("Message from ResourceManager: "
								 + response.GetDiagnosticsMessage());
							this._enclosing.context.SetDecommissioned(true);
							this._enclosing.dispatcher.GetEventHandler().Handle(new NodeManagerEvent(NodeManagerEventType
								.Shutdown));
							break;
						}
						if (response.GetNodeAction() == NodeAction.Resync)
						{
							Org.Apache.Hadoop.Yarn.Server.Nodemanager.NodeStatusUpdaterImpl.Log.Warn("Node is out of sync with ResourceManager,"
								 + " hence resyncing.");
							Org.Apache.Hadoop.Yarn.Server.Nodemanager.NodeStatusUpdaterImpl.Log.Warn("Message from ResourceManager: "
								 + response.GetDiagnosticsMessage());
							this._enclosing.rmIdentifier = ResourceManagerConstants.RmInvalidIdentifier;
							this._enclosing.dispatcher.GetEventHandler().Handle(new NodeManagerEvent(NodeManagerEventType
								.Resync));
							this._enclosing.pendingCompletedContainers.Clear();
							break;
						}
						this._enclosing.RemoveOrTrackCompletedContainersFromContext(response.GetContainersToBeRemovedFromNM
							());
						lastHeartBeatID = response.GetResponseId();
						IList<ContainerId> containersToCleanup = response.GetContainersToCleanup();
						if (!containersToCleanup.IsEmpty())
						{
							this._enclosing.dispatcher.GetEventHandler().Handle(new CMgrCompletedContainersEvent
								(containersToCleanup, CMgrCompletedContainersEvent.Reason.ByResourcemanager));
						}
						IList<ApplicationId> appsToCleanup = response.GetApplicationsToCleanup();
						this._enclosing.TrackAppsForKeepAlive(appsToCleanup);
						if (!appsToCleanup.IsEmpty())
						{
							this._enclosing.dispatcher.GetEventHandler().Handle(new CMgrCompletedAppsEvent(appsToCleanup
								, CMgrCompletedAppsEvent.Reason.ByResourcemanager));
						}
						IDictionary<ApplicationId, ByteBuffer> systemCredentials = response.GetSystemCredentialsForApps
							();
						if (systemCredentials != null && !systemCredentials.IsEmpty())
						{
							((NodeManager.NMContext)this._enclosing.context).SetSystemCrendentialsForApps(Org.Apache.Hadoop.Yarn.Server.Nodemanager.NodeStatusUpdaterImpl
								.ParseCredentials(systemCredentials));
						}
					}
					catch (ConnectException e)
					{
						this._enclosing.dispatcher.GetEventHandler().Handle(new NodeManagerEvent(NodeManagerEventType
							.Shutdown));
						throw new YarnRuntimeException(e);
					}
					catch (Exception e)
					{
						Org.Apache.Hadoop.Yarn.Server.Nodemanager.NodeStatusUpdaterImpl.Log.Error("Caught exception in status-updater"
							, e);
					}
					finally
					{
						lock (this._enclosing.heartbeatMonitor)
						{
							this._enclosing.nextHeartBeatInterval = this._enclosing.nextHeartBeatInterval <= 
								0 ? YarnConfiguration.DefaultRmNmHeartbeatIntervalMs : this._enclosing.nextHeartBeatInterval;
							try
							{
								Sharpen.Runtime.Wait(this._enclosing.heartbeatMonitor, this._enclosing.nextHeartBeatInterval
									);
							}
							catch (Exception)
							{
							}
						}
					}
				}
			}

			private void UpdateMasterKeys(NodeHeartbeatResponse response)
			{
				MasterKey updatedMasterKey = response.GetContainerTokenMasterKey();
				if (updatedMasterKey != null)
				{
					this._enclosing.context.GetContainerTokenSecretManager().SetMasterKey(updatedMasterKey
						);
				}
				updatedMasterKey = response.GetNMTokenMasterKey();
				if (updatedMasterKey != null)
				{
					this._enclosing.context.GetNMTokenSecretManager().SetMasterKey(updatedMasterKey);
				}
			}

			private readonly NodeStatusUpdaterImpl _enclosing;
		}
	}
}
