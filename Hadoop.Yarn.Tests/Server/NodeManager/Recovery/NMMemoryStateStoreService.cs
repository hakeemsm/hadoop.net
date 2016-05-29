using System.Collections.Generic;
using System.IO;
using System.Text;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Proto;
using Org.Apache.Hadoop.Yarn.Server.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Api.Records.Impl.PB;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Recovery
{
	public class NMMemoryStateStoreService : NMStateStoreService
	{
		private IDictionary<ApplicationId, YarnServerNodemanagerRecoveryProtos.ContainerManagerApplicationProto
			> apps;

		private ICollection<ApplicationId> finishedApps;

		private IDictionary<ContainerId, NMStateStoreService.RecoveredContainerState> containerStates;

		private IDictionary<NMMemoryStateStoreService.TrackerKey, NMMemoryStateStoreService.TrackerState
			> trackerStates;

		private IDictionary<int, YarnServerNodemanagerRecoveryProtos.DeletionServiceDeleteTaskProto
			> deleteTasks;

		private NMStateStoreService.RecoveredNMTokensState nmTokenState;

		private NMStateStoreService.RecoveredContainerTokensState containerTokenState;

		private IDictionary<ApplicationId, YarnServerNodemanagerRecoveryProtos.LogDeleterProto
			> logDeleterState;

		public NMMemoryStateStoreService()
			: base(typeof(Org.Apache.Hadoop.Yarn.Server.Nodemanager.Recovery.NMMemoryStateStoreService
				).FullName)
		{
		}

		protected internal override void InitStorage(Configuration conf)
		{
			apps = new Dictionary<ApplicationId, YarnServerNodemanagerRecoveryProtos.ContainerManagerApplicationProto
				>();
			finishedApps = new HashSet<ApplicationId>();
			containerStates = new Dictionary<ContainerId, NMStateStoreService.RecoveredContainerState
				>();
			nmTokenState = new NMStateStoreService.RecoveredNMTokensState();
			nmTokenState.applicationMasterKeys = new Dictionary<ApplicationAttemptId, MasterKey
				>();
			containerTokenState = new NMStateStoreService.RecoveredContainerTokensState();
			containerTokenState.activeTokens = new Dictionary<ContainerId, long>();
			trackerStates = new Dictionary<NMMemoryStateStoreService.TrackerKey, NMMemoryStateStoreService.TrackerState
				>();
			deleteTasks = new Dictionary<int, YarnServerNodemanagerRecoveryProtos.DeletionServiceDeleteTaskProto
				>();
			logDeleterState = new Dictionary<ApplicationId, YarnServerNodemanagerRecoveryProtos.LogDeleterProto
				>();
		}

		protected internal override void StartStorage()
		{
		}

		protected internal override void CloseStorage()
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public override NMStateStoreService.RecoveredApplicationsState LoadApplicationsState
			()
		{
			lock (this)
			{
				NMStateStoreService.RecoveredApplicationsState state = new NMStateStoreService.RecoveredApplicationsState
					();
				state.applications = new AList<YarnServerNodemanagerRecoveryProtos.ContainerManagerApplicationProto
					>(apps.Values);
				state.finishedApplications = new AList<ApplicationId>(finishedApps);
				return state;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void StoreApplication(ApplicationId appId, YarnServerNodemanagerRecoveryProtos.ContainerManagerApplicationProto
			 proto)
		{
			lock (this)
			{
				YarnServerNodemanagerRecoveryProtos.ContainerManagerApplicationProto protoCopy = 
					YarnServerNodemanagerRecoveryProtos.ContainerManagerApplicationProto.ParseFrom(proto
					.ToByteString());
				apps[appId] = protoCopy;
			}
		}

		public override void StoreFinishedApplication(ApplicationId appId)
		{
			lock (this)
			{
				finishedApps.AddItem(appId);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void RemoveApplication(ApplicationId appId)
		{
			lock (this)
			{
				Sharpen.Collections.Remove(apps, appId);
				finishedApps.Remove(appId);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override IList<NMStateStoreService.RecoveredContainerState> LoadContainersState
			()
		{
			lock (this)
			{
				// return a copy so caller can't modify our state
				IList<NMStateStoreService.RecoveredContainerState> result = new AList<NMStateStoreService.RecoveredContainerState
					>(containerStates.Count);
				foreach (NMStateStoreService.RecoveredContainerState rcs in containerStates.Values)
				{
					NMStateStoreService.RecoveredContainerState rcsCopy = new NMStateStoreService.RecoveredContainerState
						();
					rcsCopy.status = rcs.status;
					rcsCopy.exitCode = rcs.exitCode;
					rcsCopy.killed = rcs.killed;
					rcsCopy.diagnostics = rcs.diagnostics;
					rcsCopy.startRequest = rcs.startRequest;
					result.AddItem(rcsCopy);
				}
				return new AList<NMStateStoreService.RecoveredContainerState>();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void StoreContainer(ContainerId containerId, StartContainerRequest
			 startRequest)
		{
			lock (this)
			{
				NMStateStoreService.RecoveredContainerState rcs = new NMStateStoreService.RecoveredContainerState
					();
				rcs.startRequest = startRequest;
				containerStates[containerId] = rcs;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void StoreContainerDiagnostics(ContainerId containerId, StringBuilder
			 diagnostics)
		{
			lock (this)
			{
				NMStateStoreService.RecoveredContainerState rcs = GetRecoveredContainerState(containerId
					);
				rcs.diagnostics = diagnostics.ToString();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void StoreContainerLaunched(ContainerId containerId)
		{
			lock (this)
			{
				NMStateStoreService.RecoveredContainerState rcs = GetRecoveredContainerState(containerId
					);
				if (rcs.exitCode != ContainerExitStatus.Invalid)
				{
					throw new IOException("Container already completed");
				}
				rcs.status = NMStateStoreService.RecoveredContainerStatus.Launched;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void StoreContainerKilled(ContainerId containerId)
		{
			lock (this)
			{
				NMStateStoreService.RecoveredContainerState rcs = GetRecoveredContainerState(containerId
					);
				rcs.killed = true;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void StoreContainerCompleted(ContainerId containerId, int exitCode
			)
		{
			lock (this)
			{
				NMStateStoreService.RecoveredContainerState rcs = GetRecoveredContainerState(containerId
					);
				rcs.status = NMStateStoreService.RecoveredContainerStatus.Completed;
				rcs.exitCode = exitCode;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void RemoveContainer(ContainerId containerId)
		{
			lock (this)
			{
				Sharpen.Collections.Remove(containerStates, containerId);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private NMStateStoreService.RecoveredContainerState GetRecoveredContainerState(ContainerId
			 containerId)
		{
			NMStateStoreService.RecoveredContainerState rcs = containerStates[containerId];
			if (rcs == null)
			{
				throw new IOException("No start request for " + containerId);
			}
			return rcs;
		}

		private NMStateStoreService.LocalResourceTrackerState LoadTrackerState(NMMemoryStateStoreService.TrackerState
			 ts)
		{
			NMStateStoreService.LocalResourceTrackerState result = new NMStateStoreService.LocalResourceTrackerState
				();
			Sharpen.Collections.AddAll(result.localizedResources, ts.localizedResources.Values
				);
			foreach (KeyValuePair<Path, YarnProtos.LocalResourceProto> entry in ts.inProgressMap)
			{
				result.inProgressResources[entry.Value] = entry.Key;
			}
			return result;
		}

		private NMMemoryStateStoreService.TrackerState GetTrackerState(NMMemoryStateStoreService.TrackerKey
			 key)
		{
			NMMemoryStateStoreService.TrackerState ts = trackerStates[key];
			if (ts == null)
			{
				ts = new NMMemoryStateStoreService.TrackerState();
				trackerStates[key] = ts;
			}
			return ts;
		}

		public override NMStateStoreService.RecoveredLocalizationState LoadLocalizationState
			()
		{
			lock (this)
			{
				NMStateStoreService.RecoveredLocalizationState result = new NMStateStoreService.RecoveredLocalizationState
					();
				foreach (KeyValuePair<NMMemoryStateStoreService.TrackerKey, NMMemoryStateStoreService.TrackerState
					> e in trackerStates)
				{
					NMMemoryStateStoreService.TrackerKey tk = e.Key;
					NMMemoryStateStoreService.TrackerState ts = e.Value;
					// check what kind of tracker state we have and recover appropriately
					// public trackers have user == null
					// private trackers have a valid user but appId == null
					// app-specific trackers have a valid user and valid appId
					if (tk.user == null)
					{
						result.publicTrackerState = LoadTrackerState(ts);
					}
					else
					{
						NMStateStoreService.RecoveredUserResources rur = result.userResources[tk.user];
						if (rur == null)
						{
							rur = new NMStateStoreService.RecoveredUserResources();
							result.userResources[tk.user] = rur;
						}
						if (tk.appId == null)
						{
							rur.privateTrackerState = LoadTrackerState(ts);
						}
						else
						{
							rur.appTrackerStates[tk.appId] = LoadTrackerState(ts);
						}
					}
				}
				return result;
			}
		}

		public override void StartResourceLocalization(string user, ApplicationId appId, 
			YarnProtos.LocalResourceProto proto, Path localPath)
		{
			lock (this)
			{
				NMMemoryStateStoreService.TrackerState ts = GetTrackerState(new NMMemoryStateStoreService.TrackerKey
					(user, appId));
				ts.inProgressMap[localPath] = proto;
			}
		}

		public override void FinishResourceLocalization(string user, ApplicationId appId, 
			YarnServerNodemanagerRecoveryProtos.LocalizedResourceProto proto)
		{
			lock (this)
			{
				NMMemoryStateStoreService.TrackerState ts = GetTrackerState(new NMMemoryStateStoreService.TrackerKey
					(user, appId));
				Path localPath = new Path(proto.GetLocalPath());
				Sharpen.Collections.Remove(ts.inProgressMap, localPath);
				ts.localizedResources[localPath] = proto;
			}
		}

		public override void RemoveLocalizedResource(string user, ApplicationId appId, Path
			 localPath)
		{
			lock (this)
			{
				NMMemoryStateStoreService.TrackerState ts = trackerStates[new NMMemoryStateStoreService.TrackerKey
					(user, appId)];
				if (ts != null)
				{
					Sharpen.Collections.Remove(ts.inProgressMap, localPath);
					Sharpen.Collections.Remove(ts.localizedResources, localPath);
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override NMStateStoreService.RecoveredDeletionServiceState LoadDeletionServiceState
			()
		{
			lock (this)
			{
				NMStateStoreService.RecoveredDeletionServiceState result = new NMStateStoreService.RecoveredDeletionServiceState
					();
				result.tasks = new AList<YarnServerNodemanagerRecoveryProtos.DeletionServiceDeleteTaskProto
					>(deleteTasks.Values);
				return result;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void StoreDeletionTask(int taskId, YarnServerNodemanagerRecoveryProtos.DeletionServiceDeleteTaskProto
			 taskProto)
		{
			lock (this)
			{
				deleteTasks[taskId] = taskProto;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void RemoveDeletionTask(int taskId)
		{
			lock (this)
			{
				Sharpen.Collections.Remove(deleteTasks, taskId);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override NMStateStoreService.RecoveredNMTokensState LoadNMTokensState()
		{
			lock (this)
			{
				// return a copy so caller can't modify our state
				NMStateStoreService.RecoveredNMTokensState result = new NMStateStoreService.RecoveredNMTokensState
					();
				result.currentMasterKey = nmTokenState.currentMasterKey;
				result.previousMasterKey = nmTokenState.previousMasterKey;
				result.applicationMasterKeys = new Dictionary<ApplicationAttemptId, MasterKey>(nmTokenState
					.applicationMasterKeys);
				return result;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void StoreNMTokenCurrentMasterKey(MasterKey key)
		{
			lock (this)
			{
				MasterKeyPBImpl keypb = (MasterKeyPBImpl)key;
				nmTokenState.currentMasterKey = new MasterKeyPBImpl(keypb.GetProto());
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void StoreNMTokenPreviousMasterKey(MasterKey key)
		{
			lock (this)
			{
				MasterKeyPBImpl keypb = (MasterKeyPBImpl)key;
				nmTokenState.previousMasterKey = new MasterKeyPBImpl(keypb.GetProto());
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void StoreNMTokenApplicationMasterKey(ApplicationAttemptId attempt
			, MasterKey key)
		{
			lock (this)
			{
				MasterKeyPBImpl keypb = (MasterKeyPBImpl)key;
				nmTokenState.applicationMasterKeys[attempt] = new MasterKeyPBImpl(keypb.GetProto(
					));
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void RemoveNMTokenApplicationMasterKey(ApplicationAttemptId attempt
			)
		{
			lock (this)
			{
				Sharpen.Collections.Remove(nmTokenState.applicationMasterKeys, attempt);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override NMStateStoreService.RecoveredContainerTokensState LoadContainerTokensState
			()
		{
			lock (this)
			{
				// return a copy so caller can't modify our state
				NMStateStoreService.RecoveredContainerTokensState result = new NMStateStoreService.RecoveredContainerTokensState
					();
				result.currentMasterKey = containerTokenState.currentMasterKey;
				result.previousMasterKey = containerTokenState.previousMasterKey;
				result.activeTokens = new Dictionary<ContainerId, long>(containerTokenState.activeTokens
					);
				return result;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void StoreContainerTokenCurrentMasterKey(MasterKey key)
		{
			lock (this)
			{
				MasterKeyPBImpl keypb = (MasterKeyPBImpl)key;
				containerTokenState.currentMasterKey = new MasterKeyPBImpl(keypb.GetProto());
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void StoreContainerTokenPreviousMasterKey(MasterKey key)
		{
			lock (this)
			{
				MasterKeyPBImpl keypb = (MasterKeyPBImpl)key;
				containerTokenState.previousMasterKey = new MasterKeyPBImpl(keypb.GetProto());
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void StoreContainerToken(ContainerId containerId, long expirationTime
			)
		{
			lock (this)
			{
				containerTokenState.activeTokens[containerId] = expirationTime;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void RemoveContainerToken(ContainerId containerId)
		{
			lock (this)
			{
				Sharpen.Collections.Remove(containerTokenState.activeTokens, containerId);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override NMStateStoreService.RecoveredLogDeleterState LoadLogDeleterState(
			)
		{
			lock (this)
			{
				NMStateStoreService.RecoveredLogDeleterState state = new NMStateStoreService.RecoveredLogDeleterState
					();
				state.logDeleterMap = new Dictionary<ApplicationId, YarnServerNodemanagerRecoveryProtos.LogDeleterProto
					>(logDeleterState);
				return state;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void StoreLogDeleter(ApplicationId appId, YarnServerNodemanagerRecoveryProtos.LogDeleterProto
			 proto)
		{
			lock (this)
			{
				logDeleterState[appId] = proto;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void RemoveLogDeleter(ApplicationId appId)
		{
			lock (this)
			{
				Sharpen.Collections.Remove(logDeleterState, appId);
			}
		}

		private class TrackerState
		{
			internal IDictionary<Path, YarnProtos.LocalResourceProto> inProgressMap = new Dictionary
				<Path, YarnProtos.LocalResourceProto>();

			internal IDictionary<Path, YarnServerNodemanagerRecoveryProtos.LocalizedResourceProto
				> localizedResources = new Dictionary<Path, YarnServerNodemanagerRecoveryProtos.LocalizedResourceProto
				>();
		}

		private class TrackerKey
		{
			internal string user;

			internal ApplicationId appId;

			public TrackerKey(string user, ApplicationId appId)
			{
				this.user = user;
				this.appId = appId;
			}

			public override int GetHashCode()
			{
				int prime = 31;
				int result = 1;
				result = prime * result + ((appId == null) ? 0 : appId.GetHashCode());
				result = prime * result + ((user == null) ? 0 : user.GetHashCode());
				return result;
			}

			public override bool Equals(object obj)
			{
				if (this == obj)
				{
					return true;
				}
				if (obj == null)
				{
					return false;
				}
				if (!(obj is NMMemoryStateStoreService.TrackerKey))
				{
					return false;
				}
				NMMemoryStateStoreService.TrackerKey other = (NMMemoryStateStoreService.TrackerKey
					)obj;
				if (appId == null)
				{
					if (other.appId != null)
					{
						return false;
					}
				}
				else
				{
					if (!appId.Equals(other.appId))
					{
						return false;
					}
				}
				if (user == null)
				{
					if (other.user != null)
					{
						return false;
					}
				}
				else
				{
					if (!user.Equals(other.user))
					{
						return false;
					}
				}
				return true;
			}
		}
	}
}
