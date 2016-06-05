using System.Collections.Generic;
using System.Text;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Proto;
using Org.Apache.Hadoop.Yarn.Server.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Recovery
{
	public abstract class NMStateStoreService : AbstractService
	{
		public NMStateStoreService(string name)
			: base(name)
		{
		}

		public class RecoveredApplicationsState
		{
			internal IList<YarnServerNodemanagerRecoveryProtos.ContainerManagerApplicationProto
				> applications;

			internal IList<ApplicationId> finishedApplications;

			public virtual IList<YarnServerNodemanagerRecoveryProtos.ContainerManagerApplicationProto
				> GetApplications()
			{
				return applications;
			}

			public virtual IList<ApplicationId> GetFinishedApplications()
			{
				return finishedApplications;
			}
		}

		public enum RecoveredContainerStatus
		{
			Requested,
			Launched,
			Completed
		}

		public class RecoveredContainerState
		{
			internal NMStateStoreService.RecoveredContainerStatus status;

			internal int exitCode = ContainerExitStatus.Invalid;

			internal bool killed = false;

			internal string diagnostics = string.Empty;

			internal StartContainerRequest startRequest;

			public virtual NMStateStoreService.RecoveredContainerStatus GetStatus()
			{
				return status;
			}

			public virtual int GetExitCode()
			{
				return exitCode;
			}

			public virtual bool GetKilled()
			{
				return killed;
			}

			public virtual string GetDiagnostics()
			{
				return diagnostics;
			}

			public virtual StartContainerRequest GetStartRequest()
			{
				return startRequest;
			}
		}

		public class LocalResourceTrackerState
		{
			internal IList<YarnServerNodemanagerRecoveryProtos.LocalizedResourceProto> localizedResources
				 = new AList<YarnServerNodemanagerRecoveryProtos.LocalizedResourceProto>();

			internal IDictionary<YarnProtos.LocalResourceProto, Path> inProgressResources = new 
				Dictionary<YarnProtos.LocalResourceProto, Path>();

			public virtual IList<YarnServerNodemanagerRecoveryProtos.LocalizedResourceProto> 
				GetLocalizedResources()
			{
				return localizedResources;
			}

			public virtual IDictionary<YarnProtos.LocalResourceProto, Path> GetInProgressResources
				()
			{
				return inProgressResources;
			}

			public virtual bool IsEmpty()
			{
				return localizedResources.IsEmpty() && inProgressResources.IsEmpty();
			}
		}

		public class RecoveredUserResources
		{
			internal NMStateStoreService.LocalResourceTrackerState privateTrackerState = new 
				NMStateStoreService.LocalResourceTrackerState();

			internal IDictionary<ApplicationId, NMStateStoreService.LocalResourceTrackerState
				> appTrackerStates = new Dictionary<ApplicationId, NMStateStoreService.LocalResourceTrackerState
				>();

			public virtual NMStateStoreService.LocalResourceTrackerState GetPrivateTrackerState
				()
			{
				return privateTrackerState;
			}

			public virtual IDictionary<ApplicationId, NMStateStoreService.LocalResourceTrackerState
				> GetAppTrackerStates()
			{
				return appTrackerStates;
			}
		}

		public class RecoveredLocalizationState
		{
			internal NMStateStoreService.LocalResourceTrackerState publicTrackerState = new NMStateStoreService.LocalResourceTrackerState
				();

			internal IDictionary<string, NMStateStoreService.RecoveredUserResources> userResources
				 = new Dictionary<string, NMStateStoreService.RecoveredUserResources>();

			public virtual NMStateStoreService.LocalResourceTrackerState GetPublicTrackerState
				()
			{
				return publicTrackerState;
			}

			public virtual IDictionary<string, NMStateStoreService.RecoveredUserResources> GetUserResources
				()
			{
				return userResources;
			}
		}

		public class RecoveredDeletionServiceState
		{
			internal IList<YarnServerNodemanagerRecoveryProtos.DeletionServiceDeleteTaskProto
				> tasks;

			public virtual IList<YarnServerNodemanagerRecoveryProtos.DeletionServiceDeleteTaskProto
				> GetTasks()
			{
				return tasks;
			}
		}

		public class RecoveredNMTokensState
		{
			internal MasterKey currentMasterKey;

			internal MasterKey previousMasterKey;

			internal IDictionary<ApplicationAttemptId, MasterKey> applicationMasterKeys;

			public virtual MasterKey GetCurrentMasterKey()
			{
				return currentMasterKey;
			}

			public virtual MasterKey GetPreviousMasterKey()
			{
				return previousMasterKey;
			}

			public virtual IDictionary<ApplicationAttemptId, MasterKey> GetApplicationMasterKeys
				()
			{
				return applicationMasterKeys;
			}
		}

		public class RecoveredContainerTokensState
		{
			internal MasterKey currentMasterKey;

			internal MasterKey previousMasterKey;

			internal IDictionary<ContainerId, long> activeTokens;

			public virtual MasterKey GetCurrentMasterKey()
			{
				return currentMasterKey;
			}

			public virtual MasterKey GetPreviousMasterKey()
			{
				return previousMasterKey;
			}

			public virtual IDictionary<ContainerId, long> GetActiveTokens()
			{
				return activeTokens;
			}
		}

		public class RecoveredLogDeleterState
		{
			internal IDictionary<ApplicationId, YarnServerNodemanagerRecoveryProtos.LogDeleterProto
				> logDeleterMap;

			public virtual IDictionary<ApplicationId, YarnServerNodemanagerRecoveryProtos.LogDeleterProto
				> GetLogDeleterMap()
			{
				return logDeleterMap;
			}
		}

		/// <summary>Initialize the state storage</summary>
		/// <exception cref="System.IO.IOException"/>
		protected override void ServiceInit(Configuration conf)
		{
			InitStorage(conf);
		}

		/// <summary>Start the state storage for use</summary>
		/// <exception cref="System.IO.IOException"/>
		protected override void ServiceStart()
		{
			StartStorage();
		}

		/// <summary>Shutdown the state storage.</summary>
		/// <exception cref="System.IO.IOException"/>
		protected override void ServiceStop()
		{
			CloseStorage();
		}

		public virtual bool CanRecover()
		{
			return true;
		}

		public virtual bool IsNewlyCreated()
		{
			return false;
		}

		/// <summary>Load the state of applications</summary>
		/// <returns>recovered state for applications</returns>
		/// <exception cref="System.IO.IOException"/>
		public abstract NMStateStoreService.RecoveredApplicationsState LoadApplicationsState
			();

		/// <summary>Record the start of an application</summary>
		/// <param name="appId">the application ID</param>
		/// <param name="p">state to store for the application</param>
		/// <exception cref="System.IO.IOException"/>
		public abstract void StoreApplication(ApplicationId appId, YarnServerNodemanagerRecoveryProtos.ContainerManagerApplicationProto
			 p);

		/// <summary>Record that an application has finished</summary>
		/// <param name="appId">the application ID</param>
		/// <exception cref="System.IO.IOException"/>
		public abstract void StoreFinishedApplication(ApplicationId appId);

		/// <summary>Remove records corresponding to an application</summary>
		/// <param name="appId">the application ID</param>
		/// <exception cref="System.IO.IOException"/>
		public abstract void RemoveApplication(ApplicationId appId);

		/// <summary>Load the state of containers</summary>
		/// <returns>recovered state for containers</returns>
		/// <exception cref="System.IO.IOException"/>
		public abstract IList<NMStateStoreService.RecoveredContainerState> LoadContainersState
			();

		/// <summary>Record a container start request</summary>
		/// <param name="containerId">the container ID</param>
		/// <param name="startRequest">the container start request</param>
		/// <exception cref="System.IO.IOException"/>
		public abstract void StoreContainer(ContainerId containerId, StartContainerRequest
			 startRequest);

		/// <summary>Record that a container has been launched</summary>
		/// <param name="containerId">the container ID</param>
		/// <exception cref="System.IO.IOException"/>
		public abstract void StoreContainerLaunched(ContainerId containerId);

		/// <summary>Record that a container has completed</summary>
		/// <param name="containerId">the container ID</param>
		/// <param name="exitCode">the exit code from the container</param>
		/// <exception cref="System.IO.IOException"/>
		public abstract void StoreContainerCompleted(ContainerId containerId, int exitCode
			);

		/// <summary>Record a request to kill a container</summary>
		/// <param name="containerId">the container ID</param>
		/// <exception cref="System.IO.IOException"/>
		public abstract void StoreContainerKilled(ContainerId containerId);

		/// <summary>Record diagnostics for a container</summary>
		/// <param name="containerId">the container ID</param>
		/// <param name="diagnostics">the container diagnostics</param>
		/// <exception cref="System.IO.IOException"/>
		public abstract void StoreContainerDiagnostics(ContainerId containerId, StringBuilder
			 diagnostics);

		/// <summary>Remove records corresponding to a container</summary>
		/// <param name="containerId">the container ID</param>
		/// <exception cref="System.IO.IOException"/>
		public abstract void RemoveContainer(ContainerId containerId);

		/// <summary>Load the state of localized resources</summary>
		/// <returns>recovered localized resource state</returns>
		/// <exception cref="System.IO.IOException"/>
		public abstract NMStateStoreService.RecoveredLocalizationState LoadLocalizationState
			();

		/// <summary>Record the start of localization for a resource</summary>
		/// <param name="user">the username or null if the resource is public</param>
		/// <param name="appId">the application ID if the resource is app-specific or null</param>
		/// <param name="proto">the resource request</param>
		/// <param name="localPath">local filesystem path where the resource will be stored</param>
		/// <exception cref="System.IO.IOException"/>
		public abstract void StartResourceLocalization(string user, ApplicationId appId, 
			YarnProtos.LocalResourceProto proto, Path localPath);

		/// <summary>Record the completion of a resource localization</summary>
		/// <param name="user">the username or null if the resource is public</param>
		/// <param name="appId">the application ID if the resource is app-specific or null</param>
		/// <param name="proto">the serialized localized resource</param>
		/// <exception cref="System.IO.IOException"/>
		public abstract void FinishResourceLocalization(string user, ApplicationId appId, 
			YarnServerNodemanagerRecoveryProtos.LocalizedResourceProto proto);

		/// <summary>Remove records related to a resource localization</summary>
		/// <param name="user">the username or null if the resource is public</param>
		/// <param name="appId">the application ID if the resource is app-specific or null</param>
		/// <param name="localPath">local filesystem path where the resource will be stored</param>
		/// <exception cref="System.IO.IOException"/>
		public abstract void RemoveLocalizedResource(string user, ApplicationId appId, Path
			 localPath);

		/// <summary>Load the state of the deletion service</summary>
		/// <returns>recovered deletion service state</returns>
		/// <exception cref="System.IO.IOException"/>
		public abstract NMStateStoreService.RecoveredDeletionServiceState LoadDeletionServiceState
			();

		/// <summary>Record a deletion task</summary>
		/// <param name="taskId">the deletion task ID</param>
		/// <param name="taskProto">the deletion task protobuf</param>
		/// <exception cref="System.IO.IOException"/>
		public abstract void StoreDeletionTask(int taskId, YarnServerNodemanagerRecoveryProtos.DeletionServiceDeleteTaskProto
			 taskProto);

		/// <summary>Remove records corresponding to a deletion task</summary>
		/// <param name="taskId">the deletion task ID</param>
		/// <exception cref="System.IO.IOException"/>
		public abstract void RemoveDeletionTask(int taskId);

		/// <summary>Load the state of NM tokens</summary>
		/// <returns>recovered state of NM tokens</returns>
		/// <exception cref="System.IO.IOException"/>
		public abstract NMStateStoreService.RecoveredNMTokensState LoadNMTokensState();

		/// <summary>Record the current NM token master key</summary>
		/// <param name="key">the master key</param>
		/// <exception cref="System.IO.IOException"/>
		public abstract void StoreNMTokenCurrentMasterKey(MasterKey key);

		/// <summary>Record the previous NM token master key</summary>
		/// <param name="key">the previous master key</param>
		/// <exception cref="System.IO.IOException"/>
		public abstract void StoreNMTokenPreviousMasterKey(MasterKey key);

		/// <summary>Record a master key corresponding to an application</summary>
		/// <param name="attempt">the application attempt ID</param>
		/// <param name="key">the master key</param>
		/// <exception cref="System.IO.IOException"/>
		public abstract void StoreNMTokenApplicationMasterKey(ApplicationAttemptId attempt
			, MasterKey key);

		/// <summary>Remove a master key corresponding to an application</summary>
		/// <param name="attempt">the application attempt ID</param>
		/// <exception cref="System.IO.IOException"/>
		public abstract void RemoveNMTokenApplicationMasterKey(ApplicationAttemptId attempt
			);

		/// <summary>Load the state of container tokens</summary>
		/// <returns>recovered state of container tokens</returns>
		/// <exception cref="System.IO.IOException"/>
		public abstract NMStateStoreService.RecoveredContainerTokensState LoadContainerTokensState
			();

		/// <summary>Record the current container token master key</summary>
		/// <param name="key">the master key</param>
		/// <exception cref="System.IO.IOException"/>
		public abstract void StoreContainerTokenCurrentMasterKey(MasterKey key);

		/// <summary>Record the previous container token master key</summary>
		/// <param name="key">the previous master key</param>
		/// <exception cref="System.IO.IOException"/>
		public abstract void StoreContainerTokenPreviousMasterKey(MasterKey key);

		/// <summary>Record the expiration time for a container token</summary>
		/// <param name="containerId">the container ID</param>
		/// <param name="expirationTime">the container token expiration time</param>
		/// <exception cref="System.IO.IOException"/>
		public abstract void StoreContainerToken(ContainerId containerId, long expirationTime
			);

		/// <summary>Remove records for a container token</summary>
		/// <param name="containerId">the container ID</param>
		/// <exception cref="System.IO.IOException"/>
		public abstract void RemoveContainerToken(ContainerId containerId);

		/// <summary>Load the state of log deleters</summary>
		/// <returns>recovered log deleter state</returns>
		/// <exception cref="System.IO.IOException"/>
		public abstract NMStateStoreService.RecoveredLogDeleterState LoadLogDeleterState(
			);

		/// <summary>Store the state of a log deleter</summary>
		/// <param name="appId">the application ID for the log deleter</param>
		/// <param name="proto">the serialized state of the log deleter</param>
		/// <exception cref="System.IO.IOException"/>
		public abstract void StoreLogDeleter(ApplicationId appId, YarnServerNodemanagerRecoveryProtos.LogDeleterProto
			 proto);

		/// <summary>Remove the state of a log deleter</summary>
		/// <param name="appId">the application ID for the log deleter</param>
		/// <exception cref="System.IO.IOException"/>
		public abstract void RemoveLogDeleter(ApplicationId appId);

		/// <exception cref="System.IO.IOException"/>
		protected internal abstract void InitStorage(Configuration conf);

		/// <exception cref="System.IO.IOException"/>
		protected internal abstract void StartStorage();

		/// <exception cref="System.IO.IOException"/>
		protected internal abstract void CloseStorage();
	}
}
