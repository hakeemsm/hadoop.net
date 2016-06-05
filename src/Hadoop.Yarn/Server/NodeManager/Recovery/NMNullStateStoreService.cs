using System;
using System.Collections.Generic;
using System.Text;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Proto;
using Org.Apache.Hadoop.Yarn.Server.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Recovery
{
	public class NMNullStateStoreService : NMStateStoreService
	{
		public NMNullStateStoreService()
			: base(typeof(Org.Apache.Hadoop.Yarn.Server.Nodemanager.Recovery.NMNullStateStoreService
				).FullName)
		{
		}

		// The state store to use when state isn't being stored
		public override bool CanRecover()
		{
			return false;
		}

		/// <exception cref="System.IO.IOException"/>
		public override NMStateStoreService.RecoveredApplicationsState LoadApplicationsState
			()
		{
			throw new NotSupportedException("Recovery not supported by this state store");
		}

		/// <exception cref="System.IO.IOException"/>
		public override void StoreApplication(ApplicationId appId, YarnServerNodemanagerRecoveryProtos.ContainerManagerApplicationProto
			 p)
		{
		}

		public override void StoreFinishedApplication(ApplicationId appId)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public override void RemoveApplication(ApplicationId appId)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public override IList<NMStateStoreService.RecoveredContainerState> LoadContainersState
			()
		{
			throw new NotSupportedException("Recovery not supported by this state store");
		}

		/// <exception cref="System.IO.IOException"/>
		public override void StoreContainer(ContainerId containerId, StartContainerRequest
			 startRequest)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public override void StoreContainerDiagnostics(ContainerId containerId, StringBuilder
			 diagnostics)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public override void StoreContainerLaunched(ContainerId containerId)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public override void StoreContainerKilled(ContainerId containerId)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public override void StoreContainerCompleted(ContainerId containerId, int exitCode
			)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public override void RemoveContainer(ContainerId containerId)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public override NMStateStoreService.RecoveredLocalizationState LoadLocalizationState
			()
		{
			throw new NotSupportedException("Recovery not supported by this state store");
		}

		/// <exception cref="System.IO.IOException"/>
		public override void StartResourceLocalization(string user, ApplicationId appId, 
			YarnProtos.LocalResourceProto proto, Path localPath)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public override void FinishResourceLocalization(string user, ApplicationId appId, 
			YarnServerNodemanagerRecoveryProtos.LocalizedResourceProto proto)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public override void RemoveLocalizedResource(string user, ApplicationId appId, Path
			 localPath)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public override NMStateStoreService.RecoveredDeletionServiceState LoadDeletionServiceState
			()
		{
			throw new NotSupportedException("Recovery not supported by this state store");
		}

		/// <exception cref="System.IO.IOException"/>
		public override void StoreDeletionTask(int taskId, YarnServerNodemanagerRecoveryProtos.DeletionServiceDeleteTaskProto
			 taskProto)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public override void RemoveDeletionTask(int taskId)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public override NMStateStoreService.RecoveredNMTokensState LoadNMTokensState()
		{
			throw new NotSupportedException("Recovery not supported by this state store");
		}

		/// <exception cref="System.IO.IOException"/>
		public override void StoreNMTokenCurrentMasterKey(MasterKey key)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public override void StoreNMTokenPreviousMasterKey(MasterKey key)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public override void StoreNMTokenApplicationMasterKey(ApplicationAttemptId attempt
			, MasterKey key)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public override void RemoveNMTokenApplicationMasterKey(ApplicationAttemptId attempt
			)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public override NMStateStoreService.RecoveredContainerTokensState LoadContainerTokensState
			()
		{
			throw new NotSupportedException("Recovery not supported by this state store");
		}

		/// <exception cref="System.IO.IOException"/>
		public override void StoreContainerTokenCurrentMasterKey(MasterKey key)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public override void StoreContainerTokenPreviousMasterKey(MasterKey key)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public override void StoreContainerToken(ContainerId containerId, long expirationTime
			)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public override void RemoveContainerToken(ContainerId containerId)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public override NMStateStoreService.RecoveredLogDeleterState LoadLogDeleterState(
			)
		{
			throw new NotSupportedException("Recovery not supported by this state store");
		}

		/// <exception cref="System.IO.IOException"/>
		public override void StoreLogDeleter(ApplicationId appId, YarnServerNodemanagerRecoveryProtos.LogDeleterProto
			 proto)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public override void RemoveLogDeleter(ApplicationId appId)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void InitStorage(Configuration conf)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void StartStorage()
		{
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void CloseStorage()
		{
		}
	}
}
