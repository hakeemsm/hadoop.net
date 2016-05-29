using System.Collections.Generic;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords
{
	public interface NodeHeartbeatResponse
	{
		int GetResponseId();

		NodeAction GetNodeAction();

		IList<ContainerId> GetContainersToCleanup();

		IList<ContainerId> GetContainersToBeRemovedFromNM();

		IList<ApplicationId> GetApplicationsToCleanup();

		void SetResponseId(int responseId);

		void SetNodeAction(NodeAction action);

		MasterKey GetContainerTokenMasterKey();

		void SetContainerTokenMasterKey(MasterKey secretKey);

		MasterKey GetNMTokenMasterKey();

		void SetNMTokenMasterKey(MasterKey secretKey);

		void AddAllContainersToCleanup(IList<ContainerId> containers);

		// This tells NM to remove finished containers from its context. Currently, NM
		// will remove finished containers from its context only after AM has actually
		// received the finished containers in a previous allocate response
		void AddContainersToBeRemovedFromNM(IList<ContainerId> containers);

		void AddAllApplicationsToCleanup(IList<ApplicationId> applications);

		long GetNextHeartBeatInterval();

		void SetNextHeartBeatInterval(long nextHeartBeatInterval);

		string GetDiagnosticsMessage();

		void SetDiagnosticsMessage(string diagnosticsMessage);

		// Credentials (i.e. hdfs tokens) needed by NodeManagers for application
		// localizations and logAggreations.
		IDictionary<ApplicationId, ByteBuffer> GetSystemCredentialsForApps();

		void SetSystemCredentialsForApps(IDictionary<ApplicationId, ByteBuffer> systemCredentials
			);
	}
}
