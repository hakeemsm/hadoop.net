using System.Collections.Generic;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Recovery;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Security;
using Org.Apache.Hadoop.Yarn.Server.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager
{
	/// <summary>
	/// Context interface for sharing information across components in the
	/// NodeManager.
	/// </summary>
	public interface Context
	{
		/// <summary>Return the nodeId.</summary>
		/// <remarks>Return the nodeId. Usable only when the ContainerManager is started.</remarks>
		/// <returns>the NodeId</returns>
		NodeId GetNodeId();

		/// <summary>Return the node http-address.</summary>
		/// <remarks>Return the node http-address. Usable only after the Webserver is started.
		/// 	</remarks>
		/// <returns>the http-port</returns>
		int GetHttpPort();

		ConcurrentMap<ApplicationId, Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
			> GetApplications();

		IDictionary<ApplicationId, Credentials> GetSystemCredentialsForApps();

		ConcurrentMap<ContainerId, Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
			> GetContainers();

		NMContainerTokenSecretManager GetContainerTokenSecretManager();

		NMTokenSecretManagerInNM GetNMTokenSecretManager();

		NodeHealthStatus GetNodeHealthStatus();

		ContainerManagementProtocol GetContainerManager();

		LocalDirsHandlerService GetLocalDirsHandler();

		ApplicationACLsManager GetApplicationACLsManager();

		NMStateStoreService GetNMStateStore();

		bool GetDecommissioned();

		void SetDecommissioned(bool isDecommissioned);
	}
}
