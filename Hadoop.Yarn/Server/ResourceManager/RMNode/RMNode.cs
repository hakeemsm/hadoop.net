using System.Collections.Generic;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmnode
{
	/// <summary>
	/// Node managers information on available resources
	/// and other static information.
	/// </summary>
	public abstract class RMNode
	{
		/// <summary>negative value means no timeout</summary>
		public const int OverCommitTimeoutMillisDefault = -1;

		/// <summary>the node id of of this node.</summary>
		/// <returns>the node id of this node.</returns>
		public abstract NodeId GetNodeID();

		/// <summary>the hostname of this node</summary>
		/// <returns>hostname of this node</returns>
		public abstract string GetHostName();

		/// <summary>the command port for this node</summary>
		/// <returns>command port for this node</returns>
		public abstract int GetCommandPort();

		/// <summary>the http port for this node</summary>
		/// <returns>http port for this node</returns>
		public abstract int GetHttpPort();

		/// <summary>the ContainerManager address for this node.</summary>
		/// <returns>the ContainerManager address for this node.</returns>
		public abstract string GetNodeAddress();

		/// <summary>the http-Address for this node.</summary>
		/// <returns>the http-url address for this node</returns>
		public abstract string GetHttpAddress();

		/// <summary>the latest health report received from this node.</summary>
		/// <returns>the latest health report received from this node.</returns>
		public abstract string GetHealthReport();

		/// <summary>the time of the latest health report received from this node.</summary>
		/// <returns>the time of the latest health report received from this node.</returns>
		public abstract long GetLastHealthReportTime();

		/// <summary>
		/// the node manager version of the node received as part of the
		/// registration with the resource manager
		/// </summary>
		public abstract string GetNodeManagerVersion();

		/// <summary>the total available resource.</summary>
		/// <returns>the total available resource.</returns>
		public abstract Resource GetTotalCapability();

		/// <summary>The rack name for this node manager.</summary>
		/// <returns>the rack name.</returns>
		public abstract string GetRackName();

		/// <summary>
		/// the
		/// <see cref="Org.Apache.Hadoop.Net.Node"/>
		/// information for this node.
		/// </summary>
		/// <returns>
		/// 
		/// <see cref="Org.Apache.Hadoop.Net.Node"/>
		/// information for this node.
		/// </returns>
		public abstract Node GetNode();

		public abstract NodeState GetState();

		public abstract IList<ContainerId> GetContainersToCleanUp();

		public abstract IList<ApplicationId> GetAppsToCleanup();

		/// <summary>
		/// Update a
		/// <see cref="Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords.NodeHeartbeatResponse
		/// 	"/>
		/// with the list of containers and
		/// applications to clean up for this node.
		/// </summary>
		/// <param name="response">
		/// the
		/// <see cref="Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords.NodeHeartbeatResponse
		/// 	"/>
		/// to update
		/// </param>
		public abstract void UpdateNodeHeartbeatResponseForCleanup(NodeHeartbeatResponse 
			response);

		public abstract NodeHeartbeatResponse GetLastNodeHeartBeatResponse();

		/// <summary>Reset lastNodeHeartbeatResponse's ID to 0.</summary>
		public abstract void ResetLastNodeHeartBeatResponse();

		/// <summary>
		/// Get and clear the list of containerUpdates accumulated across NM
		/// heartbeats.
		/// </summary>
		/// <returns>containerUpdates accumulated across NM heartbeats.</returns>
		public abstract IList<UpdatedContainerInfo> PullContainerUpdates();

		/// <summary>Get set of labels in this node</summary>
		/// <returns>labels in this node</returns>
		public abstract ICollection<string> GetNodeLabels();
	}

	public static class RMNodeConstants
	{
	}
}
