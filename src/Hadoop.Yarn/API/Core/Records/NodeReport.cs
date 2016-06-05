using System.Collections.Generic;
using Org.Apache.Hadoop.Classification;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Records
{
	/// <summary>
	/// <c>NodeReport</c>
	/// is a summary of runtime information of a node
	/// in the cluster.
	/// <p>
	/// It includes details such as:
	/// <ul>
	/// <li>
	/// <see cref="NodeId"/>
	/// of the node.</li>
	/// <li>HTTP Tracking URL of the node.</li>
	/// <li>Rack name for the node.</li>
	/// <li>Used
	/// <see cref="Resource"/>
	/// on the node.</li>
	/// <li>Total available
	/// <see cref="Resource"/>
	/// of the node.</li>
	/// <li>Number of running containers on the node.</li>
	/// </ul>
	/// </summary>
	/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.ApplicationClientProtocol.GetClusterNodes(Org.Apache.Hadoop.Yarn.Api.Protocolrecords.GetClusterNodesRequest)
	/// 	"/>
	public abstract class NodeReport
	{
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public static NodeReport NewInstance(NodeId nodeId, NodeState nodeState, string httpAddress
			, string rackName, Resource used, Resource capability, int numContainers, string
			 healthReport, long lastHealthReportTime)
		{
			return NewInstance(nodeId, nodeState, httpAddress, rackName, used, capability, numContainers
				, healthReport, lastHealthReportTime, null);
		}

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public static NodeReport NewInstance(NodeId nodeId, NodeState nodeState, string httpAddress
			, string rackName, Resource used, Resource capability, int numContainers, string
			 healthReport, long lastHealthReportTime, ICollection<string> nodeLabels)
		{
			NodeReport nodeReport = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<NodeReport>
				();
			nodeReport.SetNodeId(nodeId);
			nodeReport.SetNodeState(nodeState);
			nodeReport.SetHttpAddress(httpAddress);
			nodeReport.SetRackName(rackName);
			nodeReport.SetUsed(used);
			nodeReport.SetCapability(capability);
			nodeReport.SetNumContainers(numContainers);
			nodeReport.SetHealthReport(healthReport);
			nodeReport.SetLastHealthReportTime(lastHealthReportTime);
			nodeReport.SetNodeLabels(nodeLabels);
			return nodeReport;
		}

		/// <summary>Get the <code>NodeId</code> of the node.</summary>
		/// <returns><code>NodeId</code> of the node</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract NodeId GetNodeId();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetNodeId(NodeId nodeId);

		/// <summary>Get the <code>NodeState</code> of the node.</summary>
		/// <returns><code>NodeState</code> of the node</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract NodeState GetNodeState();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetNodeState(NodeState nodeState);

		/// <summary>Get the <em>http address</em> of the node.</summary>
		/// <returns><em>http address</em> of the node</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract string GetHttpAddress();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetHttpAddress(string httpAddress);

		/// <summary>Get the <em>rack name</em> for the node.</summary>
		/// <returns><em>rack name</em> for the node</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract string GetRackName();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetRackName(string rackName);

		/// <summary>Get <em>used</em> <code>Resource</code> on the node.</summary>
		/// <returns><em>used</em> <code>Resource</code> on the node</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract Resource GetUsed();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetUsed(Resource used);

		/// <summary>Get the <em>total</em> <code>Resource</code> on the node.</summary>
		/// <returns><em>total</em> <code>Resource</code> on the node</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract Resource GetCapability();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetCapability(Resource capability);

		/// <summary>Get the <em>number of allocated containers</em> on the node.</summary>
		/// <returns><em>number of allocated containers</em> on the node</returns>
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract int GetNumContainers();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetNumContainers(int numContainers);

		/// <summary>Get the <em>diagnostic health report</em> of the node.</summary>
		/// <returns><em>diagnostic health report</em> of the node</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract string GetHealthReport();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetHealthReport(string healthReport);

		/// <summary>Get the <em>last timestamp</em> at which the health report was received.
		/// 	</summary>
		/// <returns><em>last timestamp</em> at which the health report was received</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract long GetLastHealthReportTime();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetLastHealthReportTime(long lastHealthReport);

		/// <summary>Get labels of this node</summary>
		/// <returns>labels of this node</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract ICollection<string> GetNodeLabels();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetNodeLabels(ICollection<string> nodeLabels);
	}
}
