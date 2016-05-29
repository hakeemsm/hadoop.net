using Org.Apache.Hadoop.Classification;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Api.Records
{
	/// <summary>
	/// <c>NodeHealthStatus</c>
	/// is a summary of the health status of the node.
	/// <p>
	/// It includes information such as:
	/// <ul>
	/// <li>
	/// An indicator of whether the node is healthy, as determined by the
	/// health-check script.
	/// </li>
	/// <li>The previous time at which the health status was reported.</li>
	/// <li>A diagnostic report on the health status.</li>
	/// </ul>
	/// </summary>
	/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.Records.NodeReport"/>
	/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.ApplicationClientProtocol.GetClusterNodes(Org.Apache.Hadoop.Yarn.Api.Protocolrecords.GetClusterNodesRequest)
	/// 	"/>
	public abstract class NodeHealthStatus
	{
		[InterfaceAudience.Private]
		public static NodeHealthStatus NewInstance(bool isNodeHealthy, string healthReport
			, long lastHealthReport)
		{
			NodeHealthStatus status = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<NodeHealthStatus
				>();
			status.SetIsNodeHealthy(isNodeHealthy);
			status.SetHealthReport(healthReport);
			status.SetLastHealthReportTime(lastHealthReport);
			return status;
		}

		/// <summary>Is the node healthy?</summary>
		/// <returns><code>true</code> if the node is healthy, else <code>false</code></returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract bool GetIsNodeHealthy();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetIsNodeHealthy(bool isNodeHealthy);

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
	}
}
