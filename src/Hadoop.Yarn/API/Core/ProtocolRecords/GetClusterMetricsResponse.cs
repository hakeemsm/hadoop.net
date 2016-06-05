using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords
{
	/// <summary>
	/// The response sent by the <code>ResourceManager</code> to a client
	/// requesting cluster metrics.
	/// </summary>
	/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.Records.YarnClusterMetrics"/>
	/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.ApplicationClientProtocol.GetClusterMetrics(GetClusterMetricsRequest)
	/// 	"/>
	public abstract class GetClusterMetricsResponse
	{
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public static GetClusterMetricsResponse NewInstance(YarnClusterMetrics metrics)
		{
			GetClusterMetricsResponse response = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord
				<GetClusterMetricsResponse>();
			response.SetClusterMetrics(metrics);
			return response;
		}

		/// <summary>Get the <code>YarnClusterMetrics</code> for the cluster.</summary>
		/// <returns><code>YarnClusterMetrics</code> for the cluster</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract YarnClusterMetrics GetClusterMetrics();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetClusterMetrics(YarnClusterMetrics metrics);
	}
}
