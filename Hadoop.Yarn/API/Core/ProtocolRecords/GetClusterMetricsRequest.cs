using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords
{
	/// <summary>
	/// <p>The request sent by clients to get cluster metrics from the
	/// <code>ResourceManager</code>.</p>
	/// <p>Currently, this is empty.</p>
	/// </summary>
	/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.ApplicationClientProtocol.GetClusterMetrics(GetClusterMetricsRequest)
	/// 	"/>
	public abstract class GetClusterMetricsRequest
	{
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public static GetClusterMetricsRequest NewInstance()
		{
			GetClusterMetricsRequest request = Records.NewRecord<GetClusterMetricsRequest>();
			return request;
		}
	}
}
