using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords
{
	public abstract class RefreshQueuesRequest
	{
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public static RefreshQueuesRequest NewInstance()
		{
			RefreshQueuesRequest request = Records.NewRecord<RefreshQueuesRequest>();
			return request;
		}
	}
}
