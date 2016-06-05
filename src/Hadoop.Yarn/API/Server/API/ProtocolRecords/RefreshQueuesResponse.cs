using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords
{
	public abstract class RefreshQueuesResponse
	{
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public static RefreshQueuesResponse NewInstance()
		{
			RefreshQueuesResponse response = Records.NewRecord<RefreshQueuesResponse>();
			return response;
		}
	}
}
