using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords
{
	public abstract class RefreshNodesResponse
	{
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public static RefreshNodesResponse NewInstance()
		{
			RefreshNodesResponse response = Records.NewRecord<RefreshNodesResponse>();
			return response;
		}
	}
}
