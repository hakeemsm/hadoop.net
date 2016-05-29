using Org.Apache.Hadoop.IO.Retry;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Api
{
	public interface ResourceTracker
	{
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		[Idempotent]
		RegisterNodeManagerResponse RegisterNodeManager(RegisterNodeManagerRequest request
			);

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		[AtMostOnce]
		NodeHeartbeatResponse NodeHeartbeat(NodeHeartbeatRequest request);
	}
}
