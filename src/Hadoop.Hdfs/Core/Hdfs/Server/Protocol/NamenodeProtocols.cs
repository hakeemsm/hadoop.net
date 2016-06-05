using Org.Apache.Hadoop.HA;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authorize;
using Org.Apache.Hadoop.Tools;
using Org.Apache.Hadoop.Tracing;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Protocol
{
	/// <summary>The full set of RPC methods implemented by the Namenode.</summary>
	public interface NamenodeProtocols : ClientProtocol, DatanodeProtocol, NamenodeProtocol
		, RefreshAuthorizationPolicyProtocol, RefreshUserMappingsProtocol, RefreshCallQueueProtocol
		, GenericRefreshProtocol, GetUserMappingsProtocol, HAServiceProtocol, TraceAdminProtocol
	{
	}
}
