using System.Net;
using Org.Jboss.Netty.Buffer;
using Sharpen;

namespace Org.Apache.Hadoop.Oncrpc
{
	/// <summary>RpcResponse encapsulates a response to a RPC request.</summary>
	/// <remarks>
	/// RpcResponse encapsulates a response to a RPC request. It contains the data
	/// that is going to cross the wire, as well as the information of the remote
	/// peer.
	/// </remarks>
	public class RpcResponse
	{
		private readonly ChannelBuffer data;

		private readonly EndPoint remoteAddress;

		public RpcResponse(ChannelBuffer data, EndPoint remoteAddress)
		{
			this.data = data;
			this.remoteAddress = remoteAddress;
		}

		public virtual ChannelBuffer Data()
		{
			return data;
		}

		public virtual EndPoint RemoteAddress()
		{
			return remoteAddress;
		}
	}
}
