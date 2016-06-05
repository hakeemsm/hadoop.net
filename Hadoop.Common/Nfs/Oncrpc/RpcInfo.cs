using System.Net;
using Org.Jboss.Netty.Buffer;
using Org.Jboss.Netty.Channel;


namespace Org.Apache.Hadoop.Oncrpc
{
	/// <summary>RpcInfo records all contextual information of an RPC message.</summary>
	/// <remarks>
	/// RpcInfo records all contextual information of an RPC message. It contains
	/// the RPC header, the parameters, and the information of the remote peer.
	/// </remarks>
	public sealed class RpcInfo
	{
		private readonly RpcMessage header;

		private readonly ChannelBuffer data;

		private readonly Org.Jboss.Netty.Channel.Channel channel;

		private readonly EndPoint remoteAddress;

		public RpcInfo(RpcMessage header, ChannelBuffer data, ChannelHandlerContext channelContext
			, Org.Jboss.Netty.Channel.Channel channel, EndPoint remoteAddress)
		{
			this.header = header;
			this.data = data;
			this.channel = channel;
			this.remoteAddress = remoteAddress;
		}

		public RpcMessage Header()
		{
			return header;
		}

		public ChannelBuffer Data()
		{
			return data;
		}

		public Org.Jboss.Netty.Channel.Channel Channel()
		{
			return channel;
		}

		public EndPoint RemoteAddress()
		{
			return remoteAddress;
		}
	}
}
