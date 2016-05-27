using System;
using Org.Apache.Commons.Logging;
using Org.Jboss.Netty.Buffer;
using Org.Jboss.Netty.Channel;
using Org.Jboss.Netty.Handler.Codec.Frame;
using Sharpen;

namespace Org.Apache.Hadoop.Oncrpc
{
	public sealed class RpcUtil
	{
		/// <summary>The XID in RPC call.</summary>
		/// <remarks>
		/// The XID in RPC call. It is used for starting with new seed after each
		/// reboot.
		/// </remarks>
		private static int xid = (int)(Runtime.CurrentTimeMillis() / 1000) << 12;

		public static int GetNewXid(string caller)
		{
			return xid = ++xid + caller.GetHashCode();
		}

		public static void SendRpcResponse(ChannelHandlerContext ctx, RpcResponse response
			)
		{
			Channels.FireMessageReceived(ctx, response);
		}

		public static FrameDecoder ConstructRpcFrameDecoder()
		{
			return new RpcUtil.RpcFrameDecoder();
		}

		public static readonly SimpleChannelUpstreamHandler StageRpcMessageParser = new RpcUtil.RpcMessageParserStage
			();

		public static readonly SimpleChannelUpstreamHandler StageRpcTcpResponse = new RpcUtil.RpcTcpResponseStage
			();

		public static readonly SimpleChannelUpstreamHandler StageRpcUdpResponse = new RpcUtil.RpcUdpResponseStage
			();

		/// <summary>
		/// An RPC client can separate a RPC message into several frames (i.e.,
		/// fragments) when transferring it across the wire.
		/// </summary>
		/// <remarks>
		/// An RPC client can separate a RPC message into several frames (i.e.,
		/// fragments) when transferring it across the wire. RpcFrameDecoder
		/// reconstructs a full RPC message from these fragments.
		/// RpcFrameDecoder is a stateful pipeline stage. It has to be constructed for
		/// each RPC client.
		/// </remarks>
		internal class RpcFrameDecoder : FrameDecoder
		{
			public static readonly Log Log = LogFactory.GetLog(typeof(RpcUtil.RpcFrameDecoder
				));

			private ChannelBuffer currentFrame;

			protected override object Decode(ChannelHandlerContext ctx, Org.Jboss.Netty.Channel.Channel
				 channel, ChannelBuffer buf)
			{
				if (buf.ReadableBytes() < 4)
				{
					return null;
				}
				buf.MarkReaderIndex();
				byte[] fragmentHeader = new byte[4];
				buf.ReadBytes(fragmentHeader);
				int length = XDR.FragmentSize(fragmentHeader);
				bool isLast = XDR.IsLastFragment(fragmentHeader);
				if (buf.ReadableBytes() < length)
				{
					buf.ResetReaderIndex();
					return null;
				}
				ChannelBuffer newFragment = buf.ReadSlice(length);
				if (currentFrame == null)
				{
					currentFrame = newFragment;
				}
				else
				{
					currentFrame = ChannelBuffers.WrappedBuffer(currentFrame, newFragment);
				}
				if (isLast)
				{
					ChannelBuffer completeFrame = currentFrame;
					currentFrame = null;
					return completeFrame;
				}
				else
				{
					return null;
				}
			}
		}

		/// <summary>
		/// RpcMessageParserStage parses the network bytes and encapsulates the RPC
		/// request into a RpcInfo instance.
		/// </summary>
		internal sealed class RpcMessageParserStage : SimpleChannelUpstreamHandler
		{
			private static readonly Log Log = LogFactory.GetLog(typeof(RpcUtil.RpcMessageParserStage
				));

			/// <exception cref="System.Exception"/>
			public override void MessageReceived(ChannelHandlerContext ctx, MessageEvent e)
			{
				ChannelBuffer buf = (ChannelBuffer)e.GetMessage();
				ByteBuffer b = buf.ToByteBuffer().AsReadOnlyBuffer();
				XDR @in = new XDR(b, XDR.State.Reading);
				RpcInfo info = null;
				try
				{
					RpcCall callHeader = RpcCall.Read(@in);
					ChannelBuffer dataBuffer = ChannelBuffers.WrappedBuffer(@in.Buffer().Slice());
					info = new RpcInfo(callHeader, dataBuffer, ctx, e.GetChannel(), e.GetRemoteAddress
						());
				}
				catch (Exception)
				{
					Log.Info("Malformed RPC request from " + e.GetRemoteAddress());
				}
				if (info != null)
				{
					Channels.FireMessageReceived(ctx, info);
				}
			}
		}

		/// <summary>
		/// RpcTcpResponseStage sends an RpcResponse across the wire with the
		/// appropriate fragment header.
		/// </summary>
		private class RpcTcpResponseStage : SimpleChannelUpstreamHandler
		{
			/// <exception cref="System.Exception"/>
			public override void MessageReceived(ChannelHandlerContext ctx, MessageEvent e)
			{
				RpcResponse r = (RpcResponse)e.GetMessage();
				byte[] fragmentHeader = XDR.RecordMark(r.Data().ReadableBytes(), true);
				ChannelBuffer header = ChannelBuffers.WrappedBuffer(fragmentHeader);
				ChannelBuffer d = ChannelBuffers.WrappedBuffer(header, r.Data());
				e.GetChannel().Write(d);
			}
		}

		/// <summary>
		/// RpcUdpResponseStage sends an RpcResponse as a UDP packet, which does not
		/// require a fragment header.
		/// </summary>
		private sealed class RpcUdpResponseStage : SimpleChannelUpstreamHandler
		{
			/// <exception cref="System.Exception"/>
			public override void MessageReceived(ChannelHandlerContext ctx, MessageEvent e)
			{
				RpcResponse r = (RpcResponse)e.GetMessage();
				e.GetChannel().Write(r.Data(), r.RemoteAddress());
			}
		}
	}
}
