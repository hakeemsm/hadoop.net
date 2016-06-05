using Org.Apache.Commons.Logging;
using Org.Jboss.Netty.Buffer;
using Org.Jboss.Netty.Channel;


namespace Org.Apache.Hadoop.Oncrpc
{
	/// <summary>
	/// A simple TCP based RPC client handler used by
	/// <see cref="SimpleTcpServer"/>
	/// .
	/// </summary>
	public class SimpleTcpClientHandler : SimpleChannelHandler
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(SimpleTcpClient));

		protected internal readonly XDR request;

		public SimpleTcpClientHandler(XDR request)
		{
			this.request = request;
		}

		public override void ChannelConnected(ChannelHandlerContext ctx, ChannelStateEvent
			 e)
		{
			// Send the request
			if (Log.IsDebugEnabled())
			{
				Log.Debug("sending PRC request");
			}
			ChannelBuffer outBuf = XDR.WriteMessageTcp(request, true);
			e.GetChannel().Write(outBuf);
		}

		/// <summary>Shutdown connection by default.</summary>
		/// <remarks>
		/// Shutdown connection by default. Subclass can override this method to do
		/// more interaction with the server.
		/// </remarks>
		public override void MessageReceived(ChannelHandlerContext ctx, MessageEvent e)
		{
			e.GetChannel().Close();
		}

		public override void ExceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
		{
			Log.Warn("Unexpected exception from downstream: ", e.GetCause());
			e.GetChannel().Close();
		}
	}
}
