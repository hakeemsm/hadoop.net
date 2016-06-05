using Org.Apache.Commons.Logging;
using Org.Jboss.Netty.Buffer;
using Org.Jboss.Netty.Channel;


namespace Org.Apache.Hadoop.Oncrpc
{
	/// <summary>A simple client that registers an RPC program with portmap.</summary>
	public class RegistrationClient : SimpleTcpClient
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Oncrpc.RegistrationClient
			));

		public RegistrationClient(string host, int port, XDR request)
			: base(host, port, request)
		{
		}

		/// <summary>Handler to handle response from the server.</summary>
		internal class RegistrationClientHandler : SimpleTcpClientHandler
		{
			public RegistrationClientHandler(XDR request)
				: base(request)
			{
			}

			private bool ValidMessageLength(int len)
			{
				// 28 bytes is the minimal success response size (portmapV2)
				if (len < 28)
				{
					if (Log.IsDebugEnabled())
					{
						Log.Debug("Portmap mapping registration failed," + " the response size is less than 28 bytes:"
							 + len);
					}
					return false;
				}
				return true;
			}

			public override void MessageReceived(ChannelHandlerContext ctx, MessageEvent e)
			{
				ChannelBuffer buf = (ChannelBuffer)e.GetMessage();
				// Read reply
				if (!ValidMessageLength(buf.ReadableBytes()))
				{
					e.GetChannel().Close();
					return;
				}
				// handling fragment header for TCP, 4 bytes.
				byte[] fragmentHeader = Arrays.CopyOfRange(buf.Array(), 0, 4);
				int fragmentSize = XDR.FragmentSize(fragmentHeader);
				bool isLast = XDR.IsLastFragment(fragmentHeader);
				System.Diagnostics.Debug.Assert((fragmentSize == 28 && isLast == true));
				XDR xdr = new XDR();
				xdr.WriteFixedOpaque(Arrays.CopyOfRange(buf.Array(), 4, buf.ReadableBytes()));
				RpcReply reply = RpcReply.Read(xdr);
				if (reply.GetState() == RpcReply.ReplyState.MsgAccepted)
				{
					RpcAcceptedReply acceptedReply = (RpcAcceptedReply)reply;
					Handle(acceptedReply, xdr);
				}
				else
				{
					RpcDeniedReply deniedReply = (RpcDeniedReply)reply;
					Handle(deniedReply);
				}
				e.GetChannel().Close();
			}

			// shutdown now that request is complete
			private void Handle(RpcDeniedReply deniedReply)
			{
				Log.Warn("Portmap mapping registration request was denied , " + deniedReply);
			}

			private void Handle(RpcAcceptedReply acceptedReply, XDR xdr)
			{
				RpcAcceptedReply.AcceptState acceptState = acceptedReply.GetAcceptState();
				System.Diagnostics.Debug.Assert((acceptState == RpcAcceptedReply.AcceptState.Success
					));
				bool answer = xdr.ReadBoolean();
				if (answer != true)
				{
					Log.Warn("Portmap mapping registration failed, accept state:" + acceptState);
				}
				Log.Info("Portmap mapping registration succeeded");
			}
		}
	}
}
