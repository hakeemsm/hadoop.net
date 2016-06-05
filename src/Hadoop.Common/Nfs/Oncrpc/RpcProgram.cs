using System.IO;
using System.Net;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Oncrpc.Security;
using Org.Apache.Hadoop.Portmap;
using Org.Jboss.Netty.Buffer;
using Org.Jboss.Netty.Channel;


namespace Org.Apache.Hadoop.Oncrpc
{
	/// <summary>Class for writing RPC server programs based on RFC 1050.</summary>
	/// <remarks>
	/// Class for writing RPC server programs based on RFC 1050. Extend this class
	/// and implement
	/// <see cref="HandleInternal(Org.Jboss.Netty.Channel.ChannelHandlerContext, RpcInfo)
	/// 	"/>
	/// to handle the requests received.
	/// </remarks>
	public abstract class RpcProgram : SimpleChannelUpstreamHandler
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Oncrpc.RpcProgram
			));

		public const int RpcbPort = 111;

		private readonly string program;

		private readonly string host;

		private int port;

		private readonly int progNumber;

		private readonly int lowProgVersion;

		private readonly int highProgVersion;

		protected internal readonly bool allowInsecurePorts;

		/// <summary>
		/// If not null, this will be used as the socket to use to connect to the
		/// system portmap daemon when registering this RPC server program.
		/// </summary>
		private readonly DatagramSocket registrationSocket;

		/// <summary>Constructor</summary>
		/// <param name="program">program name</param>
		/// <param name="host">host where the Rpc server program is started</param>
		/// <param name="port">port where the Rpc server program is listening to</param>
		/// <param name="progNumber">program number as defined in RFC 1050</param>
		/// <param name="lowProgVersion">lowest version of the specification supported</param>
		/// <param name="highProgVersion">highest version of the specification supported</param>
		/// <param name="registrationSocket">
		/// if not null, use this socket to register
		/// with portmap daemon
		/// </param>
		/// <param name="allowInsecurePorts">
		/// true to allow client connections from
		/// unprivileged ports, false otherwise
		/// </param>
		protected internal RpcProgram(string program, string host, int port, int progNumber
			, int lowProgVersion, int highProgVersion, DatagramSocket registrationSocket, bool
			 allowInsecurePorts)
		{
			// Ephemeral port is chosen later
			this.program = program;
			this.host = host;
			this.port = port;
			this.progNumber = progNumber;
			this.lowProgVersion = lowProgVersion;
			this.highProgVersion = highProgVersion;
			this.registrationSocket = registrationSocket;
			this.allowInsecurePorts = allowInsecurePorts;
			Log.Info("Will " + (allowInsecurePorts ? string.Empty : "not ") + "accept client "
				 + "connections from unprivileged ports");
		}

		/// <summary>Register this program with the local portmapper.</summary>
		public virtual void Register(int transport, int boundPort)
		{
			if (boundPort != port)
			{
				Log.Info("The bound port is " + boundPort + ", different with configured port " +
					 port);
				port = boundPort;
			}
			// Register all the program versions with portmapper for a given transport
			for (int vers = lowProgVersion; vers <= highProgVersion; vers++)
			{
				PortmapMapping mapEntry = new PortmapMapping(progNumber, vers, transport, port);
				Register(mapEntry, true);
			}
		}

		/// <summary>Unregister this program with the local portmapper.</summary>
		public virtual void Unregister(int transport, int boundPort)
		{
			if (boundPort != port)
			{
				Log.Info("The bound port is " + boundPort + ", different with configured port " +
					 port);
				port = boundPort;
			}
			// Unregister all the program versions with portmapper for a given transport
			for (int vers = lowProgVersion; vers <= highProgVersion; vers++)
			{
				PortmapMapping mapEntry = new PortmapMapping(progNumber, vers, transport, port);
				Register(mapEntry, false);
			}
		}

		/// <summary>Register the program with Portmap or Rpcbind</summary>
		protected internal virtual void Register(PortmapMapping mapEntry, bool set)
		{
			XDR mappingRequest = PortmapRequest.Create(mapEntry, set);
			SimpleUdpClient registrationClient = new SimpleUdpClient(host, RpcbPort, mappingRequest
				, registrationSocket);
			try
			{
				registrationClient.Run();
			}
			catch (IOException e)
			{
				string request = set ? "Registration" : "Unregistration";
				Log.Error(request + " failure with " + host + ":" + port + ", portmap entry: " + 
					mapEntry);
				throw new RuntimeException(request + " failure", e);
			}
		}

		// Start extra daemons or services
		public virtual void StartDaemons()
		{
		}

		public virtual void StopDaemons()
		{
		}

		/// <exception cref="System.Exception"/>
		public override void MessageReceived(ChannelHandlerContext ctx, MessageEvent e)
		{
			RpcInfo info = (RpcInfo)e.GetMessage();
			RpcCall call = (RpcCall)info.Header();
			EndPoint remoteAddress = info.RemoteAddress();
			if (Log.IsTraceEnabled())
			{
				Log.Trace(program + " procedure #" + call.GetProcedure());
			}
			if (this.progNumber != call.GetProgram())
			{
				Log.Warn("Invalid RPC call program " + call.GetProgram());
				SendAcceptedReply(call, remoteAddress, RpcAcceptedReply.AcceptState.ProgUnavail, 
					ctx);
				return;
			}
			int ver = call.GetVersion();
			if (ver < lowProgVersion || ver > highProgVersion)
			{
				Log.Warn("Invalid RPC call version " + ver);
				SendAcceptedReply(call, remoteAddress, RpcAcceptedReply.AcceptState.ProgMismatch, 
					ctx);
				return;
			}
			HandleInternal(ctx, info);
		}

		public virtual bool DoPortMonitoring(EndPoint remoteAddress)
		{
			if (!allowInsecurePorts)
			{
				if (Log.IsTraceEnabled())
				{
					Log.Trace("Will not allow connections from unprivileged ports. " + "Checking for valid client port..."
						);
				}
				if (remoteAddress is IPEndPoint)
				{
					IPEndPoint inetRemoteAddress = (IPEndPoint)remoteAddress;
					if (inetRemoteAddress.Port > 1023)
					{
						Log.Warn("Connection attempted from '" + inetRemoteAddress + "' " + "which is an unprivileged port. Rejecting connection."
							);
						return false;
					}
				}
				else
				{
					Log.Warn("Could not determine remote port of socket address '" + remoteAddress + 
						"'. Rejecting connection.");
					return false;
				}
			}
			return true;
		}

		private void SendAcceptedReply(RpcCall call, EndPoint remoteAddress, RpcAcceptedReply.AcceptState
			 acceptState, ChannelHandlerContext ctx)
		{
			RpcAcceptedReply reply = RpcAcceptedReply.GetInstance(call.GetXid(), acceptState, 
				Verifier.VerifierNone);
			XDR @out = new XDR();
			reply.Write(@out);
			if (acceptState == RpcAcceptedReply.AcceptState.ProgMismatch)
			{
				@out.WriteInt(lowProgVersion);
				@out.WriteInt(highProgVersion);
			}
			ChannelBuffer b = ChannelBuffers.WrappedBuffer(@out.AsReadOnlyWrap().Buffer());
			RpcResponse rsp = new RpcResponse(b, remoteAddress);
			RpcUtil.SendRpcResponse(ctx, rsp);
		}

		protected internal static void SendRejectedReply(RpcCall call, EndPoint remoteAddress
			, ChannelHandlerContext ctx)
		{
			XDR @out = new XDR();
			RpcDeniedReply reply = new RpcDeniedReply(call.GetXid(), RpcReply.ReplyState.MsgDenied
				, RpcDeniedReply.RejectState.AuthError, new VerifierNone());
			reply.Write(@out);
			ChannelBuffer buf = ChannelBuffers.WrappedBuffer(@out.AsReadOnlyWrap().Buffer());
			RpcResponse rsp = new RpcResponse(buf, remoteAddress);
			RpcUtil.SendRpcResponse(ctx, rsp);
		}

		protected internal abstract void HandleInternal(ChannelHandlerContext ctx, RpcInfo
			 info);

		public override string ToString()
		{
			return "Rpc program: " + program + " at " + host + ":" + port;
		}

		protected internal abstract bool IsIdempotent(RpcCall call);

		public virtual int GetPort()
		{
			return port;
		}
	}
}
