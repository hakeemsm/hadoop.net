using System.Net;
using Org.Apache.Commons.Logging;
using Org.Jboss.Netty.Bootstrap;
using Org.Jboss.Netty.Channel;
using Org.Jboss.Netty.Channel.Socket;
using Org.Jboss.Netty.Channel.Socket.Nio;


namespace Org.Apache.Hadoop.Oncrpc
{
	/// <summary>Simple UDP server implemented based on netty.</summary>
	public class SimpleUdpServer
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Oncrpc.SimpleUdpServer
			));

		private readonly int SendBufferSize = 65536;

		private readonly int ReceiveBufferSize = 65536;

		protected internal readonly int port;

		protected internal readonly SimpleChannelUpstreamHandler rpcProgram;

		protected internal readonly int workerCount;

		protected internal int boundPort = -1;

		private ConnectionlessBootstrap server;

		private Org.Jboss.Netty.Channel.Channel ch;

		public SimpleUdpServer(int port, SimpleChannelUpstreamHandler program, int workerCount
			)
		{
			// Will be set after server starts
			this.port = port;
			this.rpcProgram = program;
			this.workerCount = workerCount;
		}

		public virtual void Run()
		{
			// Configure the client.
			DatagramChannelFactory f = new NioDatagramChannelFactory(Executors.NewCachedThreadPool
				(), workerCount);
			server = new ConnectionlessBootstrap(f);
			server.SetPipeline(Channels.Pipeline(RpcUtil.StageRpcMessageParser, rpcProgram, RpcUtil
				.StageRpcUdpResponse));
			server.SetOption("broadcast", "false");
			server.SetOption("sendBufferSize", SendBufferSize);
			server.SetOption("receiveBufferSize", ReceiveBufferSize);
			// Listen to the UDP port
			ch = server.Bind(new IPEndPoint(port));
			IPEndPoint socketAddr = (IPEndPoint)ch.GetLocalAddress();
			boundPort = socketAddr.Port;
			Log.Info("Started listening to UDP requests at port " + boundPort + " for " + rpcProgram
				 + " with workerCount " + workerCount);
		}

		// boundPort will be set only after server starts
		public virtual int GetBoundPort()
		{
			return this.boundPort;
		}

		public virtual void Shutdown()
		{
			if (ch != null)
			{
				ch.Close().AwaitUninterruptibly();
			}
			if (server != null)
			{
				server.ReleaseExternalResources();
			}
		}
	}
}
