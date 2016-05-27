using System.Net;
using Org.Apache.Commons.Logging;
using Org.Jboss.Netty.Bootstrap;
using Org.Jboss.Netty.Channel;
using Org.Jboss.Netty.Channel.Socket.Nio;
using Sharpen;

namespace Org.Apache.Hadoop.Oncrpc
{
	/// <summary>Simple UDP server implemented using netty.</summary>
	public class SimpleTcpServer
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Oncrpc.SimpleTcpServer
			));

		protected internal readonly int port;

		protected internal int boundPort = -1;

		protected internal readonly SimpleChannelUpstreamHandler rpcProgram;

		private ServerBootstrap server;

		private Org.Jboss.Netty.Channel.Channel ch;

		/// <summary>The maximum number of I/O worker threads</summary>
		protected internal readonly int workerCount;

		/// <param name="port">TCP port where to start the server at</param>
		/// <param name="program">RPC program corresponding to the server</param>
		/// <param name="workercount">Number of worker threads</param>
		public SimpleTcpServer(int port, RpcProgram program, int workercount)
		{
			// Will be set after server starts
			this.port = port;
			this.rpcProgram = program;
			this.workerCount = workercount;
		}

		public virtual void Run()
		{
			// Configure the Server.
			ChannelFactory factory;
			if (workerCount == 0)
			{
				// Use default workers: 2 * the number of available processors
				factory = new NioServerSocketChannelFactory(Executors.NewCachedThreadPool(), Executors
					.NewCachedThreadPool());
			}
			else
			{
				factory = new NioServerSocketChannelFactory(Executors.NewCachedThreadPool(), Executors
					.NewCachedThreadPool(), workerCount);
			}
			server = new ServerBootstrap(factory);
			server.SetPipelineFactory(new _ChannelPipelineFactory_73(this));
			server.SetOption("child.tcpNoDelay", true);
			server.SetOption("child.keepAlive", true);
			// Listen to TCP port
			ch = server.Bind(new IPEndPoint(port));
			IPEndPoint socketAddr = (IPEndPoint)ch.GetLocalAddress();
			boundPort = socketAddr.Port;
			Log.Info("Started listening to TCP requests at port " + boundPort + " for " + rpcProgram
				 + " with workerCount " + workerCount);
		}

		private sealed class _ChannelPipelineFactory_73 : ChannelPipelineFactory
		{
			public _ChannelPipelineFactory_73(SimpleTcpServer _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.Exception"/>
			public ChannelPipeline GetPipeline()
			{
				return Channels.Pipeline(RpcUtil.ConstructRpcFrameDecoder(), RpcUtil.StageRpcMessageParser
					, this._enclosing.rpcProgram, RpcUtil.StageRpcTcpResponse);
			}

			private readonly SimpleTcpServer _enclosing;
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
