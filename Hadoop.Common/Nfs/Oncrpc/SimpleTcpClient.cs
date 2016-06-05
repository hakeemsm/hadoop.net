using System.Net;
using Org.Jboss.Netty.Bootstrap;
using Org.Jboss.Netty.Channel;
using Org.Jboss.Netty.Channel.Socket.Nio;


namespace Org.Apache.Hadoop.Oncrpc
{
	/// <summary>A simple TCP based RPC client which just sends a request to a server.</summary>
	public class SimpleTcpClient
	{
		protected internal readonly string host;

		protected internal readonly int port;

		protected internal readonly XDR request;

		protected internal ChannelPipelineFactory pipelineFactory;

		protected internal readonly bool oneShot;

		public SimpleTcpClient(string host, int port, XDR request)
			: this(host, port, request, true)
		{
		}

		public SimpleTcpClient(string host, int port, XDR request, bool oneShot)
		{
			this.host = host;
			this.port = port;
			this.request = request;
			this.oneShot = oneShot;
		}

		protected internal virtual ChannelPipelineFactory SetPipelineFactory()
		{
			this.pipelineFactory = new _ChannelPipelineFactory_53(this);
			return this.pipelineFactory;
		}

		private sealed class _ChannelPipelineFactory_53 : ChannelPipelineFactory
		{
			public _ChannelPipelineFactory_53(SimpleTcpClient _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public ChannelPipeline GetPipeline()
			{
				return Channels.Pipeline(RpcUtil.ConstructRpcFrameDecoder(), new SimpleTcpClientHandler
					(this._enclosing.request));
			}

			private readonly SimpleTcpClient _enclosing;
		}

		public virtual void Run()
		{
			// Configure the client.
			ChannelFactory factory = new NioClientSocketChannelFactory(Executors.NewCachedThreadPool
				(), Executors.NewCachedThreadPool(), 1, 1);
			ClientBootstrap bootstrap = new ClientBootstrap(factory);
			// Set up the pipeline factory.
			bootstrap.SetPipelineFactory(SetPipelineFactory());
			bootstrap.SetOption("tcpNoDelay", true);
			bootstrap.SetOption("keepAlive", true);
			// Start the connection attempt.
			ChannelFuture future = bootstrap.Connect(new IPEndPoint(host, port));
			if (oneShot)
			{
				// Wait until the connection is closed or the connection attempt fails.
				future.GetChannel().GetCloseFuture().AwaitUninterruptibly();
				// Shut down thread pools to exit.
				bootstrap.ReleaseExternalResources();
			}
		}
	}
}
