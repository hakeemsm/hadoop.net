using System;
using System.Net;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Oncrpc;
using Org.Apache.Hadoop.Util;
using Org.Jboss.Netty.Bootstrap;
using Org.Jboss.Netty.Channel;
using Org.Jboss.Netty.Channel.Group;
using Org.Jboss.Netty.Channel.Socket.Nio;
using Org.Jboss.Netty.Handler.Timeout;
using Org.Jboss.Netty.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Portmap
{
	/// <summary>Portmap service for binding RPC protocols.</summary>
	/// <remarks>Portmap service for binding RPC protocols. See RFC 1833 for details.</remarks>
	internal sealed class Portmap
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Portmap.Portmap
			));

		private const int DefaultIdleTimeMilliseconds = 5000;

		private ConnectionlessBootstrap udpServer;

		private ServerBootstrap tcpServer;

		private ChannelGroup allChannels = new DefaultChannelGroup();

		private Org.Jboss.Netty.Channel.Channel udpChannel;

		private Org.Jboss.Netty.Channel.Channel tcpChannel;

		private readonly RpcProgramPortmap handler;

		public static void Main(string[] args)
		{
			StringUtils.StartupShutdownMessage(typeof(Org.Apache.Hadoop.Portmap.Portmap), args
				, Log);
			int port = RpcProgram.RpcbPort;
			Org.Apache.Hadoop.Portmap.Portmap pm = new Org.Apache.Hadoop.Portmap.Portmap();
			try
			{
				pm.Start(DefaultIdleTimeMilliseconds, new IPEndPoint(port), new IPEndPoint(port));
			}
			catch (Exception e)
			{
				Log.Fatal("Failed to start the server. Cause:", e);
				pm.Shutdown();
				System.Environment.Exit(-1);
			}
		}

		internal void Shutdown()
		{
			allChannels.Close().AwaitUninterruptibly();
			tcpServer.ReleaseExternalResources();
			udpServer.ReleaseExternalResources();
		}

		[VisibleForTesting]
		internal EndPoint GetTcpServerLocalAddress()
		{
			return tcpChannel.GetLocalAddress();
		}

		[VisibleForTesting]
		internal EndPoint GetUdpServerLoAddress()
		{
			return udpChannel.GetLocalAddress();
		}

		[VisibleForTesting]
		internal RpcProgramPortmap GetHandler()
		{
			return handler;
		}

		internal void Start(int idleTimeMilliSeconds, EndPoint tcpAddress, EndPoint udpAddress
			)
		{
			tcpServer = new ServerBootstrap(new NioServerSocketChannelFactory(Executors.NewCachedThreadPool
				(), Executors.NewCachedThreadPool()));
			tcpServer.SetPipelineFactory(new _ChannelPipelineFactory_100(this, idleTimeMilliSeconds
				));
			udpServer = new ConnectionlessBootstrap(new NioDatagramChannelFactory(Executors.NewCachedThreadPool
				()));
			udpServer.SetPipeline(Channels.Pipeline(RpcUtil.StageRpcMessageParser, handler, RpcUtil
				.StageRpcUdpResponse));
			tcpChannel = tcpServer.Bind(tcpAddress);
			udpChannel = udpServer.Bind(udpAddress);
			allChannels.AddItem(tcpChannel);
			allChannels.AddItem(udpChannel);
			Log.Info("Portmap server started at tcp://" + tcpChannel.GetLocalAddress() + ", udp://"
				 + udpChannel.GetLocalAddress());
		}

		private sealed class _ChannelPipelineFactory_100 : ChannelPipelineFactory
		{
			public _ChannelPipelineFactory_100(Portmap _enclosing, int idleTimeMilliSeconds)
			{
				this._enclosing = _enclosing;
				this.idleTimeMilliSeconds = idleTimeMilliSeconds;
				this.timer = new HashedWheelTimer();
				this.idleStateHandler = new IdleStateHandler(this.timer, 0, 0, idleTimeMilliSeconds
					, TimeUnit.Milliseconds);
			}

			private readonly HashedWheelTimer timer;

			private readonly IdleStateHandler idleStateHandler;

			/// <exception cref="System.Exception"/>
			public ChannelPipeline GetPipeline()
			{
				return Channels.Pipeline(RpcUtil.ConstructRpcFrameDecoder(), RpcUtil.StageRpcMessageParser
					, this.idleStateHandler, this._enclosing.handler, RpcUtil.StageRpcTcpResponse);
			}

			private readonly Portmap _enclosing;

			private readonly int idleTimeMilliSeconds;
		}

		public Portmap()
		{
			handler = new RpcProgramPortmap(allChannels);
		}
	}
}
