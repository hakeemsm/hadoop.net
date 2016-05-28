using System;
using System.Net;
using Com.Google.Common.Annotations;
using IO.Netty.Bootstrap;
using IO.Netty.Channel;
using IO.Netty.Channel.Group;
using IO.Netty.Channel.Nio;
using IO.Netty.Channel.Socket;
using IO.Netty.Channel.Socket.Nio;
using IO.Netty.Handler.Codec.Http;
using IO.Netty.Handler.Codec.String;
using IO.Netty.Util.Concurrent;
using Org.Apache.Commons.Logging;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Tools.OfflineImageViewer
{
	/// <summary>
	/// WebImageViewer loads a fsimage and exposes read-only WebHDFS API for its
	/// namespace.
	/// </summary>
	public class WebImageViewer : IDisposable
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.Tools.OfflineImageViewer.WebImageViewer
			));

		private IO.Netty.Channel.Channel channel;

		private IPEndPoint address;

		private readonly ServerBootstrap bootstrap;

		private readonly EventLoopGroup bossGroup;

		private readonly EventLoopGroup workerGroup;

		private readonly ChannelGroup allChannels;

		public WebImageViewer(IPEndPoint address)
		{
			this.address = address;
			this.bossGroup = new NioEventLoopGroup();
			this.workerGroup = new NioEventLoopGroup();
			this.allChannels = new DefaultChannelGroup(GlobalEventExecutor.Instance);
			this.bootstrap = new ServerBootstrap().Group(bossGroup, workerGroup).Channel(typeof(
				NioServerSocketChannel));
		}

		/// <summary>Start WebImageViewer and wait until the thread is interrupted.</summary>
		/// <param name="fsimage">the fsimage to load.</param>
		/// <exception cref="System.IO.IOException">if failed to load the fsimage.</exception>
		public virtual void Start(string fsimage)
		{
			try
			{
				InitServer(fsimage);
				channel.CloseFuture().Await();
			}
			catch (Exception)
			{
				Log.Info("Interrupted. Stopping the WebImageViewer.");
				Close();
			}
		}

		/// <summary>Start WebImageViewer.</summary>
		/// <param name="fsimage">the fsimage to load.</param>
		/// <exception cref="System.IO.IOException">if fail to load the fsimage.</exception>
		/// <exception cref="System.Exception"/>
		[VisibleForTesting]
		public virtual void InitServer(string fsimage)
		{
			FSImageLoader loader = FSImageLoader.Load(fsimage);
			bootstrap.ChildHandler(new _ChannelInitializer_92(this, loader));
			channel = bootstrap.Bind(address).Sync().Channel();
			allChannels.AddItem(channel);
			address = (IPEndPoint)channel.LocalAddress();
			Log.Info("WebImageViewer started. Listening on " + address.ToString() + ". Press Ctrl+C to stop the viewer."
				);
		}

		private sealed class _ChannelInitializer_92 : ChannelInitializer<SocketChannel>
		{
			public _ChannelInitializer_92(WebImageViewer _enclosing, FSImageLoader loader)
			{
				this._enclosing = _enclosing;
				this.loader = loader;
			}

			/// <exception cref="System.Exception"/>
			protected override void InitChannel(SocketChannel ch)
			{
				ChannelPipeline p = ch.Pipeline();
				p.AddLast(new HttpRequestDecoder(), new StringEncoder(), new HttpResponseEncoder(
					), new FSImageHandler(loader, this._enclosing.allChannels));
			}

			private readonly WebImageViewer _enclosing;

			private readonly FSImageLoader loader;
		}

		/// <summary>Get the listening port.</summary>
		/// <returns>the port WebImageViewer is listening on</returns>
		[VisibleForTesting]
		public virtual int GetPort()
		{
			return address.Port;
		}

		public virtual void Close()
		{
			allChannels.Close().AwaitUninterruptibly();
			bossGroup.ShutdownGracefully();
			workerGroup.ShutdownGracefully();
		}
	}
}
