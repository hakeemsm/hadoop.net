using System;
using System.IO;
using System.Net;
using IO.Netty.Bootstrap;
using IO.Netty.Channel;
using IO.Netty.Channel.Nio;
using IO.Netty.Channel.Socket;
using IO.Netty.Channel.Socket.Nio;
using IO.Netty.Handler.Codec.Http;
using IO.Netty.Handler.Ssl;
using IO.Netty.Handler.Stream;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Http;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security.Authorize;
using Org.Apache.Hadoop.Security.Ssl;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode.Web
{
	public class DatanodeHttpServer : IDisposable
	{
		private readonly HttpServer2 infoServer;

		private readonly EventLoopGroup bossGroup;

		private readonly EventLoopGroup workerGroup;

		private readonly ServerSocketChannel externalHttpChannel;

		private readonly ServerBootstrap httpServer;

		private readonly SSLFactory sslFactory;

		private readonly ServerBootstrap httpsServer;

		private readonly Configuration conf;

		private readonly Configuration confForCreate;

		private IPEndPoint httpAddress;

		private IPEndPoint httpsAddress;

		internal static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.Server.Datanode.Web.DatanodeHttpServer
			));

		/// <exception cref="System.IO.IOException"/>
		public DatanodeHttpServer(Configuration conf, DataNode datanode, ServerSocketChannel
			 externalHttpChannel)
		{
			this.conf = conf;
			Configuration confForInfoServer = new Configuration(conf);
			confForInfoServer.SetInt(HttpServer2.HttpMaxThreads, 10);
			HttpServer2.Builder builder = new HttpServer2.Builder().SetName("datanode").SetConf
				(confForInfoServer).SetACL(new AccessControlList(conf.Get(DFSConfigKeys.DfsAdmin
				, " "))).HostName(GetHostnameForSpnegoPrincipal(confForInfoServer)).AddEndpoint(
				URI.Create("http://localhost:0")).SetFindPort(true);
			this.infoServer = builder.Build();
			this.infoServer.AddInternalServlet(null, "/streamFile/*", typeof(StreamFile));
			this.infoServer.AddInternalServlet(null, "/getFileChecksum/*", typeof(FileChecksumServlets.GetServlet
				));
			this.infoServer.SetAttribute("datanode", datanode);
			this.infoServer.SetAttribute(JspHelper.CurrentConf, conf);
			this.infoServer.AddServlet(null, "/blockScannerReport", typeof(BlockScanner.Servlet
				));
			this.infoServer.Start();
			IPEndPoint jettyAddr = infoServer.GetConnectorAddress(0);
			this.confForCreate = new Configuration(conf);
			confForCreate.Set(FsPermission.UmaskLabel, "000");
			this.bossGroup = new NioEventLoopGroup();
			this.workerGroup = new NioEventLoopGroup();
			this.externalHttpChannel = externalHttpChannel;
			HttpConfig.Policy policy = DFSUtil.GetHttpPolicy(conf);
			if (policy.IsHttpEnabled())
			{
				this.httpServer = new ServerBootstrap().Group(bossGroup, workerGroup).ChildHandler
					(new _ChannelInitializer_117(this, jettyAddr, conf));
				if (externalHttpChannel == null)
				{
					httpServer.Channel(typeof(NioServerSocketChannel));
				}
				else
				{
					httpServer.ChannelFactory(new _ChannelFactory_130(externalHttpChannel));
				}
			}
			else
			{
				// The channel has been bounded externally via JSVC,
				// thus bind() becomes a no-op.
				this.httpServer = null;
			}
			if (policy.IsHttpsEnabled())
			{
				this.sslFactory = new SSLFactory(SSLFactory.Mode.Server, conf);
				try
				{
					sslFactory.Init();
				}
				catch (GeneralSecurityException e)
				{
					throw new IOException(e);
				}
				this.httpsServer = new ServerBootstrap().Group(bossGroup, workerGroup).Channel(typeof(
					NioServerSocketChannel)).ChildHandler(new _ChannelInitializer_155(this, jettyAddr
					, conf));
			}
			else
			{
				this.httpsServer = null;
				this.sslFactory = null;
			}
		}

		private sealed class _ChannelInitializer_117 : ChannelInitializer<SocketChannel>
		{
			public _ChannelInitializer_117(DatanodeHttpServer _enclosing, IPEndPoint jettyAddr
				, Configuration conf)
			{
				this._enclosing = _enclosing;
				this.jettyAddr = jettyAddr;
				this.conf = conf;
			}

			/// <exception cref="System.Exception"/>
			protected override void InitChannel(SocketChannel ch)
			{
				ChannelPipeline p = ch.Pipeline();
				p.AddLast(new HttpRequestDecoder(), new HttpResponseEncoder(), new ChunkedWriteHandler
					(), new URLDispatcher(jettyAddr, conf, this._enclosing.confForCreate));
			}

			private readonly DatanodeHttpServer _enclosing;

			private readonly IPEndPoint jettyAddr;

			private readonly Configuration conf;
		}

		private sealed class _ChannelFactory_130 : ChannelFactory<NioServerSocketChannel>
		{
			public _ChannelFactory_130(ServerSocketChannel externalHttpChannel)
			{
				this.externalHttpChannel = externalHttpChannel;
			}

			public NioServerSocketChannel NewChannel()
			{
				return new _NioServerSocketChannel_133(externalHttpChannel);
			}

			private sealed class _NioServerSocketChannel_133 : NioServerSocketChannel
			{
				public _NioServerSocketChannel_133(ServerSocketChannel baseArg1)
					: base(baseArg1)
				{
				}

				/// <exception cref="System.Exception"/>
				protected override void DoBind(EndPoint localAddress)
				{
				}
			}

			private readonly ServerSocketChannel externalHttpChannel;
		}

		private sealed class _ChannelInitializer_155 : ChannelInitializer<SocketChannel>
		{
			public _ChannelInitializer_155(DatanodeHttpServer _enclosing, IPEndPoint jettyAddr
				, Configuration conf)
			{
				this._enclosing = _enclosing;
				this.jettyAddr = jettyAddr;
				this.conf = conf;
			}

			/// <exception cref="System.Exception"/>
			protected override void InitChannel(SocketChannel ch)
			{
				ChannelPipeline p = ch.Pipeline();
				p.AddLast(new SslHandler(this._enclosing.sslFactory.CreateSSLEngine()), new HttpRequestDecoder
					(), new HttpResponseEncoder(), new ChunkedWriteHandler(), new URLDispatcher(jettyAddr
					, conf, this._enclosing.confForCreate));
			}

			private readonly DatanodeHttpServer _enclosing;

			private readonly IPEndPoint jettyAddr;

			private readonly Configuration conf;
		}

		public virtual IPEndPoint GetHttpAddress()
		{
			return httpAddress;
		}

		public virtual IPEndPoint GetHttpsAddress()
		{
			return httpsAddress;
		}

		public virtual void Start()
		{
			if (httpServer != null)
			{
				ChannelFuture f = httpServer.Bind(DataNode.GetInfoAddr(conf));
				f.SyncUninterruptibly();
				httpAddress = (IPEndPoint)f.Channel().LocalAddress();
				Log.Info("Listening HTTP traffic on " + httpAddress);
			}
			if (httpsServer != null)
			{
				IPEndPoint secInfoSocAddr = NetUtils.CreateSocketAddr(conf.GetTrimmed(DFSConfigKeys
					.DfsDatanodeHttpsAddressKey, DFSConfigKeys.DfsDatanodeHttpsAddressDefault));
				ChannelFuture f = httpsServer.Bind(secInfoSocAddr);
				f.SyncUninterruptibly();
				httpsAddress = (IPEndPoint)f.Channel().LocalAddress();
				Log.Info("Listening HTTPS traffic on " + httpsAddress);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Close()
		{
			bossGroup.ShutdownGracefully();
			workerGroup.ShutdownGracefully();
			if (sslFactory != null)
			{
				sslFactory.Destroy();
			}
			if (externalHttpChannel != null)
			{
				externalHttpChannel.Close();
			}
			try
			{
				infoServer.Stop();
			}
			catch (Exception e)
			{
				throw new IOException(e);
			}
		}

		private static string GetHostnameForSpnegoPrincipal(Configuration conf)
		{
			string addr = conf.GetTrimmed(DFSConfigKeys.DfsDatanodeHttpAddressKey, null);
			if (addr == null)
			{
				addr = conf.GetTrimmed(DFSConfigKeys.DfsDatanodeHttpsAddressKey, DFSConfigKeys.DfsDatanodeHttpsAddressDefault
					);
			}
			IPEndPoint inetSocker = NetUtils.CreateSocketAddr(addr);
			return inetSocker.GetHostString();
		}
	}
}
