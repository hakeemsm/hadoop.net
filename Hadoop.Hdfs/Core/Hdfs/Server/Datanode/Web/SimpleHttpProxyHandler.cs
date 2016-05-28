using System;
using System.Net;
using IO.Netty.Buffer;
using IO.Netty.Channel;
using IO.Netty.Channel.Socket;
using IO.Netty.Channel.Socket.Nio;
using IO.Netty.Handler.Codec.Http;
using Org.Apache.Commons.Logging;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode.Web
{
	/// <summary>Dead simple session-layer HTTP proxy.</summary>
	/// <remarks>
	/// Dead simple session-layer HTTP proxy. It gets the HTTP responses
	/// inside the context, assuming that the remote peer is reasonable fast and
	/// the response is small. The upper layer should be filtering out malicious
	/// inputs.
	/// </remarks>
	internal class SimpleHttpProxyHandler : SimpleChannelInboundHandler<HttpRequest>
	{
		private string uri;

		private IO.Netty.Channel.Channel proxiedChannel;

		private readonly IPEndPoint host;

		internal static readonly Log Log = DatanodeHttpServer.Log;

		internal SimpleHttpProxyHandler(IPEndPoint host)
		{
			this.host = host;
		}

		private class Forwarder : ChannelInboundHandlerAdapter
		{
			private readonly string uri;

			private readonly IO.Netty.Channel.Channel client;

			private Forwarder(string uri, IO.Netty.Channel.Channel client)
			{
				this.uri = uri;
				this.client = client;
			}

			public override void ChannelInactive(ChannelHandlerContext ctx)
			{
				CloseOnFlush(client);
			}

			public override void ChannelRead(ChannelHandlerContext ctx, object msg)
			{
				client.WriteAndFlush(msg).AddListener(new _ChannelFutureListener_78(ctx));
			}

			private sealed class _ChannelFutureListener_78 : ChannelFutureListener
			{
				public _ChannelFutureListener_78(ChannelHandlerContext ctx)
				{
					this.ctx = ctx;
				}

				public void OperationComplete(ChannelFuture future)
				{
					if (future.IsSuccess())
					{
						ctx.Channel().Read();
					}
					else
					{
						SimpleHttpProxyHandler.Log.Debug("Proxy failed. Cause: ", future.Cause());
						future.Channel().Close();
					}
				}

				private readonly ChannelHandlerContext ctx;
			}

			public override void ExceptionCaught(ChannelHandlerContext ctx, Exception cause)
			{
				Log.Debug("Proxy for " + uri + " failed. cause: ", cause);
				CloseOnFlush(ctx.Channel());
			}
		}

		protected override void ChannelRead0(ChannelHandlerContext ctx, HttpRequest req)
		{
			uri = req.GetUri();
			IO.Netty.Channel.Channel client = ctx.Channel();
			IO.Netty.Bootstrap.Bootstrap proxiedServer = new IO.Netty.Bootstrap.Bootstrap().Group
				(client.EventLoop()).Channel(typeof(NioSocketChannel)).Handler(new _ChannelInitializer_106
				(this, client));
			ChannelFuture f = proxiedServer.Connect(host);
			proxiedChannel = f.Channel();
			f.AddListener(new _ChannelFutureListener_115(this, ctx, req, client));
		}

		private sealed class _ChannelInitializer_106 : ChannelInitializer<SocketChannel>
		{
			public _ChannelInitializer_106(SimpleHttpProxyHandler _enclosing, IO.Netty.Channel.Channel
				 client)
			{
				this._enclosing = _enclosing;
				this.client = client;
			}

			/// <exception cref="System.Exception"/>
			protected override void InitChannel(SocketChannel ch)
			{
				ChannelPipeline p = ch.Pipeline();
				p.AddLast(new HttpRequestEncoder(), new SimpleHttpProxyHandler.Forwarder(this._enclosing
					.uri, client));
			}

			private readonly SimpleHttpProxyHandler _enclosing;

			private readonly IO.Netty.Channel.Channel client;
		}

		private sealed class _ChannelFutureListener_115 : ChannelFutureListener
		{
			public _ChannelFutureListener_115(SimpleHttpProxyHandler _enclosing, ChannelHandlerContext
				 ctx, HttpRequest req, IO.Netty.Channel.Channel client)
			{
				this._enclosing = _enclosing;
				this.ctx = ctx;
				this.req = req;
				this.client = client;
			}

			/// <exception cref="System.Exception"/>
			public void OperationComplete(ChannelFuture future)
			{
				if (future.IsSuccess())
				{
					ctx.Channel().Pipeline().Remove<HttpResponseEncoder>();
					HttpRequest newReq = new DefaultFullHttpRequest(HttpVersion.Http11, req.GetMethod
						(), req.GetUri());
					newReq.Headers().Add(req.Headers());
					newReq.Headers().Set(HttpHeaders.Names.Connection, HttpHeaders.Values.Close);
					future.Channel().WriteAndFlush(newReq);
				}
				else
				{
					DefaultHttpResponse resp = new DefaultHttpResponse(HttpVersion.Http11, HttpResponseStatus
						.InternalServerError);
					resp.Headers().Set(HttpHeaders.Names.Connection, HttpHeaders.Values.Close);
					SimpleHttpProxyHandler.Log.Info("Proxy " + this._enclosing.uri + " failed. Cause: "
						, future.Cause());
					ctx.WriteAndFlush(resp).AddListener(ChannelFutureListener.Close);
					client.Close();
				}
			}

			private readonly SimpleHttpProxyHandler _enclosing;

			private readonly ChannelHandlerContext ctx;

			private readonly HttpRequest req;

			private readonly IO.Netty.Channel.Channel client;
		}

		public override void ChannelInactive(ChannelHandlerContext ctx)
		{
			if (proxiedChannel != null)
			{
				proxiedChannel.Close();
				proxiedChannel = null;
			}
		}

		public override void ExceptionCaught(ChannelHandlerContext ctx, Exception cause)
		{
			Log.Info("Proxy for " + uri + " failed. cause: ", cause);
			if (proxiedChannel != null)
			{
				proxiedChannel.Close();
				proxiedChannel = null;
			}
			ctx.Close();
		}

		private static void CloseOnFlush(IO.Netty.Channel.Channel ch)
		{
			if (ch.IsActive())
			{
				ch.WriteAndFlush(Unpooled.EmptyBuffer).AddListener(ChannelFutureListener.Close);
			}
		}
	}
}
