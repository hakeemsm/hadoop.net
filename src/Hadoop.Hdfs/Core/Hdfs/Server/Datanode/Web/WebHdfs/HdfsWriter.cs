using System;
using System.IO;
using IO.Netty.Channel;
using IO.Netty.Handler.Codec.Http;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode.Web.Webhdfs
{
	internal class HdfsWriter : SimpleChannelInboundHandler<HttpContent>
	{
		private readonly DFSClient client;

		private readonly OutputStream @out;

		private readonly DefaultHttpResponse response;

		private static readonly Log Log = WebHdfsHandler.Log;

		internal HdfsWriter(DFSClient client, OutputStream @out, DefaultHttpResponse response
			)
		{
			this.client = client;
			this.@out = @out;
			this.response = response;
		}

		/// <exception cref="System.Exception"/>
		public override void ChannelReadComplete(ChannelHandlerContext ctx)
		{
			ctx.Flush();
		}

		/// <exception cref="System.IO.IOException"/>
		protected override void ChannelRead0(ChannelHandlerContext ctx, HttpContent chunk
			)
		{
			chunk.Content().ReadBytes(@out, chunk.Content().ReadableBytes());
			if (chunk is LastHttpContent)
			{
				response.Headers().Set(HttpHeaders.Names.Connection, HttpHeaders.Values.Close);
				ctx.Write(response).AddListener(ChannelFutureListener.Close);
				ReleaseDfsResources();
			}
		}

		public override void ChannelInactive(ChannelHandlerContext ctx)
		{
			ReleaseDfsResources();
		}

		public override void ExceptionCaught(ChannelHandlerContext ctx, Exception cause)
		{
			ReleaseDfsResources();
			DefaultHttpResponse resp = ExceptionHandler.ExceptionCaught(cause);
			resp.Headers().Set(HttpHeaders.Names.Connection, HttpHeaders.Values.Close);
			ctx.WriteAndFlush(response).AddListener(ChannelFutureListener.Close);
		}

		private void ReleaseDfsResources()
		{
			IOUtils.Cleanup(Log, @out);
			IOUtils.Cleanup(Log, client);
		}
	}
}
