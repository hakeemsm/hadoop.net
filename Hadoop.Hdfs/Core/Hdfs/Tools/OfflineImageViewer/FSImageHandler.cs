using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Base;
using IO.Netty.Buffer;
using IO.Netty.Channel;
using IO.Netty.Channel.Group;
using IO.Netty.Handler.Codec.Http;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Hdfs.Server.Datanode.Web.Webhdfs;
using Org.Apache.Hadoop.Hdfs.Web;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Tools.OfflineImageViewer
{
	/// <summary>Implement the read-only WebHDFS API for fsimage.</summary>
	internal class FSImageHandler : SimpleChannelInboundHandler<HttpRequest>
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.Tools.OfflineImageViewer.FSImageHandler
			));

		private readonly FSImageLoader image;

		private readonly ChannelGroup activeChannels;

		/// <exception cref="System.Exception"/>
		public override void ChannelActive(ChannelHandlerContext ctx)
		{
			activeChannels.AddItem(ctx.Channel());
		}

		/// <exception cref="System.IO.IOException"/>
		internal FSImageHandler(FSImageLoader image, ChannelGroup activeChannels)
		{
			this.image = image;
			this.activeChannels = activeChannels;
		}

		/// <exception cref="System.Exception"/>
		protected override void ChannelRead0(ChannelHandlerContext ctx, HttpRequest request
			)
		{
			if (request.GetMethod() != HttpMethod.Get)
			{
				DefaultHttpResponse resp = new DefaultHttpResponse(HttpVersion.Http11, HttpResponseStatus
					.MethodNotAllowed);
				resp.Headers().Set(HttpHeaders.Names.Connection, HttpHeaders.Values.Close);
				ctx.Write(resp).AddListener(ChannelFutureListener.Close);
				return;
			}
			QueryStringDecoder decoder = new QueryStringDecoder(request.GetUri());
			string op = GetOp(decoder);
			string content;
			string path = GetPath(decoder);
			switch (op)
			{
				case "GETFILESTATUS":
				{
					content = image.GetFileStatus(path);
					break;
				}

				case "LISTSTATUS":
				{
					content = image.ListStatus(path);
					break;
				}

				case "GETACLSTATUS":
				{
					content = image.GetAclStatus(path);
					break;
				}

				default:
				{
					throw new ArgumentException("Invalid value for webhdfs parameter" + " \"op\"");
				}
			}
			Log.Info("op=" + op + " target=" + path);
			DefaultFullHttpResponse resp_1 = new DefaultFullHttpResponse(HttpVersion.Http11, 
				HttpResponseStatus.Ok, Unpooled.WrappedBuffer(Sharpen.Runtime.GetBytesForString(
				content, Charsets.Utf8)));
			resp_1.Headers().Set(HttpHeaders.Names.ContentType, WebHdfsHandler.ApplicationJsonUtf8
				);
			resp_1.Headers().Set(HttpHeaders.Names.ContentLength, resp_1.Content().ReadableBytes
				());
			resp_1.Headers().Set(HttpHeaders.Names.Connection, HttpHeaders.Values.Close);
			ctx.Write(resp_1).AddListener(ChannelFutureListener.Close);
		}

		/// <exception cref="System.Exception"/>
		public override void ChannelReadComplete(ChannelHandlerContext ctx)
		{
			ctx.Flush();
		}

		/// <exception cref="System.Exception"/>
		public override void ExceptionCaught(ChannelHandlerContext ctx, Exception cause)
		{
			Exception e = cause is Exception ? (Exception)cause : new Exception(cause);
			string output = JsonUtil.ToJsonString(e);
			ByteBuf content = Unpooled.WrappedBuffer(Sharpen.Runtime.GetBytesForString(output
				, Charsets.Utf8));
			DefaultFullHttpResponse resp = new DefaultFullHttpResponse(HttpVersion.Http11, HttpResponseStatus
				.InternalServerError, content);
			resp.Headers().Set(HttpHeaders.Names.ContentType, WebHdfsHandler.ApplicationJsonUtf8
				);
			if (e is ArgumentException)
			{
				resp.SetStatus(HttpResponseStatus.BadRequest);
			}
			else
			{
				if (e is FileNotFoundException)
				{
					resp.SetStatus(HttpResponseStatus.NotFound);
				}
			}
			resp.Headers().Set(HttpHeaders.Names.ContentLength, resp.Content().ReadableBytes(
				));
			resp.Headers().Set(HttpHeaders.Names.Connection, HttpHeaders.Values.Close);
			ctx.Write(resp).AddListener(ChannelFutureListener.Close);
		}

		private static string GetOp(QueryStringDecoder decoder)
		{
			IDictionary<string, IList<string>> parameters = decoder.Parameters();
			return parameters.Contains("op") ? StringUtils.ToUpperCase(parameters["op"][0]) : 
				null;
		}

		/// <exception cref="System.IO.FileNotFoundException"/>
		private static string GetPath(QueryStringDecoder decoder)
		{
			string path = decoder.Path();
			if (path.StartsWith(WebHdfsHandler.WebhdfsPrefix))
			{
				return Sharpen.Runtime.Substring(path, WebHdfsHandler.WebhdfsPrefixLength);
			}
			else
			{
				throw new FileNotFoundException("Path: " + path + " should " + "start with " + WebHdfsHandler
					.WebhdfsPrefix);
			}
		}
	}
}
