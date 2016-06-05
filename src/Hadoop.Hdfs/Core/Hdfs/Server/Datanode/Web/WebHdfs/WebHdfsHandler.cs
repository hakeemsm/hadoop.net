using System;
using System.IO;
using Com.Google.Common.Base;
using IO.Netty.Buffer;
using IO.Netty.Channel;
using IO.Netty.Handler.Codec.Http;
using IO.Netty.Handler.Stream;
using Org.Apache.Commons.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Client;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Security.Token.Delegation;
using Org.Apache.Hadoop.Hdfs.Web;
using Org.Apache.Hadoop.Hdfs.Web.Resources;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode.Web.Webhdfs
{
	public class WebHdfsHandler : SimpleChannelInboundHandler<HttpRequest>
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.Server.Datanode.Web.Webhdfs.WebHdfsHandler
			));

		public const string WebhdfsPrefix = WebHdfsFileSystem.PathPrefix;

		public static readonly int WebhdfsPrefixLength = WebhdfsPrefix.Length;

		public const string ApplicationOctetStream = "application/octet-stream";

		public const string ApplicationJsonUtf8 = "application/json; charset=utf-8";

		private readonly Configuration conf;

		private readonly Configuration confForCreate;

		private string path;

		private ParameterParser @params;

		private UserGroupInformation ugi;

		/// <exception cref="System.IO.IOException"/>
		public WebHdfsHandler(Configuration conf, Configuration confForCreate)
		{
			this.conf = conf;
			this.confForCreate = confForCreate;
		}

		/// <exception cref="System.Exception"/>
		protected override void ChannelRead0(ChannelHandlerContext ctx, HttpRequest req)
		{
			Preconditions.CheckArgument(req.GetUri().StartsWith(WebhdfsPrefix));
			QueryStringDecoder queryString = new QueryStringDecoder(req.GetUri());
			@params = new ParameterParser(queryString, conf);
			DataNodeUGIProvider ugiProvider = new DataNodeUGIProvider(@params);
			ugi = ugiProvider.Ugi();
			path = @params.Path();
			InjectToken();
			ugi.DoAs(new _PrivilegedExceptionAction_110(this, ctx, req));
		}

		private sealed class _PrivilegedExceptionAction_110 : PrivilegedExceptionAction<Void
			>
		{
			public _PrivilegedExceptionAction_110(WebHdfsHandler _enclosing, ChannelHandlerContext
				 ctx, HttpRequest req)
			{
				this._enclosing = _enclosing;
				this.ctx = ctx;
				this.req = req;
			}

			/// <exception cref="System.Exception"/>
			public Void Run()
			{
				this._enclosing.Handle(ctx, req);
				return null;
			}

			private readonly WebHdfsHandler _enclosing;

			private readonly ChannelHandlerContext ctx;

			private readonly HttpRequest req;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.URISyntaxException"/>
		public virtual void Handle(ChannelHandlerContext ctx, HttpRequest req)
		{
			string op = @params.Op();
			HttpMethod method = req.GetMethod();
			if (Sharpen.Runtime.EqualsIgnoreCase(PutOpParam.OP.Create.ToString(), op) && method
				 == HttpMethod.Put)
			{
				OnCreate(ctx);
			}
			else
			{
				if (Sharpen.Runtime.EqualsIgnoreCase(PostOpParam.OP.Append.ToString(), op) && method
					 == HttpMethod.Post)
				{
					OnAppend(ctx);
				}
				else
				{
					if (Sharpen.Runtime.EqualsIgnoreCase(GetOpParam.OP.Open.ToString(), op) && method
						 == HttpMethod.Get)
					{
						OnOpen(ctx);
					}
					else
					{
						if (Sharpen.Runtime.EqualsIgnoreCase(GetOpParam.OP.Getfilechecksum.ToString(), op
							) && method == HttpMethod.Get)
						{
							OnGetFileChecksum(ctx);
						}
						else
						{
							throw new ArgumentException("Invalid operation " + op);
						}
					}
				}
			}
		}

		public override void ExceptionCaught(ChannelHandlerContext ctx, Exception cause)
		{
			Log.Debug("Error ", cause);
			DefaultHttpResponse resp = ExceptionHandler.ExceptionCaught(cause);
			resp.Headers().Set(HttpHeaders.Names.Connection, HttpHeaders.Values.Close);
			ctx.WriteAndFlush(resp).AddListener(ChannelFutureListener.Close);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.URISyntaxException"/>
		private void OnCreate(ChannelHandlerContext ctx)
		{
			WriteContinueHeader(ctx);
			string nnId = @params.NamenodeId();
			int bufferSize = @params.BufferSize();
			short replication = @params.Replication();
			long blockSize = @params.BlockSize();
			FsPermission permission = @params.Permission();
			EnumSet<CreateFlag> flags = @params.Overwrite() ? EnumSet.Of(CreateFlag.Create, CreateFlag
				.Overwrite) : EnumSet.Of(CreateFlag.Create);
			DFSClient dfsClient = NewDfsClient(nnId, confForCreate);
			OutputStream @out = dfsClient.CreateWrappedOutputStream(dfsClient.Create(path, permission
				, flags, replication, blockSize, null, bufferSize, null), null);
			DefaultHttpResponse resp = new DefaultHttpResponse(HttpVersion.Http11, HttpResponseStatus
				.Created);
			URI uri = new URI(HdfsConstants.HdfsUriScheme, nnId, path, null, null);
			resp.Headers().Set(HttpHeaders.Names.Location, uri.ToString());
			resp.Headers().Set(HttpHeaders.Names.ContentLength, 0);
			ctx.Pipeline().Replace(this, typeof(HdfsWriter).Name, new HdfsWriter(dfsClient, @out
				, resp));
		}

		/// <exception cref="System.IO.IOException"/>
		private void OnAppend(ChannelHandlerContext ctx)
		{
			WriteContinueHeader(ctx);
			string nnId = @params.NamenodeId();
			int bufferSize = @params.BufferSize();
			DFSClient dfsClient = NewDfsClient(nnId, conf);
			OutputStream @out = dfsClient.Append(path, bufferSize, EnumSet.Of(CreateFlag.Append
				), null, null);
			DefaultHttpResponse resp = new DefaultHttpResponse(HttpVersion.Http11, HttpResponseStatus
				.Ok);
			resp.Headers().Set(HttpHeaders.Names.ContentLength, 0);
			ctx.Pipeline().Replace(this, typeof(HdfsWriter).Name, new HdfsWriter(dfsClient, @out
				, resp));
		}

		/// <exception cref="System.IO.IOException"/>
		private void OnOpen(ChannelHandlerContext ctx)
		{
			string nnId = @params.NamenodeId();
			int bufferSize = @params.BufferSize();
			long offset = @params.Offset();
			long length = @params.Length();
			DefaultHttpResponse response = new DefaultHttpResponse(HttpVersion.Http11, HttpResponseStatus
				.Ok);
			HttpHeaders headers = response.Headers();
			// Allow the UI to access the file
			headers.Set(HttpHeaders.Names.AccessControlAllowMethods, HttpMethod.Get);
			headers.Set(HttpHeaders.Names.AccessControlAllowOrigin, "*");
			headers.Set(HttpHeaders.Names.ContentType, ApplicationOctetStream);
			headers.Set(HttpHeaders.Names.Connection, HttpHeaders.Values.Close);
			DFSClient dfsclient = NewDfsClient(nnId, conf);
			HdfsDataInputStream @in = dfsclient.CreateWrappedInputStream(dfsclient.Open(path, 
				bufferSize, true));
			@in.Seek(offset);
			long contentLength = @in.GetVisibleLength() - offset;
			if (length >= 0)
			{
				contentLength = Math.Min(contentLength, length);
			}
			InputStream data;
			if (contentLength >= 0)
			{
				headers.Set(HttpHeaders.Names.ContentLength, contentLength);
				data = new LimitInputStream(@in, contentLength);
			}
			else
			{
				data = @in;
			}
			ctx.Write(response);
			ctx.WriteAndFlush(new _ChunkedStream_221(dfsclient, data)).AddListener(ChannelFutureListener
				.Close);
		}

		private sealed class _ChunkedStream_221 : ChunkedStream
		{
			public _ChunkedStream_221(DFSClient dfsclient, InputStream baseArg1)
				: base(baseArg1)
			{
				this.dfsclient = dfsclient;
			}

			/// <exception cref="System.Exception"/>
			public override void Close()
			{
				base.Close();
				dfsclient.Close();
			}

			private readonly DFSClient dfsclient;
		}

		/// <exception cref="System.IO.IOException"/>
		private void OnGetFileChecksum(ChannelHandlerContext ctx)
		{
			MD5MD5CRC32FileChecksum checksum = null;
			string nnId = @params.NamenodeId();
			DFSClient dfsclient = NewDfsClient(nnId, conf);
			try
			{
				checksum = dfsclient.GetFileChecksum(path, long.MaxValue);
				dfsclient.Close();
				dfsclient = null;
			}
			finally
			{
				IOUtils.Cleanup(Log, dfsclient);
			}
			byte[] js = Sharpen.Runtime.GetBytesForString(JsonUtil.ToJsonString(checksum), Charsets
				.Utf8);
			DefaultFullHttpResponse resp = new DefaultFullHttpResponse(HttpVersion.Http11, HttpResponseStatus
				.Ok, Unpooled.WrappedBuffer(js));
			resp.Headers().Set(HttpHeaders.Names.ContentType, ApplicationJsonUtf8);
			resp.Headers().Set(HttpHeaders.Names.ContentLength, js.Length);
			resp.Headers().Set(HttpHeaders.Names.Connection, HttpHeaders.Values.Close);
			ctx.WriteAndFlush(resp).AddListener(ChannelFutureListener.Close);
		}

		private static void WriteContinueHeader(ChannelHandlerContext ctx)
		{
			DefaultHttpResponse r = new DefaultFullHttpResponse(HttpVersion.Http11, HttpResponseStatus
				.Continue, Unpooled.EmptyBuffer);
			ctx.WriteAndFlush(r);
		}

		/// <exception cref="System.IO.IOException"/>
		private static DFSClient NewDfsClient(string nnId, Configuration conf)
		{
			URI uri = URI.Create(HdfsConstants.HdfsUriScheme + "://" + nnId);
			return new DFSClient(uri, conf);
		}

		/// <exception cref="System.IO.IOException"/>
		private void InjectToken()
		{
			if (UserGroupInformation.IsSecurityEnabled())
			{
				Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> token = @params
					.DelegationToken();
				token.SetKind(DelegationTokenIdentifier.HdfsDelegationKind);
				ugi.AddToken(token);
			}
		}
	}
}
