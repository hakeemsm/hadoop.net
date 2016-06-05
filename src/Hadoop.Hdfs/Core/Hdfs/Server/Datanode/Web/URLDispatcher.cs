using System.Net;
using IO.Netty.Channel;
using IO.Netty.Handler.Codec.Http;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs.Server.Datanode.Web.Webhdfs;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode.Web
{
	internal class URLDispatcher : SimpleChannelInboundHandler<HttpRequest>
	{
		private readonly IPEndPoint proxyHost;

		private readonly Configuration conf;

		private readonly Configuration confForCreate;

		internal URLDispatcher(IPEndPoint proxyHost, Configuration conf, Configuration confForCreate
			)
		{
			this.proxyHost = proxyHost;
			this.conf = conf;
			this.confForCreate = confForCreate;
		}

		/// <exception cref="System.Exception"/>
		protected override void ChannelRead0(ChannelHandlerContext ctx, HttpRequest req)
		{
			string uri = req.GetUri();
			ChannelPipeline p = ctx.Pipeline();
			if (uri.StartsWith(WebHdfsHandler.WebhdfsPrefix))
			{
				WebHdfsHandler h = new WebHdfsHandler(conf, confForCreate);
				p.Replace(this, typeof(WebHdfsHandler).Name, h);
				h.ChannelRead0(ctx, req);
			}
			else
			{
				SimpleHttpProxyHandler h = new SimpleHttpProxyHandler(proxyHost);
				p.Replace(this, typeof(SimpleHttpProxyHandler).Name, h);
				h.ChannelRead0(ctx, req);
			}
		}
	}
}
