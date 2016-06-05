using System;
using System.IO;
using System.Net;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Nfs.Conf;
using Org.Apache.Hadoop.Http;
using Org.Apache.Hadoop.Net;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Nfs.Nfs3
{
	/// <summary>Encapsulates the HTTP server started by the NFS3 gateway.</summary>
	internal class Nfs3HttpServer
	{
		private int infoPort;

		private int infoSecurePort;

		private HttpServer2 httpServer;

		private readonly NfsConfiguration conf;

		internal Nfs3HttpServer(NfsConfiguration conf)
		{
			this.conf = conf;
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void Start()
		{
			IPEndPoint httpAddr = GetHttpAddress(conf);
			string httpsAddrString = conf.Get(NfsConfigKeys.NfsHttpsAddressKey, NfsConfigKeys
				.NfsHttpsAddressDefault);
			IPEndPoint httpsAddr = NetUtils.CreateSocketAddr(httpsAddrString);
			HttpServer2.Builder builder = DFSUtil.HttpServerTemplateForNNAndJN(conf, httpAddr
				, httpsAddr, "nfs3", NfsConfigKeys.DfsNfsKerberosPrincipalKey, NfsConfigKeys.DfsNfsKeytabFileKey
				);
			this.httpServer = builder.Build();
			this.httpServer.Start();
			HttpConfig.Policy policy = DFSUtil.GetHttpPolicy(conf);
			int connIdx = 0;
			if (policy.IsHttpEnabled())
			{
				infoPort = httpServer.GetConnectorAddress(connIdx++).Port;
			}
			if (policy.IsHttpsEnabled())
			{
				infoSecurePort = httpServer.GetConnectorAddress(connIdx).Port;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void Stop()
		{
			if (httpServer != null)
			{
				try
				{
					httpServer.Stop();
				}
				catch (Exception e)
				{
					throw new IOException(e);
				}
			}
		}

		public virtual int GetPort()
		{
			return this.infoPort;
		}

		public virtual int GetSecurePort()
		{
			return this.infoSecurePort;
		}

		/// <summary>Return the URI that locates the HTTP server.</summary>
		public virtual URI GetServerURI()
		{
			// getHttpClientScheme() only returns https for HTTPS_ONLY policy. This
			// matches the behavior that the first connector is a HTTPS connector only
			// for HTTPS_ONLY policy.
			IPEndPoint addr = httpServer.GetConnectorAddress(0);
			return URI.Create(DFSUtil.GetHttpClientScheme(conf) + "://" + NetUtils.GetHostPortString
				(addr));
		}

		public virtual IPEndPoint GetHttpAddress(Configuration conf)
		{
			string addr = conf.Get(NfsConfigKeys.NfsHttpAddressKey, NfsConfigKeys.NfsHttpAddressDefault
				);
			return NetUtils.CreateSocketAddr(addr, NfsConfigKeys.NfsHttpPortDefault, NfsConfigKeys
				.NfsHttpAddressKey);
		}
	}
}
