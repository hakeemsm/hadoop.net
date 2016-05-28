using System;
using System.IO;
using System.Net;
using Javax.Servlet;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Http;
using Org.Apache.Hadoop.Net;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Qjournal.Server
{
	/// <summary>Encapsulates the HTTP server started by the Journal Service.</summary>
	public class JournalNodeHttpServer
	{
		public const string JnAttributeKey = "localjournal";

		private HttpServer2 httpServer;

		private readonly JournalNode localJournalNode;

		private readonly Configuration conf;

		internal JournalNodeHttpServer(Configuration conf, JournalNode jn)
		{
			this.conf = conf;
			this.localJournalNode = jn;
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void Start()
		{
			IPEndPoint httpAddr = GetAddress(conf);
			string httpsAddrString = conf.Get(DFSConfigKeys.DfsJournalnodeHttpsAddressKey, DFSConfigKeys
				.DfsJournalnodeHttpsAddressDefault);
			IPEndPoint httpsAddr = NetUtils.CreateSocketAddr(httpsAddrString);
			HttpServer2.Builder builder = DFSUtil.HttpServerTemplateForNNAndJN(conf, httpAddr
				, httpsAddr, "journal", DFSConfigKeys.DfsJournalnodeKerberosInternalSpnegoPrincipalKey
				, DFSConfigKeys.DfsJournalnodeKeytabFileKey);
			httpServer = builder.Build();
			httpServer.SetAttribute(JnAttributeKey, localJournalNode);
			httpServer.SetAttribute(JspHelper.CurrentConf, conf);
			httpServer.AddInternalServlet("getJournal", "/getJournal", typeof(GetJournalEditServlet
				), true);
			httpServer.Start();
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

		/// <summary>Return the actual address bound to by the running server.</summary>
		[Obsolete]
		public virtual IPEndPoint GetAddress()
		{
			IPEndPoint addr = httpServer.GetConnectorAddress(0);
			System.Diagnostics.Debug.Assert(addr.Port != 0);
			return addr;
		}

		/// <summary>Return the URI that locates the HTTP server.</summary>
		internal virtual URI GetServerURI()
		{
			// getHttpClientScheme() only returns https for HTTPS_ONLY policy. This
			// matches the behavior that the first connector is a HTTPS connector only
			// for HTTPS_ONLY policy.
			IPEndPoint addr = httpServer.GetConnectorAddress(0);
			return URI.Create(DFSUtil.GetHttpClientScheme(conf) + "://" + NetUtils.GetHostPortString
				(addr));
		}

		private static IPEndPoint GetAddress(Configuration conf)
		{
			string addr = conf.Get(DFSConfigKeys.DfsJournalnodeHttpAddressKey, DFSConfigKeys.
				DfsJournalnodeHttpAddressDefault);
			return NetUtils.CreateSocketAddr(addr, DFSConfigKeys.DfsJournalnodeHttpPortDefault
				, DFSConfigKeys.DfsJournalnodeHttpAddressKey);
		}

		/// <exception cref="System.IO.IOException"/>
		public static Journal GetJournalFromContext(ServletContext context, string jid)
		{
			JournalNode jn = (JournalNode)context.GetAttribute(JnAttributeKey);
			return jn.GetOrCreateJournal(jid);
		}

		public static Configuration GetConfFromContext(ServletContext context)
		{
			return (Configuration)context.GetAttribute(JspHelper.CurrentConf);
		}
	}
}
