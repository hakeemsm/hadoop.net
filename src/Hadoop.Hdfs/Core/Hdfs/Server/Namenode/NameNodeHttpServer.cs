using System.Collections.Generic;
using System.Net;
using Javax.Servlet;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Namenode.Startupprogress;
using Org.Apache.Hadoop.Hdfs.Server.Namenode.Web.Resources;
using Org.Apache.Hadoop.Hdfs.Web;
using Org.Apache.Hadoop.Hdfs.Web.Resources;
using Org.Apache.Hadoop.Http;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>Encapsulates the HTTP server started by the NameNode.</summary>
	public class NameNodeHttpServer
	{
		private HttpServer2 httpServer;

		private readonly Configuration conf;

		private readonly NameNode nn;

		private IPEndPoint httpAddress;

		private IPEndPoint httpsAddress;

		private readonly IPEndPoint bindAddress;

		public const string NamenodeAddressAttributeKey = "name.node.address";

		public const string FsimageAttributeKey = "name.system.image";

		protected internal const string NamenodeAttributeKey = "name.node";

		public const string StartupProgressAttributeKey = "startup.progress";

		internal NameNodeHttpServer(Configuration conf, NameNode nn, IPEndPoint bindAddress
			)
		{
			this.conf = conf;
			this.nn = nn;
			this.bindAddress = bindAddress;
		}

		/// <exception cref="System.IO.IOException"/>
		private void InitWebHdfs(Configuration conf)
		{
			if (WebHdfsFileSystem.IsEnabled(conf, HttpServer2.Log))
			{
				// set user pattern based on configuration file
				UserParam.SetUserPattern(conf.Get(DFSConfigKeys.DfsWebhdfsUserPatternKey, DFSConfigKeys
					.DfsWebhdfsUserPatternDefault));
				// add authentication filter for webhdfs
				string className = conf.Get(DFSConfigKeys.DfsWebhdfsAuthenticationFilterKey, DFSConfigKeys
					.DfsWebhdfsAuthenticationFilterDefault);
				string name = className;
				string pathSpec = WebHdfsFileSystem.PathPrefix + "/*";
				IDictionary<string, string> @params = GetAuthFilterParams(conf);
				HttpServer2.DefineFilter(httpServer.GetWebAppContext(), name, className, @params, 
					new string[] { pathSpec });
				HttpServer2.Log.Info("Added filter '" + name + "' (class=" + className + ")");
				// add webhdfs packages
				httpServer.AddJerseyResourcePackage(typeof(NamenodeWebHdfsMethods).Assembly.GetName
					() + ";" + typeof(Param).Assembly.GetName(), pathSpec);
			}
		}

		/// <seealso cref="Org.Apache.Hadoop.Hdfs.DFSUtil.GetHttpPolicy(Org.Apache.Hadoop.Conf.Configuration)
		/// 	">
		/// for information related to the different configuration options and
		/// Http Policy is decided.
		/// </seealso>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void Start()
		{
			HttpConfig.Policy policy = DFSUtil.GetHttpPolicy(conf);
			string infoHost = bindAddress.GetHostName();
			IPEndPoint httpAddr = bindAddress;
			string httpsAddrString = conf.GetTrimmed(DFSConfigKeys.DfsNamenodeHttpsAddressKey
				, DFSConfigKeys.DfsNamenodeHttpsAddressDefault);
			IPEndPoint httpsAddr = NetUtils.CreateSocketAddr(httpsAddrString);
			if (httpsAddr != null)
			{
				// If DFS_NAMENODE_HTTPS_BIND_HOST_KEY exists then it overrides the
				// host name portion of DFS_NAMENODE_HTTPS_ADDRESS_KEY.
				string bindHost = conf.GetTrimmed(DFSConfigKeys.DfsNamenodeHttpsBindHostKey);
				if (bindHost != null && !bindHost.IsEmpty())
				{
					httpsAddr = new IPEndPoint(bindHost, httpsAddr.Port);
				}
			}
			HttpServer2.Builder builder = DFSUtil.HttpServerTemplateForNNAndJN(conf, httpAddr
				, httpsAddr, "hdfs", DFSConfigKeys.DfsNamenodeKerberosInternalSpnegoPrincipalKey
				, DFSConfigKeys.DfsNamenodeKeytabFileKey);
			httpServer = builder.Build();
			if (policy.IsHttpsEnabled())
			{
				// assume same ssl port for all datanodes
				IPEndPoint datanodeSslPort = NetUtils.CreateSocketAddr(conf.GetTrimmed(DFSConfigKeys
					.DfsDatanodeHttpsAddressKey, infoHost + ":" + DFSConfigKeys.DfsDatanodeHttpsDefaultPort
					));
				httpServer.SetAttribute(DFSConfigKeys.DfsDatanodeHttpsPortKey, datanodeSslPort.Port
					);
			}
			InitWebHdfs(conf);
			httpServer.SetAttribute(NamenodeAttributeKey, nn);
			httpServer.SetAttribute(JspHelper.CurrentConf, conf);
			SetupServlets(httpServer, conf);
			httpServer.Start();
			int connIdx = 0;
			if (policy.IsHttpEnabled())
			{
				httpAddress = httpServer.GetConnectorAddress(connIdx++);
				conf.Set(DFSConfigKeys.DfsNamenodeHttpAddressKey, NetUtils.GetHostPortString(httpAddress
					));
			}
			if (policy.IsHttpsEnabled())
			{
				httpsAddress = httpServer.GetConnectorAddress(connIdx);
				conf.Set(DFSConfigKeys.DfsNamenodeHttpsAddressKey, NetUtils.GetHostPortString(httpsAddress
					));
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private IDictionary<string, string> GetAuthFilterParams(Configuration conf)
		{
			IDictionary<string, string> @params = new Dictionary<string, string>();
			string principalInConf = conf.Get(DFSConfigKeys.DfsWebAuthenticationKerberosPrincipalKey
				);
			if (principalInConf != null && !principalInConf.IsEmpty())
			{
				@params[DFSConfigKeys.DfsWebAuthenticationKerberosPrincipalKey] = SecurityUtil.GetServerPrincipal
					(principalInConf, bindAddress.GetHostName());
			}
			else
			{
				if (UserGroupInformation.IsSecurityEnabled())
				{
					HttpServer2.Log.Error("WebHDFS and security are enabled, but configuration property '"
						 + DFSConfigKeys.DfsWebAuthenticationKerberosPrincipalKey + "' is not set.");
				}
			}
			string httpKeytab = conf.Get(DFSUtil.GetSpnegoKeytabKey(conf, DFSConfigKeys.DfsNamenodeKeytabFileKey
				));
			if (httpKeytab != null && !httpKeytab.IsEmpty())
			{
				@params[DFSConfigKeys.DfsWebAuthenticationKerberosKeytabKey] = httpKeytab;
			}
			else
			{
				if (UserGroupInformation.IsSecurityEnabled())
				{
					HttpServer2.Log.Error("WebHDFS and security are enabled, but configuration property '"
						 + DFSConfigKeys.DfsWebAuthenticationKerberosKeytabKey + "' is not set.");
				}
			}
			string anonymousAllowed = conf.Get(DFSConfigKeys.DfsWebAuthenticationSimpleAnonymousAllowed
				);
			if (anonymousAllowed != null && !anonymousAllowed.IsEmpty())
			{
				@params[DFSConfigKeys.DfsWebAuthenticationSimpleAnonymousAllowed] = anonymousAllowed;
			}
			return @params;
		}

		/// <exception cref="System.Exception"/>
		internal virtual void Stop()
		{
			if (httpServer != null)
			{
				httpServer.Stop();
			}
		}

		internal virtual IPEndPoint GetHttpAddress()
		{
			return httpAddress;
		}

		internal virtual IPEndPoint GetHttpsAddress()
		{
			return httpsAddress;
		}

		/// <summary>Sets fsimage for use by servlets.</summary>
		/// <param name="fsImage">FSImage to set</param>
		internal virtual void SetFSImage(FSImage fsImage)
		{
			httpServer.SetAttribute(FsimageAttributeKey, fsImage);
		}

		/// <summary>Sets address of namenode for use by servlets.</summary>
		/// <param name="nameNodeAddress">InetSocketAddress to set</param>
		internal virtual void SetNameNodeAddress(IPEndPoint nameNodeAddress)
		{
			httpServer.SetAttribute(NamenodeAddressAttributeKey, NetUtils.GetConnectAddress(nameNodeAddress
				));
		}

		/// <summary>Sets startup progress of namenode for use by servlets.</summary>
		/// <param name="prog">StartupProgress to set</param>
		internal virtual void SetStartupProgress(StartupProgress prog)
		{
			httpServer.SetAttribute(StartupProgressAttributeKey, prog);
		}

		private static void SetupServlets(HttpServer2 httpServer, Configuration conf)
		{
			httpServer.AddInternalServlet("startupProgress", StartupProgressServlet.PathSpec, 
				typeof(StartupProgressServlet));
			httpServer.AddInternalServlet("getDelegationToken", GetDelegationTokenServlet.PathSpec
				, typeof(GetDelegationTokenServlet), true);
			httpServer.AddInternalServlet("renewDelegationToken", RenewDelegationTokenServlet
				.PathSpec, typeof(RenewDelegationTokenServlet), true);
			httpServer.AddInternalServlet("cancelDelegationToken", CancelDelegationTokenServlet
				.PathSpec, typeof(CancelDelegationTokenServlet), true);
			httpServer.AddInternalServlet("fsck", "/fsck", typeof(FsckServlet), true);
			httpServer.AddInternalServlet("imagetransfer", ImageServlet.PathSpec, typeof(ImageServlet
				), true);
			httpServer.AddInternalServlet("listPaths", "/listPaths/*", typeof(ListPathsServlet
				), false);
			httpServer.AddInternalServlet("data", "/data/*", typeof(FileDataServlet), false);
			httpServer.AddInternalServlet("checksum", "/fileChecksum/*", typeof(FileChecksumServlets.RedirectServlet
				), false);
			httpServer.AddInternalServlet("contentSummary", "/contentSummary/*", typeof(ContentSummaryServlet
				), false);
		}

		internal static FSImage GetFsImageFromContext(ServletContext context)
		{
			return (FSImage)context.GetAttribute(FsimageAttributeKey);
		}

		public static NameNode GetNameNodeFromContext(ServletContext context)
		{
			return (NameNode)context.GetAttribute(NamenodeAttributeKey);
		}

		internal static Configuration GetConfFromContext(ServletContext context)
		{
			return (Configuration)context.GetAttribute(JspHelper.CurrentConf);
		}

		public static IPEndPoint GetNameNodeAddressFromContext(ServletContext context)
		{
			return (IPEndPoint)context.GetAttribute(NamenodeAddressAttributeKey);
		}

		/// <summary>Returns StartupProgress associated with ServletContext.</summary>
		/// <param name="context">ServletContext to get</param>
		/// <returns>StartupProgress associated with context</returns>
		internal static StartupProgress GetStartupProgressFromContext(ServletContext context
			)
		{
			return (StartupProgress)context.GetAttribute(StartupProgressAttributeKey);
		}
	}
}
