using System;
using System.IO;
using Javax.Servlet.Http;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>Redirect queries about the hosted filesystem to an appropriate datanode.
	/// 	</summary>
	/// <seealso cref="Org.Apache.Hadoop.Hdfs.Web.HftpFileSystem"/>
	[System.Serializable]
	public class FileDataServlet : DfsServlet
	{
		/// <summary>For java.io.Serializable</summary>
		private const long serialVersionUID = 1L;

		/// <summary>Create a redirection URL</summary>
		/// <exception cref="System.IO.IOException"/>
		private Uri CreateRedirectURL(string path, string encodedPath, HdfsFileStatus status
			, UserGroupInformation ugi, ClientProtocol nnproxy, HttpServletRequest request, 
			string dt)
		{
			string scheme = request.GetScheme();
			LocatedBlocks blks = nnproxy.GetBlockLocations(status.GetFullPath(new Path(path))
				.ToUri().GetPath(), 0, 1);
			Configuration conf = NameNodeHttpServer.GetConfFromContext(GetServletContext());
			DatanodeID host = PickSrcDatanode(blks, status, conf);
			string hostname;
			if (host is DatanodeInfo)
			{
				hostname = host.GetHostName();
			}
			else
			{
				hostname = host.GetIpAddr();
			}
			int port = "https".Equals(scheme) ? host.GetInfoSecurePort() : host.GetInfoPort();
			string dtParam = string.Empty;
			if (dt != null)
			{
				dtParam = JspHelper.GetDelegationTokenUrlParam(dt);
			}
			// Add namenode address to the url params
			NameNode nn = NameNodeHttpServer.GetNameNodeFromContext(GetServletContext());
			string addr = nn.GetNameNodeAddressHostPortString();
			string addrParam = JspHelper.GetUrlParam(JspHelper.NamenodeAddress, addr);
			return new Uri(scheme, hostname, port, "/streamFile" + encodedPath + '?' + "ugi="
				 + ServletUtil.EncodeQueryValue(ugi.GetShortUserName()) + dtParam + addrParam);
		}

		/// <summary>Select a datanode to service this request.</summary>
		/// <remarks>
		/// Select a datanode to service this request.
		/// Currently, this looks at no more than the first five blocks of a file,
		/// selecting a datanode randomly from the most represented.
		/// </remarks>
		/// <param name="conf"></param>
		/// <exception cref="System.IO.IOException"/>
		private DatanodeID PickSrcDatanode(LocatedBlocks blks, HdfsFileStatus i, Configuration
			 conf)
		{
			if (i.GetLen() == 0 || blks.GetLocatedBlocks().Count <= 0)
			{
				// pick a random datanode
				NameNode nn = NameNodeHttpServer.GetNameNodeFromContext(GetServletContext());
				return NamenodeJspHelper.GetRandomDatanode(nn);
			}
			return JspHelper.BestNode(blks, conf);
		}

		/// <summary>Service a GET request as described below.</summary>
		/// <remarks>
		/// Service a GET request as described below.
		/// Request:
		/// <c>GET http://&lt;nn&gt;:&lt;port&gt;/data[/&lt;path&gt;] HTTP/1.1</c>
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		protected override void DoGet(HttpServletRequest request, HttpServletResponse response
			)
		{
			Configuration conf = NameNodeHttpServer.GetConfFromContext(GetServletContext());
			UserGroupInformation ugi = GetUGI(request, conf);
			try
			{
				ugi.DoAs(new _PrivilegedExceptionAction_116(this, request, response, ugi));
			}
			catch (IOException e)
			{
				response.SendError(400, e.Message);
			}
			catch (Exception e)
			{
				response.SendError(400, e.Message);
			}
		}

		private sealed class _PrivilegedExceptionAction_116 : PrivilegedExceptionAction<Void
			>
		{
			public _PrivilegedExceptionAction_116(FileDataServlet _enclosing, HttpServletRequest
				 request, HttpServletResponse response, UserGroupInformation ugi)
			{
				this._enclosing = _enclosing;
				this.request = request;
				this.response = response;
				this.ugi = ugi;
			}

			/// <exception cref="System.IO.IOException"/>
			public Void Run()
			{
				ClientProtocol nn = this._enclosing.CreateNameNodeProxy();
				string path = ServletUtil.GetDecodedPath(request, "/data");
				string encodedPath = ServletUtil.GetRawPath(request, "/data");
				string delegationToken = request.GetParameter(JspHelper.DelegationParameterName);
				HdfsFileStatus info = nn.GetFileInfo(path);
				if (info != null && !info.IsDir())
				{
					response.SendRedirect(this._enclosing.CreateRedirectURL(path, encodedPath, info, 
						ugi, nn, request, delegationToken).ToString());
				}
				else
				{
					if (info == null)
					{
						response.SendError(400, "File not found " + path);
					}
					else
					{
						response.SendError(400, path + ": is a directory");
					}
				}
				return null;
			}

			private readonly FileDataServlet _enclosing;

			private readonly HttpServletRequest request;

			private readonly HttpServletResponse response;

			private readonly UserGroupInformation ugi;
		}
	}
}
