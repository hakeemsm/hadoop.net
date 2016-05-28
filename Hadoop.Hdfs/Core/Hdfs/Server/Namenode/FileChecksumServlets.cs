using System;
using System.IO;
using Javax.Servlet;
using Javax.Servlet.Http;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Util;
using Org.Znerd.Xmlenc;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>Servlets for file checksum</summary>
	public class FileChecksumServlets
	{
		/// <summary>Redirect file checksum queries to an appropriate datanode.</summary>
		[System.Serializable]
		public class RedirectServlet : DfsServlet
		{
			/// <summary>For java.io.Serializable</summary>
			private const long serialVersionUID = 1L;

			/// <summary>Create a redirection URL</summary>
			/// <exception cref="System.IO.IOException"/>
			private Uri CreateRedirectURL(UserGroupInformation ugi, DatanodeID host, HttpServletRequest
				 request, NameNode nn)
			{
				string hostname = host is DatanodeInfo ? host.GetHostName() : host.GetIpAddr();
				string scheme = request.GetScheme();
				int port = host.GetInfoPort();
				if ("https".Equals(scheme))
				{
					int portObject = (int)GetServletContext().GetAttribute(DFSConfigKeys.DfsDatanodeHttpsPortKey
						);
					if (portObject != null)
					{
						port = portObject;
					}
				}
				string encodedPath = ServletUtil.GetRawPath(request, "/fileChecksum");
				string dtParam = string.Empty;
				if (UserGroupInformation.IsSecurityEnabled())
				{
					string tokenString = ugi.GetTokens().GetEnumerator().Next().EncodeToUrlString();
					dtParam = JspHelper.GetDelegationTokenUrlParam(tokenString);
				}
				string addr = nn.GetNameNodeAddressHostPortString();
				string addrParam = JspHelper.GetUrlParam(JspHelper.NamenodeAddress, addr);
				return new Uri(scheme, hostname, port, "/getFileChecksum" + encodedPath + '?' + "ugi="
					 + ServletUtil.EncodeQueryValue(ugi.GetShortUserName()) + dtParam + addrParam);
			}

			/// <exception cref="Javax.Servlet.ServletException"/>
			/// <exception cref="System.IO.IOException"/>
			protected override void DoGet(HttpServletRequest request, HttpServletResponse response
				)
			{
				ServletContext context = GetServletContext();
				Configuration conf = NameNodeHttpServer.GetConfFromContext(context);
				UserGroupInformation ugi = GetUGI(request, conf);
				NameNode namenode = NameNodeHttpServer.GetNameNodeFromContext(context);
				DatanodeID datanode = NamenodeJspHelper.GetRandomDatanode(namenode);
				try
				{
					response.SendRedirect(CreateRedirectURL(ugi, datanode, request, namenode).ToString
						());
				}
				catch (IOException e)
				{
					response.SendError(400, e.Message);
				}
			}
		}

		/// <summary>Get FileChecksum</summary>
		[System.Serializable]
		public class GetServlet : DfsServlet
		{
			/// <summary>For java.io.Serializable</summary>
			private const long serialVersionUID = 1L;

			/// <exception cref="Javax.Servlet.ServletException"/>
			/// <exception cref="System.IO.IOException"/>
			protected override void DoGet(HttpServletRequest request, HttpServletResponse response
				)
			{
				PrintWriter @out = response.GetWriter();
				string path = ServletUtil.GetDecodedPath(request, "/getFileChecksum");
				XMLOutputter xml = new XMLOutputter(@out, "UTF-8");
				xml.Declaration();
				ServletContext context = GetServletContext();
				DataNode datanode = (DataNode)context.GetAttribute("datanode");
				Configuration conf = new HdfsConfiguration(datanode.GetConf());
				try
				{
					DFSClient dfs = DatanodeJspHelper.GetDFSClient(request, datanode, conf, GetUGI(request
						, conf));
					MD5MD5CRC32FileChecksum checksum = dfs.GetFileChecksum(path, long.MaxValue);
					MD5MD5CRC32FileChecksum.Write(xml, checksum);
				}
				catch (IOException ioe)
				{
					WriteXml(ioe, path, xml);
				}
				catch (Exception e)
				{
					WriteXml(e, path, xml);
				}
				xml.EndDocument();
			}
		}
	}
}
