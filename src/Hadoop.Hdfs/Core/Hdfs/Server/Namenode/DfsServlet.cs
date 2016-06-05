using System;
using System.Net;
using Javax.Servlet;
using Javax.Servlet.Http;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Security;
using Org.Znerd.Xmlenc;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>A base class for the servlets in DFS.</summary>
	[System.Serializable]
	internal abstract class DfsServlet : HttpServlet
	{
		/// <summary>For java.io.Serializable</summary>
		private const long serialVersionUID = 1L;

		internal static readonly Log Log = LogFactory.GetLog(typeof(DfsServlet).GetCanonicalName
			());

		/// <summary>Write the object to XML format</summary>
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void WriteXml(Exception except, string path, XMLOutputter
			 doc)
		{
			doc.StartTag(typeof(RemoteException).Name);
			doc.Attribute("path", path);
			if (except is RemoteException)
			{
				doc.Attribute("class", ((RemoteException)except).GetClassName());
			}
			else
			{
				doc.Attribute("class", except.GetType().FullName);
			}
			string msg = except.GetLocalizedMessage();
			int i = msg.IndexOf("\n");
			if (i >= 0)
			{
				msg = Sharpen.Runtime.Substring(msg, 0, i);
			}
			doc.Attribute("message", Sharpen.Runtime.Substring(msg, msg.IndexOf(":") + 1).Trim
				());
			doc.EndTag();
		}

		/// <summary>
		/// Create a
		/// <see cref="NameNode"/>
		/// proxy from the current
		/// <see cref="Javax.Servlet.ServletContext"/>
		/// .
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual ClientProtocol CreateNameNodeProxy()
		{
			ServletContext context = GetServletContext();
			// if we are running in the Name Node, use it directly rather than via 
			// rpc
			NameNode nn = NameNodeHttpServer.GetNameNodeFromContext(context);
			if (nn != null)
			{
				return nn.GetRpcServer();
			}
			IPEndPoint nnAddr = NameNodeHttpServer.GetNameNodeAddressFromContext(context);
			Configuration conf = new HdfsConfiguration(NameNodeHttpServer.GetConfFromContext(
				context));
			return NameNodeProxies.CreateProxy<ClientProtocol>(conf, NameNode.GetUri(nnAddr))
				.GetProxy();
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual UserGroupInformation GetUGI(HttpServletRequest request
			, Configuration conf)
		{
			return JspHelper.GetUGI(GetServletContext(), request, conf);
		}
	}
}
