using System;
using System.IO;
using Javax.Servlet.Http;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Util;
using Org.Znerd.Xmlenc;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>Servlets for file checksum</summary>
	[System.Serializable]
	public class ContentSummaryServlet : DfsServlet
	{
		/// <summary>For java.io.Serializable</summary>
		private const long serialVersionUID = 1L;

		/// <exception cref="Javax.Servlet.ServletException"/>
		/// <exception cref="System.IO.IOException"/>
		protected override void DoGet(HttpServletRequest request, HttpServletResponse response
			)
		{
			Configuration conf = (Configuration)GetServletContext().GetAttribute(JspHelper.CurrentConf
				);
			UserGroupInformation ugi = GetUGI(request, conf);
			try
			{
				ugi.DoAs(new _PrivilegedExceptionAction_50(this, request, response));
			}
			catch (Exception e)
			{
				//get content summary
				//write xml
				throw new IOException(e);
			}
		}

		private sealed class _PrivilegedExceptionAction_50 : PrivilegedExceptionAction<Void
			>
		{
			public _PrivilegedExceptionAction_50(ContentSummaryServlet _enclosing, HttpServletRequest
				 request, HttpServletResponse response)
			{
				this._enclosing = _enclosing;
				this.request = request;
				this.response = response;
			}

			/// <exception cref="System.Exception"/>
			public Void Run()
			{
				string path = ServletUtil.GetDecodedPath(request, "/contentSummary");
				PrintWriter @out = response.GetWriter();
				XMLOutputter xml = new XMLOutputter(@out, "UTF-8");
				xml.Declaration();
				try
				{
					ClientProtocol nnproxy = this._enclosing.CreateNameNodeProxy();
					ContentSummary cs = nnproxy.GetContentSummary(path);
					xml.StartTag(typeof(ContentSummary).FullName);
					if (cs != null)
					{
						xml.Attribute("length", string.Empty + cs.GetLength());
						xml.Attribute("fileCount", string.Empty + cs.GetFileCount());
						xml.Attribute("directoryCount", string.Empty + cs.GetDirectoryCount());
						xml.Attribute("quota", string.Empty + cs.GetQuota());
						xml.Attribute("spaceConsumed", string.Empty + cs.GetSpaceConsumed());
						xml.Attribute("spaceQuota", string.Empty + cs.GetSpaceQuota());
					}
					xml.EndTag();
				}
				catch (IOException ioe)
				{
					this._enclosing.WriteXml(ioe, path, xml);
				}
				xml.EndDocument();
				return null;
			}

			private readonly ContentSummaryServlet _enclosing;

			private readonly HttpServletRequest request;

			private readonly HttpServletResponse response;
		}
	}
}
