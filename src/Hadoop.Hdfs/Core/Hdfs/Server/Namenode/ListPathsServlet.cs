using System;
using System.Collections.Generic;
using System.IO;
using Javax.Servlet.Http;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Web;
using Org.Apache.Hadoop.Util;
using Org.Znerd.Xmlenc;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>Obtain meta-information about a filesystem.</summary>
	/// <seealso cref="Org.Apache.Hadoop.Hdfs.Web.HftpFileSystem"/>
	[System.Serializable]
	public class ListPathsServlet : DfsServlet
	{
		/// <summary>For java.io.Serializable</summary>
		private const long serialVersionUID = 1L;

		private sealed class _ThreadLocal_56 : ThreadLocal<SimpleDateFormat>
		{
			public _ThreadLocal_56()
			{
			}

			protected override SimpleDateFormat InitialValue()
			{
				return HftpFileSystem.GetDateFormat();
			}
		}

		public static readonly ThreadLocal<SimpleDateFormat> df = new _ThreadLocal_56();

		/// <summary>Write a node to output.</summary>
		/// <remarks>
		/// Write a node to output.
		/// Node information includes path, modification, permission, owner and group.
		/// For files, it also includes size, replication and block-size.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		internal static void WriteInfo(Path fullpath, HdfsFileStatus i, XMLOutputter doc)
		{
			SimpleDateFormat ldf = df.Get();
			doc.StartTag(i.IsDir() ? "directory" : "file");
			doc.Attribute("path", fullpath.ToUri().GetPath());
			doc.Attribute("modified", ldf.Format(Sharpen.Extensions.CreateDate(i.GetModificationTime
				())));
			doc.Attribute("accesstime", ldf.Format(Sharpen.Extensions.CreateDate(i.GetAccessTime
				())));
			if (!i.IsDir())
			{
				doc.Attribute("size", i.GetLen().ToString());
				doc.Attribute("replication", i.GetReplication().ToString());
				doc.Attribute("blocksize", i.GetBlockSize().ToString());
			}
			doc.Attribute("permission", (i.IsDir() ? "d" : "-") + i.GetPermission());
			doc.Attribute("owner", i.GetOwner());
			doc.Attribute("group", i.GetGroup());
			doc.EndTag();
		}

		/// <summary>Build a map from the query string, setting values and defaults.</summary>
		protected internal virtual IDictionary<string, string> BuildRoot(HttpServletRequest
			 request, XMLOutputter doc)
		{
			string path = ServletUtil.GetDecodedPath(request, "/listPaths");
			string exclude = request.GetParameter("exclude") != null ? request.GetParameter("exclude"
				) : string.Empty;
			string filter = request.GetParameter("filter") != null ? request.GetParameter("filter"
				) : ".*";
			bool recur = request.GetParameter("recursive") != null && "yes".Equals(request.GetParameter
				("recursive"));
			IDictionary<string, string> root = new Dictionary<string, string>();
			root["path"] = path;
			root["recursive"] = recur ? "yes" : "no";
			root["filter"] = filter;
			root["exclude"] = exclude;
			root["time"] = df.Get().Format(new DateTime());
			root["version"] = VersionInfo.GetVersion();
			return root;
		}

		/// <summary>Service a GET request as described below.</summary>
		/// <remarks>
		/// Service a GET request as described below.
		/// Request:
		/// <c>GET http://&lt;nn&gt;:&lt;port&gt;/listPaths[/&lt;path&gt;][&lt;?option&gt;[&option]*] HTTP/1.1
		/// 	</c>
		/// Where <i>option</i> (default) in:
		/// recursive (&quot;no&quot;)
		/// filter (&quot;.*&quot;)
		/// exclude (&quot;\..*\.crc&quot;)
		/// Response: A flat list of files/directories in the following format:
		/// <c>
		/// &lt;listing path="..." recursive="(yes|no)" filter="..."
		/// time="yyyy-MM-dd hh:mm:ss UTC" version="..."&gt;
		/// &lt;directory path="..." modified="yyyy-MM-dd hh:mm:ss"/&gt;
		/// &lt;file path="..." modified="yyyy-MM-dd'T'hh:mm:ssZ" accesstime="yyyy-MM-dd'T'hh:mm:ssZ"
		/// blocksize="..."
		/// replication="..." size="..."/&gt;
		/// &lt;/listing&gt;
		/// </c>
		/// </remarks>
		/// <exception cref="Javax.Servlet.ServletException"/>
		/// <exception cref="System.IO.IOException"/>
		protected override void DoGet(HttpServletRequest request, HttpServletResponse response
			)
		{
			PrintWriter @out = response.GetWriter();
			XMLOutputter doc = new XMLOutputter(@out, "UTF-8");
			IDictionary<string, string> root = BuildRoot(request, doc);
			string path = root["path"];
			string filePath = ServletUtil.GetDecodedPath(request, "/listPaths");
			try
			{
				bool recur = "yes".Equals(root["recursive"]);
				Sharpen.Pattern filter = Sharpen.Pattern.Compile(root["filter"]);
				Sharpen.Pattern exclude = Sharpen.Pattern.Compile(root["exclude"]);
				Configuration conf = (Configuration)GetServletContext().GetAttribute(JspHelper.CurrentConf
					);
				GetUGI(request, conf).DoAs(new _PrivilegedExceptionAction_149(this, doc, root, filePath
					, path, exclude, filter, recur));
			}
			catch (IOException ioe)
			{
				WriteXml(ioe, path, doc);
			}
			catch (Exception e)
			{
				Log.Warn("ListPathServlet encountered InterruptedException", e);
				response.SendError(400, e.Message);
			}
			finally
			{
				if (doc != null)
				{
					doc.EndDocument();
				}
				if (@out != null)
				{
					@out.Close();
				}
			}
		}

		private sealed class _PrivilegedExceptionAction_149 : PrivilegedExceptionAction<Void
			>
		{
			public _PrivilegedExceptionAction_149(ListPathsServlet _enclosing, XMLOutputter doc
				, IDictionary<string, string> root, string filePath, string path, Sharpen.Pattern
				 exclude, Sharpen.Pattern filter, bool recur)
			{
				this._enclosing = _enclosing;
				this.doc = doc;
				this.root = root;
				this.filePath = filePath;
				this.path = path;
				this.exclude = exclude;
				this.filter = filter;
				this.recur = recur;
			}

			/// <exception cref="System.IO.IOException"/>
			public Void Run()
			{
				ClientProtocol nn = this._enclosing.CreateNameNodeProxy();
				doc.Declaration();
				doc.StartTag("listing");
				foreach (KeyValuePair<string, string> m in root)
				{
					doc.Attribute(m.Key, m.Value);
				}
				HdfsFileStatus @base = nn.GetFileInfo(filePath);
				if ((@base != null) && @base.IsDir())
				{
					ListPathsServlet.WriteInfo(@base.GetFullPath(new Path(path)), @base, doc);
				}
				Stack<string> pathstack = new Stack<string>();
				pathstack.Push(path);
				while (!pathstack.Empty())
				{
					string p = pathstack.Pop();
					try
					{
						byte[] lastReturnedName = HdfsFileStatus.EmptyName;
						DirectoryListing thisListing;
						do
						{
							System.Diagnostics.Debug.Assert(lastReturnedName != null);
							thisListing = nn.GetListing(p, lastReturnedName, false);
							if (thisListing == null)
							{
								if (lastReturnedName.Length == 0)
								{
									DfsServlet.Log.Warn("ListPathsServlet - Path " + p + " does not exist");
								}
								break;
							}
							HdfsFileStatus[] listing = thisListing.GetPartialListing();
							foreach (HdfsFileStatus i in listing)
							{
								Path fullpath = i.GetFullPath(new Path(p));
								string localName = fullpath.GetName();
								if (exclude.Matcher(localName).Matches() || !filter.Matcher(localName).Matches())
								{
									continue;
								}
								if (recur && i.IsDir())
								{
									pathstack.Push(new Path(p, localName).ToUri().GetPath());
								}
								ListPathsServlet.WriteInfo(fullpath, i, doc);
							}
							lastReturnedName = thisListing.GetLastName();
						}
						while (thisListing.HasMore());
					}
					catch (IOException re)
					{
						this._enclosing.WriteXml(re, p, doc);
					}
				}
				return null;
			}

			private readonly ListPathsServlet _enclosing;

			private readonly XMLOutputter doc;

			private readonly IDictionary<string, string> root;

			private readonly string filePath;

			private readonly string path;

			private readonly Sharpen.Pattern exclude;

			private readonly Sharpen.Pattern filter;

			private readonly bool recur;
		}
	}
}
