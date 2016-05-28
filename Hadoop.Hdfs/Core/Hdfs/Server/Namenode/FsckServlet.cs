using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using Javax.Servlet;
using Javax.Servlet.Http;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Org.Apache.Hadoop.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>This class is used in Namesystem's web server to do fsck on namenode.</summary>
	[System.Serializable]
	public class FsckServlet : DfsServlet
	{
		/// <summary>for java.io.Serializable</summary>
		private const long serialVersionUID = 1L;

		/// <summary>Handle fsck request</summary>
		/// <exception cref="System.IO.IOException"/>
		protected override void DoGet(HttpServletRequest request, HttpServletResponse response
			)
		{
			IDictionary<string, string[]> pmap = request.GetParameterMap();
			PrintWriter @out = response.GetWriter();
			IPAddress remoteAddress = Sharpen.Extensions.GetAddressByName(request.GetRemoteAddr
				());
			ServletContext context = GetServletContext();
			Configuration conf = NameNodeHttpServer.GetConfFromContext(context);
			UserGroupInformation ugi = GetUGI(request, conf);
			try
			{
				ugi.DoAs(new _PrivilegedExceptionAction_58(context, conf, pmap, @out, remoteAddress
					));
			}
			catch (Exception e)
			{
				response.SendError(400, e.Message);
			}
		}

		private sealed class _PrivilegedExceptionAction_58 : PrivilegedExceptionAction<object
			>
		{
			public _PrivilegedExceptionAction_58(ServletContext context, Configuration conf, 
				IDictionary<string, string[]> pmap, PrintWriter @out, IPAddress remoteAddress)
			{
				this.context = context;
				this.conf = conf;
				this.pmap = pmap;
				this.@out = @out;
				this.remoteAddress = remoteAddress;
			}

			/// <exception cref="System.Exception"/>
			public object Run()
			{
				NameNode nn = NameNodeHttpServer.GetNameNodeFromContext(context);
				FSNamesystem namesystem = nn.GetNamesystem();
				BlockManager bm = namesystem.GetBlockManager();
				int totalDatanodes = namesystem.GetNumberOfDatanodes(HdfsConstants.DatanodeReportType
					.Live);
				new NamenodeFsck(conf, nn, bm.GetDatanodeManager().GetNetworkTopology(), pmap, @out
					, totalDatanodes, remoteAddress).Fsck();
				return null;
			}

			private readonly ServletContext context;

			private readonly Configuration conf;

			private readonly IDictionary<string, string[]> pmap;

			private readonly PrintWriter @out;

			private readonly IPAddress remoteAddress;
		}
	}
}
