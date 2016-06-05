using Javax.Servlet.Http;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode
{
	public class DatanodeJspHelper
	{
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		private static DFSClient GetDFSClient(UserGroupInformation user, string addr, Configuration
			 conf)
		{
			return user.DoAs(new _PrivilegedExceptionAction_39(addr, conf));
		}

		private sealed class _PrivilegedExceptionAction_39 : PrivilegedExceptionAction<DFSClient
			>
		{
			public _PrivilegedExceptionAction_39(string addr, Configuration conf)
			{
				this.addr = addr;
				this.conf = conf;
			}

			/// <exception cref="System.IO.IOException"/>
			public DFSClient Run()
			{
				return new DFSClient(NetUtils.CreateSocketAddr(addr), conf);
			}

			private readonly string addr;

			private readonly Configuration conf;
		}

		/// <summary>Get DFSClient for a namenode corresponding to the BPID from a datanode</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public static DFSClient GetDFSClient(HttpServletRequest request, DataNode datanode
			, Configuration conf, UserGroupInformation ugi)
		{
			string nnAddr = request.GetParameter(JspHelper.NamenodeAddress);
			return GetDFSClient(ugi, nnAddr, conf);
		}
	}
}
