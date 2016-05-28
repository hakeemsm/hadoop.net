using Javax.Servlet.Http;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs.Security.Token.Delegation;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	internal class NamenodeJspHelper
	{
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		internal static string GetDelegationToken(NamenodeProtocols nn, HttpServletRequest
			 request, Configuration conf, UserGroupInformation ugi)
		{
			Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> token = ugi.DoAs
				(new _PrivilegedExceptionAction_39(nn, ugi));
			return token == null ? null : token.EncodeToUrlString();
		}

		private sealed class _PrivilegedExceptionAction_39 : PrivilegedExceptionAction<Org.Apache.Hadoop.Security.Token.Token
			<DelegationTokenIdentifier>>
		{
			public _PrivilegedExceptionAction_39(NamenodeProtocols nn, UserGroupInformation ugi
				)
			{
				this.nn = nn;
				this.ugi = ugi;
			}

			/// <exception cref="System.IO.IOException"/>
			public Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> Run()
			{
				return nn.GetDelegationToken(new Text(ugi.GetUserName()));
			}

			private readonly NamenodeProtocols nn;

			private readonly UserGroupInformation ugi;
		}

		/// <returns>a randomly chosen datanode.</returns>
		internal static DatanodeDescriptor GetRandomDatanode(NameNode namenode)
		{
			return (DatanodeDescriptor)namenode.GetNamesystem().GetBlockManager().GetDatanodeManager
				().GetNetworkTopology().ChooseRandom(NodeBase.Root);
		}
	}
}
