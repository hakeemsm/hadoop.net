using System.Collections.Generic;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Token.Delegation;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Security.Token.Delegation
{
	/// <summary>A delegation token that is specialized for HDFS</summary>
	public class DelegationTokenSelector : AbstractDelegationTokenSelector<DelegationTokenIdentifier
		>
	{
		public const string ServiceNameKey = "hdfs.service.host_";

		/// <summary>Select the delegation token for hdfs.</summary>
		/// <remarks>
		/// Select the delegation token for hdfs.  The port will be rewritten to
		/// the port of hdfs.service.host_$nnAddr, or the default rpc namenode port.
		/// This method should only be called by non-hdfs filesystems that do not
		/// use the rpc port to acquire tokens.  Ex. webhdfs, hftp
		/// </remarks>
		/// <param name="nnUri">of the remote namenode</param>
		/// <param name="tokens">as a collection</param>
		/// <param name="conf">hadoop configuration</param>
		/// <returns>Token</returns>
		public virtual Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> 
			SelectToken(URI nnUri, ICollection<Org.Apache.Hadoop.Security.Token.Token<object
			>> tokens, Configuration conf)
		{
			// this guesses the remote cluster's rpc service port.
			// the current token design assumes it's the same as the local cluster's
			// rpc port unless a config key is set.  there should be a way to automatic
			// and correctly determine the value
			Text serviceName = SecurityUtil.BuildTokenService(nnUri);
			string nnServiceName = conf.Get(ServiceNameKey + serviceName);
			int nnRpcPort = NameNode.DefaultPort;
			if (nnServiceName != null)
			{
				nnRpcPort = NetUtils.CreateSocketAddr(nnServiceName, nnRpcPort).Port;
			}
			// use original hostname from the uri to avoid unintentional host resolving
			serviceName = SecurityUtil.BuildTokenService(NetUtils.CreateSocketAddrForHost(nnUri
				.GetHost(), nnRpcPort));
			return SelectToken(serviceName, tokens);
		}

		public DelegationTokenSelector()
			: base(DelegationTokenIdentifier.HdfsDelegationKind)
		{
		}
	}
}
