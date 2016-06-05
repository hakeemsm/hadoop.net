using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Security.Token.Delegation;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Server.Namenode.HA;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Token;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	public class HAUtil
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.HAUtil
			));

		private static readonly DelegationTokenSelector tokenSelector = new DelegationTokenSelector
			();

		private HAUtil()
		{
		}

		/* Hidden constructor */
		/// <summary>Returns true if HA for namenode is configured for the given nameservice</summary>
		/// <param name="conf">Configuration</param>
		/// <param name="nsId">nameservice, or null if no federated NS is configured</param>
		/// <returns>true if HA is configured in the configuration; else false.</returns>
		public static bool IsHAEnabled(Configuration conf, string nsId)
		{
			IDictionary<string, IDictionary<string, IPEndPoint>> addresses = DFSUtil.GetHaNnRpcAddresses
				(conf);
			if (addresses == null)
			{
				return false;
			}
			IDictionary<string, IPEndPoint> nnMap = addresses[nsId];
			return nnMap != null && nnMap.Count > 1;
		}

		/// <summary>Returns true if HA is using a shared edits directory.</summary>
		/// <param name="conf">Configuration</param>
		/// <returns>true if HA config is using a shared edits dir, false otherwise.</returns>
		public static bool UsesSharedEditsDir(Configuration conf)
		{
			return null != conf.Get(DFSConfigKeys.DfsNamenodeSharedEditsDirKey);
		}

		/// <summary>
		/// Get the namenode Id by matching the
		/// <c>addressKey</c>
		/// with the the address of the local node.
		/// If
		/// <see cref="DFSConfigKeys.DfsHaNamenodeIdKey"/>
		/// is not specifically
		/// configured, this method determines the namenode Id by matching the local
		/// node's address with the configured addresses. When a match is found, it
		/// returns the namenode Id from the corresponding configuration key.
		/// </summary>
		/// <param name="conf">Configuration</param>
		/// <returns>namenode Id on success, null on failure.</returns>
		/// <exception cref="Org.Apache.Hadoop.HadoopIllegalArgumentException">on error</exception>
		public static string GetNameNodeId(Configuration conf, string nsId)
		{
			string namenodeId = conf.GetTrimmed(DFSConfigKeys.DfsHaNamenodeIdKey);
			if (namenodeId != null)
			{
				return namenodeId;
			}
			string[] suffixes = DFSUtil.GetSuffixIDs(conf, DFSConfigKeys.DfsNamenodeRpcAddressKey
				, nsId, null, DFSUtil.LocalAddressMatcher);
			if (suffixes == null)
			{
				string msg = "Configuration " + DFSConfigKeys.DfsNamenodeRpcAddressKey + " must be suffixed with nameservice and namenode ID for HA "
					 + "configuration.";
				throw new HadoopIllegalArgumentException(msg);
			}
			return suffixes[1];
		}

		/// <summary>
		/// Similar to
		/// <see cref="DFSUtil.GetNameServiceIdFromAddress(Org.Apache.Hadoop.Conf.Configuration, System.Net.IPEndPoint, string[])
		/// 	"/>
		/// </summary>
		public static string GetNameNodeIdFromAddress(Configuration conf, IPEndPoint address
			, params string[] keys)
		{
			// Configuration with a single namenode and no nameserviceId
			string[] ids = DFSUtil.GetSuffixIDs(conf, address, keys);
			if (ids != null && ids.Length > 1)
			{
				return ids[1];
			}
			return null;
		}

		/// <summary>Get the NN ID of the other node in an HA setup.</summary>
		/// <param name="conf">the configuration of this node</param>
		/// <returns>the NN ID of the other node in this nameservice</returns>
		public static string GetNameNodeIdOfOtherNode(Configuration conf, string nsId)
		{
			Preconditions.CheckArgument(nsId != null, "Could not determine namespace id. Please ensure that this "
				 + "machine is one of the machines listed as a NN RPC address, " + "or configure "
				 + DFSConfigKeys.DfsNameserviceId);
			ICollection<string> nnIds = DFSUtil.GetNameNodeIds(conf, nsId);
			string myNNId = conf.Get(DFSConfigKeys.DfsHaNamenodeIdKey);
			Preconditions.CheckArgument(nnIds != null, "Could not determine namenode ids in namespace '%s'. "
				 + "Please configure " + DFSUtil.AddKeySuffixes(DFSConfigKeys.DfsHaNamenodesKeyPrefix
				, nsId), nsId);
			Preconditions.CheckArgument(nnIds.Count == 2, "Expected exactly 2 NameNodes in namespace '%s'. "
				 + "Instead, got only %s (NN ids were '%s'", nsId, nnIds.Count, Joiner.On("','")
				.Join(nnIds));
			Preconditions.CheckState(myNNId != null && !myNNId.IsEmpty(), "Could not determine own NN ID in namespace '%s'. Please "
				 + "ensure that this node is one of the machines listed as an " + "NN RPC address, or configure "
				 + DFSConfigKeys.DfsHaNamenodeIdKey, nsId);
			AList<string> nnSet = Lists.NewArrayList(nnIds);
			nnSet.Remove(myNNId);
			System.Diagnostics.Debug.Assert(nnSet.Count == 1);
			return nnSet[0];
		}

		/// <summary>
		/// Given the configuration for this node, return a Configuration object for
		/// the other node in an HA setup.
		/// </summary>
		/// <param name="myConf">the configuration of this node</param>
		/// <returns>the configuration of the other node in an HA setup</returns>
		public static Configuration GetConfForOtherNode(Configuration myConf)
		{
			string nsId = DFSUtil.GetNamenodeNameServiceId(myConf);
			string otherNn = GetNameNodeIdOfOtherNode(myConf, nsId);
			// Look up the address of the active NN.
			Configuration confForOtherNode = new Configuration(myConf);
			NameNode.InitializeGenericKeys(confForOtherNode, nsId, otherNn);
			return confForOtherNode;
		}

		/// <summary>This is used only by tests at the moment.</summary>
		/// <returns>true if the NN should allow read operations while in standby mode.</returns>
		public static bool ShouldAllowStandbyReads(Configuration conf)
		{
			return conf.GetBoolean("dfs.ha.allow.stale.reads", false);
		}

		public static void SetAllowStandbyReads(Configuration conf, bool val)
		{
			conf.SetBoolean("dfs.ha.allow.stale.reads", val);
		}

		/// <returns>true if the given nameNodeUri appears to be a logical URI.</returns>
		public static bool IsLogicalUri(Configuration conf, URI nameNodeUri)
		{
			string host = nameNodeUri.GetHost();
			// A logical name must be one of the service IDs.
			return DFSUtil.GetNameServiceIds(conf).Contains(host);
		}

		/// <summary>
		/// Check whether the client has a failover proxy provider configured
		/// for the namenode/nameservice.
		/// </summary>
		/// <param name="conf">Configuration</param>
		/// <param name="nameNodeUri">The URI of namenode</param>
		/// <returns>true if failover is configured.</returns>
		public static bool IsClientFailoverConfigured(Configuration conf, URI nameNodeUri
			)
		{
			string host = nameNodeUri.GetHost();
			string configKey = DFSConfigKeys.DfsClientFailoverProxyProviderKeyPrefix + "." + 
				host;
			return conf.Get(configKey) != null;
		}

		/// <summary>
		/// Check whether logical URI is needed for the namenode and
		/// the corresponding failover proxy provider in the config.
		/// </summary>
		/// <param name="conf">Configuration</param>
		/// <param name="nameNodeUri">The URI of namenode</param>
		/// <returns>true if logical URI is needed. false, if not needed.</returns>
		/// <exception cref="System.IO.IOException">most likely due to misconfiguration.</exception>
		public static bool UseLogicalUri(Configuration conf, URI nameNodeUri)
		{
			// Create the proxy provider. Actual proxy is not created.
			AbstractNNFailoverProxyProvider<ClientProtocol> provider = NameNodeProxies.CreateFailoverProxyProvider
				<ClientProtocol>(conf, nameNodeUri, false, null);
			// No need to use logical URI since failover is not configured.
			if (provider == null)
			{
				return false;
			}
			// Check whether the failover proxy provider uses logical URI.
			return provider.UseLogicalURI();
		}

		/// <summary>Parse the file system URI out of the provided token.</summary>
		public static URI GetServiceUriFromToken<_T0>(string scheme, Org.Apache.Hadoop.Security.Token.Token
			<_T0> token)
			where _T0 : TokenIdentifier
		{
			string tokStr = token.GetService().ToString();
			string prefix = BuildTokenServicePrefixForLogicalUri(scheme);
			if (tokStr.StartsWith(prefix))
			{
				tokStr = tokStr.ReplaceFirst(prefix, string.Empty);
			}
			return URI.Create(scheme + "://" + tokStr);
		}

		/// <summary>
		/// Get the service name used in the delegation token for the given logical
		/// HA service.
		/// </summary>
		/// <param name="uri">the logical URI of the cluster</param>
		/// <param name="scheme">the scheme of the corresponding FileSystem</param>
		/// <returns>the service name</returns>
		public static Text BuildTokenServiceForLogicalUri(URI uri, string scheme)
		{
			return new Text(BuildTokenServicePrefixForLogicalUri(scheme) + uri.GetHost());
		}

		/// <returns>
		/// true if this token corresponds to a logical nameservice
		/// rather than a specific namenode.
		/// </returns>
		public static bool IsTokenForLogicalUri<_T0>(Org.Apache.Hadoop.Security.Token.Token
			<_T0> token)
			where _T0 : TokenIdentifier
		{
			return token.GetService().ToString().StartsWith(HdfsConstants.HaDtServicePrefix);
		}

		public static string BuildTokenServicePrefixForLogicalUri(string scheme)
		{
			return HdfsConstants.HaDtServicePrefix + scheme + ":";
		}

		/// <summary>
		/// Locate a delegation token associated with the given HA cluster URI, and if
		/// one is found, clone it to also represent the underlying namenode address.
		/// </summary>
		/// <param name="ugi">the UGI to modify</param>
		/// <param name="haUri">the logical URI for the cluster</param>
		/// <param name="nnAddrs">
		/// collection of NNs in the cluster to which the token
		/// applies
		/// </param>
		public static void CloneDelegationTokenForLogicalUri(UserGroupInformation ugi, URI
			 haUri, ICollection<IPEndPoint> nnAddrs)
		{
			// this cloning logic is only used by hdfs
			Text haService = Org.Apache.Hadoop.Hdfs.HAUtil.BuildTokenServiceForLogicalUri(haUri
				, HdfsConstants.HdfsUriScheme);
			Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> haToken = tokenSelector
				.SelectToken(haService, ugi.GetTokens());
			if (haToken != null)
			{
				foreach (IPEndPoint singleNNAddr in nnAddrs)
				{
					// this is a minor hack to prevent physical HA tokens from being
					// exposed to the user via UGI.getCredentials(), otherwise these
					// cloned tokens may be inadvertently propagated to jobs
					Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> specificToken = 
						new Token.PrivateToken<DelegationTokenIdentifier>(haToken);
					SecurityUtil.SetTokenService(specificToken, singleNNAddr);
					Text alias = new Text(BuildTokenServicePrefixForLogicalUri(HdfsConstants.HdfsUriScheme
						) + "//" + specificToken.GetService());
					ugi.AddToken(alias, specificToken);
					Log.Debug("Mapped HA service delegation token for logical URI " + haUri + " to namenode "
						 + singleNNAddr);
				}
			}
			else
			{
				Log.Debug("No HA service delegation token found for logical URI " + haUri);
			}
		}

		/// <summary>Get the internet address of the currently-active NN.</summary>
		/// <remarks>
		/// Get the internet address of the currently-active NN. This should rarely be
		/// used, since callers of this method who connect directly to the NN using the
		/// resulting InetSocketAddress will not be able to connect to the active NN if
		/// a failover were to occur after this method has been called.
		/// </remarks>
		/// <param name="fs">the file system to get the active address of.</param>
		/// <returns>the internet address of the currently-active NN.</returns>
		/// <exception cref="System.IO.IOException">if an error occurs while resolving the active NN.
		/// 	</exception>
		public static IPEndPoint GetAddressOfActive(FileSystem fs)
		{
			if (!(fs is DistributedFileSystem))
			{
				throw new ArgumentException("FileSystem " + fs + " is not a DFS.");
			}
			// force client address resolution.
			fs.Exists(new Path("/"));
			DistributedFileSystem dfs = (DistributedFileSystem)fs;
			DFSClient dfsClient = dfs.GetClient();
			return RPC.GetServerAddress(dfsClient.GetNamenode());
		}

		/// <summary>Get an RPC proxy for each NN in an HA nameservice.</summary>
		/// <remarks>
		/// Get an RPC proxy for each NN in an HA nameservice. Used when a given RPC
		/// call should be made on every NN in an HA nameservice, not just the active.
		/// </remarks>
		/// <param name="conf">configuration</param>
		/// <param name="nsId">the nameservice to get all of the proxies for.</param>
		/// <returns>a list of RPC proxies for each NN in the nameservice.</returns>
		/// <exception cref="System.IO.IOException">in the event of error.</exception>
		public static IList<ClientProtocol> GetProxiesForAllNameNodesInNameservice(Configuration
			 conf, string nsId)
		{
			IList<NameNodeProxies.ProxyAndInfo<ClientProtocol>> proxies = GetProxiesForAllNameNodesInNameservice
				<ClientProtocol>(conf, nsId);
			IList<ClientProtocol> namenodes = new AList<ClientProtocol>(proxies.Count);
			foreach (NameNodeProxies.ProxyAndInfo<ClientProtocol> proxy in proxies)
			{
				namenodes.AddItem(proxy.GetProxy());
			}
			return namenodes;
		}

		/// <summary>Get an RPC proxy for each NN in an HA nameservice.</summary>
		/// <remarks>
		/// Get an RPC proxy for each NN in an HA nameservice. Used when a given RPC
		/// call should be made on every NN in an HA nameservice, not just the active.
		/// </remarks>
		/// <param name="conf">configuration</param>
		/// <param name="nsId">the nameservice to get all of the proxies for.</param>
		/// <param name="xface">the protocol class.</param>
		/// <returns>a list of RPC proxies for each NN in the nameservice.</returns>
		/// <exception cref="System.IO.IOException">in the event of error.</exception>
		public static IList<NameNodeProxies.ProxyAndInfo<T>> GetProxiesForAllNameNodesInNameservice
			<T>(Configuration conf, string nsId)
		{
			System.Type xface = typeof(T);
			IDictionary<string, IPEndPoint> nnAddresses = DFSUtil.GetRpcAddressesForNameserviceId
				(conf, nsId, null);
			IList<NameNodeProxies.ProxyAndInfo<T>> proxies = new AList<NameNodeProxies.ProxyAndInfo
				<T>>(nnAddresses.Count);
			foreach (IPEndPoint nnAddress in nnAddresses.Values)
			{
				NameNodeProxies.ProxyAndInfo<T> proxyInfo = null;
				proxyInfo = NameNodeProxies.CreateNonHAProxy(conf, nnAddress, xface, UserGroupInformation
					.GetCurrentUser(), false);
				proxies.AddItem(proxyInfo);
			}
			return proxies;
		}

		/// <summary>
		/// Used to ensure that at least one of the given HA NNs is currently in the
		/// active state..
		/// </summary>
		/// <param name="namenodes">list of RPC proxies for each NN to check.</param>
		/// <returns>true if at least one NN is active, false if all are in the standby state.
		/// 	</returns>
		/// <exception cref="System.IO.IOException">in the event of error.</exception>
		public static bool IsAtLeastOneActive(IList<ClientProtocol> namenodes)
		{
			foreach (ClientProtocol namenode in namenodes)
			{
				try
				{
					namenode.GetFileInfo("/");
					return true;
				}
				catch (RemoteException re)
				{
					IOException cause = re.UnwrapRemoteException();
					if (cause is StandbyException)
					{
					}
					else
					{
						// This is expected to happen for a standby NN.
						throw;
					}
				}
			}
			return false;
		}
	}
}
