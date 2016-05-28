using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using Com.Google.Common.Base;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.IO.Retry;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.HA
{
	/// <summary>
	/// A FailoverProxyProvider implementation which allows one to configure two URIs
	/// to connect to during fail-over.
	/// </summary>
	/// <remarks>
	/// A FailoverProxyProvider implementation which allows one to configure two URIs
	/// to connect to during fail-over. The first configured address is tried first,
	/// and on a fail-over event the other address is tried.
	/// </remarks>
	public class ConfiguredFailoverProxyProvider<T> : AbstractNNFailoverProxyProvider
		<T>
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.Server.Namenode.HA.ConfiguredFailoverProxyProvider
			));

		private readonly Configuration conf;

		private readonly IList<ConfiguredFailoverProxyProvider.AddressRpcProxyPair<T>> proxies
			 = new AList<ConfiguredFailoverProxyProvider.AddressRpcProxyPair<T>>();

		private readonly UserGroupInformation ugi;

		private readonly Type xface;

		private int currentProxyIndex = 0;

		public ConfiguredFailoverProxyProvider(Configuration conf, URI uri, Type xface)
		{
			Preconditions.CheckArgument(xface.IsAssignableFrom(typeof(NamenodeProtocols)), "Interface class %s is not a valid NameNode protocol!"
				);
			this.xface = xface;
			this.conf = new Configuration(conf);
			int maxRetries = this.conf.GetInt(DFSConfigKeys.DfsClientFailoverConnectionRetriesKey
				, DFSConfigKeys.DfsClientFailoverConnectionRetriesDefault);
			this.conf.SetInt(CommonConfigurationKeysPublic.IpcClientConnectMaxRetriesKey, maxRetries
				);
			int maxRetriesOnSocketTimeouts = this.conf.GetInt(DFSConfigKeys.DfsClientFailoverConnectionRetriesOnSocketTimeoutsKey
				, DFSConfigKeys.DfsClientFailoverConnectionRetriesOnSocketTimeoutsDefault);
			this.conf.SetInt(CommonConfigurationKeysPublic.IpcClientConnectMaxRetriesOnSocketTimeoutsKey
				, maxRetriesOnSocketTimeouts);
			try
			{
				ugi = UserGroupInformation.GetCurrentUser();
				IDictionary<string, IDictionary<string, IPEndPoint>> map = DFSUtil.GetHaNnRpcAddresses
					(conf);
				IDictionary<string, IPEndPoint> addressesInNN = map[uri.GetHost()];
				if (addressesInNN == null || addressesInNN.Count == 0)
				{
					throw new RuntimeException("Could not find any configured addresses " + "for URI "
						 + uri);
				}
				ICollection<IPEndPoint> addressesOfNns = addressesInNN.Values;
				foreach (IPEndPoint address in addressesOfNns)
				{
					proxies.AddItem(new ConfiguredFailoverProxyProvider.AddressRpcProxyPair<T>(address
						));
				}
				// The client may have a delegation token set for the logical
				// URI of the cluster. Clone this token to apply to each of the
				// underlying IPC addresses so that the IPC code can find it.
				HAUtil.CloneDelegationTokenForLogicalUri(ugi, uri, addressesOfNns);
			}
			catch (IOException e)
			{
				throw new RuntimeException(e);
			}
		}

		public override Type GetInterface()
		{
			return xface;
		}

		/// <summary>Lazily initialize the RPC proxy object.</summary>
		public override FailoverProxyProvider.ProxyInfo<T> GetProxy()
		{
			lock (this)
			{
				ConfiguredFailoverProxyProvider.AddressRpcProxyPair<T> current = proxies[currentProxyIndex
					];
				if (current.namenode == null)
				{
					try
					{
						current.namenode = NameNodeProxies.CreateNonHAProxy(conf, current.address, xface, 
							ugi, false, fallbackToSimpleAuth).GetProxy();
					}
					catch (IOException e)
					{
						Log.Error("Failed to create RPC proxy to NameNode", e);
						throw new RuntimeException(e);
					}
				}
				return new FailoverProxyProvider.ProxyInfo<T>(current.namenode, current.address.ToString
					());
			}
		}

		public override void PerformFailover(T currentProxy)
		{
			lock (this)
			{
				currentProxyIndex = (currentProxyIndex + 1) % proxies.Count;
			}
		}

		/// <summary>
		/// A little pair object to store the address and connected RPC proxy object to
		/// an NN.
		/// </summary>
		/// <remarks>
		/// A little pair object to store the address and connected RPC proxy object to
		/// an NN. Note that
		/// <see cref="AddressRpcProxyPair{T}.namenode"/>
		/// may be null.
		/// </remarks>
		private class AddressRpcProxyPair<T>
		{
			public readonly IPEndPoint address;

			public T namenode;

			public AddressRpcProxyPair(IPEndPoint address)
			{
				this.address = address;
			}
		}

		/// <summary>
		/// Close all the proxy objects which have been opened over the lifetime of
		/// this proxy provider.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public override void Close()
		{
			lock (this)
			{
				foreach (ConfiguredFailoverProxyProvider.AddressRpcProxyPair<T> proxy in proxies)
				{
					if (proxy.namenode != null)
					{
						if (proxy.namenode is IDisposable)
						{
							((IDisposable)proxy.namenode).Close();
						}
						else
						{
							RPC.StopProxy(proxy.namenode);
						}
					}
				}
			}
		}

		/// <summary>Logical URI is required for this failover proxy provider.</summary>
		public override bool UseLogicalURI()
		{
			return true;
		}
	}
}
