using System;
using System.IO;
using System.Net;
using Com.Google.Common.Base;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.IO.Retry;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.HA
{
	/// <summary>A NNFailoverProxyProvider implementation which works on IP failover setup.
	/// 	</summary>
	/// <remarks>
	/// A NNFailoverProxyProvider implementation which works on IP failover setup.
	/// Only one proxy is used to connect to both servers and switching between
	/// the servers is done by the environment/infrastructure, which guarantees
	/// clients can consistently reach only one node at a time.
	/// Clients with a live connection will likely get connection reset after an
	/// IP failover. This case will be handled by the
	/// FailoverOnNetworkExceptionRetry retry policy. I.e. if the call is
	/// not idempotent, it won't get retried.
	/// A connection reset while setting up a connection (i.e. before sending a
	/// request) will be handled in ipc client.
	/// The namenode URI must contain a resolvable host name.
	/// </remarks>
	public class IPFailoverProxyProvider<T> : AbstractNNFailoverProxyProvider<T>
	{
		private readonly Configuration conf;

		private readonly Type xface;

		private readonly URI nameNodeUri;

		private FailoverProxyProvider.ProxyInfo<T> nnProxyInfo = null;

		public IPFailoverProxyProvider(Configuration conf, URI uri, Type xface)
		{
			Preconditions.CheckArgument(xface.IsAssignableFrom(typeof(NamenodeProtocols)), "Interface class %s is not a valid NameNode protocol!"
				);
			this.xface = xface;
			this.nameNodeUri = uri;
			this.conf = new Configuration(conf);
			int maxRetries = this.conf.GetInt(DFSConfigKeys.DfsClientFailoverConnectionRetriesKey
				, DFSConfigKeys.DfsClientFailoverConnectionRetriesDefault);
			this.conf.SetInt(CommonConfigurationKeysPublic.IpcClientConnectMaxRetriesKey, maxRetries
				);
			int maxRetriesOnSocketTimeouts = this.conf.GetInt(DFSConfigKeys.DfsClientFailoverConnectionRetriesOnSocketTimeoutsKey
				, DFSConfigKeys.DfsClientFailoverConnectionRetriesOnSocketTimeoutsDefault);
			this.conf.SetInt(CommonConfigurationKeysPublic.IpcClientConnectMaxRetriesOnSocketTimeoutsKey
				, maxRetriesOnSocketTimeouts);
		}

		public override Type GetInterface()
		{
			return xface;
		}

		public override FailoverProxyProvider.ProxyInfo<T> GetProxy()
		{
			lock (this)
			{
				// Create a non-ha proxy if not already created.
				if (nnProxyInfo == null)
				{
					try
					{
						// Create a proxy that is not wrapped in RetryProxy
						IPEndPoint nnAddr = NameNode.GetAddress(nameNodeUri);
						nnProxyInfo = new FailoverProxyProvider.ProxyInfo<T>(NameNodeProxies.CreateNonHAProxy
							(conf, nnAddr, xface, UserGroupInformation.GetCurrentUser(), false).GetProxy(), 
							nnAddr.ToString());
					}
					catch (IOException ioe)
					{
						throw new RuntimeException(ioe);
					}
				}
				return nnProxyInfo;
			}
		}

		/// <summary>Nothing to do for IP failover</summary>
		public override void PerformFailover(T currentProxy)
		{
		}

		/// <summary>Close the proxy,</summary>
		/// <exception cref="System.IO.IOException"/>
		public override void Close()
		{
			lock (this)
			{
				if (nnProxyInfo == null)
				{
					return;
				}
				if (nnProxyInfo.proxy is IDisposable)
				{
					((IDisposable)nnProxyInfo.proxy).Close();
				}
				else
				{
					RPC.StopProxy(nnProxyInfo.proxy);
				}
			}
		}

		/// <summary>Logical URI is not used for IP failover.</summary>
		public override bool UseLogicalURI()
		{
			return false;
		}
	}
}
