using System;
using Org.Apache.Hadoop.IO.Retry;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.HA
{
	/// <summary>
	/// A NNFailoverProxyProvider implementation which wrapps old implementations
	/// directly implementing the
	/// <see cref="Org.Apache.Hadoop.IO.Retry.FailoverProxyProvider{T}"/>
	/// interface.
	/// It is assumed that the old impelmentation is using logical URI.
	/// </summary>
	public class WrappedFailoverProxyProvider<T> : AbstractNNFailoverProxyProvider<T>
	{
		private readonly FailoverProxyProvider<T> proxyProvider;

		/// <summary>Wrap the given instance of an old FailoverProxyProvider.</summary>
		public WrappedFailoverProxyProvider(FailoverProxyProvider<T> provider)
		{
			proxyProvider = provider;
		}

		public override Type GetInterface()
		{
			return proxyProvider.GetInterface();
		}

		public override FailoverProxyProvider.ProxyInfo<T> GetProxy()
		{
			lock (this)
			{
				return proxyProvider.GetProxy();
			}
		}

		public override void PerformFailover(T currentProxy)
		{
			proxyProvider.PerformFailover(currentProxy);
		}

		/// <summary>Close the proxy,</summary>
		/// <exception cref="System.IO.IOException"/>
		public override void Close()
		{
			lock (this)
			{
				proxyProvider.Close();
			}
		}

		/// <summary>Assume logical URI is used for old proxy provider implementations.</summary>
		public override bool UseLogicalURI()
		{
			return true;
		}
	}
}
