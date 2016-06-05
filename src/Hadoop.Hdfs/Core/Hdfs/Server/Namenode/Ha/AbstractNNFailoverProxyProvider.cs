using System;
using Org.Apache.Hadoop.IO.Retry;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.HA
{
	public abstract class AbstractNNFailoverProxyProvider<T> : FailoverProxyProvider<
		T>
	{
		protected internal AtomicBoolean fallbackToSimpleAuth;

		/// <summary>Inquire whether logical HA URI is used for the implementation.</summary>
		/// <remarks>
		/// Inquire whether logical HA URI is used for the implementation. If it is
		/// used, a special token handling may be needed to make sure a token acquired
		/// from a node in the HA pair can be used against the other node.
		/// </remarks>
		/// <returns>true if logical HA URI is used. false, if not used.</returns>
		public abstract bool UseLogicalURI();

		/// <summary>Set for tracking if a secure client falls back to simple auth.</summary>
		/// <remarks>
		/// Set for tracking if a secure client falls back to simple auth.  This method
		/// is synchronized only to stifle a Findbugs warning.
		/// </remarks>
		/// <param name="fallbackToSimpleAuth">
		/// - set to true or false during this method to
		/// indicate if a secure client falls back to simple auth
		/// </param>
		public virtual void SetFallbackToSimpleAuth(AtomicBoolean fallbackToSimpleAuth)
		{
			lock (this)
			{
				this.fallbackToSimpleAuth = fallbackToSimpleAuth;
			}
		}

		public abstract void Close();

		public abstract Type GetInterface();

		public abstract FailoverProxyProvider.ProxyInfo<T> GetProxy();

		public abstract void PerformFailover(T arg1);
	}
}
