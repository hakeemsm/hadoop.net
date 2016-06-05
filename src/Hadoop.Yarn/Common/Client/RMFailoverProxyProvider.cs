using System;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO.Retry;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Client
{
	public interface RMFailoverProxyProvider<T> : FailoverProxyProvider<T>
	{
		/// <summary>Initialize internal data structures, invoked right after instantiation.</summary>
		/// <param name="conf">Configuration to use</param>
		/// <param name="proxy">
		/// The
		/// <see cref="RMProxy{T}"/>
		/// instance to use
		/// </param>
		/// <param name="protocol">The communication protocol to use</param>
		void Init(Configuration conf, RMProxy<T> proxy, Type protocol);
	}
}
