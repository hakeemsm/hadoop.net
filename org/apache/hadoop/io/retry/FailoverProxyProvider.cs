/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
using Sharpen;

namespace org.apache.hadoop.io.retry
{
	/// <summary>
	/// An implementer of this interface is capable of providing proxy objects for
	/// use in IPC communication, and potentially modifying these objects or creating
	/// entirely new ones in the event of certain types of failures.
	/// </summary>
	/// <remarks>
	/// An implementer of this interface is capable of providing proxy objects for
	/// use in IPC communication, and potentially modifying these objects or creating
	/// entirely new ones in the event of certain types of failures. The
	/// determination of whether or not to fail over is handled by
	/// <see cref="RetryPolicy"/>
	/// .
	/// </remarks>
	public abstract class FailoverProxyProvider<T> : java.io.Closeable
	{
		public sealed class ProxyInfo<T>
		{
			public readonly T proxy;

			public readonly string proxyInfo;

			public ProxyInfo(T proxy, string proxyInfo)
			{
				/*
				* The information (e.g., the IP address) of the current proxy object. It
				* provides information for debugging purposes.
				*/
				this.proxy = proxy;
				this.proxyInfo = proxyInfo;
			}
		}

		/// <summary>
		/// Get the proxy object which should be used until the next failover event
		/// occurs.
		/// </summary>
		/// <returns>the proxy object to invoke methods upon</returns>
		public abstract org.apache.hadoop.io.retry.FailoverProxyProvider.ProxyInfo<T> getProxy
			();

		/// <summary>
		/// Called whenever the associated
		/// <see cref="RetryPolicy"/>
		/// determines that an error
		/// warrants failing over.
		/// </summary>
		/// <param name="currentProxy">the proxy object which was being used before this failover event
		/// 	</param>
		public abstract void performFailover(T currentProxy);

		/// <summary>
		/// Return a reference to the interface this provider's proxy objects actually
		/// implement.
		/// </summary>
		/// <remarks>
		/// Return a reference to the interface this provider's proxy objects actually
		/// implement. If any of the methods on this interface are annotated as being
		/// <see cref="Idempotent"/>
		/// or
		/// <see cref="AtMostOnce"/>
		/// , then this fact will be passed to
		/// the
		/// <see cref="RetryPolicy.shouldRetry(System.Exception, int, int, bool)"/>
		/// method on
		/// error, for use in determining whether or not failover should be attempted.
		/// </remarks>
		/// <returns>
		/// the interface implemented by the proxy objects returned by
		/// <see cref="FailoverProxyProvider{T}.getProxy()"/>
		/// </returns>
		public abstract java.lang.Class getInterface();
	}

	public static class FailoverProxyProviderConstants
	{
	}
}
