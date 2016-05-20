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
	/// <p>
	/// A factory for creating retry proxies.
	/// </summary>
	/// <remarks>
	/// <p>
	/// A factory for creating retry proxies.
	/// </p>
	/// </remarks>
	public class RetryProxy
	{
		/// <summary>
		/// <p>
		/// Create a proxy for an interface of an implementation class
		/// using the same retry policy for each method in the interface.
		/// </summary>
		/// <remarks>
		/// <p>
		/// Create a proxy for an interface of an implementation class
		/// using the same retry policy for each method in the interface.
		/// </p>
		/// </remarks>
		/// <param name="iface">the interface that the retry will implement</param>
		/// <param name="implementation">the instance whose methods should be retried</param>
		/// <param name="retryPolicy">the policy for retrying method call failures</param>
		/// <returns>the retry proxy</returns>
		public static object create<T>(T implementation, org.apache.hadoop.io.retry.RetryPolicy
			 retryPolicy)
		{
			System.Type iface = typeof(T);
			return org.apache.hadoop.io.retry.RetryProxy.create(iface, new org.apache.hadoop.io.retry.DefaultFailoverProxyProvider
				<T>(iface, implementation), retryPolicy);
		}

		/// <summary>
		/// Create a proxy for an interface of implementations of that interface using
		/// the given
		/// <see cref="FailoverProxyProvider{T}"/>
		/// and the same retry policy for each
		/// method in the interface.
		/// </summary>
		/// <param name="iface">the interface that the retry will implement</param>
		/// <param name="proxyProvider">provides implementation instances whose methods should be retried
		/// 	</param>
		/// <param name="retryPolicy">the policy for retrying or failing over method call failures
		/// 	</param>
		/// <returns>the retry proxy</returns>
		public static object create<T>(org.apache.hadoop.io.retry.FailoverProxyProvider<T
			> proxyProvider, org.apache.hadoop.io.retry.RetryPolicy retryPolicy)
		{
			System.Type iface = typeof(T);
			return java.lang.reflect.Proxy.newProxyInstance(proxyProvider.getInterface().getClassLoader
				(), new java.lang.Class[] { iface }, new org.apache.hadoop.io.retry.RetryInvocationHandler
				<T>(proxyProvider, retryPolicy));
		}

		/// <summary>
		/// Create a proxy for an interface of an implementation class
		/// using the a set of retry policies specified by method name.
		/// </summary>
		/// <remarks>
		/// Create a proxy for an interface of an implementation class
		/// using the a set of retry policies specified by method name.
		/// If no retry policy is defined for a method then a default of
		/// <see cref="RetryPolicies.TRY_ONCE_THEN_FAIL"/>
		/// is used.
		/// </remarks>
		/// <param name="iface">the interface that the retry will implement</param>
		/// <param name="implementation">the instance whose methods should be retried</param>
		/// <param name="methodNameToPolicyMap">a map of method names to retry policies</param>
		/// <returns>the retry proxy</returns>
		public static object create<T>(T implementation, System.Collections.Generic.IDictionary
			<string, org.apache.hadoop.io.retry.RetryPolicy> methodNameToPolicyMap)
		{
			System.Type iface = typeof(T);
			return create(iface, new org.apache.hadoop.io.retry.DefaultFailoverProxyProvider<
				T>(iface, implementation), methodNameToPolicyMap, org.apache.hadoop.io.retry.RetryPolicies
				.TRY_ONCE_THEN_FAIL);
		}

		/// <summary>
		/// Create a proxy for an interface of implementations of that interface using
		/// the given
		/// <see cref="FailoverProxyProvider{T}"/>
		/// and the a set of retry policies
		/// specified by method name. If no retry policy is defined for a method then a
		/// default of
		/// <see cref="RetryPolicies.TRY_ONCE_THEN_FAIL"/>
		/// is used.
		/// </summary>
		/// <param name="iface">the interface that the retry will implement</param>
		/// <param name="proxyProvider">provides implementation instances whose methods should be retried
		/// 	</param>
		/// <param name="methodNameToPolicyMapa">map of method names to retry policies</param>
		/// <returns>the retry proxy</returns>
		public static object create<T>(org.apache.hadoop.io.retry.FailoverProxyProvider<T
			> proxyProvider, System.Collections.Generic.IDictionary<string, org.apache.hadoop.io.retry.RetryPolicy
			> methodNameToPolicyMap, org.apache.hadoop.io.retry.RetryPolicy defaultPolicy)
		{
			System.Type iface = typeof(T);
			return java.lang.reflect.Proxy.newProxyInstance(proxyProvider.getInterface().getClassLoader
				(), new java.lang.Class[] { iface }, new org.apache.hadoop.io.retry.RetryInvocationHandler
				<T>(proxyProvider, defaultPolicy, methodNameToPolicyMap));
		}
	}
}
