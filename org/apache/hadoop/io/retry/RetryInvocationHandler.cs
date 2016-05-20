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
	/// This class implements RpcInvocationHandler and supports retry on the client
	/// side.
	/// </summary>
	public class RetryInvocationHandler<T> : org.apache.hadoop.ipc.RpcInvocationHandler
	{
		public static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.retry.RetryInvocationHandler
			)));

		private readonly org.apache.hadoop.io.retry.FailoverProxyProvider<T> proxyProvider;

		/// <summary>The number of times the associated proxyProvider has ever been failed over.
		/// 	</summary>
		private long proxyProviderFailoverCount = 0;

		private volatile bool hasMadeASuccessfulCall = false;

		private readonly org.apache.hadoop.io.retry.RetryPolicy defaultPolicy;

		private readonly System.Collections.Generic.IDictionary<string, org.apache.hadoop.io.retry.RetryPolicy
			> methodNameToPolicyMap;

		private org.apache.hadoop.io.retry.FailoverProxyProvider.ProxyInfo<T> currentProxy;

		protected internal RetryInvocationHandler(org.apache.hadoop.io.retry.FailoverProxyProvider
			<T> proxyProvider, org.apache.hadoop.io.retry.RetryPolicy retryPolicy)
			: this(proxyProvider, retryPolicy, java.util.Collections.emptyMap<string, org.apache.hadoop.io.retry.RetryPolicy
				>())
		{
		}

		protected internal RetryInvocationHandler(org.apache.hadoop.io.retry.FailoverProxyProvider
			<T> proxyProvider, org.apache.hadoop.io.retry.RetryPolicy defaultPolicy, System.Collections.Generic.IDictionary
			<string, org.apache.hadoop.io.retry.RetryPolicy> methodNameToPolicyMap)
		{
			this.proxyProvider = proxyProvider;
			this.defaultPolicy = defaultPolicy;
			this.methodNameToPolicyMap = methodNameToPolicyMap;
			this.currentProxy = proxyProvider.getProxy();
		}

		/// <exception cref="System.Exception"/>
		public virtual object invoke(object proxy, java.lang.reflect.Method method, object
			[] args)
		{
			org.apache.hadoop.io.retry.RetryPolicy policy = methodNameToPolicyMap[method.getName
				()];
			if (policy == null)
			{
				policy = defaultPolicy;
			}
			// The number of times this method invocation has been failed over.
			int invocationFailoverCount = 0;
			bool isRpc = isRpcInvocation(currentProxy.proxy);
			int callId = isRpc ? org.apache.hadoop.ipc.Client.nextCallId() : org.apache.hadoop.ipc.RpcConstants
				.INVALID_CALL_ID;
			int retries = 0;
			while (true)
			{
				// The number of times this invocation handler has ever been failed over,
				// before this method invocation attempt. Used to prevent concurrent
				// failed method invocations from triggering multiple failover attempts.
				long invocationAttemptFailoverCount;
				lock (proxyProvider)
				{
					invocationAttemptFailoverCount = proxyProviderFailoverCount;
				}
				if (isRpc)
				{
					org.apache.hadoop.ipc.Client.setCallIdAndRetryCount(callId, retries);
				}
				try
				{
					object ret = invokeMethod(method, args);
					hasMadeASuccessfulCall = true;
					return ret;
				}
				catch (System.Exception e)
				{
					if (java.lang.Thread.currentThread().isInterrupted())
					{
						// If interrupted, do not retry.
						throw;
					}
					bool isIdempotentOrAtMostOnce = proxyProvider.getInterface().getMethod(method.getName
						(), method.getParameterTypes()).isAnnotationPresent(Sharpen.Runtime.getClassForType
						(typeof(org.apache.hadoop.io.retry.Idempotent)));
					if (!isIdempotentOrAtMostOnce)
					{
						isIdempotentOrAtMostOnce = proxyProvider.getInterface().getMethod(method.getName(
							), method.getParameterTypes()).isAnnotationPresent(Sharpen.Runtime.getClassForType
							(typeof(org.apache.hadoop.io.retry.AtMostOnce)));
					}
					org.apache.hadoop.io.retry.RetryPolicy.RetryAction action = policy.shouldRetry(e, 
						retries++, invocationFailoverCount, isIdempotentOrAtMostOnce);
					if (action.action == org.apache.hadoop.io.retry.RetryPolicy.RetryAction.RetryDecision
						.FAIL)
					{
						if (action.reason != null)
						{
							LOG.warn("Exception while invoking " + Sharpen.Runtime.getClassForObject(currentProxy
								.proxy) + "." + method.getName() + " over " + currentProxy.proxyInfo + ". Not retrying because "
								 + action.reason, e);
						}
						throw;
					}
					else
					{
						// retry or failover
						// avoid logging the failover if this is the first call on this
						// proxy object, and we successfully achieve the failover without
						// any flip-flopping
						bool worthLogging = !(invocationFailoverCount == 0 && !hasMadeASuccessfulCall);
						worthLogging |= LOG.isDebugEnabled();
						if (action.action == org.apache.hadoop.io.retry.RetryPolicy.RetryAction.RetryDecision
							.FAILOVER_AND_RETRY && worthLogging)
						{
							string msg = "Exception while invoking " + method.getName() + " of class " + Sharpen.Runtime.getClassForObject
								(currentProxy.proxy).getSimpleName() + " over " + currentProxy.proxyInfo;
							if (invocationFailoverCount > 0)
							{
								msg += " after " + invocationFailoverCount + " fail over attempts";
							}
							msg += ". Trying to fail over " + formatSleepMessage(action.delayMillis);
							LOG.info(msg, e);
						}
						else
						{
							if (LOG.isDebugEnabled())
							{
								LOG.debug("Exception while invoking " + method.getName() + " of class " + Sharpen.Runtime.getClassForObject
									(currentProxy.proxy).getSimpleName() + " over " + currentProxy.proxyInfo + ". Retrying "
									 + formatSleepMessage(action.delayMillis), e);
							}
						}
						if (action.delayMillis > 0)
						{
							java.lang.Thread.sleep(action.delayMillis);
						}
						if (action.action == org.apache.hadoop.io.retry.RetryPolicy.RetryAction.RetryDecision
							.FAILOVER_AND_RETRY)
						{
							// Make sure that concurrent failed method invocations only cause a
							// single actual fail over.
							lock (proxyProvider)
							{
								if (invocationAttemptFailoverCount == proxyProviderFailoverCount)
								{
									proxyProvider.performFailover(currentProxy.proxy);
									proxyProviderFailoverCount++;
								}
								else
								{
									LOG.warn("A failover has occurred since the start of this method" + " invocation attempt."
										);
								}
								currentProxy = proxyProvider.getProxy();
							}
							invocationFailoverCount++;
						}
					}
				}
			}
		}

		private static string formatSleepMessage(long millis)
		{
			if (millis > 0)
			{
				return "after sleeping for " + millis + "ms.";
			}
			else
			{
				return "immediately.";
			}
		}

		/// <exception cref="System.Exception"/>
		protected internal virtual object invokeMethod(java.lang.reflect.Method method, object
			[] args)
		{
			try
			{
				if (!method.isAccessible())
				{
					method.setAccessible(true);
				}
				return method.invoke(currentProxy.proxy, args);
			}
			catch (java.lang.reflect.InvocationTargetException e)
			{
				throw e.InnerException;
			}
		}

		[com.google.common.annotations.VisibleForTesting]
		internal static bool isRpcInvocation(object proxy)
		{
			if (proxy is org.apache.hadoop.ipc.ProtocolTranslator)
			{
				proxy = ((org.apache.hadoop.ipc.ProtocolTranslator)proxy).getUnderlyingProxyObject
					();
			}
			if (!java.lang.reflect.Proxy.isProxyClass(Sharpen.Runtime.getClassForObject(proxy
				)))
			{
				return false;
			}
			java.lang.reflect.InvocationHandler ih = java.lang.reflect.Proxy.getInvocationHandler
				(proxy);
			return ih is org.apache.hadoop.ipc.RpcInvocationHandler;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void close()
		{
			proxyProvider.close();
		}

		public virtual org.apache.hadoop.ipc.Client.ConnectionId getConnectionId()
		{
			//RpcInvocationHandler
			return org.apache.hadoop.ipc.RPC.getConnectionIdForProxy(currentProxy.proxy);
		}
	}
}
