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
using System;
using System.Collections.Generic;
using System.Reflection;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Ipc;

using Reflect;

namespace Org.Apache.Hadoop.IO.Retry
{
	/// <summary>
	/// This class implements RpcInvocationHandler and supports retry on the client
	/// side.
	/// </summary>
	public class RetryInvocationHandler<T> : RpcInvocationHandler
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.IO.Retry.RetryInvocationHandler
			));

		private readonly FailoverProxyProvider<T> proxyProvider;

		/// <summary>The number of times the associated proxyProvider has ever been failed over.
		/// 	</summary>
		private long proxyProviderFailoverCount = 0;

		private volatile bool hasMadeASuccessfulCall = false;

		private readonly RetryPolicy defaultPolicy;

		private readonly IDictionary<string, RetryPolicy> methodNameToPolicyMap;

		private FailoverProxyProvider.ProxyInfo<T> currentProxy;

		protected internal RetryInvocationHandler(FailoverProxyProvider<T> proxyProvider, 
			RetryPolicy retryPolicy)
			: this(proxyProvider, retryPolicy, Collections.EmptyMap<string, RetryPolicy
				>())
		{
		}

		protected internal RetryInvocationHandler(FailoverProxyProvider<T> proxyProvider, 
			RetryPolicy defaultPolicy, IDictionary<string, RetryPolicy> methodNameToPolicyMap
			)
		{
			this.proxyProvider = proxyProvider;
			this.defaultPolicy = defaultPolicy;
			this.methodNameToPolicyMap = methodNameToPolicyMap;
			this.currentProxy = proxyProvider.GetProxy();
		}

		/// <exception cref="System.Exception"/>
		public virtual object Invoke(object proxy, MethodInfo method, object[] args)
		{
			RetryPolicy policy = methodNameToPolicyMap[method.Name];
			if (policy == null)
			{
				policy = defaultPolicy;
			}
			// The number of times this method invocation has been failed over.
			int invocationFailoverCount = 0;
			bool isRpc = IsRpcInvocation(currentProxy.proxy);
			int callId = isRpc ? Client.NextCallId() : RpcConstants.InvalidCallId;
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
					Client.SetCallIdAndRetryCount(callId, retries);
				}
				try
				{
					object ret = InvokeMethod(method, args);
					hasMadeASuccessfulCall = true;
					return ret;
				}
				catch (Exception e)
				{
					if (Thread.CurrentThread().IsInterrupted())
					{
						// If interrupted, do not retry.
						throw;
					}
					bool isIdempotentOrAtMostOnce = proxyProvider.GetInterface().GetMethod(method.Name
						, Runtime.GetParameterTypes(method)).IsAnnotationPresent(typeof(Idempotent
						));
					if (!isIdempotentOrAtMostOnce)
					{
						isIdempotentOrAtMostOnce = proxyProvider.GetInterface().GetMethod(method.Name, Runtime.GetParameterTypes
							(method)).IsAnnotationPresent(typeof(AtMostOnce));
					}
					RetryPolicy.RetryAction action = policy.ShouldRetry(e, retries++, invocationFailoverCount
						, isIdempotentOrAtMostOnce);
					if (action.action == RetryPolicy.RetryAction.RetryDecision.Fail)
					{
						if (action.reason != null)
						{
							Log.Warn("Exception while invoking " + currentProxy.proxy.GetType() + "." + method
								.Name + " over " + currentProxy.proxyInfo + ". Not retrying because " + action.reason
								, e);
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
						worthLogging |= Log.IsDebugEnabled();
						if (action.action == RetryPolicy.RetryAction.RetryDecision.FailoverAndRetry && worthLogging)
						{
							string msg = "Exception while invoking " + method.Name + " of class " + currentProxy
								.proxy.GetType().Name + " over " + currentProxy.proxyInfo;
							if (invocationFailoverCount > 0)
							{
								msg += " after " + invocationFailoverCount + " fail over attempts";
							}
							msg += ". Trying to fail over " + FormatSleepMessage(action.delayMillis);
							Log.Info(msg, e);
						}
						else
						{
							if (Log.IsDebugEnabled())
							{
								Log.Debug("Exception while invoking " + method.Name + " of class " + currentProxy
									.proxy.GetType().Name + " over " + currentProxy.proxyInfo + ". Retrying " + FormatSleepMessage
									(action.delayMillis), e);
							}
						}
						if (action.delayMillis > 0)
						{
							Thread.Sleep(action.delayMillis);
						}
						if (action.action == RetryPolicy.RetryAction.RetryDecision.FailoverAndRetry)
						{
							// Make sure that concurrent failed method invocations only cause a
							// single actual fail over.
							lock (proxyProvider)
							{
								if (invocationAttemptFailoverCount == proxyProviderFailoverCount)
								{
									proxyProvider.PerformFailover(currentProxy.proxy);
									proxyProviderFailoverCount++;
								}
								else
								{
									Log.Warn("A failover has occurred since the start of this method" + " invocation attempt."
										);
								}
								currentProxy = proxyProvider.GetProxy();
							}
							invocationFailoverCount++;
						}
					}
				}
			}
		}

		private static string FormatSleepMessage(long millis)
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
		protected internal virtual object InvokeMethod(MethodInfo method, object[] args)
		{
			try
			{
				if (!method.IsAccessible())
				{
				}
				return method.Invoke(currentProxy.proxy, args);
			}
			catch (TargetInvocationException e)
			{
				throw e.InnerException;
			}
		}

		[VisibleForTesting]
		internal static bool IsRpcInvocation(object proxy)
		{
			if (proxy is ProtocolTranslator)
			{
				proxy = ((ProtocolTranslator)proxy).GetUnderlyingProxyObject();
			}
			if (!Proxy.IsProxyClass(proxy.GetType()))
			{
				return false;
			}
			InvocationHandler ih = Proxy.GetInvocationHandler(proxy);
			return ih is RpcInvocationHandler;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Close()
		{
			proxyProvider.Close();
		}

		public virtual Client.ConnectionId GetConnectionId()
		{
			//RpcInvocationHandler
			return RPC.GetConnectionIdForProxy(currentProxy.proxy);
		}
	}
}
