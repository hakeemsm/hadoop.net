using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO.Retry;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Ipc;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Client
{
	public class RMProxy<T>
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Client.RMProxy
			));

		protected internal RMProxy()
		{
		}

		/// <summary>Verify the passed protocol is supported.</summary>
		[InterfaceAudience.Private]
		protected internal virtual void CheckAllowedProtocols(Type protocol)
		{
		}

		/// <summary>
		/// Get the ResourceManager address from the provided Configuration for the
		/// given protocol.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[InterfaceAudience.Private]
		protected internal virtual IPEndPoint GetRMAddress(YarnConfiguration conf, Type protocol
			)
		{
			throw new NotSupportedException("This method should be invoked " + "from an instance of ClientRMProxy or ServerRMProxy"
				);
		}

		/// <summary>Create a proxy for the specified protocol.</summary>
		/// <remarks>
		/// Create a proxy for the specified protocol. For non-HA,
		/// this is a direct connection to the ResourceManager address. When HA is
		/// enabled, the proxy handles the failover between the ResourceManagers as
		/// well.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[InterfaceAudience.Private]
		protected internal static T CreateRMProxy<T>(Configuration configuration, Org.Apache.Hadoop.Yarn.Client.RMProxy
			 instance)
		{
			System.Type protocol = typeof(T);
			YarnConfiguration conf = (configuration is YarnConfiguration) ? (YarnConfiguration
				)configuration : new YarnConfiguration(configuration);
			RetryPolicy retryPolicy = CreateRetryPolicy(conf);
			if (HAUtil.IsHAEnabled(conf))
			{
				RMFailoverProxyProvider<T> provider = instance.CreateRMFailoverProxyProvider(conf
					, protocol);
				return (T)RetryProxy.Create(protocol, provider, retryPolicy);
			}
			else
			{
				IPEndPoint rmAddress = instance.GetRMAddress(conf, protocol);
				Log.Info("Connecting to ResourceManager at " + rmAddress);
				T proxy = Org.Apache.Hadoop.Yarn.Client.RMProxy.GetProxy<T>(conf, protocol, rmAddress
					);
				return (T)RetryProxy.Create(protocol, proxy, retryPolicy);
			}
		}

		/// <param name="conf">Configuration to generate retry policy</param>
		/// <param name="protocol">Protocol for the proxy</param>
		/// <param name="rmAddress">Address of the ResourceManager</param>
		/// <?/>
		/// <returns>Proxy to the RM</returns>
		/// <exception cref="System.IO.IOException"/>
		[System.ObsoleteAttribute(@"This method is deprecated and is not used by YARN internally any more. To create a proxy to the RM, use ClientRMProxy#createRMProxy or ServerRMProxy#createRMProxy. Create a proxy to the ResourceManager at the specified address."
			)]
		public static T CreateRMProxy<T>(Configuration conf, IPEndPoint rmAddress)
		{
			System.Type protocol = typeof(T);
			RetryPolicy retryPolicy = CreateRetryPolicy(conf);
			T proxy = Org.Apache.Hadoop.Yarn.Client.RMProxy.GetProxy<T>(conf, protocol, rmAddress
				);
			Log.Info("Connecting to ResourceManager at " + rmAddress);
			return (T)RetryProxy.Create(protocol, proxy, retryPolicy);
		}

		/// <summary>Get a proxy to the RM at the specified address.</summary>
		/// <remarks>
		/// Get a proxy to the RM at the specified address. To be used to create a
		/// RetryProxy.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[InterfaceAudience.Private]
		internal static T GetProxy<T>(Configuration conf, IPEndPoint rmAddress)
		{
			System.Type protocol = typeof(T);
			return UserGroupInformation.GetCurrentUser().DoAs(new _PrivilegedAction_137(conf, 
				protocol, rmAddress));
		}

		private sealed class _PrivilegedAction_137 : PrivilegedAction<T>
		{
			public _PrivilegedAction_137(Configuration conf, Type protocol, IPEndPoint rmAddress
				)
			{
				this.conf = conf;
				this.protocol = protocol;
				this.rmAddress = rmAddress;
			}

			public T Run()
			{
				return (T)YarnRPC.Create(conf).GetProxy(protocol, rmAddress, conf);
			}

			private readonly Configuration conf;

			private readonly Type protocol;

			private readonly IPEndPoint rmAddress;
		}

		/// <summary>Helper method to create FailoverProxyProvider.</summary>
		private RMFailoverProxyProvider<T> CreateRMFailoverProxyProvider<T>(Configuration
			 conf)
		{
			System.Type protocol = typeof(T);
			Type defaultProviderClass;
			try
			{
				defaultProviderClass = (Type)Sharpen.Runtime.GetType(YarnConfiguration.DefaultClientFailoverProxyProvider
					);
			}
			catch (Exception e)
			{
				throw new YarnRuntimeException("Invalid default failover provider class" + YarnConfiguration
					.DefaultClientFailoverProxyProvider, e);
			}
			RMFailoverProxyProvider<T> provider = ReflectionUtils.NewInstance(conf.GetClass<RMFailoverProxyProvider
				>(YarnConfiguration.ClientFailoverProxyProvider, defaultProviderClass), conf);
			provider.Init(conf, (Org.Apache.Hadoop.Yarn.Client.RMProxy<T>)this, protocol);
			return provider;
		}

		/// <summary>Fetch retry policy from Configuration</summary>
		[InterfaceAudience.Private]
		[VisibleForTesting]
		public static RetryPolicy CreateRetryPolicy(Configuration conf)
		{
			long rmConnectWaitMS = conf.GetLong(YarnConfiguration.ResourcemanagerConnectMaxWaitMs
				, YarnConfiguration.DefaultResourcemanagerConnectMaxWaitMs);
			long rmConnectionRetryIntervalMS = conf.GetLong(YarnConfiguration.ResourcemanagerConnectRetryIntervalMs
				, YarnConfiguration.DefaultResourcemanagerConnectRetryIntervalMs);
			bool waitForEver = (rmConnectWaitMS == -1);
			if (!waitForEver)
			{
				if (rmConnectWaitMS < 0)
				{
					throw new YarnRuntimeException("Invalid Configuration. " + YarnConfiguration.ResourcemanagerConnectMaxWaitMs
						 + " can be -1, but can not be other negative numbers");
				}
				// try connect once
				if (rmConnectWaitMS < rmConnectionRetryIntervalMS)
				{
					Log.Warn(YarnConfiguration.ResourcemanagerConnectMaxWaitMs + " is smaller than " 
						+ YarnConfiguration.ResourcemanagerConnectRetryIntervalMs + ". Only try connect once."
						);
					rmConnectWaitMS = 0;
				}
			}
			// Handle HA case first
			if (HAUtil.IsHAEnabled(conf))
			{
				long failoverSleepBaseMs = conf.GetLong(YarnConfiguration.ClientFailoverSleeptimeBaseMs
					, rmConnectionRetryIntervalMS);
				long failoverSleepMaxMs = conf.GetLong(YarnConfiguration.ClientFailoverSleeptimeMaxMs
					, rmConnectionRetryIntervalMS);
				int maxFailoverAttempts = conf.GetInt(YarnConfiguration.ClientFailoverMaxAttempts
					, -1);
				if (maxFailoverAttempts == -1)
				{
					if (waitForEver)
					{
						maxFailoverAttempts = int.MaxValue;
					}
					else
					{
						maxFailoverAttempts = (int)(rmConnectWaitMS / failoverSleepBaseMs);
					}
				}
				return RetryPolicies.FailoverOnNetworkException(RetryPolicies.TryOnceThenFail, maxFailoverAttempts
					, failoverSleepBaseMs, failoverSleepMaxMs);
			}
			if (rmConnectionRetryIntervalMS < 0)
			{
				throw new YarnRuntimeException("Invalid Configuration. " + YarnConfiguration.ResourcemanagerConnectRetryIntervalMs
					 + " should not be negative.");
			}
			RetryPolicy retryPolicy = null;
			if (waitForEver)
			{
				retryPolicy = RetryPolicies.RetryForever;
			}
			else
			{
				retryPolicy = RetryPolicies.RetryUpToMaximumTimeWithFixedSleep(rmConnectWaitMS, rmConnectionRetryIntervalMS
					, TimeUnit.Milliseconds);
			}
			IDictionary<Type, RetryPolicy> exceptionToPolicyMap = new Dictionary<Type, RetryPolicy
				>();
			exceptionToPolicyMap[typeof(EOFException)] = retryPolicy;
			exceptionToPolicyMap[typeof(ConnectException)] = retryPolicy;
			exceptionToPolicyMap[typeof(NoRouteToHostException)] = retryPolicy;
			exceptionToPolicyMap[typeof(UnknownHostException)] = retryPolicy;
			exceptionToPolicyMap[typeof(ConnectTimeoutException)] = retryPolicy;
			exceptionToPolicyMap[typeof(RetriableException)] = retryPolicy;
			exceptionToPolicyMap[typeof(SocketException)] = retryPolicy;
			return RetryPolicies.RetryByException(RetryPolicies.TryOnceThenFail, exceptionToPolicyMap
				);
		}
	}
}
