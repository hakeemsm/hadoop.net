using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.ProtocolPB;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Server.Namenode.HA;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.IO.Retry;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Ipc.ProtocolPB;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authorize;
using Org.Apache.Hadoop.Security.ProtocolPB;
using Org.Apache.Hadoop.Tools;
using Org.Apache.Hadoop.Tools.ProtocolPB;
using Sharpen;
using Sharpen.Reflect;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>Create proxy objects to communicate with a remote NN.</summary>
	/// <remarks>
	/// Create proxy objects to communicate with a remote NN. All remote access to an
	/// NN should be funneled through this class. Most of the time you'll want to use
	/// <see cref="CreateProxy{T}(Org.Apache.Hadoop.Conf.Configuration, Sharpen.URI, System.Type{T})
	/// 	"/>
	/// , which will
	/// create either an HA- or non-HA-enabled client proxy as appropriate.
	/// </remarks>
	public class NameNodeProxies
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(NameNodeProxies));

		/// <summary>Wrapper for a client proxy as well as its associated service ID.</summary>
		/// <remarks>
		/// Wrapper for a client proxy as well as its associated service ID.
		/// This is simply used as a tuple-like return type for
		/// <see cref="NameNodeProxies.CreateProxy{T}(Org.Apache.Hadoop.Conf.Configuration, Sharpen.URI, System.Type{T})
		/// 	"/>
		/// and
		/// <see cref="NameNodeProxies.CreateNonHAProxy{T}(Org.Apache.Hadoop.Conf.Configuration, System.Net.IPEndPoint, System.Type{T}, Org.Apache.Hadoop.Security.UserGroupInformation, bool)
		/// 	"/>
		/// .
		/// </remarks>
		public class ProxyAndInfo<Proxytype>
		{
			private readonly PROXYTYPE proxy;

			private readonly Text dtService;

			private readonly IPEndPoint address;

			public ProxyAndInfo(PROXYTYPE proxy, Text dtService, IPEndPoint address)
			{
				this.proxy = proxy;
				this.dtService = dtService;
				this.address = address;
			}

			public virtual PROXYTYPE GetProxy()
			{
				return proxy;
			}

			public virtual Text GetDelegationTokenService()
			{
				return dtService;
			}

			public virtual IPEndPoint GetAddress()
			{
				return address;
			}
		}

		/// <summary>Creates the namenode proxy with the passed protocol.</summary>
		/// <remarks>
		/// Creates the namenode proxy with the passed protocol. This will handle
		/// creation of either HA- or non-HA-enabled proxy objects, depending upon
		/// if the provided URI is a configured logical URI.
		/// </remarks>
		/// <param name="conf">
		/// the configuration containing the required IPC
		/// properties, client failover configurations, etc.
		/// </param>
		/// <param name="nameNodeUri">
		/// the URI pointing either to a specific NameNode
		/// or to a logical nameservice.
		/// </param>
		/// <param name="xface">the IPC interface which should be created</param>
		/// <returns>
		/// an object containing both the proxy and the associated
		/// delegation token service it corresponds to
		/// </returns>
		/// <exception cref="System.IO.IOException">if there is an error creating the proxy</exception>
		public static NameNodeProxies.ProxyAndInfo<T> CreateProxy<T>(Configuration conf, 
			URI nameNodeUri)
		{
			System.Type xface = typeof(T);
			return CreateProxy(conf, nameNodeUri, xface, null);
		}

		/// <summary>Creates the namenode proxy with the passed protocol.</summary>
		/// <remarks>
		/// Creates the namenode proxy with the passed protocol. This will handle
		/// creation of either HA- or non-HA-enabled proxy objects, depending upon
		/// if the provided URI is a configured logical URI.
		/// </remarks>
		/// <param name="conf">
		/// the configuration containing the required IPC
		/// properties, client failover configurations, etc.
		/// </param>
		/// <param name="nameNodeUri">
		/// the URI pointing either to a specific NameNode
		/// or to a logical nameservice.
		/// </param>
		/// <param name="xface">the IPC interface which should be created</param>
		/// <param name="fallbackToSimpleAuth">
		/// set to true or false during calls to indicate if
		/// a secure client falls back to simple auth
		/// </param>
		/// <returns>
		/// an object containing both the proxy and the associated
		/// delegation token service it corresponds to
		/// </returns>
		/// <exception cref="System.IO.IOException">if there is an error creating the proxy</exception>
		public static NameNodeProxies.ProxyAndInfo<T> CreateProxy<T>(Configuration conf, 
			URI nameNodeUri, AtomicBoolean fallbackToSimpleAuth)
		{
			System.Type xface = typeof(T);
			AbstractNNFailoverProxyProvider<T> failoverProxyProvider = CreateFailoverProxyProvider
				(conf, nameNodeUri, xface, true, fallbackToSimpleAuth);
			if (failoverProxyProvider == null)
			{
				// Non-HA case
				return CreateNonHAProxy(conf, NameNode.GetAddress(nameNodeUri), xface, UserGroupInformation
					.GetCurrentUser(), true, fallbackToSimpleAuth);
			}
			else
			{
				// HA case
				DFSClient.Conf config = new DFSClient.Conf(conf);
				T proxy = (T)RetryProxy.Create(xface, failoverProxyProvider, RetryPolicies.FailoverOnNetworkException
					(RetryPolicies.TryOnceThenFail, config.maxFailoverAttempts, config.maxRetryAttempts
					, config.failoverSleepBaseMillis, config.failoverSleepMaxMillis));
				Text dtService;
				if (failoverProxyProvider.UseLogicalURI())
				{
					dtService = HAUtil.BuildTokenServiceForLogicalUri(nameNodeUri, HdfsConstants.HdfsUriScheme
						);
				}
				else
				{
					dtService = SecurityUtil.BuildTokenService(NameNode.GetAddress(nameNodeUri));
				}
				return new NameNodeProxies.ProxyAndInfo<T>(proxy, dtService, NameNode.GetAddress(
					nameNodeUri));
			}
		}

		/// <summary>
		/// Generate a dummy namenode proxy instance that utilizes our hacked
		/// <see cref="Org.Apache.Hadoop.IO.Retry.LossyRetryInvocationHandler{T}"/>
		/// . Proxy instance generated using this
		/// method will proactively drop RPC responses. Currently this method only
		/// support HA setup. null will be returned if the given configuration is not
		/// for HA.
		/// </summary>
		/// <param name="config">
		/// the configuration containing the required IPC
		/// properties, client failover configurations, etc.
		/// </param>
		/// <param name="nameNodeUri">
		/// the URI pointing either to a specific NameNode
		/// or to a logical nameservice.
		/// </param>
		/// <param name="xface">the IPC interface which should be created</param>
		/// <param name="numResponseToDrop">The number of responses to drop for each RPC call
		/// 	</param>
		/// <param name="fallbackToSimpleAuth">
		/// set to true or false during calls to indicate if
		/// a secure client falls back to simple auth
		/// </param>
		/// <returns>
		/// an object containing both the proxy and the associated
		/// delegation token service it corresponds to. Will return null of the
		/// given configuration does not support HA.
		/// </returns>
		/// <exception cref="System.IO.IOException">if there is an error creating the proxy</exception>
		public static NameNodeProxies.ProxyAndInfo<T> CreateProxyWithLossyRetryHandler<T>
			(Configuration config, URI nameNodeUri, int numResponseToDrop, AtomicBoolean fallbackToSimpleAuth
			)
		{
			System.Type xface = typeof(T);
			Preconditions.CheckArgument(numResponseToDrop > 0);
			AbstractNNFailoverProxyProvider<T> failoverProxyProvider = CreateFailoverProxyProvider
				(config, nameNodeUri, xface, true, fallbackToSimpleAuth);
			if (failoverProxyProvider != null)
			{
				// HA case
				int delay = config.GetInt(DFSConfigKeys.DfsClientFailoverSleeptimeBaseKey, DFSConfigKeys
					.DfsClientFailoverSleeptimeBaseDefault);
				int maxCap = config.GetInt(DFSConfigKeys.DfsClientFailoverSleeptimeMaxKey, DFSConfigKeys
					.DfsClientFailoverSleeptimeMaxDefault);
				int maxFailoverAttempts = config.GetInt(DFSConfigKeys.DfsClientFailoverMaxAttemptsKey
					, DFSConfigKeys.DfsClientFailoverMaxAttemptsDefault);
				int maxRetryAttempts = config.GetInt(DFSConfigKeys.DfsClientRetryMaxAttemptsKey, 
					DFSConfigKeys.DfsClientRetryMaxAttemptsDefault);
				InvocationHandler dummyHandler = new LossyRetryInvocationHandler<T>(numResponseToDrop
					, failoverProxyProvider, RetryPolicies.FailoverOnNetworkException(RetryPolicies.
					TryOnceThenFail, maxFailoverAttempts, Math.Max(numResponseToDrop + 1, maxRetryAttempts
					), delay, maxCap));
				T proxy = (T)Proxy.NewProxyInstance(failoverProxyProvider.GetInterface().GetClassLoader
					(), new Type[] { xface }, dummyHandler);
				Text dtService;
				if (failoverProxyProvider.UseLogicalURI())
				{
					dtService = HAUtil.BuildTokenServiceForLogicalUri(nameNodeUri, HdfsConstants.HdfsUriScheme
						);
				}
				else
				{
					dtService = SecurityUtil.BuildTokenService(NameNode.GetAddress(nameNodeUri));
				}
				return new NameNodeProxies.ProxyAndInfo<T>(proxy, dtService, NameNode.GetAddress(
					nameNodeUri));
			}
			else
			{
				Log.Warn("Currently creating proxy using " + "LossyRetryInvocationHandler requires NN HA setup"
					);
				return null;
			}
		}

		/// <summary>Creates an explicitly non-HA-enabled proxy object.</summary>
		/// <remarks>
		/// Creates an explicitly non-HA-enabled proxy object. Most of the time you
		/// don't want to use this, and should instead use
		/// <see cref="CreateProxy{T}(Org.Apache.Hadoop.Conf.Configuration, Sharpen.URI, System.Type{T})
		/// 	"/>
		/// .
		/// </remarks>
		/// <param name="conf">the configuration object</param>
		/// <param name="nnAddr">address of the remote NN to connect to</param>
		/// <param name="xface">the IPC interface which should be created</param>
		/// <param name="ugi">the user who is making the calls on the proxy object</param>
		/// <param name="withRetries">certain interfaces have a non-standard retry policy</param>
		/// <returns>
		/// an object containing both the proxy and the associated
		/// delegation token service it corresponds to
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		public static NameNodeProxies.ProxyAndInfo<T> CreateNonHAProxy<T>(Configuration conf
			, IPEndPoint nnAddr, UserGroupInformation ugi, bool withRetries)
		{
			System.Type xface = typeof(T);
			return CreateNonHAProxy(conf, nnAddr, xface, ugi, withRetries, null);
		}

		/// <summary>Creates an explicitly non-HA-enabled proxy object.</summary>
		/// <remarks>
		/// Creates an explicitly non-HA-enabled proxy object. Most of the time you
		/// don't want to use this, and should instead use
		/// <see cref="CreateProxy{T}(Org.Apache.Hadoop.Conf.Configuration, Sharpen.URI, System.Type{T})
		/// 	"/>
		/// .
		/// </remarks>
		/// <param name="conf">the configuration object</param>
		/// <param name="nnAddr">address of the remote NN to connect to</param>
		/// <param name="xface">the IPC interface which should be created</param>
		/// <param name="ugi">the user who is making the calls on the proxy object</param>
		/// <param name="withRetries">certain interfaces have a non-standard retry policy</param>
		/// <param name="fallbackToSimpleAuth">
		/// - set to true or false during this method to
		/// indicate if a secure client falls back to simple auth
		/// </param>
		/// <returns>
		/// an object containing both the proxy and the associated
		/// delegation token service it corresponds to
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		public static NameNodeProxies.ProxyAndInfo<T> CreateNonHAProxy<T>(Configuration conf
			, IPEndPoint nnAddr, UserGroupInformation ugi, bool withRetries, AtomicBoolean fallbackToSimpleAuth
			)
		{
			System.Type xface = typeof(T);
			Text dtService = SecurityUtil.BuildTokenService(nnAddr);
			T proxy;
			if (xface == typeof(ClientProtocol))
			{
				proxy = (T)CreateNNProxyWithClientProtocol(nnAddr, conf, ugi, withRetries, fallbackToSimpleAuth
					);
			}
			else
			{
				if (xface == typeof(JournalProtocol))
				{
					proxy = (T)CreateNNProxyWithJournalProtocol(nnAddr, conf, ugi);
				}
				else
				{
					if (xface == typeof(NamenodeProtocol))
					{
						proxy = (T)CreateNNProxyWithNamenodeProtocol(nnAddr, conf, ugi, withRetries);
					}
					else
					{
						if (xface == typeof(GetUserMappingsProtocol))
						{
							proxy = (T)CreateNNProxyWithGetUserMappingsProtocol(nnAddr, conf, ugi);
						}
						else
						{
							if (xface == typeof(RefreshUserMappingsProtocol))
							{
								proxy = (T)CreateNNProxyWithRefreshUserMappingsProtocol(nnAddr, conf, ugi);
							}
							else
							{
								if (xface == typeof(RefreshAuthorizationPolicyProtocol))
								{
									proxy = (T)CreateNNProxyWithRefreshAuthorizationPolicyProtocol(nnAddr, conf, ugi);
								}
								else
								{
									if (xface == typeof(RefreshCallQueueProtocol))
									{
										proxy = (T)CreateNNProxyWithRefreshCallQueueProtocol(nnAddr, conf, ugi);
									}
									else
									{
										string message = "Unsupported protocol found when creating the proxy " + "connection to NameNode: "
											 + ((xface != null) ? xface.GetType().FullName : "null");
										Log.Error(message);
										throw new InvalidOperationException(message);
									}
								}
							}
						}
					}
				}
			}
			return new NameNodeProxies.ProxyAndInfo<T>(proxy, dtService, nnAddr);
		}

		/// <exception cref="System.IO.IOException"/>
		private static JournalProtocol CreateNNProxyWithJournalProtocol(IPEndPoint address
			, Configuration conf, UserGroupInformation ugi)
		{
			JournalProtocolPB proxy = (JournalProtocolPB)CreateNameNodeProxy(address, conf, ugi
				, typeof(JournalProtocolPB));
			return new JournalProtocolTranslatorPB(proxy);
		}

		/// <exception cref="System.IO.IOException"/>
		private static RefreshAuthorizationPolicyProtocol CreateNNProxyWithRefreshAuthorizationPolicyProtocol
			(IPEndPoint address, Configuration conf, UserGroupInformation ugi)
		{
			RefreshAuthorizationPolicyProtocolPB proxy = (RefreshAuthorizationPolicyProtocolPB
				)CreateNameNodeProxy(address, conf, ugi, typeof(RefreshAuthorizationPolicyProtocolPB
				));
			return new RefreshAuthorizationPolicyProtocolClientSideTranslatorPB(proxy);
		}

		/// <exception cref="System.IO.IOException"/>
		private static RefreshUserMappingsProtocol CreateNNProxyWithRefreshUserMappingsProtocol
			(IPEndPoint address, Configuration conf, UserGroupInformation ugi)
		{
			RefreshUserMappingsProtocolPB proxy = (RefreshUserMappingsProtocolPB)CreateNameNodeProxy
				(address, conf, ugi, typeof(RefreshUserMappingsProtocolPB));
			return new RefreshUserMappingsProtocolClientSideTranslatorPB(proxy);
		}

		/// <exception cref="System.IO.IOException"/>
		private static RefreshCallQueueProtocol CreateNNProxyWithRefreshCallQueueProtocol
			(IPEndPoint address, Configuration conf, UserGroupInformation ugi)
		{
			RefreshCallQueueProtocolPB proxy = (RefreshCallQueueProtocolPB)CreateNameNodeProxy
				(address, conf, ugi, typeof(RefreshCallQueueProtocolPB));
			return new RefreshCallQueueProtocolClientSideTranslatorPB(proxy);
		}

		/// <exception cref="System.IO.IOException"/>
		private static GetUserMappingsProtocol CreateNNProxyWithGetUserMappingsProtocol(IPEndPoint
			 address, Configuration conf, UserGroupInformation ugi)
		{
			GetUserMappingsProtocolPB proxy = (GetUserMappingsProtocolPB)CreateNameNodeProxy(
				address, conf, ugi, typeof(GetUserMappingsProtocolPB));
			return new GetUserMappingsProtocolClientSideTranslatorPB(proxy);
		}

		/// <exception cref="System.IO.IOException"/>
		private static NamenodeProtocol CreateNNProxyWithNamenodeProtocol(IPEndPoint address
			, Configuration conf, UserGroupInformation ugi, bool withRetries)
		{
			NamenodeProtocolPB proxy = (NamenodeProtocolPB)CreateNameNodeProxy(address, conf, 
				ugi, typeof(NamenodeProtocolPB));
			if (withRetries)
			{
				// create the proxy with retries
				RetryPolicy timeoutPolicy = RetryPolicies.ExponentialBackoffRetry(5, 200, TimeUnit
					.Milliseconds);
				IDictionary<string, RetryPolicy> methodNameToPolicyMap = new Dictionary<string, RetryPolicy
					>();
				methodNameToPolicyMap["getBlocks"] = timeoutPolicy;
				methodNameToPolicyMap["getAccessKeys"] = timeoutPolicy;
				NamenodeProtocol translatorProxy = new NamenodeProtocolTranslatorPB(proxy);
				return (NamenodeProtocol)RetryProxy.Create<NamenodeProtocol>(translatorProxy, methodNameToPolicyMap
					);
			}
			else
			{
				return new NamenodeProtocolTranslatorPB(proxy);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static ClientProtocol CreateNNProxyWithClientProtocol(IPEndPoint address, 
			Configuration conf, UserGroupInformation ugi, bool withRetries, AtomicBoolean fallbackToSimpleAuth
			)
		{
			RPC.SetProtocolEngine(conf, typeof(ClientNamenodeProtocolPB), typeof(ProtobufRpcEngine
				));
			RetryPolicy defaultPolicy = RetryUtils.GetDefaultRetryPolicy(conf, DFSConfigKeys.
				DfsClientRetryPolicyEnabledKey, DFSConfigKeys.DfsClientRetryPolicyEnabledDefault
				, DFSConfigKeys.DfsClientRetryPolicySpecKey, DFSConfigKeys.DfsClientRetryPolicySpecDefault
				, typeof(SafeModeException));
			long version = RPC.GetProtocolVersion(typeof(ClientNamenodeProtocolPB));
			ClientNamenodeProtocolPB proxy = RPC.GetProtocolProxy<ClientNamenodeProtocolPB>(version
				, address, ugi, conf, NetUtils.GetDefaultSocketFactory(conf), Client.GetTimeout(
				conf), defaultPolicy, fallbackToSimpleAuth).GetProxy();
			if (withRetries)
			{
				// create the proxy with retries
				IDictionary<string, RetryPolicy> methodNameToPolicyMap = new Dictionary<string, RetryPolicy
					>();
				ClientProtocol translatorProxy = new ClientNamenodeProtocolTranslatorPB(proxy);
				return (ClientProtocol)RetryProxy.Create<ClientProtocol>(new DefaultFailoverProxyProvider
					<ClientProtocol>(typeof(ClientProtocol), translatorProxy), methodNameToPolicyMap
					, defaultPolicy);
			}
			else
			{
				return new ClientNamenodeProtocolTranslatorPB(proxy);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static object CreateNameNodeProxy(IPEndPoint address, Configuration conf, 
			UserGroupInformation ugi, Type xface)
		{
			RPC.SetProtocolEngine(conf, xface, typeof(ProtobufRpcEngine));
			object proxy = RPC.GetProxy(xface, RPC.GetProtocolVersion(xface), address, ugi, conf
				, NetUtils.GetDefaultSocketFactory(conf));
			return proxy;
		}

		/// <summary>Gets the configured Failover proxy provider's class</summary>
		/// <exception cref="System.IO.IOException"/>
		[VisibleForTesting]
		public static Type GetFailoverProxyProviderClass<T>(Configuration conf, URI nameNodeUri
			)
		{
			if (nameNodeUri == null)
			{
				return null;
			}
			string host = nameNodeUri.GetHost();
			string configKey = DFSConfigKeys.DfsClientFailoverProxyProviderKeyPrefix + "." + 
				host;
			try
			{
				Type ret = (Type)conf.GetClass<FailoverProxyProvider>(configKey, null);
				return ret;
			}
			catch (RuntimeException e)
			{
				if (e.InnerException is TypeLoadException)
				{
					throw new IOException("Could not load failover proxy provider class " + conf.Get(
						configKey) + " which is configured for authority " + nameNodeUri, e);
				}
				else
				{
					throw;
				}
			}
		}

		/// <summary>Creates the Failover proxy provider instance</summary>
		/// <exception cref="System.IO.IOException"/>
		[VisibleForTesting]
		public static AbstractNNFailoverProxyProvider<T> CreateFailoverProxyProvider<T>(Configuration
			 conf, URI nameNodeUri, bool checkPort, AtomicBoolean fallbackToSimpleAuth)
		{
			System.Type xface = typeof(T);
			Type failoverProxyProviderClass = null;
			AbstractNNFailoverProxyProvider<T> providerNN;
			Preconditions.CheckArgument(xface.IsAssignableFrom(typeof(NamenodeProtocols)), "Interface %s is not a NameNode protocol"
				, xface);
			try
			{
				// Obtain the class of the proxy provider
				failoverProxyProviderClass = GetFailoverProxyProviderClass(conf, nameNodeUri);
				if (failoverProxyProviderClass == null)
				{
					return null;
				}
				// Create a proxy provider instance.
				Constructor<FailoverProxyProvider<T>> ctor = failoverProxyProviderClass.GetConstructor
					(typeof(Configuration), typeof(URI), typeof(Type));
				FailoverProxyProvider<T> provider = ctor.NewInstance(conf, nameNodeUri, xface);
				// If the proxy provider is of an old implementation, wrap it.
				if (!(provider is AbstractNNFailoverProxyProvider))
				{
					providerNN = new WrappedFailoverProxyProvider<T>(provider);
				}
				else
				{
					providerNN = (AbstractNNFailoverProxyProvider<T>)provider;
				}
			}
			catch (Exception e)
			{
				string message = "Couldn't create proxy provider " + failoverProxyProviderClass;
				if (Log.IsDebugEnabled())
				{
					Log.Debug(message, e);
				}
				if (e.InnerException is IOException)
				{
					throw (IOException)e.InnerException;
				}
				else
				{
					throw new IOException(message, e);
				}
			}
			// Check the port in the URI, if it is logical.
			if (checkPort && providerNN.UseLogicalURI())
			{
				int port = nameNodeUri.GetPort();
				if (port > 0 && port != NameNode.DefaultPort)
				{
					// Throwing here without any cleanup is fine since we have not
					// actually created the underlying proxies yet.
					throw new IOException("Port " + port + " specified in URI " + nameNodeUri + " but host '"
						 + nameNodeUri.GetHost() + "' is a logical (HA) namenode" + " and does not use port information."
						);
				}
			}
			providerNN.SetFallbackToSimpleAuth(fallbackToSimpleAuth);
			return providerNN;
		}
	}
}
