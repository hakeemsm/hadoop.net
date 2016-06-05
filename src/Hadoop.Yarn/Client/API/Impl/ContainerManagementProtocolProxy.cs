using System;
using System.Collections.Generic;
using System.Net;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Client;
using Org.Apache.Hadoop.Yarn.Client.Api;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Ipc;
using Org.Apache.Hadoop.Yarn.Security;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Client.Api.Impl
{
	/// <summary>Helper class to manage container manager proxies</summary>
	public class ContainerManagementProtocolProxy
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Client.Api.Impl.ContainerManagementProtocolProxy
			));

		private readonly int maxConnectedNMs;

		private readonly IDictionary<string, ContainerManagementProtocolProxy.ContainerManagementProtocolProxyData
			> cmProxy;

		private readonly Configuration conf;

		private readonly YarnRPC rpc;

		private NMTokenCache nmTokenCache;

		public ContainerManagementProtocolProxy(Configuration conf)
			: this(conf, NMTokenCache.GetSingleton())
		{
		}

		public ContainerManagementProtocolProxy(Configuration conf, NMTokenCache nmTokenCache
			)
		{
			this.conf = new Configuration(conf);
			this.nmTokenCache = nmTokenCache;
			maxConnectedNMs = conf.GetInt(YarnConfiguration.NmClientMaxNmProxies, YarnConfiguration
				.DefaultNmClientMaxNmProxies);
			if (maxConnectedNMs < 0)
			{
				throw new YarnRuntimeException(YarnConfiguration.NmClientMaxNmProxies + " (" + maxConnectedNMs
					 + ") can not be less than 0.");
			}
			Log.Info(YarnConfiguration.NmClientMaxNmProxies + " : " + maxConnectedNMs);
			if (maxConnectedNMs > 0)
			{
				cmProxy = new LinkedHashMap<string, ContainerManagementProtocolProxy.ContainerManagementProtocolProxyData
					>();
			}
			else
			{
				cmProxy = Sharpen.Collections.EmptyMap();
				// Connections are not being cached so ensure connections close quickly
				// to avoid creating thousands of RPC client threads on large clusters.
				this.conf.SetInt(CommonConfigurationKeysPublic.IpcClientConnectionMaxidletimeKey, 
					0);
			}
			rpc = YarnRPC.Create(conf);
		}

		/// <exception cref="Org.Apache.Hadoop.Security.Token.SecretManager.InvalidToken"/>
		public virtual ContainerManagementProtocolProxy.ContainerManagementProtocolProxyData
			 GetProxy(string containerManagerBindAddr, ContainerId containerId)
		{
			lock (this)
			{
				// This get call will update the map which is working as LRU cache.
				ContainerManagementProtocolProxy.ContainerManagementProtocolProxyData proxy = cmProxy
					[containerManagerBindAddr];
				while (proxy != null && !proxy.token.GetIdentifier().Equals(nmTokenCache.GetToken
					(containerManagerBindAddr).GetIdentifier()))
				{
					Log.Info("Refreshing proxy as NMToken got updated for node : " + containerManagerBindAddr
						);
					// Token is updated. check if anyone has already tried closing it.
					if (!proxy.scheduledForClose)
					{
						// try closing the proxy. Here if someone is already using it
						// then we might not close it. In which case we will wait.
						RemoveProxy(proxy);
					}
					else
					{
						try
						{
							Sharpen.Runtime.Wait(this);
						}
						catch (Exception e)
						{
							Sharpen.Runtime.PrintStackTrace(e);
						}
					}
					if (proxy.activeCallers < 0)
					{
						proxy = cmProxy[containerManagerBindAddr];
					}
				}
				if (proxy == null)
				{
					proxy = new ContainerManagementProtocolProxy.ContainerManagementProtocolProxyData
						(this, rpc, containerManagerBindAddr, containerId, nmTokenCache.GetToken(containerManagerBindAddr
						));
					if (maxConnectedNMs > 0)
					{
						AddProxyToCache(containerManagerBindAddr, proxy);
					}
				}
				// This is to track active users of this proxy.
				proxy.activeCallers++;
				UpdateLRUCache(containerManagerBindAddr);
				return proxy;
			}
		}

		private void AddProxyToCache(string containerManagerBindAddr, ContainerManagementProtocolProxy.ContainerManagementProtocolProxyData
			 proxy)
		{
			while (cmProxy.Count >= maxConnectedNMs)
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Cleaning up the proxy cache, size=" + cmProxy.Count + " max=" + maxConnectedNMs
						);
				}
				bool removedProxy = false;
				foreach (ContainerManagementProtocolProxy.ContainerManagementProtocolProxyData otherProxy
					 in cmProxy.Values)
				{
					removedProxy = RemoveProxy(otherProxy);
					if (removedProxy)
					{
						break;
					}
				}
				if (!removedProxy)
				{
					// all of the proxies are currently in use and already scheduled
					// for removal, so we need to wait until at least one of them closes
					try
					{
						Sharpen.Runtime.Wait(this);
					}
					catch (Exception e)
					{
						Sharpen.Runtime.PrintStackTrace(e);
					}
				}
			}
			if (maxConnectedNMs > 0)
			{
				cmProxy[containerManagerBindAddr] = proxy;
			}
		}

		private void UpdateLRUCache(string containerManagerBindAddr)
		{
			if (maxConnectedNMs > 0)
			{
				ContainerManagementProtocolProxy.ContainerManagementProtocolProxyData proxy = Sharpen.Collections.Remove
					(cmProxy, containerManagerBindAddr);
				cmProxy[containerManagerBindAddr] = proxy;
			}
		}

		public virtual void MayBeCloseProxy(ContainerManagementProtocolProxy.ContainerManagementProtocolProxyData
			 proxy)
		{
			lock (this)
			{
				TryCloseProxy(proxy);
			}
		}

		private bool TryCloseProxy(ContainerManagementProtocolProxy.ContainerManagementProtocolProxyData
			 proxy)
		{
			proxy.activeCallers--;
			if (proxy.scheduledForClose && proxy.activeCallers < 0)
			{
				Log.Info("Closing proxy : " + proxy.containerManagerBindAddr);
				Sharpen.Collections.Remove(cmProxy, proxy.containerManagerBindAddr);
				try
				{
					rpc.StopProxy(proxy.GetContainerManagementProtocol(), conf);
				}
				finally
				{
					Sharpen.Runtime.NotifyAll(this);
				}
				return true;
			}
			return false;
		}

		private bool RemoveProxy(ContainerManagementProtocolProxy.ContainerManagementProtocolProxyData
			 proxy)
		{
			lock (this)
			{
				if (!proxy.scheduledForClose)
				{
					proxy.scheduledForClose = true;
					return TryCloseProxy(proxy);
				}
				return false;
			}
		}

		public virtual void StopAllProxies()
		{
			lock (this)
			{
				IList<string> nodeIds = new AList<string>();
				Sharpen.Collections.AddAll(nodeIds, this.cmProxy.Keys);
				foreach (string nodeId in nodeIds)
				{
					ContainerManagementProtocolProxy.ContainerManagementProtocolProxyData proxy = cmProxy
						[nodeId];
					// Explicitly reducing the proxy count to allow stopping proxy.
					proxy.activeCallers = 0;
					try
					{
						RemoveProxy(proxy);
					}
					catch (Exception t)
					{
						Log.Error("Error closing connection", t);
					}
				}
				cmProxy.Clear();
			}
		}

		public class ContainerManagementProtocolProxyData
		{
			private readonly string containerManagerBindAddr;

			private readonly ContainerManagementProtocol proxy;

			private int activeCallers;

			private bool scheduledForClose;

			private readonly Token token;

			/// <exception cref="Org.Apache.Hadoop.Security.Token.SecretManager.InvalidToken"/>
			[InterfaceAudience.Private]
			[VisibleForTesting]
			public ContainerManagementProtocolProxyData(ContainerManagementProtocolProxy _enclosing
				, YarnRPC rpc, string containerManagerBindAddr, ContainerId containerId, Token token
				)
			{
				this._enclosing = _enclosing;
				this.containerManagerBindAddr = containerManagerBindAddr;
				this.activeCallers = 0;
				this.scheduledForClose = false;
				this.token = token;
				this.proxy = this.NewProxy(rpc, containerManagerBindAddr, containerId, token);
			}

			/// <exception cref="Org.Apache.Hadoop.Security.Token.SecretManager.InvalidToken"/>
			[InterfaceAudience.Private]
			[VisibleForTesting]
			protected internal virtual ContainerManagementProtocol NewProxy(YarnRPC rpc, string
				 containerManagerBindAddr, ContainerId containerId, Token token)
			{
				if (token == null)
				{
					throw new SecretManager.InvalidToken("No NMToken sent for " + containerManagerBindAddr
						);
				}
				IPEndPoint cmAddr = NetUtils.CreateSocketAddr(containerManagerBindAddr);
				ContainerManagementProtocolProxy.Log.Info("Opening proxy : " + containerManagerBindAddr
					);
				// the user in createRemoteUser in this context has to be ContainerID
				UserGroupInformation user = UserGroupInformation.CreateRemoteUser(containerId.GetApplicationAttemptId
					().ToString());
				Org.Apache.Hadoop.Security.Token.Token<NMTokenIdentifier> nmToken = ConverterUtils
					.ConvertFromYarn(token, cmAddr);
				user.AddToken(nmToken);
				return NMProxy.CreateNMProxy<ContainerManagementProtocol>(this._enclosing.conf, user
					, rpc, cmAddr);
			}

			public virtual ContainerManagementProtocol GetContainerManagementProtocol()
			{
				return this.proxy;
			}

			private readonly ContainerManagementProtocolProxy _enclosing;
		}
	}
}
