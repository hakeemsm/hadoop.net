using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO.Retry;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Yarn.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Client
{
	public class ConfiguredRMFailoverProxyProvider<T> : RMFailoverProxyProvider<T>
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(ConfiguredRMFailoverProxyProvider
			));

		private int currentProxyIndex = 0;

		internal IDictionary<string, T> proxies = new Dictionary<string, T>();

		private RMProxy<T> rmProxy;

		private Type protocol;

		protected internal YarnConfiguration conf;

		protected internal string[] rmServiceIds;

		public virtual void Init(Configuration configuration, RMProxy<T> rmProxy, Type protocol
			)
		{
			this.rmProxy = rmProxy;
			this.protocol = protocol;
			this.rmProxy.CheckAllowedProtocols(this.protocol);
			this.conf = new YarnConfiguration(configuration);
			ICollection<string> rmIds = HAUtil.GetRMHAIds(conf);
			this.rmServiceIds = Sharpen.Collections.ToArray(rmIds, new string[rmIds.Count]);
			conf.Set(YarnConfiguration.RmHaId, rmServiceIds[currentProxyIndex]);
			conf.SetInt(CommonConfigurationKeysPublic.IpcClientConnectMaxRetriesKey, conf.GetInt
				(YarnConfiguration.ClientFailoverRetries, YarnConfiguration.DefaultClientFailoverRetries
				));
			conf.SetInt(CommonConfigurationKeysPublic.IpcClientConnectMaxRetriesOnSocketTimeoutsKey
				, conf.GetInt(YarnConfiguration.ClientFailoverRetriesOnSocketTimeouts, YarnConfiguration
				.DefaultClientFailoverRetriesOnSocketTimeouts));
		}

		private T GetProxyInternal()
		{
			try
			{
				IPEndPoint rmAddress = rmProxy.GetRMAddress(conf, protocol);
				return RMProxy.GetProxy(conf, protocol, rmAddress);
			}
			catch (IOException ioe)
			{
				Log.Error("Unable to create proxy to the ResourceManager " + rmServiceIds[currentProxyIndex
					], ioe);
				return null;
			}
		}

		public virtual FailoverProxyProvider.ProxyInfo<T> GetProxy()
		{
			lock (this)
			{
				string rmId = rmServiceIds[currentProxyIndex];
				T current = proxies[rmId];
				if (current == null)
				{
					current = GetProxyInternal();
					proxies[rmId] = current;
				}
				return new FailoverProxyProvider.ProxyInfo<T>(current, rmId);
			}
		}

		public virtual void PerformFailover(T currentProxy)
		{
			lock (this)
			{
				currentProxyIndex = (currentProxyIndex + 1) % rmServiceIds.Length;
				conf.Set(YarnConfiguration.RmHaId, rmServiceIds[currentProxyIndex]);
				Log.Info("Failing over to " + rmServiceIds[currentProxyIndex]);
			}
		}

		public virtual Type GetInterface()
		{
			return protocol;
		}

		/// <summary>
		/// Close all the proxy objects which have been opened over the lifetime of
		/// this proxy provider.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void Close()
		{
			lock (this)
			{
				foreach (T proxy in proxies.Values)
				{
					if (proxy is IDisposable)
					{
						((IDisposable)proxy).Close();
					}
					else
					{
						RPC.StopProxy(proxy);
					}
				}
			}
		}
	}
}
