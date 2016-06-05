using System;
using System.Net;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Ipc
{
	/// <summary>Abstraction to get the RPC implementation for Yarn.</summary>
	public abstract class YarnRPC
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(YarnRPC));

		public abstract object GetProxy(Type protocol, IPEndPoint addr, Configuration conf
			);

		public abstract void StopProxy(object proxy, Configuration conf);

		public abstract Server GetServer<_T0>(Type protocol, object instance, IPEndPoint 
			addr, Configuration conf, SecretManager<_T0> secretManager, int numHandlers, string
			 portRangeConfig)
			where _T0 : TokenIdentifier;

		public virtual Server GetServer<_T0>(Type protocol, object instance, IPEndPoint addr
			, Configuration conf, SecretManager<_T0> secretManager, int numHandlers)
			where _T0 : TokenIdentifier
		{
			return GetServer(protocol, instance, addr, conf, secretManager, numHandlers, null
				);
		}

		public static YarnRPC Create(Configuration conf)
		{
			Log.Debug("Creating YarnRPC for " + conf.Get(YarnConfiguration.IpcRpcImpl));
			string clazzName = conf.Get(YarnConfiguration.IpcRpcImpl);
			if (clazzName == null)
			{
				clazzName = YarnConfiguration.DefaultIpcRpcImpl;
			}
			try
			{
				return (YarnRPC)System.Activator.CreateInstance(Sharpen.Runtime.GetType(clazzName
					));
			}
			catch (Exception e)
			{
				throw new YarnRuntimeException(e);
			}
		}
	}
}
