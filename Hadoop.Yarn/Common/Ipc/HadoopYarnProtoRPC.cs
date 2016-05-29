using System;
using System.Net;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Ipc
{
	/// <summary>This uses Hadoop RPC.</summary>
	/// <remarks>
	/// This uses Hadoop RPC. Uses a tunnel ProtoSpecificRpcEngine over
	/// Hadoop connection.
	/// This does not give cross-language wire compatibility, since the Hadoop
	/// RPC wire format is non-standard, but it does permit use of Protocol Buffers
	/// protocol versioning features for inter-Java RPCs.
	/// </remarks>
	public class HadoopYarnProtoRPC : YarnRPC
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(HadoopYarnProtoRPC));

		public override object GetProxy(Type protocol, IPEndPoint addr, Configuration conf
			)
		{
			Log.Debug("Creating a HadoopYarnProtoRpc proxy for protocol " + protocol);
			return RpcFactoryProvider.GetClientFactory(conf).GetClient(protocol, 1, addr, conf
				);
		}

		public override void StopProxy(object proxy, Configuration conf)
		{
			RpcFactoryProvider.GetClientFactory(conf).StopClient(proxy);
		}

		public override Server GetServer<_T0>(Type protocol, object instance, IPEndPoint 
			addr, Configuration conf, SecretManager<_T0> secretManager, int numHandlers, string
			 portRangeConfig)
		{
			Log.Debug("Creating a HadoopYarnProtoRpc server for protocol " + protocol + " with "
				 + numHandlers + " handlers");
			return RpcFactoryProvider.GetServerFactory(conf).GetServer(protocol, instance, addr
				, conf, secretManager, numHandlers, portRangeConfig);
		}
	}
}
