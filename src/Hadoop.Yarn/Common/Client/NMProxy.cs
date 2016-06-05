using System.Net;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO.Retry;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Ipc;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Client
{
	public class NMProxy : ServerProxy
	{
		public static T CreateNMProxy<T>(Configuration conf, UserGroupInformation ugi, YarnRPC
			 rpc, IPEndPoint serverAddress)
		{
			System.Type protocol = typeof(T);
			RetryPolicy retryPolicy = CreateRetryPolicy(conf, YarnConfiguration.ClientNmConnectMaxWaitMs
				, YarnConfiguration.DefaultClientNmConnectMaxWaitMs, YarnConfiguration.ClientNmConnectRetryIntervalMs
				, YarnConfiguration.DefaultClientNmConnectRetryIntervalMs);
			return CreateRetriableProxy(conf, protocol, ugi, rpc, serverAddress, retryPolicy);
		}
	}
}
