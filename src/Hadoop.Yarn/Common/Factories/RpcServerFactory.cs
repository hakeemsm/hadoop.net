using System;
using System.Net;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Security.Token;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Factories
{
	public interface RpcServerFactory
	{
		Server GetServer<_T0>(Type protocol, object instance, IPEndPoint addr, Configuration
			 conf, SecretManager<_T0> secretManager, int numHandlers, string portRangeConfig
			)
			where _T0 : TokenIdentifier;
	}
}
