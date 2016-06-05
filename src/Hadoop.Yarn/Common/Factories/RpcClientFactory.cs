using System;
using System.Net;
using Org.Apache.Hadoop.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Factories
{
	public interface RpcClientFactory
	{
		object GetClient(Type protocol, long clientVersion, IPEndPoint addr, Configuration
			 conf);

		void StopClient(object proxy);
	}
}
