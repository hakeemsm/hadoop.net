using System;
using System.Net;
using Hadoop.Common.Core.Conf;
using Javax.Net;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO.Retry;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Token;
using Sharpen;

namespace Org.Apache.Hadoop.Ipc
{
	/// <summary>An RPC implementation.</summary>
	public interface RpcEngine
	{
		/// <summary>Construct a client-side proxy object.</summary>
		/// <?/>
		/// <exception cref="System.IO.IOException"/>
		ProtocolProxy<T> GetProxy<T>(long clientVersion, IPEndPoint addr, UserGroupInformation
			 ticket, Configuration conf, SocketFactory factory, int rpcTimeout, RetryPolicy 
			connectionRetryPolicy);

		/// <summary>Construct a client-side proxy object.</summary>
		/// <exception cref="System.IO.IOException"/>
		ProtocolProxy<T> GetProxy<T>(long clientVersion, IPEndPoint addr, UserGroupInformation
			 ticket, Configuration conf, SocketFactory factory, int rpcTimeout, RetryPolicy 
			connectionRetryPolicy, AtomicBoolean fallbackToSimpleAuth);

		/// <summary>Construct a server for a protocol implementation instance.</summary>
		/// <param name="protocol">the class of protocol to use</param>
		/// <param name="instance">the instance of protocol whose methods will be called</param>
		/// <param name="conf">the configuration to use</param>
		/// <param name="bindAddress">the address to bind on to listen for connection</param>
		/// <param name="port">the port to listen for connections on</param>
		/// <param name="numHandlers">the number of method handler threads to run</param>
		/// <param name="numReaders">the number of reader threads to run</param>
		/// <param name="queueSizePerHandler">the size of the queue per hander thread</param>
		/// <param name="verbose">whether each call should be logged</param>
		/// <param name="secretManager">The secret manager to use to validate incoming requests.
		/// 	</param>
		/// <param name="portRangeConfig">
		/// A config parameter that can be used to restrict
		/// the range of ports used when port is 0 (an ephemeral port)
		/// </param>
		/// <returns>The Server instance</returns>
		/// <exception cref="System.IO.IOException">on any error</exception>
		RPC.Server GetServer<_T0>(Type protocol, object instance, string bindAddress, int
			 port, int numHandlers, int numReaders, int queueSizePerHandler, bool verbose, Configuration
			 conf, SecretManager<_T0> secretManager, string portRangeConfig)
			where _T0 : TokenIdentifier;

		/// <summary>
		/// Returns a proxy for ProtocolMetaInfoPB, which uses the given connection
		/// id.
		/// </summary>
		/// <?/>
		/// <?/>
		/// <?/>
		/// <returns>Proxy object.</returns>
		/// <exception cref="System.IO.IOException"/>
		ProtocolProxy<ProtocolMetaInfoPB> GetProtocolMetaInfoProxy(Client.ConnectionId connId
			, Configuration conf, SocketFactory factory);
	}
}
