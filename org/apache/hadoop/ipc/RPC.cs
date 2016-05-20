using Sharpen;

namespace org.apache.hadoop.ipc
{
	/// <summary>A simple RPC mechanism.</summary>
	/// <remarks>
	/// A simple RPC mechanism.
	/// A <i>protocol</i> is a Java interface.  All parameters and return types must
	/// be one of:
	/// <ul> <li>a primitive type, <code>boolean</code>, <code>byte</code>,
	/// <code>char</code>, <code>short</code>, <code>int</code>, <code>long</code>,
	/// <code>float</code>, <code>double</code>, or <code>void</code>; or</li>
	/// <li>a
	/// <see cref="string"/>
	/// ; or</li>
	/// <li>a
	/// <see cref="org.apache.hadoop.io.Writable"/>
	/// ; or</li>
	/// <li>an array of the above types</li> </ul>
	/// All methods in the protocol should throw only IOException.  No field data of
	/// the protocol instance is transmitted.
	/// </remarks>
	public class RPC
	{
		internal const int RPC_SERVICE_CLASS_DEFAULT = 0;

		[System.Serializable]
		public sealed class RpcKind
		{
			public static readonly org.apache.hadoop.ipc.RPC.RpcKind RPC_BUILTIN = new org.apache.hadoop.ipc.RPC.RpcKind
				((short)1);

			public static readonly org.apache.hadoop.ipc.RPC.RpcKind RPC_WRITABLE = new org.apache.hadoop.ipc.RPC.RpcKind
				((short)2);

			public static readonly org.apache.hadoop.ipc.RPC.RpcKind RPC_PROTOCOL_BUFFER = new 
				org.apache.hadoop.ipc.RPC.RpcKind((short)3);

			internal static readonly short MAX_INDEX = org.apache.hadoop.ipc.RPC.RpcKind.RPC_PROTOCOL_BUFFER
				.value;

			public readonly short value;

			internal RpcKind(short val)
			{
				// Used for built in calls by tests
				// Use WritableRpcEngine 
				// Use ProtobufRpcEngine
				// used for array size
				//TODO make it private
				this.value = val;
			}
		}

		internal interface RpcInvoker
		{
			/// <summary>Process a client call on the server side</summary>
			/// <param name="server">the server within whose context this rpc call is made</param>
			/// <param name="protocol">
			/// - the protocol name (the class of the client proxy
			/// used to make calls to the rpc server.
			/// </param>
			/// <param name="rpcRequest">- deserialized</param>
			/// <param name="receiveTime">time at which the call received (for metrics)</param>
			/// <returns>the call's return</returns>
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			org.apache.hadoop.io.Writable call(org.apache.hadoop.ipc.RPC.Server server, string
				 protocol, org.apache.hadoop.io.Writable rpcRequest, long receiveTime);
		}

		internal static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ipc.RPC)));

		/// <summary>Get all superInterfaces that extend VersionedProtocol</summary>
		/// <param name="childInterfaces"/>
		/// <returns>the super interfaces that extend VersionedProtocol</returns>
		internal static java.lang.Class[] getSuperInterfaces(java.lang.Class[] childInterfaces
			)
		{
			System.Collections.Generic.IList<java.lang.Class> allInterfaces = new System.Collections.Generic.List
				<java.lang.Class>();
			foreach (java.lang.Class childInterface in childInterfaces)
			{
				if (Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ipc.VersionedProtocol
					)).isAssignableFrom(childInterface))
				{
					allInterfaces.add(childInterface);
					Sharpen.Collections.AddAll(allInterfaces, java.util.Arrays.asList(getSuperInterfaces
						(childInterface.getInterfaces())));
				}
				else
				{
					LOG.warn("Interface " + childInterface + " ignored because it does not extend VersionedProtocol"
						);
				}
			}
			return Sharpen.Collections.ToArray(allInterfaces, new java.lang.Class[allInterfaces
				.Count]);
		}

		/// <summary>
		/// Get all interfaces that the given protocol implements or extends
		/// which are assignable from VersionedProtocol.
		/// </summary>
		internal static java.lang.Class[] getProtocolInterfaces(java.lang.Class protocol)
		{
			java.lang.Class[] interfaces = protocol.getInterfaces();
			return getSuperInterfaces(interfaces);
		}

		/// <summary>Get the protocol name.</summary>
		/// <remarks>
		/// Get the protocol name.
		/// If the protocol class has a ProtocolAnnotation, then get the protocol
		/// name from the annotation; otherwise the class name is the protocol name.
		/// </remarks>
		public static string getProtocolName(java.lang.Class protocol)
		{
			if (protocol == null)
			{
				return null;
			}
			org.apache.hadoop.ipc.ProtocolInfo anno = protocol.getAnnotation<org.apache.hadoop.ipc.ProtocolInfo
				>();
			return (anno == null) ? protocol.getName() : anno.protocolName();
		}

		/// <summary>Get the protocol version from protocol class.</summary>
		/// <remarks>
		/// Get the protocol version from protocol class.
		/// If the protocol class has a ProtocolAnnotation, then get the protocol
		/// name from the annotation; otherwise the class name is the protocol name.
		/// </remarks>
		public static long getProtocolVersion(java.lang.Class protocol)
		{
			if (protocol == null)
			{
				throw new System.ArgumentException("Null protocol");
			}
			long version;
			org.apache.hadoop.ipc.ProtocolInfo anno = protocol.getAnnotation<org.apache.hadoop.ipc.ProtocolInfo
				>();
			if (anno != null)
			{
				version = anno.protocolVersion();
				if (version != -1)
				{
					return version;
				}
			}
			try
			{
				java.lang.reflect.Field versionField = protocol.getField("versionID");
				versionField.setAccessible(true);
				return versionField.getLong(protocol);
			}
			catch (java.lang.NoSuchFieldException ex)
			{
				throw new System.Exception(ex);
			}
			catch (java.lang.IllegalAccessException ex)
			{
				throw new System.Exception(ex);
			}
		}

		private RPC()
		{
		}

		private static readonly System.Collections.Generic.IDictionary<java.lang.Class, org.apache.hadoop.ipc.RpcEngine
			> PROTOCOL_ENGINES = new System.Collections.Generic.Dictionary<java.lang.Class, 
			org.apache.hadoop.ipc.RpcEngine>();

		private const string ENGINE_PROP = "rpc.engine";

		// no public ctor
		// cache of RpcEngines by protocol
		/// <summary>Set a protocol to use a non-default RpcEngine.</summary>
		/// <param name="conf">configuration to use</param>
		/// <param name="protocol">the protocol interface</param>
		/// <param name="engine">the RpcEngine impl</param>
		public static void setProtocolEngine(org.apache.hadoop.conf.Configuration conf, java.lang.Class
			 protocol, java.lang.Class engine)
		{
			conf.setClass(ENGINE_PROP + "." + protocol.getName(), engine, Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.ipc.RpcEngine)));
		}

		// return the RpcEngine configured to handle a protocol
		internal static org.apache.hadoop.ipc.RpcEngine getProtocolEngine(java.lang.Class
			 protocol, org.apache.hadoop.conf.Configuration conf)
		{
			lock (typeof(RPC))
			{
				org.apache.hadoop.ipc.RpcEngine engine = PROTOCOL_ENGINES[protocol];
				if (engine == null)
				{
					java.lang.Class impl = conf.getClass(ENGINE_PROP + "." + protocol.getName(), Sharpen.Runtime.getClassForType
						(typeof(org.apache.hadoop.ipc.WritableRpcEngine)));
					engine = (org.apache.hadoop.ipc.RpcEngine)org.apache.hadoop.util.ReflectionUtils.
						newInstance(impl, conf);
					PROTOCOL_ENGINES[protocol] = engine;
				}
				return engine;
			}
		}

		/// <summary>A version mismatch for the RPC protocol.</summary>
		[System.Serializable]
		public class VersionMismatch : org.apache.hadoop.ipc.RpcServerException
		{
			private const long serialVersionUID = 0;

			private string interfaceName;

			private long clientVersion;

			private long serverVersion;

			/// <summary>Create a version mismatch exception</summary>
			/// <param name="interfaceName">the name of the protocol mismatch</param>
			/// <param name="clientVersion">the client's version of the protocol</param>
			/// <param name="serverVersion">the server's version of the protocol</param>
			public VersionMismatch(string interfaceName, long clientVersion, long serverVersion
				)
				: base("Protocol " + interfaceName + " version mismatch. (client = " + clientVersion
					 + ", server = " + serverVersion + ")")
			{
				this.interfaceName = interfaceName;
				this.clientVersion = clientVersion;
				this.serverVersion = serverVersion;
			}

			/// <summary>Get the interface name</summary>
			/// <returns>
			/// the java class name
			/// (eg. org.apache.hadoop.mapred.InterTrackerProtocol)
			/// </returns>
			public virtual string getInterfaceName()
			{
				return interfaceName;
			}

			/// <summary>Get the client's preferred version</summary>
			public virtual long getClientVersion()
			{
				return clientVersion;
			}

			/// <summary>Get the server's agreed to version.</summary>
			public virtual long getServerVersion()
			{
				return serverVersion;
			}

			/// <summary>get the rpc status corresponding to this exception</summary>
			public override org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto
				 getRpcStatusProto()
			{
				return org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto
					.ERROR;
			}

			/// <summary>get the detailed rpc status corresponding to this exception</summary>
			public override org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto
				 getRpcErrorCodeProto()
			{
				return org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto
					.ERROR_RPC_VERSION_MISMATCH;
			}
		}

		/// <summary>Get a proxy connection to a remote server</summary>
		/// <param name="protocol">protocol class</param>
		/// <param name="clientVersion">client version</param>
		/// <param name="addr">remote address</param>
		/// <param name="conf">configuration to use</param>
		/// <returns>the proxy</returns>
		/// <exception cref="System.IO.IOException">if the far end through a RemoteException</exception>
		public static T waitForProxy<T>(long clientVersion, java.net.InetSocketAddress addr
			, org.apache.hadoop.conf.Configuration conf)
		{
			System.Type protocol = typeof(T);
			return waitForProtocolProxy(protocol, clientVersion, addr, conf).getProxy();
		}

		/// <summary>
		/// Get a protocol proxy that contains a proxy connection to a remote server
		/// and a set of methods that are supported by the server
		/// </summary>
		/// <param name="protocol">protocol class</param>
		/// <param name="clientVersion">client version</param>
		/// <param name="addr">remote address</param>
		/// <param name="conf">configuration to use</param>
		/// <returns>the protocol proxy</returns>
		/// <exception cref="System.IO.IOException">if the far end through a RemoteException</exception>
		public static org.apache.hadoop.ipc.ProtocolProxy<T> waitForProtocolProxy<T>(long
			 clientVersion, java.net.InetSocketAddress addr, org.apache.hadoop.conf.Configuration
			 conf)
		{
			System.Type protocol = typeof(T);
			return waitForProtocolProxy(protocol, clientVersion, addr, conf, long.MaxValue);
		}

		/// <summary>Get a proxy connection to a remote server</summary>
		/// <param name="protocol">protocol class</param>
		/// <param name="clientVersion">client version</param>
		/// <param name="addr">remote address</param>
		/// <param name="conf">configuration to use</param>
		/// <param name="connTimeout">time in milliseconds before giving up</param>
		/// <returns>the proxy</returns>
		/// <exception cref="System.IO.IOException">if the far end through a RemoteException</exception>
		public static T waitForProxy<T>(long clientVersion, java.net.InetSocketAddress addr
			, org.apache.hadoop.conf.Configuration conf, long connTimeout)
		{
			System.Type protocol = typeof(T);
			return waitForProtocolProxy(protocol, clientVersion, addr, conf, connTimeout).getProxy
				();
		}

		/// <summary>
		/// Get a protocol proxy that contains a proxy connection to a remote server
		/// and a set of methods that are supported by the server
		/// </summary>
		/// <param name="protocol">protocol class</param>
		/// <param name="clientVersion">client version</param>
		/// <param name="addr">remote address</param>
		/// <param name="conf">configuration to use</param>
		/// <param name="connTimeout">time in milliseconds before giving up</param>
		/// <returns>the protocol proxy</returns>
		/// <exception cref="System.IO.IOException">if the far end through a RemoteException</exception>
		public static org.apache.hadoop.ipc.ProtocolProxy<T> waitForProtocolProxy<T>(long
			 clientVersion, java.net.InetSocketAddress addr, org.apache.hadoop.conf.Configuration
			 conf, long connTimeout)
		{
			System.Type protocol = typeof(T);
			return waitForProtocolProxy(protocol, clientVersion, addr, conf, 0, null, connTimeout
				);
		}

		/// <summary>Get a proxy connection to a remote server</summary>
		/// <param name="protocol">protocol class</param>
		/// <param name="clientVersion">client version</param>
		/// <param name="addr">remote address</param>
		/// <param name="conf">configuration to use</param>
		/// <param name="rpcTimeout">timeout for each RPC</param>
		/// <param name="timeout">time in milliseconds before giving up</param>
		/// <returns>the proxy</returns>
		/// <exception cref="System.IO.IOException">if the far end through a RemoteException</exception>
		public static T waitForProxy<T>(long clientVersion, java.net.InetSocketAddress addr
			, org.apache.hadoop.conf.Configuration conf, int rpcTimeout, long timeout)
		{
			System.Type protocol = typeof(T);
			return waitForProtocolProxy(protocol, clientVersion, addr, conf, rpcTimeout, null
				, timeout).getProxy();
		}

		/// <summary>
		/// Get a protocol proxy that contains a proxy connection to a remote server
		/// and a set of methods that are supported by the server
		/// </summary>
		/// <param name="protocol">protocol class</param>
		/// <param name="clientVersion">client version</param>
		/// <param name="addr">remote address</param>
		/// <param name="conf">configuration to use</param>
		/// <param name="rpcTimeout">timeout for each RPC</param>
		/// <param name="timeout">time in milliseconds before giving up</param>
		/// <returns>the proxy</returns>
		/// <exception cref="System.IO.IOException">if the far end through a RemoteException</exception>
		public static org.apache.hadoop.ipc.ProtocolProxy<T> waitForProtocolProxy<T>(long
			 clientVersion, java.net.InetSocketAddress addr, org.apache.hadoop.conf.Configuration
			 conf, int rpcTimeout, org.apache.hadoop.io.retry.RetryPolicy connectionRetryPolicy
			, long timeout)
		{
			System.Type protocol = typeof(T);
			long startTime = org.apache.hadoop.util.Time.now();
			System.IO.IOException ioe;
			while (true)
			{
				try
				{
					return getProtocolProxy(protocol, clientVersion, addr, org.apache.hadoop.security.UserGroupInformation
						.getCurrentUser(), conf, org.apache.hadoop.net.NetUtils.getDefaultSocketFactory(
						conf), rpcTimeout, connectionRetryPolicy);
				}
				catch (java.net.ConnectException se)
				{
					// namenode has not been started
					LOG.info("Server at " + addr + " not available yet, Zzzzz...");
					ioe = se;
				}
				catch (java.net.SocketTimeoutException te)
				{
					// namenode is busy
					LOG.info("Problem connecting to server: " + addr);
					ioe = te;
				}
				catch (java.net.NoRouteToHostException nrthe)
				{
					// perhaps a VIP is failing over
					LOG.info("No route to host for server: " + addr);
					ioe = nrthe;
				}
				// check if timed out
				if (org.apache.hadoop.util.Time.now() - timeout >= startTime)
				{
					throw ioe;
				}
				if (java.lang.Thread.currentThread().isInterrupted())
				{
					// interrupted during some IO; this may not have been caught
					throw new java.io.InterruptedIOException("Interrupted waiting for the proxy");
				}
				// wait for retry
				try
				{
					java.lang.Thread.sleep(1000);
				}
				catch (System.Exception)
				{
					java.lang.Thread.currentThread().interrupt();
					throw (System.IO.IOException)new java.io.InterruptedIOException("Interrupted waiting for the proxy"
						).initCause(ioe);
				}
			}
		}

		/// <summary>
		/// Construct a client-side proxy object that implements the named protocol,
		/// talking to a server at the named address.
		/// </summary>
		/// <?/>
		/// <exception cref="System.IO.IOException"/>
		public static T getProxy<T>(long clientVersion, java.net.InetSocketAddress addr, 
			org.apache.hadoop.conf.Configuration conf, javax.net.SocketFactory factory)
		{
			System.Type protocol = typeof(T);
			return getProtocolProxy(protocol, clientVersion, addr, conf, factory).getProxy();
		}

		/// <summary>
		/// Get a protocol proxy that contains a proxy connection to a remote server
		/// and a set of methods that are supported by the server
		/// </summary>
		/// <param name="protocol">protocol class</param>
		/// <param name="clientVersion">client version</param>
		/// <param name="addr">remote address</param>
		/// <param name="conf">configuration to use</param>
		/// <param name="factory">socket factory</param>
		/// <returns>the protocol proxy</returns>
		/// <exception cref="System.IO.IOException">if the far end through a RemoteException</exception>
		public static org.apache.hadoop.ipc.ProtocolProxy<T> getProtocolProxy<T>(long clientVersion
			, java.net.InetSocketAddress addr, org.apache.hadoop.conf.Configuration conf, javax.net.SocketFactory
			 factory)
		{
			System.Type protocol = typeof(T);
			org.apache.hadoop.security.UserGroupInformation ugi = org.apache.hadoop.security.UserGroupInformation
				.getCurrentUser();
			return getProtocolProxy(protocol, clientVersion, addr, ugi, conf, factory);
		}

		/// <summary>
		/// Construct a client-side proxy object that implements the named protocol,
		/// talking to a server at the named address.
		/// </summary>
		/// <?/>
		/// <exception cref="System.IO.IOException"/>
		public static T getProxy<T>(long clientVersion, java.net.InetSocketAddress addr, 
			org.apache.hadoop.security.UserGroupInformation ticket, org.apache.hadoop.conf.Configuration
			 conf, javax.net.SocketFactory factory)
		{
			System.Type protocol = typeof(T);
			return getProtocolProxy(protocol, clientVersion, addr, ticket, conf, factory).getProxy
				();
		}

		/// <summary>
		/// Get a protocol proxy that contains a proxy connection to a remote server
		/// and a set of methods that are supported by the server
		/// </summary>
		/// <param name="protocol">protocol class</param>
		/// <param name="clientVersion">client version</param>
		/// <param name="addr">remote address</param>
		/// <param name="ticket">user group information</param>
		/// <param name="conf">configuration to use</param>
		/// <param name="factory">socket factory</param>
		/// <returns>the protocol proxy</returns>
		/// <exception cref="System.IO.IOException">if the far end through a RemoteException</exception>
		public static org.apache.hadoop.ipc.ProtocolProxy<T> getProtocolProxy<T>(long clientVersion
			, java.net.InetSocketAddress addr, org.apache.hadoop.security.UserGroupInformation
			 ticket, org.apache.hadoop.conf.Configuration conf, javax.net.SocketFactory factory
			)
		{
			System.Type protocol = typeof(T);
			return getProtocolProxy(protocol, clientVersion, addr, ticket, conf, factory, 0, 
				null);
		}

		/// <summary>
		/// Construct a client-side proxy that implements the named protocol,
		/// talking to a server at the named address.
		/// </summary>
		/// <?/>
		/// <param name="protocol">protocol</param>
		/// <param name="clientVersion">client's version</param>
		/// <param name="addr">server address</param>
		/// <param name="ticket">security ticket</param>
		/// <param name="conf">configuration</param>
		/// <param name="factory">socket factory</param>
		/// <param name="rpcTimeout">max time for each rpc; 0 means no timeout</param>
		/// <returns>the proxy</returns>
		/// <exception cref="System.IO.IOException">if any error occurs</exception>
		public static T getProxy<T>(long clientVersion, java.net.InetSocketAddress addr, 
			org.apache.hadoop.security.UserGroupInformation ticket, org.apache.hadoop.conf.Configuration
			 conf, javax.net.SocketFactory factory, int rpcTimeout)
		{
			System.Type protocol = typeof(T);
			return getProtocolProxy(protocol, clientVersion, addr, ticket, conf, factory, rpcTimeout
				, null).getProxy();
		}

		/// <summary>
		/// Get a protocol proxy that contains a proxy connection to a remote server
		/// and a set of methods that are supported by the server
		/// </summary>
		/// <param name="protocol">protocol</param>
		/// <param name="clientVersion">client's version</param>
		/// <param name="addr">server address</param>
		/// <param name="ticket">security ticket</param>
		/// <param name="conf">configuration</param>
		/// <param name="factory">socket factory</param>
		/// <param name="rpcTimeout">max time for each rpc; 0 means no timeout</param>
		/// <param name="connectionRetryPolicy">retry policy</param>
		/// <returns>the proxy</returns>
		/// <exception cref="System.IO.IOException">if any error occurs</exception>
		public static org.apache.hadoop.ipc.ProtocolProxy<T> getProtocolProxy<T>(long clientVersion
			, java.net.InetSocketAddress addr, org.apache.hadoop.security.UserGroupInformation
			 ticket, org.apache.hadoop.conf.Configuration conf, javax.net.SocketFactory factory
			, int rpcTimeout, org.apache.hadoop.io.retry.RetryPolicy connectionRetryPolicy)
		{
			System.Type protocol = typeof(T);
			return getProtocolProxy(protocol, clientVersion, addr, ticket, conf, factory, rpcTimeout
				, connectionRetryPolicy, null);
		}

		/// <summary>
		/// Get a protocol proxy that contains a proxy connection to a remote server
		/// and a set of methods that are supported by the server
		/// </summary>
		/// <param name="protocol">protocol</param>
		/// <param name="clientVersion">client's version</param>
		/// <param name="addr">server address</param>
		/// <param name="ticket">security ticket</param>
		/// <param name="conf">configuration</param>
		/// <param name="factory">socket factory</param>
		/// <param name="rpcTimeout">max time for each rpc; 0 means no timeout</param>
		/// <param name="connectionRetryPolicy">retry policy</param>
		/// <param name="fallbackToSimpleAuth">
		/// set to true or false during calls to indicate if
		/// a secure client falls back to simple auth
		/// </param>
		/// <returns>the proxy</returns>
		/// <exception cref="System.IO.IOException">if any error occurs</exception>
		public static org.apache.hadoop.ipc.ProtocolProxy<T> getProtocolProxy<T>(long clientVersion
			, java.net.InetSocketAddress addr, org.apache.hadoop.security.UserGroupInformation
			 ticket, org.apache.hadoop.conf.Configuration conf, javax.net.SocketFactory factory
			, int rpcTimeout, org.apache.hadoop.io.retry.RetryPolicy connectionRetryPolicy, 
			java.util.concurrent.atomic.AtomicBoolean fallbackToSimpleAuth)
		{
			System.Type protocol = typeof(T);
			if (org.apache.hadoop.security.UserGroupInformation.isSecurityEnabled())
			{
				org.apache.hadoop.security.SaslRpcServer.init(conf);
			}
			return getProtocolEngine(protocol, conf).getProxy(protocol, clientVersion, addr, 
				ticket, conf, factory, rpcTimeout, connectionRetryPolicy, fallbackToSimpleAuth);
		}

		/// <summary>Construct a client-side proxy object with the default SocketFactory</summary>
		/// <?/>
		/// <param name="protocol"/>
		/// <param name="clientVersion"/>
		/// <param name="addr"/>
		/// <param name="conf"/>
		/// <returns>a proxy instance</returns>
		/// <exception cref="System.IO.IOException"/>
		public static T getProxy<T>(long clientVersion, java.net.InetSocketAddress addr, 
			org.apache.hadoop.conf.Configuration conf)
		{
			System.Type protocol = typeof(T);
			return getProtocolProxy(protocol, clientVersion, addr, conf).getProxy();
		}

		/// <summary>Returns the server address for a given proxy.</summary>
		public static java.net.InetSocketAddress getServerAddress(object proxy)
		{
			return getConnectionIdForProxy(proxy).getAddress();
		}

		/// <summary>Return the connection ID of the given object.</summary>
		/// <remarks>
		/// Return the connection ID of the given object. If the provided object is in
		/// fact a protocol translator, we'll get the connection ID of the underlying
		/// proxy object.
		/// </remarks>
		/// <param name="proxy">the proxy object to get the connection ID of.</param>
		/// <returns>the connection ID for the provided proxy object.</returns>
		public static org.apache.hadoop.ipc.Client.ConnectionId getConnectionIdForProxy(object
			 proxy)
		{
			if (proxy is org.apache.hadoop.ipc.ProtocolTranslator)
			{
				proxy = ((org.apache.hadoop.ipc.ProtocolTranslator)proxy).getUnderlyingProxyObject
					();
			}
			org.apache.hadoop.ipc.RpcInvocationHandler inv = (org.apache.hadoop.ipc.RpcInvocationHandler
				)java.lang.reflect.Proxy.getInvocationHandler(proxy);
			return inv.getConnectionId();
		}

		/// <summary>
		/// Get a protocol proxy that contains a proxy connection to a remote server
		/// and a set of methods that are supported by the server
		/// </summary>
		/// <param name="protocol"/>
		/// <param name="clientVersion"/>
		/// <param name="addr"/>
		/// <param name="conf"/>
		/// <returns>a protocol proxy</returns>
		/// <exception cref="System.IO.IOException"/>
		public static org.apache.hadoop.ipc.ProtocolProxy<T> getProtocolProxy<T>(long clientVersion
			, java.net.InetSocketAddress addr, org.apache.hadoop.conf.Configuration conf)
		{
			System.Type protocol = typeof(T);
			return getProtocolProxy(protocol, clientVersion, addr, conf, org.apache.hadoop.net.NetUtils
				.getDefaultSocketFactory(conf));
		}

		/// <summary>Stop the proxy.</summary>
		/// <remarks>
		/// Stop the proxy. Proxy must either implement
		/// <see cref="java.io.Closeable"/>
		/// or must have
		/// associated
		/// <see cref="RpcInvocationHandler"/>
		/// .
		/// </remarks>
		/// <param name="proxy">the RPC proxy object to be stopped</param>
		/// <exception cref="org.apache.hadoop.HadoopIllegalArgumentException">
		/// if the proxy does not implement
		/// <see cref="java.io.Closeable"/>
		/// interface or
		/// does not have closeable
		/// <see cref="java.lang.reflect.InvocationHandler"/>
		/// </exception>
		public static void stopProxy(object proxy)
		{
			if (proxy == null)
			{
				throw new org.apache.hadoop.HadoopIllegalArgumentException("Cannot close proxy since it is null"
					);
			}
			try
			{
				if (proxy is java.io.Closeable)
				{
					((java.io.Closeable)proxy).close();
					return;
				}
				else
				{
					java.lang.reflect.InvocationHandler handler = java.lang.reflect.Proxy.getInvocationHandler
						(proxy);
					if (handler is java.io.Closeable)
					{
						((java.io.Closeable)handler).close();
						return;
					}
				}
			}
			catch (System.IO.IOException e)
			{
				LOG.error("Closing proxy or invocation handler caused exception", e);
			}
			catch (System.ArgumentException e)
			{
				LOG.error("RPC.stopProxy called on non proxy: class=" + Sharpen.Runtime.getClassForObject
					(proxy).getName(), e);
			}
			// If you see this error on a mock object in a unit test you're
			// developing, make sure to use MockitoUtil.mockProtocol() to
			// create your mock.
			throw new org.apache.hadoop.HadoopIllegalArgumentException("Cannot close proxy - is not Closeable or "
				 + "does not provide closeable invocation handler " + Sharpen.Runtime.getClassForObject
				(proxy));
		}

		/// <summary>Class to construct instances of RPC server with specific options.</summary>
		public class Builder
		{
			private java.lang.Class protocol = null;

			private object instance = null;

			private string bindAddress = "0.0.0.0";

			private int port = 0;

			private int numHandlers = 1;

			private int numReaders = -1;

			private int queueSizePerHandler = -1;

			private bool verbose = false;

			private readonly org.apache.hadoop.conf.Configuration conf;

			private org.apache.hadoop.security.token.SecretManager<org.apache.hadoop.security.token.TokenIdentifier
				> secretManager = null;

			private string portRangeConfig = null;

			public Builder(org.apache.hadoop.conf.Configuration conf)
			{
				this.conf = conf;
			}

			/// <summary>Mandatory field</summary>
			public virtual org.apache.hadoop.ipc.RPC.Builder setProtocol(java.lang.Class protocol
				)
			{
				this.protocol = protocol;
				return this;
			}

			/// <summary>Mandatory field</summary>
			public virtual org.apache.hadoop.ipc.RPC.Builder setInstance(object instance)
			{
				this.instance = instance;
				return this;
			}

			/// <summary>Default: 0.0.0.0</summary>
			public virtual org.apache.hadoop.ipc.RPC.Builder setBindAddress(string bindAddress
				)
			{
				this.bindAddress = bindAddress;
				return this;
			}

			/// <summary>Default: 0</summary>
			public virtual org.apache.hadoop.ipc.RPC.Builder setPort(int port)
			{
				this.port = port;
				return this;
			}

			/// <summary>Default: 1</summary>
			public virtual org.apache.hadoop.ipc.RPC.Builder setNumHandlers(int numHandlers)
			{
				this.numHandlers = numHandlers;
				return this;
			}

			/// <summary>Default: -1</summary>
			public virtual org.apache.hadoop.ipc.RPC.Builder setnumReaders(int numReaders)
			{
				this.numReaders = numReaders;
				return this;
			}

			/// <summary>Default: -1</summary>
			public virtual org.apache.hadoop.ipc.RPC.Builder setQueueSizePerHandler(int queueSizePerHandler
				)
			{
				this.queueSizePerHandler = queueSizePerHandler;
				return this;
			}

			/// <summary>Default: false</summary>
			public virtual org.apache.hadoop.ipc.RPC.Builder setVerbose(bool verbose)
			{
				this.verbose = verbose;
				return this;
			}

			/// <summary>Default: null</summary>
			public virtual org.apache.hadoop.ipc.RPC.Builder setSecretManager<_T0>(org.apache.hadoop.security.token.SecretManager
				<_T0> secretManager)
				where _T0 : org.apache.hadoop.security.token.TokenIdentifier
			{
				this.secretManager = secretManager;
				return this;
			}

			/// <summary>Default: null</summary>
			public virtual org.apache.hadoop.ipc.RPC.Builder setPortRangeConfig(string portRangeConfig
				)
			{
				this.portRangeConfig = portRangeConfig;
				return this;
			}

			/// <summary>Build the RPC Server.</summary>
			/// <exception cref="System.IO.IOException">on error</exception>
			/// <exception cref="org.apache.hadoop.HadoopIllegalArgumentException">when mandatory fields are not set
			/// 	</exception>
			public virtual org.apache.hadoop.ipc.RPC.Server build()
			{
				if (this.conf == null)
				{
					throw new org.apache.hadoop.HadoopIllegalArgumentException("conf is not set");
				}
				if (this.protocol == null)
				{
					throw new org.apache.hadoop.HadoopIllegalArgumentException("protocol is not set");
				}
				if (this.instance == null)
				{
					throw new org.apache.hadoop.HadoopIllegalArgumentException("instance is not set");
				}
				return getProtocolEngine(this.protocol, this.conf).getServer(this.protocol, this.
					instance, this.bindAddress, this.port, this.numHandlers, this.numReaders, this.queueSizePerHandler
					, this.verbose, this.conf, this.secretManager, this.portRangeConfig);
			}
		}

		/// <summary>An RPC Server.</summary>
		public abstract class Server : org.apache.hadoop.ipc.Server
		{
			internal bool verbose;

			internal static string classNameBase(string className)
			{
				string[] names = className.split("\\.", -1);
				if (names == null || names.Length == 0)
				{
					return className;
				}
				return names[names.Length - 1];
			}

			/// <summary>The key in Map</summary>
			internal class ProtoNameVer
			{
				internal readonly string protocol;

				internal readonly long version;

				internal ProtoNameVer(string protocol, long ver)
				{
					this.protocol = protocol;
					this.version = ver;
				}

				public override bool Equals(object o)
				{
					if (o == null)
					{
						return false;
					}
					if (this == o)
					{
						return true;
					}
					if (!(o is org.apache.hadoop.ipc.RPC.Server.ProtoNameVer))
					{
						return false;
					}
					org.apache.hadoop.ipc.RPC.Server.ProtoNameVer pv = (org.apache.hadoop.ipc.RPC.Server.ProtoNameVer
						)o;
					return ((pv.protocol.Equals(this.protocol)) && (pv.version == this.version));
				}

				public override int GetHashCode()
				{
					return protocol.GetHashCode() * 37 + (int)version;
				}
			}

			/// <summary>The value in map</summary>
			internal class ProtoClassProtoImpl
			{
				internal readonly java.lang.Class protocolClass;

				internal readonly object protocolImpl;

				internal ProtoClassProtoImpl(java.lang.Class protocolClass, object protocolImpl)
				{
					this.protocolClass = protocolClass;
					this.protocolImpl = protocolImpl;
				}
			}

			internal System.Collections.Generic.List<System.Collections.Generic.IDictionary<org.apache.hadoop.ipc.RPC.Server.ProtoNameVer
				, org.apache.hadoop.ipc.RPC.Server.ProtoClassProtoImpl>> protocolImplMapArray = 
				new System.Collections.Generic.List<System.Collections.Generic.IDictionary<org.apache.hadoop.ipc.RPC.Server.ProtoNameVer
				, org.apache.hadoop.ipc.RPC.Server.ProtoClassProtoImpl>>(org.apache.hadoop.ipc.RPC.RpcKind
				.MAX_INDEX);

			internal virtual System.Collections.Generic.IDictionary<org.apache.hadoop.ipc.RPC.Server.ProtoNameVer
				, org.apache.hadoop.ipc.RPC.Server.ProtoClassProtoImpl> getProtocolImplMap(org.apache.hadoop.ipc.RPC.RpcKind
				 rpcKind)
			{
				if (protocolImplMapArray.Count == 0)
				{
					// initialize for all rpc kinds
					for (int i = 0; i <= org.apache.hadoop.ipc.RPC.RpcKind.MAX_INDEX; ++i)
					{
						protocolImplMapArray.add(new System.Collections.Generic.Dictionary<org.apache.hadoop.ipc.RPC.Server.ProtoNameVer
							, org.apache.hadoop.ipc.RPC.Server.ProtoClassProtoImpl>(10));
					}
				}
				return protocolImplMapArray[(int)(rpcKind)];
			}

			// Register  protocol and its impl for rpc calls
			internal virtual void registerProtocolAndImpl(org.apache.hadoop.ipc.RPC.RpcKind rpcKind
				, java.lang.Class protocolClass, object protocolImpl)
			{
				string protocolName = org.apache.hadoop.ipc.RPC.getProtocolName(protocolClass);
				long version;
				try
				{
					version = org.apache.hadoop.ipc.RPC.getProtocolVersion(protocolClass);
				}
				catch (System.Exception)
				{
					LOG.warn("Protocol " + protocolClass + " NOT registered as cannot get protocol version "
						);
					return;
				}
				getProtocolImplMap(rpcKind)[new org.apache.hadoop.ipc.RPC.Server.ProtoNameVer(protocolName
					, version)] = new org.apache.hadoop.ipc.RPC.Server.ProtoClassProtoImpl(protocolClass
					, protocolImpl);
				LOG.debug("RpcKind = " + rpcKind + " Protocol Name = " + protocolName + " version="
					 + version + " ProtocolImpl=" + Sharpen.Runtime.getClassForObject(protocolImpl).
					getName() + " protocolClass=" + protocolClass.getName());
			}

			internal class VerProtocolImpl
			{
				internal readonly long version;

				internal readonly org.apache.hadoop.ipc.RPC.Server.ProtoClassProtoImpl protocolTarget;

				internal VerProtocolImpl(long ver, org.apache.hadoop.ipc.RPC.Server.ProtoClassProtoImpl
					 protocolTarget)
				{
					this.version = ver;
					this.protocolTarget = protocolTarget;
				}
			}

			internal virtual org.apache.hadoop.ipc.RPC.Server.VerProtocolImpl[] getSupportedProtocolVersions
				(org.apache.hadoop.ipc.RPC.RpcKind rpcKind, string protocolName)
			{
				org.apache.hadoop.ipc.RPC.Server.VerProtocolImpl[] resultk = new org.apache.hadoop.ipc.RPC.Server.VerProtocolImpl
					[getProtocolImplMap(rpcKind).Count];
				int i = 0;
				foreach (System.Collections.Generic.KeyValuePair<org.apache.hadoop.ipc.RPC.Server.ProtoNameVer
					, org.apache.hadoop.ipc.RPC.Server.ProtoClassProtoImpl> pv in getProtocolImplMap
					(rpcKind))
				{
					if (pv.Key.protocol.Equals(protocolName))
					{
						resultk[i++] = new org.apache.hadoop.ipc.RPC.Server.VerProtocolImpl(pv.Key.version
							, pv.Value);
					}
				}
				if (i == 0)
				{
					return null;
				}
				org.apache.hadoop.ipc.RPC.Server.VerProtocolImpl[] result = new org.apache.hadoop.ipc.RPC.Server.VerProtocolImpl
					[i];
				System.Array.Copy(resultk, 0, result, 0, i);
				return result;
			}

			internal virtual org.apache.hadoop.ipc.RPC.Server.VerProtocolImpl getHighestSupportedProtocol
				(org.apache.hadoop.ipc.RPC.RpcKind rpcKind, string protocolName)
			{
				long highestVersion = 0L;
				org.apache.hadoop.ipc.RPC.Server.ProtoClassProtoImpl highest = null;
				if (LOG.isDebugEnabled())
				{
					LOG.debug("Size of protoMap for " + rpcKind + " =" + getProtocolImplMap(rpcKind).
						Count);
				}
				foreach (System.Collections.Generic.KeyValuePair<org.apache.hadoop.ipc.RPC.Server.ProtoNameVer
					, org.apache.hadoop.ipc.RPC.Server.ProtoClassProtoImpl> pv in getProtocolImplMap
					(rpcKind))
				{
					if (pv.Key.protocol.Equals(protocolName))
					{
						if ((highest == null) || (pv.Key.version > highestVersion))
						{
							highest = pv.Value;
							highestVersion = pv.Key.version;
						}
					}
				}
				if (highest == null)
				{
					return null;
				}
				return new org.apache.hadoop.ipc.RPC.Server.VerProtocolImpl(highestVersion, highest
					);
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal Server(string bindAddress, int port, java.lang.Class paramClass
				, int handlerCount, int numReaders, int queueSizePerHandler, org.apache.hadoop.conf.Configuration
				 conf, string serverName, org.apache.hadoop.security.token.SecretManager<org.apache.hadoop.security.token.TokenIdentifier
				> secretManager, string portRangeConfig)
				: base(bindAddress, port, paramClass, handlerCount, numReaders, queueSizePerHandler
					, conf, serverName, secretManager, portRangeConfig)
			{
				initProtocolMetaInfo(conf);
			}

			private void initProtocolMetaInfo(org.apache.hadoop.conf.Configuration conf)
			{
				org.apache.hadoop.ipc.RPC.setProtocolEngine(conf, Sharpen.Runtime.getClassForType
					(typeof(org.apache.hadoop.ipc.ProtocolMetaInfoPB)), Sharpen.Runtime.getClassForType
					(typeof(org.apache.hadoop.ipc.ProtobufRpcEngine)));
				org.apache.hadoop.ipc.ProtocolMetaInfoServerSideTranslatorPB xlator = new org.apache.hadoop.ipc.ProtocolMetaInfoServerSideTranslatorPB
					(this);
				com.google.protobuf.BlockingService protocolInfoBlockingService = org.apache.hadoop.ipc.protobuf.ProtocolInfoProtos.ProtocolInfoService
					.newReflectiveBlockingService(xlator);
				addProtocol(org.apache.hadoop.ipc.RPC.RpcKind.RPC_PROTOCOL_BUFFER, Sharpen.Runtime.getClassForType
					(typeof(org.apache.hadoop.ipc.ProtocolMetaInfoPB)), protocolInfoBlockingService);
			}

			/// <summary>Add a protocol to the existing server.</summary>
			/// <param name="protocolClass">- the protocol class</param>
			/// <param name="protocolImpl">- the impl of the protocol that will be called</param>
			/// <returns>the server (for convenience)</returns>
			public virtual org.apache.hadoop.ipc.RPC.Server addProtocol(org.apache.hadoop.ipc.RPC.RpcKind
				 rpcKind, java.lang.Class protocolClass, object protocolImpl)
			{
				registerProtocolAndImpl(rpcKind, protocolClass, protocolImpl);
				return this;
			}

			/// <exception cref="System.Exception"/>
			public override org.apache.hadoop.io.Writable call(org.apache.hadoop.ipc.RPC.RpcKind
				 rpcKind, string protocol, org.apache.hadoop.io.Writable rpcRequest, long receiveTime
				)
			{
				return getRpcInvoker(rpcKind).call(this, protocol, rpcRequest, receiveTime);
			}
		}
	}
}
