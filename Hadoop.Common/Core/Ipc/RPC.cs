using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Reflection;
using System.Threading;
using Com.Google.Protobuf;
using Hadoop.Common.Core.Conf;
using Hadoop.Common.Core.IO;
using Hadoop.Common.Core.Util;
using Javax.Net;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.IO.Retry;
using Org.Apache.Hadoop.Ipc.Protobuf;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Util;
using Sharpen;
using Sharpen.Reflect;

namespace Org.Apache.Hadoop.Ipc
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
	/// <see cref="IWritable"/>
	/// ; or</li>
	/// <li>an array of the above types</li> </ul>
	/// All methods in the protocol should throw only IOException.  No field data of
	/// the protocol instance is transmitted.
	/// </remarks>
	public class RPC
	{
		internal const int RpcServiceClassDefault = 0;

		[System.Serializable]
		public sealed class RpcKind
		{
			public static readonly RPC.RpcKind RpcBuiltin = new RPC.RpcKind((short)1);

			public static readonly RPC.RpcKind RpcWritable = new RPC.RpcKind((short)2);

			public static readonly RPC.RpcKind RpcProtocolBuffer = new RPC.RpcKind((short)3);

			internal static readonly short MaxIndex = RPC.RpcKind.RpcProtocolBuffer.value;

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
			IWritable Call(RPC.Server server, string protocol, IWritable rpcRequest, long receiveTime
				);
		}

		internal static readonly Log Log = LogFactory.GetLog(typeof(RPC));

		/// <summary>Get all superInterfaces that extend VersionedProtocol</summary>
		/// <param name="childInterfaces"/>
		/// <returns>the super interfaces that extend VersionedProtocol</returns>
		internal static Type[] GetSuperInterfaces(Type[] childInterfaces)
		{
			IList<Type> allInterfaces = new AList<Type>();
			foreach (Type childInterface in childInterfaces)
			{
				if (typeof(VersionedProtocol).IsAssignableFrom(childInterface))
				{
					allInterfaces.AddItem(childInterface);
					Sharpen.Collections.AddAll(allInterfaces, Arrays.AsList(GetSuperInterfaces(childInterface
						.GetInterfaces())));
				}
				else
				{
					Log.Warn("Interface " + childInterface + " ignored because it does not extend VersionedProtocol"
						);
				}
			}
			return Sharpen.Collections.ToArray(allInterfaces, new Type[allInterfaces.Count]);
		}

		/// <summary>
		/// Get all interfaces that the given protocol implements or extends
		/// which are assignable from VersionedProtocol.
		/// </summary>
		internal static Type[] GetProtocolInterfaces(Type protocol)
		{
			Type[] interfaces = protocol.GetInterfaces();
			return GetSuperInterfaces(interfaces);
		}

		/// <summary>Get the protocol name.</summary>
		/// <remarks>
		/// Get the protocol name.
		/// If the protocol class has a ProtocolAnnotation, then get the protocol
		/// name from the annotation; otherwise the class name is the protocol name.
		/// </remarks>
		public static string GetProtocolName(Type protocol)
		{
			if (protocol == null)
			{
				return null;
			}
			ProtocolInfo anno = protocol.GetAnnotation<ProtocolInfo>();
			return (anno == null) ? protocol.FullName : anno.ProtocolName();
		}

		/// <summary>Get the protocol version from protocol class.</summary>
		/// <remarks>
		/// Get the protocol version from protocol class.
		/// If the protocol class has a ProtocolAnnotation, then get the protocol
		/// name from the annotation; otherwise the class name is the protocol name.
		/// </remarks>
		public static long GetProtocolVersion(Type protocol)
		{
			if (protocol == null)
			{
				throw new ArgumentException("Null protocol");
			}
			long version;
			ProtocolInfo anno = protocol.GetAnnotation<ProtocolInfo>();
			if (anno != null)
			{
				version = anno.ProtocolVersion();
				if (version != -1)
				{
					return version;
				}
			}
			try
			{
				FieldInfo versionField = protocol.GetField("versionID");
				return versionField.GetLong(protocol);
			}
			catch (NoSuchFieldException ex)
			{
				throw new RuntimeException(ex);
			}
			catch (MemberAccessException ex)
			{
				throw new RuntimeException(ex);
			}
		}

		private RPC()
		{
		}

		private static readonly IDictionary<Type, RpcEngine> ProtocolEngines = new Dictionary
			<Type, RpcEngine>();

		private const string EngineProp = "rpc.engine";

		// no public ctor
		// cache of RpcEngines by protocol
		/// <summary>Set a protocol to use a non-default RpcEngine.</summary>
		/// <param name="conf">configuration to use</param>
		/// <param name="protocol">the protocol interface</param>
		/// <param name="engine">the RpcEngine impl</param>
		public static void SetProtocolEngine(Configuration conf, Type protocol, Type engine
			)
		{
			conf.SetClass(EngineProp + "." + protocol.FullName, engine, typeof(RpcEngine));
		}

		// return the RpcEngine configured to handle a protocol
		internal static RpcEngine GetProtocolEngine(Type protocol, Configuration conf)
		{
			lock (typeof(RPC))
			{
				RpcEngine engine = ProtocolEngines[protocol];
				if (engine == null)
				{
					Type impl = conf.GetClass(EngineProp + "." + protocol.FullName, typeof(WritableRpcEngine
						));
					engine = (RpcEngine)ReflectionUtils.NewInstance(impl, conf);
					ProtocolEngines[protocol] = engine;
				}
				return engine;
			}
		}

		/// <summary>A version mismatch for the RPC protocol.</summary>
		[System.Serializable]
		public class VersionMismatch : RpcServerException
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
			public virtual string GetInterfaceName()
			{
				return interfaceName;
			}

			/// <summary>Get the client's preferred version</summary>
			public virtual long GetClientVersion()
			{
				return clientVersion;
			}

			/// <summary>Get the server's agreed to version.</summary>
			public virtual long GetServerVersion()
			{
				return serverVersion;
			}

			/// <summary>get the rpc status corresponding to this exception</summary>
			public override RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto GetRpcStatusProto
				()
			{
				return RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto.Error;
			}

			/// <summary>get the detailed rpc status corresponding to this exception</summary>
			public override RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto GetRpcErrorCodeProto
				()
			{
				return RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto.ErrorRpcVersionMismatch;
			}
		}

		/// <summary>Get a proxy connection to a remote server</summary>
		/// <param name="protocol">protocol class</param>
		/// <param name="clientVersion">client version</param>
		/// <param name="addr">remote address</param>
		/// <param name="conf">configuration to use</param>
		/// <returns>the proxy</returns>
		/// <exception cref="System.IO.IOException">if the far end through a RemoteException</exception>
		public static T WaitForProxy<T>(long clientVersion, IPEndPoint addr, Configuration
			 conf)
		{
			System.Type protocol = typeof(T);
			return WaitForProtocolProxy(protocol, clientVersion, addr, conf).GetProxy();
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
		public static ProtocolProxy<T> WaitForProtocolProxy<T>(long clientVersion, IPEndPoint
			 addr, Configuration conf)
		{
			System.Type protocol = typeof(T);
			return WaitForProtocolProxy(protocol, clientVersion, addr, conf, long.MaxValue);
		}

		/// <summary>Get a proxy connection to a remote server</summary>
		/// <param name="protocol">protocol class</param>
		/// <param name="clientVersion">client version</param>
		/// <param name="addr">remote address</param>
		/// <param name="conf">configuration to use</param>
		/// <param name="connTimeout">time in milliseconds before giving up</param>
		/// <returns>the proxy</returns>
		/// <exception cref="System.IO.IOException">if the far end through a RemoteException</exception>
		public static T WaitForProxy<T>(long clientVersion, IPEndPoint addr, Configuration
			 conf, long connTimeout)
		{
			System.Type protocol = typeof(T);
			return WaitForProtocolProxy(protocol, clientVersion, addr, conf, connTimeout).GetProxy
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
		public static ProtocolProxy<T> WaitForProtocolProxy<T>(long clientVersion, IPEndPoint
			 addr, Configuration conf, long connTimeout)
		{
			System.Type protocol = typeof(T);
			return WaitForProtocolProxy(protocol, clientVersion, addr, conf, 0, null, connTimeout
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
		public static T WaitForProxy<T>(long clientVersion, IPEndPoint addr, Configuration
			 conf, int rpcTimeout, long timeout)
		{
			System.Type protocol = typeof(T);
			return WaitForProtocolProxy(protocol, clientVersion, addr, conf, rpcTimeout, null
				, timeout).GetProxy();
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
		public static ProtocolProxy<T> WaitForProtocolProxy<T>(long clientVersion, IPEndPoint
			 addr, Configuration conf, int rpcTimeout, RetryPolicy connectionRetryPolicy, long
			 timeout)
		{
			System.Type protocol = typeof(T);
			long startTime = Time.Now();
			IOException ioe;
			while (true)
			{
				try
				{
					return GetProtocolProxy(protocol, clientVersion, addr, UserGroupInformation.GetCurrentUser
						(), conf, NetUtils.GetDefaultSocketFactory(conf), rpcTimeout, connectionRetryPolicy
						);
				}
				catch (ConnectException se)
				{
					// namenode has not been started
					Log.Info("Server at " + addr + " not available yet, Zzzzz...");
					ioe = se;
				}
				catch (SocketTimeoutException te)
				{
					// namenode is busy
					Log.Info("Problem connecting to server: " + addr);
					ioe = te;
				}
				catch (NoRouteToHostException nrthe)
				{
					// perhaps a VIP is failing over
					Log.Info("No route to host for server: " + addr);
					ioe = nrthe;
				}
				// check if timed out
				if (Time.Now() - timeout >= startTime)
				{
					throw ioe;
				}
				if (Sharpen.Thread.CurrentThread().IsInterrupted())
				{
					// interrupted during some IO; this may not have been caught
					throw new ThreadInterruptedException("Interrupted waiting for the proxy");
				}
				// wait for retry
				try
				{
					Sharpen.Thread.Sleep(1000);
				}
				catch (Exception)
				{
					Sharpen.Thread.CurrentThread().Interrupt();
					throw (IOException)Sharpen.Extensions.InitCause(new ThreadInterruptedException("Interrupted waiting for the proxy"
						), ioe);
				}
			}
		}

		/// <summary>
		/// Construct a client-side proxy object that implements the named protocol,
		/// talking to a server at the named address.
		/// </summary>
		/// <?/>
		/// <exception cref="System.IO.IOException"/>
		public static T GetProxy<T>(long clientVersion, IPEndPoint addr, Configuration conf
			, SocketFactory factory)
		{
			System.Type protocol = typeof(T);
			return GetProtocolProxy(protocol, clientVersion, addr, conf, factory).GetProxy();
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
		public static ProtocolProxy<T> GetProtocolProxy<T>(long clientVersion, IPEndPoint
			 addr, Configuration conf, SocketFactory factory)
		{
			System.Type protocol = typeof(T);
			UserGroupInformation ugi = UserGroupInformation.GetCurrentUser();
			return GetProtocolProxy(protocol, clientVersion, addr, ugi, conf, factory);
		}

		/// <summary>
		/// Construct a client-side proxy object that implements the named protocol,
		/// talking to a server at the named address.
		/// </summary>
		/// <?/>
		/// <exception cref="System.IO.IOException"/>
		public static T GetProxy<T>(long clientVersion, IPEndPoint addr, UserGroupInformation
			 ticket, Configuration conf, SocketFactory factory)
		{
			System.Type protocol = typeof(T);
			return GetProtocolProxy(protocol, clientVersion, addr, ticket, conf, factory).GetProxy
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
		public static ProtocolProxy<T> GetProtocolProxy<T>(long clientVersion, IPEndPoint
			 addr, UserGroupInformation ticket, Configuration conf, SocketFactory factory)
		{
			System.Type protocol = typeof(T);
			return GetProtocolProxy(protocol, clientVersion, addr, ticket, conf, factory, 0, 
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
		public static T GetProxy<T>(long clientVersion, IPEndPoint addr, UserGroupInformation
			 ticket, Configuration conf, SocketFactory factory, int rpcTimeout)
		{
			System.Type protocol = typeof(T);
			return GetProtocolProxy(protocol, clientVersion, addr, ticket, conf, factory, rpcTimeout
				, null).GetProxy();
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
		public static ProtocolProxy<T> GetProtocolProxy<T>(long clientVersion, IPEndPoint
			 addr, UserGroupInformation ticket, Configuration conf, SocketFactory factory, int
			 rpcTimeout, RetryPolicy connectionRetryPolicy)
		{
			System.Type protocol = typeof(T);
			return GetProtocolProxy(protocol, clientVersion, addr, ticket, conf, factory, rpcTimeout
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
		public static ProtocolProxy<T> GetProtocolProxy<T>(long clientVersion, IPEndPoint
			 addr, UserGroupInformation ticket, Configuration conf, SocketFactory factory, int
			 rpcTimeout, RetryPolicy connectionRetryPolicy, AtomicBoolean fallbackToSimpleAuth
			)
		{
			System.Type protocol = typeof(T);
			if (UserGroupInformation.IsSecurityEnabled())
			{
				SaslRpcServer.Init(conf);
			}
			return GetProtocolEngine(protocol, conf).GetProxy(protocol, clientVersion, addr, 
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
		public static T GetProxy<T>(long clientVersion, IPEndPoint addr, Configuration conf
			)
		{
			System.Type protocol = typeof(T);
			return GetProtocolProxy(protocol, clientVersion, addr, conf).GetProxy();
		}

		/// <summary>Returns the server address for a given proxy.</summary>
		public static IPEndPoint GetServerAddress(object proxy)
		{
			return GetConnectionIdForProxy(proxy).GetAddress();
		}

		/// <summary>Return the connection ID of the given object.</summary>
		/// <remarks>
		/// Return the connection ID of the given object. If the provided object is in
		/// fact a protocol translator, we'll get the connection ID of the underlying
		/// proxy object.
		/// </remarks>
		/// <param name="proxy">the proxy object to get the connection ID of.</param>
		/// <returns>the connection ID for the provided proxy object.</returns>
		public static Client.ConnectionId GetConnectionIdForProxy(object proxy)
		{
			if (proxy is ProtocolTranslator)
			{
				proxy = ((ProtocolTranslator)proxy).GetUnderlyingProxyObject();
			}
			RpcInvocationHandler inv = (RpcInvocationHandler)Proxy.GetInvocationHandler(proxy
				);
			return inv.GetConnectionId();
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
		public static ProtocolProxy<T> GetProtocolProxy<T>(long clientVersion, IPEndPoint
			 addr, Configuration conf)
		{
			System.Type protocol = typeof(T);
			return GetProtocolProxy(protocol, clientVersion, addr, conf, NetUtils.GetDefaultSocketFactory
				(conf));
		}

		/// <summary>Stop the proxy.</summary>
		/// <remarks>
		/// Stop the proxy. Proxy must either implement
		/// <see cref="System.IDisposable"/>
		/// or must have
		/// associated
		/// <see cref="RpcInvocationHandler"/>
		/// .
		/// </remarks>
		/// <param name="proxy">the RPC proxy object to be stopped</param>
		/// <exception cref="Org.Apache.Hadoop.HadoopIllegalArgumentException">
		/// if the proxy does not implement
		/// <see cref="System.IDisposable"/>
		/// interface or
		/// does not have closeable
		/// <see cref="Sharpen.Reflect.InvocationHandler"/>
		/// </exception>
		public static void StopProxy(object proxy)
		{
			if (proxy == null)
			{
				throw new HadoopIllegalArgumentException("Cannot close proxy since it is null");
			}
			try
			{
				if (proxy is IDisposable)
				{
					((IDisposable)proxy).Close();
					return;
				}
				else
				{
					InvocationHandler handler = Proxy.GetInvocationHandler(proxy);
					if (handler is IDisposable)
					{
						((IDisposable)handler).Close();
						return;
					}
				}
			}
			catch (IOException e)
			{
				Log.Error("Closing proxy or invocation handler caused exception", e);
			}
			catch (ArgumentException e)
			{
				Log.Error("RPC.stopProxy called on non proxy: class=" + proxy.GetType().FullName, 
					e);
			}
			// If you see this error on a mock object in a unit test you're
			// developing, make sure to use MockitoUtil.mockProtocol() to
			// create your mock.
			throw new HadoopIllegalArgumentException("Cannot close proxy - is not Closeable or "
				 + "does not provide closeable invocation handler " + proxy.GetType());
		}

		/// <summary>Class to construct instances of RPC server with specific options.</summary>
		public class Builder
		{
			private Type protocol = null;

			private object instance = null;

			private string bindAddress = "0.0.0.0";

			private int port = 0;

			private int numHandlers = 1;

			private int numReaders = -1;

			private int queueSizePerHandler = -1;

			private bool verbose = false;

			private readonly Configuration conf;

			private SecretManager<TokenIdentifier> secretManager = null;

			private string portRangeConfig = null;

			public Builder(Configuration conf)
			{
				this.conf = conf;
			}

			/// <summary>Mandatory field</summary>
			public virtual RPC.Builder SetProtocol(Type protocol)
			{
				this.protocol = protocol;
				return this;
			}

			/// <summary>Mandatory field</summary>
			public virtual RPC.Builder SetInstance(object instance)
			{
				this.instance = instance;
				return this;
			}

			/// <summary>Default: 0.0.0.0</summary>
			public virtual RPC.Builder SetBindAddress(string bindAddress)
			{
				this.bindAddress = bindAddress;
				return this;
			}

			/// <summary>Default: 0</summary>
			public virtual RPC.Builder SetPort(int port)
			{
				this.port = port;
				return this;
			}

			/// <summary>Default: 1</summary>
			public virtual RPC.Builder SetNumHandlers(int numHandlers)
			{
				this.numHandlers = numHandlers;
				return this;
			}

			/// <summary>Default: -1</summary>
			public virtual RPC.Builder SetnumReaders(int numReaders)
			{
				this.numReaders = numReaders;
				return this;
			}

			/// <summary>Default: -1</summary>
			public virtual RPC.Builder SetQueueSizePerHandler(int queueSizePerHandler)
			{
				this.queueSizePerHandler = queueSizePerHandler;
				return this;
			}

			/// <summary>Default: false</summary>
			public virtual RPC.Builder SetVerbose(bool verbose)
			{
				this.verbose = verbose;
				return this;
			}

			/// <summary>Default: null</summary>
			public virtual RPC.Builder SetSecretManager<_T0>(SecretManager<_T0> secretManager
				)
				where _T0 : TokenIdentifier
			{
				this.secretManager = secretManager;
				return this;
			}

			/// <summary>Default: null</summary>
			public virtual RPC.Builder SetPortRangeConfig(string portRangeConfig)
			{
				this.portRangeConfig = portRangeConfig;
				return this;
			}

			/// <summary>Build the RPC Server.</summary>
			/// <exception cref="System.IO.IOException">on error</exception>
			/// <exception cref="Org.Apache.Hadoop.HadoopIllegalArgumentException">when mandatory fields are not set
			/// 	</exception>
			public virtual RPC.Server Build()
			{
				if (this.conf == null)
				{
					throw new HadoopIllegalArgumentException("conf is not set");
				}
				if (this.protocol == null)
				{
					throw new HadoopIllegalArgumentException("protocol is not set");
				}
				if (this.instance == null)
				{
					throw new HadoopIllegalArgumentException("instance is not set");
				}
				return GetProtocolEngine(this.protocol, this.conf).GetServer(this.protocol, this.
					instance, this.bindAddress, this.port, this.numHandlers, this.numReaders, this.queueSizePerHandler
					, this.verbose, this.conf, this.secretManager, this.portRangeConfig);
			}
		}

		/// <summary>An RPC Server.</summary>
		public abstract class Server : Org.Apache.Hadoop.Ipc.Server
		{
			internal bool verbose;

			internal static string ClassNameBase(string className)
			{
				string[] names = className.Split("\\.", -1);
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
					if (!(o is RPC.Server.ProtoNameVer))
					{
						return false;
					}
					RPC.Server.ProtoNameVer pv = (RPC.Server.ProtoNameVer)o;
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
				internal readonly Type protocolClass;

				internal readonly object protocolImpl;

				internal ProtoClassProtoImpl(Type protocolClass, object protocolImpl)
				{
					this.protocolClass = protocolClass;
					this.protocolImpl = protocolImpl;
				}
			}

			internal AList<IDictionary<RPC.Server.ProtoNameVer, RPC.Server.ProtoClassProtoImpl
				>> protocolImplMapArray = new AList<IDictionary<RPC.Server.ProtoNameVer, RPC.Server.ProtoClassProtoImpl
				>>(RPC.RpcKind.MaxIndex);

			internal virtual IDictionary<RPC.Server.ProtoNameVer, RPC.Server.ProtoClassProtoImpl
				> GetProtocolImplMap(RPC.RpcKind rpcKind)
			{
				if (protocolImplMapArray.Count == 0)
				{
					// initialize for all rpc kinds
					for (int i = 0; i <= RPC.RpcKind.MaxIndex; ++i)
					{
						protocolImplMapArray.AddItem(new Dictionary<RPC.Server.ProtoNameVer, RPC.Server.ProtoClassProtoImpl
							>(10));
					}
				}
				return protocolImplMapArray[(int)(rpcKind)];
			}

			// Register  protocol and its impl for rpc calls
			internal virtual void RegisterProtocolAndImpl(RPC.RpcKind rpcKind, Type protocolClass
				, object protocolImpl)
			{
				string protocolName = RPC.GetProtocolName(protocolClass);
				long version;
				try
				{
					version = RPC.GetProtocolVersion(protocolClass);
				}
				catch (Exception)
				{
					Log.Warn("Protocol " + protocolClass + " NOT registered as cannot get protocol version "
						);
					return;
				}
				GetProtocolImplMap(rpcKind)[new RPC.Server.ProtoNameVer(protocolName, version)] =
					 new RPC.Server.ProtoClassProtoImpl(protocolClass, protocolImpl);
				Log.Debug("RpcKind = " + rpcKind + " Protocol Name = " + protocolName + " version="
					 + version + " ProtocolImpl=" + protocolImpl.GetType().FullName + " protocolClass="
					 + protocolClass.FullName);
			}

			internal class VerProtocolImpl
			{
				internal readonly long version;

				internal readonly RPC.Server.ProtoClassProtoImpl protocolTarget;

				internal VerProtocolImpl(long ver, RPC.Server.ProtoClassProtoImpl protocolTarget)
				{
					this.version = ver;
					this.protocolTarget = protocolTarget;
				}
			}

			internal virtual RPC.Server.VerProtocolImpl[] GetSupportedProtocolVersions(RPC.RpcKind
				 rpcKind, string protocolName)
			{
				RPC.Server.VerProtocolImpl[] resultk = new RPC.Server.VerProtocolImpl[GetProtocolImplMap
					(rpcKind).Count];
				int i = 0;
				foreach (KeyValuePair<RPC.Server.ProtoNameVer, RPC.Server.ProtoClassProtoImpl> pv
					 in GetProtocolImplMap(rpcKind))
				{
					if (pv.Key.protocol.Equals(protocolName))
					{
						resultk[i++] = new RPC.Server.VerProtocolImpl(pv.Key.version, pv.Value);
					}
				}
				if (i == 0)
				{
					return null;
				}
				RPC.Server.VerProtocolImpl[] result = new RPC.Server.VerProtocolImpl[i];
				System.Array.Copy(resultk, 0, result, 0, i);
				return result;
			}

			internal virtual RPC.Server.VerProtocolImpl GetHighestSupportedProtocol(RPC.RpcKind
				 rpcKind, string protocolName)
			{
				long highestVersion = 0L;
				RPC.Server.ProtoClassProtoImpl highest = null;
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Size of protoMap for " + rpcKind + " =" + GetProtocolImplMap(rpcKind).
						Count);
				}
				foreach (KeyValuePair<RPC.Server.ProtoNameVer, RPC.Server.ProtoClassProtoImpl> pv
					 in GetProtocolImplMap(rpcKind))
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
				return new RPC.Server.VerProtocolImpl(highestVersion, highest);
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal Server(string bindAddress, int port, Type paramClass, int handlerCount
				, int numReaders, int queueSizePerHandler, Configuration conf, string serverName
				, SecretManager<TokenIdentifier> secretManager, string portRangeConfig)
				: base(bindAddress, port, paramClass, handlerCount, numReaders, queueSizePerHandler
					, conf, serverName, secretManager, portRangeConfig)
			{
				InitProtocolMetaInfo(conf);
			}

			private void InitProtocolMetaInfo(Configuration conf)
			{
				RPC.SetProtocolEngine(conf, typeof(ProtocolMetaInfoPB), typeof(ProtobufRpcEngine)
					);
				ProtocolMetaInfoServerSideTranslatorPB xlator = new ProtocolMetaInfoServerSideTranslatorPB
					(this);
				BlockingService protocolInfoBlockingService = ProtocolInfoProtos.ProtocolInfoService
					.NewReflectiveBlockingService(xlator);
				AddProtocol(RPC.RpcKind.RpcProtocolBuffer, typeof(ProtocolMetaInfoPB), protocolInfoBlockingService
					);
			}

			/// <summary>Add a protocol to the existing server.</summary>
			/// <param name="protocolClass">- the protocol class</param>
			/// <param name="protocolImpl">- the impl of the protocol that will be called</param>
			/// <returns>the server (for convenience)</returns>
			public virtual RPC.Server AddProtocol(RPC.RpcKind rpcKind, Type protocolClass, object
				 protocolImpl)
			{
				RegisterProtocolAndImpl(rpcKind, protocolClass, protocolImpl);
				return this;
			}

			/// <exception cref="System.Exception"/>
			public override IWritable Call(RPC.RpcKind rpcKind, string protocol, IWritable rpcRequest
				, long receiveTime)
			{
				return GetRpcInvoker(rpcKind).Call(this, protocol, rpcRequest, receiveTime);
			}
		}
	}
}
