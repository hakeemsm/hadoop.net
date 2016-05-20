using Sharpen;

namespace org.apache.hadoop.ipc
{
	/// <summary>An RpcEngine implementation for Writable data.</summary>
	public class WritableRpcEngine : org.apache.hadoop.ipc.RpcEngine
	{
		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ipc.RPC)));

		public const long writableRpcVersion = 2L;

		/// <summary>Whether or not this class has been initialized.</summary>
		private static bool isInitialized = false;

		static WritableRpcEngine()
		{
			//writableRpcVersion should be updated if there is a change
			//in format of the rpc messages.
			// 2L - added declared class to Invocation
			ensureInitialized();
		}

		/// <summary>Initialize this class if it isn't already.</summary>
		public static void ensureInitialized()
		{
			lock (typeof(WritableRpcEngine))
			{
				if (!isInitialized)
				{
					initialize();
				}
			}
		}

		/// <summary>Register the rpcRequest deserializer for WritableRpcEngine</summary>
		private static void initialize()
		{
			lock (typeof(WritableRpcEngine))
			{
				org.apache.hadoop.ipc.Server.registerProtocolEngine(org.apache.hadoop.ipc.RPC.RpcKind
					.RPC_WRITABLE, Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ipc.WritableRpcEngine.Invocation
					)), new org.apache.hadoop.ipc.WritableRpcEngine.Server.WritableRpcInvoker());
				isInitialized = true;
			}
		}

		/// <summary>A method invocation, including the method name and its parameters.</summary>
		private class Invocation : org.apache.hadoop.io.Writable, org.apache.hadoop.conf.Configurable
		{
			private string methodName;

			private java.lang.Class[] parameterClasses;

			private object[] parameters;

			private org.apache.hadoop.conf.Configuration conf;

			private long clientVersion;

			private int clientMethodsHash;

			private string declaringClassProtocolName;

			private long rpcVersion;

			public Invocation()
			{
			}

			public Invocation(java.lang.reflect.Method method, object[] parameters)
			{
				//This could be different from static writableRpcVersion when received
				//at server, if client is using a different version.
				// called when deserializing an invocation
				this.methodName = method.getName();
				this.parameterClasses = method.getParameterTypes();
				this.parameters = parameters;
				rpcVersion = writableRpcVersion;
				if (method.getDeclaringClass().Equals(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ipc.VersionedProtocol
					))))
				{
					//VersionedProtocol is exempted from version check.
					clientVersion = 0;
					clientMethodsHash = 0;
				}
				else
				{
					this.clientVersion = org.apache.hadoop.ipc.RPC.getProtocolVersion(method.getDeclaringClass
						());
					this.clientMethodsHash = org.apache.hadoop.ipc.ProtocolSignature.getFingerprint(method
						.getDeclaringClass().getMethods());
				}
				this.declaringClassProtocolName = org.apache.hadoop.ipc.RPC.getProtocolName(method
					.getDeclaringClass());
			}

			/// <summary>The name of the method invoked.</summary>
			public virtual string getMethodName()
			{
				return methodName;
			}

			/// <summary>The parameter classes.</summary>
			public virtual java.lang.Class[] getParameterClasses()
			{
				return parameterClasses;
			}

			/// <summary>The parameter instances.</summary>
			public virtual object[] getParameters()
			{
				return parameters;
			}

			private long getProtocolVersion()
			{
				return clientVersion;
			}

			private int getClientMethodsHash()
			{
				return clientMethodsHash;
			}

			/// <summary>Returns the rpc version used by the client.</summary>
			/// <returns>rpcVersion</returns>
			public virtual long getRpcVersion()
			{
				return rpcVersion;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void readFields(java.io.DataInput @in)
			{
				rpcVersion = @in.readLong();
				declaringClassProtocolName = org.apache.hadoop.io.UTF8.readString(@in);
				methodName = org.apache.hadoop.io.UTF8.readString(@in);
				clientVersion = @in.readLong();
				clientMethodsHash = @in.readInt();
				parameters = new object[@in.readInt()];
				parameterClasses = new java.lang.Class[parameters.Length];
				org.apache.hadoop.io.ObjectWritable objectWritable = new org.apache.hadoop.io.ObjectWritable
					();
				for (int i = 0; i < parameters.Length; i++)
				{
					parameters[i] = org.apache.hadoop.io.ObjectWritable.readObject(@in, objectWritable
						, this.conf);
					parameterClasses[i] = objectWritable.getDeclaredClass();
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void write(java.io.DataOutput @out)
			{
				@out.writeLong(rpcVersion);
				org.apache.hadoop.io.UTF8.writeString(@out, declaringClassProtocolName);
				org.apache.hadoop.io.UTF8.writeString(@out, methodName);
				@out.writeLong(clientVersion);
				@out.writeInt(clientMethodsHash);
				@out.writeInt(parameterClasses.Length);
				for (int i = 0; i < parameterClasses.Length; i++)
				{
					org.apache.hadoop.io.ObjectWritable.writeObject(@out, parameters[i], parameterClasses
						[i], conf, true);
				}
			}

			public override string ToString()
			{
				java.lang.StringBuilder buffer = new java.lang.StringBuilder();
				buffer.Append(methodName);
				buffer.Append("(");
				for (int i = 0; i < parameters.Length; i++)
				{
					if (i != 0)
					{
						buffer.Append(", ");
					}
					buffer.Append(parameters[i]);
				}
				buffer.Append(")");
				buffer.Append(", rpc version=" + rpcVersion);
				buffer.Append(", client version=" + clientVersion);
				buffer.Append(", methodsFingerPrint=" + clientMethodsHash);
				return buffer.ToString();
			}

			public virtual void setConf(org.apache.hadoop.conf.Configuration conf)
			{
				this.conf = conf;
			}

			public virtual org.apache.hadoop.conf.Configuration getConf()
			{
				return this.conf;
			}
		}

		private static org.apache.hadoop.ipc.ClientCache CLIENTS = new org.apache.hadoop.ipc.ClientCache
			();

		private class Invoker : org.apache.hadoop.ipc.RpcInvocationHandler
		{
			private org.apache.hadoop.ipc.Client.ConnectionId remoteId;

			private org.apache.hadoop.ipc.Client client;

			private bool isClosed = false;

			private readonly java.util.concurrent.atomic.AtomicBoolean fallbackToSimpleAuth;

			/// <exception cref="System.IO.IOException"/>
			public Invoker(java.lang.Class protocol, java.net.InetSocketAddress address, org.apache.hadoop.security.UserGroupInformation
				 ticket, org.apache.hadoop.conf.Configuration conf, javax.net.SocketFactory factory
				, int rpcTimeout, java.util.concurrent.atomic.AtomicBoolean fallbackToSimpleAuth
				)
			{
				this.remoteId = org.apache.hadoop.ipc.Client.ConnectionId.getConnectionId(address
					, protocol, ticket, rpcTimeout, conf);
				this.client = CLIENTS.getClient(conf, factory);
				this.fallbackToSimpleAuth = fallbackToSimpleAuth;
			}

			/// <exception cref="System.Exception"/>
			public virtual object invoke(object proxy, java.lang.reflect.Method method, object
				[] args)
			{
				long startTime = 0;
				if (LOG.isDebugEnabled())
				{
					startTime = org.apache.hadoop.util.Time.now();
				}
				org.apache.htrace.TraceScope traceScope = null;
				if (org.apache.htrace.Trace.isTracing())
				{
					traceScope = org.apache.htrace.Trace.startSpan(org.apache.hadoop.ipc.RpcClientUtil
						.methodToTraceString(method));
				}
				org.apache.hadoop.io.ObjectWritable value;
				try
				{
					value = (org.apache.hadoop.io.ObjectWritable)client.call(org.apache.hadoop.ipc.RPC.RpcKind
						.RPC_WRITABLE, new org.apache.hadoop.ipc.WritableRpcEngine.Invocation(method, args
						), remoteId, fallbackToSimpleAuth);
				}
				finally
				{
					if (traceScope != null)
					{
						traceScope.close();
					}
				}
				if (LOG.isDebugEnabled())
				{
					long callTime = org.apache.hadoop.util.Time.now() - startTime;
					LOG.debug("Call: " + method.getName() + " " + callTime);
				}
				return value.get();
			}

			/* close the IPC client that's responsible for this invoker's RPCs */
			public virtual void close()
			{
				lock (this)
				{
					if (!isClosed)
					{
						isClosed = true;
						CLIENTS.stopClient(client);
					}
				}
			}

			public virtual org.apache.hadoop.ipc.Client.ConnectionId getConnectionId()
			{
				return remoteId;
			}
		}

		// for unit testing only
		[org.apache.hadoop.classification.InterfaceAudience.Private]
		[org.apache.hadoop.classification.InterfaceStability.Unstable]
		internal static org.apache.hadoop.ipc.Client getClient(org.apache.hadoop.conf.Configuration
			 conf)
		{
			return CLIENTS.getClient(conf);
		}

		/// <summary>
		/// Construct a client-side proxy object that implements the named protocol,
		/// talking to a server at the named address.
		/// </summary>
		/// <?/>
		/// <exception cref="System.IO.IOException"/>
		public virtual org.apache.hadoop.ipc.ProtocolProxy<T> getProxy<T>(long clientVersion
			, java.net.InetSocketAddress addr, org.apache.hadoop.security.UserGroupInformation
			 ticket, org.apache.hadoop.conf.Configuration conf, javax.net.SocketFactory factory
			, int rpcTimeout, org.apache.hadoop.io.retry.RetryPolicy connectionRetryPolicy)
		{
			System.Type protocol = typeof(T);
			return getProxy(protocol, clientVersion, addr, ticket, conf, factory, rpcTimeout, 
				connectionRetryPolicy, null);
		}

		/// <summary>
		/// Construct a client-side proxy object that implements the named protocol,
		/// talking to a server at the named address.
		/// </summary>
		/// <?/>
		/// <exception cref="System.IO.IOException"/>
		public virtual org.apache.hadoop.ipc.ProtocolProxy<T> getProxy<T>(long clientVersion
			, java.net.InetSocketAddress addr, org.apache.hadoop.security.UserGroupInformation
			 ticket, org.apache.hadoop.conf.Configuration conf, javax.net.SocketFactory factory
			, int rpcTimeout, org.apache.hadoop.io.retry.RetryPolicy connectionRetryPolicy, 
			java.util.concurrent.atomic.AtomicBoolean fallbackToSimpleAuth)
		{
			System.Type protocol = typeof(T);
			if (connectionRetryPolicy != null)
			{
				throw new System.NotSupportedException("Not supported: connectionRetryPolicy=" + 
					connectionRetryPolicy);
			}
			T proxy = (T)java.lang.reflect.Proxy.newProxyInstance(protocol.getClassLoader(), 
				new java.lang.Class[] { protocol }, new org.apache.hadoop.ipc.WritableRpcEngine.Invoker
				(protocol, addr, ticket, conf, factory, rpcTimeout, fallbackToSimpleAuth));
			return new org.apache.hadoop.ipc.ProtocolProxy<T>(protocol, proxy, true);
		}

		/* Construct a server for a protocol implementation instance listening on a
		* port and address. */
		/// <exception cref="System.IO.IOException"/>
		public virtual org.apache.hadoop.ipc.RPC.Server getServer<_T0>(java.lang.Class protocolClass
			, object protocolImpl, string bindAddress, int port, int numHandlers, int numReaders
			, int queueSizePerHandler, bool verbose, org.apache.hadoop.conf.Configuration conf
			, org.apache.hadoop.security.token.SecretManager<_T0> secretManager, string portRangeConfig
			)
			where _T0 : org.apache.hadoop.security.token.TokenIdentifier
		{
			return new org.apache.hadoop.ipc.WritableRpcEngine.Server(protocolClass, protocolImpl
				, conf, bindAddress, port, numHandlers, numReaders, queueSizePerHandler, verbose
				, secretManager, portRangeConfig);
		}

		/// <summary>An RPC Server.</summary>
		public class Server : org.apache.hadoop.ipc.RPC.Server
		{
			/// <summary>Construct an RPC server.</summary>
			/// <param name="instance">the instance whose methods will be called</param>
			/// <param name="conf">the configuration to use</param>
			/// <param name="bindAddress">the address to bind on to listen for connection</param>
			/// <param name="port">the port to listen for connections on</param>
			/// <exception cref="System.IO.IOException"/>
			[System.ObsoleteAttribute(@"Use #Server(Class, Object, Configuration, String, int)"
				)]
			public Server(object instance, org.apache.hadoop.conf.Configuration conf, string 
				bindAddress, int port)
				: this(null, instance, conf, bindAddress, port)
			{
			}

			/// <summary>Construct an RPC server.</summary>
			/// <param name="protocolClass">class</param>
			/// <param name="protocolImpl">the instance whose methods will be called</param>
			/// <param name="conf">the configuration to use</param>
			/// <param name="bindAddress">the address to bind on to listen for connection</param>
			/// <param name="port">the port to listen for connections on</param>
			/// <exception cref="System.IO.IOException"/>
			public Server(java.lang.Class protocolClass, object protocolImpl, org.apache.hadoop.conf.Configuration
				 conf, string bindAddress, int port)
				: this(protocolClass, protocolImpl, conf, bindAddress, port, 1, -1, -1, false, null
					, null)
			{
			}

			/// <summary>Construct an RPC server.</summary>
			/// <param name="protocolImpl">the instance whose methods will be called</param>
			/// <param name="conf">the configuration to use</param>
			/// <param name="bindAddress">the address to bind on to listen for connection</param>
			/// <param name="port">the port to listen for connections on</param>
			/// <param name="numHandlers">the number of method handler threads to run</param>
			/// <param name="verbose">whether each call should be logged</param>
			/// <exception cref="System.IO.IOException"/>
			[System.ObsoleteAttribute(@"use Server#Server(Class, Object, Configuration, String, int, int, int, int, boolean, SecretManager)"
				)]
			public Server(object protocolImpl, org.apache.hadoop.conf.Configuration conf, string
				 bindAddress, int port, int numHandlers, int numReaders, int queueSizePerHandler
				, bool verbose, org.apache.hadoop.security.token.SecretManager<org.apache.hadoop.security.token.TokenIdentifier
				> secretManager)
				: this(null, protocolImpl, conf, bindAddress, port, numHandlers, numReaders, queueSizePerHandler
					, verbose, secretManager, null)
			{
			}

			/// <summary>Construct an RPC server.</summary>
			/// <param name="protocolClass">
			/// - the protocol being registered
			/// can be null for compatibility with old usage (see below for details)
			/// </param>
			/// <param name="protocolImpl">the protocol impl that will be called</param>
			/// <param name="conf">the configuration to use</param>
			/// <param name="bindAddress">the address to bind on to listen for connection</param>
			/// <param name="port">the port to listen for connections on</param>
			/// <param name="numHandlers">the number of method handler threads to run</param>
			/// <param name="verbose">whether each call should be logged</param>
			/// <exception cref="System.IO.IOException"/>
			public Server(java.lang.Class protocolClass, object protocolImpl, org.apache.hadoop.conf.Configuration
				 conf, string bindAddress, int port, int numHandlers, int numReaders, int queueSizePerHandler
				, bool verbose, org.apache.hadoop.security.token.SecretManager<org.apache.hadoop.security.token.TokenIdentifier
				> secretManager, string portRangeConfig)
				: base(bindAddress, port, null, numHandlers, numReaders, queueSizePerHandler, conf
					, classNameBase(Sharpen.Runtime.getClassForObject(protocolImpl).getName()), secretManager
					, portRangeConfig)
			{
				this.verbose = verbose;
				java.lang.Class[] protocols;
				if (protocolClass == null)
				{
					// derive protocol from impl
					/*
					* In order to remain compatible with the old usage where a single
					* target protocolImpl is suppled for all protocol interfaces, and
					* the protocolImpl is derived from the protocolClass(es)
					* we register all interfaces extended by the protocolImpl
					*/
					protocols = org.apache.hadoop.ipc.RPC.getProtocolInterfaces(Sharpen.Runtime.getClassForObject
						(protocolImpl));
				}
				else
				{
					if (!protocolClass.isAssignableFrom(Sharpen.Runtime.getClassForObject(protocolImpl
						)))
					{
						throw new System.IO.IOException("protocolClass " + protocolClass + " is not implemented by protocolImpl which is of class "
							 + Sharpen.Runtime.getClassForObject(protocolImpl));
					}
					// register protocol class and its super interfaces
					registerProtocolAndImpl(org.apache.hadoop.ipc.RPC.RpcKind.RPC_WRITABLE, protocolClass
						, protocolImpl);
					protocols = org.apache.hadoop.ipc.RPC.getProtocolInterfaces(protocolClass);
				}
				foreach (java.lang.Class p in protocols)
				{
					if (!p.Equals(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ipc.VersionedProtocol
						))))
					{
						registerProtocolAndImpl(org.apache.hadoop.ipc.RPC.RpcKind.RPC_WRITABLE, p, protocolImpl
							);
					}
				}
			}

			private static void log(string value)
			{
				if (value != null && value.Length > 55)
				{
					value = Sharpen.Runtime.substring(value, 0, 55) + "...";
				}
				LOG.info(value);
			}

			internal class WritableRpcInvoker : org.apache.hadoop.ipc.RPC.RpcInvoker
			{
				/// <exception cref="System.IO.IOException"/>
				/// <exception cref="org.apache.hadoop.ipc.RPC.VersionMismatch"/>
				public virtual org.apache.hadoop.io.Writable call(org.apache.hadoop.ipc.RPC.Server
					 server, string protocolName, org.apache.hadoop.io.Writable rpcRequest, long receivedTime
					)
				{
					org.apache.hadoop.ipc.WritableRpcEngine.Invocation call = (org.apache.hadoop.ipc.WritableRpcEngine.Invocation
						)rpcRequest;
					if (server.verbose)
					{
						log("Call: " + call);
					}
					// Verify writable rpc version
					if (call.getRpcVersion() != writableRpcVersion)
					{
						// Client is using a different version of WritableRpc
						throw new org.apache.hadoop.ipc.RpcServerException("WritableRpc version mismatch, client side version="
							 + call.getRpcVersion() + ", server side version=" + writableRpcVersion);
					}
					long clientVersion = call.getProtocolVersion();
					string protoName;
					org.apache.hadoop.ipc.RPC.Server.ProtoClassProtoImpl protocolImpl;
					if (call.declaringClassProtocolName.Equals(Sharpen.Runtime.getClassForType(typeof(
						org.apache.hadoop.ipc.VersionedProtocol)).getName()))
					{
						// VersionProtocol methods are often used by client to figure out
						// which version of protocol to use.
						//
						// Versioned protocol methods should go the protocolName protocol
						// rather than the declaring class of the method since the
						// the declaring class is VersionedProtocol which is not 
						// registered directly.
						// Send the call to the highest  protocol version
						org.apache.hadoop.ipc.RPC.Server.VerProtocolImpl highest = server.getHighestSupportedProtocol
							(org.apache.hadoop.ipc.RPC.RpcKind.RPC_WRITABLE, protocolName);
						if (highest == null)
						{
							throw new org.apache.hadoop.ipc.RpcServerException("Unknown protocol: " + protocolName
								);
						}
						protocolImpl = highest.protocolTarget;
					}
					else
					{
						protoName = call.declaringClassProtocolName;
						// Find the right impl for the protocol based on client version.
						org.apache.hadoop.ipc.RPC.Server.ProtoNameVer pv = new org.apache.hadoop.ipc.RPC.Server.ProtoNameVer
							(call.declaringClassProtocolName, clientVersion);
						protocolImpl = server.getProtocolImplMap(org.apache.hadoop.ipc.RPC.RpcKind.RPC_WRITABLE
							)[pv];
						if (protocolImpl == null)
						{
							// no match for Protocol AND Version
							org.apache.hadoop.ipc.RPC.Server.VerProtocolImpl highest = server.getHighestSupportedProtocol
								(org.apache.hadoop.ipc.RPC.RpcKind.RPC_WRITABLE, protoName);
							if (highest == null)
							{
								throw new org.apache.hadoop.ipc.RpcServerException("Unknown protocol: " + protoName
									);
							}
							else
							{
								// protocol supported but not the version that client wants
								throw new org.apache.hadoop.ipc.RPC.VersionMismatch(protoName, clientVersion, highest
									.version);
							}
						}
					}
					// Invoke the protocol method
					long startTime = org.apache.hadoop.util.Time.now();
					int qTime = (int)(startTime - receivedTime);
					System.Exception exception = null;
					try
					{
						java.lang.reflect.Method method = protocolImpl.protocolClass.getMethod(call.getMethodName
							(), call.getParameterClasses());
						method.setAccessible(true);
						server.rpcDetailedMetrics.init(protocolImpl.protocolClass);
						object value = method.invoke(protocolImpl.protocolImpl, call.getParameters());
						if (server.verbose)
						{
							log("Return: " + value);
						}
						return new org.apache.hadoop.io.ObjectWritable(method.getReturnType(), value);
					}
					catch (java.lang.reflect.InvocationTargetException e)
					{
						System.Exception target = e.getTargetException();
						if (target is System.IO.IOException)
						{
							exception = (System.IO.IOException)target;
							throw (System.IO.IOException)target;
						}
						else
						{
							System.IO.IOException ioe = new System.IO.IOException(target.ToString());
							ioe.setStackTrace(target.getStackTrace());
							exception = ioe;
							throw ioe;
						}
					}
					catch (System.Exception e)
					{
						if (!(e is System.IO.IOException))
						{
							LOG.error("Unexpected throwable object ", e);
						}
						System.IO.IOException ioe = new System.IO.IOException(e.ToString());
						ioe.setStackTrace(e.getStackTrace());
						exception = ioe;
						throw ioe;
					}
					finally
					{
						int processingTime = (int)(org.apache.hadoop.util.Time.now() - startTime);
						if (LOG.isDebugEnabled())
						{
							string msg = "Served: " + call.getMethodName() + " queueTime= " + qTime + " procesingTime= "
								 + processingTime;
							if (exception != null)
							{
								msg += " exception= " + Sharpen.Runtime.getClassForObject(exception).getSimpleName
									();
							}
							LOG.debug(msg);
						}
						string detailedMetricsName = (exception == null) ? call.getMethodName() : Sharpen.Runtime.getClassForObject
							(exception).getSimpleName();
						server.rpcMetrics.addRpcQueueTime(qTime);
						server.rpcMetrics.addRpcProcessingTime(processingTime);
						server.rpcDetailedMetrics.addProcessingTime(detailedMetricsName, processingTime);
					}
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual org.apache.hadoop.ipc.ProtocolProxy<org.apache.hadoop.ipc.ProtocolMetaInfoPB
			> getProtocolMetaInfoProxy(org.apache.hadoop.ipc.Client.ConnectionId connId, org.apache.hadoop.conf.Configuration
			 conf, javax.net.SocketFactory factory)
		{
			throw new System.NotSupportedException("This proxy is not supported");
		}
	}
}
