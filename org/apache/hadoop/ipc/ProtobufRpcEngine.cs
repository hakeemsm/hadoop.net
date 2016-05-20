using Sharpen;

namespace org.apache.hadoop.ipc
{
	/// <summary>RPC Engine for for protobuf based RPCs.</summary>
	public class ProtobufRpcEngine : org.apache.hadoop.ipc.RpcEngine
	{
		public static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ipc.ProtobufRpcEngine
			)));

		static ProtobufRpcEngine()
		{
			// Register the rpcRequest deserializer for WritableRpcEngine 
			org.apache.hadoop.ipc.Server.registerProtocolEngine(org.apache.hadoop.ipc.RPC.RpcKind
				.RPC_PROTOCOL_BUFFER, Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ipc.ProtobufRpcEngine.RpcRequestWrapper
				)), new org.apache.hadoop.ipc.ProtobufRpcEngine.Server.ProtoBufRpcInvoker());
		}

		private static readonly org.apache.hadoop.ipc.ClientCache CLIENTS = new org.apache.hadoop.ipc.ClientCache
			();

		/// <exception cref="System.IO.IOException"/>
		public virtual org.apache.hadoop.ipc.ProtocolProxy<T> getProxy<T>(long clientVersion
			, java.net.InetSocketAddress addr, org.apache.hadoop.security.UserGroupInformation
			 ticket, org.apache.hadoop.conf.Configuration conf, javax.net.SocketFactory factory
			, int rpcTimeout)
		{
			System.Type protocol = typeof(T);
			return getProxy(protocol, clientVersion, addr, ticket, conf, factory, rpcTimeout, 
				null);
		}

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

		/// <exception cref="System.IO.IOException"/>
		public virtual org.apache.hadoop.ipc.ProtocolProxy<T> getProxy<T>(long clientVersion
			, java.net.InetSocketAddress addr, org.apache.hadoop.security.UserGroupInformation
			 ticket, org.apache.hadoop.conf.Configuration conf, javax.net.SocketFactory factory
			, int rpcTimeout, org.apache.hadoop.io.retry.RetryPolicy connectionRetryPolicy, 
			java.util.concurrent.atomic.AtomicBoolean fallbackToSimpleAuth)
		{
			System.Type protocol = typeof(T);
			org.apache.hadoop.ipc.ProtobufRpcEngine.Invoker invoker = new org.apache.hadoop.ipc.ProtobufRpcEngine.Invoker
				(protocol, addr, ticket, conf, factory, rpcTimeout, connectionRetryPolicy, fallbackToSimpleAuth
				);
			return new org.apache.hadoop.ipc.ProtocolProxy<T>(protocol, (T)java.lang.reflect.Proxy
				.newProxyInstance(protocol.getClassLoader(), new java.lang.Class[] { protocol }, 
				invoker), false);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual org.apache.hadoop.ipc.ProtocolProxy<org.apache.hadoop.ipc.ProtocolMetaInfoPB
			> getProtocolMetaInfoProxy(org.apache.hadoop.ipc.Client.ConnectionId connId, org.apache.hadoop.conf.Configuration
			 conf, javax.net.SocketFactory factory)
		{
			java.lang.Class protocol = Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ipc.ProtocolMetaInfoPB
				));
			return new org.apache.hadoop.ipc.ProtocolProxy<org.apache.hadoop.ipc.ProtocolMetaInfoPB
				>(protocol, (org.apache.hadoop.ipc.ProtocolMetaInfoPB)java.lang.reflect.Proxy.newProxyInstance
				(protocol.getClassLoader(), new java.lang.Class[] { protocol }, new org.apache.hadoop.ipc.ProtobufRpcEngine.Invoker
				(protocol, connId, conf, factory)), false);
		}

		private class Invoker : org.apache.hadoop.ipc.RpcInvocationHandler
		{
			private readonly System.Collections.Generic.IDictionary<string, com.google.protobuf.Message
				> returnTypes = new java.util.concurrent.ConcurrentHashMap<string, com.google.protobuf.Message
				>();

			private bool isClosed = false;

			private readonly org.apache.hadoop.ipc.Client.ConnectionId remoteId;

			private readonly org.apache.hadoop.ipc.Client client;

			private readonly long clientProtocolVersion;

			private readonly string protocolName;

			private java.util.concurrent.atomic.AtomicBoolean fallbackToSimpleAuth;

			/// <exception cref="System.IO.IOException"/>
			private Invoker(java.lang.Class protocol, java.net.InetSocketAddress addr, org.apache.hadoop.security.UserGroupInformation
				 ticket, org.apache.hadoop.conf.Configuration conf, javax.net.SocketFactory factory
				, int rpcTimeout, org.apache.hadoop.io.retry.RetryPolicy connectionRetryPolicy, 
				java.util.concurrent.atomic.AtomicBoolean fallbackToSimpleAuth)
				: this(protocol, org.apache.hadoop.ipc.Client.ConnectionId.getConnectionId(addr, 
					protocol, ticket, rpcTimeout, connectionRetryPolicy, conf), conf, factory)
			{
				this.fallbackToSimpleAuth = fallbackToSimpleAuth;
			}

			/// <summary>This constructor takes a connectionId, instead of creating a new one.</summary>
			private Invoker(java.lang.Class protocol, org.apache.hadoop.ipc.Client.ConnectionId
				 connId, org.apache.hadoop.conf.Configuration conf, javax.net.SocketFactory factory
				)
			{
				this.remoteId = connId;
				this.client = CLIENTS.getClient(conf, factory, Sharpen.Runtime.getClassForType(typeof(
					org.apache.hadoop.ipc.ProtobufRpcEngine.RpcResponseWrapper)));
				this.protocolName = org.apache.hadoop.ipc.RPC.getProtocolName(protocol);
				this.clientProtocolVersion = org.apache.hadoop.ipc.RPC.getProtocolVersion(protocol
					);
			}

			private org.apache.hadoop.ipc.protobuf.ProtobufRpcEngineProtos.RequestHeaderProto
				 constructRpcRequestHeader(java.lang.reflect.Method method)
			{
				org.apache.hadoop.ipc.protobuf.ProtobufRpcEngineProtos.RequestHeaderProto.Builder
					 builder = org.apache.hadoop.ipc.protobuf.ProtobufRpcEngineProtos.RequestHeaderProto
					.newBuilder();
				builder.setMethodName(method.getName());
				// For protobuf, {@code protocol} used when creating client side proxy is
				// the interface extending BlockingInterface, which has the annotations 
				// such as ProtocolName etc.
				//
				// Using Method.getDeclaringClass(), as in WritableEngine to get at
				// the protocol interface will return BlockingInterface, from where 
				// the annotation ProtocolName and Version cannot be
				// obtained.
				//
				// Hence we simply use the protocol class used to create the proxy.
				// For PB this may limit the use of mixins on client side.
				builder.setDeclaringClassProtocolName(protocolName);
				builder.setClientProtocolVersion(clientProtocolVersion);
				return ((org.apache.hadoop.ipc.protobuf.ProtobufRpcEngineProtos.RequestHeaderProto
					)builder.build());
			}

			/// <summary>This is the client side invoker of RPC method.</summary>
			/// <remarks>
			/// This is the client side invoker of RPC method. It only throws
			/// ServiceException, since the invocation proxy expects only
			/// ServiceException to be thrown by the method in case protobuf service.
			/// ServiceException has the following causes:
			/// <ol>
			/// <li>Exceptions encountered on the client side in this method are
			/// set as cause in ServiceException as is.</li>
			/// <li>Exceptions from the server are wrapped in RemoteException and are
			/// set as cause in ServiceException</li>
			/// </ol>
			/// Note that the client calling protobuf RPC methods, must handle
			/// ServiceException by getting the cause from the ServiceException. If the
			/// cause is RemoteException, then unwrap it to get the exception thrown by
			/// the server.
			/// </remarks>
			/// <exception cref="com.google.protobuf.ServiceException"/>
			public virtual object invoke(object proxy, java.lang.reflect.Method method, object
				[] args)
			{
				long startTime = 0;
				if (LOG.isDebugEnabled())
				{
					startTime = org.apache.hadoop.util.Time.now();
				}
				if (args.Length != 2)
				{
					// RpcController + Message
					throw new com.google.protobuf.ServiceException("Too many parameters for request. Method: ["
						 + method.getName() + "]" + ", Expected: 2, Actual: " + args.Length);
				}
				if (args[1] == null)
				{
					throw new com.google.protobuf.ServiceException("null param while calling Method: ["
						 + method.getName() + "]");
				}
				org.apache.htrace.TraceScope traceScope = null;
				// if Tracing is on then start a new span for this rpc.
				// guard it in the if statement to make sure there isn't
				// any extra string manipulation.
				if (org.apache.htrace.Trace.isTracing())
				{
					traceScope = org.apache.htrace.Trace.startSpan(org.apache.hadoop.ipc.RpcClientUtil
						.methodToTraceString(method));
				}
				org.apache.hadoop.ipc.protobuf.ProtobufRpcEngineProtos.RequestHeaderProto rpcRequestHeader
					 = constructRpcRequestHeader(method);
				if (LOG.isTraceEnabled())
				{
					LOG.trace(java.lang.Thread.currentThread().getId() + ": Call -> " + remoteId + ": "
						 + method.getName() + " {" + com.google.protobuf.TextFormat.shortDebugString((com.google.protobuf.Message
						)args[1]) + "}");
				}
				com.google.protobuf.Message theRequest = (com.google.protobuf.Message)args[1];
				org.apache.hadoop.ipc.ProtobufRpcEngine.RpcResponseWrapper val;
				try
				{
					val = (org.apache.hadoop.ipc.ProtobufRpcEngine.RpcResponseWrapper)client.call(org.apache.hadoop.ipc.RPC.RpcKind
						.RPC_PROTOCOL_BUFFER, new org.apache.hadoop.ipc.ProtobufRpcEngine.RpcRequestWrapper
						(rpcRequestHeader, theRequest), remoteId, fallbackToSimpleAuth);
				}
				catch (System.Exception e)
				{
					if (LOG.isTraceEnabled())
					{
						LOG.trace(java.lang.Thread.currentThread().getId() + ": Exception <- " + remoteId
							 + ": " + method.getName() + " {" + e + "}");
					}
					if (org.apache.htrace.Trace.isTracing())
					{
						traceScope.getSpan().addTimelineAnnotation("Call got exception: " + e.Message);
					}
					throw new com.google.protobuf.ServiceException(e);
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
					LOG.debug("Call: " + method.getName() + " took " + callTime + "ms");
				}
				com.google.protobuf.Message prototype = null;
				try
				{
					prototype = getReturnProtoType(method);
				}
				catch (System.Exception e)
				{
					throw new com.google.protobuf.ServiceException(e);
				}
				com.google.protobuf.Message returnMessage;
				try
				{
					returnMessage = prototype.newBuilderForType().mergeFrom(val.theResponseRead).build
						();
					if (LOG.isTraceEnabled())
					{
						LOG.trace(java.lang.Thread.currentThread().getId() + ": Response <- " + remoteId 
							+ ": " + method.getName() + " {" + com.google.protobuf.TextFormat.shortDebugString
							(returnMessage) + "}");
					}
				}
				catch (System.Exception e)
				{
					throw new com.google.protobuf.ServiceException(e);
				}
				return returnMessage;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void close()
			{
				if (!isClosed)
				{
					isClosed = true;
					CLIENTS.stopClient(client);
				}
			}

			/// <exception cref="System.Exception"/>
			private com.google.protobuf.Message getReturnProtoType(java.lang.reflect.Method method
				)
			{
				if (returnTypes.Contains(method.getName()))
				{
					return returnTypes[method.getName()];
				}
				java.lang.Class returnType = method.getReturnType();
				java.lang.reflect.Method newInstMethod = returnType.getMethod("getDefaultInstance"
					);
				newInstMethod.setAccessible(true);
				com.google.protobuf.Message prototype = (com.google.protobuf.Message)newInstMethod
					.invoke(null, (object[])null);
				returnTypes[method.getName()] = prototype;
				return prototype;
			}

			public virtual org.apache.hadoop.ipc.Client.ConnectionId getConnectionId()
			{
				//RpcInvocationHandler
				return remoteId;
			}
		}

		internal interface RpcWrapper : org.apache.hadoop.io.Writable
		{
			int getLength();
		}

		/// <summary>
		/// Wrapper for Protocol Buffer Requests
		/// Note while this wrapper is writable, the request on the wire is in
		/// Protobuf.
		/// </summary>
		/// <remarks>
		/// Wrapper for Protocol Buffer Requests
		/// Note while this wrapper is writable, the request on the wire is in
		/// Protobuf. Several methods on
		/// <see cref="Server">and RPC</see>
		/// 
		/// use type Writable as a wrapper to work across multiple RpcEngine kinds.
		/// </remarks>
		private abstract class RpcMessageWithHeader<T> : org.apache.hadoop.ipc.ProtobufRpcEngine.RpcWrapper
			where T : com.google.protobuf.GeneratedMessage
		{
			internal T requestHeader;

			internal com.google.protobuf.Message theRequest;

			internal byte[] theRequestRead;

			public RpcMessageWithHeader()
			{
			}

			public RpcMessageWithHeader(T requestHeader, com.google.protobuf.Message theRequest
				)
			{
				// for clientSide, the request is here
				// for server side, the request is here
				this.requestHeader = requestHeader;
				this.theRequest = theRequest;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void write(java.io.DataOutput @out)
			{
				java.io.OutputStream os = org.apache.hadoop.io.DataOutputOutputStream.constructOutputStream
					(@out);
				((com.google.protobuf.Message)requestHeader).writeDelimitedTo(os);
				theRequest.writeDelimitedTo(os);
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void readFields(java.io.DataInput @in)
			{
				requestHeader = parseHeaderFrom(readVarintBytes(@in));
				theRequestRead = readMessageRequest(@in);
			}

			/// <exception cref="System.IO.IOException"/>
			internal abstract T parseHeaderFrom(byte[] bytes);

			/// <exception cref="System.IO.IOException"/>
			internal virtual byte[] readMessageRequest(java.io.DataInput @in)
			{
				return readVarintBytes(@in);
			}

			/// <exception cref="System.IO.IOException"/>
			private static byte[] readVarintBytes(java.io.DataInput @in)
			{
				int length = org.apache.hadoop.util.ProtoUtil.readRawVarint32(@in);
				byte[] bytes = new byte[length];
				@in.readFully(bytes);
				return bytes;
			}

			public virtual T getMessageHeader()
			{
				return requestHeader;
			}

			public virtual byte[] getMessageBytes()
			{
				return theRequestRead;
			}

			public virtual int getLength()
			{
				int headerLen = requestHeader.getSerializedSize();
				int reqLen;
				if (theRequest != null)
				{
					reqLen = theRequest.getSerializedSize();
				}
				else
				{
					if (theRequestRead != null)
					{
						reqLen = theRequestRead.Length;
					}
					else
					{
						throw new System.ArgumentException("getLength on uninitialized RpcWrapper");
					}
				}
				return com.google.protobuf.CodedOutputStream.computeRawVarint32Size(headerLen) + 
					headerLen + com.google.protobuf.CodedOutputStream.computeRawVarint32Size(reqLen)
					 + reqLen;
			}
		}

		private class RpcRequestWrapper : org.apache.hadoop.ipc.ProtobufRpcEngine.RpcMessageWithHeader
			<org.apache.hadoop.ipc.protobuf.ProtobufRpcEngineProtos.RequestHeaderProto>
		{
			public RpcRequestWrapper()
			{
			}

			public RpcRequestWrapper(org.apache.hadoop.ipc.protobuf.ProtobufRpcEngineProtos.RequestHeaderProto
				 requestHeader, com.google.protobuf.Message theRequest)
				: base(requestHeader, theRequest)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			internal override org.apache.hadoop.ipc.protobuf.ProtobufRpcEngineProtos.RequestHeaderProto
				 parseHeaderFrom(byte[] bytes)
			{
				return org.apache.hadoop.ipc.protobuf.ProtobufRpcEngineProtos.RequestHeaderProto.
					parseFrom(bytes);
			}

			public override string ToString()
			{
				return requestHeader.getDeclaringClassProtocolName() + "." + requestHeader.getMethodName
					();
			}
		}

		public class RpcRequestMessageWrapper : org.apache.hadoop.ipc.ProtobufRpcEngine.RpcMessageWithHeader
			<org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcRequestHeaderProto>
		{
			public RpcRequestMessageWrapper()
			{
			}

			public RpcRequestMessageWrapper(org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcRequestHeaderProto
				 requestHeader, com.google.protobuf.Message theRequest)
				: base(requestHeader, theRequest)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			internal override org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcRequestHeaderProto
				 parseHeaderFrom(byte[] bytes)
			{
				return org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcRequestHeaderProto.parseFrom
					(bytes);
			}
		}

		public class RpcResponseMessageWrapper : org.apache.hadoop.ipc.ProtobufRpcEngine.RpcMessageWithHeader
			<org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto>
		{
			public RpcResponseMessageWrapper()
			{
			}

			public RpcResponseMessageWrapper(org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto
				 responseHeader, com.google.protobuf.Message theRequest)
				: base(responseHeader, theRequest)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			internal override byte[] readMessageRequest(java.io.DataInput @in)
			{
				switch (requestHeader.getStatus())
				{
					case org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto
						.ERROR:
					case org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto
						.FATAL:
					{
						// error message contain no message body
						return null;
					}

					default:
					{
						return base.readMessageRequest(@in);
					}
				}
			}

			/// <exception cref="System.IO.IOException"/>
			internal override org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto
				 parseHeaderFrom(byte[] bytes)
			{
				return org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.parseFrom
					(bytes);
			}
		}

		/// <summary>
		/// Wrapper for Protocol Buffer Responses
		/// Note while this wrapper is writable, the request on the wire is in
		/// Protobuf.
		/// </summary>
		/// <remarks>
		/// Wrapper for Protocol Buffer Responses
		/// Note while this wrapper is writable, the request on the wire is in
		/// Protobuf. Several methods on
		/// <see cref="Server">and RPC</see>
		/// 
		/// use type Writable as a wrapper to work across multiple RpcEngine kinds.
		/// </remarks>
		public class RpcResponseWrapper : org.apache.hadoop.ipc.ProtobufRpcEngine.RpcWrapper
		{
			internal com.google.protobuf.Message theResponse;

			internal byte[] theResponseRead;

			public RpcResponseWrapper()
			{
			}

			public RpcResponseWrapper(com.google.protobuf.Message message)
			{
				// temporarily exposed 
				// for senderSide, the response is here
				// for receiver side, the response is here
				this.theResponse = message;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void write(java.io.DataOutput @out)
			{
				java.io.OutputStream os = org.apache.hadoop.io.DataOutputOutputStream.constructOutputStream
					(@out);
				theResponse.writeDelimitedTo(os);
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void readFields(java.io.DataInput @in)
			{
				int length = org.apache.hadoop.util.ProtoUtil.readRawVarint32(@in);
				theResponseRead = new byte[length];
				@in.readFully(theResponseRead);
			}

			public virtual int getLength()
			{
				int resLen;
				if (theResponse != null)
				{
					resLen = theResponse.getSerializedSize();
				}
				else
				{
					if (theResponseRead != null)
					{
						resLen = theResponseRead.Length;
					}
					else
					{
						throw new System.ArgumentException("getLength on uninitialized RpcWrapper");
					}
				}
				return com.google.protobuf.CodedOutputStream.computeRawVarint32Size(resLen) + resLen;
			}
		}

		[com.google.common.annotations.VisibleForTesting]
		[org.apache.hadoop.classification.InterfaceAudience.Private]
		[org.apache.hadoop.classification.InterfaceStability.Unstable]
		internal static org.apache.hadoop.ipc.Client getClient(org.apache.hadoop.conf.Configuration
			 conf)
		{
			return CLIENTS.getClient(conf, javax.net.SocketFactory.getDefault(), Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.ipc.ProtobufRpcEngine.RpcResponseWrapper)));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual org.apache.hadoop.ipc.RPC.Server getServer<_T0>(java.lang.Class protocol
			, object protocolImpl, string bindAddress, int port, int numHandlers, int numReaders
			, int queueSizePerHandler, bool verbose, org.apache.hadoop.conf.Configuration conf
			, org.apache.hadoop.security.token.SecretManager<_T0> secretManager, string portRangeConfig
			)
			where _T0 : org.apache.hadoop.security.token.TokenIdentifier
		{
			return new org.apache.hadoop.ipc.ProtobufRpcEngine.Server(protocol, protocolImpl, 
				conf, bindAddress, port, numHandlers, numReaders, queueSizePerHandler, verbose, 
				secretManager, portRangeConfig);
		}

		public class Server : org.apache.hadoop.ipc.RPC.Server
		{
			/// <summary>Construct an RPC server.</summary>
			/// <param name="protocolClass">the class of protocol</param>
			/// <param name="protocolImpl">the protocolImpl whose methods will be called</param>
			/// <param name="conf">the configuration to use</param>
			/// <param name="bindAddress">the address to bind on to listen for connection</param>
			/// <param name="port">the port to listen for connections on</param>
			/// <param name="numHandlers">the number of method handler threads to run</param>
			/// <param name="verbose">whether each call should be logged</param>
			/// <param name="portRangeConfig">
			/// A config parameter that can be used to restrict
			/// the range of ports used when port is 0 (an ephemeral port)
			/// </param>
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
				registerProtocolAndImpl(org.apache.hadoop.ipc.RPC.RpcKind.RPC_PROTOCOL_BUFFER, protocolClass
					, protocolImpl);
			}

			/// <summary>
			/// Protobuf invoker for
			/// <see cref="RpcInvoker"/>
			/// </summary>
			internal class ProtoBufRpcInvoker : org.apache.hadoop.ipc.RPC.RpcInvoker
			{
				/// <exception cref="org.apache.hadoop.ipc.RpcServerException"/>
				private static org.apache.hadoop.ipc.RPC.Server.ProtoClassProtoImpl getProtocolImpl
					(org.apache.hadoop.ipc.RPC.Server server, string protoName, long clientVersion)
				{
					org.apache.hadoop.ipc.RPC.Server.ProtoNameVer pv = new org.apache.hadoop.ipc.RPC.Server.ProtoNameVer
						(protoName, clientVersion);
					org.apache.hadoop.ipc.RPC.Server.ProtoClassProtoImpl impl = server.getProtocolImplMap
						(org.apache.hadoop.ipc.RPC.RpcKind.RPC_PROTOCOL_BUFFER)[pv];
					if (impl == null)
					{
						// no match for Protocol AND Version
						org.apache.hadoop.ipc.RPC.Server.VerProtocolImpl highest = server.getHighestSupportedProtocol
							(org.apache.hadoop.ipc.RPC.RpcKind.RPC_PROTOCOL_BUFFER, protoName);
						if (highest == null)
						{
							throw new org.apache.hadoop.ipc.RpcNoSuchProtocolException("Unknown protocol: " +
								 protoName);
						}
						// protocol supported but not the version that client wants
						throw new org.apache.hadoop.ipc.RPC.VersionMismatch(protoName, clientVersion, highest
							.version);
					}
					return impl;
				}

				/// <exception cref="System.Exception"/>
				public virtual org.apache.hadoop.io.Writable call(org.apache.hadoop.ipc.RPC.Server
					 server, string protocol, org.apache.hadoop.io.Writable writableRequest, long receiveTime
					)
				{
					org.apache.hadoop.ipc.ProtobufRpcEngine.RpcRequestWrapper request = (org.apache.hadoop.ipc.ProtobufRpcEngine.RpcRequestWrapper
						)writableRequest;
					org.apache.hadoop.ipc.protobuf.ProtobufRpcEngineProtos.RequestHeaderProto rpcRequest
						 = request.requestHeader;
					string methodName = rpcRequest.getMethodName();
					string protoName = rpcRequest.getDeclaringClassProtocolName();
					long clientVersion = rpcRequest.getClientProtocolVersion();
					if (server.verbose)
					{
						LOG.info("Call: protocol=" + protocol + ", method=" + methodName);
					}
					org.apache.hadoop.ipc.RPC.Server.ProtoClassProtoImpl protocolImpl = getProtocolImpl
						(server, protoName, clientVersion);
					com.google.protobuf.BlockingService service = (com.google.protobuf.BlockingService
						)protocolImpl.protocolImpl;
					com.google.protobuf.Descriptors.MethodDescriptor methodDescriptor = service.getDescriptorForType
						().findMethodByName(methodName);
					if (methodDescriptor == null)
					{
						string msg = "Unknown method " + methodName + " called on " + protocol + " protocol.";
						LOG.warn(msg);
						throw new org.apache.hadoop.ipc.RpcNoSuchMethodException(msg);
					}
					com.google.protobuf.Message prototype = service.getRequestPrototype(methodDescriptor
						);
					com.google.protobuf.Message param = prototype.newBuilderForType().mergeFrom(request
						.theRequestRead).build();
					com.google.protobuf.Message result;
					long startTime = org.apache.hadoop.util.Time.now();
					int qTime = (int)(startTime - receiveTime);
					System.Exception exception = null;
					try
					{
						server.rpcDetailedMetrics.init(protocolImpl.protocolClass);
						result = service.callBlockingMethod(methodDescriptor, null, param);
					}
					catch (com.google.protobuf.ServiceException e)
					{
						exception = (System.Exception)e.InnerException;
						throw (System.Exception)e.InnerException;
					}
					catch (System.Exception e)
					{
						exception = e;
						throw;
					}
					finally
					{
						int processingTime = (int)(org.apache.hadoop.util.Time.now() - startTime);
						if (LOG.isDebugEnabled())
						{
							string msg = "Served: " + methodName + " queueTime= " + qTime + " procesingTime= "
								 + processingTime;
							if (exception != null)
							{
								msg += " exception= " + Sharpen.Runtime.getClassForObject(exception).getSimpleName
									();
							}
							LOG.debug(msg);
						}
						string detailedMetricsName = (exception == null) ? methodName : Sharpen.Runtime.getClassForObject
							(exception).getSimpleName();
						server.rpcMetrics.addRpcQueueTime(qTime);
						server.rpcMetrics.addRpcProcessingTime(processingTime);
						server.rpcDetailedMetrics.addProcessingTime(detailedMetricsName, processingTime);
					}
					return new org.apache.hadoop.ipc.ProtobufRpcEngine.RpcResponseWrapper(result);
				}
			}
		}
	}
}
