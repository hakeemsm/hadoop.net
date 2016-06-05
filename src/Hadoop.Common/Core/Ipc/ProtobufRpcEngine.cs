using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Reflection;
using Com.Google.Common.Annotations;
using Com.Google.Protobuf;
using Hadoop.Common.Core.Conf;
using Hadoop.Common.Core.IO;
using Javax.Net;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.IO.Retry;
using Org.Apache.Hadoop.Ipc.Protobuf;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Util;
using Org.Apache.Htrace;

using Reflect;

namespace Org.Apache.Hadoop.Ipc
{
	/// <summary>RPC Engine for for protobuf based RPCs.</summary>
	public class ProtobufRpcEngine : RpcEngine
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(ProtobufRpcEngine));

		static ProtobufRpcEngine()
		{
			// Register the rpcRequest deserializer for WritableRpcEngine 
			Server.RegisterProtocolEngine(RPC.RpcKind.RpcProtocolBuffer, typeof(ProtobufRpcEngine.RpcRequestWrapper
				), new ProtobufRpcEngine.Server.ProtoBufRpcInvoker());
		}

		private static readonly ClientCache Clients = new ClientCache();

		/// <exception cref="System.IO.IOException"/>
		public virtual ProtocolProxy<T> GetProxy<T>(long clientVersion, IPEndPoint addr, 
			UserGroupInformation ticket, Configuration conf, SocketFactory factory, int rpcTimeout
			)
		{
			System.Type protocol = typeof(T);
			return GetProxy(protocol, clientVersion, addr, ticket, conf, factory, rpcTimeout, 
				null);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual ProtocolProxy<T> GetProxy<T>(long clientVersion, IPEndPoint addr, 
			UserGroupInformation ticket, Configuration conf, SocketFactory factory, int rpcTimeout
			, RetryPolicy connectionRetryPolicy)
		{
			System.Type protocol = typeof(T);
			return GetProxy(protocol, clientVersion, addr, ticket, conf, factory, rpcTimeout, 
				connectionRetryPolicy, null);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual ProtocolProxy<T> GetProxy<T>(long clientVersion, IPEndPoint addr, 
			UserGroupInformation ticket, Configuration conf, SocketFactory factory, int rpcTimeout
			, RetryPolicy connectionRetryPolicy, AtomicBoolean fallbackToSimpleAuth)
		{
			System.Type protocol = typeof(T);
			ProtobufRpcEngine.Invoker invoker = new ProtobufRpcEngine.Invoker(protocol, addr, 
				ticket, conf, factory, rpcTimeout, connectionRetryPolicy, fallbackToSimpleAuth);
			return new ProtocolProxy<T>(protocol, (T)Proxy.NewProxyInstance(protocol.GetClassLoader
				(), new Type[] { protocol }, invoker), false);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual ProtocolProxy<ProtocolMetaInfoPB> GetProtocolMetaInfoProxy(Client.ConnectionId
			 connId, Configuration conf, SocketFactory factory)
		{
			Type protocol = typeof(ProtocolMetaInfoPB);
			return new ProtocolProxy<ProtocolMetaInfoPB>(protocol, (ProtocolMetaInfoPB)Proxy.
				NewProxyInstance(protocol.GetClassLoader(), new Type[] { protocol }, new ProtobufRpcEngine.Invoker
				(protocol, connId, conf, factory)), false);
		}

		private class Invoker : RpcInvocationHandler
		{
			private readonly IDictionary<string, Message> returnTypes = new ConcurrentHashMap
				<string, Message>();

			private bool isClosed = false;

			private readonly Client.ConnectionId remoteId;

			private readonly Client client;

			private readonly long clientProtocolVersion;

			private readonly string protocolName;

			private AtomicBoolean fallbackToSimpleAuth;

			/// <exception cref="System.IO.IOException"/>
			private Invoker(Type protocol, IPEndPoint addr, UserGroupInformation ticket, Configuration
				 conf, SocketFactory factory, int rpcTimeout, RetryPolicy connectionRetryPolicy, 
				AtomicBoolean fallbackToSimpleAuth)
				: this(protocol, Client.ConnectionId.GetConnectionId(addr, protocol, ticket, rpcTimeout
					, connectionRetryPolicy, conf), conf, factory)
			{
				this.fallbackToSimpleAuth = fallbackToSimpleAuth;
			}

			/// <summary>This constructor takes a connectionId, instead of creating a new one.</summary>
			private Invoker(Type protocol, Client.ConnectionId connId, Configuration conf, SocketFactory
				 factory)
			{
				this.remoteId = connId;
				this.client = Clients.GetClient(conf, factory, typeof(ProtobufRpcEngine.RpcResponseWrapper
					));
				this.protocolName = RPC.GetProtocolName(protocol);
				this.clientProtocolVersion = RPC.GetProtocolVersion(protocol);
			}

			private ProtobufRpcEngineProtos.RequestHeaderProto ConstructRpcRequestHeader(MethodInfo
				 method)
			{
				ProtobufRpcEngineProtos.RequestHeaderProto.Builder builder = ProtobufRpcEngineProtos.RequestHeaderProto
					.NewBuilder();
				builder.SetMethodName(method.Name);
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
				builder.SetDeclaringClassProtocolName(protocolName);
				builder.SetClientProtocolVersion(clientProtocolVersion);
				return ((ProtobufRpcEngineProtos.RequestHeaderProto)builder.Build());
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
			/// <exception cref="Com.Google.Protobuf.ServiceException"/>
			public virtual object Invoke(object proxy, MethodInfo method, object[] args)
			{
				long startTime = 0;
				if (Log.IsDebugEnabled())
				{
					startTime = Time.Now();
				}
				if (args.Length != 2)
				{
					// RpcController + Message
					throw new ServiceException("Too many parameters for request. Method: [" + method.
						Name + "]" + ", Expected: 2, Actual: " + args.Length);
				}
				if (args[1] == null)
				{
					throw new ServiceException("null param while calling Method: [" + method.Name + "]"
						);
				}
				TraceScope traceScope = null;
				// if Tracing is on then start a new span for this rpc.
				// guard it in the if statement to make sure there isn't
				// any extra string manipulation.
				if (Trace.IsTracing())
				{
					traceScope = Trace.StartSpan(RpcClientUtil.MethodToTraceString(method));
				}
				ProtobufRpcEngineProtos.RequestHeaderProto rpcRequestHeader = ConstructRpcRequestHeader
					(method);
				if (Log.IsTraceEnabled())
				{
					Log.Trace(Thread.CurrentThread().GetId() + ": Call -> " + remoteId + ": "
						 + method.Name + " {" + TextFormat.ShortDebugString((Message)args[1]) + "}");
				}
				Message theRequest = (Message)args[1];
				ProtobufRpcEngine.RpcResponseWrapper val;
				try
				{
					val = (ProtobufRpcEngine.RpcResponseWrapper)client.Call(RPC.RpcKind.RpcProtocolBuffer
						, new ProtobufRpcEngine.RpcRequestWrapper(rpcRequestHeader, theRequest), remoteId
						, fallbackToSimpleAuth);
				}
				catch (Exception e)
				{
					if (Log.IsTraceEnabled())
					{
						Log.Trace(Thread.CurrentThread().GetId() + ": Exception <- " + remoteId +
							 ": " + method.Name + " {" + e + "}");
					}
					if (Trace.IsTracing())
					{
						traceScope.GetSpan().AddTimelineAnnotation("Call got exception: " + e.Message);
					}
					throw new ServiceException(e);
				}
				finally
				{
					if (traceScope != null)
					{
						traceScope.Close();
					}
				}
				if (Log.IsDebugEnabled())
				{
					long callTime = Time.Now() - startTime;
					Log.Debug("Call: " + method.Name + " took " + callTime + "ms");
				}
				Message prototype = null;
				try
				{
					prototype = GetReturnProtoType(method);
				}
				catch (Exception e)
				{
					throw new ServiceException(e);
				}
				Message returnMessage;
				try
				{
					returnMessage = prototype.NewBuilderForType().MergeFrom(val.theResponseRead).Build
						();
					if (Log.IsTraceEnabled())
					{
						Log.Trace(Thread.CurrentThread().GetId() + ": Response <- " + remoteId + 
							": " + method.Name + " {" + TextFormat.ShortDebugString(returnMessage) + "}");
					}
				}
				catch (Exception e)
				{
					throw new ServiceException(e);
				}
				return returnMessage;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Close()
			{
				if (!isClosed)
				{
					isClosed = true;
					Clients.StopClient(client);
				}
			}

			/// <exception cref="System.Exception"/>
			private Message GetReturnProtoType(MethodInfo method)
			{
				if (returnTypes.Contains(method.Name))
				{
					return returnTypes[method.Name];
				}
				Type returnType = method.ReturnType;
				MethodInfo newInstMethod = returnType.GetMethod("getDefaultInstance");
				Message prototype = (Message)newInstMethod.Invoke(null, (object[])null);
				returnTypes[method.Name] = prototype;
				return prototype;
			}

			public virtual Client.ConnectionId GetConnectionId()
			{
				//RpcInvocationHandler
				return remoteId;
			}
		}

		internal interface RpcWrapper : IWritable
		{
			int GetLength();
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
		private abstract class RpcMessageWithHeader<T> : ProtobufRpcEngine.RpcWrapper
			where T : GeneratedMessage
		{
			internal T requestHeader;

			internal Message theRequest;

			internal byte[] theRequestRead;

			public RpcMessageWithHeader()
			{
			}

			public RpcMessageWithHeader(T requestHeader, Message theRequest)
			{
				// for clientSide, the request is here
				// for server side, the request is here
				this.requestHeader = requestHeader;
				this.theRequest = theRequest;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Write(BinaryWriter @out)
			{
				OutputStream os = DataOutputOutputStream.ConstructOutputStream(@out);
				((Message)requestHeader).WriteDelimitedTo(os);
				theRequest.WriteDelimitedTo(os);
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void ReadFields(BinaryReader @in)
			{
				requestHeader = ParseHeaderFrom(ReadVarintBytes(@in));
				theRequestRead = ReadMessageRequest(@in);
			}

			/// <exception cref="System.IO.IOException"/>
			internal abstract T ParseHeaderFrom(byte[] bytes);

			/// <exception cref="System.IO.IOException"/>
			internal virtual byte[] ReadMessageRequest(BinaryReader @in)
			{
				return ReadVarintBytes(@in);
			}

			/// <exception cref="System.IO.IOException"/>
			private static byte[] ReadVarintBytes(BinaryReader @in)
			{
				int length = ProtoUtil.ReadRawVarint32(@in);
				byte[] bytes = new byte[length];
				@in.ReadFully(bytes);
				return bytes;
			}

			public virtual T GetMessageHeader()
			{
				return requestHeader;
			}

			public virtual byte[] GetMessageBytes()
			{
				return theRequestRead;
			}

			public virtual int GetLength()
			{
				int headerLen = requestHeader.GetSerializedSize();
				int reqLen;
				if (theRequest != null)
				{
					reqLen = theRequest.GetSerializedSize();
				}
				else
				{
					if (theRequestRead != null)
					{
						reqLen = theRequestRead.Length;
					}
					else
					{
						throw new ArgumentException("getLength on uninitialized RpcWrapper");
					}
				}
				return CodedOutputStream.ComputeRawVarint32Size(headerLen) + headerLen + CodedOutputStream
					.ComputeRawVarint32Size(reqLen) + reqLen;
			}
		}

		private class RpcRequestWrapper : ProtobufRpcEngine.RpcMessageWithHeader<ProtobufRpcEngineProtos.RequestHeaderProto
			>
		{
			public RpcRequestWrapper()
			{
			}

			public RpcRequestWrapper(ProtobufRpcEngineProtos.RequestHeaderProto requestHeader
				, Message theRequest)
				: base(requestHeader, theRequest)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			internal override ProtobufRpcEngineProtos.RequestHeaderProto ParseHeaderFrom(byte
				[] bytes)
			{
				return ProtobufRpcEngineProtos.RequestHeaderProto.ParseFrom(bytes);
			}

			public override string ToString()
			{
				return requestHeader.GetDeclaringClassProtocolName() + "." + requestHeader.GetMethodName
					();
			}
		}

		public class RpcRequestMessageWrapper : ProtobufRpcEngine.RpcMessageWithHeader<RpcHeaderProtos.RpcRequestHeaderProto
			>
		{
			public RpcRequestMessageWrapper()
			{
			}

			public RpcRequestMessageWrapper(RpcHeaderProtos.RpcRequestHeaderProto requestHeader
				, Message theRequest)
				: base(requestHeader, theRequest)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			internal override RpcHeaderProtos.RpcRequestHeaderProto ParseHeaderFrom(byte[] bytes
				)
			{
				return RpcHeaderProtos.RpcRequestHeaderProto.ParseFrom(bytes);
			}
		}

		public class RpcResponseMessageWrapper : ProtobufRpcEngine.RpcMessageWithHeader<RpcHeaderProtos.RpcResponseHeaderProto
			>
		{
			public RpcResponseMessageWrapper()
			{
			}

			public RpcResponseMessageWrapper(RpcHeaderProtos.RpcResponseHeaderProto responseHeader
				, Message theRequest)
				: base(responseHeader, theRequest)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			internal override byte[] ReadMessageRequest(BinaryReader @in)
			{
				switch (requestHeader.GetStatus())
				{
					case RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto.Error:
					case RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto.Fatal:
					{
						// error message contain no message body
						return null;
					}

					default:
					{
						return base.ReadMessageRequest(@in);
					}
				}
			}

			/// <exception cref="System.IO.IOException"/>
			internal override RpcHeaderProtos.RpcResponseHeaderProto ParseHeaderFrom(byte[] bytes
				)
			{
				return RpcHeaderProtos.RpcResponseHeaderProto.ParseFrom(bytes);
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
		public class RpcResponseWrapper : ProtobufRpcEngine.RpcWrapper
		{
			internal Message theResponse;

			internal byte[] theResponseRead;

			public RpcResponseWrapper()
			{
			}

			public RpcResponseWrapper(Message message)
			{
				// temporarily exposed 
				// for senderSide, the response is here
				// for receiver side, the response is here
				this.theResponse = message;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Write(BinaryWriter @out)
			{
				OutputStream os = DataOutputOutputStream.ConstructOutputStream(@out);
				theResponse.WriteDelimitedTo(os);
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void ReadFields(BinaryReader @in)
			{
				int length = ProtoUtil.ReadRawVarint32(@in);
				theResponseRead = new byte[length];
				@in.ReadFully(theResponseRead);
			}

			public virtual int GetLength()
			{
				int resLen;
				if (theResponse != null)
				{
					resLen = theResponse.GetSerializedSize();
				}
				else
				{
					if (theResponseRead != null)
					{
						resLen = theResponseRead.Length;
					}
					else
					{
						throw new ArgumentException("getLength on uninitialized RpcWrapper");
					}
				}
				return CodedOutputStream.ComputeRawVarint32Size(resLen) + resLen;
			}
		}

		[VisibleForTesting]
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		internal static Client GetClient(Configuration conf)
		{
			return Clients.GetClient(conf, SocketFactory.GetDefault(), typeof(ProtobufRpcEngine.RpcResponseWrapper
				));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual RPC.Server GetServer<_T0>(Type protocol, object protocolImpl, string
			 bindAddress, int port, int numHandlers, int numReaders, int queueSizePerHandler
			, bool verbose, Configuration conf, SecretManager<_T0> secretManager, string portRangeConfig
			)
			where _T0 : TokenIdentifier
		{
			return new ProtobufRpcEngine.Server(protocol, protocolImpl, conf, bindAddress, port
				, numHandlers, numReaders, queueSizePerHandler, verbose, secretManager, portRangeConfig
				);
		}

		public class Server : RPC.Server
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
			public Server(Type protocolClass, object protocolImpl, Configuration conf, string
				 bindAddress, int port, int numHandlers, int numReaders, int queueSizePerHandler
				, bool verbose, SecretManager<TokenIdentifier> secretManager, string portRangeConfig
				)
				: base(bindAddress, port, null, numHandlers, numReaders, queueSizePerHandler, conf
					, ClassNameBase(protocolImpl.GetType().FullName), secretManager, portRangeConfig
					)
			{
				this.verbose = verbose;
				RegisterProtocolAndImpl(RPC.RpcKind.RpcProtocolBuffer, protocolClass, protocolImpl
					);
			}

			/// <summary>
			/// Protobuf invoker for
			/// <see cref="RpcInvoker"/>
			/// </summary>
			internal class ProtoBufRpcInvoker : RPC.RpcInvoker
			{
				/// <exception cref="Org.Apache.Hadoop.Ipc.RpcServerException"/>
				private static RPC.Server.ProtoClassProtoImpl GetProtocolImpl(RPC.Server server, 
					string protoName, long clientVersion)
				{
					RPC.Server.ProtoNameVer pv = new RPC.Server.ProtoNameVer(protoName, clientVersion
						);
					RPC.Server.ProtoClassProtoImpl impl = server.GetProtocolImplMap(RPC.RpcKind.RpcProtocolBuffer
						)[pv];
					if (impl == null)
					{
						// no match for Protocol AND Version
						RPC.Server.VerProtocolImpl highest = server.GetHighestSupportedProtocol(RPC.RpcKind
							.RpcProtocolBuffer, protoName);
						if (highest == null)
						{
							throw new RpcNoSuchProtocolException("Unknown protocol: " + protoName);
						}
						// protocol supported but not the version that client wants
						throw new RPC.VersionMismatch(protoName, clientVersion, highest.version);
					}
					return impl;
				}

				/// <exception cref="System.Exception"/>
				public virtual IWritable Call(RPC.Server server, string protocol, IWritable writableRequest
					, long receiveTime)
				{
					ProtobufRpcEngine.RpcRequestWrapper request = (ProtobufRpcEngine.RpcRequestWrapper
						)writableRequest;
					ProtobufRpcEngineProtos.RequestHeaderProto rpcRequest = request.requestHeader;
					string methodName = rpcRequest.GetMethodName();
					string protoName = rpcRequest.GetDeclaringClassProtocolName();
					long clientVersion = rpcRequest.GetClientProtocolVersion();
					if (server.verbose)
					{
						Log.Info("Call: protocol=" + protocol + ", method=" + methodName);
					}
					RPC.Server.ProtoClassProtoImpl protocolImpl = GetProtocolImpl(server, protoName, 
						clientVersion);
					BlockingService service = (BlockingService)protocolImpl.protocolImpl;
					Descriptors.MethodDescriptor methodDescriptor = service.GetDescriptorForType().FindMethodByName
						(methodName);
					if (methodDescriptor == null)
					{
						string msg = "Unknown method " + methodName + " called on " + protocol + " protocol.";
						Log.Warn(msg);
						throw new RpcNoSuchMethodException(msg);
					}
					Message prototype = service.GetRequestPrototype(methodDescriptor);
					Message param = prototype.NewBuilderForType().MergeFrom(request.theRequestRead).Build
						();
					Message result;
					long startTime = Time.Now();
					int qTime = (int)(startTime - receiveTime);
					Exception exception = null;
					try
					{
						server.rpcDetailedMetrics.Init(protocolImpl.protocolClass);
						result = service.CallBlockingMethod(methodDescriptor, null, param);
					}
					catch (ServiceException e)
					{
						exception = (Exception)e.InnerException;
						throw (Exception)e.InnerException;
					}
					catch (Exception e)
					{
						exception = e;
						throw;
					}
					finally
					{
						int processingTime = (int)(Time.Now() - startTime);
						if (Log.IsDebugEnabled())
						{
							string msg = "Served: " + methodName + " queueTime= " + qTime + " procesingTime= "
								 + processingTime;
							if (exception != null)
							{
								msg += " exception= " + exception.GetType().Name;
							}
							Log.Debug(msg);
						}
						string detailedMetricsName = (exception == null) ? methodName : exception.GetType
							().Name;
						server.rpcMetrics.AddRpcQueueTime(qTime);
						server.rpcMetrics.AddRpcProcessingTime(processingTime);
						server.rpcDetailedMetrics.AddProcessingTime(detailedMetricsName, processingTime);
					}
					return new ProtobufRpcEngine.RpcResponseWrapper(result);
				}
			}
		}
	}
}
