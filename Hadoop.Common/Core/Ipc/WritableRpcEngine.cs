using System;
using System.IO;
using System.Net;
using System.Reflection;
using System.Text;
using Hadoop.Common.Core.Conf;
using Hadoop.Common.Core.IO;
using Javax.Net;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.IO.Retry;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Util;
using Org.Apache.Htrace;
using Sharpen;
using Sharpen.Reflect;

namespace Org.Apache.Hadoop.Ipc
{
	/// <summary>An RpcEngine implementation for Writable data.</summary>
	public class WritableRpcEngine : RpcEngine
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(RPC));

		public const long writableRpcVersion = 2L;

		/// <summary>Whether or not this class has been initialized.</summary>
		private static bool isInitialized = false;

		static WritableRpcEngine()
		{
			//writableRpcVersion should be updated if there is a change
			//in format of the rpc messages.
			// 2L - added declared class to Invocation
			EnsureInitialized();
		}

		/// <summary>Initialize this class if it isn't already.</summary>
		public static void EnsureInitialized()
		{
			lock (typeof(WritableRpcEngine))
			{
				if (!isInitialized)
				{
					Initialize();
				}
			}
		}

		/// <summary>Register the rpcRequest deserializer for WritableRpcEngine</summary>
		private static void Initialize()
		{
			lock (typeof(WritableRpcEngine))
			{
				Server.RegisterProtocolEngine(RPC.RpcKind.RpcWritable, typeof(WritableRpcEngine.Invocation
					), new WritableRpcEngine.Server.WritableRpcInvoker());
				isInitialized = true;
			}
		}

		/// <summary>A method invocation, including the method name and its parameters.</summary>
		private class Invocation : IWritable, Configurable
		{
			private string methodName;

			private Type[] parameterClasses;

			private object[] parameters;

			private Configuration conf;

			private long clientVersion;

			private int clientMethodsHash;

			private string declaringClassProtocolName;

			private long rpcVersion;

			public Invocation()
			{
			}

			public Invocation(MethodInfo method, object[] parameters)
			{
				//This could be different from static writableRpcVersion when received
				//at server, if client is using a different version.
				// called when deserializing an invocation
				this.methodName = method.Name;
				this.parameterClasses = Sharpen.Runtime.GetParameterTypes(method);
				this.parameters = parameters;
				rpcVersion = writableRpcVersion;
				if (method.DeclaringType.Equals(typeof(VersionedProtocol)))
				{
					//VersionedProtocol is exempted from version check.
					clientVersion = 0;
					clientMethodsHash = 0;
				}
				else
				{
					this.clientVersion = RPC.GetProtocolVersion(method.DeclaringType);
					this.clientMethodsHash = ProtocolSignature.GetFingerprint(method.DeclaringType.GetMethods
						());
				}
				this.declaringClassProtocolName = RPC.GetProtocolName(method.DeclaringType);
			}

			/// <summary>The name of the method invoked.</summary>
			public virtual string GetMethodName()
			{
				return methodName;
			}

			/// <summary>The parameter classes.</summary>
			public virtual Type[] GetParameterClasses()
			{
				return parameterClasses;
			}

			/// <summary>The parameter instances.</summary>
			public virtual object[] GetParameters()
			{
				return parameters;
			}

			private long GetProtocolVersion()
			{
				return clientVersion;
			}

			private int GetClientMethodsHash()
			{
				return clientMethodsHash;
			}

			/// <summary>Returns the rpc version used by the client.</summary>
			/// <returns>rpcVersion</returns>
			public virtual long GetRpcVersion()
			{
				return rpcVersion;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void ReadFields(BinaryReader @in)
			{
				rpcVersion = @in.ReadLong();
				declaringClassProtocolName = UTF8.ReadString(@in);
				methodName = UTF8.ReadString(@in);
				clientVersion = @in.ReadLong();
				clientMethodsHash = @in.ReadInt();
				parameters = new object[@in.ReadInt()];
				parameterClasses = new Type[parameters.Length];
				ObjectWritable objectWritable = new ObjectWritable();
				for (int i = 0; i < parameters.Length; i++)
				{
					parameters[i] = ObjectWritable.ReadObject(@in, objectWritable, this.conf);
					parameterClasses[i] = objectWritable.GetDeclaredClass();
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Write(DataOutput @out)
			{
				@out.WriteLong(rpcVersion);
				UTF8.WriteString(@out, declaringClassProtocolName);
				UTF8.WriteString(@out, methodName);
				@out.WriteLong(clientVersion);
				@out.WriteInt(clientMethodsHash);
				@out.WriteInt(parameterClasses.Length);
				for (int i = 0; i < parameterClasses.Length; i++)
				{
					ObjectWritable.WriteObject(@out, parameters[i], parameterClasses[i], conf, true);
				}
			}

			public override string ToString()
			{
				StringBuilder buffer = new StringBuilder();
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

			public virtual void SetConf(Configuration conf)
			{
				this.conf = conf;
			}

			public virtual Configuration GetConf()
			{
				return this.conf;
			}
		}

		private static ClientCache Clients = new ClientCache();

		private class Invoker : RpcInvocationHandler
		{
			private Client.ConnectionId remoteId;

			private Client client;

			private bool isClosed = false;

			private readonly AtomicBoolean fallbackToSimpleAuth;

			/// <exception cref="System.IO.IOException"/>
			public Invoker(Type protocol, IPEndPoint address, UserGroupInformation ticket, Configuration
				 conf, SocketFactory factory, int rpcTimeout, AtomicBoolean fallbackToSimpleAuth
				)
			{
				this.remoteId = Client.ConnectionId.GetConnectionId(address, protocol, ticket, rpcTimeout
					, conf);
				this.client = Clients.GetClient(conf, factory);
				this.fallbackToSimpleAuth = fallbackToSimpleAuth;
			}

			/// <exception cref="System.Exception"/>
			public virtual object Invoke(object proxy, MethodInfo method, object[] args)
			{
				long startTime = 0;
				if (Log.IsDebugEnabled())
				{
					startTime = Time.Now();
				}
				TraceScope traceScope = null;
				if (Trace.IsTracing())
				{
					traceScope = Trace.StartSpan(RpcClientUtil.MethodToTraceString(method));
				}
				ObjectWritable value;
				try
				{
					value = (ObjectWritable)client.Call(RPC.RpcKind.RpcWritable, new WritableRpcEngine.Invocation
						(method, args), remoteId, fallbackToSimpleAuth);
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
					Log.Debug("Call: " + method.Name + " " + callTime);
				}
				return value.Get();
			}

			/* close the IPC client that's responsible for this invoker's RPCs */
			public virtual void Close()
			{
				lock (this)
				{
					if (!isClosed)
					{
						isClosed = true;
						Clients.StopClient(client);
					}
				}
			}

			public virtual Client.ConnectionId GetConnectionId()
			{
				return remoteId;
			}
		}

		// for unit testing only
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		internal static Client GetClient(Configuration conf)
		{
			return Clients.GetClient(conf);
		}

		/// <summary>
		/// Construct a client-side proxy object that implements the named protocol,
		/// talking to a server at the named address.
		/// </summary>
		/// <?/>
		/// <exception cref="System.IO.IOException"/>
		public virtual ProtocolProxy<T> GetProxy<T>(long clientVersion, IPEndPoint addr, 
			UserGroupInformation ticket, Configuration conf, SocketFactory factory, int rpcTimeout
			, RetryPolicy connectionRetryPolicy)
		{
			System.Type protocol = typeof(T);
			return GetProxy(protocol, clientVersion, addr, ticket, conf, factory, rpcTimeout, 
				connectionRetryPolicy, null);
		}

		/// <summary>
		/// Construct a client-side proxy object that implements the named protocol,
		/// talking to a server at the named address.
		/// </summary>
		/// <?/>
		/// <exception cref="System.IO.IOException"/>
		public virtual ProtocolProxy<T> GetProxy<T>(long clientVersion, IPEndPoint addr, 
			UserGroupInformation ticket, Configuration conf, SocketFactory factory, int rpcTimeout
			, RetryPolicy connectionRetryPolicy, AtomicBoolean fallbackToSimpleAuth)
		{
			System.Type protocol = typeof(T);
			if (connectionRetryPolicy != null)
			{
				throw new NotSupportedException("Not supported: connectionRetryPolicy=" + connectionRetryPolicy
					);
			}
			T proxy = (T)Proxy.NewProxyInstance(protocol.GetClassLoader(), new Type[] { protocol
				 }, new WritableRpcEngine.Invoker(protocol, addr, ticket, conf, factory, rpcTimeout
				, fallbackToSimpleAuth));
			return new ProtocolProxy<T>(protocol, proxy, true);
		}

		/* Construct a server for a protocol implementation instance listening on a
		* port and address. */
		/// <exception cref="System.IO.IOException"/>
		public virtual RPC.Server GetServer<_T0>(Type protocolClass, object protocolImpl, 
			string bindAddress, int port, int numHandlers, int numReaders, int queueSizePerHandler
			, bool verbose, Configuration conf, SecretManager<_T0> secretManager, string portRangeConfig
			)
			where _T0 : TokenIdentifier
		{
			return new WritableRpcEngine.Server(protocolClass, protocolImpl, conf, bindAddress
				, port, numHandlers, numReaders, queueSizePerHandler, verbose, secretManager, portRangeConfig
				);
		}

		/// <summary>An RPC Server.</summary>
		public class Server : RPC.Server
		{
			/// <summary>Construct an RPC server.</summary>
			/// <param name="instance">the instance whose methods will be called</param>
			/// <param name="conf">the configuration to use</param>
			/// <param name="bindAddress">the address to bind on to listen for connection</param>
			/// <param name="port">the port to listen for connections on</param>
			/// <exception cref="System.IO.IOException"/>
			[System.ObsoleteAttribute(@"Use #Server(Class, Object, Configuration, String, int)"
				)]
			public Server(object instance, Configuration conf, string bindAddress, int port)
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
			public Server(Type protocolClass, object protocolImpl, Configuration conf, string
				 bindAddress, int port)
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
			public Server(object protocolImpl, Configuration conf, string bindAddress, int port
				, int numHandlers, int numReaders, int queueSizePerHandler, bool verbose, SecretManager
				<TokenIdentifier> secretManager)
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
			public Server(Type protocolClass, object protocolImpl, Configuration conf, string
				 bindAddress, int port, int numHandlers, int numReaders, int queueSizePerHandler
				, bool verbose, SecretManager<TokenIdentifier> secretManager, string portRangeConfig
				)
				: base(bindAddress, port, null, numHandlers, numReaders, queueSizePerHandler, conf
					, ClassNameBase(protocolImpl.GetType().FullName), secretManager, portRangeConfig
					)
			{
				this.verbose = verbose;
				Type[] protocols;
				if (protocolClass == null)
				{
					// derive protocol from impl
					/*
					* In order to remain compatible with the old usage where a single
					* target protocolImpl is suppled for all protocol interfaces, and
					* the protocolImpl is derived from the protocolClass(es)
					* we register all interfaces extended by the protocolImpl
					*/
					protocols = RPC.GetProtocolInterfaces(protocolImpl.GetType());
				}
				else
				{
					if (!protocolClass.IsAssignableFrom(protocolImpl.GetType()))
					{
						throw new IOException("protocolClass " + protocolClass + " is not implemented by protocolImpl which is of class "
							 + protocolImpl.GetType());
					}
					// register protocol class and its super interfaces
					RegisterProtocolAndImpl(RPC.RpcKind.RpcWritable, protocolClass, protocolImpl);
					protocols = RPC.GetProtocolInterfaces(protocolClass);
				}
				foreach (Type p in protocols)
				{
					if (!p.Equals(typeof(VersionedProtocol)))
					{
						RegisterProtocolAndImpl(RPC.RpcKind.RpcWritable, p, protocolImpl);
					}
				}
			}

			private static void Log(string value)
			{
				if (value != null && value.Length > 55)
				{
					value = Sharpen.Runtime.Substring(value, 0, 55) + "...";
				}
				Log.Info(value);
			}

			internal class WritableRpcInvoker : RPC.RpcInvoker
			{
				/// <exception cref="System.IO.IOException"/>
				/// <exception cref="Org.Apache.Hadoop.Ipc.RPC.VersionMismatch"/>
				public virtual IWritable Call(RPC.Server server, string protocolName, IWritable rpcRequest
					, long receivedTime)
				{
					WritableRpcEngine.Invocation call = (WritableRpcEngine.Invocation)rpcRequest;
					if (server.verbose)
					{
						Log("Call: " + call);
					}
					// Verify writable rpc version
					if (call.GetRpcVersion() != writableRpcVersion)
					{
						// Client is using a different version of WritableRpc
						throw new RpcServerException("WritableRpc version mismatch, client side version="
							 + call.GetRpcVersion() + ", server side version=" + writableRpcVersion);
					}
					long clientVersion = call.GetProtocolVersion();
					string protoName;
					RPC.Server.ProtoClassProtoImpl protocolImpl;
					if (call.declaringClassProtocolName.Equals(typeof(VersionedProtocol).FullName))
					{
						// VersionProtocol methods are often used by client to figure out
						// which version of protocol to use.
						//
						// Versioned protocol methods should go the protocolName protocol
						// rather than the declaring class of the method since the
						// the declaring class is VersionedProtocol which is not 
						// registered directly.
						// Send the call to the highest  protocol version
						RPC.Server.VerProtocolImpl highest = server.GetHighestSupportedProtocol(RPC.RpcKind
							.RpcWritable, protocolName);
						if (highest == null)
						{
							throw new RpcServerException("Unknown protocol: " + protocolName);
						}
						protocolImpl = highest.protocolTarget;
					}
					else
					{
						protoName = call.declaringClassProtocolName;
						// Find the right impl for the protocol based on client version.
						RPC.Server.ProtoNameVer pv = new RPC.Server.ProtoNameVer(call.declaringClassProtocolName
							, clientVersion);
						protocolImpl = server.GetProtocolImplMap(RPC.RpcKind.RpcWritable)[pv];
						if (protocolImpl == null)
						{
							// no match for Protocol AND Version
							RPC.Server.VerProtocolImpl highest = server.GetHighestSupportedProtocol(RPC.RpcKind
								.RpcWritable, protoName);
							if (highest == null)
							{
								throw new RpcServerException("Unknown protocol: " + protoName);
							}
							else
							{
								// protocol supported but not the version that client wants
								throw new RPC.VersionMismatch(protoName, clientVersion, highest.version);
							}
						}
					}
					// Invoke the protocol method
					long startTime = Time.Now();
					int qTime = (int)(startTime - receivedTime);
					Exception exception = null;
					try
					{
						MethodInfo method = protocolImpl.protocolClass.GetMethod(call.GetMethodName(), call
							.GetParameterClasses());
						server.rpcDetailedMetrics.Init(protocolImpl.protocolClass);
						object value = method.Invoke(protocolImpl.protocolImpl, call.GetParameters());
						if (server.verbose)
						{
							Log("Return: " + value);
						}
						return new ObjectWritable(method.ReturnType, value);
					}
					catch (TargetInvocationException e)
					{
						Exception target = e.InnerException;
						if (target is IOException)
						{
							exception = (IOException)target;
							throw (IOException)target;
						}
						else
						{
							IOException ioe = new IOException(target.ToString());
							ioe.SetStackTrace(target.GetStackTrace());
							exception = ioe;
							throw ioe;
						}
					}
					catch (Exception e)
					{
						if (!(e is IOException))
						{
							Log.Error("Unexpected throwable object ", e);
						}
						IOException ioe = new IOException(e.ToString());
						ioe.SetStackTrace(e.GetStackTrace());
						exception = ioe;
						throw ioe;
					}
					finally
					{
						int processingTime = (int)(Time.Now() - startTime);
						if (Log.IsDebugEnabled())
						{
							string msg = "Served: " + call.GetMethodName() + " queueTime= " + qTime + " procesingTime= "
								 + processingTime;
							if (exception != null)
							{
								msg += " exception= " + exception.GetType().Name;
							}
							Log.Debug(msg);
						}
						string detailedMetricsName = (exception == null) ? call.GetMethodName() : exception
							.GetType().Name;
						server.rpcMetrics.AddRpcQueueTime(qTime);
						server.rpcMetrics.AddRpcProcessingTime(processingTime);
						server.rpcDetailedMetrics.AddProcessingTime(detailedMetricsName, processingTime);
					}
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual ProtocolProxy<ProtocolMetaInfoPB> GetProtocolMetaInfoProxy(Client.ConnectionId
			 connId, Configuration conf, SocketFactory factory)
		{
			throw new NotSupportedException("This proxy is not supported");
		}
	}
}
