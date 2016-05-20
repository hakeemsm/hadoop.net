using Sharpen;

namespace org.apache.hadoop.ipc
{
	/// <summary>An abstract IPC service.</summary>
	/// <remarks>
	/// An abstract IPC service.  IPC calls take a single
	/// <see cref="org.apache.hadoop.io.Writable"/>
	/// as a
	/// parameter, and return a
	/// <see cref="org.apache.hadoop.io.Writable"/>
	/// as their value.  A service runs on
	/// a port and is defined by a parameter class and a value class.
	/// </remarks>
	/// <seealso cref="Client"/>
	public abstract class Server
	{
		private readonly bool authorize;

		private System.Collections.Generic.IList<org.apache.hadoop.security.SaslRpcServer.AuthMethod
			> enabledAuthMethods;

		private org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto negotiateResponse;

		private org.apache.hadoop.ipc.Server.ExceptionsHandler exceptionsHandler = new org.apache.hadoop.ipc.Server.ExceptionsHandler
			();

		public virtual void addTerseExceptions(params java.lang.Class[] exceptionClass)
		{
			exceptionsHandler.addTerseExceptions(exceptionClass);
		}

		/// <summary>
		/// ExceptionsHandler manages Exception groups for special handling
		/// e.g., terse exception group for concise logging messages
		/// </summary>
		internal class ExceptionsHandler
		{
			private volatile System.Collections.Generic.ICollection<string> terseExceptions = 
				new java.util.HashSet<string>();

			/// <summary>Add exception class so server won't log its stack trace.</summary>
			/// <remarks>
			/// Add exception class so server won't log its stack trace.
			/// Modifying the terseException through this method is thread safe.
			/// </remarks>
			/// <param name="exceptionClass">exception classes</param>
			internal virtual void addTerseExceptions(params java.lang.Class[] exceptionClass)
			{
				// Make a copy of terseException for performing modification
				java.util.HashSet<string> newSet = new java.util.HashSet<string>(terseExceptions);
				// Add all class names into the HashSet
				foreach (java.lang.Class name in exceptionClass)
				{
					newSet.add(name.ToString());
				}
				// Replace terseException set
				terseExceptions = java.util.Collections.unmodifiableSet(newSet);
			}

			internal virtual bool isTerse(java.lang.Class t)
			{
				return terseExceptions.contains(t.ToString());
			}
		}

		/// <summary>
		/// If the user accidentally sends an HTTP GET to an IPC port, we detect this
		/// and send back a nicer response.
		/// </summary>
		private static readonly java.nio.ByteBuffer HTTP_GET_BYTES = java.nio.ByteBuffer.
			wrap(Sharpen.Runtime.getBytesForString("GET ", org.apache.commons.io.Charsets.UTF_8
			));

		/// <summary>
		/// An HTTP response to send back if we detect an HTTP request to our IPC
		/// port.
		/// </summary>
		internal const string RECEIVED_HTTP_REQ_RESPONSE = "HTTP/1.1 404 Not Found\r\n" +
			 "Content-type: text/plain\r\n\r\n" + "It looks like you are making an HTTP request to a Hadoop IPC port. "
			 + "This is not the correct port for the web interface on this daemon.\r\n";

		/// <summary>Initial and max size of response buffer</summary>
		internal static int INITIAL_RESP_BUF_SIZE = 10240;

		internal class RpcKindMapValue
		{
			internal readonly java.lang.Class rpcRequestWrapperClass;

			internal readonly org.apache.hadoop.ipc.RPC.RpcInvoker rpcInvoker;

			internal RpcKindMapValue(java.lang.Class rpcRequestWrapperClass, org.apache.hadoop.ipc.RPC.RpcInvoker
				 rpcInvoker)
			{
				this.rpcInvoker = rpcInvoker;
				this.rpcRequestWrapperClass = rpcRequestWrapperClass;
			}
		}

		internal static System.Collections.Generic.IDictionary<org.apache.hadoop.ipc.RPC.RpcKind
			, org.apache.hadoop.ipc.Server.RpcKindMapValue> rpcKindMap = new System.Collections.Generic.Dictionary
			<org.apache.hadoop.ipc.RPC.RpcKind, org.apache.hadoop.ipc.Server.RpcKindMapValue
			>(4);

		/// <summary>Register a RPC kind and the class to deserialize the rpc request.</summary>
		/// <remarks>
		/// Register a RPC kind and the class to deserialize the rpc request.
		/// Called by static initializers of rpcKind Engines
		/// </remarks>
		/// <param name="rpcKind"/>
		/// <param name="rpcRequestWrapperClass">
		/// - this class is used to deserialze the
		/// the rpc request.
		/// </param>
		/// <param name="rpcInvoker">- use to process the calls on SS.</param>
		public static void registerProtocolEngine(org.apache.hadoop.ipc.RPC.RpcKind rpcKind
			, java.lang.Class rpcRequestWrapperClass, org.apache.hadoop.ipc.RPC.RpcInvoker rpcInvoker
			)
		{
			org.apache.hadoop.ipc.Server.RpcKindMapValue old = rpcKindMap[rpcKind] = new org.apache.hadoop.ipc.Server.RpcKindMapValue
				(rpcRequestWrapperClass, rpcInvoker);
			if (old != null)
			{
				rpcKindMap[rpcKind] = old;
				throw new System.ArgumentException("ReRegistration of rpcKind: " + rpcKind);
			}
			LOG.debug("rpcKind=" + rpcKind + ", rpcRequestWrapperClass=" + rpcRequestWrapperClass
				 + ", rpcInvoker=" + rpcInvoker);
		}

		public virtual java.lang.Class getRpcRequestWrapper(org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcKindProto
			 rpcKind)
		{
			if (rpcRequestClass != null)
			{
				return rpcRequestClass;
			}
			org.apache.hadoop.ipc.Server.RpcKindMapValue val = rpcKindMap[org.apache.hadoop.util.ProtoUtil
				.convert(rpcKind)];
			return (val == null) ? null : val.rpcRequestWrapperClass;
		}

		public static org.apache.hadoop.ipc.RPC.RpcInvoker getRpcInvoker(org.apache.hadoop.ipc.RPC.RpcKind
			 rpcKind)
		{
			org.apache.hadoop.ipc.Server.RpcKindMapValue val = rpcKindMap[rpcKind];
			return (val == null) ? null : val.rpcInvoker;
		}

		public static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ipc.Server)));

		public static readonly org.apache.commons.logging.Log AUDITLOG = org.apache.commons.logging.LogFactory
			.getLog("SecurityLogger." + Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ipc.Server
			)).getName());

		private const string AUTH_FAILED_FOR = "Auth failed for ";

		private const string AUTH_SUCCESSFUL_FOR = "Auth successful for ";

		private static readonly java.lang.ThreadLocal<org.apache.hadoop.ipc.Server> SERVER
			 = new java.lang.ThreadLocal<org.apache.hadoop.ipc.Server>();

		private static readonly System.Collections.Generic.IDictionary<string, java.lang.Class
			> PROTOCOL_CACHE = new java.util.concurrent.ConcurrentHashMap<string, java.lang.Class
			>();

		/// <exception cref="java.lang.ClassNotFoundException"/>
		internal static java.lang.Class getProtocolClass(string protocolName, org.apache.hadoop.conf.Configuration
			 conf)
		{
			java.lang.Class protocol = PROTOCOL_CACHE[protocolName];
			if (protocol == null)
			{
				protocol = conf.getClassByName(protocolName);
				PROTOCOL_CACHE[protocolName] = protocol;
			}
			return protocol;
		}

		/// <summary>Returns the server instance called under or null.</summary>
		/// <remarks>
		/// Returns the server instance called under or null.  May be called under
		/// <see cref="call(org.apache.hadoop.io.Writable, long)"/>
		/// implementations, and under
		/// <see cref="org.apache.hadoop.io.Writable"/>
		/// methods of paramters and return values.  Permits applications to access
		/// the server context.
		/// </remarks>
		public static org.apache.hadoop.ipc.Server get()
		{
			return SERVER.get();
		}

		/// <summary>
		/// This is set to Call object before Handler invokes an RPC and reset
		/// after the call returns.
		/// </summary>
		private static readonly java.lang.ThreadLocal<org.apache.hadoop.ipc.Server.Call> 
			CurCall = new java.lang.ThreadLocal<org.apache.hadoop.ipc.Server.Call>();

		/// <summary>Get the current call</summary>
		[com.google.common.annotations.VisibleForTesting]
		public static java.lang.ThreadLocal<org.apache.hadoop.ipc.Server.Call> getCurCall
			()
		{
			return CurCall;
		}

		/// <summary>Returns the currently active RPC call's sequential ID number.</summary>
		/// <remarks>
		/// Returns the currently active RPC call's sequential ID number.  A negative
		/// call ID indicates an invalid value, such as if there is no currently active
		/// RPC call.
		/// </remarks>
		/// <returns>int sequential ID number of currently active RPC call</returns>
		public static int getCallId()
		{
			org.apache.hadoop.ipc.Server.Call call = CurCall.get();
			return call != null ? call.callId : org.apache.hadoop.ipc.RpcConstants.INVALID_CALL_ID;
		}

		/// <returns>
		/// The current active RPC call's retry count. -1 indicates the retry
		/// cache is not supported in the client side.
		/// </returns>
		public static int getCallRetryCount()
		{
			org.apache.hadoop.ipc.Server.Call call = CurCall.get();
			return call != null ? call.retryCount : org.apache.hadoop.ipc.RpcConstants.INVALID_RETRY_COUNT;
		}

		/// <summary>
		/// Returns the remote side ip address when invoked inside an RPC
		/// Returns null incase of an error.
		/// </summary>
		public static java.net.InetAddress getRemoteIp()
		{
			org.apache.hadoop.ipc.Server.Call call = CurCall.get();
			return (call != null && call.connection != null) ? call.connection.getHostInetAddress
				() : null;
		}

		/// <summary>Returns the clientId from the current RPC request</summary>
		public static byte[] getClientId()
		{
			org.apache.hadoop.ipc.Server.Call call = CurCall.get();
			return call != null ? call.clientId : org.apache.hadoop.ipc.RpcConstants.DUMMY_CLIENT_ID;
		}

		/// <summary>Returns remote address as a string when invoked inside an RPC.</summary>
		/// <remarks>
		/// Returns remote address as a string when invoked inside an RPC.
		/// Returns null in case of an error.
		/// </remarks>
		public static string getRemoteAddress()
		{
			java.net.InetAddress addr = getRemoteIp();
			return (addr == null) ? null : addr.getHostAddress();
		}

		/// <summary>Returns the RPC remote user when invoked inside an RPC.</summary>
		/// <remarks>
		/// Returns the RPC remote user when invoked inside an RPC.  Note this
		/// may be different than the current user if called within another doAs
		/// </remarks>
		/// <returns>connection's UGI or null if not an RPC</returns>
		public static org.apache.hadoop.security.UserGroupInformation getRemoteUser()
		{
			org.apache.hadoop.ipc.Server.Call call = CurCall.get();
			return (call != null && call.connection != null) ? call.connection.user : null;
		}

		/// <summary>Return true if the invocation was through an RPC.</summary>
		public static bool isRpcInvocation()
		{
			return CurCall.get() != null;
		}

		private string bindAddress;

		private int port;

		private int handlerCount;

		private int readThreads;

		private int readerPendingConnectionQueue;

		private java.lang.Class rpcRequestClass;

		protected internal readonly org.apache.hadoop.ipc.metrics.RpcMetrics rpcMetrics;

		protected internal readonly org.apache.hadoop.ipc.metrics.RpcDetailedMetrics rpcDetailedMetrics;

		private org.apache.hadoop.conf.Configuration conf;

		private string portRangeConfig = null;

		private org.apache.hadoop.security.token.SecretManager<org.apache.hadoop.security.token.TokenIdentifier
			> secretManager;

		private org.apache.hadoop.security.SaslPropertiesResolver saslPropsResolver;

		private org.apache.hadoop.security.authorize.ServiceAuthorizationManager serviceAuthorizationManager
			 = new org.apache.hadoop.security.authorize.ServiceAuthorizationManager();

		private int maxQueueSize;

		private readonly int maxRespSize;

		private int socketSendBufferSize;

		private readonly int maxDataLength;

		private readonly bool tcpNoDelay;

		private volatile bool running = true;

		private org.apache.hadoop.ipc.CallQueueManager<org.apache.hadoop.ipc.Server.Call>
			 callQueue;

		private org.apache.hadoop.ipc.Server.ConnectionManager connectionManager;

		private org.apache.hadoop.ipc.Server.Listener listener = null;

		private org.apache.hadoop.ipc.Server.Responder responder = null;

		private org.apache.hadoop.ipc.Server.Handler[] handlers = null;

		// port we listen on
		// number of handler threads
		// number of read threads
		// number of connections to queue per read thread
		// class used for deserializing the rpc request
		// if T then disable Nagle's Algorithm
		// true while server runs
		// maintains the set of client connections and handles idle timeouts
		/// <summary>
		/// A convenience method to bind to a given address and report
		/// better exceptions if the address is not a valid host.
		/// </summary>
		/// <param name="socket">the socket to bind</param>
		/// <param name="address">the address to bind to</param>
		/// <param name="backlog">the number of connections allowed in the queue</param>
		/// <exception cref="java.net.BindException">if the address can't be bound</exception>
		/// <exception cref="java.net.UnknownHostException">if the address isn't a valid host name
		/// 	</exception>
		/// <exception cref="System.IO.IOException">other random errors from bind</exception>
		public static void bind(java.net.ServerSocket socket, java.net.InetSocketAddress 
			address, int backlog)
		{
			bind(socket, address, backlog, null, null);
		}

		/// <exception cref="System.IO.IOException"/>
		public static void bind(java.net.ServerSocket socket, java.net.InetSocketAddress 
			address, int backlog, org.apache.hadoop.conf.Configuration conf, string rangeConf
			)
		{
			try
			{
				org.apache.hadoop.conf.Configuration.IntegerRanges range = null;
				if (rangeConf != null)
				{
					range = conf.getRange(rangeConf, string.Empty);
				}
				if (range == null || range.isEmpty() || (address.getPort() != 0))
				{
					socket.bind(address, backlog);
				}
				else
				{
					foreach (int port in range)
					{
						if (socket.isBound())
						{
							break;
						}
						try
						{
							java.net.InetSocketAddress temp = new java.net.InetSocketAddress(address.getAddress
								(), port);
							socket.bind(temp, backlog);
						}
						catch (java.net.BindException)
						{
						}
					}
					//Ignored
					if (!socket.isBound())
					{
						throw new java.net.BindException("Could not find a free port in " + range);
					}
				}
			}
			catch (System.Net.Sockets.SocketException e)
			{
				throw org.apache.hadoop.net.NetUtils.wrapException(null, 0, address.getHostName()
					, address.getPort(), e);
			}
		}

		/// <summary>Returns a handle to the rpcMetrics (required in tests)</summary>
		/// <returns>rpc metrics</returns>
		[com.google.common.annotations.VisibleForTesting]
		public virtual org.apache.hadoop.ipc.metrics.RpcMetrics getRpcMetrics()
		{
			return rpcMetrics;
		}

		[com.google.common.annotations.VisibleForTesting]
		public virtual org.apache.hadoop.ipc.metrics.RpcDetailedMetrics getRpcDetailedMetrics
			()
		{
			return rpcDetailedMetrics;
		}

		[com.google.common.annotations.VisibleForTesting]
		internal virtual System.Collections.Generic.IEnumerable<java.lang.Thread> getHandlers
			()
		{
			return java.util.Arrays.asList(handlers);
		}

		[com.google.common.annotations.VisibleForTesting]
		internal virtual org.apache.hadoop.ipc.Server.Connection[] getConnections()
		{
			return connectionManager.toArray();
		}

		/// <summary>Refresh the service authorization ACL for the service handled by this server.
		/// 	</summary>
		public virtual void refreshServiceAcl(org.apache.hadoop.conf.Configuration conf, 
			org.apache.hadoop.security.authorize.PolicyProvider provider)
		{
			serviceAuthorizationManager.refresh(conf, provider);
		}

		/// <summary>
		/// Refresh the service authorization ACL for the service handled by this server
		/// using the specified Configuration.
		/// </summary>
		[org.apache.hadoop.classification.InterfaceAudience.Private]
		public virtual void refreshServiceAclWithLoadedConfiguration(org.apache.hadoop.conf.Configuration
			 conf, org.apache.hadoop.security.authorize.PolicyProvider provider)
		{
			serviceAuthorizationManager.refreshWithLoadedConfiguration(conf, provider);
		}

		/// <summary>Returns a handle to the serviceAuthorizationManager (required in tests)</summary>
		/// <returns>instance of ServiceAuthorizationManager for this server</returns>
		public virtual org.apache.hadoop.security.authorize.ServiceAuthorizationManager getServiceAuthorizationManager
			()
		{
			return serviceAuthorizationManager;
		}

		internal static java.lang.Class getQueueClass(string prefix, org.apache.hadoop.conf.Configuration
			 conf)
		{
			string name = prefix + "." + org.apache.hadoop.fs.CommonConfigurationKeys.IPC_CALLQUEUE_IMPL_KEY;
			java.lang.Class queueClass = conf.getClass(name, Sharpen.Runtime.getClassForType(
				typeof(java.util.concurrent.LinkedBlockingQueue)));
			return org.apache.hadoop.ipc.CallQueueManager.convertQueueClass<org.apache.hadoop.ipc.Server.Call
				>(queueClass);
		}

		private string getQueueClassPrefix()
		{
			return org.apache.hadoop.fs.CommonConfigurationKeys.IPC_CALLQUEUE_NAMESPACE + "."
				 + port;
		}

		/*
		* Refresh the call queue
		*/
		public virtual void refreshCallQueue(org.apache.hadoop.conf.Configuration conf)
		{
			lock (this)
			{
				// Create the next queue
				string prefix = getQueueClassPrefix();
				callQueue.swapQueue(getQueueClass(prefix, conf), maxQueueSize, prefix, conf);
			}
		}

		/// <summary>A call queued for handling.</summary>
		public class Call : org.apache.hadoop.ipc.Schedulable
		{
			private readonly int callId;

			private readonly int retryCount;

			private readonly org.apache.hadoop.io.Writable rpcRequest;

			private readonly org.apache.hadoop.ipc.Server.Connection connection;

			private long timestamp;

			private java.nio.ByteBuffer rpcResponse;

			private readonly org.apache.hadoop.ipc.RPC.RpcKind rpcKind;

			private readonly byte[] clientId;

			private readonly org.apache.htrace.Span traceSpan;

			public Call(int id, int retryCount, org.apache.hadoop.io.Writable param, org.apache.hadoop.ipc.Server.Connection
				 connection)
				: this(id, retryCount, param, connection, org.apache.hadoop.ipc.RPC.RpcKind.RPC_BUILTIN
					, org.apache.hadoop.ipc.RpcConstants.DUMMY_CLIENT_ID)
			{
			}

			public Call(int id, int retryCount, org.apache.hadoop.io.Writable param, org.apache.hadoop.ipc.Server.Connection
				 connection, org.apache.hadoop.ipc.RPC.RpcKind kind, byte[] clientId)
				: this(id, retryCount, param, connection, kind, clientId, null)
			{
			}

			public Call(int id, int retryCount, org.apache.hadoop.io.Writable param, org.apache.hadoop.ipc.Server.Connection
				 connection, org.apache.hadoop.ipc.RPC.RpcKind kind, byte[] clientId, org.apache.htrace.Span
				 span)
			{
				// the client's call id
				// the retry count of the call
				// Serialized Rpc request from client
				// connection to client
				// time received when response is null
				// time served when response is not null
				// the response for this call
				// the tracing span on the server side
				this.callId = id;
				this.retryCount = retryCount;
				this.rpcRequest = param;
				this.connection = connection;
				this.timestamp = org.apache.hadoop.util.Time.now();
				this.rpcResponse = null;
				this.rpcKind = kind;
				this.clientId = clientId;
				this.traceSpan = span;
			}

			public override string ToString()
			{
				return rpcRequest + " from " + connection + " Call#" + callId + " Retry#" + retryCount;
			}

			public virtual void setResponse(java.nio.ByteBuffer response)
			{
				this.rpcResponse = response;
			}

			// For Schedulable
			public virtual org.apache.hadoop.security.UserGroupInformation getUserGroupInformation
				()
			{
				return connection.user;
			}
		}

		/// <summary>Listens on the socket.</summary>
		/// <remarks>Listens on the socket. Creates jobs for the handler threads</remarks>
		private class Listener : java.lang.Thread
		{
			private java.nio.channels.ServerSocketChannel acceptChannel = null;

			private java.nio.channels.Selector selector = null;

			private org.apache.hadoop.ipc.Server.Listener.Reader[] readers = null;

			private int currentReader = 0;

			private java.net.InetSocketAddress address;

			private int backlogLength = this._enclosing.conf.getInt(org.apache.hadoop.fs.CommonConfigurationKeysPublic
				.IPC_SERVER_LISTEN_QUEUE_SIZE_KEY, org.apache.hadoop.fs.CommonConfigurationKeysPublic
				.IPC_SERVER_LISTEN_QUEUE_SIZE_DEFAULT);

			/// <exception cref="System.IO.IOException"/>
			public Listener(Server _enclosing)
			{
				this._enclosing = _enclosing;
				//the accept channel
				//the selector that we use for the server
				//the address we bind at
				this.address = new java.net.InetSocketAddress(this._enclosing.bindAddress, this._enclosing
					.port);
				// Create a new server socket and set to non blocking mode
				this.acceptChannel = java.nio.channels.ServerSocketChannel.open();
				this.acceptChannel.configureBlocking(false);
				// Bind the server socket to the local host and port
				org.apache.hadoop.ipc.Server.bind(this.acceptChannel.socket(), this.address, this
					.backlogLength, this._enclosing.conf, this._enclosing.portRangeConfig);
				this._enclosing.port = this.acceptChannel.socket().getLocalPort();
				//Could be an ephemeral port
				// create a selector;
				this.selector = java.nio.channels.Selector.open();
				this.readers = new org.apache.hadoop.ipc.Server.Listener.Reader[this._enclosing.readThreads
					];
				for (int i = 0; i < this._enclosing.readThreads; i++)
				{
					org.apache.hadoop.ipc.Server.Listener.Reader reader = new org.apache.hadoop.ipc.Server.Listener.Reader
						(this, "Socket Reader #" + (i + 1) + " for port " + this._enclosing.port);
					this.readers[i] = reader;
					reader.start();
				}
				// Register accepts on the server socket with the selector.
				this.acceptChannel.register(this.selector, java.nio.channels.SelectionKey.OP_ACCEPT
					);
				this.setName("IPC Server listener on " + this._enclosing.port);
				this.setDaemon(true);
			}

			private class Reader : java.lang.Thread
			{
				private readonly java.util.concurrent.BlockingQueue<org.apache.hadoop.ipc.Server.Connection
					> pendingConnections;

				private readonly java.nio.channels.Selector readSelector;

				/// <exception cref="System.IO.IOException"/>
				internal Reader(Listener _enclosing, string name)
					: base(name)
				{
					this._enclosing = _enclosing;
					this.pendingConnections = new java.util.concurrent.LinkedBlockingQueue<org.apache.hadoop.ipc.Server.Connection
						>(this._enclosing._enclosing.readerPendingConnectionQueue);
					this.readSelector = java.nio.channels.Selector.open();
				}

				public override void run()
				{
					org.apache.hadoop.ipc.Server.LOG.info("Starting " + java.lang.Thread.currentThread
						().getName());
					try
					{
						this.doRunLoop();
					}
					finally
					{
						try
						{
							this.readSelector.close();
						}
						catch (System.IO.IOException ioe)
						{
							org.apache.hadoop.ipc.Server.LOG.error("Error closing read selector in " + java.lang.Thread
								.currentThread().getName(), ioe);
						}
					}
				}

				private void doRunLoop()
				{
					lock (this)
					{
						while (this._enclosing._enclosing.running)
						{
							java.nio.channels.SelectionKey key = null;
							try
							{
								// consume as many connections as currently queued to avoid
								// unbridled acceptance of connections that starves the select
								int size = this.pendingConnections.Count;
								for (int i = size; i > 0; i--)
								{
									org.apache.hadoop.ipc.Server.Connection conn = this.pendingConnections.take();
									conn.channel.register(this.readSelector, java.nio.channels.SelectionKey.OP_READ, 
										conn);
								}
								this.readSelector.select();
								System.Collections.Generic.IEnumerator<java.nio.channels.SelectionKey> iter = this
									.readSelector.selectedKeys().GetEnumerator();
								while (iter.MoveNext())
								{
									key = iter.Current;
									iter.remove();
									if (key.isValid())
									{
										if (key.isReadable())
										{
											this._enclosing.doRead(key);
										}
									}
									key = null;
								}
							}
							catch (System.Exception e)
							{
								if (this._enclosing._enclosing.running)
								{
									// unexpected -- log it
									org.apache.hadoop.ipc.Server.LOG.info(java.lang.Thread.currentThread().getName() 
										+ " unexpectedly interrupted", e);
								}
							}
							catch (System.IO.IOException ex)
							{
								org.apache.hadoop.ipc.Server.LOG.error("Error in Reader", ex);
							}
						}
					}
				}

				/// <summary>
				/// Updating the readSelector while it's being used is not thread-safe,
				/// so the connection must be queued.
				/// </summary>
				/// <remarks>
				/// Updating the readSelector while it's being used is not thread-safe,
				/// so the connection must be queued.  The reader will drain the queue
				/// and update its readSelector before performing the next select
				/// </remarks>
				/// <exception cref="System.Exception"/>
				public virtual void addConnection(org.apache.hadoop.ipc.Server.Connection conn)
				{
					this.pendingConnections.put(conn);
					this.readSelector.wakeup();
				}

				internal virtual void shutdown()
				{
					System.Diagnostics.Debug.Assert(!this._enclosing._enclosing.running);
					this.readSelector.wakeup();
					try
					{
						base.interrupt();
						base.join();
					}
					catch (System.Exception)
					{
						java.lang.Thread.currentThread().interrupt();
					}
				}

				private readonly Listener _enclosing;
			}

			public override void run()
			{
				org.apache.hadoop.ipc.Server.LOG.info(java.lang.Thread.currentThread().getName() 
					+ ": starting");
				org.apache.hadoop.ipc.Server.SERVER.set(this._enclosing);
				this._enclosing.connectionManager.startIdleScan();
				while (this._enclosing.running)
				{
					java.nio.channels.SelectionKey key = null;
					try
					{
						this.getSelector().select();
						System.Collections.Generic.IEnumerator<java.nio.channels.SelectionKey> iter = this
							.getSelector().selectedKeys().GetEnumerator();
						while (iter.MoveNext())
						{
							key = iter.Current;
							iter.remove();
							try
							{
								if (key.isValid())
								{
									if (key.isAcceptable())
									{
										this.doAccept(key);
									}
								}
							}
							catch (System.IO.IOException)
							{
							}
							key = null;
						}
					}
					catch (System.OutOfMemoryException e)
					{
						// we can run out of memory if we have too many threads
						// log the event and sleep for a minute and give 
						// some thread(s) a chance to finish
						org.apache.hadoop.ipc.Server.LOG.warn("Out of Memory in server select", e);
						this.closeCurrentConnection(key, e);
						this._enclosing.connectionManager.closeIdle(true);
						try
						{
							java.lang.Thread.sleep(60000);
						}
						catch (System.Exception)
						{
						}
					}
					catch (System.Exception e)
					{
						this.closeCurrentConnection(key, e);
					}
				}
				org.apache.hadoop.ipc.Server.LOG.info("Stopping " + java.lang.Thread.currentThread
					().getName());
				lock (this)
				{
					try
					{
						this.acceptChannel.close();
						this.selector.close();
					}
					catch (System.IO.IOException)
					{
					}
					this.selector = null;
					this.acceptChannel = null;
					// close all connections
					this._enclosing.connectionManager.stopIdleScan();
					this._enclosing.connectionManager.closeAll();
				}
			}

			private void closeCurrentConnection(java.nio.channels.SelectionKey key, System.Exception
				 e)
			{
				if (key != null)
				{
					org.apache.hadoop.ipc.Server.Connection c = (org.apache.hadoop.ipc.Server.Connection
						)key.attachment();
					if (c != null)
					{
						this._enclosing.closeConnection(c);
						c = null;
					}
				}
			}

			internal virtual java.net.InetSocketAddress getAddress()
			{
				return (java.net.InetSocketAddress)this.acceptChannel.socket().getLocalSocketAddress
					();
			}

			/// <exception cref="System.Exception"/>
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.OutOfMemoryException"/>
			internal virtual void doAccept(java.nio.channels.SelectionKey key)
			{
				java.nio.channels.ServerSocketChannel server = (java.nio.channels.ServerSocketChannel
					)key.channel();
				java.nio.channels.SocketChannel channel;
				while ((channel = server.accept()) != null)
				{
					channel.configureBlocking(false);
					channel.socket().setTcpNoDelay(this._enclosing.tcpNoDelay);
					channel.socket().setKeepAlive(true);
					org.apache.hadoop.ipc.Server.Listener.Reader reader = this.getReader();
					org.apache.hadoop.ipc.Server.Connection c = this._enclosing.connectionManager.register
						(channel);
					// If the connectionManager can't take it, close the connection.
					if (c == null)
					{
						if (channel.isOpen())
						{
							org.apache.hadoop.io.IOUtils.cleanup(null, channel);
						}
						continue;
					}
					key.attach(c);
					// so closeCurrentConnection can get the object
					reader.addConnection(c);
				}
			}

			/// <exception cref="System.Exception"/>
			internal virtual void doRead(java.nio.channels.SelectionKey key)
			{
				int count = 0;
				org.apache.hadoop.ipc.Server.Connection c = (org.apache.hadoop.ipc.Server.Connection
					)key.attachment();
				if (c == null)
				{
					return;
				}
				c.setLastContact(org.apache.hadoop.util.Time.now());
				try
				{
					count = c.readAndProcess();
				}
				catch (System.Exception ieo)
				{
					org.apache.hadoop.ipc.Server.LOG.info(java.lang.Thread.currentThread().getName() 
						+ ": readAndProcess caught InterruptedException", ieo);
					throw;
				}
				catch (System.Exception e)
				{
					// a WrappedRpcServerException is an exception that has been sent
					// to the client, so the stacktrace is unnecessary; any other
					// exceptions are unexpected internal server errors and thus the
					// stacktrace should be logged
					org.apache.hadoop.ipc.Server.LOG.info(java.lang.Thread.currentThread().getName() 
						+ ": readAndProcess from client " + c.getHostAddress() + " threw exception [" + 
						e + "]", (e is org.apache.hadoop.ipc.Server.WrappedRpcServerException) ? null : 
						e);
					count = -1;
				}
				//so that the (count < 0) block is executed
				if (count < 0)
				{
					this._enclosing.closeConnection(c);
					c = null;
				}
				else
				{
					c.setLastContact(org.apache.hadoop.util.Time.now());
				}
			}

			internal virtual void doStop()
			{
				lock (this)
				{
					if (this.selector != null)
					{
						this.selector.wakeup();
						java.lang.Thread.yield();
					}
					if (this.acceptChannel != null)
					{
						try
						{
							this.acceptChannel.socket().close();
						}
						catch (System.IO.IOException e)
						{
							org.apache.hadoop.ipc.Server.LOG.info(java.lang.Thread.currentThread().getName() 
								+ ":Exception in closing listener socket. " + e);
						}
					}
					foreach (org.apache.hadoop.ipc.Server.Listener.Reader r in this.readers)
					{
						r.shutdown();
					}
				}
			}

			internal virtual java.nio.channels.Selector getSelector()
			{
				lock (this)
				{
					return this.selector;
				}
			}

			// The method that will return the next reader to work with
			// Simplistic implementation of round robin for now
			internal virtual org.apache.hadoop.ipc.Server.Listener.Reader getReader()
			{
				this.currentReader = (this.currentReader + 1) % this.readers.Length;
				return this.readers[this.currentReader];
			}

			private readonly Server _enclosing;
		}

		private class Responder : java.lang.Thread
		{
			private readonly java.nio.channels.Selector writeSelector;

			private int pending;

			internal const int PURGE_INTERVAL = 900000;

			/// <exception cref="System.IO.IOException"/>
			internal Responder(Server _enclosing)
			{
				this._enclosing = _enclosing;
				// Sends responses of RPC back to clients.
				// connections waiting to register
				// 15mins
				this.setName("IPC Server Responder");
				this.setDaemon(true);
				this.writeSelector = java.nio.channels.Selector.open();
				// create a selector
				this.pending = 0;
			}

			public override void run()
			{
				org.apache.hadoop.ipc.Server.LOG.info(java.lang.Thread.currentThread().getName() 
					+ ": starting");
				org.apache.hadoop.ipc.Server.SERVER.set(this._enclosing);
				try
				{
					this.doRunLoop();
				}
				finally
				{
					org.apache.hadoop.ipc.Server.LOG.info("Stopping " + java.lang.Thread.currentThread
						().getName());
					try
					{
						this.writeSelector.close();
					}
					catch (System.IO.IOException ioe)
					{
						org.apache.hadoop.ipc.Server.LOG.error("Couldn't close write selector in " + java.lang.Thread
							.currentThread().getName(), ioe);
					}
				}
			}

			private void doRunLoop()
			{
				long lastPurgeTime = 0;
				// last check for old calls.
				while (this._enclosing.running)
				{
					try
					{
						this.waitPending();
						// If a channel is being registered, wait.
						this.writeSelector.select(org.apache.hadoop.ipc.Server.Responder.PURGE_INTERVAL);
						System.Collections.Generic.IEnumerator<java.nio.channels.SelectionKey> iter = this
							.writeSelector.selectedKeys().GetEnumerator();
						while (iter.MoveNext())
						{
							java.nio.channels.SelectionKey key = iter.Current;
							iter.remove();
							try
							{
								if (key.isValid() && key.isWritable())
								{
									this.doAsyncWrite(key);
								}
							}
							catch (System.IO.IOException e)
							{
								org.apache.hadoop.ipc.Server.LOG.info(java.lang.Thread.currentThread().getName() 
									+ ": doAsyncWrite threw exception " + e);
							}
						}
						long now = org.apache.hadoop.util.Time.now();
						if (now < lastPurgeTime + org.apache.hadoop.ipc.Server.Responder.PURGE_INTERVAL)
						{
							continue;
						}
						lastPurgeTime = now;
						//
						// If there were some calls that have not been sent out for a
						// long time, discard them.
						//
						if (org.apache.hadoop.ipc.Server.LOG.isDebugEnabled())
						{
							org.apache.hadoop.ipc.Server.LOG.debug("Checking for old call responses.");
						}
						System.Collections.Generic.List<org.apache.hadoop.ipc.Server.Call> calls;
						// get the list of channels from list of keys.
						lock (this.writeSelector.keys())
						{
							calls = new System.Collections.Generic.List<org.apache.hadoop.ipc.Server.Call>(this
								.writeSelector.keys().Count);
							iter = this.writeSelector.keys().GetEnumerator();
							while (iter.MoveNext())
							{
								java.nio.channels.SelectionKey key = iter.Current;
								org.apache.hadoop.ipc.Server.Call call = (org.apache.hadoop.ipc.Server.Call)key.attachment
									();
								if (call != null && key.channel() == call.connection.channel)
								{
									calls.add(call);
								}
							}
						}
						foreach (org.apache.hadoop.ipc.Server.Call call_1 in calls)
						{
							this.doPurge(call_1, now);
						}
					}
					catch (System.OutOfMemoryException e)
					{
						//
						// we can run out of memory if we have too many threads
						// log the event and sleep for a minute and give
						// some thread(s) a chance to finish
						//
						org.apache.hadoop.ipc.Server.LOG.warn("Out of Memory in server select", e);
						try
						{
							java.lang.Thread.sleep(60000);
						}
						catch (System.Exception)
						{
						}
					}
					catch (System.Exception e)
					{
						org.apache.hadoop.ipc.Server.LOG.warn("Exception in Responder", e);
					}
				}
			}

			/// <exception cref="System.IO.IOException"/>
			private void doAsyncWrite(java.nio.channels.SelectionKey key)
			{
				org.apache.hadoop.ipc.Server.Call call = (org.apache.hadoop.ipc.Server.Call)key.attachment
					();
				if (call == null)
				{
					return;
				}
				if (key.channel() != call.connection.channel)
				{
					throw new System.IO.IOException("doAsyncWrite: bad channel");
				}
				lock (call.connection.responseQueue)
				{
					if (this.processResponse(call.connection.responseQueue, false))
					{
						try
						{
							key.interestOps(0);
						}
						catch (java.nio.channels.CancelledKeyException e)
						{
							/* The Listener/reader might have closed the socket.
							* We don't explicitly cancel the key, so not sure if this will
							* ever fire.
							* This warning could be removed.
							*/
							org.apache.hadoop.ipc.Server.LOG.warn("Exception while changing ops : " + e);
						}
					}
				}
			}

			//
			// Remove calls that have been pending in the responseQueue 
			// for a long time.
			//
			private void doPurge(org.apache.hadoop.ipc.Server.Call call, long now)
			{
				System.Collections.Generic.LinkedList<org.apache.hadoop.ipc.Server.Call> responseQueue
					 = call.connection.responseQueue;
				lock (responseQueue)
				{
					System.Collections.Generic.IEnumerator<org.apache.hadoop.ipc.Server.Call> iter = 
						responseQueue.listIterator(0);
					while (iter.MoveNext())
					{
						call = iter.Current;
						if (now > call.timestamp + org.apache.hadoop.ipc.Server.Responder.PURGE_INTERVAL)
						{
							this._enclosing.closeConnection(call.connection);
							break;
						}
					}
				}
			}

			// Processes one response. Returns true if there are no more pending
			// data for this channel.
			//
			/// <exception cref="System.IO.IOException"/>
			private bool processResponse(System.Collections.Generic.LinkedList<org.apache.hadoop.ipc.Server.Call
				> responseQueue, bool inHandler)
			{
				bool error = true;
				bool done = false;
				// there is more data for this channel.
				int numElements = 0;
				org.apache.hadoop.ipc.Server.Call call = null;
				try
				{
					lock (responseQueue)
					{
						//
						// If there are no items for this channel, then we are done
						//
						numElements = responseQueue.Count;
						if (numElements == 0)
						{
							error = false;
							return true;
						}
						// no more data for this channel.
						//
						// Extract the first call
						//
						call = responseQueue.removeFirst();
						java.nio.channels.SocketChannel channel = call.connection.channel;
						if (org.apache.hadoop.ipc.Server.LOG.isDebugEnabled())
						{
							org.apache.hadoop.ipc.Server.LOG.debug(java.lang.Thread.currentThread().getName()
								 + ": responding to " + call);
						}
						//
						// Send as much data as we can in the non-blocking fashion
						//
						int numBytes = this._enclosing.channelWrite(channel, call.rpcResponse);
						if (numBytes < 0)
						{
							return true;
						}
						if (!call.rpcResponse.hasRemaining())
						{
							//Clear out the response buffer so it can be collected
							call.rpcResponse = null;
							call.connection.decRpcCount();
							if (numElements == 1)
							{
								// last call fully processes.
								done = true;
							}
							else
							{
								// no more data for this channel.
								done = false;
							}
							// more calls pending to be sent.
							if (org.apache.hadoop.ipc.Server.LOG.isDebugEnabled())
							{
								org.apache.hadoop.ipc.Server.LOG.debug(java.lang.Thread.currentThread().getName()
									 + ": responding to " + call + " Wrote " + numBytes + " bytes.");
							}
						}
						else
						{
							//
							// If we were unable to write the entire response out, then 
							// insert in Selector queue. 
							//
							call.connection.responseQueue.addFirst(call);
							if (inHandler)
							{
								// set the serve time when the response has to be sent later
								call.timestamp = org.apache.hadoop.util.Time.now();
								this.incPending();
								try
								{
									// Wakeup the thread blocked on select, only then can the call 
									// to channel.register() complete.
									this.writeSelector.wakeup();
									channel.register(this.writeSelector, java.nio.channels.SelectionKey.OP_WRITE, call
										);
								}
								catch (java.nio.channels.ClosedChannelException)
								{
									//Its ok. channel might be closed else where.
									done = true;
								}
								finally
								{
									this.decPending();
								}
							}
							if (org.apache.hadoop.ipc.Server.LOG.isDebugEnabled())
							{
								org.apache.hadoop.ipc.Server.LOG.debug(java.lang.Thread.currentThread().getName()
									 + ": responding to " + call + " Wrote partial " + numBytes + " bytes.");
							}
						}
						error = false;
					}
				}
				finally
				{
					// everything went off well
					if (error && call != null)
					{
						org.apache.hadoop.ipc.Server.LOG.warn(java.lang.Thread.currentThread().getName() 
							+ ", call " + call + ": output error");
						done = true;
						// error. no more data for this channel.
						this._enclosing.closeConnection(call.connection);
					}
				}
				return done;
			}

			//
			// Enqueue a response from the application.
			//
			/// <exception cref="System.IO.IOException"/>
			internal virtual void doRespond(org.apache.hadoop.ipc.Server.Call call)
			{
				lock (call.connection.responseQueue)
				{
					call.connection.responseQueue.addLast(call);
					if (call.connection.responseQueue.Count == 1)
					{
						this.processResponse(call.connection.responseQueue, true);
					}
				}
			}

			private void incPending()
			{
				lock (this)
				{
					// call waiting to be enqueued.
					this.pending++;
				}
			}

			private void decPending()
			{
				lock (this)
				{
					// call done enqueueing.
					this.pending--;
					Sharpen.Runtime.notify(this);
				}
			}

			/// <exception cref="System.Exception"/>
			private void waitPending()
			{
				lock (this)
				{
					while (this.pending > 0)
					{
						Sharpen.Runtime.wait(this);
					}
				}
			}

			private readonly Server _enclosing;
		}

		[System.Serializable]
		public sealed class AuthProtocol
		{
			public static readonly org.apache.hadoop.ipc.Server.AuthProtocol NONE = new org.apache.hadoop.ipc.Server.AuthProtocol
				(0);

			public static readonly org.apache.hadoop.ipc.Server.AuthProtocol SASL = new org.apache.hadoop.ipc.Server.AuthProtocol
				(-33);

			public readonly int callId;

			internal AuthProtocol(int callId)
			{
				this.callId = callId;
			}

			internal static org.apache.hadoop.ipc.Server.AuthProtocol valueOf(int callId)
			{
				foreach (org.apache.hadoop.ipc.Server.AuthProtocol authType in org.apache.hadoop.ipc.Server.AuthProtocol
					.values())
				{
					if (authType.callId == callId)
					{
						return authType;
					}
				}
				return null;
			}
		}

		/// <summary>Wrapper for RPC IOExceptions to be returned to the client.</summary>
		/// <remarks>
		/// Wrapper for RPC IOExceptions to be returned to the client.  Used to
		/// let exceptions bubble up to top of processOneRpc where the correct
		/// callId can be associated with the response.  Also used to prevent
		/// unnecessary stack trace logging if it's not an internal server error.
		/// </remarks>
		[System.Serializable]
		private class WrappedRpcServerException : org.apache.hadoop.ipc.RpcServerException
		{
			private readonly org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto
				 errCode;

			public WrappedRpcServerException(org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto
				 errCode, System.IO.IOException ioe)
				: base(ioe.ToString(), ioe)
			{
				this.errCode = errCode;
			}

			public WrappedRpcServerException(org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto
				 errCode, string message)
				: this(errCode, new org.apache.hadoop.ipc.RpcServerException(message))
			{
			}

			public override org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto
				 getRpcErrorCodeProto()
			{
				return errCode;
			}

			public override string ToString()
			{
				return InnerException.ToString();
			}
		}

		/// <summary>Reads calls from a connection and queues them for handling.</summary>
		public class Connection
		{
			private bool connectionHeaderRead = false;

			private bool connectionContextRead = false;

			private java.nio.channels.SocketChannel channel;

			private java.nio.ByteBuffer data;

			private java.nio.ByteBuffer dataLengthBuffer;

			private System.Collections.Generic.LinkedList<org.apache.hadoop.ipc.Server.Call> 
				responseQueue;

			private java.util.concurrent.atomic.AtomicInteger rpcCount = new java.util.concurrent.atomic.AtomicInteger
				();

			private long lastContact;

			private int dataLength;

			private java.net.Socket socket;

			private string hostAddress;

			private int remotePort;

			private java.net.InetAddress addr;

			internal org.apache.hadoop.ipc.protobuf.IpcConnectionContextProtos.IpcConnectionContextProto
				 connectionContext;

			internal string protocolName;

			internal javax.security.sasl.SaslServer saslServer;

			private org.apache.hadoop.security.SaslRpcServer.AuthMethod authMethod;

			private org.apache.hadoop.ipc.Server.AuthProtocol authProtocol;

			private bool saslContextEstablished;

			private java.nio.ByteBuffer connectionHeaderBuf = null;

			private java.nio.ByteBuffer unwrappedData;

			private java.nio.ByteBuffer unwrappedDataLengthBuffer;

			private int serviceClass;

			internal org.apache.hadoop.security.UserGroupInformation user = null;

			public org.apache.hadoop.security.UserGroupInformation attemptingUser = null;

			private readonly org.apache.hadoop.ipc.Server.Call authFailedCall;

			private java.io.ByteArrayOutputStream authFailedResponse = new java.io.ByteArrayOutputStream
				();

			private readonly org.apache.hadoop.ipc.Server.Call saslCall;

			private readonly java.io.ByteArrayOutputStream saslResponse = new java.io.ByteArrayOutputStream
				();

			private bool sentNegotiate = false;

			private bool useWrap = false;

			public Connection(Server _enclosing, java.nio.channels.SocketChannel channel, long
				 lastContact)
			{
				this._enclosing = _enclosing;
				authFailedCall = new org.apache.hadoop.ipc.Server.Call(org.apache.hadoop.ipc.RpcConstants
					.AUTHORIZATION_FAILED_CALL_ID, org.apache.hadoop.ipc.RpcConstants.INVALID_RETRY_COUNT
					, null, this);
				saslCall = new org.apache.hadoop.ipc.Server.Call(org.apache.hadoop.ipc.Server.AuthProtocol
					.SASL.callId, org.apache.hadoop.ipc.RpcConstants.INVALID_RETRY_COUNT, null, this
					);
				// connection  header is read?
				//if connection context that
				//follows connection header is read
				// number of outstanding rpcs
				// Cache the remote host & port info so that even if the socket is 
				// disconnected, we can say where it used to connect to.
				// user name before auth
				// Fake 'call' for failed authorization response
				this.channel = channel;
				this.lastContact = lastContact;
				this.data = null;
				this.dataLengthBuffer = java.nio.ByteBuffer.allocate(4);
				this.unwrappedData = null;
				this.unwrappedDataLengthBuffer = java.nio.ByteBuffer.allocate(4);
				this.socket = channel.socket();
				this.addr = this.socket.getInetAddress();
				if (this.addr == null)
				{
					this.hostAddress = "*Unknown*";
				}
				else
				{
					this.hostAddress = this.addr.getHostAddress();
				}
				this.remotePort = this.socket.getPort();
				this.responseQueue = new System.Collections.Generic.LinkedList<org.apache.hadoop.ipc.Server.Call
					>();
				if (this._enclosing.socketSendBufferSize != 0)
				{
					try
					{
						this.socket.setSendBufferSize(this._enclosing.socketSendBufferSize);
					}
					catch (System.IO.IOException)
					{
						org.apache.hadoop.ipc.Server.LOG.warn("Connection: unable to set socket send buffer size to "
							 + this._enclosing.socketSendBufferSize);
					}
				}
			}

			public override string ToString()
			{
				return this.getHostAddress() + ":" + this.remotePort;
			}

			public virtual string getHostAddress()
			{
				return this.hostAddress;
			}

			public virtual java.net.InetAddress getHostInetAddress()
			{
				return this.addr;
			}

			public virtual void setLastContact(long lastContact)
			{
				this.lastContact = lastContact;
			}

			public virtual long getLastContact()
			{
				return this.lastContact;
			}

			/* Return true if the connection has no outstanding rpc */
			private bool isIdle()
			{
				return this.rpcCount.get() == 0;
			}

			/* Decrement the outstanding RPC count */
			private void decRpcCount()
			{
				this.rpcCount.decrementAndGet();
			}

			/* Increment the outstanding RPC count */
			private void incRpcCount()
			{
				this.rpcCount.incrementAndGet();
			}

			/// <exception cref="org.apache.hadoop.security.token.SecretManager.InvalidToken"/>
			/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
			private org.apache.hadoop.security.UserGroupInformation getAuthorizedUgi(string authorizedId
				)
			{
				if (this.authMethod == org.apache.hadoop.security.SaslRpcServer.AuthMethod.TOKEN)
				{
					org.apache.hadoop.security.token.TokenIdentifier tokenId = org.apache.hadoop.security.SaslRpcServer
						.getIdentifier(authorizedId, this._enclosing.secretManager);
					org.apache.hadoop.security.UserGroupInformation ugi = tokenId.getUser();
					if (ugi == null)
					{
						throw new org.apache.hadoop.security.AccessControlException("Can't retrieve username from tokenIdentifier."
							);
					}
					ugi.addTokenIdentifier(tokenId);
					return ugi;
				}
				else
				{
					return org.apache.hadoop.security.UserGroupInformation.createRemoteUser(authorizedId
						, this.authMethod);
				}
			}

			/// <exception cref="org.apache.hadoop.ipc.Server.WrappedRpcServerException"/>
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			private void saslReadAndProcess(java.io.DataInputStream dis)
			{
				org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto saslMessage = this.decodeProtobufFromStream
					(org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto.newBuilder(), dis);
				switch (saslMessage.getState())
				{
					case org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto.SaslState.WRAP:
					{
						if (!this.saslContextEstablished || !this.useWrap)
						{
							throw new org.apache.hadoop.ipc.Server.WrappedRpcServerException(org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto
								.FATAL_INVALID_RPC_HEADER, new javax.security.sasl.SaslException("Server is not wrapping data"
								));
						}
						// loops over decoded data and calls processOneRpc
						this.unwrapPacketAndProcessRpcs(saslMessage.getToken().toByteArray());
						break;
					}

					default:
					{
						this.saslProcess(saslMessage);
						break;
					}
				}
			}

			private System.Exception getCauseForInvalidToken(System.IO.IOException e)
			{
				System.Exception cause = e;
				while (cause != null)
				{
					if (cause is org.apache.hadoop.ipc.RetriableException)
					{
						return cause;
					}
					else
					{
						if (cause is org.apache.hadoop.ipc.StandbyException)
						{
							return cause;
						}
						else
						{
							if (cause is org.apache.hadoop.security.token.SecretManager.InvalidToken)
							{
								// FIXME: hadoop method signatures are restricting the SASL
								// callbacks to only returning InvalidToken, but some services
								// need to throw other exceptions (ex. NN + StandyException),
								// so for now we'll tunnel the real exceptions via an
								// InvalidToken's cause which normally is not set 
								if (cause.InnerException != null)
								{
									cause = cause.InnerException;
								}
								return cause;
							}
						}
					}
					cause = cause.InnerException;
				}
				return e;
			}

			/// <exception cref="org.apache.hadoop.ipc.Server.WrappedRpcServerException"/>
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			private void saslProcess(org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto
				 saslMessage)
			{
				if (this.saslContextEstablished)
				{
					throw new org.apache.hadoop.ipc.Server.WrappedRpcServerException(org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto
						.FATAL_INVALID_RPC_HEADER, new javax.security.sasl.SaslException("Negotiation is already complete"
						));
				}
				org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto saslResponse = null;
				try
				{
					try
					{
						saslResponse = this.processSaslMessage(saslMessage);
					}
					catch (System.IO.IOException e)
					{
						this._enclosing.rpcMetrics.incrAuthenticationFailures();
						// attempting user could be null
						org.apache.hadoop.ipc.Server.AUDITLOG.warn(org.apache.hadoop.ipc.Server.AUTH_FAILED_FOR
							 + this.ToString() + ":" + this.attemptingUser + " (" + e.getLocalizedMessage() 
							+ ")");
						throw (System.IO.IOException)this.getCauseForInvalidToken(e);
					}
					if (this.saslServer != null && this.saslServer.isComplete())
					{
						if (org.apache.hadoop.ipc.Server.LOG.isDebugEnabled())
						{
							org.apache.hadoop.ipc.Server.LOG.debug("SASL server context established. Negotiated QoP is "
								 + this.saslServer.getNegotiatedProperty(javax.security.sasl.Sasl.QOP));
						}
						this.user = this.getAuthorizedUgi(this.saslServer.getAuthorizationID());
						if (org.apache.hadoop.ipc.Server.LOG.isDebugEnabled())
						{
							org.apache.hadoop.ipc.Server.LOG.debug("SASL server successfully authenticated client: "
								 + this.user);
						}
						this._enclosing.rpcMetrics.incrAuthenticationSuccesses();
						org.apache.hadoop.ipc.Server.AUDITLOG.info(org.apache.hadoop.ipc.Server.AUTH_SUCCESSFUL_FOR
							 + this.user);
						this.saslContextEstablished = true;
					}
				}
				catch (org.apache.hadoop.ipc.Server.WrappedRpcServerException wrse)
				{
					// don't re-wrap
					throw;
				}
				catch (System.IO.IOException ioe)
				{
					throw new org.apache.hadoop.ipc.Server.WrappedRpcServerException(org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto
						.FATAL_UNAUTHORIZED, ioe);
				}
				// send back response if any, may throw IOException
				if (saslResponse != null)
				{
					this.doSaslReply(saslResponse);
				}
				// do NOT enable wrapping until the last auth response is sent
				if (this.saslContextEstablished)
				{
					string qop = (string)this.saslServer.getNegotiatedProperty(javax.security.sasl.Sasl
						.QOP);
					// SASL wrapping is only used if the connection has a QOP, and
					// the value is not auth.  ex. auth-int & auth-priv
					this.useWrap = (qop != null && !Sharpen.Runtime.equalsIgnoreCase("auth", qop));
				}
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			private org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto processSaslMessage
				(org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto saslMessage)
			{
				org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto saslResponse;
				org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto.SaslState state = saslMessage
					.getState();
				switch (state)
				{
					case org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto.SaslState.NEGOTIATE
						:
					{
						// required      
						if (this.sentNegotiate)
						{
							throw new org.apache.hadoop.security.AccessControlException("Client already attempted negotiation"
								);
						}
						saslResponse = this.buildSaslNegotiateResponse();
						// simple-only server negotiate response is success which client
						// interprets as switch to simple
						if (saslResponse.getState() == org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto.SaslState
							.SUCCESS)
						{
							this.switchToSimple();
						}
						break;
					}

					case org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto.SaslState.INITIATE
						:
					{
						if (saslMessage.getAuthsCount() != 1)
						{
							throw new javax.security.sasl.SaslException("Client mechanism is malformed");
						}
						// verify the client requested an advertised authType
						org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto.SaslAuth clientSaslAuth
							 = saslMessage.getAuths(0);
						if (!this._enclosing.negotiateResponse.getAuthsList().contains(clientSaslAuth))
						{
							if (this.sentNegotiate)
							{
								throw new org.apache.hadoop.security.AccessControlException(clientSaslAuth.getMethod
									() + " authentication is not enabled." + "  Available:" + this._enclosing.enabledAuthMethods
									);
							}
							saslResponse = this.buildSaslNegotiateResponse();
							break;
						}
						this.authMethod = org.apache.hadoop.security.SaslRpcServer.AuthMethod.valueOf(clientSaslAuth
							.getMethod());
						// abort SASL for SIMPLE auth, server has already ensured that
						// SIMPLE is a legit option above.  we will send no response
						if (this.authMethod == org.apache.hadoop.security.SaslRpcServer.AuthMethod.SIMPLE)
						{
							this.switchToSimple();
							saslResponse = null;
							break;
						}
						// sasl server for tokens may already be instantiated
						if (this.saslServer == null || this.authMethod != org.apache.hadoop.security.SaslRpcServer.AuthMethod
							.TOKEN)
						{
							this.saslServer = this.createSaslServer(this.authMethod);
						}
						saslResponse = this.processSaslToken(saslMessage);
						break;
					}

					case org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto.SaslState.RESPONSE
						:
					{
						saslResponse = this.processSaslToken(saslMessage);
						break;
					}

					default:
					{
						throw new javax.security.sasl.SaslException("Client sent unsupported state " + state
							);
					}
				}
				return saslResponse;
			}

			/// <exception cref="javax.security.sasl.SaslException"/>
			private org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto processSaslToken
				(org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto saslMessage)
			{
				if (!saslMessage.hasToken())
				{
					throw new javax.security.sasl.SaslException("Client did not send a token");
				}
				byte[] saslToken = saslMessage.getToken().toByteArray();
				if (org.apache.hadoop.ipc.Server.LOG.isDebugEnabled())
				{
					org.apache.hadoop.ipc.Server.LOG.debug("Have read input token of size " + saslToken
						.Length + " for processing by saslServer.evaluateResponse()");
				}
				saslToken = this.saslServer.evaluateResponse(saslToken);
				return this.buildSaslResponse(this.saslServer.isComplete() ? org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto.SaslState
					.SUCCESS : org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto.SaslState
					.CHALLENGE, saslToken);
			}

			private void switchToSimple()
			{
				// disable SASL and blank out any SASL server
				this.authProtocol = org.apache.hadoop.ipc.Server.AuthProtocol.NONE;
				this.saslServer = null;
			}

			private org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto buildSaslResponse
				(org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto.SaslState state, byte
				[] replyToken)
			{
				if (org.apache.hadoop.ipc.Server.LOG.isDebugEnabled())
				{
					org.apache.hadoop.ipc.Server.LOG.debug("Will send " + state + " token of size " +
						 ((replyToken != null) ? replyToken.Length : null) + " from saslServer.");
				}
				org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto.Builder response = org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto
					.newBuilder();
				response.setState(state);
				if (replyToken != null)
				{
					response.setToken(com.google.protobuf.ByteString.copyFrom(replyToken));
				}
				return ((org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto)response.build
					());
			}

			/// <exception cref="System.IO.IOException"/>
			private void doSaslReply(com.google.protobuf.Message message)
			{
				if (org.apache.hadoop.ipc.Server.LOG.isDebugEnabled())
				{
					org.apache.hadoop.ipc.Server.LOG.debug("Sending sasl message " + message);
				}
				this._enclosing.setupResponse(this.saslResponse, this.saslCall, org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto
					.SUCCESS, null, new org.apache.hadoop.ipc.ProtobufRpcEngine.RpcResponseWrapper(message
					), null, null);
				this._enclosing.responder.doRespond(this.saslCall);
			}

			/// <exception cref="System.IO.IOException"/>
			private void doSaslReply(System.Exception ioe)
			{
				this._enclosing.setupResponse(this.authFailedResponse, this.authFailedCall, org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto
					.FATAL, org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto
					.FATAL_UNAUTHORIZED, null, Sharpen.Runtime.getClassForObject(ioe).getName(), ioe
					.getLocalizedMessage());
				this._enclosing.responder.doRespond(this.authFailedCall);
			}

			private void disposeSasl()
			{
				if (this.saslServer != null)
				{
					try
					{
						this.saslServer.dispose();
					}
					catch (javax.security.sasl.SaslException)
					{
					}
				}
			}

			/// <exception cref="System.IO.IOException"/>
			private void checkDataLength(int dataLength)
			{
				if (dataLength < 0)
				{
					string error = "Unexpected data length " + dataLength + "!! from " + this.getHostAddress
						();
					org.apache.hadoop.ipc.Server.LOG.warn(error);
					throw new System.IO.IOException(error);
				}
				else
				{
					if (dataLength > this._enclosing.maxDataLength)
					{
						string error = "Requested data length " + dataLength + " is longer than maximum configured RPC length "
							 + this._enclosing.maxDataLength + ".  RPC came from " + this.getHostAddress();
						org.apache.hadoop.ipc.Server.LOG.warn(error);
						throw new System.IO.IOException(error);
					}
				}
			}

			/// <exception cref="org.apache.hadoop.ipc.Server.WrappedRpcServerException"/>
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public virtual int readAndProcess()
			{
				while (true)
				{
					/* Read at most one RPC. If the header is not read completely yet
					* then iterate until we read first RPC or until there is no data left.
					*/
					int count = -1;
					if (this.dataLengthBuffer.remaining() > 0)
					{
						count = this._enclosing.channelRead(this.channel, this.dataLengthBuffer);
						if (count < 0 || this.dataLengthBuffer.remaining() > 0)
						{
							return count;
						}
					}
					if (!this.connectionHeaderRead)
					{
						//Every connection is expected to send the header.
						if (this.connectionHeaderBuf == null)
						{
							this.connectionHeaderBuf = java.nio.ByteBuffer.allocate(3);
						}
						count = this._enclosing.channelRead(this.channel, this.connectionHeaderBuf);
						if (count < 0 || this.connectionHeaderBuf.remaining() > 0)
						{
							return count;
						}
						int version = this.connectionHeaderBuf.get(0);
						// TODO we should add handler for service class later
						this.setServiceClass(this.connectionHeaderBuf.get(1));
						this.dataLengthBuffer.flip();
						// Check if it looks like the user is hitting an IPC port
						// with an HTTP GET - this is a common error, so we can
						// send back a simple string indicating as much.
						if (org.apache.hadoop.ipc.Server.HTTP_GET_BYTES.Equals(this.dataLengthBuffer))
						{
							this.setupHttpRequestOnIpcPortResponse();
							return -1;
						}
						if (!org.apache.hadoop.ipc.RpcConstants.HEADER.Equals(this.dataLengthBuffer) || version
							 != org.apache.hadoop.ipc.RpcConstants.CURRENT_VERSION)
						{
							//Warning is ok since this is not supposed to happen.
							org.apache.hadoop.ipc.Server.LOG.warn("Incorrect header or version mismatch from "
								 + this.hostAddress + ":" + this.remotePort + " got version " + version + " expected version "
								 + org.apache.hadoop.ipc.RpcConstants.CURRENT_VERSION);
							this.setupBadVersionResponse(version);
							return -1;
						}
						// this may switch us into SIMPLE
						this.authProtocol = this.initializeAuthContext(this.connectionHeaderBuf.get(2));
						this.dataLengthBuffer.clear();
						this.connectionHeaderBuf = null;
						this.connectionHeaderRead = true;
						continue;
					}
					if (this.data == null)
					{
						this.dataLengthBuffer.flip();
						this.dataLength = this.dataLengthBuffer.getInt();
						this.checkDataLength(this.dataLength);
						this.data = java.nio.ByteBuffer.allocate(this.dataLength);
					}
					count = this._enclosing.channelRead(this.channel, this.data);
					if (this.data.remaining() == 0)
					{
						this.dataLengthBuffer.clear();
						this.data.flip();
						bool isHeaderRead = this.connectionContextRead;
						this.processOneRpc(((byte[])this.data.array()));
						this.data = null;
						if (!isHeaderRead)
						{
							continue;
						}
					}
					return count;
				}
			}

			/// <exception cref="System.IO.IOException"/>
			private org.apache.hadoop.ipc.Server.AuthProtocol initializeAuthContext(int authType
				)
			{
				org.apache.hadoop.ipc.Server.AuthProtocol authProtocol = org.apache.hadoop.ipc.Server.AuthProtocol
					.valueOf(authType);
				if (authProtocol == null)
				{
					System.IO.IOException ioe = new org.apache.hadoop.ipc.IpcException("Unknown auth protocol:"
						 + authType);
					this.doSaslReply(ioe);
					throw ioe;
				}
				bool isSimpleEnabled = this._enclosing.enabledAuthMethods.contains(org.apache.hadoop.security.SaslRpcServer.AuthMethod
					.SIMPLE);
				switch (authProtocol)
				{
					case org.apache.hadoop.ipc.Server.AuthProtocol.NONE:
					{
						// don't reply if client is simple and server is insecure
						if (!isSimpleEnabled)
						{
							System.IO.IOException ioe = new org.apache.hadoop.security.AccessControlException
								("SIMPLE authentication is not enabled." + "  Available:" + this._enclosing.enabledAuthMethods
								);
							this.doSaslReply(ioe);
							throw ioe;
						}
						break;
					}

					default:
					{
						break;
					}
				}
				return authProtocol;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			private org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto buildSaslNegotiateResponse
				()
			{
				org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto negotiateMessage = this
					._enclosing.negotiateResponse;
				// accelerate token negotiation by sending initial challenge
				// in the negotiation response
				if (this._enclosing.enabledAuthMethods.contains(org.apache.hadoop.security.SaslRpcServer.AuthMethod
					.TOKEN))
				{
					this.saslServer = this.createSaslServer(org.apache.hadoop.security.SaslRpcServer.AuthMethod
						.TOKEN);
					byte[] challenge = this.saslServer.evaluateResponse(new byte[0]);
					org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto.Builder negotiateBuilder
						 = org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto.newBuilder(this._enclosing
						.negotiateResponse);
					negotiateBuilder.getAuthsBuilder(0).setChallenge(com.google.protobuf.ByteString.copyFrom
						(challenge));
					// TOKEN is always first
					negotiateMessage = ((org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto)
						negotiateBuilder.build());
				}
				this.sentNegotiate = true;
				return negotiateMessage;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			private javax.security.sasl.SaslServer createSaslServer(org.apache.hadoop.security.SaslRpcServer.AuthMethod
				 authMethod)
			{
				System.Collections.Generic.IDictionary<string, object> saslProps = this._enclosing
					.saslPropsResolver.getServerProperties(this.addr);
				return new org.apache.hadoop.security.SaslRpcServer(authMethod).create(this, saslProps
					, this._enclosing.secretManager);
			}

			/// <summary>
			/// Try to set up the response to indicate that the client version
			/// is incompatible with the server.
			/// </summary>
			/// <remarks>
			/// Try to set up the response to indicate that the client version
			/// is incompatible with the server. This can contain special-case
			/// code to speak enough of past IPC protocols to pass back
			/// an exception to the caller.
			/// </remarks>
			/// <param name="clientVersion">the version the caller is using</param>
			/// <exception cref="System.IO.IOException"/>
			private void setupBadVersionResponse(int clientVersion)
			{
				string errMsg = "Server IPC version " + org.apache.hadoop.ipc.RpcConstants.CURRENT_VERSION
					 + " cannot communicate with client version " + clientVersion;
				java.io.ByteArrayOutputStream buffer = new java.io.ByteArrayOutputStream();
				if (clientVersion >= 9)
				{
					// Versions >>9  understand the normal response
					org.apache.hadoop.ipc.Server.Call fakeCall = new org.apache.hadoop.ipc.Server.Call
						(-1, org.apache.hadoop.ipc.RpcConstants.INVALID_RETRY_COUNT, null, this);
					this._enclosing.setupResponse(buffer, fakeCall, org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto
						.FATAL, org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto
						.FATAL_VERSION_MISMATCH, null, Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ipc.RPC.VersionMismatch
						)).getName(), errMsg);
					this._enclosing.responder.doRespond(fakeCall);
				}
				else
				{
					if (clientVersion >= 3)
					{
						org.apache.hadoop.ipc.Server.Call fakeCall = new org.apache.hadoop.ipc.Server.Call
							(-1, org.apache.hadoop.ipc.RpcConstants.INVALID_RETRY_COUNT, null, this);
						// Versions 3 to 8 use older response
						this._enclosing.setupResponseOldVersionFatal(buffer, fakeCall, null, Sharpen.Runtime.getClassForType
							(typeof(org.apache.hadoop.ipc.RPC.VersionMismatch)).getName(), errMsg);
						this._enclosing.responder.doRespond(fakeCall);
					}
					else
					{
						if (clientVersion == 2)
						{
							// Hadoop 0.18.3
							org.apache.hadoop.ipc.Server.Call fakeCall = new org.apache.hadoop.ipc.Server.Call
								(0, org.apache.hadoop.ipc.RpcConstants.INVALID_RETRY_COUNT, null, this);
							java.io.DataOutputStream @out = new java.io.DataOutputStream(buffer);
							@out.writeInt(0);
							// call ID
							@out.writeBoolean(true);
							// error
							org.apache.hadoop.io.WritableUtils.writeString(@out, Sharpen.Runtime.getClassForType
								(typeof(org.apache.hadoop.ipc.RPC.VersionMismatch)).getName());
							org.apache.hadoop.io.WritableUtils.writeString(@out, errMsg);
							fakeCall.setResponse(java.nio.ByteBuffer.wrap(buffer.toByteArray()));
							this._enclosing.responder.doRespond(fakeCall);
						}
					}
				}
			}

			/// <exception cref="System.IO.IOException"/>
			private void setupHttpRequestOnIpcPortResponse()
			{
				org.apache.hadoop.ipc.Server.Call fakeCall = new org.apache.hadoop.ipc.Server.Call
					(0, org.apache.hadoop.ipc.RpcConstants.INVALID_RETRY_COUNT, null, this);
				fakeCall.setResponse(java.nio.ByteBuffer.wrap(Sharpen.Runtime.getBytesForString(org.apache.hadoop.ipc.Server
					.RECEIVED_HTTP_REQ_RESPONSE, org.apache.commons.io.Charsets.UTF_8)));
				this._enclosing.responder.doRespond(fakeCall);
			}

			/// <summary>Reads the connection context following the connection header</summary>
			/// <param name="dis">- DataInputStream from which to read the header</param>
			/// <exception cref="WrappedRpcServerException">
			/// - if the header cannot be
			/// deserialized, or the user is not authorized
			/// </exception>
			/// <exception cref="org.apache.hadoop.ipc.Server.WrappedRpcServerException"/>
			private void processConnectionContext(java.io.DataInputStream dis)
			{
				// allow only one connection context during a session
				if (this.connectionContextRead)
				{
					throw new org.apache.hadoop.ipc.Server.WrappedRpcServerException(org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto
						.FATAL_INVALID_RPC_HEADER, "Connection context already processed");
				}
				this.connectionContext = this.decodeProtobufFromStream(org.apache.hadoop.ipc.protobuf.IpcConnectionContextProtos.IpcConnectionContextProto
					.newBuilder(), dis);
				this.protocolName = this.connectionContext.hasProtocol() ? this.connectionContext
					.getProtocol() : null;
				org.apache.hadoop.security.UserGroupInformation protocolUser = org.apache.hadoop.util.ProtoUtil
					.getUgi(this.connectionContext);
				if (this.saslServer == null)
				{
					this.user = protocolUser;
				}
				else
				{
					// user is authenticated
					this.user.setAuthenticationMethod(this.authMethod);
					//Now we check if this is a proxy user case. If the protocol user is
					//different from the 'user', it is a proxy user scenario. However, 
					//this is not allowed if user authenticated with DIGEST.
					if ((protocolUser != null) && (!protocolUser.getUserName().Equals(this.user.getUserName
						())))
					{
						if (this.authMethod == org.apache.hadoop.security.SaslRpcServer.AuthMethod.TOKEN)
						{
							// Not allowed to doAs if token authentication is used
							throw new org.apache.hadoop.ipc.Server.WrappedRpcServerException(org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto
								.FATAL_UNAUTHORIZED, new org.apache.hadoop.security.AccessControlException("Authenticated user ("
								 + this.user + ") doesn't match what the client claims to be (" + protocolUser +
								 ")"));
						}
						else
						{
							// Effective user can be different from authenticated user
							// for simple auth or kerberos auth
							// The user is the real user. Now we create a proxy user
							org.apache.hadoop.security.UserGroupInformation realUser = this.user;
							this.user = org.apache.hadoop.security.UserGroupInformation.createProxyUser(protocolUser
								.getUserName(), realUser);
						}
					}
				}
				this.authorizeConnection();
				// don't set until after authz because connection isn't established
				this.connectionContextRead = true;
			}

			/// <summary>
			/// Process a wrapped RPC Request - unwrap the SASL packet and process
			/// each embedded RPC request
			/// </summary>
			/// <param name="buf">- SASL wrapped request of one or more RPCs</param>
			/// <exception cref="System.IO.IOException">- SASL packet cannot be unwrapped</exception>
			/// <exception cref="System.Exception"/>
			/// <exception cref="org.apache.hadoop.ipc.Server.WrappedRpcServerException"/>
			private void unwrapPacketAndProcessRpcs(byte[] inBuf)
			{
				if (org.apache.hadoop.ipc.Server.LOG.isDebugEnabled())
				{
					org.apache.hadoop.ipc.Server.LOG.debug("Have read input token of size " + inBuf.Length
						 + " for processing by saslServer.unwrap()");
				}
				inBuf = this.saslServer.unwrap(inBuf, 0, inBuf.Length);
				java.nio.channels.ReadableByteChannel ch = java.nio.channels.Channels.newChannel(
					new java.io.ByteArrayInputStream(inBuf));
				// Read all RPCs contained in the inBuf, even partial ones
				while (true)
				{
					int count = -1;
					if (this.unwrappedDataLengthBuffer.remaining() > 0)
					{
						count = this._enclosing.channelRead(ch, this.unwrappedDataLengthBuffer);
						if (count <= 0 || this.unwrappedDataLengthBuffer.remaining() > 0)
						{
							return;
						}
					}
					if (this.unwrappedData == null)
					{
						this.unwrappedDataLengthBuffer.flip();
						int unwrappedDataLength = this.unwrappedDataLengthBuffer.getInt();
						this.unwrappedData = java.nio.ByteBuffer.allocate(unwrappedDataLength);
					}
					count = this._enclosing.channelRead(ch, this.unwrappedData);
					if (count <= 0 || this.unwrappedData.remaining() > 0)
					{
						return;
					}
					if (this.unwrappedData.remaining() == 0)
					{
						this.unwrappedDataLengthBuffer.clear();
						this.unwrappedData.flip();
						this.processOneRpc(((byte[])this.unwrappedData.array()));
						this.unwrappedData = null;
					}
				}
			}

			/// <summary>
			/// Process an RPC Request - handle connection setup and decoding of
			/// request into a Call
			/// </summary>
			/// <param name="buf">- contains the RPC request header and the rpc request</param>
			/// <exception cref="System.IO.IOException">
			/// - internal error that should not be returned to
			/// client, typically failure to respond to client
			/// </exception>
			/// <exception cref="WrappedRpcServerException">
			/// - an exception to be sent back to
			/// the client that does not require verbose logging by the
			/// Listener thread
			/// </exception>
			/// <exception cref="System.Exception"/>
			/// <exception cref="org.apache.hadoop.ipc.Server.WrappedRpcServerException"/>
			private void processOneRpc(byte[] buf)
			{
				int callId = -1;
				int retry = org.apache.hadoop.ipc.RpcConstants.INVALID_RETRY_COUNT;
				try
				{
					java.io.DataInputStream dis = new java.io.DataInputStream(new java.io.ByteArrayInputStream
						(buf));
					org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcRequestHeaderProto header = this
						.decodeProtobufFromStream(org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcRequestHeaderProto
						.newBuilder(), dis);
					callId = header.getCallId();
					retry = header.getRetryCount();
					if (org.apache.hadoop.ipc.Server.LOG.isDebugEnabled())
					{
						org.apache.hadoop.ipc.Server.LOG.debug(" got #" + callId);
					}
					this.checkRpcHeaders(header);
					if (callId < 0)
					{
						// callIds typically used during connection setup
						this.processRpcOutOfBandRequest(header, dis);
					}
					else
					{
						if (!this.connectionContextRead)
						{
							throw new org.apache.hadoop.ipc.Server.WrappedRpcServerException(org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto
								.FATAL_INVALID_RPC_HEADER, "Connection context not established");
						}
						else
						{
							this.processRpcRequest(header, dis);
						}
					}
				}
				catch (org.apache.hadoop.ipc.Server.WrappedRpcServerException wrse)
				{
					// inform client of error
					System.Exception ioe = wrse.InnerException;
					org.apache.hadoop.ipc.Server.Call call = new org.apache.hadoop.ipc.Server.Call(callId
						, retry, null, this);
					this._enclosing.setupResponse(this.authFailedResponse, call, org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto
						.FATAL, wrse.getRpcErrorCodeProto(), null, Sharpen.Runtime.getClassForObject(ioe
						).getName(), ioe.Message);
					this._enclosing.responder.doRespond(call);
					throw;
				}
			}

			/// <summary>Verify RPC header is valid</summary>
			/// <param name="header">- RPC request header</param>
			/// <exception cref="WrappedRpcServerException">- header contains invalid values</exception>
			/// <exception cref="org.apache.hadoop.ipc.Server.WrappedRpcServerException"/>
			private void checkRpcHeaders(org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcRequestHeaderProto
				 header)
			{
				if (!header.hasRpcOp())
				{
					string err = " IPC Server: No rpc op in rpcRequestHeader";
					throw new org.apache.hadoop.ipc.Server.WrappedRpcServerException(org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto
						.FATAL_INVALID_RPC_HEADER, err);
				}
				if (header.getRpcOp() != org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcRequestHeaderProto.OperationProto
					.RPC_FINAL_PACKET)
				{
					string err = "IPC Server does not implement rpc header operation" + header.getRpcOp
						();
					throw new org.apache.hadoop.ipc.Server.WrappedRpcServerException(org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto
						.FATAL_INVALID_RPC_HEADER, err);
				}
				// If we know the rpc kind, get its class so that we can deserialize
				// (Note it would make more sense to have the handler deserialize but 
				// we continue with this original design.
				if (!header.hasRpcKind())
				{
					string err = " IPC Server: No rpc kind in rpcRequestHeader";
					throw new org.apache.hadoop.ipc.Server.WrappedRpcServerException(org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto
						.FATAL_INVALID_RPC_HEADER, err);
				}
			}

			/// <summary>
			/// Process an RPC Request - the connection headers and context must
			/// have been already read
			/// </summary>
			/// <param name="header">- RPC request header</param>
			/// <param name="dis">- stream to request payload</param>
			/// <exception cref="WrappedRpcServerException">
			/// - due to fatal rpc layer issues such
			/// as invalid header or deserialization error. In this case a RPC fatal
			/// status response will later be sent back to client.
			/// </exception>
			/// <exception cref="System.Exception"/>
			/// <exception cref="org.apache.hadoop.ipc.Server.WrappedRpcServerException"/>
			private void processRpcRequest(org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcRequestHeaderProto
				 header, java.io.DataInputStream dis)
			{
				java.lang.Class rpcRequestClass = this._enclosing.getRpcRequestWrapper(header.getRpcKind
					());
				if (rpcRequestClass == null)
				{
					org.apache.hadoop.ipc.Server.LOG.warn("Unknown rpc kind " + header.getRpcKind() +
						 " from client " + this.getHostAddress());
					string err = "Unknown rpc kind in rpc header" + header.getRpcKind();
					throw new org.apache.hadoop.ipc.Server.WrappedRpcServerException(org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto
						.FATAL_INVALID_RPC_HEADER, err);
				}
				org.apache.hadoop.io.Writable rpcRequest;
				try
				{
					//Read the rpc request
					rpcRequest = org.apache.hadoop.util.ReflectionUtils.newInstance(rpcRequestClass, 
						this._enclosing.conf);
					rpcRequest.readFields(dis);
				}
				catch (System.Exception t)
				{
					// includes runtime exception from newInstance
					org.apache.hadoop.ipc.Server.LOG.warn("Unable to read call parameters for client "
						 + this.getHostAddress() + "on connection protocol " + this.protocolName + " for rpcKind "
						 + header.getRpcKind(), t);
					string err = "IPC server unable to read call parameters: " + t.Message;
					throw new org.apache.hadoop.ipc.Server.WrappedRpcServerException(org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto
						.FATAL_DESERIALIZING_REQUEST, err);
				}
				org.apache.htrace.Span traceSpan = null;
				if (header.hasTraceInfo())
				{
					// If the incoming RPC included tracing info, always continue the trace
					org.apache.htrace.TraceInfo parentSpan = new org.apache.htrace.TraceInfo(header.getTraceInfo
						().getTraceId(), header.getTraceInfo().getParentId());
					traceSpan = org.apache.htrace.Trace.startSpan(rpcRequest.ToString(), parentSpan).
						detach();
				}
				org.apache.hadoop.ipc.Server.Call call = new org.apache.hadoop.ipc.Server.Call(header
					.getCallId(), header.getRetryCount(), rpcRequest, this, org.apache.hadoop.util.ProtoUtil
					.convert(header.getRpcKind()), header.getClientId().toByteArray(), traceSpan);
				this._enclosing.callQueue.put(call);
				// queue the call; maybe blocked here
				this.incRpcCount();
			}

			// Increment the rpc count
			/// <summary>
			/// Establish RPC connection setup by negotiating SASL if required, then
			/// reading and authorizing the connection header
			/// </summary>
			/// <param name="header">- RPC header</param>
			/// <param name="dis">- stream to request payload</param>
			/// <exception cref="WrappedRpcServerException">
			/// - setup failed due to SASL
			/// negotiation failure, premature or invalid connection context,
			/// or other state errors
			/// </exception>
			/// <exception cref="System.IO.IOException">- failed to send a response back to the client
			/// 	</exception>
			/// <exception cref="System.Exception"/>
			/// <exception cref="org.apache.hadoop.ipc.Server.WrappedRpcServerException"/>
			private void processRpcOutOfBandRequest(org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcRequestHeaderProto
				 header, java.io.DataInputStream dis)
			{
				int callId = header.getCallId();
				if (callId == org.apache.hadoop.ipc.RpcConstants.CONNECTION_CONTEXT_CALL_ID)
				{
					// SASL must be established prior to connection context
					if (this.authProtocol == org.apache.hadoop.ipc.Server.AuthProtocol.SASL && !this.
						saslContextEstablished)
					{
						throw new org.apache.hadoop.ipc.Server.WrappedRpcServerException(org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto
							.FATAL_INVALID_RPC_HEADER, "Connection header sent during SASL negotiation");
					}
					// read and authorize the user
					this.processConnectionContext(dis);
				}
				else
				{
					if (callId == org.apache.hadoop.ipc.Server.AuthProtocol.SASL.callId)
					{
						// if client was switched to simple, ignore first SASL message
						if (this.authProtocol != org.apache.hadoop.ipc.Server.AuthProtocol.SASL)
						{
							throw new org.apache.hadoop.ipc.Server.WrappedRpcServerException(org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto
								.FATAL_INVALID_RPC_HEADER, "SASL protocol not requested by client");
						}
						this.saslReadAndProcess(dis);
					}
					else
					{
						if (callId == org.apache.hadoop.ipc.RpcConstants.PING_CALL_ID)
						{
							org.apache.hadoop.ipc.Server.LOG.debug("Received ping message");
						}
						else
						{
							throw new org.apache.hadoop.ipc.Server.WrappedRpcServerException(org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto
								.FATAL_INVALID_RPC_HEADER, "Unknown out of band call #" + callId);
						}
					}
				}
			}

			/// <summary>Authorize proxy users to access this server</summary>
			/// <exception cref="WrappedRpcServerException">- user is not allowed to proxy</exception>
			/// <exception cref="org.apache.hadoop.ipc.Server.WrappedRpcServerException"/>
			private void authorizeConnection()
			{
				try
				{
					// If auth method is TOKEN, the token was obtained by the
					// real user for the effective user, therefore not required to
					// authorize real user. doAs is allowed only for simple or kerberos
					// authentication
					if (this.user != null && this.user.getRealUser() != null && (this.authMethod != org.apache.hadoop.security.SaslRpcServer.AuthMethod
						.TOKEN))
					{
						org.apache.hadoop.security.authorize.ProxyUsers.authorize(this.user, this.getHostAddress
							());
					}
					this._enclosing.authorize(this.user, this.protocolName, this.getHostInetAddress()
						);
					if (org.apache.hadoop.ipc.Server.LOG.isDebugEnabled())
					{
						org.apache.hadoop.ipc.Server.LOG.debug("Successfully authorized " + this.connectionContext
							);
					}
					this._enclosing.rpcMetrics.incrAuthorizationSuccesses();
				}
				catch (org.apache.hadoop.security.authorize.AuthorizationException ae)
				{
					org.apache.hadoop.ipc.Server.LOG.info("Connection from " + this + " for protocol "
						 + this.connectionContext.getProtocol() + " is unauthorized for user " + this.user
						);
					this._enclosing.rpcMetrics.incrAuthorizationFailures();
					throw new org.apache.hadoop.ipc.Server.WrappedRpcServerException(org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto
						.FATAL_UNAUTHORIZED, ae);
				}
			}

			/// <summary>Decode the a protobuf from the given input stream</summary>
			/// <param name="builder">- Builder of the protobuf to decode</param>
			/// <param name="dis">- DataInputStream to read the protobuf</param>
			/// <returns>Message - decoded protobuf</returns>
			/// <exception cref="WrappedRpcServerException">- deserialization failed</exception>
			/// <exception cref="org.apache.hadoop.ipc.Server.WrappedRpcServerException"/>
			private T decodeProtobufFromStream<T>(com.google.protobuf.Message.Builder builder
				, java.io.DataInputStream dis)
				where T : com.google.protobuf.Message
			{
				try
				{
					builder.mergeDelimitedFrom(dis);
					return (T)builder.build();
				}
				catch (System.Exception ioe)
				{
					java.lang.Class protoClass = Sharpen.Runtime.getClassForObject(builder.getDefaultInstanceForType
						());
					throw new org.apache.hadoop.ipc.Server.WrappedRpcServerException(org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto
						.FATAL_DESERIALIZING_REQUEST, "Error decoding " + protoClass.getSimpleName() + ": "
						 + ioe);
				}
			}

			/// <summary>Get service class for connection</summary>
			/// <returns>the serviceClass</returns>
			public virtual int getServiceClass()
			{
				return this.serviceClass;
			}

			/// <summary>Set service class for connection</summary>
			/// <param name="serviceClass">the serviceClass to set</param>
			public virtual void setServiceClass(int serviceClass)
			{
				this.serviceClass = serviceClass;
			}

			private void close()
			{
				lock (this)
				{
					this.disposeSasl();
					this.data = null;
					this.dataLengthBuffer = null;
					if (!this.channel.isOpen())
					{
						return;
					}
					try
					{
						this.socket.shutdownOutput();
					}
					catch (System.Exception e)
					{
						org.apache.hadoop.ipc.Server.LOG.debug("Ignoring socket shutdown exception", e);
					}
					if (this.channel.isOpen())
					{
						org.apache.hadoop.io.IOUtils.cleanup(null, this.channel);
					}
					org.apache.hadoop.io.IOUtils.cleanup(null, this.socket);
				}
			}

			private readonly Server _enclosing;
		}

		/// <summary>Handles queued calls .</summary>
		private class Handler : java.lang.Thread
		{
			public Handler(Server _enclosing, int instanceNumber)
			{
				this._enclosing = _enclosing;
				this.setDaemon(true);
				this.setName("IPC Server handler " + instanceNumber + " on " + this._enclosing.port
					);
			}

			public override void run()
			{
				org.apache.hadoop.ipc.Server.LOG.debug(java.lang.Thread.currentThread().getName()
					 + ": starting");
				org.apache.hadoop.ipc.Server.SERVER.set(this._enclosing);
				java.io.ByteArrayOutputStream buf = new java.io.ByteArrayOutputStream(org.apache.hadoop.ipc.Server
					.INITIAL_RESP_BUF_SIZE);
				while (this._enclosing.running)
				{
					org.apache.htrace.TraceScope traceScope = null;
					try
					{
						org.apache.hadoop.ipc.Server.Call call = this._enclosing.callQueue.take();
						// pop the queue; maybe blocked here
						if (org.apache.hadoop.ipc.Server.LOG.isDebugEnabled())
						{
							org.apache.hadoop.ipc.Server.LOG.debug(java.lang.Thread.currentThread().getName()
								 + ": " + call + " for RpcKind " + call.rpcKind);
						}
						if (!call.connection.channel.isOpen())
						{
							org.apache.hadoop.ipc.Server.LOG.info(java.lang.Thread.currentThread().getName() 
								+ ": skipped " + call);
							continue;
						}
						string errorClass = null;
						string error = null;
						org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto
							 returnStatus = org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto
							.SUCCESS;
						org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto
							 detailedErr = null;
						org.apache.hadoop.io.Writable value = null;
						org.apache.hadoop.ipc.Server.CurCall.set(call);
						if (call.traceSpan != null)
						{
							traceScope = org.apache.htrace.Trace.continueSpan(call.traceSpan);
						}
						try
						{
							// Make the call as the user via Subject.doAs, thus associating
							// the call with the Subject
							if (call.connection.user == null)
							{
								value = this._enclosing.call(call.rpcKind, call.connection.protocolName, call.rpcRequest
									, call.timestamp);
							}
							else
							{
								value = call.connection.user.doAs(new _PrivilegedExceptionAction_2045(this, call)
									);
							}
						}
						catch (System.Exception e)
						{
							// make the call
							if (e is java.lang.reflect.UndeclaredThrowableException)
							{
								e = e.InnerException;
							}
							string logMsg = java.lang.Thread.currentThread().getName() + ", call " + call;
							if (this._enclosing.exceptionsHandler.isTerse(Sharpen.Runtime.getClassForObject(e
								)))
							{
								// Don't log the whole stack trace. Way too noisy!
								org.apache.hadoop.ipc.Server.LOG.info(logMsg + ": " + e);
							}
							else
							{
								if (e is System.Exception || e is System.Exception)
								{
									// These exception types indicate something is probably wrong
									// on the server side, as opposed to just a normal exceptional
									// result.
									org.apache.hadoop.ipc.Server.LOG.warn(logMsg, e);
								}
								else
								{
									org.apache.hadoop.ipc.Server.LOG.info(logMsg, e);
								}
							}
							if (e is org.apache.hadoop.ipc.RpcServerException)
							{
								org.apache.hadoop.ipc.RpcServerException rse = ((org.apache.hadoop.ipc.RpcServerException
									)e);
								returnStatus = rse.getRpcStatusProto();
								detailedErr = rse.getRpcErrorCodeProto();
							}
							else
							{
								returnStatus = org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto
									.ERROR;
								detailedErr = org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto
									.ERROR_APPLICATION;
							}
							errorClass = Sharpen.Runtime.getClassForObject(e).getName();
							error = org.apache.hadoop.util.StringUtils.stringifyException(e);
							// Remove redundant error class name from the beginning of the stack trace
							string exceptionHdr = errorClass + ": ";
							if (error.StartsWith(exceptionHdr))
							{
								error = Sharpen.Runtime.substring(error, exceptionHdr.Length);
							}
						}
						org.apache.hadoop.ipc.Server.CurCall.set(null);
						lock (call.connection.responseQueue)
						{
							// setupResponse() needs to be sync'ed together with 
							// responder.doResponse() since setupResponse may use
							// SASL to encrypt response data and SASL enforces
							// its own message ordering.
							this._enclosing.setupResponse(buf, call, returnStatus, detailedErr, value, errorClass
								, error);
							// Discard the large buf and reset it back to smaller size 
							// to free up heap
							if (buf.size() > this._enclosing.maxRespSize)
							{
								org.apache.hadoop.ipc.Server.LOG.warn("Large response size " + buf.size() + " for call "
									 + call.ToString());
								buf = new java.io.ByteArrayOutputStream(org.apache.hadoop.ipc.Server.INITIAL_RESP_BUF_SIZE
									);
							}
							this._enclosing.responder.doRespond(call);
						}
					}
					catch (System.Exception e)
					{
						if (this._enclosing.running)
						{
							// unexpected -- log it
							org.apache.hadoop.ipc.Server.LOG.info(java.lang.Thread.currentThread().getName() 
								+ " unexpectedly interrupted", e);
							if (org.apache.htrace.Trace.isTracing())
							{
								traceScope.getSpan().addTimelineAnnotation("unexpectedly interrupted: " + org.apache.hadoop.util.StringUtils
									.stringifyException(e));
							}
						}
					}
					catch (System.Exception e)
					{
						org.apache.hadoop.ipc.Server.LOG.info(java.lang.Thread.currentThread().getName() 
							+ " caught an exception", e);
						if (org.apache.htrace.Trace.isTracing())
						{
							traceScope.getSpan().addTimelineAnnotation("Exception: " + org.apache.hadoop.util.StringUtils
								.stringifyException(e));
						}
					}
					finally
					{
						if (traceScope != null)
						{
							traceScope.close();
						}
						org.apache.hadoop.io.IOUtils.cleanup(org.apache.hadoop.ipc.Server.LOG, traceScope
							);
					}
				}
				org.apache.hadoop.ipc.Server.LOG.debug(java.lang.Thread.currentThread().getName()
					 + ": exiting");
			}

			private sealed class _PrivilegedExceptionAction_2045 : java.security.PrivilegedExceptionAction
				<org.apache.hadoop.io.Writable>
			{
				public _PrivilegedExceptionAction_2045(Handler _enclosing, org.apache.hadoop.ipc.Server.Call
					 call)
				{
					this._enclosing = _enclosing;
					this.call = call;
				}

				/// <exception cref="System.Exception"/>
				public org.apache.hadoop.io.Writable run()
				{
					return this._enclosing._enclosing.call(call.rpcKind, call.connection.protocolName
						, call.rpcRequest, call.timestamp);
				}

				private readonly Handler _enclosing;

				private readonly org.apache.hadoop.ipc.Server.Call call;
			}

			private readonly Server _enclosing;
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal Server(string bindAddress, int port, java.lang.Class paramClass
			, int handlerCount, org.apache.hadoop.conf.Configuration conf)
			: this(bindAddress, port, paramClass, handlerCount, -1, -1, conf, int.toString(port
				), null, null)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal Server(string bindAddress, int port, java.lang.Class rpcRequestClass
			, int handlerCount, int numReaders, int queueSizePerHandler, org.apache.hadoop.conf.Configuration
			 conf, string serverName, org.apache.hadoop.security.token.SecretManager<org.apache.hadoop.security.token.TokenIdentifier
			> secretManager)
			: this(bindAddress, port, rpcRequestClass, handlerCount, numReaders, queueSizePerHandler
				, conf, serverName, secretManager, null)
		{
		}

		/// <summary>Constructs a server listening on the named port and address.</summary>
		/// <remarks>
		/// Constructs a server listening on the named port and address.  Parameters passed must
		/// be of the named class.  The <code>handlerCount</handlerCount> determines
		/// the number of handler threads that will be used to process calls.
		/// If queueSizePerHandler or numReaders are not -1 they will be used instead of parameters
		/// from configuration. Otherwise the configuration will be picked up.
		/// If rpcRequestClass is null then the rpcRequestClass must have been
		/// registered via
		/// <see cref="#registerProtocolEngine(RpcPayloadHeader.RpcKind,Class,RPC.RpcInvoker)
		/// 	"/>
		/// This parameter has been retained for compatibility with existing tests
		/// and usage.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		protected internal Server(string bindAddress, int port, java.lang.Class rpcRequestClass
			, int handlerCount, int numReaders, int queueSizePerHandler, org.apache.hadoop.conf.Configuration
			 conf, string serverName, org.apache.hadoop.security.token.SecretManager<org.apache.hadoop.security.token.TokenIdentifier
			> secretManager, string portRangeConfig)
		{
			this.bindAddress = bindAddress;
			this.conf = conf;
			this.portRangeConfig = portRangeConfig;
			this.port = port;
			this.rpcRequestClass = rpcRequestClass;
			this.handlerCount = handlerCount;
			this.socketSendBufferSize = 0;
			this.maxDataLength = conf.getInt(org.apache.hadoop.fs.CommonConfigurationKeys.IPC_MAXIMUM_DATA_LENGTH
				, org.apache.hadoop.fs.CommonConfigurationKeys.IPC_MAXIMUM_DATA_LENGTH_DEFAULT);
			if (queueSizePerHandler != -1)
			{
				this.maxQueueSize = queueSizePerHandler;
			}
			else
			{
				this.maxQueueSize = handlerCount * conf.getInt(org.apache.hadoop.fs.CommonConfigurationKeys
					.IPC_SERVER_HANDLER_QUEUE_SIZE_KEY, org.apache.hadoop.fs.CommonConfigurationKeys
					.IPC_SERVER_HANDLER_QUEUE_SIZE_DEFAULT);
			}
			this.maxRespSize = conf.getInt(org.apache.hadoop.fs.CommonConfigurationKeys.IPC_SERVER_RPC_MAX_RESPONSE_SIZE_KEY
				, org.apache.hadoop.fs.CommonConfigurationKeys.IPC_SERVER_RPC_MAX_RESPONSE_SIZE_DEFAULT
				);
			if (numReaders != -1)
			{
				this.readThreads = numReaders;
			}
			else
			{
				this.readThreads = conf.getInt(org.apache.hadoop.fs.CommonConfigurationKeys.IPC_SERVER_RPC_READ_THREADS_KEY
					, org.apache.hadoop.fs.CommonConfigurationKeys.IPC_SERVER_RPC_READ_THREADS_DEFAULT
					);
			}
			this.readerPendingConnectionQueue = conf.getInt(org.apache.hadoop.fs.CommonConfigurationKeys
				.IPC_SERVER_RPC_READ_CONNECTION_QUEUE_SIZE_KEY, org.apache.hadoop.fs.CommonConfigurationKeys
				.IPC_SERVER_RPC_READ_CONNECTION_QUEUE_SIZE_DEFAULT);
			// Setup appropriate callqueue
			string prefix = getQueueClassPrefix();
			this.callQueue = new org.apache.hadoop.ipc.CallQueueManager<org.apache.hadoop.ipc.Server.Call
				>(getQueueClass(prefix, conf), maxQueueSize, prefix, conf);
			this.secretManager = (org.apache.hadoop.security.token.SecretManager<org.apache.hadoop.security.token.TokenIdentifier
				>)secretManager;
			this.authorize = conf.getBoolean(org.apache.hadoop.fs.CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION
				, false);
			// configure supported authentications
			this.enabledAuthMethods = getAuthMethods(secretManager, conf);
			this.negotiateResponse = buildNegotiateResponse(enabledAuthMethods);
			// Start the listener here and let it bind to the port
			listener = new org.apache.hadoop.ipc.Server.Listener(this);
			this.port = listener.getAddress().getPort();
			connectionManager = new org.apache.hadoop.ipc.Server.ConnectionManager(this);
			this.rpcMetrics = org.apache.hadoop.ipc.metrics.RpcMetrics.create(this, conf);
			this.rpcDetailedMetrics = org.apache.hadoop.ipc.metrics.RpcDetailedMetrics.create
				(this.port);
			this.tcpNoDelay = conf.getBoolean(org.apache.hadoop.fs.CommonConfigurationKeysPublic
				.IPC_SERVER_TCPNODELAY_KEY, org.apache.hadoop.fs.CommonConfigurationKeysPublic.IPC_SERVER_TCPNODELAY_DEFAULT
				);
			// Create the responder here
			responder = new org.apache.hadoop.ipc.Server.Responder(this);
			if (secretManager != null || org.apache.hadoop.security.UserGroupInformation.isSecurityEnabled
				())
			{
				org.apache.hadoop.security.SaslRpcServer.init(conf);
				saslPropsResolver = org.apache.hadoop.security.SaslPropertiesResolver.getInstance
					(conf);
			}
			this.exceptionsHandler.addTerseExceptions(Sharpen.Runtime.getClassForType(typeof(
				org.apache.hadoop.ipc.StandbyException)));
		}

		/// <exception cref="System.IO.IOException"/>
		private org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto buildNegotiateResponse
			(System.Collections.Generic.IList<org.apache.hadoop.security.SaslRpcServer.AuthMethod
			> authMethods)
		{
			org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto.Builder negotiateBuilder
				 = org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto.newBuilder();
			if (authMethods.contains(org.apache.hadoop.security.SaslRpcServer.AuthMethod.SIMPLE
				) && authMethods.Count == 1)
			{
				// SIMPLE-only servers return success in response to negotiate
				negotiateBuilder.setState(org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto.SaslState
					.SUCCESS);
			}
			else
			{
				negotiateBuilder.setState(org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto.SaslState
					.NEGOTIATE);
				foreach (org.apache.hadoop.security.SaslRpcServer.AuthMethod authMethod in authMethods)
				{
					org.apache.hadoop.security.SaslRpcServer saslRpcServer = new org.apache.hadoop.security.SaslRpcServer
						(authMethod);
					org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto.SaslAuth.Builder builder
						 = negotiateBuilder.addAuthsBuilder().setMethod(authMethod.ToString()).setMechanism
						(saslRpcServer.mechanism);
					if (saslRpcServer.protocol != null)
					{
						builder.setProtocol(saslRpcServer.protocol);
					}
					if (saslRpcServer.serverId != null)
					{
						builder.setServerId(saslRpcServer.serverId);
					}
				}
			}
			return ((org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto)negotiateBuilder
				.build());
		}

		// get the security type from the conf. implicitly include token support
		// if a secret manager is provided, or fail if token is the conf value but
		// there is no secret manager
		private System.Collections.Generic.IList<org.apache.hadoop.security.SaslRpcServer.AuthMethod
			> getAuthMethods<_T0>(org.apache.hadoop.security.token.SecretManager<_T0> secretManager
			, org.apache.hadoop.conf.Configuration conf)
			where _T0 : org.apache.hadoop.security.token.TokenIdentifier
		{
			org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod confAuthenticationMethod
				 = org.apache.hadoop.security.SecurityUtil.getAuthenticationMethod(conf);
			System.Collections.Generic.IList<org.apache.hadoop.security.SaslRpcServer.AuthMethod
				> authMethods = new System.Collections.Generic.List<org.apache.hadoop.security.SaslRpcServer.AuthMethod
				>();
			if (confAuthenticationMethod == org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod
				.TOKEN)
			{
				if (secretManager == null)
				{
					throw new System.ArgumentException(org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod
						.TOKEN + " authentication requires a secret manager");
				}
			}
			else
			{
				if (secretManager != null)
				{
					LOG.debug(org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod.TOKEN
						 + " authentication enabled for secret manager");
					// most preferred, go to the front of the line!
					authMethods.add(org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod
						.TOKEN.getAuthMethod());
				}
			}
			authMethods.add(confAuthenticationMethod.getAuthMethod());
			LOG.debug("Server accepts auth methods:" + authMethods);
			return authMethods;
		}

		private void closeConnection(org.apache.hadoop.ipc.Server.Connection connection)
		{
			connectionManager.close(connection);
		}

		/// <summary>Setup response for the IPC Call.</summary>
		/// <param name="responseBuf">buffer to serialize the response into</param>
		/// <param name="call">
		/// 
		/// <see cref="Call"/>
		/// to which we are setting up the response
		/// </param>
		/// <param name="status">of the IPC call</param>
		/// <param name="rv">return value for the IPC Call, if the call was successful</param>
		/// <param name="errorClass">error class, if the the call failed</param>
		/// <param name="error">error message, if the call failed</param>
		/// <exception cref="System.IO.IOException"/>
		private void setupResponse(java.io.ByteArrayOutputStream responseBuf, org.apache.hadoop.ipc.Server.Call
			 call, org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto
			 status, org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto
			 erCode, org.apache.hadoop.io.Writable rv, string errorClass, string error)
		{
			responseBuf.reset();
			java.io.DataOutputStream @out = new java.io.DataOutputStream(responseBuf);
			org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.Builder headerBuilder
				 = org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.newBuilder
				();
			headerBuilder.setClientId(com.google.protobuf.ByteString.copyFrom(call.clientId));
			headerBuilder.setCallId(call.callId);
			headerBuilder.setRetryCount(call.retryCount);
			headerBuilder.setStatus(status);
			headerBuilder.setServerIpcVersionNum(org.apache.hadoop.ipc.RpcConstants.CURRENT_VERSION
				);
			if (status == org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto
				.SUCCESS)
			{
				org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto header = ((
					org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto)headerBuilder
					.build());
				int headerLen = header.getSerializedSize();
				int fullLength = com.google.protobuf.CodedOutputStream.computeRawVarint32Size(headerLen
					) + headerLen;
				try
				{
					if (rv is org.apache.hadoop.ipc.ProtobufRpcEngine.RpcWrapper)
					{
						org.apache.hadoop.ipc.ProtobufRpcEngine.RpcWrapper resWrapper = (org.apache.hadoop.ipc.ProtobufRpcEngine.RpcWrapper
							)rv;
						fullLength += resWrapper.getLength();
						@out.writeInt(fullLength);
						header.writeDelimitedTo(@out);
						rv.write(@out);
					}
					else
					{
						// Have to serialize to buffer to get len
						org.apache.hadoop.io.DataOutputBuffer buf = new org.apache.hadoop.io.DataOutputBuffer
							();
						rv.write(buf);
						byte[] data = buf.getData();
						fullLength += buf.getLength();
						@out.writeInt(fullLength);
						header.writeDelimitedTo(@out);
						@out.write(data, 0, buf.getLength());
					}
				}
				catch (System.Exception t)
				{
					LOG.warn("Error serializing call response for call " + call, t);
					// Call back to same function - this is OK since the
					// buffer is reset at the top, and since status is changed
					// to ERROR it won't infinite loop.
					setupResponse(responseBuf, call, org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto
						.ERROR, org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto
						.ERROR_SERIALIZING_RESPONSE, null, Sharpen.Runtime.getClassForObject(t).getName(
						), org.apache.hadoop.util.StringUtils.stringifyException(t));
					return;
				}
			}
			else
			{
				// Rpc Failure
				headerBuilder.setExceptionClassName(errorClass);
				headerBuilder.setErrorMsg(error);
				headerBuilder.setErrorDetail(erCode);
				org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto header = ((
					org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto)headerBuilder
					.build());
				int headerLen = header.getSerializedSize();
				int fullLength = com.google.protobuf.CodedOutputStream.computeRawVarint32Size(headerLen
					) + headerLen;
				@out.writeInt(fullLength);
				header.writeDelimitedTo(@out);
			}
			if (call.connection.useWrap)
			{
				wrapWithSasl(responseBuf, call);
			}
			call.setResponse(java.nio.ByteBuffer.wrap(responseBuf.toByteArray()));
		}

		/// <summary>
		/// Setup response for the IPC Call on Fatal Error from a
		/// client that is using old version of Hadoop.
		/// </summary>
		/// <remarks>
		/// Setup response for the IPC Call on Fatal Error from a
		/// client that is using old version of Hadoop.
		/// The response is serialized using the previous protocol's response
		/// layout.
		/// </remarks>
		/// <param name="response">buffer to serialize the response into</param>
		/// <param name="call">
		/// 
		/// <see cref="Call"/>
		/// to which we are setting up the response
		/// </param>
		/// <param name="rv">return value for the IPC Call, if the call was successful</param>
		/// <param name="errorClass">error class, if the the call failed</param>
		/// <param name="error">error message, if the call failed</param>
		/// <exception cref="System.IO.IOException"/>
		private void setupResponseOldVersionFatal(java.io.ByteArrayOutputStream response, 
			org.apache.hadoop.ipc.Server.Call call, org.apache.hadoop.io.Writable rv, string
			 errorClass, string error)
		{
			int OLD_VERSION_FATAL_STATUS = -1;
			response.reset();
			java.io.DataOutputStream @out = new java.io.DataOutputStream(response);
			@out.writeInt(call.callId);
			// write call id
			@out.writeInt(OLD_VERSION_FATAL_STATUS);
			// write FATAL_STATUS
			org.apache.hadoop.io.WritableUtils.writeString(@out, errorClass);
			org.apache.hadoop.io.WritableUtils.writeString(@out, error);
			if (call.connection.useWrap)
			{
				wrapWithSasl(response, call);
			}
			call.setResponse(java.nio.ByteBuffer.wrap(response.toByteArray()));
		}

		/// <exception cref="System.IO.IOException"/>
		private void wrapWithSasl(java.io.ByteArrayOutputStream response, org.apache.hadoop.ipc.Server.Call
			 call)
		{
			if (call.connection.saslServer != null)
			{
				byte[] token = response.toByteArray();
				// synchronization may be needed since there can be multiple Handler
				// threads using saslServer to wrap responses.
				lock (call.connection.saslServer)
				{
					token = call.connection.saslServer.wrap(token, 0, token.Length);
				}
				if (LOG.isDebugEnabled())
				{
					LOG.debug("Adding saslServer wrapped token of size " + token.Length + " as call response."
						);
				}
				response.reset();
				// rebuild with sasl header and payload
				org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto saslHeader = 
					((org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto)org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto
					.newBuilder().setCallId(org.apache.hadoop.ipc.Server.AuthProtocol.SASL.callId).setStatus
					(org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto
					.SUCCESS).build());
				org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto saslMessage = ((org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto
					)org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto.newBuilder().setState
					(org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto.SaslState.WRAP).setToken
					(com.google.protobuf.ByteString.copyFrom(token, 0, token.Length)).build());
				org.apache.hadoop.ipc.ProtobufRpcEngine.RpcResponseMessageWrapper saslResponse = 
					new org.apache.hadoop.ipc.ProtobufRpcEngine.RpcResponseMessageWrapper(saslHeader
					, saslMessage);
				java.io.DataOutputStream @out = new java.io.DataOutputStream(response);
				@out.writeInt(saslResponse.getLength());
				saslResponse.write(@out);
			}
		}

		internal virtual org.apache.hadoop.conf.Configuration getConf()
		{
			return conf;
		}

		/// <summary>Sets the socket buffer size used for responding to RPCs</summary>
		public virtual void setSocketSendBufSize(int size)
		{
			this.socketSendBufferSize = size;
		}

		/// <summary>Starts the service.</summary>
		/// <remarks>Starts the service.  Must be called before any calls will be handled.</remarks>
		public virtual void start()
		{
			lock (this)
			{
				responder.start();
				listener.start();
				handlers = new org.apache.hadoop.ipc.Server.Handler[handlerCount];
				for (int i = 0; i < handlerCount; i++)
				{
					handlers[i] = new org.apache.hadoop.ipc.Server.Handler(this, i);
					handlers[i].start();
				}
			}
		}

		/// <summary>Stops the service.</summary>
		/// <remarks>Stops the service.  No new calls will be handled after this is called.</remarks>
		public virtual void stop()
		{
			lock (this)
			{
				LOG.info("Stopping server on " + port);
				running = false;
				if (handlers != null)
				{
					for (int i = 0; i < handlerCount; i++)
					{
						if (handlers[i] != null)
						{
							handlers[i].interrupt();
						}
					}
				}
				listener.interrupt();
				listener.doStop();
				responder.interrupt();
				Sharpen.Runtime.notifyAll(this);
				this.rpcMetrics.shutdown();
				this.rpcDetailedMetrics.shutdown();
			}
		}

		/// <summary>Wait for the server to be stopped.</summary>
		/// <remarks>
		/// Wait for the server to be stopped.
		/// Does not wait for all subthreads to finish.
		/// See
		/// <see cref="stop()"/>
		/// .
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void join()
		{
			lock (this)
			{
				while (running)
				{
					Sharpen.Runtime.wait(this);
				}
			}
		}

		/// <summary>Return the socket (ip+port) on which the RPC server is listening to.</summary>
		/// <returns>the socket (ip+port) on which the RPC server is listening to.</returns>
		public virtual java.net.InetSocketAddress getListenerAddress()
		{
			lock (this)
			{
				return listener.getAddress();
			}
		}

		/// <summary>Called for each call.</summary>
		/// <exception cref="System.Exception"/>
		[System.ObsoleteAttribute(@"Use  #call(RpcPayloadHeader.RpcKind,String,Writable,long) instead"
			)]
		public virtual org.apache.hadoop.io.Writable call(org.apache.hadoop.io.Writable param
			, long receiveTime)
		{
			return call(org.apache.hadoop.ipc.RPC.RpcKind.RPC_BUILTIN, null, param, receiveTime
				);
		}

		/// <summary>Called for each call.</summary>
		/// <exception cref="System.Exception"/>
		public abstract org.apache.hadoop.io.Writable call(org.apache.hadoop.ipc.RPC.RpcKind
			 rpcKind, string protocol, org.apache.hadoop.io.Writable param, long receiveTime
			);

		/// <summary>Authorize the incoming client connection.</summary>
		/// <param name="user">client user</param>
		/// <param name="protocolName">- the protocol</param>
		/// <param name="addr">InetAddress of incoming connection</param>
		/// <exception cref="org.apache.hadoop.security.authorize.AuthorizationException">when the client isn't authorized to talk the protocol
		/// 	</exception>
		private void authorize(org.apache.hadoop.security.UserGroupInformation user, string
			 protocolName, java.net.InetAddress addr)
		{
			if (authorize)
			{
				if (protocolName == null)
				{
					throw new org.apache.hadoop.security.authorize.AuthorizationException("Null protocol not authorized"
						);
				}
				java.lang.Class protocol = null;
				try
				{
					protocol = getProtocolClass(protocolName, getConf());
				}
				catch (java.lang.ClassNotFoundException)
				{
					throw new org.apache.hadoop.security.authorize.AuthorizationException("Unknown protocol: "
						 + protocolName);
				}
				serviceAuthorizationManager.authorize(user, protocol, getConf(), addr);
			}
		}

		/// <summary>Get the port on which the IPC Server is listening for incoming connections.
		/// 	</summary>
		/// <remarks>
		/// Get the port on which the IPC Server is listening for incoming connections.
		/// This could be an ephemeral port too, in which case we return the real
		/// port on which the Server has bound.
		/// </remarks>
		/// <returns>port on which IPC Server is listening</returns>
		public virtual int getPort()
		{
			return port;
		}

		/// <summary>The number of open RPC conections</summary>
		/// <returns>the number of open rpc connections</returns>
		public virtual int getNumOpenConnections()
		{
			return connectionManager.size();
		}

		/// <summary>The number of rpc calls in the queue.</summary>
		/// <returns>The number of rpc calls in the queue.</returns>
		public virtual int getCallQueueLen()
		{
			return callQueue.size();
		}

		/// <summary>The maximum size of the rpc call queue of this server.</summary>
		/// <returns>The maximum size of the rpc call queue.</returns>
		public virtual int getMaxQueueSize()
		{
			return maxQueueSize;
		}

		/// <summary>The number of reader threads for this server.</summary>
		/// <returns>The number of reader threads.</returns>
		public virtual int getNumReaders()
		{
			return readThreads;
		}

		/// <summary>
		/// When the read or write buffer size is larger than this limit, i/o will be
		/// done in chunks of this size.
		/// </summary>
		/// <remarks>
		/// When the read or write buffer size is larger than this limit, i/o will be
		/// done in chunks of this size. Most RPC requests and responses would be
		/// be smaller.
		/// </remarks>
		private static int NIO_BUFFER_LIMIT = 8 * 1024;

		//should not be more than 64KB.
		/// <summary>
		/// This is a wrapper around
		/// <see cref="java.nio.channels.WritableByteChannel.write(java.nio.ByteBuffer)"/>
		/// .
		/// If the amount of data is large, it writes to channel in smaller chunks.
		/// This is to avoid jdk from creating many direct buffers as the size of
		/// buffer increases. This also minimizes extra copies in NIO layer
		/// as a result of multiple write operations required to write a large
		/// buffer.
		/// </summary>
		/// <seealso cref="java.nio.channels.WritableByteChannel.write(java.nio.ByteBuffer)"/
		/// 	>
		/// <exception cref="System.IO.IOException"/>
		private int channelWrite(java.nio.channels.WritableByteChannel channel, java.nio.ByteBuffer
			 buffer)
		{
			int count = (buffer.remaining() <= NIO_BUFFER_LIMIT) ? channel.write(buffer) : channelIO
				(null, channel, buffer);
			if (count > 0)
			{
				rpcMetrics.incrSentBytes(count);
			}
			return count;
		}

		/// <summary>
		/// This is a wrapper around
		/// <see cref="java.nio.channels.ReadableByteChannel.read(java.nio.ByteBuffer)"/>
		/// .
		/// If the amount of data is large, it writes to channel in smaller chunks.
		/// This is to avoid jdk from creating many direct buffers as the size of
		/// ByteBuffer increases. There should not be any performance degredation.
		/// </summary>
		/// <seealso cref="java.nio.channels.ReadableByteChannel.read(java.nio.ByteBuffer)"/>
		/// <exception cref="System.IO.IOException"/>
		private int channelRead(java.nio.channels.ReadableByteChannel channel, java.nio.ByteBuffer
			 buffer)
		{
			int count = (buffer.remaining() <= NIO_BUFFER_LIMIT) ? channel.read(buffer) : channelIO
				(channel, null, buffer);
			if (count > 0)
			{
				rpcMetrics.incrReceivedBytes(count);
			}
			return count;
		}

		/// <summary>
		/// Helper for
		/// <see cref="channelRead(java.nio.channels.ReadableByteChannel, java.nio.ByteBuffer)
		/// 	"/>
		/// and
		/// <see cref="channelWrite(java.nio.channels.WritableByteChannel, java.nio.ByteBuffer)
		/// 	"/>
		/// . Only
		/// one of readCh or writeCh should be non-null.
		/// </summary>
		/// <seealso cref="channelRead(java.nio.channels.ReadableByteChannel, java.nio.ByteBuffer)
		/// 	"/>
		/// <seealso cref="channelWrite(java.nio.channels.WritableByteChannel, java.nio.ByteBuffer)
		/// 	"/>
		/// <exception cref="System.IO.IOException"/>
		private static int channelIO(java.nio.channels.ReadableByteChannel readCh, java.nio.channels.WritableByteChannel
			 writeCh, java.nio.ByteBuffer buf)
		{
			int originalLimit = buf.limit();
			int initialRemaining = buf.remaining();
			int ret = 0;
			while (buf.remaining() > 0)
			{
				try
				{
					int ioSize = System.Math.min(buf.remaining(), NIO_BUFFER_LIMIT);
					buf.limit(buf.position() + ioSize);
					ret = (readCh == null) ? writeCh.write(buf) : readCh.read(buf);
					if (ret < ioSize)
					{
						break;
					}
				}
				finally
				{
					buf.limit(originalLimit);
				}
			}
			int nBytes = initialRemaining - buf.remaining();
			return (nBytes > 0) ? nBytes : ret;
		}

		private class ConnectionManager
		{
			private readonly java.util.concurrent.atomic.AtomicInteger count = new java.util.concurrent.atomic.AtomicInteger
				();

			private readonly System.Collections.Generic.ICollection<org.apache.hadoop.ipc.Server.Connection
				> connections;

			private readonly java.util.Timer idleScanTimer;

			private readonly int idleScanThreshold;

			private readonly int idleScanInterval;

			private readonly int maxIdleTime;

			private readonly int maxIdleToClose;

			private readonly int maxConnections;

			internal ConnectionManager(Server _enclosing)
			{
				this._enclosing = _enclosing;
				this.idleScanTimer = new java.util.Timer("IPC Server idle connection scanner for port "
					 + this._enclosing.getPort(), true);
				this.idleScanThreshold = this._enclosing.conf.getInt(org.apache.hadoop.fs.CommonConfigurationKeysPublic
					.IPC_CLIENT_IDLETHRESHOLD_KEY, org.apache.hadoop.fs.CommonConfigurationKeysPublic
					.IPC_CLIENT_IDLETHRESHOLD_DEFAULT);
				this.idleScanInterval = this._enclosing.conf.getInt(org.apache.hadoop.fs.CommonConfigurationKeys
					.IPC_CLIENT_CONNECTION_IDLESCANINTERVAL_KEY, org.apache.hadoop.fs.CommonConfigurationKeys
					.IPC_CLIENT_CONNECTION_IDLESCANINTERVAL_DEFAULT);
				this.maxIdleTime = 2 * this._enclosing.conf.getInt(org.apache.hadoop.fs.CommonConfigurationKeysPublic
					.IPC_CLIENT_CONNECTION_MAXIDLETIME_KEY, org.apache.hadoop.fs.CommonConfigurationKeysPublic
					.IPC_CLIENT_CONNECTION_MAXIDLETIME_DEFAULT);
				this.maxIdleToClose = this._enclosing.conf.getInt(org.apache.hadoop.fs.CommonConfigurationKeysPublic
					.IPC_CLIENT_KILL_MAX_KEY, org.apache.hadoop.fs.CommonConfigurationKeysPublic.IPC_CLIENT_KILL_MAX_DEFAULT
					);
				this.maxConnections = this._enclosing.conf.getInt(org.apache.hadoop.fs.CommonConfigurationKeysPublic
					.IPC_SERVER_MAX_CONNECTIONS_KEY, org.apache.hadoop.fs.CommonConfigurationKeysPublic
					.IPC_SERVER_MAX_CONNECTIONS_DEFAULT);
				// create a set with concurrency -and- a thread-safe iterator, add 2
				// for listener and idle closer threads
				this.connections = java.util.Collections.newSetFromMap(new java.util.concurrent.ConcurrentHashMap
					<org.apache.hadoop.ipc.Server.Connection, bool>(this._enclosing.maxQueueSize, 0.75f
					, this._enclosing.readThreads + 2));
			}

			private bool add(org.apache.hadoop.ipc.Server.Connection connection)
			{
				bool added = this.connections.add(connection);
				if (added)
				{
					this.count.getAndIncrement();
				}
				return added;
			}

			private bool remove(org.apache.hadoop.ipc.Server.Connection connection)
			{
				bool removed = this.connections.remove(connection);
				if (removed)
				{
					this.count.getAndDecrement();
				}
				return removed;
			}

			internal virtual int size()
			{
				return this.count.get();
			}

			internal virtual bool isFull()
			{
				// The check is disabled when maxConnections <= 0.
				return ((this.maxConnections > 0) && (this.size() >= this.maxConnections));
			}

			internal virtual org.apache.hadoop.ipc.Server.Connection[] toArray()
			{
				return Sharpen.Collections.ToArray(this.connections, new org.apache.hadoop.ipc.Server.Connection
					[0]);
			}

			internal virtual org.apache.hadoop.ipc.Server.Connection register(java.nio.channels.SocketChannel
				 channel)
			{
				if (this.isFull())
				{
					return null;
				}
				org.apache.hadoop.ipc.Server.Connection connection = new org.apache.hadoop.ipc.Server.Connection
					(this, channel, org.apache.hadoop.util.Time.now());
				this.add(connection);
				if (org.apache.hadoop.ipc.Server.LOG.isDebugEnabled())
				{
					org.apache.hadoop.ipc.Server.LOG.debug("Server connection from " + connection + "; # active connections: "
						 + this.size() + "; # queued calls: " + this._enclosing.callQueue.size());
				}
				return connection;
			}

			internal virtual bool close(org.apache.hadoop.ipc.Server.Connection connection)
			{
				bool exists = this.remove(connection);
				if (exists)
				{
					if (org.apache.hadoop.ipc.Server.LOG.isDebugEnabled())
					{
						org.apache.hadoop.ipc.Server.LOG.debug(java.lang.Thread.currentThread().getName()
							 + ": disconnecting client " + connection + ". Number of active connections: " +
							 this.size());
					}
					// only close if actually removed to avoid double-closing due
					// to possible races
					connection.close();
				}
				return exists;
			}

			// synch'ed to avoid explicit invocation upon OOM from colliding with
			// timer task firing
			internal virtual void closeIdle(bool scanAll)
			{
				lock (this)
				{
					long minLastContact = org.apache.hadoop.util.Time.now() - this.maxIdleTime;
					// concurrent iterator might miss new connections added
					// during the iteration, but that's ok because they won't
					// be idle yet anyway and will be caught on next scan
					int closed = 0;
					foreach (org.apache.hadoop.ipc.Server.Connection connection in this.connections)
					{
						// stop if connections dropped below threshold unless scanning all
						if (!scanAll && this.size() < this.idleScanThreshold)
						{
							break;
						}
						// stop if not scanning all and max connections are closed
						if (connection.isIdle() && connection.getLastContact() < minLastContact && this.close
							(connection) && !scanAll && (++closed == this.maxIdleToClose))
						{
							break;
						}
					}
				}
			}

			internal virtual void closeAll()
			{
				// use a copy of the connections to be absolutely sure the concurrent
				// iterator doesn't miss a connection
				foreach (org.apache.hadoop.ipc.Server.Connection connection in this.toArray())
				{
					this.close(connection);
				}
			}

			internal virtual void startIdleScan()
			{
				this.scheduleIdleScanTask();
			}

			internal virtual void stopIdleScan()
			{
				this.idleScanTimer.cancel();
			}

			private void scheduleIdleScanTask()
			{
				if (!this._enclosing.running)
				{
					return;
				}
				java.util.TimerTask idleScanTask = new _TimerTask_2784(this);
				// explicitly reschedule so next execution occurs relative
				// to the end of this scan, not the beginning
				this.idleScanTimer.schedule(idleScanTask, this.idleScanInterval);
			}

			private sealed class _TimerTask_2784 : java.util.TimerTask
			{
				public _TimerTask_2784(ConnectionManager _enclosing)
				{
					this._enclosing = _enclosing;
				}

				public override void run()
				{
					if (!this._enclosing._enclosing.running)
					{
						return;
					}
					if (org.apache.hadoop.ipc.Server.LOG.isDebugEnabled())
					{
						org.apache.hadoop.ipc.Server.LOG.debug(java.lang.Thread.currentThread().getName()
							 + ": task running");
					}
					try
					{
						this._enclosing.closeIdle(false);
					}
					finally
					{
						this._enclosing.scheduleIdleScanTask();
					}
				}

				private readonly ConnectionManager _enclosing;
			}

			private readonly Server _enclosing;
		}
	}
}
