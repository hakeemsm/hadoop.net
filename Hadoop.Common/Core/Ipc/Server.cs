using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using Com.Google.Common.Annotations;
using Com.Google.Protobuf;
using Hadoop.Common.Core.Conf;
using Hadoop.Common.Core.IO;
using Hadoop.Common.Core.Util;
using Javax.Security.Sasl;
using Org.Apache.Commons.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Ipc.Metrics;
using Org.Apache.Hadoop.Ipc.Protobuf;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authorize;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Util;
using Org.Apache.Htrace;
using Sharpen;
using Sharpen.Reflect;

namespace Org.Apache.Hadoop.Ipc
{
	/// <summary>An abstract IPC service.</summary>
	/// <remarks>
	/// An abstract IPC service.  IPC calls take a single
	/// <see cref="IWritable"/>
	/// as a
	/// parameter, and return a
	/// <see cref="IWritable"/>
	/// as their value.  A service runs on
	/// a port and is defined by a parameter class and a value class.
	/// </remarks>
	/// <seealso cref="Client"/>
	public abstract class Server
	{
		private readonly bool authorize;

		private IList<SaslRpcServer.AuthMethod> enabledAuthMethods;

		private RpcHeaderProtos.RpcSaslProto negotiateResponse;

		private Server.ExceptionsHandler exceptionsHandler = new Server.ExceptionsHandler
			();

		public virtual void AddTerseExceptions(params Type[] exceptionClass)
		{
			exceptionsHandler.AddTerseExceptions(exceptionClass);
		}

		/// <summary>
		/// ExceptionsHandler manages Exception groups for special handling
		/// e.g., terse exception group for concise logging messages
		/// </summary>
		internal class ExceptionsHandler
		{
			private volatile ICollection<string> terseExceptions = new HashSet<string>();

			/// <summary>Add exception class so server won't log its stack trace.</summary>
			/// <remarks>
			/// Add exception class so server won't log its stack trace.
			/// Modifying the terseException through this method is thread safe.
			/// </remarks>
			/// <param name="exceptionClass">exception classes</param>
			internal virtual void AddTerseExceptions(params Type[] exceptionClass)
			{
				// Make a copy of terseException for performing modification
				HashSet<string> newSet = new HashSet<string>(terseExceptions);
				// Add all class names into the HashSet
				foreach (Type name in exceptionClass)
				{
					newSet.AddItem(name.ToString());
				}
				// Replace terseException set
				terseExceptions = Sharpen.Collections.UnmodifiableSet(newSet);
			}

			internal virtual bool IsTerse(Type t)
			{
				return terseExceptions.Contains(t.ToString());
			}
		}

		/// <summary>
		/// If the user accidentally sends an HTTP GET to an IPC port, we detect this
		/// and send back a nicer response.
		/// </summary>
		private static readonly ByteBuffer HttpGetBytes = ByteBuffer.Wrap(Sharpen.Runtime.GetBytesForString
			("GET ", Charsets.Utf8));

		/// <summary>
		/// An HTTP response to send back if we detect an HTTP request to our IPC
		/// port.
		/// </summary>
		internal const string ReceivedHttpReqResponse = "HTTP/1.1 404 Not Found\r\n" + "Content-type: text/plain\r\n\r\n"
			 + "It looks like you are making an HTTP request to a Hadoop IPC port. " + "This is not the correct port for the web interface on this daemon.\r\n";

		/// <summary>Initial and max size of response buffer</summary>
		internal static int InitialRespBufSize = 10240;

		internal class RpcKindMapValue
		{
			internal readonly Type rpcRequestWrapperClass;

			internal readonly RPC.RpcInvoker rpcInvoker;

			internal RpcKindMapValue(Type rpcRequestWrapperClass, RPC.RpcInvoker rpcInvoker)
			{
				this.rpcInvoker = rpcInvoker;
				this.rpcRequestWrapperClass = rpcRequestWrapperClass;
			}
		}

		internal static IDictionary<RPC.RpcKind, Server.RpcKindMapValue> rpcKindMap = new 
			Dictionary<RPC.RpcKind, Server.RpcKindMapValue>(4);

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
		public static void RegisterProtocolEngine(RPC.RpcKind rpcKind, Type rpcRequestWrapperClass
			, RPC.RpcInvoker rpcInvoker)
		{
			Server.RpcKindMapValue old = rpcKindMap[rpcKind] = new Server.RpcKindMapValue(rpcRequestWrapperClass
				, rpcInvoker);
			if (old != null)
			{
				rpcKindMap[rpcKind] = old;
				throw new ArgumentException("ReRegistration of rpcKind: " + rpcKind);
			}
			Log.Debug("rpcKind=" + rpcKind + ", rpcRequestWrapperClass=" + rpcRequestWrapperClass
				 + ", rpcInvoker=" + rpcInvoker);
		}

		public virtual Type GetRpcRequestWrapper(RpcHeaderProtos.RpcKindProto rpcKind)
		{
			if (rpcRequestClass != null)
			{
				return rpcRequestClass;
			}
			Server.RpcKindMapValue val = rpcKindMap[ProtoUtil.Convert(rpcKind)];
			return (val == null) ? null : val.rpcRequestWrapperClass;
		}

		public static RPC.RpcInvoker GetRpcInvoker(RPC.RpcKind rpcKind)
		{
			Server.RpcKindMapValue val = rpcKindMap[rpcKind];
			return (val == null) ? null : val.rpcInvoker;
		}

		public static readonly Log Log = LogFactory.GetLog(typeof(Server));

		public static readonly Log Auditlog = LogFactory.GetLog("SecurityLogger." + typeof(
			Server).FullName);

		private const string AuthFailedFor = "Auth failed for ";

		private const string AuthSuccessfulFor = "Auth successful for ";

		private static readonly ThreadLocal<Server> Server = new ThreadLocal<Server>();

		private static readonly IDictionary<string, Type> ProtocolCache = new ConcurrentHashMap
			<string, Type>();

		/// <exception cref="System.TypeLoadException"/>
		internal static Type GetProtocolClass(string protocolName, Configuration conf)
		{
			Type protocol = ProtocolCache[protocolName];
			if (protocol == null)
			{
				protocol = conf.GetClassByName(protocolName);
				ProtocolCache[protocolName] = protocol;
			}
			return protocol;
		}

		/// <summary>Returns the server instance called under or null.</summary>
		/// <remarks>
		/// Returns the server instance called under or null.  May be called under
		/// <see cref="Call(IWritable, long)"/>
		/// implementations, and under
		/// <see cref="IWritable"/>
		/// methods of paramters and return values.  Permits applications to access
		/// the server context.
		/// </remarks>
		public static Server Get()
		{
			return Server.Get();
		}

		/// <summary>
		/// This is set to Call object before Handler invokes an RPC and reset
		/// after the call returns.
		/// </summary>
		private static readonly ThreadLocal<Server.Call> CurCall = new ThreadLocal<Server.Call
			>();

		/// <summary>Get the current call</summary>
		[VisibleForTesting]
		public static ThreadLocal<Server.Call> GetCurCall()
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
		public static int GetCallId()
		{
			Server.Call call = CurCall.Get();
			return call != null ? call.callId : RpcConstants.InvalidCallId;
		}

		/// <returns>
		/// The current active RPC call's retry count. -1 indicates the retry
		/// cache is not supported in the client side.
		/// </returns>
		public static int GetCallRetryCount()
		{
			Server.Call call = CurCall.Get();
			return call != null ? call.retryCount : RpcConstants.InvalidRetryCount;
		}

		/// <summary>
		/// Returns the remote side ip address when invoked inside an RPC
		/// Returns null incase of an error.
		/// </summary>
		public static IPAddress GetRemoteIp()
		{
			Server.Call call = CurCall.Get();
			return (call != null && call.connection != null) ? call.connection.GetHostInetAddress
				() : null;
		}

		/// <summary>Returns the clientId from the current RPC request</summary>
		public static byte[] GetClientId()
		{
			Server.Call call = CurCall.Get();
			return call != null ? call.clientId : RpcConstants.DummyClientId;
		}

		/// <summary>Returns remote address as a string when invoked inside an RPC.</summary>
		/// <remarks>
		/// Returns remote address as a string when invoked inside an RPC.
		/// Returns null in case of an error.
		/// </remarks>
		public static string GetRemoteAddress()
		{
			IPAddress addr = GetRemoteIp();
			return (addr == null) ? null : addr.GetHostAddress();
		}

		/// <summary>Returns the RPC remote user when invoked inside an RPC.</summary>
		/// <remarks>
		/// Returns the RPC remote user when invoked inside an RPC.  Note this
		/// may be different than the current user if called within another doAs
		/// </remarks>
		/// <returns>connection's UGI or null if not an RPC</returns>
		public static UserGroupInformation GetRemoteUser()
		{
			Server.Call call = CurCall.Get();
			return (call != null && call.connection != null) ? call.connection.user : null;
		}

		/// <summary>Return true if the invocation was through an RPC.</summary>
		public static bool IsRpcInvocation()
		{
			return CurCall.Get() != null;
		}

		private string bindAddress;

		private int port;

		private int handlerCount;

		private int readThreads;

		private int readerPendingConnectionQueue;

		private Type rpcRequestClass;

		protected internal readonly RpcMetrics rpcMetrics;

		protected internal readonly RpcDetailedMetrics rpcDetailedMetrics;

		private Configuration conf;

		private string portRangeConfig = null;

		private SecretManager<TokenIdentifier> secretManager;

		private SaslPropertiesResolver saslPropsResolver;

		private ServiceAuthorizationManager serviceAuthorizationManager = new ServiceAuthorizationManager
			();

		private int maxQueueSize;

		private readonly int maxRespSize;

		private int socketSendBufferSize;

		private readonly int maxDataLength;

		private readonly bool tcpNoDelay;

		private volatile bool running = true;

		private CallQueueManager<Server.Call> callQueue;

		private Server.ConnectionManager connectionManager;

		private Server.Listener listener = null;

		private Server.Responder responder = null;

		private Server.Handler[] handlers = null;

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
		/// <exception cref="Sharpen.BindException">if the address can't be bound</exception>
		/// <exception cref="Sharpen.UnknownHostException">if the address isn't a valid host name
		/// 	</exception>
		/// <exception cref="System.IO.IOException">other random errors from bind</exception>
		public static void Bind(Socket socket, IPEndPoint address, int backlog)
		{
			Bind(socket, address, backlog, null, null);
		}

		/// <exception cref="System.IO.IOException"/>
		public static void Bind(Socket socket, IPEndPoint address, int backlog, Configuration
			 conf, string rangeConf)
		{
			try
			{
				Configuration.IntegerRanges range = null;
				if (rangeConf != null)
				{
					range = conf.GetRange(rangeConf, string.Empty);
				}
				if (range == null || range.IsEmpty() || (address.Port != 0))
				{
					socket.Bind(address, backlog);
				}
				else
				{
					foreach (int port in range)
					{
						if (socket.IsBound())
						{
							break;
						}
						try
						{
							IPEndPoint temp = new IPEndPoint(address.Address, port);
							socket.Bind(temp, backlog);
						}
						catch (BindException)
						{
						}
					}
					//Ignored
					if (!socket.IsBound())
					{
						throw new BindException("Could not find a free port in " + range);
					}
				}
			}
			catch (SocketException e)
			{
				throw NetUtils.WrapException(null, 0, address.GetHostName(), address.Port, e);
			}
		}

		/// <summary>Returns a handle to the rpcMetrics (required in tests)</summary>
		/// <returns>rpc metrics</returns>
		[VisibleForTesting]
		public virtual RpcMetrics GetRpcMetrics()
		{
			return rpcMetrics;
		}

		[VisibleForTesting]
		public virtual RpcDetailedMetrics GetRpcDetailedMetrics()
		{
			return rpcDetailedMetrics;
		}

		[VisibleForTesting]
		internal virtual IEnumerable<Sharpen.Thread> GetHandlers()
		{
			return Arrays.AsList(handlers);
		}

		[VisibleForTesting]
		internal virtual Server.Connection[] GetConnections()
		{
			return connectionManager.ToArray();
		}

		/// <summary>Refresh the service authorization ACL for the service handled by this server.
		/// 	</summary>
		public virtual void RefreshServiceAcl(Configuration conf, PolicyProvider provider
			)
		{
			serviceAuthorizationManager.Refresh(conf, provider);
		}

		/// <summary>
		/// Refresh the service authorization ACL for the service handled by this server
		/// using the specified Configuration.
		/// </summary>
		[InterfaceAudience.Private]
		public virtual void RefreshServiceAclWithLoadedConfiguration(Configuration conf, 
			PolicyProvider provider)
		{
			serviceAuthorizationManager.RefreshWithLoadedConfiguration(conf, provider);
		}

		/// <summary>Returns a handle to the serviceAuthorizationManager (required in tests)</summary>
		/// <returns>instance of ServiceAuthorizationManager for this server</returns>
		public virtual ServiceAuthorizationManager GetServiceAuthorizationManager()
		{
			return serviceAuthorizationManager;
		}

		internal static Type GetQueueClass(string prefix, Configuration conf)
		{
			string name = prefix + "." + CommonConfigurationKeys.IpcCallqueueImplKey;
			Type queueClass = conf.GetClass(name, typeof(LinkedBlockingQueue));
			return CallQueueManager.ConvertQueueClass<Server.Call>(queueClass);
		}

		private string GetQueueClassPrefix()
		{
			return CommonConfigurationKeys.IpcCallqueueNamespace + "." + port;
		}

		/*
		* Refresh the call queue
		*/
		public virtual void RefreshCallQueue(Configuration conf)
		{
			lock (this)
			{
				// Create the next queue
				string prefix = GetQueueClassPrefix();
				callQueue.SwapQueue(GetQueueClass(prefix, conf), maxQueueSize, prefix, conf);
			}
		}

		/// <summary>A call queued for handling.</summary>
		public class Call : Schedulable
		{
			private readonly int callId;

			private readonly int retryCount;

			private readonly IWritable rpcRequest;

			private readonly Server.Connection connection;

			private long timestamp;

			private ByteBuffer rpcResponse;

			private readonly RPC.RpcKind rpcKind;

			private readonly byte[] clientId;

			private readonly Span traceSpan;

			public Call(int id, int retryCount, IWritable param, Server.Connection connection)
				: this(id, retryCount, param, connection, RPC.RpcKind.RpcBuiltin, RpcConstants.DummyClientId
					)
			{
			}

			public Call(int id, int retryCount, IWritable param, Server.Connection connection, 
				RPC.RpcKind kind, byte[] clientId)
				: this(id, retryCount, param, connection, kind, clientId, null)
			{
			}

			public Call(int id, int retryCount, IWritable param, Server.Connection connection, 
				RPC.RpcKind kind, byte[] clientId, Span span)
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
				this.timestamp = Time.Now();
				this.rpcResponse = null;
				this.rpcKind = kind;
				this.clientId = clientId;
				this.traceSpan = span;
			}

			public override string ToString()
			{
				return rpcRequest + " from " + connection + " Call#" + callId + " Retry#" + retryCount;
			}

			public virtual void SetResponse(ByteBuffer response)
			{
				this.rpcResponse = response;
			}

			// For Schedulable
			public virtual UserGroupInformation GetUserGroupInformation()
			{
				return connection.user;
			}
		}

		/// <summary>Listens on the socket.</summary>
		/// <remarks>Listens on the socket. Creates jobs for the handler threads</remarks>
		private class Listener : Sharpen.Thread
		{
			private ServerSocketChannel acceptChannel = null;

			private Selector selector = null;

			private Server.Listener.Reader[] readers = null;

			private int currentReader = 0;

			private IPEndPoint address;

			private int backlogLength = this._enclosing.conf.GetInt(CommonConfigurationKeysPublic
				.IpcServerListenQueueSizeKey, CommonConfigurationKeysPublic.IpcServerListenQueueSizeDefault
				);

			/// <exception cref="System.IO.IOException"/>
			public Listener(Server _enclosing)
			{
				this._enclosing = _enclosing;
				//the accept channel
				//the selector that we use for the server
				//the address we bind at
				this.address = new IPEndPoint(this._enclosing.bindAddress, this._enclosing.port);
				// Create a new server socket and set to non blocking mode
				this.acceptChannel = ServerSocketChannel.Open();
				this.acceptChannel.ConfigureBlocking(false);
				// Bind the server socket to the local host and port
				Server.Bind(this.acceptChannel.Socket(), this.address, this.backlogLength, this._enclosing
					.conf, this._enclosing.portRangeConfig);
				this._enclosing.port = this.acceptChannel.Socket().GetLocalPort();
				//Could be an ephemeral port
				// create a selector;
				this.selector = Selector.Open();
				this.readers = new Server.Listener.Reader[this._enclosing.readThreads];
				for (int i = 0; i < this._enclosing.readThreads; i++)
				{
					Server.Listener.Reader reader = new Server.Listener.Reader(this, "Socket Reader #"
						 + (i + 1) + " for port " + this._enclosing.port);
					this.readers[i] = reader;
					reader.Start();
				}
				// Register accepts on the server socket with the selector.
				this.acceptChannel.Register(this.selector, SelectionKey.OpAccept);
				this.SetName("IPC Server listener on " + this._enclosing.port);
				this.SetDaemon(true);
			}

			private class Reader : Sharpen.Thread
			{
				private readonly BlockingQueue<Server.Connection> pendingConnections;

				private readonly Selector readSelector;

				/// <exception cref="System.IO.IOException"/>
				internal Reader(Listener _enclosing, string name)
					: base(name)
				{
					this._enclosing = _enclosing;
					this.pendingConnections = new LinkedBlockingQueue<Server.Connection>(this._enclosing
						._enclosing.readerPendingConnectionQueue);
					this.readSelector = Selector.Open();
				}

				public override void Run()
				{
					Server.Log.Info("Starting " + Sharpen.Thread.CurrentThread().GetName());
					try
					{
						this.DoRunLoop();
					}
					finally
					{
						try
						{
							this.readSelector.Close();
						}
						catch (IOException ioe)
						{
							Server.Log.Error("Error closing read selector in " + Sharpen.Thread.CurrentThread
								().GetName(), ioe);
						}
					}
				}

				private void DoRunLoop()
				{
					lock (this)
					{
						while (this._enclosing._enclosing.running)
						{
							SelectionKey key = null;
							try
							{
								// consume as many connections as currently queued to avoid
								// unbridled acceptance of connections that starves the select
								int size = this.pendingConnections.Count;
								for (int i = size; i > 0; i--)
								{
									Server.Connection conn = this.pendingConnections.Take();
									conn.channel.Register(this.readSelector, SelectionKey.OpRead, conn);
								}
								this.readSelector.Select();
								IEnumerator<SelectionKey> iter = this.readSelector.SelectedKeys().GetEnumerator();
								while (iter.HasNext())
								{
									key = iter.Next();
									iter.Remove();
									if (key.IsValid())
									{
										if (key.IsReadable())
										{
											this._enclosing.DoRead(key);
										}
									}
									key = null;
								}
							}
							catch (Exception e)
							{
								if (this._enclosing._enclosing.running)
								{
									// unexpected -- log it
									Server.Log.Info(Sharpen.Thread.CurrentThread().GetName() + " unexpectedly interrupted"
										, e);
								}
							}
							catch (IOException ex)
							{
								Server.Log.Error("Error in Reader", ex);
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
				public virtual void AddConnection(Server.Connection conn)
				{
					this.pendingConnections.Put(conn);
					this.readSelector.Wakeup();
				}

				internal virtual void Shutdown()
				{
					System.Diagnostics.Debug.Assert(!this._enclosing._enclosing.running);
					this.readSelector.Wakeup();
					try
					{
						base.Interrupt();
						base.Join();
					}
					catch (Exception)
					{
						Sharpen.Thread.CurrentThread().Interrupt();
					}
				}

				private readonly Listener _enclosing;
			}

			public override void Run()
			{
				Server.Log.Info(Sharpen.Thread.CurrentThread().GetName() + ": starting");
				Server.Server.Set(this._enclosing);
				this._enclosing.connectionManager.StartIdleScan();
				while (this._enclosing.running)
				{
					SelectionKey key = null;
					try
					{
						this.GetSelector().Select();
						IEnumerator<SelectionKey> iter = this.GetSelector().SelectedKeys().GetEnumerator(
							);
						while (iter.HasNext())
						{
							key = iter.Next();
							iter.Remove();
							try
							{
								if (key.IsValid())
								{
									if (key.IsAcceptable())
									{
										this.DoAccept(key);
									}
								}
							}
							catch (IOException)
							{
							}
							key = null;
						}
					}
					catch (OutOfMemoryException e)
					{
						// we can run out of memory if we have too many threads
						// log the event and sleep for a minute and give 
						// some thread(s) a chance to finish
						Server.Log.Warn("Out of Memory in server select", e);
						this.CloseCurrentConnection(key, e);
						this._enclosing.connectionManager.CloseIdle(true);
						try
						{
							Sharpen.Thread.Sleep(60000);
						}
						catch (Exception)
						{
						}
					}
					catch (Exception e)
					{
						this.CloseCurrentConnection(key, e);
					}
				}
				Server.Log.Info("Stopping " + Sharpen.Thread.CurrentThread().GetName());
				lock (this)
				{
					try
					{
						this.acceptChannel.Close();
						this.selector.Close();
					}
					catch (IOException)
					{
					}
					this.selector = null;
					this.acceptChannel = null;
					// close all connections
					this._enclosing.connectionManager.StopIdleScan();
					this._enclosing.connectionManager.CloseAll();
				}
			}

			private void CloseCurrentConnection(SelectionKey key, Exception e)
			{
				if (key != null)
				{
					Server.Connection c = (Server.Connection)key.Attachment();
					if (c != null)
					{
						this._enclosing.CloseConnection(c);
						c = null;
					}
				}
			}

			internal virtual IPEndPoint GetAddress()
			{
				return (IPEndPoint)this.acceptChannel.Socket().LocalEndPoint;
			}

			/// <exception cref="System.Exception"/>
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.OutOfMemoryException"/>
			internal virtual void DoAccept(SelectionKey key)
			{
				ServerSocketChannel server = (ServerSocketChannel)key.Channel();
				SocketChannel channel;
				while ((channel = server.Accept()) != null)
				{
					channel.ConfigureBlocking(false);
					channel.Socket().NoDelay = this._enclosing.tcpNoDelay;
					channel.Socket().SetKeepAlive(true);
					Server.Listener.Reader reader = this.GetReader();
					Server.Connection c = this._enclosing.connectionManager.Register(channel);
					// If the connectionManager can't take it, close the connection.
					if (c == null)
					{
						if (channel.IsOpen())
						{
							IOUtils.Cleanup(null, channel);
						}
						continue;
					}
					key.Attach(c);
					// so closeCurrentConnection can get the object
					reader.AddConnection(c);
				}
			}

			/// <exception cref="System.Exception"/>
			internal virtual void DoRead(SelectionKey key)
			{
				int count = 0;
				Server.Connection c = (Server.Connection)key.Attachment();
				if (c == null)
				{
					return;
				}
				c.SetLastContact(Time.Now());
				try
				{
					count = c.ReadAndProcess();
				}
				catch (Exception ieo)
				{
					Server.Log.Info(Sharpen.Thread.CurrentThread().GetName() + ": readAndProcess caught InterruptedException"
						, ieo);
					throw;
				}
				catch (Exception e)
				{
					// a WrappedRpcServerException is an exception that has been sent
					// to the client, so the stacktrace is unnecessary; any other
					// exceptions are unexpected internal server errors and thus the
					// stacktrace should be logged
					Server.Log.Info(Sharpen.Thread.CurrentThread().GetName() + ": readAndProcess from client "
						 + c.GetHostAddress() + " threw exception [" + e + "]", (e is Server.WrappedRpcServerException
						) ? null : e);
					count = -1;
				}
				//so that the (count < 0) block is executed
				if (count < 0)
				{
					this._enclosing.CloseConnection(c);
					c = null;
				}
				else
				{
					c.SetLastContact(Time.Now());
				}
			}

			internal virtual void DoStop()
			{
				lock (this)
				{
					if (this.selector != null)
					{
						this.selector.Wakeup();
						Sharpen.Thread.Yield();
					}
					if (this.acceptChannel != null)
					{
						try
						{
							this.acceptChannel.Socket().Close();
						}
						catch (IOException e)
						{
							Server.Log.Info(Sharpen.Thread.CurrentThread().GetName() + ":Exception in closing listener socket. "
								 + e);
						}
					}
					foreach (Server.Listener.Reader r in this.readers)
					{
						r.Shutdown();
					}
				}
			}

			internal virtual Selector GetSelector()
			{
				lock (this)
				{
					return this.selector;
				}
			}

			// The method that will return the next reader to work with
			// Simplistic implementation of round robin for now
			internal virtual Server.Listener.Reader GetReader()
			{
				this.currentReader = (this.currentReader + 1) % this.readers.Length;
				return this.readers[this.currentReader];
			}

			private readonly Server _enclosing;
		}

		private class Responder : Sharpen.Thread
		{
			private readonly Selector writeSelector;

			private int pending;

			internal const int PurgeInterval = 900000;

			/// <exception cref="System.IO.IOException"/>
			internal Responder(Server _enclosing)
			{
				this._enclosing = _enclosing;
				// Sends responses of RPC back to clients.
				// connections waiting to register
				// 15mins
				this.SetName("IPC Server Responder");
				this.SetDaemon(true);
				this.writeSelector = Selector.Open();
				// create a selector
				this.pending = 0;
			}

			public override void Run()
			{
				Server.Log.Info(Sharpen.Thread.CurrentThread().GetName() + ": starting");
				Server.Server.Set(this._enclosing);
				try
				{
					this.DoRunLoop();
				}
				finally
				{
					Server.Log.Info("Stopping " + Sharpen.Thread.CurrentThread().GetName());
					try
					{
						this.writeSelector.Close();
					}
					catch (IOException ioe)
					{
						Server.Log.Error("Couldn't close write selector in " + Sharpen.Thread.CurrentThread
							().GetName(), ioe);
					}
				}
			}

			private void DoRunLoop()
			{
				long lastPurgeTime = 0;
				// last check for old calls.
				while (this._enclosing.running)
				{
					try
					{
						this.WaitPending();
						// If a channel is being registered, wait.
						this.writeSelector.Select(Server.Responder.PurgeInterval);
						IEnumerator<SelectionKey> iter = this.writeSelector.SelectedKeys().GetEnumerator(
							);
						while (iter.HasNext())
						{
							SelectionKey key = iter.Next();
							iter.Remove();
							try
							{
								if (key.IsValid() && key.IsWritable())
								{
									this.DoAsyncWrite(key);
								}
							}
							catch (IOException e)
							{
								Server.Log.Info(Sharpen.Thread.CurrentThread().GetName() + ": doAsyncWrite threw exception "
									 + e);
							}
						}
						long now = Time.Now();
						if (now < lastPurgeTime + Server.Responder.PurgeInterval)
						{
							continue;
						}
						lastPurgeTime = now;
						//
						// If there were some calls that have not been sent out for a
						// long time, discard them.
						//
						if (Server.Log.IsDebugEnabled())
						{
							Server.Log.Debug("Checking for old call responses.");
						}
						AList<Server.Call> calls;
						// get the list of channels from list of keys.
						lock (this.writeSelector.Keys())
						{
							calls = new AList<Server.Call>(this.writeSelector.Keys().Count);
							iter = this.writeSelector.Keys().GetEnumerator();
							while (iter.HasNext())
							{
								SelectionKey key = iter.Next();
								Server.Call call = (Server.Call)key.Attachment();
								if (call != null && key.Channel() == call.connection.channel)
								{
									calls.AddItem(call);
								}
							}
						}
						foreach (Server.Call call_1 in calls)
						{
							this.DoPurge(call_1, now);
						}
					}
					catch (OutOfMemoryException e)
					{
						//
						// we can run out of memory if we have too many threads
						// log the event and sleep for a minute and give
						// some thread(s) a chance to finish
						//
						Server.Log.Warn("Out of Memory in server select", e);
						try
						{
							Sharpen.Thread.Sleep(60000);
						}
						catch (Exception)
						{
						}
					}
					catch (Exception e)
					{
						Server.Log.Warn("Exception in Responder", e);
					}
				}
			}

			/// <exception cref="System.IO.IOException"/>
			private void DoAsyncWrite(SelectionKey key)
			{
				Server.Call call = (Server.Call)key.Attachment();
				if (call == null)
				{
					return;
				}
				if (key.Channel() != call.connection.channel)
				{
					throw new IOException("doAsyncWrite: bad channel");
				}
				lock (call.connection.responseQueue)
				{
					if (this.ProcessResponse(call.connection.responseQueue, false))
					{
						try
						{
							key.InterestOps(0);
						}
						catch (CancelledKeyException e)
						{
							/* The Listener/reader might have closed the socket.
							* We don't explicitly cancel the key, so not sure if this will
							* ever fire.
							* This warning could be removed.
							*/
							Server.Log.Warn("Exception while changing ops : " + e);
						}
					}
				}
			}

			//
			// Remove calls that have been pending in the responseQueue 
			// for a long time.
			//
			private void DoPurge(Server.Call call, long now)
			{
				List<Server.Call> responseQueue = call.connection.responseQueue;
				lock (responseQueue)
				{
					IEnumerator<Server.Call> iter = responseQueue.ListIterator(0);
					while (iter.HasNext())
					{
						call = iter.Next();
						if (now > call.timestamp + Server.Responder.PurgeInterval)
						{
							this._enclosing.CloseConnection(call.connection);
							break;
						}
					}
				}
			}

			// Processes one response. Returns true if there are no more pending
			// data for this channel.
			//
			/// <exception cref="System.IO.IOException"/>
			private bool ProcessResponse(List<Server.Call> responseQueue, bool inHandler)
			{
				bool error = true;
				bool done = false;
				// there is more data for this channel.
				int numElements = 0;
				Server.Call call = null;
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
						call = responseQueue.RemoveFirst();
						SocketChannel channel = call.connection.channel;
						if (Server.Log.IsDebugEnabled())
						{
							Server.Log.Debug(Sharpen.Thread.CurrentThread().GetName() + ": responding to " + 
								call);
						}
						//
						// Send as much data as we can in the non-blocking fashion
						//
						int numBytes = this._enclosing.ChannelWrite(channel, call.rpcResponse);
						if (numBytes < 0)
						{
							return true;
						}
						if (!call.rpcResponse.HasRemaining())
						{
							//Clear out the response buffer so it can be collected
							call.rpcResponse = null;
							call.connection.DecRpcCount();
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
							if (Server.Log.IsDebugEnabled())
							{
								Server.Log.Debug(Sharpen.Thread.CurrentThread().GetName() + ": responding to " + 
									call + " Wrote " + numBytes + " bytes.");
							}
						}
						else
						{
							//
							// If we were unable to write the entire response out, then 
							// insert in Selector queue. 
							//
							call.connection.responseQueue.AddFirst(call);
							if (inHandler)
							{
								// set the serve time when the response has to be sent later
								call.timestamp = Time.Now();
								this.IncPending();
								try
								{
									// Wakeup the thread blocked on select, only then can the call 
									// to channel.register() complete.
									this.writeSelector.Wakeup();
									channel.Register(this.writeSelector, SelectionKey.OpWrite, call);
								}
								catch (ClosedChannelException)
								{
									//Its ok. channel might be closed else where.
									done = true;
								}
								finally
								{
									this.DecPending();
								}
							}
							if (Server.Log.IsDebugEnabled())
							{
								Server.Log.Debug(Sharpen.Thread.CurrentThread().GetName() + ": responding to " + 
									call + " Wrote partial " + numBytes + " bytes.");
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
						Server.Log.Warn(Sharpen.Thread.CurrentThread().GetName() + ", call " + call + ": output error"
							);
						done = true;
						// error. no more data for this channel.
						this._enclosing.CloseConnection(call.connection);
					}
				}
				return done;
			}

			//
			// Enqueue a response from the application.
			//
			/// <exception cref="System.IO.IOException"/>
			internal virtual void DoRespond(Server.Call call)
			{
				lock (call.connection.responseQueue)
				{
					call.connection.responseQueue.AddLast(call);
					if (call.connection.responseQueue.Count == 1)
					{
						this.ProcessResponse(call.connection.responseQueue, true);
					}
				}
			}

			private void IncPending()
			{
				lock (this)
				{
					// call waiting to be enqueued.
					this.pending++;
				}
			}

			private void DecPending()
			{
				lock (this)
				{
					// call done enqueueing.
					this.pending--;
					Sharpen.Runtime.Notify(this);
				}
			}

			/// <exception cref="System.Exception"/>
			private void WaitPending()
			{
				lock (this)
				{
					while (this.pending > 0)
					{
						Sharpen.Runtime.Wait(this);
					}
				}
			}

			private readonly Server _enclosing;
		}

		[System.Serializable]
		public sealed class AuthProtocol
		{
			public static readonly Server.AuthProtocol None = new Server.AuthProtocol(0);

			public static readonly Server.AuthProtocol Sasl = new Server.AuthProtocol(-33);

			public readonly int callId;

			internal AuthProtocol(int callId)
			{
				this.callId = callId;
			}

			internal static Server.AuthProtocol ValueOf(int callId)
			{
				foreach (Server.AuthProtocol authType in Server.AuthProtocol.Values())
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
		private class WrappedRpcServerException : RpcServerException
		{
			private readonly RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto errCode;

			public WrappedRpcServerException(RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto
				 errCode, IOException ioe)
				: base(ioe.ToString(), ioe)
			{
				this.errCode = errCode;
			}

			public WrappedRpcServerException(RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto
				 errCode, string message)
				: this(errCode, new RpcServerException(message))
			{
			}

			public override RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto GetRpcErrorCodeProto
				()
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

			private SocketChannel channel;

			private ByteBuffer data;

			private ByteBuffer dataLengthBuffer;

			private List<Server.Call> responseQueue;

			private AtomicInteger rpcCount = new AtomicInteger();

			private long lastContact;

			private int dataLength;

			private Socket socket;

			private string hostAddress;

			private int remotePort;

			private IPAddress addr;

			internal IpcConnectionContextProtos.IpcConnectionContextProto connectionContext;

			internal string protocolName;

			internal SaslServer saslServer;

			private SaslRpcServer.AuthMethod authMethod;

			private Server.AuthProtocol authProtocol;

			private bool saslContextEstablished;

			private ByteBuffer connectionHeaderBuf = null;

			private ByteBuffer unwrappedData;

			private ByteBuffer unwrappedDataLengthBuffer;

			private int serviceClass;

			internal UserGroupInformation user = null;

			public UserGroupInformation attemptingUser = null;

			private readonly Server.Call authFailedCall;

			private ByteArrayOutputStream authFailedResponse = new ByteArrayOutputStream();

			private readonly Server.Call saslCall;

			private readonly ByteArrayOutputStream saslResponse = new ByteArrayOutputStream();

			private bool sentNegotiate = false;

			private bool useWrap = false;

			public Connection(Server _enclosing, SocketChannel channel, long lastContact)
			{
				this._enclosing = _enclosing;
				authFailedCall = new Server.Call(RpcConstants.AuthorizationFailedCallId, RpcConstants
					.InvalidRetryCount, null, this);
				saslCall = new Server.Call(Server.AuthProtocol.Sasl.callId, RpcConstants.InvalidRetryCount
					, null, this);
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
				this.dataLengthBuffer = ByteBuffer.Allocate(4);
				this.unwrappedData = null;
				this.unwrappedDataLengthBuffer = ByteBuffer.Allocate(4);
				this.socket = channel.Socket();
				this.addr = this.socket.GetInetAddress();
				if (this.addr == null)
				{
					this.hostAddress = "*Unknown*";
				}
				else
				{
					this.hostAddress = this.addr.GetHostAddress();
				}
				this.remotePort = this.socket.GetPort();
				this.responseQueue = new List<Server.Call>();
				if (this._enclosing.socketSendBufferSize != 0)
				{
					try
					{
						this.socket.SetSendBufferSize(this._enclosing.socketSendBufferSize);
					}
					catch (IOException)
					{
						Server.Log.Warn("Connection: unable to set socket send buffer size to " + this._enclosing
							.socketSendBufferSize);
					}
				}
			}

			public override string ToString()
			{
				return this.GetHostAddress() + ":" + this.remotePort;
			}

			public virtual string GetHostAddress()
			{
				return this.hostAddress;
			}

			public virtual IPAddress GetHostInetAddress()
			{
				return this.addr;
			}

			public virtual void SetLastContact(long lastContact)
			{
				this.lastContact = lastContact;
			}

			public virtual long GetLastContact()
			{
				return this.lastContact;
			}

			/* Return true if the connection has no outstanding rpc */
			private bool IsIdle()
			{
				return this.rpcCount.Get() == 0;
			}

			/* Decrement the outstanding RPC count */
			private void DecRpcCount()
			{
				this.rpcCount.DecrementAndGet();
			}

			/* Increment the outstanding RPC count */
			private void IncRpcCount()
			{
				this.rpcCount.IncrementAndGet();
			}

			/// <exception cref="Org.Apache.Hadoop.Security.Token.SecretManager.InvalidToken"/>
			/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
			private UserGroupInformation GetAuthorizedUgi(string authorizedId)
			{
				if (this.authMethod == SaslRpcServer.AuthMethod.Token)
				{
					TokenIdentifier tokenId = SaslRpcServer.GetIdentifier(authorizedId, this._enclosing
						.secretManager);
					UserGroupInformation ugi = tokenId.GetUser();
					if (ugi == null)
					{
						throw new AccessControlException("Can't retrieve username from tokenIdentifier.");
					}
					ugi.AddTokenIdentifier(tokenId);
					return ugi;
				}
				else
				{
					return UserGroupInformation.CreateRemoteUser(authorizedId, this.authMethod);
				}
			}

			/// <exception cref="Org.Apache.Hadoop.Ipc.Server.WrappedRpcServerException"/>
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			private void SaslReadAndProcess(DataInputStream dis)
			{
				RpcHeaderProtos.RpcSaslProto saslMessage = this.DecodeProtobufFromStream(RpcHeaderProtos.RpcSaslProto
					.NewBuilder(), dis);
				switch (saslMessage.GetState())
				{
					case RpcHeaderProtos.RpcSaslProto.SaslState.Wrap:
					{
						if (!this.saslContextEstablished || !this.useWrap)
						{
							throw new Server.WrappedRpcServerException(RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto
								.FatalInvalidRpcHeader, new SaslException("Server is not wrapping data"));
						}
						// loops over decoded data and calls processOneRpc
						this.UnwrapPacketAndProcessRpcs(saslMessage.GetToken().ToByteArray());
						break;
					}

					default:
					{
						this.SaslProcess(saslMessage);
						break;
					}
				}
			}

			private Exception GetCauseForInvalidToken(IOException e)
			{
				Exception cause = e;
				while (cause != null)
				{
					if (cause is RetriableException)
					{
						return cause;
					}
					else
					{
						if (cause is StandbyException)
						{
							return cause;
						}
						else
						{
							if (cause is SecretManager.InvalidToken)
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

			/// <exception cref="Org.Apache.Hadoop.Ipc.Server.WrappedRpcServerException"/>
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			private void SaslProcess(RpcHeaderProtos.RpcSaslProto saslMessage)
			{
				if (this.saslContextEstablished)
				{
					throw new Server.WrappedRpcServerException(RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto
						.FatalInvalidRpcHeader, new SaslException("Negotiation is already complete"));
				}
				RpcHeaderProtos.RpcSaslProto saslResponse = null;
				try
				{
					try
					{
						saslResponse = this.ProcessSaslMessage(saslMessage);
					}
					catch (IOException e)
					{
						this._enclosing.rpcMetrics.IncrAuthenticationFailures();
						// attempting user could be null
						Server.Auditlog.Warn(Server.AuthFailedFor + this.ToString() + ":" + this.attemptingUser
							 + " (" + e.GetLocalizedMessage() + ")");
						throw (IOException)this.GetCauseForInvalidToken(e);
					}
					if (this.saslServer != null && this.saslServer.IsComplete())
					{
						if (Server.Log.IsDebugEnabled())
						{
							Server.Log.Debug("SASL server context established. Negotiated QoP is " + this.saslServer
								.GetNegotiatedProperty(Javax.Security.Sasl.Sasl.Qop));
						}
						this.user = this.GetAuthorizedUgi(this.saslServer.GetAuthorizationID());
						if (Server.Log.IsDebugEnabled())
						{
							Server.Log.Debug("SASL server successfully authenticated client: " + this.user);
						}
						this._enclosing.rpcMetrics.IncrAuthenticationSuccesses();
						Server.Auditlog.Info(Server.AuthSuccessfulFor + this.user);
						this.saslContextEstablished = true;
					}
				}
				catch (Server.WrappedRpcServerException wrse)
				{
					// don't re-wrap
					throw;
				}
				catch (IOException ioe)
				{
					throw new Server.WrappedRpcServerException(RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto
						.FatalUnauthorized, ioe);
				}
				// send back response if any, may throw IOException
				if (saslResponse != null)
				{
					this.DoSaslReply(saslResponse);
				}
				// do NOT enable wrapping until the last auth response is sent
				if (this.saslContextEstablished)
				{
					string qop = (string)this.saslServer.GetNegotiatedProperty(Javax.Security.Sasl.Sasl
						.Qop);
					// SASL wrapping is only used if the connection has a QOP, and
					// the value is not auth.  ex. auth-int & auth-priv
					this.useWrap = (qop != null && !Sharpen.Runtime.EqualsIgnoreCase("auth", qop));
				}
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			private RpcHeaderProtos.RpcSaslProto ProcessSaslMessage(RpcHeaderProtos.RpcSaslProto
				 saslMessage)
			{
				RpcHeaderProtos.RpcSaslProto saslResponse;
				RpcHeaderProtos.RpcSaslProto.SaslState state = saslMessage.GetState();
				switch (state)
				{
					case RpcHeaderProtos.RpcSaslProto.SaslState.Negotiate:
					{
						// required      
						if (this.sentNegotiate)
						{
							throw new AccessControlException("Client already attempted negotiation");
						}
						saslResponse = this.BuildSaslNegotiateResponse();
						// simple-only server negotiate response is success which client
						// interprets as switch to simple
						if (saslResponse.GetState() == RpcHeaderProtos.RpcSaslProto.SaslState.Success)
						{
							this.SwitchToSimple();
						}
						break;
					}

					case RpcHeaderProtos.RpcSaslProto.SaslState.Initiate:
					{
						if (saslMessage.GetAuthsCount() != 1)
						{
							throw new SaslException("Client mechanism is malformed");
						}
						// verify the client requested an advertised authType
						RpcHeaderProtos.RpcSaslProto.SaslAuth clientSaslAuth = saslMessage.GetAuths(0);
						if (!this._enclosing.negotiateResponse.GetAuthsList().Contains(clientSaslAuth))
						{
							if (this.sentNegotiate)
							{
								throw new AccessControlException(clientSaslAuth.GetMethod() + " authentication is not enabled."
									 + "  Available:" + this._enclosing.enabledAuthMethods);
							}
							saslResponse = this.BuildSaslNegotiateResponse();
							break;
						}
						this.authMethod = SaslRpcServer.AuthMethod.ValueOf(clientSaslAuth.GetMethod());
						// abort SASL for SIMPLE auth, server has already ensured that
						// SIMPLE is a legit option above.  we will send no response
						if (this.authMethod == SaslRpcServer.AuthMethod.Simple)
						{
							this.SwitchToSimple();
							saslResponse = null;
							break;
						}
						// sasl server for tokens may already be instantiated
						if (this.saslServer == null || this.authMethod != SaslRpcServer.AuthMethod.Token)
						{
							this.saslServer = this.CreateSaslServer(this.authMethod);
						}
						saslResponse = this.ProcessSaslToken(saslMessage);
						break;
					}

					case RpcHeaderProtos.RpcSaslProto.SaslState.Response:
					{
						saslResponse = this.ProcessSaslToken(saslMessage);
						break;
					}

					default:
					{
						throw new SaslException("Client sent unsupported state " + state);
					}
				}
				return saslResponse;
			}

			/// <exception cref="Javax.Security.Sasl.SaslException"/>
			private RpcHeaderProtos.RpcSaslProto ProcessSaslToken(RpcHeaderProtos.RpcSaslProto
				 saslMessage)
			{
				if (!saslMessage.HasToken())
				{
					throw new SaslException("Client did not send a token");
				}
				byte[] saslToken = saslMessage.GetToken().ToByteArray();
				if (Server.Log.IsDebugEnabled())
				{
					Server.Log.Debug("Have read input token of size " + saslToken.Length + " for processing by saslServer.evaluateResponse()"
						);
				}
				saslToken = this.saslServer.EvaluateResponse(saslToken);
				return this.BuildSaslResponse(this.saslServer.IsComplete() ? RpcHeaderProtos.RpcSaslProto.SaslState
					.Success : RpcHeaderProtos.RpcSaslProto.SaslState.Challenge, saslToken);
			}

			private void SwitchToSimple()
			{
				// disable SASL and blank out any SASL server
				this.authProtocol = Server.AuthProtocol.None;
				this.saslServer = null;
			}

			private RpcHeaderProtos.RpcSaslProto BuildSaslResponse(RpcHeaderProtos.RpcSaslProto.SaslState
				 state, byte[] replyToken)
			{
				if (Server.Log.IsDebugEnabled())
				{
					Server.Log.Debug("Will send " + state + " token of size " + ((replyToken != null)
						 ? replyToken.Length : null) + " from saslServer.");
				}
				RpcHeaderProtos.RpcSaslProto.Builder response = RpcHeaderProtos.RpcSaslProto.NewBuilder
					();
				response.SetState(state);
				if (replyToken != null)
				{
					response.SetToken(ByteString.CopyFrom(replyToken));
				}
				return ((RpcHeaderProtos.RpcSaslProto)response.Build());
			}

			/// <exception cref="System.IO.IOException"/>
			private void DoSaslReply(Message message)
			{
				if (Server.Log.IsDebugEnabled())
				{
					Server.Log.Debug("Sending sasl message " + message);
				}
				this._enclosing.SetupResponse(this.saslResponse, this.saslCall, RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto
					.Success, null, new ProtobufRpcEngine.RpcResponseWrapper(message), null, null);
				this._enclosing.responder.DoRespond(this.saslCall);
			}

			/// <exception cref="System.IO.IOException"/>
			private void DoSaslReply(Exception ioe)
			{
				this._enclosing.SetupResponse(this.authFailedResponse, this.authFailedCall, RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto
					.Fatal, RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto.FatalUnauthorized
					, null, ioe.GetType().FullName, ioe.GetLocalizedMessage());
				this._enclosing.responder.DoRespond(this.authFailedCall);
			}

			private void DisposeSasl()
			{
				if (this.saslServer != null)
				{
					try
					{
						this.saslServer.Dispose();
					}
					catch (SaslException)
					{
					}
				}
			}

			/// <exception cref="System.IO.IOException"/>
			private void CheckDataLength(int dataLength)
			{
				if (dataLength < 0)
				{
					string error = "Unexpected data length " + dataLength + "!! from " + this.GetHostAddress
						();
					Server.Log.Warn(error);
					throw new IOException(error);
				}
				else
				{
					if (dataLength > this._enclosing.maxDataLength)
					{
						string error = "Requested data length " + dataLength + " is longer than maximum configured RPC length "
							 + this._enclosing.maxDataLength + ".  RPC came from " + this.GetHostAddress();
						Server.Log.Warn(error);
						throw new IOException(error);
					}
				}
			}

			/// <exception cref="Org.Apache.Hadoop.Ipc.Server.WrappedRpcServerException"/>
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public virtual int ReadAndProcess()
			{
				while (true)
				{
					/* Read at most one RPC. If the header is not read completely yet
					* then iterate until we read first RPC or until there is no data left.
					*/
					int count = -1;
					if (this.dataLengthBuffer.Remaining() > 0)
					{
						count = this._enclosing.ChannelRead(this.channel, this.dataLengthBuffer);
						if (count < 0 || this.dataLengthBuffer.Remaining() > 0)
						{
							return count;
						}
					}
					if (!this.connectionHeaderRead)
					{
						//Every connection is expected to send the header.
						if (this.connectionHeaderBuf == null)
						{
							this.connectionHeaderBuf = ByteBuffer.Allocate(3);
						}
						count = this._enclosing.ChannelRead(this.channel, this.connectionHeaderBuf);
						if (count < 0 || this.connectionHeaderBuf.Remaining() > 0)
						{
							return count;
						}
						int version = this.connectionHeaderBuf.Get(0);
						// TODO we should add handler for service class later
						this.SetServiceClass(this.connectionHeaderBuf.Get(1));
						this.dataLengthBuffer.Flip();
						// Check if it looks like the user is hitting an IPC port
						// with an HTTP GET - this is a common error, so we can
						// send back a simple string indicating as much.
						if (Server.HttpGetBytes.Equals(this.dataLengthBuffer))
						{
							this.SetupHttpRequestOnIpcPortResponse();
							return -1;
						}
						if (!RpcConstants.Header.Equals(this.dataLengthBuffer) || version != RpcConstants
							.CurrentVersion)
						{
							//Warning is ok since this is not supposed to happen.
							Server.Log.Warn("Incorrect header or version mismatch from " + this.hostAddress +
								 ":" + this.remotePort + " got version " + version + " expected version " + RpcConstants
								.CurrentVersion);
							this.SetupBadVersionResponse(version);
							return -1;
						}
						// this may switch us into SIMPLE
						this.authProtocol = this.InitializeAuthContext(this.connectionHeaderBuf.Get(2));
						this.dataLengthBuffer.Clear();
						this.connectionHeaderBuf = null;
						this.connectionHeaderRead = true;
						continue;
					}
					if (this.data == null)
					{
						this.dataLengthBuffer.Flip();
						this.dataLength = this.dataLengthBuffer.GetInt();
						this.CheckDataLength(this.dataLength);
						this.data = ByteBuffer.Allocate(this.dataLength);
					}
					count = this._enclosing.ChannelRead(this.channel, this.data);
					if (this.data.Remaining() == 0)
					{
						this.dataLengthBuffer.Clear();
						this.data.Flip();
						bool isHeaderRead = this.connectionContextRead;
						this.ProcessOneRpc(((byte[])this.data.Array()));
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
			private Server.AuthProtocol InitializeAuthContext(int authType)
			{
				Server.AuthProtocol authProtocol = Server.AuthProtocol.ValueOf(authType);
				if (authProtocol == null)
				{
					IOException ioe = new IpcException("Unknown auth protocol:" + authType);
					this.DoSaslReply(ioe);
					throw ioe;
				}
				bool isSimpleEnabled = this._enclosing.enabledAuthMethods.Contains(SaslRpcServer.AuthMethod
					.Simple);
				switch (authProtocol)
				{
					case Server.AuthProtocol.None:
					{
						// don't reply if client is simple and server is insecure
						if (!isSimpleEnabled)
						{
							IOException ioe = new AccessControlException("SIMPLE authentication is not enabled."
								 + "  Available:" + this._enclosing.enabledAuthMethods);
							this.DoSaslReply(ioe);
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
			private RpcHeaderProtos.RpcSaslProto BuildSaslNegotiateResponse()
			{
				RpcHeaderProtos.RpcSaslProto negotiateMessage = this._enclosing.negotiateResponse;
				// accelerate token negotiation by sending initial challenge
				// in the negotiation response
				if (this._enclosing.enabledAuthMethods.Contains(SaslRpcServer.AuthMethod.Token))
				{
					this.saslServer = this.CreateSaslServer(SaslRpcServer.AuthMethod.Token);
					byte[] challenge = this.saslServer.EvaluateResponse(new byte[0]);
					RpcHeaderProtos.RpcSaslProto.Builder negotiateBuilder = RpcHeaderProtos.RpcSaslProto
						.NewBuilder(this._enclosing.negotiateResponse);
					negotiateBuilder.GetAuthsBuilder(0).SetChallenge(ByteString.CopyFrom(challenge));
					// TOKEN is always first
					negotiateMessage = ((RpcHeaderProtos.RpcSaslProto)negotiateBuilder.Build());
				}
				this.sentNegotiate = true;
				return negotiateMessage;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			private SaslServer CreateSaslServer(SaslRpcServer.AuthMethod authMethod)
			{
				IDictionary<string, object> saslProps = this._enclosing.saslPropsResolver.GetServerProperties
					(this.addr);
				return new SaslRpcServer(authMethod).Create(this, saslProps, this._enclosing.secretManager
					);
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
			private void SetupBadVersionResponse(int clientVersion)
			{
				string errMsg = "Server IPC version " + RpcConstants.CurrentVersion + " cannot communicate with client version "
					 + clientVersion;
				ByteArrayOutputStream buffer = new ByteArrayOutputStream();
				if (clientVersion >= 9)
				{
					// Versions >>9  understand the normal response
					Server.Call fakeCall = new Server.Call(-1, RpcConstants.InvalidRetryCount, null, 
						this);
					this._enclosing.SetupResponse(buffer, fakeCall, RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto
						.Fatal, RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto.FatalVersionMismatch
						, null, typeof(RPC.VersionMismatch).FullName, errMsg);
					this._enclosing.responder.DoRespond(fakeCall);
				}
				else
				{
					if (clientVersion >= 3)
					{
						Server.Call fakeCall = new Server.Call(-1, RpcConstants.InvalidRetryCount, null, 
							this);
						// Versions 3 to 8 use older response
						this._enclosing.SetupResponseOldVersionFatal(buffer, fakeCall, null, typeof(RPC.VersionMismatch
							).FullName, errMsg);
						this._enclosing.responder.DoRespond(fakeCall);
					}
					else
					{
						if (clientVersion == 2)
						{
							// Hadoop 0.18.3
							Server.Call fakeCall = new Server.Call(0, RpcConstants.InvalidRetryCount, null, this
								);
							DataOutputStream @out = new DataOutputStream(buffer);
							@out.WriteInt(0);
							// call ID
							@out.WriteBoolean(true);
							// error
							WritableUtils.WriteString(@out, typeof(RPC.VersionMismatch).FullName);
							WritableUtils.WriteString(@out, errMsg);
							fakeCall.SetResponse(ByteBuffer.Wrap(buffer.ToByteArray()));
							this._enclosing.responder.DoRespond(fakeCall);
						}
					}
				}
			}

			/// <exception cref="System.IO.IOException"/>
			private void SetupHttpRequestOnIpcPortResponse()
			{
				Server.Call fakeCall = new Server.Call(0, RpcConstants.InvalidRetryCount, null, this
					);
				fakeCall.SetResponse(ByteBuffer.Wrap(Sharpen.Runtime.GetBytesForString(Server.ReceivedHttpReqResponse
					, Charsets.Utf8)));
				this._enclosing.responder.DoRespond(fakeCall);
			}

			/// <summary>Reads the connection context following the connection header</summary>
			/// <param name="dis">- DataInputStream from which to read the header</param>
			/// <exception cref="WrappedRpcServerException">
			/// - if the header cannot be
			/// deserialized, or the user is not authorized
			/// </exception>
			/// <exception cref="Org.Apache.Hadoop.Ipc.Server.WrappedRpcServerException"/>
			private void ProcessConnectionContext(DataInputStream dis)
			{
				// allow only one connection context during a session
				if (this.connectionContextRead)
				{
					throw new Server.WrappedRpcServerException(RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto
						.FatalInvalidRpcHeader, "Connection context already processed");
				}
				this.connectionContext = this.DecodeProtobufFromStream(IpcConnectionContextProtos.IpcConnectionContextProto
					.NewBuilder(), dis);
				this.protocolName = this.connectionContext.HasProtocol() ? this.connectionContext
					.GetProtocol() : null;
				UserGroupInformation protocolUser = ProtoUtil.GetUgi(this.connectionContext);
				if (this.saslServer == null)
				{
					this.user = protocolUser;
				}
				else
				{
					// user is authenticated
					this.user.SetAuthenticationMethod(this.authMethod);
					//Now we check if this is a proxy user case. If the protocol user is
					//different from the 'user', it is a proxy user scenario. However, 
					//this is not allowed if user authenticated with DIGEST.
					if ((protocolUser != null) && (!protocolUser.GetUserName().Equals(this.user.GetUserName
						())))
					{
						if (this.authMethod == SaslRpcServer.AuthMethod.Token)
						{
							// Not allowed to doAs if token authentication is used
							throw new Server.WrappedRpcServerException(RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto
								.FatalUnauthorized, new AccessControlException("Authenticated user (" + this.user
								 + ") doesn't match what the client claims to be (" + protocolUser + ")"));
						}
						else
						{
							// Effective user can be different from authenticated user
							// for simple auth or kerberos auth
							// The user is the real user. Now we create a proxy user
							UserGroupInformation realUser = this.user;
							this.user = UserGroupInformation.CreateProxyUser(protocolUser.GetUserName(), realUser
								);
						}
					}
				}
				this.AuthorizeConnection();
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
			/// <exception cref="Org.Apache.Hadoop.Ipc.Server.WrappedRpcServerException"/>
			private void UnwrapPacketAndProcessRpcs(byte[] inBuf)
			{
				if (Server.Log.IsDebugEnabled())
				{
					Server.Log.Debug("Have read input token of size " + inBuf.Length + " for processing by saslServer.unwrap()"
						);
				}
				inBuf = this.saslServer.Unwrap(inBuf, 0, inBuf.Length);
				ReadableByteChannel ch = Channels.NewChannel(new ByteArrayInputStream(inBuf));
				// Read all RPCs contained in the inBuf, even partial ones
				while (true)
				{
					int count = -1;
					if (this.unwrappedDataLengthBuffer.Remaining() > 0)
					{
						count = this._enclosing.ChannelRead(ch, this.unwrappedDataLengthBuffer);
						if (count <= 0 || this.unwrappedDataLengthBuffer.Remaining() > 0)
						{
							return;
						}
					}
					if (this.unwrappedData == null)
					{
						this.unwrappedDataLengthBuffer.Flip();
						int unwrappedDataLength = this.unwrappedDataLengthBuffer.GetInt();
						this.unwrappedData = ByteBuffer.Allocate(unwrappedDataLength);
					}
					count = this._enclosing.ChannelRead(ch, this.unwrappedData);
					if (count <= 0 || this.unwrappedData.Remaining() > 0)
					{
						return;
					}
					if (this.unwrappedData.Remaining() == 0)
					{
						this.unwrappedDataLengthBuffer.Clear();
						this.unwrappedData.Flip();
						this.ProcessOneRpc(((byte[])this.unwrappedData.Array()));
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
			/// <exception cref="Org.Apache.Hadoop.Ipc.Server.WrappedRpcServerException"/>
			private void ProcessOneRpc(byte[] buf)
			{
				int callId = -1;
				int retry = RpcConstants.InvalidRetryCount;
				try
				{
					DataInputStream dis = new DataInputStream(new ByteArrayInputStream(buf));
					RpcHeaderProtos.RpcRequestHeaderProto header = this.DecodeProtobufFromStream(RpcHeaderProtos.RpcRequestHeaderProto
						.NewBuilder(), dis);
					callId = header.GetCallId();
					retry = header.GetRetryCount();
					if (Server.Log.IsDebugEnabled())
					{
						Server.Log.Debug(" got #" + callId);
					}
					this.CheckRpcHeaders(header);
					if (callId < 0)
					{
						// callIds typically used during connection setup
						this.ProcessRpcOutOfBandRequest(header, dis);
					}
					else
					{
						if (!this.connectionContextRead)
						{
							throw new Server.WrappedRpcServerException(RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto
								.FatalInvalidRpcHeader, "Connection context not established");
						}
						else
						{
							this.ProcessRpcRequest(header, dis);
						}
					}
				}
				catch (Server.WrappedRpcServerException wrse)
				{
					// inform client of error
					Exception ioe = wrse.InnerException;
					Server.Call call = new Server.Call(callId, retry, null, this);
					this._enclosing.SetupResponse(this.authFailedResponse, call, RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto
						.Fatal, wrse.GetRpcErrorCodeProto(), null, ioe.GetType().FullName, ioe.Message);
					this._enclosing.responder.DoRespond(call);
					throw;
				}
			}

			/// <summary>Verify RPC header is valid</summary>
			/// <param name="header">- RPC request header</param>
			/// <exception cref="WrappedRpcServerException">- header contains invalid values</exception>
			/// <exception cref="Org.Apache.Hadoop.Ipc.Server.WrappedRpcServerException"/>
			private void CheckRpcHeaders(RpcHeaderProtos.RpcRequestHeaderProto header)
			{
				if (!header.HasRpcOp())
				{
					string err = " IPC Server: No rpc op in rpcRequestHeader";
					throw new Server.WrappedRpcServerException(RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto
						.FatalInvalidRpcHeader, err);
				}
				if (header.GetRpcOp() != RpcHeaderProtos.RpcRequestHeaderProto.OperationProto.RpcFinalPacket)
				{
					string err = "IPC Server does not implement rpc header operation" + header.GetRpcOp
						();
					throw new Server.WrappedRpcServerException(RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto
						.FatalInvalidRpcHeader, err);
				}
				// If we know the rpc kind, get its class so that we can deserialize
				// (Note it would make more sense to have the handler deserialize but 
				// we continue with this original design.
				if (!header.HasRpcKind())
				{
					string err = " IPC Server: No rpc kind in rpcRequestHeader";
					throw new Server.WrappedRpcServerException(RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto
						.FatalInvalidRpcHeader, err);
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
			/// <exception cref="Org.Apache.Hadoop.Ipc.Server.WrappedRpcServerException"/>
			private void ProcessRpcRequest(RpcHeaderProtos.RpcRequestHeaderProto header, DataInputStream
				 dis)
			{
				Type rpcRequestClass = this._enclosing.GetRpcRequestWrapper(header.GetRpcKind());
				if (rpcRequestClass == null)
				{
					Server.Log.Warn("Unknown rpc kind " + header.GetRpcKind() + " from client " + this
						.GetHostAddress());
					string err = "Unknown rpc kind in rpc header" + header.GetRpcKind();
					throw new Server.WrappedRpcServerException(RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto
						.FatalInvalidRpcHeader, err);
				}
				IWritable rpcRequest;
				try
				{
					//Read the rpc request
					rpcRequest = ReflectionUtils.NewInstance(rpcRequestClass, this._enclosing.conf);
					rpcRequest.ReadFields(dis);
				}
				catch (Exception t)
				{
					// includes runtime exception from newInstance
					Server.Log.Warn("Unable to read call parameters for client " + this.GetHostAddress
						() + "on connection protocol " + this.protocolName + " for rpcKind " + header.GetRpcKind
						(), t);
					string err = "IPC server unable to read call parameters: " + t.Message;
					throw new Server.WrappedRpcServerException(RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto
						.FatalDeserializingRequest, err);
				}
				Span traceSpan = null;
				if (header.HasTraceInfo())
				{
					// If the incoming RPC included tracing info, always continue the trace
					TraceInfo parentSpan = new TraceInfo(header.GetTraceInfo().GetTraceId(), header.GetTraceInfo
						().GetParentId());
					traceSpan = Trace.StartSpan(rpcRequest.ToString(), parentSpan).Detach();
				}
				Server.Call call = new Server.Call(header.GetCallId(), header.GetRetryCount(), rpcRequest
					, this, ProtoUtil.Convert(header.GetRpcKind()), header.GetClientId().ToByteArray
					(), traceSpan);
				this._enclosing.callQueue.Put(call);
				// queue the call; maybe blocked here
				this.IncRpcCount();
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
			/// <exception cref="Org.Apache.Hadoop.Ipc.Server.WrappedRpcServerException"/>
			private void ProcessRpcOutOfBandRequest(RpcHeaderProtos.RpcRequestHeaderProto header
				, DataInputStream dis)
			{
				int callId = header.GetCallId();
				if (callId == RpcConstants.ConnectionContextCallId)
				{
					// SASL must be established prior to connection context
					if (this.authProtocol == Server.AuthProtocol.Sasl && !this.saslContextEstablished)
					{
						throw new Server.WrappedRpcServerException(RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto
							.FatalInvalidRpcHeader, "Connection header sent during SASL negotiation");
					}
					// read and authorize the user
					this.ProcessConnectionContext(dis);
				}
				else
				{
					if (callId == Server.AuthProtocol.Sasl.callId)
					{
						// if client was switched to simple, ignore first SASL message
						if (this.authProtocol != Server.AuthProtocol.Sasl)
						{
							throw new Server.WrappedRpcServerException(RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto
								.FatalInvalidRpcHeader, "SASL protocol not requested by client");
						}
						this.SaslReadAndProcess(dis);
					}
					else
					{
						if (callId == RpcConstants.PingCallId)
						{
							Server.Log.Debug("Received ping message");
						}
						else
						{
							throw new Server.WrappedRpcServerException(RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto
								.FatalInvalidRpcHeader, "Unknown out of band call #" + callId);
						}
					}
				}
			}

			/// <summary>Authorize proxy users to access this server</summary>
			/// <exception cref="WrappedRpcServerException">- user is not allowed to proxy</exception>
			/// <exception cref="Org.Apache.Hadoop.Ipc.Server.WrappedRpcServerException"/>
			private void AuthorizeConnection()
			{
				try
				{
					// If auth method is TOKEN, the token was obtained by the
					// real user for the effective user, therefore not required to
					// authorize real user. doAs is allowed only for simple or kerberos
					// authentication
					if (this.user != null && this.user.GetRealUser() != null && (this.authMethod != SaslRpcServer.AuthMethod
						.Token))
					{
						ProxyUsers.Authorize(this.user, this.GetHostAddress());
					}
					this._enclosing.Authorize(this.user, this.protocolName, this.GetHostInetAddress()
						);
					if (Server.Log.IsDebugEnabled())
					{
						Server.Log.Debug("Successfully authorized " + this.connectionContext);
					}
					this._enclosing.rpcMetrics.IncrAuthorizationSuccesses();
				}
				catch (AuthorizationException ae)
				{
					Server.Log.Info("Connection from " + this + " for protocol " + this.connectionContext
						.GetProtocol() + " is unauthorized for user " + this.user);
					this._enclosing.rpcMetrics.IncrAuthorizationFailures();
					throw new Server.WrappedRpcServerException(RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto
						.FatalUnauthorized, ae);
				}
			}

			/// <summary>Decode the a protobuf from the given input stream</summary>
			/// <param name="builder">- Builder of the protobuf to decode</param>
			/// <param name="dis">- DataInputStream to read the protobuf</param>
			/// <returns>Message - decoded protobuf</returns>
			/// <exception cref="WrappedRpcServerException">- deserialization failed</exception>
			/// <exception cref="Org.Apache.Hadoop.Ipc.Server.WrappedRpcServerException"/>
			private T DecodeProtobufFromStream<T>(Message.Builder builder, DataInputStream dis
				)
				where T : Message
			{
				try
				{
					builder.MergeDelimitedFrom(dis);
					return (T)builder.Build();
				}
				catch (Exception ioe)
				{
					Type protoClass = builder.GetDefaultInstanceForType().GetType();
					throw new Server.WrappedRpcServerException(RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto
						.FatalDeserializingRequest, "Error decoding " + protoClass.Name + ": " + ioe);
				}
			}

			/// <summary>Get service class for connection</summary>
			/// <returns>the serviceClass</returns>
			public virtual int GetServiceClass()
			{
				return this.serviceClass;
			}

			/// <summary>Set service class for connection</summary>
			/// <param name="serviceClass">the serviceClass to set</param>
			public virtual void SetServiceClass(int serviceClass)
			{
				this.serviceClass = serviceClass;
			}

			private void Close()
			{
				lock (this)
				{
					this.DisposeSasl();
					this.data = null;
					this.dataLengthBuffer = null;
					if (!this.channel.IsOpen())
					{
						return;
					}
					try
					{
						this.socket.ShutdownOutput();
					}
					catch (Exception e)
					{
						Server.Log.Debug("Ignoring socket shutdown exception", e);
					}
					if (this.channel.IsOpen())
					{
						IOUtils.Cleanup(null, this.channel);
					}
					IOUtils.Cleanup(null, this.socket);
				}
			}

			private readonly Server _enclosing;
		}

		/// <summary>Handles queued calls .</summary>
		private class Handler : Sharpen.Thread
		{
			public Handler(Server _enclosing, int instanceNumber)
			{
				this._enclosing = _enclosing;
				this.SetDaemon(true);
				this.SetName("IPC Server handler " + instanceNumber + " on " + this._enclosing.port
					);
			}

			public override void Run()
			{
				Server.Log.Debug(Sharpen.Thread.CurrentThread().GetName() + ": starting");
				Server.Server.Set(this._enclosing);
				ByteArrayOutputStream buf = new ByteArrayOutputStream(Server.InitialRespBufSize);
				while (this._enclosing.running)
				{
					TraceScope traceScope = null;
					try
					{
						Server.Call call = this._enclosing.callQueue.Take();
						// pop the queue; maybe blocked here
						if (Server.Log.IsDebugEnabled())
						{
							Server.Log.Debug(Sharpen.Thread.CurrentThread().GetName() + ": " + call + " for RpcKind "
								 + call.rpcKind);
						}
						if (!call.connection.channel.IsOpen())
						{
							Server.Log.Info(Sharpen.Thread.CurrentThread().GetName() + ": skipped " + call);
							continue;
						}
						string errorClass = null;
						string error = null;
						RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto returnStatus = RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto
							.Success;
						RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto detailedErr = null;
						IWritable value = null;
						Server.CurCall.Set(call);
						if (call.traceSpan != null)
						{
							traceScope = Trace.ContinueSpan(call.traceSpan);
						}
						try
						{
							// Make the call as the user via Subject.doAs, thus associating
							// the call with the Subject
							if (call.connection.user == null)
							{
								value = this._enclosing.Call(call.rpcKind, call.connection.protocolName, call.rpcRequest
									, call.timestamp);
							}
							else
							{
								value = call.connection.user.DoAs(new _PrivilegedExceptionAction_2045(this, call)
									);
							}
						}
						catch (Exception e)
						{
							// make the call
							if (e is UndeclaredThrowableException)
							{
								e = e.InnerException;
							}
							string logMsg = Sharpen.Thread.CurrentThread().GetName() + ", call " + call;
							if (this._enclosing.exceptionsHandler.IsTerse(e.GetType()))
							{
								// Don't log the whole stack trace. Way too noisy!
								Server.Log.Info(logMsg + ": " + e);
							}
							else
							{
								if (e is RuntimeException || e is Error)
								{
									// These exception types indicate something is probably wrong
									// on the server side, as opposed to just a normal exceptional
									// result.
									Server.Log.Warn(logMsg, e);
								}
								else
								{
									Server.Log.Info(logMsg, e);
								}
							}
							if (e is RpcServerException)
							{
								RpcServerException rse = ((RpcServerException)e);
								returnStatus = rse.GetRpcStatusProto();
								detailedErr = rse.GetRpcErrorCodeProto();
							}
							else
							{
								returnStatus = RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto.Error;
								detailedErr = RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto.ErrorApplication;
							}
							errorClass = e.GetType().FullName;
							error = StringUtils.StringifyException(e);
							// Remove redundant error class name from the beginning of the stack trace
							string exceptionHdr = errorClass + ": ";
							if (error.StartsWith(exceptionHdr))
							{
								error = Sharpen.Runtime.Substring(error, exceptionHdr.Length);
							}
						}
						Server.CurCall.Set(null);
						lock (call.connection.responseQueue)
						{
							// setupResponse() needs to be sync'ed together with 
							// responder.doResponse() since setupResponse may use
							// SASL to encrypt response data and SASL enforces
							// its own message ordering.
							this._enclosing.SetupResponse(buf, call, returnStatus, detailedErr, value, errorClass
								, error);
							// Discard the large buf and reset it back to smaller size 
							// to free up heap
							if (buf.Size() > this._enclosing.maxRespSize)
							{
								Server.Log.Warn("Large response size " + buf.Size() + " for call " + call.ToString
									());
								buf = new ByteArrayOutputStream(Server.InitialRespBufSize);
							}
							this._enclosing.responder.DoRespond(call);
						}
					}
					catch (Exception e)
					{
						if (this._enclosing.running)
						{
							// unexpected -- log it
							Server.Log.Info(Sharpen.Thread.CurrentThread().GetName() + " unexpectedly interrupted"
								, e);
							if (Trace.IsTracing())
							{
								traceScope.GetSpan().AddTimelineAnnotation("unexpectedly interrupted: " + StringUtils
									.StringifyException(e));
							}
						}
					}
					catch (Exception e)
					{
						Server.Log.Info(Sharpen.Thread.CurrentThread().GetName() + " caught an exception"
							, e);
						if (Trace.IsTracing())
						{
							traceScope.GetSpan().AddTimelineAnnotation("Exception: " + StringUtils.StringifyException
								(e));
						}
					}
					finally
					{
						if (traceScope != null)
						{
							traceScope.Close();
						}
						IOUtils.Cleanup(Server.Log, traceScope);
					}
				}
				Server.Log.Debug(Sharpen.Thread.CurrentThread().GetName() + ": exiting");
			}

			private sealed class _PrivilegedExceptionAction_2045 : PrivilegedExceptionAction<
				IWritable>
			{
				public _PrivilegedExceptionAction_2045(Handler _enclosing, Server.Call call)
				{
					this._enclosing = _enclosing;
					this.call = call;
				}

				/// <exception cref="System.Exception"/>
				public IWritable Run()
				{
					return this._enclosing._enclosing.Call(call.rpcKind, call.connection.protocolName
						, call.rpcRequest, call.timestamp);
				}

				private readonly Handler _enclosing;

				private readonly Server.Call call;
			}

			private readonly Server _enclosing;
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal Server(string bindAddress, int port, Type paramClass, int handlerCount
			, Configuration conf)
			: this(bindAddress, port, paramClass, handlerCount, -1, -1, conf, Sharpen.Extensions.ToString
				(port), null, null)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal Server(string bindAddress, int port, Type rpcRequestClass, int
			 handlerCount, int numReaders, int queueSizePerHandler, Configuration conf, string
			 serverName, SecretManager<TokenIdentifier> secretManager)
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
		protected internal Server(string bindAddress, int port, Type rpcRequestClass, int
			 handlerCount, int numReaders, int queueSizePerHandler, Configuration conf, string
			 serverName, SecretManager<TokenIdentifier> secretManager, string portRangeConfig
			)
		{
			this.bindAddress = bindAddress;
			this.conf = conf;
			this.portRangeConfig = portRangeConfig;
			this.port = port;
			this.rpcRequestClass = rpcRequestClass;
			this.handlerCount = handlerCount;
			this.socketSendBufferSize = 0;
			this.maxDataLength = conf.GetInt(CommonConfigurationKeys.IpcMaximumDataLength, CommonConfigurationKeys
				.IpcMaximumDataLengthDefault);
			if (queueSizePerHandler != -1)
			{
				this.maxQueueSize = queueSizePerHandler;
			}
			else
			{
				this.maxQueueSize = handlerCount * conf.GetInt(CommonConfigurationKeys.IpcServerHandlerQueueSizeKey
					, CommonConfigurationKeys.IpcServerHandlerQueueSizeDefault);
			}
			this.maxRespSize = conf.GetInt(CommonConfigurationKeys.IpcServerRpcMaxResponseSizeKey
				, CommonConfigurationKeys.IpcServerRpcMaxResponseSizeDefault);
			if (numReaders != -1)
			{
				this.readThreads = numReaders;
			}
			else
			{
				this.readThreads = conf.GetInt(CommonConfigurationKeys.IpcServerRpcReadThreadsKey
					, CommonConfigurationKeys.IpcServerRpcReadThreadsDefault);
			}
			this.readerPendingConnectionQueue = conf.GetInt(CommonConfigurationKeys.IpcServerRpcReadConnectionQueueSizeKey
				, CommonConfigurationKeys.IpcServerRpcReadConnectionQueueSizeDefault);
			// Setup appropriate callqueue
			string prefix = GetQueueClassPrefix();
			this.callQueue = new CallQueueManager<Server.Call>(GetQueueClass(prefix, conf), maxQueueSize
				, prefix, conf);
			this.secretManager = (SecretManager<TokenIdentifier>)secretManager;
			this.authorize = conf.GetBoolean(CommonConfigurationKeys.HadoopSecurityAuthorization
				, false);
			// configure supported authentications
			this.enabledAuthMethods = GetAuthMethods(secretManager, conf);
			this.negotiateResponse = BuildNegotiateResponse(enabledAuthMethods);
			// Start the listener here and let it bind to the port
			listener = new Server.Listener(this);
			this.port = listener.GetAddress().Port;
			connectionManager = new Server.ConnectionManager(this);
			this.rpcMetrics = RpcMetrics.Create(this, conf);
			this.rpcDetailedMetrics = RpcDetailedMetrics.Create(this.port);
			this.tcpNoDelay = conf.GetBoolean(CommonConfigurationKeysPublic.IpcServerTcpnodelayKey
				, CommonConfigurationKeysPublic.IpcServerTcpnodelayDefault);
			// Create the responder here
			responder = new Server.Responder(this);
			if (secretManager != null || UserGroupInformation.IsSecurityEnabled())
			{
				SaslRpcServer.Init(conf);
				saslPropsResolver = SaslPropertiesResolver.GetInstance(conf);
			}
			this.exceptionsHandler.AddTerseExceptions(typeof(StandbyException));
		}

		/// <exception cref="System.IO.IOException"/>
		private RpcHeaderProtos.RpcSaslProto BuildNegotiateResponse(IList<SaslRpcServer.AuthMethod
			> authMethods)
		{
			RpcHeaderProtos.RpcSaslProto.Builder negotiateBuilder = RpcHeaderProtos.RpcSaslProto
				.NewBuilder();
			if (authMethods.Contains(SaslRpcServer.AuthMethod.Simple) && authMethods.Count ==
				 1)
			{
				// SIMPLE-only servers return success in response to negotiate
				negotiateBuilder.SetState(RpcHeaderProtos.RpcSaslProto.SaslState.Success);
			}
			else
			{
				negotiateBuilder.SetState(RpcHeaderProtos.RpcSaslProto.SaslState.Negotiate);
				foreach (SaslRpcServer.AuthMethod authMethod in authMethods)
				{
					SaslRpcServer saslRpcServer = new SaslRpcServer(authMethod);
					RpcHeaderProtos.RpcSaslProto.SaslAuth.Builder builder = negotiateBuilder.AddAuthsBuilder
						().SetMethod(authMethod.ToString()).SetMechanism(saslRpcServer.mechanism);
					if (saslRpcServer.protocol != null)
					{
						builder.SetProtocol(saslRpcServer.protocol);
					}
					if (saslRpcServer.serverId != null)
					{
						builder.SetServerId(saslRpcServer.serverId);
					}
				}
			}
			return ((RpcHeaderProtos.RpcSaslProto)negotiateBuilder.Build());
		}

		// get the security type from the conf. implicitly include token support
		// if a secret manager is provided, or fail if token is the conf value but
		// there is no secret manager
		private IList<SaslRpcServer.AuthMethod> GetAuthMethods<_T0>(SecretManager<_T0> secretManager
			, Configuration conf)
			where _T0 : TokenIdentifier
		{
			UserGroupInformation.AuthenticationMethod confAuthenticationMethod = SecurityUtil
				.GetAuthenticationMethod(conf);
			IList<SaslRpcServer.AuthMethod> authMethods = new AList<SaslRpcServer.AuthMethod>
				();
			if (confAuthenticationMethod == UserGroupInformation.AuthenticationMethod.Token)
			{
				if (secretManager == null)
				{
					throw new ArgumentException(UserGroupInformation.AuthenticationMethod.Token + " authentication requires a secret manager"
						);
				}
			}
			else
			{
				if (secretManager != null)
				{
					Log.Debug(UserGroupInformation.AuthenticationMethod.Token + " authentication enabled for secret manager"
						);
					// most preferred, go to the front of the line!
					authMethods.AddItem(UserGroupInformation.AuthenticationMethod.Token.GetAuthMethod
						());
				}
			}
			authMethods.AddItem(confAuthenticationMethod.GetAuthMethod());
			Log.Debug("Server accepts auth methods:" + authMethods);
			return authMethods;
		}

		private void CloseConnection(Server.Connection connection)
		{
			connectionManager.Close(connection);
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
		private void SetupResponse(ByteArrayOutputStream responseBuf, Server.Call call, RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto
			 status, RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto erCode, IWritable
			 rv, string errorClass, string error)
		{
			responseBuf.Reset();
			DataOutputStream @out = new DataOutputStream(responseBuf);
			RpcHeaderProtos.RpcResponseHeaderProto.Builder headerBuilder = RpcHeaderProtos.RpcResponseHeaderProto
				.NewBuilder();
			headerBuilder.SetClientId(ByteString.CopyFrom(call.clientId));
			headerBuilder.SetCallId(call.callId);
			headerBuilder.SetRetryCount(call.retryCount);
			headerBuilder.SetStatus(status);
			headerBuilder.SetServerIpcVersionNum(RpcConstants.CurrentVersion);
			if (status == RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto.Success)
			{
				RpcHeaderProtos.RpcResponseHeaderProto header = ((RpcHeaderProtos.RpcResponseHeaderProto
					)headerBuilder.Build());
				int headerLen = header.GetSerializedSize();
				int fullLength = CodedOutputStream.ComputeRawVarint32Size(headerLen) + headerLen;
				try
				{
					if (rv is ProtobufRpcEngine.RpcWrapper)
					{
						ProtobufRpcEngine.RpcWrapper resWrapper = (ProtobufRpcEngine.RpcWrapper)rv;
						fullLength += resWrapper.GetLength();
						@out.WriteInt(fullLength);
						header.WriteDelimitedTo(@out);
						rv.Write(@out);
					}
					else
					{
						// Have to serialize to buffer to get len
						DataOutputBuffer buf = new DataOutputBuffer();
						rv.Write(buf);
						byte[] data = buf.GetData();
						fullLength += buf.GetLength();
						@out.WriteInt(fullLength);
						header.WriteDelimitedTo(@out);
						@out.Write(data, 0, buf.GetLength());
					}
				}
				catch (Exception t)
				{
					Log.Warn("Error serializing call response for call " + call, t);
					// Call back to same function - this is OK since the
					// buffer is reset at the top, and since status is changed
					// to ERROR it won't infinite loop.
					SetupResponse(responseBuf, call, RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto
						.Error, RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto.ErrorSerializingResponse
						, null, t.GetType().FullName, StringUtils.StringifyException(t));
					return;
				}
			}
			else
			{
				// Rpc Failure
				headerBuilder.SetExceptionClassName(errorClass);
				headerBuilder.SetErrorMsg(error);
				headerBuilder.SetErrorDetail(erCode);
				RpcHeaderProtos.RpcResponseHeaderProto header = ((RpcHeaderProtos.RpcResponseHeaderProto
					)headerBuilder.Build());
				int headerLen = header.GetSerializedSize();
				int fullLength = CodedOutputStream.ComputeRawVarint32Size(headerLen) + headerLen;
				@out.WriteInt(fullLength);
				header.WriteDelimitedTo(@out);
			}
			if (call.connection.useWrap)
			{
				WrapWithSasl(responseBuf, call);
			}
			call.SetResponse(ByteBuffer.Wrap(responseBuf.ToByteArray()));
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
		private void SetupResponseOldVersionFatal(ByteArrayOutputStream response, Server.Call
			 call, IWritable rv, string errorClass, string error)
		{
			int OldVersionFatalStatus = -1;
			response.Reset();
			DataOutputStream @out = new DataOutputStream(response);
			@out.WriteInt(call.callId);
			// write call id
			@out.WriteInt(OldVersionFatalStatus);
			// write FATAL_STATUS
			WritableUtils.WriteString(@out, errorClass);
			WritableUtils.WriteString(@out, error);
			if (call.connection.useWrap)
			{
				WrapWithSasl(response, call);
			}
			call.SetResponse(ByteBuffer.Wrap(response.ToByteArray()));
		}

		/// <exception cref="System.IO.IOException"/>
		private void WrapWithSasl(ByteArrayOutputStream response, Server.Call call)
		{
			if (call.connection.saslServer != null)
			{
				byte[] token = response.ToByteArray();
				// synchronization may be needed since there can be multiple Handler
				// threads using saslServer to wrap responses.
				lock (call.connection.saslServer)
				{
					token = call.connection.saslServer.Wrap(token, 0, token.Length);
				}
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Adding saslServer wrapped token of size " + token.Length + " as call response."
						);
				}
				response.Reset();
				// rebuild with sasl header and payload
				RpcHeaderProtos.RpcResponseHeaderProto saslHeader = ((RpcHeaderProtos.RpcResponseHeaderProto
					)RpcHeaderProtos.RpcResponseHeaderProto.NewBuilder().SetCallId(Server.AuthProtocol
					.Sasl.callId).SetStatus(RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto.Success
					).Build());
				RpcHeaderProtos.RpcSaslProto saslMessage = ((RpcHeaderProtos.RpcSaslProto)RpcHeaderProtos.RpcSaslProto
					.NewBuilder().SetState(RpcHeaderProtos.RpcSaslProto.SaslState.Wrap).SetToken(ByteString
					.CopyFrom(token, 0, token.Length)).Build());
				ProtobufRpcEngine.RpcResponseMessageWrapper saslResponse = new ProtobufRpcEngine.RpcResponseMessageWrapper
					(saslHeader, saslMessage);
				DataOutputStream @out = new DataOutputStream(response);
				@out.WriteInt(saslResponse.GetLength());
				saslResponse.Write(@out);
			}
		}

		internal virtual Configuration GetConf()
		{
			return conf;
		}

		/// <summary>Sets the socket buffer size used for responding to RPCs</summary>
		public virtual void SetSocketSendBufSize(int size)
		{
			this.socketSendBufferSize = size;
		}

		/// <summary>Starts the service.</summary>
		/// <remarks>Starts the service.  Must be called before any calls will be handled.</remarks>
		public virtual void Start()
		{
			lock (this)
			{
				responder.Start();
				listener.Start();
				handlers = new Server.Handler[handlerCount];
				for (int i = 0; i < handlerCount; i++)
				{
					handlers[i] = new Server.Handler(this, i);
					handlers[i].Start();
				}
			}
		}

		/// <summary>Stops the service.</summary>
		/// <remarks>Stops the service.  No new calls will be handled after this is called.</remarks>
		public virtual void Stop()
		{
			lock (this)
			{
				Log.Info("Stopping server on " + port);
				running = false;
				if (handlers != null)
				{
					for (int i = 0; i < handlerCount; i++)
					{
						if (handlers[i] != null)
						{
							handlers[i].Interrupt();
						}
					}
				}
				listener.Interrupt();
				listener.DoStop();
				responder.Interrupt();
				Sharpen.Runtime.NotifyAll(this);
				this.rpcMetrics.Shutdown();
				this.rpcDetailedMetrics.Shutdown();
			}
		}

		/// <summary>Wait for the server to be stopped.</summary>
		/// <remarks>
		/// Wait for the server to be stopped.
		/// Does not wait for all subthreads to finish.
		/// See
		/// <see cref="Stop()"/>
		/// .
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void Join()
		{
			lock (this)
			{
				while (running)
				{
					Sharpen.Runtime.Wait(this);
				}
			}
		}

		/// <summary>Return the socket (ip+port) on which the RPC server is listening to.</summary>
		/// <returns>the socket (ip+port) on which the RPC server is listening to.</returns>
		public virtual IPEndPoint GetListenerAddress()
		{
			lock (this)
			{
				return listener.GetAddress();
			}
		}

		/// <summary>Called for each call.</summary>
		/// <exception cref="System.Exception"/>
		[System.ObsoleteAttribute(@"Use  #call(RpcPayloadHeader.RpcKind,String,Writable,long) instead"
			)]
		public virtual IWritable Call(IWritable param, long receiveTime)
		{
			return Call(RPC.RpcKind.RpcBuiltin, null, param, receiveTime);
		}

		/// <summary>Called for each call.</summary>
		/// <exception cref="System.Exception"/>
		public abstract IWritable Call(RPC.RpcKind rpcKind, string protocol, IWritable param
			, long receiveTime);

		/// <summary>Authorize the incoming client connection.</summary>
		/// <param name="user">client user</param>
		/// <param name="protocolName">- the protocol</param>
		/// <param name="addr">InetAddress of incoming connection</param>
		/// <exception cref="Org.Apache.Hadoop.Security.Authorize.AuthorizationException">when the client isn't authorized to talk the protocol
		/// 	</exception>
		private void Authorize(UserGroupInformation user, string protocolName, IPAddress 
			addr)
		{
			if (authorize)
			{
				if (protocolName == null)
				{
					throw new AuthorizationException("Null protocol not authorized");
				}
				Type protocol = null;
				try
				{
					protocol = GetProtocolClass(protocolName, GetConf());
				}
				catch (TypeLoadException)
				{
					throw new AuthorizationException("Unknown protocol: " + protocolName);
				}
				serviceAuthorizationManager.Authorize(user, protocol, GetConf(), addr);
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
		public virtual int GetPort()
		{
			return port;
		}

		/// <summary>The number of open RPC conections</summary>
		/// <returns>the number of open rpc connections</returns>
		public virtual int GetNumOpenConnections()
		{
			return connectionManager.Size();
		}

		/// <summary>The number of rpc calls in the queue.</summary>
		/// <returns>The number of rpc calls in the queue.</returns>
		public virtual int GetCallQueueLen()
		{
			return callQueue.Size();
		}

		/// <summary>The maximum size of the rpc call queue of this server.</summary>
		/// <returns>The maximum size of the rpc call queue.</returns>
		public virtual int GetMaxQueueSize()
		{
			return maxQueueSize;
		}

		/// <summary>The number of reader threads for this server.</summary>
		/// <returns>The number of reader threads.</returns>
		public virtual int GetNumReaders()
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
		private static int NioBufferLimit = 8 * 1024;

		//should not be more than 64KB.
		/// <summary>
		/// This is a wrapper around
		/// <see cref="Sharpen.WritableByteChannel.Write(Sharpen.ByteBuffer)"/>
		/// .
		/// If the amount of data is large, it writes to channel in smaller chunks.
		/// This is to avoid jdk from creating many direct buffers as the size of
		/// buffer increases. This also minimizes extra copies in NIO layer
		/// as a result of multiple write operations required to write a large
		/// buffer.
		/// </summary>
		/// <seealso cref="Sharpen.WritableByteChannel.Write(Sharpen.ByteBuffer)"/>
		/// <exception cref="System.IO.IOException"/>
		private int ChannelWrite(WritableByteChannel channel, ByteBuffer buffer)
		{
			int count = (buffer.Remaining() <= NioBufferLimit) ? channel.Write(buffer) : ChannelIO
				(null, channel, buffer);
			if (count > 0)
			{
				rpcMetrics.IncrSentBytes(count);
			}
			return count;
		}

		/// <summary>
		/// This is a wrapper around
		/// <see cref="Sharpen.ReadableByteChannel.Read(Sharpen.ByteBuffer)"/>
		/// .
		/// If the amount of data is large, it writes to channel in smaller chunks.
		/// This is to avoid jdk from creating many direct buffers as the size of
		/// ByteBuffer increases. There should not be any performance degredation.
		/// </summary>
		/// <seealso cref="Sharpen.ReadableByteChannel.Read(Sharpen.ByteBuffer)"/>
		/// <exception cref="System.IO.IOException"/>
		private int ChannelRead(ReadableByteChannel channel, ByteBuffer buffer)
		{
			int count = (buffer.Remaining() <= NioBufferLimit) ? channel.Read(buffer) : ChannelIO
				(channel, null, buffer);
			if (count > 0)
			{
				rpcMetrics.IncrReceivedBytes(count);
			}
			return count;
		}

		/// <summary>
		/// Helper for
		/// <see cref="ChannelRead(Sharpen.ReadableByteChannel, Sharpen.ByteBuffer)"/>
		/// and
		/// <see cref="ChannelWrite(Sharpen.WritableByteChannel, Sharpen.ByteBuffer)"/>
		/// . Only
		/// one of readCh or writeCh should be non-null.
		/// </summary>
		/// <seealso cref="ChannelRead(Sharpen.ReadableByteChannel, Sharpen.ByteBuffer)"/>
		/// <seealso cref="ChannelWrite(Sharpen.WritableByteChannel, Sharpen.ByteBuffer)"/>
		/// <exception cref="System.IO.IOException"/>
		private static int ChannelIO(ReadableByteChannel readCh, WritableByteChannel writeCh
			, ByteBuffer buf)
		{
			int originalLimit = buf.Limit();
			int initialRemaining = buf.Remaining();
			int ret = 0;
			while (buf.Remaining() > 0)
			{
				try
				{
					int ioSize = Math.Min(buf.Remaining(), NioBufferLimit);
					buf.Limit(buf.Position() + ioSize);
					ret = (readCh == null) ? writeCh.Write(buf) : readCh.Read(buf);
					if (ret < ioSize)
					{
						break;
					}
				}
				finally
				{
					buf.Limit(originalLimit);
				}
			}
			int nBytes = initialRemaining - buf.Remaining();
			return (nBytes > 0) ? nBytes : ret;
		}

		private class ConnectionManager
		{
			private readonly AtomicInteger count = new AtomicInteger();

			private readonly ICollection<Server.Connection> connections;

			private readonly Timer idleScanTimer;

			private readonly int idleScanThreshold;

			private readonly int idleScanInterval;

			private readonly int maxIdleTime;

			private readonly int maxIdleToClose;

			private readonly int maxConnections;

			internal ConnectionManager(Server _enclosing)
			{
				this._enclosing = _enclosing;
				this.idleScanTimer = new Timer("IPC Server idle connection scanner for port " + this
					._enclosing.GetPort(), true);
				this.idleScanThreshold = this._enclosing.conf.GetInt(CommonConfigurationKeysPublic
					.IpcClientIdlethresholdKey, CommonConfigurationKeysPublic.IpcClientIdlethresholdDefault
					);
				this.idleScanInterval = this._enclosing.conf.GetInt(CommonConfigurationKeys.IpcClientConnectionIdlescanintervalKey
					, CommonConfigurationKeys.IpcClientConnectionIdlescanintervalDefault);
				this.maxIdleTime = 2 * this._enclosing.conf.GetInt(CommonConfigurationKeysPublic.
					IpcClientConnectionMaxidletimeKey, CommonConfigurationKeysPublic.IpcClientConnectionMaxidletimeDefault
					);
				this.maxIdleToClose = this._enclosing.conf.GetInt(CommonConfigurationKeysPublic.IpcClientKillMaxKey
					, CommonConfigurationKeysPublic.IpcClientKillMaxDefault);
				this.maxConnections = this._enclosing.conf.GetInt(CommonConfigurationKeysPublic.IpcServerMaxConnectionsKey
					, CommonConfigurationKeysPublic.IpcServerMaxConnectionsDefault);
				// create a set with concurrency -and- a thread-safe iterator, add 2
				// for listener and idle closer threads
				this.connections = Sharpen.Collections.NewSetFromMap(new ConcurrentHashMap<Server.Connection
					, bool>(this._enclosing.maxQueueSize, 0.75f, this._enclosing.readThreads + 2));
			}

			private bool Add(Server.Connection connection)
			{
				bool added = this.connections.AddItem(connection);
				if (added)
				{
					this.count.GetAndIncrement();
				}
				return added;
			}

			private bool Remove(Server.Connection connection)
			{
				bool removed = this.connections.Remove(connection);
				if (removed)
				{
					this.count.GetAndDecrement();
				}
				return removed;
			}

			internal virtual int Size()
			{
				return this.count.Get();
			}

			internal virtual bool IsFull()
			{
				// The check is disabled when maxConnections <= 0.
				return ((this.maxConnections > 0) && (this.Size() >= this.maxConnections));
			}

			internal virtual Server.Connection[] ToArray()
			{
				return Sharpen.Collections.ToArray(this.connections, new Server.Connection[0]);
			}

			internal virtual Server.Connection Register(SocketChannel channel)
			{
				if (this.IsFull())
				{
					return null;
				}
				Server.Connection connection = new Server.Connection(this, channel, Time.Now());
				this.Add(connection);
				if (Server.Log.IsDebugEnabled())
				{
					Server.Log.Debug("Server connection from " + connection + "; # active connections: "
						 + this.Size() + "; # queued calls: " + this._enclosing.callQueue.Size());
				}
				return connection;
			}

			internal virtual bool Close(Server.Connection connection)
			{
				bool exists = this.Remove(connection);
				if (exists)
				{
					if (Server.Log.IsDebugEnabled())
					{
						Server.Log.Debug(Sharpen.Thread.CurrentThread().GetName() + ": disconnecting client "
							 + connection + ". Number of active connections: " + this.Size());
					}
					// only close if actually removed to avoid double-closing due
					// to possible races
					connection.Close();
				}
				return exists;
			}

			// synch'ed to avoid explicit invocation upon OOM from colliding with
			// timer task firing
			internal virtual void CloseIdle(bool scanAll)
			{
				lock (this)
				{
					long minLastContact = Time.Now() - this.maxIdleTime;
					// concurrent iterator might miss new connections added
					// during the iteration, but that's ok because they won't
					// be idle yet anyway and will be caught on next scan
					int closed = 0;
					foreach (Server.Connection connection in this.connections)
					{
						// stop if connections dropped below threshold unless scanning all
						if (!scanAll && this.Size() < this.idleScanThreshold)
						{
							break;
						}
						// stop if not scanning all and max connections are closed
						if (connection.IsIdle() && connection.GetLastContact() < minLastContact && this.Close
							(connection) && !scanAll && (++closed == this.maxIdleToClose))
						{
							break;
						}
					}
				}
			}

			internal virtual void CloseAll()
			{
				// use a copy of the connections to be absolutely sure the concurrent
				// iterator doesn't miss a connection
				foreach (Server.Connection connection in this.ToArray())
				{
					this.Close(connection);
				}
			}

			internal virtual void StartIdleScan()
			{
				this.ScheduleIdleScanTask();
			}

			internal virtual void StopIdleScan()
			{
				this.idleScanTimer.Cancel();
			}

			private void ScheduleIdleScanTask()
			{
				if (!this._enclosing.running)
				{
					return;
				}
				TimerTask idleScanTask = new _TimerTask_2784(this);
				// explicitly reschedule so next execution occurs relative
				// to the end of this scan, not the beginning
				this.idleScanTimer.Schedule(idleScanTask, this.idleScanInterval);
			}

			private sealed class _TimerTask_2784 : TimerTask
			{
				public _TimerTask_2784(ConnectionManager _enclosing)
				{
					this._enclosing = _enclosing;
				}

				public override void Run()
				{
					if (!this._enclosing._enclosing.running)
					{
						return;
					}
					if (Server.Log.IsDebugEnabled())
					{
						Server.Log.Debug(Sharpen.Thread.CurrentThread().GetName() + ": task running");
					}
					try
					{
						this._enclosing.CloseIdle(false);
					}
					finally
					{
						this._enclosing.ScheduleIdleScanTask();
					}
				}

				private readonly ConnectionManager _enclosing;
			}

			private readonly Server _enclosing;
		}
	}
}
