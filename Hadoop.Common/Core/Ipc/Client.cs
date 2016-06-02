using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Com.Google.Common.Util.Concurrent;
using Com.Google.Protobuf;
using Hadoop.Common.Core.IO;
using Javax.Net;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.IO.Retry;
using Org.Apache.Hadoop.Ipc.Protobuf;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Util;
using Org.Apache.Htrace;
using Sharpen;

namespace Org.Apache.Hadoop.Ipc
{
	/// <summary>A client for an IPC service.</summary>
	/// <remarks>
	/// A client for an IPC service.  IPC calls take a single
	/// <see cref="Writable"/>
	/// as a
	/// parameter, and return a
	/// <see cref="Writable"/>
	/// as their value.  A service runs on
	/// a port and is defined by a parameter class and a value class.
	/// </remarks>
	/// <seealso cref="Server"/>
	public class Client
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Ipc.Client
			));

		/// <summary>A counter for generating call IDs.</summary>
		private static readonly AtomicInteger callIdCounter = new AtomicInteger();

		private static readonly ThreadLocal<int> callId = new ThreadLocal<int>();

		private static readonly ThreadLocal<int> retryCount = new ThreadLocal<int>();

		/// <summary>Set call id and retry count for the next call.</summary>
		public static void SetCallIdAndRetryCount(int cid, int rc)
		{
			Preconditions.CheckArgument(cid != RpcConstants.InvalidCallId);
			Preconditions.CheckState(callId.Get() == null);
			Preconditions.CheckArgument(rc != RpcConstants.InvalidRetryCount);
			callId.Set(cid);
			retryCount.Set(rc);
		}

		private Hashtable<Client.ConnectionId, Client.Connection> connections = new Hashtable
			<Client.ConnectionId, Client.Connection>();

		private Type valueClass;

		private AtomicBoolean running = new AtomicBoolean(true);

		private readonly Configuration conf;

		private SocketFactory socketFactory;

		private int refCount = 1;

		private readonly int connectionTimeout;

		private readonly bool fallbackAllowed;

		private readonly byte[] clientId;

		internal const int ConnectionContextCallId = -3;

		/// <summary>Executor on which IPC calls' parameters are sent.</summary>
		/// <remarks>
		/// Executor on which IPC calls' parameters are sent.
		/// Deferring the sending of parameters to a separate
		/// thread isolates them from thread interruptions in the
		/// calling code.
		/// </remarks>
		private readonly ExecutorService sendParamsExecutor;

		private static readonly Client.ClientExecutorServiceFactory clientExcecutorFactory
			 = new Client.ClientExecutorServiceFactory();

		private class ClientExecutorServiceFactory
		{
			private int executorRefCount = 0;

			private ExecutorService clientExecutor = null;

			// class of call values
			// if client runs
			// how to create sockets
			/// <summary>Get Executor on which IPC calls' parameters are sent.</summary>
			/// <remarks>
			/// Get Executor on which IPC calls' parameters are sent.
			/// If the internal reference counter is zero, this method
			/// creates the instance of Executor. If not, this method
			/// just returns the reference of clientExecutor.
			/// </remarks>
			/// <returns>An ExecutorService instance</returns>
			internal virtual ExecutorService RefAndGetInstance()
			{
				lock (this)
				{
					if (executorRefCount == 0)
					{
						clientExecutor = Executors.NewCachedThreadPool(new ThreadFactoryBuilder().SetDaemon
							(true).SetNameFormat("IPC Parameter Sending Thread #%d").Build());
					}
					executorRefCount++;
					return clientExecutor;
				}
			}

			/// <summary>Cleanup Executor on which IPC calls' parameters are sent.</summary>
			/// <remarks>
			/// Cleanup Executor on which IPC calls' parameters are sent.
			/// If reference counter is zero, this method discards the
			/// instance of the Executor. If not, this method
			/// just decrements the internal reference counter.
			/// </remarks>
			/// <returns>
			/// An ExecutorService instance if it exists.
			/// Null is returned if not.
			/// </returns>
			internal virtual ExecutorService UnrefAndCleanup()
			{
				lock (this)
				{
					executorRefCount--;
					System.Diagnostics.Debug.Assert((executorRefCount >= 0));
					if (executorRefCount == 0)
					{
						clientExecutor.Shutdown();
						try
						{
							if (!clientExecutor.AwaitTermination(1, TimeUnit.Minutes))
							{
								clientExecutor.ShutdownNow();
							}
						}
						catch (Exception)
						{
							Log.Warn("Interrupted while waiting for clientExecutor" + " to stop");
							clientExecutor.ShutdownNow();
							Sharpen.Thread.CurrentThread().Interrupt();
						}
						clientExecutor = null;
					}
					return clientExecutor;
				}
			}
		}

		/// <summary>set the ping interval value in configuration</summary>
		/// <param name="conf">Configuration</param>
		/// <param name="pingInterval">the ping interval</param>
		public static void SetPingInterval(Configuration conf, int pingInterval)
		{
			conf.SetInt(CommonConfigurationKeys.IpcPingIntervalKey, pingInterval);
		}

		/// <summary>
		/// Get the ping interval from configuration;
		/// If not set in the configuration, return the default value.
		/// </summary>
		/// <param name="conf">Configuration</param>
		/// <returns>the ping interval</returns>
		public static int GetPingInterval(Configuration conf)
		{
			return conf.GetInt(CommonConfigurationKeys.IpcPingIntervalKey, CommonConfigurationKeys
				.IpcPingIntervalDefault);
		}

		/// <summary>The time after which a RPC will timeout.</summary>
		/// <remarks>
		/// The time after which a RPC will timeout.
		/// If ping is not enabled (via ipc.client.ping), then the timeout value is the
		/// same as the pingInterval.
		/// If ping is enabled, then there is no timeout value.
		/// </remarks>
		/// <param name="conf">Configuration</param>
		/// <returns>the timeout period in milliseconds. -1 if no timeout value is set</returns>
		public static int GetTimeout(Configuration conf)
		{
			if (!conf.GetBoolean(CommonConfigurationKeys.IpcClientPingKey, CommonConfigurationKeys
				.IpcClientPingDefault))
			{
				return GetPingInterval(conf);
			}
			return -1;
		}

		/// <summary>set the connection timeout value in configuration</summary>
		/// <param name="conf">Configuration</param>
		/// <param name="timeout">the socket connect timeout value</param>
		public static void SetConnectTimeout(Configuration conf, int timeout)
		{
			conf.SetInt(CommonConfigurationKeys.IpcClientConnectTimeoutKey, timeout);
		}

		[VisibleForTesting]
		public static ExecutorService GetClientExecutor()
		{
			return Client.clientExcecutorFactory.clientExecutor;
		}

		/// <summary>Increment this client's reference count</summary>
		internal virtual void IncCount()
		{
			lock (this)
			{
				refCount++;
			}
		}

		/// <summary>Decrement this client's reference count</summary>
		internal virtual void DecCount()
		{
			lock (this)
			{
				refCount--;
			}
		}

		/// <summary>Return if this client has no reference</summary>
		/// <returns>true if this client has no reference; false otherwise</returns>
		internal virtual bool IsZeroReference()
		{
			lock (this)
			{
				return refCount == 0;
			}
		}

		/// <summary>Check the rpc response header.</summary>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void CheckResponse(RpcHeaderProtos.RpcResponseHeaderProto header
			)
		{
			if (header == null)
			{
				throw new EOFException("Response is null.");
			}
			if (header.HasClientId())
			{
				// check client IDs
				byte[] id = header.GetClientId().ToByteArray();
				if (!Arrays.Equals(id, RpcConstants.DummyClientId))
				{
					if (!Arrays.Equals(id, clientId))
					{
						throw new IOException("Client IDs not matched: local ID=" + StringUtils.ByteToHexString
							(clientId) + ", ID in response=" + StringUtils.ByteToHexString(header.GetClientId
							().ToByteArray()));
					}
				}
			}
		}

		internal virtual Client.Call CreateCall(RPC.RpcKind rpcKind, Writable rpcRequest)
		{
			return new Client.Call(rpcKind, rpcRequest);
		}

		/// <summary>Class that represents an RPC call</summary>
		internal class Call
		{
			internal readonly int id;

			internal readonly int retry;

			internal readonly Writable rpcRequest;

			internal Writable rpcResponse;

			internal IOException error;

			internal readonly RPC.RpcKind rpcKind;

			internal bool done;

			private Call(RPC.RpcKind rpcKind, Writable param)
			{
				// call id
				// retry count
				// the serialized rpc request
				// null if rpc has error
				// exception, null if success
				// Rpc EngineKind
				// true when call is done
				this.rpcKind = rpcKind;
				this.rpcRequest = param;
				int id = callId.Get();
				if (id == null)
				{
					this.id = NextCallId();
				}
				else
				{
					callId.Set(null);
					this.id = id;
				}
				int rc = retryCount.Get();
				if (rc == null)
				{
					this.retry = 0;
				}
				else
				{
					this.retry = rc;
				}
			}

			/// <summary>
			/// Indicate when the call is complete and the
			/// value or error are available.
			/// </summary>
			/// <remarks>
			/// Indicate when the call is complete and the
			/// value or error are available.  Notifies by default.
			/// </remarks>
			protected internal virtual void CallComplete()
			{
				lock (this)
				{
					this.done = true;
					Sharpen.Runtime.Notify(this);
				}
			}

			// notify caller
			/// <summary>Set the exception when there is an error.</summary>
			/// <remarks>
			/// Set the exception when there is an error.
			/// Notify the caller the call is done.
			/// </remarks>
			/// <param name="error">exception thrown by the call; either local or remote</param>
			public virtual void SetException(IOException error)
			{
				lock (this)
				{
					this.error = error;
					CallComplete();
				}
			}

			/// <summary>Set the return value when there is no error.</summary>
			/// <remarks>
			/// Set the return value when there is no error.
			/// Notify the caller the call is done.
			/// </remarks>
			/// <param name="rpcResponse">return value of the rpc call.</param>
			public virtual void SetRpcResponse(Writable rpcResponse)
			{
				lock (this)
				{
					this.rpcResponse = rpcResponse;
					CallComplete();
				}
			}

			public virtual Writable GetRpcResponse()
			{
				lock (this)
				{
					return rpcResponse;
				}
			}
		}

		/// <summary>Thread that reads responses and notifies callers.</summary>
		/// <remarks>
		/// Thread that reads responses and notifies callers.  Each connection owns a
		/// socket connected to a remote address.  Calls are multiplexed through this
		/// socket: responses may be delivered out of order.
		/// </remarks>
		private class Connection : Sharpen.Thread
		{
			private IPEndPoint server;

			private readonly Client.ConnectionId remoteId;

			private SaslRpcServer.AuthMethod authMethod;

			private Server.AuthProtocol authProtocol;

			private int serviceClass;

			private SaslRpcClient saslRpcClient;

			private Socket socket = null;

			private DataInputStream @in;

			private DataOutputStream @out;

			private int rpcTimeout;

			private int maxIdleTime;

			private readonly RetryPolicy connectionRetryPolicy;

			private readonly int maxRetriesOnSasl;

			private int maxRetriesOnSocketTimeouts;

			private bool tcpNoDelay;

			private bool doPing;

			private int pingInterval;

			private ByteArrayOutputStream pingRequest;

			private Hashtable<int, Client.Call> calls = new Hashtable<int, Client.Call>();

			private AtomicLong lastActivity = new AtomicLong();

			private AtomicBoolean shouldCloseConnection = new AtomicBoolean();

			private IOException closeException;

			private readonly object sendRpcRequestLock = new object();

			/// <exception cref="System.IO.IOException"/>
			public Connection(Client _enclosing, Client.ConnectionId remoteId, int serviceClass
				)
			{
				this._enclosing = _enclosing;
				// server ip:port
				// connection id
				// authentication method
				// connected socket
				//connections will be culled if it was idle for 
				//maxIdleTime msecs
				// if T then disable Nagle's Algorithm
				//do we need to send ping message
				// how often sends ping to the server in msecs
				// ping message
				// currently active calls
				// last I/O activity time
				// indicate if the connection is closed
				// close reason
				this.remoteId = remoteId;
				this.server = remoteId.GetAddress();
				if (this.server.IsUnresolved())
				{
					throw NetUtils.WrapException(this.server.GetHostName(), this.server.Port, null, 0
						, new UnknownHostException());
				}
				this.rpcTimeout = remoteId.GetRpcTimeout();
				this.maxIdleTime = remoteId.GetMaxIdleTime();
				this.connectionRetryPolicy = remoteId.connectionRetryPolicy;
				this.maxRetriesOnSasl = remoteId.GetMaxRetriesOnSasl();
				this.maxRetriesOnSocketTimeouts = remoteId.GetMaxRetriesOnSocketTimeouts();
				this.tcpNoDelay = remoteId.GetTcpNoDelay();
				this.doPing = remoteId.GetDoPing();
				if (this.doPing)
				{
					// construct a RPC header with the callId as the ping callId
					this.pingRequest = new ByteArrayOutputStream();
					RpcHeaderProtos.RpcRequestHeaderProto pingHeader = ProtoUtil.MakeRpcRequestHeader
						(RPC.RpcKind.RpcProtocolBuffer, RpcHeaderProtos.RpcRequestHeaderProto.OperationProto
						.RpcFinalPacket, RpcConstants.PingCallId, RpcConstants.InvalidRetryCount, this._enclosing
						.clientId);
					pingHeader.WriteDelimitedTo(this.pingRequest);
				}
				this.pingInterval = remoteId.GetPingInterval();
				this.serviceClass = serviceClass;
				if (Client.Log.IsDebugEnabled())
				{
					Client.Log.Debug("The ping interval is " + this.pingInterval + " ms.");
				}
				UserGroupInformation ticket = remoteId.GetTicket();
				// try SASL if security is enabled or if the ugi contains tokens.
				// this causes a SIMPLE client with tokens to attempt SASL
				bool trySasl = UserGroupInformation.IsSecurityEnabled() || (ticket != null && !ticket
					.GetTokens().IsEmpty());
				this.authProtocol = trySasl ? Server.AuthProtocol.Sasl : Server.AuthProtocol.None;
				this.SetName("IPC Client (" + this._enclosing.socketFactory.GetHashCode() + ") connection to "
					 + this.server.ToString() + " from " + ((ticket == null) ? "an unknown user" : ticket
					.GetUserName()));
				this.SetDaemon(true);
			}

			/// <summary>Update lastActivity with the current time.</summary>
			private void Touch()
			{
				this.lastActivity.Set(Time.Now());
			}

			/// <summary>
			/// Add a call to this connection's call queue and notify
			/// a listener; synchronized.
			/// </summary>
			/// <remarks>
			/// Add a call to this connection's call queue and notify
			/// a listener; synchronized.
			/// Returns false if called during shutdown.
			/// </remarks>
			/// <param name="call">to add</param>
			/// <returns>true if the call was added.</returns>
			private bool AddCall(Client.Call call)
			{
				lock (this)
				{
					if (this.shouldCloseConnection.Get())
					{
						return false;
					}
					this.calls[call.id] = call;
					Sharpen.Runtime.Notify(this);
					return true;
				}
			}

			/// <summary>
			/// This class sends a ping to the remote side when timeout on
			/// reading.
			/// </summary>
			/// <remarks>
			/// This class sends a ping to the remote side when timeout on
			/// reading. If no failure is detected, it retries until at least
			/// a byte is read.
			/// </remarks>
			private class PingInputStream : FilterInputStream
			{
				protected internal PingInputStream(Connection _enclosing, InputStream @in)
					: base(@in)
				{
					this._enclosing = _enclosing;
				}

				/* constructor */
				/* Process timeout exception
				* if the connection is not going to be closed or
				* is not configured to have a RPC timeout, send a ping.
				* (if rpcTimeout is not set to be 0, then RPC should timeout.
				* otherwise, throw the timeout exception.
				*/
				/// <exception cref="System.IO.IOException"/>
				private void HandleTimeout(SocketTimeoutException e)
				{
					if (this._enclosing.shouldCloseConnection.Get() || !this._enclosing._enclosing.running
						.Get() || this._enclosing.rpcTimeout > 0)
					{
						throw e;
					}
					else
					{
						this._enclosing.SendPing();
					}
				}

				/// <summary>Read a byte from the stream.</summary>
				/// <remarks>
				/// Read a byte from the stream.
				/// Send a ping if timeout on read. Retries if no failure is detected
				/// until a byte is read.
				/// </remarks>
				/// <exception cref="System.IO.IOException">for any IO problem other than socket timeout
				/// 	</exception>
				public override int Read()
				{
					do
					{
						try
						{
							return base.Read();
						}
						catch (SocketTimeoutException e)
						{
							this.HandleTimeout(e);
						}
					}
					while (true);
				}

				/// <summary>
				/// Read bytes into a buffer starting from offset <code>off</code>
				/// Send a ping if timeout on read.
				/// </summary>
				/// <remarks>
				/// Read bytes into a buffer starting from offset <code>off</code>
				/// Send a ping if timeout on read. Retries if no failure is detected
				/// until a byte is read.
				/// </remarks>
				/// <returns>the total number of bytes read; -1 if the connection is closed.</returns>
				/// <exception cref="System.IO.IOException"/>
				public override int Read(byte[] buf, int off, int len)
				{
					do
					{
						try
						{
							return base.Read(buf, off, len);
						}
						catch (SocketTimeoutException e)
						{
							this.HandleTimeout(e);
						}
					}
					while (true);
				}

				private readonly Connection _enclosing;
			}

			private void DisposeSasl()
			{
				lock (this)
				{
					if (this.saslRpcClient != null)
					{
						try
						{
							this.saslRpcClient.Dispose();
							this.saslRpcClient = null;
						}
						catch (IOException)
						{
						}
					}
				}
			}

			/// <exception cref="System.IO.IOException"/>
			private bool ShouldAuthenticateOverKrb()
			{
				lock (this)
				{
					UserGroupInformation loginUser = UserGroupInformation.GetLoginUser();
					UserGroupInformation currentUser = UserGroupInformation.GetCurrentUser();
					UserGroupInformation realUser = currentUser.GetRealUser();
					if (this.authMethod == SaslRpcServer.AuthMethod.Kerberos && loginUser != null && 
						loginUser.HasKerberosCredentials() && (loginUser.Equals(currentUser) || loginUser
						.Equals(realUser)))
					{
						// Make sure user logged in using Kerberos either keytab or TGT
						// relogin only in case it is the login user (e.g. JT)
						// or superuser (like oozie).
						return true;
					}
					return false;
				}
			}

			/// <exception cref="System.IO.IOException"/>
			private SaslRpcServer.AuthMethod SetupSaslConnection(InputStream in2, OutputStream
				 out2)
			{
				lock (this)
				{
					// Do not use Client.conf here! We must use ConnectionId.conf, since the
					// Client object is cached and shared between all RPC clients, even those
					// for separate services.
					this.saslRpcClient = new SaslRpcClient(this.remoteId.GetTicket(), this.remoteId.GetProtocol
						(), this.remoteId.GetAddress(), this.remoteId.conf);
					return this.saslRpcClient.SaslConnect(in2, out2);
				}
			}

			/// <summary>
			/// Update the server address if the address corresponding to the host
			/// name has changed.
			/// </summary>
			/// <returns>true if an addr change was detected.</returns>
			/// <exception cref="System.IO.IOException">when the hostname cannot be resolved.</exception>
			private bool UpdateAddress()
			{
				lock (this)
				{
					// Do a fresh lookup with the old host name.
					IPEndPoint currentAddr = NetUtils.CreateSocketAddrForHost(this.server.GetHostName
						(), this.server.Port);
					if (!this.server.Equals(currentAddr))
					{
						Client.Log.Warn("Address change detected. Old: " + this.server.ToString() + " New: "
							 + currentAddr.ToString());
						this.server = currentAddr;
						return true;
					}
					return false;
				}
			}

			/// <exception cref="System.IO.IOException"/>
			private void SetupConnection()
			{
				lock (this)
				{
					short ioFailures = 0;
					short timeoutFailures = 0;
					while (true)
					{
						try
						{
							this.socket = this._enclosing.socketFactory.CreateSocket();
							this.socket.NoDelay = this.tcpNoDelay;
							this.socket.SetKeepAlive(true);
							/*
							* Bind the socket to the host specified in the principal name of the
							* client, to ensure Server matching address of the client connection
							* to host name in principal passed.
							*/
							UserGroupInformation ticket = this.remoteId.GetTicket();
							if (ticket != null && ticket.HasKerberosCredentials())
							{
								KerberosInfo krbInfo = this.remoteId.GetProtocol().GetAnnotation<KerberosInfo>();
								if (krbInfo != null && krbInfo.ClientPrincipal() != null)
								{
									string host = SecurityUtil.GetHostFromPrincipal(this.remoteId.GetTicket().GetUserName
										());
									// If host name is a valid local address then bind socket to it
									IPAddress localAddr = NetUtils.GetLocalInetAddress(host);
									if (localAddr != null)
									{
										this.socket.Bind2(new IPEndPoint(localAddr, 0));
									}
								}
							}
							NetUtils.Connect(this.socket, this.server, this._enclosing.connectionTimeout);
							if (this.rpcTimeout > 0)
							{
								this.pingInterval = this.rpcTimeout;
							}
							// rpcTimeout overwrites pingInterval
							this.socket.ReceiveTimeout = this.pingInterval;
							return;
						}
						catch (ConnectTimeoutException toe)
						{
							/* Check for an address change and update the local reference.
							* Reset the failure counter if the address was changed
							*/
							if (this.UpdateAddress())
							{
								timeoutFailures = ioFailures = 0;
							}
							this.HandleConnectionTimeout(timeoutFailures++, this.maxRetriesOnSocketTimeouts, 
								toe);
						}
						catch (IOException ie)
						{
							if (this.UpdateAddress())
							{
								timeoutFailures = ioFailures = 0;
							}
							this.HandleConnectionFailure(ioFailures++, ie);
						}
					}
				}
			}

			/// <summary>
			/// If multiple clients with the same principal try to connect to the same
			/// server at the same time, the server assumes a replay attack is in
			/// progress.
			/// </summary>
			/// <remarks>
			/// If multiple clients with the same principal try to connect to the same
			/// server at the same time, the server assumes a replay attack is in
			/// progress. This is a feature of kerberos. In order to work around this,
			/// what is done is that the client backs off randomly and tries to initiate
			/// the connection again. The other problem is to do with ticket expiry. To
			/// handle that, a relogin is attempted.
			/// </remarks>
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			private void HandleSaslConnectionFailure(int currRetries, int maxRetries, Exception
				 ex, Random rand, UserGroupInformation ugi)
			{
				lock (this)
				{
					ugi.DoAs(new _PrivilegedExceptionAction_650(this, currRetries, maxRetries, ex, rand
						));
				}
			}

			private sealed class _PrivilegedExceptionAction_650 : PrivilegedExceptionAction<object
				>
			{
				public _PrivilegedExceptionAction_650(Connection _enclosing, int currRetries, int
					 maxRetries, Exception ex, Random rand)
				{
					this._enclosing = _enclosing;
					this.currRetries = currRetries;
					this.maxRetries = maxRetries;
					this.ex = ex;
					this.rand = rand;
				}

				/// <exception cref="System.IO.IOException"/>
				/// <exception cref="System.Exception"/>
				public object Run()
				{
					short MaxBackoff = 5000;
					this._enclosing.CloseConnection();
					this._enclosing.DisposeSasl();
					if (this._enclosing.ShouldAuthenticateOverKrb())
					{
						if (currRetries < maxRetries)
						{
							if (Client.Log.IsDebugEnabled())
							{
								Client.Log.Debug("Exception encountered while connecting to " + "the server : " +
									 ex);
							}
							// try re-login
							if (UserGroupInformation.IsLoginKeytabBased())
							{
								UserGroupInformation.GetLoginUser().ReloginFromKeytab();
							}
							else
							{
								if (UserGroupInformation.IsLoginTicketBased())
								{
									UserGroupInformation.GetLoginUser().ReloginFromTicketCache();
								}
							}
							// have granularity of milliseconds
							//we are sleeping with the Connection lock held but since this
							//connection instance is being used for connecting to the server
							//in question, it is okay
							Sharpen.Thread.Sleep((rand.Next(MaxBackoff) + 1));
							return null;
						}
						else
						{
							string msg = "Couldn't setup connection for " + UserGroupInformation.GetLoginUser
								().GetUserName() + " to " + this._enclosing.remoteId;
							Client.Log.Warn(msg, ex);
							throw (IOException)Sharpen.Extensions.InitCause(new IOException(msg), ex);
						}
					}
					else
					{
						Client.Log.Warn("Exception encountered while connecting to " + "the server : " + 
							ex);
					}
					if (ex is RemoteException)
					{
						throw (RemoteException)ex;
					}
					throw new IOException(ex);
				}

				private readonly Connection _enclosing;

				private readonly int currRetries;

				private readonly int maxRetries;

				private readonly Exception ex;

				private readonly Random rand;
			}

			/// <summary>Connect to the server and set up the I/O streams.</summary>
			/// <remarks>
			/// Connect to the server and set up the I/O streams. It then sends
			/// a header to the server and starts
			/// the connection thread that waits for responses.
			/// </remarks>
			private void SetupIOstreams(AtomicBoolean fallbackToSimpleAuth)
			{
				lock (this)
				{
					if (this.socket != null || this.shouldCloseConnection.Get())
					{
						return;
					}
					try
					{
						if (Client.Log.IsDebugEnabled())
						{
							Client.Log.Debug("Connecting to " + this.server);
						}
						if (Trace.IsTracing())
						{
							Trace.AddTimelineAnnotation("IPC client connecting to " + this.server);
						}
						short numRetries = 0;
						Random rand = null;
						while (true)
						{
							this.SetupConnection();
							InputStream inStream = NetUtils.GetInputStream(this.socket);
							OutputStream outStream = NetUtils.GetOutputStream(this.socket);
							this.WriteConnectionHeader(outStream);
							if (this.authProtocol == Server.AuthProtocol.Sasl)
							{
								InputStream in2 = inStream;
								OutputStream out2 = outStream;
								UserGroupInformation ticket = this.remoteId.GetTicket();
								if (ticket.GetRealUser() != null)
								{
									ticket = ticket.GetRealUser();
								}
								try
								{
									this.authMethod = ticket.DoAs(new _PrivilegedExceptionAction_725(this, in2, out2)
										);
								}
								catch (Exception ex)
								{
									this.authMethod = this.saslRpcClient.GetAuthMethod();
									if (rand == null)
									{
										rand = new Random();
									}
									this.HandleSaslConnectionFailure(numRetries++, this.maxRetriesOnSasl, ex, rand, ticket
										);
									continue;
								}
								if (this.authMethod != SaslRpcServer.AuthMethod.Simple)
								{
									// Sasl connect is successful. Let's set up Sasl i/o streams.
									inStream = this.saslRpcClient.GetInputStream(inStream);
									outStream = this.saslRpcClient.GetOutputStream(outStream);
									// for testing
									this.remoteId.saslQop = (string)this.saslRpcClient.GetNegotiatedProperty(Javax.Security.Sasl.Sasl
										.Qop);
									Client.Log.Debug("Negotiated QOP is :" + this.remoteId.saslQop);
									if (fallbackToSimpleAuth != null)
									{
										fallbackToSimpleAuth.Set(false);
									}
								}
								else
								{
									if (UserGroupInformation.IsSecurityEnabled())
									{
										if (!this._enclosing.fallbackAllowed)
										{
											throw new IOException("Server asks us to fall back to SIMPLE " + "auth, but this client is configured to only allow secure "
												 + "connections.");
										}
										if (fallbackToSimpleAuth != null)
										{
											fallbackToSimpleAuth.Set(true);
										}
									}
								}
							}
							if (this.doPing)
							{
								inStream = new Client.Connection.PingInputStream(this, inStream);
							}
							this.@in = new DataInputStream(new BufferedInputStream(inStream));
							// SASL may have already buffered the stream
							if (!(outStream is BufferedOutputStream))
							{
								outStream = new BufferedOutputStream(outStream);
							}
							this.@out = new DataOutputStream(outStream);
							this.WriteConnectionContext(this.remoteId, this.authMethod);
							// update last activity time
							this.Touch();
							if (Trace.IsTracing())
							{
								Trace.AddTimelineAnnotation("IPC client connected to " + this.server);
							}
							// start the receiver thread after the socket connection has been set
							// up
							this.Start();
							return;
						}
					}
					catch (Exception t)
					{
						if (t is IOException)
						{
							this.MarkClosed((IOException)t);
						}
						else
						{
							this.MarkClosed(new IOException("Couldn't set up IO streams", t));
						}
						this.Close();
					}
				}
			}

			private sealed class _PrivilegedExceptionAction_725 : PrivilegedExceptionAction<SaslRpcServer.AuthMethod
				>
			{
				public _PrivilegedExceptionAction_725(Connection _enclosing, InputStream in2, OutputStream
					 out2)
				{
					this._enclosing = _enclosing;
					this.in2 = in2;
					this.out2 = out2;
				}

				/// <exception cref="System.IO.IOException"/>
				/// <exception cref="System.Exception"/>
				public SaslRpcServer.AuthMethod Run()
				{
					return this._enclosing.SetupSaslConnection(in2, out2);
				}

				private readonly Connection _enclosing;

				private readonly InputStream in2;

				private readonly OutputStream out2;
			}

			private void CloseConnection()
			{
				if (this.socket == null)
				{
					return;
				}
				// close the current connection
				try
				{
					this.socket.Close();
				}
				catch (IOException e)
				{
					Client.Log.Warn("Not able to close a socket", e);
				}
				// set socket to null so that the next call to setupIOstreams
				// can start the process of connect all over again.
				this.socket = null;
			}

			/* Handle connection failures due to timeout on connect
			*
			* If the current number of retries is equal to the max number of retries,
			* stop retrying and throw the exception; Otherwise backoff 1 second and
			* try connecting again.
			*
			* This Method is only called from inside setupIOstreams(), which is
			* synchronized. Hence the sleep is synchronized; the locks will be retained.
			*
			* @param curRetries current number of retries
			* @param maxRetries max number of retries allowed
			* @param ioe failure reason
			* @throws IOException if max number of retries is reached
			*/
			/// <exception cref="System.IO.IOException"/>
			private void HandleConnectionTimeout(int curRetries, int maxRetries, IOException 
				ioe)
			{
				this.CloseConnection();
				// throw the exception if the maximum number of retries is reached
				if (curRetries >= maxRetries)
				{
					throw ioe;
				}
				Client.Log.Info("Retrying connect to server: " + this.server + ". Already tried "
					 + curRetries + " time(s); maxRetries=" + maxRetries);
			}

			/// <exception cref="System.IO.IOException"/>
			private void HandleConnectionFailure(int curRetries, IOException ioe)
			{
				this.CloseConnection();
				RetryPolicy.RetryAction action;
				try
				{
					action = this.connectionRetryPolicy.ShouldRetry(ioe, curRetries, 0, true);
				}
				catch (Exception e)
				{
					throw e is IOException ? (IOException)e : new IOException(e);
				}
				if (action.action == RetryPolicy.RetryAction.RetryDecision.Fail)
				{
					if (action.reason != null)
					{
						Client.Log.Warn("Failed to connect to server: " + this.server + ": " + action.reason
							, ioe);
					}
					throw ioe;
				}
				// Throw the exception if the thread is interrupted
				if (Sharpen.Thread.CurrentThread().IsInterrupted())
				{
					Client.Log.Warn("Interrupted while trying for connection");
					throw ioe;
				}
				try
				{
					Sharpen.Thread.Sleep(action.delayMillis);
				}
				catch (Exception e)
				{
					throw (IOException)Sharpen.Extensions.InitCause(new ThreadInterruptedException("Interrupted: action="
						 + action + ", retry policy=" + this.connectionRetryPolicy), e);
				}
				Client.Log.Info("Retrying connect to server: " + this.server + ". Already tried "
					 + curRetries + " time(s); retry policy is " + this.connectionRetryPolicy);
			}

			/// <summary>
			/// Write the connection header - this is sent when connection is established
			/// +----------------------------------+
			/// |  "hrpc" 4 bytes                  |
			/// +----------------------------------+
			/// |  Version (1 byte)                |
			/// +----------------------------------+
			/// |  Service Class (1 byte)          |
			/// +----------------------------------+
			/// |  AuthProtocol (1 byte)           |
			/// +----------------------------------+
			/// </summary>
			/// <exception cref="System.IO.IOException"/>
			private void WriteConnectionHeader(OutputStream outStream)
			{
				DataOutputStream @out = new DataOutputStream(new BufferedOutputStream(outStream));
				// Write out the header, version and authentication method
				@out.Write(((byte[])RpcConstants.Header.Array()));
				@out.Write(RpcConstants.CurrentVersion);
				@out.Write(this.serviceClass);
				@out.Write(this.authProtocol.callId);
				@out.Flush();
			}

			/* Write the connection context header for each connection
			* Out is not synchronized because only the first thread does this.
			*/
			/// <exception cref="System.IO.IOException"/>
			private void WriteConnectionContext(Client.ConnectionId remoteId, SaslRpcServer.AuthMethod
				 authMethod)
			{
				// Write out the ConnectionHeader
				IpcConnectionContextProtos.IpcConnectionContextProto message = ProtoUtil.MakeIpcConnectionContext
					(RPC.GetProtocolName(remoteId.GetProtocol()), remoteId.GetTicket(), authMethod);
				RpcHeaderProtos.RpcRequestHeaderProto connectionContextHeader = ProtoUtil.MakeRpcRequestHeader
					(RPC.RpcKind.RpcProtocolBuffer, RpcHeaderProtos.RpcRequestHeaderProto.OperationProto
					.RpcFinalPacket, Client.ConnectionContextCallId, RpcConstants.InvalidRetryCount, 
					this._enclosing.clientId);
				ProtobufRpcEngine.RpcRequestMessageWrapper request = new ProtobufRpcEngine.RpcRequestMessageWrapper
					(connectionContextHeader, message);
				// Write out the packet length
				this.@out.WriteInt(request.GetLength());
				request.Write(this.@out);
			}

			/* wait till someone signals us to start reading RPC response or
			* it is idle too long, it is marked as to be closed,
			* or the client is marked as not running.
			*
			* Return true if it is time to read a response; false otherwise.
			*/
			private bool WaitForWork()
			{
				lock (this)
				{
					if (this.calls.IsEmpty() && !this.shouldCloseConnection.Get() && this._enclosing.
						running.Get())
					{
						long timeout = this.maxIdleTime - (Time.Now() - this.lastActivity.Get());
						if (timeout > 0)
						{
							try
							{
								Sharpen.Runtime.Wait(this, timeout);
							}
							catch (Exception)
							{
							}
						}
					}
					if (!this.calls.IsEmpty() && !this.shouldCloseConnection.Get() && this._enclosing
						.running.Get())
					{
						return true;
					}
					else
					{
						if (this.shouldCloseConnection.Get())
						{
							return false;
						}
						else
						{
							if (this.calls.IsEmpty())
							{
								// idle connection closed or stopped
								this.MarkClosed(null);
								return false;
							}
							else
							{
								// get stopped but there are still pending requests 
								this.MarkClosed((IOException)Sharpen.Extensions.InitCause(new IOException(), new 
									Exception()));
								return false;
							}
						}
					}
				}
			}

			public virtual IPEndPoint GetRemoteAddress()
			{
				return this.server;
			}

			/* Send a ping to the server if the time elapsed
			* since last I/O activity is equal to or greater than the ping interval
			*/
			/// <exception cref="System.IO.IOException"/>
			private void SendPing()
			{
				lock (this)
				{
					long curTime = Time.Now();
					if (curTime - this.lastActivity.Get() >= this.pingInterval)
					{
						this.lastActivity.Set(curTime);
						lock (this.@out)
						{
							this.@out.WriteInt(this.pingRequest.Size());
							this.pingRequest.WriteTo(this.@out);
							this.@out.Flush();
						}
					}
				}
			}

			public override void Run()
			{
				if (Client.Log.IsDebugEnabled())
				{
					Client.Log.Debug(this.GetName() + ": starting, having connections " + this._enclosing
						.connections.Count);
				}
				try
				{
					while (this.WaitForWork())
					{
						//wait here for work - read or close connection
						this.ReceiveRpcResponse();
					}
				}
				catch (Exception t)
				{
					// This truly is unexpected, since we catch IOException in receiveResponse
					// -- this is only to be really sure that we don't leave a client hanging
					// forever.
					Client.Log.Warn("Unexpected error reading responses on connection " + this, t);
					this.MarkClosed(new IOException("Error reading responses", t));
				}
				this.Close();
				if (Client.Log.IsDebugEnabled())
				{
					Client.Log.Debug(this.GetName() + ": stopped, remaining connections " + this._enclosing
						.connections.Count);
				}
			}

			/// <summary>Initiates a rpc call by sending the rpc request to the remote server.</summary>
			/// <remarks>
			/// Initiates a rpc call by sending the rpc request to the remote server.
			/// Note: this is not called from the Connection thread, but by other
			/// threads.
			/// </remarks>
			/// <param name="call">- the rpc request</param>
			/// <exception cref="System.Exception"/>
			/// <exception cref="System.IO.IOException"/>
			public virtual void SendRpcRequest(Client.Call call)
			{
				if (this.shouldCloseConnection.Get())
				{
					return;
				}
				// Serialize the call to be sent. This is done from the actual
				// caller thread, rather than the sendParamsExecutor thread,
				// so that if the serialization throws an error, it is reported
				// properly. This also parallelizes the serialization.
				//
				// Format of a call on the wire:
				// 0) Length of rest below (1 + 2)
				// 1) RpcRequestHeader  - is serialized Delimited hence contains length
				// 2) RpcRequest
				//
				// Items '1' and '2' are prepared here. 
				DataOutputBuffer d = new DataOutputBuffer();
				RpcHeaderProtos.RpcRequestHeaderProto header = ProtoUtil.MakeRpcRequestHeader(call
					.rpcKind, RpcHeaderProtos.RpcRequestHeaderProto.OperationProto.RpcFinalPacket, call
					.id, call.retry, this._enclosing.clientId);
				header.WriteDelimitedTo(d);
				call.rpcRequest.Write(d);
				lock (this.sendRpcRequestLock)
				{
					Future<object> senderFuture = this._enclosing.sendParamsExecutor.Submit(new _Runnable_1027
						(this, call, d));
					// Total Length
					// RpcRequestHeader + RpcRequest
					// exception at this point would leave the connection in an
					// unrecoverable state (eg half a call left on the wire).
					// So, close the connection, killing any outstanding calls
					//the buffer is just an in-memory buffer, but it is still polite to
					// close early
					try
					{
						senderFuture.Get();
					}
					catch (ExecutionException e)
					{
						Exception cause = e.InnerException;
						// cause should only be a RuntimeException as the Runnable above
						// catches IOException
						if (cause is RuntimeException)
						{
							throw (RuntimeException)cause;
						}
						else
						{
							throw new RuntimeException("unexpected checked exception", cause);
						}
					}
				}
			}

			private sealed class _Runnable_1027 : Runnable
			{
				public _Runnable_1027(Connection _enclosing, Client.Call call, DataOutputBuffer d
					)
				{
					this._enclosing = _enclosing;
					this.call = call;
					this.d = d;
				}

				public void Run()
				{
					try
					{
						lock (this._enclosing.@out)
						{
							if (this._enclosing.shouldCloseConnection.Get())
							{
								return;
							}
							if (Client.Log.IsDebugEnabled())
							{
								Client.Log.Debug(this._enclosing.GetName() + " sending #" + call.id);
							}
							byte[] data = d.GetData();
							int totalLength = d.GetLength();
							this._enclosing.@out.WriteInt(totalLength);
							this._enclosing.@out.Write(data, 0, totalLength);
							this._enclosing.@out.Flush();
						}
					}
					catch (IOException e)
					{
						this._enclosing.MarkClosed(e);
					}
					finally
					{
						IOUtils.CloseStream(d);
					}
				}

				private readonly Connection _enclosing;

				private readonly Client.Call call;

				private readonly DataOutputBuffer d;
			}

			/* Receive a response.
			* Because only one receiver, so no synchronization on in.
			*/
			private void ReceiveRpcResponse()
			{
				if (this.shouldCloseConnection.Get())
				{
					return;
				}
				this.Touch();
				try
				{
					int totalLen = this.@in.ReadInt();
					RpcHeaderProtos.RpcResponseHeaderProto header = RpcHeaderProtos.RpcResponseHeaderProto
						.ParseDelimitedFrom(this.@in);
					this._enclosing.CheckResponse(header);
					int headerLen = header.GetSerializedSize();
					headerLen += CodedOutputStream.ComputeRawVarint32Size(headerLen);
					int callId = header.GetCallId();
					if (Client.Log.IsDebugEnabled())
					{
						Client.Log.Debug(this.GetName() + " got value #" + callId);
					}
					Client.Call call = this.calls[callId];
					RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto status = header.GetStatus();
					if (status == RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto.Success)
					{
						Writable value = ReflectionUtils.NewInstance(this._enclosing.valueClass, this._enclosing
							.conf);
						value.ReadFields(this.@in);
						// read value
						this.calls.Remove(callId);
						call.SetRpcResponse(value);
						// verify that length was correct
						// only for ProtobufEngine where len can be verified easily
						if (call.GetRpcResponse() is ProtobufRpcEngine.RpcWrapper)
						{
							ProtobufRpcEngine.RpcWrapper resWrapper = (ProtobufRpcEngine.RpcWrapper)call.GetRpcResponse
								();
							if (totalLen != headerLen + resWrapper.GetLength())
							{
								throw new RpcClientException("RPC response length mismatch on rpc success");
							}
						}
					}
					else
					{
						// Rpc Request failed
						// Verify that length was correct
						if (totalLen != headerLen)
						{
							throw new RpcClientException("RPC response length mismatch on rpc error");
						}
						string exceptionClassName = header.HasExceptionClassName() ? header.GetExceptionClassName
							() : "ServerDidNotSetExceptionClassName";
						string errorMsg = header.HasErrorMsg() ? header.GetErrorMsg() : "ServerDidNotSetErrorMsg";
						RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto erCode = (header.HasErrorDetail
							() ? header.GetErrorDetail() : null);
						if (erCode == null)
						{
							Client.Log.Warn("Detailed error code not set by server on rpc error");
						}
						RemoteException re = ((erCode == null) ? new RemoteException(exceptionClassName, 
							errorMsg) : new RemoteException(exceptionClassName, errorMsg, erCode));
						if (status == RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto.Error)
						{
							this.calls.Remove(callId);
							call.SetException(re);
						}
						else
						{
							if (status == RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto.Fatal)
							{
								// Close the connection
								this.MarkClosed(re);
							}
						}
					}
				}
				catch (IOException e)
				{
					this.MarkClosed(e);
				}
			}

			private void MarkClosed(IOException e)
			{
				lock (this)
				{
					if (this.shouldCloseConnection.CompareAndSet(false, true))
					{
						this.closeException = e;
						Sharpen.Runtime.NotifyAll(this);
					}
				}
			}

			/// <summary>Close the connection.</summary>
			private void Close()
			{
				lock (this)
				{
					if (!this.shouldCloseConnection.Get())
					{
						Client.Log.Error("The connection is not in the closed state");
						return;
					}
					// release the resources
					// first thing to do;take the connection out of the connection list
					lock (this._enclosing.connections)
					{
						if (this._enclosing.connections[this.remoteId] == this)
						{
							this._enclosing.connections.Remove(this.remoteId);
						}
					}
					// close the streams and therefore the socket
					IOUtils.CloseStream(this.@out);
					IOUtils.CloseStream(this.@in);
					this.DisposeSasl();
					// clean up all calls
					if (this.closeException == null)
					{
						if (!this.calls.IsEmpty())
						{
							Client.Log.Warn("A connection is closed for no cause and calls are not empty");
							// clean up calls anyway
							this.closeException = new IOException("Unexpected closed connection");
							this.CleanupCalls();
						}
					}
					else
					{
						// log the info
						if (Client.Log.IsDebugEnabled())
						{
							Client.Log.Debug("closing ipc connection to " + this.server + ": " + this.closeException
								.Message, this.closeException);
						}
						// cleanup calls
						this.CleanupCalls();
					}
					this.CloseConnection();
					if (Client.Log.IsDebugEnabled())
					{
						Client.Log.Debug(this.GetName() + ": closed");
					}
				}
			}

			/* Cleanup all calls and mark them as done */
			private void CleanupCalls()
			{
				IEnumerator<KeyValuePair<int, Client.Call>> itor = this.calls.GetEnumerator();
				while (itor.HasNext())
				{
					Client.Call c = itor.Next().Value;
					itor.Remove();
					c.SetException(this.closeException);
				}
			}

			private readonly Client _enclosing;
			// local exception
		}

		/// <summary>
		/// Construct an IPC client whose values are of the given
		/// <see cref="Writable"/>
		/// class.
		/// </summary>
		public Client(Type valueClass, Configuration conf, SocketFactory factory)
		{
			this.valueClass = valueClass;
			this.conf = conf;
			this.socketFactory = factory;
			this.connectionTimeout = conf.GetInt(CommonConfigurationKeys.IpcClientConnectTimeoutKey
				, CommonConfigurationKeys.IpcClientConnectTimeoutDefault);
			this.fallbackAllowed = conf.GetBoolean(CommonConfigurationKeys.IpcClientFallbackToSimpleAuthAllowedKey
				, CommonConfigurationKeys.IpcClientFallbackToSimpleAuthAllowedDefault);
			this.clientId = ClientId.GetClientId();
			this.sendParamsExecutor = clientExcecutorFactory.RefAndGetInstance();
		}

		/// <summary>Construct an IPC client with the default SocketFactory</summary>
		/// <param name="valueClass"/>
		/// <param name="conf"/>
		public Client(Type valueClass, Configuration conf)
			: this(valueClass, conf, NetUtils.GetDefaultSocketFactory(conf))
		{
		}

		/// <summary>Return the socket factory of this client</summary>
		/// <returns>this client's socket factory</returns>
		internal virtual SocketFactory GetSocketFactory()
		{
			return socketFactory;
		}

		/// <summary>Stop all threads related to this client.</summary>
		/// <remarks>
		/// Stop all threads related to this client.  No further calls may be made
		/// using this client.
		/// </remarks>
		public virtual void Stop()
		{
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Stopping client");
			}
			if (!running.CompareAndSet(true, false))
			{
				return;
			}
			// wake up all connections
			lock (connections)
			{
				foreach (Client.Connection conn in connections.Values)
				{
					conn.Interrupt();
				}
			}
			// wait until all connections are closed
			while (!connections.IsEmpty())
			{
				try
				{
					Sharpen.Thread.Sleep(100);
				}
				catch (Exception)
				{
				}
			}
			clientExcecutorFactory.UnrefAndCleanup();
		}

		/// <summary>
		/// Same as
		/// <see cref="Call(RpcKind, Writable, ConnectionId)"/>
		/// for RPC_BUILTIN
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual Writable Call(Writable param, IPEndPoint address)
		{
			return Call(RPC.RpcKind.RpcBuiltin, param, address);
		}

		/// <summary>
		/// Make a call, passing <code>param</code>, to the IPC server running at
		/// <code>address</code>, returning the value.
		/// </summary>
		/// <remarks>
		/// Make a call, passing <code>param</code>, to the IPC server running at
		/// <code>address</code>, returning the value.  Throws exceptions if there are
		/// network problems or if the remote code threw an exception.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[System.ObsoleteAttribute(@"Use Call(RpcKind, Org.Apache.Hadoop.IO.Writable, ConnectionId) instead"
			)]
		public virtual Writable Call(RPC.RpcKind rpcKind, Writable param, IPEndPoint address
			)
		{
			return Call(rpcKind, param, address, null);
		}

		/// <summary>
		/// Make a call, passing <code>param</code>, to the IPC server running at
		/// <code>address</code> with the <code>ticket</code> credentials, returning
		/// the value.
		/// </summary>
		/// <remarks>
		/// Make a call, passing <code>param</code>, to the IPC server running at
		/// <code>address</code> with the <code>ticket</code> credentials, returning
		/// the value.
		/// Throws exceptions if there are network problems or if the remote code
		/// threw an exception.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[System.ObsoleteAttribute(@"Use Call(RpcKind, Org.Apache.Hadoop.IO.Writable, ConnectionId) instead"
			)]
		public virtual Writable Call(RPC.RpcKind rpcKind, Writable param, IPEndPoint addr
			, UserGroupInformation ticket)
		{
			Client.ConnectionId remoteId = Client.ConnectionId.GetConnectionId(addr, null, ticket
				, 0, conf);
			return Call(rpcKind, param, remoteId);
		}

		/// <summary>
		/// Make a call, passing <code>param</code>, to the IPC server running at
		/// <code>address</code> which is servicing the <code>protocol</code> protocol,
		/// with the <code>ticket</code> credentials and <code>rpcTimeout</code> as
		/// timeout, returning the value.
		/// </summary>
		/// <remarks>
		/// Make a call, passing <code>param</code>, to the IPC server running at
		/// <code>address</code> which is servicing the <code>protocol</code> protocol,
		/// with the <code>ticket</code> credentials and <code>rpcTimeout</code> as
		/// timeout, returning the value.
		/// Throws exceptions if there are network problems or if the remote code
		/// threw an exception.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[System.ObsoleteAttribute(@"Use Call(RpcKind, Org.Apache.Hadoop.IO.Writable, ConnectionId) instead"
			)]
		public virtual Writable Call(RPC.RpcKind rpcKind, Writable param, IPEndPoint addr
			, Type protocol, UserGroupInformation ticket, int rpcTimeout)
		{
			Client.ConnectionId remoteId = Client.ConnectionId.GetConnectionId(addr, protocol
				, ticket, rpcTimeout, conf);
			return Call(rpcKind, param, remoteId);
		}

		/// <summary>
		/// Same as
		/// <see cref="Call(RpcKind, Writable, System.Net.IPEndPoint, System.Type{T}, Org.Apache.Hadoop.Security.UserGroupInformation, int, Org.Apache.Hadoop.Conf.Configuration)
		/// 	"/>
		/// except that rpcKind is writable.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual Writable Call(Writable param, IPEndPoint addr, Type protocol, UserGroupInformation
			 ticket, int rpcTimeout, Configuration conf)
		{
			Client.ConnectionId remoteId = Client.ConnectionId.GetConnectionId(addr, protocol
				, ticket, rpcTimeout, conf);
			return Call(RPC.RpcKind.RpcBuiltin, param, remoteId);
		}

		/// <summary>
		/// Same as
		/// <see cref="Call(Writable, System.Net.IPEndPoint, System.Type{T}, Org.Apache.Hadoop.Security.UserGroupInformation, int, Org.Apache.Hadoop.Conf.Configuration)
		/// 	"/>
		/// except that specifying serviceClass.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual Writable Call(Writable param, IPEndPoint addr, Type protocol, UserGroupInformation
			 ticket, int rpcTimeout, int serviceClass, Configuration conf)
		{
			Client.ConnectionId remoteId = Client.ConnectionId.GetConnectionId(addr, protocol
				, ticket, rpcTimeout, conf);
			return Call(RPC.RpcKind.RpcBuiltin, param, remoteId, serviceClass);
		}

		/// <summary>
		/// Make a call, passing <code>param</code>, to the IPC server running at
		/// <code>address</code> which is servicing the <code>protocol</code> protocol,
		/// with the <code>ticket</code> credentials, <code>rpcTimeout</code> as
		/// timeout and <code>conf</code> as conf for this connection, returning the
		/// value.
		/// </summary>
		/// <remarks>
		/// Make a call, passing <code>param</code>, to the IPC server running at
		/// <code>address</code> which is servicing the <code>protocol</code> protocol,
		/// with the <code>ticket</code> credentials, <code>rpcTimeout</code> as
		/// timeout and <code>conf</code> as conf for this connection, returning the
		/// value. Throws exceptions if there are network problems or if the remote
		/// code threw an exception.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public virtual Writable Call(RPC.RpcKind rpcKind, Writable param, IPEndPoint addr
			, Type protocol, UserGroupInformation ticket, int rpcTimeout, Configuration conf
			)
		{
			Client.ConnectionId remoteId = Client.ConnectionId.GetConnectionId(addr, protocol
				, ticket, rpcTimeout, conf);
			return Call(rpcKind, param, remoteId);
		}

		/// <summary>
		/// Same as {link
		/// <see cref="Call(RpcKind, Writable, ConnectionId)"/>
		/// except the rpcKind is RPC_BUILTIN
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual Writable Call(Writable param, Client.ConnectionId remoteId)
		{
			return Call(RPC.RpcKind.RpcBuiltin, param, remoteId);
		}

		/// <summary>
		/// Make a call, passing <code>rpcRequest</code>, to the IPC server defined by
		/// <code>remoteId</code>, returning the rpc respond.
		/// </summary>
		/// <param name="rpcKind"/>
		/// <param name="rpcRequest">-  contains serialized method and method parameters</param>
		/// <param name="remoteId">- the target rpc server</param>
		/// <returns>
		/// the rpc response
		/// Throws exceptions if there are network problems or if the remote code
		/// threw an exception.
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual Writable Call(RPC.RpcKind rpcKind, Writable rpcRequest, Client.ConnectionId
			 remoteId)
		{
			return Call(rpcKind, rpcRequest, remoteId, RPC.RpcServiceClassDefault);
		}

		/// <summary>
		/// Make a call, passing <code>rpcRequest</code>, to the IPC server defined by
		/// <code>remoteId</code>, returning the rpc respond.
		/// </summary>
		/// <param name="rpcKind"/>
		/// <param name="rpcRequest">-  contains serialized method and method parameters</param>
		/// <param name="remoteId">- the target rpc server</param>
		/// <param name="fallbackToSimpleAuth">
		/// - set to true or false during this method to
		/// indicate if a secure client falls back to simple auth
		/// </param>
		/// <returns>
		/// the rpc response
		/// Throws exceptions if there are network problems or if the remote code
		/// threw an exception.
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual Writable Call(RPC.RpcKind rpcKind, Writable rpcRequest, Client.ConnectionId
			 remoteId, AtomicBoolean fallbackToSimpleAuth)
		{
			return Call(rpcKind, rpcRequest, remoteId, RPC.RpcServiceClassDefault, fallbackToSimpleAuth
				);
		}

		/// <summary>
		/// Make a call, passing <code>rpcRequest</code>, to the IPC server defined by
		/// <code>remoteId</code>, returning the rpc response.
		/// </summary>
		/// <param name="rpcKind"/>
		/// <param name="rpcRequest">-  contains serialized method and method parameters</param>
		/// <param name="remoteId">- the target rpc server</param>
		/// <param name="serviceClass">- service class for RPC</param>
		/// <returns>
		/// the rpc response
		/// Throws exceptions if there are network problems or if the remote code
		/// threw an exception.
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual Writable Call(RPC.RpcKind rpcKind, Writable rpcRequest, Client.ConnectionId
			 remoteId, int serviceClass)
		{
			return Call(rpcKind, rpcRequest, remoteId, serviceClass, null);
		}

		/// <summary>
		/// Make a call, passing <code>rpcRequest</code>, to the IPC server defined by
		/// <code>remoteId</code>, returning the rpc response.
		/// </summary>
		/// <param name="rpcKind"/>
		/// <param name="rpcRequest">-  contains serialized method and method parameters</param>
		/// <param name="remoteId">- the target rpc server</param>
		/// <param name="serviceClass">- service class for RPC</param>
		/// <param name="fallbackToSimpleAuth">
		/// - set to true or false during this method to
		/// indicate if a secure client falls back to simple auth
		/// </param>
		/// <returns>
		/// the rpc response
		/// Throws exceptions if there are network problems or if the remote code
		/// threw an exception.
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual Writable Call(RPC.RpcKind rpcKind, Writable rpcRequest, Client.ConnectionId
			 remoteId, int serviceClass, AtomicBoolean fallbackToSimpleAuth)
		{
			Client.Call call = CreateCall(rpcKind, rpcRequest);
			Client.Connection connection = GetConnection(remoteId, call, serviceClass, fallbackToSimpleAuth
				);
			try
			{
				connection.SendRpcRequest(call);
			}
			catch (RejectedExecutionException e)
			{
				// send the rpc request
				throw new IOException("connection has been closed", e);
			}
			catch (Exception e)
			{
				Sharpen.Thread.CurrentThread().Interrupt();
				Log.Warn("interrupted waiting to send rpc request to server", e);
				throw new IOException(e);
			}
			lock (call)
			{
				while (!call.done)
				{
					try
					{
						Sharpen.Runtime.Wait(call);
					}
					catch (Exception)
					{
						// wait for the result
						Sharpen.Thread.CurrentThread().Interrupt();
						throw new ThreadInterruptedException("Call interrupted");
					}
				}
				if (call.error != null)
				{
					if (call.error is RemoteException)
					{
						call.error.FillInStackTrace();
						throw call.error;
					}
					else
					{
						// local exception
						IPEndPoint address = connection.GetRemoteAddress();
						throw NetUtils.WrapException(address.GetHostName(), address.Port, NetUtils.GetHostname
							(), 0, call.error);
					}
				}
				else
				{
					return call.GetRpcResponse();
				}
			}
		}

		// for unit testing only
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		internal virtual ICollection<Client.ConnectionId> GetConnectionIds()
		{
			lock (connections)
			{
				return connections.Keys;
			}
		}

		/// <summary>
		/// Get a connection from the pool, or create a new one and add it to the
		/// pool.
		/// </summary>
		/// <remarks>
		/// Get a connection from the pool, or create a new one and add it to the
		/// pool.  Connections to a given ConnectionId are reused.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		private Client.Connection GetConnection(Client.ConnectionId remoteId, Client.Call
			 call, int serviceClass, AtomicBoolean fallbackToSimpleAuth)
		{
			if (!running.Get())
			{
				// the client is stopped
				throw new IOException("The client is stopped");
			}
			Client.Connection connection;
			do
			{
				/* we could avoid this allocation for each RPC by having a
				* connectionsId object and with set() method. We need to manage the
				* refs for keys in HashMap properly. For now its ok.
				*/
				lock (connections)
				{
					connection = connections[remoteId];
					if (connection == null)
					{
						connection = new Client.Connection(this, remoteId, serviceClass);
						connections[remoteId] = connection;
					}
				}
			}
			while (!connection.AddCall(call));
			//we don't invoke the method below inside "synchronized (connections)"
			//block above. The reason for that is if the server happens to be slow,
			//it will take longer to establish a connection and that will slow the
			//entire system down.
			connection.SetupIOstreams(fallbackToSimpleAuth);
			return connection;
		}

		/// <summary>This class holds the address and the user ticket.</summary>
		/// <remarks>
		/// This class holds the address and the user ticket. The client connections
		/// to servers are uniquely identified by <remoteAddress, protocol, ticket>
		/// </remarks>
		public class ConnectionId
		{
			internal IPEndPoint address;

			internal UserGroupInformation ticket;

			internal readonly Type protocol;

			private const int Prime = 16777619;

			private readonly int rpcTimeout;

			private readonly int maxIdleTime;

			private readonly RetryPolicy connectionRetryPolicy;

			private readonly int maxRetriesOnSasl;

			private readonly int maxRetriesOnSocketTimeouts;

			private readonly bool tcpNoDelay;

			private readonly bool doPing;

			private readonly int pingInterval;

			private string saslQop;

			private readonly Configuration conf;

			internal ConnectionId(IPEndPoint address, Type protocol, UserGroupInformation ticket
				, int rpcTimeout, RetryPolicy connectionRetryPolicy, Configuration conf)
			{
				//connections will be culled if it was idle for 
				//maxIdleTime msecs
				// the max. no. of retries for socket connections on time out exceptions
				// if T then disable Nagle's Algorithm
				//do we need to send ping message
				// how often sends ping to the server in msecs
				// here for testing
				// used to get the expected kerberos principal name
				this.protocol = protocol;
				this.address = address;
				this.ticket = ticket;
				this.rpcTimeout = rpcTimeout;
				this.connectionRetryPolicy = connectionRetryPolicy;
				this.maxIdleTime = conf.GetInt(CommonConfigurationKeysPublic.IpcClientConnectionMaxidletimeKey
					, CommonConfigurationKeysPublic.IpcClientConnectionMaxidletimeDefault);
				this.maxRetriesOnSasl = conf.GetInt(CommonConfigurationKeys.IpcClientConnectMaxRetriesOnSaslKey
					, CommonConfigurationKeys.IpcClientConnectMaxRetriesOnSaslDefault);
				this.maxRetriesOnSocketTimeouts = conf.GetInt(CommonConfigurationKeysPublic.IpcClientConnectMaxRetriesOnSocketTimeoutsKey
					, CommonConfigurationKeysPublic.IpcClientConnectMaxRetriesOnSocketTimeoutsDefault
					);
				this.tcpNoDelay = conf.GetBoolean(CommonConfigurationKeysPublic.IpcClientTcpnodelayKey
					, CommonConfigurationKeysPublic.IpcClientTcpnodelayDefault);
				this.doPing = conf.GetBoolean(CommonConfigurationKeys.IpcClientPingKey, CommonConfigurationKeys
					.IpcClientPingDefault);
				this.pingInterval = (doPing ? Client.GetPingInterval(conf) : 0);
				this.conf = conf;
			}

			internal virtual IPEndPoint GetAddress()
			{
				return address;
			}

			internal virtual Type GetProtocol()
			{
				return protocol;
			}

			internal virtual UserGroupInformation GetTicket()
			{
				return ticket;
			}

			private int GetRpcTimeout()
			{
				return rpcTimeout;
			}

			internal virtual int GetMaxIdleTime()
			{
				return maxIdleTime;
			}

			public virtual int GetMaxRetriesOnSasl()
			{
				return maxRetriesOnSasl;
			}

			/// <summary>max connection retries on socket time outs</summary>
			public virtual int GetMaxRetriesOnSocketTimeouts()
			{
				return maxRetriesOnSocketTimeouts;
			}

			internal virtual bool GetTcpNoDelay()
			{
				return tcpNoDelay;
			}

			internal virtual bool GetDoPing()
			{
				return doPing;
			}

			internal virtual int GetPingInterval()
			{
				return pingInterval;
			}

			[VisibleForTesting]
			internal virtual string GetSaslQop()
			{
				return saslQop;
			}

			/// <exception cref="System.IO.IOException"/>
			internal static Client.ConnectionId GetConnectionId(IPEndPoint addr, Type protocol
				, UserGroupInformation ticket, int rpcTimeout, Configuration conf)
			{
				return GetConnectionId(addr, protocol, ticket, rpcTimeout, null, conf);
			}

			/// <summary>Returns a ConnectionId object.</summary>
			/// <param name="addr">Remote address for the connection.</param>
			/// <param name="protocol">Protocol for RPC.</param>
			/// <param name="ticket">UGI</param>
			/// <param name="rpcTimeout">timeout</param>
			/// <param name="conf">Configuration object</param>
			/// <returns>A ConnectionId instance</returns>
			/// <exception cref="System.IO.IOException"/>
			internal static Client.ConnectionId GetConnectionId(IPEndPoint addr, Type protocol
				, UserGroupInformation ticket, int rpcTimeout, RetryPolicy connectionRetryPolicy
				, Configuration conf)
			{
				if (connectionRetryPolicy == null)
				{
					int max = conf.GetInt(CommonConfigurationKeysPublic.IpcClientConnectMaxRetriesKey
						, CommonConfigurationKeysPublic.IpcClientConnectMaxRetriesDefault);
					int retryInterval = conf.GetInt(CommonConfigurationKeysPublic.IpcClientConnectRetryIntervalKey
						, CommonConfigurationKeysPublic.IpcClientConnectRetryIntervalDefault);
					connectionRetryPolicy = RetryPolicies.RetryUpToMaximumCountWithFixedSleep(max, retryInterval
						, TimeUnit.Milliseconds);
				}
				return new Client.ConnectionId(addr, protocol, ticket, rpcTimeout, connectionRetryPolicy
					, conf);
			}

			internal static bool IsEqual(object a, object b)
			{
				return a == null ? b == null : a.Equals(b);
			}

			public override bool Equals(object obj)
			{
				if (obj == this)
				{
					return true;
				}
				if (obj is Client.ConnectionId)
				{
					Client.ConnectionId that = (Client.ConnectionId)obj;
					return IsEqual(this.address, that.address) && this.doPing == that.doPing && this.
						maxIdleTime == that.maxIdleTime && IsEqual(this.connectionRetryPolicy, that.connectionRetryPolicy
						) && this.pingInterval == that.pingInterval && IsEqual(this.protocol, that.protocol
						) && this.rpcTimeout == that.rpcTimeout && this.tcpNoDelay == that.tcpNoDelay &&
						 IsEqual(this.ticket, that.ticket);
				}
				return false;
			}

			public override int GetHashCode()
			{
				int result = connectionRetryPolicy.GetHashCode();
				result = Prime * result + ((address == null) ? 0 : address.GetHashCode());
				result = Prime * result + (doPing ? 1231 : 1237);
				result = Prime * result + maxIdleTime;
				result = Prime * result + pingInterval;
				result = Prime * result + ((protocol == null) ? 0 : protocol.GetHashCode());
				result = Prime * result + rpcTimeout;
				result = Prime * result + (tcpNoDelay ? 1231 : 1237);
				result = Prime * result + ((ticket == null) ? 0 : ticket.GetHashCode());
				return result;
			}

			public override string ToString()
			{
				return address.ToString();
			}
		}

		/// <summary>
		/// Returns the next valid sequential call ID by incrementing an atomic counter
		/// and masking off the sign bit.
		/// </summary>
		/// <remarks>
		/// Returns the next valid sequential call ID by incrementing an atomic counter
		/// and masking off the sign bit.  Valid call IDs are non-negative integers in
		/// the range [ 0, 2^31 - 1 ].  Negative numbers are reserved for special
		/// purposes.  The values can overflow back to 0 and be reused.  Note that prior
		/// versions of the client did not mask off the sign bit, so a server may still
		/// see a negative call ID if it receives connections from an old client.
		/// </remarks>
		/// <returns>next call ID</returns>
		public static int NextCallId()
		{
			return callIdCounter.GetAndIncrement() & unchecked((int)(0x7FFFFFFF));
		}
	}
}
