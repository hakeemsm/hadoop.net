using Sharpen;

namespace org.apache.hadoop.ipc
{
	/// <summary>A client for an IPC service.</summary>
	/// <remarks>
	/// A client for an IPC service.  IPC calls take a single
	/// <see cref="org.apache.hadoop.io.Writable"/>
	/// as a
	/// parameter, and return a
	/// <see cref="org.apache.hadoop.io.Writable"/>
	/// as their value.  A service runs on
	/// a port and is defined by a parameter class and a value class.
	/// </remarks>
	/// <seealso cref="Server"/>
	public class Client
	{
		public static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ipc.Client)));

		/// <summary>A counter for generating call IDs.</summary>
		private static readonly java.util.concurrent.atomic.AtomicInteger callIdCounter = 
			new java.util.concurrent.atomic.AtomicInteger();

		private static readonly java.lang.ThreadLocal<int> callId = new java.lang.ThreadLocal
			<int>();

		private static readonly java.lang.ThreadLocal<int> retryCount = new java.lang.ThreadLocal
			<int>();

		/// <summary>Set call id and retry count for the next call.</summary>
		public static void setCallIdAndRetryCount(int cid, int rc)
		{
			com.google.common.@base.Preconditions.checkArgument(cid != org.apache.hadoop.ipc.RpcConstants
				.INVALID_CALL_ID);
			com.google.common.@base.Preconditions.checkState(callId.get() == null);
			com.google.common.@base.Preconditions.checkArgument(rc != org.apache.hadoop.ipc.RpcConstants
				.INVALID_RETRY_COUNT);
			callId.set(cid);
			retryCount.set(rc);
		}

		private java.util.Hashtable<org.apache.hadoop.ipc.Client.ConnectionId, org.apache.hadoop.ipc.Client.Connection
			> connections = new java.util.Hashtable<org.apache.hadoop.ipc.Client.ConnectionId
			, org.apache.hadoop.ipc.Client.Connection>();

		private java.lang.Class valueClass;

		private java.util.concurrent.atomic.AtomicBoolean running = new java.util.concurrent.atomic.AtomicBoolean
			(true);

		private readonly org.apache.hadoop.conf.Configuration conf;

		private javax.net.SocketFactory socketFactory;

		private int refCount = 1;

		private readonly int connectionTimeout;

		private readonly bool fallbackAllowed;

		private readonly byte[] clientId;

		internal const int CONNECTION_CONTEXT_CALL_ID = -3;

		/// <summary>Executor on which IPC calls' parameters are sent.</summary>
		/// <remarks>
		/// Executor on which IPC calls' parameters are sent.
		/// Deferring the sending of parameters to a separate
		/// thread isolates them from thread interruptions in the
		/// calling code.
		/// </remarks>
		private readonly java.util.concurrent.ExecutorService sendParamsExecutor;

		private static readonly org.apache.hadoop.ipc.Client.ClientExecutorServiceFactory
			 clientExcecutorFactory = new org.apache.hadoop.ipc.Client.ClientExecutorServiceFactory
			();

		private class ClientExecutorServiceFactory
		{
			private int executorRefCount = 0;

			private java.util.concurrent.ExecutorService clientExecutor = null;

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
			internal virtual java.util.concurrent.ExecutorService refAndGetInstance()
			{
				lock (this)
				{
					if (executorRefCount == 0)
					{
						clientExecutor = java.util.concurrent.Executors.newCachedThreadPool(new com.google.common.util.concurrent.ThreadFactoryBuilder
							().setDaemon(true).setNameFormat("IPC Parameter Sending Thread #%d").build());
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
			internal virtual java.util.concurrent.ExecutorService unrefAndCleanup()
			{
				lock (this)
				{
					executorRefCount--;
					System.Diagnostics.Debug.Assert((executorRefCount >= 0));
					if (executorRefCount == 0)
					{
						clientExecutor.shutdown();
						try
						{
							if (!clientExecutor.awaitTermination(1, java.util.concurrent.TimeUnit.MINUTES))
							{
								clientExecutor.shutdownNow();
							}
						}
						catch (System.Exception)
						{
							LOG.warn("Interrupted while waiting for clientExecutor" + " to stop");
							clientExecutor.shutdownNow();
							java.lang.Thread.currentThread().interrupt();
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
		public static void setPingInterval(org.apache.hadoop.conf.Configuration conf, int
			 pingInterval)
		{
			conf.setInt(org.apache.hadoop.fs.CommonConfigurationKeys.IPC_PING_INTERVAL_KEY, pingInterval
				);
		}

		/// <summary>
		/// Get the ping interval from configuration;
		/// If not set in the configuration, return the default value.
		/// </summary>
		/// <param name="conf">Configuration</param>
		/// <returns>the ping interval</returns>
		public static int getPingInterval(org.apache.hadoop.conf.Configuration conf)
		{
			return conf.getInt(org.apache.hadoop.fs.CommonConfigurationKeys.IPC_PING_INTERVAL_KEY
				, org.apache.hadoop.fs.CommonConfigurationKeys.IPC_PING_INTERVAL_DEFAULT);
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
		public static int getTimeout(org.apache.hadoop.conf.Configuration conf)
		{
			if (!conf.getBoolean(org.apache.hadoop.fs.CommonConfigurationKeys.IPC_CLIENT_PING_KEY
				, org.apache.hadoop.fs.CommonConfigurationKeys.IPC_CLIENT_PING_DEFAULT))
			{
				return getPingInterval(conf);
			}
			return -1;
		}

		/// <summary>set the connection timeout value in configuration</summary>
		/// <param name="conf">Configuration</param>
		/// <param name="timeout">the socket connect timeout value</param>
		public static void setConnectTimeout(org.apache.hadoop.conf.Configuration conf, int
			 timeout)
		{
			conf.setInt(org.apache.hadoop.fs.CommonConfigurationKeys.IPC_CLIENT_CONNECT_TIMEOUT_KEY
				, timeout);
		}

		[com.google.common.annotations.VisibleForTesting]
		public static java.util.concurrent.ExecutorService getClientExecutor()
		{
			return org.apache.hadoop.ipc.Client.clientExcecutorFactory.clientExecutor;
		}

		/// <summary>Increment this client's reference count</summary>
		internal virtual void incCount()
		{
			lock (this)
			{
				refCount++;
			}
		}

		/// <summary>Decrement this client's reference count</summary>
		internal virtual void decCount()
		{
			lock (this)
			{
				refCount--;
			}
		}

		/// <summary>Return if this client has no reference</summary>
		/// <returns>true if this client has no reference; false otherwise</returns>
		internal virtual bool isZeroReference()
		{
			lock (this)
			{
				return refCount == 0;
			}
		}

		/// <summary>Check the rpc response header.</summary>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void checkResponse(org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto
			 header)
		{
			if (header == null)
			{
				throw new java.io.EOFException("Response is null.");
			}
			if (header.hasClientId())
			{
				// check client IDs
				byte[] id = header.getClientId().toByteArray();
				if (!java.util.Arrays.equals(id, org.apache.hadoop.ipc.RpcConstants.DUMMY_CLIENT_ID
					))
				{
					if (!java.util.Arrays.equals(id, clientId))
					{
						throw new System.IO.IOException("Client IDs not matched: local ID=" + org.apache.hadoop.util.StringUtils
							.byteToHexString(clientId) + ", ID in response=" + org.apache.hadoop.util.StringUtils
							.byteToHexString(header.getClientId().toByteArray()));
					}
				}
			}
		}

		internal virtual org.apache.hadoop.ipc.Client.Call createCall(org.apache.hadoop.ipc.RPC.RpcKind
			 rpcKind, org.apache.hadoop.io.Writable rpcRequest)
		{
			return new org.apache.hadoop.ipc.Client.Call(rpcKind, rpcRequest);
		}

		/// <summary>Class that represents an RPC call</summary>
		internal class Call
		{
			internal readonly int id;

			internal readonly int retry;

			internal readonly org.apache.hadoop.io.Writable rpcRequest;

			internal org.apache.hadoop.io.Writable rpcResponse;

			internal System.IO.IOException error;

			internal readonly org.apache.hadoop.ipc.RPC.RpcKind rpcKind;

			internal bool done;

			private Call(org.apache.hadoop.ipc.RPC.RpcKind rpcKind, org.apache.hadoop.io.Writable
				 param)
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
				int id = callId.get();
				if (id == null)
				{
					this.id = nextCallId();
				}
				else
				{
					callId.set(null);
					this.id = id;
				}
				int rc = retryCount.get();
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
			protected internal virtual void callComplete()
			{
				lock (this)
				{
					this.done = true;
					Sharpen.Runtime.notify(this);
				}
			}

			// notify caller
			/// <summary>Set the exception when there is an error.</summary>
			/// <remarks>
			/// Set the exception when there is an error.
			/// Notify the caller the call is done.
			/// </remarks>
			/// <param name="error">exception thrown by the call; either local or remote</param>
			public virtual void setException(System.IO.IOException error)
			{
				lock (this)
				{
					this.error = error;
					callComplete();
				}
			}

			/// <summary>Set the return value when there is no error.</summary>
			/// <remarks>
			/// Set the return value when there is no error.
			/// Notify the caller the call is done.
			/// </remarks>
			/// <param name="rpcResponse">return value of the rpc call.</param>
			public virtual void setRpcResponse(org.apache.hadoop.io.Writable rpcResponse)
			{
				lock (this)
				{
					this.rpcResponse = rpcResponse;
					callComplete();
				}
			}

			public virtual org.apache.hadoop.io.Writable getRpcResponse()
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
		private class Connection : java.lang.Thread
		{
			private java.net.InetSocketAddress server;

			private readonly org.apache.hadoop.ipc.Client.ConnectionId remoteId;

			private org.apache.hadoop.security.SaslRpcServer.AuthMethod authMethod;

			private org.apache.hadoop.ipc.Server.AuthProtocol authProtocol;

			private int serviceClass;

			private org.apache.hadoop.security.SaslRpcClient saslRpcClient;

			private java.net.Socket socket = null;

			private java.io.DataInputStream @in;

			private java.io.DataOutputStream @out;

			private int rpcTimeout;

			private int maxIdleTime;

			private readonly org.apache.hadoop.io.retry.RetryPolicy connectionRetryPolicy;

			private readonly int maxRetriesOnSasl;

			private int maxRetriesOnSocketTimeouts;

			private bool tcpNoDelay;

			private bool doPing;

			private int pingInterval;

			private java.io.ByteArrayOutputStream pingRequest;

			private java.util.Hashtable<int, org.apache.hadoop.ipc.Client.Call> calls = new java.util.Hashtable
				<int, org.apache.hadoop.ipc.Client.Call>();

			private java.util.concurrent.atomic.AtomicLong lastActivity = new java.util.concurrent.atomic.AtomicLong
				();

			private java.util.concurrent.atomic.AtomicBoolean shouldCloseConnection = new java.util.concurrent.atomic.AtomicBoolean
				();

			private System.IO.IOException closeException;

			private readonly object sendRpcRequestLock = new object();

			/// <exception cref="System.IO.IOException"/>
			public Connection(Client _enclosing, org.apache.hadoop.ipc.Client.ConnectionId remoteId
				, int serviceClass)
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
				this.server = remoteId.getAddress();
				if (this.server.isUnresolved())
				{
					throw org.apache.hadoop.net.NetUtils.wrapException(this.server.getHostName(), this
						.server.getPort(), null, 0, new java.net.UnknownHostException());
				}
				this.rpcTimeout = remoteId.getRpcTimeout();
				this.maxIdleTime = remoteId.getMaxIdleTime();
				this.connectionRetryPolicy = remoteId.connectionRetryPolicy;
				this.maxRetriesOnSasl = remoteId.getMaxRetriesOnSasl();
				this.maxRetriesOnSocketTimeouts = remoteId.getMaxRetriesOnSocketTimeouts();
				this.tcpNoDelay = remoteId.getTcpNoDelay();
				this.doPing = remoteId.getDoPing();
				if (this.doPing)
				{
					// construct a RPC header with the callId as the ping callId
					this.pingRequest = new java.io.ByteArrayOutputStream();
					org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcRequestHeaderProto pingHeader = 
						org.apache.hadoop.util.ProtoUtil.makeRpcRequestHeader(org.apache.hadoop.ipc.RPC.RpcKind
						.RPC_PROTOCOL_BUFFER, org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcRequestHeaderProto.OperationProto
						.RPC_FINAL_PACKET, org.apache.hadoop.ipc.RpcConstants.PING_CALL_ID, org.apache.hadoop.ipc.RpcConstants
						.INVALID_RETRY_COUNT, this._enclosing.clientId);
					pingHeader.writeDelimitedTo(this.pingRequest);
				}
				this.pingInterval = remoteId.getPingInterval();
				this.serviceClass = serviceClass;
				if (org.apache.hadoop.ipc.Client.LOG.isDebugEnabled())
				{
					org.apache.hadoop.ipc.Client.LOG.debug("The ping interval is " + this.pingInterval
						 + " ms.");
				}
				org.apache.hadoop.security.UserGroupInformation ticket = remoteId.getTicket();
				// try SASL if security is enabled or if the ugi contains tokens.
				// this causes a SIMPLE client with tokens to attempt SASL
				bool trySasl = org.apache.hadoop.security.UserGroupInformation.isSecurityEnabled(
					) || (ticket != null && !ticket.getTokens().isEmpty());
				this.authProtocol = trySasl ? org.apache.hadoop.ipc.Server.AuthProtocol.SASL : org.apache.hadoop.ipc.Server.AuthProtocol
					.NONE;
				this.setName("IPC Client (" + this._enclosing.socketFactory.GetHashCode() + ") connection to "
					 + this.server.ToString() + " from " + ((ticket == null) ? "an unknown user" : ticket
					.getUserName()));
				this.setDaemon(true);
			}

			/// <summary>Update lastActivity with the current time.</summary>
			private void touch()
			{
				this.lastActivity.set(org.apache.hadoop.util.Time.now());
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
			private bool addCall(org.apache.hadoop.ipc.Client.Call call)
			{
				lock (this)
				{
					if (this.shouldCloseConnection.get())
					{
						return false;
					}
					this.calls[call.id] = call;
					Sharpen.Runtime.notify(this);
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
			private class PingInputStream : java.io.FilterInputStream
			{
				protected internal PingInputStream(Connection _enclosing, java.io.InputStream @in
					)
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
				private void handleTimeout(java.net.SocketTimeoutException e)
				{
					if (this._enclosing.shouldCloseConnection.get() || !this._enclosing._enclosing.running
						.get() || this._enclosing.rpcTimeout > 0)
					{
						throw e;
					}
					else
					{
						this._enclosing.sendPing();
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
				public override int read()
				{
					do
					{
						try
						{
							return base.read();
						}
						catch (java.net.SocketTimeoutException e)
						{
							this.handleTimeout(e);
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
				public override int read(byte[] buf, int off, int len)
				{
					do
					{
						try
						{
							return base.read(buf, off, len);
						}
						catch (java.net.SocketTimeoutException e)
						{
							this.handleTimeout(e);
						}
					}
					while (true);
				}

				private readonly Connection _enclosing;
			}

			private void disposeSasl()
			{
				lock (this)
				{
					if (this.saslRpcClient != null)
					{
						try
						{
							this.saslRpcClient.dispose();
							this.saslRpcClient = null;
						}
						catch (System.IO.IOException)
						{
						}
					}
				}
			}

			/// <exception cref="System.IO.IOException"/>
			private bool shouldAuthenticateOverKrb()
			{
				lock (this)
				{
					org.apache.hadoop.security.UserGroupInformation loginUser = org.apache.hadoop.security.UserGroupInformation
						.getLoginUser();
					org.apache.hadoop.security.UserGroupInformation currentUser = org.apache.hadoop.security.UserGroupInformation
						.getCurrentUser();
					org.apache.hadoop.security.UserGroupInformation realUser = currentUser.getRealUser
						();
					if (this.authMethod == org.apache.hadoop.security.SaslRpcServer.AuthMethod.KERBEROS
						 && loginUser != null && loginUser.hasKerberosCredentials() && (loginUser.Equals
						(currentUser) || loginUser.Equals(realUser)))
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
			private org.apache.hadoop.security.SaslRpcServer.AuthMethod setupSaslConnection(java.io.InputStream
				 in2, java.io.OutputStream out2)
			{
				lock (this)
				{
					// Do not use Client.conf here! We must use ConnectionId.conf, since the
					// Client object is cached and shared between all RPC clients, even those
					// for separate services.
					this.saslRpcClient = new org.apache.hadoop.security.SaslRpcClient(this.remoteId.getTicket
						(), this.remoteId.getProtocol(), this.remoteId.getAddress(), this.remoteId.conf);
					return this.saslRpcClient.saslConnect(in2, out2);
				}
			}

			/// <summary>
			/// Update the server address if the address corresponding to the host
			/// name has changed.
			/// </summary>
			/// <returns>true if an addr change was detected.</returns>
			/// <exception cref="System.IO.IOException">when the hostname cannot be resolved.</exception>
			private bool updateAddress()
			{
				lock (this)
				{
					// Do a fresh lookup with the old host name.
					java.net.InetSocketAddress currentAddr = org.apache.hadoop.net.NetUtils.createSocketAddrForHost
						(this.server.getHostName(), this.server.getPort());
					if (!this.server.Equals(currentAddr))
					{
						org.apache.hadoop.ipc.Client.LOG.warn("Address change detected. Old: " + this.server
							.ToString() + " New: " + currentAddr.ToString());
						this.server = currentAddr;
						return true;
					}
					return false;
				}
			}

			/// <exception cref="System.IO.IOException"/>
			private void setupConnection()
			{
				lock (this)
				{
					short ioFailures = 0;
					short timeoutFailures = 0;
					while (true)
					{
						try
						{
							this.socket = this._enclosing.socketFactory.createSocket();
							this.socket.setTcpNoDelay(this.tcpNoDelay);
							this.socket.setKeepAlive(true);
							/*
							* Bind the socket to the host specified in the principal name of the
							* client, to ensure Server matching address of the client connection
							* to host name in principal passed.
							*/
							org.apache.hadoop.security.UserGroupInformation ticket = this.remoteId.getTicket(
								);
							if (ticket != null && ticket.hasKerberosCredentials())
							{
								org.apache.hadoop.security.KerberosInfo krbInfo = this.remoteId.getProtocol().getAnnotation
									<org.apache.hadoop.security.KerberosInfo>();
								if (krbInfo != null && krbInfo.clientPrincipal() != null)
								{
									string host = org.apache.hadoop.security.SecurityUtil.getHostFromPrincipal(this.remoteId
										.getTicket().getUserName());
									// If host name is a valid local address then bind socket to it
									java.net.InetAddress localAddr = org.apache.hadoop.net.NetUtils.getLocalInetAddress
										(host);
									if (localAddr != null)
									{
										this.socket.bind(new java.net.InetSocketAddress(localAddr, 0));
									}
								}
							}
							org.apache.hadoop.net.NetUtils.connect(this.socket, this.server, this._enclosing.
								connectionTimeout);
							if (this.rpcTimeout > 0)
							{
								this.pingInterval = this.rpcTimeout;
							}
							// rpcTimeout overwrites pingInterval
							this.socket.setSoTimeout(this.pingInterval);
							return;
						}
						catch (org.apache.hadoop.net.ConnectTimeoutException toe)
						{
							/* Check for an address change and update the local reference.
							* Reset the failure counter if the address was changed
							*/
							if (this.updateAddress())
							{
								timeoutFailures = ioFailures = 0;
							}
							this.handleConnectionTimeout(timeoutFailures++, this.maxRetriesOnSocketTimeouts, 
								toe);
						}
						catch (System.IO.IOException ie)
						{
							if (this.updateAddress())
							{
								timeoutFailures = ioFailures = 0;
							}
							this.handleConnectionFailure(ioFailures++, ie);
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
			private void handleSaslConnectionFailure(int currRetries, int maxRetries, System.Exception
				 ex, java.util.Random rand, org.apache.hadoop.security.UserGroupInformation ugi)
			{
				lock (this)
				{
					ugi.doAs(new _PrivilegedExceptionAction_650(this, currRetries, maxRetries, ex, rand
						));
				}
			}

			private sealed class _PrivilegedExceptionAction_650 : java.security.PrivilegedExceptionAction
				<object>
			{
				public _PrivilegedExceptionAction_650(Connection _enclosing, int currRetries, int
					 maxRetries, System.Exception ex, java.util.Random rand)
				{
					this._enclosing = _enclosing;
					this.currRetries = currRetries;
					this.maxRetries = maxRetries;
					this.ex = ex;
					this.rand = rand;
				}

				/// <exception cref="System.IO.IOException"/>
				/// <exception cref="System.Exception"/>
				public object run()
				{
					short MAX_BACKOFF = 5000;
					this._enclosing.closeConnection();
					this._enclosing.disposeSasl();
					if (this._enclosing.shouldAuthenticateOverKrb())
					{
						if (currRetries < maxRetries)
						{
							if (org.apache.hadoop.ipc.Client.LOG.isDebugEnabled())
							{
								org.apache.hadoop.ipc.Client.LOG.debug("Exception encountered while connecting to "
									 + "the server : " + ex);
							}
							// try re-login
							if (org.apache.hadoop.security.UserGroupInformation.isLoginKeytabBased())
							{
								org.apache.hadoop.security.UserGroupInformation.getLoginUser().reloginFromKeytab(
									);
							}
							else
							{
								if (org.apache.hadoop.security.UserGroupInformation.isLoginTicketBased())
								{
									org.apache.hadoop.security.UserGroupInformation.getLoginUser().reloginFromTicketCache
										();
								}
							}
							// have granularity of milliseconds
							//we are sleeping with the Connection lock held but since this
							//connection instance is being used for connecting to the server
							//in question, it is okay
							java.lang.Thread.sleep((rand.nextInt(MAX_BACKOFF) + 1));
							return null;
						}
						else
						{
							string msg = "Couldn't setup connection for " + org.apache.hadoop.security.UserGroupInformation
								.getLoginUser().getUserName() + " to " + this._enclosing.remoteId;
							org.apache.hadoop.ipc.Client.LOG.warn(msg, ex);
							throw (System.IO.IOException)new System.IO.IOException(msg).initCause(ex);
						}
					}
					else
					{
						org.apache.hadoop.ipc.Client.LOG.warn("Exception encountered while connecting to "
							 + "the server : " + ex);
					}
					if (ex is org.apache.hadoop.ipc.RemoteException)
					{
						throw (org.apache.hadoop.ipc.RemoteException)ex;
					}
					throw new System.IO.IOException(ex);
				}

				private readonly Connection _enclosing;

				private readonly int currRetries;

				private readonly int maxRetries;

				private readonly System.Exception ex;

				private readonly java.util.Random rand;
			}

			/// <summary>Connect to the server and set up the I/O streams.</summary>
			/// <remarks>
			/// Connect to the server and set up the I/O streams. It then sends
			/// a header to the server and starts
			/// the connection thread that waits for responses.
			/// </remarks>
			private void setupIOstreams(java.util.concurrent.atomic.AtomicBoolean fallbackToSimpleAuth
				)
			{
				lock (this)
				{
					if (this.socket != null || this.shouldCloseConnection.get())
					{
						return;
					}
					try
					{
						if (org.apache.hadoop.ipc.Client.LOG.isDebugEnabled())
						{
							org.apache.hadoop.ipc.Client.LOG.debug("Connecting to " + this.server);
						}
						if (org.apache.htrace.Trace.isTracing())
						{
							org.apache.htrace.Trace.addTimelineAnnotation("IPC client connecting to " + this.
								server);
						}
						short numRetries = 0;
						java.util.Random rand = null;
						while (true)
						{
							this.setupConnection();
							java.io.InputStream inStream = org.apache.hadoop.net.NetUtils.getInputStream(this
								.socket);
							java.io.OutputStream outStream = org.apache.hadoop.net.NetUtils.getOutputStream(this
								.socket);
							this.writeConnectionHeader(outStream);
							if (this.authProtocol == org.apache.hadoop.ipc.Server.AuthProtocol.SASL)
							{
								java.io.InputStream in2 = inStream;
								java.io.OutputStream out2 = outStream;
								org.apache.hadoop.security.UserGroupInformation ticket = this.remoteId.getTicket(
									);
								if (ticket.getRealUser() != null)
								{
									ticket = ticket.getRealUser();
								}
								try
								{
									this.authMethod = ticket.doAs(new _PrivilegedExceptionAction_725(this, in2, out2)
										);
								}
								catch (System.Exception ex)
								{
									this.authMethod = this.saslRpcClient.getAuthMethod();
									if (rand == null)
									{
										rand = new java.util.Random();
									}
									this.handleSaslConnectionFailure(numRetries++, this.maxRetriesOnSasl, ex, rand, ticket
										);
									continue;
								}
								if (this.authMethod != org.apache.hadoop.security.SaslRpcServer.AuthMethod.SIMPLE)
								{
									// Sasl connect is successful. Let's set up Sasl i/o streams.
									inStream = this.saslRpcClient.getInputStream(inStream);
									outStream = this.saslRpcClient.getOutputStream(outStream);
									// for testing
									this.remoteId.saslQop = (string)this.saslRpcClient.getNegotiatedProperty(javax.security.sasl.Sasl
										.QOP);
									org.apache.hadoop.ipc.Client.LOG.debug("Negotiated QOP is :" + this.remoteId.saslQop
										);
									if (fallbackToSimpleAuth != null)
									{
										fallbackToSimpleAuth.set(false);
									}
								}
								else
								{
									if (org.apache.hadoop.security.UserGroupInformation.isSecurityEnabled())
									{
										if (!this._enclosing.fallbackAllowed)
										{
											throw new System.IO.IOException("Server asks us to fall back to SIMPLE " + "auth, but this client is configured to only allow secure "
												 + "connections.");
										}
										if (fallbackToSimpleAuth != null)
										{
											fallbackToSimpleAuth.set(true);
										}
									}
								}
							}
							if (this.doPing)
							{
								inStream = new org.apache.hadoop.ipc.Client.Connection.PingInputStream(this, inStream
									);
							}
							this.@in = new java.io.DataInputStream(new java.io.BufferedInputStream(inStream));
							// SASL may have already buffered the stream
							if (!(outStream is java.io.BufferedOutputStream))
							{
								outStream = new java.io.BufferedOutputStream(outStream);
							}
							this.@out = new java.io.DataOutputStream(outStream);
							this.writeConnectionContext(this.remoteId, this.authMethod);
							// update last activity time
							this.touch();
							if (org.apache.htrace.Trace.isTracing())
							{
								org.apache.htrace.Trace.addTimelineAnnotation("IPC client connected to " + this.server
									);
							}
							// start the receiver thread after the socket connection has been set
							// up
							this.start();
							return;
						}
					}
					catch (System.Exception t)
					{
						if (t is System.IO.IOException)
						{
							this.markClosed((System.IO.IOException)t);
						}
						else
						{
							this.markClosed(new System.IO.IOException("Couldn't set up IO streams", t));
						}
						this.close();
					}
				}
			}

			private sealed class _PrivilegedExceptionAction_725 : java.security.PrivilegedExceptionAction
				<org.apache.hadoop.security.SaslRpcServer.AuthMethod>
			{
				public _PrivilegedExceptionAction_725(Connection _enclosing, java.io.InputStream 
					in2, java.io.OutputStream out2)
				{
					this._enclosing = _enclosing;
					this.in2 = in2;
					this.out2 = out2;
				}

				/// <exception cref="System.IO.IOException"/>
				/// <exception cref="System.Exception"/>
				public org.apache.hadoop.security.SaslRpcServer.AuthMethod run()
				{
					return this._enclosing.setupSaslConnection(in2, out2);
				}

				private readonly Connection _enclosing;

				private readonly java.io.InputStream in2;

				private readonly java.io.OutputStream out2;
			}

			private void closeConnection()
			{
				if (this.socket == null)
				{
					return;
				}
				// close the current connection
				try
				{
					this.socket.close();
				}
				catch (System.IO.IOException e)
				{
					org.apache.hadoop.ipc.Client.LOG.warn("Not able to close a socket", e);
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
			private void handleConnectionTimeout(int curRetries, int maxRetries, System.IO.IOException
				 ioe)
			{
				this.closeConnection();
				// throw the exception if the maximum number of retries is reached
				if (curRetries >= maxRetries)
				{
					throw ioe;
				}
				org.apache.hadoop.ipc.Client.LOG.info("Retrying connect to server: " + this.server
					 + ". Already tried " + curRetries + " time(s); maxRetries=" + maxRetries);
			}

			/// <exception cref="System.IO.IOException"/>
			private void handleConnectionFailure(int curRetries, System.IO.IOException ioe)
			{
				this.closeConnection();
				org.apache.hadoop.io.retry.RetryPolicy.RetryAction action;
				try
				{
					action = this.connectionRetryPolicy.shouldRetry(ioe, curRetries, 0, true);
				}
				catch (System.Exception e)
				{
					throw e is System.IO.IOException ? (System.IO.IOException)e : new System.IO.IOException
						(e);
				}
				if (action.action == org.apache.hadoop.io.retry.RetryPolicy.RetryAction.RetryDecision
					.FAIL)
				{
					if (action.reason != null)
					{
						org.apache.hadoop.ipc.Client.LOG.warn("Failed to connect to server: " + this.server
							 + ": " + action.reason, ioe);
					}
					throw ioe;
				}
				// Throw the exception if the thread is interrupted
				if (java.lang.Thread.currentThread().isInterrupted())
				{
					org.apache.hadoop.ipc.Client.LOG.warn("Interrupted while trying for connection");
					throw ioe;
				}
				try
				{
					java.lang.Thread.sleep(action.delayMillis);
				}
				catch (System.Exception e)
				{
					throw (System.IO.IOException)new java.io.InterruptedIOException("Interrupted: action="
						 + action + ", retry policy=" + this.connectionRetryPolicy).initCause(e);
				}
				org.apache.hadoop.ipc.Client.LOG.info("Retrying connect to server: " + this.server
					 + ". Already tried " + curRetries + " time(s); retry policy is " + this.connectionRetryPolicy
					);
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
			private void writeConnectionHeader(java.io.OutputStream outStream)
			{
				java.io.DataOutputStream @out = new java.io.DataOutputStream(new java.io.BufferedOutputStream
					(outStream));
				// Write out the header, version and authentication method
				@out.write(((byte[])org.apache.hadoop.ipc.RpcConstants.HEADER.array()));
				@out.write(org.apache.hadoop.ipc.RpcConstants.CURRENT_VERSION);
				@out.write(this.serviceClass);
				@out.write(this.authProtocol.callId);
				@out.flush();
			}

			/* Write the connection context header for each connection
			* Out is not synchronized because only the first thread does this.
			*/
			/// <exception cref="System.IO.IOException"/>
			private void writeConnectionContext(org.apache.hadoop.ipc.Client.ConnectionId remoteId
				, org.apache.hadoop.security.SaslRpcServer.AuthMethod authMethod)
			{
				// Write out the ConnectionHeader
				org.apache.hadoop.ipc.protobuf.IpcConnectionContextProtos.IpcConnectionContextProto
					 message = org.apache.hadoop.util.ProtoUtil.makeIpcConnectionContext(org.apache.hadoop.ipc.RPC
					.getProtocolName(remoteId.getProtocol()), remoteId.getTicket(), authMethod);
				org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcRequestHeaderProto connectionContextHeader
					 = org.apache.hadoop.util.ProtoUtil.makeRpcRequestHeader(org.apache.hadoop.ipc.RPC.RpcKind
					.RPC_PROTOCOL_BUFFER, org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcRequestHeaderProto.OperationProto
					.RPC_FINAL_PACKET, org.apache.hadoop.ipc.Client.CONNECTION_CONTEXT_CALL_ID, org.apache.hadoop.ipc.RpcConstants
					.INVALID_RETRY_COUNT, this._enclosing.clientId);
				org.apache.hadoop.ipc.ProtobufRpcEngine.RpcRequestMessageWrapper request = new org.apache.hadoop.ipc.ProtobufRpcEngine.RpcRequestMessageWrapper
					(connectionContextHeader, message);
				// Write out the packet length
				this.@out.writeInt(request.getLength());
				request.write(this.@out);
			}

			/* wait till someone signals us to start reading RPC response or
			* it is idle too long, it is marked as to be closed,
			* or the client is marked as not running.
			*
			* Return true if it is time to read a response; false otherwise.
			*/
			private bool waitForWork()
			{
				lock (this)
				{
					if (this.calls.isEmpty() && !this.shouldCloseConnection.get() && this._enclosing.
						running.get())
					{
						long timeout = this.maxIdleTime - (org.apache.hadoop.util.Time.now() - this.lastActivity
							.get());
						if (timeout > 0)
						{
							try
							{
								Sharpen.Runtime.wait(this, timeout);
							}
							catch (System.Exception)
							{
							}
						}
					}
					if (!this.calls.isEmpty() && !this.shouldCloseConnection.get() && this._enclosing
						.running.get())
					{
						return true;
					}
					else
					{
						if (this.shouldCloseConnection.get())
						{
							return false;
						}
						else
						{
							if (this.calls.isEmpty())
							{
								// idle connection closed or stopped
								this.markClosed(null);
								return false;
							}
							else
							{
								// get stopped but there are still pending requests 
								this.markClosed((System.IO.IOException)new System.IO.IOException().initCause(new 
									System.Exception()));
								return false;
							}
						}
					}
				}
			}

			public virtual java.net.InetSocketAddress getRemoteAddress()
			{
				return this.server;
			}

			/* Send a ping to the server if the time elapsed
			* since last I/O activity is equal to or greater than the ping interval
			*/
			/// <exception cref="System.IO.IOException"/>
			private void sendPing()
			{
				lock (this)
				{
					long curTime = org.apache.hadoop.util.Time.now();
					if (curTime - this.lastActivity.get() >= this.pingInterval)
					{
						this.lastActivity.set(curTime);
						lock (this.@out)
						{
							this.@out.writeInt(this.pingRequest.size());
							this.pingRequest.writeTo(this.@out);
							this.@out.flush();
						}
					}
				}
			}

			public override void run()
			{
				if (org.apache.hadoop.ipc.Client.LOG.isDebugEnabled())
				{
					org.apache.hadoop.ipc.Client.LOG.debug(this.getName() + ": starting, having connections "
						 + this._enclosing.connections.Count);
				}
				try
				{
					while (this.waitForWork())
					{
						//wait here for work - read or close connection
						this.receiveRpcResponse();
					}
				}
				catch (System.Exception t)
				{
					// This truly is unexpected, since we catch IOException in receiveResponse
					// -- this is only to be really sure that we don't leave a client hanging
					// forever.
					org.apache.hadoop.ipc.Client.LOG.warn("Unexpected error reading responses on connection "
						 + this, t);
					this.markClosed(new System.IO.IOException("Error reading responses", t));
				}
				this.close();
				if (org.apache.hadoop.ipc.Client.LOG.isDebugEnabled())
				{
					org.apache.hadoop.ipc.Client.LOG.debug(this.getName() + ": stopped, remaining connections "
						 + this._enclosing.connections.Count);
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
			public virtual void sendRpcRequest(org.apache.hadoop.ipc.Client.Call call)
			{
				if (this.shouldCloseConnection.get())
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
				org.apache.hadoop.io.DataOutputBuffer d = new org.apache.hadoop.io.DataOutputBuffer
					();
				org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcRequestHeaderProto header = org.apache.hadoop.util.ProtoUtil
					.makeRpcRequestHeader(call.rpcKind, org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcRequestHeaderProto.OperationProto
					.RPC_FINAL_PACKET, call.id, call.retry, this._enclosing.clientId);
				header.writeDelimitedTo(d);
				call.rpcRequest.write(d);
				lock (this.sendRpcRequestLock)
				{
					java.util.concurrent.Future<object> senderFuture = this._enclosing.sendParamsExecutor
						.submit(new _Runnable_1027(this, call, d));
					// Total Length
					// RpcRequestHeader + RpcRequest
					// exception at this point would leave the connection in an
					// unrecoverable state (eg half a call left on the wire).
					// So, close the connection, killing any outstanding calls
					//the buffer is just an in-memory buffer, but it is still polite to
					// close early
					try
					{
						senderFuture.get();
					}
					catch (java.util.concurrent.ExecutionException e)
					{
						System.Exception cause = e.InnerException;
						// cause should only be a RuntimeException as the Runnable above
						// catches IOException
						if (cause is System.Exception)
						{
							throw (System.Exception)cause;
						}
						else
						{
							throw new System.Exception("unexpected checked exception", cause);
						}
					}
				}
			}

			private sealed class _Runnable_1027 : java.lang.Runnable
			{
				public _Runnable_1027(Connection _enclosing, org.apache.hadoop.ipc.Client.Call call
					, org.apache.hadoop.io.DataOutputBuffer d)
				{
					this._enclosing = _enclosing;
					this.call = call;
					this.d = d;
				}

				public void run()
				{
					try
					{
						lock (this._enclosing.@out)
						{
							if (this._enclosing.shouldCloseConnection.get())
							{
								return;
							}
							if (org.apache.hadoop.ipc.Client.LOG.isDebugEnabled())
							{
								org.apache.hadoop.ipc.Client.LOG.debug(this._enclosing.getName() + " sending #" +
									 call.id);
							}
							byte[] data = d.getData();
							int totalLength = d.getLength();
							this._enclosing.@out.writeInt(totalLength);
							this._enclosing.@out.write(data, 0, totalLength);
							this._enclosing.@out.flush();
						}
					}
					catch (System.IO.IOException e)
					{
						this._enclosing.markClosed(e);
					}
					finally
					{
						org.apache.hadoop.io.IOUtils.closeStream(d);
					}
				}

				private readonly Connection _enclosing;

				private readonly org.apache.hadoop.ipc.Client.Call call;

				private readonly org.apache.hadoop.io.DataOutputBuffer d;
			}

			/* Receive a response.
			* Because only one receiver, so no synchronization on in.
			*/
			private void receiveRpcResponse()
			{
				if (this.shouldCloseConnection.get())
				{
					return;
				}
				this.touch();
				try
				{
					int totalLen = this.@in.readInt();
					org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto header = org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto
						.parseDelimitedFrom(this.@in);
					this._enclosing.checkResponse(header);
					int headerLen = header.getSerializedSize();
					headerLen += com.google.protobuf.CodedOutputStream.computeRawVarint32Size(headerLen
						);
					int callId = header.getCallId();
					if (org.apache.hadoop.ipc.Client.LOG.isDebugEnabled())
					{
						org.apache.hadoop.ipc.Client.LOG.debug(this.getName() + " got value #" + callId);
					}
					org.apache.hadoop.ipc.Client.Call call = this.calls[callId];
					org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto
						 status = header.getStatus();
					if (status == org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto
						.SUCCESS)
					{
						org.apache.hadoop.io.Writable value = org.apache.hadoop.util.ReflectionUtils.newInstance
							(this._enclosing.valueClass, this._enclosing.conf);
						value.readFields(this.@in);
						// read value
						this.calls.remove(callId);
						call.setRpcResponse(value);
						// verify that length was correct
						// only for ProtobufEngine where len can be verified easily
						if (call.getRpcResponse() is org.apache.hadoop.ipc.ProtobufRpcEngine.RpcWrapper)
						{
							org.apache.hadoop.ipc.ProtobufRpcEngine.RpcWrapper resWrapper = (org.apache.hadoop.ipc.ProtobufRpcEngine.RpcWrapper
								)call.getRpcResponse();
							if (totalLen != headerLen + resWrapper.getLength())
							{
								throw new org.apache.hadoop.ipc.RpcClientException("RPC response length mismatch on rpc success"
									);
							}
						}
					}
					else
					{
						// Rpc Request failed
						// Verify that length was correct
						if (totalLen != headerLen)
						{
							throw new org.apache.hadoop.ipc.RpcClientException("RPC response length mismatch on rpc error"
								);
						}
						string exceptionClassName = header.hasExceptionClassName() ? header.getExceptionClassName
							() : "ServerDidNotSetExceptionClassName";
						string errorMsg = header.hasErrorMsg() ? header.getErrorMsg() : "ServerDidNotSetErrorMsg";
						org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto
							 erCode = (header.hasErrorDetail() ? header.getErrorDetail() : null);
						if (erCode == null)
						{
							org.apache.hadoop.ipc.Client.LOG.warn("Detailed error code not set by server on rpc error"
								);
						}
						org.apache.hadoop.ipc.RemoteException re = ((erCode == null) ? new org.apache.hadoop.ipc.RemoteException
							(exceptionClassName, errorMsg) : new org.apache.hadoop.ipc.RemoteException(exceptionClassName
							, errorMsg, erCode));
						if (status == org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto
							.ERROR)
						{
							this.calls.remove(callId);
							call.setException(re);
						}
						else
						{
							if (status == org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto
								.FATAL)
							{
								// Close the connection
								this.markClosed(re);
							}
						}
					}
				}
				catch (System.IO.IOException e)
				{
					this.markClosed(e);
				}
			}

			private void markClosed(System.IO.IOException e)
			{
				lock (this)
				{
					if (this.shouldCloseConnection.compareAndSet(false, true))
					{
						this.closeException = e;
						Sharpen.Runtime.notifyAll(this);
					}
				}
			}

			/// <summary>Close the connection.</summary>
			private void close()
			{
				lock (this)
				{
					if (!this.shouldCloseConnection.get())
					{
						org.apache.hadoop.ipc.Client.LOG.error("The connection is not in the closed state"
							);
						return;
					}
					// release the resources
					// first thing to do;take the connection out of the connection list
					lock (this._enclosing.connections)
					{
						if (this._enclosing.connections[this.remoteId] == this)
						{
							this._enclosing.connections.remove(this.remoteId);
						}
					}
					// close the streams and therefore the socket
					org.apache.hadoop.io.IOUtils.closeStream(this.@out);
					org.apache.hadoop.io.IOUtils.closeStream(this.@in);
					this.disposeSasl();
					// clean up all calls
					if (this.closeException == null)
					{
						if (!this.calls.isEmpty())
						{
							org.apache.hadoop.ipc.Client.LOG.warn("A connection is closed for no cause and calls are not empty"
								);
							// clean up calls anyway
							this.closeException = new System.IO.IOException("Unexpected closed connection");
							this.cleanupCalls();
						}
					}
					else
					{
						// log the info
						if (org.apache.hadoop.ipc.Client.LOG.isDebugEnabled())
						{
							org.apache.hadoop.ipc.Client.LOG.debug("closing ipc connection to " + this.server
								 + ": " + this.closeException.Message, this.closeException);
						}
						// cleanup calls
						this.cleanupCalls();
					}
					this.closeConnection();
					if (org.apache.hadoop.ipc.Client.LOG.isDebugEnabled())
					{
						org.apache.hadoop.ipc.Client.LOG.debug(this.getName() + ": closed");
					}
				}
			}

			/* Cleanup all calls and mark them as done */
			private void cleanupCalls()
			{
				System.Collections.Generic.IEnumerator<System.Collections.Generic.KeyValuePair<int
					, org.apache.hadoop.ipc.Client.Call>> itor = this.calls.GetEnumerator();
				while (itor.MoveNext())
				{
					org.apache.hadoop.ipc.Client.Call c = itor.Current.Value;
					itor.remove();
					c.setException(this.closeException);
				}
			}

			private readonly Client _enclosing;
			// local exception
		}

		/// <summary>
		/// Construct an IPC client whose values are of the given
		/// <see cref="org.apache.hadoop.io.Writable"/>
		/// class.
		/// </summary>
		public Client(java.lang.Class valueClass, org.apache.hadoop.conf.Configuration conf
			, javax.net.SocketFactory factory)
		{
			this.valueClass = valueClass;
			this.conf = conf;
			this.socketFactory = factory;
			this.connectionTimeout = conf.getInt(org.apache.hadoop.fs.CommonConfigurationKeys
				.IPC_CLIENT_CONNECT_TIMEOUT_KEY, org.apache.hadoop.fs.CommonConfigurationKeys.IPC_CLIENT_CONNECT_TIMEOUT_DEFAULT
				);
			this.fallbackAllowed = conf.getBoolean(org.apache.hadoop.fs.CommonConfigurationKeys
				.IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH_ALLOWED_KEY, org.apache.hadoop.fs.CommonConfigurationKeys
				.IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH_ALLOWED_DEFAULT);
			this.clientId = org.apache.hadoop.ipc.ClientId.getClientId();
			this.sendParamsExecutor = clientExcecutorFactory.refAndGetInstance();
		}

		/// <summary>Construct an IPC client with the default SocketFactory</summary>
		/// <param name="valueClass"/>
		/// <param name="conf"/>
		public Client(java.lang.Class valueClass, org.apache.hadoop.conf.Configuration conf
			)
			: this(valueClass, conf, org.apache.hadoop.net.NetUtils.getDefaultSocketFactory(conf
				))
		{
		}

		/// <summary>Return the socket factory of this client</summary>
		/// <returns>this client's socket factory</returns>
		internal virtual javax.net.SocketFactory getSocketFactory()
		{
			return socketFactory;
		}

		/// <summary>Stop all threads related to this client.</summary>
		/// <remarks>
		/// Stop all threads related to this client.  No further calls may be made
		/// using this client.
		/// </remarks>
		public virtual void stop()
		{
			if (LOG.isDebugEnabled())
			{
				LOG.debug("Stopping client");
			}
			if (!running.compareAndSet(true, false))
			{
				return;
			}
			// wake up all connections
			lock (connections)
			{
				foreach (org.apache.hadoop.ipc.Client.Connection conn in connections.Values)
				{
					conn.interrupt();
				}
			}
			// wait until all connections are closed
			while (!connections.isEmpty())
			{
				try
				{
					java.lang.Thread.sleep(100);
				}
				catch (System.Exception)
				{
				}
			}
			clientExcecutorFactory.unrefAndCleanup();
		}

		/// <summary>
		/// Same as
		/// <see cref="call(RpcKind, org.apache.hadoop.io.Writable, ConnectionId)"/>
		/// for RPC_BUILTIN
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual org.apache.hadoop.io.Writable call(org.apache.hadoop.io.Writable param
			, java.net.InetSocketAddress address)
		{
			return call(org.apache.hadoop.ipc.RPC.RpcKind.RPC_BUILTIN, param, address);
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
		[System.ObsoleteAttribute(@"Use call(RpcKind, org.apache.hadoop.io.Writable, ConnectionId) instead"
			)]
		public virtual org.apache.hadoop.io.Writable call(org.apache.hadoop.ipc.RPC.RpcKind
			 rpcKind, org.apache.hadoop.io.Writable param, java.net.InetSocketAddress address
			)
		{
			return call(rpcKind, param, address, null);
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
		[System.ObsoleteAttribute(@"Use call(RpcKind, org.apache.hadoop.io.Writable, ConnectionId) instead"
			)]
		public virtual org.apache.hadoop.io.Writable call(org.apache.hadoop.ipc.RPC.RpcKind
			 rpcKind, org.apache.hadoop.io.Writable param, java.net.InetSocketAddress addr, 
			org.apache.hadoop.security.UserGroupInformation ticket)
		{
			org.apache.hadoop.ipc.Client.ConnectionId remoteId = org.apache.hadoop.ipc.Client.ConnectionId
				.getConnectionId(addr, null, ticket, 0, conf);
			return call(rpcKind, param, remoteId);
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
		[System.ObsoleteAttribute(@"Use call(RpcKind, org.apache.hadoop.io.Writable, ConnectionId) instead"
			)]
		public virtual org.apache.hadoop.io.Writable call(org.apache.hadoop.ipc.RPC.RpcKind
			 rpcKind, org.apache.hadoop.io.Writable param, java.net.InetSocketAddress addr, 
			java.lang.Class protocol, org.apache.hadoop.security.UserGroupInformation ticket
			, int rpcTimeout)
		{
			org.apache.hadoop.ipc.Client.ConnectionId remoteId = org.apache.hadoop.ipc.Client.ConnectionId
				.getConnectionId(addr, protocol, ticket, rpcTimeout, conf);
			return call(rpcKind, param, remoteId);
		}

		/// <summary>
		/// Same as
		/// <see cref="call(RpcKind, org.apache.hadoop.io.Writable, java.net.InetSocketAddress, java.lang.Class{T}, org.apache.hadoop.security.UserGroupInformation, int, org.apache.hadoop.conf.Configuration)
		/// 	"/>
		/// except that rpcKind is writable.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual org.apache.hadoop.io.Writable call(org.apache.hadoop.io.Writable param
			, java.net.InetSocketAddress addr, java.lang.Class protocol, org.apache.hadoop.security.UserGroupInformation
			 ticket, int rpcTimeout, org.apache.hadoop.conf.Configuration conf)
		{
			org.apache.hadoop.ipc.Client.ConnectionId remoteId = org.apache.hadoop.ipc.Client.ConnectionId
				.getConnectionId(addr, protocol, ticket, rpcTimeout, conf);
			return call(org.apache.hadoop.ipc.RPC.RpcKind.RPC_BUILTIN, param, remoteId);
		}

		/// <summary>
		/// Same as
		/// <see cref="call(org.apache.hadoop.io.Writable, java.net.InetSocketAddress, java.lang.Class{T}, org.apache.hadoop.security.UserGroupInformation, int, org.apache.hadoop.conf.Configuration)
		/// 	"/>
		/// except that specifying serviceClass.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual org.apache.hadoop.io.Writable call(org.apache.hadoop.io.Writable param
			, java.net.InetSocketAddress addr, java.lang.Class protocol, org.apache.hadoop.security.UserGroupInformation
			 ticket, int rpcTimeout, int serviceClass, org.apache.hadoop.conf.Configuration 
			conf)
		{
			org.apache.hadoop.ipc.Client.ConnectionId remoteId = org.apache.hadoop.ipc.Client.ConnectionId
				.getConnectionId(addr, protocol, ticket, rpcTimeout, conf);
			return call(org.apache.hadoop.ipc.RPC.RpcKind.RPC_BUILTIN, param, remoteId, serviceClass
				);
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
		public virtual org.apache.hadoop.io.Writable call(org.apache.hadoop.ipc.RPC.RpcKind
			 rpcKind, org.apache.hadoop.io.Writable param, java.net.InetSocketAddress addr, 
			java.lang.Class protocol, org.apache.hadoop.security.UserGroupInformation ticket
			, int rpcTimeout, org.apache.hadoop.conf.Configuration conf)
		{
			org.apache.hadoop.ipc.Client.ConnectionId remoteId = org.apache.hadoop.ipc.Client.ConnectionId
				.getConnectionId(addr, protocol, ticket, rpcTimeout, conf);
			return call(rpcKind, param, remoteId);
		}

		/// <summary>
		/// Same as {link
		/// <see cref="call(RpcKind, org.apache.hadoop.io.Writable, ConnectionId)"/>
		/// except the rpcKind is RPC_BUILTIN
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual org.apache.hadoop.io.Writable call(org.apache.hadoop.io.Writable param
			, org.apache.hadoop.ipc.Client.ConnectionId remoteId)
		{
			return call(org.apache.hadoop.ipc.RPC.RpcKind.RPC_BUILTIN, param, remoteId);
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
		public virtual org.apache.hadoop.io.Writable call(org.apache.hadoop.ipc.RPC.RpcKind
			 rpcKind, org.apache.hadoop.io.Writable rpcRequest, org.apache.hadoop.ipc.Client.ConnectionId
			 remoteId)
		{
			return call(rpcKind, rpcRequest, remoteId, org.apache.hadoop.ipc.RPC.RPC_SERVICE_CLASS_DEFAULT
				);
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
		public virtual org.apache.hadoop.io.Writable call(org.apache.hadoop.ipc.RPC.RpcKind
			 rpcKind, org.apache.hadoop.io.Writable rpcRequest, org.apache.hadoop.ipc.Client.ConnectionId
			 remoteId, java.util.concurrent.atomic.AtomicBoolean fallbackToSimpleAuth)
		{
			return call(rpcKind, rpcRequest, remoteId, org.apache.hadoop.ipc.RPC.RPC_SERVICE_CLASS_DEFAULT
				, fallbackToSimpleAuth);
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
		public virtual org.apache.hadoop.io.Writable call(org.apache.hadoop.ipc.RPC.RpcKind
			 rpcKind, org.apache.hadoop.io.Writable rpcRequest, org.apache.hadoop.ipc.Client.ConnectionId
			 remoteId, int serviceClass)
		{
			return call(rpcKind, rpcRequest, remoteId, serviceClass, null);
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
		public virtual org.apache.hadoop.io.Writable call(org.apache.hadoop.ipc.RPC.RpcKind
			 rpcKind, org.apache.hadoop.io.Writable rpcRequest, org.apache.hadoop.ipc.Client.ConnectionId
			 remoteId, int serviceClass, java.util.concurrent.atomic.AtomicBoolean fallbackToSimpleAuth
			)
		{
			org.apache.hadoop.ipc.Client.Call call = createCall(rpcKind, rpcRequest);
			org.apache.hadoop.ipc.Client.Connection connection = getConnection(remoteId, call
				, serviceClass, fallbackToSimpleAuth);
			try
			{
				connection.sendRpcRequest(call);
			}
			catch (java.util.concurrent.RejectedExecutionException e)
			{
				// send the rpc request
				throw new System.IO.IOException("connection has been closed", e);
			}
			catch (System.Exception e)
			{
				java.lang.Thread.currentThread().interrupt();
				LOG.warn("interrupted waiting to send rpc request to server", e);
				throw new System.IO.IOException(e);
			}
			lock (call)
			{
				while (!call.done)
				{
					try
					{
						Sharpen.Runtime.wait(call);
					}
					catch (System.Exception)
					{
						// wait for the result
						java.lang.Thread.currentThread().interrupt();
						throw new java.io.InterruptedIOException("Call interrupted");
					}
				}
				if (call.error != null)
				{
					if (call.error is org.apache.hadoop.ipc.RemoteException)
					{
						call.error.fillInStackTrace();
						throw call.error;
					}
					else
					{
						// local exception
						java.net.InetSocketAddress address = connection.getRemoteAddress();
						throw org.apache.hadoop.net.NetUtils.wrapException(address.getHostName(), address
							.getPort(), org.apache.hadoop.net.NetUtils.getHostname(), 0, call.error);
					}
				}
				else
				{
					return call.getRpcResponse();
				}
			}
		}

		// for unit testing only
		[org.apache.hadoop.classification.InterfaceAudience.Private]
		[org.apache.hadoop.classification.InterfaceStability.Unstable]
		internal virtual System.Collections.Generic.ICollection<org.apache.hadoop.ipc.Client.ConnectionId
			> getConnectionIds()
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
		private org.apache.hadoop.ipc.Client.Connection getConnection(org.apache.hadoop.ipc.Client.ConnectionId
			 remoteId, org.apache.hadoop.ipc.Client.Call call, int serviceClass, java.util.concurrent.atomic.AtomicBoolean
			 fallbackToSimpleAuth)
		{
			if (!running.get())
			{
				// the client is stopped
				throw new System.IO.IOException("The client is stopped");
			}
			org.apache.hadoop.ipc.Client.Connection connection;
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
						connection = new org.apache.hadoop.ipc.Client.Connection(this, remoteId, serviceClass
							);
						connections[remoteId] = connection;
					}
				}
			}
			while (!connection.addCall(call));
			//we don't invoke the method below inside "synchronized (connections)"
			//block above. The reason for that is if the server happens to be slow,
			//it will take longer to establish a connection and that will slow the
			//entire system down.
			connection.setupIOstreams(fallbackToSimpleAuth);
			return connection;
		}

		/// <summary>This class holds the address and the user ticket.</summary>
		/// <remarks>
		/// This class holds the address and the user ticket. The client connections
		/// to servers are uniquely identified by <remoteAddress, protocol, ticket>
		/// </remarks>
		public class ConnectionId
		{
			internal java.net.InetSocketAddress address;

			internal org.apache.hadoop.security.UserGroupInformation ticket;

			internal readonly java.lang.Class protocol;

			private const int PRIME = 16777619;

			private readonly int rpcTimeout;

			private readonly int maxIdleTime;

			private readonly org.apache.hadoop.io.retry.RetryPolicy connectionRetryPolicy;

			private readonly int maxRetriesOnSasl;

			private readonly int maxRetriesOnSocketTimeouts;

			private readonly bool tcpNoDelay;

			private readonly bool doPing;

			private readonly int pingInterval;

			private string saslQop;

			private readonly org.apache.hadoop.conf.Configuration conf;

			internal ConnectionId(java.net.InetSocketAddress address, java.lang.Class protocol
				, org.apache.hadoop.security.UserGroupInformation ticket, int rpcTimeout, org.apache.hadoop.io.retry.RetryPolicy
				 connectionRetryPolicy, org.apache.hadoop.conf.Configuration conf)
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
				this.maxIdleTime = conf.getInt(org.apache.hadoop.fs.CommonConfigurationKeysPublic
					.IPC_CLIENT_CONNECTION_MAXIDLETIME_KEY, org.apache.hadoop.fs.CommonConfigurationKeysPublic
					.IPC_CLIENT_CONNECTION_MAXIDLETIME_DEFAULT);
				this.maxRetriesOnSasl = conf.getInt(org.apache.hadoop.fs.CommonConfigurationKeys.
					IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SASL_KEY, org.apache.hadoop.fs.CommonConfigurationKeys
					.IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SASL_DEFAULT);
				this.maxRetriesOnSocketTimeouts = conf.getInt(org.apache.hadoop.fs.CommonConfigurationKeysPublic
					.IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_KEY, org.apache.hadoop.fs.CommonConfigurationKeysPublic
					.IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_DEFAULT);
				this.tcpNoDelay = conf.getBoolean(org.apache.hadoop.fs.CommonConfigurationKeysPublic
					.IPC_CLIENT_TCPNODELAY_KEY, org.apache.hadoop.fs.CommonConfigurationKeysPublic.IPC_CLIENT_TCPNODELAY_DEFAULT
					);
				this.doPing = conf.getBoolean(org.apache.hadoop.fs.CommonConfigurationKeys.IPC_CLIENT_PING_KEY
					, org.apache.hadoop.fs.CommonConfigurationKeys.IPC_CLIENT_PING_DEFAULT);
				this.pingInterval = (doPing ? org.apache.hadoop.ipc.Client.getPingInterval(conf) : 
					0);
				this.conf = conf;
			}

			internal virtual java.net.InetSocketAddress getAddress()
			{
				return address;
			}

			internal virtual java.lang.Class getProtocol()
			{
				return protocol;
			}

			internal virtual org.apache.hadoop.security.UserGroupInformation getTicket()
			{
				return ticket;
			}

			private int getRpcTimeout()
			{
				return rpcTimeout;
			}

			internal virtual int getMaxIdleTime()
			{
				return maxIdleTime;
			}

			public virtual int getMaxRetriesOnSasl()
			{
				return maxRetriesOnSasl;
			}

			/// <summary>max connection retries on socket time outs</summary>
			public virtual int getMaxRetriesOnSocketTimeouts()
			{
				return maxRetriesOnSocketTimeouts;
			}

			internal virtual bool getTcpNoDelay()
			{
				return tcpNoDelay;
			}

			internal virtual bool getDoPing()
			{
				return doPing;
			}

			internal virtual int getPingInterval()
			{
				return pingInterval;
			}

			[com.google.common.annotations.VisibleForTesting]
			internal virtual string getSaslQop()
			{
				return saslQop;
			}

			/// <exception cref="System.IO.IOException"/>
			internal static org.apache.hadoop.ipc.Client.ConnectionId getConnectionId(java.net.InetSocketAddress
				 addr, java.lang.Class protocol, org.apache.hadoop.security.UserGroupInformation
				 ticket, int rpcTimeout, org.apache.hadoop.conf.Configuration conf)
			{
				return getConnectionId(addr, protocol, ticket, rpcTimeout, null, conf);
			}

			/// <summary>Returns a ConnectionId object.</summary>
			/// <param name="addr">Remote address for the connection.</param>
			/// <param name="protocol">Protocol for RPC.</param>
			/// <param name="ticket">UGI</param>
			/// <param name="rpcTimeout">timeout</param>
			/// <param name="conf">Configuration object</param>
			/// <returns>A ConnectionId instance</returns>
			/// <exception cref="System.IO.IOException"/>
			internal static org.apache.hadoop.ipc.Client.ConnectionId getConnectionId(java.net.InetSocketAddress
				 addr, java.lang.Class protocol, org.apache.hadoop.security.UserGroupInformation
				 ticket, int rpcTimeout, org.apache.hadoop.io.retry.RetryPolicy connectionRetryPolicy
				, org.apache.hadoop.conf.Configuration conf)
			{
				if (connectionRetryPolicy == null)
				{
					int max = conf.getInt(org.apache.hadoop.fs.CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY
						, org.apache.hadoop.fs.CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_DEFAULT
						);
					int retryInterval = conf.getInt(org.apache.hadoop.fs.CommonConfigurationKeysPublic
						.IPC_CLIENT_CONNECT_RETRY_INTERVAL_KEY, org.apache.hadoop.fs.CommonConfigurationKeysPublic
						.IPC_CLIENT_CONNECT_RETRY_INTERVAL_DEFAULT);
					connectionRetryPolicy = org.apache.hadoop.io.retry.RetryPolicies.retryUpToMaximumCountWithFixedSleep
						(max, retryInterval, java.util.concurrent.TimeUnit.MILLISECONDS);
				}
				return new org.apache.hadoop.ipc.Client.ConnectionId(addr, protocol, ticket, rpcTimeout
					, connectionRetryPolicy, conf);
			}

			internal static bool isEqual(object a, object b)
			{
				return a == null ? b == null : a.Equals(b);
			}

			public override bool Equals(object obj)
			{
				if (obj == this)
				{
					return true;
				}
				if (obj is org.apache.hadoop.ipc.Client.ConnectionId)
				{
					org.apache.hadoop.ipc.Client.ConnectionId that = (org.apache.hadoop.ipc.Client.ConnectionId
						)obj;
					return isEqual(this.address, that.address) && this.doPing == that.doPing && this.
						maxIdleTime == that.maxIdleTime && isEqual(this.connectionRetryPolicy, that.connectionRetryPolicy
						) && this.pingInterval == that.pingInterval && isEqual(this.protocol, that.protocol
						) && this.rpcTimeout == that.rpcTimeout && this.tcpNoDelay == that.tcpNoDelay &&
						 isEqual(this.ticket, that.ticket);
				}
				return false;
			}

			public override int GetHashCode()
			{
				int result = connectionRetryPolicy.GetHashCode();
				result = PRIME * result + ((address == null) ? 0 : address.GetHashCode());
				result = PRIME * result + (doPing ? 1231 : 1237);
				result = PRIME * result + maxIdleTime;
				result = PRIME * result + pingInterval;
				result = PRIME * result + ((protocol == null) ? 0 : protocol.GetHashCode());
				result = PRIME * result + rpcTimeout;
				result = PRIME * result + (tcpNoDelay ? 1231 : 1237);
				result = PRIME * result + ((ticket == null) ? 0 : ticket.GetHashCode());
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
		public static int nextCallId()
		{
			return callIdCounter.getAndIncrement() & unchecked((int)(0x7FFFFFFF));
		}
	}
}
