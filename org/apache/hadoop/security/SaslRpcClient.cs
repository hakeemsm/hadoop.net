using Sharpen;

namespace org.apache.hadoop.security
{
	/// <summary>A utility class that encapsulates SASL logic for RPC client</summary>
	public class SaslRpcClient
	{
		public static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.security.SaslRpcClient
			)));

		private readonly org.apache.hadoop.security.UserGroupInformation ugi;

		private readonly java.lang.Class protocol;

		private readonly java.net.InetSocketAddress serverAddr;

		private readonly org.apache.hadoop.conf.Configuration conf;

		private javax.security.sasl.SaslClient saslClient;

		private org.apache.hadoop.security.SaslPropertiesResolver saslPropsResolver;

		private org.apache.hadoop.security.SaslRpcServer.AuthMethod authMethod;

		private static readonly org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcRequestHeaderProto
			 saslHeader = org.apache.hadoop.util.ProtoUtil.makeRpcRequestHeader(org.apache.hadoop.ipc.RPC.RpcKind
			.RPC_PROTOCOL_BUFFER, org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcRequestHeaderProto.OperationProto
			.RPC_FINAL_PACKET, org.apache.hadoop.ipc.Server.AuthProtocol.SASL.callId, org.apache.hadoop.ipc.RpcConstants
			.INVALID_RETRY_COUNT, org.apache.hadoop.ipc.RpcConstants.DUMMY_CLIENT_ID);

		private static readonly org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto
			 negotiateRequest = ((org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto
			)org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto.newBuilder().setState
			(org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto.SaslState.NEGOTIATE
			).build());

		/// <summary>
		/// Create a SaslRpcClient that can be used by a RPC client to negotiate
		/// SASL authentication with a RPC server
		/// </summary>
		/// <param name="ugi">- connecting user</param>
		/// <param name="protocol">- RPC protocol</param>
		/// <param name="serverAddr">- InetSocketAddress of remote server</param>
		/// <param name="conf">- Configuration</param>
		public SaslRpcClient(org.apache.hadoop.security.UserGroupInformation ugi, java.lang.Class
			 protocol, java.net.InetSocketAddress serverAddr, org.apache.hadoop.conf.Configuration
			 conf)
		{
			this.ugi = ugi;
			this.protocol = protocol;
			this.serverAddr = serverAddr;
			this.conf = conf;
			this.saslPropsResolver = org.apache.hadoop.security.SaslPropertiesResolver.getInstance
				(conf);
		}

		[com.google.common.annotations.VisibleForTesting]
		[org.apache.hadoop.classification.InterfaceAudience.Private]
		public virtual object getNegotiatedProperty(string key)
		{
			return (saslClient != null) ? saslClient.getNegotiatedProperty(key) : null;
		}

		// the RPC Client has an inelegant way of handling expiration of TGTs
		// acquired via a keytab.  any connection failure causes a relogin, so
		// the Client needs to know what authMethod was being attempted if an
		// exception occurs.  the SASL prep for a kerberos connection should
		// ideally relogin if necessary instead of exposing this detail to the
		// Client
		[org.apache.hadoop.classification.InterfaceAudience.Private]
		public virtual org.apache.hadoop.security.SaslRpcServer.AuthMethod getAuthMethod(
			)
		{
			return authMethod;
		}

		/// <summary>
		/// Instantiate a sasl client for the first supported auth type in the
		/// given list.
		/// </summary>
		/// <remarks>
		/// Instantiate a sasl client for the first supported auth type in the
		/// given list.  The auth type must be defined, enabled, and the user
		/// must possess the required credentials, else the next auth is tried.
		/// </remarks>
		/// <param name="authTypes">to attempt in the given order</param>
		/// <returns>SaslAuth of instantiated client</returns>
		/// <exception cref="AccessControlException">- client doesn't support any of the auths
		/// 	</exception>
		/// <exception cref="System.IO.IOException">- misc errors</exception>
		/// <exception cref="javax.security.sasl.SaslException"/>
		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		private org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto.SaslAuth selectSaslClient
			(System.Collections.Generic.IList<org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto.SaslAuth
			> authTypes)
		{
			org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto.SaslAuth selectedAuthType
				 = null;
			bool switchToSimple = false;
			foreach (org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto.SaslAuth authType
				 in authTypes)
			{
				if (!isValidAuthType(authType))
				{
					continue;
				}
				// don't know what it is, try next
				org.apache.hadoop.security.SaslRpcServer.AuthMethod authMethod = org.apache.hadoop.security.SaslRpcServer.AuthMethod
					.valueOf(authType.getMethod());
				if (authMethod == org.apache.hadoop.security.SaslRpcServer.AuthMethod.SIMPLE)
				{
					switchToSimple = true;
				}
				else
				{
					saslClient = createSaslClient(authType);
					if (saslClient == null)
					{
						// client lacks credentials, try next
						continue;
					}
				}
				selectedAuthType = authType;
				break;
			}
			if (saslClient == null && !switchToSimple)
			{
				System.Collections.Generic.IList<string> serverAuthMethods = new System.Collections.Generic.List
					<string>();
				foreach (org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto.SaslAuth authType_1
					 in authTypes)
				{
					serverAuthMethods.add(authType_1.getMethod());
				}
				throw new org.apache.hadoop.security.AccessControlException("Client cannot authenticate via:"
					 + serverAuthMethods);
			}
			if (LOG.isDebugEnabled())
			{
				LOG.debug("Use " + selectedAuthType.getMethod() + " authentication for protocol "
					 + protocol.getSimpleName());
			}
			return selectedAuthType;
		}

		private bool isValidAuthType(org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto.SaslAuth
			 authType)
		{
			org.apache.hadoop.security.SaslRpcServer.AuthMethod authMethod;
			try
			{
				authMethod = org.apache.hadoop.security.SaslRpcServer.AuthMethod.valueOf(authType
					.getMethod());
			}
			catch (System.ArgumentException)
			{
				// unknown auth
				authMethod = null;
			}
			// do we know what it is?  is it using our mechanism?
			return authMethod != null && authMethod.getMechanismName().Equals(authType.getMechanism
				());
		}

		/// <summary>Try to create a SaslClient for an authentication type.</summary>
		/// <remarks>
		/// Try to create a SaslClient for an authentication type.  May return
		/// null if the type isn't supported or the client lacks the required
		/// credentials.
		/// </remarks>
		/// <param name="authType">- the requested authentication method</param>
		/// <returns>SaslClient for the authType or null</returns>
		/// <exception cref="javax.security.sasl.SaslException">- error instantiating client</exception>
		/// <exception cref="System.IO.IOException">- misc errors</exception>
		private javax.security.sasl.SaslClient createSaslClient(org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto.SaslAuth
			 authType)
		{
			string saslUser = null;
			// SASL requires the client and server to use the same proto and serverId
			// if necessary, auth types below will verify they are valid
			string saslProtocol = authType.getProtocol();
			string saslServerName = authType.getServerId();
			System.Collections.Generic.IDictionary<string, string> saslProperties = saslPropsResolver
				.getClientProperties(serverAddr.getAddress());
			javax.security.auth.callback.CallbackHandler saslCallback = null;
			org.apache.hadoop.security.SaslRpcServer.AuthMethod method = org.apache.hadoop.security.SaslRpcServer.AuthMethod
				.valueOf(authType.getMethod());
			switch (method)
			{
				case org.apache.hadoop.security.SaslRpcServer.AuthMethod.TOKEN:
				{
					org.apache.hadoop.security.token.Token<object> token = getServerToken(authType);
					if (token == null)
					{
						return null;
					}
					// tokens aren't supported or user doesn't have one
					saslCallback = new org.apache.hadoop.security.SaslRpcClient.SaslClientCallbackHandler
						(token);
					break;
				}

				case org.apache.hadoop.security.SaslRpcServer.AuthMethod.KERBEROS:
				{
					if (ugi.getRealAuthenticationMethod().getAuthMethod() != org.apache.hadoop.security.SaslRpcServer.AuthMethod
						.KERBEROS)
					{
						return null;
					}
					// client isn't using kerberos
					string serverPrincipal = getServerPrincipal(authType);
					if (serverPrincipal == null)
					{
						return null;
					}
					// protocol doesn't use kerberos
					if (LOG.isDebugEnabled())
					{
						LOG.debug("RPC Server's Kerberos principal name for protocol=" + protocol.getCanonicalName
							() + " is " + serverPrincipal);
					}
					break;
				}

				default:
				{
					throw new System.IO.IOException("Unknown authentication method " + method);
				}
			}
			string mechanism = method.getMechanismName();
			if (LOG.isDebugEnabled())
			{
				LOG.debug("Creating SASL " + mechanism + "(" + method + ") " + " client to authenticate to service at "
					 + saslServerName);
			}
			return javax.security.sasl.Sasl.createSaslClient(new string[] { mechanism }, saslUser
				, saslProtocol, saslServerName, saslProperties, saslCallback);
		}

		/// <summary>Try to locate the required token for the server.</summary>
		/// <param name="authType">of the SASL client</param>
		/// <returns>Token<?> for server, or null if no token available</returns>
		/// <exception cref="System.IO.IOException">- token selector cannot be instantiated</exception>
		private org.apache.hadoop.security.token.Token<object> getServerToken(org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto.SaslAuth
			 authType)
		{
			org.apache.hadoop.security.token.TokenInfo tokenInfo = org.apache.hadoop.security.SecurityUtil
				.getTokenInfo(protocol, conf);
			LOG.debug("Get token info proto:" + protocol + " info:" + tokenInfo);
			if (tokenInfo == null)
			{
				// protocol has no support for tokens
				return null;
			}
			org.apache.hadoop.security.token.TokenSelector<object> tokenSelector = null;
			try
			{
				tokenSelector = tokenInfo.value().newInstance();
			}
			catch (java.lang.InstantiationException e)
			{
				throw new System.IO.IOException(e.ToString());
			}
			catch (java.lang.IllegalAccessException e)
			{
				throw new System.IO.IOException(e.ToString());
			}
			return tokenSelector.selectToken(org.apache.hadoop.security.SecurityUtil.buildTokenService
				(serverAddr), ugi.getTokens());
		}

		/// <summary>Get the remote server's principal.</summary>
		/// <remarks>
		/// Get the remote server's principal.  The value will be obtained from
		/// the config and cross-checked against the server's advertised principal.
		/// </remarks>
		/// <param name="authType">of the SASL client</param>
		/// <returns>String of the server's principal</returns>
		/// <exception cref="System.IO.IOException">- error determining configured principal</exception>
		[com.google.common.annotations.VisibleForTesting]
		internal virtual string getServerPrincipal(org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto.SaslAuth
			 authType)
		{
			org.apache.hadoop.security.KerberosInfo krbInfo = org.apache.hadoop.security.SecurityUtil
				.getKerberosInfo(protocol, conf);
			LOG.debug("Get kerberos info proto:" + protocol + " info:" + krbInfo);
			if (krbInfo == null)
			{
				// protocol has no support for kerberos
				return null;
			}
			string serverKey = krbInfo.serverPrincipal();
			if (serverKey == null)
			{
				throw new System.ArgumentException("Can't obtain server Kerberos config key from protocol="
					 + protocol.getCanonicalName());
			}
			// construct server advertised principal for comparision
			string serverPrincipal = new javax.security.auth.kerberos.KerberosPrincipal(authType
				.getProtocol() + "/" + authType.getServerId(), javax.security.auth.kerberos.KerberosPrincipal
				.KRB_NT_SRV_HST).getName();
			bool isPrincipalValid = false;
			// use the pattern if defined
			string serverKeyPattern = conf.get(serverKey + ".pattern");
			if (serverKeyPattern != null && !serverKeyPattern.isEmpty())
			{
				java.util.regex.Pattern pattern = org.apache.hadoop.fs.GlobPattern.compile(serverKeyPattern
					);
				isPrincipalValid = pattern.matcher(serverPrincipal).matches();
			}
			else
			{
				// check that the server advertised principal matches our conf
				string confPrincipal = org.apache.hadoop.security.SecurityUtil.getServerPrincipal
					(conf.get(serverKey), serverAddr.getAddress());
				if (LOG.isDebugEnabled())
				{
					LOG.debug("getting serverKey: " + serverKey + " conf value: " + conf.get(serverKey
						) + " principal: " + confPrincipal);
				}
				if (confPrincipal == null || confPrincipal.isEmpty())
				{
					throw new System.ArgumentException("Failed to specify server's Kerberos principal name"
						);
				}
				org.apache.hadoop.security.authentication.util.KerberosName name = new org.apache.hadoop.security.authentication.util.KerberosName
					(confPrincipal);
				if (name.getHostName() == null)
				{
					throw new System.ArgumentException("Kerberos principal name does NOT have the expected hostname part: "
						 + confPrincipal);
				}
				isPrincipalValid = serverPrincipal.Equals(confPrincipal);
			}
			if (!isPrincipalValid)
			{
				throw new System.ArgumentException("Server has invalid Kerberos principal: " + serverPrincipal
					);
			}
			return serverPrincipal;
		}

		/// <summary>
		/// Do client side SASL authentication with server via the given InputStream
		/// and OutputStream
		/// </summary>
		/// <param name="inS">InputStream to use</param>
		/// <param name="outS">OutputStream to use</param>
		/// <returns>AuthMethod used to negotiate the connection</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual org.apache.hadoop.security.SaslRpcServer.AuthMethod saslConnect(java.io.InputStream
			 inS, java.io.OutputStream outS)
		{
			java.io.DataInputStream inStream = new java.io.DataInputStream(new java.io.BufferedInputStream
				(inS));
			java.io.DataOutputStream outStream = new java.io.DataOutputStream(new java.io.BufferedOutputStream
				(outS));
			// redefined if/when a SASL negotiation starts, can be queried if the
			// negotiation fails
			authMethod = org.apache.hadoop.security.SaslRpcServer.AuthMethod.SIMPLE;
			sendSaslMessage(outStream, negotiateRequest);
			// loop until sasl is complete or a rpc error occurs
			bool done = false;
			do
			{
				int totalLen = inStream.readInt();
				org.apache.hadoop.ipc.ProtobufRpcEngine.RpcResponseMessageWrapper responseWrapper
					 = new org.apache.hadoop.ipc.ProtobufRpcEngine.RpcResponseMessageWrapper();
				responseWrapper.readFields(inStream);
				org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto header = responseWrapper
					.getMessageHeader();
				switch (header.getStatus())
				{
					case org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto
						.ERROR:
					case org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto
						.FATAL:
					{
						// might get a RPC error during 
						throw new org.apache.hadoop.ipc.RemoteException(header.getExceptionClassName(), header
							.getErrorMsg());
					}

					default:
					{
						break;
					}
				}
				if (totalLen != responseWrapper.getLength())
				{
					throw new javax.security.sasl.SaslException("Received malformed response length");
				}
				if (header.getCallId() != org.apache.hadoop.ipc.Server.AuthProtocol.SASL.callId)
				{
					throw new javax.security.sasl.SaslException("Non-SASL response during negotiation"
						);
				}
				org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto saslMessage = org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto
					.parseFrom(responseWrapper.getMessageBytes());
				if (LOG.isDebugEnabled())
				{
					LOG.debug("Received SASL message " + saslMessage);
				}
				// handle sasl negotiation process
				org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto.Builder response = null;
				switch (saslMessage.getState())
				{
					case org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto.SaslState.NEGOTIATE
						:
					{
						// create a compatible SASL client, throws if no supported auths
						org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto.SaslAuth saslAuthType
							 = selectSaslClient(saslMessage.getAuthsList());
						// define auth being attempted, caller can query if connect fails
						authMethod = org.apache.hadoop.security.SaslRpcServer.AuthMethod.valueOf(saslAuthType
							.getMethod());
						byte[] responseToken = null;
						if (authMethod == org.apache.hadoop.security.SaslRpcServer.AuthMethod.SIMPLE)
						{
							// switching to SIMPLE
							done = true;
						}
						else
						{
							// not going to wait for success ack
							byte[] challengeToken = null;
							if (saslAuthType.hasChallenge())
							{
								// server provided the first challenge
								challengeToken = saslAuthType.getChallenge().toByteArray();
								saslAuthType = ((org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto.SaslAuth
									)org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto.SaslAuth.newBuilder
									(saslAuthType).clearChallenge().build());
							}
							else
							{
								if (saslClient.hasInitialResponse())
								{
									challengeToken = new byte[0];
								}
							}
							responseToken = (challengeToken != null) ? saslClient.evaluateChallenge(challengeToken
								) : new byte[0];
						}
						response = createSaslReply(org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto.SaslState
							.INITIATE, responseToken);
						response.addAuths(saslAuthType);
						break;
					}

					case org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto.SaslState.CHALLENGE
						:
					{
						if (saslClient == null)
						{
							// should probably instantiate a client to allow a server to
							// demand a specific negotiation
							throw new javax.security.sasl.SaslException("Server sent unsolicited challenge");
						}
						byte[] responseToken = saslEvaluateToken(saslMessage, false);
						response = createSaslReply(org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto.SaslState
							.RESPONSE, responseToken);
						break;
					}

					case org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto.SaslState.SUCCESS
						:
					{
						// simple server sends immediate success to a SASL client for
						// switch to simple
						if (saslClient == null)
						{
							authMethod = org.apache.hadoop.security.SaslRpcServer.AuthMethod.SIMPLE;
						}
						else
						{
							saslEvaluateToken(saslMessage, true);
						}
						done = true;
						break;
					}

					default:
					{
						throw new javax.security.sasl.SaslException("RPC client doesn't support SASL " + 
							saslMessage.getState());
					}
				}
				if (response != null)
				{
					sendSaslMessage(outStream, ((org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto
						)response.build()));
				}
			}
			while (!done);
			return authMethod;
		}

		/// <exception cref="System.IO.IOException"/>
		private void sendSaslMessage(java.io.DataOutputStream @out, org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto
			 message)
		{
			if (LOG.isDebugEnabled())
			{
				LOG.debug("Sending sasl message " + message);
			}
			org.apache.hadoop.ipc.ProtobufRpcEngine.RpcRequestMessageWrapper request = new org.apache.hadoop.ipc.ProtobufRpcEngine.RpcRequestMessageWrapper
				(saslHeader, message);
			@out.writeInt(request.getLength());
			request.write(@out);
			@out.flush();
		}

		/// <summary>Evaluate the server provided challenge.</summary>
		/// <remarks>
		/// Evaluate the server provided challenge.  The server must send a token
		/// if it's not done.  If the server is done, the challenge token is
		/// optional because not all mechanisms send a final token for the client to
		/// update its internal state.  The client must also be done after
		/// evaluating the optional token to ensure a malicious server doesn't
		/// prematurely end the negotiation with a phony success.
		/// </remarks>
		/// <param name="saslResponse">- client response to challenge</param>
		/// <param name="serverIsDone">- server negotiation state</param>
		/// <exception cref="javax.security.sasl.SaslException">- any problems with negotiation
		/// 	</exception>
		private byte[] saslEvaluateToken(org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto
			 saslResponse, bool serverIsDone)
		{
			byte[] saslToken = null;
			if (saslResponse.hasToken())
			{
				saslToken = saslResponse.getToken().toByteArray();
				saslToken = saslClient.evaluateChallenge(saslToken);
			}
			else
			{
				if (!serverIsDone)
				{
					// the server may only omit a token when it's done
					throw new javax.security.sasl.SaslException("Server challenge contains no token");
				}
			}
			if (serverIsDone)
			{
				// server tried to report success before our client completed
				if (!saslClient.isComplete())
				{
					throw new javax.security.sasl.SaslException("Client is out of sync with server");
				}
				// a client cannot generate a response to a success message
				if (saslToken != null)
				{
					throw new javax.security.sasl.SaslException("Client generated spurious response");
				}
			}
			return saslToken;
		}

		private org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto.Builder createSaslReply
			(org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto.SaslState state, byte
			[] responseToken)
		{
			org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto.Builder response = org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto
				.newBuilder();
			response.setState(state);
			if (responseToken != null)
			{
				response.setToken(com.google.protobuf.ByteString.copyFrom(responseToken));
			}
			return response;
		}

		private bool useWrap()
		{
			// getNegotiatedProperty throws if client isn't complete
			string qop = (string)saslClient.getNegotiatedProperty(javax.security.sasl.Sasl.QOP
				);
			// SASL wrapping is only used if the connection has a QOP, and
			// the value is not auth.  ex. auth-int & auth-priv
			return qop != null && !Sharpen.Runtime.equalsIgnoreCase("auth", qop);
		}

		/// <summary>
		/// Get SASL wrapped InputStream if SASL QoP requires unwrapping,
		/// otherwise return original stream.
		/// </summary>
		/// <remarks>
		/// Get SASL wrapped InputStream if SASL QoP requires unwrapping,
		/// otherwise return original stream.  Can be called only after
		/// saslConnect() has been called.
		/// </remarks>
		/// <param name="in">- InputStream used to make the connection</param>
		/// <returns>InputStream that may be using SASL unwrap</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual java.io.InputStream getInputStream(java.io.InputStream @in)
		{
			if (useWrap())
			{
				@in = new org.apache.hadoop.security.SaslRpcClient.WrappedInputStream(this, @in);
			}
			return @in;
		}

		/// <summary>
		/// Get SASL wrapped OutputStream if SASL QoP requires wrapping,
		/// otherwise return original stream.
		/// </summary>
		/// <remarks>
		/// Get SASL wrapped OutputStream if SASL QoP requires wrapping,
		/// otherwise return original stream.  Can be called only after
		/// saslConnect() has been called.
		/// </remarks>
		/// <param name="in">- InputStream used to make the connection</param>
		/// <returns>InputStream that may be using SASL unwrap</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual java.io.OutputStream getOutputStream(java.io.OutputStream @out)
		{
			if (useWrap())
			{
				// the client and server negotiate a maximum buffer size that can be
				// wrapped
				string maxBuf = (string)saslClient.getNegotiatedProperty(javax.security.sasl.Sasl
					.RAW_SEND_SIZE);
				@out = new java.io.BufferedOutputStream(new org.apache.hadoop.security.SaslRpcClient.WrappedOutputStream
					(this, @out), System.Convert.ToInt32(maxBuf));
			}
			return @out;
		}

		internal class WrappedInputStream : java.io.FilterInputStream
		{
			private java.nio.ByteBuffer unwrappedRpcBuffer = java.nio.ByteBuffer.allocate(0);

			/// <exception cref="System.IO.IOException"/>
			public WrappedInputStream(SaslRpcClient _enclosing, java.io.InputStream @in)
				: base(@in)
			{
				this._enclosing = _enclosing;
			}

			// ideally this should be folded into the RPC decoding loop but it's
			// currently split across Client and SaslRpcClient...
			/// <exception cref="System.IO.IOException"/>
			public override int read()
			{
				byte[] b = new byte[1];
				int n = this.read(b, 0, 1);
				return (n != -1) ? b[0] : -1;
			}

			/// <exception cref="System.IO.IOException"/>
			public override int read(byte[] b)
			{
				return this.read(b, 0, b.Length);
			}

			/// <exception cref="System.IO.IOException"/>
			public override int read(byte[] buf, int off, int len)
			{
				lock (this)
				{
					// fill the buffer with the next RPC message
					if (this.unwrappedRpcBuffer.remaining() == 0)
					{
						this.readNextRpcPacket();
					}
					// satisfy as much of the request as possible
					int readLen = System.Math.min(len, this.unwrappedRpcBuffer.remaining());
					this.unwrappedRpcBuffer.get(buf, off, readLen);
					return readLen;
				}
			}

			// all messages must be RPC SASL wrapped, else an exception is thrown
			/// <exception cref="System.IO.IOException"/>
			private void readNextRpcPacket()
			{
				org.apache.hadoop.security.SaslRpcClient.LOG.debug("reading next wrapped RPC packet"
					);
				java.io.DataInputStream dis = new java.io.DataInputStream(this.@in);
				int rpcLen = dis.readInt();
				byte[] rpcBuf = new byte[rpcLen];
				dis.readFully(rpcBuf);
				// decode the RPC header
				java.io.ByteArrayInputStream bis = new java.io.ByteArrayInputStream(rpcBuf);
				org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.Builder headerBuilder
					 = org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.newBuilder
					();
				headerBuilder.mergeDelimitedFrom(bis);
				bool isWrapped = false;
				// Must be SASL wrapped, verify and decode.
				if (headerBuilder.getCallId() == org.apache.hadoop.ipc.Server.AuthProtocol.SASL.callId)
				{
					org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto.Builder saslMessage = 
						org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto.newBuilder();
					saslMessage.mergeDelimitedFrom(bis);
					if (saslMessage.getState() == org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto.SaslState
						.WRAP)
					{
						isWrapped = true;
						byte[] token = saslMessage.getToken().toByteArray();
						if (org.apache.hadoop.security.SaslRpcClient.LOG.isDebugEnabled())
						{
							org.apache.hadoop.security.SaslRpcClient.LOG.debug("unwrapping token of length:" 
								+ token.Length);
						}
						token = this._enclosing.saslClient.unwrap(token, 0, token.Length);
						this.unwrappedRpcBuffer = java.nio.ByteBuffer.wrap(token);
					}
				}
				if (!isWrapped)
				{
					throw new javax.security.sasl.SaslException("Server sent non-wrapped response");
				}
			}

			private readonly SaslRpcClient _enclosing;
		}

		internal class WrappedOutputStream : java.io.FilterOutputStream
		{
			/// <exception cref="System.IO.IOException"/>
			public WrappedOutputStream(SaslRpcClient _enclosing, java.io.OutputStream @out)
				: base(@out)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void write(byte[] buf, int off, int len)
			{
				if (org.apache.hadoop.security.SaslRpcClient.LOG.isDebugEnabled())
				{
					org.apache.hadoop.security.SaslRpcClient.LOG.debug("wrapping token of length:" + 
						len);
				}
				buf = this._enclosing.saslClient.wrap(buf, off, len);
				org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto saslMessage = ((org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto
					)org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto.newBuilder().setState
					(org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto.SaslState.WRAP).setToken
					(com.google.protobuf.ByteString.copyFrom(buf, 0, buf.Length)).build());
				org.apache.hadoop.ipc.ProtobufRpcEngine.RpcRequestMessageWrapper request = new org.apache.hadoop.ipc.ProtobufRpcEngine.RpcRequestMessageWrapper
					(org.apache.hadoop.security.SaslRpcClient.saslHeader, saslMessage);
				java.io.DataOutputStream dob = new java.io.DataOutputStream(this.@out);
				dob.writeInt(request.getLength());
				request.write(dob);
			}

			private readonly SaslRpcClient _enclosing;
		}

		/// <summary>Release resources used by wrapped saslClient</summary>
		/// <exception cref="javax.security.sasl.SaslException"/>
		public virtual void dispose()
		{
			if (saslClient != null)
			{
				saslClient.dispose();
				saslClient = null;
			}
		}

		private class SaslClientCallbackHandler : javax.security.auth.callback.CallbackHandler
		{
			private readonly string userName;

			private readonly char[] userPassword;

			public SaslClientCallbackHandler(org.apache.hadoop.security.token.Token<org.apache.hadoop.security.token.TokenIdentifier
				> token)
			{
				this.userName = org.apache.hadoop.security.SaslRpcServer.encodeIdentifier(token.getIdentifier
					());
				this.userPassword = org.apache.hadoop.security.SaslRpcServer.encodePassword(token
					.getPassword());
			}

			/// <exception cref="javax.security.auth.callback.UnsupportedCallbackException"/>
			public virtual void handle(javax.security.auth.callback.Callback[] callbacks)
			{
				javax.security.auth.callback.NameCallback nc = null;
				javax.security.auth.callback.PasswordCallback pc = null;
				javax.security.sasl.RealmCallback rc = null;
				foreach (javax.security.auth.callback.Callback callback in callbacks)
				{
					if (callback is javax.security.sasl.RealmChoiceCallback)
					{
						continue;
					}
					else
					{
						if (callback is javax.security.auth.callback.NameCallback)
						{
							nc = (javax.security.auth.callback.NameCallback)callback;
						}
						else
						{
							if (callback is javax.security.auth.callback.PasswordCallback)
							{
								pc = (javax.security.auth.callback.PasswordCallback)callback;
							}
							else
							{
								if (callback is javax.security.sasl.RealmCallback)
								{
									rc = (javax.security.sasl.RealmCallback)callback;
								}
								else
								{
									throw new javax.security.auth.callback.UnsupportedCallbackException(callback, "Unrecognized SASL client callback"
										);
								}
							}
						}
					}
				}
				if (nc != null)
				{
					if (LOG.isDebugEnabled())
					{
						LOG.debug("SASL client callback: setting username: " + userName);
					}
					nc.setName(userName);
				}
				if (pc != null)
				{
					if (LOG.isDebugEnabled())
					{
						LOG.debug("SASL client callback: setting userPassword");
					}
					pc.setPassword(userPassword);
				}
				if (rc != null)
				{
					if (LOG.isDebugEnabled())
					{
						LOG.debug("SASL client callback: setting realm: " + rc.getDefaultText());
					}
					rc.setText(rc.getDefaultText());
				}
			}
		}
	}
}
