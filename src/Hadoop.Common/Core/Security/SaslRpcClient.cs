using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using Com.Google.Common.Annotations;
using Com.Google.Protobuf;
using Hadoop.Common.Core.Conf;
using Javax.Security.Auth.Callback;
using Javax.Security.Auth.Kerberos;
using Javax.Security.Sasl;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Ipc.Protobuf;
using Org.Apache.Hadoop.Security.Authentication.Util;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Util;


namespace Org.Apache.Hadoop.Security
{
	/// <summary>A utility class that encapsulates SASL logic for RPC client</summary>
	public class SaslRpcClient
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Security.SaslRpcClient
			));

		private readonly UserGroupInformation ugi;

		private readonly Type protocol;

		private readonly IPEndPoint serverAddr;

		private readonly Configuration conf;

		private SaslClient saslClient;

		private SaslPropertiesResolver saslPropsResolver;

		private SaslRpcServer.AuthMethod authMethod;

		private static readonly RpcHeaderProtos.RpcRequestHeaderProto saslHeader = ProtoUtil
			.MakeRpcRequestHeader(RPC.RpcKind.RpcProtocolBuffer, RpcHeaderProtos.RpcRequestHeaderProto.OperationProto
			.RpcFinalPacket, Server.AuthProtocol.Sasl.callId, RpcConstants.InvalidRetryCount
			, RpcConstants.DummyClientId);

		private static readonly RpcHeaderProtos.RpcSaslProto negotiateRequest = ((RpcHeaderProtos.RpcSaslProto
			)RpcHeaderProtos.RpcSaslProto.NewBuilder().SetState(RpcHeaderProtos.RpcSaslProto.SaslState
			.Negotiate).Build());

		/// <summary>
		/// Create a SaslRpcClient that can be used by a RPC client to negotiate
		/// SASL authentication with a RPC server
		/// </summary>
		/// <param name="ugi">- connecting user</param>
		/// <param name="protocol">- RPC protocol</param>
		/// <param name="serverAddr">- InetSocketAddress of remote server</param>
		/// <param name="conf">- Configuration</param>
		public SaslRpcClient(UserGroupInformation ugi, Type protocol, IPEndPoint serverAddr
			, Configuration conf)
		{
			this.ugi = ugi;
			this.protocol = protocol;
			this.serverAddr = serverAddr;
			this.conf = conf;
			this.saslPropsResolver = SaslPropertiesResolver.GetInstance(conf);
		}

		[VisibleForTesting]
		[InterfaceAudience.Private]
		public virtual object GetNegotiatedProperty(string key)
		{
			return (saslClient != null) ? saslClient.GetNegotiatedProperty(key) : null;
		}

		// the RPC Client has an inelegant way of handling expiration of TGTs
		// acquired via a keytab.  any connection failure causes a relogin, so
		// the Client needs to know what authMethod was being attempted if an
		// exception occurs.  the SASL prep for a kerberos connection should
		// ideally relogin if necessary instead of exposing this detail to the
		// Client
		[InterfaceAudience.Private]
		public virtual SaslRpcServer.AuthMethod GetAuthMethod()
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
		/// <exception cref="Javax.Security.Sasl.SaslException"/>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		private RpcHeaderProtos.RpcSaslProto.SaslAuth SelectSaslClient(IList<RpcHeaderProtos.RpcSaslProto.SaslAuth
			> authTypes)
		{
			RpcHeaderProtos.RpcSaslProto.SaslAuth selectedAuthType = null;
			bool switchToSimple = false;
			foreach (RpcHeaderProtos.RpcSaslProto.SaslAuth authType in authTypes)
			{
				if (!IsValidAuthType(authType))
				{
					continue;
				}
				// don't know what it is, try next
				SaslRpcServer.AuthMethod authMethod = SaslRpcServer.AuthMethod.ValueOf(authType.GetMethod
					());
				if (authMethod == SaslRpcServer.AuthMethod.Simple)
				{
					switchToSimple = true;
				}
				else
				{
					saslClient = CreateSaslClient(authType);
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
				IList<string> serverAuthMethods = new AList<string>();
				foreach (RpcHeaderProtos.RpcSaslProto.SaslAuth authType_1 in authTypes)
				{
					serverAuthMethods.AddItem(authType_1.GetMethod());
				}
				throw new AccessControlException("Client cannot authenticate via:" + serverAuthMethods
					);
			}
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Use " + selectedAuthType.GetMethod() + " authentication for protocol "
					 + protocol.Name);
			}
			return selectedAuthType;
		}

		private bool IsValidAuthType(RpcHeaderProtos.RpcSaslProto.SaslAuth authType)
		{
			SaslRpcServer.AuthMethod authMethod;
			try
			{
				authMethod = SaslRpcServer.AuthMethod.ValueOf(authType.GetMethod());
			}
			catch (ArgumentException)
			{
				// unknown auth
				authMethod = null;
			}
			// do we know what it is?  is it using our mechanism?
			return authMethod != null && authMethod.GetMechanismName().Equals(authType.GetMechanism
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
		/// <exception cref="Javax.Security.Sasl.SaslException">- error instantiating client</exception>
		/// <exception cref="System.IO.IOException">- misc errors</exception>
		private SaslClient CreateSaslClient(RpcHeaderProtos.RpcSaslProto.SaslAuth authType
			)
		{
			string saslUser = null;
			// SASL requires the client and server to use the same proto and serverId
			// if necessary, auth types below will verify they are valid
			string saslProtocol = authType.GetProtocol();
			string saslServerName = authType.GetServerId();
			IDictionary<string, string> saslProperties = saslPropsResolver.GetClientProperties
				(serverAddr.Address);
			CallbackHandler saslCallback = null;
			SaslRpcServer.AuthMethod method = SaslRpcServer.AuthMethod.ValueOf(authType.GetMethod
				());
			switch (method)
			{
				case SaslRpcServer.AuthMethod.Token:
				{
					Org.Apache.Hadoop.Security.Token.Token<object> token = GetServerToken(authType);
					if (token == null)
					{
						return null;
					}
					// tokens aren't supported or user doesn't have one
					saslCallback = new SaslRpcClient.SaslClientCallbackHandler(token);
					break;
				}

				case SaslRpcServer.AuthMethod.Kerberos:
				{
					if (ugi.GetRealAuthenticationMethod().GetAuthMethod() != SaslRpcServer.AuthMethod
						.Kerberos)
					{
						return null;
					}
					// client isn't using kerberos
					string serverPrincipal = GetServerPrincipal(authType);
					if (serverPrincipal == null)
					{
						return null;
					}
					// protocol doesn't use kerberos
					if (Log.IsDebugEnabled())
					{
						Log.Debug("RPC Server's Kerberos principal name for protocol=" + protocol.GetCanonicalName
							() + " is " + serverPrincipal);
					}
					break;
				}

				default:
				{
					throw new IOException("Unknown authentication method " + method);
				}
			}
			string mechanism = method.GetMechanismName();
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Creating SASL " + mechanism + "(" + method + ") " + " client to authenticate to service at "
					 + saslServerName);
			}
			return Javax.Security.Sasl.Sasl.CreateSaslClient(new string[] { mechanism }, saslUser
				, saslProtocol, saslServerName, saslProperties, saslCallback);
		}

		/// <summary>Try to locate the required token for the server.</summary>
		/// <param name="authType">of the SASL client</param>
		/// <returns>Token<?> for server, or null if no token available</returns>
		/// <exception cref="System.IO.IOException">- token selector cannot be instantiated</exception>
		private Org.Apache.Hadoop.Security.Token.Token<object> GetServerToken(RpcHeaderProtos.RpcSaslProto.SaslAuth
			 authType)
		{
			TokenInfo tokenInfo = SecurityUtil.GetTokenInfo(protocol, conf);
			Log.Debug("Get token info proto:" + protocol + " info:" + tokenInfo);
			if (tokenInfo == null)
			{
				// protocol has no support for tokens
				return null;
			}
			TokenSelector<object> tokenSelector = null;
			try
			{
				tokenSelector = System.Activator.CreateInstance(tokenInfo.Value());
			}
			catch (InstantiationException e)
			{
				throw new IOException(e.ToString());
			}
			catch (MemberAccessException e)
			{
				throw new IOException(e.ToString());
			}
			return tokenSelector.SelectToken(SecurityUtil.BuildTokenService(serverAddr), ugi.
				GetTokens());
		}

		/// <summary>Get the remote server's principal.</summary>
		/// <remarks>
		/// Get the remote server's principal.  The value will be obtained from
		/// the config and cross-checked against the server's advertised principal.
		/// </remarks>
		/// <param name="authType">of the SASL client</param>
		/// <returns>String of the server's principal</returns>
		/// <exception cref="System.IO.IOException">- error determining configured principal</exception>
		[VisibleForTesting]
		internal virtual string GetServerPrincipal(RpcHeaderProtos.RpcSaslProto.SaslAuth 
			authType)
		{
			KerberosInfo krbInfo = SecurityUtil.GetKerberosInfo(protocol, conf);
			Log.Debug("Get kerberos info proto:" + protocol + " info:" + krbInfo);
			if (krbInfo == null)
			{
				// protocol has no support for kerberos
				return null;
			}
			string serverKey = krbInfo.ServerPrincipal();
			if (serverKey == null)
			{
				throw new ArgumentException("Can't obtain server Kerberos config key from protocol="
					 + protocol.GetCanonicalName());
			}
			// construct server advertised principal for comparision
			string serverPrincipal = new KerberosPrincipal(authType.GetProtocol() + "/" + authType
				.GetServerId(), KerberosPrincipal.KrbNtSrvHst).GetName();
			bool isPrincipalValid = false;
			// use the pattern if defined
			string serverKeyPattern = conf.Get(serverKey + ".pattern");
			if (serverKeyPattern != null && !serverKeyPattern.IsEmpty())
			{
				Pattern pattern = GlobPattern.Compile(serverKeyPattern);
				isPrincipalValid = pattern.Matcher(serverPrincipal).Matches();
			}
			else
			{
				// check that the server advertised principal matches our conf
				string confPrincipal = SecurityUtil.GetServerPrincipal(conf.Get(serverKey), serverAddr
					.Address);
				if (Log.IsDebugEnabled())
				{
					Log.Debug("getting serverKey: " + serverKey + " conf value: " + conf.Get(serverKey
						) + " principal: " + confPrincipal);
				}
				if (confPrincipal == null || confPrincipal.IsEmpty())
				{
					throw new ArgumentException("Failed to specify server's Kerberos principal name");
				}
				KerberosName name = new KerberosName(confPrincipal);
				if (name.GetHostName() == null)
				{
					throw new ArgumentException("Kerberos principal name does NOT have the expected hostname part: "
						 + confPrincipal);
				}
				isPrincipalValid = serverPrincipal.Equals(confPrincipal);
			}
			if (!isPrincipalValid)
			{
				throw new ArgumentException("Server has invalid Kerberos principal: " + serverPrincipal
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
		public virtual SaslRpcServer.AuthMethod SaslConnect(InputStream inS, OutputStream
			 outS)
		{
			DataInputStream inStream = new DataInputStream(new BufferedInputStream(inS));
			DataOutputStream outStream = new DataOutputStream(new BufferedOutputStream(outS));
			// redefined if/when a SASL negotiation starts, can be queried if the
			// negotiation fails
			authMethod = SaslRpcServer.AuthMethod.Simple;
			SendSaslMessage(outStream, negotiateRequest);
			// loop until sasl is complete or a rpc error occurs
			bool done = false;
			do
			{
				int totalLen = inStream.ReadInt();
				ProtobufRpcEngine.RpcResponseMessageWrapper responseWrapper = new ProtobufRpcEngine.RpcResponseMessageWrapper
					();
				responseWrapper.ReadFields(inStream);
				RpcHeaderProtos.RpcResponseHeaderProto header = responseWrapper.GetMessageHeader(
					);
				switch (header.GetStatus())
				{
					case RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto.Error:
					case RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto.Fatal:
					{
						// might get a RPC error during 
						throw new RemoteException(header.GetExceptionClassName(), header.GetErrorMsg());
					}

					default:
					{
						break;
					}
				}
				if (totalLen != responseWrapper.GetLength())
				{
					throw new SaslException("Received malformed response length");
				}
				if (header.GetCallId() != Server.AuthProtocol.Sasl.callId)
				{
					throw new SaslException("Non-SASL response during negotiation");
				}
				RpcHeaderProtos.RpcSaslProto saslMessage = RpcHeaderProtos.RpcSaslProto.ParseFrom
					(responseWrapper.GetMessageBytes());
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Received SASL message " + saslMessage);
				}
				// handle sasl negotiation process
				RpcHeaderProtos.RpcSaslProto.Builder response = null;
				switch (saslMessage.GetState())
				{
					case RpcHeaderProtos.RpcSaslProto.SaslState.Negotiate:
					{
						// create a compatible SASL client, throws if no supported auths
						RpcHeaderProtos.RpcSaslProto.SaslAuth saslAuthType = SelectSaslClient(saslMessage
							.GetAuthsList());
						// define auth being attempted, caller can query if connect fails
						authMethod = SaslRpcServer.AuthMethod.ValueOf(saslAuthType.GetMethod());
						byte[] responseToken = null;
						if (authMethod == SaslRpcServer.AuthMethod.Simple)
						{
							// switching to SIMPLE
							done = true;
						}
						else
						{
							// not going to wait for success ack
							byte[] challengeToken = null;
							if (saslAuthType.HasChallenge())
							{
								// server provided the first challenge
								challengeToken = saslAuthType.GetChallenge().ToByteArray();
								saslAuthType = ((RpcHeaderProtos.RpcSaslProto.SaslAuth)RpcHeaderProtos.RpcSaslProto.SaslAuth
									.NewBuilder(saslAuthType).ClearChallenge().Build());
							}
							else
							{
								if (saslClient.HasInitialResponse())
								{
									challengeToken = new byte[0];
								}
							}
							responseToken = (challengeToken != null) ? saslClient.EvaluateChallenge(challengeToken
								) : new byte[0];
						}
						response = CreateSaslReply(RpcHeaderProtos.RpcSaslProto.SaslState.Initiate, responseToken
							);
						response.AddAuths(saslAuthType);
						break;
					}

					case RpcHeaderProtos.RpcSaslProto.SaslState.Challenge:
					{
						if (saslClient == null)
						{
							// should probably instantiate a client to allow a server to
							// demand a specific negotiation
							throw new SaslException("Server sent unsolicited challenge");
						}
						byte[] responseToken = SaslEvaluateToken(saslMessage, false);
						response = CreateSaslReply(RpcHeaderProtos.RpcSaslProto.SaslState.Response, responseToken
							);
						break;
					}

					case RpcHeaderProtos.RpcSaslProto.SaslState.Success:
					{
						// simple server sends immediate success to a SASL client for
						// switch to simple
						if (saslClient == null)
						{
							authMethod = SaslRpcServer.AuthMethod.Simple;
						}
						else
						{
							SaslEvaluateToken(saslMessage, true);
						}
						done = true;
						break;
					}

					default:
					{
						throw new SaslException("RPC client doesn't support SASL " + saslMessage.GetState
							());
					}
				}
				if (response != null)
				{
					SendSaslMessage(outStream, ((RpcHeaderProtos.RpcSaslProto)response.Build()));
				}
			}
			while (!done);
			return authMethod;
		}

		/// <exception cref="System.IO.IOException"/>
		private void SendSaslMessage(DataOutputStream @out, RpcHeaderProtos.RpcSaslProto 
			message)
		{
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Sending sasl message " + message);
			}
			ProtobufRpcEngine.RpcRequestMessageWrapper request = new ProtobufRpcEngine.RpcRequestMessageWrapper
				(saslHeader, message);
			@out.WriteInt(request.GetLength());
			request.Write(@out);
			@out.Flush();
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
		/// <exception cref="Javax.Security.Sasl.SaslException">- any problems with negotiation
		/// 	</exception>
		private byte[] SaslEvaluateToken(RpcHeaderProtos.RpcSaslProto saslResponse, bool 
			serverIsDone)
		{
			byte[] saslToken = null;
			if (saslResponse.HasToken())
			{
				saslToken = saslResponse.GetToken().ToByteArray();
				saslToken = saslClient.EvaluateChallenge(saslToken);
			}
			else
			{
				if (!serverIsDone)
				{
					// the server may only omit a token when it's done
					throw new SaslException("Server challenge contains no token");
				}
			}
			if (serverIsDone)
			{
				// server tried to report success before our client completed
				if (!saslClient.IsComplete())
				{
					throw new SaslException("Client is out of sync with server");
				}
				// a client cannot generate a response to a success message
				if (saslToken != null)
				{
					throw new SaslException("Client generated spurious response");
				}
			}
			return saslToken;
		}

		private RpcHeaderProtos.RpcSaslProto.Builder CreateSaslReply(RpcHeaderProtos.RpcSaslProto.SaslState
			 state, byte[] responseToken)
		{
			RpcHeaderProtos.RpcSaslProto.Builder response = RpcHeaderProtos.RpcSaslProto.NewBuilder
				();
			response.SetState(state);
			if (responseToken != null)
			{
				response.SetToken(ByteString.CopyFrom(responseToken));
			}
			return response;
		}

		private bool UseWrap()
		{
			// getNegotiatedProperty throws if client isn't complete
			string qop = (string)saslClient.GetNegotiatedProperty(Javax.Security.Sasl.Sasl.Qop
				);
			// SASL wrapping is only used if the connection has a QOP, and
			// the value is not auth.  ex. auth-int & auth-priv
			return qop != null && !Runtime.EqualsIgnoreCase("auth", qop);
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
		public virtual InputStream GetInputStream(InputStream @in)
		{
			if (UseWrap())
			{
				@in = new SaslRpcClient.WrappedInputStream(this, @in);
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
		public virtual OutputStream GetOutputStream(OutputStream @out)
		{
			if (UseWrap())
			{
				// the client and server negotiate a maximum buffer size that can be
				// wrapped
				string maxBuf = (string)saslClient.GetNegotiatedProperty(Javax.Security.Sasl.Sasl
					.RawSendSize);
				@out = new BufferedOutputStream(new SaslRpcClient.WrappedOutputStream(this, @out)
					, System.Convert.ToInt32(maxBuf));
			}
			return @out;
		}

		internal class WrappedInputStream : FilterInputStream
		{
			private ByteBuffer unwrappedRpcBuffer = ByteBuffer.Allocate(0);

			/// <exception cref="System.IO.IOException"/>
			public WrappedInputStream(SaslRpcClient _enclosing, InputStream @in)
				: base(@in)
			{
				this._enclosing = _enclosing;
			}

			// ideally this should be folded into the RPC decoding loop but it's
			// currently split across Client and SaslRpcClient...
			/// <exception cref="System.IO.IOException"/>
			public override int Read()
			{
				byte[] b = new byte[1];
				int n = this.Read(b, 0, 1);
				return (n != -1) ? b[0] : -1;
			}

			/// <exception cref="System.IO.IOException"/>
			public override int Read(byte[] b)
			{
				return this.Read(b, 0, b.Length);
			}

			/// <exception cref="System.IO.IOException"/>
			public override int Read(byte[] buf, int off, int len)
			{
				lock (this)
				{
					// fill the buffer with the next RPC message
					if (this.unwrappedRpcBuffer.Remaining() == 0)
					{
						this.ReadNextRpcPacket();
					}
					// satisfy as much of the request as possible
					int readLen = Math.Min(len, this.unwrappedRpcBuffer.Remaining());
					this.unwrappedRpcBuffer.Get(buf, off, readLen);
					return readLen;
				}
			}

			// all messages must be RPC SASL wrapped, else an exception is thrown
			/// <exception cref="System.IO.IOException"/>
			private void ReadNextRpcPacket()
			{
				SaslRpcClient.Log.Debug("reading next wrapped RPC packet");
				DataInputStream dis = new DataInputStream(this.@in);
				int rpcLen = dis.ReadInt();
				byte[] rpcBuf = new byte[rpcLen];
				dis.ReadFully(rpcBuf);
				// decode the RPC header
				ByteArrayInputStream bis = new ByteArrayInputStream(rpcBuf);
				RpcHeaderProtos.RpcResponseHeaderProto.Builder headerBuilder = RpcHeaderProtos.RpcResponseHeaderProto
					.NewBuilder();
				headerBuilder.MergeDelimitedFrom(bis);
				bool isWrapped = false;
				// Must be SASL wrapped, verify and decode.
				if (headerBuilder.GetCallId() == Server.AuthProtocol.Sasl.callId)
				{
					RpcHeaderProtos.RpcSaslProto.Builder saslMessage = RpcHeaderProtos.RpcSaslProto.NewBuilder
						();
					saslMessage.MergeDelimitedFrom(bis);
					if (saslMessage.GetState() == RpcHeaderProtos.RpcSaslProto.SaslState.Wrap)
					{
						isWrapped = true;
						byte[] token = saslMessage.GetToken().ToByteArray();
						if (SaslRpcClient.Log.IsDebugEnabled())
						{
							SaslRpcClient.Log.Debug("unwrapping token of length:" + token.Length);
						}
						token = this._enclosing.saslClient.Unwrap(token, 0, token.Length);
						this.unwrappedRpcBuffer = ByteBuffer.Wrap(token);
					}
				}
				if (!isWrapped)
				{
					throw new SaslException("Server sent non-wrapped response");
				}
			}

			private readonly SaslRpcClient _enclosing;
		}

		internal class WrappedOutputStream : FilterOutputStream
		{
			/// <exception cref="System.IO.IOException"/>
			public WrappedOutputStream(SaslRpcClient _enclosing, OutputStream @out)
				: base(@out)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Write(byte[] buf, int off, int len)
			{
				if (SaslRpcClient.Log.IsDebugEnabled())
				{
					SaslRpcClient.Log.Debug("wrapping token of length:" + len);
				}
				buf = this._enclosing.saslClient.Wrap(buf, off, len);
				RpcHeaderProtos.RpcSaslProto saslMessage = ((RpcHeaderProtos.RpcSaslProto)RpcHeaderProtos.RpcSaslProto
					.NewBuilder().SetState(RpcHeaderProtos.RpcSaslProto.SaslState.Wrap).SetToken(ByteString
					.CopyFrom(buf, 0, buf.Length)).Build());
				ProtobufRpcEngine.RpcRequestMessageWrapper request = new ProtobufRpcEngine.RpcRequestMessageWrapper
					(SaslRpcClient.saslHeader, saslMessage);
				DataOutputStream dob = new DataOutputStream(this.@out);
				dob.WriteInt(request.GetLength());
				request.Write(dob);
			}

			private readonly SaslRpcClient _enclosing;
		}

		/// <summary>Release resources used by wrapped saslClient</summary>
		/// <exception cref="Javax.Security.Sasl.SaslException"/>
		public virtual void Dispose()
		{
			if (saslClient != null)
			{
				saslClient.Dispose();
				saslClient = null;
			}
		}

		private class SaslClientCallbackHandler : CallbackHandler
		{
			private readonly string userName;

			private readonly char[] userPassword;

			public SaslClientCallbackHandler(Org.Apache.Hadoop.Security.Token.Token<TokenIdentifier
				> token)
			{
				this.userName = SaslRpcServer.EncodeIdentifier(token.GetIdentifier());
				this.userPassword = SaslRpcServer.EncodePassword(token.GetPassword());
			}

			/// <exception cref="Javax.Security.Auth.Callback.UnsupportedCallbackException"/>
			public virtual void Handle(Javax.Security.Auth.Callback.Callback[] callbacks)
			{
				NameCallback nc = null;
				PasswordCallback pc = null;
				RealmCallback rc = null;
				foreach (Javax.Security.Auth.Callback.Callback callback in callbacks)
				{
					if (callback is RealmChoiceCallback)
					{
						continue;
					}
					else
					{
						if (callback is NameCallback)
						{
							nc = (NameCallback)callback;
						}
						else
						{
							if (callback is PasswordCallback)
							{
								pc = (PasswordCallback)callback;
							}
							else
							{
								if (callback is RealmCallback)
								{
									rc = (RealmCallback)callback;
								}
								else
								{
									throw new UnsupportedCallbackException(callback, "Unrecognized SASL client callback"
										);
								}
							}
						}
					}
				}
				if (nc != null)
				{
					if (Log.IsDebugEnabled())
					{
						Log.Debug("SASL client callback: setting username: " + userName);
					}
					nc.SetName(userName);
				}
				if (pc != null)
				{
					if (Log.IsDebugEnabled())
					{
						Log.Debug("SASL client callback: setting userPassword");
					}
					pc.SetPassword(userPassword);
				}
				if (rc != null)
				{
					if (Log.IsDebugEnabled())
					{
						Log.Debug("SASL client callback: setting realm: " + rc.GetDefaultText());
					}
					rc.SetText(rc.GetDefaultText());
				}
			}
		}
	}
}
