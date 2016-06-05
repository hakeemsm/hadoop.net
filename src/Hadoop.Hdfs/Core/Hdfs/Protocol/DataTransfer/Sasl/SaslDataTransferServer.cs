using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Javax.Security.Auth.Callback;
using Javax.Security.Sasl;
using Org.Apache.Commons.Codec.Binary;
using Org.Apache.Hadoop.Crypto;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Net;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Protocol.Datatransfer;
using Org.Apache.Hadoop.Hdfs.Protocol.Proto;
using Org.Apache.Hadoop.Hdfs.Security.Token.Block;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Security;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Protocol.Datatransfer.Sasl
{
	/// <summary>Negotiates SASL for DataTransferProtocol on behalf of a server.</summary>
	/// <remarks>
	/// Negotiates SASL for DataTransferProtocol on behalf of a server.  There are
	/// two possible supported variants of SASL negotiation: either a general-purpose
	/// negotiation supporting any quality of protection, or a specialized
	/// negotiation that enforces privacy as the quality of protection using a
	/// cryptographically strong encryption key.
	/// This class is used in the DataNode for handling inbound connections.
	/// </remarks>
	public class SaslDataTransferServer
	{
		private static readonly Logger Log = LoggerFactory.GetLogger(typeof(Org.Apache.Hadoop.Hdfs.Protocol.Datatransfer.Sasl.SaslDataTransferServer
			));

		private readonly BlockPoolTokenSecretManager blockPoolTokenSecretManager;

		private readonly DNConf dnConf;

		/// <summary>Creates a new SaslDataTransferServer.</summary>
		/// <param name="dnConf">configuration of DataNode</param>
		/// <param name="blockPoolTokenSecretManager">
		/// used for checking block access tokens
		/// and encryption keys
		/// </param>
		public SaslDataTransferServer(DNConf dnConf, BlockPoolTokenSecretManager blockPoolTokenSecretManager
			)
		{
			this.blockPoolTokenSecretManager = blockPoolTokenSecretManager;
			this.dnConf = dnConf;
		}

		/// <summary>Receives SASL negotiation from a peer on behalf of a server.</summary>
		/// <param name="peer">connection peer</param>
		/// <param name="underlyingOut">connection output stream</param>
		/// <param name="underlyingIn">connection input stream</param>
		/// <?/>
		/// <param name="datanodeId">ID of DataNode accepting connection</param>
		/// <returns>new pair of streams, wrapped after SASL negotiation</returns>
		/// <exception cref="System.IO.IOException">for any error</exception>
		public virtual IOStreamPair Receive(Peer peer, OutputStream underlyingOut, InputStream
			 underlyingIn, int xferPort, DatanodeID datanodeId)
		{
			if (dnConf.GetEncryptDataTransfer())
			{
				Log.Debug("SASL server doing encrypted handshake for peer = {}, datanodeId = {}", 
					peer, datanodeId);
				return GetEncryptedStreams(peer, underlyingOut, underlyingIn);
			}
			else
			{
				if (!UserGroupInformation.IsSecurityEnabled())
				{
					Log.Debug("SASL server skipping handshake in unsecured configuration for " + "peer = {}, datanodeId = {}"
						, peer, datanodeId);
					return new IOStreamPair(underlyingIn, underlyingOut);
				}
				else
				{
					if (SecurityUtil.IsPrivilegedPort(xferPort))
					{
						Log.Debug("SASL server skipping handshake in secured configuration for " + "peer = {}, datanodeId = {}"
							, peer, datanodeId);
						return new IOStreamPair(underlyingIn, underlyingOut);
					}
					else
					{
						if (dnConf.GetSaslPropsResolver() != null)
						{
							Log.Debug("SASL server doing general handshake for peer = {}, datanodeId = {}", peer
								, datanodeId);
							return GetSaslStreams(peer, underlyingOut, underlyingIn);
						}
						else
						{
							if (dnConf.GetIgnoreSecurePortsForTesting())
							{
								// It's a secured cluster using non-privileged ports, but no SASL.  The
								// only way this can happen is if the DataNode has
								// ignore.secure.ports.for.testing configured, so this is a rare edge case.
								Log.Debug("SASL server skipping handshake in secured configuration with no SASL "
									 + "protection configured for peer = {}, datanodeId = {}", peer, datanodeId);
								return new IOStreamPair(underlyingIn, underlyingOut);
							}
							else
							{
								// The error message here intentionally does not mention
								// ignore.secure.ports.for.testing.  That's intended for dev use only.
								// This code path is not expected to execute ever, because DataNode startup
								// checks for invalid configuration and aborts.
								throw new IOException(string.Format("Cannot create a secured " + "connection if DataNode listens on unprivileged port (%d) and no "
									 + "protection is defined in configuration property %s.", datanodeId.GetXferPort
									(), DFSConfigKeys.DfsDataTransferProtectionKey));
							}
						}
					}
				}
			}
		}

		/// <summary>Receives SASL negotiation for specialized encrypted handshake.</summary>
		/// <param name="peer">connection peer</param>
		/// <param name="underlyingOut">connection output stream</param>
		/// <param name="underlyingIn">connection input stream</param>
		/// <returns>new pair of streams, wrapped after SASL negotiation</returns>
		/// <exception cref="System.IO.IOException">for any error</exception>
		private IOStreamPair GetEncryptedStreams(Peer peer, OutputStream underlyingOut, InputStream
			 underlyingIn)
		{
			if (peer.HasSecureChannel() || dnConf.GetTrustedChannelResolver().IsTrusted(DataTransferSaslUtil.GetPeerAddress
				(peer)))
			{
				return new IOStreamPair(underlyingIn, underlyingOut);
			}
			IDictionary<string, string> saslProps = DataTransferSaslUtil.CreateSaslPropertiesForEncryption
				(dnConf.GetEncryptionAlgorithm());
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Server using encryption algorithm " + dnConf.GetEncryptionAlgorithm());
			}
			CallbackHandler callbackHandler = new SaslDataTransferServer.SaslServerCallbackHandler
				(new _PasswordFunction_172(this));
			return DoSaslHandshake(underlyingOut, underlyingIn, saslProps, callbackHandler);
		}

		private sealed class _PasswordFunction_172 : SaslDataTransferServer.PasswordFunction
		{
			public _PasswordFunction_172(SaslDataTransferServer _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.IO.IOException"/>
			public char[] Apply(string userName)
			{
				return DataTransferSaslUtil.EncryptionKeyToPassword(this._enclosing.GetEncryptionKeyFromUserName
					(userName));
			}

			private readonly SaslDataTransferServer _enclosing;
		}

		/// <summary>The SASL handshake for encrypted vs.</summary>
		/// <remarks>
		/// The SASL handshake for encrypted vs. general-purpose uses different logic
		/// for determining the password.  This interface is used to parameterize that
		/// logic.  It's similar to a Guava Function, but we need to let it throw
		/// exceptions.
		/// </remarks>
		private interface PasswordFunction
		{
			/// <summary>Returns the SASL password for the given user name.</summary>
			/// <param name="userName">SASL user name</param>
			/// <returns>SASL password</returns>
			/// <exception cref="System.IO.IOException">for any error</exception>
			char[] Apply(string userName);
		}

		/// <summary>Sets user name and password when asked by the server-side SASL object.</summary>
		private sealed class SaslServerCallbackHandler : CallbackHandler
		{
			private readonly SaslDataTransferServer.PasswordFunction passwordFunction;

			/// <summary>Creates a new SaslServerCallbackHandler.</summary>
			/// <param name="passwordFunction">for determing the user's password</param>
			public SaslServerCallbackHandler(SaslDataTransferServer.PasswordFunction passwordFunction
				)
			{
				this.passwordFunction = passwordFunction;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Javax.Security.Auth.Callback.UnsupportedCallbackException"/>
			public void Handle(Javax.Security.Auth.Callback.Callback[] callbacks)
			{
				NameCallback nc = null;
				PasswordCallback pc = null;
				AuthorizeCallback ac = null;
				foreach (Javax.Security.Auth.Callback.Callback callback in callbacks)
				{
					if (callback is AuthorizeCallback)
					{
						ac = (AuthorizeCallback)callback;
					}
					else
					{
						if (callback is PasswordCallback)
						{
							pc = (PasswordCallback)callback;
						}
						else
						{
							if (callback is NameCallback)
							{
								nc = (NameCallback)callback;
							}
							else
							{
								if (callback is RealmCallback)
								{
									continue;
								}
								else
								{
									// realm is ignored
									throw new UnsupportedCallbackException(callback, "Unrecognized SASL DIGEST-MD5 Callback: "
										 + callback);
								}
							}
						}
					}
				}
				if (pc != null)
				{
					pc.SetPassword(passwordFunction.Apply(nc.GetDefaultName()));
				}
				if (ac != null)
				{
					ac.SetAuthorized(true);
					ac.SetAuthorizedID(ac.GetAuthorizationID());
				}
			}
		}

		/// <summary>
		/// Given a secret manager and a username encoded for the encrypted handshake,
		/// determine the encryption key.
		/// </summary>
		/// <param name="userName">containing the keyId, blockPoolId, and nonce.</param>
		/// <returns>secret encryption key.</returns>
		/// <exception cref="System.IO.IOException"/>
		private byte[] GetEncryptionKeyFromUserName(string userName)
		{
			string[] nameComponents = userName.Split(NameDelimiter);
			if (nameComponents.Length != 3)
			{
				throw new IOException("Provided name '" + userName + "' has " + nameComponents.Length
					 + " components instead of the expected 3.");
			}
			int keyId = System.Convert.ToInt32(nameComponents[0]);
			string blockPoolId = nameComponents[1];
			byte[] nonce = Base64.DecodeBase64(nameComponents[2]);
			return blockPoolTokenSecretManager.RetrieveDataEncryptionKey(keyId, blockPoolId, 
				nonce);
		}

		/// <summary>Receives SASL negotiation for general-purpose handshake.</summary>
		/// <param name="peer">connection peer</param>
		/// <param name="underlyingOut">connection output stream</param>
		/// <param name="underlyingIn">connection input stream</param>
		/// <returns>new pair of streams, wrapped after SASL negotiation</returns>
		/// <exception cref="System.IO.IOException">for any error</exception>
		private IOStreamPair GetSaslStreams(Peer peer, OutputStream underlyingOut, InputStream
			 underlyingIn)
		{
			if (peer.HasSecureChannel() || dnConf.GetTrustedChannelResolver().IsTrusted(DataTransferSaslUtil.GetPeerAddress
				(peer)))
			{
				return new IOStreamPair(underlyingIn, underlyingOut);
			}
			SaslPropertiesResolver saslPropsResolver = dnConf.GetSaslPropsResolver();
			IDictionary<string, string> saslProps = saslPropsResolver.GetServerProperties(DataTransferSaslUtil.GetPeerAddress
				(peer));
			CallbackHandler callbackHandler = new SaslDataTransferServer.SaslServerCallbackHandler
				(new _PasswordFunction_292(this));
			return DoSaslHandshake(underlyingOut, underlyingIn, saslProps, callbackHandler);
		}

		private sealed class _PasswordFunction_292 : SaslDataTransferServer.PasswordFunction
		{
			public _PasswordFunction_292(SaslDataTransferServer _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.IO.IOException"/>
			public char[] Apply(string userName)
			{
				return this._enclosing.BuildServerPassword(userName);
			}

			private readonly SaslDataTransferServer _enclosing;
		}

		/// <summary>
		/// Calculates the expected correct password on the server side for the
		/// general-purpose handshake.
		/// </summary>
		/// <remarks>
		/// Calculates the expected correct password on the server side for the
		/// general-purpose handshake.  The password consists of the block access
		/// token's password (known to the DataNode via its secret manager).  This
		/// expects that the client has supplied a user name consisting of its
		/// serialized block access token identifier.
		/// </remarks>
		/// <param name="userName">
		/// SASL user name containing serialized block access token
		/// identifier
		/// </param>
		/// <returns>expected correct SASL password</returns>
		/// <exception cref="System.IO.IOException">for any error</exception>
		private char[] BuildServerPassword(string userName)
		{
			BlockTokenIdentifier identifier = DeserializeIdentifier(userName);
			byte[] tokenPassword = blockPoolTokenSecretManager.RetrievePassword(identifier);
			return (new string(Base64.EncodeBase64(tokenPassword, false), Charsets.Utf8)).ToCharArray
				();
		}

		/// <summary>
		/// Deserializes a base64-encoded binary representation of a block access
		/// token.
		/// </summary>
		/// <param name="str">String to deserialize</param>
		/// <returns>BlockTokenIdentifier deserialized from str</returns>
		/// <exception cref="System.IO.IOException">if there is any I/O error</exception>
		private BlockTokenIdentifier DeserializeIdentifier(string str)
		{
			BlockTokenIdentifier identifier = new BlockTokenIdentifier();
			identifier.ReadFields(new DataInputStream(new ByteArrayInputStream(Base64.DecodeBase64
				(str))));
			return identifier;
		}

		/// <summary>This method actually executes the server-side SASL handshake.</summary>
		/// <param name="underlyingOut">connection output stream</param>
		/// <param name="underlyingIn">connection input stream</param>
		/// <param name="saslProps">properties of SASL negotiation</param>
		/// <param name="callbackHandler">for responding to SASL callbacks</param>
		/// <returns>new pair of streams, wrapped after SASL negotiation</returns>
		/// <exception cref="System.IO.IOException">for any error</exception>
		private IOStreamPair DoSaslHandshake(OutputStream underlyingOut, InputStream underlyingIn
			, IDictionary<string, string> saslProps, CallbackHandler callbackHandler)
		{
			DataInputStream @in = new DataInputStream(underlyingIn);
			DataOutputStream @out = new DataOutputStream(underlyingOut);
			SaslParticipant sasl = SaslParticipant.CreateServerSaslParticipant(saslProps, callbackHandler
				);
			int magicNumber = @in.ReadInt();
			if (magicNumber != SaslTransferMagicNumber)
			{
				throw new InvalidMagicNumberException(magicNumber, dnConf.GetEncryptDataTransfer(
					));
			}
			try
			{
				// step 1
				byte[] remoteResponse = DataTransferSaslUtil.ReadSaslMessage(@in);
				byte[] localResponse = sasl.EvaluateChallengeOrResponse(remoteResponse);
				DataTransferSaslUtil.SendSaslMessage(@out, localResponse);
				// step 2 (server-side only)
				IList<CipherOption> cipherOptions = Lists.NewArrayList();
				remoteResponse = DataTransferSaslUtil.ReadSaslMessageAndNegotiationCipherOptions(
					@in, cipherOptions);
				localResponse = sasl.EvaluateChallengeOrResponse(remoteResponse);
				// SASL handshake is complete
				DataTransferSaslUtil.CheckSaslComplete(sasl, saslProps);
				CipherOption cipherOption = null;
				if (sasl.IsNegotiatedQopPrivacy())
				{
					// Negotiate a cipher option
					cipherOption = DataTransferSaslUtil.NegotiateCipherOption(dnConf.GetConf(), cipherOptions
						);
					if (cipherOption != null)
					{
						if (Log.IsDebugEnabled())
						{
							Log.Debug("Server using cipher suite " + cipherOption.GetCipherSuite().GetName());
						}
					}
				}
				// If negotiated cipher option is not null, wrap it before sending.
				DataTransferSaslUtil.SendSaslMessageAndNegotiatedCipherOption(@out, localResponse
					, DataTransferSaslUtil.Wrap(cipherOption, sasl));
				// If negotiated cipher option is not null, we will use it to create 
				// stream pair.
				return cipherOption != null ? DataTransferSaslUtil.CreateStreamPair(dnConf.GetConf
					(), cipherOption, underlyingOut, underlyingIn, true) : sasl.CreateStreamPair(@out
					, @in);
			}
			catch (IOException ioe)
			{
				if (ioe is SaslException && ioe.InnerException != null && ioe.InnerException is InvalidEncryptionKeyException)
				{
					// This could just be because the client is long-lived and hasn't gotten
					// a new encryption key from the NN in a while. Upon receiving this
					// error, the client will get a new encryption key from the NN and retry
					// connecting to this DN.
					SendInvalidKeySaslErrorMessage(@out, ioe.InnerException.Message);
				}
				else
				{
					DataTransferSaslUtil.SendGenericSaslErrorMessage(@out, ioe.Message);
				}
				throw;
			}
		}

		/// <summary>Sends a SASL negotiation message indicating an invalid key error.</summary>
		/// <param name="out">stream to receive message</param>
		/// <param name="message">to send</param>
		/// <exception cref="System.IO.IOException">for any error</exception>
		private static void SendInvalidKeySaslErrorMessage(DataOutputStream @out, string 
			message)
		{
			DataTransferSaslUtil.SendSaslMessage(@out, DataTransferProtos.DataTransferEncryptorMessageProto.DataTransferEncryptorStatus
				.ErrorUnknownKey, null, message);
		}
	}
}
