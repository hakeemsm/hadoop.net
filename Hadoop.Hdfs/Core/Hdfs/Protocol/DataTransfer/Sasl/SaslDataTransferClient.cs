using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Javax.Security.Auth.Callback;
using Javax.Security.Sasl;
using Org.Apache.Commons.Codec.Binary;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Crypto;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Net;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Protocol.Datatransfer;
using Org.Apache.Hadoop.Hdfs.Security.Token.Block;
using Org.Apache.Hadoop.Security;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Protocol.Datatransfer.Sasl
{
	/// <summary>Negotiates SASL for DataTransferProtocol on behalf of a client.</summary>
	/// <remarks>
	/// Negotiates SASL for DataTransferProtocol on behalf of a client.  There are
	/// two possible supported variants of SASL negotiation: either a general-purpose
	/// negotiation supporting any quality of protection, or a specialized
	/// negotiation that enforces privacy as the quality of protection using a
	/// cryptographically strong encryption key.
	/// This class is used in both the HDFS client and the DataNode.  The DataNode
	/// needs it, because it acts as a client to other DataNodes during write
	/// pipelines and block transfers.
	/// </remarks>
	public class SaslDataTransferClient
	{
		private static readonly Logger Log = LoggerFactory.GetLogger(typeof(Org.Apache.Hadoop.Hdfs.Protocol.Datatransfer.Sasl.SaslDataTransferClient
			));

		private readonly Configuration conf;

		private readonly AtomicBoolean fallbackToSimpleAuth;

		private readonly SaslPropertiesResolver saslPropsResolver;

		private readonly TrustedChannelResolver trustedChannelResolver;

		/// <summary>Creates a new SaslDataTransferClient.</summary>
		/// <remarks>
		/// Creates a new SaslDataTransferClient.  This constructor is used in cases
		/// where it is not relevant to track if a secure client did a fallback to
		/// simple auth.  For intra-cluster connections between data nodes in the same
		/// cluster, we can assume that all run under the same security configuration.
		/// </remarks>
		/// <param name="conf">the configuration</param>
		/// <param name="saslPropsResolver">for determining properties of SASL negotiation</param>
		/// <param name="trustedChannelResolver">
		/// for identifying trusted connections that do
		/// not require SASL negotiation
		/// </param>
		public SaslDataTransferClient(Configuration conf, SaslPropertiesResolver saslPropsResolver
			, TrustedChannelResolver trustedChannelResolver)
			: this(conf, saslPropsResolver, trustedChannelResolver, null)
		{
		}

		/// <summary>Creates a new SaslDataTransferClient.</summary>
		/// <param name="conf">the configuration</param>
		/// <param name="saslPropsResolver">for determining properties of SASL negotiation</param>
		/// <param name="trustedChannelResolver">
		/// for identifying trusted connections that do
		/// not require SASL negotiation
		/// </param>
		/// <param name="fallbackToSimpleAuth">
		/// checked on each attempt at general SASL
		/// handshake, if true forces use of simple auth
		/// </param>
		public SaslDataTransferClient(Configuration conf, SaslPropertiesResolver saslPropsResolver
			, TrustedChannelResolver trustedChannelResolver, AtomicBoolean fallbackToSimpleAuth
			)
		{
			this.conf = conf;
			this.fallbackToSimpleAuth = fallbackToSimpleAuth;
			this.saslPropsResolver = saslPropsResolver;
			this.trustedChannelResolver = trustedChannelResolver;
		}

		/// <summary>Sends client SASL negotiation for a newly allocated socket if required.</summary>
		/// <param name="socket">connection socket</param>
		/// <param name="underlyingOut">connection output stream</param>
		/// <param name="underlyingIn">connection input stream</param>
		/// <param name="encryptionKeyFactory">for creation of an encryption key</param>
		/// <param name="accessToken">connection block access token</param>
		/// <param name="datanodeId">ID of destination DataNode</param>
		/// <returns>new pair of streams, wrapped after SASL negotiation</returns>
		/// <exception cref="System.IO.IOException">for any error</exception>
		public virtual IOStreamPair NewSocketSend(Socket socket, OutputStream underlyingOut
			, InputStream underlyingIn, DataEncryptionKeyFactory encryptionKeyFactory, Org.Apache.Hadoop.Security.Token.Token
			<BlockTokenIdentifier> accessToken, DatanodeID datanodeId)
		{
			// The encryption key factory only returns a key if encryption is enabled.
			DataEncryptionKey encryptionKey = !trustedChannelResolver.IsTrusted() ? encryptionKeyFactory
				.NewDataEncryptionKey() : null;
			IOStreamPair ios = Send(socket.GetInetAddress(), underlyingOut, underlyingIn, encryptionKey
				, accessToken, datanodeId);
			return ios != null ? ios : new IOStreamPair(underlyingIn, underlyingOut);
		}

		/// <summary>Sends client SASL negotiation for a peer if required.</summary>
		/// <param name="peer">connection peer</param>
		/// <param name="encryptionKeyFactory">for creation of an encryption key</param>
		/// <param name="accessToken">connection block access token</param>
		/// <param name="datanodeId">ID of destination DataNode</param>
		/// <returns>new pair of streams, wrapped after SASL negotiation</returns>
		/// <exception cref="System.IO.IOException">for any error</exception>
		public virtual Peer PeerSend(Peer peer, DataEncryptionKeyFactory encryptionKeyFactory
			, Org.Apache.Hadoop.Security.Token.Token<BlockTokenIdentifier> accessToken, DatanodeID
			 datanodeId)
		{
			IOStreamPair ios = CheckTrustAndSend(DataTransferSaslUtil.GetPeerAddress(peer), peer
				.GetOutputStream(), peer.GetInputStream(), encryptionKeyFactory, accessToken, datanodeId
				);
			// TODO: Consider renaming EncryptedPeer to SaslPeer.
			return ios != null ? new EncryptedPeer(peer, ios) : peer;
		}

		/// <summary>Sends client SASL negotiation for a socket if required.</summary>
		/// <param name="socket">connection socket</param>
		/// <param name="underlyingOut">connection output stream</param>
		/// <param name="underlyingIn">connection input stream</param>
		/// <param name="encryptionKeyFactory">for creation of an encryption key</param>
		/// <param name="accessToken">connection block access token</param>
		/// <param name="datanodeId">ID of destination DataNode</param>
		/// <returns>new pair of streams, wrapped after SASL negotiation</returns>
		/// <exception cref="System.IO.IOException">for any error</exception>
		public virtual IOStreamPair SocketSend(Socket socket, OutputStream underlyingOut, 
			InputStream underlyingIn, DataEncryptionKeyFactory encryptionKeyFactory, Org.Apache.Hadoop.Security.Token.Token
			<BlockTokenIdentifier> accessToken, DatanodeID datanodeId)
		{
			IOStreamPair ios = CheckTrustAndSend(socket.GetInetAddress(), underlyingOut, underlyingIn
				, encryptionKeyFactory, accessToken, datanodeId);
			return ios != null ? ios : new IOStreamPair(underlyingIn, underlyingOut);
		}

		/// <summary>
		/// Checks if an address is already trusted and then sends client SASL
		/// negotiation if required.
		/// </summary>
		/// <param name="addr">connection address</param>
		/// <param name="underlyingOut">connection output stream</param>
		/// <param name="underlyingIn">connection input stream</param>
		/// <param name="encryptionKeyFactory">for creation of an encryption key</param>
		/// <param name="accessToken">connection block access token</param>
		/// <param name="datanodeId">ID of destination DataNode</param>
		/// <returns>new pair of streams, wrapped after SASL negotiation</returns>
		/// <exception cref="System.IO.IOException">for any error</exception>
		private IOStreamPair CheckTrustAndSend(IPAddress addr, OutputStream underlyingOut
			, InputStream underlyingIn, DataEncryptionKeyFactory encryptionKeyFactory, Org.Apache.Hadoop.Security.Token.Token
			<BlockTokenIdentifier> accessToken, DatanodeID datanodeId)
		{
			if (!trustedChannelResolver.IsTrusted() && !trustedChannelResolver.IsTrusted(addr
				))
			{
				// The encryption key factory only returns a key if encryption is enabled.
				DataEncryptionKey encryptionKey = encryptionKeyFactory.NewDataEncryptionKey();
				return Send(addr, underlyingOut, underlyingIn, encryptionKey, accessToken, datanodeId
					);
			}
			else
			{
				Log.Debug("SASL client skipping handshake on trusted connection for addr = {}, " 
					+ "datanodeId = {}", addr, datanodeId);
				return null;
			}
		}

		/// <summary>Sends client SASL negotiation if required.</summary>
		/// <remarks>
		/// Sends client SASL negotiation if required.  Determines the correct type of
		/// SASL handshake based on configuration.
		/// </remarks>
		/// <param name="addr">connection address</param>
		/// <param name="underlyingOut">connection output stream</param>
		/// <param name="underlyingIn">connection input stream</param>
		/// <param name="encryptionKey">for an encrypted SASL handshake</param>
		/// <param name="accessToken">connection block access token</param>
		/// <param name="datanodeId">ID of destination DataNode</param>
		/// <returns>new pair of streams, wrapped after SASL negotiation</returns>
		/// <exception cref="System.IO.IOException">for any error</exception>
		private IOStreamPair Send(IPAddress addr, OutputStream underlyingOut, InputStream
			 underlyingIn, DataEncryptionKey encryptionKey, Org.Apache.Hadoop.Security.Token.Token
			<BlockTokenIdentifier> accessToken, DatanodeID datanodeId)
		{
			if (encryptionKey != null)
			{
				Log.Debug("SASL client doing encrypted handshake for addr = {}, datanodeId = {}", 
					addr, datanodeId);
				return GetEncryptedStreams(underlyingOut, underlyingIn, encryptionKey);
			}
			else
			{
				if (!UserGroupInformation.IsSecurityEnabled())
				{
					Log.Debug("SASL client skipping handshake in unsecured configuration for " + "addr = {}, datanodeId = {}"
						, addr, datanodeId);
					return null;
				}
				else
				{
					if (SecurityUtil.IsPrivilegedPort(datanodeId.GetXferPort()))
					{
						Log.Debug("SASL client skipping handshake in secured configuration with " + "privileged port for addr = {}, datanodeId = {}"
							, addr, datanodeId);
						return null;
					}
					else
					{
						if (fallbackToSimpleAuth != null && fallbackToSimpleAuth.Get())
						{
							Log.Debug("SASL client skipping handshake in secured configuration with " + "unsecured cluster for addr = {}, datanodeId = {}"
								, addr, datanodeId);
							return null;
						}
						else
						{
							if (saslPropsResolver != null)
							{
								Log.Debug("SASL client doing general handshake for addr = {}, datanodeId = {}", addr
									, datanodeId);
								return GetSaslStreams(addr, underlyingOut, underlyingIn, accessToken, datanodeId);
							}
							else
							{
								// It's a secured cluster using non-privileged ports, but no SASL.  The
								// only way this can happen is if the DataNode has
								// ignore.secure.ports.for.testing configured, so this is a rare edge case.
								Log.Debug("SASL client skipping handshake in secured configuration with no SASL "
									 + "protection configured for addr = {}, datanodeId = {}", addr, datanodeId);
								return null;
							}
						}
					}
				}
			}
		}

		/// <summary>Sends client SASL negotiation for specialized encrypted handshake.</summary>
		/// <param name="underlyingOut">connection output stream</param>
		/// <param name="underlyingIn">connection input stream</param>
		/// <param name="encryptionKey">for an encrypted SASL handshake</param>
		/// <returns>new pair of streams, wrapped after SASL negotiation</returns>
		/// <exception cref="System.IO.IOException">for any error</exception>
		private IOStreamPair GetEncryptedStreams(OutputStream underlyingOut, InputStream 
			underlyingIn, DataEncryptionKey encryptionKey)
		{
			IDictionary<string, string> saslProps = DataTransferSaslUtil.CreateSaslPropertiesForEncryption
				(encryptionKey.encryptionAlgorithm);
			Log.Debug("Client using encryption algorithm {}", encryptionKey.encryptionAlgorithm
				);
			string userName = GetUserNameFromEncryptionKey(encryptionKey);
			char[] password = DataTransferSaslUtil.EncryptionKeyToPassword(encryptionKey.encryptionKey
				);
			CallbackHandler callbackHandler = new SaslDataTransferClient.SaslClientCallbackHandler
				(userName, password);
			return DoSaslHandshake(underlyingOut, underlyingIn, userName, saslProps, callbackHandler
				);
		}

		/// <summary>
		/// The SASL username for an encrypted handshake consists of the keyId,
		/// blockPoolId, and nonce with the first two encoded as Strings, and the third
		/// encoded using Base64.
		/// </summary>
		/// <remarks>
		/// The SASL username for an encrypted handshake consists of the keyId,
		/// blockPoolId, and nonce with the first two encoded as Strings, and the third
		/// encoded using Base64. The fields are each separated by a single space.
		/// </remarks>
		/// <param name="encryptionKey">the encryption key to encode as a SASL username.</param>
		/// <returns>encoded username containing keyId, blockPoolId, and nonce</returns>
		private static string GetUserNameFromEncryptionKey(DataEncryptionKey encryptionKey
			)
		{
			return encryptionKey.keyId + NameDelimiter + encryptionKey.blockPoolId + NameDelimiter
				 + new string(Base64.EncodeBase64(encryptionKey.nonce, false), Charsets.Utf8);
		}

		/// <summary>Sets user name and password when asked by the client-side SASL object.</summary>
		private sealed class SaslClientCallbackHandler : CallbackHandler
		{
			private readonly char[] password;

			private readonly string userName;

			/// <summary>Creates a new SaslClientCallbackHandler.</summary>
			/// <param name="userName">SASL user name</param>
			/// <Param>password SASL password</Param>
			public SaslClientCallbackHandler(string userName, char[] password)
			{
				this.password = password;
				this.userName = userName;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Javax.Security.Auth.Callback.UnsupportedCallbackException"/>
			public void Handle(Javax.Security.Auth.Callback.Callback[] callbacks)
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
					nc.SetName(userName);
				}
				if (pc != null)
				{
					pc.SetPassword(password);
				}
				if (rc != null)
				{
					rc.SetText(rc.GetDefaultText());
				}
			}
		}

		/// <summary>Sends client SASL negotiation for general-purpose handshake.</summary>
		/// <param name="addr">connection address</param>
		/// <param name="underlyingOut">connection output stream</param>
		/// <param name="underlyingIn">connection input stream</param>
		/// <param name="accessToken">connection block access token</param>
		/// <param name="datanodeId">ID of destination DataNode</param>
		/// <returns>new pair of streams, wrapped after SASL negotiation</returns>
		/// <exception cref="System.IO.IOException">for any error</exception>
		private IOStreamPair GetSaslStreams(IPAddress addr, OutputStream underlyingOut, InputStream
			 underlyingIn, Org.Apache.Hadoop.Security.Token.Token<BlockTokenIdentifier> accessToken
			, DatanodeID datanodeId)
		{
			IDictionary<string, string> saslProps = saslPropsResolver.GetClientProperties(addr
				);
			string userName = BuildUserName(accessToken);
			char[] password = BuildClientPassword(accessToken);
			CallbackHandler callbackHandler = new SaslDataTransferClient.SaslClientCallbackHandler
				(userName, password);
			return DoSaslHandshake(underlyingOut, underlyingIn, userName, saslProps, callbackHandler
				);
		}

		/// <summary>
		/// Builds the client's user name for the general-purpose handshake, consisting
		/// of the base64-encoded serialized block access token identifier.
		/// </summary>
		/// <remarks>
		/// Builds the client's user name for the general-purpose handshake, consisting
		/// of the base64-encoded serialized block access token identifier.  Note that
		/// this includes only the token identifier, not the token itself, which would
		/// include the password.  The password is a shared secret, and we must not
		/// write it on the network during the SASL authentication exchange.
		/// </remarks>
		/// <param name="blockToken">for block access</param>
		/// <returns>SASL user name</returns>
		private static string BuildUserName(Org.Apache.Hadoop.Security.Token.Token<BlockTokenIdentifier
			> blockToken)
		{
			return new string(Base64.EncodeBase64(blockToken.GetIdentifier(), false), Charsets
				.Utf8);
		}

		/// <summary>
		/// Calculates the password on the client side for the general-purpose
		/// handshake.
		/// </summary>
		/// <remarks>
		/// Calculates the password on the client side for the general-purpose
		/// handshake.  The password consists of the block access token's password.
		/// </remarks>
		/// <param name="blockToken">for block access</param>
		/// <returns>SASL password</returns>
		private char[] BuildClientPassword(Org.Apache.Hadoop.Security.Token.Token<BlockTokenIdentifier
			> blockToken)
		{
			return new string(Base64.EncodeBase64(blockToken.GetPassword(), false), Charsets.
				Utf8).ToCharArray();
		}

		/// <summary>This method actually executes the client-side SASL handshake.</summary>
		/// <param name="underlyingOut">connection output stream</param>
		/// <param name="underlyingIn">connection input stream</param>
		/// <param name="userName">SASL user name</param>
		/// <param name="saslProps">properties of SASL negotiation</param>
		/// <param name="callbackHandler">for responding to SASL callbacks</param>
		/// <returns>new pair of streams, wrapped after SASL negotiation</returns>
		/// <exception cref="System.IO.IOException">for any error</exception>
		private IOStreamPair DoSaslHandshake(OutputStream underlyingOut, InputStream underlyingIn
			, string userName, IDictionary<string, string> saslProps, CallbackHandler callbackHandler
			)
		{
			DataOutputStream @out = new DataOutputStream(underlyingOut);
			DataInputStream @in = new DataInputStream(underlyingIn);
			SaslParticipant sasl = SaslParticipant.CreateClientSaslParticipant(userName, saslProps
				, callbackHandler);
			@out.WriteInt(SaslTransferMagicNumber);
			@out.Flush();
			try
			{
				// Start of handshake - "initial response" in SASL terminology.
				DataTransferSaslUtil.SendSaslMessage(@out, new byte[0]);
				// step 1
				byte[] remoteResponse = DataTransferSaslUtil.ReadSaslMessage(@in);
				byte[] localResponse = sasl.EvaluateChallengeOrResponse(remoteResponse);
				IList<CipherOption> cipherOptions = null;
				if (DataTransferSaslUtil.RequestedQopContainsPrivacy(saslProps))
				{
					// Negotiate cipher suites if configured.  Currently, the only supported
					// cipher suite is AES/CTR/NoPadding, but the protocol allows multiple
					// values for future expansion.
					string cipherSuites = conf.Get(DFSConfigKeys.DfsEncryptDataTransferCipherSuitesKey
						);
					if (cipherSuites != null && !cipherSuites.IsEmpty())
					{
						if (!cipherSuites.Equals(CipherSuite.AesCtrNopadding.GetName()))
						{
							throw new IOException(string.Format("Invalid cipher suite, %s=%s", DFSConfigKeys.
								DfsEncryptDataTransferCipherSuitesKey, cipherSuites));
						}
						CipherOption option = new CipherOption(CipherSuite.AesCtrNopadding);
						cipherOptions = Lists.NewArrayListWithCapacity(1);
						cipherOptions.AddItem(option);
					}
				}
				DataTransferSaslUtil.SendSaslMessageAndNegotiationCipherOptions(@out, localResponse
					, cipherOptions);
				// step 2 (client-side only)
				SaslResponseWithNegotiatedCipherOption response = DataTransferSaslUtil.ReadSaslMessageAndNegotiatedCipherOption
					(@in);
				localResponse = sasl.EvaluateChallengeOrResponse(response.payload);
				System.Diagnostics.Debug.Assert(localResponse == null);
				// SASL handshake is complete
				DataTransferSaslUtil.CheckSaslComplete(sasl, saslProps);
				CipherOption cipherOption = null;
				if (sasl.IsNegotiatedQopPrivacy())
				{
					// Unwrap the negotiated cipher option
					cipherOption = DataTransferSaslUtil.Unwrap(response.cipherOption, sasl);
				}
				// If negotiated cipher option is not null, we will use it to create 
				// stream pair.
				return cipherOption != null ? DataTransferSaslUtil.CreateStreamPair(conf, cipherOption
					, underlyingOut, underlyingIn, false) : sasl.CreateStreamPair(@out, @in);
			}
			catch (IOException ioe)
			{
				DataTransferSaslUtil.SendGenericSaslErrorMessage(@out, ioe.Message);
				throw;
			}
		}
	}
}
