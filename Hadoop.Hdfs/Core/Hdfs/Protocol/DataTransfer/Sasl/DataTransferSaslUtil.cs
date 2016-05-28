using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Com.Google.Common.Net;
using Com.Google.Protobuf;
using Org.Apache.Commons.Codec.Binary;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Crypto;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Net;
using Org.Apache.Hadoop.Hdfs.Protocol.Datatransfer;
using Org.Apache.Hadoop.Hdfs.Protocol.Proto;
using Org.Apache.Hadoop.Hdfs.ProtocolPB;
using Org.Apache.Hadoop.Security;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Protocol.Datatransfer.Sasl
{
	/// <summary>Utility methods implementing SASL negotiation for DataTransferProtocol.</summary>
	public sealed class DataTransferSaslUtil
	{
		private static readonly Logger Log = LoggerFactory.GetLogger(typeof(Org.Apache.Hadoop.Hdfs.Protocol.Datatransfer.Sasl.DataTransferSaslUtil
			));

		/// <summary>Delimiter for the three-part SASL username string.</summary>
		public const string NameDelimiter = " ";

		/// <summary>Sent by clients and validated by servers.</summary>
		/// <remarks>
		/// Sent by clients and validated by servers. We use a number that's unlikely
		/// to ever be sent as the value of the DATA_TRANSFER_VERSION.
		/// </remarks>
		public const int SaslTransferMagicNumber = unchecked((int)(0xDEADBEEF));

		/// <summary>
		/// Checks that SASL negotiation has completed for the given participant, and
		/// the negotiated quality of protection is included in the given SASL
		/// properties and therefore acceptable.
		/// </summary>
		/// <param name="sasl">participant to check</param>
		/// <param name="saslProps">properties of SASL negotiation</param>
		/// <exception cref="System.IO.IOException">for any error</exception>
		public static void CheckSaslComplete(SaslParticipant sasl, IDictionary<string, string
			> saslProps)
		{
			if (!sasl.IsComplete())
			{
				throw new IOException("Failed to complete SASL handshake");
			}
			ICollection<string> requestedQop = ImmutableSet.CopyOf(Arrays.AsList(saslProps[Javax.Security.Sasl.Sasl
				.Qop].Split(",")));
			string negotiatedQop = sasl.GetNegotiatedQop();
			Log.Debug("Verifying QOP, requested QOP = {}, negotiated QOP = {}", requestedQop, 
				negotiatedQop);
			if (!requestedQop.Contains(negotiatedQop))
			{
				throw new IOException(string.Format("SASL handshake completed, but " + "channel does not have acceptable quality of protection, "
					 + "requested = %s, negotiated = %s", requestedQop, negotiatedQop));
			}
		}

		/// <summary>Check whether requested SASL Qop contains privacy.</summary>
		/// <param name="saslProps">properties of SASL negotiation</param>
		/// <returns>boolean true if privacy exists</returns>
		public static bool RequestedQopContainsPrivacy(IDictionary<string, string> saslProps
			)
		{
			ICollection<string> requestedQop = ImmutableSet.CopyOf(Arrays.AsList(saslProps[Javax.Security.Sasl.Sasl
				.Qop].Split(",")));
			return requestedQop.Contains("auth-conf");
		}

		/// <summary>Creates SASL properties required for an encrypted SASL negotiation.</summary>
		/// <param name="encryptionAlgorithm">to use for SASL negotation</param>
		/// <returns>properties of encrypted SASL negotiation</returns>
		public static IDictionary<string, string> CreateSaslPropertiesForEncryption(string
			 encryptionAlgorithm)
		{
			IDictionary<string, string> saslProps = Maps.NewHashMapWithExpectedSize(3);
			saslProps[Javax.Security.Sasl.Sasl.Qop] = SaslRpcServer.QualityOfProtection.Privacy
				.GetSaslQop();
			saslProps[Javax.Security.Sasl.Sasl.ServerAuth] = "true";
			saslProps["com.sun.security.sasl.digest.cipher"] = encryptionAlgorithm;
			return saslProps;
		}

		/// <summary>
		/// For an encrypted SASL negotiation, encodes an encryption key to a SASL
		/// password.
		/// </summary>
		/// <param name="encryptionKey">to encode</param>
		/// <returns>key encoded as SASL password</returns>
		public static char[] EncryptionKeyToPassword(byte[] encryptionKey)
		{
			return new string(Base64.EncodeBase64(encryptionKey, false), Charsets.Utf8).ToCharArray
				();
		}

		/// <summary>Returns InetAddress from peer.</summary>
		/// <remarks>
		/// Returns InetAddress from peer.  The getRemoteAddressString has the form
		/// [host][/ip-address]:port.  The host may be missing.  The IP address (and
		/// preceding '/') may be missing.  The port preceded by ':' is always present.
		/// </remarks>
		/// <param name="peer"/>
		/// <returns>InetAddress from peer</returns>
		public static IPAddress GetPeerAddress(Peer peer)
		{
			string remoteAddr = peer.GetRemoteAddressString().Split(":")[0];
			int slashIdx = remoteAddr.IndexOf('/');
			return InetAddresses.ForString(slashIdx != -1 ? Sharpen.Runtime.Substring(remoteAddr
				, slashIdx + 1, remoteAddr.Length) : remoteAddr);
		}

		/// <summary>Creates a SaslPropertiesResolver from the given configuration.</summary>
		/// <remarks>
		/// Creates a SaslPropertiesResolver from the given configuration.  This method
		/// works by cloning the configuration, translating configuration properties
		/// specific to DataTransferProtocol to what SaslPropertiesResolver expects,
		/// and then delegating to SaslPropertiesResolver for initialization.  This
		/// method returns null if SASL protection has not been configured for
		/// DataTransferProtocol.
		/// </remarks>
		/// <param name="conf">configuration to read</param>
		/// <returns>
		/// SaslPropertiesResolver for DataTransferProtocol, or null if not
		/// configured
		/// </returns>
		public static SaslPropertiesResolver GetSaslPropertiesResolver(Configuration conf
			)
		{
			string qops = conf.Get(DFSConfigKeys.DfsDataTransferProtectionKey);
			if (qops == null || qops.IsEmpty())
			{
				Log.Debug("DataTransferProtocol not using SaslPropertiesResolver, no " + "QOP found in configuration for {}"
					, DFSConfigKeys.DfsDataTransferProtectionKey);
				return null;
			}
			Configuration saslPropsResolverConf = new Configuration(conf);
			saslPropsResolverConf.Set(CommonConfigurationKeysPublic.HadoopRpcProtection, qops
				);
			Type resolverClass = conf.GetClass<SaslPropertiesResolver>(CommonConfigurationKeysPublic
				.HadoopSecuritySaslPropsResolverClass, typeof(SaslPropertiesResolver));
			resolverClass = conf.GetClass<SaslPropertiesResolver>(DFSConfigKeys.DfsDataTransferSaslPropsResolverClassKey
				, resolverClass);
			saslPropsResolverConf.SetClass(CommonConfigurationKeysPublic.HadoopSecuritySaslPropsResolverClass
				, resolverClass, typeof(SaslPropertiesResolver));
			SaslPropertiesResolver resolver = SaslPropertiesResolver.GetInstance(saslPropsResolverConf
				);
			Log.Debug("DataTransferProtocol using SaslPropertiesResolver, configured " + "QOP {} = {}, configured class {} = {}"
				, DFSConfigKeys.DfsDataTransferProtectionKey, qops, DFSConfigKeys.DfsDataTransferSaslPropsResolverClassKey
				, resolverClass);
			return resolver;
		}

		/// <summary>Reads a SASL negotiation message.</summary>
		/// <param name="in">stream to read</param>
		/// <returns>bytes of SASL negotiation messsage</returns>
		/// <exception cref="System.IO.IOException">for any error</exception>
		public static byte[] ReadSaslMessage(InputStream @in)
		{
			DataTransferProtos.DataTransferEncryptorMessageProto proto = DataTransferProtos.DataTransferEncryptorMessageProto
				.ParseFrom(PBHelper.VintPrefixed(@in));
			if (proto.GetStatus() == DataTransferProtos.DataTransferEncryptorMessageProto.DataTransferEncryptorStatus
				.ErrorUnknownKey)
			{
				throw new InvalidEncryptionKeyException(proto.GetMessage());
			}
			else
			{
				if (proto.GetStatus() == DataTransferProtos.DataTransferEncryptorMessageProto.DataTransferEncryptorStatus
					.Error)
				{
					throw new IOException(proto.GetMessage());
				}
				else
				{
					return proto.GetPayload().ToByteArray();
				}
			}
		}

		/// <summary>Reads a SASL negotiation message and negotiation cipher options.</summary>
		/// <param name="in">stream to read</param>
		/// <param name="cipherOptions">list to store negotiation cipher options</param>
		/// <returns>byte[] SASL negotiation message</returns>
		/// <exception cref="System.IO.IOException">for any error</exception>
		public static byte[] ReadSaslMessageAndNegotiationCipherOptions(InputStream @in, 
			IList<CipherOption> cipherOptions)
		{
			DataTransferProtos.DataTransferEncryptorMessageProto proto = DataTransferProtos.DataTransferEncryptorMessageProto
				.ParseFrom(PBHelper.VintPrefixed(@in));
			if (proto.GetStatus() == DataTransferProtos.DataTransferEncryptorMessageProto.DataTransferEncryptorStatus
				.ErrorUnknownKey)
			{
				throw new InvalidEncryptionKeyException(proto.GetMessage());
			}
			else
			{
				if (proto.GetStatus() == DataTransferProtos.DataTransferEncryptorMessageProto.DataTransferEncryptorStatus
					.Error)
				{
					throw new IOException(proto.GetMessage());
				}
				else
				{
					IList<HdfsProtos.CipherOptionProto> optionProtos = proto.GetCipherOptionList();
					if (optionProtos != null)
					{
						foreach (HdfsProtos.CipherOptionProto optionProto in optionProtos)
						{
							cipherOptions.AddItem(PBHelper.Convert(optionProto));
						}
					}
					return proto.GetPayload().ToByteArray();
				}
			}
		}

		/// <summary>Negotiate a cipher option which server supports.</summary>
		/// <param name="conf">the configuration</param>
		/// <param name="options">the cipher options which client supports</param>
		/// <returns>CipherOption negotiated cipher option</returns>
		/// <exception cref="System.IO.IOException"/>
		public static CipherOption NegotiateCipherOption(Configuration conf, IList<CipherOption
			> options)
		{
			// Negotiate cipher suites if configured.  Currently, the only supported
			// cipher suite is AES/CTR/NoPadding, but the protocol allows multiple
			// values for future expansion.
			string cipherSuites = conf.Get(DFSConfigKeys.DfsEncryptDataTransferCipherSuitesKey
				);
			if (cipherSuites == null || cipherSuites.IsEmpty())
			{
				return null;
			}
			if (!cipherSuites.Equals(CipherSuite.AesCtrNopadding.GetName()))
			{
				throw new IOException(string.Format("Invalid cipher suite, %s=%s", DFSConfigKeys.
					DfsEncryptDataTransferCipherSuitesKey, cipherSuites));
			}
			if (options != null)
			{
				foreach (CipherOption option in options)
				{
					CipherSuite suite = option.GetCipherSuite();
					if (suite == CipherSuite.AesCtrNopadding)
					{
						int keyLen = conf.GetInt(DFSConfigKeys.DfsEncryptDataTransferCipherKeyBitlengthKey
							, DFSConfigKeys.DfsEncryptDataTransferCipherKeyBitlengthDefault) / 8;
						CryptoCodec codec = CryptoCodec.GetInstance(conf, suite);
						byte[] inKey = new byte[keyLen];
						byte[] inIv = new byte[suite.GetAlgorithmBlockSize()];
						byte[] outKey = new byte[keyLen];
						byte[] outIv = new byte[suite.GetAlgorithmBlockSize()];
						codec.GenerateSecureRandom(inKey);
						codec.GenerateSecureRandom(inIv);
						codec.GenerateSecureRandom(outKey);
						codec.GenerateSecureRandom(outIv);
						return new CipherOption(suite, inKey, inIv, outKey, outIv);
					}
				}
			}
			return null;
		}

		/// <summary>Send SASL message and negotiated cipher option to client.</summary>
		/// <param name="out">stream to receive message</param>
		/// <param name="payload">to send</param>
		/// <param name="option">negotiated cipher option</param>
		/// <exception cref="System.IO.IOException">for any error</exception>
		public static void SendSaslMessageAndNegotiatedCipherOption(OutputStream @out, byte
			[] payload, CipherOption option)
		{
			DataTransferProtos.DataTransferEncryptorMessageProto.Builder builder = DataTransferProtos.DataTransferEncryptorMessageProto
				.NewBuilder();
			builder.SetStatus(DataTransferProtos.DataTransferEncryptorMessageProto.DataTransferEncryptorStatus
				.Success);
			if (payload != null)
			{
				builder.SetPayload(ByteString.CopyFrom(payload));
			}
			if (option != null)
			{
				builder.AddCipherOption(PBHelper.Convert(option));
			}
			DataTransferProtos.DataTransferEncryptorMessageProto proto = ((DataTransferProtos.DataTransferEncryptorMessageProto
				)builder.Build());
			proto.WriteDelimitedTo(@out);
			@out.Flush();
		}

		/// <summary>
		/// Create IOStreamPair of
		/// <see cref="Org.Apache.Hadoop.Crypto.CryptoInputStream"/>
		/// and
		/// <see cref="Org.Apache.Hadoop.Crypto.CryptoOutputStream"/>
		/// </summary>
		/// <param name="conf">the configuration</param>
		/// <param name="cipherOption">negotiated cipher option</param>
		/// <param name="out">underlying output stream</param>
		/// <param name="in">underlying input stream</param>
		/// <param name="isServer">is server side</param>
		/// <returns>IOStreamPair the stream pair</returns>
		/// <exception cref="System.IO.IOException">for any error</exception>
		public static IOStreamPair CreateStreamPair(Configuration conf, CipherOption cipherOption
			, OutputStream @out, InputStream @in, bool isServer)
		{
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Creating IOStreamPair of CryptoInputStream and " + "CryptoOutputStream."
					);
			}
			CryptoCodec codec = CryptoCodec.GetInstance(conf, cipherOption.GetCipherSuite());
			byte[] inKey = cipherOption.GetInKey();
			byte[] inIv = cipherOption.GetInIv();
			byte[] outKey = cipherOption.GetOutKey();
			byte[] outIv = cipherOption.GetOutIv();
			InputStream cIn = new CryptoInputStream(@in, codec, isServer ? inKey : outKey, isServer
				 ? inIv : outIv);
			OutputStream cOut = new CryptoOutputStream(@out, codec, isServer ? outKey : inKey
				, isServer ? outIv : inIv);
			return new IOStreamPair(cIn, cOut);
		}

		/// <summary>Sends a SASL negotiation message indicating an error.</summary>
		/// <param name="out">stream to receive message</param>
		/// <param name="message">to send</param>
		/// <exception cref="System.IO.IOException">for any error</exception>
		public static void SendGenericSaslErrorMessage(OutputStream @out, string message)
		{
			SendSaslMessage(@out, DataTransferProtos.DataTransferEncryptorMessageProto.DataTransferEncryptorStatus
				.Error, null, message);
		}

		/// <summary>Sends a SASL negotiation message.</summary>
		/// <param name="out">stream to receive message</param>
		/// <param name="payload">to send</param>
		/// <exception cref="System.IO.IOException">for any error</exception>
		public static void SendSaslMessage(OutputStream @out, byte[] payload)
		{
			SendSaslMessage(@out, DataTransferProtos.DataTransferEncryptorMessageProto.DataTransferEncryptorStatus
				.Success, payload, null);
		}

		/// <summary>Send a SASL negotiation message and negotiation cipher options to server.
		/// 	</summary>
		/// <param name="out">stream to receive message</param>
		/// <param name="payload">to send</param>
		/// <param name="options">cipher options to negotiate</param>
		/// <exception cref="System.IO.IOException">for any error</exception>
		public static void SendSaslMessageAndNegotiationCipherOptions(OutputStream @out, 
			byte[] payload, IList<CipherOption> options)
		{
			DataTransferProtos.DataTransferEncryptorMessageProto.Builder builder = DataTransferProtos.DataTransferEncryptorMessageProto
				.NewBuilder();
			builder.SetStatus(DataTransferProtos.DataTransferEncryptorMessageProto.DataTransferEncryptorStatus
				.Success);
			if (payload != null)
			{
				builder.SetPayload(ByteString.CopyFrom(payload));
			}
			if (options != null)
			{
				builder.AddAllCipherOption(PBHelper.ConvertCipherOptions(options));
			}
			DataTransferProtos.DataTransferEncryptorMessageProto proto = ((DataTransferProtos.DataTransferEncryptorMessageProto
				)builder.Build());
			proto.WriteDelimitedTo(@out);
			@out.Flush();
		}

		/// <summary>Read SASL message and negotiated cipher option from server.</summary>
		/// <param name="in">stream to read</param>
		/// <returns>
		/// SaslResponseWithNegotiatedCipherOption SASL message and
		/// negotiated cipher option
		/// </returns>
		/// <exception cref="System.IO.IOException">for any error</exception>
		public static SaslResponseWithNegotiatedCipherOption ReadSaslMessageAndNegotiatedCipherOption
			(InputStream @in)
		{
			DataTransferProtos.DataTransferEncryptorMessageProto proto = DataTransferProtos.DataTransferEncryptorMessageProto
				.ParseFrom(PBHelper.VintPrefixed(@in));
			if (proto.GetStatus() == DataTransferProtos.DataTransferEncryptorMessageProto.DataTransferEncryptorStatus
				.ErrorUnknownKey)
			{
				throw new InvalidEncryptionKeyException(proto.GetMessage());
			}
			else
			{
				if (proto.GetStatus() == DataTransferProtos.DataTransferEncryptorMessageProto.DataTransferEncryptorStatus
					.Error)
				{
					throw new IOException(proto.GetMessage());
				}
				else
				{
					byte[] response = proto.GetPayload().ToByteArray();
					IList<CipherOption> options = PBHelper.ConvertCipherOptionProtos(proto.GetCipherOptionList
						());
					CipherOption option = null;
					if (options != null && !options.IsEmpty())
					{
						option = options[0];
					}
					return new SaslResponseWithNegotiatedCipherOption(response, option);
				}
			}
		}

		/// <summary>Encrypt the key and iv of the negotiated cipher option.</summary>
		/// <param name="option">negotiated cipher option</param>
		/// <param name="sasl">SASL participant representing server</param>
		/// <returns>
		/// CipherOption negotiated cipher option which contains the
		/// encrypted key and iv
		/// </returns>
		/// <exception cref="System.IO.IOException">for any error</exception>
		public static CipherOption Wrap(CipherOption option, SaslParticipant sasl)
		{
			if (option != null)
			{
				byte[] inKey = option.GetInKey();
				if (inKey != null)
				{
					inKey = sasl.Wrap(inKey, 0, inKey.Length);
				}
				byte[] outKey = option.GetOutKey();
				if (outKey != null)
				{
					outKey = sasl.Wrap(outKey, 0, outKey.Length);
				}
				return new CipherOption(option.GetCipherSuite(), inKey, option.GetInIv(), outKey, 
					option.GetOutIv());
			}
			return null;
		}

		/// <summary>Decrypt the key and iv of the negotiated cipher option.</summary>
		/// <param name="option">negotiated cipher option</param>
		/// <param name="sasl">SASL participant representing client</param>
		/// <returns>
		/// CipherOption negotiated cipher option which contains the
		/// decrypted key and iv
		/// </returns>
		/// <exception cref="System.IO.IOException">for any error</exception>
		public static CipherOption Unwrap(CipherOption option, SaslParticipant sasl)
		{
			if (option != null)
			{
				byte[] inKey = option.GetInKey();
				if (inKey != null)
				{
					inKey = sasl.Unwrap(inKey, 0, inKey.Length);
				}
				byte[] outKey = option.GetOutKey();
				if (outKey != null)
				{
					outKey = sasl.Unwrap(outKey, 0, outKey.Length);
				}
				return new CipherOption(option.GetCipherSuite(), inKey, option.GetInIv(), outKey, 
					option.GetOutIv());
			}
			return null;
		}

		/// <summary>Sends a SASL negotiation message.</summary>
		/// <param name="out">stream to receive message</param>
		/// <param name="status">negotiation status</param>
		/// <param name="payload">to send</param>
		/// <param name="message">to send</param>
		/// <exception cref="System.IO.IOException">for any error</exception>
		public static void SendSaslMessage(OutputStream @out, DataTransferProtos.DataTransferEncryptorMessageProto.DataTransferEncryptorStatus
			 status, byte[] payload, string message)
		{
			DataTransferProtos.DataTransferEncryptorMessageProto.Builder builder = DataTransferProtos.DataTransferEncryptorMessageProto
				.NewBuilder();
			builder.SetStatus(status);
			if (payload != null)
			{
				builder.SetPayload(ByteString.CopyFrom(payload));
			}
			if (message != null)
			{
				builder.SetMessage(message);
			}
			DataTransferProtos.DataTransferEncryptorMessageProto proto = ((DataTransferProtos.DataTransferEncryptorMessageProto
				)builder.Build());
			proto.WriteDelimitedTo(@out);
			@out.Flush();
		}

		/// <summary>There is no reason to instantiate this class.</summary>
		private DataTransferSaslUtil()
		{
		}
	}
}
