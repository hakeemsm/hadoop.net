using System.Collections.Generic;
using System.IO;
using Javax.Security.Auth.Callback;
using Javax.Security.Sasl;
using Org.Apache.Hadoop.Hdfs.Protocol.Datatransfer;
using Org.Apache.Hadoop.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Protocol.Datatransfer.Sasl
{
	/// <summary>Strongly inspired by Thrift's TSaslTransport class.</summary>
	/// <remarks>
	/// Strongly inspired by Thrift's TSaslTransport class.
	/// Used to abstract over the <code>SaslServer</code> and
	/// <code>SaslClient</code> classes, which share a lot of their interface, but
	/// unfortunately don't share a common superclass.
	/// </remarks>
	internal class SaslParticipant
	{
		private const string ServerName = "0";

		private const string Protocol = "hdfs";

		private const string Mechanism = "DIGEST-MD5";

		private readonly SaslServer saslServer;

		private readonly SaslClient saslClient;

		// This has to be set as part of the SASL spec, but it don't matter for
		// our purposes, but may not be empty. It's sent over the wire, so use
		// a short string.
		// One of these will always be null.
		/// <summary>Creates a SaslParticipant wrapping a SaslServer.</summary>
		/// <param name="saslProps">properties of SASL negotiation</param>
		/// <param name="callbackHandler">for handling all SASL callbacks</param>
		/// <returns>SaslParticipant wrapping SaslServer</returns>
		/// <exception cref="Javax.Security.Sasl.SaslException">for any error</exception>
		public static Org.Apache.Hadoop.Hdfs.Protocol.Datatransfer.Sasl.SaslParticipant CreateServerSaslParticipant
			(IDictionary<string, string> saslProps, CallbackHandler callbackHandler)
		{
			return new Org.Apache.Hadoop.Hdfs.Protocol.Datatransfer.Sasl.SaslParticipant(Javax.Security.Sasl.Sasl
				.CreateSaslServer(Mechanism, Protocol, ServerName, saslProps, callbackHandler));
		}

		/// <summary>Creates a SaslParticipant wrapping a SaslClient.</summary>
		/// <param name="userName">SASL user name</param>
		/// <param name="saslProps">properties of SASL negotiation</param>
		/// <param name="callbackHandler">for handling all SASL callbacks</param>
		/// <returns>SaslParticipant wrapping SaslClient</returns>
		/// <exception cref="Javax.Security.Sasl.SaslException">for any error</exception>
		public static Org.Apache.Hadoop.Hdfs.Protocol.Datatransfer.Sasl.SaslParticipant CreateClientSaslParticipant
			(string userName, IDictionary<string, string> saslProps, CallbackHandler callbackHandler
			)
		{
			return new Org.Apache.Hadoop.Hdfs.Protocol.Datatransfer.Sasl.SaslParticipant(Javax.Security.Sasl.Sasl
				.CreateSaslClient(new string[] { Mechanism }, userName, Protocol, ServerName, saslProps
				, callbackHandler));
		}

		/// <summary>Private constructor wrapping a SaslServer.</summary>
		/// <param name="saslServer">to wrap</param>
		private SaslParticipant(SaslServer saslServer)
		{
			this.saslServer = saslServer;
			this.saslClient = null;
		}

		/// <summary>Private constructor wrapping a SaslClient.</summary>
		/// <param name="saslClient">to wrap</param>
		private SaslParticipant(SaslClient saslClient)
		{
			this.saslServer = null;
			this.saslClient = saslClient;
		}

		/// <seealso>
		/// 
		/// <see cref="Javax.Security.Sasl.SaslServer.EvaluateResponse(byte[])"/>
		/// </seealso>
		/// <seealso>
		/// 
		/// <see cref="Javax.Security.Sasl.SaslClient.EvaluateChallenge(byte[])"/>
		/// </seealso>
		/// <exception cref="Javax.Security.Sasl.SaslException"/>
		public virtual byte[] EvaluateChallengeOrResponse(byte[] challengeOrResponse)
		{
			if (saslClient != null)
			{
				return saslClient.EvaluateChallenge(challengeOrResponse);
			}
			else
			{
				return saslServer.EvaluateResponse(challengeOrResponse);
			}
		}

		/// <summary>
		/// After successful SASL negotation, returns the negotiated quality of
		/// protection.
		/// </summary>
		/// <returns>negotiated quality of protection</returns>
		public virtual string GetNegotiatedQop()
		{
			if (saslClient != null)
			{
				return (string)saslClient.GetNegotiatedProperty(Javax.Security.Sasl.Sasl.Qop);
			}
			else
			{
				return (string)saslServer.GetNegotiatedProperty(Javax.Security.Sasl.Sasl.Qop);
			}
		}

		/// <summary>After successful SASL negotiation, returns whether it's QOP privacy</summary>
		/// <returns>boolean whether it's QOP privacy</returns>
		public virtual bool IsNegotiatedQopPrivacy()
		{
			string qop = GetNegotiatedQop();
			return qop != null && Sharpen.Runtime.EqualsIgnoreCase("auth-conf", qop);
		}

		/// <summary>Wraps a byte array.</summary>
		/// <param name="bytes">The array containing the bytes to wrap.</param>
		/// <param name="off">The starting position at the array</param>
		/// <param name="len">The number of bytes to wrap</param>
		/// <returns>byte[] wrapped bytes</returns>
		/// <exception cref="Javax.Security.Sasl.SaslException">if the bytes cannot be successfully wrapped
		/// 	</exception>
		public virtual byte[] Wrap(byte[] bytes, int off, int len)
		{
			if (saslClient != null)
			{
				return saslClient.Wrap(bytes, off, len);
			}
			else
			{
				return saslServer.Wrap(bytes, off, len);
			}
		}

		/// <summary>Unwraps a byte array.</summary>
		/// <param name="bytes">The array containing the bytes to unwrap.</param>
		/// <param name="off">The starting position at the array</param>
		/// <param name="len">The number of bytes to unwrap</param>
		/// <returns>byte[] unwrapped bytes</returns>
		/// <exception cref="Javax.Security.Sasl.SaslException">if the bytes cannot be successfully unwrapped
		/// 	</exception>
		public virtual byte[] Unwrap(byte[] bytes, int off, int len)
		{
			if (saslClient != null)
			{
				return saslClient.Unwrap(bytes, off, len);
			}
			else
			{
				return saslServer.Unwrap(bytes, off, len);
			}
		}

		/// <summary>Returns true if SASL negotiation is complete.</summary>
		/// <returns>true if SASL negotiation is complete</returns>
		public virtual bool IsComplete()
		{
			if (saslClient != null)
			{
				return saslClient.IsComplete();
			}
			else
			{
				return saslServer.IsComplete();
			}
		}

		/// <summary>
		/// Return some input/output streams that may henceforth have their
		/// communication encrypted, depending on the negotiated quality of protection.
		/// </summary>
		/// <param name="out">output stream to wrap</param>
		/// <param name="in">input stream to wrap</param>
		/// <returns>IOStreamPair wrapping the streams</returns>
		public virtual IOStreamPair CreateStreamPair(DataOutputStream @out, DataInputStream
			 @in)
		{
			if (saslClient != null)
			{
				return new IOStreamPair(new SaslInputStream(@in, saslClient), new SaslOutputStream
					(@out, saslClient));
			}
			else
			{
				return new IOStreamPair(new SaslInputStream(@in, saslServer), new SaslOutputStream
					(@out, saslServer));
			}
		}
	}
}
