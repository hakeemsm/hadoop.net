using System;
using System.Collections.Generic;
using System.IO;
using Hadoop.Common.Core.Conf;
using Javax.Security.Auth.Callback;
using Javax.Security.Sasl;
using Org.Apache.Commons.Codec.Binary;
using Org.Apache.Commons.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Security.Token;


namespace Org.Apache.Hadoop.Security
{
	/// <summary>A utility class for dealing with SASL on RPC server</summary>
	public class SaslRpcServer
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Security.SaslRpcServer
			));

		public const string SaslDefaultRealm = "default";

		private static SaslServerFactory saslFactory;

		[System.Serializable]
		public sealed class QualityOfProtection
		{
			public static readonly SaslRpcServer.QualityOfProtection Authentication = new SaslRpcServer.QualityOfProtection
				("auth");

			public static readonly SaslRpcServer.QualityOfProtection Integrity = new SaslRpcServer.QualityOfProtection
				("auth-int");

			public static readonly SaslRpcServer.QualityOfProtection Privacy = new SaslRpcServer.QualityOfProtection
				("auth-conf");

			public readonly string saslQop;

			private QualityOfProtection(string saslQop)
			{
				this.saslQop = saslQop;
			}

			public string GetSaslQop()
			{
				return SaslRpcServer.QualityOfProtection.saslQop;
			}
		}

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public SaslRpcServer.AuthMethod authMethod;

		public string mechanism;

		public string protocol;

		public string serverId;

		/// <exception cref="System.IO.IOException"/>
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public SaslRpcServer(SaslRpcServer.AuthMethod authMethod)
		{
			this.authMethod = authMethod;
			mechanism = authMethod.GetMechanismName();
			switch (authMethod)
			{
				case SaslRpcServer.AuthMethod.Simple:
				{
					return;
				}

				case SaslRpcServer.AuthMethod.Token:
				{
					// no sasl for simple
					protocol = string.Empty;
					serverId = SaslRpcServer.SaslDefaultRealm;
					break;
				}

				case SaslRpcServer.AuthMethod.Kerberos:
				{
					string fullName = UserGroupInformation.GetCurrentUser().GetUserName();
					if (Log.IsDebugEnabled())
					{
						Log.Debug("Kerberos principal name is " + fullName);
					}
					// don't use KerberosName because we don't want auth_to_local
					string[] parts = fullName.Split("[/@]", 3);
					protocol = parts[0];
					// should verify service host is present here rather than in create()
					// but lazy tests are using a UGI that isn't a SPN...
					serverId = (parts.Length < 2) ? string.Empty : parts[1];
					break;
				}

				default:
				{
					// we should never be able to get here
					throw new AccessControlException("Server does not support SASL " + authMethod);
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public virtual SaslServer Create(Server.Connection connection, IDictionary<string
			, object> saslProperties, SecretManager<TokenIdentifier> secretManager)
		{
			UserGroupInformation ugi = null;
			CallbackHandler callback;
			switch (authMethod)
			{
				case SaslRpcServer.AuthMethod.Token:
				{
					callback = new SaslRpcServer.SaslDigestCallbackHandler(secretManager, connection);
					break;
				}

				case SaslRpcServer.AuthMethod.Kerberos:
				{
					ugi = UserGroupInformation.GetCurrentUser();
					if (serverId.IsEmpty())
					{
						throw new AccessControlException("Kerberos principal name does NOT have the expected "
							 + "hostname part: " + ugi.GetUserName());
					}
					callback = new SaslRpcServer.SaslGssCallbackHandler();
					break;
				}

				default:
				{
					// we should never be able to get here
					throw new AccessControlException("Server does not support SASL " + authMethod);
				}
			}
			SaslServer saslServer;
			if (ugi != null)
			{
				saslServer = ugi.DoAs(new _PrivilegedExceptionAction_159(this, saslProperties, callback
					));
			}
			else
			{
				saslServer = saslFactory.CreateSaslServer(mechanism, protocol, serverId, saslProperties
					, callback);
			}
			if (saslServer == null)
			{
				throw new AccessControlException("Unable to find SASL server implementation for "
					 + mechanism);
			}
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Created SASL server with mechanism = " + mechanism);
			}
			return saslServer;
		}

		private sealed class _PrivilegedExceptionAction_159 : PrivilegedExceptionAction<SaslServer
			>
		{
			public _PrivilegedExceptionAction_159(SaslRpcServer _enclosing, IDictionary<string
				, object> saslProperties, CallbackHandler callback)
			{
				this._enclosing = _enclosing;
				this.saslProperties = saslProperties;
				this.callback = callback;
			}

			/// <exception cref="Javax.Security.Sasl.SaslException"/>
			public SaslServer Run()
			{
				return SaslRpcServer.saslFactory.CreateSaslServer(this._enclosing.mechanism, this
					._enclosing.protocol, this._enclosing.serverId, saslProperties, callback);
			}

			private readonly SaslRpcServer _enclosing;

			private readonly IDictionary<string, object> saslProperties;

			private readonly CallbackHandler callback;
		}

		public static void Init(Configuration conf)
		{
			Security.AddProvider(new SaslPlainServer.SecurityProvider());
			// passing null so factory is populated with all possibilities.  the
			// properties passed when instantiating a server are what really matter
			saslFactory = new SaslRpcServer.FastSaslServerFactory(null);
		}

		internal static string EncodeIdentifier(byte[] identifier)
		{
			return new string(Base64.EncodeBase64(identifier), Charsets.Utf8);
		}

		internal static byte[] DecodeIdentifier(string identifier)
		{
			return Base64.DecodeBase64(Runtime.GetBytesForString(identifier, Charsets
				.Utf8));
		}

		/// <exception cref="Org.Apache.Hadoop.Security.Token.SecretManager.InvalidToken"/>
		public static T GetIdentifier<T>(string id, SecretManager<T> secretManager)
			where T : TokenIdentifier
		{
			byte[] tokenId = DecodeIdentifier(id);
			T tokenIdentifier = secretManager.CreateIdentifier();
			try
			{
				tokenIdentifier.ReadFields(new DataInputStream(new ByteArrayInputStream(tokenId))
					);
			}
			catch (IOException e)
			{
				throw (SecretManager.InvalidToken)Extensions.InitCause(new SecretManager.InvalidToken
					("Can't de-serialize tokenIdentifier"), e);
			}
			return tokenIdentifier;
		}

		internal static char[] EncodePassword(byte[] password)
		{
			return new string(Base64.EncodeBase64(password), Charsets.Utf8).ToCharArray();
		}

		/// <summary>Splitting fully qualified Kerberos name into parts</summary>
		public static string[] SplitKerberosName(string fullName)
		{
			return fullName.Split("[/@]");
		}

		/// <summary>Authentication method</summary>
		[System.Serializable]
		public sealed class AuthMethod
		{
			public static readonly SaslRpcServer.AuthMethod Simple = new SaslRpcServer.AuthMethod
				(unchecked((byte)80), string.Empty);

			public static readonly SaslRpcServer.AuthMethod Kerberos = new SaslRpcServer.AuthMethod
				(unchecked((byte)81), "GSSAPI");

			[Obsolete]
			public static readonly SaslRpcServer.AuthMethod Digest = new SaslRpcServer.AuthMethod
				(unchecked((byte)82), "DIGEST-MD5");

			public static readonly SaslRpcServer.AuthMethod Token = new SaslRpcServer.AuthMethod
				(unchecked((byte)82), "DIGEST-MD5");

			public static readonly SaslRpcServer.AuthMethod Plain = new SaslRpcServer.AuthMethod
				(unchecked((byte)83), "PLAIN");

			/// <summary>The code for this method.</summary>
			public readonly byte code;

			public readonly string mechanismName;

			private AuthMethod(byte code, string mechanismName)
			{
				this.code = code;
				this.mechanismName = mechanismName;
			}

			private static readonly int FirstCode = Values()[0].code;

			/// <summary>Return the object represented by the code.</summary>
			private static SaslRpcServer.AuthMethod ValueOf(byte code)
			{
				int i = (code & unchecked((int)(0xff))) - SaslRpcServer.AuthMethod.FirstCode;
				return i < 0 || i >= Values().Length ? null : Values()[i];
			}

			/// <summary>Return the SASL mechanism name</summary>
			public string GetMechanismName()
			{
				return SaslRpcServer.AuthMethod.mechanismName;
			}

			/// <summary>Read from in</summary>
			/// <exception cref="System.IO.IOException"/>
			public static SaslRpcServer.AuthMethod Read(BinaryReader reader)
			{
				return ValueOf(@in.ReadByte());
			}

			/// <summary>Write to out</summary>
			/// <exception cref="System.IO.IOException"/>
			public void Write(BinaryWriter writer)
			{
				@out.Write(SaslRpcServer.AuthMethod.code);
			}
		}

		/// <summary>CallbackHandler for SASL DIGEST-MD5 mechanism</summary>
		public class SaslDigestCallbackHandler : CallbackHandler
		{
			private SecretManager<TokenIdentifier> secretManager;

			private Server.Connection connection;

			public SaslDigestCallbackHandler(SecretManager<TokenIdentifier> secretManager, Server.Connection
				 connection)
			{
				this.secretManager = secretManager;
				this.connection = connection;
			}

			/// <exception cref="Org.Apache.Hadoop.Security.Token.SecretManager.InvalidToken"/>
			/// <exception cref="Org.Apache.Hadoop.Ipc.StandbyException"/>
			/// <exception cref="Org.Apache.Hadoop.Ipc.RetriableException"/>
			/// <exception cref="System.IO.IOException"/>
			private char[] GetPassword(TokenIdentifier tokenid)
			{
				return EncodePassword(secretManager.RetriableRetrievePassword(tokenid));
			}

			/// <exception cref="Org.Apache.Hadoop.Security.Token.SecretManager.InvalidToken"/>
			/// <exception cref="Javax.Security.Auth.Callback.UnsupportedCallbackException"/>
			/// <exception cref="Org.Apache.Hadoop.Ipc.StandbyException"/>
			/// <exception cref="Org.Apache.Hadoop.Ipc.RetriableException"/>
			/// <exception cref="System.IO.IOException"/>
			public virtual void Handle(Javax.Security.Auth.Callback.Callback[] callbacks)
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
									continue;
								}
								else
								{
									// realm is ignored
									throw new UnsupportedCallbackException(callback, "Unrecognized SASL DIGEST-MD5 Callback"
										);
								}
							}
						}
					}
				}
				if (pc != null)
				{
					TokenIdentifier tokenIdentifier = GetIdentifier(nc.GetDefaultName(), secretManager
						);
					char[] password = GetPassword(tokenIdentifier);
					UserGroupInformation user = null;
					user = tokenIdentifier.GetUser();
					// may throw exception
					connection.attemptingUser = user;
					if (Log.IsDebugEnabled())
					{
						Log.Debug("SASL server DIGEST-MD5 callback: setting password " + "for client: " +
							 tokenIdentifier.GetUser());
					}
					pc.SetPassword(password);
				}
				if (ac != null)
				{
					string authid = ac.GetAuthenticationID();
					string authzid = ac.GetAuthorizationID();
					if (authid.Equals(authzid))
					{
						ac.SetAuthorized(true);
					}
					else
					{
						ac.SetAuthorized(false);
					}
					if (ac.IsAuthorized())
					{
						if (Log.IsDebugEnabled())
						{
							string username = GetIdentifier(authzid, secretManager).GetUser().GetUserName();
							Log.Debug("SASL server DIGEST-MD5 callback: setting " + "canonicalized client ID: "
								 + username);
						}
						ac.SetAuthorizedID(authzid);
					}
				}
			}
		}

		/// <summary>CallbackHandler for SASL GSSAPI Kerberos mechanism</summary>
		public class SaslGssCallbackHandler : CallbackHandler
		{
			/// <exception cref="Javax.Security.Auth.Callback.UnsupportedCallbackException"/>
			public virtual void Handle(Javax.Security.Auth.Callback.Callback[] callbacks)
			{
				AuthorizeCallback ac = null;
				foreach (Javax.Security.Auth.Callback.Callback callback in callbacks)
				{
					if (callback is AuthorizeCallback)
					{
						ac = (AuthorizeCallback)callback;
					}
					else
					{
						throw new UnsupportedCallbackException(callback, "Unrecognized SASL GSSAPI Callback"
							);
					}
				}
				if (ac != null)
				{
					string authid = ac.GetAuthenticationID();
					string authzid = ac.GetAuthorizationID();
					if (authid.Equals(authzid))
					{
						ac.SetAuthorized(true);
					}
					else
					{
						ac.SetAuthorized(false);
					}
					if (ac.IsAuthorized())
					{
						if (Log.IsDebugEnabled())
						{
							Log.Debug("SASL server GSSAPI callback: setting " + "canonicalized client ID: " +
								 authzid);
						}
						ac.SetAuthorizedID(authzid);
					}
				}
			}
		}

		private class FastSaslServerFactory : SaslServerFactory
		{
			private readonly IDictionary<string, IList<SaslServerFactory>> factoryCache = new 
				Dictionary<string, IList<SaslServerFactory>>();

			internal FastSaslServerFactory(IDictionary<string, object> props)
			{
				// Sasl.createSaslServer is 100-200X slower than caching the factories!
				Enumeration<SaslServerFactory> factories = Javax.Security.Sasl.Sasl.GetSaslServerFactories
					();
				while (factories.MoveNext())
				{
					SaslServerFactory factory = factories.Current;
					foreach (string mech in factory.GetMechanismNames(props))
					{
						if (!factoryCache.Contains(mech))
						{
							factoryCache[mech] = new AList<SaslServerFactory>();
						}
						factoryCache[mech].AddItem(factory);
					}
				}
			}

			/// <exception cref="Javax.Security.Sasl.SaslException"/>
			public virtual SaslServer CreateSaslServer(string mechanism, string protocol, string
				 serverName, IDictionary<string, object> props, CallbackHandler cbh)
			{
				SaslServer saslServer = null;
				IList<SaslServerFactory> factories = factoryCache[mechanism];
				if (factories != null)
				{
					foreach (SaslServerFactory factory in factories)
					{
						saslServer = factory.CreateSaslServer(mechanism, protocol, serverName, props, cbh
							);
						if (saslServer != null)
						{
							break;
						}
					}
				}
				return saslServer;
			}

			public virtual string[] GetMechanismNames(IDictionary<string, object> props)
			{
				return Collections.ToArray(factoryCache.Keys, new string[0]);
			}
		}
	}
}
