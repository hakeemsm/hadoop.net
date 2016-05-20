using Sharpen;

namespace org.apache.hadoop.security
{
	/// <summary>A utility class for dealing with SASL on RPC server</summary>
	public class SaslRpcServer
	{
		public static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.security.SaslRpcServer
			)));

		public const string SASL_DEFAULT_REALM = "default";

		private static javax.security.sasl.SaslServerFactory saslFactory;

		[System.Serializable]
		public sealed class QualityOfProtection
		{
			public static readonly org.apache.hadoop.security.SaslRpcServer.QualityOfProtection
				 AUTHENTICATION = new org.apache.hadoop.security.SaslRpcServer.QualityOfProtection
				("auth");

			public static readonly org.apache.hadoop.security.SaslRpcServer.QualityOfProtection
				 INTEGRITY = new org.apache.hadoop.security.SaslRpcServer.QualityOfProtection("auth-int"
				);

			public static readonly org.apache.hadoop.security.SaslRpcServer.QualityOfProtection
				 PRIVACY = new org.apache.hadoop.security.SaslRpcServer.QualityOfProtection("auth-conf"
				);

			public readonly string saslQop;

			private QualityOfProtection(string saslQop)
			{
				this.saslQop = saslQop;
			}

			public string getSaslQop()
			{
				return org.apache.hadoop.security.SaslRpcServer.QualityOfProtection.saslQop;
			}
		}

		[org.apache.hadoop.classification.InterfaceAudience.Private]
		[org.apache.hadoop.classification.InterfaceStability.Unstable]
		public org.apache.hadoop.security.SaslRpcServer.AuthMethod authMethod;

		public string mechanism;

		public string protocol;

		public string serverId;

		/// <exception cref="System.IO.IOException"/>
		[org.apache.hadoop.classification.InterfaceAudience.Private]
		[org.apache.hadoop.classification.InterfaceStability.Unstable]
		public SaslRpcServer(org.apache.hadoop.security.SaslRpcServer.AuthMethod authMethod
			)
		{
			this.authMethod = authMethod;
			mechanism = authMethod.getMechanismName();
			switch (authMethod)
			{
				case org.apache.hadoop.security.SaslRpcServer.AuthMethod.SIMPLE:
				{
					return;
				}

				case org.apache.hadoop.security.SaslRpcServer.AuthMethod.TOKEN:
				{
					// no sasl for simple
					protocol = string.Empty;
					serverId = org.apache.hadoop.security.SaslRpcServer.SASL_DEFAULT_REALM;
					break;
				}

				case org.apache.hadoop.security.SaslRpcServer.AuthMethod.KERBEROS:
				{
					string fullName = org.apache.hadoop.security.UserGroupInformation.getCurrentUser(
						).getUserName();
					if (LOG.isDebugEnabled())
					{
						LOG.debug("Kerberos principal name is " + fullName);
					}
					// don't use KerberosName because we don't want auth_to_local
					string[] parts = fullName.split("[/@]", 3);
					protocol = parts[0];
					// should verify service host is present here rather than in create()
					// but lazy tests are using a UGI that isn't a SPN...
					serverId = (parts.Length < 2) ? string.Empty : parts[1];
					break;
				}

				default:
				{
					// we should never be able to get here
					throw new org.apache.hadoop.security.AccessControlException("Server does not support SASL "
						 + authMethod);
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[org.apache.hadoop.classification.InterfaceAudience.Private]
		[org.apache.hadoop.classification.InterfaceStability.Unstable]
		public virtual javax.security.sasl.SaslServer create(org.apache.hadoop.ipc.Server.Connection
			 connection, System.Collections.Generic.IDictionary<string, object> saslProperties
			, org.apache.hadoop.security.token.SecretManager<org.apache.hadoop.security.token.TokenIdentifier
			> secretManager)
		{
			org.apache.hadoop.security.UserGroupInformation ugi = null;
			javax.security.auth.callback.CallbackHandler callback;
			switch (authMethod)
			{
				case org.apache.hadoop.security.SaslRpcServer.AuthMethod.TOKEN:
				{
					callback = new org.apache.hadoop.security.SaslRpcServer.SaslDigestCallbackHandler
						(secretManager, connection);
					break;
				}

				case org.apache.hadoop.security.SaslRpcServer.AuthMethod.KERBEROS:
				{
					ugi = org.apache.hadoop.security.UserGroupInformation.getCurrentUser();
					if (serverId.isEmpty())
					{
						throw new org.apache.hadoop.security.AccessControlException("Kerberos principal name does NOT have the expected "
							 + "hostname part: " + ugi.getUserName());
					}
					callback = new org.apache.hadoop.security.SaslRpcServer.SaslGssCallbackHandler();
					break;
				}

				default:
				{
					// we should never be able to get here
					throw new org.apache.hadoop.security.AccessControlException("Server does not support SASL "
						 + authMethod);
				}
			}
			javax.security.sasl.SaslServer saslServer;
			if (ugi != null)
			{
				saslServer = ugi.doAs(new _PrivilegedExceptionAction_159(this, saslProperties, callback
					));
			}
			else
			{
				saslServer = saslFactory.createSaslServer(mechanism, protocol, serverId, saslProperties
					, callback);
			}
			if (saslServer == null)
			{
				throw new org.apache.hadoop.security.AccessControlException("Unable to find SASL server implementation for "
					 + mechanism);
			}
			if (LOG.isDebugEnabled())
			{
				LOG.debug("Created SASL server with mechanism = " + mechanism);
			}
			return saslServer;
		}

		private sealed class _PrivilegedExceptionAction_159 : java.security.PrivilegedExceptionAction
			<javax.security.sasl.SaslServer>
		{
			public _PrivilegedExceptionAction_159(SaslRpcServer _enclosing, System.Collections.Generic.IDictionary
				<string, object> saslProperties, javax.security.auth.callback.CallbackHandler callback
				)
			{
				this._enclosing = _enclosing;
				this.saslProperties = saslProperties;
				this.callback = callback;
			}

			/// <exception cref="javax.security.sasl.SaslException"/>
			public javax.security.sasl.SaslServer run()
			{
				return org.apache.hadoop.security.SaslRpcServer.saslFactory.createSaslServer(this
					._enclosing.mechanism, this._enclosing.protocol, this._enclosing.serverId, saslProperties
					, callback);
			}

			private readonly SaslRpcServer _enclosing;

			private readonly System.Collections.Generic.IDictionary<string, object> saslProperties;

			private readonly javax.security.auth.callback.CallbackHandler callback;
		}

		public static void init(org.apache.hadoop.conf.Configuration conf)
		{
			java.security.Security.addProvider(new org.apache.hadoop.security.SaslPlainServer.SecurityProvider
				());
			// passing null so factory is populated with all possibilities.  the
			// properties passed when instantiating a server are what really matter
			saslFactory = new org.apache.hadoop.security.SaslRpcServer.FastSaslServerFactory(
				null);
		}

		internal static string encodeIdentifier(byte[] identifier)
		{
			return new string(org.apache.commons.codec.binary.Base64.encodeBase64(identifier)
				, org.apache.commons.io.Charsets.UTF_8);
		}

		internal static byte[] decodeIdentifier(string identifier)
		{
			return org.apache.commons.codec.binary.Base64.decodeBase64(Sharpen.Runtime.getBytesForString
				(identifier, org.apache.commons.io.Charsets.UTF_8));
		}

		/// <exception cref="org.apache.hadoop.security.token.SecretManager.InvalidToken"/>
		public static T getIdentifier<T>(string id, org.apache.hadoop.security.token.SecretManager
			<T> secretManager)
			where T : org.apache.hadoop.security.token.TokenIdentifier
		{
			byte[] tokenId = decodeIdentifier(id);
			T tokenIdentifier = secretManager.createIdentifier();
			try
			{
				tokenIdentifier.readFields(new java.io.DataInputStream(new java.io.ByteArrayInputStream
					(tokenId)));
			}
			catch (System.IO.IOException e)
			{
				throw (org.apache.hadoop.security.token.SecretManager.InvalidToken)new org.apache.hadoop.security.token.SecretManager.InvalidToken
					("Can't de-serialize tokenIdentifier").initCause(e);
			}
			return tokenIdentifier;
		}

		internal static char[] encodePassword(byte[] password)
		{
			return new string(org.apache.commons.codec.binary.Base64.encodeBase64(password), 
				org.apache.commons.io.Charsets.UTF_8).ToCharArray();
		}

		/// <summary>Splitting fully qualified Kerberos name into parts</summary>
		public static string[] splitKerberosName(string fullName)
		{
			return fullName.split("[/@]");
		}

		/// <summary>Authentication method</summary>
		[System.Serializable]
		public sealed class AuthMethod
		{
			public static readonly org.apache.hadoop.security.SaslRpcServer.AuthMethod SIMPLE
				 = new org.apache.hadoop.security.SaslRpcServer.AuthMethod(unchecked((byte)80), 
				string.Empty);

			public static readonly org.apache.hadoop.security.SaslRpcServer.AuthMethod KERBEROS
				 = new org.apache.hadoop.security.SaslRpcServer.AuthMethod(unchecked((byte)81), 
				"GSSAPI");

			[System.Obsolete]
			public static readonly org.apache.hadoop.security.SaslRpcServer.AuthMethod DIGEST
				 = new org.apache.hadoop.security.SaslRpcServer.AuthMethod(unchecked((byte)82), 
				"DIGEST-MD5");

			public static readonly org.apache.hadoop.security.SaslRpcServer.AuthMethod TOKEN = 
				new org.apache.hadoop.security.SaslRpcServer.AuthMethod(unchecked((byte)82), "DIGEST-MD5"
				);

			public static readonly org.apache.hadoop.security.SaslRpcServer.AuthMethod PLAIN = 
				new org.apache.hadoop.security.SaslRpcServer.AuthMethod(unchecked((byte)83), "PLAIN"
				);

			/// <summary>The code for this method.</summary>
			public readonly byte code;

			public readonly string mechanismName;

			private AuthMethod(byte code, string mechanismName)
			{
				this.code = code;
				this.mechanismName = mechanismName;
			}

			private static readonly int FIRST_CODE = values()[0].code;

			/// <summary>Return the object represented by the code.</summary>
			private static org.apache.hadoop.security.SaslRpcServer.AuthMethod valueOf(byte code
				)
			{
				int i = (code & unchecked((int)(0xff))) - org.apache.hadoop.security.SaslRpcServer.AuthMethod
					.FIRST_CODE;
				return i < 0 || i >= values().Length ? null : values()[i];
			}

			/// <summary>Return the SASL mechanism name</summary>
			public string getMechanismName()
			{
				return org.apache.hadoop.security.SaslRpcServer.AuthMethod.mechanismName;
			}

			/// <summary>Read from in</summary>
			/// <exception cref="System.IO.IOException"/>
			public static org.apache.hadoop.security.SaslRpcServer.AuthMethod read(java.io.DataInput
				 @in)
			{
				return valueOf(@in.readByte());
			}

			/// <summary>Write to out</summary>
			/// <exception cref="System.IO.IOException"/>
			public void write(java.io.DataOutput @out)
			{
				@out.write(org.apache.hadoop.security.SaslRpcServer.AuthMethod.code);
			}
		}

		/// <summary>CallbackHandler for SASL DIGEST-MD5 mechanism</summary>
		public class SaslDigestCallbackHandler : javax.security.auth.callback.CallbackHandler
		{
			private org.apache.hadoop.security.token.SecretManager<org.apache.hadoop.security.token.TokenIdentifier
				> secretManager;

			private org.apache.hadoop.ipc.Server.Connection connection;

			public SaslDigestCallbackHandler(org.apache.hadoop.security.token.SecretManager<org.apache.hadoop.security.token.TokenIdentifier
				> secretManager, org.apache.hadoop.ipc.Server.Connection connection)
			{
				this.secretManager = secretManager;
				this.connection = connection;
			}

			/// <exception cref="org.apache.hadoop.security.token.SecretManager.InvalidToken"/>
			/// <exception cref="org.apache.hadoop.ipc.StandbyException"/>
			/// <exception cref="org.apache.hadoop.ipc.RetriableException"/>
			/// <exception cref="System.IO.IOException"/>
			private char[] getPassword(org.apache.hadoop.security.token.TokenIdentifier tokenid
				)
			{
				return encodePassword(secretManager.retriableRetrievePassword(tokenid));
			}

			/// <exception cref="org.apache.hadoop.security.token.SecretManager.InvalidToken"/>
			/// <exception cref="javax.security.auth.callback.UnsupportedCallbackException"/>
			/// <exception cref="org.apache.hadoop.ipc.StandbyException"/>
			/// <exception cref="org.apache.hadoop.ipc.RetriableException"/>
			/// <exception cref="System.IO.IOException"/>
			public virtual void handle(javax.security.auth.callback.Callback[] callbacks)
			{
				javax.security.auth.callback.NameCallback nc = null;
				javax.security.auth.callback.PasswordCallback pc = null;
				javax.security.sasl.AuthorizeCallback ac = null;
				foreach (javax.security.auth.callback.Callback callback in callbacks)
				{
					if (callback is javax.security.sasl.AuthorizeCallback)
					{
						ac = (javax.security.sasl.AuthorizeCallback)callback;
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
									continue;
								}
								else
								{
									// realm is ignored
									throw new javax.security.auth.callback.UnsupportedCallbackException(callback, "Unrecognized SASL DIGEST-MD5 Callback"
										);
								}
							}
						}
					}
				}
				if (pc != null)
				{
					org.apache.hadoop.security.token.TokenIdentifier tokenIdentifier = getIdentifier(
						nc.getDefaultName(), secretManager);
					char[] password = getPassword(tokenIdentifier);
					org.apache.hadoop.security.UserGroupInformation user = null;
					user = tokenIdentifier.getUser();
					// may throw exception
					connection.attemptingUser = user;
					if (LOG.isDebugEnabled())
					{
						LOG.debug("SASL server DIGEST-MD5 callback: setting password " + "for client: " +
							 tokenIdentifier.getUser());
					}
					pc.setPassword(password);
				}
				if (ac != null)
				{
					string authid = ac.getAuthenticationID();
					string authzid = ac.getAuthorizationID();
					if (authid.Equals(authzid))
					{
						ac.setAuthorized(true);
					}
					else
					{
						ac.setAuthorized(false);
					}
					if (ac.isAuthorized())
					{
						if (LOG.isDebugEnabled())
						{
							string username = getIdentifier(authzid, secretManager).getUser().getUserName();
							LOG.debug("SASL server DIGEST-MD5 callback: setting " + "canonicalized client ID: "
								 + username);
						}
						ac.setAuthorizedID(authzid);
					}
				}
			}
		}

		/// <summary>CallbackHandler for SASL GSSAPI Kerberos mechanism</summary>
		public class SaslGssCallbackHandler : javax.security.auth.callback.CallbackHandler
		{
			/// <exception cref="javax.security.auth.callback.UnsupportedCallbackException"/>
			public virtual void handle(javax.security.auth.callback.Callback[] callbacks)
			{
				javax.security.sasl.AuthorizeCallback ac = null;
				foreach (javax.security.auth.callback.Callback callback in callbacks)
				{
					if (callback is javax.security.sasl.AuthorizeCallback)
					{
						ac = (javax.security.sasl.AuthorizeCallback)callback;
					}
					else
					{
						throw new javax.security.auth.callback.UnsupportedCallbackException(callback, "Unrecognized SASL GSSAPI Callback"
							);
					}
				}
				if (ac != null)
				{
					string authid = ac.getAuthenticationID();
					string authzid = ac.getAuthorizationID();
					if (authid.Equals(authzid))
					{
						ac.setAuthorized(true);
					}
					else
					{
						ac.setAuthorized(false);
					}
					if (ac.isAuthorized())
					{
						if (LOG.isDebugEnabled())
						{
							LOG.debug("SASL server GSSAPI callback: setting " + "canonicalized client ID: " +
								 authzid);
						}
						ac.setAuthorizedID(authzid);
					}
				}
			}
		}

		private class FastSaslServerFactory : javax.security.sasl.SaslServerFactory
		{
			private readonly System.Collections.Generic.IDictionary<string, System.Collections.Generic.IList
				<javax.security.sasl.SaslServerFactory>> factoryCache = new System.Collections.Generic.Dictionary
				<string, System.Collections.Generic.IList<javax.security.sasl.SaslServerFactory>
				>();

			internal FastSaslServerFactory(System.Collections.Generic.IDictionary<string, object
				> props)
			{
				// Sasl.createSaslServer is 100-200X slower than caching the factories!
				java.util.Enumeration<javax.security.sasl.SaslServerFactory> factories = javax.security.sasl.Sasl
					.getSaslServerFactories();
				while (factories.MoveNext())
				{
					javax.security.sasl.SaslServerFactory factory = factories.Current;
					foreach (string mech in factory.getMechanismNames(props))
					{
						if (!factoryCache.Contains(mech))
						{
							factoryCache[mech] = new System.Collections.Generic.List<javax.security.sasl.SaslServerFactory
								>();
						}
						factoryCache[mech].add(factory);
					}
				}
			}

			/// <exception cref="javax.security.sasl.SaslException"/>
			public virtual javax.security.sasl.SaslServer createSaslServer(string mechanism, 
				string protocol, string serverName, System.Collections.Generic.IDictionary<string
				, object> props, javax.security.auth.callback.CallbackHandler cbh)
			{
				javax.security.sasl.SaslServer saslServer = null;
				System.Collections.Generic.IList<javax.security.sasl.SaslServerFactory> factories
					 = factoryCache[mechanism];
				if (factories != null)
				{
					foreach (javax.security.sasl.SaslServerFactory factory in factories)
					{
						saslServer = factory.createSaslServer(mechanism, protocol, serverName, props, cbh
							);
						if (saslServer != null)
						{
							break;
						}
					}
				}
				return saslServer;
			}

			public virtual string[] getMechanismNames(System.Collections.Generic.IDictionary<
				string, object> props)
			{
				return Sharpen.Collections.ToArray(factoryCache.Keys, new string[0]);
			}
		}
	}
}
