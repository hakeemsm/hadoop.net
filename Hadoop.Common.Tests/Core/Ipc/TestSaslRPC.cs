using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Text;
using Javax.Security.Auth.Callback;
using Javax.Security.Sasl;
using NUnit.Framework;
using NUnit.Framework.Runners;
using Org.Apache.Commons.Lang;
using Org.Apache.Commons.Logging;
using Org.Apache.Commons.Logging.Impl;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Util;
using Org.Apache.Log4j;
using Sharpen;

namespace Org.Apache.Hadoop.Ipc
{
	/// <summary>Unit tests for using Sasl over RPC.</summary>
	public class TestSaslRPC
	{
		[Parameterized.Parameters]
		public static ICollection<object[]> Data()
		{
			ICollection<object[]> @params = new AList<object[]>();
			foreach (SaslRpcServer.QualityOfProtection qop in SaslRpcServer.QualityOfProtection
				.Values())
			{
				@params.AddItem(new object[] { new SaslRpcServer.QualityOfProtection[] { qop }, qop
					, null });
			}
			@params.AddItem(new object[] { new SaslRpcServer.QualityOfProtection[] { SaslRpcServer.QualityOfProtection
				.Privacy, SaslRpcServer.QualityOfProtection.Authentication }, SaslRpcServer.QualityOfProtection
				.Privacy, null });
			@params.AddItem(new object[] { new SaslRpcServer.QualityOfProtection[] { SaslRpcServer.QualityOfProtection
				.Privacy, SaslRpcServer.QualityOfProtection.Authentication }, SaslRpcServer.QualityOfProtection
				.Authentication, "org.apache.hadoop.ipc.TestSaslRPC$AuthSaslPropertiesResolver" }
				);
			return @params;
		}

		internal SaslRpcServer.QualityOfProtection[] qop;

		internal SaslRpcServer.QualityOfProtection expectedQop;

		internal string saslPropertiesResolver;

		public TestSaslRPC(SaslRpcServer.QualityOfProtection[] qop, SaslRpcServer.QualityOfProtection
			 expectedQop, string saslPropertiesResolver)
		{
			this.qop = qop;
			this.expectedQop = expectedQop;
			this.saslPropertiesResolver = saslPropertiesResolver;
		}

		private const string Address = "0.0.0.0";

		public static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Ipc.TestSaslRPC
			));

		internal const string ErrorMessage = "Token is invalid";

		internal const string ServerPrincipalKey = "test.ipc.server.principal";

		internal const string ServerKeytabKey = "test.ipc.server.keytab";

		internal const string ServerPrincipal1 = "p1/foo@BAR";

		internal const string ServerPrincipal2 = "p2/foo@BAR";

		private static Configuration conf;

		internal static bool enableSecretManager = null;

		internal static bool forceSecretManager = null;

		internal static bool clientFallBackToSimpleAllowed = true;

		internal enum UseToken
		{
			None,
			Valid,
			Invalid,
			Other
		}

		// If this is set to true AND the auth-method is not simple, secretManager
		// will be enabled.
		// If this is set to true, secretManager will be forecefully enabled
		// irrespective of auth-method.
		[BeforeClass]
		public static void SetupKerb()
		{
			Runtime.SetProperty("java.security.krb5.kdc", string.Empty);
			Runtime.SetProperty("java.security.krb5.realm", "NONE");
			Sharpen.Security.AddProvider(new SaslPlainServer.SecurityProvider());
		}

		[SetUp]
		public virtual void Setup()
		{
			Log.Info("---------------------------------");
			Log.Info("Testing QOP:" + GetQOPNames(qop));
			Log.Info("---------------------------------");
			conf = new Configuration();
			// the specific tests for kerberos will enable kerberos.  forcing it
			// for all tests will cause tests to fail if the user has a TGT
			conf.Set(CommonConfigurationKeysPublic.HadoopSecurityAuthentication, SaslRpcServer.AuthMethod
				.Simple.ToString());
			conf.Set(CommonConfigurationKeysPublic.HadoopRpcProtection, GetQOPNames(qop));
			if (saslPropertiesResolver != null)
			{
				conf.Set(CommonConfigurationKeys.HadoopSecuritySaslPropsResolverClass, saslPropertiesResolver
					);
			}
			UserGroupInformation.SetConfiguration(conf);
			enableSecretManager = null;
			forceSecretManager = null;
			clientFallBackToSimpleAllowed = true;
		}

		internal static string GetQOPNames(SaslRpcServer.QualityOfProtection[] qops)
		{
			StringBuilder sb = new StringBuilder();
			int i = 0;
			foreach (SaslRpcServer.QualityOfProtection qop in qops)
			{
				sb.Append(StringUtils.ToLowerCase(qop.ToString()));
				if (++i < qops.Length)
				{
					sb.Append(",");
				}
			}
			return sb.ToString();
		}

		static TestSaslRPC()
		{
			((Log4JLogger)Client.Log).GetLogger().SetLevel(Level.All);
			((Log4JLogger)Server.Log).GetLogger().SetLevel(Level.All);
			((Log4JLogger)SaslRpcClient.Log).GetLogger().SetLevel(Level.All);
			((Log4JLogger)SaslRpcServer.Log).GetLogger().SetLevel(Level.All);
			((Log4JLogger)SaslInputStream.Log).GetLogger().SetLevel(Level.All);
			((Log4JLogger)SecurityUtil.Log).GetLogger().SetLevel(Level.All);
		}

		public class TestTokenIdentifier : TokenIdentifier
		{
			private Org.Apache.Hadoop.IO.Text tokenid;

			private Org.Apache.Hadoop.IO.Text realUser;

			internal static readonly Org.Apache.Hadoop.IO.Text KindName = new Org.Apache.Hadoop.IO.Text
				("test.token");

			public TestTokenIdentifier()
				: this(new Org.Apache.Hadoop.IO.Text(), new Org.Apache.Hadoop.IO.Text())
			{
			}

			public TestTokenIdentifier(Org.Apache.Hadoop.IO.Text tokenid)
				: this(tokenid, new Org.Apache.Hadoop.IO.Text())
			{
			}

			public TestTokenIdentifier(Org.Apache.Hadoop.IO.Text tokenid, Org.Apache.Hadoop.IO.Text
				 realUser)
			{
				this.tokenid = tokenid == null ? new Org.Apache.Hadoop.IO.Text() : tokenid;
				this.realUser = realUser == null ? new Org.Apache.Hadoop.IO.Text() : realUser;
			}

			public override Org.Apache.Hadoop.IO.Text GetKind()
			{
				return KindName;
			}

			public override UserGroupInformation GetUser()
			{
				if (realUser.ToString().IsEmpty())
				{
					return UserGroupInformation.CreateRemoteUser(tokenid.ToString());
				}
				else
				{
					UserGroupInformation realUgi = UserGroupInformation.CreateRemoteUser(realUser.ToString
						());
					return UserGroupInformation.CreateProxyUser(tokenid.ToString(), realUgi);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public override void ReadFields(BinaryReader @in)
			{
				tokenid.ReadFields(@in);
				realUser.ReadFields(@in);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Write(DataOutput @out)
			{
				tokenid.Write(@out);
				realUser.Write(@out);
			}
		}

		public class TestTokenSecretManager : SecretManager<TestSaslRPC.TestTokenIdentifier
			>
		{
			protected internal override byte[] CreatePassword(TestSaslRPC.TestTokenIdentifier
				 id)
			{
				return id.GetBytes();
			}

			/// <exception cref="Org.Apache.Hadoop.Security.Token.SecretManager.InvalidToken"/>
			public override byte[] RetrievePassword(TestSaslRPC.TestTokenIdentifier id)
			{
				return id.GetBytes();
			}

			public override TestSaslRPC.TestTokenIdentifier CreateIdentifier()
			{
				return new TestSaslRPC.TestTokenIdentifier();
			}
		}

		public class BadTokenSecretManager : TestSaslRPC.TestTokenSecretManager
		{
			/// <exception cref="Org.Apache.Hadoop.Security.Token.SecretManager.InvalidToken"/>
			public override byte[] RetrievePassword(TestSaslRPC.TestTokenIdentifier id)
			{
				throw new SecretManager.InvalidToken(ErrorMessage);
			}
		}

		public class TestTokenSelector : TokenSelector<TestSaslRPC.TestTokenIdentifier>
		{
			public virtual Org.Apache.Hadoop.Security.Token.Token<TestSaslRPC.TestTokenIdentifier
				> SelectToken(Org.Apache.Hadoop.IO.Text service, ICollection<Org.Apache.Hadoop.Security.Token.Token
				<TokenIdentifier>> tokens)
			{
				if (service == null)
				{
					return null;
				}
				foreach (Org.Apache.Hadoop.Security.Token.Token<TokenIdentifier> token in tokens)
				{
					if (TestSaslRPC.TestTokenIdentifier.KindName.Equals(token.GetKind()) && service.Equals
						(token.GetService()))
					{
						return (Org.Apache.Hadoop.Security.Token.Token<TestSaslRPC.TestTokenIdentifier>)token;
					}
				}
				return null;
			}
		}

		public interface TestSaslProtocol : TestRPC.TestProtocol
		{
			/// <exception cref="System.IO.IOException"/>
			SaslRpcServer.AuthMethod GetAuthMethod();

			/// <exception cref="System.IO.IOException"/>
			string GetAuthUser();
		}

		public class TestSaslImpl : TestRPC.TestImpl, TestSaslRPC.TestSaslProtocol
		{
			/// <exception cref="System.IO.IOException"/>
			public virtual SaslRpcServer.AuthMethod GetAuthMethod()
			{
				return UserGroupInformation.GetCurrentUser().GetAuthenticationMethod().GetAuthMethod
					();
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual string GetAuthUser()
			{
				return UserGroupInformation.GetCurrentUser().GetUserName();
			}
		}

		public class CustomSecurityInfo : SecurityInfo
		{
			public override KerberosInfo GetKerberosInfo(Type protocol, Configuration conf)
			{
				return new _KerberosInfo_316();
			}

			private sealed class _KerberosInfo_316 : KerberosInfo
			{
				public _KerberosInfo_316()
				{
				}

				public Type AnnotationType()
				{
					return null;
				}

				public string ServerPrincipal()
				{
					return TestSaslRPC.ServerPrincipalKey;
				}

				public string ClientPrincipal()
				{
					return null;
				}
			}

			public override TokenInfo GetTokenInfo(Type protocol, Configuration conf)
			{
				return new _TokenInfo_334();
			}

			private sealed class _TokenInfo_334 : TokenInfo
			{
				public _TokenInfo_334()
				{
				}

				public Type Value()
				{
					return typeof(TestSaslRPC.TestTokenSelector);
				}

				public Type AnnotationType()
				{
					return null;
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDigestRpc()
		{
			TestSaslRPC.TestTokenSecretManager sm = new TestSaslRPC.TestTokenSecretManager();
			Server server = new RPC.Builder(conf).SetProtocol(typeof(TestSaslRPC.TestSaslProtocol
				)).SetInstance(new TestSaslRPC.TestSaslImpl()).SetBindAddress(Address).SetPort(0
				).SetNumHandlers(5).SetVerbose(true).SetSecretManager(sm).Build();
			DoDigestRpc(server, sm);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDigestRpcWithoutAnnotation()
		{
			TestSaslRPC.TestTokenSecretManager sm = new TestSaslRPC.TestTokenSecretManager();
			try
			{
				SecurityUtil.SetSecurityInfoProviders(new TestSaslRPC.CustomSecurityInfo());
				Server server = new RPC.Builder(conf).SetProtocol(typeof(TestSaslRPC.TestSaslProtocol
					)).SetInstance(new TestSaslRPC.TestSaslImpl()).SetBindAddress(Address).SetPort(0
					).SetNumHandlers(5).SetVerbose(true).SetSecretManager(sm).Build();
				DoDigestRpc(server, sm);
			}
			finally
			{
				SecurityUtil.SetSecurityInfoProviders(new SecurityInfo[0]);
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestErrorMessage()
		{
			TestSaslRPC.BadTokenSecretManager sm = new TestSaslRPC.BadTokenSecretManager();
			Server server = new RPC.Builder(conf).SetProtocol(typeof(TestSaslRPC.TestSaslProtocol
				)).SetInstance(new TestSaslRPC.TestSaslImpl()).SetBindAddress(Address).SetPort(0
				).SetNumHandlers(5).SetVerbose(true).SetSecretManager(sm).Build();
			bool succeeded = false;
			try
			{
				DoDigestRpc(server, sm);
			}
			catch (RemoteException e)
			{
				Log.Info("LOGGING MESSAGE: " + e.GetLocalizedMessage());
				NUnit.Framework.Assert.AreEqual(ErrorMessage, e.GetLocalizedMessage());
				NUnit.Framework.Assert.IsTrue(e.UnwrapRemoteException() is SecretManager.InvalidToken
					);
				succeeded = true;
			}
			NUnit.Framework.Assert.IsTrue(succeeded);
		}

		/// <exception cref="System.Exception"/>
		private void DoDigestRpc(Server server, TestSaslRPC.TestTokenSecretManager sm)
		{
			server.Start();
			UserGroupInformation current = UserGroupInformation.GetCurrentUser();
			IPEndPoint addr = NetUtils.GetConnectAddress(server);
			TestSaslRPC.TestTokenIdentifier tokenId = new TestSaslRPC.TestTokenIdentifier(new 
				Org.Apache.Hadoop.IO.Text(current.GetUserName()));
			Org.Apache.Hadoop.Security.Token.Token<TestSaslRPC.TestTokenIdentifier> token = new 
				Org.Apache.Hadoop.Security.Token.Token<TestSaslRPC.TestTokenIdentifier>(tokenId, 
				sm);
			SecurityUtil.SetTokenService(token, addr);
			current.AddToken(token);
			TestSaslRPC.TestSaslProtocol proxy = null;
			try
			{
				proxy = RPC.GetProxy<TestSaslRPC.TestSaslProtocol>(TestSaslRPC.TestSaslProtocol.versionID
					, addr, conf);
				SaslRpcServer.AuthMethod authMethod = proxy.GetAuthMethod();
				NUnit.Framework.Assert.AreEqual(SaslRpcServer.AuthMethod.Token, authMethod);
				//QOP must be auth
				NUnit.Framework.Assert.AreEqual(expectedQop.saslQop, RPC.GetConnectionIdForProxy(
					proxy).GetSaslQop());
				proxy.Ping();
			}
			finally
			{
				server.Stop();
				if (proxy != null)
				{
					RPC.StopProxy(proxy);
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestPingInterval()
		{
			Configuration newConf = new Configuration(conf);
			newConf.Set(ServerPrincipalKey, ServerPrincipal1);
			conf.SetInt(CommonConfigurationKeys.IpcPingIntervalKey, CommonConfigurationKeys.IpcPingIntervalDefault
				);
			// set doPing to true
			newConf.SetBoolean(CommonConfigurationKeys.IpcClientPingKey, true);
			Client.ConnectionId remoteId = Client.ConnectionId.GetConnectionId(new IPEndPoint
				(0), typeof(TestSaslRPC.TestSaslProtocol), null, 0, newConf);
			NUnit.Framework.Assert.AreEqual(CommonConfigurationKeys.IpcPingIntervalDefault, remoteId
				.GetPingInterval());
			// set doPing to false
			newConf.SetBoolean(CommonConfigurationKeys.IpcClientPingKey, false);
			remoteId = Client.ConnectionId.GetConnectionId(new IPEndPoint(0), typeof(TestSaslRPC.TestSaslProtocol
				), null, 0, newConf);
			NUnit.Framework.Assert.AreEqual(0, remoteId.GetPingInterval());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestPerConnectionConf()
		{
			TestSaslRPC.TestTokenSecretManager sm = new TestSaslRPC.TestTokenSecretManager();
			Server server = new RPC.Builder(conf).SetProtocol(typeof(TestSaslRPC.TestSaslProtocol
				)).SetInstance(new TestSaslRPC.TestSaslImpl()).SetBindAddress(Address).SetPort(0
				).SetNumHandlers(5).SetVerbose(true).SetSecretManager(sm).Build();
			server.Start();
			UserGroupInformation current = UserGroupInformation.GetCurrentUser();
			IPEndPoint addr = NetUtils.GetConnectAddress(server);
			TestSaslRPC.TestTokenIdentifier tokenId = new TestSaslRPC.TestTokenIdentifier(new 
				Org.Apache.Hadoop.IO.Text(current.GetUserName()));
			Org.Apache.Hadoop.Security.Token.Token<TestSaslRPC.TestTokenIdentifier> token = new 
				Org.Apache.Hadoop.Security.Token.Token<TestSaslRPC.TestTokenIdentifier>(tokenId, 
				sm);
			SecurityUtil.SetTokenService(token, addr);
			current.AddToken(token);
			Configuration newConf = new Configuration(conf);
			newConf.Set(CommonConfigurationKeysPublic.HadoopRpcSocketFactoryClassDefaultKey, 
				string.Empty);
			Client client = null;
			TestSaslRPC.TestSaslProtocol proxy1 = null;
			TestSaslRPC.TestSaslProtocol proxy2 = null;
			TestSaslRPC.TestSaslProtocol proxy3 = null;
			int[] timeouts = new int[] { 111222, 3333333 };
			try
			{
				newConf.SetInt(CommonConfigurationKeysPublic.IpcClientConnectionMaxidletimeKey, timeouts
					[0]);
				proxy1 = RPC.GetProxy<TestSaslRPC.TestSaslProtocol>(TestSaslRPC.TestSaslProtocol.
					versionID, addr, newConf);
				proxy1.GetAuthMethod();
				client = WritableRpcEngine.GetClient(newConf);
				ICollection<Client.ConnectionId> conns = client.GetConnectionIds();
				NUnit.Framework.Assert.AreEqual("number of connections in cache is wrong", 1, conns
					.Count);
				// same conf, connection should be re-used
				proxy2 = RPC.GetProxy<TestSaslRPC.TestSaslProtocol>(TestSaslRPC.TestSaslProtocol.
					versionID, addr, newConf);
				proxy2.GetAuthMethod();
				NUnit.Framework.Assert.AreEqual("number of connections in cache is wrong", 1, conns
					.Count);
				// different conf, new connection should be set up
				newConf.SetInt(CommonConfigurationKeysPublic.IpcClientConnectionMaxidletimeKey, timeouts
					[1]);
				proxy3 = RPC.GetProxy<TestSaslRPC.TestSaslProtocol>(TestSaslRPC.TestSaslProtocol.
					versionID, addr, newConf);
				proxy3.GetAuthMethod();
				NUnit.Framework.Assert.AreEqual("number of connections in cache is wrong", 2, conns
					.Count);
				// now verify the proxies have the correct connection ids and timeouts
				Client.ConnectionId[] connsArray = new Client.ConnectionId[] { RPC.GetConnectionIdForProxy
					(proxy1), RPC.GetConnectionIdForProxy(proxy2), RPC.GetConnectionIdForProxy(proxy3
					) };
				NUnit.Framework.Assert.AreEqual(connsArray[0], connsArray[1]);
				NUnit.Framework.Assert.AreEqual(connsArray[0].GetMaxIdleTime(), timeouts[0]);
				NUnit.Framework.Assert.IsFalse(connsArray[0].Equals(connsArray[2]));
				NUnit.Framework.Assert.AreNotSame(connsArray[2].GetMaxIdleTime(), timeouts[1]);
			}
			finally
			{
				server.Stop();
				// this is dirty, but clear out connection cache for next run
				if (client != null)
				{
					client.GetConnectionIds().Clear();
				}
				if (proxy1 != null)
				{
					RPC.StopProxy(proxy1);
				}
				if (proxy2 != null)
				{
					RPC.StopProxy(proxy2);
				}
				if (proxy3 != null)
				{
					RPC.StopProxy(proxy3);
				}
			}
		}

		/// <exception cref="System.Exception"/>
		internal static void TestKerberosRpc(string principal, string keytab)
		{
			Configuration newConf = new Configuration(conf);
			newConf.Set(ServerPrincipalKey, principal);
			newConf.Set(ServerKeytabKey, keytab);
			SecurityUtil.Login(newConf, ServerKeytabKey, ServerPrincipalKey);
			TestUserGroupInformation.VerifyLoginMetrics(1, 0);
			UserGroupInformation current = UserGroupInformation.GetCurrentUser();
			System.Console.Out.WriteLine("UGI: " + current);
			Server server = new RPC.Builder(newConf).SetProtocol(typeof(TestSaslRPC.TestSaslProtocol
				)).SetInstance(new TestSaslRPC.TestSaslImpl()).SetBindAddress(Address).SetPort(0
				).SetNumHandlers(5).SetVerbose(true).Build();
			TestSaslRPC.TestSaslProtocol proxy = null;
			server.Start();
			IPEndPoint addr = NetUtils.GetConnectAddress(server);
			try
			{
				proxy = RPC.GetProxy<TestSaslRPC.TestSaslProtocol>(TestSaslRPC.TestSaslProtocol.versionID
					, addr, newConf);
				proxy.Ping();
			}
			finally
			{
				server.Stop();
				if (proxy != null)
				{
					RPC.StopProxy(proxy);
				}
			}
			System.Console.Out.WriteLine("Test is successful.");
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestSaslPlainServer()
		{
			RunNegotiation(new TestSaslRPC.TestPlainCallbacks.Client("user", "pass"), new TestSaslRPC.TestPlainCallbacks.Server
				("user", "pass"));
		}

		[NUnit.Framework.Test]
		public virtual void TestSaslPlainServerBadPassword()
		{
			SaslException e = null;
			try
			{
				RunNegotiation(new TestSaslRPC.TestPlainCallbacks.Client("user", "pass1"), new TestSaslRPC.TestPlainCallbacks.Server
					("user", "pass2"));
			}
			catch (SaslException se)
			{
				e = se;
			}
			NUnit.Framework.Assert.IsNotNull(e);
			NUnit.Framework.Assert.AreEqual("PLAIN auth failed: wrong password", e.Message);
		}

		/// <exception cref="Javax.Security.Sasl.SaslException"/>
		private void RunNegotiation(CallbackHandler clientCbh, CallbackHandler serverCbh)
		{
			string mechanism = SaslRpcServer.AuthMethod.Plain.GetMechanismName();
			SaslClient saslClient = Javax.Security.Sasl.Sasl.CreateSaslClient(new string[] { 
				mechanism }, null, null, null, null, clientCbh);
			NUnit.Framework.Assert.IsNotNull(saslClient);
			SaslServer saslServer = Javax.Security.Sasl.Sasl.CreateSaslServer(mechanism, null
				, "localhost", null, serverCbh);
			NUnit.Framework.Assert.IsNotNull("failed to find PLAIN server", saslServer);
			byte[] response = saslClient.EvaluateChallenge(new byte[0]);
			NUnit.Framework.Assert.IsNotNull(response);
			NUnit.Framework.Assert.IsTrue(saslClient.IsComplete());
			response = saslServer.EvaluateResponse(response);
			NUnit.Framework.Assert.IsNull(response);
			NUnit.Framework.Assert.IsTrue(saslServer.IsComplete());
			NUnit.Framework.Assert.IsNotNull(saslServer.GetAuthorizationID());
		}

		internal class TestPlainCallbacks
		{
			public class Client : CallbackHandler
			{
				internal string user = null;

				internal string password = null;

				internal Client(string user, string password)
				{
					this.user = user;
					this.password = password;
				}

				/// <exception cref="Javax.Security.Auth.Callback.UnsupportedCallbackException"/>
				public virtual void Handle(Javax.Security.Auth.Callback.Callback[] callbacks)
				{
					foreach (Javax.Security.Auth.Callback.Callback callback in callbacks)
					{
						if (callback is NameCallback)
						{
							((NameCallback)callback).SetName(user);
						}
						else
						{
							if (callback is PasswordCallback)
							{
								((PasswordCallback)callback).SetPassword(password.ToCharArray());
							}
							else
							{
								throw new UnsupportedCallbackException(callback, "Unrecognized SASL PLAIN Callback"
									);
							}
						}
					}
				}
			}

			public class Server : CallbackHandler
			{
				internal string user = null;

				internal string password = null;

				internal Server(string user, string password)
				{
					this.user = user;
					this.password = password;
				}

				/// <exception cref="Javax.Security.Auth.Callback.UnsupportedCallbackException"/>
				/// <exception cref="Javax.Security.Sasl.SaslException"/>
				public virtual void Handle(Javax.Security.Auth.Callback.Callback[] callbacks)
				{
					NameCallback nc = null;
					PasswordCallback pc = null;
					AuthorizeCallback ac = null;
					foreach (Javax.Security.Auth.Callback.Callback callback in callbacks)
					{
						if (callback is NameCallback)
						{
							nc = (NameCallback)callback;
							NUnit.Framework.Assert.AreEqual(user, nc.GetName());
						}
						else
						{
							if (callback is PasswordCallback)
							{
								pc = (PasswordCallback)callback;
								if (!password.Equals(new string(pc.GetPassword())))
								{
									throw new ArgumentException("wrong password");
								}
							}
							else
							{
								if (callback is AuthorizeCallback)
								{
									ac = (AuthorizeCallback)callback;
									NUnit.Framework.Assert.AreEqual(user, ac.GetAuthorizationID());
									NUnit.Framework.Assert.AreEqual(user, ac.GetAuthenticationID());
									ac.SetAuthorized(true);
									ac.SetAuthorizedID(ac.GetAuthenticationID());
								}
								else
								{
									throw new UnsupportedCallbackException(callback, "Unsupported SASL PLAIN Callback"
										);
								}
							}
						}
					}
					NUnit.Framework.Assert.IsNotNull(nc);
					NUnit.Framework.Assert.IsNotNull(pc);
					NUnit.Framework.Assert.IsNotNull(ac);
				}
			}
		}

		private static Sharpen.Pattern BadToken = Sharpen.Pattern.Compile(".*DIGEST-MD5: digest response format violation.*"
			);

		private static Sharpen.Pattern KrbFailed = Sharpen.Pattern.Compile(".*Failed on local exception:.* "
			 + "Failed to specify server's Kerberos principal name.*");

		private static Sharpen.Pattern Denied(SaslRpcServer.AuthMethod method)
		{
			return Sharpen.Pattern.Compile(".*RemoteException.*AccessControlException.*: " + 
				method + " authentication is not enabled.*");
		}

		private static Sharpen.Pattern No(params SaslRpcServer.AuthMethod[] method)
		{
			string methods = StringUtils.Join(method, ",\\s*");
			return Sharpen.Pattern.Compile(".*Failed on local exception:.* " + "Client cannot authenticate via:\\["
				 + methods + "\\].*");
		}

		private static Sharpen.Pattern NoTokenAuth = Sharpen.Pattern.Compile(".*IllegalArgumentException: "
			 + "TOKEN authentication requires a secret manager");

		private static Sharpen.Pattern NoFallback = Sharpen.Pattern.Compile(".*Failed on local exception:.* "
			 + "Server asks us to fall back to SIMPLE auth, " + "but this client is configured to only allow secure connections.*"
			);

		/*
		*  simple server
		*/
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSimpleServer()
		{
			AssertAuthEquals(SaslRpcServer.AuthMethod.Simple, GetAuthMethod(SaslRpcServer.AuthMethod
				.Simple, SaslRpcServer.AuthMethod.Simple));
			AssertAuthEquals(SaslRpcServer.AuthMethod.Simple, GetAuthMethod(SaslRpcServer.AuthMethod
				.Simple, SaslRpcServer.AuthMethod.Simple, TestSaslRPC.UseToken.Other));
			// SASL methods are normally reverted to SIMPLE
			AssertAuthEquals(SaslRpcServer.AuthMethod.Simple, GetAuthMethod(SaslRpcServer.AuthMethod
				.Kerberos, SaslRpcServer.AuthMethod.Simple));
			AssertAuthEquals(SaslRpcServer.AuthMethod.Simple, GetAuthMethod(SaslRpcServer.AuthMethod
				.Kerberos, SaslRpcServer.AuthMethod.Simple, TestSaslRPC.UseToken.Other));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNoClientFallbackToSimple()
		{
			clientFallBackToSimpleAllowed = false;
			// tokens are irrelevant w/o secret manager enabled
			AssertAuthEquals(SaslRpcServer.AuthMethod.Simple, GetAuthMethod(SaslRpcServer.AuthMethod
				.Simple, SaslRpcServer.AuthMethod.Simple));
			AssertAuthEquals(SaslRpcServer.AuthMethod.Simple, GetAuthMethod(SaslRpcServer.AuthMethod
				.Simple, SaslRpcServer.AuthMethod.Simple, TestSaslRPC.UseToken.Other));
			AssertAuthEquals(SaslRpcServer.AuthMethod.Simple, GetAuthMethod(SaslRpcServer.AuthMethod
				.Simple, SaslRpcServer.AuthMethod.Simple, TestSaslRPC.UseToken.Valid));
			AssertAuthEquals(SaslRpcServer.AuthMethod.Simple, GetAuthMethod(SaslRpcServer.AuthMethod
				.Simple, SaslRpcServer.AuthMethod.Simple, TestSaslRPC.UseToken.Invalid));
			// A secure client must not fallback
			AssertAuthEquals(NoFallback, GetAuthMethod(SaslRpcServer.AuthMethod.Kerberos, SaslRpcServer.AuthMethod
				.Simple));
			AssertAuthEquals(NoFallback, GetAuthMethod(SaslRpcServer.AuthMethod.Kerberos, SaslRpcServer.AuthMethod
				.Simple, TestSaslRPC.UseToken.Other));
			AssertAuthEquals(NoFallback, GetAuthMethod(SaslRpcServer.AuthMethod.Kerberos, SaslRpcServer.AuthMethod
				.Simple, TestSaslRPC.UseToken.Valid));
			AssertAuthEquals(NoFallback, GetAuthMethod(SaslRpcServer.AuthMethod.Kerberos, SaslRpcServer.AuthMethod
				.Simple, TestSaslRPC.UseToken.Invalid));
			// Now set server to simple and also force the secret-manager. Now server
			// should have both simple and token enabled.
			forceSecretManager = true;
			AssertAuthEquals(SaslRpcServer.AuthMethod.Simple, GetAuthMethod(SaslRpcServer.AuthMethod
				.Simple, SaslRpcServer.AuthMethod.Simple));
			AssertAuthEquals(SaslRpcServer.AuthMethod.Simple, GetAuthMethod(SaslRpcServer.AuthMethod
				.Simple, SaslRpcServer.AuthMethod.Simple, TestSaslRPC.UseToken.Other));
			AssertAuthEquals(SaslRpcServer.AuthMethod.Token, GetAuthMethod(SaslRpcServer.AuthMethod
				.Simple, SaslRpcServer.AuthMethod.Simple, TestSaslRPC.UseToken.Valid));
			AssertAuthEquals(BadToken, GetAuthMethod(SaslRpcServer.AuthMethod.Simple, SaslRpcServer.AuthMethod
				.Simple, TestSaslRPC.UseToken.Invalid));
			// A secure client must not fallback
			AssertAuthEquals(NoFallback, GetAuthMethod(SaslRpcServer.AuthMethod.Kerberos, SaslRpcServer.AuthMethod
				.Simple));
			AssertAuthEquals(NoFallback, GetAuthMethod(SaslRpcServer.AuthMethod.Kerberos, SaslRpcServer.AuthMethod
				.Simple, TestSaslRPC.UseToken.Other));
			AssertAuthEquals(SaslRpcServer.AuthMethod.Token, GetAuthMethod(SaslRpcServer.AuthMethod
				.Kerberos, SaslRpcServer.AuthMethod.Simple, TestSaslRPC.UseToken.Valid));
			AssertAuthEquals(BadToken, GetAuthMethod(SaslRpcServer.AuthMethod.Kerberos, SaslRpcServer.AuthMethod
				.Simple, TestSaslRPC.UseToken.Invalid));
			// doesn't try SASL
			AssertAuthEquals(Denied(SaslRpcServer.AuthMethod.Simple), GetAuthMethod(SaslRpcServer.AuthMethod
				.Simple, SaslRpcServer.AuthMethod.Token));
			// does try SASL
			AssertAuthEquals(No(SaslRpcServer.AuthMethod.Token), GetAuthMethod(SaslRpcServer.AuthMethod
				.Simple, SaslRpcServer.AuthMethod.Token, TestSaslRPC.UseToken.Other));
			AssertAuthEquals(SaslRpcServer.AuthMethod.Token, GetAuthMethod(SaslRpcServer.AuthMethod
				.Simple, SaslRpcServer.AuthMethod.Token, TestSaslRPC.UseToken.Valid));
			AssertAuthEquals(BadToken, GetAuthMethod(SaslRpcServer.AuthMethod.Simple, SaslRpcServer.AuthMethod
				.Token, TestSaslRPC.UseToken.Invalid));
			AssertAuthEquals(No(SaslRpcServer.AuthMethod.Token), GetAuthMethod(SaslRpcServer.AuthMethod
				.Kerberos, SaslRpcServer.AuthMethod.Token));
			AssertAuthEquals(No(SaslRpcServer.AuthMethod.Token), GetAuthMethod(SaslRpcServer.AuthMethod
				.Kerberos, SaslRpcServer.AuthMethod.Token, TestSaslRPC.UseToken.Other));
			AssertAuthEquals(SaslRpcServer.AuthMethod.Token, GetAuthMethod(SaslRpcServer.AuthMethod
				.Kerberos, SaslRpcServer.AuthMethod.Token, TestSaslRPC.UseToken.Valid));
			AssertAuthEquals(BadToken, GetAuthMethod(SaslRpcServer.AuthMethod.Kerberos, SaslRpcServer.AuthMethod
				.Token, TestSaslRPC.UseToken.Invalid));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSimpleServerWithTokens()
		{
			// Client not using tokens
			AssertAuthEquals(SaslRpcServer.AuthMethod.Simple, GetAuthMethod(SaslRpcServer.AuthMethod
				.Simple, SaslRpcServer.AuthMethod.Simple));
			// SASL methods are reverted to SIMPLE
			AssertAuthEquals(SaslRpcServer.AuthMethod.Simple, GetAuthMethod(SaslRpcServer.AuthMethod
				.Kerberos, SaslRpcServer.AuthMethod.Simple));
			// Use tokens. But tokens are ignored because client is reverted to simple
			// due to server not using tokens
			AssertAuthEquals(SaslRpcServer.AuthMethod.Simple, GetAuthMethod(SaslRpcServer.AuthMethod
				.Kerberos, SaslRpcServer.AuthMethod.Simple, TestSaslRPC.UseToken.Valid));
			AssertAuthEquals(SaslRpcServer.AuthMethod.Simple, GetAuthMethod(SaslRpcServer.AuthMethod
				.Kerberos, SaslRpcServer.AuthMethod.Simple, TestSaslRPC.UseToken.Other));
			// server isn't really advertising tokens
			enableSecretManager = true;
			AssertAuthEquals(SaslRpcServer.AuthMethod.Simple, GetAuthMethod(SaslRpcServer.AuthMethod
				.Simple, SaslRpcServer.AuthMethod.Simple, TestSaslRPC.UseToken.Valid));
			AssertAuthEquals(SaslRpcServer.AuthMethod.Simple, GetAuthMethod(SaslRpcServer.AuthMethod
				.Simple, SaslRpcServer.AuthMethod.Simple, TestSaslRPC.UseToken.Other));
			AssertAuthEquals(SaslRpcServer.AuthMethod.Simple, GetAuthMethod(SaslRpcServer.AuthMethod
				.Kerberos, SaslRpcServer.AuthMethod.Simple, TestSaslRPC.UseToken.Valid));
			AssertAuthEquals(SaslRpcServer.AuthMethod.Simple, GetAuthMethod(SaslRpcServer.AuthMethod
				.Kerberos, SaslRpcServer.AuthMethod.Simple, TestSaslRPC.UseToken.Other));
			// now the simple server takes tokens
			forceSecretManager = true;
			AssertAuthEquals(SaslRpcServer.AuthMethod.Token, GetAuthMethod(SaslRpcServer.AuthMethod
				.Simple, SaslRpcServer.AuthMethod.Simple, TestSaslRPC.UseToken.Valid));
			AssertAuthEquals(SaslRpcServer.AuthMethod.Simple, GetAuthMethod(SaslRpcServer.AuthMethod
				.Simple, SaslRpcServer.AuthMethod.Simple, TestSaslRPC.UseToken.Other));
			AssertAuthEquals(SaslRpcServer.AuthMethod.Token, GetAuthMethod(SaslRpcServer.AuthMethod
				.Kerberos, SaslRpcServer.AuthMethod.Simple, TestSaslRPC.UseToken.Valid));
			AssertAuthEquals(SaslRpcServer.AuthMethod.Simple, GetAuthMethod(SaslRpcServer.AuthMethod
				.Kerberos, SaslRpcServer.AuthMethod.Simple, TestSaslRPC.UseToken.Other));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSimpleServerWithInvalidTokens()
		{
			// Tokens are ignored because client is reverted to simple
			AssertAuthEquals(SaslRpcServer.AuthMethod.Simple, GetAuthMethod(SaslRpcServer.AuthMethod
				.Simple, SaslRpcServer.AuthMethod.Simple, TestSaslRPC.UseToken.Invalid));
			AssertAuthEquals(SaslRpcServer.AuthMethod.Simple, GetAuthMethod(SaslRpcServer.AuthMethod
				.Kerberos, SaslRpcServer.AuthMethod.Simple, TestSaslRPC.UseToken.Invalid));
			enableSecretManager = true;
			AssertAuthEquals(SaslRpcServer.AuthMethod.Simple, GetAuthMethod(SaslRpcServer.AuthMethod
				.Simple, SaslRpcServer.AuthMethod.Simple, TestSaslRPC.UseToken.Invalid));
			AssertAuthEquals(SaslRpcServer.AuthMethod.Simple, GetAuthMethod(SaslRpcServer.AuthMethod
				.Kerberos, SaslRpcServer.AuthMethod.Simple, TestSaslRPC.UseToken.Invalid));
			forceSecretManager = true;
			AssertAuthEquals(BadToken, GetAuthMethod(SaslRpcServer.AuthMethod.Simple, SaslRpcServer.AuthMethod
				.Simple, TestSaslRPC.UseToken.Invalid));
			AssertAuthEquals(BadToken, GetAuthMethod(SaslRpcServer.AuthMethod.Kerberos, SaslRpcServer.AuthMethod
				.Simple, TestSaslRPC.UseToken.Invalid));
		}

		/*
		*  token server
		*/
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestTokenOnlyServer()
		{
			// simple client w/o tokens won't try SASL, so server denies
			AssertAuthEquals(Denied(SaslRpcServer.AuthMethod.Simple), GetAuthMethod(SaslRpcServer.AuthMethod
				.Simple, SaslRpcServer.AuthMethod.Token));
			AssertAuthEquals(No(SaslRpcServer.AuthMethod.Token), GetAuthMethod(SaslRpcServer.AuthMethod
				.Simple, SaslRpcServer.AuthMethod.Token, TestSaslRPC.UseToken.Other));
			AssertAuthEquals(No(SaslRpcServer.AuthMethod.Token), GetAuthMethod(SaslRpcServer.AuthMethod
				.Kerberos, SaslRpcServer.AuthMethod.Token));
			AssertAuthEquals(No(SaslRpcServer.AuthMethod.Token), GetAuthMethod(SaslRpcServer.AuthMethod
				.Kerberos, SaslRpcServer.AuthMethod.Token, TestSaslRPC.UseToken.Other));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestTokenOnlyServerWithTokens()
		{
			AssertAuthEquals(SaslRpcServer.AuthMethod.Token, GetAuthMethod(SaslRpcServer.AuthMethod
				.Simple, SaslRpcServer.AuthMethod.Token, TestSaslRPC.UseToken.Valid));
			AssertAuthEquals(SaslRpcServer.AuthMethod.Token, GetAuthMethod(SaslRpcServer.AuthMethod
				.Kerberos, SaslRpcServer.AuthMethod.Token, TestSaslRPC.UseToken.Valid));
			enableSecretManager = false;
			AssertAuthEquals(NoTokenAuth, GetAuthMethod(SaslRpcServer.AuthMethod.Simple, SaslRpcServer.AuthMethod
				.Token, TestSaslRPC.UseToken.Valid));
			AssertAuthEquals(NoTokenAuth, GetAuthMethod(SaslRpcServer.AuthMethod.Kerberos, SaslRpcServer.AuthMethod
				.Token, TestSaslRPC.UseToken.Valid));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestTokenOnlyServerWithInvalidTokens()
		{
			AssertAuthEquals(BadToken, GetAuthMethod(SaslRpcServer.AuthMethod.Simple, SaslRpcServer.AuthMethod
				.Token, TestSaslRPC.UseToken.Invalid));
			AssertAuthEquals(BadToken, GetAuthMethod(SaslRpcServer.AuthMethod.Kerberos, SaslRpcServer.AuthMethod
				.Token, TestSaslRPC.UseToken.Invalid));
			enableSecretManager = false;
			AssertAuthEquals(NoTokenAuth, GetAuthMethod(SaslRpcServer.AuthMethod.Simple, SaslRpcServer.AuthMethod
				.Token, TestSaslRPC.UseToken.Invalid));
			AssertAuthEquals(NoTokenAuth, GetAuthMethod(SaslRpcServer.AuthMethod.Kerberos, SaslRpcServer.AuthMethod
				.Token, TestSaslRPC.UseToken.Invalid));
		}

		/*
		* kerberos server
		*/
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestKerberosServer()
		{
			// doesn't try SASL
			AssertAuthEquals(Denied(SaslRpcServer.AuthMethod.Simple), GetAuthMethod(SaslRpcServer.AuthMethod
				.Simple, SaslRpcServer.AuthMethod.Kerberos));
			// does try SASL
			AssertAuthEquals(No(SaslRpcServer.AuthMethod.Token, SaslRpcServer.AuthMethod.Kerberos
				), GetAuthMethod(SaslRpcServer.AuthMethod.Simple, SaslRpcServer.AuthMethod.Kerberos
				, TestSaslRPC.UseToken.Other));
			// no tgt
			AssertAuthEquals(KrbFailed, GetAuthMethod(SaslRpcServer.AuthMethod.Kerberos, SaslRpcServer.AuthMethod
				.Kerberos));
			AssertAuthEquals(KrbFailed, GetAuthMethod(SaslRpcServer.AuthMethod.Kerberos, SaslRpcServer.AuthMethod
				.Kerberos, TestSaslRPC.UseToken.Other));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestKerberosServerWithTokens()
		{
			// can use tokens regardless of auth
			AssertAuthEquals(SaslRpcServer.AuthMethod.Token, GetAuthMethod(SaslRpcServer.AuthMethod
				.Simple, SaslRpcServer.AuthMethod.Kerberos, TestSaslRPC.UseToken.Valid));
			AssertAuthEquals(SaslRpcServer.AuthMethod.Token, GetAuthMethod(SaslRpcServer.AuthMethod
				.Kerberos, SaslRpcServer.AuthMethod.Kerberos, TestSaslRPC.UseToken.Valid));
			enableSecretManager = false;
			// shouldn't even try token because server didn't tell us to
			AssertAuthEquals(No(SaslRpcServer.AuthMethod.Kerberos), GetAuthMethod(SaslRpcServer.AuthMethod
				.Simple, SaslRpcServer.AuthMethod.Kerberos, TestSaslRPC.UseToken.Valid));
			AssertAuthEquals(KrbFailed, GetAuthMethod(SaslRpcServer.AuthMethod.Kerberos, SaslRpcServer.AuthMethod
				.Kerberos, TestSaslRPC.UseToken.Valid));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestKerberosServerWithInvalidTokens()
		{
			AssertAuthEquals(BadToken, GetAuthMethod(SaslRpcServer.AuthMethod.Simple, SaslRpcServer.AuthMethod
				.Kerberos, TestSaslRPC.UseToken.Invalid));
			AssertAuthEquals(BadToken, GetAuthMethod(SaslRpcServer.AuthMethod.Kerberos, SaslRpcServer.AuthMethod
				.Kerberos, TestSaslRPC.UseToken.Invalid));
			enableSecretManager = false;
			AssertAuthEquals(No(SaslRpcServer.AuthMethod.Kerberos), GetAuthMethod(SaslRpcServer.AuthMethod
				.Simple, SaslRpcServer.AuthMethod.Kerberos, TestSaslRPC.UseToken.Invalid));
			AssertAuthEquals(KrbFailed, GetAuthMethod(SaslRpcServer.AuthMethod.Kerberos, SaslRpcServer.AuthMethod
				.Kerberos, TestSaslRPC.UseToken.Invalid));
		}

		// test helpers
		/// <exception cref="System.Exception"/>
		private string GetAuthMethod(SaslRpcServer.AuthMethod clientAuth, SaslRpcServer.AuthMethod
			 serverAuth)
		{
			try
			{
				return InternalGetAuthMethod(clientAuth, serverAuth, TestSaslRPC.UseToken.None);
			}
			catch (Exception e)
			{
				Log.Warn("Auth method failure", e);
				return e.ToString();
			}
		}

		/// <exception cref="System.Exception"/>
		private string GetAuthMethod(SaslRpcServer.AuthMethod clientAuth, SaslRpcServer.AuthMethod
			 serverAuth, TestSaslRPC.UseToken tokenType)
		{
			try
			{
				return InternalGetAuthMethod(clientAuth, serverAuth, tokenType);
			}
			catch (Exception e)
			{
				Log.Warn("Auth method failure", e);
				return e.ToString();
			}
		}

		/// <exception cref="System.Exception"/>
		private string InternalGetAuthMethod(SaslRpcServer.AuthMethod clientAuth, SaslRpcServer.AuthMethod
			 serverAuth, TestSaslRPC.UseToken tokenType)
		{
			Configuration serverConf = new Configuration(conf);
			serverConf.Set(CommonConfigurationKeysPublic.HadoopSecurityAuthentication, serverAuth
				.ToString());
			UserGroupInformation.SetConfiguration(serverConf);
			UserGroupInformation serverUgi = (serverAuth == SaslRpcServer.AuthMethod.Kerberos
				) ? UserGroupInformation.CreateRemoteUser("server/localhost@NONE") : UserGroupInformation
				.CreateRemoteUser("server");
			serverUgi.SetAuthenticationMethod(serverAuth);
			TestSaslRPC.TestTokenSecretManager sm = new TestSaslRPC.TestTokenSecretManager();
			bool useSecretManager = (serverAuth != SaslRpcServer.AuthMethod.Simple);
			if (enableSecretManager != null)
			{
				useSecretManager &= enableSecretManager;
			}
			if (forceSecretManager != null)
			{
				useSecretManager |= forceSecretManager;
			}
			SecretManager<object> serverSm = useSecretManager ? sm : null;
			Org.Apache.Hadoop.Ipc.Server server = serverUgi.DoAs(new _PrivilegedExceptionAction_890
				(serverConf, serverSm));
			Configuration clientConf = new Configuration(conf);
			clientConf.Set(CommonConfigurationKeysPublic.HadoopSecurityAuthentication, clientAuth
				.ToString());
			clientConf.SetBoolean(CommonConfigurationKeys.IpcClientFallbackToSimpleAuthAllowedKey
				, clientFallBackToSimpleAllowed);
			UserGroupInformation.SetConfiguration(clientConf);
			UserGroupInformation clientUgi = UserGroupInformation.CreateRemoteUser("client");
			clientUgi.SetAuthenticationMethod(clientAuth);
			IPEndPoint addr = NetUtils.GetConnectAddress(server);
			if (tokenType != TestSaslRPC.UseToken.None)
			{
				TestSaslRPC.TestTokenIdentifier tokenId = new TestSaslRPC.TestTokenIdentifier(new 
					Org.Apache.Hadoop.IO.Text(clientUgi.GetUserName()));
				Org.Apache.Hadoop.Security.Token.Token<TestSaslRPC.TestTokenIdentifier> token = null;
				switch (tokenType)
				{
					case TestSaslRPC.UseToken.Valid:
					{
						token = new Org.Apache.Hadoop.Security.Token.Token<TestSaslRPC.TestTokenIdentifier
							>(tokenId, sm);
						SecurityUtil.SetTokenService(token, addr);
						break;
					}

					case TestSaslRPC.UseToken.Invalid:
					{
						token = new Org.Apache.Hadoop.Security.Token.Token<TestSaslRPC.TestTokenIdentifier
							>(tokenId.GetBytes(), Sharpen.Runtime.GetBytesForString("bad-password!"), tokenId
							.GetKind(), null);
						SecurityUtil.SetTokenService(token, addr);
						break;
					}

					case TestSaslRPC.UseToken.Other:
					{
						token = new Org.Apache.Hadoop.Security.Token.Token<TestSaslRPC.TestTokenIdentifier
							>();
						break;
					}

					case TestSaslRPC.UseToken.None:
					{
						break;
					}
				}
				// won't get here
				clientUgi.AddToken(token);
			}
			try
			{
				Log.Info("trying ugi:" + clientUgi + " tokens:" + clientUgi.GetTokens());
				return clientUgi.DoAs(new _PrivilegedExceptionAction_941(this, addr, clientConf, 
					clientUgi));
			}
			finally
			{
				// make sure the other side thinks we are who we said we are!!!
				// verify sasl completed with correct QOP
				server.Stop();
			}
		}

		private sealed class _PrivilegedExceptionAction_890 : PrivilegedExceptionAction<Org.Apache.Hadoop.Ipc.Server
			>
		{
			public _PrivilegedExceptionAction_890(Configuration serverConf, SecretManager<object
				> serverSm)
			{
				this.serverConf = serverConf;
				this.serverSm = serverSm;
			}

			/// <exception cref="System.IO.IOException"/>
			public Org.Apache.Hadoop.Ipc.Server Run()
			{
				Org.Apache.Hadoop.Ipc.Server server = new RPC.Builder(serverConf).SetProtocol(typeof(
					TestSaslRPC.TestSaslProtocol)).SetInstance(new TestSaslRPC.TestSaslImpl()).SetBindAddress
					(TestSaslRPC.Address).SetPort(0).SetNumHandlers(5).SetVerbose(true).SetSecretManager
					(serverSm).Build();
				server.Start();
				return server;
			}

			private readonly Configuration serverConf;

			private readonly SecretManager<object> serverSm;
		}

		private sealed class _PrivilegedExceptionAction_941 : PrivilegedExceptionAction<string
			>
		{
			public _PrivilegedExceptionAction_941(TestSaslRPC _enclosing, IPEndPoint addr, Configuration
				 clientConf, UserGroupInformation clientUgi)
			{
				this._enclosing = _enclosing;
				this.addr = addr;
				this.clientConf = clientConf;
				this.clientUgi = clientUgi;
			}

			/// <exception cref="System.IO.IOException"/>
			public string Run()
			{
				TestSaslRPC.TestSaslProtocol proxy = null;
				try
				{
					proxy = RPC.GetProxy<TestSaslRPC.TestSaslProtocol>(TestSaslRPC.TestSaslProtocol.versionID
						, addr, clientConf);
					proxy.Ping();
					NUnit.Framework.Assert.AreEqual(clientUgi.GetUserName(), proxy.GetAuthUser());
					SaslRpcServer.AuthMethod authMethod = proxy.GetAuthMethod();
					NUnit.Framework.Assert.AreEqual((authMethod != SaslRpcServer.AuthMethod.Simple) ? 
						this._enclosing.expectedQop.saslQop : null, RPC.GetConnectionIdForProxy(proxy).GetSaslQop
						());
					return authMethod.ToString();
				}
				finally
				{
					if (proxy != null)
					{
						RPC.StopProxy(proxy);
					}
				}
			}

			private readonly TestSaslRPC _enclosing;

			private readonly IPEndPoint addr;

			private readonly Configuration clientConf;

			private readonly UserGroupInformation clientUgi;
		}

		private static void AssertAuthEquals(SaslRpcServer.AuthMethod expect, string actual
			)
		{
			NUnit.Framework.Assert.AreEqual(expect.ToString(), actual);
		}

		private static void AssertAuthEquals(Sharpen.Pattern expect, string actual)
		{
			// this allows us to see the regexp and the value it didn't match
			if (!expect.Matcher(actual).Matches())
			{
				NUnit.Framework.Assert.AreEqual(expect, actual);
			}
			else
			{
				// it failed
				NUnit.Framework.Assert.IsTrue(true);
			}
		}

		internal class AuthSaslPropertiesResolver : SaslPropertiesResolver
		{
			// it matched
			/*
			* Class used to test overriding QOP values using SaslPropertiesResolver
			*/
			public override IDictionary<string, string> GetServerProperties(IPAddress address
				)
			{
				IDictionary<string, string> newPropertes = new Dictionary<string, string>(GetDefaultProperties
					());
				newPropertes[Javax.Security.Sasl.Sasl.Qop] = SaslRpcServer.QualityOfProtection.Authentication
					.GetSaslQop();
				return newPropertes;
			}
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			System.Console.Out.WriteLine("Testing Kerberos authentication over RPC");
			if (args.Length != 2)
			{
				System.Console.Error.WriteLine("Usage: java <options> org.apache.hadoop.ipc.TestSaslRPC "
					 + " <serverPrincipal> <keytabFile>");
				System.Environment.Exit(-1);
			}
			string principal = args[0];
			string keytab = args[1];
			TestKerberosRpc(principal, keytab);
		}
	}
}
