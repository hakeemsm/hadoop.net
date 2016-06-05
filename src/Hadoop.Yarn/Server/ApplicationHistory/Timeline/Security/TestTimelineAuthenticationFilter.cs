using System;
using System.Collections.Generic;
using NUnit.Framework;
using NUnit.Framework.Runners;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Http;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Minikdc;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authentication;
using Org.Apache.Hadoop.Security.Authentication.Client;
using Org.Apache.Hadoop.Security.Authentication.Server;
using Org.Apache.Hadoop.Security.Authorize;
using Org.Apache.Hadoop.Security.Ssl;
using Org.Apache.Hadoop.Yarn.Api.Records.Timeline;
using Org.Apache.Hadoop.Yarn.Client.Api;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Security.Client;
using Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice;
using Org.Apache.Hadoop.Yarn.Server.Timeline;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Timeline.Security
{
	public class TestTimelineAuthenticationFilter
	{
		private const string FooUser = "foo";

		private const string BarUser = "bar";

		private const string HttpUser = "HTTP";

		private static readonly FilePath testRootDir = new FilePath(Runtime.GetProperty("test.build.dir"
			, "target/test-dir"), typeof(Org.Apache.Hadoop.Yarn.Server.Timeline.Security.TestTimelineAuthenticationFilter
			).FullName + "-root");

		private static FilePath httpSpnegoKeytabFile = new FilePath(KerberosTestUtils.GetKeytabFile
			());

		private static string httpSpnegoPrincipal = KerberosTestUtils.GetServerPrincipal(
			);

		private static readonly string Basedir = Runtime.GetProperty("test.build.dir", "target/test-dir"
			) + "/" + typeof(Org.Apache.Hadoop.Yarn.Server.Timeline.Security.TestTimelineAuthenticationFilter
			).Name;

		[Parameterized.Parameters]
		public static ICollection<object[]> WithSsl()
		{
			return Arrays.AsList(new object[][] { new object[] { false }, new object[] { true
				 } });
		}

		private static MiniKdc testMiniKDC;

		private static string keystoresDir;

		private static string sslConfDir;

		private static ApplicationHistoryServer testTimelineServer;

		private static Configuration conf;

		private static bool withSsl;

		public TestTimelineAuthenticationFilter(bool withSsl)
		{
			Org.Apache.Hadoop.Yarn.Server.Timeline.Security.TestTimelineAuthenticationFilter.
				withSsl = withSsl;
		}

		[BeforeClass]
		public static void Setup()
		{
			try
			{
				testMiniKDC = new MiniKdc(MiniKdc.CreateConf(), testRootDir);
				testMiniKDC.Start();
				testMiniKDC.CreatePrincipal(httpSpnegoKeytabFile, HttpUser + "/localhost");
			}
			catch (Exception)
			{
				NUnit.Framework.Assert.IsTrue("Couldn't setup MiniKDC", false);
			}
			try
			{
				testTimelineServer = new ApplicationHistoryServer();
				conf = new Configuration(false);
				conf.SetStrings(TimelineAuthenticationFilterInitializer.Prefix + "type", "kerberos"
					);
				conf.Set(TimelineAuthenticationFilterInitializer.Prefix + KerberosAuthenticationHandler
					.Principal, httpSpnegoPrincipal);
				conf.Set(TimelineAuthenticationFilterInitializer.Prefix + KerberosAuthenticationHandler
					.Keytab, httpSpnegoKeytabFile.GetAbsolutePath());
				conf.Set(CommonConfigurationKeysPublic.HadoopSecurityAuthentication, "kerberos");
				conf.Set(YarnConfiguration.TimelineServicePrincipal, httpSpnegoPrincipal);
				conf.Set(YarnConfiguration.TimelineServiceKeytab, httpSpnegoKeytabFile.GetAbsolutePath
					());
				conf.SetBoolean(YarnConfiguration.TimelineServiceEnabled, true);
				conf.SetClass(YarnConfiguration.TimelineServiceStore, typeof(MemoryTimelineStore)
					, typeof(TimelineStore));
				conf.Set(YarnConfiguration.TimelineServiceAddress, "localhost:10200");
				conf.Set(YarnConfiguration.TimelineServiceWebappAddress, "localhost:8188");
				conf.Set(YarnConfiguration.TimelineServiceWebappHttpsAddress, "localhost:8190");
				conf.Set("hadoop.proxyuser.HTTP.hosts", "*");
				conf.Set("hadoop.proxyuser.HTTP.users", FooUser);
				conf.SetInt(YarnConfiguration.TimelineServiceClientMaxRetries, 1);
				if (withSsl)
				{
					conf.Set(YarnConfiguration.YarnHttpPolicyKey, HttpConfig.Policy.HttpsOnly.ToString
						());
					FilePath @base = new FilePath(Basedir);
					FileUtil.FullyDelete(@base);
					@base.Mkdirs();
					keystoresDir = new FilePath(Basedir).GetAbsolutePath();
					sslConfDir = KeyStoreTestUtil.GetClasspathDir(typeof(Org.Apache.Hadoop.Yarn.Server.Timeline.Security.TestTimelineAuthenticationFilter
						));
					KeyStoreTestUtil.SetupSSLConfig(keystoresDir, sslConfDir, conf, false);
				}
				UserGroupInformation.SetConfiguration(conf);
				testTimelineServer.Init(conf);
				testTimelineServer.Start();
			}
			catch (Exception)
			{
				NUnit.Framework.Assert.IsTrue("Couldn't setup TimelineServer", false);
			}
		}

		private TimelineClient CreateTimelineClientForUGI()
		{
			TimelineClient client = TimelineClient.CreateTimelineClient();
			client.Init(conf);
			client.Start();
			return client;
		}

		/// <exception cref="System.Exception"/>
		[AfterClass]
		public static void TearDown()
		{
			if (testMiniKDC != null)
			{
				testMiniKDC.Stop();
			}
			if (testTimelineServer != null)
			{
				testTimelineServer.Stop();
			}
			if (withSsl)
			{
				KeyStoreTestUtil.CleanupSSLConfig(keystoresDir, sslConfDir);
				FilePath @base = new FilePath(Basedir);
				FileUtil.FullyDelete(@base);
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestPutTimelineEntities()
		{
			KerberosTestUtils.DoAs(HttpUser + "/localhost", new _Callable_178(this));
		}

		private sealed class _Callable_178 : Callable<Void>
		{
			public _Callable_178(TestTimelineAuthenticationFilter _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.Exception"/>
			public Void Call()
			{
				TimelineClient client = this._enclosing.CreateTimelineClientForUGI();
				TimelineEntity entityToStore = new TimelineEntity();
				entityToStore.SetEntityType(typeof(Org.Apache.Hadoop.Yarn.Server.Timeline.Security.TestTimelineAuthenticationFilter
					).FullName);
				entityToStore.SetEntityId("entity1");
				entityToStore.SetStartTime(0L);
				TimelinePutResponse putResponse = client.PutEntities(entityToStore);
				NUnit.Framework.Assert.AreEqual(0, putResponse.GetErrors().Count);
				TimelineEntity entityToRead = Org.Apache.Hadoop.Yarn.Server.Timeline.Security.TestTimelineAuthenticationFilter
					.testTimelineServer.GetTimelineStore().GetEntity("entity1", typeof(Org.Apache.Hadoop.Yarn.Server.Timeline.Security.TestTimelineAuthenticationFilter
					).FullName, null);
				NUnit.Framework.Assert.IsNotNull(entityToRead);
				return null;
			}

			private readonly TestTimelineAuthenticationFilter _enclosing;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestPutDomains()
		{
			KerberosTestUtils.DoAs(HttpUser + "/localhost", new _Callable_200(this));
		}

		private sealed class _Callable_200 : Callable<Void>
		{
			public _Callable_200(TestTimelineAuthenticationFilter _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.Exception"/>
			public Void Call()
			{
				TimelineClient client = this._enclosing.CreateTimelineClientForUGI();
				TimelineDomain domainToStore = new TimelineDomain();
				domainToStore.SetId(typeof(Org.Apache.Hadoop.Yarn.Server.Timeline.Security.TestTimelineAuthenticationFilter
					).FullName);
				domainToStore.SetReaders("*");
				domainToStore.SetWriters("*");
				client.PutDomain(domainToStore);
				TimelineDomain domainToRead = Org.Apache.Hadoop.Yarn.Server.Timeline.Security.TestTimelineAuthenticationFilter
					.testTimelineServer.GetTimelineStore().GetDomain(typeof(Org.Apache.Hadoop.Yarn.Server.Timeline.Security.TestTimelineAuthenticationFilter
					).FullName);
				NUnit.Framework.Assert.IsNotNull(domainToRead);
				return null;
			}

			private readonly TestTimelineAuthenticationFilter _enclosing;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDelegationTokenOperations()
		{
			TimelineClient httpUserClient = KerberosTestUtils.DoAs(HttpUser + "/localhost", new 
				_Callable_221(this));
			UserGroupInformation httpUser = KerberosTestUtils.DoAs(HttpUser + "/localhost", new 
				_Callable_228());
			// Let HTTP user to get the delegation for itself
			Org.Apache.Hadoop.Security.Token.Token<TimelineDelegationTokenIdentifier> token = 
				httpUserClient.GetDelegationToken(httpUser.GetShortUserName());
			NUnit.Framework.Assert.IsNotNull(token);
			TimelineDelegationTokenIdentifier tDT = token.DecodeIdentifier();
			NUnit.Framework.Assert.IsNotNull(tDT);
			NUnit.Framework.Assert.AreEqual(new Text(HttpUser), tDT.GetOwner());
			// Renew token
			NUnit.Framework.Assert.IsFalse(token.GetService().ToString().IsEmpty());
			// Renew the token from the token service address
			long renewTime1 = httpUserClient.RenewDelegationToken(token);
			Sharpen.Thread.Sleep(100);
			token.SetService(new Text());
			NUnit.Framework.Assert.IsTrue(token.GetService().ToString().IsEmpty());
			// If the token service address is not avaiable, it still can be renewed
			// from the configured address
			long renewTime2 = httpUserClient.RenewDelegationToken(token);
			NUnit.Framework.Assert.IsTrue(renewTime1 < renewTime2);
			// Cancel token
			NUnit.Framework.Assert.IsTrue(token.GetService().ToString().IsEmpty());
			// If the token service address is not avaiable, it still can be canceled
			// from the configured address
			httpUserClient.CancelDelegationToken(token);
			// Renew should not be successful because the token is canceled
			try
			{
				httpUserClient.RenewDelegationToken(token);
				NUnit.Framework.Assert.Fail();
			}
			catch (Exception e)
			{
				NUnit.Framework.Assert.IsTrue(e.Message.Contains("Renewal request for unknown token"
					));
			}
			// Let HTTP user to get the delegation token for FOO user
			UserGroupInformation fooUgi = UserGroupInformation.CreateProxyUser(FooUser, httpUser
				);
			TimelineClient fooUserClient = fooUgi.DoAs(new _PrivilegedExceptionAction_272(this
				));
			token = fooUserClient.GetDelegationToken(httpUser.GetShortUserName());
			NUnit.Framework.Assert.IsNotNull(token);
			tDT = token.DecodeIdentifier();
			NUnit.Framework.Assert.IsNotNull(tDT);
			NUnit.Framework.Assert.AreEqual(new Text(FooUser), tDT.GetOwner());
			NUnit.Framework.Assert.AreEqual(new Text(HttpUser), tDT.GetRealUser());
			// Renew token as the renewer
			Org.Apache.Hadoop.Security.Token.Token<TimelineDelegationTokenIdentifier> tokenToRenew
				 = token;
			renewTime1 = httpUserClient.RenewDelegationToken(tokenToRenew);
			renewTime2 = httpUserClient.RenewDelegationToken(tokenToRenew);
			NUnit.Framework.Assert.IsTrue(renewTime1 < renewTime2);
			// Cancel token
			NUnit.Framework.Assert.IsFalse(tokenToRenew.GetService().ToString().IsEmpty());
			// Cancel the token from the token service address
			fooUserClient.CancelDelegationToken(tokenToRenew);
			// Renew should not be successful because the token is canceled
			try
			{
				httpUserClient.RenewDelegationToken(tokenToRenew);
				NUnit.Framework.Assert.Fail();
			}
			catch (Exception e)
			{
				NUnit.Framework.Assert.IsTrue(e.Message.Contains("Renewal request for unknown token"
					));
			}
			// Let HTTP user to get the delegation token for BAR user
			UserGroupInformation barUgi = UserGroupInformation.CreateProxyUser(BarUser, httpUser
				);
			TimelineClient barUserClient = barUgi.DoAs(new _PrivilegedExceptionAction_309(this
				));
			try
			{
				barUserClient.GetDelegationToken(httpUser.GetShortUserName());
				NUnit.Framework.Assert.Fail();
			}
			catch (Exception e)
			{
				NUnit.Framework.Assert.IsTrue(e.InnerException is AuthorizationException || e.InnerException
					 is AuthenticationException);
			}
		}

		private sealed class _Callable_221 : Callable<TimelineClient>
		{
			public _Callable_221(TestTimelineAuthenticationFilter _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.Exception"/>
			public TimelineClient Call()
			{
				return this._enclosing.CreateTimelineClientForUGI();
			}

			private readonly TestTimelineAuthenticationFilter _enclosing;
		}

		private sealed class _Callable_228 : Callable<UserGroupInformation>
		{
			public _Callable_228()
			{
			}

			/// <exception cref="System.Exception"/>
			public UserGroupInformation Call()
			{
				return UserGroupInformation.GetCurrentUser();
			}
		}

		private sealed class _PrivilegedExceptionAction_272 : PrivilegedExceptionAction<TimelineClient
			>
		{
			public _PrivilegedExceptionAction_272(TestTimelineAuthenticationFilter _enclosing
				)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.Exception"/>
			public TimelineClient Run()
			{
				return this._enclosing.CreateTimelineClientForUGI();
			}

			private readonly TestTimelineAuthenticationFilter _enclosing;
		}

		private sealed class _PrivilegedExceptionAction_309 : PrivilegedExceptionAction<TimelineClient
			>
		{
			public _PrivilegedExceptionAction_309(TestTimelineAuthenticationFilter _enclosing
				)
			{
				this._enclosing = _enclosing;
			}

			public TimelineClient Run()
			{
				return this._enclosing.CreateTimelineClientForUGI();
			}

			private readonly TestTimelineAuthenticationFilter _enclosing;
		}
	}
}
