using System;
using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Security.Alias;
using Sharpen;

namespace Org.Apache.Hadoop.Security.Ssl
{
	public class TestSSLFactory
	{
		private static readonly string Basedir = Runtime.GetProperty("test.build.dir", "target/test-dir"
			) + "/" + typeof(TestSSLFactory).Name;

		private static readonly string KeystoresDir = new FilePath(Basedir).GetAbsolutePath
			();

		private string sslConfsDir;

		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void SetUp()
		{
			FilePath @base = new FilePath(Basedir);
			FileUtil.FullyDelete(@base);
			@base.Mkdirs();
		}

		/// <exception cref="System.Exception"/>
		private Configuration CreateConfiguration(bool clientCert, bool trustStore)
		{
			Configuration conf = new Configuration();
			KeyStoreTestUtil.SetupSSLConfig(KeystoresDir, sslConfsDir, conf, clientCert, trustStore
				);
			return conf;
		}

		/// <exception cref="System.Exception"/>
		[TearDown]
		[NUnit.Framework.SetUp]
		public virtual void CleanUp()
		{
			sslConfsDir = KeyStoreTestUtil.GetClasspathDir(typeof(TestSSLFactory));
			KeyStoreTestUtil.CleanupSSLConfig(KeystoresDir, sslConfsDir);
		}

		/// <exception cref="System.Exception"/>
		public virtual void ClientMode()
		{
			Configuration conf = CreateConfiguration(false, true);
			SSLFactory sslFactory = new SSLFactory(SSLFactory.Mode.Client, conf);
			try
			{
				sslFactory.Init();
				NUnit.Framework.Assert.IsNotNull(sslFactory.CreateSSLSocketFactory());
				NUnit.Framework.Assert.IsNotNull(sslFactory.GetHostnameVerifier());
				sslFactory.CreateSSLServerSocketFactory();
			}
			finally
			{
				sslFactory.Destroy();
			}
		}

		/// <exception cref="System.Exception"/>
		private void ServerMode(bool clientCert, bool socket)
		{
			Configuration conf = CreateConfiguration(clientCert, true);
			SSLFactory sslFactory = new SSLFactory(SSLFactory.Mode.Server, conf);
			try
			{
				sslFactory.Init();
				NUnit.Framework.Assert.IsNotNull(sslFactory.CreateSSLServerSocketFactory());
				Assert.Equal(clientCert, sslFactory.IsClientCertRequired());
				if (socket)
				{
					sslFactory.CreateSSLSocketFactory();
				}
				else
				{
					sslFactory.GetHostnameVerifier();
				}
			}
			finally
			{
				sslFactory.Destroy();
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void ServerModeWithoutClientCertsSocket()
		{
			ServerMode(false, true);
		}

		/// <exception cref="System.Exception"/>
		public virtual void ServerModeWithClientCertsSocket()
		{
			ServerMode(true, true);
		}

		/// <exception cref="System.Exception"/>
		public virtual void ServerModeWithoutClientCertsVerifier()
		{
			ServerMode(false, false);
		}

		/// <exception cref="System.Exception"/>
		public virtual void ServerModeWithClientCertsVerifier()
		{
			ServerMode(true, false);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void ValidHostnameVerifier()
		{
			Configuration conf = CreateConfiguration(false, true);
			conf.Unset(SSLFactory.SslHostnameVerifierKey);
			SSLFactory sslFactory = new SSLFactory(SSLFactory.Mode.Client, conf);
			sslFactory.Init();
			Assert.Equal("DEFAULT", sslFactory.GetHostnameVerifier().ToString
				());
			sslFactory.Destroy();
			conf.Set(SSLFactory.SslHostnameVerifierKey, "ALLOW_ALL");
			sslFactory = new SSLFactory(SSLFactory.Mode.Client, conf);
			sslFactory.Init();
			Assert.Equal("ALLOW_ALL", sslFactory.GetHostnameVerifier().ToString
				());
			sslFactory.Destroy();
			conf.Set(SSLFactory.SslHostnameVerifierKey, "DEFAULT_AND_LOCALHOST");
			sslFactory = new SSLFactory(SSLFactory.Mode.Client, conf);
			sslFactory.Init();
			Assert.Equal("DEFAULT_AND_LOCALHOST", sslFactory.GetHostnameVerifier
				().ToString());
			sslFactory.Destroy();
			conf.Set(SSLFactory.SslHostnameVerifierKey, "STRICT");
			sslFactory = new SSLFactory(SSLFactory.Mode.Client, conf);
			sslFactory.Init();
			Assert.Equal("STRICT", sslFactory.GetHostnameVerifier().ToString
				());
			sslFactory.Destroy();
			conf.Set(SSLFactory.SslHostnameVerifierKey, "STRICT_IE6");
			sslFactory = new SSLFactory(SSLFactory.Mode.Client, conf);
			sslFactory.Init();
			Assert.Equal("STRICT_IE6", sslFactory.GetHostnameVerifier().ToString
				());
			sslFactory.Destroy();
		}

		/// <exception cref="System.Exception"/>
		public virtual void InvalidHostnameVerifier()
		{
			Configuration conf = CreateConfiguration(false, true);
			conf.Set(SSLFactory.SslHostnameVerifierKey, "foo");
			SSLFactory sslFactory = new SSLFactory(SSLFactory.Mode.Client, conf);
			try
			{
				sslFactory.Init();
			}
			finally
			{
				sslFactory.Destroy();
			}
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestConnectionConfigurator()
		{
			Configuration conf = CreateConfiguration(false, true);
			conf.Set(SSLFactory.SslHostnameVerifierKey, "STRICT_IE6");
			SSLFactory sslFactory = new SSLFactory(SSLFactory.Mode.Client, conf);
			try
			{
				sslFactory.Init();
				HttpsURLConnection sslConn = (HttpsURLConnection)new Uri("https://foo").OpenConnection
					();
				NUnit.Framework.Assert.AreNotSame("STRICT_IE6", sslConn.GetHostnameVerifier().ToString
					());
				sslFactory.Configure(sslConn);
				Assert.Equal("STRICT_IE6", sslConn.GetHostnameVerifier().ToString
					());
			}
			finally
			{
				sslFactory.Destroy();
			}
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestServerDifferentPasswordAndKeyPassword()
		{
			CheckSSLFactoryInitWithPasswords(SSLFactory.Mode.Server, "password", "keyPassword"
				, "password", "keyPassword");
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestServerKeyPasswordDefaultsToPassword()
		{
			CheckSSLFactoryInitWithPasswords(SSLFactory.Mode.Server, "password", "password", 
				"password", null);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestClientDifferentPasswordAndKeyPassword()
		{
			CheckSSLFactoryInitWithPasswords(SSLFactory.Mode.Client, "password", "keyPassword"
				, "password", "keyPassword");
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestClientKeyPasswordDefaultsToPassword()
		{
			CheckSSLFactoryInitWithPasswords(SSLFactory.Mode.Client, "password", "password", 
				"password", null);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestServerCredProviderPasswords()
		{
			KeyStoreTestUtil.ProvisionPasswordsToCredentialProvider();
			CheckSSLFactoryInitWithPasswords(SSLFactory.Mode.Server, "storepass", "keypass", 
				null, null, true);
		}

		/// <summary>
		/// Checks that SSLFactory initialization is successful with the given
		/// arguments.
		/// </summary>
		/// <remarks>
		/// Checks that SSLFactory initialization is successful with the given
		/// arguments.  This is a helper method for writing test cases that cover
		/// different combinations of settings for the store password and key password.
		/// It takes care of bootstrapping a keystore, a truststore, and SSL client or
		/// server configuration.  Then, it initializes an SSLFactory.  If no exception
		/// is thrown, then initialization was successful.
		/// </remarks>
		/// <param name="mode">SSLFactory.Mode mode to test</param>
		/// <param name="password">String store password to set on keystore</param>
		/// <param name="keyPassword">String key password to set on keystore</param>
		/// <param name="confPassword">
		/// String store password to set in SSL config file, or null
		/// to avoid setting in SSL config file
		/// </param>
		/// <param name="confKeyPassword">
		/// String key password to set in SSL config file, or
		/// null to avoid setting in SSL config file
		/// </param>
		/// <exception cref="System.Exception">for any error</exception>
		private void CheckSSLFactoryInitWithPasswords(SSLFactory.Mode mode, string password
			, string keyPassword, string confPassword, string confKeyPassword)
		{
			CheckSSLFactoryInitWithPasswords(mode, password, keyPassword, confPassword, confKeyPassword
				, false);
		}

		/// <summary>
		/// Checks that SSLFactory initialization is successful with the given
		/// arguments.
		/// </summary>
		/// <remarks>
		/// Checks that SSLFactory initialization is successful with the given
		/// arguments.  This is a helper method for writing test cases that cover
		/// different combinations of settings for the store password and key password.
		/// It takes care of bootstrapping a keystore, a truststore, and SSL client or
		/// server configuration.  Then, it initializes an SSLFactory.  If no exception
		/// is thrown, then initialization was successful.
		/// </remarks>
		/// <param name="mode">SSLFactory.Mode mode to test</param>
		/// <param name="password">String store password to set on keystore</param>
		/// <param name="keyPassword">String key password to set on keystore</param>
		/// <param name="confPassword">
		/// String store password to set in SSL config file, or null
		/// to avoid setting in SSL config file
		/// </param>
		/// <param name="confKeyPassword">
		/// String key password to set in SSL config file, or
		/// null to avoid setting in SSL config file
		/// </param>
		/// <param name="useCredProvider">
		/// boolean to indicate whether passwords should be set
		/// into the config or not. When set to true nulls are set and aliases are
		/// expected to be resolved through credential provider API through the
		/// Configuration.getPassword method
		/// </param>
		/// <exception cref="System.Exception">for any error</exception>
		private void CheckSSLFactoryInitWithPasswords(SSLFactory.Mode mode, string password
			, string keyPassword, string confPassword, string confKeyPassword, bool useCredProvider
			)
		{
			string keystore = new FilePath(KeystoresDir, "keystore.jks").GetAbsolutePath();
			string truststore = new FilePath(KeystoresDir, "truststore.jks").GetAbsolutePath(
				);
			string trustPassword = "trustP";
			// Create keys, certs, keystore, and truststore.
			Sharpen.KeyPair keyPair = KeyStoreTestUtil.GenerateKeyPair("RSA");
			X509Certificate cert = KeyStoreTestUtil.GenerateCertificate("CN=Test", keyPair, 30
				, "SHA1withRSA");
			KeyStoreTestUtil.CreateKeyStore(keystore, password, keyPassword, "Test", keyPair.
				GetPrivate(), cert);
			IDictionary<string, X509Certificate> certs = Collections.SingletonMap("server", cert
				);
			KeyStoreTestUtil.CreateTrustStore(truststore, trustPassword, certs);
			// Create SSL configuration file, for either server or client.
			string sslConfFileName;
			Configuration sslConf;
			// if the passwords are provisioned in a cred provider then don't set them
			// in the configuration properly - expect them to be resolved through the
			// provider
			if (useCredProvider)
			{
				confPassword = null;
				confKeyPassword = null;
			}
			if (mode == SSLFactory.Mode.Server)
			{
				sslConfFileName = "ssl-server.xml";
				sslConf = KeyStoreTestUtil.CreateServerSSLConfig(keystore, confPassword, confKeyPassword
					, truststore);
				if (useCredProvider)
				{
					FilePath testDir = new FilePath(Runtime.GetProperty("test.build.data", "target/test-dir"
						));
					Path jksPath = new Path(testDir.ToString(), "test.jks");
					string ourUrl = JavaKeyStoreProvider.SchemeName + "://file" + jksPath.ToUri();
					sslConf.Set(CredentialProviderFactory.CredentialProviderPath, ourUrl);
				}
			}
			else
			{
				sslConfFileName = "ssl-client.xml";
				sslConf = KeyStoreTestUtil.CreateClientSSLConfig(keystore, confPassword, confKeyPassword
					, truststore);
			}
			KeyStoreTestUtil.SaveConfig(new FilePath(sslConfsDir, sslConfFileName), sslConf);
			// Create the master configuration for use by the SSLFactory, which by
			// default refers to the ssl-server.xml or ssl-client.xml created above.
			Configuration conf = new Configuration();
			conf.SetBoolean(SSLFactory.SslRequireClientCertKey, true);
			// Try initializing an SSLFactory.
			SSLFactory sslFactory = new SSLFactory(mode, conf);
			try
			{
				sslFactory.Init();
			}
			finally
			{
				sslFactory.Destroy();
			}
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestNoClientCertsInitialization()
		{
			Configuration conf = CreateConfiguration(false, true);
			conf.Unset(SSLFactory.SslRequireClientCertKey);
			SSLFactory sslFactory = new SSLFactory(SSLFactory.Mode.Client, conf);
			try
			{
				sslFactory.Init();
			}
			finally
			{
				sslFactory.Destroy();
			}
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestNoTrustStore()
		{
			Configuration conf = CreateConfiguration(false, false);
			conf.Unset(SSLFactory.SslRequireClientCertKey);
			SSLFactory sslFactory = new SSLFactory(SSLFactory.Mode.Server, conf);
			try
			{
				sslFactory.Init();
			}
			finally
			{
				sslFactory.Destroy();
			}
		}
	}
}
