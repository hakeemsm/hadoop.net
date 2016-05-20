using Sharpen;

namespace org.apache.hadoop.security.ssl
{
	/// <summary>
	/// Factory that creates SSLEngine and SSLSocketFactory instances using
	/// Hadoop configuration information.
	/// </summary>
	/// <remarks>
	/// Factory that creates SSLEngine and SSLSocketFactory instances using
	/// Hadoop configuration information.
	/// <p/>
	/// This SSLFactory uses a
	/// <see cref="ReloadingX509TrustManager"/>
	/// instance,
	/// which reloads public keys if the truststore file changes.
	/// <p/>
	/// This factory is used to configure HTTPS in Hadoop HTTP based endpoints, both
	/// client and server.
	/// </remarks>
	public class SSLFactory : org.apache.hadoop.security.authentication.client.ConnectionConfigurator
	{
		public enum Mode
		{
			CLIENT,
			SERVER
		}

		public const string SSL_REQUIRE_CLIENT_CERT_KEY = "hadoop.ssl.require.client.cert";

		public const string SSL_HOSTNAME_VERIFIER_KEY = "hadoop.ssl.hostname.verifier";

		public const string SSL_CLIENT_CONF_KEY = "hadoop.ssl.client.conf";

		public const string SSL_SERVER_CONF_KEY = "hadoop.ssl.server.conf";

		public static readonly string SSLCERTIFICATE = org.apache.hadoop.util.PlatformName
			.IBM_JAVA ? "ibmX509" : "SunX509";

		public const bool DEFAULT_SSL_REQUIRE_CLIENT_CERT = false;

		public const string KEYSTORES_FACTORY_CLASS_KEY = "hadoop.ssl.keystores.factory.class";

		public const string SSL_ENABLED_PROTOCOLS = "hadoop.ssl.enabled.protocols";

		public const string DEFAULT_SSL_ENABLED_PROTOCOLS = "TLSv1";

		private org.apache.hadoop.conf.Configuration conf;

		private org.apache.hadoop.security.ssl.SSLFactory.Mode mode;

		private bool requireClientCert;

		private javax.net.ssl.SSLContext context;

		private javax.net.ssl.HostnameVerifier hostnameVerifier;

		private org.apache.hadoop.security.ssl.KeyStoresFactory keystoresFactory;

		private string[] enabledProtocols = null;

		/// <summary>Creates an SSLFactory.</summary>
		/// <param name="mode">SSLFactory mode, client or server.</param>
		/// <param name="conf">
		/// Hadoop configuration from where the SSLFactory configuration
		/// will be read.
		/// </param>
		public SSLFactory(org.apache.hadoop.security.ssl.SSLFactory.Mode mode, org.apache.hadoop.conf.Configuration
			 conf)
		{
			this.conf = conf;
			if (mode == null)
			{
				throw new System.ArgumentException("mode cannot be NULL");
			}
			this.mode = mode;
			requireClientCert = conf.getBoolean(SSL_REQUIRE_CLIENT_CERT_KEY, DEFAULT_SSL_REQUIRE_CLIENT_CERT
				);
			org.apache.hadoop.conf.Configuration sslConf = readSSLConfiguration(mode);
			java.lang.Class klass = conf.getClass<org.apache.hadoop.security.ssl.KeyStoresFactory
				>(KEYSTORES_FACTORY_CLASS_KEY, Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.security.ssl.FileBasedKeyStoresFactory
				)));
			keystoresFactory = org.apache.hadoop.util.ReflectionUtils.newInstance(klass, sslConf
				);
			enabledProtocols = conf.getStrings(SSL_ENABLED_PROTOCOLS, DEFAULT_SSL_ENABLED_PROTOCOLS
				);
		}

		private org.apache.hadoop.conf.Configuration readSSLConfiguration(org.apache.hadoop.security.ssl.SSLFactory.Mode
			 mode)
		{
			org.apache.hadoop.conf.Configuration sslConf = new org.apache.hadoop.conf.Configuration
				(false);
			sslConf.setBoolean(SSL_REQUIRE_CLIENT_CERT_KEY, requireClientCert);
			string sslConfResource;
			if (mode == org.apache.hadoop.security.ssl.SSLFactory.Mode.CLIENT)
			{
				sslConfResource = conf.get(SSL_CLIENT_CONF_KEY, "ssl-client.xml");
			}
			else
			{
				sslConfResource = conf.get(SSL_SERVER_CONF_KEY, "ssl-server.xml");
			}
			sslConf.addResource(sslConfResource);
			return sslConf;
		}

		/// <summary>Initializes the factory.</summary>
		/// <exception cref="java.security.GeneralSecurityException">
		/// thrown if an SSL initialization error
		/// happened.
		/// </exception>
		/// <exception cref="System.IO.IOException">
		/// thrown if an IO error happened while reading the SSL
		/// configuration.
		/// </exception>
		public virtual void init()
		{
			keystoresFactory.init(mode);
			context = javax.net.ssl.SSLContext.getInstance("TLS");
			context.init(keystoresFactory.getKeyManagers(), keystoresFactory.getTrustManagers
				(), null);
			context.getDefaultSSLParameters().setProtocols(enabledProtocols);
			hostnameVerifier = getHostnameVerifier(conf);
		}

		/// <exception cref="java.security.GeneralSecurityException"/>
		/// <exception cref="System.IO.IOException"/>
		private javax.net.ssl.HostnameVerifier getHostnameVerifier(org.apache.hadoop.conf.Configuration
			 conf)
		{
			return getHostnameVerifier(org.apache.hadoop.util.StringUtils.toUpperCase(conf.get
				(SSL_HOSTNAME_VERIFIER_KEY, "DEFAULT").Trim()));
		}

		/// <exception cref="java.security.GeneralSecurityException"/>
		/// <exception cref="System.IO.IOException"/>
		public static javax.net.ssl.HostnameVerifier getHostnameVerifier(string verifier)
		{
			javax.net.ssl.HostnameVerifier hostnameVerifier;
			if (verifier.Equals("DEFAULT"))
			{
				hostnameVerifier = org.apache.hadoop.security.ssl.SSLHostnameVerifier.DEFAULT;
			}
			else
			{
				if (verifier.Equals("DEFAULT_AND_LOCALHOST"))
				{
					hostnameVerifier = org.apache.hadoop.security.ssl.SSLHostnameVerifier.DEFAULT_AND_LOCALHOST;
				}
				else
				{
					if (verifier.Equals("STRICT"))
					{
						hostnameVerifier = org.apache.hadoop.security.ssl.SSLHostnameVerifier.STRICT;
					}
					else
					{
						if (verifier.Equals("STRICT_IE6"))
						{
							hostnameVerifier = org.apache.hadoop.security.ssl.SSLHostnameVerifier.STRICT_IE6;
						}
						else
						{
							if (verifier.Equals("ALLOW_ALL"))
							{
								hostnameVerifier = org.apache.hadoop.security.ssl.SSLHostnameVerifier.ALLOW_ALL;
							}
							else
							{
								throw new java.security.GeneralSecurityException("Invalid hostname verifier: " + 
									verifier);
							}
						}
					}
				}
			}
			return hostnameVerifier;
		}

		/// <summary>Releases any resources being used.</summary>
		public virtual void destroy()
		{
			keystoresFactory.destroy();
		}

		/// <summary>Returns the SSLFactory KeyStoresFactory instance.</summary>
		/// <returns>the SSLFactory KeyStoresFactory instance.</returns>
		public virtual org.apache.hadoop.security.ssl.KeyStoresFactory getKeystoresFactory
			()
		{
			return keystoresFactory;
		}

		/// <summary>Returns a configured SSLEngine.</summary>
		/// <returns>the configured SSLEngine.</returns>
		/// <exception cref="java.security.GeneralSecurityException">
		/// thrown if the SSL engine could not
		/// be initialized.
		/// </exception>
		/// <exception cref="System.IO.IOException">
		/// thrown if and IO error occurred while loading
		/// the server keystore.
		/// </exception>
		public virtual javax.net.ssl.SSLEngine createSSLEngine()
		{
			javax.net.ssl.SSLEngine sslEngine = context.createSSLEngine();
			if (mode == org.apache.hadoop.security.ssl.SSLFactory.Mode.CLIENT)
			{
				sslEngine.setUseClientMode(true);
			}
			else
			{
				sslEngine.setUseClientMode(false);
				sslEngine.setNeedClientAuth(requireClientCert);
			}
			sslEngine.setEnabledProtocols(enabledProtocols);
			return sslEngine;
		}

		/// <summary>Returns a configured SSLServerSocketFactory.</summary>
		/// <returns>the configured SSLSocketFactory.</returns>
		/// <exception cref="java.security.GeneralSecurityException">
		/// thrown if the SSLSocketFactory could not
		/// be initialized.
		/// </exception>
		/// <exception cref="System.IO.IOException">
		/// thrown if and IO error occurred while loading
		/// the server keystore.
		/// </exception>
		public virtual javax.net.ssl.SSLServerSocketFactory createSSLServerSocketFactory(
			)
		{
			if (mode != org.apache.hadoop.security.ssl.SSLFactory.Mode.SERVER)
			{
				throw new System.InvalidOperationException("Factory is in CLIENT mode");
			}
			return context.getServerSocketFactory();
		}

		/// <summary>Returns a configured SSLSocketFactory.</summary>
		/// <returns>the configured SSLSocketFactory.</returns>
		/// <exception cref="java.security.GeneralSecurityException">
		/// thrown if the SSLSocketFactory could not
		/// be initialized.
		/// </exception>
		/// <exception cref="System.IO.IOException">
		/// thrown if and IO error occurred while loading
		/// the server keystore.
		/// </exception>
		public virtual javax.net.ssl.SSLSocketFactory createSSLSocketFactory()
		{
			if (mode != org.apache.hadoop.security.ssl.SSLFactory.Mode.CLIENT)
			{
				throw new System.InvalidOperationException("Factory is in CLIENT mode");
			}
			return context.getSocketFactory();
		}

		/// <summary>Returns the hostname verifier it should be used in HttpsURLConnections.</summary>
		/// <returns>the hostname verifier.</returns>
		public virtual javax.net.ssl.HostnameVerifier getHostnameVerifier()
		{
			if (mode != org.apache.hadoop.security.ssl.SSLFactory.Mode.CLIENT)
			{
				throw new System.InvalidOperationException("Factory is in CLIENT mode");
			}
			return hostnameVerifier;
		}

		/// <summary>Returns if client certificates are required or not.</summary>
		/// <returns>if client certificates are required or not.</returns>
		public virtual bool isClientCertRequired()
		{
			return requireClientCert;
		}

		/// <summary>
		/// If the given
		/// <see cref="java.net.HttpURLConnection"/>
		/// is an
		/// <see cref="javax.net.ssl.HttpsURLConnection"/>
		/// configures the connection with the
		/// <see cref="javax.net.ssl.SSLSocketFactory"/>
		/// and
		/// <see cref="javax.net.ssl.HostnameVerifier"/>
		/// of this SSLFactory, otherwise does nothing.
		/// </summary>
		/// <param name="conn">
		/// the
		/// <see cref="java.net.HttpURLConnection"/>
		/// instance to configure.
		/// </param>
		/// <returns>
		/// the configured
		/// <see cref="java.net.HttpURLConnection"/>
		/// instance.
		/// </returns>
		/// <exception cref="System.IO.IOException">if an IO error occurred.</exception>
		public virtual java.net.HttpURLConnection configure(java.net.HttpURLConnection conn
			)
		{
			if (conn is javax.net.ssl.HttpsURLConnection)
			{
				javax.net.ssl.HttpsURLConnection sslConn = (javax.net.ssl.HttpsURLConnection)conn;
				try
				{
					sslConn.setSSLSocketFactory(createSSLSocketFactory());
				}
				catch (java.security.GeneralSecurityException ex)
				{
					throw new System.IO.IOException(ex);
				}
				sslConn.setHostnameVerifier(getHostnameVerifier());
				conn = sslConn;
			}
			return conn;
		}
	}
}
