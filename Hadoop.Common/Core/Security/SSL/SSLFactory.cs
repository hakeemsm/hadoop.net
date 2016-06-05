using System;
using System.IO;
using Hadoop.Common.Core.Conf;
using Hadoop.Common.Core.Util;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Security.Authentication.Client;
using Org.Apache.Hadoop.Util;


namespace Org.Apache.Hadoop.Security.Ssl
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
	public class SSLFactory : ConnectionConfigurator
	{
		public enum Mode
		{
			Client,
			Server
		}

		public const string SslRequireClientCertKey = "hadoop.ssl.require.client.cert";

		public const string SslHostnameVerifierKey = "hadoop.ssl.hostname.verifier";

		public const string SslClientConfKey = "hadoop.ssl.client.conf";

		public const string SslServerConfKey = "hadoop.ssl.server.conf";

		public static readonly string Sslcertificate = PlatformName.IbmJava ? "ibmX509" : 
			"SunX509";

		public const bool DefaultSslRequireClientCert = false;

		public const string KeystoresFactoryClassKey = "hadoop.ssl.keystores.factory.class";

		public const string SslEnabledProtocols = "hadoop.ssl.enabled.protocols";

		public const string DefaultSslEnabledProtocols = "TLSv1";

		private Configuration conf;

		private SSLFactory.Mode mode;

		private bool requireClientCert;

		private SSLContext context;

		private HostnameVerifier hostnameVerifier;

		private KeyStoresFactory keystoresFactory;

		private string[] enabledProtocols = null;

		/// <summary>Creates an SSLFactory.</summary>
		/// <param name="mode">SSLFactory mode, client or server.</param>
		/// <param name="conf">
		/// Hadoop configuration from where the SSLFactory configuration
		/// will be read.
		/// </param>
		public SSLFactory(SSLFactory.Mode mode, Configuration conf)
		{
			this.conf = conf;
			if (mode == null)
			{
				throw new ArgumentException("mode cannot be NULL");
			}
			this.mode = mode;
			requireClientCert = conf.GetBoolean(SslRequireClientCertKey, DefaultSslRequireClientCert
				);
			Configuration sslConf = ReadSSLConfiguration(mode);
			Type klass = conf.GetClass<KeyStoresFactory>(KeystoresFactoryClassKey, typeof(FileBasedKeyStoresFactory
				));
			keystoresFactory = ReflectionUtils.NewInstance(klass, sslConf);
			enabledProtocols = conf.GetStrings(SslEnabledProtocols, DefaultSslEnabledProtocols
				);
		}

		private Configuration ReadSSLConfiguration(SSLFactory.Mode mode)
		{
			Configuration sslConf = new Configuration(false);
			sslConf.SetBoolean(SslRequireClientCertKey, requireClientCert);
			string sslConfResource;
			if (mode == SSLFactory.Mode.Client)
			{
				sslConfResource = conf.Get(SslClientConfKey, "ssl-client.xml");
			}
			else
			{
				sslConfResource = conf.Get(SslServerConfKey, "ssl-server.xml");
			}
			sslConf.AddResource(sslConfResource);
			return sslConf;
		}

		/// <summary>Initializes the factory.</summary>
		/// <exception cref="GeneralSecurityException">
		/// thrown if an SSL initialization error
		/// happened.
		/// </exception>
		/// <exception cref="System.IO.IOException">
		/// thrown if an IO error happened while reading the SSL
		/// configuration.
		/// </exception>
		public virtual void Init()
		{
			keystoresFactory.Init(mode);
			context = SSLContext.GetInstance("TLS");
			context.Init(keystoresFactory.GetKeyManagers(), keystoresFactory.GetTrustManagers
				(), null);
			context.GetDefaultSSLParameters().SetProtocols(enabledProtocols);
			hostnameVerifier = GetHostnameVerifier(conf);
		}

		/// <exception cref="GeneralSecurityException"/>
		/// <exception cref="System.IO.IOException"/>
		private HostnameVerifier GetHostnameVerifier(Configuration conf)
		{
			return GetHostnameVerifier(StringUtils.ToUpperCase(conf.Get(SslHostnameVerifierKey
				, "DEFAULT").Trim()));
		}

		/// <exception cref="GeneralSecurityException"/>
		/// <exception cref="System.IO.IOException"/>
		public static HostnameVerifier GetHostnameVerifier(string verifier)
		{
			HostnameVerifier hostnameVerifier;
			if (verifier.Equals("DEFAULT"))
			{
				hostnameVerifier = SSLHostnameVerifier.Default;
			}
			else
			{
				if (verifier.Equals("DEFAULT_AND_LOCALHOST"))
				{
					hostnameVerifier = SSLHostnameVerifier.DefaultAndLocalhost;
				}
				else
				{
					if (verifier.Equals("STRICT"))
					{
						hostnameVerifier = SSLHostnameVerifier.Strict;
					}
					else
					{
						if (verifier.Equals("STRICT_IE6"))
						{
							hostnameVerifier = SSLHostnameVerifier.StrictIe6;
						}
						else
						{
							if (verifier.Equals("ALLOW_ALL"))
							{
								hostnameVerifier = SSLHostnameVerifier.AllowAll;
							}
							else
							{
								throw new GeneralSecurityException("Invalid hostname verifier: " + verifier);
							}
						}
					}
				}
			}
			return hostnameVerifier;
		}

		/// <summary>Releases any resources being used.</summary>
		public virtual void Destroy()
		{
			keystoresFactory.Destroy();
		}

		/// <summary>Returns the SSLFactory KeyStoresFactory instance.</summary>
		/// <returns>the SSLFactory KeyStoresFactory instance.</returns>
		public virtual KeyStoresFactory GetKeystoresFactory()
		{
			return keystoresFactory;
		}

		/// <summary>Returns a configured SSLEngine.</summary>
		/// <returns>the configured SSLEngine.</returns>
		/// <exception cref="GeneralSecurityException">
		/// thrown if the SSL engine could not
		/// be initialized.
		/// </exception>
		/// <exception cref="System.IO.IOException">
		/// thrown if and IO error occurred while loading
		/// the server keystore.
		/// </exception>
		public virtual SSLEngine CreateSSLEngine()
		{
			SSLEngine sslEngine = context.CreateSSLEngine();
			if (mode == SSLFactory.Mode.Client)
			{
				sslEngine.SetUseClientMode(true);
			}
			else
			{
				sslEngine.SetUseClientMode(false);
				sslEngine.SetNeedClientAuth(requireClientCert);
			}
			sslEngine.SetEnabledProtocols(enabledProtocols);
			return sslEngine;
		}

		/// <summary>Returns a configured SSLServerSocketFactory.</summary>
		/// <returns>the configured SSLSocketFactory.</returns>
		/// <exception cref="GeneralSecurityException">
		/// thrown if the SSLSocketFactory could not
		/// be initialized.
		/// </exception>
		/// <exception cref="System.IO.IOException">
		/// thrown if and IO error occurred while loading
		/// the server keystore.
		/// </exception>
		public virtual SSLServerSocketFactory CreateSSLServerSocketFactory()
		{
			if (mode != SSLFactory.Mode.Server)
			{
				throw new InvalidOperationException("Factory is in CLIENT mode");
			}
			return context.GetServerSocketFactory();
		}

		/// <summary>Returns a configured SSLSocketFactory.</summary>
		/// <returns>the configured SSLSocketFactory.</returns>
		/// <exception cref="GeneralSecurityException">
		/// thrown if the SSLSocketFactory could not
		/// be initialized.
		/// </exception>
		/// <exception cref="System.IO.IOException">
		/// thrown if and IO error occurred while loading
		/// the server keystore.
		/// </exception>
		public virtual SSLSocketFactory CreateSSLSocketFactory()
		{
			if (mode != SSLFactory.Mode.Client)
			{
				throw new InvalidOperationException("Factory is in CLIENT mode");
			}
			return context.GetSocketFactory();
		}

		/// <summary>Returns the hostname verifier it should be used in HttpsURLConnections.</summary>
		/// <returns>the hostname verifier.</returns>
		public virtual HostnameVerifier GetHostnameVerifier()
		{
			if (mode != SSLFactory.Mode.Client)
			{
				throw new InvalidOperationException("Factory is in CLIENT mode");
			}
			return hostnameVerifier;
		}

		/// <summary>Returns if client certificates are required or not.</summary>
		/// <returns>if client certificates are required or not.</returns>
		public virtual bool IsClientCertRequired()
		{
			return requireClientCert;
		}

		/// <summary>
		/// If the given
		/// <see cref="HttpURLConnection"/>
		/// is an
		/// <see cref="HttpsURLConnection"/>
		/// configures the connection with the
		/// <see cref="SSLSocketFactory"/>
		/// and
		/// <see cref="HostnameVerifier"/>
		/// of this SSLFactory, otherwise does nothing.
		/// </summary>
		/// <param name="conn">
		/// the
		/// <see cref="HttpURLConnection"/>
		/// instance to configure.
		/// </param>
		/// <returns>
		/// the configured
		/// <see cref="HttpURLConnection"/>
		/// instance.
		/// </returns>
		/// <exception cref="System.IO.IOException">if an IO error occurred.</exception>
		public virtual HttpURLConnection Configure(HttpURLConnection conn)
		{
			if (conn is HttpsURLConnection)
			{
				HttpsURLConnection sslConn = (HttpsURLConnection)conn;
				try
				{
					sslConn.SetSSLSocketFactory(CreateSSLSocketFactory());
				}
				catch (GeneralSecurityException ex)
				{
					throw new IOException(ex);
				}
				sslConn.SetHostnameVerifier(GetHostnameVerifier());
				conn = sslConn;
			}
			return conn;
		}
	}
}
