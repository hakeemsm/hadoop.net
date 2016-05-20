using Sharpen;

namespace org.apache.hadoop.security.ssl
{
	/// <summary>
	/// <see cref="KeyStoresFactory"/>
	/// implementation that reads the certificates from
	/// keystore files.
	/// <p/>
	/// if the trust certificates keystore file changes, the
	/// <see cref="javax.net.ssl.TrustManager"/>
	/// is refreshed with the new trust certificate entries (using a
	/// <see cref="ReloadingX509TrustManager"/>
	/// trustmanager).
	/// </summary>
	public class FileBasedKeyStoresFactory : org.apache.hadoop.security.ssl.KeyStoresFactory
	{
		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.security.ssl.FileBasedKeyStoresFactory
			)));

		public const string SSL_KEYSTORE_LOCATION_TPL_KEY = "ssl.{0}.keystore.location";

		public const string SSL_KEYSTORE_PASSWORD_TPL_KEY = "ssl.{0}.keystore.password";

		public const string SSL_KEYSTORE_KEYPASSWORD_TPL_KEY = "ssl.{0}.keystore.keypassword";

		public const string SSL_KEYSTORE_TYPE_TPL_KEY = "ssl.{0}.keystore.type";

		public const string SSL_TRUSTSTORE_RELOAD_INTERVAL_TPL_KEY = "ssl.{0}.truststore.reload.interval";

		public const string SSL_TRUSTSTORE_LOCATION_TPL_KEY = "ssl.{0}.truststore.location";

		public const string SSL_TRUSTSTORE_PASSWORD_TPL_KEY = "ssl.{0}.truststore.password";

		public const string SSL_TRUSTSTORE_TYPE_TPL_KEY = "ssl.{0}.truststore.type";

		/// <summary>Default format of the keystore files.</summary>
		public const string DEFAULT_KEYSTORE_TYPE = "jks";

		/// <summary>Reload interval in milliseconds.</summary>
		public const int DEFAULT_SSL_TRUSTSTORE_RELOAD_INTERVAL = 10000;

		private org.apache.hadoop.conf.Configuration conf;

		private javax.net.ssl.KeyManager[] keyManagers;

		private javax.net.ssl.TrustManager[] trustManagers;

		private org.apache.hadoop.security.ssl.ReloadingX509TrustManager trustManager;

		/// <summary>Resolves a property name to its client/server version if applicable.</summary>
		/// <remarks>
		/// Resolves a property name to its client/server version if applicable.
		/// <p/>
		/// NOTE: This method is public for testing purposes.
		/// </remarks>
		/// <param name="mode">client/server mode.</param>
		/// <param name="template">property name template.</param>
		/// <returns>the resolved property name.</returns>
		[com.google.common.annotations.VisibleForTesting]
		public static string resolvePropertyName(org.apache.hadoop.security.ssl.SSLFactory.Mode
			 mode, string template)
		{
			return java.text.MessageFormat.format(template, org.apache.hadoop.util.StringUtils
				.toLowerCase(mode.ToString()));
		}

		/// <summary>Sets the configuration for the factory.</summary>
		/// <param name="conf">the configuration for the factory.</param>
		public virtual void setConf(org.apache.hadoop.conf.Configuration conf)
		{
			this.conf = conf;
		}

		/// <summary>Returns the configuration of the factory.</summary>
		/// <returns>the configuration of the factory.</returns>
		public virtual org.apache.hadoop.conf.Configuration getConf()
		{
			return conf;
		}

		/// <summary>Initializes the keystores of the factory.</summary>
		/// <param name="mode">if the keystores are to be used in client or server mode.</param>
		/// <exception cref="System.IO.IOException">
		/// thrown if the keystores could not be initialized due
		/// to an IO error.
		/// </exception>
		/// <exception cref="java.security.GeneralSecurityException">
		/// thrown if the keystores could not be
		/// initialized due to a security error.
		/// </exception>
		public virtual void init(org.apache.hadoop.security.ssl.SSLFactory.Mode mode)
		{
			bool requireClientCert = conf.getBoolean(org.apache.hadoop.security.ssl.SSLFactory
				.SSL_REQUIRE_CLIENT_CERT_KEY, org.apache.hadoop.security.ssl.SSLFactory.DEFAULT_SSL_REQUIRE_CLIENT_CERT
				);
			// certificate store
			string keystoreType = conf.get(resolvePropertyName(mode, SSL_KEYSTORE_TYPE_TPL_KEY
				), DEFAULT_KEYSTORE_TYPE);
			java.security.KeyStore keystore = java.security.KeyStore.getInstance(keystoreType
				);
			string keystoreKeyPassword = null;
			if (requireClientCert || mode == org.apache.hadoop.security.ssl.SSLFactory.Mode.SERVER)
			{
				string locationProperty = resolvePropertyName(mode, SSL_KEYSTORE_LOCATION_TPL_KEY
					);
				string keystoreLocation = conf.get(locationProperty, string.Empty);
				if (keystoreLocation.isEmpty())
				{
					throw new java.security.GeneralSecurityException("The property '" + locationProperty
						 + "' has not been set in the ssl configuration file.");
				}
				string passwordProperty = resolvePropertyName(mode, SSL_KEYSTORE_PASSWORD_TPL_KEY
					);
				string keystorePassword = getPassword(conf, passwordProperty, string.Empty);
				if (keystorePassword.isEmpty())
				{
					throw new java.security.GeneralSecurityException("The property '" + passwordProperty
						 + "' has not been set in the ssl configuration file.");
				}
				string keyPasswordProperty = resolvePropertyName(mode, SSL_KEYSTORE_KEYPASSWORD_TPL_KEY
					);
				// Key password defaults to the same value as store password for
				// compatibility with legacy configurations that did not use a separate
				// configuration property for key password.
				keystoreKeyPassword = getPassword(conf, keyPasswordProperty, keystorePassword);
				LOG.debug(mode.ToString() + " KeyStore: " + keystoreLocation);
				java.io.InputStream @is = new java.io.FileInputStream(keystoreLocation);
				try
				{
					keystore.load(@is, keystorePassword.ToCharArray());
				}
				finally
				{
					@is.close();
				}
				LOG.debug(mode.ToString() + " Loaded KeyStore: " + keystoreLocation);
			}
			else
			{
				keystore.load(null, null);
			}
			javax.net.ssl.KeyManagerFactory keyMgrFactory = javax.net.ssl.KeyManagerFactory.getInstance
				(org.apache.hadoop.security.ssl.SSLFactory.SSLCERTIFICATE);
			keyMgrFactory.init(keystore, (keystoreKeyPassword != null) ? keystoreKeyPassword.
				ToCharArray() : null);
			keyManagers = keyMgrFactory.getKeyManagers();
			//trust store
			string truststoreType = conf.get(resolvePropertyName(mode, SSL_TRUSTSTORE_TYPE_TPL_KEY
				), DEFAULT_KEYSTORE_TYPE);
			string locationProperty_1 = resolvePropertyName(mode, SSL_TRUSTSTORE_LOCATION_TPL_KEY
				);
			string truststoreLocation = conf.get(locationProperty_1, string.Empty);
			if (!truststoreLocation.isEmpty())
			{
				string passwordProperty = resolvePropertyName(mode, SSL_TRUSTSTORE_PASSWORD_TPL_KEY
					);
				string truststorePassword = getPassword(conf, passwordProperty, string.Empty);
				if (truststorePassword.isEmpty())
				{
					throw new java.security.GeneralSecurityException("The property '" + passwordProperty
						 + "' has not been set in the ssl configuration file.");
				}
				long truststoreReloadInterval = conf.getLong(resolvePropertyName(mode, SSL_TRUSTSTORE_RELOAD_INTERVAL_TPL_KEY
					), DEFAULT_SSL_TRUSTSTORE_RELOAD_INTERVAL);
				LOG.debug(mode.ToString() + " TrustStore: " + truststoreLocation);
				trustManager = new org.apache.hadoop.security.ssl.ReloadingX509TrustManager(truststoreType
					, truststoreLocation, truststorePassword, truststoreReloadInterval);
				trustManager.init();
				LOG.debug(mode.ToString() + " Loaded TrustStore: " + truststoreLocation);
				trustManagers = new javax.net.ssl.TrustManager[] { trustManager };
			}
			else
			{
				LOG.debug("The property '" + locationProperty_1 + "' has not been set, " + "no TrustStore will be loaded"
					);
				trustManagers = null;
			}
		}

		internal virtual string getPassword(org.apache.hadoop.conf.Configuration conf, string
			 alias, string defaultPass)
		{
			string password = defaultPass;
			try
			{
				char[] passchars = conf.getPassword(alias);
				if (passchars != null)
				{
					password = new string(passchars);
				}
			}
			catch (System.IO.IOException ioe)
			{
				LOG.warn("Exception while trying to get password for alias " + alias + ": " + ioe
					.Message);
			}
			return password;
		}

		/// <summary>Releases any resources being used.</summary>
		public virtual void destroy()
		{
			lock (this)
			{
				if (trustManager != null)
				{
					trustManager.destroy();
					trustManager = null;
					keyManagers = null;
					trustManagers = null;
				}
			}
		}

		/// <summary>Returns the keymanagers for owned certificates.</summary>
		/// <returns>the keymanagers for owned certificates.</returns>
		public virtual javax.net.ssl.KeyManager[] getKeyManagers()
		{
			return keyManagers;
		}

		/// <summary>Returns the trustmanagers for trusted certificates.</summary>
		/// <returns>the trustmanagers for trusted certificates.</returns>
		public virtual javax.net.ssl.TrustManager[] getTrustManagers()
		{
			return trustManagers;
		}
	}
}
