using System.IO;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Security.Ssl
{
	/// <summary>
	/// <see cref="KeyStoresFactory"/>
	/// implementation that reads the certificates from
	/// keystore files.
	/// <p/>
	/// if the trust certificates keystore file changes, the
	/// <see cref="Sharpen.TrustManager"/>
	/// is refreshed with the new trust certificate entries (using a
	/// <see cref="ReloadingX509TrustManager"/>
	/// trustmanager).
	/// </summary>
	public class FileBasedKeyStoresFactory : KeyStoresFactory
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(FileBasedKeyStoresFactory
			));

		public const string SslKeystoreLocationTplKey = "ssl.{0}.keystore.location";

		public const string SslKeystorePasswordTplKey = "ssl.{0}.keystore.password";

		public const string SslKeystoreKeypasswordTplKey = "ssl.{0}.keystore.keypassword";

		public const string SslKeystoreTypeTplKey = "ssl.{0}.keystore.type";

		public const string SslTruststoreReloadIntervalTplKey = "ssl.{0}.truststore.reload.interval";

		public const string SslTruststoreLocationTplKey = "ssl.{0}.truststore.location";

		public const string SslTruststorePasswordTplKey = "ssl.{0}.truststore.password";

		public const string SslTruststoreTypeTplKey = "ssl.{0}.truststore.type";

		/// <summary>Default format of the keystore files.</summary>
		public const string DefaultKeystoreType = "jks";

		/// <summary>Reload interval in milliseconds.</summary>
		public const int DefaultSslTruststoreReloadInterval = 10000;

		private Configuration conf;

		private KeyManager[] keyManagers;

		private TrustManager[] trustManagers;

		private ReloadingX509TrustManager trustManager;

		/// <summary>Resolves a property name to its client/server version if applicable.</summary>
		/// <remarks>
		/// Resolves a property name to its client/server version if applicable.
		/// <p/>
		/// NOTE: This method is public for testing purposes.
		/// </remarks>
		/// <param name="mode">client/server mode.</param>
		/// <param name="template">property name template.</param>
		/// <returns>the resolved property name.</returns>
		[VisibleForTesting]
		public static string ResolvePropertyName(SSLFactory.Mode mode, string template)
		{
			return MessageFormat.Format(template, StringUtils.ToLowerCase(mode.ToString()));
		}

		/// <summary>Sets the configuration for the factory.</summary>
		/// <param name="conf">the configuration for the factory.</param>
		public virtual void SetConf(Configuration conf)
		{
			this.conf = conf;
		}

		/// <summary>Returns the configuration of the factory.</summary>
		/// <returns>the configuration of the factory.</returns>
		public virtual Configuration GetConf()
		{
			return conf;
		}

		/// <summary>Initializes the keystores of the factory.</summary>
		/// <param name="mode">if the keystores are to be used in client or server mode.</param>
		/// <exception cref="System.IO.IOException">
		/// thrown if the keystores could not be initialized due
		/// to an IO error.
		/// </exception>
		/// <exception cref="Sharpen.GeneralSecurityException">
		/// thrown if the keystores could not be
		/// initialized due to a security error.
		/// </exception>
		public virtual void Init(SSLFactory.Mode mode)
		{
			bool requireClientCert = conf.GetBoolean(SSLFactory.SslRequireClientCertKey, SSLFactory
				.DefaultSslRequireClientCert);
			// certificate store
			string keystoreType = conf.Get(ResolvePropertyName(mode, SslKeystoreTypeTplKey), 
				DefaultKeystoreType);
			KeyStore keystore = KeyStore.GetInstance(keystoreType);
			string keystoreKeyPassword = null;
			if (requireClientCert || mode == SSLFactory.Mode.Server)
			{
				string locationProperty = ResolvePropertyName(mode, SslKeystoreLocationTplKey);
				string keystoreLocation = conf.Get(locationProperty, string.Empty);
				if (keystoreLocation.IsEmpty())
				{
					throw new GeneralSecurityException("The property '" + locationProperty + "' has not been set in the ssl configuration file."
						);
				}
				string passwordProperty = ResolvePropertyName(mode, SslKeystorePasswordTplKey);
				string keystorePassword = GetPassword(conf, passwordProperty, string.Empty);
				if (keystorePassword.IsEmpty())
				{
					throw new GeneralSecurityException("The property '" + passwordProperty + "' has not been set in the ssl configuration file."
						);
				}
				string keyPasswordProperty = ResolvePropertyName(mode, SslKeystoreKeypasswordTplKey
					);
				// Key password defaults to the same value as store password for
				// compatibility with legacy configurations that did not use a separate
				// configuration property for key password.
				keystoreKeyPassword = GetPassword(conf, keyPasswordProperty, keystorePassword);
				Log.Debug(mode.ToString() + " KeyStore: " + keystoreLocation);
				InputStream @is = new FileInputStream(keystoreLocation);
				try
				{
					keystore.Load(@is, keystorePassword.ToCharArray());
				}
				finally
				{
					@is.Close();
				}
				Log.Debug(mode.ToString() + " Loaded KeyStore: " + keystoreLocation);
			}
			else
			{
				keystore.Load(null, null);
			}
			KeyManagerFactory keyMgrFactory = KeyManagerFactory.GetInstance(SSLFactory.Sslcertificate
				);
			keyMgrFactory.Init(keystore, (keystoreKeyPassword != null) ? keystoreKeyPassword.
				ToCharArray() : null);
			keyManagers = keyMgrFactory.GetKeyManagers();
			//trust store
			string truststoreType = conf.Get(ResolvePropertyName(mode, SslTruststoreTypeTplKey
				), DefaultKeystoreType);
			string locationProperty_1 = ResolvePropertyName(mode, SslTruststoreLocationTplKey
				);
			string truststoreLocation = conf.Get(locationProperty_1, string.Empty);
			if (!truststoreLocation.IsEmpty())
			{
				string passwordProperty = ResolvePropertyName(mode, SslTruststorePasswordTplKey);
				string truststorePassword = GetPassword(conf, passwordProperty, string.Empty);
				if (truststorePassword.IsEmpty())
				{
					throw new GeneralSecurityException("The property '" + passwordProperty + "' has not been set in the ssl configuration file."
						);
				}
				long truststoreReloadInterval = conf.GetLong(ResolvePropertyName(mode, SslTruststoreReloadIntervalTplKey
					), DefaultSslTruststoreReloadInterval);
				Log.Debug(mode.ToString() + " TrustStore: " + truststoreLocation);
				trustManager = new ReloadingX509TrustManager(truststoreType, truststoreLocation, 
					truststorePassword, truststoreReloadInterval);
				trustManager.Init();
				Log.Debug(mode.ToString() + " Loaded TrustStore: " + truststoreLocation);
				trustManagers = new TrustManager[] { trustManager };
			}
			else
			{
				Log.Debug("The property '" + locationProperty_1 + "' has not been set, " + "no TrustStore will be loaded"
					);
				trustManagers = null;
			}
		}

		internal virtual string GetPassword(Configuration conf, string alias, string defaultPass
			)
		{
			string password = defaultPass;
			try
			{
				char[] passchars = conf.GetPassword(alias);
				if (passchars != null)
				{
					password = new string(passchars);
				}
			}
			catch (IOException ioe)
			{
				Log.Warn("Exception while trying to get password for alias " + alias + ": " + ioe
					.Message);
			}
			return password;
		}

		/// <summary>Releases any resources being used.</summary>
		public virtual void Destroy()
		{
			lock (this)
			{
				if (trustManager != null)
				{
					trustManager.Destroy();
					trustManager = null;
					keyManagers = null;
					trustManagers = null;
				}
			}
		}

		/// <summary>Returns the keymanagers for owned certificates.</summary>
		/// <returns>the keymanagers for owned certificates.</returns>
		public virtual KeyManager[] GetKeyManagers()
		{
			return keyManagers;
		}

		/// <summary>Returns the trustmanagers for trusted certificates.</summary>
		/// <returns>the trustmanagers for trusted certificates.</returns>
		public virtual TrustManager[] GetTrustManagers()
		{
			return trustManagers;
		}
	}
}
