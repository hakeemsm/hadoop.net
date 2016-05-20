using Sharpen;

namespace org.apache.hadoop.security.ssl
{
	/// <summary>
	/// A
	/// <see cref="javax.net.ssl.TrustManager"/>
	/// implementation that reloads its configuration when
	/// the truststore file on disk changes.
	/// </summary>
	public sealed class ReloadingX509TrustManager : javax.net.ssl.X509TrustManager, java.lang.Runnable
	{
		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.security.ssl.ReloadingX509TrustManager
			)));

		private string type;

		private java.io.File file;

		private string password;

		private long lastLoaded;

		private long reloadInterval;

		private java.util.concurrent.atomic.AtomicReference<javax.net.ssl.X509TrustManager
			> trustManagerRef;

		private volatile bool running;

		private java.lang.Thread reloader;

		/// <summary>Creates a reloadable trustmanager.</summary>
		/// <remarks>
		/// Creates a reloadable trustmanager. The trustmanager reloads itself
		/// if the underlying trustore file has changed.
		/// </remarks>
		/// <param name="type">type of truststore file, typically 'jks'.</param>
		/// <param name="location">local path to the truststore file.</param>
		/// <param name="password">password of the truststore file.</param>
		/// <param name="reloadInterval">
		/// interval to check if the truststore file has
		/// changed, in milliseconds.
		/// </param>
		/// <exception cref="System.IO.IOException">
		/// thrown if the truststore could not be initialized due
		/// to an IO error.
		/// </exception>
		/// <exception cref="java.security.GeneralSecurityException">
		/// thrown if the truststore could not be
		/// initialized due to a security error.
		/// </exception>
		public ReloadingX509TrustManager(string type, string location, string password, long
			 reloadInterval)
		{
			this.type = type;
			file = new java.io.File(location);
			this.password = password;
			trustManagerRef = new java.util.concurrent.atomic.AtomicReference<javax.net.ssl.X509TrustManager
				>();
			trustManagerRef.set(loadTrustManager());
			this.reloadInterval = reloadInterval;
		}

		/// <summary>Starts the reloader thread.</summary>
		public void init()
		{
			reloader = new java.lang.Thread(this, "Truststore reloader thread");
			reloader.setDaemon(true);
			running = true;
			reloader.start();
		}

		/// <summary>Stops the reloader thread.</summary>
		public void destroy()
		{
			running = false;
			reloader.interrupt();
		}

		/// <summary>Returns the reload check interval.</summary>
		/// <returns>the reload check interval, in milliseconds.</returns>
		public long getReloadInterval()
		{
			return reloadInterval;
		}

		/// <exception cref="java.security.cert.CertificateException"/>
		public void checkClientTrusted(java.security.cert.X509Certificate[] chain, string
			 authType)
		{
			javax.net.ssl.X509TrustManager tm = trustManagerRef.get();
			if (tm != null)
			{
				tm.checkClientTrusted(chain, authType);
			}
			else
			{
				throw new java.security.cert.CertificateException("Unknown client chain certificate: "
					 + chain[0].ToString());
			}
		}

		/// <exception cref="java.security.cert.CertificateException"/>
		public void checkServerTrusted(java.security.cert.X509Certificate[] chain, string
			 authType)
		{
			javax.net.ssl.X509TrustManager tm = trustManagerRef.get();
			if (tm != null)
			{
				tm.checkServerTrusted(chain, authType);
			}
			else
			{
				throw new java.security.cert.CertificateException("Unknown server chain certificate: "
					 + chain[0].ToString());
			}
		}

		private static readonly java.security.cert.X509Certificate[] EMPTY = new java.security.cert.X509Certificate
			[0];

		public java.security.cert.X509Certificate[] getAcceptedIssuers()
		{
			java.security.cert.X509Certificate[] issuers = EMPTY;
			javax.net.ssl.X509TrustManager tm = trustManagerRef.get();
			if (tm != null)
			{
				issuers = tm.getAcceptedIssuers();
			}
			return issuers;
		}

		internal bool needsReload()
		{
			bool reload = true;
			if (file.exists())
			{
				if (file.lastModified() == lastLoaded)
				{
					reload = false;
				}
			}
			else
			{
				lastLoaded = 0;
			}
			return reload;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="java.security.GeneralSecurityException"/>
		internal javax.net.ssl.X509TrustManager loadTrustManager()
		{
			javax.net.ssl.X509TrustManager trustManager = null;
			java.security.KeyStore ks = java.security.KeyStore.getInstance(type);
			lastLoaded = file.lastModified();
			java.io.FileInputStream @in = new java.io.FileInputStream(file);
			try
			{
				ks.load(@in, password.ToCharArray());
				LOG.debug("Loaded truststore '" + file + "'");
			}
			finally
			{
				@in.close();
			}
			javax.net.ssl.TrustManagerFactory trustManagerFactory = javax.net.ssl.TrustManagerFactory
				.getInstance(org.apache.hadoop.security.ssl.SSLFactory.SSLCERTIFICATE);
			trustManagerFactory.init(ks);
			javax.net.ssl.TrustManager[] trustManagers = trustManagerFactory.getTrustManagers
				();
			foreach (javax.net.ssl.TrustManager trustManager1 in trustManagers)
			{
				if (trustManager1 is javax.net.ssl.X509TrustManager)
				{
					trustManager = (javax.net.ssl.X509TrustManager)trustManager1;
					break;
				}
			}
			return trustManager;
		}

		public void run()
		{
			while (running)
			{
				try
				{
					java.lang.Thread.sleep(reloadInterval);
				}
				catch (System.Exception)
				{
				}
				//NOP
				if (running && needsReload())
				{
					try
					{
						trustManagerRef.set(loadTrustManager());
					}
					catch (System.Exception ex)
					{
						LOG.warn("Could not load truststore (keep using existing one) : " + ex.ToString()
							, ex);
					}
				}
			}
		}
	}
}
