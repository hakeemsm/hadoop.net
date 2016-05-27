using System;
using System.IO;
using Org.Apache.Commons.Logging;
using Sharpen;

namespace Org.Apache.Hadoop.Security.Ssl
{
	/// <summary>
	/// A
	/// <see cref="Sharpen.TrustManager"/>
	/// implementation that reloads its configuration when
	/// the truststore file on disk changes.
	/// </summary>
	public sealed class ReloadingX509TrustManager : X509TrustManager, Runnable
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Security.Ssl.ReloadingX509TrustManager
			));

		private string type;

		private FilePath file;

		private string password;

		private long lastLoaded;

		private long reloadInterval;

		private AtomicReference<X509TrustManager> trustManagerRef;

		private volatile bool running;

		private Sharpen.Thread reloader;

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
		/// <exception cref="Sharpen.GeneralSecurityException">
		/// thrown if the truststore could not be
		/// initialized due to a security error.
		/// </exception>
		public ReloadingX509TrustManager(string type, string location, string password, long
			 reloadInterval)
		{
			this.type = type;
			file = new FilePath(location);
			this.password = password;
			trustManagerRef = new AtomicReference<X509TrustManager>();
			trustManagerRef.Set(LoadTrustManager());
			this.reloadInterval = reloadInterval;
		}

		/// <summary>Starts the reloader thread.</summary>
		public void Init()
		{
			reloader = new Sharpen.Thread(this, "Truststore reloader thread");
			reloader.SetDaemon(true);
			running = true;
			reloader.Start();
		}

		/// <summary>Stops the reloader thread.</summary>
		public void Destroy()
		{
			running = false;
			reloader.Interrupt();
		}

		/// <summary>Returns the reload check interval.</summary>
		/// <returns>the reload check interval, in milliseconds.</returns>
		public long GetReloadInterval()
		{
			return reloadInterval;
		}

		/// <exception cref="Sharpen.CertificateException"/>
		public void CheckClientTrusted(X509Certificate[] chain, string authType)
		{
			X509TrustManager tm = trustManagerRef.Get();
			if (tm != null)
			{
				tm.CheckClientTrusted(chain, authType);
			}
			else
			{
				throw new CertificateException("Unknown client chain certificate: " + chain[0].ToString
					());
			}
		}

		/// <exception cref="Sharpen.CertificateException"/>
		public void CheckServerTrusted(X509Certificate[] chain, string authType)
		{
			X509TrustManager tm = trustManagerRef.Get();
			if (tm != null)
			{
				tm.CheckServerTrusted(chain, authType);
			}
			else
			{
				throw new CertificateException("Unknown server chain certificate: " + chain[0].ToString
					());
			}
		}

		private static readonly X509Certificate[] Empty = new X509Certificate[0];

		public X509Certificate[] GetAcceptedIssuers()
		{
			X509Certificate[] issuers = Empty;
			X509TrustManager tm = trustManagerRef.Get();
			if (tm != null)
			{
				issuers = tm.GetAcceptedIssuers();
			}
			return issuers;
		}

		internal bool NeedsReload()
		{
			bool reload = true;
			if (file.Exists())
			{
				if (file.LastModified() == lastLoaded)
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
		/// <exception cref="Sharpen.GeneralSecurityException"/>
		internal X509TrustManager LoadTrustManager()
		{
			X509TrustManager trustManager = null;
			KeyStore ks = KeyStore.GetInstance(type);
			lastLoaded = file.LastModified();
			FileInputStream @in = new FileInputStream(file);
			try
			{
				ks.Load(@in, password.ToCharArray());
				Log.Debug("Loaded truststore '" + file + "'");
			}
			finally
			{
				@in.Close();
			}
			TrustManagerFactory trustManagerFactory = TrustManagerFactory.GetInstance(SSLFactory
				.Sslcertificate);
			trustManagerFactory.Init(ks);
			TrustManager[] trustManagers = trustManagerFactory.GetTrustManagers();
			foreach (TrustManager trustManager1 in trustManagers)
			{
				if (trustManager1 is X509TrustManager)
				{
					trustManager = (X509TrustManager)trustManager1;
					break;
				}
			}
			return trustManager;
		}

		public void Run()
		{
			while (running)
			{
				try
				{
					Sharpen.Thread.Sleep(reloadInterval);
				}
				catch (Exception)
				{
				}
				//NOP
				if (running && NeedsReload())
				{
					try
					{
						trustManagerRef.Set(LoadTrustManager());
					}
					catch (Exception ex)
					{
						Log.Warn("Could not load truststore (keep using existing one) : " + ex.ToString()
							, ex);
					}
				}
			}
		}
	}
}
