using System;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authentication.Client;
using Org.Apache.Hadoop.Security.Ssl;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Web
{
	/// <summary>Utilities for handling URLs</summary>
	public class URLConnectionFactory
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.Web.URLConnectionFactory
			));

		/// <summary>Timeout for socket connects and reads</summary>
		public const int DefaultSocketTimeout = 1 * 60 * 1000;

		private readonly ConnectionConfigurator connConfigurator;

		private sealed class _ConnectionConfigurator_58 : ConnectionConfigurator
		{
			public _ConnectionConfigurator_58()
			{
			}

			// 1 minute
			/// <exception cref="System.IO.IOException"/>
			public HttpURLConnection Configure(HttpURLConnection conn)
			{
				Org.Apache.Hadoop.Hdfs.Web.URLConnectionFactory.SetTimeouts(conn, Org.Apache.Hadoop.Hdfs.Web.URLConnectionFactory
					.DefaultSocketTimeout);
				return conn;
			}
		}

		private static readonly ConnectionConfigurator DefaultTimeoutConnConfigurator = new 
			_ConnectionConfigurator_58();

		/// <summary>
		/// The URLConnectionFactory that sets the default timeout and it only trusts
		/// Java's SSL certificates.
		/// </summary>
		public static readonly Org.Apache.Hadoop.Hdfs.Web.URLConnectionFactory DefaultSystemConnectionFactory
			 = new Org.Apache.Hadoop.Hdfs.Web.URLConnectionFactory(DefaultTimeoutConnConfigurator
			);

		/// <summary>Construct a new URLConnectionFactory based on the configuration.</summary>
		/// <remarks>
		/// Construct a new URLConnectionFactory based on the configuration. It will
		/// try to load SSL certificates when it is specified.
		/// </remarks>
		public static Org.Apache.Hadoop.Hdfs.Web.URLConnectionFactory NewDefaultURLConnectionFactory
			(Configuration conf)
		{
			ConnectionConfigurator conn = null;
			try
			{
				conn = NewSslConnConfigurator(DefaultSocketTimeout, conf);
			}
			catch (Exception e)
			{
				Log.Debug("Cannot load customized ssl related configuration. Fallback to system-generic settings."
					, e);
				conn = DefaultTimeoutConnConfigurator;
			}
			return new Org.Apache.Hadoop.Hdfs.Web.URLConnectionFactory(conn);
		}

		[VisibleForTesting]
		internal URLConnectionFactory(ConnectionConfigurator connConfigurator)
		{
			this.connConfigurator = connConfigurator;
		}

		/// <summary>Create a new ConnectionConfigurator for SSL connections</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.GeneralSecurityException"/>
		private static ConnectionConfigurator NewSslConnConfigurator(int timeout, Configuration
			 conf)
		{
			SSLFactory factory;
			SSLSocketFactory sf;
			HostnameVerifier hv;
			factory = new SSLFactory(SSLFactory.Mode.Client, conf);
			factory.Init();
			sf = factory.CreateSSLSocketFactory();
			hv = factory.GetHostnameVerifier();
			return new _ConnectionConfigurator_110(sf, hv, timeout);
		}

		private sealed class _ConnectionConfigurator_110 : ConnectionConfigurator
		{
			public _ConnectionConfigurator_110(SSLSocketFactory sf, HostnameVerifier hv, int 
				timeout)
			{
				this.sf = sf;
				this.hv = hv;
				this.timeout = timeout;
			}

			/// <exception cref="System.IO.IOException"/>
			public HttpURLConnection Configure(HttpURLConnection conn)
			{
				if (conn is HttpsURLConnection)
				{
					HttpsURLConnection c = (HttpsURLConnection)conn;
					c.SetSSLSocketFactory(sf);
					c.SetHostnameVerifier(hv);
				}
				Org.Apache.Hadoop.Hdfs.Web.URLConnectionFactory.SetTimeouts(conn, timeout);
				return conn;
			}

			private readonly SSLSocketFactory sf;

			private readonly HostnameVerifier hv;

			private readonly int timeout;
		}

		/// <summary>Opens a url with read and connect timeouts</summary>
		/// <param name="url">to open</param>
		/// <returns>URLConnection</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual URLConnection OpenConnection(Uri url)
		{
			try
			{
				return OpenConnection(url, false);
			}
			catch (AuthenticationException)
			{
				// Unreachable
				return null;
			}
		}

		/// <summary>Opens a url with read and connect timeouts</summary>
		/// <param name="url">URL to open</param>
		/// <param name="isSpnego">whether the url should be authenticated via SPNEGO</param>
		/// <returns>URLConnection</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Security.Authentication.Client.AuthenticationException
		/// 	"/>
		public virtual URLConnection OpenConnection(Uri url, bool isSpnego)
		{
			if (isSpnego)
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug("open AuthenticatedURL connection" + url);
				}
				UserGroupInformation.GetCurrentUser().CheckTGTAndReloginFromKeytab();
				AuthenticatedURL.Token authToken = new AuthenticatedURL.Token();
				return new AuthenticatedURL(new KerberosUgiAuthenticator(), connConfigurator).OpenConnection
					(url, authToken);
			}
			else
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug("open URL connection");
				}
				URLConnection connection = url.OpenConnection();
				if (connection is HttpURLConnection)
				{
					connConfigurator.Configure((HttpURLConnection)connection);
				}
				return connection;
			}
		}

		/// <summary>Sets timeout parameters on the given URLConnection.</summary>
		/// <param name="connection">URLConnection to set</param>
		/// <param name="socketTimeout">the connection and read timeout of the connection.</param>
		private static void SetTimeouts(URLConnection connection, int socketTimeout)
		{
			connection.SetConnectTimeout(socketTimeout);
			connection.SetReadTimeout(socketTimeout);
		}
	}
}
