using Sharpen;

namespace org.apache.hadoop.security.ssl
{
	/// <summary>
	/// This subclass of the Jetty SslSocketConnector exists solely to control
	/// the TLS protocol versions allowed.
	/// </summary>
	/// <remarks>
	/// This subclass of the Jetty SslSocketConnector exists solely to control
	/// the TLS protocol versions allowed.  This is fallout from the POODLE
	/// vulnerability (CVE-2014-3566), which requires that SSLv3 be disabled.
	/// Only TLS 1.0 and later protocols are allowed.
	/// </remarks>
	public class SslSocketConnectorSecure : org.mortbay.jetty.security.SslSocketConnector
	{
		public SslSocketConnectorSecure()
			: base()
		{
		}

		/// <summary>
		/// Create a new ServerSocket that will not accept SSLv3 connections,
		/// but will accept TLSv1.x connections.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		protected override java.net.ServerSocket newServerSocket(string host, int port, int
			 backlog)
		{
			javax.net.ssl.SSLServerSocket socket = (javax.net.ssl.SSLServerSocket)base.newServerSocket
				(host, port, backlog);
			System.Collections.Generic.List<string> nonSSLProtocols = new System.Collections.Generic.List
				<string>();
			foreach (string p in socket.getEnabledProtocols())
			{
				if (!p.contains("SSLv3"))
				{
					nonSSLProtocols.add(p);
				}
			}
			socket.setEnabledProtocols(Sharpen.Collections.ToArray(nonSSLProtocols, new string
				[nonSSLProtocols.Count]));
			return socket;
		}
	}
}
