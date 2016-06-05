using System.Net.Sockets;
using Org.Mortbay.Jetty.Security;


namespace Org.Apache.Hadoop.Security.Ssl
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
	public class SslSocketConnectorSecure : SslSocketConnector
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
		protected override Socket NewServerSocket(string host, int port, int backlog)
		{
			SSLServerSocket socket = (SSLServerSocket)base.NewServerSocket(host, port, backlog
				);
			AList<string> nonSSLProtocols = new AList<string>();
			foreach (string p in socket.GetEnabledProtocols())
			{
				if (!p.Contains("SSLv3"))
				{
					nonSSLProtocols.AddItem(p);
				}
			}
			socket.SetEnabledProtocols(Collections.ToArray(nonSSLProtocols, new string
				[nonSSLProtocols.Count]));
			return socket;
		}
	}
}
