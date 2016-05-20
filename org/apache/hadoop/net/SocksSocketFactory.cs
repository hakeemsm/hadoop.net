using Sharpen;

namespace org.apache.hadoop.net
{
	/// <summary>Specialized SocketFactory to create sockets with a SOCKS proxy</summary>
	public class SocksSocketFactory : javax.net.SocketFactory, org.apache.hadoop.conf.Configurable
	{
		private org.apache.hadoop.conf.Configuration conf;

		private java.net.Proxy proxy;

		/// <summary>Default empty constructor (for use with the reflection API).</summary>
		public SocksSocketFactory()
		{
			this.proxy = java.net.Proxy.NO_PROXY;
		}

		/// <summary>Constructor with a supplied Proxy</summary>
		/// <param name="proxy">the proxy to use to create sockets</param>
		public SocksSocketFactory(java.net.Proxy proxy)
		{
			this.proxy = proxy;
		}

		/// <exception cref="System.IO.IOException"/>
		public override java.net.Socket createSocket()
		{
			return new java.net.Socket(proxy);
		}

		/// <exception cref="System.IO.IOException"/>
		public override java.net.Socket createSocket(java.net.InetAddress addr, int port)
		{
			java.net.Socket socket = createSocket();
			socket.connect(new java.net.InetSocketAddress(addr, port));
			return socket;
		}

		/// <exception cref="System.IO.IOException"/>
		public override java.net.Socket createSocket(java.net.InetAddress addr, int port, 
			java.net.InetAddress localHostAddr, int localPort)
		{
			java.net.Socket socket = createSocket();
			socket.bind(new java.net.InetSocketAddress(localHostAddr, localPort));
			socket.connect(new java.net.InetSocketAddress(addr, port));
			return socket;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="java.net.UnknownHostException"/>
		public override java.net.Socket createSocket(string host, int port)
		{
			java.net.Socket socket = createSocket();
			socket.connect(new java.net.InetSocketAddress(host, port));
			return socket;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="java.net.UnknownHostException"/>
		public override java.net.Socket createSocket(string host, int port, java.net.InetAddress
			 localHostAddr, int localPort)
		{
			java.net.Socket socket = createSocket();
			socket.bind(new java.net.InetSocketAddress(localHostAddr, localPort));
			socket.connect(new java.net.InetSocketAddress(host, port));
			return socket;
		}

		public override int GetHashCode()
		{
			return proxy.GetHashCode();
		}

		public override bool Equals(object obj)
		{
			if (this == obj)
			{
				return true;
			}
			if (obj == null)
			{
				return false;
			}
			if (!(obj is org.apache.hadoop.net.SocksSocketFactory))
			{
				return false;
			}
			org.apache.hadoop.net.SocksSocketFactory other = (org.apache.hadoop.net.SocksSocketFactory
				)obj;
			if (proxy == null)
			{
				if (other.proxy != null)
				{
					return false;
				}
			}
			else
			{
				if (!proxy.Equals(other.proxy))
				{
					return false;
				}
			}
			return true;
		}

		public virtual org.apache.hadoop.conf.Configuration getConf()
		{
			return this.conf;
		}

		public virtual void setConf(org.apache.hadoop.conf.Configuration conf)
		{
			this.conf = conf;
			string proxyStr = conf.get("hadoop.socks.server");
			if ((proxyStr != null) && (proxyStr.Length > 0))
			{
				setProxy(proxyStr);
			}
		}

		/// <summary>
		/// Set the proxy of this socket factory as described in the string
		/// parameter
		/// </summary>
		/// <param name="proxyStr">the proxy address using the format "host:port"</param>
		private void setProxy(string proxyStr)
		{
			string[] strs = proxyStr.split(":", 2);
			if (strs.Length != 2)
			{
				throw new System.Exception("Bad SOCKS proxy parameter: " + proxyStr);
			}
			string host = strs[0];
			int port = System.Convert.ToInt32(strs[1]);
			this.proxy = new java.net.Proxy(java.net.Proxy.Type.SOCKS, java.net.InetSocketAddress
				.createUnresolved(host, port));
		}
	}
}
