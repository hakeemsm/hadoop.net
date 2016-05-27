using System.Net;
using System.Net.Sockets;
using Javax.Net;
using Org.Apache.Hadoop.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Net
{
	/// <summary>Specialized SocketFactory to create sockets with a SOCKS proxy</summary>
	public class SocksSocketFactory : SocketFactory, Configurable
	{
		private Configuration conf;

		private Proxy proxy;

		/// <summary>Default empty constructor (for use with the reflection API).</summary>
		public SocksSocketFactory()
		{
			this.proxy = Proxy.NoProxy;
		}

		/// <summary>Constructor with a supplied Proxy</summary>
		/// <param name="proxy">the proxy to use to create sockets</param>
		public SocksSocketFactory(Proxy proxy)
		{
			this.proxy = proxy;
		}

		/// <exception cref="System.IO.IOException"/>
		public override Socket CreateSocket()
		{
			return Sharpen.Extensions.CreateSocket(proxy);
		}

		/// <exception cref="System.IO.IOException"/>
		public override Socket CreateSocket(IPAddress addr, int port)
		{
			Socket socket = CreateSocket();
			socket.Connect(new IPEndPoint(addr, port));
			return socket;
		}

		/// <exception cref="System.IO.IOException"/>
		public override Socket CreateSocket(IPAddress addr, int port, IPAddress localHostAddr
			, int localPort)
		{
			Socket socket = CreateSocket();
			socket.Bind2(new IPEndPoint(localHostAddr, localPort));
			socket.Connect(new IPEndPoint(addr, port));
			return socket;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.UnknownHostException"/>
		public override Socket CreateSocket(string host, int port)
		{
			Socket socket = CreateSocket();
			socket.Connect(new IPEndPoint(host, port));
			return socket;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.UnknownHostException"/>
		public override Socket CreateSocket(string host, int port, IPAddress localHostAddr
			, int localPort)
		{
			Socket socket = CreateSocket();
			socket.Bind2(new IPEndPoint(localHostAddr, localPort));
			socket.Connect(new IPEndPoint(host, port));
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
			if (!(obj is Org.Apache.Hadoop.Net.SocksSocketFactory))
			{
				return false;
			}
			Org.Apache.Hadoop.Net.SocksSocketFactory other = (Org.Apache.Hadoop.Net.SocksSocketFactory
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

		public virtual Configuration GetConf()
		{
			return this.conf;
		}

		public virtual void SetConf(Configuration conf)
		{
			this.conf = conf;
			string proxyStr = conf.Get("hadoop.socks.server");
			if ((proxyStr != null) && (proxyStr.Length > 0))
			{
				SetProxy(proxyStr);
			}
		}

		/// <summary>
		/// Set the proxy of this socket factory as described in the string
		/// parameter
		/// </summary>
		/// <param name="proxyStr">the proxy address using the format "host:port"</param>
		private void SetProxy(string proxyStr)
		{
			string[] strs = proxyStr.Split(":", 2);
			if (strs.Length != 2)
			{
				throw new RuntimeException("Bad SOCKS proxy parameter: " + proxyStr);
			}
			string host = strs[0];
			int port = System.Convert.ToInt32(strs[1]);
			this.proxy = new Proxy(Proxy.Type.Socks, IPEndPoint.CreateUnresolved(host, port));
		}
	}
}
