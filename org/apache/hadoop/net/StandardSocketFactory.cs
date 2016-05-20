using Sharpen;

namespace org.apache.hadoop.net
{
	/// <summary>Specialized SocketFactory to create sockets with a SOCKS proxy</summary>
	public class StandardSocketFactory : javax.net.SocketFactory
	{
		/// <summary>Default empty constructor (for use with the reflection API).</summary>
		public StandardSocketFactory()
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public override java.net.Socket createSocket()
		{
			/*
			* NOTE: This returns an NIO socket so that it has an associated
			* SocketChannel. As of now, this unfortunately makes streams returned
			* by Socket.getInputStream() and Socket.getOutputStream() unusable
			* (because a blocking read on input stream blocks write on output stream
			* and vice versa).
			*
			* So users of these socket factories should use
			* NetUtils.getInputStream(socket) and
			* NetUtils.getOutputStream(socket) instead.
			*
			* A solution for hiding from this from user is to write a
			* 'FilterSocket' on the lines of FilterInputStream and extend it by
			* overriding getInputStream() and getOutputStream().
			*/
			return java.nio.channels.SocketChannel.open().socket();
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
			return Sharpen.Runtime.getClassForObject(obj).Equals(Sharpen.Runtime.getClassForObject
				(this));
		}

		public override int GetHashCode()
		{
			return Sharpen.Runtime.getClassForObject(this).GetHashCode();
		}
	}
}
