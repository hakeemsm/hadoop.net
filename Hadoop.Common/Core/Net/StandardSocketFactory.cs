using System.Net;
using System.Net.Sockets;
using Javax.Net;


namespace Org.Apache.Hadoop.Net
{
	/// <summary>Specialized SocketFactory to create sockets with a SOCKS proxy</summary>
	public class StandardSocketFactory : SocketFactory
	{
		/// <summary>Default empty constructor (for use with the reflection API).</summary>
		public StandardSocketFactory()
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public override Socket CreateSocket()
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
			return SocketChannel.Open().Socket();
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
		/// <exception cref="UnknownHostException"/>
		public override Socket CreateSocket(string host, int port)
		{
			Socket socket = CreateSocket();
			socket.Connect(new IPEndPoint(host, port));
			return socket;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="UnknownHostException"/>
		public override Socket CreateSocket(string host, int port, IPAddress localHostAddr
			, int localPort)
		{
			Socket socket = CreateSocket();
			socket.Bind2(new IPEndPoint(localHostAddr, localPort));
			socket.Connect(new IPEndPoint(host, port));
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
			return obj.GetType().Equals(this.GetType());
		}

		public override int GetHashCode()
		{
			return this.GetType().GetHashCode();
		}
	}
}
