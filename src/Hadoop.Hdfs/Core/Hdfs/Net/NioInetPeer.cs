using System.IO;
using System.Net.Sockets;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Net.Unix;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Net
{
	/// <summary>
	/// Represents a peer that we communicate with by using non-blocking I/O
	/// on a Socket.
	/// </summary>
	internal class NioInetPeer : Peer
	{
		private readonly Socket socket;

		/// <summary>An InputStream which simulates blocking I/O with timeouts using NIO.</summary>
		private readonly SocketInputStream @in;

		/// <summary>An OutputStream which simulates blocking I/O with timeouts using NIO.</summary>
		private readonly SocketOutputStream @out;

		private readonly bool isLocal;

		/// <exception cref="System.IO.IOException"/>
		internal NioInetPeer(Socket socket)
		{
			this.socket = socket;
			this.@in = new SocketInputStream(socket.GetChannel(), 0);
			this.@out = new SocketOutputStream(socket.GetChannel(), 0);
			this.isLocal = socket.GetInetAddress().Equals(socket.GetLocalAddress());
		}

		public virtual ReadableByteChannel GetInputStreamChannel()
		{
			return @in;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void SetReadTimeout(int timeoutMs)
		{
			@in.SetTimeout(timeoutMs);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual int GetReceiveBufferSize()
		{
			return socket.GetReceiveBufferSize();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual bool GetTcpNoDelay()
		{
			return socket.GetTcpNoDelay();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void SetWriteTimeout(int timeoutMs)
		{
			@out.SetTimeout(timeoutMs);
		}

		public virtual bool IsClosed()
		{
			return socket.IsClosed();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Close()
		{
			// We always close the outermost streams-- in this case, 'in' and 'out'
			// Closing either one of these will also close the Socket.
			try
			{
				@in.Close();
			}
			finally
			{
				@out.Close();
			}
		}

		public virtual string GetRemoteAddressString()
		{
			return socket.RemoteEndPoint.ToString();
		}

		public virtual string GetLocalAddressString()
		{
			return socket.GetLocalSocketAddress().ToString();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual InputStream GetInputStream()
		{
			return @in;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual OutputStream GetOutputStream()
		{
			return @out;
		}

		public virtual bool IsLocal()
		{
			return isLocal;
		}

		public override string ToString()
		{
			return "NioInetPeer(" + socket.ToString() + ")";
		}

		public virtual DomainSocket GetDomainSocket()
		{
			return null;
		}

		public virtual bool HasSecureChannel()
		{
			return false;
		}
	}
}
