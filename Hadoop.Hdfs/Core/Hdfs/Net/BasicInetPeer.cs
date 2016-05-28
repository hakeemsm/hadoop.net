using System.IO;
using System.Net.Sockets;
using Org.Apache.Hadoop.Net.Unix;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Net
{
	/// <summary>
	/// Represents a peer that we communicate with by using a basic Socket
	/// that has no associated Channel.
	/// </summary>
	internal class BasicInetPeer : Peer
	{
		private readonly Socket socket;

		private readonly OutputStream @out;

		private readonly InputStream @in;

		private readonly bool isLocal;

		/// <exception cref="System.IO.IOException"/>
		public BasicInetPeer(Socket socket)
		{
			this.socket = socket;
			this.@out = socket.GetOutputStream();
			this.@in = socket.GetInputStream();
			this.isLocal = socket.GetInetAddress().Equals(socket.GetLocalAddress());
		}

		public virtual ReadableByteChannel GetInputStreamChannel()
		{
			/*
			* This Socket has no channel, so there's nothing to return here.
			*/
			return null;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void SetReadTimeout(int timeoutMs)
		{
			socket.ReceiveTimeout = timeoutMs;
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

		public virtual void SetWriteTimeout(int timeoutMs)
		{
		}

		/*
		* We can't implement write timeouts. :(
		*
		* Java provides no facility to set a blocking write timeout on a Socket.
		* You can simulate a blocking write with a timeout by using
		* non-blocking I/O.  However, we can't use nio here, because this Socket
		* doesn't have an associated Channel.
		*
		* See http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4031100 for
		* more details.
		*/
		public virtual bool IsClosed()
		{
			return socket.IsClosed();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Close()
		{
			socket.Close();
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
			return "BasicInetPeer(" + socket.ToString() + ")";
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
