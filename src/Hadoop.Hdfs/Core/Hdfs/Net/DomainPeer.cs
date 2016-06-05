using System.IO;
using Org.Apache.Hadoop.Net.Unix;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Net
{
	/// <summary>
	/// Represents a peer that we communicate with by using blocking I/O
	/// on a UNIX domain socket.
	/// </summary>
	public class DomainPeer : Peer
	{
		private readonly DomainSocket socket;

		private readonly OutputStream @out;

		private readonly InputStream @in;

		private readonly ReadableByteChannel channel;

		public DomainPeer(DomainSocket socket)
		{
			this.socket = socket;
			this.@out = socket.GetOutputStream();
			this.@in = socket.GetInputStream();
			this.channel = socket.GetChannel();
		}

		public virtual ReadableByteChannel GetInputStreamChannel()
		{
			return channel;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void SetReadTimeout(int timeoutMs)
		{
			socket.SetAttribute(DomainSocket.ReceiveTimeout, timeoutMs);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual int GetReceiveBufferSize()
		{
			return socket.GetAttribute(DomainSocket.ReceiveBufferSize);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual bool GetTcpNoDelay()
		{
			/* No TCP, no TCP_NODELAY. */
			return false;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void SetWriteTimeout(int timeoutMs)
		{
			socket.SetAttribute(DomainSocket.SendTimeout, timeoutMs);
		}

		public virtual bool IsClosed()
		{
			return !socket.IsOpen();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Close()
		{
			socket.Close();
		}

		public virtual string GetRemoteAddressString()
		{
			return "unix:" + socket.GetPath();
		}

		public virtual string GetLocalAddressString()
		{
			return "<local>";
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
			/* UNIX domain sockets can only be used for local communication. */
			return true;
		}

		public override string ToString()
		{
			return "DomainPeer(" + GetRemoteAddressString() + ")";
		}

		public virtual DomainSocket GetDomainSocket()
		{
			return socket;
		}

		public virtual bool HasSecureChannel()
		{
			//
			// Communication over domain sockets is assumed to be secure, since it
			// doesn't pass over any network.  We also carefully control the privileges
			// that can be used on the domain socket inode and its parent directories.
			// See #{java.org.apache.hadoop.net.unix.DomainSocket#validateSocketPathSecurity0}
			// for details.
			//
			// So unless you are running as root or the hdfs superuser, you cannot
			// launch a man-in-the-middle attach on UNIX domain socket traffic.
			//
			return true;
		}
	}
}
