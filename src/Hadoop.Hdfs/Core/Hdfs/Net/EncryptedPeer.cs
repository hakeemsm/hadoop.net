using System.IO;
using Org.Apache.Hadoop.Hdfs.Protocol.Datatransfer;
using Org.Apache.Hadoop.Net.Unix;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Net
{
	/// <summary>
	/// Represents a peer that we communicate with by using an encrypted
	/// communications medium.
	/// </summary>
	public class EncryptedPeer : Peer
	{
		private readonly Peer enclosedPeer;

		/// <summary>An encrypted InputStream.</summary>
		private readonly InputStream @in;

		/// <summary>An encrypted OutputStream.</summary>
		private readonly OutputStream @out;

		/// <summary>An encrypted ReadableByteChannel.</summary>
		private readonly ReadableByteChannel channel;

		public EncryptedPeer(Peer enclosedPeer, IOStreamPair ios)
		{
			this.enclosedPeer = enclosedPeer;
			this.@in = ios.@in;
			this.@out = ios.@out;
			this.channel = ios.@in is ReadableByteChannel ? (ReadableByteChannel)ios.@in : null;
		}

		public virtual ReadableByteChannel GetInputStreamChannel()
		{
			return channel;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void SetReadTimeout(int timeoutMs)
		{
			enclosedPeer.SetReadTimeout(timeoutMs);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual int GetReceiveBufferSize()
		{
			return enclosedPeer.GetReceiveBufferSize();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual bool GetTcpNoDelay()
		{
			return enclosedPeer.GetTcpNoDelay();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void SetWriteTimeout(int timeoutMs)
		{
			enclosedPeer.SetWriteTimeout(timeoutMs);
		}

		public virtual bool IsClosed()
		{
			return enclosedPeer.IsClosed();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Close()
		{
			try
			{
				@in.Close();
			}
			finally
			{
				try
				{
					@out.Close();
				}
				finally
				{
					enclosedPeer.Close();
				}
			}
		}

		public virtual string GetRemoteAddressString()
		{
			return enclosedPeer.GetRemoteAddressString();
		}

		public virtual string GetLocalAddressString()
		{
			return enclosedPeer.GetLocalAddressString();
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
			return enclosedPeer.IsLocal();
		}

		public override string ToString()
		{
			return "EncryptedPeer(" + enclosedPeer + ")";
		}

		public virtual DomainSocket GetDomainSocket()
		{
			return enclosedPeer.GetDomainSocket();
		}

		public virtual bool HasSecureChannel()
		{
			return true;
		}
	}
}
