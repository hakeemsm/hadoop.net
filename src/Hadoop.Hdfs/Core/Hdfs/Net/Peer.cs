using System;
using System.IO;
using Org.Apache.Hadoop.Net.Unix;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Net
{
	/// <summary>Represents a connection to a peer.</summary>
	public interface Peer : IDisposable
	{
		/// <returns>
		/// The input stream channel associated with this
		/// peer, or null if it has none.
		/// </returns>
		ReadableByteChannel GetInputStreamChannel();

		/// <summary>Set the read timeout on this peer.</summary>
		/// <param name="timeoutMs">The timeout in milliseconds.</param>
		/// <exception cref="System.IO.IOException"/>
		void SetReadTimeout(int timeoutMs);

		/// <returns>The receive buffer size.</returns>
		/// <exception cref="System.IO.IOException"/>
		int GetReceiveBufferSize();

		/// <returns>True if TCP_NODELAY is turned on.</returns>
		/// <exception cref="System.IO.IOException"/>
		bool GetTcpNoDelay();

		/// <summary>Set the write timeout on this peer.</summary>
		/// <remarks>
		/// Set the write timeout on this peer.
		/// Note: this is not honored for BasicInetPeer.
		/// See
		/// <see cref="BasicSocketPeer#setWriteTimeout"/>
		/// for details.
		/// </remarks>
		/// <param name="timeoutMs">The timeout in milliseconds.</param>
		/// <exception cref="System.IO.IOException"/>
		void SetWriteTimeout(int timeoutMs);

		/// <returns>true only if the peer is closed.</returns>
		bool IsClosed();

		/// <summary>Close the peer.</summary>
		/// <remarks>
		/// Close the peer.
		/// It's safe to re-close a Peer that is already closed.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		void Close();

		/// <returns>
		/// A string representing the remote end of our
		/// connection to the peer.
		/// </returns>
		string GetRemoteAddressString();

		/// <returns>
		/// A string representing the local end of our
		/// connection to the peer.
		/// </returns>
		string GetLocalAddressString();

		/// <returns>
		/// An InputStream associated with the Peer.
		/// This InputStream will be valid until you close
		/// this peer with Peer#close.
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		InputStream GetInputStream();

		/// <returns>
		/// An OutputStream associated with the Peer.
		/// This OutputStream will be valid until you close
		/// this peer with Peer#close.
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		OutputStream GetOutputStream();

		/// <returns>
		/// True if the peer resides on the same
		/// computer as we.
		/// </returns>
		bool IsLocal();

		/// <returns>
		/// The DomainSocket associated with the current
		/// peer, or null if there is none.
		/// </returns>
		DomainSocket GetDomainSocket();

		/// <summary>Return true if the channel is secure.</summary>
		/// <returns>
		/// True if our channel to this peer is not
		/// susceptible to man-in-the-middle attacks.
		/// </returns>
		bool HasSecureChannel();
	}
}
