using System;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Net
{
	public interface PeerServer : IDisposable
	{
		/// <summary>Set the receive buffer size of the PeerServer.</summary>
		/// <param name="size">The receive buffer size.</param>
		/// <exception cref="System.IO.IOException"/>
		void SetReceiveBufferSize(int size);

		/// <summary>
		/// Listens for a connection to be made to this server and accepts
		/// it.
		/// </summary>
		/// <remarks>
		/// Listens for a connection to be made to this server and accepts
		/// it. The method blocks until a connection is made.
		/// </remarks>
		/// <exception>
		/// IOException
		/// if an I/O error occurs when waiting for a
		/// connection.
		/// </exception>
		/// <exception>
		/// SecurityException
		/// if a security manager exists and its
		/// <code>checkAccept</code> method doesn't allow the operation.
		/// </exception>
		/// <exception>
		/// SocketTimeoutException
		/// if a timeout was previously set and
		/// the timeout has been reached.
		/// </exception>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.SocketTimeoutException"/>
		Peer Accept();

		/// <returns>
		/// A string representation of the address we're
		/// listening on.
		/// </returns>
		string GetListeningString();

		/// <summary>Free the resources associated with this peer server.</summary>
		/// <remarks>
		/// Free the resources associated with this peer server.
		/// This normally includes sockets, etc.
		/// </remarks>
		/// <exception cref="System.IO.IOException">If there is an error closing the PeerServer
		/// 	</exception>
		void Close();
	}
}
