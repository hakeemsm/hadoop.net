using System.IO;
using System.Net;
using System.Net.Sockets;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Protocol.Datatransfer.Sasl;
using Org.Apache.Hadoop.Hdfs.Security.Token.Block;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Ipc;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Net
{
	public class TcpPeerServer : PeerServer
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.Net.TcpPeerServer
			));

		private readonly Socket serverSocket;

		/// <exception cref="System.IO.IOException"/>
		public static Peer PeerFromSocket(Socket socket)
		{
			Peer peer = null;
			bool success = false;
			try
			{
				// TCP_NODELAY is crucial here because of bad interactions between
				// Nagle's Algorithm and Delayed ACKs. With connection keepalive
				// between the client and DN, the conversation looks like:
				//   1. Client -> DN: Read block X
				//   2. DN -> Client: data for block X
				//   3. Client -> DN: Status OK (successful read)
				//   4. Client -> DN: Read block Y
				// The fact that step #3 and #4 are both in the client->DN direction
				// triggers Nagling. If the DN is using delayed ACKs, this results
				// in a delay of 40ms or more.
				//
				// TCP_NODELAY disables nagling and thus avoids this performance
				// disaster.
				socket.NoDelay = true;
				SocketChannel channel = socket.GetChannel();
				if (channel == null)
				{
					peer = new BasicInetPeer(socket);
				}
				else
				{
					peer = new NioInetPeer(socket);
				}
				success = true;
				return peer;
			}
			finally
			{
				if (!success)
				{
					if (peer != null)
					{
						peer.Close();
					}
					socket.Close();
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public static Peer PeerFromSocketAndKey(SaslDataTransferClient saslClient, Socket
			 s, DataEncryptionKeyFactory keyFactory, Org.Apache.Hadoop.Security.Token.Token<
			BlockTokenIdentifier> blockToken, DatanodeID datanodeId)
		{
			Peer peer = null;
			bool success = false;
			try
			{
				peer = PeerFromSocket(s);
				peer = saslClient.PeerSend(peer, keyFactory, blockToken, datanodeId);
				success = true;
				return peer;
			}
			finally
			{
				if (!success)
				{
					IOUtils.Cleanup(null, peer);
				}
			}
		}

		/// <summary>Create a non-secure TcpPeerServer.</summary>
		/// <param name="socketWriteTimeout">The Socket write timeout in ms.</param>
		/// <param name="bindAddr">The address to bind to.</param>
		/// <exception cref="System.IO.IOException"/>
		public TcpPeerServer(int socketWriteTimeout, IPEndPoint bindAddr)
		{
			this.serverSocket = (socketWriteTimeout > 0) ? ServerSocketChannel.Open().Socket(
				) : new Socket();
			Server.Bind(serverSocket, bindAddr, 0);
		}

		/// <summary>Create a secure TcpPeerServer.</summary>
		/// <param name="secureResources">Security resources.</param>
		public TcpPeerServer(SecureDataNodeStarter.SecureResources secureResources)
		{
			this.serverSocket = secureResources.GetStreamingSocket();
		}

		/// <returns>the IP address which this TcpPeerServer is listening on.</returns>
		public virtual IPEndPoint GetStreamingAddr()
		{
			return new IPEndPoint(serverSocket.GetInetAddress().GetHostAddress(), serverSocket
				.GetLocalPort());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void SetReceiveBufferSize(int size)
		{
			this.serverSocket.SetReceiveBufferSize(size);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.SocketTimeoutException"/>
		public virtual Peer Accept()
		{
			Peer peer = PeerFromSocket(serverSocket.Accept());
			return peer;
		}

		public virtual string GetListeningString()
		{
			return serverSocket.LocalEndPoint.ToString();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Close()
		{
			try
			{
				serverSocket.Close();
			}
			catch (IOException e)
			{
				Log.Error("error closing TcpPeerServer: ", e);
			}
		}

		public override string ToString()
		{
			return "TcpPeerServer(" + GetListeningString() + ")";
		}
	}
}
