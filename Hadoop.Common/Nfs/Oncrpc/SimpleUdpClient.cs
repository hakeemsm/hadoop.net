using System.IO;
using System.Net;
using Sharpen;

namespace Org.Apache.Hadoop.Oncrpc
{
	/// <summary>A simple UDP based RPC client which just sends one request to a server.</summary>
	public class SimpleUdpClient
	{
		protected internal readonly string host;

		protected internal readonly int port;

		protected internal readonly XDR request;

		protected internal readonly bool oneShot;

		protected internal readonly DatagramSocket clientSocket;

		public SimpleUdpClient(string host, int port, XDR request, DatagramSocket clientSocket
			)
			: this(host, port, request, true, clientSocket)
		{
		}

		public SimpleUdpClient(string host, int port, XDR request, bool oneShot, DatagramSocket
			 clientSocket)
		{
			this.host = host;
			this.port = port;
			this.request = request;
			this.oneShot = oneShot;
			this.clientSocket = clientSocket;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Run()
		{
			IPAddress IPAddress = Sharpen.Extensions.GetAddressByName(host);
			byte[] sendData = request.GetBytes();
			byte[] receiveData = new byte[65535];
			// Use the provided socket if there is one, else just make a new one.
			DatagramSocket socket = this.clientSocket == null ? new DatagramSocket() : this.clientSocket;
			try
			{
				DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.Length, IPAddress
					, port);
				socket.Send(sendPacket);
				socket.SetSoTimeout(500);
				DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.Length
					);
				socket.Receive(receivePacket);
				// Check reply status
				XDR xdr = new XDR(Arrays.CopyOfRange(receiveData, 0, receivePacket.GetLength()));
				RpcReply reply = RpcReply.Read(xdr);
				if (reply.GetState() != RpcReply.ReplyState.MsgAccepted)
				{
					throw new IOException("Request failed: " + reply.GetState());
				}
			}
			finally
			{
				// If the client socket was passed in to this UDP client, it's on the
				// caller of this UDP client to close that socket.
				if (this.clientSocket == null)
				{
					socket.Close();
				}
			}
		}
	}
}
