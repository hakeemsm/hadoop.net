using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Net.Unix;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Net
{
	public class DomainPeerServer : PeerServer
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.Net.DomainPeerServer
			));

		private readonly DomainSocket sock;

		internal DomainPeerServer(DomainSocket sock)
		{
			this.sock = sock;
		}

		/// <exception cref="System.IO.IOException"/>
		public DomainPeerServer(string path, int port)
			: this(DomainSocket.BindAndListen(DomainSocket.GetEffectivePath(path, port)))
		{
		}

		public virtual string GetBindPath()
		{
			return sock.GetPath();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void SetReceiveBufferSize(int size)
		{
			sock.SetAttribute(DomainSocket.ReceiveBufferSize, size);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.SocketTimeoutException"/>
		public virtual Peer Accept()
		{
			DomainSocket connSock = sock.Accept();
			Peer peer = null;
			bool success = false;
			try
			{
				peer = new DomainPeer(connSock);
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
					connSock.Close();
				}
			}
		}

		public virtual string GetListeningString()
		{
			return "unix:" + sock.GetPath();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Close()
		{
			try
			{
				sock.Close();
			}
			catch (IOException e)
			{
				Log.Error("error closing DomainPeerServer: ", e);
			}
		}

		public override string ToString()
		{
			return "DomainPeerServer(" + GetListeningString() + ")";
		}
	}
}
