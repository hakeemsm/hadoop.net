using Sharpen;

namespace org.apache.hadoop.net
{
	/// <summary>
	/// Thrown by
	/// <see cref="NetUtils.connect(java.net.Socket, java.net.SocketAddress, int)"/>
	/// if it times out while connecting to the remote host.
	/// </summary>
	[System.Serializable]
	public class ConnectTimeoutException : java.net.SocketTimeoutException
	{
		private const long serialVersionUID = 1L;

		public ConnectTimeoutException(string msg)
			: base(msg)
		{
		}
	}
}
