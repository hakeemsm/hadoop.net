

namespace Org.Apache.Hadoop.Net
{
	/// <summary>
	/// Thrown by
	/// <see cref="NetUtils.Connect(System.Net.Sockets.Socket, System.Net.EndPoint, int)"
	/// 	/>
	/// if it times out while connecting to the remote host.
	/// </summary>
	[System.Serializable]
	public class ConnectTimeoutException : SocketTimeoutException
	{
		private const long serialVersionUID = 1L;

		public ConnectTimeoutException(string msg)
			: base(msg)
		{
		}
	}
}
