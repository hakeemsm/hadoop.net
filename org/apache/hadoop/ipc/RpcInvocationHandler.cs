using Sharpen;

namespace org.apache.hadoop.ipc
{
	/// <summary>
	/// This interface must be implemented by all InvocationHandler
	/// implementations.
	/// </summary>
	public interface RpcInvocationHandler : java.lang.reflect.InvocationHandler, java.io.Closeable
	{
		/// <summary>Returns the connection id associated with the InvocationHandler instance.
		/// 	</summary>
		/// <returns>ConnectionId</returns>
		org.apache.hadoop.ipc.Client.ConnectionId getConnectionId();
	}
}
