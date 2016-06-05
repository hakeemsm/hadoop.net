using System;

using Reflect;

namespace Org.Apache.Hadoop.Ipc
{
	/// <summary>
	/// This interface must be implemented by all InvocationHandler
	/// implementations.
	/// </summary>
	public interface RpcInvocationHandler : InvocationHandler, IDisposable
	{
		/// <summary>Returns the connection id associated with the InvocationHandler instance.
		/// 	</summary>
		/// <returns>ConnectionId</returns>
		Client.ConnectionId GetConnectionId();
	}
}
