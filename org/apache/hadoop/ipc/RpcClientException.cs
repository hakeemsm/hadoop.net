using Sharpen;

namespace org.apache.hadoop.ipc
{
	/// <summary>Indicates an exception in the RPC client</summary>
	[System.Serializable]
	public class RpcClientException : org.apache.hadoop.ipc.RpcException
	{
		private const long serialVersionUID = 1L;

		/// <summary>Constructs exception with the specified detail message.</summary>
		/// <param name="messages">detailed message.</param>
		internal RpcClientException(string message)
			: base(message)
		{
		}

		/// <summary>Constructs exception with the specified detail message and cause.</summary>
		/// <param name="message">message.</param>
		/// <param name="cause">that cause this exception</param>
		/// <param name="cause">
		/// the cause (can be retried by the
		/// <see cref="System.Exception.InnerException()"/>
		/// method).
		/// (A <tt>null</tt> value is permitted, and indicates that the cause
		/// is nonexistent or unknown.)
		/// </param>
		internal RpcClientException(string message, System.Exception cause)
			: base(message, cause)
		{
		}
	}
}
