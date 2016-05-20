using Sharpen;

namespace org.apache.hadoop.ipc
{
	/// <summary>
	/// Indicates that the RPC server encountered an undeclared exception from the
	/// service
	/// </summary>
	[System.Serializable]
	public class UnexpectedServerException : org.apache.hadoop.ipc.RpcException
	{
		private const long serialVersionUID = 1L;

		/// <summary>Constructs exception with the specified detail message.</summary>
		/// <param name="messages">detailed message.</param>
		internal UnexpectedServerException(string message)
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
		internal UnexpectedServerException(string message, System.Exception cause)
			: base(message, cause)
		{
		}
	}
}
