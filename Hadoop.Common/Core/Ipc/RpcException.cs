using System;
using System.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Ipc
{
	/// <summary>Indicates an exception during the execution of remote procedure call.</summary>
	[System.Serializable]
	public class RpcException : IOException
	{
		private const long serialVersionUID = 1L;

		/// <summary>Constructs exception with the specified detail message.</summary>
		/// <param name="messages">detailed message.</param>
		internal RpcException(string message)
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
		internal RpcException(string message, Exception cause)
			: base(message, cause)
		{
		}
	}
}
