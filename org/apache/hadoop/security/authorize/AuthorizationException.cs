using Sharpen;

namespace org.apache.hadoop.security.authorize
{
	/// <summary>An exception class for authorization-related issues.</summary>
	/// <remarks>
	/// An exception class for authorization-related issues.
	/// This class <em>does not</em> provide the stack trace for security purposes.
	/// </remarks>
	[System.Serializable]
	public class AuthorizationException : org.apache.hadoop.security.AccessControlException
	{
		private const long serialVersionUID = 1L;

		public AuthorizationException()
			: base()
		{
		}

		public AuthorizationException(string message)
			: base(message)
		{
		}

		/// <summary>
		/// Constructs a new exception with the specified cause and a detail
		/// message of <tt>(cause==null ? null : cause.toString())</tt> (which
		/// typically contains the class and detail message of <tt>cause</tt>).
		/// </summary>
		/// <param name="cause">
		/// the cause (which is saved for later retrieval by the
		/// <see cref="System.Exception.InnerException()"/>
		/// method).  (A <tt>null</tt> value is
		/// permitted, and indicates that the cause is nonexistent or
		/// unknown.)
		/// </param>
		public AuthorizationException(System.Exception cause)
			: base(cause)
		{
		}

		private static java.lang.StackTraceElement[] stackTrace = new java.lang.StackTraceElement
			[0];

		public override java.lang.StackTraceElement[] getStackTrace()
		{
			// Do not provide the stack-trace
			return stackTrace;
		}

		public override void printStackTrace()
		{
		}

		// Do not provide the stack-trace
		public override void printStackTrace(System.IO.TextWriter s)
		{
		}

		// Do not provide the stack-trace
		public override void printStackTrace(java.io.PrintWriter s)
		{
		}
		// Do not provide the stack-trace
	}
}
