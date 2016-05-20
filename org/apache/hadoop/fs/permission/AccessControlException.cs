using Sharpen;

namespace org.apache.hadoop.fs.permission
{
	/// <summary>An exception class for access control related issues.</summary>
	[System.Serializable]
	[System.ObsoleteAttribute(@"Use org.apache.hadoop.security.AccessControlException instead."
		)]
	public class AccessControlException : System.IO.IOException
	{
		private const long serialVersionUID = 1L;

		/// <summary>
		/// Default constructor is needed for unwrapping from
		/// <see cref="org.apache.hadoop.ipc.RemoteException"/>
		/// .
		/// </summary>
		public AccessControlException()
			: base("Permission denied.")
		{
		}

		/// <summary>
		/// Constructs an
		/// <see cref="AccessControlException"/>
		/// with the specified detail message.
		/// </summary>
		/// <param name="s">the detail message.</param>
		public AccessControlException(string s)
			: base(s)
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
		public AccessControlException(System.Exception cause)
			: base(cause)
		{
		}
		//Required by {@link java.io.Serializable}.
	}
}
