using System;
using System.IO;


namespace Org.Apache.Hadoop.FS.Permission
{
	/// <summary>An exception class for access control related issues.</summary>
	[System.Serializable]
	[System.ObsoleteAttribute(@"Use Org.Apache.Hadoop.Security.AccessControlException instead."
		)]
	public class AccessControlException : IOException
	{
		private const long serialVersionUID = 1L;

		/// <summary>
		/// Default constructor is needed for unwrapping from
		/// <see cref="Org.Apache.Hadoop.Ipc.RemoteException"/>
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
		public AccessControlException(Exception cause)
			: base(cause)
		{
		}
		//Required by {@link java.io.Serializable}.
	}
}
