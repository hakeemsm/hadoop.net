

namespace Org.Apache.Hadoop.Nfs.Nfs3
{
	/// <summary>Success or error status is reported in NFS3 responses.</summary>
	public class Nfs3Status
	{
		/// <summary>Indicates the call completed successfully.</summary>
		public const int Nfs3Ok = 0;

		/// <summary>
		/// The operation was not allowed because the caller is either not a
		/// privileged user (root) or not the owner of the target of the operation.
		/// </summary>
		public const int Nfs3errPerm = 1;

		/// <summary>No such file or directory.</summary>
		/// <remarks>
		/// No such file or directory. The file or directory name specified does not
		/// exist.
		/// </remarks>
		public const int Nfs3errNoent = 2;

		/// <summary>I/O error.</summary>
		/// <remarks>
		/// I/O error. A hard error (for example, a disk error) occurred while
		/// processing the requested operation.
		/// </remarks>
		public const int Nfs3errIo = 5;

		/// <summary>I/O error.</summary>
		/// <remarks>I/O error. No such device or address.</remarks>
		public const int Nfs3errNxio = 6;

		/// <summary>Permission denied.</summary>
		/// <remarks>
		/// Permission denied. The caller does not have the correct permission to
		/// perform the requested operation. Contrast this with NFS3ERR_PERM, which
		/// restricts itself to owner or privileged user permission failures.
		/// </remarks>
		public const int Nfs3errAcces = 13;

		/// <summary>File exists.</summary>
		/// <remarks>File exists. The file specified already exists.</remarks>
		public const int Nfs3errExist = 17;

		/// <summary>Attempt to do a cross-device hard link.</summary>
		public const int Nfs3errXdev = 18;

		/// <summary>No such device.</summary>
		public const int Nfs3errNodev = 19;

		/// <summary>The caller specified a non-directory in a directory operation.</summary>
		public static int Nfs3errNotdir = 20;

		/// <summary>The caller specified a directory in a non-directory operation.</summary>
		public const int Nfs3errIsdir = 21;

		/// <summary>Invalid argument or unsupported argument for an operation.</summary>
		/// <remarks>
		/// Invalid argument or unsupported argument for an operation. Two examples are
		/// attempting a READLINK on an object other than a symbolic link or attempting
		/// to SETATTR a time field on a server that does not support this operation.
		/// </remarks>
		public const int Nfs3errInval = 22;

		/// <summary>File too large.</summary>
		/// <remarks>
		/// File too large. The operation would have caused a file to grow beyond the
		/// server's limit.
		/// </remarks>
		public const int Nfs3errFbig = 27;

		/// <summary>No space left on device.</summary>
		/// <remarks>
		/// No space left on device. The operation would have caused the server's file
		/// system to exceed its limit.
		/// </remarks>
		public const int Nfs3errNospc = 28;

		/// <summary>Read-only file system.</summary>
		/// <remarks>
		/// Read-only file system. A modifying operation was attempted on a read-only
		/// file system.
		/// </remarks>
		public const int Nfs3errRofs = 30;

		/// <summary>Too many hard links.</summary>
		public const int Nfs3errMlink = 31;

		/// <summary>The filename in an operation was too long.</summary>
		public const int Nfs3errNametoolong = 63;

		/// <summary>An attempt was made to remove a directory that was not empty.</summary>
		public const int Nfs3errNotempty = 66;

		/// <summary>Resource (quota) hard limit exceeded.</summary>
		/// <remarks>
		/// Resource (quota) hard limit exceeded. The user's resource limit on the
		/// server has been exceeded.
		/// </remarks>
		public const int Nfs3errDquot = 69;

		/// <summary>The file handle given in the arguments was invalid.</summary>
		/// <remarks>
		/// The file handle given in the arguments was invalid. The file referred to by
		/// that file handle no longer exists or access to it has been revoked.
		/// </remarks>
		public const int Nfs3errStale = 70;

		/// <summary>
		/// The file handle given in the arguments referred to a file on a non-local
		/// file system on the server.
		/// </summary>
		public const int Nfs3errRemote = 71;

		/// <summary>The file handle failed internal consistency checks</summary>
		public const int Nfs3errBadhandle = 10001;

		/// <summary>Update synchronization mismatch was detected during a SETATTR operation.
		/// 	</summary>
		public const int Nfs3errNotSync = 10002;

		/// <summary>READDIR or READDIRPLUS cookie is stale</summary>
		public const int Nfs3errBadCookie = 10003;

		/// <summary>Operation is not supported</summary>
		public const int Nfs3errNotsupp = 10004;

		/// <summary>Buffer or request is too small</summary>
		public const int Nfs3errToosmall = 10005;

		/// <summary>
		/// An error occurred on the server which does not map to any of the legal NFS
		/// version 3 protocol error values.
		/// </summary>
		/// <remarks>
		/// An error occurred on the server which does not map to any of the legal NFS
		/// version 3 protocol error values. The client should translate this into an
		/// appropriate error. UNIX clients may choose to translate this to EIO.
		/// </remarks>
		public const int Nfs3errServerfault = 10006;

		/// <summary>
		/// An attempt was made to create an object of a type not supported by the
		/// server.
		/// </summary>
		public const int Nfs3errBadtype = 10007;

		/// <summary>
		/// The server initiated the request, but was not able to complete it in a
		/// timely fashion.
		/// </summary>
		/// <remarks>
		/// The server initiated the request, but was not able to complete it in a
		/// timely fashion. The client should wait and then try the request with a new
		/// RPC transaction ID. For example, this error should be returned from a
		/// server that supports hierarchical storage and receives a request to process
		/// a file that has been migrated. In this case, the server should start the
		/// immigration process and respond to client with this error.
		/// </remarks>
		public const int Nfs3errJukebox = 10008;
	}
}
