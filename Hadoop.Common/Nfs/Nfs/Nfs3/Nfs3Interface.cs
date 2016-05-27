using Org.Apache.Hadoop.Nfs.Nfs3.Response;
using Org.Apache.Hadoop.Oncrpc;
using Sharpen;

namespace Org.Apache.Hadoop.Nfs.Nfs3
{
	/// <summary>RPC procedures as defined in RFC 1813.</summary>
	public interface Nfs3Interface
	{
		/// <summary>NULL: Do nothing</summary>
		NFS3Response NullProcedure();

		/// <summary>GETATTR: Get file attributes</summary>
		NFS3Response Getattr(XDR xdr, RpcInfo info);

		/// <summary>SETATTR: Set file attributes</summary>
		NFS3Response Setattr(XDR xdr, RpcInfo info);

		/// <summary>LOOKUP: Lookup filename</summary>
		NFS3Response Lookup(XDR xdr, RpcInfo info);

		/// <summary>ACCESS: Check access permission</summary>
		NFS3Response Access(XDR xdr, RpcInfo info);

		/// <summary>READLINK: Read from symbolic link</summary>
		NFS3Response Readlink(XDR xdr, RpcInfo info);

		/// <summary>READ: Read from file</summary>
		NFS3Response Read(XDR xdr, RpcInfo info);

		/// <summary>WRITE: Write to file</summary>
		NFS3Response Write(XDR xdr, RpcInfo info);

		/// <summary>CREATE: Create a file</summary>
		NFS3Response Create(XDR xdr, RpcInfo info);

		/// <summary>MKDIR: Create a directory</summary>
		NFS3Response Mkdir(XDR xdr, RpcInfo info);

		/// <summary>SYMLINK: Create a symbolic link</summary>
		NFS3Response Symlink(XDR xdr, RpcInfo info);

		/// <summary>MKNOD: Create a special device</summary>
		NFS3Response Mknod(XDR xdr, RpcInfo info);

		/// <summary>REMOVE: Remove a file</summary>
		NFS3Response Remove(XDR xdr, RpcInfo info);

		/// <summary>RMDIR: Remove a directory</summary>
		NFS3Response Rmdir(XDR xdr, RpcInfo info);

		/// <summary>RENAME: Rename a file or directory</summary>
		NFS3Response Rename(XDR xdr, RpcInfo info);

		/// <summary>LINK: create link to an object</summary>
		NFS3Response Link(XDR xdr, RpcInfo info);

		/// <summary>READDIR: Read From directory</summary>
		NFS3Response Readdir(XDR xdr, RpcInfo info);

		/// <summary>READDIRPLUS: Extended read from directory</summary>
		NFS3Response Readdirplus(XDR xdr, RpcInfo info);

		/// <summary>FSSTAT: Get dynamic file system information</summary>
		NFS3Response Fsstat(XDR xdr, RpcInfo info);

		/// <summary>FSINFO: Get static file system information</summary>
		NFS3Response Fsinfo(XDR xdr, RpcInfo info);

		/// <summary>PATHCONF: Retrieve POSIX information</summary>
		NFS3Response Pathconf(XDR xdr, RpcInfo info);

		/// <summary>COMMIT: Commit cached data on a server to stable storage</summary>
		NFS3Response Commit(XDR xdr, RpcInfo info);
	}
}
