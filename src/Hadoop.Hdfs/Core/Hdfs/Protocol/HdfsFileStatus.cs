using System.Text;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Protocol
{
	/// <summary>Interface that represents the over the wire information for a file.</summary>
	public class HdfsFileStatus
	{
		private readonly byte[] path;

		private readonly byte[] symlink;

		private readonly long length;

		private readonly bool isdir;

		private readonly short block_replication;

		private readonly long blocksize;

		private readonly long modification_time;

		private readonly long access_time;

		private readonly FsPermission permission;

		private readonly string owner;

		private readonly string group;

		private readonly long fileId;

		private readonly FileEncryptionInfo feInfo;

		private readonly int childrenNum;

		private readonly byte storagePolicy;

		public static readonly byte[] EmptyName = new byte[0];

		/// <summary>Constructor</summary>
		/// <param name="length">the number of bytes the file has</param>
		/// <param name="isdir">if the path is a directory</param>
		/// <param name="block_replication">the replication factor</param>
		/// <param name="blocksize">the block size</param>
		/// <param name="modification_time">modification time</param>
		/// <param name="access_time">access time</param>
		/// <param name="permission">permission</param>
		/// <param name="owner">the owner of the path</param>
		/// <param name="group">the group of the path</param>
		/// <param name="path">the local name in java UTF8 encoding the same as that in-memory
		/// 	</param>
		/// <param name="fileId">the file id</param>
		/// <param name="feInfo">the file's encryption info</param>
		public HdfsFileStatus(long length, bool isdir, int block_replication, long blocksize
			, long modification_time, long access_time, FsPermission permission, string owner
			, string group, byte[] symlink, byte[] path, long fileId, int childrenNum, FileEncryptionInfo
			 feInfo, byte storagePolicy)
		{
			// local name of the inode that's encoded in java UTF8
			// symlink target encoded in java UTF8 or null
			// Used by dir, not including dot and dotdot. Always zero for a regular file.
			this.length = length;
			this.isdir = isdir;
			this.block_replication = (short)block_replication;
			this.blocksize = blocksize;
			this.modification_time = modification_time;
			this.access_time = access_time;
			this.permission = (permission == null) ? ((isdir || symlink != null) ? FsPermission
				.GetDefault() : FsPermission.GetFileDefault()) : permission;
			this.owner = (owner == null) ? string.Empty : owner;
			this.group = (group == null) ? string.Empty : group;
			this.symlink = symlink;
			this.path = path;
			this.fileId = fileId;
			this.childrenNum = childrenNum;
			this.feInfo = feInfo;
			this.storagePolicy = storagePolicy;
		}

		/// <summary>Get the length of this file, in bytes.</summary>
		/// <returns>the length of this file, in bytes.</returns>
		public long GetLen()
		{
			return length;
		}

		/// <summary>Is this a directory?</summary>
		/// <returns>true if this is a directory</returns>
		public bool IsDir()
		{
			return isdir;
		}

		/// <summary>Is this a symbolic link?</summary>
		/// <returns>true if this is a symbolic link</returns>
		public virtual bool IsSymlink()
		{
			return symlink != null;
		}

		/// <summary>Get the block size of the file.</summary>
		/// <returns>the number of bytes</returns>
		public long GetBlockSize()
		{
			return blocksize;
		}

		/// <summary>Get the replication factor of a file.</summary>
		/// <returns>the replication factor of a file.</returns>
		public short GetReplication()
		{
			return block_replication;
		}

		/// <summary>Get the modification time of the file.</summary>
		/// <returns>the modification time of file in milliseconds since January 1, 1970 UTC.
		/// 	</returns>
		public long GetModificationTime()
		{
			return modification_time;
		}

		/// <summary>Get the access time of the file.</summary>
		/// <returns>the access time of file in milliseconds since January 1, 1970 UTC.</returns>
		public long GetAccessTime()
		{
			return access_time;
		}

		/// <summary>Get FsPermission associated with the file.</summary>
		/// <returns>permssion</returns>
		public FsPermission GetPermission()
		{
			return permission;
		}

		/// <summary>Get the owner of the file.</summary>
		/// <returns>owner of the file</returns>
		public string GetOwner()
		{
			return owner;
		}

		/// <summary>Get the group associated with the file.</summary>
		/// <returns>group for the file.</returns>
		public string GetGroup()
		{
			return group;
		}

		/// <summary>Check if the local name is empty</summary>
		/// <returns>true if the name is empty</returns>
		public bool IsEmptyLocalName()
		{
			return path.Length == 0;
		}

		/// <summary>Get the string representation of the local name</summary>
		/// <returns>the local name in string</returns>
		public string GetLocalName()
		{
			return DFSUtil.Bytes2String(path);
		}

		/// <summary>Get the Java UTF8 representation of the local name</summary>
		/// <returns>the local name in java UTF8</returns>
		public byte[] GetLocalNameInBytes()
		{
			return path;
		}

		/// <summary>Get the string representation of the full path name</summary>
		/// <param name="parent">the parent path</param>
		/// <returns>the full path in string</returns>
		public string GetFullName(string parent)
		{
			if (IsEmptyLocalName())
			{
				return parent;
			}
			StringBuilder fullName = new StringBuilder(parent);
			if (!parent.EndsWith(Path.Separator))
			{
				fullName.Append(Path.Separator);
			}
			fullName.Append(GetLocalName());
			return fullName.ToString();
		}

		/// <summary>Get the full path</summary>
		/// <param name="parent">the parent path</param>
		/// <returns>the full path</returns>
		public Path GetFullPath(Path parent)
		{
			if (IsEmptyLocalName())
			{
				return parent;
			}
			return new Path(parent, GetLocalName());
		}

		/// <summary>Get the string representation of the symlink.</summary>
		/// <returns>the symlink as a string.</returns>
		public string GetSymlink()
		{
			return DFSUtil.Bytes2String(symlink);
		}

		public byte[] GetSymlinkInBytes()
		{
			return symlink;
		}

		public long GetFileId()
		{
			return fileId;
		}

		public FileEncryptionInfo GetFileEncryptionInfo()
		{
			return feInfo;
		}

		public int GetChildrenNum()
		{
			return childrenNum;
		}

		/// <returns>the storage policy id</returns>
		public byte GetStoragePolicy()
		{
			return storagePolicy;
		}

		public FileStatus MakeQualified(URI defaultUri, Path path)
		{
			return new FileStatus(GetLen(), IsDir(), GetReplication(), GetBlockSize(), GetModificationTime
				(), GetAccessTime(), GetPermission(), GetOwner(), GetGroup(), IsSymlink() ? new 
				Path(GetSymlink()) : null, (GetFullPath(path)).MakeQualified(defaultUri, null));
		}
		// fully-qualify path
	}
}
