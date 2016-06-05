using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Protocol
{
	/// <summary>
	/// Interface that represents the over the wire information
	/// including block locations for a file.
	/// </summary>
	public class HdfsLocatedFileStatus : HdfsFileStatus
	{
		private readonly LocatedBlocks locations;

		/// <summary>Constructor</summary>
		/// <param name="length">size</param>
		/// <param name="isdir">if this is directory</param>
		/// <param name="block_replication">the file's replication factor</param>
		/// <param name="blocksize">the file's block size</param>
		/// <param name="modification_time">most recent modification time</param>
		/// <param name="access_time">most recent access time</param>
		/// <param name="permission">permission</param>
		/// <param name="owner">owner</param>
		/// <param name="group">group</param>
		/// <param name="symlink">symbolic link</param>
		/// <param name="path">local path name in java UTF8 format</param>
		/// <param name="fileId">the file id</param>
		/// <param name="locations">block locations</param>
		/// <param name="feInfo">file encryption info</param>
		public HdfsLocatedFileStatus(long length, bool isdir, int block_replication, long
			 blocksize, long modification_time, long access_time, FsPermission permission, string
			 owner, string group, byte[] symlink, byte[] path, long fileId, LocatedBlocks locations
			, int childrenNum, FileEncryptionInfo feInfo, byte storagePolicy)
			: base(length, isdir, block_replication, blocksize, modification_time, access_time
				, permission, owner, group, symlink, path, fileId, childrenNum, feInfo, storagePolicy
				)
		{
			this.locations = locations;
		}

		public virtual LocatedBlocks GetBlockLocations()
		{
			return locations;
		}

		public LocatedFileStatus MakeQualifiedLocated(URI defaultUri, Path path)
		{
			return new LocatedFileStatus(GetLen(), IsDir(), GetReplication(), GetBlockSize(), 
				GetModificationTime(), GetAccessTime(), GetPermission(), GetOwner(), GetGroup(), 
				IsSymlink() ? new Path(GetSymlink()) : null, (GetFullPath(path)).MakeQualified(defaultUri
				, null), DFSUtil.LocatedBlocks2Locations(GetBlockLocations()));
		}
		// fully-qualify path
	}
}
