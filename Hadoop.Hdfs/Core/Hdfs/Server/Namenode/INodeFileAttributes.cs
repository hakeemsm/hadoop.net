using Org.Apache.Hadoop.FS.Permission;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>The attributes of a file.</summary>
	public abstract class INodeFileAttributes : INodeAttributes
	{
		/// <returns>the file replication.</returns>
		public abstract short GetFileReplication();

		/// <returns>preferred block size in bytes</returns>
		public abstract long GetPreferredBlockSize();

		/// <returns>the header as a long.</returns>
		public abstract long GetHeaderLong();

		public abstract bool MetadataEquals(INodeFileAttributes other);

		public abstract byte GetLocalStoragePolicyID();

		/// <summary>A copy of the inode file attributes</summary>
		public class SnapshotCopy : INodeAttributes.SnapshotCopy, INodeFileAttributes
		{
			private readonly long header;

			public SnapshotCopy(byte[] name, PermissionStatus permissions, AclFeature aclFeature
				, long modificationTime, long accessTime, short replication, long preferredBlockSize
				, byte storagePolicyID, XAttrFeature xAttrsFeature)
				: base(name, permissions, aclFeature, modificationTime, accessTime, xAttrsFeature
					)
			{
				header = INodeFile.HeaderFormat.ToLong(preferredBlockSize, replication, storagePolicyID
					);
			}

			public SnapshotCopy(INodeFile file)
				: base(file)
			{
				this.header = file.GetHeaderLong();
			}

			public override bool IsDirectory()
			{
				return false;
			}

			public override short GetFileReplication()
			{
				return INodeFile.HeaderFormat.GetReplication(header);
			}

			public override long GetPreferredBlockSize()
			{
				return INodeFile.HeaderFormat.GetPreferredBlockSize(header);
			}

			public override byte GetLocalStoragePolicyID()
			{
				return INodeFile.HeaderFormat.GetStoragePolicyID(header);
			}

			public override long GetHeaderLong()
			{
				return header;
			}

			public override bool MetadataEquals(INodeFileAttributes other)
			{
				return other != null && GetHeaderLong() == other.GetHeaderLong() && GetPermissionLong
					() == other.GetPermissionLong() && GetAclFeature() == other.GetAclFeature() && GetXAttrFeature
					() == other.GetXAttrFeature();
			}
		}
	}

	public static class INodeFileAttributesConstants
	{
	}
}
