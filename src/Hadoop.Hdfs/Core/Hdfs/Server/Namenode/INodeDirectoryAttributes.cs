using Com.Google.Common.Base;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>The attributes of an inode.</summary>
	public abstract class INodeDirectoryAttributes : INodeAttributes
	{
		public abstract QuotaCounts GetQuotaCounts();

		public abstract bool MetadataEquals(INodeDirectoryAttributes other);

		/// <summary>A copy of the inode directory attributes</summary>
		public class SnapshotCopy : INodeAttributes.SnapshotCopy, INodeDirectoryAttributes
		{
			public SnapshotCopy(byte[] name, PermissionStatus permissions, AclFeature aclFeature
				, long modificationTime, XAttrFeature xAttrsFeature)
				: base(name, permissions, aclFeature, modificationTime, 0L, xAttrsFeature)
			{
			}

			public SnapshotCopy(INodeDirectory dir)
				: base(dir)
			{
			}

			public override QuotaCounts GetQuotaCounts()
			{
				return new QuotaCounts.Builder().NameSpace(-1).StorageSpace(-1).TypeSpaces(-1).Build
					();
			}

			public override bool IsDirectory()
			{
				return true;
			}

			public override bool MetadataEquals(INodeDirectoryAttributes other)
			{
				return other != null && GetQuotaCounts().Equals(other.GetQuotaCounts()) && GetPermissionLong
					() == other.GetPermissionLong() && GetAclFeature() == other.GetAclFeature() && GetXAttrFeature
					() == other.GetXAttrFeature();
			}
		}

		public class CopyWithQuota : INodeDirectoryAttributes.SnapshotCopy
		{
			private QuotaCounts quota;

			public CopyWithQuota(byte[] name, PermissionStatus permissions, AclFeature aclFeature
				, long modificationTime, long nsQuota, long dsQuota, EnumCounters<StorageType> typeQuotas
				, XAttrFeature xAttrsFeature)
				: base(name, permissions, aclFeature, modificationTime, xAttrsFeature)
			{
				this.quota = new QuotaCounts.Builder().NameSpace(nsQuota).StorageSpace(dsQuota).TypeSpaces
					(typeQuotas).Build();
			}

			public CopyWithQuota(INodeDirectory dir)
				: base(dir)
			{
				Preconditions.CheckArgument(dir.IsQuotaSet());
				QuotaCounts q = dir.GetQuotaCounts();
				this.quota = new QuotaCounts.Builder().QuotaCount(q).Build();
			}

			public override QuotaCounts GetQuotaCounts()
			{
				return new QuotaCounts.Builder().QuotaCount(quota).Build();
			}
		}
	}

	public static class INodeDirectoryAttributesConstants
	{
	}
}
