using Org.Apache.Hadoop.FS.Permission;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>The attributes of an inode.</summary>
	public abstract class INodeAttributes
	{
		public abstract bool IsDirectory();

		/// <returns>
		/// null if the local name is null;
		/// otherwise, return the local name byte array.
		/// </returns>
		public abstract byte[] GetLocalNameBytes();

		/// <returns>the user name.</returns>
		public abstract string GetUserName();

		/// <returns>the group name.</returns>
		public abstract string GetGroupName();

		/// <returns>the permission.</returns>
		public abstract FsPermission GetFsPermission();

		/// <returns>the permission as a short.</returns>
		public abstract short GetFsPermissionShort();

		/// <returns>the permission information as a long.</returns>
		public abstract long GetPermissionLong();

		/// <returns>the ACL feature.</returns>
		public abstract AclFeature GetAclFeature();

		/// <returns>the XAttrs feature.</returns>
		public abstract XAttrFeature GetXAttrFeature();

		/// <returns>the modification time.</returns>
		public abstract long GetModificationTime();

		/// <returns>the access time.</returns>
		public abstract long GetAccessTime();

		/// <summary>A read-only copy of the inode attributes.</summary>
		public abstract class SnapshotCopy : INodeAttributes
		{
			private readonly byte[] name;

			private readonly long permission;

			private readonly AclFeature aclFeature;

			private readonly long modificationTime;

			private readonly long accessTime;

			private XAttrFeature xAttrFeature;

			internal SnapshotCopy(byte[] name, PermissionStatus permissions, AclFeature aclFeature
				, long modificationTime, long accessTime, XAttrFeature xAttrFeature)
			{
				this.name = name;
				this.permission = INodeWithAdditionalFields.PermissionStatusFormat.ToLong(permissions
					);
				if (aclFeature != null)
				{
					aclFeature = AclStorage.AddAclFeature(aclFeature);
				}
				this.aclFeature = aclFeature;
				this.modificationTime = modificationTime;
				this.accessTime = accessTime;
				this.xAttrFeature = xAttrFeature;
			}

			internal SnapshotCopy(INode inode)
			{
				this.name = inode.GetLocalNameBytes();
				this.permission = inode.GetPermissionLong();
				if (inode.GetAclFeature() != null)
				{
					aclFeature = AclStorage.AddAclFeature(inode.GetAclFeature());
				}
				else
				{
					aclFeature = null;
				}
				this.modificationTime = inode.GetModificationTime();
				this.accessTime = inode.GetAccessTime();
				this.xAttrFeature = inode.GetXAttrFeature();
			}

			public sealed override byte[] GetLocalNameBytes()
			{
				return name;
			}

			public sealed override string GetUserName()
			{
				return INodeWithAdditionalFields.PermissionStatusFormat.GetUser(permission);
			}

			public sealed override string GetGroupName()
			{
				return INodeWithAdditionalFields.PermissionStatusFormat.GetGroup(permission);
			}

			public sealed override FsPermission GetFsPermission()
			{
				return new FsPermission(GetFsPermissionShort());
			}

			public sealed override short GetFsPermissionShort()
			{
				return INodeWithAdditionalFields.PermissionStatusFormat.GetMode(permission);
			}

			public override long GetPermissionLong()
			{
				return permission;
			}

			public override AclFeature GetAclFeature()
			{
				return aclFeature;
			}

			public sealed override long GetModificationTime()
			{
				return modificationTime;
			}

			public sealed override long GetAccessTime()
			{
				return accessTime;
			}

			public sealed override XAttrFeature GetXAttrFeature()
			{
				return xAttrFeature;
			}

			public abstract bool IsDirectory();
		}
	}

	public static class INodeAttributesConstants
	{
	}
}
