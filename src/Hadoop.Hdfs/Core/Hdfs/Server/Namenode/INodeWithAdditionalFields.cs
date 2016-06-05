using System;
using Com.Google.Common.Base;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs.Util;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>
	/// <see cref="INode"/>
	/// with additional fields including id, name, permission,
	/// access time and modification time.
	/// </summary>
	public abstract class INodeWithAdditionalFields : INode, LightWeightGSet.LinkedElement
	{
		[System.Serializable]
		internal sealed class PermissionStatusFormat
		{
			public static readonly INodeWithAdditionalFields.PermissionStatusFormat Mode = new 
				INodeWithAdditionalFields.PermissionStatusFormat(null, 16);

			public static readonly INodeWithAdditionalFields.PermissionStatusFormat Group = new 
				INodeWithAdditionalFields.PermissionStatusFormat(INodeWithAdditionalFields.PermissionStatusFormat
				.Mode.Bits, 25);

			public static readonly INodeWithAdditionalFields.PermissionStatusFormat User = new 
				INodeWithAdditionalFields.PermissionStatusFormat(INodeWithAdditionalFields.PermissionStatusFormat
				.Group.Bits, 23);

			internal readonly LongBitFormat Bits;

			private PermissionStatusFormat(LongBitFormat previous, int length)
			{
				INodeWithAdditionalFields.PermissionStatusFormat.Bits = new LongBitFormat(Name(), 
					previous, length, 0);
			}

			internal static string GetUser(long permission)
			{
				int n = (int)INodeWithAdditionalFields.PermissionStatusFormat.User.Bits.Retrieve(
					permission);
				return SerialNumberManager.Instance.GetUser(n);
			}

			internal static string GetGroup(long permission)
			{
				int n = (int)INodeWithAdditionalFields.PermissionStatusFormat.Group.Bits.Retrieve
					(permission);
				return SerialNumberManager.Instance.GetGroup(n);
			}

			internal static short GetMode(long permission)
			{
				return (short)INodeWithAdditionalFields.PermissionStatusFormat.Mode.Bits.Retrieve
					(permission);
			}

			/// <summary>
			/// Encode the
			/// <see cref="Org.Apache.Hadoop.FS.Permission.PermissionStatus"/>
			/// to a long.
			/// </summary>
			internal static long ToLong(PermissionStatus ps)
			{
				long permission = 0L;
				int user = SerialNumberManager.Instance.GetUserSerialNumber(ps.GetUserName());
				permission = INodeWithAdditionalFields.PermissionStatusFormat.User.Bits.Combine(user
					, permission);
				int group = SerialNumberManager.Instance.GetGroupSerialNumber(ps.GetGroupName());
				permission = INodeWithAdditionalFields.PermissionStatusFormat.Group.Bits.Combine(
					group, permission);
				int mode = ps.GetPermission().ToShort();
				permission = INodeWithAdditionalFields.PermissionStatusFormat.Mode.Bits.Combine(mode
					, permission);
				return permission;
			}
		}

		/// <summary>The inode id.</summary>
		private readonly long id;

		/// <summary>
		/// The inode name is in java UTF8 encoding;
		/// The name in HdfsFileStatus should keep the same encoding as this.
		/// </summary>
		/// <remarks>
		/// The inode name is in java UTF8 encoding;
		/// The name in HdfsFileStatus should keep the same encoding as this.
		/// if this encoding is changed, implicitly getFileInfo and listStatus in
		/// clientProtocol are changed; The decoding at the client
		/// side should change accordingly.
		/// </remarks>
		private byte[] name = null;

		/// <summary>
		/// Permission encoded using
		/// <see cref="PermissionStatusFormat"/>
		/// .
		/// Codes other than
		/// <see cref="ClonePermissionStatus(INodeWithAdditionalFields)"/>
		/// and
		/// <see cref="UpdatePermissionStatus(PermissionStatusFormat, long)"/>
		/// should not modify it.
		/// </summary>
		private long permission = 0L;

		/// <summary>The last modification time</summary>
		private long modificationTime = 0L;

		/// <summary>The last access time</summary>
		private long accessTime = 0L;

		/// <summary>
		/// For implementing
		/// <see cref="Org.Apache.Hadoop.Util.LightWeightGSet.LinkedElement"/>
		/// .
		/// </summary>
		private LightWeightGSet.LinkedElement next = null;

		/// <summary>
		/// An array
		/// <see cref="Feature"/>
		/// s.
		/// </summary>
		private static readonly INode.Feature[] EmptyFeature = new INode.Feature[0];

		protected internal INode.Feature[] features = EmptyFeature;

		private INodeWithAdditionalFields(INode parent, long id, byte[] name, long permission
			, long modificationTime, long accessTime)
			: base(parent)
		{
			this.id = id;
			this.name = name;
			this.permission = permission;
			this.modificationTime = modificationTime;
			this.accessTime = accessTime;
		}

		internal INodeWithAdditionalFields(long id, byte[] name, PermissionStatus permissions
			, long modificationTime, long accessTime)
			: this(null, id, name, INodeWithAdditionalFields.PermissionStatusFormat.ToLong(permissions
				), modificationTime, accessTime)
		{
		}

		/// <param name="other">Other node to be copied</param>
		internal INodeWithAdditionalFields(INodeWithAdditionalFields other)
			: this(other.GetParentReference() != null ? other.GetParentReference() : other.GetParent
				(), other.GetId(), other.GetLocalNameBytes(), other.permission, other.modificationTime
				, other.accessTime)
		{
		}

		public virtual void SetNext(LightWeightGSet.LinkedElement next)
		{
			this.next = next;
		}

		public virtual LightWeightGSet.LinkedElement GetNext()
		{
			return next;
		}

		/// <summary>Get inode id</summary>
		public sealed override long GetId()
		{
			return this.id;
		}

		public sealed override byte[] GetLocalNameBytes()
		{
			return name;
		}

		public sealed override void SetLocalName(byte[] name)
		{
			this.name = name;
		}

		/// <summary>
		/// Clone the
		/// <see cref="Org.Apache.Hadoop.FS.Permission.PermissionStatus"/>
		/// .
		/// </summary>
		internal void ClonePermissionStatus(INodeWithAdditionalFields that)
		{
			this.permission = that.permission;
		}

		internal sealed override PermissionStatus GetPermissionStatus(int snapshotId)
		{
			return new PermissionStatus(GetUserName(snapshotId), GetGroupName(snapshotId), GetFsPermission
				(snapshotId));
		}

		private void UpdatePermissionStatus(INodeWithAdditionalFields.PermissionStatusFormat
			 f, long n)
		{
			this.permission = f.Bits.Combine(n, permission);
		}

		internal sealed override string GetUserName(int snapshotId)
		{
			if (snapshotId != Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.CurrentStateId)
			{
				return GetSnapshotINode(snapshotId).GetUserName();
			}
			return INodeWithAdditionalFields.PermissionStatusFormat.GetUser(permission);
		}

		internal sealed override void SetUser(string user)
		{
			int n = SerialNumberManager.Instance.GetUserSerialNumber(user);
			UpdatePermissionStatus(INodeWithAdditionalFields.PermissionStatusFormat.User, n);
		}

		internal sealed override string GetGroupName(int snapshotId)
		{
			if (snapshotId != Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.CurrentStateId)
			{
				return GetSnapshotINode(snapshotId).GetGroupName();
			}
			return INodeWithAdditionalFields.PermissionStatusFormat.GetGroup(permission);
		}

		internal sealed override void SetGroup(string group)
		{
			int n = SerialNumberManager.Instance.GetGroupSerialNumber(group);
			UpdatePermissionStatus(INodeWithAdditionalFields.PermissionStatusFormat.Group, n);
		}

		internal sealed override FsPermission GetFsPermission(int snapshotId)
		{
			if (snapshotId != Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.CurrentStateId)
			{
				return GetSnapshotINode(snapshotId).GetFsPermission();
			}
			return new FsPermission(GetFsPermissionShort());
		}

		public sealed override short GetFsPermissionShort()
		{
			return INodeWithAdditionalFields.PermissionStatusFormat.GetMode(permission);
		}

		internal override void SetPermission(FsPermission permission)
		{
			short mode = permission.ToShort();
			UpdatePermissionStatus(INodeWithAdditionalFields.PermissionStatusFormat.Mode, mode
				);
		}

		public override long GetPermissionLong()
		{
			return permission;
		}

		internal sealed override AclFeature GetAclFeature(int snapshotId)
		{
			if (snapshotId != Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.CurrentStateId)
			{
				return GetSnapshotINode(snapshotId).GetAclFeature();
			}
			return GetFeature(typeof(AclFeature));
		}

		internal sealed override long GetModificationTime(int snapshotId)
		{
			if (snapshotId != Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.CurrentStateId)
			{
				return GetSnapshotINode(snapshotId).GetModificationTime();
			}
			return this.modificationTime;
		}

		/// <summary>Update modification time if it is larger than the current value.</summary>
		public sealed override INode UpdateModificationTime(long mtime, int latestSnapshotId
			)
		{
			Preconditions.CheckState(IsDirectory());
			if (mtime <= modificationTime)
			{
				return this;
			}
			return SetModificationTime(mtime, latestSnapshotId);
		}

		internal void CloneModificationTime(INodeWithAdditionalFields that)
		{
			this.modificationTime = that.modificationTime;
		}

		public sealed override void SetModificationTime(long modificationTime)
		{
			this.modificationTime = modificationTime;
		}

		internal sealed override long GetAccessTime(int snapshotId)
		{
			if (snapshotId != Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.CurrentStateId)
			{
				return GetSnapshotINode(snapshotId).GetAccessTime();
			}
			return accessTime;
		}

		/// <summary>Set last access time of inode.</summary>
		public sealed override void SetAccessTime(long accessTime)
		{
			this.accessTime = accessTime;
		}

		protected internal virtual void AddFeature(INode.Feature f)
		{
			int size = features.Length;
			INode.Feature[] arr = new INode.Feature[size + 1];
			if (size != 0)
			{
				System.Array.Copy(features, 0, arr, 0, size);
			}
			arr[size] = f;
			features = arr;
		}

		protected internal virtual void RemoveFeature(INode.Feature f)
		{
			int size = features.Length;
			Preconditions.CheckState(size > 0, "Feature " + f.GetType().Name + " not found.");
			if (size == 1)
			{
				Preconditions.CheckState(features[0] == f, "Feature " + f.GetType().Name + " not found."
					);
				features = EmptyFeature;
				return;
			}
			INode.Feature[] arr = new INode.Feature[size - 1];
			int j = 0;
			bool overflow = false;
			foreach (INode.Feature f1 in features)
			{
				if (f1 != f)
				{
					if (j == size - 1)
					{
						overflow = true;
						break;
					}
					else
					{
						arr[j++] = f1;
					}
				}
			}
			Preconditions.CheckState(!overflow && j == size - 1, "Feature " + f.GetType().Name
				 + " not found.");
			features = arr;
		}

		protected internal virtual T GetFeature<T>(Type clazz)
			where T : INode.Feature
		{
			Preconditions.CheckArgument(clazz != null);
			foreach (INode.Feature f in features)
			{
				if (clazz.IsAssignableFrom(f.GetType()))
				{
					T ret = (T)f;
					return ret;
				}
			}
			return null;
		}

		internal override void RemoveAclFeature()
		{
			AclFeature f = GetAclFeature();
			Preconditions.CheckNotNull(f);
			RemoveFeature(f);
			AclStorage.RemoveAclFeature(f);
		}

		internal override void AddAclFeature(AclFeature f)
		{
			AclFeature f1 = GetAclFeature();
			if (f1 != null)
			{
				throw new InvalidOperationException("Duplicated ACLFeature");
			}
			AddFeature(AclStorage.AddAclFeature(f));
		}

		internal override XAttrFeature GetXAttrFeature(int snapshotId)
		{
			if (snapshotId != Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.CurrentStateId)
			{
				return GetSnapshotINode(snapshotId).GetXAttrFeature();
			}
			return GetFeature(typeof(XAttrFeature));
		}

		internal override void RemoveXAttrFeature()
		{
			XAttrFeature f = GetXAttrFeature();
			Preconditions.CheckNotNull(f);
			RemoveFeature(f);
		}

		internal override void AddXAttrFeature(XAttrFeature f)
		{
			XAttrFeature f1 = GetXAttrFeature();
			Preconditions.CheckState(f1 == null, "Duplicated XAttrFeature");
			AddFeature(f);
		}

		public INode.Feature[] GetFeatures()
		{
			return features;
		}
	}
}
