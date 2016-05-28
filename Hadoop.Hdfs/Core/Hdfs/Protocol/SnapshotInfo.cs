using Org.Apache.Hadoop.Hdfs.Protocol.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Protocol
{
	/// <summary>SnapshotInfo maintains information for a snapshot</summary>
	public class SnapshotInfo
	{
		private readonly string snapshotName;

		private readonly string snapshotRoot;

		private readonly string createTime;

		private readonly HdfsProtos.FsPermissionProto permission;

		private readonly string owner;

		private readonly string group;

		public SnapshotInfo(string sname, string sroot, string ctime, HdfsProtos.FsPermissionProto
			 permission, string owner, string group)
		{
			this.snapshotName = sname;
			this.snapshotRoot = sroot;
			this.createTime = ctime;
			this.permission = permission;
			this.owner = owner;
			this.group = group;
		}

		public string GetSnapshotName()
		{
			return snapshotName;
		}

		public string GetSnapshotRoot()
		{
			return snapshotRoot;
		}

		public string GetCreateTime()
		{
			return createTime;
		}

		public HdfsProtos.FsPermissionProto GetPermission()
		{
			return permission;
		}

		public string GetOwner()
		{
			return owner;
		}

		public string GetGroup()
		{
			return group;
		}

		public override string ToString()
		{
			return GetType().Name + "{snapshotName=" + snapshotName + "; snapshotRoot=" + snapshotRoot
				 + "; createTime=" + createTime + "; permission=" + permission + "; owner=" + owner
				 + "; group=" + group + "}";
		}

		public class Bean
		{
			private readonly string snapshotID;

			private readonly string snapshotDirectory;

			private readonly long modificationTime;

			public Bean(string snapshotID, string snapshotDirectory, long modificationTime)
			{
				this.snapshotID = snapshotID;
				this.snapshotDirectory = snapshotDirectory;
				this.modificationTime = modificationTime;
			}

			public virtual string GetSnapshotID()
			{
				return snapshotID;
			}

			public virtual string GetSnapshotDirectory()
			{
				return snapshotDirectory;
			}

			public virtual long GetModificationTime()
			{
				return modificationTime;
			}
		}
	}
}
