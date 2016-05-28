using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Protocol
{
	/// <summary>Metadata about a snapshottable directory</summary>
	public class SnapshottableDirectoryStatus
	{
		private sealed class _IComparer_36 : IComparer<Org.Apache.Hadoop.Hdfs.Protocol.SnapshottableDirectoryStatus
			>
		{
			public _IComparer_36()
			{
			}

			public int Compare(Org.Apache.Hadoop.Hdfs.Protocol.SnapshottableDirectoryStatus left
				, Org.Apache.Hadoop.Hdfs.Protocol.SnapshottableDirectoryStatus right)
			{
				int d = DFSUtil.CompareBytes(left.parentFullPath, right.parentFullPath);
				return d != 0 ? d : DFSUtil.CompareBytes(left.dirStatus.GetLocalNameInBytes(), right
					.dirStatus.GetLocalNameInBytes());
			}
		}

		/// <summary>Compare the statuses by full paths.</summary>
		public static readonly IComparer<Org.Apache.Hadoop.Hdfs.Protocol.SnapshottableDirectoryStatus
			> Comparator = new _IComparer_36();

		/// <summary>Basic information of the snapshottable directory</summary>
		private readonly HdfsFileStatus dirStatus;

		/// <summary>Number of snapshots that have been taken</summary>
		private readonly int snapshotNumber;

		/// <summary>Number of snapshots allowed.</summary>
		private readonly int snapshotQuota;

		/// <summary>Full path of the parent.</summary>
		private readonly byte[] parentFullPath;

		public SnapshottableDirectoryStatus(long modification_time, long access_time, FsPermission
			 permission, string owner, string group, byte[] localName, long inodeId, int childrenNum
			, int snapshotNumber, int snapshotQuota, byte[] parentFullPath)
		{
			this.dirStatus = new HdfsFileStatus(0, true, 0, 0, modification_time, access_time
				, permission, owner, group, null, localName, inodeId, childrenNum, null, BlockStoragePolicySuite
				.IdUnspecified);
			this.snapshotNumber = snapshotNumber;
			this.snapshotQuota = snapshotQuota;
			this.parentFullPath = parentFullPath;
		}

		/// <returns>Number of snapshots that have been taken for the directory</returns>
		public virtual int GetSnapshotNumber()
		{
			return snapshotNumber;
		}

		/// <returns>Number of snapshots allowed for the directory</returns>
		public virtual int GetSnapshotQuota()
		{
			return snapshotQuota;
		}

		/// <returns>Full path of the parent</returns>
		public virtual byte[] GetParentFullPath()
		{
			return parentFullPath;
		}

		/// <returns>The basic information of the directory</returns>
		public virtual HdfsFileStatus GetDirStatus()
		{
			return dirStatus;
		}

		/// <returns>Full path of the file</returns>
		public virtual Path GetFullPath()
		{
			string parentFullPathStr = (parentFullPath == null || parentFullPath.Length == 0)
				 ? null : DFSUtil.Bytes2String(parentFullPath);
			if (parentFullPathStr == null && dirStatus.GetLocalNameInBytes().Length == 0)
			{
				// root
				return new Path("/");
			}
			else
			{
				return parentFullPathStr == null ? new Path(dirStatus.GetLocalName()) : new Path(
					parentFullPathStr, dirStatus.GetLocalName());
			}
		}

		/// <summary>
		/// Print a list of
		/// <see cref="SnapshottableDirectoryStatus"/>
		/// out to a given stream.
		/// </summary>
		/// <param name="stats">
		/// The list of
		/// <see cref="SnapshottableDirectoryStatus"/>
		/// </param>
		/// <param name="out">The given stream for printing.</param>
		public static void Print(Org.Apache.Hadoop.Hdfs.Protocol.SnapshottableDirectoryStatus
			[] stats, TextWriter @out)
		{
			if (stats == null || stats.Length == 0)
			{
				@out.WriteLine();
				return;
			}
			int maxRepl = 0;
			int maxLen = 0;
			int maxOwner = 0;
			int maxGroup = 0;
			int maxSnapshotNum = 0;
			int maxSnapshotQuota = 0;
			foreach (Org.Apache.Hadoop.Hdfs.Protocol.SnapshottableDirectoryStatus status in stats)
			{
				maxRepl = MaxLength(maxRepl, status.dirStatus.GetReplication());
				maxLen = MaxLength(maxLen, status.dirStatus.GetLen());
				maxOwner = MaxLength(maxOwner, status.dirStatus.GetOwner());
				maxGroup = MaxLength(maxGroup, status.dirStatus.GetGroup());
				maxSnapshotNum = MaxLength(maxSnapshotNum, status.snapshotNumber);
				maxSnapshotQuota = MaxLength(maxSnapshotQuota, status.snapshotQuota);
			}
			StringBuilder fmt = new StringBuilder();
			fmt.Append("%s%s ");
			// permission string
			fmt.Append("%" + maxRepl + "s ");
			fmt.Append((maxOwner > 0) ? "%-" + maxOwner + "s " : "%s");
			fmt.Append((maxGroup > 0) ? "%-" + maxGroup + "s " : "%s");
			fmt.Append("%" + maxLen + "s ");
			fmt.Append("%s ");
			// mod time
			fmt.Append("%" + maxSnapshotNum + "s ");
			fmt.Append("%" + maxSnapshotQuota + "s ");
			fmt.Append("%s");
			// path
			string lineFormat = fmt.ToString();
			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
			foreach (Org.Apache.Hadoop.Hdfs.Protocol.SnapshottableDirectoryStatus status_1 in 
				stats)
			{
				string line = string.Format(lineFormat, "d", status_1.dirStatus.GetPermission(), 
					status_1.dirStatus.GetReplication(), status_1.dirStatus.GetOwner(), status_1.dirStatus
					.GetGroup(), status_1.dirStatus.GetLen().ToString(), dateFormat.Format(Sharpen.Extensions.CreateDate
					(status_1.dirStatus.GetModificationTime())), status_1.snapshotNumber, status_1.snapshotQuota
					, status_1.GetFullPath().ToString());
				@out.WriteLine(line);
			}
		}

		private static int MaxLength(int n, object value)
		{
			return Math.Max(n, value.ToString().Length);
		}

		public class Bean
		{
			private readonly string path;

			private readonly int snapshotNumber;

			private readonly int snapshotQuota;

			private readonly long modificationTime;

			private readonly short permission;

			private readonly string owner;

			private readonly string group;

			public Bean(string path, int snapshotNumber, int snapshotQuota, long modificationTime
				, short permission, string owner, string group)
			{
				this.path = path;
				this.snapshotNumber = snapshotNumber;
				this.snapshotQuota = snapshotQuota;
				this.modificationTime = modificationTime;
				this.permission = permission;
				this.owner = owner;
				this.group = group;
			}

			public virtual string GetPath()
			{
				return path;
			}

			public virtual int GetSnapshotNumber()
			{
				return snapshotNumber;
			}

			public virtual int GetSnapshotQuota()
			{
				return snapshotQuota;
			}

			public virtual long GetModificationTime()
			{
				return modificationTime;
			}

			public virtual short GetPermission()
			{
				return permission;
			}

			public virtual string GetOwner()
			{
				return owner;
			}

			public virtual string GetGroup()
			{
				return group;
			}
		}
	}
}
