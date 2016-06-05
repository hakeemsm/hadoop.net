using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Base;
using Javax.Management;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Metrics2.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot
{
	/// <summary>Manage snapshottable directories and their snapshots.</summary>
	/// <remarks>
	/// Manage snapshottable directories and their snapshots.
	/// This class includes operations that create, access, modify snapshots and/or
	/// snapshot-related data. In general, the locking structure of snapshot
	/// operations is: <br />
	/// 1. Lock the
	/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.FSNamesystem"/>
	/// lock in
	/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.FSNamesystem"/>
	/// before calling
	/// into
	/// <see cref="SnapshotManager"/>
	/// methods.<br />
	/// 2. Lock the
	/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.FSDirectory"/>
	/// lock for the
	/// <see cref="SnapshotManager"/>
	/// methods
	/// if necessary.
	/// </remarks>
	public class SnapshotManager : SnapshotStatsMXBean
	{
		private bool allowNestedSnapshots = false;

		private readonly FSDirectory fsdir;

		private const int SnapshotIdBitWidth = 24;

		private readonly AtomicInteger numSnapshots = new AtomicInteger();

		private int snapshotCounter = 0;

		/// <summary>All snapshottable directories in the namesystem.</summary>
		private readonly IDictionary<long, INodeDirectory> snapshottables = new Dictionary
			<long, INodeDirectory>();

		public SnapshotManager(FSDirectory fsdir)
		{
			this.fsdir = fsdir;
		}

		/// <summary>Used in tests only</summary>
		internal virtual void SetAllowNestedSnapshots(bool allowNestedSnapshots)
		{
			this.allowNestedSnapshots = allowNestedSnapshots;
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.SnapshotException"/>
		private void CheckNestedSnapshottable(INodeDirectory dir, string path)
		{
			if (allowNestedSnapshots)
			{
				return;
			}
			foreach (INodeDirectory s in snapshottables.Values)
			{
				if (s.IsAncestorDirectory(dir))
				{
					throw new SnapshotException("Nested snapshottable directories not allowed: path="
						 + path + ", the subdirectory " + s.GetFullPathName() + " is already a snapshottable directory."
						);
				}
				if (dir.IsAncestorDirectory(s))
				{
					throw new SnapshotException("Nested snapshottable directories not allowed: path="
						 + path + ", the ancestor " + s.GetFullPathName() + " is already a snapshottable directory."
						);
				}
			}
		}

		/// <summary>Set the given directory as a snapshottable directory.</summary>
		/// <remarks>
		/// Set the given directory as a snapshottable directory.
		/// If the path is already a snapshottable directory, update the quota.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public virtual void SetSnapshottable(string path, bool checkNestedSnapshottable)
		{
			INodesInPath iip = fsdir.GetINodesInPath4Write(path);
			INodeDirectory d = INodeDirectory.ValueOf(iip.GetLastINode(), path);
			if (checkNestedSnapshottable)
			{
				CheckNestedSnapshottable(d, path);
			}
			if (d.IsSnapshottable())
			{
				//The directory is already a snapshottable directory.
				d.SetSnapshotQuota(DirectorySnapshottableFeature.SnapshotLimit);
			}
			else
			{
				d.AddSnapshottableFeature();
			}
			AddSnapshottable(d);
		}

		/// <summary>
		/// Add the given snapshottable directory to
		/// <see cref="snapshottables"/>
		/// .
		/// </summary>
		public virtual void AddSnapshottable(INodeDirectory dir)
		{
			Preconditions.CheckArgument(dir.IsSnapshottable());
			snapshottables[dir.GetId()] = dir;
		}

		/// <summary>
		/// Remove the given snapshottable directory from
		/// <see cref="snapshottables"/>
		/// .
		/// </summary>
		private void RemoveSnapshottable(INodeDirectory s)
		{
			Sharpen.Collections.Remove(snapshottables, s.GetId());
		}

		/// <summary>
		/// Remove snapshottable directories from
		/// <see cref="snapshottables"/>
		/// 
		/// </summary>
		public virtual void RemoveSnapshottable(IList<INodeDirectory> toRemove)
		{
			if (toRemove != null)
			{
				foreach (INodeDirectory s in toRemove)
				{
					RemoveSnapshottable(s);
				}
			}
		}

		/// <summary>Set the given snapshottable directory to non-snapshottable.</summary>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.SnapshotException">if there are snapshots in the directory.
		/// 	</exception>
		/// <exception cref="System.IO.IOException"/>
		public virtual void ResetSnapshottable(string path)
		{
			INodesInPath iip = fsdir.GetINodesInPath4Write(path);
			INodeDirectory d = INodeDirectory.ValueOf(iip.GetLastINode(), path);
			DirectorySnapshottableFeature sf = d.GetDirectorySnapshottableFeature();
			if (sf == null)
			{
				// the directory is already non-snapshottable
				return;
			}
			if (sf.GetNumSnapshots() > 0)
			{
				throw new SnapshotException("The directory " + path + " has snapshot(s). " + "Please redo the operation after removing all the snapshots."
					);
			}
			if (d == fsdir.GetRoot())
			{
				d.SetSnapshotQuota(0);
			}
			else
			{
				d.RemoveSnapshottableFeature();
			}
			RemoveSnapshottable(d);
		}

		/// <summary>
		/// Find the source root directory where the snapshot will be taken
		/// for a given path.
		/// </summary>
		/// <returns>Snapshottable directory.</returns>
		/// <exception cref="System.IO.IOException">
		/// Throw IOException when the given path does not lead to an
		/// existing snapshottable directory.
		/// </exception>
		public virtual INodeDirectory GetSnapshottableRoot(INodesInPath iip)
		{
			string path = iip.GetPath();
			INodeDirectory dir = INodeDirectory.ValueOf(iip.GetLastINode(), path);
			if (!dir.IsSnapshottable())
			{
				throw new SnapshotException("Directory is not a snapshottable directory: " + path
					);
			}
			return dir;
		}

		/// <summary>Create a snapshot of the given path.</summary>
		/// <remarks>
		/// Create a snapshot of the given path.
		/// It is assumed that the caller will perform synchronization.
		/// </remarks>
		/// <param name="iip">the INodes resolved from the snapshottable directory's path</param>
		/// <param name="snapshotName">The name of the snapshot.</param>
		/// <exception cref="System.IO.IOException">
		/// Throw IOException when 1) the given path does not lead to an
		/// existing snapshottable directory, and/or 2) there exists a
		/// snapshot with the given name for the directory, and/or 3)
		/// snapshot number exceeds quota
		/// </exception>
		public virtual string CreateSnapshot(INodesInPath iip, string snapshotRoot, string
			 snapshotName)
		{
			INodeDirectory srcRoot = GetSnapshottableRoot(iip);
			if (snapshotCounter == GetMaxSnapshotID())
			{
				// We have reached the maximum allowable snapshot ID and since we don't
				// handle rollover we will fail all subsequent snapshot creation
				// requests.
				//
				throw new SnapshotException("Failed to create the snapshot. The FileSystem has run out of "
					 + "snapshot IDs and ID rollover is not supported.");
			}
			srcRoot.AddSnapshot(snapshotCounter, snapshotName);
			//create success, update id
			snapshotCounter++;
			numSnapshots.GetAndIncrement();
			return Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.GetSnapshotPath(snapshotRoot
				, snapshotName);
		}

		/// <summary>Delete a snapshot for a snapshottable directory</summary>
		/// <param name="snapshotName">Name of the snapshot to be deleted</param>
		/// <param name="collectedBlocks">Used to collect information to update blocksMap</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual void DeleteSnapshot(INodesInPath iip, string snapshotName, INode.BlocksMapUpdateInfo
			 collectedBlocks, IList<INode> removedINodes)
		{
			INodeDirectory srcRoot = GetSnapshottableRoot(iip);
			srcRoot.RemoveSnapshot(fsdir.GetBlockStoragePolicySuite(), snapshotName, collectedBlocks
				, removedINodes);
			numSnapshots.GetAndDecrement();
		}

		/// <summary>Rename the given snapshot</summary>
		/// <param name="oldSnapshotName">Old name of the snapshot</param>
		/// <param name="newSnapshotName">New name of the snapshot</param>
		/// <exception cref="System.IO.IOException">
		/// Throw IOException when 1) the given path does not lead to an
		/// existing snapshottable directory, and/or 2) the snapshot with the
		/// old name does not exist for the directory, and/or 3) there exists
		/// a snapshot with the new name for the directory
		/// </exception>
		public virtual void RenameSnapshot(INodesInPath iip, string snapshotRoot, string 
			oldSnapshotName, string newSnapshotName)
		{
			INodeDirectory srcRoot = GetSnapshottableRoot(iip);
			srcRoot.RenameSnapshot(snapshotRoot, oldSnapshotName, newSnapshotName);
		}

		public virtual int GetNumSnapshottableDirs()
		{
			return snapshottables.Count;
		}

		public virtual int GetNumSnapshots()
		{
			return numSnapshots.Get();
		}

		internal virtual void SetNumSnapshots(int num)
		{
			numSnapshots.Set(num);
		}

		internal virtual int GetSnapshotCounter()
		{
			return snapshotCounter;
		}

		internal virtual void SetSnapshotCounter(int counter)
		{
			snapshotCounter = counter;
		}

		internal virtual INodeDirectory[] GetSnapshottableDirs()
		{
			return Sharpen.Collections.ToArray(snapshottables.Values, new INodeDirectory[snapshottables
				.Count]);
		}

		/// <summary>
		/// Write
		/// <see cref="snapshotCounter"/>
		/// ,
		/// <see cref="numSnapshots"/>
		/// ,
		/// and all snapshots to the DataOutput.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void Write(DataOutput @out)
		{
			@out.WriteInt(snapshotCounter);
			@out.WriteInt(numSnapshots.Get());
			// write all snapshots.
			foreach (INodeDirectory snapshottableDir in snapshottables.Values)
			{
				foreach (Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot s in snapshottableDir
					.GetDirectorySnapshottableFeature().GetSnapshotList())
				{
					s.Write(@out);
				}
			}
		}

		/// <summary>
		/// Read values of
		/// <see cref="snapshotCounter"/>
		/// ,
		/// <see cref="numSnapshots"/>
		/// , and
		/// all snapshots from the DataInput
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual IDictionary<int, Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
			> Read(DataInput @in, FSImageFormat.Loader loader)
		{
			snapshotCounter = @in.ReadInt();
			numSnapshots.Set(@in.ReadInt());
			// read snapshots
			IDictionary<int, Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot> snapshotMap
				 = new Dictionary<int, Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot>
				();
			for (int i = 0; i < numSnapshots.Get(); i++)
			{
				Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot s = Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
					.Read(@in, loader);
				snapshotMap[s.GetId()] = s;
			}
			return snapshotMap;
		}

		/// <summary>List all the snapshottable directories that are owned by the current user.
		/// 	</summary>
		/// <param name="userName">Current user name.</param>
		/// <returns>
		/// Snapshottable directories that are owned by the current user,
		/// represented as an array of
		/// <see cref="Org.Apache.Hadoop.Hdfs.Protocol.SnapshottableDirectoryStatus"/>
		/// . If
		/// <paramref name="userName"/>
		/// is null, return all the snapshottable dirs.
		/// </returns>
		public virtual SnapshottableDirectoryStatus[] GetSnapshottableDirListing(string userName
			)
		{
			if (snapshottables.IsEmpty())
			{
				return null;
			}
			IList<SnapshottableDirectoryStatus> statusList = new AList<SnapshottableDirectoryStatus
				>();
			foreach (INodeDirectory dir in snapshottables.Values)
			{
				if (userName == null || userName.Equals(dir.GetUserName()))
				{
					SnapshottableDirectoryStatus status = new SnapshottableDirectoryStatus(dir.GetModificationTime
						(), dir.GetAccessTime(), dir.GetFsPermission(), dir.GetUserName(), dir.GetGroupName
						(), dir.GetLocalNameBytes(), dir.GetId(), dir.GetChildrenNum(Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
						.CurrentStateId), dir.GetDirectorySnapshottableFeature().GetNumSnapshots(), dir.
						GetDirectorySnapshottableFeature().GetSnapshotQuota(), dir.GetParent() == null ? 
						DFSUtil.EmptyBytes : DFSUtil.String2Bytes(dir.GetParent().GetFullPathName()));
					statusList.AddItem(status);
				}
			}
			statusList.Sort(SnapshottableDirectoryStatus.Comparator);
			return Sharpen.Collections.ToArray(statusList, new SnapshottableDirectoryStatus[statusList
				.Count]);
		}

		/// <summary>
		/// Compute the difference between two snapshots of a directory, or between a
		/// snapshot of the directory and its current tree.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual SnapshotDiffReport Diff(INodesInPath iip, string snapshotRootPath, 
			string from, string to)
		{
			// Find the source root directory path where the snapshots were taken.
			// All the check for path has been included in the valueOf method.
			INodeDirectory snapshotRoot = GetSnapshottableRoot(iip);
			if ((from == null || from.IsEmpty()) && (to == null || to.IsEmpty()))
			{
				// both fromSnapshot and toSnapshot indicate the current tree
				return new SnapshotDiffReport(snapshotRootPath, from, to, Sharpen.Collections.EmptyList
					<SnapshotDiffReport.DiffReportEntry>());
			}
			SnapshotDiffInfo diffs = snapshotRoot.GetDirectorySnapshottableFeature().ComputeDiff
				(snapshotRoot, from, to);
			return diffs != null ? diffs.GenerateReport() : new SnapshotDiffReport(snapshotRootPath
				, from, to, Sharpen.Collections.EmptyList<SnapshotDiffReport.DiffReportEntry>());
		}

		public virtual void ClearSnapshottableDirs()
		{
			snapshottables.Clear();
		}

		/// <summary>
		/// Returns the maximum allowable snapshot ID based on the bit width of the
		/// snapshot ID.
		/// </summary>
		/// <returns>maximum allowable snapshot ID.</returns>
		public virtual int GetMaxSnapshotID()
		{
			return ((1 << SnapshotIdBitWidth) - 1);
		}

		private ObjectName mxBeanName;

		public virtual void RegisterMXBean()
		{
			mxBeanName = MBeans.Register("NameNode", "SnapshotInfo", this);
		}

		public virtual void Shutdown()
		{
			MBeans.Unregister(mxBeanName);
			mxBeanName = null;
		}

		public virtual SnapshottableDirectoryStatus.Bean[] GetSnapshottableDirectories()
		{
			// SnapshotStatsMXBean
			IList<SnapshottableDirectoryStatus.Bean> beans = new AList<SnapshottableDirectoryStatus.Bean
				>();
			foreach (INodeDirectory d in GetSnapshottableDirs())
			{
				beans.AddItem(ToBean(d));
			}
			return Sharpen.Collections.ToArray(beans, new SnapshottableDirectoryStatus.Bean[beans
				.Count]);
		}

		public virtual SnapshotInfo.Bean[] GetSnapshots()
		{
			// SnapshotStatsMXBean
			IList<SnapshotInfo.Bean> beans = new AList<SnapshotInfo.Bean>();
			foreach (INodeDirectory d in GetSnapshottableDirs())
			{
				foreach (Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot s in d.GetDirectorySnapshottableFeature
					().GetSnapshotList())
				{
					beans.AddItem(ToBean(s));
				}
			}
			return Sharpen.Collections.ToArray(beans, new SnapshotInfo.Bean[beans.Count]);
		}

		public static SnapshottableDirectoryStatus.Bean ToBean(INodeDirectory d)
		{
			return new SnapshottableDirectoryStatus.Bean(d.GetFullPathName(), d.GetDirectorySnapshottableFeature
				().GetNumSnapshots(), d.GetDirectorySnapshottableFeature().GetSnapshotQuota(), d
				.GetModificationTime(), short.ValueOf(Sharpen.Extensions.ToOctalString(d.GetFsPermissionShort
				())), d.GetUserName(), d.GetGroupName());
		}

		public static SnapshotInfo.Bean ToBean(Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
			 s)
		{
			return new SnapshotInfo.Bean(s.GetRoot().GetLocalName(), s.GetRoot().GetFullPathName
				(), s.GetRoot().GetModificationTime());
		}
	}
}
