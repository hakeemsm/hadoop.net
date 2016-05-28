using System.Collections.Generic;
using Org.Apache.Hadoop;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	internal class FSDirSnapshotOp
	{
		/// <summary>Verify if the snapshot name is legal.</summary>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.FSLimitException.PathComponentTooLongException
		/// 	"/>
		internal static void VerifySnapshotName(FSDirectory fsd, string snapshotName, string
			 path)
		{
			if (snapshotName.Contains(Path.Separator))
			{
				throw new HadoopIllegalArgumentException("Snapshot name cannot contain \"" + Path
					.Separator + "\"");
			}
			byte[] bytes = DFSUtil.String2Bytes(snapshotName);
			fsd.VerifyINodeName(bytes);
			fsd.VerifyMaxComponentLength(bytes, path);
		}

		/// <summary>Allow snapshot on a directory.</summary>
		/// <exception cref="System.IO.IOException"/>
		internal static void AllowSnapshot(FSDirectory fsd, SnapshotManager snapshotManager
			, string path)
		{
			fsd.WriteLock();
			try
			{
				snapshotManager.SetSnapshottable(path, true);
			}
			finally
			{
				fsd.WriteUnlock();
			}
			fsd.GetEditLog().LogAllowSnapshot(path);
		}

		/// <exception cref="System.IO.IOException"/>
		internal static void DisallowSnapshot(FSDirectory fsd, SnapshotManager snapshotManager
			, string path)
		{
			fsd.WriteLock();
			try
			{
				snapshotManager.ResetSnapshottable(path);
			}
			finally
			{
				fsd.WriteUnlock();
			}
			fsd.GetEditLog().LogDisallowSnapshot(path);
		}

		/// <summary>Create a snapshot</summary>
		/// <param name="snapshotRoot">The directory path where the snapshot is taken</param>
		/// <param name="snapshotName">The name of the snapshot</param>
		/// <exception cref="System.IO.IOException"/>
		internal static string CreateSnapshot(FSDirectory fsd, SnapshotManager snapshotManager
			, string snapshotRoot, string snapshotName, bool logRetryCache)
		{
			INodesInPath iip = fsd.GetINodesInPath4Write(snapshotRoot);
			if (fsd.IsPermissionEnabled())
			{
				FSPermissionChecker pc = fsd.GetPermissionChecker();
				fsd.CheckOwner(pc, iip);
			}
			if (snapshotName == null || snapshotName.IsEmpty())
			{
				snapshotName = Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.GenerateDefaultSnapshotName
					();
			}
			else
			{
				if (!DFSUtil.IsValidNameForComponent(snapshotName))
				{
					throw new InvalidPathException("Invalid snapshot name: " + snapshotName);
				}
			}
			string snapshotPath = null;
			VerifySnapshotName(fsd, snapshotName, snapshotRoot);
			fsd.WriteLock();
			try
			{
				snapshotPath = snapshotManager.CreateSnapshot(iip, snapshotRoot, snapshotName);
			}
			finally
			{
				fsd.WriteUnlock();
			}
			fsd.GetEditLog().LogCreateSnapshot(snapshotRoot, snapshotName, logRetryCache);
			return snapshotPath;
		}

		/// <exception cref="System.IO.IOException"/>
		internal static void RenameSnapshot(FSDirectory fsd, SnapshotManager snapshotManager
			, string path, string snapshotOldName, string snapshotNewName, bool logRetryCache
			)
		{
			INodesInPath iip = fsd.GetINodesInPath4Write(path);
			if (fsd.IsPermissionEnabled())
			{
				FSPermissionChecker pc = fsd.GetPermissionChecker();
				fsd.CheckOwner(pc, iip);
			}
			VerifySnapshotName(fsd, snapshotNewName, path);
			fsd.WriteLock();
			try
			{
				snapshotManager.RenameSnapshot(iip, path, snapshotOldName, snapshotNewName);
			}
			finally
			{
				fsd.WriteUnlock();
			}
			fsd.GetEditLog().LogRenameSnapshot(path, snapshotOldName, snapshotNewName, logRetryCache
				);
		}

		/// <exception cref="System.IO.IOException"/>
		internal static SnapshottableDirectoryStatus[] GetSnapshottableDirListing(FSDirectory
			 fsd, SnapshotManager snapshotManager)
		{
			FSPermissionChecker pc = fsd.GetPermissionChecker();
			fsd.ReadLock();
			try
			{
				string user = pc.IsSuperUser() ? null : pc.GetUser();
				return snapshotManager.GetSnapshottableDirListing(user);
			}
			finally
			{
				fsd.ReadUnlock();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal static SnapshotDiffReport GetSnapshotDiffReport(FSDirectory fsd, SnapshotManager
			 snapshotManager, string path, string fromSnapshot, string toSnapshot)
		{
			SnapshotDiffReport diffs;
			FSPermissionChecker pc = fsd.GetPermissionChecker();
			fsd.ReadLock();
			try
			{
				if (fsd.IsPermissionEnabled())
				{
					CheckSubtreeReadPermission(fsd, pc, path, fromSnapshot);
					CheckSubtreeReadPermission(fsd, pc, path, toSnapshot);
				}
				INodesInPath iip = fsd.GetINodesInPath(path, true);
				diffs = snapshotManager.Diff(iip, path, fromSnapshot, toSnapshot);
			}
			finally
			{
				fsd.ReadUnlock();
			}
			return diffs;
		}

		/// <summary>Delete a snapshot of a snapshottable directory</summary>
		/// <param name="snapshotRoot">The snapshottable directory</param>
		/// <param name="snapshotName">The name of the to-be-deleted snapshot</param>
		/// <exception cref="System.IO.IOException"/>
		internal static INode.BlocksMapUpdateInfo DeleteSnapshot(FSDirectory fsd, SnapshotManager
			 snapshotManager, string snapshotRoot, string snapshotName, bool logRetryCache)
		{
			INodesInPath iip = fsd.GetINodesInPath4Write(snapshotRoot);
			if (fsd.IsPermissionEnabled())
			{
				FSPermissionChecker pc = fsd.GetPermissionChecker();
				fsd.CheckOwner(pc, iip);
			}
			INode.BlocksMapUpdateInfo collectedBlocks = new INode.BlocksMapUpdateInfo();
			ChunkedArrayList<INode> removedINodes = new ChunkedArrayList<INode>();
			fsd.WriteLock();
			try
			{
				snapshotManager.DeleteSnapshot(iip, snapshotName, collectedBlocks, removedINodes);
				fsd.RemoveFromInodeMap(removedINodes);
			}
			finally
			{
				fsd.WriteUnlock();
			}
			removedINodes.Clear();
			fsd.GetEditLog().LogDeleteSnapshot(snapshotRoot, snapshotName, logRetryCache);
			return collectedBlocks;
		}

		/// <exception cref="System.IO.IOException"/>
		private static void CheckSubtreeReadPermission(FSDirectory fsd, FSPermissionChecker
			 pc, string snapshottablePath, string snapshot)
		{
			string fromPath = snapshot == null ? snapshottablePath : Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
				.GetSnapshotPath(snapshottablePath, snapshot);
			INodesInPath iip = fsd.GetINodesInPath(fromPath, true);
			fsd.CheckPermission(pc, iip, false, null, null, FsAction.Read, FsAction.Read);
		}

		/// <summary>
		/// Check if the given INode (or one of its descendants) is snapshottable and
		/// already has snapshots.
		/// </summary>
		/// <param name="target">The given INode</param>
		/// <param name="snapshottableDirs">
		/// The list of directories that are snapshottable
		/// but do not have snapshots yet
		/// </param>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.SnapshotException"/>
		internal static void CheckSnapshot(INode target, IList<INodeDirectory> snapshottableDirs
			)
		{
			if (target.IsDirectory())
			{
				INodeDirectory targetDir = target.AsDirectory();
				DirectorySnapshottableFeature sf = targetDir.GetDirectorySnapshottableFeature();
				if (sf != null)
				{
					if (sf.GetNumSnapshots() > 0)
					{
						string fullPath = targetDir.GetFullPathName();
						throw new SnapshotException("The directory " + fullPath + " cannot be deleted since "
							 + fullPath + " is snapshottable and already has snapshots");
					}
					else
					{
						if (snapshottableDirs != null)
						{
							snapshottableDirs.AddItem(targetDir);
						}
					}
				}
				foreach (INode child in targetDir.GetChildrenList(Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
					.CurrentStateId))
				{
					CheckSnapshot(child, snapshottableDirs);
				}
			}
		}
	}
}
