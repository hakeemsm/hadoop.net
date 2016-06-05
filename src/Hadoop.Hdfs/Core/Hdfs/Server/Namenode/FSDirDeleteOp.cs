using System.Collections.Generic;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	internal class FSDirDeleteOp
	{
		/// <summary>Delete the target directory and collect the blocks under it</summary>
		/// <param name="iip">the INodesInPath instance containing all the INodes for the path
		/// 	</param>
		/// <param name="collectedBlocks">Blocks under the deleted directory</param>
		/// <param name="removedINodes">INodes that should be removed from inodeMap</param>
		/// <returns>the number of files that have been removed</returns>
		/// <exception cref="System.IO.IOException"/>
		internal static long Delete(FSDirectory fsd, INodesInPath iip, INode.BlocksMapUpdateInfo
			 collectedBlocks, IList<INode> removedINodes, long mtime)
		{
			if (NameNode.stateChangeLog.IsDebugEnabled())
			{
				NameNode.stateChangeLog.Debug("DIR* FSDirectory.delete: " + iip.GetPath());
			}
			long filesRemoved;
			fsd.WriteLock();
			try
			{
				if (!DeleteAllowed(iip, iip.GetPath()))
				{
					filesRemoved = -1;
				}
				else
				{
					IList<INodeDirectory> snapshottableDirs = new AList<INodeDirectory>();
					FSDirSnapshotOp.CheckSnapshot(iip.GetLastINode(), snapshottableDirs);
					filesRemoved = UnprotectedDelete(fsd, iip, collectedBlocks, removedINodes, mtime);
					fsd.GetFSNamesystem().RemoveSnapshottableDirs(snapshottableDirs);
				}
			}
			finally
			{
				fsd.WriteUnlock();
			}
			return filesRemoved;
		}

		/// <summary>Remove a file/directory from the namespace.</summary>
		/// <remarks>
		/// Remove a file/directory from the namespace.
		/// <p>
		/// For large directories, deletion is incremental. The blocks under
		/// the directory are collected and deleted a small number at a time holding
		/// the
		/// <see cref="FSNamesystem"/>
		/// lock.
		/// <p>
		/// For small directory or file the deletion is done in one shot.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		internal static INode.BlocksMapUpdateInfo Delete(FSNamesystem fsn, string src, bool
			 recursive, bool logRetryCache)
		{
			FSDirectory fsd = fsn.GetFSDirectory();
			FSPermissionChecker pc = fsd.GetPermissionChecker();
			byte[][] pathComponents = FSDirectory.GetPathComponentsForReservedPath(src);
			src = fsd.ResolvePath(pc, src, pathComponents);
			INodesInPath iip = fsd.GetINodesInPath4Write(src, false);
			if (!recursive && fsd.IsNonEmptyDirectory(iip))
			{
				throw new PathIsNotEmptyDirectoryException(src + " is non empty");
			}
			if (fsd.IsPermissionEnabled())
			{
				fsd.CheckPermission(pc, iip, false, null, FsAction.Write, null, FsAction.All, true
					);
			}
			return DeleteInternal(fsn, src, iip, logRetryCache);
		}

		/// <summary>
		/// Delete a path from the name space
		/// Update the count at each ancestor directory with quota
		/// <br />
		/// Note: This is to be used by
		/// <see cref="FSEditLog"/>
		/// only.
		/// <br />
		/// </summary>
		/// <param name="src">a string representation of a path to an inode</param>
		/// <param name="mtime">the time the inode is removed</param>
		/// <exception cref="System.IO.IOException"/>
		internal static void DeleteForEditLog(FSDirectory fsd, string src, long mtime)
		{
			System.Diagnostics.Debug.Assert(fsd.HasWriteLock());
			FSNamesystem fsn = fsd.GetFSNamesystem();
			INode.BlocksMapUpdateInfo collectedBlocks = new INode.BlocksMapUpdateInfo();
			IList<INode> removedINodes = new ChunkedArrayList<INode>();
			INodesInPath iip = fsd.GetINodesInPath4Write(FSDirectory.NormalizePath(src), false
				);
			if (!DeleteAllowed(iip, src))
			{
				return;
			}
			IList<INodeDirectory> snapshottableDirs = new AList<INodeDirectory>();
			FSDirSnapshotOp.CheckSnapshot(iip.GetLastINode(), snapshottableDirs);
			long filesRemoved = UnprotectedDelete(fsd, iip, collectedBlocks, removedINodes, mtime
				);
			fsn.RemoveSnapshottableDirs(snapshottableDirs);
			if (filesRemoved >= 0)
			{
				fsn.RemoveLeasesAndINodes(src, removedINodes, false);
				fsn.RemoveBlocksAndUpdateSafemodeTotal(collectedBlocks);
			}
		}

		/// <summary>Remove a file/directory from the namespace.</summary>
		/// <remarks>
		/// Remove a file/directory from the namespace.
		/// <p>
		/// For large directories, deletion is incremental. The blocks under
		/// the directory are collected and deleted a small number at a time holding
		/// the
		/// <see cref="FSNamesystem"/>
		/// lock.
		/// <p>
		/// For small directory or file the deletion is done in one shot.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		internal static INode.BlocksMapUpdateInfo DeleteInternal(FSNamesystem fsn, string
			 src, INodesInPath iip, bool logRetryCache)
		{
			System.Diagnostics.Debug.Assert(fsn.HasWriteLock());
			if (NameNode.stateChangeLog.IsDebugEnabled())
			{
				NameNode.stateChangeLog.Debug("DIR* NameSystem.delete: " + src);
			}
			FSDirectory fsd = fsn.GetFSDirectory();
			INode.BlocksMapUpdateInfo collectedBlocks = new INode.BlocksMapUpdateInfo();
			IList<INode> removedINodes = new ChunkedArrayList<INode>();
			long mtime = Time.Now();
			// Unlink the target directory from directory tree
			long filesRemoved = Delete(fsd, iip, collectedBlocks, removedINodes, mtime);
			if (filesRemoved < 0)
			{
				return null;
			}
			fsd.GetEditLog().LogDelete(src, mtime, logRetryCache);
			IncrDeletedFileCount(filesRemoved);
			fsn.RemoveLeasesAndINodes(src, removedINodes, true);
			if (NameNode.stateChangeLog.IsDebugEnabled())
			{
				NameNode.stateChangeLog.Debug("DIR* Namesystem.delete: " + src + " is removed");
			}
			return collectedBlocks;
		}

		internal static void IncrDeletedFileCount(long count)
		{
			NameNode.GetNameNodeMetrics().IncrFilesDeleted(count);
		}

		private static bool DeleteAllowed(INodesInPath iip, string src)
		{
			if (iip.Length() < 1 || iip.GetLastINode() == null)
			{
				if (NameNode.stateChangeLog.IsDebugEnabled())
				{
					NameNode.stateChangeLog.Debug("DIR* FSDirectory.unprotectedDelete: failed to remove "
						 + src + " because it does not exist");
				}
				return false;
			}
			else
			{
				if (iip.Length() == 1)
				{
					// src is the root
					NameNode.stateChangeLog.Warn("DIR* FSDirectory.unprotectedDelete: failed to remove "
						 + src + " because the root is not allowed to be deleted");
					return false;
				}
			}
			return true;
		}

		/// <summary>
		/// Delete a path from the name space
		/// Update the count at each ancestor directory with quota
		/// </summary>
		/// <param name="iip">the inodes resolved from the path</param>
		/// <param name="collectedBlocks">blocks collected from the deleted path</param>
		/// <param name="removedINodes">inodes that should be removed from inodeMap</param>
		/// <param name="mtime">the time the inode is removed</param>
		/// <returns>the number of inodes deleted; 0 if no inodes are deleted.</returns>
		private static long UnprotectedDelete(FSDirectory fsd, INodesInPath iip, INode.BlocksMapUpdateInfo
			 collectedBlocks, IList<INode> removedINodes, long mtime)
		{
			System.Diagnostics.Debug.Assert(fsd.HasWriteLock());
			// check if target node exists
			INode targetNode = iip.GetLastINode();
			if (targetNode == null)
			{
				return -1;
			}
			// record modification
			int latestSnapshot = iip.GetLatestSnapshotId();
			targetNode.RecordModification(latestSnapshot);
			// Remove the node from the namespace
			long removed = fsd.RemoveLastINode(iip);
			if (removed == -1)
			{
				return -1;
			}
			// set the parent's modification time
			INodeDirectory parent = targetNode.GetParent();
			parent.UpdateModificationTime(mtime, latestSnapshot);
			fsd.UpdateCountForDelete(targetNode, iip);
			if (removed == 0)
			{
				return 0;
			}
			// collect block and update quota
			if (!targetNode.IsInLatestSnapshot(latestSnapshot))
			{
				targetNode.DestroyAndCollectBlocks(fsd.GetBlockStoragePolicySuite(), collectedBlocks
					, removedINodes);
			}
			else
			{
				QuotaCounts counts = targetNode.CleanSubtree(fsd.GetBlockStoragePolicySuite(), Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
					.CurrentStateId, latestSnapshot, collectedBlocks, removedINodes);
				removed = counts.GetNameSpace();
				fsd.UpdateCountNoQuotaCheck(iip, iip.Length() - 1, counts.Negation());
			}
			if (NameNode.stateChangeLog.IsDebugEnabled())
			{
				NameNode.stateChangeLog.Debug("DIR* FSDirectory.unprotectedDelete: " + iip.GetPath
					() + " is removed");
			}
			return removed;
		}
	}
}
