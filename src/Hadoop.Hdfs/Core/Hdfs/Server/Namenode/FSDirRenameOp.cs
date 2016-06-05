using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Base;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Org.Apache.Hadoop.Hdfs.Util;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	internal class FSDirRenameOp
	{
		/// <exception cref="System.IO.IOException"/>
		[Obsolete]
		internal static FSDirRenameOp.RenameOldResult RenameToInt(FSDirectory fsd, string
			 srcArg, string dstArg, bool logRetryCache)
		{
			string src = srcArg;
			string dst = dstArg;
			if (NameNode.stateChangeLog.IsDebugEnabled())
			{
				NameNode.stateChangeLog.Debug("DIR* NameSystem.renameTo: " + src + " to " + dst);
			}
			if (!DFSUtil.IsValidName(dst))
			{
				throw new IOException("Invalid name: " + dst);
			}
			FSPermissionChecker pc = fsd.GetPermissionChecker();
			byte[][] srcComponents = FSDirectory.GetPathComponentsForReservedPath(src);
			byte[][] dstComponents = FSDirectory.GetPathComponentsForReservedPath(dst);
			HdfsFileStatus resultingStat = null;
			src = fsd.ResolvePath(pc, src, srcComponents);
			dst = fsd.ResolvePath(pc, dst, dstComponents);
			bool status = RenameTo(fsd, pc, src, dst, logRetryCache);
			if (status)
			{
				INodesInPath dstIIP = fsd.GetINodesInPath(dst, false);
				resultingStat = fsd.GetAuditFileInfo(dstIIP);
			}
			return new FSDirRenameOp.RenameOldResult(status, resultingStat);
		}

		/// <summary>
		/// Verify quota for rename operation where srcInodes[srcInodes.length-1] moves
		/// dstInodes[dstInodes.length-1]
		/// </summary>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.QuotaExceededException"/>
		private static void VerifyQuotaForRename(FSDirectory fsd, INodesInPath src, INodesInPath
			 dst)
		{
			if (!fsd.GetFSNamesystem().IsImageLoaded() || fsd.ShouldSkipQuotaChecks())
			{
				// Do not check quota if edits log is still being processed
				return;
			}
			int i = 0;
			while (src.GetINode(i) == dst.GetINode(i))
			{
				i++;
			}
			// src[i - 1] is the last common ancestor.
			BlockStoragePolicySuite bsps = fsd.GetBlockStoragePolicySuite();
			QuotaCounts delta = src.GetLastINode().ComputeQuotaUsage(bsps);
			// Reduce the required quota by dst that is being removed
			INode dstINode = dst.GetLastINode();
			if (dstINode != null)
			{
				delta.Subtract(dstINode.ComputeQuotaUsage(bsps));
			}
			FSDirectory.VerifyQuota(dst, dst.Length() - 1, delta, src.GetINode(i - 1));
		}

		/// <summary>
		/// Checks file system limits (max component length and max directory items)
		/// during a rename operation.
		/// </summary>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.FSLimitException.PathComponentTooLongException
		/// 	"/>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.FSLimitException.MaxDirectoryItemsExceededException
		/// 	"/>
		internal static void VerifyFsLimitsForRename(FSDirectory fsd, INodesInPath srcIIP
			, INodesInPath dstIIP)
		{
			byte[] dstChildName = dstIIP.GetLastLocalName();
			string parentPath = dstIIP.GetParentPath();
			fsd.VerifyMaxComponentLength(dstChildName, parentPath);
			// Do not enforce max directory items if renaming within same directory.
			if (srcIIP.GetINode(-2) != dstIIP.GetINode(-2))
			{
				fsd.VerifyMaxDirItems(dstIIP.GetINode(-2).AsDirectory(), parentPath);
			}
		}

		/// <summary>
		/// <br />
		/// Note: This is to be used by
		/// <see cref="FSEditLogLoader"/>
		/// only.
		/// <br />
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[Obsolete]
		internal static bool RenameForEditLog(FSDirectory fsd, string src, string dst, long
			 timestamp)
		{
			if (fsd.IsDir(dst))
			{
				dst += Path.Separator + new Path(src).GetName();
			}
			INodesInPath srcIIP = fsd.GetINodesInPath4Write(src, false);
			INodesInPath dstIIP = fsd.GetINodesInPath4Write(dst, false);
			return UnprotectedRenameTo(fsd, src, dst, srcIIP, dstIIP, timestamp);
		}

		/// <summary>Change a path name</summary>
		/// <param name="fsd">FSDirectory</param>
		/// <param name="src">source path</param>
		/// <param name="dst">destination path</param>
		/// <returns>true if rename succeeds; false otherwise</returns>
		/// <exception cref="System.IO.IOException"/>
		[System.ObsoleteAttribute(@"See RenameToInt(FSDirectory, string, string, bool, Org.Apache.Hadoop.FS.Options.Rename[])"
			)]
		internal static bool UnprotectedRenameTo(FSDirectory fsd, string src, string dst, 
			INodesInPath srcIIP, INodesInPath dstIIP, long timestamp)
		{
			System.Diagnostics.Debug.Assert(fsd.HasWriteLock());
			INode srcInode = srcIIP.GetLastINode();
			try
			{
				ValidateRenameSource(srcIIP);
			}
			catch (SnapshotException e)
			{
				throw;
			}
			catch (IOException)
			{
				return false;
			}
			// validate the destination
			if (dst.Equals(src))
			{
				return true;
			}
			try
			{
				ValidateDestination(src, dst, srcInode);
			}
			catch (IOException)
			{
				return false;
			}
			if (dstIIP.GetLastINode() != null)
			{
				NameNode.stateChangeLog.Warn("DIR* FSDirectory.unprotectedRenameTo: " + "failed to rename "
					 + src + " to " + dst + " because destination " + "exists");
				return false;
			}
			INode dstParent = dstIIP.GetINode(-2);
			if (dstParent == null)
			{
				NameNode.stateChangeLog.Warn("DIR* FSDirectory.unprotectedRenameTo: " + "failed to rename "
					 + src + " to " + dst + " because destination's " + "parent does not exist");
				return false;
			}
			fsd.ezManager.CheckMoveValidity(srcIIP, dstIIP, src);
			// Ensure dst has quota to accommodate rename
			VerifyFsLimitsForRename(fsd, srcIIP, dstIIP);
			VerifyQuotaForRename(fsd, srcIIP, dstIIP);
			FSDirRenameOp.RenameOperation tx = new FSDirRenameOp.RenameOperation(fsd, src, dst
				, srcIIP, dstIIP);
			bool added = false;
			try
			{
				// remove src
				if (!tx.RemoveSrc4OldRename())
				{
					return false;
				}
				added = tx.AddSourceToDestination();
				if (added)
				{
					if (NameNode.stateChangeLog.IsDebugEnabled())
					{
						NameNode.stateChangeLog.Debug("DIR* FSDirectory" + ".unprotectedRenameTo: " + src
							 + " is renamed to " + dst);
					}
					tx.UpdateMtimeAndLease(timestamp);
					tx.UpdateQuotasInSourceTree(fsd.GetBlockStoragePolicySuite());
					return true;
				}
			}
			finally
			{
				if (!added)
				{
					tx.RestoreSource();
				}
			}
			NameNode.stateChangeLog.Warn("DIR* FSDirectory.unprotectedRenameTo: " + "failed to rename "
				 + src + " to " + dst);
			return false;
		}

		/// <summary>The new rename which has the POSIX semantic.</summary>
		/// <exception cref="System.IO.IOException"/>
		internal static KeyValuePair<INode.BlocksMapUpdateInfo, HdfsFileStatus> RenameToInt
			(FSDirectory fsd, string srcArg, string dstArg, bool logRetryCache, params Options.Rename
			[] options)
		{
			string src = srcArg;
			string dst = dstArg;
			if (NameNode.stateChangeLog.IsDebugEnabled())
			{
				NameNode.stateChangeLog.Debug("DIR* NameSystem.renameTo: with options -" + " " + 
					src + " to " + dst);
			}
			if (!DFSUtil.IsValidName(dst))
			{
				throw new InvalidPathException("Invalid name: " + dst);
			}
			FSPermissionChecker pc = fsd.GetPermissionChecker();
			byte[][] srcComponents = FSDirectory.GetPathComponentsForReservedPath(src);
			byte[][] dstComponents = FSDirectory.GetPathComponentsForReservedPath(dst);
			INode.BlocksMapUpdateInfo collectedBlocks = new INode.BlocksMapUpdateInfo();
			src = fsd.ResolvePath(pc, src, srcComponents);
			dst = fsd.ResolvePath(pc, dst, dstComponents);
			RenameTo(fsd, pc, src, dst, collectedBlocks, logRetryCache, options);
			INodesInPath dstIIP = fsd.GetINodesInPath(dst, false);
			HdfsFileStatus resultingStat = fsd.GetAuditFileInfo(dstIIP);
			return new AbstractMap.SimpleImmutableEntry<INode.BlocksMapUpdateInfo, HdfsFileStatus
				>(collectedBlocks, resultingStat);
		}

		/// <seealso>
		/// 
		/// <see cref="UnprotectedRenameTo(FSDirectory, string, string, INodesInPath, INodesInPath, long, BlocksMapUpdateInfo, Org.Apache.Hadoop.FS.Options.Rename[])
		/// 	"/>
		/// </seealso>
		/// <exception cref="System.IO.IOException"/>
		internal static void RenameTo(FSDirectory fsd, FSPermissionChecker pc, string src
			, string dst, INode.BlocksMapUpdateInfo collectedBlocks, bool logRetryCache, params 
			Options.Rename[] options)
		{
			INodesInPath srcIIP = fsd.GetINodesInPath4Write(src, false);
			INodesInPath dstIIP = fsd.GetINodesInPath4Write(dst, false);
			if (fsd.IsPermissionEnabled())
			{
				// Rename does not operate on link targets
				// Do not resolveLink when checking permissions of src and dst
				// Check write access to parent of src
				fsd.CheckPermission(pc, srcIIP, false, null, FsAction.Write, null, null, false);
				// Check write access to ancestor of dst
				fsd.CheckPermission(pc, dstIIP, false, FsAction.Write, null, null, null, false);
			}
			if (NameNode.stateChangeLog.IsDebugEnabled())
			{
				NameNode.stateChangeLog.Debug("DIR* FSDirectory.renameTo: " + src + " to " + dst);
			}
			long mtime = Time.Now();
			fsd.WriteLock();
			try
			{
				if (UnprotectedRenameTo(fsd, src, dst, srcIIP, dstIIP, mtime, collectedBlocks, options
					))
				{
					FSDirDeleteOp.IncrDeletedFileCount(1);
				}
			}
			finally
			{
				fsd.WriteUnlock();
			}
			fsd.GetEditLog().LogRename(src, dst, mtime, logRetryCache, options);
		}

		/// <summary>Rename src to dst.</summary>
		/// <remarks>
		/// Rename src to dst.
		/// <br />
		/// Note: This is to be used by
		/// <see cref="FSEditLogLoader"/>
		/// only.
		/// <br />
		/// </remarks>
		/// <param name="fsd">FSDirectory</param>
		/// <param name="src">source path</param>
		/// <param name="dst">destination path</param>
		/// <param name="timestamp">modification time</param>
		/// <param name="options">Rename options</param>
		/// <exception cref="System.IO.IOException"/>
		internal static bool RenameForEditLog(FSDirectory fsd, string src, string dst, long
			 timestamp, params Options.Rename[] options)
		{
			INode.BlocksMapUpdateInfo collectedBlocks = new INode.BlocksMapUpdateInfo();
			INodesInPath srcIIP = fsd.GetINodesInPath4Write(src, false);
			INodesInPath dstIIP = fsd.GetINodesInPath4Write(dst, false);
			bool ret = UnprotectedRenameTo(fsd, src, dst, srcIIP, dstIIP, timestamp, collectedBlocks
				, options);
			if (!collectedBlocks.GetToDeleteList().IsEmpty())
			{
				fsd.GetFSNamesystem().RemoveBlocksAndUpdateSafemodeTotal(collectedBlocks);
			}
			return ret;
		}

		/// <summary>Rename src to dst.</summary>
		/// <remarks>
		/// Rename src to dst.
		/// See
		/// <see cref="Org.Apache.Hadoop.Hdfs.DistributedFileSystem.Rename(Org.Apache.Hadoop.FS.Path, Org.Apache.Hadoop.FS.Path, Org.Apache.Hadoop.FS.Options.Rename[])
		/// 	"/>
		/// for details related to rename semantics and exceptions.
		/// </remarks>
		/// <param name="fsd">FSDirectory</param>
		/// <param name="src">source path</param>
		/// <param name="dst">destination path</param>
		/// <param name="timestamp">modification time</param>
		/// <param name="collectedBlocks">blocks to be removed</param>
		/// <param name="options">Rename options</param>
		/// <returns>whether a file/directory gets overwritten in the dst path</returns>
		/// <exception cref="System.IO.IOException"/>
		internal static bool UnprotectedRenameTo(FSDirectory fsd, string src, string dst, 
			INodesInPath srcIIP, INodesInPath dstIIP, long timestamp, INode.BlocksMapUpdateInfo
			 collectedBlocks, params Options.Rename[] options)
		{
			System.Diagnostics.Debug.Assert(fsd.HasWriteLock());
			bool overwrite = options != null && Arrays.AsList(options).Contains(Options.Rename
				.Overwrite);
			string error;
			INode srcInode = srcIIP.GetLastINode();
			ValidateRenameSource(srcIIP);
			// validate the destination
			if (dst.Equals(src))
			{
				throw new FileAlreadyExistsException("The source " + src + " and destination " + 
					dst + " are the same");
			}
			ValidateDestination(src, dst, srcInode);
			if (dstIIP.Length() == 1)
			{
				error = "rename destination cannot be the root";
				NameNode.stateChangeLog.Warn("DIR* FSDirectory.unprotectedRenameTo: " + error);
				throw new IOException(error);
			}
			BlockStoragePolicySuite bsps = fsd.GetBlockStoragePolicySuite();
			fsd.ezManager.CheckMoveValidity(srcIIP, dstIIP, src);
			INode dstInode = dstIIP.GetLastINode();
			IList<INodeDirectory> snapshottableDirs = new AList<INodeDirectory>();
			if (dstInode != null)
			{
				// Destination exists
				ValidateOverwrite(src, dst, overwrite, srcInode, dstInode);
				FSDirSnapshotOp.CheckSnapshot(dstInode, snapshottableDirs);
			}
			INode dstParent = dstIIP.GetINode(-2);
			if (dstParent == null)
			{
				error = "rename destination parent " + dst + " not found.";
				NameNode.stateChangeLog.Warn("DIR* FSDirectory.unprotectedRenameTo: " + error);
				throw new FileNotFoundException(error);
			}
			if (!dstParent.IsDirectory())
			{
				error = "rename destination parent " + dst + " is a file.";
				NameNode.stateChangeLog.Warn("DIR* FSDirectory.unprotectedRenameTo: " + error);
				throw new ParentNotDirectoryException(error);
			}
			// Ensure dst has quota to accommodate rename
			VerifyFsLimitsForRename(fsd, srcIIP, dstIIP);
			VerifyQuotaForRename(fsd, srcIIP, dstIIP);
			FSDirRenameOp.RenameOperation tx = new FSDirRenameOp.RenameOperation(fsd, src, dst
				, srcIIP, dstIIP);
			bool undoRemoveSrc = true;
			tx.RemoveSrc();
			bool undoRemoveDst = false;
			long removedNum = 0;
			try
			{
				if (dstInode != null)
				{
					// dst exists, remove it
					removedNum = tx.RemoveDst();
					if (removedNum != -1)
					{
						undoRemoveDst = true;
					}
				}
				// add src as dst to complete rename
				if (tx.AddSourceToDestination())
				{
					undoRemoveSrc = false;
					if (NameNode.stateChangeLog.IsDebugEnabled())
					{
						NameNode.stateChangeLog.Debug("DIR* FSDirectory.unprotectedRenameTo: " + src + " is renamed to "
							 + dst);
					}
					tx.UpdateMtimeAndLease(timestamp);
					// Collect the blocks and remove the lease for previous dst
					bool filesDeleted = false;
					if (undoRemoveDst)
					{
						undoRemoveDst = false;
						if (removedNum > 0)
						{
							filesDeleted = tx.CleanDst(bsps, collectedBlocks);
						}
					}
					if (snapshottableDirs.Count > 0)
					{
						// There are snapshottable directories (without snapshots) to be
						// deleted. Need to update the SnapshotManager.
						fsd.GetFSNamesystem().RemoveSnapshottableDirs(snapshottableDirs);
					}
					tx.UpdateQuotasInSourceTree(bsps);
					return filesDeleted;
				}
			}
			finally
			{
				if (undoRemoveSrc)
				{
					tx.RestoreSource();
				}
				if (undoRemoveDst)
				{
					// Rename failed - restore dst
					tx.RestoreDst(bsps);
				}
			}
			NameNode.stateChangeLog.Warn("DIR* FSDirectory.unprotectedRenameTo: " + "failed to rename "
				 + src + " to " + dst);
			throw new IOException("rename from " + src + " to " + dst + " failed.");
		}

		/// <exception cref="System.IO.IOException"/>
		[System.ObsoleteAttribute(@"Use RenameToInt(FSDirectory, string, string, bool, Org.Apache.Hadoop.FS.Options.Rename[])"
			)]
		private static bool RenameTo(FSDirectory fsd, FSPermissionChecker pc, string src, 
			string dst, bool logRetryCache)
		{
			// Rename does not operate on link targets
			// Do not resolveLink when checking permissions of src and dst
			// Check write access to parent of src
			INodesInPath srcIIP = fsd.GetINodesInPath4Write(src, false);
			// Note: We should not be doing this.  This is move() not renameTo().
			string actualDst = fsd.IsDir(dst) ? dst + Path.Separator + new Path(src).GetName(
				) : dst;
			INodesInPath dstIIP = fsd.GetINodesInPath4Write(actualDst, false);
			if (fsd.IsPermissionEnabled())
			{
				fsd.CheckPermission(pc, srcIIP, false, null, FsAction.Write, null, null, false);
				// Check write access to ancestor of dst
				fsd.CheckPermission(pc, dstIIP, false, FsAction.Write, null, null, null, false);
			}
			if (NameNode.stateChangeLog.IsDebugEnabled())
			{
				NameNode.stateChangeLog.Debug("DIR* FSDirectory.renameTo: " + src + " to " + dst);
			}
			long mtime = Time.Now();
			bool stat = false;
			fsd.WriteLock();
			try
			{
				stat = UnprotectedRenameTo(fsd, src, actualDst, srcIIP, dstIIP, mtime);
			}
			finally
			{
				fsd.WriteUnlock();
			}
			if (stat)
			{
				fsd.GetEditLog().LogRename(src, actualDst, mtime, logRetryCache);
				return true;
			}
			return false;
		}

		/// <exception cref="System.IO.IOException"/>
		private static void ValidateDestination(string src, string dst, INode srcInode)
		{
			string error;
			if (srcInode.IsSymlink() && dst.Equals(srcInode.AsSymlink().GetSymlinkString()))
			{
				throw new FileAlreadyExistsException("Cannot rename symlink " + src + " to its target "
					 + dst);
			}
			// dst cannot be a directory or a file under src
			if (dst.StartsWith(src) && dst[src.Length] == Path.SeparatorChar)
			{
				error = "Rename destination " + dst + " is a directory or file under source " + src;
				NameNode.stateChangeLog.Warn("DIR* FSDirectory.unprotectedRenameTo: " + error);
				throw new IOException(error);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static void ValidateOverwrite(string src, string dst, bool overwrite, INode
			 srcInode, INode dstInode)
		{
			string error;
			// It's OK to rename a file to a symlink and vice versa
			if (dstInode.IsDirectory() != srcInode.IsDirectory())
			{
				error = "Source " + src + " and destination " + dst + " must both be directories";
				NameNode.stateChangeLog.Warn("DIR* FSDirectory.unprotectedRenameTo: " + error);
				throw new IOException(error);
			}
			if (!overwrite)
			{
				// If destination exists, overwrite flag must be true
				error = "rename destination " + dst + " already exists";
				NameNode.stateChangeLog.Warn("DIR* FSDirectory.unprotectedRenameTo: " + error);
				throw new FileAlreadyExistsException(error);
			}
			if (dstInode.IsDirectory())
			{
				ReadOnlyList<INode> children = dstInode.AsDirectory().GetChildrenList(Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
					.CurrentStateId);
				if (!children.IsEmpty())
				{
					error = "rename destination directory is not empty: " + dst;
					NameNode.stateChangeLog.Warn("DIR* FSDirectory.unprotectedRenameTo: " + error);
					throw new IOException(error);
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static void ValidateRenameSource(INodesInPath srcIIP)
		{
			string error;
			INode srcInode = srcIIP.GetLastINode();
			// validate source
			if (srcInode == null)
			{
				error = "rename source " + srcIIP.GetPath() + " is not found.";
				NameNode.stateChangeLog.Warn("DIR* FSDirectory.unprotectedRenameTo: " + error);
				throw new FileNotFoundException(error);
			}
			if (srcIIP.Length() == 1)
			{
				error = "rename source cannot be the root";
				NameNode.stateChangeLog.Warn("DIR* FSDirectory.unprotectedRenameTo: " + error);
				throw new IOException(error);
			}
			// srcInode and its subtree cannot contain snapshottable directories with
			// snapshots
			FSDirSnapshotOp.CheckSnapshot(srcInode, null);
		}

		private class RenameOperation
		{
			private readonly FSDirectory fsd;

			private INodesInPath srcIIP;

			private readonly INodesInPath srcParentIIP;

			private INodesInPath dstIIP;

			private readonly INodesInPath dstParentIIP;

			private readonly string src;

			private readonly string dst;

			private readonly INodeReference.WithCount withCount;

			private readonly int srcRefDstSnapshot;

			private readonly INodeDirectory srcParent;

			private readonly byte[] srcChildName;

			private readonly bool isSrcInSnapshot;

			private readonly bool srcChildIsReference;

			private readonly QuotaCounts oldSrcCounts;

			private INode srcChild;

			private INode oldDstChild;

			/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.QuotaExceededException"/>
			internal RenameOperation(FSDirectory fsd, string src, string dst, INodesInPath srcIIP
				, INodesInPath dstIIP)
			{
				this.fsd = fsd;
				this.src = src;
				this.dst = dst;
				this.srcIIP = srcIIP;
				this.dstIIP = dstIIP;
				this.srcParentIIP = srcIIP.GetParentINodesInPath();
				this.dstParentIIP = dstIIP.GetParentINodesInPath();
				BlockStoragePolicySuite bsps = fsd.GetBlockStoragePolicySuite();
				srcChild = this.srcIIP.GetLastINode();
				srcChildName = srcChild.GetLocalNameBytes();
				int srcLatestSnapshotId = srcIIP.GetLatestSnapshotId();
				isSrcInSnapshot = srcChild.IsInLatestSnapshot(srcLatestSnapshotId);
				srcChildIsReference = srcChild.IsReference();
				srcParent = this.srcIIP.GetINode(-2).AsDirectory();
				// Record the snapshot on srcChild. After the rename, before any new
				// snapshot is taken on the dst tree, changes will be recorded in the
				// latest snapshot of the src tree.
				if (isSrcInSnapshot)
				{
					srcChild.RecordModification(srcLatestSnapshotId);
				}
				// check srcChild for reference
				srcRefDstSnapshot = srcChildIsReference ? srcChild.AsReference().GetDstSnapshotId
					() : Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.CurrentStateId;
				oldSrcCounts = new QuotaCounts.Builder().Build();
				if (isSrcInSnapshot)
				{
					INodeReference.WithName withName = srcParent.ReplaceChild4ReferenceWithName(srcChild
						, srcLatestSnapshotId);
					withCount = (INodeReference.WithCount)withName.GetReferredINode();
					srcChild = withName;
					this.srcIIP = INodesInPath.Replace(srcIIP, srcIIP.Length() - 1, srcChild);
					// get the counts before rename
					withCount.GetReferredINode().ComputeQuotaUsage(bsps, oldSrcCounts, true);
				}
				else
				{
					if (srcChildIsReference)
					{
						// srcChild is reference but srcChild is not in latest snapshot
						withCount = (INodeReference.WithCount)srcChild.AsReference().GetReferredINode();
					}
					else
					{
						withCount = null;
					}
				}
			}

			/// <exception cref="System.IO.IOException"/>
			internal virtual long RemoveSrc()
			{
				long removedNum = fsd.RemoveLastINode(srcIIP);
				if (removedNum == -1)
				{
					string error = "Failed to rename " + src + " to " + dst + " because the source can not be removed";
					NameNode.stateChangeLog.Warn("DIR* FSDirRenameOp.unprotectedRenameTo:" + error);
					throw new IOException(error);
				}
				else
				{
					// update the quota count if necessary
					fsd.UpdateCountForDelete(srcChild, srcIIP);
					srcIIP = INodesInPath.Replace(srcIIP, srcIIP.Length() - 1, null);
					return removedNum;
				}
			}

			internal virtual bool RemoveSrc4OldRename()
			{
				long removedSrc = fsd.RemoveLastINode(srcIIP);
				if (removedSrc == -1)
				{
					NameNode.stateChangeLog.Warn("DIR* FSDirRenameOp.unprotectedRenameTo: " + "failed to rename "
						 + src + " to " + dst + " because the source" + " can not be removed");
					return false;
				}
				else
				{
					// update the quota count if necessary
					fsd.UpdateCountForDelete(srcChild, srcIIP);
					srcIIP = INodesInPath.Replace(srcIIP, srcIIP.Length() - 1, null);
					return true;
				}
			}

			internal virtual long RemoveDst()
			{
				long removedNum = fsd.RemoveLastINode(dstIIP);
				if (removedNum != -1)
				{
					oldDstChild = dstIIP.GetLastINode();
					// update the quota count if necessary
					fsd.UpdateCountForDelete(oldDstChild, dstIIP);
					dstIIP = INodesInPath.Replace(dstIIP, dstIIP.Length() - 1, null);
				}
				return removedNum;
			}

			internal virtual bool AddSourceToDestination()
			{
				INode dstParent = dstParentIIP.GetLastINode();
				byte[] dstChildName = dstIIP.GetLastLocalName();
				INode toDst;
				if (withCount == null)
				{
					srcChild.SetLocalName(dstChildName);
					toDst = srcChild;
				}
				else
				{
					withCount.GetReferredINode().SetLocalName(dstChildName);
					toDst = new INodeReference.DstReference(dstParent.AsDirectory(), withCount, dstIIP
						.GetLatestSnapshotId());
				}
				return fsd.AddLastINodeNoQuotaCheck(dstParentIIP, toDst) != null;
			}

			/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.QuotaExceededException"/>
			internal virtual void UpdateMtimeAndLease(long timestamp)
			{
				srcParent.UpdateModificationTime(timestamp, srcIIP.GetLatestSnapshotId());
				INode dstParent = dstParentIIP.GetLastINode();
				dstParent.UpdateModificationTime(timestamp, dstIIP.GetLatestSnapshotId());
				// update moved lease with new filename
				fsd.GetFSNamesystem().UnprotectedChangeLease(src, dst);
			}

			/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.QuotaExceededException"/>
			internal virtual void RestoreSource()
			{
				// Rename failed - restore src
				INode oldSrcChild = srcChild;
				// put it back
				if (withCount == null)
				{
					srcChild.SetLocalName(srcChildName);
				}
				else
				{
					if (!srcChildIsReference)
					{
						// src must be in snapshot
						// the withCount node will no longer be used thus no need to update
						// its reference number here
						srcChild = withCount.GetReferredINode();
						srcChild.SetLocalName(srcChildName);
					}
					else
					{
						withCount.RemoveReference(oldSrcChild.AsReference());
						srcChild = new INodeReference.DstReference(srcParent, withCount, srcRefDstSnapshot
							);
						withCount.GetReferredINode().SetLocalName(srcChildName);
					}
				}
				if (isSrcInSnapshot)
				{
					srcParent.UndoRename4ScrParent(oldSrcChild.AsReference(), srcChild);
				}
				else
				{
					// srcParent is not an INodeDirectoryWithSnapshot, we only need to add
					// the srcChild back
					fsd.AddLastINodeNoQuotaCheck(srcParentIIP, srcChild);
				}
			}

			/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.QuotaExceededException"/>
			internal virtual void RestoreDst(BlockStoragePolicySuite bsps)
			{
				Preconditions.CheckState(oldDstChild != null);
				INodeDirectory dstParent = dstParentIIP.GetLastINode().AsDirectory();
				if (dstParent.IsWithSnapshot())
				{
					dstParent.UndoRename4DstParent(bsps, oldDstChild, dstIIP.GetLatestSnapshotId());
				}
				else
				{
					fsd.AddLastINodeNoQuotaCheck(dstParentIIP, oldDstChild);
				}
				if (oldDstChild != null && oldDstChild.IsReference())
				{
					INodeReference removedDstRef = oldDstChild.AsReference();
					INodeReference.WithCount wc = (INodeReference.WithCount)removedDstRef.GetReferredINode
						().AsReference();
					wc.AddReference(removedDstRef);
				}
			}

			/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.QuotaExceededException"/>
			internal virtual bool CleanDst(BlockStoragePolicySuite bsps, INode.BlocksMapUpdateInfo
				 collectedBlocks)
			{
				Preconditions.CheckState(oldDstChild != null);
				IList<INode> removedINodes = new ChunkedArrayList<INode>();
				bool filesDeleted;
				if (!oldDstChild.IsInLatestSnapshot(dstIIP.GetLatestSnapshotId()))
				{
					oldDstChild.DestroyAndCollectBlocks(bsps, collectedBlocks, removedINodes);
					filesDeleted = true;
				}
				else
				{
					filesDeleted = oldDstChild.CleanSubtree(bsps, Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
						.CurrentStateId, dstIIP.GetLatestSnapshotId(), collectedBlocks, removedINodes).GetNameSpace
						() >= 0;
				}
				fsd.GetFSNamesystem().RemoveLeasesAndINodes(src, removedINodes, false);
				return filesDeleted;
			}

			/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.QuotaExceededException"/>
			internal virtual void UpdateQuotasInSourceTree(BlockStoragePolicySuite bsps)
			{
				// update the quota usage in src tree
				if (isSrcInSnapshot)
				{
					// get the counts after rename
					QuotaCounts newSrcCounts = srcChild.ComputeQuotaUsage(bsps, new QuotaCounts.Builder
						().Build(), false);
					newSrcCounts.Subtract(oldSrcCounts);
					srcParent.AddSpaceConsumed(newSrcCounts, false);
				}
			}
		}

		internal class RenameOldResult
		{
			internal readonly bool success;

			internal readonly HdfsFileStatus auditStat;

			internal RenameOldResult(bool success, HdfsFileStatus auditStat)
			{
				this.success = success;
				this.auditStat = auditStat;
			}
		}
	}
}
