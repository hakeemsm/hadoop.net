using System;
using System.IO;
using Com.Google.Common.Base;
using Org.Apache.Commons.IO;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot;
using Org.Apache.Hadoop.Hdfs.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	internal class FSDirStatAndListingOp
	{
		/// <exception cref="System.IO.IOException"/>
		internal static DirectoryListing GetListingInt(FSDirectory fsd, string srcArg, byte
			[] startAfter, bool needLocation)
		{
			FSPermissionChecker pc = fsd.GetPermissionChecker();
			byte[][] pathComponents = FSDirectory.GetPathComponentsForReservedPath(srcArg);
			string startAfterString = new string(startAfter, Charsets.Utf8);
			string src = fsd.ResolvePath(pc, srcArg, pathComponents);
			INodesInPath iip = fsd.GetINodesInPath(src, true);
			// Get file name when startAfter is an INodePath
			if (FSDirectory.IsReservedName(startAfterString))
			{
				byte[][] startAfterComponents = FSDirectory.GetPathComponentsForReservedPath(startAfterString
					);
				try
				{
					string tmp = FSDirectory.ResolvePath(src, startAfterComponents, fsd);
					byte[][] regularPath = INode.GetPathComponents(tmp);
					startAfter = regularPath[regularPath.Length - 1];
				}
				catch (IOException)
				{
					// Possibly the inode is deleted
					throw new DirectoryListingStartAfterNotFoundException("Can't find startAfter " + 
						startAfterString);
				}
			}
			bool isSuperUser = true;
			if (fsd.IsPermissionEnabled())
			{
				if (iip.GetLastINode() != null && iip.GetLastINode().IsDirectory())
				{
					fsd.CheckPathAccess(pc, iip, FsAction.ReadExecute);
				}
				else
				{
					fsd.CheckTraverse(pc, iip);
				}
				isSuperUser = pc.IsSuperUser();
			}
			return GetListing(fsd, iip, src, startAfter, needLocation, isSuperUser);
		}

		/// <summary>Get the file info for a specific file.</summary>
		/// <param name="srcArg">The string representation of the path to the file</param>
		/// <param name="resolveLink">
		/// whether to throw UnresolvedLinkException
		/// if src refers to a symlink
		/// </param>
		/// <returns>
		/// object containing information regarding the file
		/// or null if file not found
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		internal static HdfsFileStatus GetFileInfo(FSDirectory fsd, string srcArg, bool resolveLink
			)
		{
			string src = srcArg;
			if (!DFSUtil.IsValidName(src))
			{
				throw new InvalidPathException("Invalid file name: " + src);
			}
			FSPermissionChecker pc = fsd.GetPermissionChecker();
			byte[][] pathComponents = FSDirectory.GetPathComponentsForReservedPath(src);
			src = fsd.ResolvePath(pc, src, pathComponents);
			INodesInPath iip = fsd.GetINodesInPath(src, resolveLink);
			bool isSuperUser = true;
			if (fsd.IsPermissionEnabled())
			{
				fsd.CheckPermission(pc, iip, false, null, null, null, null, false);
				isSuperUser = pc.IsSuperUser();
			}
			return GetFileInfo(fsd, src, resolveLink, FSDirectory.IsReservedRawName(srcArg), 
				isSuperUser);
		}

		/// <summary>Returns true if the file is closed</summary>
		/// <exception cref="System.IO.IOException"/>
		internal static bool IsFileClosed(FSDirectory fsd, string src)
		{
			FSPermissionChecker pc = fsd.GetPermissionChecker();
			byte[][] pathComponents = FSDirectory.GetPathComponentsForReservedPath(src);
			src = fsd.ResolvePath(pc, src, pathComponents);
			INodesInPath iip = fsd.GetINodesInPath(src, true);
			if (fsd.IsPermissionEnabled())
			{
				fsd.CheckTraverse(pc, iip);
			}
			return !INodeFile.ValueOf(iip.GetLastINode(), src).IsUnderConstruction();
		}

		/// <exception cref="System.IO.IOException"/>
		internal static ContentSummary GetContentSummary(FSDirectory fsd, string src)
		{
			byte[][] pathComponents = FSDirectory.GetPathComponentsForReservedPath(src);
			FSPermissionChecker pc = fsd.GetPermissionChecker();
			src = fsd.ResolvePath(pc, src, pathComponents);
			INodesInPath iip = fsd.GetINodesInPath(src, false);
			if (fsd.IsPermissionEnabled())
			{
				fsd.CheckPermission(pc, iip, false, null, null, null, FsAction.ReadExecute);
			}
			return GetContentSummaryInt(fsd, iip);
		}

		private static byte GetStoragePolicyID(byte inodePolicy, byte parentPolicy)
		{
			return inodePolicy != BlockStoragePolicySuite.IdUnspecified ? inodePolicy : parentPolicy;
		}

		/// <summary>
		/// Get a partial listing of the indicated directory
		/// We will stop when any of the following conditions is met:
		/// 1) this.lsLimit files have been added
		/// 2) needLocation is true AND enough files have been added such
		/// that at least this.lsLimit block locations are in the response
		/// </summary>
		/// <param name="fsd">FSDirectory</param>
		/// <param name="iip">
		/// the INodesInPath instance containing all the INodes along the
		/// path
		/// </param>
		/// <param name="src">the directory name</param>
		/// <param name="startAfter">the name to start listing after</param>
		/// <param name="needLocation">if block locations are returned</param>
		/// <returns>a partial listing starting after startAfter</returns>
		/// <exception cref="System.IO.IOException"/>
		private static DirectoryListing GetListing(FSDirectory fsd, INodesInPath iip, string
			 src, byte[] startAfter, bool needLocation, bool isSuperUser)
		{
			string srcs = FSDirectory.NormalizePath(src);
			bool isRawPath = FSDirectory.IsReservedRawName(src);
			fsd.ReadLock();
			try
			{
				if (srcs.EndsWith(HdfsConstants.SeparatorDotSnapshotDir))
				{
					return GetSnapshotsListing(fsd, srcs, startAfter);
				}
				int snapshot = iip.GetPathSnapshotId();
				INode targetNode = iip.GetLastINode();
				if (targetNode == null)
				{
					return null;
				}
				byte parentStoragePolicy = isSuperUser ? targetNode.GetStoragePolicyID() : BlockStoragePolicySuite
					.IdUnspecified;
				if (!targetNode.IsDirectory())
				{
					return new DirectoryListing(new HdfsFileStatus[] { CreateFileStatus(fsd, src, HdfsFileStatus
						.EmptyName, targetNode, needLocation, parentStoragePolicy, snapshot, isRawPath, 
						iip) }, 0);
				}
				INodeDirectory dirInode = targetNode.AsDirectory();
				ReadOnlyList<INode> contents = dirInode.GetChildrenList(snapshot);
				int startChild = INodeDirectory.NextChild(contents, startAfter);
				int totalNumChildren = contents.Size();
				int numOfListing = Math.Min(totalNumChildren - startChild, fsd.GetLsLimit());
				int locationBudget = fsd.GetLsLimit();
				int listingCnt = 0;
				HdfsFileStatus[] listing = new HdfsFileStatus[numOfListing];
				for (int i = 0; i < numOfListing && locationBudget > 0; i++)
				{
					INode cur = contents.Get(startChild + i);
					byte curPolicy = isSuperUser && !cur.IsSymlink() ? cur.GetLocalStoragePolicyID() : 
						BlockStoragePolicySuite.IdUnspecified;
					listing[i] = CreateFileStatus(fsd, src, cur.GetLocalNameBytes(), cur, needLocation
						, GetStoragePolicyID(curPolicy, parentStoragePolicy), snapshot, isRawPath, iip);
					listingCnt++;
					if (needLocation)
					{
						// Once we  hit lsLimit locations, stop.
						// This helps to prevent excessively large response payloads.
						// Approximate #locations with locatedBlockCount() * repl_factor
						LocatedBlocks blks = ((HdfsLocatedFileStatus)listing[i]).GetBlockLocations();
						locationBudget -= (blks == null) ? 0 : blks.LocatedBlockCount() * listing[i].GetReplication
							();
					}
				}
				// truncate return array if necessary
				if (listingCnt < numOfListing)
				{
					listing = Arrays.CopyOf(listing, listingCnt);
				}
				return new DirectoryListing(listing, totalNumChildren - startChild - listingCnt);
			}
			finally
			{
				fsd.ReadUnlock();
			}
		}

		/// <summary>Get a listing of all the snapshots of a snapshottable directory</summary>
		/// <exception cref="System.IO.IOException"/>
		private static DirectoryListing GetSnapshotsListing(FSDirectory fsd, string src, 
			byte[] startAfter)
		{
			Preconditions.CheckState(fsd.HasReadLock());
			Preconditions.CheckArgument(src.EndsWith(HdfsConstants.SeparatorDotSnapshotDir), 
				"%s does not end with %s", src, HdfsConstants.SeparatorDotSnapshotDir);
			string dirPath = FSDirectory.NormalizePath(Sharpen.Runtime.Substring(src, 0, src.
				Length - HdfsConstants.DotSnapshotDir.Length));
			INode node = fsd.GetINode(dirPath);
			INodeDirectory dirNode = INodeDirectory.ValueOf(node, dirPath);
			DirectorySnapshottableFeature sf = dirNode.GetDirectorySnapshottableFeature();
			if (sf == null)
			{
				throw new SnapshotException("Directory is not a snapshottable directory: " + dirPath
					);
			}
			ReadOnlyList<Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot> snapshots = 
				sf.GetSnapshotList();
			int skipSize = ReadOnlyList.Util.BinarySearch(snapshots, startAfter);
			skipSize = skipSize < 0 ? -skipSize - 1 : skipSize + 1;
			int numOfListing = Math.Min(snapshots.Size() - skipSize, fsd.GetLsLimit());
			HdfsFileStatus[] listing = new HdfsFileStatus[numOfListing];
			for (int i = 0; i < numOfListing; i++)
			{
				Snapshot.Root sRoot = snapshots.Get(i + skipSize).GetRoot();
				listing[i] = CreateFileStatus(fsd, src, sRoot.GetLocalNameBytes(), sRoot, BlockStoragePolicySuite
					.IdUnspecified, Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.CurrentStateId
					, false, INodesInPath.FromINode(sRoot));
			}
			return new DirectoryListing(listing, snapshots.Size() - skipSize - numOfListing);
		}

		/// <summary>Get the file info for a specific file.</summary>
		/// <param name="fsd">FSDirectory</param>
		/// <param name="src">The string representation of the path to the file</param>
		/// <param name="isRawPath">true if a /.reserved/raw pathname was passed by the user</param>
		/// <param name="includeStoragePolicy">whether to include storage policy</param>
		/// <returns>
		/// object containing information regarding the file
		/// or null if file not found
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		internal static HdfsFileStatus GetFileInfo(FSDirectory fsd, string path, INodesInPath
			 src, bool isRawPath, bool includeStoragePolicy)
		{
			fsd.ReadLock();
			try
			{
				INode i = src.GetLastINode();
				byte policyId = includeStoragePolicy && i != null && !i.IsSymlink() ? i.GetStoragePolicyID
					() : BlockStoragePolicySuite.IdUnspecified;
				return i == null ? null : CreateFileStatus(fsd, path, HdfsFileStatus.EmptyName, i
					, policyId, src.GetPathSnapshotId(), isRawPath, src);
			}
			finally
			{
				fsd.ReadUnlock();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal static HdfsFileStatus GetFileInfo(FSDirectory fsd, string src, bool resolveLink
			, bool isRawPath, bool includeStoragePolicy)
		{
			string srcs = FSDirectory.NormalizePath(src);
			if (srcs.EndsWith(HdfsConstants.SeparatorDotSnapshotDir))
			{
				if (fsd.GetINode4DotSnapshot(srcs) != null)
				{
					return new HdfsFileStatus(0, true, 0, 0, 0, 0, null, null, null, null, HdfsFileStatus
						.EmptyName, -1L, 0, null, BlockStoragePolicySuite.IdUnspecified);
				}
				return null;
			}
			fsd.ReadLock();
			try
			{
				INodesInPath iip = fsd.GetINodesInPath(srcs, resolveLink);
				return GetFileInfo(fsd, src, iip, isRawPath, includeStoragePolicy);
			}
			finally
			{
				fsd.ReadUnlock();
			}
		}

		/// <summary>
		/// Currently we only support "ls /xxx/.snapshot" which will return all the
		/// snapshots of a directory.
		/// </summary>
		/// <remarks>
		/// Currently we only support "ls /xxx/.snapshot" which will return all the
		/// snapshots of a directory. The FSCommand Ls will first call getFileInfo to
		/// make sure the file/directory exists (before the real getListing call).
		/// Since we do not have a real INode for ".snapshot", we return an empty
		/// non-null HdfsFileStatus here.
		/// </remarks>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		private static HdfsFileStatus GetFileInfo4DotSnapshot(FSDirectory fsd, string src
			)
		{
			if (fsd.GetINode4DotSnapshot(src) != null)
			{
				return new HdfsFileStatus(0, true, 0, 0, 0, 0, null, null, null, null, HdfsFileStatus
					.EmptyName, -1L, 0, null, BlockStoragePolicySuite.IdUnspecified);
			}
			return null;
		}

		/// <summary>create an hdfs file status from an inode</summary>
		/// <param name="fsd">FSDirectory</param>
		/// <param name="path">the local name</param>
		/// <param name="node">inode</param>
		/// <param name="needLocation">if block locations need to be included or not</param>
		/// <param name="isRawPath">
		/// true if this is being called on behalf of a path in
		/// /.reserved/raw
		/// </param>
		/// <returns>a file status</returns>
		/// <exception cref="System.IO.IOException">if any error occurs</exception>
		internal static HdfsFileStatus CreateFileStatus(FSDirectory fsd, string fullPath, 
			byte[] path, INode node, bool needLocation, byte storagePolicy, int snapshot, bool
			 isRawPath, INodesInPath iip)
		{
			if (needLocation)
			{
				return CreateLocatedFileStatus(fsd, fullPath, path, node, storagePolicy, snapshot
					, isRawPath, iip);
			}
			else
			{
				return CreateFileStatus(fsd, fullPath, path, node, storagePolicy, snapshot, isRawPath
					, iip);
			}
		}

		/// <summary>Create FileStatus by file INode</summary>
		/// <exception cref="System.IO.IOException"/>
		internal static HdfsFileStatus CreateFileStatus(FSDirectory fsd, string fullPath, 
			byte[] path, INode node, byte storagePolicy, int snapshot, bool isRawPath, INodesInPath
			 iip)
		{
			long size = 0;
			// length is zero for directories
			short replication = 0;
			long blocksize = 0;
			bool isEncrypted;
			FileEncryptionInfo feInfo = isRawPath ? null : fsd.GetFileEncryptionInfo(node, snapshot
				, iip);
			if (node.IsFile())
			{
				INodeFile fileNode = node.AsFile();
				size = fileNode.ComputeFileSize(snapshot);
				replication = fileNode.GetFileReplication(snapshot);
				blocksize = fileNode.GetPreferredBlockSize();
				isEncrypted = (feInfo != null) || (isRawPath && fsd.IsInAnEZ(INodesInPath.FromINode
					(node)));
			}
			else
			{
				isEncrypted = fsd.IsInAnEZ(INodesInPath.FromINode(node));
			}
			int childrenNum = node.IsDirectory() ? node.AsDirectory().GetChildrenNum(snapshot
				) : 0;
			INodeAttributes nodeAttrs = fsd.GetAttributes(fullPath, path, node, snapshot);
			return new HdfsFileStatus(size, node.IsDirectory(), replication, blocksize, node.
				GetModificationTime(snapshot), node.GetAccessTime(snapshot), GetPermissionForFileStatus
				(nodeAttrs, isEncrypted), nodeAttrs.GetUserName(), nodeAttrs.GetGroupName(), node
				.IsSymlink() ? node.AsSymlink().GetSymlink() : null, path, node.GetId(), childrenNum
				, feInfo, storagePolicy);
		}

		/// <summary>Create FileStatus with location info by file INode</summary>
		/// <exception cref="System.IO.IOException"/>
		private static HdfsLocatedFileStatus CreateLocatedFileStatus(FSDirectory fsd, string
			 fullPath, byte[] path, INode node, byte storagePolicy, int snapshot, bool isRawPath
			, INodesInPath iip)
		{
			System.Diagnostics.Debug.Assert(fsd.HasReadLock());
			long size = 0;
			// length is zero for directories
			short replication = 0;
			long blocksize = 0;
			LocatedBlocks loc = null;
			bool isEncrypted;
			FileEncryptionInfo feInfo = isRawPath ? null : fsd.GetFileEncryptionInfo(node, snapshot
				, iip);
			if (node.IsFile())
			{
				INodeFile fileNode = node.AsFile();
				size = fileNode.ComputeFileSize(snapshot);
				replication = fileNode.GetFileReplication(snapshot);
				blocksize = fileNode.GetPreferredBlockSize();
				bool inSnapshot = snapshot != Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
					.CurrentStateId;
				bool isUc = !inSnapshot && fileNode.IsUnderConstruction();
				long fileSize = !inSnapshot && isUc ? fileNode.ComputeFileSizeNotIncludingLastUcBlock
					() : size;
				loc = fsd.GetFSNamesystem().GetBlockManager().CreateLocatedBlocks(fileNode.GetBlocks
					(snapshot), fileSize, isUc, 0L, size, false, inSnapshot, feInfo);
				if (loc == null)
				{
					loc = new LocatedBlocks();
				}
				isEncrypted = (feInfo != null) || (isRawPath && fsd.IsInAnEZ(INodesInPath.FromINode
					(node)));
			}
			else
			{
				isEncrypted = fsd.IsInAnEZ(INodesInPath.FromINode(node));
			}
			int childrenNum = node.IsDirectory() ? node.AsDirectory().GetChildrenNum(snapshot
				) : 0;
			INodeAttributes nodeAttrs = fsd.GetAttributes(fullPath, path, node, snapshot);
			HdfsLocatedFileStatus status = new HdfsLocatedFileStatus(size, node.IsDirectory()
				, replication, blocksize, node.GetModificationTime(snapshot), node.GetAccessTime
				(snapshot), GetPermissionForFileStatus(nodeAttrs, isEncrypted), nodeAttrs.GetUserName
				(), nodeAttrs.GetGroupName(), node.IsSymlink() ? node.AsSymlink().GetSymlink() : 
				null, path, node.GetId(), loc, childrenNum, feInfo, storagePolicy);
			// Set caching information for the located blocks.
			if (loc != null)
			{
				CacheManager cacheManager = fsd.GetFSNamesystem().GetCacheManager();
				foreach (LocatedBlock lb in loc.GetLocatedBlocks())
				{
					cacheManager.SetCachedLocations(lb);
				}
			}
			return status;
		}

		/// <summary>Returns an inode's FsPermission for use in an outbound FileStatus.</summary>
		/// <remarks>
		/// Returns an inode's FsPermission for use in an outbound FileStatus.  If the
		/// inode has an ACL or is for an encrypted file/dir, then this method will
		/// return an FsPermissionExtension.
		/// </remarks>
		/// <param name="node">INode to check</param>
		/// <param name="snapshot">int snapshot ID</param>
		/// <param name="isEncrypted">boolean true if the file/dir is encrypted</param>
		/// <returns>
		/// FsPermission from inode, with ACL bit on if the inode has an ACL
		/// and encrypted bit on if it represents an encrypted file/dir.
		/// </returns>
		private static FsPermission GetPermissionForFileStatus(INodeAttributes node, bool
			 isEncrypted)
		{
			FsPermission perm = node.GetFsPermission();
			bool hasAcl = node.GetAclFeature() != null;
			if (hasAcl || isEncrypted)
			{
				perm = new FsPermissionExtension(perm, hasAcl, isEncrypted);
			}
			return perm;
		}

		/// <exception cref="System.IO.IOException"/>
		private static ContentSummary GetContentSummaryInt(FSDirectory fsd, INodesInPath 
			iip)
		{
			fsd.ReadLock();
			try
			{
				INode targetNode = iip.GetLastINode();
				if (targetNode == null)
				{
					throw new FileNotFoundException("File does not exist: " + iip.GetPath());
				}
				else
				{
					// Make it relinquish locks everytime contentCountLimit entries are
					// processed. 0 means disabled. I.e. blocking for the entire duration.
					ContentSummaryComputationContext cscc = new ContentSummaryComputationContext(fsd, 
						fsd.GetFSNamesystem(), fsd.GetContentCountLimit(), fsd.GetContentSleepMicroSec()
						);
					ContentSummary cs = targetNode.ComputeAndConvertContentSummary(cscc);
					fsd.AddYieldCount(cscc.GetYieldCount());
					return cs;
				}
			}
			finally
			{
				fsd.ReadUnlock();
			}
		}
	}
}
