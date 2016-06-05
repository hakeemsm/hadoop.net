using System;
using System.Collections.Generic;
using System.IO;
using Org.Apache.Hadoop;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Org.Apache.Hadoop.Hdfs.Util;
using Org.Apache.Hadoop.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	public class FSDirAttrOp
	{
		/// <exception cref="System.IO.IOException"/>
		internal static HdfsFileStatus SetPermission(FSDirectory fsd, string srcArg, FsPermission
			 permission)
		{
			string src = srcArg;
			FSPermissionChecker pc = fsd.GetPermissionChecker();
			byte[][] pathComponents = FSDirectory.GetPathComponentsForReservedPath(src);
			INodesInPath iip;
			fsd.WriteLock();
			try
			{
				src = fsd.ResolvePath(pc, src, pathComponents);
				iip = fsd.GetINodesInPath4Write(src);
				fsd.CheckOwner(pc, iip);
				UnprotectedSetPermission(fsd, src, permission);
			}
			finally
			{
				fsd.WriteUnlock();
			}
			fsd.GetEditLog().LogSetPermissions(src, permission);
			return fsd.GetAuditFileInfo(iip);
		}

		/// <exception cref="System.IO.IOException"/>
		internal static HdfsFileStatus SetOwner(FSDirectory fsd, string src, string username
			, string group)
		{
			FSPermissionChecker pc = fsd.GetPermissionChecker();
			byte[][] pathComponents = FSDirectory.GetPathComponentsForReservedPath(src);
			INodesInPath iip;
			fsd.WriteLock();
			try
			{
				src = fsd.ResolvePath(pc, src, pathComponents);
				iip = fsd.GetINodesInPath4Write(src);
				fsd.CheckOwner(pc, iip);
				if (!pc.IsSuperUser())
				{
					if (username != null && !pc.GetUser().Equals(username))
					{
						throw new AccessControlException("Non-super user cannot change owner");
					}
					if (group != null && !pc.ContainsGroup(group))
					{
						throw new AccessControlException("User does not belong to " + group);
					}
				}
				UnprotectedSetOwner(fsd, src, username, group);
			}
			finally
			{
				fsd.WriteUnlock();
			}
			fsd.GetEditLog().LogSetOwner(src, username, group);
			return fsd.GetAuditFileInfo(iip);
		}

		/// <exception cref="System.IO.IOException"/>
		internal static HdfsFileStatus SetTimes(FSDirectory fsd, string src, long mtime, 
			long atime)
		{
			if (!fsd.IsAccessTimeSupported() && atime != -1)
			{
				throw new IOException("Access time for hdfs is not configured. " + " Please set "
					 + DFSConfigKeys.DfsNamenodeAccesstimePrecisionKey + " configuration parameter."
					);
			}
			FSPermissionChecker pc = fsd.GetPermissionChecker();
			byte[][] pathComponents = FSDirectory.GetPathComponentsForReservedPath(src);
			INodesInPath iip;
			fsd.WriteLock();
			try
			{
				src = fsd.ResolvePath(pc, src, pathComponents);
				iip = fsd.GetINodesInPath4Write(src);
				// Write access is required to set access and modification times
				if (fsd.IsPermissionEnabled())
				{
					fsd.CheckPathAccess(pc, iip, FsAction.Write);
				}
				INode inode = iip.GetLastINode();
				if (inode == null)
				{
					throw new FileNotFoundException("File/Directory " + src + " does not exist.");
				}
				bool changed = UnprotectedSetTimes(fsd, inode, mtime, atime, true, iip.GetLatestSnapshotId
					());
				if (changed)
				{
					fsd.GetEditLog().LogTimes(src, mtime, atime);
				}
			}
			finally
			{
				fsd.WriteUnlock();
			}
			return fsd.GetAuditFileInfo(iip);
		}

		/// <exception cref="System.IO.IOException"/>
		internal static bool SetReplication(FSDirectory fsd, BlockManager bm, string src, 
			short replication)
		{
			bm.VerifyReplication(src, replication, null);
			bool isFile;
			FSPermissionChecker pc = fsd.GetPermissionChecker();
			byte[][] pathComponents = FSDirectory.GetPathComponentsForReservedPath(src);
			fsd.WriteLock();
			try
			{
				src = fsd.ResolvePath(pc, src, pathComponents);
				INodesInPath iip = fsd.GetINodesInPath4Write(src);
				if (fsd.IsPermissionEnabled())
				{
					fsd.CheckPathAccess(pc, iip, FsAction.Write);
				}
				short[] blockRepls = new short[2];
				// 0: old, 1: new
				Block[] blocks = UnprotectedSetReplication(fsd, src, replication, blockRepls);
				isFile = blocks != null;
				if (isFile)
				{
					fsd.GetEditLog().LogSetReplication(src, replication);
					bm.SetReplication(blockRepls[0], blockRepls[1], src, blocks);
				}
			}
			finally
			{
				fsd.WriteUnlock();
			}
			return isFile;
		}

		/// <exception cref="System.IO.IOException"/>
		internal static HdfsFileStatus SetStoragePolicy(FSDirectory fsd, BlockManager bm, 
			string src, string policyName)
		{
			if (!fsd.IsStoragePolicyEnabled())
			{
				throw new IOException("Failed to set storage policy since " + DFSConfigKeys.DfsStoragePolicyEnabledKey
					 + " is set to false.");
			}
			FSPermissionChecker pc = fsd.GetPermissionChecker();
			byte[][] pathComponents = FSDirectory.GetPathComponentsForReservedPath(src);
			INodesInPath iip;
			fsd.WriteLock();
			try
			{
				src = FSDirectory.ResolvePath(src, pathComponents, fsd);
				iip = fsd.GetINodesInPath4Write(src);
				if (fsd.IsPermissionEnabled())
				{
					fsd.CheckPathAccess(pc, iip, FsAction.Write);
				}
				// get the corresponding policy and make sure the policy name is valid
				BlockStoragePolicy policy = bm.GetStoragePolicy(policyName);
				if (policy == null)
				{
					throw new HadoopIllegalArgumentException("Cannot find a block policy with the name "
						 + policyName);
				}
				UnprotectedSetStoragePolicy(fsd, bm, iip, policy.GetId());
				fsd.GetEditLog().LogSetStoragePolicy(src, policy.GetId());
			}
			finally
			{
				fsd.WriteUnlock();
			}
			return fsd.GetAuditFileInfo(iip);
		}

		/// <exception cref="System.IO.IOException"/>
		internal static BlockStoragePolicy[] GetStoragePolicies(BlockManager bm)
		{
			return bm.GetStoragePolicies();
		}

		/// <exception cref="System.IO.IOException"/>
		internal static long GetPreferredBlockSize(FSDirectory fsd, string src)
		{
			FSPermissionChecker pc = fsd.GetPermissionChecker();
			byte[][] pathComponents = FSDirectory.GetPathComponentsForReservedPath(src);
			fsd.ReadLock();
			try
			{
				src = fsd.ResolvePath(pc, src, pathComponents);
				INodesInPath iip = fsd.GetINodesInPath(src, false);
				if (fsd.IsPermissionEnabled())
				{
					fsd.CheckTraverse(pc, iip);
				}
				return INodeFile.ValueOf(iip.GetLastINode(), src).GetPreferredBlockSize();
			}
			finally
			{
				fsd.ReadUnlock();
			}
		}

		/// <summary>Set the namespace, storagespace and typespace quota for a directory.</summary>
		/// <remarks>
		/// Set the namespace, storagespace and typespace quota for a directory.
		/// Note: This does not support ".inodes" relative path.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		internal static void SetQuota(FSDirectory fsd, string src, long nsQuota, long ssQuota
			, StorageType type)
		{
			if (fsd.IsPermissionEnabled())
			{
				FSPermissionChecker pc = fsd.GetPermissionChecker();
				pc.CheckSuperuserPrivilege();
			}
			fsd.WriteLock();
			try
			{
				INodeDirectory changed = UnprotectedSetQuota(fsd, src, nsQuota, ssQuota, type);
				if (changed != null)
				{
					QuotaCounts q = changed.GetQuotaCounts();
					if (type == null)
					{
						fsd.GetEditLog().LogSetQuota(src, q.GetNameSpace(), q.GetStorageSpace());
					}
					else
					{
						fsd.GetEditLog().LogSetQuotaByStorageType(src, q.GetTypeSpaces().Get(type), type);
					}
				}
			}
			finally
			{
				fsd.WriteUnlock();
			}
		}

		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.QuotaExceededException"/>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.SnapshotAccessControlException"/
		/// 	>
		internal static void UnprotectedSetPermission(FSDirectory fsd, string src, FsPermission
			 permissions)
		{
			System.Diagnostics.Debug.Assert(fsd.HasWriteLock());
			INodesInPath inodesInPath = fsd.GetINodesInPath4Write(src, true);
			INode inode = inodesInPath.GetLastINode();
			if (inode == null)
			{
				throw new FileNotFoundException("File does not exist: " + src);
			}
			int snapshotId = inodesInPath.GetLatestSnapshotId();
			inode.SetPermission(permissions, snapshotId);
		}

		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.QuotaExceededException"/>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.SnapshotAccessControlException"/
		/// 	>
		internal static void UnprotectedSetOwner(FSDirectory fsd, string src, string username
			, string groupname)
		{
			System.Diagnostics.Debug.Assert(fsd.HasWriteLock());
			INodesInPath inodesInPath = fsd.GetINodesInPath4Write(src, true);
			INode inode = inodesInPath.GetLastINode();
			if (inode == null)
			{
				throw new FileNotFoundException("File does not exist: " + src);
			}
			if (username != null)
			{
				inode = inode.SetUser(username, inodesInPath.GetLatestSnapshotId());
			}
			if (groupname != null)
			{
				inode.SetGroup(groupname, inodesInPath.GetLatestSnapshotId());
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.QuotaExceededException"/>
		internal static bool SetTimes(FSDirectory fsd, INode inode, long mtime, long atime
			, bool force, int latestSnapshotId)
		{
			fsd.WriteLock();
			try
			{
				return UnprotectedSetTimes(fsd, inode, mtime, atime, force, latestSnapshotId);
			}
			finally
			{
				fsd.WriteUnlock();
			}
		}

		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.QuotaExceededException"/>
		internal static bool UnprotectedSetTimes(FSDirectory fsd, string src, long mtime, 
			long atime, bool force)
		{
			System.Diagnostics.Debug.Assert(fsd.HasWriteLock());
			INodesInPath i = fsd.GetINodesInPath(src, true);
			return UnprotectedSetTimes(fsd, i.GetLastINode(), mtime, atime, force, i.GetLatestSnapshotId
				());
		}

		/// <summary>
		/// See
		/// <see cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.SetQuota(string, long, long, Org.Apache.Hadoop.FS.StorageType)
		/// 	"/>
		/// for the contract.
		/// Sets quota for for a directory.
		/// </summary>
		/// <returns>INodeDirectory if any of the quotas have changed. null otherwise.</returns>
		/// <exception cref="System.IO.FileNotFoundException">if the path does not exist.</exception>
		/// <exception cref="Org.Apache.Hadoop.FS.PathIsNotDirectoryException">if the path is not a directory.
		/// 	</exception>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.QuotaExceededException">
		/// if the directory tree size is
		/// greater than the given quota
		/// </exception>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException">if a symlink is encountered in src.
		/// 	</exception>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.SnapshotAccessControlException">if path is in RO snapshot
		/// 	</exception>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.UnsupportedActionException
		/// 	"/>
		internal static INodeDirectory UnprotectedSetQuota(FSDirectory fsd, string src, long
			 nsQuota, long ssQuota, StorageType type)
		{
			System.Diagnostics.Debug.Assert(fsd.HasWriteLock());
			// sanity check
			if ((nsQuota < 0 && nsQuota != HdfsConstants.QuotaDontSet && nsQuota != HdfsConstants
				.QuotaReset) || (ssQuota < 0 && ssQuota != HdfsConstants.QuotaDontSet && ssQuota
				 != HdfsConstants.QuotaReset))
			{
				throw new ArgumentException("Illegal value for nsQuota or " + "ssQuota : " + nsQuota
					 + " and " + ssQuota);
			}
			// sanity check for quota by storage type
			if ((type != null) && (!fsd.IsQuotaByStorageTypeEnabled() || nsQuota != HdfsConstants
				.QuotaDontSet))
			{
				throw new UnsupportedActionException("Failed to set quota by storage type because either"
					 + DFSConfigKeys.DfsQuotaByStoragetypeEnabledKey + " is set to " + fsd.IsQuotaByStorageTypeEnabled
					() + " or nsQuota value is illegal " + nsQuota);
			}
			string srcs = FSDirectory.NormalizePath(src);
			INodesInPath iip = fsd.GetINodesInPath4Write(srcs, true);
			INodeDirectory dirNode = INodeDirectory.ValueOf(iip.GetLastINode(), srcs);
			if (dirNode.IsRoot() && nsQuota == HdfsConstants.QuotaReset)
			{
				throw new ArgumentException("Cannot clear namespace quota on root.");
			}
			else
			{
				// a directory inode
				QuotaCounts oldQuota = dirNode.GetQuotaCounts();
				long oldNsQuota = oldQuota.GetNameSpace();
				long oldSsQuota = oldQuota.GetStorageSpace();
				if (nsQuota == HdfsConstants.QuotaDontSet)
				{
					nsQuota = oldNsQuota;
				}
				if (ssQuota == HdfsConstants.QuotaDontSet)
				{
					ssQuota = oldSsQuota;
				}
				// unchanged space/namespace quota
				if (type == null && oldNsQuota == nsQuota && oldSsQuota == ssQuota)
				{
					return null;
				}
				// unchanged type quota
				if (type != null)
				{
					EnumCounters<StorageType> oldTypeQuotas = oldQuota.GetTypeSpaces();
					if (oldTypeQuotas != null && oldTypeQuotas.Get(type) == ssQuota)
					{
						return null;
					}
				}
				int latest = iip.GetLatestSnapshotId();
				dirNode.RecordModification(latest);
				dirNode.SetQuota(fsd.GetBlockStoragePolicySuite(), nsQuota, ssQuota, type);
				return dirNode;
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.QuotaExceededException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.SnapshotAccessControlException"/
		/// 	>
		internal static Block[] UnprotectedSetReplication(FSDirectory fsd, string src, short
			 replication, short[] blockRepls)
		{
			System.Diagnostics.Debug.Assert(fsd.HasWriteLock());
			INodesInPath iip = fsd.GetINodesInPath4Write(src, true);
			INode inode = iip.GetLastINode();
			if (inode == null || !inode.IsFile())
			{
				return null;
			}
			INodeFile file = inode.AsFile();
			short oldBR = file.GetBlockReplication();
			// before setFileReplication, check for increasing block replication.
			// if replication > oldBR, then newBR == replication.
			// if replication < oldBR, we don't know newBR yet.
			if (replication > oldBR)
			{
				long dsDelta = file.StoragespaceConsumed() / oldBR;
				fsd.UpdateCount(iip, 0L, dsDelta, oldBR, replication, true);
			}
			file.SetFileReplication(replication, iip.GetLatestSnapshotId());
			short newBR = file.GetBlockReplication();
			// check newBR < oldBR case.
			if (newBR < oldBR)
			{
				long dsDelta = file.StoragespaceConsumed() / newBR;
				fsd.UpdateCount(iip, 0L, dsDelta, oldBR, newBR, true);
			}
			if (blockRepls != null)
			{
				blockRepls[0] = oldBR;
				blockRepls[1] = newBR;
			}
			return file.GetBlocks();
		}

		/// <exception cref="System.IO.IOException"/>
		internal static void UnprotectedSetStoragePolicy(FSDirectory fsd, BlockManager bm
			, INodesInPath iip, byte policyId)
		{
			System.Diagnostics.Debug.Assert(fsd.HasWriteLock());
			INode inode = iip.GetLastINode();
			if (inode == null)
			{
				throw new FileNotFoundException("File/Directory does not exist: " + iip.GetPath()
					);
			}
			int snapshotId = iip.GetLatestSnapshotId();
			if (inode.IsFile())
			{
				BlockStoragePolicy newPolicy = bm.GetStoragePolicy(policyId);
				if (newPolicy.IsCopyOnCreateFile())
				{
					throw new HadoopIllegalArgumentException("Policy " + newPolicy + " cannot be set after file creation."
						);
				}
				BlockStoragePolicy currentPolicy = bm.GetStoragePolicy(inode.GetLocalStoragePolicyID
					());
				if (currentPolicy != null && currentPolicy.IsCopyOnCreateFile())
				{
					throw new HadoopIllegalArgumentException("Existing policy " + currentPolicy.GetName
						() + " cannot be changed after file creation.");
				}
				inode.AsFile().SetStoragePolicyID(policyId, snapshotId);
			}
			else
			{
				if (inode.IsDirectory())
				{
					SetDirStoragePolicy(fsd, inode.AsDirectory(), policyId, snapshotId);
				}
				else
				{
					throw new FileNotFoundException(iip.GetPath() + " is not a file or directory");
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static void SetDirStoragePolicy(FSDirectory fsd, INodeDirectory inode, byte
			 policyId, int latestSnapshotId)
		{
			IList<XAttr> existingXAttrs = XAttrStorage.ReadINodeXAttrs(inode);
			XAttr xAttr = BlockStoragePolicySuite.BuildXAttr(policyId);
			IList<XAttr> newXAttrs = FSDirXAttrOp.SetINodeXAttrs(fsd, existingXAttrs, Arrays.
				AsList(xAttr), EnumSet.Of(XAttrSetFlag.Create, XAttrSetFlag.Replace));
			XAttrStorage.UpdateINodeXAttrs(inode, newXAttrs, latestSnapshotId);
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.QuotaExceededException"/>
		private static bool UnprotectedSetTimes(FSDirectory fsd, INode inode, long mtime, 
			long atime, bool force, int latest)
		{
			System.Diagnostics.Debug.Assert(fsd.HasWriteLock());
			bool status = false;
			if (mtime != -1)
			{
				inode = inode.SetModificationTime(mtime, latest);
				status = true;
			}
			if (atime != -1)
			{
				long inodeTime = inode.GetAccessTime();
				// if the last access time update was within the last precision interval, then
				// no need to store access time
				if (atime <= inodeTime + fsd.GetFSNamesystem().GetAccessTimePrecision() && !force)
				{
					status = false;
				}
				else
				{
					inode.SetAccessTime(atime, latest);
					status = true;
				}
			}
			return status;
		}
	}
}
