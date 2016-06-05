using System.Collections.Generic;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	internal class FSDirAclOp
	{
		/// <exception cref="System.IO.IOException"/>
		internal static HdfsFileStatus ModifyAclEntries(FSDirectory fsd, string srcArg, IList
			<AclEntry> aclSpec)
		{
			string src = srcArg;
			CheckAclsConfigFlag(fsd);
			FSPermissionChecker pc = fsd.GetPermissionChecker();
			byte[][] pathComponents = FSDirectory.GetPathComponentsForReservedPath(src);
			src = fsd.ResolvePath(pc, src, pathComponents);
			INodesInPath iip;
			fsd.WriteLock();
			try
			{
				iip = fsd.GetINodesInPath4Write(FSDirectory.NormalizePath(src), true);
				fsd.CheckOwner(pc, iip);
				INode inode = FSDirectory.ResolveLastINode(iip);
				int snapshotId = iip.GetLatestSnapshotId();
				IList<AclEntry> existingAcl = AclStorage.ReadINodeLogicalAcl(inode);
				IList<AclEntry> newAcl = AclTransformation.MergeAclEntries(existingAcl, aclSpec);
				AclStorage.UpdateINodeAcl(inode, newAcl, snapshotId);
				fsd.GetEditLog().LogSetAcl(src, newAcl);
			}
			finally
			{
				fsd.WriteUnlock();
			}
			return fsd.GetAuditFileInfo(iip);
		}

		/// <exception cref="System.IO.IOException"/>
		internal static HdfsFileStatus RemoveAclEntries(FSDirectory fsd, string srcArg, IList
			<AclEntry> aclSpec)
		{
			string src = srcArg;
			CheckAclsConfigFlag(fsd);
			FSPermissionChecker pc = fsd.GetPermissionChecker();
			byte[][] pathComponents = FSDirectory.GetPathComponentsForReservedPath(src);
			src = fsd.ResolvePath(pc, src, pathComponents);
			INodesInPath iip;
			fsd.WriteLock();
			try
			{
				iip = fsd.GetINodesInPath4Write(FSDirectory.NormalizePath(src), true);
				fsd.CheckOwner(pc, iip);
				INode inode = FSDirectory.ResolveLastINode(iip);
				int snapshotId = iip.GetLatestSnapshotId();
				IList<AclEntry> existingAcl = AclStorage.ReadINodeLogicalAcl(inode);
				IList<AclEntry> newAcl = AclTransformation.FilterAclEntriesByAclSpec(existingAcl, 
					aclSpec);
				AclStorage.UpdateINodeAcl(inode, newAcl, snapshotId);
				fsd.GetEditLog().LogSetAcl(src, newAcl);
			}
			finally
			{
				fsd.WriteUnlock();
			}
			return fsd.GetAuditFileInfo(iip);
		}

		/// <exception cref="System.IO.IOException"/>
		internal static HdfsFileStatus RemoveDefaultAcl(FSDirectory fsd, string srcArg)
		{
			string src = srcArg;
			CheckAclsConfigFlag(fsd);
			FSPermissionChecker pc = fsd.GetPermissionChecker();
			byte[][] pathComponents = FSDirectory.GetPathComponentsForReservedPath(src);
			src = fsd.ResolvePath(pc, src, pathComponents);
			INodesInPath iip;
			fsd.WriteLock();
			try
			{
				iip = fsd.GetINodesInPath4Write(FSDirectory.NormalizePath(src), true);
				fsd.CheckOwner(pc, iip);
				INode inode = FSDirectory.ResolveLastINode(iip);
				int snapshotId = iip.GetLatestSnapshotId();
				IList<AclEntry> existingAcl = AclStorage.ReadINodeLogicalAcl(inode);
				IList<AclEntry> newAcl = AclTransformation.FilterDefaultAclEntries(existingAcl);
				AclStorage.UpdateINodeAcl(inode, newAcl, snapshotId);
				fsd.GetEditLog().LogSetAcl(src, newAcl);
			}
			finally
			{
				fsd.WriteUnlock();
			}
			return fsd.GetAuditFileInfo(iip);
		}

		/// <exception cref="System.IO.IOException"/>
		internal static HdfsFileStatus RemoveAcl(FSDirectory fsd, string srcArg)
		{
			string src = srcArg;
			CheckAclsConfigFlag(fsd);
			FSPermissionChecker pc = fsd.GetPermissionChecker();
			byte[][] pathComponents = FSDirectory.GetPathComponentsForReservedPath(src);
			src = fsd.ResolvePath(pc, src, pathComponents);
			INodesInPath iip;
			fsd.WriteLock();
			try
			{
				iip = fsd.GetINodesInPath4Write(src);
				fsd.CheckOwner(pc, iip);
				UnprotectedRemoveAcl(fsd, iip);
			}
			finally
			{
				fsd.WriteUnlock();
			}
			fsd.GetEditLog().LogSetAcl(src, AclFeature.EmptyEntryList);
			return fsd.GetAuditFileInfo(iip);
		}

		/// <exception cref="System.IO.IOException"/>
		internal static HdfsFileStatus SetAcl(FSDirectory fsd, string srcArg, IList<AclEntry
			> aclSpec)
		{
			string src = srcArg;
			CheckAclsConfigFlag(fsd);
			byte[][] pathComponents = FSDirectory.GetPathComponentsForReservedPath(src);
			FSPermissionChecker pc = fsd.GetPermissionChecker();
			src = fsd.ResolvePath(pc, src, pathComponents);
			INodesInPath iip;
			fsd.WriteLock();
			try
			{
				iip = fsd.GetINodesInPath4Write(src);
				fsd.CheckOwner(pc, iip);
				IList<AclEntry> newAcl = UnprotectedSetAcl(fsd, src, aclSpec, false);
				fsd.GetEditLog().LogSetAcl(src, newAcl);
			}
			finally
			{
				fsd.WriteUnlock();
			}
			return fsd.GetAuditFileInfo(iip);
		}

		/// <exception cref="System.IO.IOException"/>
		internal static AclStatus GetAclStatus(FSDirectory fsd, string src)
		{
			CheckAclsConfigFlag(fsd);
			FSPermissionChecker pc = fsd.GetPermissionChecker();
			byte[][] pathComponents = FSDirectory.GetPathComponentsForReservedPath(src);
			src = fsd.ResolvePath(pc, src, pathComponents);
			string srcs = FSDirectory.NormalizePath(src);
			fsd.ReadLock();
			try
			{
				// There is no real inode for the path ending in ".snapshot", so return a
				// non-null, unpopulated AclStatus.  This is similar to getFileInfo.
				if (srcs.EndsWith(HdfsConstants.SeparatorDotSnapshotDir) && fsd.GetINode4DotSnapshot
					(srcs) != null)
				{
					return new AclStatus.Builder().Owner(string.Empty).Group(string.Empty).Build();
				}
				INodesInPath iip = fsd.GetINodesInPath(srcs, true);
				if (fsd.IsPermissionEnabled())
				{
					fsd.CheckTraverse(pc, iip);
				}
				INode inode = FSDirectory.ResolveLastINode(iip);
				int snapshotId = iip.GetPathSnapshotId();
				IList<AclEntry> acl = AclStorage.ReadINodeAcl(fsd.GetAttributes(src, inode.GetLocalNameBytes
					(), inode, snapshotId));
				FsPermission fsPermission = inode.GetFsPermission(snapshotId);
				return new AclStatus.Builder().Owner(inode.GetUserName()).Group(inode.GetGroupName
					()).StickyBit(fsPermission.GetStickyBit()).SetPermission(fsPermission).AddEntries
					(acl).Build();
			}
			finally
			{
				fsd.ReadUnlock();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal static IList<AclEntry> UnprotectedSetAcl(FSDirectory fsd, string src, IList
			<AclEntry> aclSpec, bool fromEdits)
		{
			System.Diagnostics.Debug.Assert(fsd.HasWriteLock());
			INodesInPath iip = fsd.GetINodesInPath4Write(FSDirectory.NormalizePath(src), true
				);
			// ACL removal is logged to edits as OP_SET_ACL with an empty list.
			if (aclSpec.IsEmpty())
			{
				UnprotectedRemoveAcl(fsd, iip);
				return AclFeature.EmptyEntryList;
			}
			INode inode = FSDirectory.ResolveLastINode(iip);
			int snapshotId = iip.GetLatestSnapshotId();
			IList<AclEntry> newAcl = aclSpec;
			if (!fromEdits)
			{
				IList<AclEntry> existingAcl = AclStorage.ReadINodeLogicalAcl(inode);
				newAcl = AclTransformation.ReplaceAclEntries(existingAcl, aclSpec);
			}
			AclStorage.UpdateINodeAcl(inode, newAcl, snapshotId);
			return newAcl;
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.AclException"/>
		private static void CheckAclsConfigFlag(FSDirectory fsd)
		{
			if (!fsd.IsAclsEnabled())
			{
				throw new AclException(string.Format("The ACL operation has been rejected.  " + "Support for ACLs has been disabled by setting %s to false."
					, DFSConfigKeys.DfsNamenodeAclsEnabledKey));
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static void UnprotectedRemoveAcl(FSDirectory fsd, INodesInPath iip)
		{
			System.Diagnostics.Debug.Assert(fsd.HasWriteLock());
			INode inode = FSDirectory.ResolveLastINode(iip);
			int snapshotId = iip.GetLatestSnapshotId();
			AclFeature f = inode.GetAclFeature();
			if (f == null)
			{
				return;
			}
			FsPermission perm = inode.GetFsPermission();
			IList<AclEntry> featureEntries = AclStorage.GetEntriesFromAclFeature(f);
			if (featureEntries[0].GetScope() == AclEntryScope.Access)
			{
				// Restore group permissions from the feature's entry to permission
				// bits, overwriting the mask, which is not part of a minimal ACL.
				AclEntry groupEntryKey = new AclEntry.Builder().SetScope(AclEntryScope.Access).SetType
					(AclEntryType.Group).Build();
				int groupEntryIndex = Sharpen.Collections.BinarySearch(featureEntries, groupEntryKey
					, AclTransformation.AclEntryComparator);
				System.Diagnostics.Debug.Assert(groupEntryIndex >= 0);
				FsAction groupPerm = featureEntries[groupEntryIndex].GetPermission();
				FsPermission newPerm = new FsPermission(perm.GetUserAction(), groupPerm, perm.GetOtherAction
					(), perm.GetStickyBit());
				inode.SetPermission(newPerm, snapshotId);
			}
			inode.RemoveAclFeature(snapshotId);
		}
	}
}
