using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Org.Apache.Commons.Logging;
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
	/// <summary>We keep an in-memory representation of the file/block hierarchy.</summary>
	/// <remarks>
	/// We keep an in-memory representation of the file/block hierarchy.
	/// This is a base INode class containing common fields for file and
	/// directory inodes.
	/// </remarks>
	public abstract class INode : INodeAttributes, Diff.Element<byte[]>
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.Server.Namenode.INode
			));

		/// <summary>
		/// parent is either an
		/// <see cref="INodeDirectory"/>
		/// or an
		/// <see cref="INodeReference"/>
		/// .
		/// </summary>
		private Org.Apache.Hadoop.Hdfs.Server.Namenode.INode parent = null;

		internal INode(Org.Apache.Hadoop.Hdfs.Server.Namenode.INode parent)
		{
			this.parent = parent;
		}

		/// <summary>Get inode id</summary>
		public abstract long GetId();

		/// <summary>Check whether this is the root inode.</summary>
		internal bool IsRoot()
		{
			return GetLocalNameBytes().Length == 0;
		}

		/// <summary>
		/// Get the
		/// <see cref="Org.Apache.Hadoop.FS.Permission.PermissionStatus"/>
		/// 
		/// </summary>
		internal abstract PermissionStatus GetPermissionStatus(int snapshotId);

		/// <summary>The same as getPermissionStatus(null).</summary>
		internal PermissionStatus GetPermissionStatus()
		{
			return GetPermissionStatus(Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
				.CurrentStateId);
		}

		/// <param name="snapshotId">
		/// if it is not
		/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.CurrentStateId
		/// 	"/>
		/// , get the result
		/// from the given snapshot; otherwise, get the result from the
		/// current inode.
		/// </param>
		/// <returns>user name</returns>
		internal abstract string GetUserName(int snapshotId);

		/// <summary>The same as getUserName(Snapshot.CURRENT_STATE_ID).</summary>
		public string GetUserName()
		{
			return GetUserName(Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.CurrentStateId
				);
		}

		/// <summary>Set user</summary>
		internal abstract void SetUser(string user);

		/// <summary>Set user</summary>
		internal Org.Apache.Hadoop.Hdfs.Server.Namenode.INode SetUser(string user, int latestSnapshotId
			)
		{
			RecordModification(latestSnapshotId);
			SetUser(user);
			return this;
		}

		/// <param name="snapshotId">
		/// if it is not
		/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.CurrentStateId
		/// 	"/>
		/// , get the result
		/// from the given snapshot; otherwise, get the result from the
		/// current inode.
		/// </param>
		/// <returns>group name</returns>
		internal abstract string GetGroupName(int snapshotId);

		/// <summary>The same as getGroupName(Snapshot.CURRENT_STATE_ID).</summary>
		public string GetGroupName()
		{
			return GetGroupName(Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.CurrentStateId
				);
		}

		/// <summary>Set group</summary>
		internal abstract void SetGroup(string group);

		/// <summary>Set group</summary>
		internal Org.Apache.Hadoop.Hdfs.Server.Namenode.INode SetGroup(string group, int 
			latestSnapshotId)
		{
			RecordModification(latestSnapshotId);
			SetGroup(group);
			return this;
		}

		/// <param name="snapshotId">
		/// if it is not
		/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.CurrentStateId
		/// 	"/>
		/// , get the result
		/// from the given snapshot; otherwise, get the result from the
		/// current inode.
		/// </param>
		/// <returns>permission.</returns>
		internal abstract FsPermission GetFsPermission(int snapshotId);

		/// <summary>The same as getFsPermission(Snapshot.CURRENT_STATE_ID).</summary>
		public FsPermission GetFsPermission()
		{
			return GetFsPermission(Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.CurrentStateId
				);
		}

		/// <summary>
		/// Set the
		/// <see cref="Org.Apache.Hadoop.FS.Permission.FsPermission"/>
		/// of this
		/// <see cref="INode"/>
		/// 
		/// </summary>
		internal abstract void SetPermission(FsPermission permission);

		/// <summary>
		/// Set the
		/// <see cref="Org.Apache.Hadoop.FS.Permission.FsPermission"/>
		/// of this
		/// <see cref="INode"/>
		/// 
		/// </summary>
		internal virtual Org.Apache.Hadoop.Hdfs.Server.Namenode.INode SetPermission(FsPermission
			 permission, int latestSnapshotId)
		{
			RecordModification(latestSnapshotId);
			SetPermission(permission);
			return this;
		}

		internal abstract AclFeature GetAclFeature(int snapshotId);

		public AclFeature GetAclFeature()
		{
			return GetAclFeature(Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.CurrentStateId
				);
		}

		internal abstract void AddAclFeature(AclFeature aclFeature);

		internal Org.Apache.Hadoop.Hdfs.Server.Namenode.INode AddAclFeature(AclFeature aclFeature
			, int latestSnapshotId)
		{
			RecordModification(latestSnapshotId);
			AddAclFeature(aclFeature);
			return this;
		}

		internal abstract void RemoveAclFeature();

		internal Org.Apache.Hadoop.Hdfs.Server.Namenode.INode RemoveAclFeature(int latestSnapshotId
			)
		{
			RecordModification(latestSnapshotId);
			RemoveAclFeature();
			return this;
		}

		/// <param name="snapshotId">
		/// if it is not
		/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.CurrentStateId
		/// 	"/>
		/// , get the result
		/// from the given snapshot; otherwise, get the result from the
		/// current inode.
		/// </param>
		/// <returns>XAttrFeature</returns>
		internal abstract XAttrFeature GetXAttrFeature(int snapshotId);

		public XAttrFeature GetXAttrFeature()
		{
			return GetXAttrFeature(Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.CurrentStateId
				);
		}

		/// <summary>Set <code>XAttrFeature</code></summary>
		internal abstract void AddXAttrFeature(XAttrFeature xAttrFeature);

		internal Org.Apache.Hadoop.Hdfs.Server.Namenode.INode AddXAttrFeature(XAttrFeature
			 xAttrFeature, int latestSnapshotId)
		{
			RecordModification(latestSnapshotId);
			AddXAttrFeature(xAttrFeature);
			return this;
		}

		/// <summary>Remove <code>XAttrFeature</code></summary>
		internal abstract void RemoveXAttrFeature();

		internal Org.Apache.Hadoop.Hdfs.Server.Namenode.INode RemoveXAttrFeature(int lastestSnapshotId
			)
		{
			RecordModification(lastestSnapshotId);
			RemoveXAttrFeature();
			return this;
		}

		/// <returns>
		/// if the given snapshot id is
		/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.CurrentStateId
		/// 	"/>
		/// ,
		/// return this; otherwise return the corresponding snapshot inode.
		/// </returns>
		public virtual INodeAttributes GetSnapshotINode(int snapshotId)
		{
			return this;
		}

		/// <summary>Is this inode in the latest snapshot?</summary>
		public bool IsInLatestSnapshot(int latestSnapshotId)
		{
			if (latestSnapshotId == Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.
				CurrentStateId || latestSnapshotId == Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
				.NoSnapshotId)
			{
				return false;
			}
			// if parent is a reference node, parent must be a renamed node. We can 
			// stop the check at the reference node.
			if (parent != null && parent.IsReference())
			{
				return true;
			}
			INodeDirectory parentDir = GetParent();
			if (parentDir == null)
			{
				// root
				return true;
			}
			if (!parentDir.IsInLatestSnapshot(latestSnapshotId))
			{
				return false;
			}
			Org.Apache.Hadoop.Hdfs.Server.Namenode.INode child = parentDir.GetChild(GetLocalNameBytes
				(), latestSnapshotId);
			if (this == child)
			{
				return true;
			}
			return child != null && child.IsReference() && this == child.AsReference().GetReferredINode
				();
		}

		/// <returns>true if the given inode is an ancestor directory of this inode.</returns>
		public bool IsAncestorDirectory(INodeDirectory dir)
		{
			for (INodeDirectory p = GetParent(); p != null; p = p.GetParent())
			{
				if (p == dir)
				{
					return true;
				}
			}
			return false;
		}

		/// <summary>
		/// When
		/// <see cref="RecordModification(int)"/>
		/// is called on a referred node,
		/// this method tells which snapshot the modification should be
		/// associated with: the snapshot that belongs to the SRC tree of the rename
		/// operation, or the snapshot belonging to the DST tree.
		/// </summary>
		/// <param name="latestInDst">id of the latest snapshot in the DST tree above the reference node
		/// 	</param>
		/// <returns>
		/// True: the modification should be recorded in the snapshot that
		/// belongs to the SRC tree. False: the modification should be
		/// recorded in the snapshot that belongs to the DST tree.
		/// </returns>
		public bool ShouldRecordInSrcSnapshot(int latestInDst)
		{
			Preconditions.CheckState(!IsReference());
			if (latestInDst == Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.CurrentStateId)
			{
				return true;
			}
			INodeReference withCount = GetParentReference();
			if (withCount != null)
			{
				int dstSnapshotId = withCount.GetParentReference().GetDstSnapshotId();
				if (dstSnapshotId != Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.CurrentStateId
					 && dstSnapshotId >= latestInDst)
				{
					return true;
				}
			}
			return false;
		}

		/// <summary>This inode is being modified.</summary>
		/// <remarks>
		/// This inode is being modified.  The previous version of the inode needs to
		/// be recorded in the latest snapshot.
		/// </remarks>
		/// <param name="latestSnapshotId">
		/// The id of the latest snapshot that has been taken.
		/// Note that it is
		/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.CurrentStateId
		/// 	"/>
		/// 
		/// if no snapshots have been taken.
		/// </param>
		internal abstract void RecordModification(int latestSnapshotId);

		/// <summary>Check whether it's a reference.</summary>
		public virtual bool IsReference()
		{
			return false;
		}

		/// <summary>
		/// Cast this inode to an
		/// <see cref="INodeReference"/>
		/// .
		/// </summary>
		public virtual INodeReference AsReference()
		{
			throw new InvalidOperationException("Current inode is not a reference: " + this.ToDetailString
				());
		}

		/// <summary>Check whether it's a file.</summary>
		public virtual bool IsFile()
		{
			return false;
		}

		/// <summary>
		/// Cast this inode to an
		/// <see cref="INodeFile"/>
		/// .
		/// </summary>
		public virtual INodeFile AsFile()
		{
			throw new InvalidOperationException("Current inode is not a file: " + this.ToDetailString
				());
		}

		/// <summary>Check whether it's a directory</summary>
		public virtual bool IsDirectory()
		{
			return false;
		}

		/// <summary>
		/// Cast this inode to an
		/// <see cref="INodeDirectory"/>
		/// .
		/// </summary>
		public virtual INodeDirectory AsDirectory()
		{
			throw new InvalidOperationException("Current inode is not a directory: " + this.ToDetailString
				());
		}

		/// <summary>Check whether it's a symlink</summary>
		public virtual bool IsSymlink()
		{
			return false;
		}

		/// <summary>
		/// Cast this inode to an
		/// <see cref="INodeSymlink"/>
		/// .
		/// </summary>
		public virtual INodeSymlink AsSymlink()
		{
			throw new InvalidOperationException("Current inode is not a symlink: " + this.ToDetailString
				());
		}

		/// <summary>
		/// Clean the subtree under this inode and collect the blocks from the descents
		/// for further block deletion/update.
		/// </summary>
		/// <remarks>
		/// Clean the subtree under this inode and collect the blocks from the descents
		/// for further block deletion/update. The current inode can either resides in
		/// the current tree or be stored as a snapshot copy.
		/// <pre>
		/// In general, we have the following rules.
		/// 1. When deleting a file/directory in the current tree, we have different
		/// actions according to the type of the node to delete.
		/// 1.1 The current inode (this) is an
		/// <see cref="INodeFile"/>
		/// .
		/// 1.1.1 If
		/// <c>prior</c>
		/// is null, there is no snapshot taken on ancestors
		/// before. Thus we simply destroy (i.e., to delete completely, no need to save
		/// snapshot copy) the current INode and collect its blocks for further
		/// cleansing.
		/// 1.1.2 Else do nothing since the current INode will be stored as a snapshot
		/// copy.
		/// 1.2 The current inode is an
		/// <see cref="INodeDirectory"/>
		/// .
		/// 1.2.1 If
		/// <c>prior</c>
		/// is null, there is no snapshot taken on ancestors
		/// before. Similarly, we destroy the whole subtree and collect blocks.
		/// 1.2.2 Else do nothing with the current INode. Recursively clean its
		/// children.
		/// 1.3 The current inode is a file with snapshot.
		/// Call recordModification(..) to capture the current states.
		/// Mark the INode as deleted.
		/// 1.4 The current inode is an
		/// <see cref="INodeDirectory"/>
		/// with snapshot feature.
		/// Call recordModification(..) to capture the current states.
		/// Destroy files/directories created after the latest snapshot
		/// (i.e., the inodes stored in the created list of the latest snapshot).
		/// Recursively clean remaining children.
		/// 2. When deleting a snapshot.
		/// 2.1 To clean
		/// <see cref="INodeFile"/>
		/// : do nothing.
		/// 2.2 To clean
		/// <see cref="INodeDirectory"/>
		/// : recursively clean its children.
		/// 2.3 To clean INodeFile with snapshot: delete the corresponding snapshot in
		/// its diff list.
		/// 2.4 To clean
		/// <see cref="INodeDirectory"/>
		/// with snapshot: delete the corresponding
		/// snapshot in its diff list. Recursively clean its children.
		/// </pre>
		/// </remarks>
		/// <param name="bsps">block storage policy suite to calculate intended storage type usage
		/// 	</param>
		/// <param name="snapshotId">
		/// The id of the snapshot to delete.
		/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.CurrentStateId
		/// 	"/>
		/// means to delete the current
		/// file/directory.
		/// </param>
		/// <param name="priorSnapshotId">
		/// The id of the latest snapshot before the to-be-deleted snapshot.
		/// When deleting a current inode, this parameter captures the latest
		/// snapshot.
		/// </param>
		/// <param name="collectedBlocks">
		/// blocks collected from the descents for further block
		/// deletion/update will be added to the given map.
		/// </param>
		/// <param name="removedINodes">
		/// INodes collected from the descents for further cleaning up of
		/// inodeMap
		/// </param>
		/// <returns>quota usage delta when deleting a snapshot</returns>
		public abstract QuotaCounts CleanSubtree(BlockStoragePolicySuite bsps, int snapshotId
			, int priorSnapshotId, INode.BlocksMapUpdateInfo collectedBlocks, IList<Org.Apache.Hadoop.Hdfs.Server.Namenode.INode
			> removedINodes);

		/// <summary>
		/// Destroy self and clear everything! If the INode is a file, this method
		/// collects its blocks for further block deletion.
		/// </summary>
		/// <remarks>
		/// Destroy self and clear everything! If the INode is a file, this method
		/// collects its blocks for further block deletion. If the INode is a
		/// directory, the method goes down the subtree and collects blocks from the
		/// descents, and clears its parent/children references as well. The method
		/// also clears the diff list if the INode contains snapshot diff list.
		/// </remarks>
		/// <param name="bsps">
		/// block storage policy suite to calculate intended storage type usage
		/// This is needed because INodeReference#destroyAndCollectBlocks() needs
		/// to call INode#cleanSubtree(), which calls INode#computeQuotaUsage().
		/// </param>
		/// <param name="collectedBlocks">
		/// blocks collected from the descents for further block
		/// deletion/update will be added to this map.
		/// </param>
		/// <param name="removedINodes">
		/// INodes collected from the descents for further cleaning up of
		/// inodeMap
		/// </param>
		public abstract void DestroyAndCollectBlocks(BlockStoragePolicySuite bsps, INode.BlocksMapUpdateInfo
			 collectedBlocks, IList<Org.Apache.Hadoop.Hdfs.Server.Namenode.INode> removedINodes
			);

		/// <summary>
		/// Compute
		/// <see cref="Org.Apache.Hadoop.FS.ContentSummary"/>
		/// . Blocking call
		/// </summary>
		public ContentSummary ComputeContentSummary(BlockStoragePolicySuite bsps)
		{
			return ComputeAndConvertContentSummary(new ContentSummaryComputationContext(bsps)
				);
		}

		/// <summary>
		/// Compute
		/// <see cref="Org.Apache.Hadoop.FS.ContentSummary"/>
		/// .
		/// </summary>
		public ContentSummary ComputeAndConvertContentSummary(ContentSummaryComputationContext
			 summary)
		{
			ContentCounts counts = ComputeContentSummary(summary).GetCounts();
			QuotaCounts q = GetQuotaCounts();
			return new ContentSummary.Builder().Length(counts.GetLength()).FileCount(counts.GetFileCount
				() + counts.GetSymlinkCount()).DirectoryCount(counts.GetDirectoryCount()).Quota(
				q.GetNameSpace()).SpaceConsumed(counts.GetStoragespace()).SpaceQuota(q.GetStorageSpace
				()).TypeConsumed(counts.GetTypeSpaces()).TypeQuota(q.GetTypeSpaces().AsArray()).
				Build();
		}

		/// <summary>
		/// Count subtree content summary with a
		/// <see cref="ContentCounts"/>
		/// .
		/// </summary>
		/// <param name="summary">the context object holding counts for the subtree.</param>
		/// <returns>The same objects as summary.</returns>
		public abstract ContentSummaryComputationContext ComputeContentSummary(ContentSummaryComputationContext
			 summary);

		/// <summary>Check and add namespace/storagespace/storagetype consumed to itself and the ancestors.
		/// 	</summary>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.QuotaExceededException">if quote is violated.
		/// 	</exception>
		public virtual void AddSpaceConsumed(QuotaCounts counts, bool verify)
		{
			AddSpaceConsumed2Parent(counts, verify);
		}

		/// <summary>Check and add namespace/storagespace/storagetype consumed to itself and the ancestors.
		/// 	</summary>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.QuotaExceededException">if quote is violated.
		/// 	</exception>
		internal virtual void AddSpaceConsumed2Parent(QuotaCounts counts, bool verify)
		{
			if (parent != null)
			{
				parent.AddSpaceConsumed(counts, verify);
			}
		}

		/// <summary>Get the quota set for this inode</summary>
		/// <returns>the quota counts.  The count is -1 if it is not set.</returns>
		public virtual QuotaCounts GetQuotaCounts()
		{
			return new QuotaCounts.Builder().NameSpace(HdfsConstants.QuotaReset).StorageSpace
				(HdfsConstants.QuotaReset).TypeSpaces(HdfsConstants.QuotaReset).Build();
		}

		public bool IsQuotaSet()
		{
			QuotaCounts qc = GetQuotaCounts();
			return qc.AnyNsSsCountGreaterOrEqual(0) || qc.AnyTypeSpaceCountGreaterOrEqual(0);
		}

		/// <summary>
		/// Count subtree
		/// <see cref="Quota.Namespace"/>
		/// and
		/// <see cref="Quota.Storagespace"/>
		/// usages.
		/// Entry point for FSDirectory where blockStoragePolicyId is given its initial
		/// value.
		/// </summary>
		public QuotaCounts ComputeQuotaUsage(BlockStoragePolicySuite bsps)
		{
			byte storagePolicyId = IsSymlink() ? BlockStoragePolicySuite.IdUnspecified : GetStoragePolicyID
				();
			return ComputeQuotaUsage(bsps, storagePolicyId, new QuotaCounts.Builder().Build()
				, true, Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.CurrentStateId);
		}

		/// <summary>
		/// Count subtree
		/// <see cref="Quota.Namespace"/>
		/// and
		/// <see cref="Quota.Storagespace"/>
		/// usages.
		/// With the existence of
		/// <see cref="INodeReference"/>
		/// , the same inode and its
		/// subtree may be referred by multiple
		/// <see cref="WithName"/>
		/// nodes and a
		/// <see cref="DstReference"/>
		/// node. To avoid circles while quota usage computation,
		/// we have the following rules:
		/// <pre>
		/// 1. For a
		/// <see cref="DstReference"/>
		/// node, since the node must be in the current
		/// tree (or has been deleted as the end point of a series of rename
		/// operations), we compute the quota usage of the referred node (and its
		/// subtree) in the regular manner, i.e., including every inode in the current
		/// tree and in snapshot copies, as well as the size of diff list.
		/// 2. For a
		/// <see cref="WithName"/>
		/// node, since the node must be in a snapshot, we
		/// only count the quota usage for those nodes that still existed at the
		/// creation time of the snapshot associated with the
		/// <see cref="WithName"/>
		/// node.
		/// We do not count in the size of the diff list.
		/// <pre>
		/// </summary>
		/// <param name="bsps">Block storage policy suite to calculate intended storage type usage
		/// 	</param>
		/// <param name="blockStoragePolicyId">block storage policy id of the current INode</param>
		/// <param name="counts">The subtree counts for returning.</param>
		/// <param name="useCache">
		/// Whether to use cached quota usage. Note that
		/// <see cref="WithName"/>
		/// node never uses cache for its subtree.
		/// </param>
		/// <param name="lastSnapshotId">
		/// 
		/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.CurrentStateId
		/// 	"/>
		/// indicates the
		/// computation is in the current tree. Otherwise the id
		/// indicates the computation range for a
		/// <see cref="WithName"/>
		/// node.
		/// </param>
		/// <returns>The same objects as the counts parameter.</returns>
		public abstract QuotaCounts ComputeQuotaUsage(BlockStoragePolicySuite bsps, byte 
			blockStoragePolicyId, QuotaCounts counts, bool useCache, int lastSnapshotId);

		public QuotaCounts ComputeQuotaUsage(BlockStoragePolicySuite bsps, QuotaCounts counts
			, bool useCache)
		{
			byte storagePolicyId = IsSymlink() ? BlockStoragePolicySuite.IdUnspecified : GetStoragePolicyID
				();
			return ComputeQuotaUsage(bsps, storagePolicyId, counts, useCache, Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
				.CurrentStateId);
		}

		/// <returns>null if the local name is null; otherwise, return the local name.</returns>
		public string GetLocalName()
		{
			byte[] name = GetLocalNameBytes();
			return name == null ? null : DFSUtil.Bytes2String(name);
		}

		public byte[] GetKey()
		{
			return GetLocalNameBytes();
		}

		/// <summary>Set local file name</summary>
		public abstract void SetLocalName(byte[] name);

		public virtual string GetFullPathName()
		{
			// Get the full path name of this inode.
			return FSDirectory.GetFullPathName(this);
		}

		public override string ToString()
		{
			return GetLocalName();
		}

		[VisibleForTesting]
		public string GetObjectString()
		{
			return GetType().Name + "@" + Sharpen.Extensions.ToHexString(base.GetHashCode());
		}

		/// <returns>a string description of the parent.</returns>
		[VisibleForTesting]
		public string GetParentString()
		{
			INodeReference parentRef = GetParentReference();
			if (parentRef != null)
			{
				return "parentRef=" + parentRef.GetLocalName() + "->";
			}
			else
			{
				INodeDirectory parentDir = GetParent();
				if (parentDir != null)
				{
					return "parentDir=" + parentDir.GetLocalName() + "/";
				}
				else
				{
					return "parent=null";
				}
			}
		}

		[VisibleForTesting]
		public virtual string ToDetailString()
		{
			return ToString() + "(" + GetObjectString() + "), " + GetParentString();
		}

		/// <returns>the parent directory</returns>
		public INodeDirectory GetParent()
		{
			return parent == null ? null : parent.IsReference() ? GetParentReference().GetParent
				() : parent.AsDirectory();
		}

		/// <returns>
		/// the parent as a reference if this is a referred inode;
		/// otherwise, return null.
		/// </returns>
		public virtual INodeReference GetParentReference()
		{
			return parent == null || !parent.IsReference() ? null : (INodeReference)parent;
		}

		/// <summary>Set parent directory</summary>
		public void SetParent(INodeDirectory parent)
		{
			this.parent = parent;
		}

		/// <summary>Set container.</summary>
		public void SetParentReference(INodeReference parent)
		{
			this.parent = parent;
		}

		/// <summary>Clear references to other objects.</summary>
		public virtual void Clear()
		{
			SetParent(null);
		}

		/// <param name="snapshotId">
		/// if it is not
		/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.CurrentStateId
		/// 	"/>
		/// , get the result
		/// from the given snapshot; otherwise, get the result from the
		/// current inode.
		/// </param>
		/// <returns>modification time.</returns>
		internal abstract long GetModificationTime(int snapshotId);

		/// <summary>The same as getModificationTime(Snapshot.CURRENT_STATE_ID).</summary>
		public long GetModificationTime()
		{
			return GetModificationTime(Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
				.CurrentStateId);
		}

		/// <summary>Update modification time if it is larger than the current value.</summary>
		public abstract Org.Apache.Hadoop.Hdfs.Server.Namenode.INode UpdateModificationTime
			(long mtime, int latestSnapshotId);

		/// <summary>Set the last modification time of inode.</summary>
		public abstract void SetModificationTime(long modificationTime);

		/// <summary>Set the last modification time of inode.</summary>
		public Org.Apache.Hadoop.Hdfs.Server.Namenode.INode SetModificationTime(long modificationTime
			, int latestSnapshotId)
		{
			RecordModification(latestSnapshotId);
			SetModificationTime(modificationTime);
			return this;
		}

		/// <param name="snapshotId">
		/// if it is not
		/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.CurrentStateId
		/// 	"/>
		/// , get the result
		/// from the given snapshot; otherwise, get the result from the
		/// current inode.
		/// </param>
		/// <returns>access time</returns>
		internal abstract long GetAccessTime(int snapshotId);

		/// <summary>The same as getAccessTime(Snapshot.CURRENT_STATE_ID).</summary>
		public long GetAccessTime()
		{
			return GetAccessTime(Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.CurrentStateId
				);
		}

		/// <summary>Set last access time of inode.</summary>
		public abstract void SetAccessTime(long accessTime);

		/// <summary>Set last access time of inode.</summary>
		public Org.Apache.Hadoop.Hdfs.Server.Namenode.INode SetAccessTime(long accessTime
			, int latestSnapshotId)
		{
			RecordModification(latestSnapshotId);
			SetAccessTime(accessTime);
			return this;
		}

		/// <returns>
		/// the latest block storage policy id of the INode. Specifically,
		/// if a storage policy is directly specified on the INode then return the ID
		/// of that policy. Otherwise follow the latest parental path and return the
		/// ID of the first specified storage policy.
		/// </returns>
		public abstract byte GetStoragePolicyID();

		/// <returns>
		/// the storage policy directly specified on the INode. Return
		/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Blockmanagement.BlockStoragePolicySuite.IdUnspecified
		/// 	"/>
		/// if no policy has
		/// been specified.
		/// </returns>
		public abstract byte GetLocalStoragePolicyID();

		/// <summary>Get the storage policy ID while computing quota usage</summary>
		/// <param name="parentStoragePolicyId">the storage policy ID of the parent directory
		/// 	</param>
		/// <returns>
		/// the storage policy ID of this INode. Note that for an
		/// <see cref="INodeSymlink"/>
		/// we return
		/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Blockmanagement.BlockStoragePolicySuite.IdUnspecified
		/// 	"/>
		/// instead of throwing Exception
		/// </returns>
		public virtual byte GetStoragePolicyIDForQuota(byte parentStoragePolicyId)
		{
			byte localId = IsSymlink() ? BlockStoragePolicySuite.IdUnspecified : GetLocalStoragePolicyID
				();
			return localId != BlockStoragePolicySuite.IdUnspecified ? localId : parentStoragePolicyId;
		}

		/// <summary>
		/// Breaks
		/// <paramref name="path"/>
		/// into components.
		/// </summary>
		/// <returns>
		/// array of byte arrays each of which represents
		/// a single path component.
		/// </returns>
		[VisibleForTesting]
		public static byte[][] GetPathComponents(string path)
		{
			return GetPathComponents(GetPathNames(path));
		}

		/// <summary>Convert strings to byte arrays for path components.</summary>
		internal static byte[][] GetPathComponents(string[] strings)
		{
			if (strings.Length == 0)
			{
				return new byte[][] { null };
			}
			byte[][] bytes = new byte[strings.Length][];
			for (int i = 0; i < strings.Length; i++)
			{
				bytes[i] = DFSUtil.String2Bytes(strings[i]);
			}
			return bytes;
		}

		/// <summary>
		/// Splits an absolute
		/// <paramref name="path"/>
		/// into an array of path components.
		/// </summary>
		/// <exception cref="System.Exception">if the given path is invalid.</exception>
		/// <returns>array of path components.</returns>
		public static string[] GetPathNames(string path)
		{
			if (path == null || !path.StartsWith(Path.Separator))
			{
				throw new Exception("Absolute path required");
			}
			return StringUtils.Split(path, Path.SeparatorChar);
		}

		public int CompareTo(byte[] bytes)
		{
			return DFSUtil.CompareBytes(GetLocalNameBytes(), bytes);
		}

		public sealed override bool Equals(object that)
		{
			if (this == that)
			{
				return true;
			}
			if (that == null || !(that is Org.Apache.Hadoop.Hdfs.Server.Namenode.INode))
			{
				return false;
			}
			return GetId() == ((Org.Apache.Hadoop.Hdfs.Server.Namenode.INode)that).GetId();
		}

		public sealed override int GetHashCode()
		{
			long id = GetId();
			return (int)(id ^ ((long)(((ulong)id) >> 32)));
		}

		/// <summary>Dump the subtree starting from this inode.</summary>
		/// <returns>a text representation of the tree.</returns>
		[VisibleForTesting]
		public StringBuilder DumpTreeRecursively()
		{
			StringWriter @out = new StringWriter();
			DumpTreeRecursively(new PrintWriter(@out, true), new StringBuilder(), Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
				.CurrentStateId);
			return @out.GetBuffer();
		}

		[VisibleForTesting]
		public void DumpTreeRecursively(TextWriter @out)
		{
			@out.WriteLine(DumpTreeRecursively().ToString());
		}

		/// <summary>Dump tree recursively.</summary>
		/// <param name="prefix">The prefix string that each line should print.</param>
		[VisibleForTesting]
		public virtual void DumpTreeRecursively(PrintWriter @out, StringBuilder prefix, int
			 snapshotId)
		{
			@out.Write(prefix);
			@out.Write(" ");
			string name = GetLocalName();
			@out.Write(name.IsEmpty() ? "/" : name);
			@out.Write("   (");
			@out.Write(GetObjectString());
			@out.Write("), ");
			@out.Write(GetParentString());
			@out.Write(", " + GetPermissionStatus(snapshotId));
		}

		/// <summary>Information used for updating the blocksMap when deleting files.</summary>
		public class BlocksMapUpdateInfo
		{
			/// <summary>The list of blocks that need to be removed from blocksMap</summary>
			private readonly IList<Block> toDeleteList;

			public BlocksMapUpdateInfo()
			{
				toDeleteList = new ChunkedArrayList<Block>();
			}

			/// <returns>The list of blocks that need to be removed from blocksMap</returns>
			public virtual IList<Block> GetToDeleteList()
			{
				return toDeleteList;
			}

			/// <summary>
			/// Add a to-be-deleted block into the
			/// <see cref="toDeleteList"/>
			/// </summary>
			/// <param name="toDelete">the to-be-deleted block</param>
			public virtual void AddDeleteBlock(Block toDelete)
			{
				System.Diagnostics.Debug.Assert(toDelete != null, "toDelete is null");
				toDeleteList.AddItem(toDelete);
			}

			public virtual void RemoveDeleteBlock(Block block)
			{
				System.Diagnostics.Debug.Assert(block != null, "block is null");
				toDeleteList.Remove(block);
			}

			/// <summary>
			/// Clear
			/// <see cref="toDeleteList"/>
			/// </summary>
			public virtual void Clear()
			{
				toDeleteList.Clear();
			}
		}

		/// <summary>
		/// INode feature such as
		/// <see cref="FileUnderConstructionFeature"/>
		/// and
		/// <see cref="DirectoryWithQuotaFeature"/>
		/// .
		/// </summary>
		public interface Feature
		{
		}

		public abstract short GetFsPermissionShort();

		public abstract byte[] GetLocalNameBytes();

		public abstract long GetPermissionLong();
	}
}
