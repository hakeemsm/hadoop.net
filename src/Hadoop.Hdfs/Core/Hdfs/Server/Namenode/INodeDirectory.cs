using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot;
using Org.Apache.Hadoop.Hdfs.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>Directory INode class.</summary>
	public class INodeDirectory : INodeWithAdditionalFields, INodeDirectoryAttributes
	{
		/// <summary>Cast INode to INodeDirectory.</summary>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.PathIsNotDirectoryException"/>
		public static Org.Apache.Hadoop.Hdfs.Server.Namenode.INodeDirectory ValueOf(INode
			 inode, object path)
		{
			if (inode == null)
			{
				throw new FileNotFoundException("Directory does not exist: " + DFSUtil.Path2String
					(path));
			}
			if (!inode.IsDirectory())
			{
				throw new PathIsNotDirectoryException(DFSUtil.Path2String(path));
			}
			return inode.AsDirectory();
		}

		protected internal const int DefaultFilesPerDirectory = 5;

		internal static readonly byte[] RootName = DFSUtil.String2Bytes(string.Empty);

		private IList<INode> children = null;

		/// <summary>constructor</summary>
		public INodeDirectory(long id, byte[] name, PermissionStatus permissions, long mtime
			)
			: base(id, name, permissions, mtime, 0L)
		{
		}

		/// <summary>Copy constructor</summary>
		/// <param name="other">The INodeDirectory to be copied</param>
		/// <param name="adopt">
		/// Indicate whether or not need to set the parent field of child
		/// INodes to the new node
		/// </param>
		/// <param name="featuresToCopy">
		/// any number of features to copy to the new node.
		/// The method will do a reference copy, not a deep copy.
		/// </param>
		public INodeDirectory(Org.Apache.Hadoop.Hdfs.Server.Namenode.INodeDirectory other
			, bool adopt, params INode.Feature[] featuresToCopy)
			: base(other)
		{
			this.children = other.children;
			if (adopt && this.children != null)
			{
				foreach (INode child in children)
				{
					child.SetParent(this);
				}
			}
			this.features = featuresToCopy;
			AclFeature aclFeature = GetFeature(typeof(AclFeature));
			if (aclFeature != null)
			{
				// for the de-duplication of AclFeature
				RemoveFeature(aclFeature);
				AddFeature(AclStorage.AddAclFeature(aclFeature));
			}
		}

		/// <returns>true unconditionally.</returns>
		public sealed override bool IsDirectory()
		{
			return true;
		}

		/// <returns>this object.</returns>
		public sealed override Org.Apache.Hadoop.Hdfs.Server.Namenode.INodeDirectory AsDirectory
			()
		{
			return this;
		}

		public override byte GetLocalStoragePolicyID()
		{
			XAttrFeature f = GetXAttrFeature();
			ImmutableList<XAttr> xattrs = f == null ? ImmutableList.Of<XAttr>() : f.GetXAttrs
				();
			foreach (XAttr xattr in xattrs)
			{
				if (BlockStoragePolicySuite.IsStoragePolicyXAttr(xattr))
				{
					return (xattr.GetValue())[0];
				}
			}
			return BlockStoragePolicySuite.IdUnspecified;
		}

		public override byte GetStoragePolicyID()
		{
			byte id = GetLocalStoragePolicyID();
			if (id != BlockStoragePolicySuite.IdUnspecified)
			{
				return id;
			}
			// if it is unspecified, check its parent
			return GetParent() != null ? GetParent().GetStoragePolicyID() : BlockStoragePolicySuite
				.IdUnspecified;
		}

		internal virtual void SetQuota(BlockStoragePolicySuite bsps, long nsQuota, long ssQuota
			, StorageType type)
		{
			DirectoryWithQuotaFeature quota = GetDirectoryWithQuotaFeature();
			if (quota != null)
			{
				// already has quota; so set the quota to the new values
				if (type != null)
				{
					quota.SetQuota(ssQuota, type);
				}
				else
				{
					quota.SetQuota(nsQuota, ssQuota);
				}
				if (!IsQuotaSet() && !IsRoot())
				{
					RemoveFeature(quota);
				}
			}
			else
			{
				QuotaCounts c = ComputeQuotaUsage(bsps);
				DirectoryWithQuotaFeature.Builder builder = new DirectoryWithQuotaFeature.Builder
					().NameSpaceQuota(nsQuota);
				if (type != null)
				{
					builder.TypeQuota(type, ssQuota);
				}
				else
				{
					builder.StorageSpaceQuota(ssQuota);
				}
				AddDirectoryWithQuotaFeature(builder.Build()).SetSpaceConsumed(c);
			}
		}

		public override QuotaCounts GetQuotaCounts()
		{
			DirectoryWithQuotaFeature q = GetDirectoryWithQuotaFeature();
			return q != null ? q.GetQuota() : base.GetQuotaCounts();
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.QuotaExceededException"/>
		public override void AddSpaceConsumed(QuotaCounts counts, bool verify)
		{
			DirectoryWithQuotaFeature q = GetDirectoryWithQuotaFeature();
			if (q != null)
			{
				q.AddSpaceConsumed(this, counts, verify);
			}
			else
			{
				AddSpaceConsumed2Parent(counts, verify);
			}
		}

		/// <summary>
		/// If the directory contains a
		/// <see cref="DirectoryWithQuotaFeature"/>
		/// , return it;
		/// otherwise, return null.
		/// </summary>
		public DirectoryWithQuotaFeature GetDirectoryWithQuotaFeature()
		{
			return GetFeature(typeof(DirectoryWithQuotaFeature));
		}

		/// <summary>Is this directory with quota?</summary>
		internal bool IsWithQuota()
		{
			return GetDirectoryWithQuotaFeature() != null;
		}

		internal virtual DirectoryWithQuotaFeature AddDirectoryWithQuotaFeature(DirectoryWithQuotaFeature
			 q)
		{
			Preconditions.CheckState(!IsWithQuota(), "Directory is already with quota");
			AddFeature(q);
			return q;
		}

		internal virtual int SearchChildren(byte[] name)
		{
			return children == null ? -1 : Sharpen.Collections.BinarySearch(children, name);
		}

		public virtual DirectoryWithSnapshotFeature AddSnapshotFeature(DirectoryWithSnapshotFeature.DirectoryDiffList
			 diffs)
		{
			Preconditions.CheckState(!IsWithSnapshot(), "Directory is already with snapshot");
			DirectoryWithSnapshotFeature sf = new DirectoryWithSnapshotFeature(diffs);
			AddFeature(sf);
			return sf;
		}

		/// <summary>
		/// If feature list contains a
		/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.DirectoryWithSnapshotFeature
		/// 	"/>
		/// , return it;
		/// otherwise, return null.
		/// </summary>
		public DirectoryWithSnapshotFeature GetDirectoryWithSnapshotFeature()
		{
			return GetFeature(typeof(DirectoryWithSnapshotFeature));
		}

		/// <summary>Is this file has the snapshot feature?</summary>
		public bool IsWithSnapshot()
		{
			return GetDirectoryWithSnapshotFeature() != null;
		}

		public virtual DirectoryWithSnapshotFeature.DirectoryDiffList GetDiffs()
		{
			DirectoryWithSnapshotFeature sf = GetDirectoryWithSnapshotFeature();
			return sf != null ? sf.GetDiffs() : null;
		}

		public override INodeAttributes GetSnapshotINode(int snapshotId)
		{
			DirectoryWithSnapshotFeature sf = GetDirectoryWithSnapshotFeature();
			return sf == null ? this : sf.GetDiffs().GetSnapshotINode(snapshotId, this);
		}

		public override string ToDetailString()
		{
			DirectoryWithSnapshotFeature sf = this.GetDirectoryWithSnapshotFeature();
			return base.ToDetailString() + (sf == null ? string.Empty : ", " + sf.GetDiffs());
		}

		public virtual DirectorySnapshottableFeature GetDirectorySnapshottableFeature()
		{
			return GetFeature(typeof(DirectorySnapshottableFeature));
		}

		public virtual bool IsSnapshottable()
		{
			return GetDirectorySnapshottableFeature() != null;
		}

		public virtual Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot GetSnapshot
			(byte[] snapshotName)
		{
			return GetDirectorySnapshottableFeature().GetSnapshot(snapshotName);
		}

		public virtual void SetSnapshotQuota(int snapshotQuota)
		{
			GetDirectorySnapshottableFeature().SetSnapshotQuota(snapshotQuota);
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.SnapshotException"/>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.QuotaExceededException"/>
		public virtual Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot AddSnapshot
			(int id, string name)
		{
			return GetDirectorySnapshottableFeature().AddSnapshot(this, id, name);
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.SnapshotException"/>
		public virtual Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot RemoveSnapshot
			(BlockStoragePolicySuite bsps, string snapshotName, INode.BlocksMapUpdateInfo collectedBlocks
			, IList<INode> removedINodes)
		{
			return GetDirectorySnapshottableFeature().RemoveSnapshot(bsps, this, snapshotName
				, collectedBlocks, removedINodes);
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.SnapshotException"/>
		public virtual void RenameSnapshot(string path, string oldName, string newName)
		{
			GetDirectorySnapshottableFeature().RenameSnapshot(path, oldName, newName);
		}

		/// <summary>add DirectorySnapshottableFeature</summary>
		public virtual void AddSnapshottableFeature()
		{
			Preconditions.CheckState(!IsSnapshottable(), "this is already snapshottable, this=%s"
				, this);
			DirectoryWithSnapshotFeature s = this.GetDirectoryWithSnapshotFeature();
			DirectorySnapshottableFeature snapshottable = new DirectorySnapshottableFeature(s
				);
			if (s != null)
			{
				this.RemoveFeature(s);
			}
			this.AddFeature(snapshottable);
		}

		/// <summary>remove DirectorySnapshottableFeature</summary>
		public virtual void RemoveSnapshottableFeature()
		{
			DirectorySnapshottableFeature s = GetDirectorySnapshottableFeature();
			Preconditions.CheckState(s != null, "The dir does not have snapshottable feature: this=%s"
				, this);
			this.RemoveFeature(s);
			if (s.GetDiffs().AsList().Count > 0)
			{
				// add a DirectoryWithSnapshotFeature back
				DirectoryWithSnapshotFeature sf = new DirectoryWithSnapshotFeature(s.GetDiffs());
				AddFeature(sf);
			}
		}

		/// <summary>Replace the given child with a new child.</summary>
		/// <remarks>
		/// Replace the given child with a new child. Note that we no longer need to
		/// replace an normal INodeDirectory or INodeFile into an
		/// INodeDirectoryWithSnapshot or INodeFileUnderConstruction. The only cases
		/// for child replacement is for reference nodes.
		/// </remarks>
		public virtual void ReplaceChild(INode oldChild, INode newChild, INodeMap inodeMap
			)
		{
			Preconditions.CheckNotNull(children);
			int i = SearchChildren(newChild.GetLocalNameBytes());
			Preconditions.CheckState(i >= 0);
			Preconditions.CheckState(oldChild == children[i] || oldChild == children[i].AsReference
				().GetReferredINode().AsReference().GetReferredINode());
			oldChild = children[i];
			if (oldChild.IsReference() && newChild.IsReference())
			{
				// both are reference nodes, e.g., DstReference -> WithName
				INodeReference.WithCount withCount = (INodeReference.WithCount)oldChild.AsReference
					().GetReferredINode();
				withCount.RemoveReference(oldChild.AsReference());
			}
			children.Set(i, newChild);
			// replace the instance in the created list of the diff list
			DirectoryWithSnapshotFeature sf = this.GetDirectoryWithSnapshotFeature();
			if (sf != null)
			{
				sf.GetDiffs().ReplaceChild(Diff.ListType.Created, oldChild, newChild);
			}
			// update the inodeMap
			if (inodeMap != null)
			{
				inodeMap.Put(newChild);
			}
		}

		internal virtual INodeReference.WithName ReplaceChild4ReferenceWithName(INode oldChild
			, int latestSnapshotId)
		{
			Preconditions.CheckArgument(latestSnapshotId != Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
				.CurrentStateId);
			if (oldChild is INodeReference.WithName)
			{
				return (INodeReference.WithName)oldChild;
			}
			INodeReference.WithCount withCount;
			if (oldChild.IsReference())
			{
				Preconditions.CheckState(oldChild is INodeReference.DstReference);
				withCount = (INodeReference.WithCount)oldChild.AsReference().GetReferredINode();
			}
			else
			{
				withCount = new INodeReference.WithCount(null, oldChild);
			}
			INodeReference.WithName @ref = new INodeReference.WithName(this, withCount, oldChild
				.GetLocalNameBytes(), latestSnapshotId);
			ReplaceChild(oldChild, @ref, null);
			return @ref;
		}

		internal override void RecordModification(int latestSnapshotId)
		{
			if (IsInLatestSnapshot(latestSnapshotId) && !ShouldRecordInSrcSnapshot(latestSnapshotId
				))
			{
				// add snapshot feature if necessary
				DirectoryWithSnapshotFeature sf = GetDirectoryWithSnapshotFeature();
				if (sf == null)
				{
					sf = AddSnapshotFeature(null);
				}
				// record self in the diff list if necessary
				sf.GetDiffs().SaveSelf2Snapshot(latestSnapshotId, this, null);
			}
		}

		/// <summary>Save the child to the latest snapshot.</summary>
		/// <returns>the child inode, which may be replaced.</returns>
		public virtual INode SaveChild2Snapshot(INode child, int latestSnapshotId, INode 
			snapshotCopy)
		{
			if (latestSnapshotId == Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.
				CurrentStateId)
			{
				return child;
			}
			// add snapshot feature if necessary
			DirectoryWithSnapshotFeature sf = GetDirectoryWithSnapshotFeature();
			if (sf == null)
			{
				sf = this.AddSnapshotFeature(null);
			}
			return sf.SaveChild2Snapshot(this, child, latestSnapshotId, snapshotCopy);
		}

		/// <param name="name">the name of the child</param>
		/// <param name="snapshotId">
		/// if it is not
		/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.CurrentStateId
		/// 	"/>
		/// , get the result
		/// from the corresponding snapshot; otherwise, get the result from
		/// the current directory.
		/// </param>
		/// <returns>the child inode.</returns>
		public virtual INode GetChild(byte[] name, int snapshotId)
		{
			DirectoryWithSnapshotFeature sf;
			if (snapshotId == Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.CurrentStateId
				 || (sf = GetDirectoryWithSnapshotFeature()) == null)
			{
				ReadOnlyList<INode> c = GetCurrentChildrenList();
				int i = ReadOnlyList.Util.BinarySearch(c, name);
				return i < 0 ? null : c.Get(i);
			}
			return sf.GetChild(this, name, snapshotId);
		}

		/// <summary>
		/// Search for the given INode in the children list and the deleted lists of
		/// snapshots.
		/// </summary>
		/// <returns>
		/// 
		/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.CurrentStateId
		/// 	"/>
		/// if the inode is in the children
		/// list;
		/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.NoSnapshotId"
		/// 	/>
		/// if the inode is neither in the
		/// children list nor in any snapshot; otherwise the snapshot id of the
		/// corresponding snapshot diff list.
		/// </returns>
		public virtual int SearchChild(INode inode)
		{
			INode child = GetChild(inode.GetLocalNameBytes(), Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
				.CurrentStateId);
			if (child != inode)
			{
				// inode is not in parent's children list, thus inode must be in
				// snapshot. identify the snapshot id and later add it into the path
				DirectoryWithSnapshotFeature.DirectoryDiffList diffs = GetDiffs();
				if (diffs == null)
				{
					return Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.NoSnapshotId;
				}
				return diffs.FindSnapshotDeleted(inode);
			}
			else
			{
				return Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.CurrentStateId;
			}
		}

		/// <param name="snapshotId">
		/// if it is not
		/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.CurrentStateId
		/// 	"/>
		/// , get the result
		/// from the corresponding snapshot; otherwise, get the result from
		/// the current directory.
		/// </param>
		/// <returns>
		/// the current children list if the specified snapshot is null;
		/// otherwise, return the children list corresponding to the snapshot.
		/// Note that the returned list is never null.
		/// </returns>
		public virtual ReadOnlyList<INode> GetChildrenList(int snapshotId)
		{
			DirectoryWithSnapshotFeature sf;
			if (snapshotId == Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.CurrentStateId
				 || (sf = this.GetDirectoryWithSnapshotFeature()) == null)
			{
				return GetCurrentChildrenList();
			}
			return sf.GetChildrenList(this, snapshotId);
		}

		private ReadOnlyList<INode> GetCurrentChildrenList()
		{
			return children == null ? ReadOnlyList.Util.EmptyList<INode>() : ReadOnlyList.Util
				.AsReadOnlyList(children);
		}

		/// <summary>Given a child's name, return the index of the next child</summary>
		/// <param name="name">a child's name</param>
		/// <returns>the index of the next child</returns>
		internal static int NextChild(ReadOnlyList<INode> children, byte[] name)
		{
			if (name.Length == 0)
			{
				// empty name
				return 0;
			}
			int nextPos = ReadOnlyList.Util.BinarySearch(children, name) + 1;
			if (nextPos >= 0)
			{
				return nextPos;
			}
			return -nextPos;
		}

		/// <summary>Remove the specified child from this directory.</summary>
		public virtual bool RemoveChild(INode child, int latestSnapshotId)
		{
			if (IsInLatestSnapshot(latestSnapshotId))
			{
				// create snapshot feature if necessary
				DirectoryWithSnapshotFeature sf = this.GetDirectoryWithSnapshotFeature();
				if (sf == null)
				{
					sf = this.AddSnapshotFeature(null);
				}
				return sf.RemoveChild(this, child, latestSnapshotId);
			}
			return RemoveChild(child);
		}

		/// <summary>Remove the specified child from this directory.</summary>
		/// <remarks>
		/// Remove the specified child from this directory.
		/// The basic remove method which actually calls children.remove(..).
		/// </remarks>
		/// <param name="child">the child inode to be removed</param>
		/// <returns>true if the child is removed; false if the child is not found.</returns>
		public virtual bool RemoveChild(INode child)
		{
			int i = SearchChildren(child.GetLocalNameBytes());
			if (i < 0)
			{
				return false;
			}
			INode removed = children.Remove(i);
			Preconditions.CheckState(removed == child);
			return true;
		}

		/// <summary>Add a child inode to the directory.</summary>
		/// <param name="node">INode to insert</param>
		/// <param name="setModTime">
		/// set modification time for the parent node
		/// not needed when replaying the addition and
		/// the parent already has the proper mod time
		/// </param>
		/// <returns>
		/// false if the child with this name already exists;
		/// otherwise, return true;
		/// </returns>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.QuotaExceededException"/>
		public virtual bool AddChild(INode node, bool setModTime, int latestSnapshotId)
		{
			int low = SearchChildren(node.GetLocalNameBytes());
			if (low >= 0)
			{
				return false;
			}
			if (IsInLatestSnapshot(latestSnapshotId))
			{
				// create snapshot feature if necessary
				DirectoryWithSnapshotFeature sf = this.GetDirectoryWithSnapshotFeature();
				if (sf == null)
				{
					sf = this.AddSnapshotFeature(null);
				}
				return sf.AddChild(this, node, setModTime, latestSnapshotId);
			}
			AddChild(node, low);
			if (setModTime)
			{
				// update modification time of the parent directory
				UpdateModificationTime(node.GetModificationTime(), latestSnapshotId);
			}
			return true;
		}

		public virtual bool AddChild(INode node)
		{
			int low = SearchChildren(node.GetLocalNameBytes());
			if (low >= 0)
			{
				return false;
			}
			AddChild(node, low);
			return true;
		}

		/// <summary>Add the node to the children list at the given insertion point.</summary>
		/// <remarks>
		/// Add the node to the children list at the given insertion point.
		/// The basic add method which actually calls children.add(..).
		/// </remarks>
		private void AddChild(INode node, int insertionPoint)
		{
			if (children == null)
			{
				children = new AList<INode>(DefaultFilesPerDirectory);
			}
			node.SetParent(this);
			children.Add(-insertionPoint - 1, node);
			if (node.GetGroupName() == null)
			{
				node.SetGroup(GetGroupName());
			}
		}

		public override QuotaCounts ComputeQuotaUsage(BlockStoragePolicySuite bsps, byte 
			blockStoragePolicyId, QuotaCounts counts, bool useCache, int lastSnapshotId)
		{
			DirectoryWithSnapshotFeature sf = GetDirectoryWithSnapshotFeature();
			// we are computing the quota usage for a specific snapshot here, i.e., the
			// computation only includes files/directories that exist at the time of the
			// given snapshot
			if (sf != null && lastSnapshotId != Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
				.CurrentStateId && !(useCache && IsQuotaSet()))
			{
				ReadOnlyList<INode> childrenList = GetChildrenList(lastSnapshotId);
				foreach (INode child in childrenList)
				{
					byte childPolicyId = child.GetStoragePolicyIDForQuota(blockStoragePolicyId);
					child.ComputeQuotaUsage(bsps, childPolicyId, counts, useCache, lastSnapshotId);
				}
				counts.AddNameSpace(1);
				return counts;
			}
			// compute the quota usage in the scope of the current directory tree
			DirectoryWithQuotaFeature q = GetDirectoryWithQuotaFeature();
			if (useCache && q != null && q.IsQuotaSet())
			{
				// use the cached quota
				return q.AddCurrentSpaceUsage(counts);
			}
			else
			{
				useCache = q != null && !q.IsQuotaSet() ? false : useCache;
				return ComputeDirectoryQuotaUsage(bsps, blockStoragePolicyId, counts, useCache, lastSnapshotId
					);
			}
		}

		private QuotaCounts ComputeDirectoryQuotaUsage(BlockStoragePolicySuite bsps, byte
			 blockStoragePolicyId, QuotaCounts counts, bool useCache, int lastSnapshotId)
		{
			if (children != null)
			{
				foreach (INode child in children)
				{
					byte childPolicyId = child.GetStoragePolicyIDForQuota(blockStoragePolicyId);
					child.ComputeQuotaUsage(bsps, childPolicyId, counts, useCache, lastSnapshotId);
				}
			}
			return ComputeQuotaUsage4CurrentDirectory(bsps, blockStoragePolicyId, counts);
		}

		/// <summary>Add quota usage for this inode excluding children.</summary>
		public virtual QuotaCounts ComputeQuotaUsage4CurrentDirectory(BlockStoragePolicySuite
			 bsps, byte storagePolicyId, QuotaCounts counts)
		{
			counts.AddNameSpace(1);
			// include the diff list
			DirectoryWithSnapshotFeature sf = GetDirectoryWithSnapshotFeature();
			if (sf != null)
			{
				sf.ComputeQuotaUsage4CurrentDirectory(bsps, storagePolicyId, counts);
			}
			return counts;
		}

		public override ContentSummaryComputationContext ComputeContentSummary(ContentSummaryComputationContext
			 summary)
		{
			DirectoryWithSnapshotFeature sf = GetDirectoryWithSnapshotFeature();
			if (sf != null)
			{
				sf.ComputeContentSummary4Snapshot(summary.GetBlockStoragePolicySuite(), summary.GetCounts
					());
			}
			DirectoryWithQuotaFeature q = GetDirectoryWithQuotaFeature();
			if (q != null)
			{
				return q.ComputeContentSummary(this, summary);
			}
			else
			{
				return ComputeDirectoryContentSummary(summary, Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
					.CurrentStateId);
			}
		}

		protected internal virtual ContentSummaryComputationContext ComputeDirectoryContentSummary
			(ContentSummaryComputationContext summary, int snapshotId)
		{
			ReadOnlyList<INode> childrenList = GetChildrenList(snapshotId);
			// Explicit traversing is done to enable repositioning after relinquishing
			// and reacquiring locks.
			for (int i = 0; i < childrenList.Size(); i++)
			{
				INode child = childrenList.Get(i);
				byte[] childName = child.GetLocalNameBytes();
				long lastYieldCount = summary.GetYieldCount();
				child.ComputeContentSummary(summary);
				// Check whether the computation was paused in the subtree.
				// The counts may be off, but traversing the rest of children
				// should be made safe.
				if (lastYieldCount == summary.GetYieldCount())
				{
					continue;
				}
				// The locks were released and reacquired. Check parent first.
				if (GetParent() == null)
				{
					// Stop further counting and return whatever we have so far.
					break;
				}
				// Obtain the children list again since it may have been modified.
				childrenList = GetChildrenList(snapshotId);
				// Reposition in case the children list is changed. Decrement by 1
				// since it will be incremented when loops.
				i = NextChild(childrenList, childName) - 1;
			}
			// Increment the directory count for this directory.
			summary.GetCounts().AddContent(Content.Directory, 1);
			// Relinquish and reacquire locks if necessary.
			summary.Yield();
			return summary;
		}

		/// <summary>This method is usually called by the undo section of rename.</summary>
		/// <remarks>
		/// This method is usually called by the undo section of rename.
		/// Before calling this function, in the rename operation, we replace the
		/// original src node (of the rename operation) with a reference node (WithName
		/// instance) in both the children list and a created list, delete the
		/// reference node from the children list, and add it to the corresponding
		/// deleted list.
		/// To undo the above operations, we have the following steps in particular:
		/// <pre>
		/// 1) remove the WithName node from the deleted list (if it exists)
		/// 2) replace the WithName node in the created list with srcChild
		/// 3) add srcChild back as a child of srcParent. Note that we already add
		/// the node into the created list of a snapshot diff in step 2, we do not need
		/// to add srcChild to the created list of the latest snapshot.
		/// </pre>
		/// We do not need to update quota usage because the old child is in the
		/// deleted list before.
		/// </remarks>
		/// <param name="oldChild">The reference node to be removed/replaced</param>
		/// <param name="newChild">The node to be added back</param>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.QuotaExceededException">should not throw this exception
		/// 	</exception>
		public virtual void UndoRename4ScrParent(INodeReference oldChild, INode newChild)
		{
			DirectoryWithSnapshotFeature sf = GetDirectoryWithSnapshotFeature();
			Preconditions.CheckState(sf != null, "Directory does not have snapshot feature");
			sf.GetDiffs().RemoveChild(Diff.ListType.Deleted, oldChild);
			sf.GetDiffs().ReplaceChild(Diff.ListType.Created, oldChild, newChild);
			AddChild(newChild, true, Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
				.CurrentStateId);
		}

		/// <summary>
		/// Undo the rename operation for the dst tree, i.e., if the rename operation
		/// (with OVERWRITE option) removes a file/dir from the dst tree, add it back
		/// and delete possible record in the deleted list.
		/// </summary>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.QuotaExceededException"/>
		public virtual void UndoRename4DstParent(BlockStoragePolicySuite bsps, INode deletedChild
			, int latestSnapshotId)
		{
			DirectoryWithSnapshotFeature sf = GetDirectoryWithSnapshotFeature();
			Preconditions.CheckState(sf != null, "Directory does not have snapshot feature");
			bool removeDeletedChild = sf.GetDiffs().RemoveChild(Diff.ListType.Deleted, deletedChild
				);
			int sid = removeDeletedChild ? Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
				.CurrentStateId : latestSnapshotId;
			bool added = AddChild(deletedChild, true, sid);
			// update quota usage if adding is successfully and the old child has not
			// been stored in deleted list before
			if (added && !removeDeletedChild)
			{
				QuotaCounts counts = deletedChild.ComputeQuotaUsage(bsps);
				AddSpaceConsumed(counts, false);
			}
		}

		/// <summary>Set the children list to null.</summary>
		public virtual void ClearChildren()
		{
			this.children = null;
		}

		public override void Clear()
		{
			base.Clear();
			ClearChildren();
		}

		/// <summary>Call cleanSubtree(..) recursively down the subtree.</summary>
		public virtual QuotaCounts CleanSubtreeRecursively(BlockStoragePolicySuite bsps, 
			int snapshot, int prior, INode.BlocksMapUpdateInfo collectedBlocks, IList<INode>
			 removedINodes, IDictionary<INode, INode> excludedNodes)
		{
			QuotaCounts counts = new QuotaCounts.Builder().Build();
			// in case of deletion snapshot, since this call happens after we modify
			// the diff list, the snapshot to be deleted has been combined or renamed
			// to its latest previous snapshot. (besides, we also need to consider nodes
			// created after prior but before snapshot. this will be done in 
			// DirectoryWithSnapshotFeature)
			int s = snapshot != Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.CurrentStateId
				 && prior != Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.NoSnapshotId
				 ? prior : snapshot;
			foreach (INode child in GetChildrenList(s))
			{
				if (snapshot != Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.CurrentStateId
					 && excludedNodes != null && excludedNodes.Contains(child))
				{
					continue;
				}
				else
				{
					QuotaCounts childCounts = child.CleanSubtree(bsps, snapshot, prior, collectedBlocks
						, removedINodes);
					counts.Add(childCounts);
				}
			}
			return counts;
		}

		public override void DestroyAndCollectBlocks(BlockStoragePolicySuite bsps, INode.BlocksMapUpdateInfo
			 collectedBlocks, IList<INode> removedINodes)
		{
			DirectoryWithSnapshotFeature sf = GetDirectoryWithSnapshotFeature();
			if (sf != null)
			{
				sf.Clear(bsps, this, collectedBlocks, removedINodes);
			}
			foreach (INode child in GetChildrenList(Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
				.CurrentStateId))
			{
				child.DestroyAndCollectBlocks(bsps, collectedBlocks, removedINodes);
			}
			if (GetAclFeature() != null)
			{
				AclStorage.RemoveAclFeature(GetAclFeature());
			}
			Clear();
			removedINodes.AddItem(this);
		}

		public override QuotaCounts CleanSubtree(BlockStoragePolicySuite bsps, int snapshotId
			, int priorSnapshotId, INode.BlocksMapUpdateInfo collectedBlocks, IList<INode> removedINodes
			)
		{
			DirectoryWithSnapshotFeature sf = GetDirectoryWithSnapshotFeature();
			// there is snapshot data
			if (sf != null)
			{
				return sf.CleanDirectory(bsps, this, snapshotId, priorSnapshotId, collectedBlocks
					, removedINodes);
			}
			// there is no snapshot data
			if (priorSnapshotId == Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.NoSnapshotId
				 && snapshotId == Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.CurrentStateId)
			{
				// destroy the whole subtree and collect blocks that should be deleted
				QuotaCounts counts = new QuotaCounts.Builder().Build();
				this.ComputeQuotaUsage(bsps, counts, true);
				DestroyAndCollectBlocks(bsps, collectedBlocks, removedINodes);
				return counts;
			}
			else
			{
				// process recursively down the subtree
				QuotaCounts counts = CleanSubtreeRecursively(bsps, snapshotId, priorSnapshotId, collectedBlocks
					, removedINodes, null);
				if (IsQuotaSet())
				{
					GetDirectoryWithQuotaFeature().AddSpaceConsumed2Cache(counts.Negation());
				}
				return counts;
			}
		}

		/// <summary>Compare the metadata with another INodeDirectory</summary>
		public override bool MetadataEquals(INodeDirectoryAttributes other)
		{
			return other != null && GetQuotaCounts().Equals(other.GetQuotaCounts()) && GetPermissionLong
				() == other.GetPermissionLong() && GetAclFeature() == other.GetAclFeature() && GetXAttrFeature
				() == other.GetXAttrFeature();
		}

		internal const string DumptreeExceptLastItem = "+-";

		internal const string DumptreeLastItem = "\\-";

		/*
		* The following code is to dump the tree recursively for testing.
		*
		*      \- foo   (INodeDirectory@33dd2717)
		*        \- sub1   (INodeDirectory@442172)
		*          +- file1   (INodeFile@78392d4)
		*          +- file2   (INodeFile@78392d5)
		*          +- sub11   (INodeDirectory@8400cff)
		*            \- file3   (INodeFile@78392d6)
		*          \- z_file4   (INodeFile@45848712)
		*/
		[VisibleForTesting]
		public override void DumpTreeRecursively(PrintWriter @out, StringBuilder prefix, 
			int snapshot)
		{
			base.DumpTreeRecursively(@out, prefix, snapshot);
			@out.Write(", childrenSize=" + GetChildrenList(snapshot).Size());
			DirectoryWithQuotaFeature q = GetDirectoryWithQuotaFeature();
			if (q != null)
			{
				@out.Write(", " + q);
			}
			if (this is Snapshot.Root)
			{
				@out.Write(", snapshotId=" + snapshot);
			}
			@out.WriteLine();
			if (prefix.Length >= 2)
			{
				prefix.Length = prefix.Length - 2;
				prefix.Append("  ");
			}
			DumpTreeRecursively(@out, prefix, new _IEnumerable_874(this, snapshot));
			DirectorySnapshottableFeature s = GetDirectorySnapshottableFeature();
			if (s != null)
			{
				s.DumpTreeRecursively(this, @out, prefix, snapshot);
			}
		}

		private sealed class _IEnumerable_874 : IEnumerable<INodeDirectory.SnapshotAndINode
			>
		{
			public _IEnumerable_874(INodeDirectory _enclosing, int snapshot)
			{
				this._enclosing = _enclosing;
				this.snapshot = snapshot;
				this.i = this._enclosing.GetChildrenList(snapshot).GetEnumerator();
			}

			internal readonly IEnumerator<INode> i;

			public override IEnumerator<INodeDirectory.SnapshotAndINode> GetEnumerator()
			{
				return new _IEnumerator_879(this, snapshot);
			}

			private sealed class _IEnumerator_879 : IEnumerator<INodeDirectory.SnapshotAndINode
				>
			{
				public _IEnumerator_879(_IEnumerable_874 _enclosing, int snapshot)
				{
					this._enclosing = _enclosing;
					this.snapshot = snapshot;
				}

				public override bool HasNext()
				{
					return this._enclosing.i.HasNext();
				}

				public override INodeDirectory.SnapshotAndINode Next()
				{
					return new INodeDirectory.SnapshotAndINode(snapshot, this._enclosing.i.Next());
				}

				public override void Remove()
				{
					throw new NotSupportedException();
				}

				private readonly _IEnumerable_874 _enclosing;

				private readonly int snapshot;
			}

			private readonly INodeDirectory _enclosing;

			private readonly int snapshot;
		}

		/// <summary>Dump the given subtrees.</summary>
		/// <param name="prefix">The prefix string that each line should print.</param>
		/// <param name="subs">The subtrees.</param>
		[VisibleForTesting]
		public static void DumpTreeRecursively(PrintWriter @out, StringBuilder prefix, IEnumerable
			<INodeDirectory.SnapshotAndINode> subs)
		{
			if (subs != null)
			{
				for (IEnumerator<INodeDirectory.SnapshotAndINode> i = subs.GetEnumerator(); i.HasNext
					(); )
				{
					INodeDirectory.SnapshotAndINode pair = i.Next();
					prefix.Append(i.HasNext() ? DumptreeExceptLastItem : DumptreeLastItem);
					pair.inode.DumpTreeRecursively(@out, prefix, pair.snapshotId);
					prefix.Length = prefix.Length - 2;
				}
			}
		}

		/// <summary>A pair of Snapshot and INode objects.</summary>
		public class SnapshotAndINode
		{
			public readonly int snapshotId;

			public readonly INode inode;

			public SnapshotAndINode(int snapshot, INode inode)
			{
				this.snapshotId = snapshot;
				this.inode = inode;
			}
		}

		public int GetChildrenNum(int snapshotId)
		{
			return GetChildrenList(snapshotId).Size();
		}
	}
}
