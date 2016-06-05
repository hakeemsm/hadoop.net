using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>
	/// An
	/// <see cref="INode"/>
	/// representing a symbolic link.
	/// </summary>
	public class INodeSymlink : INodeWithAdditionalFields
	{
		private readonly byte[] symlink;

		internal INodeSymlink(long id, byte[] name, PermissionStatus permissions, long mtime
			, long atime, string symlink)
			: base(id, name, permissions, mtime, atime)
		{
			// The target URI
			this.symlink = DFSUtil.String2Bytes(symlink);
		}

		internal INodeSymlink(Org.Apache.Hadoop.Hdfs.Server.Namenode.INodeSymlink that)
			: base(that)
		{
			this.symlink = that.symlink;
		}

		internal override void RecordModification(int latestSnapshotId)
		{
			if (IsInLatestSnapshot(latestSnapshotId))
			{
				INodeDirectory parent = GetParent();
				parent.SaveChild2Snapshot(this, latestSnapshotId, new Org.Apache.Hadoop.Hdfs.Server.Namenode.INodeSymlink
					(this));
			}
		}

		/// <returns>true unconditionally.</returns>
		public override bool IsSymlink()
		{
			return true;
		}

		/// <returns>this object.</returns>
		public override Org.Apache.Hadoop.Hdfs.Server.Namenode.INodeSymlink AsSymlink()
		{
			return this;
		}

		public virtual string GetSymlinkString()
		{
			return DFSUtil.Bytes2String(symlink);
		}

		public virtual byte[] GetSymlink()
		{
			return symlink;
		}

		public override QuotaCounts CleanSubtree(BlockStoragePolicySuite bsps, int snapshotId
			, int priorSnapshotId, INode.BlocksMapUpdateInfo collectedBlocks, IList<INode> removedINodes
			)
		{
			if (snapshotId == Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.CurrentStateId
				 && priorSnapshotId == Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.
				NoSnapshotId)
			{
				DestroyAndCollectBlocks(bsps, collectedBlocks, removedINodes);
			}
			return new QuotaCounts.Builder().NameSpace(1).Build();
		}

		public override void DestroyAndCollectBlocks(BlockStoragePolicySuite bsps, INode.BlocksMapUpdateInfo
			 collectedBlocks, IList<INode> removedINodes)
		{
			removedINodes.AddItem(this);
		}

		public override QuotaCounts ComputeQuotaUsage(BlockStoragePolicySuite bsps, byte 
			blockStoragePolicyId, QuotaCounts counts, bool useCache, int lastSnapshotId)
		{
			counts.AddNameSpace(1);
			return counts;
		}

		public override ContentSummaryComputationContext ComputeContentSummary(ContentSummaryComputationContext
			 summary)
		{
			summary.GetCounts().AddContent(Content.Symlink, 1);
			return summary;
		}

		public override void DumpTreeRecursively(PrintWriter @out, StringBuilder prefix, 
			int snapshot)
		{
			base.DumpTreeRecursively(@out, prefix, snapshot);
			@out.WriteLine();
		}

		/// <summary>
		/// getAclFeature is not overridden because it is needed for resolving
		/// symlinks.
		/// </summary>
		/// <Override>
		/// final AclFeature getAclFeature(int snapshotId) {
		/// throw new UnsupportedOperationException("ACLs are not supported on symlinks");
		/// }
		/// </Override>
		internal override void RemoveAclFeature()
		{
			throw new NotSupportedException("ACLs are not supported on symlinks");
		}

		internal override void AddAclFeature(AclFeature f)
		{
			throw new NotSupportedException("ACLs are not supported on symlinks");
		}

		internal sealed override XAttrFeature GetXAttrFeature(int snapshotId)
		{
			throw new NotSupportedException("XAttrs are not supported on symlinks");
		}

		internal override void RemoveXAttrFeature()
		{
			throw new NotSupportedException("XAttrs are not supported on symlinks");
		}

		internal override void AddXAttrFeature(XAttrFeature f)
		{
			throw new NotSupportedException("XAttrs are not supported on symlinks");
		}

		public override byte GetStoragePolicyID()
		{
			throw new NotSupportedException("Storage policy are not supported on symlinks");
		}

		public override byte GetLocalStoragePolicyID()
		{
			throw new NotSupportedException("Storage policy are not supported on symlinks");
		}
	}
}
