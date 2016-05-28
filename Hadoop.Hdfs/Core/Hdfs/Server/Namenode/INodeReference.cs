using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Com.Google.Common.Base;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>An anonymous reference to an inode.</summary>
	/// <remarks>
	/// An anonymous reference to an inode.
	/// This class and its subclasses are used to support multiple access paths.
	/// A file/directory may have multiple access paths when it is stored in some
	/// snapshots and it is renamed/moved to other locations.
	/// For example,
	/// (1) Suppose we have /abc/foo, say the inode of foo is inode(id=1000,name=foo)
	/// (2) create snapshot s0 for /abc
	/// (3) mv /abc/foo /xyz/bar, i.e. inode(id=1000,name=...) is renamed from "foo"
	/// to "bar" and its parent becomes /xyz.
	/// Then, /xyz/bar and /abc/.snapshot/s0/foo are two different access paths to
	/// the same inode, inode(id=1000,name=bar).
	/// With references, we have the following
	/// - /abc has a child ref(id=1001,name=foo).
	/// - /xyz has a child ref(id=1002)
	/// - Both ref(id=1001,name=foo) and ref(id=1002) point to another reference,
	/// ref(id=1003,count=2).
	/// - Finally, ref(id=1003,count=2) points to inode(id=1000,name=bar).
	/// Note 1: For a reference without name, e.g. ref(id=1002), it uses the name
	/// of the referred inode.
	/// Note 2: getParent() always returns the parent in the current state, e.g.
	/// inode(id=1000,name=bar).getParent() returns /xyz but not /abc.
	/// </remarks>
	public abstract class INodeReference : INode
	{
		/// <summary>Try to remove the given reference and then return the reference count.</summary>
		/// <remarks>
		/// Try to remove the given reference and then return the reference count.
		/// If the given inode is not a reference, return -1;
		/// </remarks>
		public static int TryRemoveReference(INode inode)
		{
			if (!inode.IsReference())
			{
				return -1;
			}
			return RemoveReference(inode.AsReference());
		}

		/// <summary>Remove the given reference and then return the reference count.</summary>
		/// <remarks>
		/// Remove the given reference and then return the reference count.
		/// If the referred inode is not a WithCount, return -1;
		/// </remarks>
		private static int RemoveReference(Org.Apache.Hadoop.Hdfs.Server.Namenode.INodeReference
			 @ref)
		{
			INode referred = @ref.GetReferredINode();
			if (!(referred is INodeReference.WithCount))
			{
				return -1;
			}
			INodeReference.WithCount wc = (INodeReference.WithCount)referred;
			wc.RemoveReference(@ref);
			return wc.GetReferenceCount();
		}

		/// <summary>
		/// When destroying a reference node (WithName or DstReference), we call this
		/// method to identify the snapshot which is the latest snapshot before the
		/// reference node's creation.
		/// </summary>
		internal static int GetPriorSnapshot(Org.Apache.Hadoop.Hdfs.Server.Namenode.INodeReference
			 @ref)
		{
			INodeReference.WithCount wc = (INodeReference.WithCount)@ref.GetReferredINode();
			INodeReference.WithName wn = null;
			if (@ref is INodeReference.DstReference)
			{
				wn = wc.GetLastWithName();
			}
			else
			{
				if (@ref is INodeReference.WithName)
				{
					wn = wc.GetPriorWithName((INodeReference.WithName)@ref);
				}
			}
			if (wn != null)
			{
				INode referred = wc.GetReferredINode();
				if (referred.IsFile() && referred.AsFile().IsWithSnapshot())
				{
					return referred.AsFile().GetDiffs().GetPrior(wn.lastSnapshotId);
				}
				else
				{
					if (referred.IsDirectory())
					{
						DirectoryWithSnapshotFeature sf = referred.AsDirectory().GetDirectoryWithSnapshotFeature
							();
						if (sf != null)
						{
							return sf.GetDiffs().GetPrior(wn.lastSnapshotId);
						}
					}
				}
			}
			return Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.NoSnapshotId;
		}

		private INode referred;

		public INodeReference(INode parent, INode referred)
			: base(parent)
		{
			this.referred = referred;
		}

		public INode GetReferredINode()
		{
			return referred;
		}

		public void SetReferredINode(INode referred)
		{
			this.referred = referred;
		}

		public sealed override bool IsReference()
		{
			return true;
		}

		public sealed override Org.Apache.Hadoop.Hdfs.Server.Namenode.INodeReference AsReference
			()
		{
			return this;
		}

		public sealed override bool IsFile()
		{
			return referred.IsFile();
		}

		public sealed override INodeFile AsFile()
		{
			return referred.AsFile();
		}

		public sealed override bool IsDirectory()
		{
			return referred.IsDirectory();
		}

		public sealed override INodeDirectory AsDirectory()
		{
			return referred.AsDirectory();
		}

		public sealed override bool IsSymlink()
		{
			return referred.IsSymlink();
		}

		public sealed override INodeSymlink AsSymlink()
		{
			return referred.AsSymlink();
		}

		public override byte[] GetLocalNameBytes()
		{
			return referred.GetLocalNameBytes();
		}

		public override void SetLocalName(byte[] name)
		{
			referred.SetLocalName(name);
		}

		public sealed override long GetId()
		{
			return referred.GetId();
		}

		internal sealed override PermissionStatus GetPermissionStatus(int snapshotId)
		{
			return referred.GetPermissionStatus(snapshotId);
		}

		internal sealed override string GetUserName(int snapshotId)
		{
			return referred.GetUserName(snapshotId);
		}

		internal sealed override void SetUser(string user)
		{
			referred.SetUser(user);
		}

		internal sealed override string GetGroupName(int snapshotId)
		{
			return referred.GetGroupName(snapshotId);
		}

		internal sealed override void SetGroup(string group)
		{
			referred.SetGroup(group);
		}

		internal sealed override FsPermission GetFsPermission(int snapshotId)
		{
			return referred.GetFsPermission(snapshotId);
		}

		internal sealed override AclFeature GetAclFeature(int snapshotId)
		{
			return referred.GetAclFeature(snapshotId);
		}

		internal sealed override void AddAclFeature(AclFeature aclFeature)
		{
			referred.AddAclFeature(aclFeature);
		}

		internal sealed override void RemoveAclFeature()
		{
			referred.RemoveAclFeature();
		}

		internal sealed override XAttrFeature GetXAttrFeature(int snapshotId)
		{
			return referred.GetXAttrFeature(snapshotId);
		}

		internal sealed override void AddXAttrFeature(XAttrFeature xAttrFeature)
		{
			referred.AddXAttrFeature(xAttrFeature);
		}

		internal sealed override void RemoveXAttrFeature()
		{
			referred.RemoveXAttrFeature();
		}

		public sealed override short GetFsPermissionShort()
		{
			return referred.GetFsPermissionShort();
		}

		internal override void SetPermission(FsPermission permission)
		{
			referred.SetPermission(permission);
		}

		public override long GetPermissionLong()
		{
			return referred.GetPermissionLong();
		}

		internal sealed override long GetModificationTime(int snapshotId)
		{
			return referred.GetModificationTime(snapshotId);
		}

		public sealed override INode UpdateModificationTime(long mtime, int latestSnapshotId
			)
		{
			return referred.UpdateModificationTime(mtime, latestSnapshotId);
		}

		public sealed override void SetModificationTime(long modificationTime)
		{
			referred.SetModificationTime(modificationTime);
		}

		internal sealed override long GetAccessTime(int snapshotId)
		{
			return referred.GetAccessTime(snapshotId);
		}

		public sealed override void SetAccessTime(long accessTime)
		{
			referred.SetAccessTime(accessTime);
		}

		public sealed override byte GetStoragePolicyID()
		{
			return referred.GetStoragePolicyID();
		}

		public sealed override byte GetLocalStoragePolicyID()
		{
			return referred.GetLocalStoragePolicyID();
		}

		internal sealed override void RecordModification(int latestSnapshotId)
		{
			referred.RecordModification(latestSnapshotId);
		}

		public override QuotaCounts CleanSubtree(BlockStoragePolicySuite bsps, int snapshot
			, int prior, INode.BlocksMapUpdateInfo collectedBlocks, IList<INode> removedINodes
			)
		{
			// used by WithCount
			return referred.CleanSubtree(bsps, snapshot, prior, collectedBlocks, removedINodes
				);
		}

		public override void DestroyAndCollectBlocks(BlockStoragePolicySuite bsps, INode.BlocksMapUpdateInfo
			 collectedBlocks, IList<INode> removedINodes)
		{
			// used by WithCount
			if (RemoveReference(this) <= 0)
			{
				referred.DestroyAndCollectBlocks(bsps, collectedBlocks, removedINodes);
			}
		}

		public override ContentSummaryComputationContext ComputeContentSummary(ContentSummaryComputationContext
			 summary)
		{
			return referred.ComputeContentSummary(summary);
		}

		public override QuotaCounts ComputeQuotaUsage(BlockStoragePolicySuite bsps, byte 
			blockStoragePolicyId, QuotaCounts counts, bool useCache, int lastSnapshotId)
		{
			return referred.ComputeQuotaUsage(bsps, blockStoragePolicyId, counts, useCache, lastSnapshotId
				);
		}

		public sealed override INodeAttributes GetSnapshotINode(int snapshotId)
		{
			return referred.GetSnapshotINode(snapshotId);
		}

		public override QuotaCounts GetQuotaCounts()
		{
			return referred.GetQuotaCounts();
		}

		public sealed override void Clear()
		{
			base.Clear();
			referred = null;
		}

		public override void DumpTreeRecursively(PrintWriter @out, StringBuilder prefix, 
			int snapshot)
		{
			base.DumpTreeRecursively(@out, prefix, snapshot);
			if (this is INodeReference.DstReference)
			{
				@out.Write(", dstSnapshotId=" + ((INodeReference.DstReference)this).dstSnapshotId
					);
			}
			if (this is INodeReference.WithCount)
			{
				@out.Write(", count=" + ((INodeReference.WithCount)this).GetReferenceCount());
			}
			@out.WriteLine();
			StringBuilder b = new StringBuilder();
			for (int i = 0; i < prefix.Length; i++)
			{
				b.Append(' ');
			}
			b.Append("->");
			GetReferredINode().DumpTreeRecursively(@out, b, snapshot);
		}

		public virtual int GetDstSnapshotId()
		{
			return Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.CurrentStateId;
		}

		/// <summary>An anonymous reference with reference count.</summary>
		public class WithCount : INodeReference
		{
			private readonly IList<INodeReference.WithName> withNameList = new AList<INodeReference.WithName
				>();

			private sealed class _IComparer_385 : IComparer<INodeReference.WithName>
			{
				public _IComparer_385()
				{
				}

				public int Compare(INodeReference.WithName left, INodeReference.WithName right)
				{
					return left.lastSnapshotId - right.lastSnapshotId;
				}
			}

			/// <summary>
			/// Compare snapshot with IDs, where null indicates the current status thus
			/// is greater than any non-null snapshot.
			/// </summary>
			public static readonly IComparer<INodeReference.WithName> WithnameComparator = new 
				_IComparer_385();

			public WithCount(INodeReference parent, INode referred)
				: base(parent, referred)
			{
				Preconditions.CheckArgument(!referred.IsReference());
				referred.SetParentReference(this);
			}

			public virtual int GetReferenceCount()
			{
				int count = withNameList.Count;
				if (GetParentReference() != null)
				{
					count++;
				}
				return count;
			}

			/// <summary>Increment and then return the reference count.</summary>
			public virtual void AddReference(INodeReference @ref)
			{
				if (@ref is INodeReference.WithName)
				{
					INodeReference.WithName refWithName = (INodeReference.WithName)@ref;
					int i = Sharpen.Collections.BinarySearch(withNameList, refWithName, WithnameComparator
						);
					Preconditions.CheckState(i < 0);
					withNameList.Add(-i - 1, refWithName);
				}
				else
				{
					if (@ref is INodeReference.DstReference)
					{
						SetParentReference(@ref);
					}
				}
			}

			/// <summary>Decrement and then return the reference count.</summary>
			private override int RemoveReference(INodeReference @ref)
			{
				if (@ref is INodeReference.WithName)
				{
					int i = Sharpen.Collections.BinarySearch(withNameList, (INodeReference.WithName)@ref
						, WithnameComparator);
					if (i >= 0)
					{
						withNameList.Remove(i);
					}
				}
				else
				{
					if (@ref == GetParentReference())
					{
						SetParent(null);
					}
				}
			}

			internal virtual INodeReference.WithName GetLastWithName()
			{
				return withNameList.Count > 0 ? withNameList[withNameList.Count - 1] : null;
			}

			internal virtual INodeReference.WithName GetPriorWithName(INodeReference.WithName
				 post)
			{
				int i = Sharpen.Collections.BinarySearch(withNameList, post, WithnameComparator);
				if (i > 0)
				{
					return withNameList[i - 1];
				}
				else
				{
					if (i == 0 || i == -1)
					{
						return null;
					}
					else
					{
						return withNameList[-i - 2];
					}
				}
			}

			/// <returns>the WithName/DstReference node contained in the given snapshot.</returns>
			public virtual INodeReference GetParentRef(int snapshotId)
			{
				int start = 0;
				int end = withNameList.Count - 1;
				while (start < end)
				{
					int mid = start + (end - start) / 2;
					int sid = withNameList[mid].lastSnapshotId;
					if (sid == snapshotId)
					{
						return withNameList[mid];
					}
					else
					{
						if (sid < snapshotId)
						{
							start = mid + 1;
						}
						else
						{
							end = mid;
						}
					}
				}
				if (start < withNameList.Count && withNameList[start].lastSnapshotId >= snapshotId)
				{
					return withNameList[start];
				}
				else
				{
					return this.GetParentReference();
				}
			}
		}

		/// <summary>A reference with a fixed name.</summary>
		public class WithName : INodeReference
		{
			private readonly byte[] name;

			/// <summary>
			/// The id of the last snapshot in the src tree when this WithName node was
			/// generated.
			/// </summary>
			/// <remarks>
			/// The id of the last snapshot in the src tree when this WithName node was
			/// generated. When calculating the quota usage of the referred node, only
			/// the files/dirs existing when this snapshot was taken will be counted for
			/// this WithName node and propagated along its ancestor path.
			/// </remarks>
			private readonly int lastSnapshotId;

			public WithName(INodeDirectory parent, INodeReference.WithCount referred, byte[] 
				name, int lastSnapshotId)
				: base(parent, referred)
			{
				this.name = name;
				this.lastSnapshotId = lastSnapshotId;
				referred.AddReference(this);
			}

			public sealed override byte[] GetLocalNameBytes()
			{
				return name;
			}

			public sealed override void SetLocalName(byte[] name)
			{
				throw new NotSupportedException("Cannot set name: " + GetType() + " is immutable."
					);
			}

			public virtual int GetLastSnapshotId()
			{
				return lastSnapshotId;
			}

			public sealed override ContentSummaryComputationContext ComputeContentSummary(ContentSummaryComputationContext
				 summary)
			{
				//only count storagespace for WithName
				QuotaCounts q = new QuotaCounts.Builder().Build();
				ComputeQuotaUsage(summary.GetBlockStoragePolicySuite(), GetStoragePolicyID(), q, 
					false, lastSnapshotId);
				summary.GetCounts().AddContent(Content.Diskspace, q.GetStorageSpace());
				summary.GetCounts().AddTypeSpaces(q.GetTypeSpaces());
				return summary;
			}

			public sealed override QuotaCounts ComputeQuotaUsage(BlockStoragePolicySuite bsps
				, byte blockStoragePolicyId, QuotaCounts counts, bool useCache, int lastSnapshotId
				)
			{
				// if this.lastSnapshotId < lastSnapshotId, the rename of the referred 
				// node happened before the rename of its ancestor. This should be 
				// impossible since for WithName node we only count its children at the 
				// time of the rename. 
				Preconditions.CheckState(lastSnapshotId == Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
					.CurrentStateId || this.lastSnapshotId >= lastSnapshotId);
				INode referred = this.GetReferredINode().AsReference().GetReferredINode();
				// We will continue the quota usage computation using the same snapshot id
				// as time line (if the given snapshot id is valid). Also, we cannot use 
				// cache for the referred node since its cached quota may have already 
				// been updated by changes in the current tree.
				int id = lastSnapshotId != Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
					.CurrentStateId ? lastSnapshotId : this.lastSnapshotId;
				return referred.ComputeQuotaUsage(bsps, blockStoragePolicyId, counts, false, id);
			}

			public override QuotaCounts CleanSubtree(BlockStoragePolicySuite bsps, int snapshot
				, int prior, INode.BlocksMapUpdateInfo collectedBlocks, IList<INode> removedINodes
				)
			{
				// since WithName node resides in deleted list acting as a snapshot copy,
				// the parameter snapshot must be non-null
				Preconditions.CheckArgument(snapshot != Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
					.CurrentStateId);
				// if prior is NO_SNAPSHOT_ID, we need to check snapshot belonging to the
				// previous WithName instance
				if (prior == Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.NoSnapshotId)
				{
					prior = GetPriorSnapshot(this);
				}
				if (prior != Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.NoSnapshotId
					 && Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.IdIntegerComparator
					.Compare(snapshot, prior) <= 0)
				{
					return new QuotaCounts.Builder().Build();
				}
				QuotaCounts counts = GetReferredINode().CleanSubtree(bsps, snapshot, prior, collectedBlocks
					, removedINodes);
				INodeReference @ref = GetReferredINode().GetParentReference();
				if (@ref != null)
				{
					try
					{
						@ref.AddSpaceConsumed(counts.Negation(), true);
					}
					catch (QuotaExceededException)
					{
						Org.Mortbay.Log.Log.Warn("Should not have QuotaExceededException");
					}
				}
				if (snapshot < lastSnapshotId)
				{
					// for a WithName node, when we compute its quota usage, we only count
					// in all the nodes existing at the time of the corresponding rename op.
					// Thus if we are deleting a snapshot before/at the snapshot associated 
					// with lastSnapshotId, we do not need to update the quota upwards.
					counts = new QuotaCounts.Builder().Build();
				}
				return counts;
			}

			public override void DestroyAndCollectBlocks(BlockStoragePolicySuite bsps, INode.BlocksMapUpdateInfo
				 collectedBlocks, IList<INode> removedINodes)
			{
				int snapshot = GetSelfSnapshot();
				if (RemoveReference(this) <= 0)
				{
					GetReferredINode().DestroyAndCollectBlocks(bsps, collectedBlocks, removedINodes);
				}
				else
				{
					int prior = GetPriorSnapshot(this);
					INode referred = GetReferredINode().AsReference().GetReferredINode();
					if (snapshot != Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.NoSnapshotId)
					{
						if (prior != Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.NoSnapshotId
							 && snapshot <= prior)
						{
							// the snapshot to be deleted has been deleted while traversing 
							// the src tree of the previous rename operation. This usually 
							// happens when rename's src and dst are under the same 
							// snapshottable directory. E.g., the following operation sequence:
							// 1. create snapshot s1 on /test
							// 2. rename /test/foo/bar to /test/foo2/bar
							// 3. create snapshot s2 on /test
							// 4. rename foo2 again
							// 5. delete snapshot s2
							return;
						}
						try
						{
							QuotaCounts counts = referred.CleanSubtree(bsps, snapshot, prior, collectedBlocks
								, removedINodes);
							INodeReference @ref = GetReferredINode().GetParentReference();
							if (@ref != null)
							{
								@ref.AddSpaceConsumed(counts.Negation(), true);
							}
						}
						catch (QuotaExceededException e)
						{
							Log.Error("should not exceed quota while snapshot deletion", e);
						}
					}
				}
			}

			private int GetSelfSnapshot()
			{
				INode referred = GetReferredINode().AsReference().GetReferredINode();
				int snapshot = Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.NoSnapshotId;
				if (referred.IsFile() && referred.AsFile().IsWithSnapshot())
				{
					snapshot = referred.AsFile().GetDiffs().GetPrior(lastSnapshotId);
				}
				else
				{
					if (referred.IsDirectory())
					{
						DirectoryWithSnapshotFeature sf = referred.AsDirectory().GetDirectoryWithSnapshotFeature
							();
						if (sf != null)
						{
							snapshot = sf.GetDiffs().GetPrior(lastSnapshotId);
						}
					}
				}
				return snapshot;
			}
		}

		public class DstReference : INodeReference
		{
			/// <summary>Record the latest snapshot of the dst subtree before the rename.</summary>
			/// <remarks>
			/// Record the latest snapshot of the dst subtree before the rename. For
			/// later operations on the moved/renamed files/directories, if the latest
			/// snapshot is after this dstSnapshot, changes will be recorded to the
			/// latest snapshot. Otherwise changes will be recorded to the snapshot
			/// belonging to the src of the rename.
			/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.NoSnapshotId"
			/// 	/>
			/// means no dstSnapshot (e.g., src of the
			/// first-time rename).
			/// </remarks>
			private readonly int dstSnapshotId;

			public sealed override int GetDstSnapshotId()
			{
				return dstSnapshotId;
			}

			public DstReference(INodeDirectory parent, INodeReference.WithCount referred, int
				 dstSnapshotId)
				: base(parent, referred)
			{
				this.dstSnapshotId = dstSnapshotId;
				referred.AddReference(this);
			}

			public override QuotaCounts CleanSubtree(BlockStoragePolicySuite bsps, int snapshot
				, int prior, INode.BlocksMapUpdateInfo collectedBlocks, IList<INode> removedINodes
				)
			{
				if (snapshot == Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.CurrentStateId
					 && prior == Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.NoSnapshotId)
				{
					QuotaCounts counts = new QuotaCounts.Builder().Build();
					this.ComputeQuotaUsage(bsps, counts, true);
					DestroyAndCollectBlocks(bsps, collectedBlocks, removedINodes);
					return counts;
				}
				else
				{
					// if prior is NO_SNAPSHOT_ID, we need to check snapshot belonging to 
					// the previous WithName instance
					if (prior == Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.NoSnapshotId)
					{
						prior = GetPriorSnapshot(this);
					}
					// if prior is not NO_SNAPSHOT_ID, and prior is not before the
					// to-be-deleted snapshot, we can quit here and leave the snapshot
					// deletion work to the src tree of rename
					if (snapshot != Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.CurrentStateId
						 && prior != Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.NoSnapshotId
						 && Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.IdIntegerComparator
						.Compare(snapshot, prior) <= 0)
					{
						return new QuotaCounts.Builder().Build();
					}
					return GetReferredINode().CleanSubtree(bsps, snapshot, prior, collectedBlocks, removedINodes
						);
				}
			}

			/// <summary>
			/// <inheritDoc/>
			/// <br/>
			/// To destroy a DstReference node, we first remove its link with the
			/// referred node. If the reference number of the referred node is &lt;= 0, we
			/// destroy the subtree of the referred node. Otherwise, we clean the
			/// referred node's subtree and delete everything created after the last
			/// rename operation, i.e., everything outside of the scope of the prior
			/// WithName nodes.
			/// </summary>
			public override void DestroyAndCollectBlocks(BlockStoragePolicySuite bsps, INode.BlocksMapUpdateInfo
				 collectedBlocks, IList<INode> removedINodes)
			{
				if (RemoveReference(this) <= 0)
				{
					GetReferredINode().DestroyAndCollectBlocks(bsps, collectedBlocks, removedINodes);
				}
				else
				{
					// we will clean everything, including files, directories, and 
					// snapshots, that were created after this prior snapshot
					int prior = GetPriorSnapshot(this);
					// prior must be non-null, otherwise we do not have any previous 
					// WithName nodes, and the reference number will be 0.
					Preconditions.CheckState(prior != Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
						.NoSnapshotId);
					// identify the snapshot created after prior
					int snapshot = GetSelfSnapshot(prior);
					INode referred = GetReferredINode().AsReference().GetReferredINode();
					if (referred.IsFile())
					{
						// if referred is a file, it must be a file with snapshot since we did
						// recordModification before the rename
						INodeFile file = referred.AsFile();
						Preconditions.CheckState(file.IsWithSnapshot());
						// make sure we mark the file as deleted
						file.GetFileWithSnapshotFeature().DeleteCurrentFile();
						// when calling cleanSubtree of the referred node, since we
						// compute quota usage updates before calling this destroy
						// function, we use true for countDiffChange
						referred.CleanSubtree(bsps, snapshot, prior, collectedBlocks, removedINodes);
					}
					else
					{
						if (referred.IsDirectory())
						{
							// similarly, if referred is a directory, it must be an
							// INodeDirectory with snapshot
							INodeDirectory dir = referred.AsDirectory();
							Preconditions.CheckState(dir.IsWithSnapshot());
							try
							{
								DirectoryWithSnapshotFeature.DestroyDstSubtree(bsps, dir, snapshot, prior, collectedBlocks
									, removedINodes);
							}
							catch (QuotaExceededException e)
							{
								Log.Error("should not exceed quota while snapshot deletion", e);
							}
						}
					}
				}
			}

			private int GetSelfSnapshot(int prior)
			{
				INodeReference.WithCount wc = (INodeReference.WithCount)GetReferredINode().AsReference
					();
				INode referred = wc.GetReferredINode();
				int lastSnapshot = Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.CurrentStateId;
				if (referred.IsFile() && referred.AsFile().IsWithSnapshot())
				{
					lastSnapshot = referred.AsFile().GetDiffs().GetLastSnapshotId();
				}
				else
				{
					if (referred.IsDirectory())
					{
						DirectoryWithSnapshotFeature sf = referred.AsDirectory().GetDirectoryWithSnapshotFeature
							();
						if (sf != null)
						{
							lastSnapshot = sf.GetLastSnapshotId();
						}
					}
				}
				if (lastSnapshot != Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.CurrentStateId
					 && lastSnapshot != prior)
				{
					return lastSnapshot;
				}
				else
				{
					return Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.CurrentStateId;
				}
			}
		}
	}
}
