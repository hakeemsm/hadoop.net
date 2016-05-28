using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Base;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot
{
	/// <summary>
	/// Feature used to store and process the snapshot diff information for a
	/// directory.
	/// </summary>
	/// <remarks>
	/// Feature used to store and process the snapshot diff information for a
	/// directory. In particular, it contains a directory diff list recording changes
	/// made to the directory and its children for each snapshot.
	/// </remarks>
	public class DirectoryWithSnapshotFeature : INode.Feature
	{
		/// <summary>
		/// The difference between the current state and a previous snapshot
		/// of the children list of an INodeDirectory.
		/// </summary>
		internal class ChildrenDiff : Diff<byte[], INode>
		{
			internal ChildrenDiff()
			{
			}

			private ChildrenDiff(IList<INode> created, IList<INode> deleted)
				: base(created, deleted)
			{
			}

			/// <summary>Replace the given child from the created/deleted list.</summary>
			/// <returns>true if the child is replaced; false if the child is not found.</returns>
			private bool Replace(Diff.ListType type, INode oldChild, INode newChild)
			{
				IList<INode> list = GetList(type);
				int i = Search(list, oldChild.GetLocalNameBytes());
				if (i < 0 || list[i].GetId() != oldChild.GetId())
				{
					return false;
				}
				INode removed = list.Set(i, newChild);
				Preconditions.CheckState(removed == oldChild);
				return true;
			}

			private bool RemoveChild(Diff.ListType type, INode child)
			{
				IList<INode> list = GetList(type);
				int i = SearchIndex(type, child.GetLocalNameBytes());
				if (i >= 0 && list[i] == child)
				{
					list.Remove(i);
					return true;
				}
				return false;
			}

			/// <summary>clear the created list</summary>
			private QuotaCounts DestroyCreatedList(BlockStoragePolicySuite bsps, INodeDirectory
				 currentINode, INode.BlocksMapUpdateInfo collectedBlocks, IList<INode> removedINodes
				)
			{
				QuotaCounts counts = new QuotaCounts.Builder().Build();
				IList<INode> createdList = GetList(Diff.ListType.Created);
				foreach (INode c in createdList)
				{
					c.ComputeQuotaUsage(bsps, counts, true);
					c.DestroyAndCollectBlocks(bsps, collectedBlocks, removedINodes);
					// c should be contained in the children list, remove it
					currentINode.RemoveChild(c);
				}
				createdList.Clear();
				return counts;
			}

			/// <summary>clear the deleted list</summary>
			private QuotaCounts DestroyDeletedList(BlockStoragePolicySuite bsps, INode.BlocksMapUpdateInfo
				 collectedBlocks, IList<INode> removedINodes)
			{
				QuotaCounts counts = new QuotaCounts.Builder().Build();
				IList<INode> deletedList = GetList(Diff.ListType.Deleted);
				foreach (INode d in deletedList)
				{
					d.ComputeQuotaUsage(bsps, counts, false);
					d.DestroyAndCollectBlocks(bsps, collectedBlocks, removedINodes);
				}
				deletedList.Clear();
				return counts;
			}

			/// <summary>
			/// Serialize
			/// <see cref="#created"/>
			/// 
			/// </summary>
			/// <exception cref="System.IO.IOException"/>
			private void WriteCreated(DataOutput @out)
			{
				IList<INode> created = GetList(Diff.ListType.Created);
				@out.WriteInt(created.Count);
				foreach (INode node in created)
				{
					// For INode in created list, we only need to record its local name
					byte[] name = node.GetLocalNameBytes();
					@out.WriteShort(name.Length);
					@out.Write(name);
				}
			}

			/// <summary>
			/// Serialize
			/// <see cref="#deleted"/>
			/// 
			/// </summary>
			/// <exception cref="System.IO.IOException"/>
			private void WriteDeleted(DataOutput @out, SnapshotFSImageFormat.ReferenceMap referenceMap
				)
			{
				IList<INode> deleted = GetList(Diff.ListType.Deleted);
				@out.WriteInt(deleted.Count);
				foreach (INode node in deleted)
				{
					FSImageSerialization.SaveINode2Image(node, @out, true, referenceMap);
				}
			}

			/// <summary>Serialize to out</summary>
			/// <exception cref="System.IO.IOException"/>
			private void Write(DataOutput @out, SnapshotFSImageFormat.ReferenceMap referenceMap
				)
			{
				WriteCreated(@out);
				WriteDeleted(@out, referenceMap);
			}

			/// <summary>Get the list of INodeDirectory contained in the deleted list</summary>
			private void GetDirsInDeleted(IList<INodeDirectory> dirList)
			{
				foreach (INode node in GetList(Diff.ListType.Deleted))
				{
					if (node.IsDirectory())
					{
						dirList.AddItem(node.AsDirectory());
					}
				}
			}
		}

		/// <summary>
		/// The difference of an
		/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.INodeDirectory"/>
		/// between two snapshots.
		/// </summary>
		public class DirectoryDiff : AbstractINodeDiff<INodeDirectory, INodeDirectoryAttributes
			, DirectoryWithSnapshotFeature.DirectoryDiff>
		{
			/// <summary>The size of the children list at snapshot creation time.</summary>
			private readonly int childrenSize;

			/// <summary>The children list diff.</summary>
			private readonly DirectoryWithSnapshotFeature.ChildrenDiff diff;

			private bool isSnapshotRoot = false;

			private DirectoryDiff(int snapshotId, INodeDirectory dir)
				: base(snapshotId, null, null)
			{
				this.childrenSize = dir.GetChildrenList(Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
					.CurrentStateId).Size();
				this.diff = new DirectoryWithSnapshotFeature.ChildrenDiff();
			}

			/// <summary>Constructor used by FSImage loading</summary>
			internal DirectoryDiff(int snapshotId, INodeDirectoryAttributes snapshotINode, DirectoryWithSnapshotFeature.DirectoryDiff
				 posteriorDiff, int childrenSize, IList<INode> createdList, IList<INode> deletedList
				, bool isSnapshotRoot)
				: base(snapshotId, snapshotINode, posteriorDiff)
			{
				this.childrenSize = childrenSize;
				this.diff = new DirectoryWithSnapshotFeature.ChildrenDiff(createdList, deletedList
					);
				this.isSnapshotRoot = isSnapshotRoot;
			}

			public virtual DirectoryWithSnapshotFeature.ChildrenDiff GetChildrenDiff()
			{
				return diff;
			}

			internal virtual void SetSnapshotRoot(INodeDirectoryAttributes root)
			{
				this.snapshotINode = root;
				this.isSnapshotRoot = true;
			}

			internal virtual bool IsSnapshotRoot()
			{
				return isSnapshotRoot;
			}

			internal override QuotaCounts CombinePosteriorAndCollectBlocks(BlockStoragePolicySuite
				 bsps, INodeDirectory currentDir, DirectoryWithSnapshotFeature.DirectoryDiff posterior
				, INode.BlocksMapUpdateInfo collectedBlocks, IList<INode> removedINodes)
			{
				QuotaCounts counts = new QuotaCounts.Builder().Build();
				diff.CombinePosterior(posterior.diff, new _Processor_218(bsps, counts, collectedBlocks
					, removedINodes));
				return counts;
			}

			private sealed class _Processor_218 : Diff.Processor<INode>
			{
				public _Processor_218(BlockStoragePolicySuite bsps, QuotaCounts counts, INode.BlocksMapUpdateInfo
					 collectedBlocks, IList<INode> removedINodes)
				{
					this.bsps = bsps;
					this.counts = counts;
					this.collectedBlocks = collectedBlocks;
					this.removedINodes = removedINodes;
				}

				/// <summary>Collect blocks for deleted files.</summary>
				public void Process(INode inode)
				{
					if (inode != null)
					{
						inode.ComputeQuotaUsage(bsps, counts, false);
						inode.DestroyAndCollectBlocks(bsps, collectedBlocks, removedINodes);
					}
				}

				private readonly BlockStoragePolicySuite bsps;

				private readonly QuotaCounts counts;

				private readonly INode.BlocksMapUpdateInfo collectedBlocks;

				private readonly IList<INode> removedINodes;
			}

			/// <returns>
			/// The children list of a directory in a snapshot.
			/// Since the snapshot is read-only, the logical view of the list is
			/// never changed although the internal data structure may mutate.
			/// </returns>
			private ReadOnlyList<INode> GetChildrenList(INodeDirectory currentDir)
			{
				return new _ReadOnlyList_237(this, currentDir);
			}

			private sealed class _ReadOnlyList_237 : ReadOnlyList<INode>
			{
				public _ReadOnlyList_237(DirectoryDiff _enclosing, INodeDirectory currentDir)
				{
					this._enclosing = _enclosing;
					this.currentDir = currentDir;
					this.children = null;
				}

				private IList<INode> children;

				private IList<INode> InitChildren()
				{
					if (this.children == null)
					{
						DirectoryWithSnapshotFeature.ChildrenDiff combined = new DirectoryWithSnapshotFeature.ChildrenDiff
							();
						for (DirectoryWithSnapshotFeature.DirectoryDiff d = this._enclosing; d != null; d
							 = d.GetPosterior())
						{
							combined.CombinePosterior(d.diff, null);
						}
						this.children = combined.Apply2Current(ReadOnlyList.Util.AsList(currentDir.GetChildrenList
							(Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.CurrentStateId)));
					}
					return this.children;
				}

				public IEnumerator<INode> GetEnumerator()
				{
					return this.InitChildren().GetEnumerator();
				}

				public override bool IsEmpty()
				{
					return this._enclosing.childrenSize == 0;
				}

				public override int Size()
				{
					return this._enclosing.childrenSize;
				}

				public override INode Get(int i)
				{
					return this.InitChildren()[i];
				}

				private readonly DirectoryDiff _enclosing;

				private readonly INodeDirectory currentDir;
			}

			/// <returns>the child with the given name.</returns>
			internal virtual INode GetChild(byte[] name, bool checkPosterior, INodeDirectory 
				currentDir)
			{
				for (DirectoryWithSnapshotFeature.DirectoryDiff d = this; ; d = d.GetPosterior())
				{
					Diff.Container<INode> returned = d.diff.AccessPrevious(name);
					if (returned != null)
					{
						// the diff is able to determine the inode
						return returned.GetElement();
					}
					else
					{
						if (!checkPosterior)
						{
							// Since checkPosterior is false, return null, i.e. not found.
							return null;
						}
						else
						{
							if (d.GetPosterior() == null)
							{
								// no more posterior diff, get from current inode.
								return currentDir.GetChild(name, Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
									.CurrentStateId);
							}
						}
					}
				}
			}

			public override string ToString()
			{
				return base.ToString() + " childrenSize=" + childrenSize + ", " + diff;
			}

			internal virtual int GetChildrenSize()
			{
				return childrenSize;
			}

			/// <exception cref="System.IO.IOException"/>
			internal override void Write(DataOutput @out, SnapshotFSImageFormat.ReferenceMap 
				referenceMap)
			{
				WriteSnapshot(@out);
				@out.WriteInt(childrenSize);
				// Write snapshotINode
				@out.WriteBoolean(isSnapshotRoot);
				if (!isSnapshotRoot)
				{
					if (snapshotINode != null)
					{
						@out.WriteBoolean(true);
						FSImageSerialization.WriteINodeDirectoryAttributes(snapshotINode, @out);
					}
					else
					{
						@out.WriteBoolean(false);
					}
				}
				// Write diff. Node need to write poseriorDiff, since diffs is a list.
				diff.Write(@out, referenceMap);
			}

			internal override QuotaCounts DestroyDiffAndCollectBlocks(BlockStoragePolicySuite
				 bsps, INodeDirectory currentINode, INode.BlocksMapUpdateInfo collectedBlocks, IList
				<INode> removedINodes)
			{
				// this diff has been deleted
				QuotaCounts counts = new QuotaCounts.Builder().Build();
				counts.Add(diff.DestroyDeletedList(bsps, collectedBlocks, removedINodes));
				INodeDirectoryAttributes snapshotINode = GetSnapshotINode();
				if (snapshotINode != null && snapshotINode.GetAclFeature() != null)
				{
					AclStorage.RemoveAclFeature(snapshotINode.GetAclFeature());
				}
				return counts;
			}
		}

		/// <summary>A list of directory diffs.</summary>
		public class DirectoryDiffList : AbstractINodeDiffList<INodeDirectory, INodeDirectoryAttributes
			, DirectoryWithSnapshotFeature.DirectoryDiff>
		{
			internal override DirectoryWithSnapshotFeature.DirectoryDiff CreateDiff(int snapshot
				, INodeDirectory currentDir)
			{
				return new DirectoryWithSnapshotFeature.DirectoryDiff(snapshot, currentDir);
			}

			internal override INodeDirectoryAttributes CreateSnapshotCopy(INodeDirectory currentDir
				)
			{
				return currentDir.IsQuotaSet() ? new INodeDirectoryAttributes.CopyWithQuota(currentDir
					) : new INodeDirectoryAttributes.SnapshotCopy(currentDir);
			}

			/// <summary>Replace the given child in the created/deleted list, if there is any.</summary>
			public virtual bool ReplaceChild(Diff.ListType type, INode oldChild, INode newChild
				)
			{
				IList<DirectoryWithSnapshotFeature.DirectoryDiff> diffList = AsList();
				for (int i = diffList.Count - 1; i >= 0; i--)
				{
					DirectoryWithSnapshotFeature.ChildrenDiff diff = diffList[i].diff;
					if (diff.Replace(type, oldChild, newChild))
					{
						return true;
					}
				}
				return false;
			}

			/// <summary>Remove the given child in the created/deleted list, if there is any.</summary>
			public virtual bool RemoveChild(Diff.ListType type, INode child)
			{
				IList<DirectoryWithSnapshotFeature.DirectoryDiff> diffList = AsList();
				for (int i = diffList.Count - 1; i >= 0; i--)
				{
					DirectoryWithSnapshotFeature.ChildrenDiff diff = diffList[i].diff;
					if (diff.RemoveChild(type, child))
					{
						return true;
					}
				}
				return false;
			}

			/// <summary>
			/// Find the corresponding snapshot whose deleted list contains the given
			/// inode.
			/// </summary>
			/// <returns>
			/// the id of the snapshot.
			/// <see cref="Snapshot.NoSnapshotId"/>
			/// if the
			/// given inode is not in any of the snapshot.
			/// </returns>
			public virtual int FindSnapshotDeleted(INode child)
			{
				IList<DirectoryWithSnapshotFeature.DirectoryDiff> diffList = AsList();
				for (int i = diffList.Count - 1; i >= 0; i--)
				{
					DirectoryWithSnapshotFeature.ChildrenDiff diff = diffList[i].diff;
					int d = diff.SearchIndex(Diff.ListType.Deleted, child.GetLocalNameBytes());
					if (d >= 0 && diff.GetList(Diff.ListType.Deleted)[d] == child)
					{
						return diffList[i].GetSnapshotId();
					}
				}
				return Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.NoSnapshotId;
			}
		}

		private static IDictionary<INode, INode> CloneDiffList(IList<INode> diffList)
		{
			if (diffList == null || diffList.Count == 0)
			{
				return null;
			}
			IDictionary<INode, INode> map = new Dictionary<INode, INode>(diffList.Count);
			foreach (INode node in diffList)
			{
				map[node] = node;
			}
			return map;
		}

		/// <summary>Destroy a subtree under a DstReference node.</summary>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.QuotaExceededException"/>
		public static void DestroyDstSubtree(BlockStoragePolicySuite bsps, INode inode, int
			 snapshot, int prior, INode.BlocksMapUpdateInfo collectedBlocks, IList<INode> removedINodes
			)
		{
			Preconditions.CheckArgument(prior != Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
				.NoSnapshotId);
			if (inode.IsReference())
			{
				if (inode is INodeReference.WithName && snapshot != Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
					.CurrentStateId)
				{
					// this inode has been renamed before the deletion of the DstReference
					// subtree
					inode.CleanSubtree(bsps, snapshot, prior, collectedBlocks, removedINodes);
				}
				else
				{
					// for DstReference node, continue this process to its subtree
					DestroyDstSubtree(bsps, inode.AsReference().GetReferredINode(), snapshot, prior, 
						collectedBlocks, removedINodes);
				}
			}
			else
			{
				if (inode.IsFile())
				{
					inode.CleanSubtree(bsps, snapshot, prior, collectedBlocks, removedINodes);
				}
				else
				{
					if (inode.IsDirectory())
					{
						IDictionary<INode, INode> excludedNodes = null;
						INodeDirectory dir = inode.AsDirectory();
						DirectoryWithSnapshotFeature sf = dir.GetDirectoryWithSnapshotFeature();
						if (sf != null)
						{
							DirectoryWithSnapshotFeature.DirectoryDiffList diffList = sf.GetDiffs();
							DirectoryWithSnapshotFeature.DirectoryDiff priorDiff = diffList.GetDiffById(prior
								);
							if (priorDiff != null && priorDiff.GetSnapshotId() == prior)
							{
								IList<INode> dList = priorDiff.diff.GetList(Diff.ListType.Deleted);
								excludedNodes = CloneDiffList(dList);
							}
							if (snapshot != Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.CurrentStateId)
							{
								diffList.DeleteSnapshotDiff(bsps, snapshot, prior, dir, collectedBlocks, removedINodes
									);
							}
							priorDiff = diffList.GetDiffById(prior);
							if (priorDiff != null && priorDiff.GetSnapshotId() == prior)
							{
								priorDiff.diff.DestroyCreatedList(bsps, dir, collectedBlocks, removedINodes);
							}
						}
						foreach (INode child in inode.AsDirectory().GetChildrenList(prior))
						{
							if (excludedNodes != null && excludedNodes.Contains(child))
							{
								continue;
							}
							DestroyDstSubtree(bsps, child, snapshot, prior, collectedBlocks, removedINodes);
						}
					}
				}
			}
		}

		/// <summary>
		/// Clean an inode while we move it from the deleted list of post to the
		/// deleted list of prior.
		/// </summary>
		/// <param name="bsps">The block storage policy suite.</param>
		/// <param name="inode">The inode to clean.</param>
		/// <param name="post">The post snapshot.</param>
		/// <param name="prior">The id of the prior snapshot.</param>
		/// <param name="collectedBlocks">Used to collect blocks for later deletion.</param>
		/// <returns>Quota usage update.</returns>
		private static QuotaCounts CleanDeletedINode(BlockStoragePolicySuite bsps, INode 
			inode, int post, int prior, INode.BlocksMapUpdateInfo collectedBlocks, IList<INode
			> removedINodes)
		{
			QuotaCounts counts = new QuotaCounts.Builder().Build();
			Deque<INode> queue = new ArrayDeque<INode>();
			queue.AddLast(inode);
			while (!queue.IsEmpty())
			{
				INode topNode = queue.PollFirst();
				if (topNode is INodeReference.WithName)
				{
					INodeReference.WithName wn = (INodeReference.WithName)topNode;
					if (wn.GetLastSnapshotId() >= post)
					{
						wn.CleanSubtree(bsps, post, prior, collectedBlocks, removedINodes);
					}
				}
				else
				{
					// For DstReference node, since the node is not in the created list of
					// prior, we should treat it as regular file/dir
					if (topNode.IsFile() && topNode.AsFile().IsWithSnapshot())
					{
						INodeFile file = topNode.AsFile();
						counts.Add(file.GetDiffs().DeleteSnapshotDiff(bsps, post, prior, file, collectedBlocks
							, removedINodes));
					}
					else
					{
						if (topNode.IsDirectory())
						{
							INodeDirectory dir = topNode.AsDirectory();
							DirectoryWithSnapshotFeature.ChildrenDiff priorChildrenDiff = null;
							DirectoryWithSnapshotFeature sf = dir.GetDirectoryWithSnapshotFeature();
							if (sf != null)
							{
								// delete files/dirs created after prior. Note that these
								// files/dirs, along with inode, were deleted right after post.
								DirectoryWithSnapshotFeature.DirectoryDiff priorDiff = sf.GetDiffs().GetDiffById(
									prior);
								if (priorDiff != null && priorDiff.GetSnapshotId() == prior)
								{
									priorChildrenDiff = priorDiff.GetChildrenDiff();
									counts.Add(priorChildrenDiff.DestroyCreatedList(bsps, dir, collectedBlocks, removedINodes
										));
								}
							}
							foreach (INode child in dir.GetChildrenList(prior))
							{
								if (priorChildrenDiff != null && priorChildrenDiff.Search(Diff.ListType.Deleted, 
									child.GetLocalNameBytes()) != null)
								{
									continue;
								}
								queue.AddLast(child);
							}
						}
					}
				}
			}
			return counts;
		}

		/// <summary>Diff list sorted by snapshot IDs, i.e.</summary>
		/// <remarks>Diff list sorted by snapshot IDs, i.e. in chronological order.</remarks>
		private readonly DirectoryWithSnapshotFeature.DirectoryDiffList diffs;

		public DirectoryWithSnapshotFeature(DirectoryWithSnapshotFeature.DirectoryDiffList
			 diffs)
		{
			this.diffs = diffs != null ? diffs : new DirectoryWithSnapshotFeature.DirectoryDiffList
				();
		}

		/// <returns>the last snapshot.</returns>
		public virtual int GetLastSnapshotId()
		{
			return diffs.GetLastSnapshotId();
		}

		/// <returns>the snapshot diff list.</returns>
		public virtual DirectoryWithSnapshotFeature.DirectoryDiffList GetDiffs()
		{
			return diffs;
		}

		/// <summary>
		/// Get all the directories that are stored in some snapshot but not in the
		/// current children list.
		/// </summary>
		/// <remarks>
		/// Get all the directories that are stored in some snapshot but not in the
		/// current children list. These directories are equivalent to the directories
		/// stored in the deletes lists.
		/// </remarks>
		public virtual void GetSnapshotDirectory(IList<INodeDirectory> snapshotDir)
		{
			foreach (DirectoryWithSnapshotFeature.DirectoryDiff sdiff in diffs)
			{
				sdiff.GetChildrenDiff().GetDirsInDeleted(snapshotDir);
			}
		}

		/// <summary>Add an inode into parent's children list.</summary>
		/// <remarks>
		/// Add an inode into parent's children list. The caller of this method needs
		/// to make sure that parent is in the given snapshot "latest".
		/// </remarks>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.QuotaExceededException"/>
		public virtual bool AddChild(INodeDirectory parent, INode inode, bool setModTime, 
			int latestSnapshotId)
		{
			DirectoryWithSnapshotFeature.ChildrenDiff diff = diffs.CheckAndAddLatestSnapshotDiff
				(latestSnapshotId, parent).diff;
			int undoInfo = diff.Create(inode);
			bool added = parent.AddChild(inode, setModTime, Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
				.CurrentStateId);
			if (!added)
			{
				diff.UndoCreate(inode, undoInfo);
			}
			return added;
		}

		/// <summary>Remove an inode from parent's children list.</summary>
		/// <remarks>
		/// Remove an inode from parent's children list. The caller of this method
		/// needs to make sure that parent is in the given snapshot "latest".
		/// </remarks>
		public virtual bool RemoveChild(INodeDirectory parent, INode child, int latestSnapshotId
			)
		{
			// For a directory that is not a renamed node, if isInLatestSnapshot returns
			// false, the directory is not in the latest snapshot, thus we do not need
			// to record the removed child in any snapshot.
			// For a directory that was moved/renamed, note that if the directory is in
			// any of the previous snapshots, we will create a reference node for the
			// directory while rename, and isInLatestSnapshot will return true in that
			// scenario (if all previous snapshots have been deleted, isInLatestSnapshot
			// still returns false). Thus if isInLatestSnapshot returns false, the
			// directory node cannot be in any snapshot (not in current tree, nor in
			// previous src tree). Thus we do not need to record the removed child in
			// any snapshot.
			DirectoryWithSnapshotFeature.ChildrenDiff diff = diffs.CheckAndAddLatestSnapshotDiff
				(latestSnapshotId, parent).diff;
			Diff.UndoInfo<INode> undoInfo = diff.Delete(child);
			bool removed = parent.RemoveChild(child);
			if (!removed && undoInfo != null)
			{
				// remove failed, undo
				diff.UndoDelete(child, undoInfo);
			}
			return removed;
		}

		/// <returns>
		/// If there is no corresponding directory diff for the given
		/// snapshot, this means that the current children list should be
		/// returned for the snapshot. Otherwise we calculate the children list
		/// for the snapshot and return it.
		/// </returns>
		public virtual ReadOnlyList<INode> GetChildrenList(INodeDirectory currentINode, int
			 snapshotId)
		{
			DirectoryWithSnapshotFeature.DirectoryDiff diff = diffs.GetDiffById(snapshotId);
			return diff != null ? diff.GetChildrenList(currentINode) : currentINode.GetChildrenList
				(Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.CurrentStateId);
		}

		public virtual INode GetChild(INodeDirectory currentINode, byte[] name, int snapshotId
			)
		{
			DirectoryWithSnapshotFeature.DirectoryDiff diff = diffs.GetDiffById(snapshotId);
			return diff != null ? diff.GetChild(name, true, currentINode) : currentINode.GetChild
				(name, Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.CurrentStateId);
		}

		/// <summary>Used to record the modification of a symlink node</summary>
		public virtual INode SaveChild2Snapshot(INodeDirectory currentINode, INode child, 
			int latestSnapshotId, INode snapshotCopy)
		{
			Preconditions.CheckArgument(!child.IsDirectory(), "child is a directory, child=%s"
				, child);
			Preconditions.CheckArgument(latestSnapshotId != Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
				.CurrentStateId);
			DirectoryWithSnapshotFeature.DirectoryDiff diff = diffs.CheckAndAddLatestSnapshotDiff
				(latestSnapshotId, currentINode);
			if (diff.GetChild(child.GetLocalNameBytes(), false, currentINode) != null)
			{
				// it was already saved in the latest snapshot earlier.  
				return child;
			}
			diff.diff.Modify(snapshotCopy, child);
			return child;
		}

		public virtual void Clear(BlockStoragePolicySuite bsps, INodeDirectory currentINode
			, INode.BlocksMapUpdateInfo collectedBlocks, IList<INode> removedINodes)
		{
			// destroy its diff list
			foreach (DirectoryWithSnapshotFeature.DirectoryDiff diff in diffs)
			{
				diff.DestroyDiffAndCollectBlocks(bsps, currentINode, collectedBlocks, removedINodes
					);
			}
			diffs.Clear();
		}

		public virtual QuotaCounts ComputeQuotaUsage4CurrentDirectory(BlockStoragePolicySuite
			 bsps, byte storagePolicyId, QuotaCounts counts)
		{
			foreach (DirectoryWithSnapshotFeature.DirectoryDiff d in diffs)
			{
				foreach (INode deleted in d.GetChildrenDiff().GetList(Diff.ListType.Deleted))
				{
					byte childPolicyId = deleted.GetStoragePolicyIDForQuota(storagePolicyId);
					deleted.ComputeQuotaUsage(bsps, childPolicyId, counts, false, Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
						.CurrentStateId);
				}
			}
			return counts;
		}

		public virtual void ComputeContentSummary4Snapshot(BlockStoragePolicySuite bsps, 
			ContentCounts counts)
		{
			// Create a new blank summary context for blocking processing of subtree.
			ContentSummaryComputationContext summary = new ContentSummaryComputationContext(bsps
				);
			foreach (DirectoryWithSnapshotFeature.DirectoryDiff d in diffs)
			{
				foreach (INode deleted in d.GetChildrenDiff().GetList(Diff.ListType.Deleted))
				{
					deleted.ComputeContentSummary(summary);
				}
			}
			// Add the counts from deleted trees.
			counts.AddContents(summary.GetCounts());
			// Add the deleted directory count.
			counts.AddContent(Content.Directory, diffs.AsList().Count);
		}

		/// <summary>Compute the difference between Snapshots.</summary>
		/// <param name="fromSnapshot">
		/// Start point of the diff computation. Null indicates
		/// current tree.
		/// </param>
		/// <param name="toSnapshot">
		/// End point of the diff computation. Null indicates current
		/// tree.
		/// </param>
		/// <param name="diff">
		/// Used to capture the changes happening to the children. Note
		/// that the diff still represents (later_snapshot - earlier_snapshot)
		/// although toSnapshot can be before fromSnapshot.
		/// </param>
		/// <param name="currentINode">
		/// The
		/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.INodeDirectory"/>
		/// this feature belongs to.
		/// </param>
		/// <returns>Whether changes happened between the startSnapshot and endSnaphsot.</returns>
		internal virtual bool ComputeDiffBetweenSnapshots(Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
			 fromSnapshot, Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot toSnapshot
			, DirectoryWithSnapshotFeature.ChildrenDiff diff, INodeDirectory currentINode)
		{
			int[] diffIndexPair = diffs.ChangedBetweenSnapshots(fromSnapshot, toSnapshot);
			if (diffIndexPair == null)
			{
				return false;
			}
			int earlierDiffIndex = diffIndexPair[0];
			int laterDiffIndex = diffIndexPair[1];
			bool dirMetadataChanged = false;
			INodeDirectoryAttributes dirCopy = null;
			IList<DirectoryWithSnapshotFeature.DirectoryDiff> difflist = diffs.AsList();
			for (int i = earlierDiffIndex; i < laterDiffIndex; i++)
			{
				DirectoryWithSnapshotFeature.DirectoryDiff sdiff = difflist[i];
				diff.CombinePosterior(sdiff.diff, null);
				if (!dirMetadataChanged && sdiff.snapshotINode != null)
				{
					if (dirCopy == null)
					{
						dirCopy = sdiff.snapshotINode;
					}
					else
					{
						if (!dirCopy.MetadataEquals(sdiff.snapshotINode))
						{
							dirMetadataChanged = true;
						}
					}
				}
			}
			if (!diff.IsEmpty() || dirMetadataChanged)
			{
				return true;
			}
			else
			{
				if (dirCopy != null)
				{
					for (int i_1 = laterDiffIndex; i_1 < difflist.Count; i_1++)
					{
						if (!dirCopy.MetadataEquals(difflist[i_1].snapshotINode))
						{
							return true;
						}
					}
					return !dirCopy.MetadataEquals(currentINode);
				}
				else
				{
					return false;
				}
			}
		}

		public virtual QuotaCounts CleanDirectory(BlockStoragePolicySuite bsps, INodeDirectory
			 currentINode, int snapshot, int prior, INode.BlocksMapUpdateInfo collectedBlocks
			, IList<INode> removedINodes)
		{
			QuotaCounts counts = new QuotaCounts.Builder().Build();
			IDictionary<INode, INode> priorCreated = null;
			IDictionary<INode, INode> priorDeleted = null;
			if (snapshot == Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.CurrentStateId)
			{
				// delete the current directory
				currentINode.RecordModification(prior);
				// delete everything in created list
				DirectoryWithSnapshotFeature.DirectoryDiff lastDiff = diffs.GetLast();
				if (lastDiff != null)
				{
					counts.Add(lastDiff.diff.DestroyCreatedList(bsps, currentINode, collectedBlocks, 
						removedINodes));
				}
				counts.Add(currentINode.CleanSubtreeRecursively(bsps, snapshot, prior, collectedBlocks
					, removedINodes, priorDeleted));
			}
			else
			{
				// update prior
				prior = GetDiffs().UpdatePrior(snapshot, prior);
				// if there is a snapshot diff associated with prior, we need to record
				// its original created and deleted list before deleting post
				if (prior != Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.NoSnapshotId)
				{
					DirectoryWithSnapshotFeature.DirectoryDiff priorDiff = this.GetDiffs().GetDiffById
						(prior);
					if (priorDiff != null && priorDiff.GetSnapshotId() == prior)
					{
						IList<INode> cList = priorDiff.diff.GetList(Diff.ListType.Created);
						IList<INode> dList = priorDiff.diff.GetList(Diff.ListType.Deleted);
						priorCreated = CloneDiffList(cList);
						priorDeleted = CloneDiffList(dList);
					}
				}
				counts.Add(GetDiffs().DeleteSnapshotDiff(bsps, snapshot, prior, currentINode, collectedBlocks
					, removedINodes));
				counts.Add(currentINode.CleanSubtreeRecursively(bsps, snapshot, prior, collectedBlocks
					, removedINodes, priorDeleted));
				// check priorDiff again since it may be created during the diff deletion
				if (prior != Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.NoSnapshotId)
				{
					DirectoryWithSnapshotFeature.DirectoryDiff priorDiff = this.GetDiffs().GetDiffById
						(prior);
					if (priorDiff != null && priorDiff.GetSnapshotId() == prior)
					{
						// For files/directories created between "prior" and "snapshot", 
						// we need to clear snapshot copies for "snapshot". Note that we must
						// use null as prior in the cleanSubtree call. Files/directories that
						// were created before "prior" will be covered by the later 
						// cleanSubtreeRecursively call.
						if (priorCreated != null)
						{
							// we only check the node originally in prior's created list
							foreach (INode cNode in priorDiff.GetChildrenDiff().GetList(Diff.ListType.Created
								))
							{
								if (priorCreated.Contains(cNode))
								{
									counts.Add(cNode.CleanSubtree(bsps, snapshot, Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
										.NoSnapshotId, collectedBlocks, removedINodes));
								}
							}
						}
						// When a directory is moved from the deleted list of the posterior
						// diff to the deleted list of this diff, we need to destroy its
						// descendants that were 1) created after taking this diff and 2)
						// deleted after taking posterior diff.
						// For files moved from posterior's deleted list, we also need to
						// delete its snapshot copy associated with the posterior snapshot.
						foreach (INode dNode in priorDiff.GetChildrenDiff().GetList(Diff.ListType.Deleted
							))
						{
							if (priorDeleted == null || !priorDeleted.Contains(dNode))
							{
								counts.Add(CleanDeletedINode(bsps, dNode, snapshot, prior, collectedBlocks, removedINodes
									));
							}
						}
					}
				}
			}
			if (currentINode.IsQuotaSet())
			{
				currentINode.GetDirectoryWithQuotaFeature().AddSpaceConsumed2Cache(counts.Negation
					());
			}
			return counts;
		}
	}
}
