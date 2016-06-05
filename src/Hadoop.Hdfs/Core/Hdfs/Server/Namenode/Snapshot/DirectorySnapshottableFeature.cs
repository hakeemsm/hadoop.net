using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Org.Apache.Hadoop;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Util;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot
{
	/// <summary>
	/// A directory with this feature is a snapshottable directory, where snapshots
	/// can be taken.
	/// </summary>
	/// <remarks>
	/// A directory with this feature is a snapshottable directory, where snapshots
	/// can be taken. This feature extends
	/// <see cref="DirectoryWithSnapshotFeature"/>
	/// , and
	/// maintains extra information about all the snapshots taken on this directory.
	/// </remarks>
	public class DirectorySnapshottableFeature : DirectoryWithSnapshotFeature
	{
		/// <summary>Limit the number of snapshot per snapshottable directory.</summary>
		internal const int SnapshotLimit = 1 << 16;

		/// <summary>Snapshots of this directory in ascending order of snapshot names.</summary>
		/// <remarks>
		/// Snapshots of this directory in ascending order of snapshot names.
		/// Note that snapshots in ascending order of snapshot id are stored in
		/// <see cref="DirectoryWithSnapshotFeature"/>
		/// .diffs (a private field).
		/// </remarks>
		private readonly IList<Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot> 
			snapshotsByNames = new AList<Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
			>();

		/// <summary>Number of snapshots allowed.</summary>
		private int snapshotQuota = SnapshotLimit;

		public DirectorySnapshottableFeature(DirectoryWithSnapshotFeature feature)
			: base(feature == null ? null : feature.GetDiffs())
		{
		}

		/// <returns>the number of existing snapshots.</returns>
		public virtual int GetNumSnapshots()
		{
			return snapshotsByNames.Count;
		}

		private int SearchSnapshot(byte[] snapshotName)
		{
			return Sharpen.Collections.BinarySearch(snapshotsByNames, snapshotName);
		}

		/// <returns>the snapshot with the given name.</returns>
		public virtual Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot GetSnapshot
			(byte[] snapshotName)
		{
			int i = SearchSnapshot(snapshotName);
			return i < 0 ? null : snapshotsByNames[i];
		}

		public virtual Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot GetSnapshotById
			(int sid)
		{
			foreach (Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot s in snapshotsByNames)
			{
				if (s.GetId() == sid)
				{
					return s;
				}
			}
			return null;
		}

		/// <returns>
		/// 
		/// <see cref="snapshotsByNames"/>
		/// as a
		/// <see cref="Org.Apache.Hadoop.Hdfs.Util.ReadOnlyList{E}"/>
		/// 
		/// </returns>
		public virtual ReadOnlyList<Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
			> GetSnapshotList()
		{
			return ReadOnlyList.Util.AsReadOnlyList(snapshotsByNames);
		}

		/// <summary>Rename a snapshot</summary>
		/// <param name="path">
		/// The directory path where the snapshot was taken. Used for
		/// generating exception message.
		/// </param>
		/// <param name="oldName">Old name of the snapshot</param>
		/// <param name="newName">New name the snapshot will be renamed to</param>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.SnapshotException">
		/// Throw SnapshotException when either the snapshot with the old
		/// name does not exist or a snapshot with the new name already
		/// exists
		/// </exception>
		public virtual void RenameSnapshot(string path, string oldName, string newName)
		{
			if (newName.Equals(oldName))
			{
				return;
			}
			int indexOfOld = SearchSnapshot(DFSUtil.String2Bytes(oldName));
			if (indexOfOld < 0)
			{
				throw new SnapshotException("The snapshot " + oldName + " does not exist for directory "
					 + path);
			}
			else
			{
				byte[] newNameBytes = DFSUtil.String2Bytes(newName);
				int indexOfNew = SearchSnapshot(newNameBytes);
				if (indexOfNew >= 0)
				{
					throw new SnapshotException("The snapshot " + newName + " already exists for directory "
						 + path);
				}
				// remove the one with old name from snapshotsByNames
				Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot snapshot = snapshotsByNames
					.Remove(indexOfOld);
				INodeDirectory ssRoot = snapshot.GetRoot();
				ssRoot.SetLocalName(newNameBytes);
				indexOfNew = -indexOfNew - 1;
				if (indexOfNew <= indexOfOld)
				{
					snapshotsByNames.Add(indexOfNew, snapshot);
				}
				else
				{
					// indexOfNew > indexOfOld
					snapshotsByNames.Add(indexOfNew - 1, snapshot);
				}
			}
		}

		public virtual int GetSnapshotQuota()
		{
			return snapshotQuota;
		}

		public virtual void SetSnapshotQuota(int snapshotQuota)
		{
			if (snapshotQuota < 0)
			{
				throw new HadoopIllegalArgumentException("Cannot set snapshot quota to " + snapshotQuota
					 + " < 0");
			}
			this.snapshotQuota = snapshotQuota;
		}

		/// <summary>
		/// Simply add a snapshot into the
		/// <see cref="snapshotsByNames"/>
		/// . Used when loading
		/// fsimage.
		/// </summary>
		internal virtual void AddSnapshot(Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
			 snapshot)
		{
			this.snapshotsByNames.AddItem(snapshot);
		}

		/// <summary>Add a snapshot.</summary>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.SnapshotException"/>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.QuotaExceededException"/>
		public virtual Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot AddSnapshot
			(INodeDirectory snapshotRoot, int id, string name)
		{
			//check snapshot quota
			int n = GetNumSnapshots();
			if (n + 1 > snapshotQuota)
			{
				throw new SnapshotException("Failed to add snapshot: there are already " + n + " snapshot(s) and the snapshot quota is "
					 + snapshotQuota);
			}
			Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot s = new Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
				(id, name, snapshotRoot);
			byte[] nameBytes = s.GetRoot().GetLocalNameBytes();
			int i = SearchSnapshot(nameBytes);
			if (i >= 0)
			{
				throw new SnapshotException("Failed to add snapshot: there is already a " + "snapshot with the same name \""
					 + Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.GetSnapshotName(s) +
					 "\".");
			}
			DirectoryWithSnapshotFeature.DirectoryDiff d = GetDiffs().AddDiff(id, snapshotRoot
				);
			d.SetSnapshotRoot(s.GetRoot());
			snapshotsByNames.Add(-i - 1, s);
			// set modification time
			long now = Time.Now();
			snapshotRoot.UpdateModificationTime(now, Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
				.CurrentStateId);
			s.GetRoot().SetModificationTime(now, Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
				.CurrentStateId);
			return s;
		}

		/// <summary>
		/// Remove the snapshot with the given name from
		/// <see cref="snapshotsByNames"/>
		/// ,
		/// and delete all the corresponding DirectoryDiff.
		/// </summary>
		/// <param name="snapshotRoot">The directory where we take snapshots</param>
		/// <param name="snapshotName">The name of the snapshot to be removed</param>
		/// <param name="collectedBlocks">Used to collect information to update blocksMap</param>
		/// <returns>
		/// The removed snapshot. Null if no snapshot with the given name
		/// exists.
		/// </returns>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.SnapshotException"/>
		public virtual Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot RemoveSnapshot
			(BlockStoragePolicySuite bsps, INodeDirectory snapshotRoot, string snapshotName, 
			INode.BlocksMapUpdateInfo collectedBlocks, IList<INode> removedINodes)
		{
			int i = SearchSnapshot(DFSUtil.String2Bytes(snapshotName));
			if (i < 0)
			{
				throw new SnapshotException("Cannot delete snapshot " + snapshotName + " from path "
					 + snapshotRoot.GetFullPathName() + ": the snapshot does not exist.");
			}
			else
			{
				Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot snapshot = snapshotsByNames
					[i];
				int prior = Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.FindLatestSnapshot
					(snapshotRoot, snapshot.GetId());
				try
				{
					QuotaCounts counts = snapshotRoot.CleanSubtree(bsps, snapshot.GetId(), prior, collectedBlocks
						, removedINodes);
					INodeDirectory parent = snapshotRoot.GetParent();
					if (parent != null)
					{
						// there will not be any WithName node corresponding to the deleted
						// snapshot, thus only update the quota usage in the current tree
						parent.AddSpaceConsumed(counts.Negation(), true);
					}
				}
				catch (QuotaExceededException e)
				{
					INode.Log.Error("BUG: removeSnapshot increases namespace usage.", e);
				}
				// remove from snapshotsByNames after successfully cleaning the subtree
				snapshotsByNames.Remove(i);
				return snapshot;
			}
		}

		public virtual ContentSummaryComputationContext ComputeContentSummary(BlockStoragePolicySuite
			 bsps, INodeDirectory snapshotRoot, ContentSummaryComputationContext summary)
		{
			snapshotRoot.ComputeContentSummary(summary);
			summary.GetCounts().AddContent(Content.Snapshot, snapshotsByNames.Count);
			summary.GetCounts().AddContent(Content.SnapshottableDirectory, 1);
			return summary;
		}

		/// <summary>
		/// Compute the difference between two snapshots (or a snapshot and the current
		/// directory) of the directory.
		/// </summary>
		/// <param name="from">
		/// The name of the start point of the comparison. Null indicating
		/// the current tree.
		/// </param>
		/// <param name="to">The name of the end point. Null indicating the current tree.</param>
		/// <returns>The difference between the start/end points.</returns>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.SnapshotException">
		/// If there is no snapshot matching the starting
		/// point, or if endSnapshotName is not null but cannot be identified
		/// as a previous snapshot.
		/// </exception>
		internal virtual SnapshotDiffInfo ComputeDiff(INodeDirectory snapshotRoot, string
			 from, string to)
		{
			Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot fromSnapshot = GetSnapshotByName
				(snapshotRoot, from);
			Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot toSnapshot = GetSnapshotByName
				(snapshotRoot, to);
			// if the start point is equal to the end point, return null
			if (from.Equals(to))
			{
				return null;
			}
			SnapshotDiffInfo diffs = new SnapshotDiffInfo(snapshotRoot, fromSnapshot, toSnapshot
				);
			ComputeDiffRecursively(snapshotRoot, snapshotRoot, new AList<byte[]>(), diffs);
			return diffs;
		}

		/// <summary>Find the snapshot matching the given name.</summary>
		/// <param name="snapshotRoot">The directory where snapshots were taken.</param>
		/// <param name="snapshotName">The name of the snapshot.</param>
		/// <returns>The corresponding snapshot. Null if snapshotName is null or empty.</returns>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.SnapshotException">
		/// If snapshotName is not null or empty, but there
		/// is no snapshot matching the name.
		/// </exception>
		private Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot GetSnapshotByName
			(INodeDirectory snapshotRoot, string snapshotName)
		{
			Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot s = null;
			if (snapshotName != null && !snapshotName.IsEmpty())
			{
				int index = SearchSnapshot(DFSUtil.String2Bytes(snapshotName));
				if (index < 0)
				{
					throw new SnapshotException("Cannot find the snapshot of directory " + snapshotRoot
						.GetFullPathName() + " with name " + snapshotName);
				}
				s = snapshotsByNames[index];
			}
			return s;
		}

		/// <summary>
		/// Recursively compute the difference between snapshots under a given
		/// directory/file.
		/// </summary>
		/// <param name="snapshotRoot">The directory where snapshots were taken.</param>
		/// <param name="node">The directory/file under which the diff is computed.</param>
		/// <param name="parentPath">
		/// Relative path (corresponding to the snapshot root) of
		/// the node's parent.
		/// </param>
		/// <param name="diffReport">data structure used to store the diff.</param>
		private void ComputeDiffRecursively(INodeDirectory snapshotRoot, INode node, IList
			<byte[]> parentPath, SnapshotDiffInfo diffReport)
		{
			Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot earlierSnapshot = diffReport
				.IsFromEarlier() ? diffReport.GetFrom() : diffReport.GetTo();
			Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot laterSnapshot = diffReport
				.IsFromEarlier() ? diffReport.GetTo() : diffReport.GetFrom();
			byte[][] relativePath = Sharpen.Collections.ToArray(parentPath, new byte[parentPath
				.Count][]);
			if (node.IsDirectory())
			{
				DirectoryWithSnapshotFeature.ChildrenDiff diff = new DirectoryWithSnapshotFeature.ChildrenDiff
					();
				INodeDirectory dir = node.AsDirectory();
				DirectoryWithSnapshotFeature sf = dir.GetDirectoryWithSnapshotFeature();
				if (sf != null)
				{
					bool change = sf.ComputeDiffBetweenSnapshots(earlierSnapshot, laterSnapshot, diff
						, dir);
					if (change)
					{
						diffReport.AddDirDiff(dir, relativePath, diff);
					}
				}
				ReadOnlyList<INode> children = dir.GetChildrenList(earlierSnapshot.GetId());
				foreach (INode child in children)
				{
					byte[] name = child.GetLocalNameBytes();
					bool toProcess = diff.SearchIndex(Diff.ListType.Deleted, name) < 0;
					if (!toProcess && child is INodeReference.WithName)
					{
						byte[][] renameTargetPath = FindRenameTargetPath(snapshotRoot, (INodeReference.WithName
							)child, laterSnapshot == null ? Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
							.CurrentStateId : laterSnapshot.GetId());
						if (renameTargetPath != null)
						{
							toProcess = true;
							diffReport.SetRenameTarget(child.GetId(), renameTargetPath);
						}
					}
					if (toProcess)
					{
						parentPath.AddItem(name);
						ComputeDiffRecursively(snapshotRoot, child, parentPath, diffReport);
						parentPath.Remove(parentPath.Count - 1);
					}
				}
			}
			else
			{
				if (node.IsFile() && node.AsFile().IsWithSnapshot())
				{
					INodeFile file = node.AsFile();
					bool change = file.GetFileWithSnapshotFeature().ChangedBetweenSnapshots(file, earlierSnapshot
						, laterSnapshot);
					if (change)
					{
						diffReport.AddFileDiff(file, relativePath);
					}
				}
			}
		}

		/// <summary>We just found a deleted WithName node as the source of a rename operation.
		/// 	</summary>
		/// <remarks>
		/// We just found a deleted WithName node as the source of a rename operation.
		/// However, we should include it in our snapshot diff report as rename only
		/// if the rename target is also under the same snapshottable directory.
		/// </remarks>
		private byte[][] FindRenameTargetPath(INodeDirectory snapshotRoot, INodeReference.WithName
			 wn, int snapshotId)
		{
			INode inode = wn.GetReferredINode();
			List<byte[]> ancestors = Lists.NewLinkedList();
			while (inode != null)
			{
				if (inode == snapshotRoot)
				{
					return Sharpen.Collections.ToArray(ancestors, new byte[ancestors.Count][]);
				}
				if (inode is INodeReference.WithCount)
				{
					inode = ((INodeReference.WithCount)inode).GetParentRef(snapshotId);
				}
				else
				{
					INode parent = inode.GetParentReference() != null ? inode.GetParentReference() : 
						inode.GetParent();
					if (parent != null && parent is INodeDirectory)
					{
						int sid = parent.AsDirectory().SearchChild(inode);
						if (sid < snapshotId)
						{
							return null;
						}
					}
					if (!(parent is INodeReference.WithCount))
					{
						ancestors.AddFirst(inode.GetLocalNameBytes());
					}
					inode = parent;
				}
			}
			return null;
		}

		public override string ToString()
		{
			return "snapshotsByNames=" + snapshotsByNames;
		}

		[VisibleForTesting]
		public virtual void DumpTreeRecursively(INodeDirectory snapshotRoot, PrintWriter 
			@out, StringBuilder prefix, int snapshot)
		{
			if (snapshot == Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.CurrentStateId)
			{
				@out.WriteLine();
				@out.Write(prefix);
				@out.Write("Snapshot of ");
				string name = snapshotRoot.GetLocalName();
				@out.Write(name.IsEmpty() ? "/" : name);
				@out.Write(": quota=");
				@out.Write(GetSnapshotQuota());
				int n = 0;
				foreach (DirectoryWithSnapshotFeature.DirectoryDiff diff in GetDiffs())
				{
					if (diff.IsSnapshotRoot())
					{
						n++;
					}
				}
				Preconditions.CheckState(n == snapshotsByNames.Count, "#n=" + n + ", snapshotsByNames.size()="
					 + snapshotsByNames.Count);
				@out.Write(", #snapshot=");
				@out.WriteLine(n);
				INodeDirectory.DumpTreeRecursively(@out, prefix, new _IEnumerable_416(this));
			}
		}

		private sealed class _IEnumerable_416 : IEnumerable<INodeDirectory.SnapshotAndINode
			>
		{
			public _IEnumerable_416(DirectorySnapshottableFeature _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public override IEnumerator<INodeDirectory.SnapshotAndINode> GetEnumerator()
			{
				return new _IEnumerator_419(this);
			}

			private sealed class _IEnumerator_419 : IEnumerator<INodeDirectory.SnapshotAndINode
				>
			{
				public _IEnumerator_419(_IEnumerable_416 _enclosing)
				{
					this._enclosing = _enclosing;
					this.i = this._enclosing._enclosing.GetDiffs().GetEnumerator();
					this.next = this.FindNext();
				}

				internal readonly IEnumerator<DirectoryWithSnapshotFeature.DirectoryDiff> i;

				private DirectoryWithSnapshotFeature.DirectoryDiff next;

				private DirectoryWithSnapshotFeature.DirectoryDiff FindNext()
				{
					for (; this.i.HasNext(); )
					{
						DirectoryWithSnapshotFeature.DirectoryDiff diff = this.i.Next();
						if (diff.IsSnapshotRoot())
						{
							return diff;
						}
					}
					return null;
				}

				public override bool HasNext()
				{
					return this.next != null;
				}

				public override INodeDirectory.SnapshotAndINode Next()
				{
					INodeDirectory.SnapshotAndINode pair = new INodeDirectory.SnapshotAndINode(this.next
						.GetSnapshotId(), this._enclosing._enclosing.GetSnapshotById(this.next.GetSnapshotId
						()).GetRoot());
					this.next = this.FindNext();
					return pair;
				}

				public override void Remove()
				{
					throw new NotSupportedException();
				}

				private readonly _IEnumerable_416 _enclosing;
			}

			private readonly DirectorySnapshottableFeature _enclosing;
		}
	}
}
