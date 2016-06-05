using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Base;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot
{
	/// <summary>The difference of an inode between in two snapshots.</summary>
	/// <remarks>
	/// The difference of an inode between in two snapshots.
	/// <see cref="AbstractINodeDiffList{N, A, D}"/>
	/// maintains a list of snapshot diffs,
	/// <pre>
	/// d_1 -&gt; d_2 -&gt; ... -&gt; d_n -&gt; null,
	/// </pre>
	/// where -&gt; denotes the
	/// <see cref="AbstractINodeDiff{N, A, D}.posteriorDiff"/>
	/// reference. The
	/// current directory state is stored in the field of
	/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.INode"/>
	/// .
	/// The snapshot state can be obtained by applying the diffs one-by-one in
	/// reversed chronological order.  Let s_1, s_2, ..., s_n be the corresponding
	/// snapshots.  Then,
	/// <pre>
	/// s_n                     = (current state) - d_n;
	/// s_{n-1} = s_n - d_{n-1} = (current state) - d_n - d_{n-1};
	/// ...
	/// s_k     = s_{k+1} - d_k = (current state) - d_n - d_{n-1} - ... - d_k.
	/// </pre>
	/// </remarks>
	internal abstract class AbstractINodeDiff<N, A, D> : Comparable<int>
		where N : INode
		where A : INodeAttributes
		where D : Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.AbstractINodeDiff<N, A, 
			D>
	{
		/// <summary>The id of the corresponding snapshot.</summary>
		private int snapshotId;

		/// <summary>The snapshot inode data.</summary>
		/// <remarks>The snapshot inode data.  It is null when there is no change.</remarks>
		internal A snapshotINode;

		/// <summary>Posterior diff is the diff happened after this diff.</summary>
		/// <remarks>
		/// Posterior diff is the diff happened after this diff.
		/// The posterior diff should be first applied to obtain the posterior
		/// snapshot and then apply this diff in order to obtain this snapshot.
		/// If the posterior diff is null, the posterior state is the current state.
		/// </remarks>
		private D posteriorDiff;

		internal AbstractINodeDiff(int snapshotId, A snapshotINode, D posteriorDiff)
		{
			this.snapshotId = snapshotId;
			this.snapshotINode = snapshotINode;
			this.posteriorDiff = posteriorDiff;
		}

		/// <summary>Compare diffs with snapshot ID.</summary>
		public int CompareTo(int that)
		{
			return Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.IdIntegerComparator
				.Compare(this.snapshotId, that);
		}

		/// <returns>the snapshot object of this diff.</returns>
		public int GetSnapshotId()
		{
			return snapshotId;
		}

		internal void SetSnapshotId(int snapshot)
		{
			this.snapshotId = snapshot;
		}

		/// <returns>the posterior diff.</returns>
		internal D GetPosterior()
		{
			return posteriorDiff;
		}

		internal void SetPosterior(D posterior)
		{
			posteriorDiff = posterior;
		}

		/// <summary>Save the INode state to the snapshot if it is not done already.</summary>
		internal virtual void SaveSnapshotCopy(A snapshotCopy)
		{
			Preconditions.CheckState(snapshotINode == null, "Expected snapshotINode to be null"
				);
			snapshotINode = snapshotCopy;
		}

		/// <returns>the inode corresponding to the snapshot.</returns>
		internal virtual A GetSnapshotINode()
		{
			// get from this diff, then the posterior diff
			// and then null for the current inode
			for (Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.AbstractINodeDiff<N, A, D> d
				 = this; ; d = d.posteriorDiff)
			{
				if (d.snapshotINode != null)
				{
					return d.snapshotINode;
				}
				else
				{
					if (d.posteriorDiff == null)
					{
						return null;
					}
				}
			}
		}

		/// <summary>Combine the posterior diff and collect blocks for deletion.</summary>
		internal abstract QuotaCounts CombinePosteriorAndCollectBlocks(BlockStoragePolicySuite
			 bsps, N currentINode, D posterior, INode.BlocksMapUpdateInfo collectedBlocks, IList
			<INode> removedINodes);

		/// <summary>Delete and clear self.</summary>
		/// <param name="bsps">The block storage policy suite used to retrieve storage policy
		/// 	</param>
		/// <param name="currentINode">The inode where the deletion happens.</param>
		/// <param name="collectedBlocks">Used to collect blocks for deletion.</param>
		/// <param name="removedINodes">INodes removed</param>
		/// <returns>quota usage delta</returns>
		internal abstract QuotaCounts DestroyDiffAndCollectBlocks(BlockStoragePolicySuite
			 bsps, N currentINode, INode.BlocksMapUpdateInfo collectedBlocks, IList<INode> removedINodes
			);

		public override string ToString()
		{
			return GetType().Name + ": " + this.GetSnapshotId() + " (post=" + (posteriorDiff 
				== null ? null : posteriorDiff.GetSnapshotId()) + ")";
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void WriteSnapshot(DataOutput @out)
		{
			@out.WriteInt(snapshotId);
		}

		/// <exception cref="System.IO.IOException"/>
		internal abstract void Write(DataOutput @out, SnapshotFSImageFormat.ReferenceMap 
			referenceMap);
	}
}
