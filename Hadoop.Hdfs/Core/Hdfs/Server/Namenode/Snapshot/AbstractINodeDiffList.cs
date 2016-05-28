using System.Collections.Generic;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot
{
	/// <summary>A list of snapshot diffs for storing snapshot data.</summary>
	/// <?/>
	/// <?/>
	internal abstract class AbstractINodeDiffList<N, A, D> : IEnumerable<D>
		where N : INode
		where A : INodeAttributes
		where D : AbstractINodeDiff<N, A, D>
	{
		/// <summary>Diff list sorted by snapshot IDs, i.e.</summary>
		/// <remarks>Diff list sorted by snapshot IDs, i.e. in chronological order.</remarks>
		private readonly IList<D> diffs = new AList<D>();

		/// <returns>
		/// this list as a unmodifiable
		/// <see cref="System.Collections.IList{E}"/>
		/// .
		/// </returns>
		public IList<D> AsList()
		{
			return Sharpen.Collections.UnmodifiableList(diffs);
		}

		/// <summary>Get the size of the list and then clear it.</summary>
		public virtual void Clear()
		{
			diffs.Clear();
		}

		/// <returns>
		/// an
		/// <see cref="AbstractINodeDiff{N, A, D}"/>
		/// .
		/// </returns>
		internal abstract D CreateDiff(int snapshotId, N currentINode);

		/// <returns>a snapshot copy of the current inode.</returns>
		internal abstract A CreateSnapshotCopy(N currentINode);

		/// <summary>Delete a snapshot.</summary>
		/// <remarks>
		/// Delete a snapshot. The synchronization of the diff list will be done
		/// outside. If the diff to remove is not the first one in the diff list, we
		/// need to combine the diff with its previous one.
		/// </remarks>
		/// <param name="snapshot">The id of the snapshot to be deleted</param>
		/// <param name="prior">The id of the snapshot taken before the to-be-deleted snapshot
		/// 	</param>
		/// <param name="collectedBlocks">Used to collect information for blocksMap update</param>
		/// <returns>delta in namespace.</returns>
		public QuotaCounts DeleteSnapshotDiff(BlockStoragePolicySuite bsps, int snapshot, 
			int prior, N currentINode, INode.BlocksMapUpdateInfo collectedBlocks, IList<INode
			> removedINodes)
		{
			int snapshotIndex = Sharpen.Collections.BinarySearch(diffs, snapshot);
			QuotaCounts counts = new QuotaCounts.Builder().Build();
			D removed = null;
			if (snapshotIndex == 0)
			{
				if (prior != Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.NoSnapshotId)
				{
					// there is still snapshot before
					// set the snapshot to latestBefore
					diffs[snapshotIndex].SetSnapshotId(prior);
				}
				else
				{
					// there is no snapshot before
					removed = diffs.Remove(0);
					counts.Add(removed.DestroyDiffAndCollectBlocks(bsps, currentINode, collectedBlocks
						, removedINodes));
				}
			}
			else
			{
				if (snapshotIndex > 0)
				{
					AbstractINodeDiff<N, A, D> previous = diffs[snapshotIndex - 1];
					if (previous.GetSnapshotId() != prior)
					{
						diffs[snapshotIndex].SetSnapshotId(prior);
					}
					else
					{
						// combine the to-be-removed diff with its previous diff
						removed = diffs.Remove(snapshotIndex);
						if (previous.snapshotINode == null)
						{
							previous.snapshotINode = removed.snapshotINode;
						}
						counts.Add(previous.CombinePosteriorAndCollectBlocks(bsps, currentINode, removed, 
							collectedBlocks, removedINodes));
						previous.SetPosterior(removed.GetPosterior());
						removed.SetPosterior(null);
					}
				}
			}
			return counts;
		}

		/// <summary>
		/// Add an
		/// <see cref="AbstractINodeDiff{N, A, D}"/>
		/// for the given snapshot.
		/// </summary>
		internal D AddDiff(int latestSnapshotId, N currentINode)
		{
			return AddLast(CreateDiff(latestSnapshotId, currentINode));
		}

		/// <summary>Append the diff at the end of the list.</summary>
		private D AddLast(D diff)
		{
			D last = GetLast();
			diffs.AddItem(diff);
			if (last != null)
			{
				last.SetPosterior(diff);
			}
			return diff;
		}

		/// <summary>Add the diff to the beginning of the list.</summary>
		internal void AddFirst(D diff)
		{
			D first = diffs.IsEmpty() ? null : diffs[0];
			diffs.Add(0, diff);
			diff.SetPosterior(first);
		}

		/// <returns>the last diff.</returns>
		public D GetLast()
		{
			int n = diffs.Count;
			return n == 0 ? null : diffs[n - 1];
		}

		/// <returns>the id of the last snapshot.</returns>
		public int GetLastSnapshotId()
		{
			AbstractINodeDiff<N, A, D> last = GetLast();
			return last == null ? Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.CurrentStateId
				 : last.GetSnapshotId();
		}

		/// <summary>Find the latest snapshot before a given snapshot.</summary>
		/// <param name="anchorId">
		/// The returned snapshot's id must be &lt;= or &lt; this given
		/// snapshot id.
		/// </param>
		/// <param name="exclusive">
		/// True means the returned snapshot's id must be &lt; the given
		/// id, otherwise &lt;=.
		/// </param>
		/// <returns>The id of the latest snapshot before the given snapshot.</returns>
		public int GetPrior(int anchorId, bool exclusive)
		{
			if (anchorId == Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.CurrentStateId)
			{
				int last = GetLastSnapshotId();
				if (exclusive && last == anchorId)
				{
					return Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.NoSnapshotId;
				}
				return last;
			}
			int i = Sharpen.Collections.BinarySearch(diffs, anchorId);
			if (exclusive)
			{
				// must be the one before
				if (i == -1 || i == 0)
				{
					return Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.NoSnapshotId;
				}
				else
				{
					int priorIndex = i > 0 ? i - 1 : -i - 2;
					return diffs[priorIndex].GetSnapshotId();
				}
			}
			else
			{
				// the one, or the one before if not existing
				if (i >= 0)
				{
					return diffs[i].GetSnapshotId();
				}
				else
				{
					if (i < -1)
					{
						return diffs[-i - 2].GetSnapshotId();
					}
					else
					{
						// i == -1
						return Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.NoSnapshotId;
					}
				}
			}
		}

		public int GetPrior(int snapshotId)
		{
			return GetPrior(snapshotId, false);
		}

		/// <summary>Update the prior snapshot.</summary>
		internal int UpdatePrior(int snapshot, int prior)
		{
			int p = GetPrior(snapshot, true);
			if (p != Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.CurrentStateId 
				&& Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.IdIntegerComparator.
				Compare(p, prior) > 0)
			{
				return p;
			}
			return prior;
		}

		public D GetDiffById(int snapshotId)
		{
			if (snapshotId == Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.CurrentStateId)
			{
				return null;
			}
			int i = Sharpen.Collections.BinarySearch(diffs, snapshotId);
			if (i >= 0)
			{
				// exact match
				return diffs[i];
			}
			else
			{
				// Exact match not found means that there were no changes between
				// given snapshot and the next state so that the diff for the given
				// snapshot was not recorded. Thus, return the next state.
				int j = -i - 1;
				return j < diffs.Count ? diffs[j] : null;
			}
		}

		/// <summary>
		/// Search for the snapshot whose id is 1) no less than the given id,
		/// and 2) most close to the given id.
		/// </summary>
		public int GetSnapshotById(int snapshotId)
		{
			D diff = GetDiffById(snapshotId);
			return diff == null ? Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.CurrentStateId
				 : diff.GetSnapshotId();
		}

		internal int[] ChangedBetweenSnapshots(Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
			 from, Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot to)
		{
			Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot earlier = from;
			Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot later = to;
			if (Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.IdComparator.Compare
				(from, to) > 0)
			{
				earlier = to;
				later = from;
			}
			int size = diffs.Count;
			int earlierDiffIndex = Sharpen.Collections.BinarySearch(diffs, earlier.GetId());
			int laterDiffIndex = later == null ? size : Sharpen.Collections.BinarySearch(diffs
				, later.GetId());
			if (-earlierDiffIndex - 1 == size)
			{
				// if the earlierSnapshot is after the latest SnapshotDiff stored in
				// diffs, no modification happened after the earlierSnapshot
				return null;
			}
			if (laterDiffIndex == -1 || laterDiffIndex == 0)
			{
				// if the laterSnapshot is the earliest SnapshotDiff stored in diffs, or
				// before it, no modification happened before the laterSnapshot
				return null;
			}
			earlierDiffIndex = earlierDiffIndex < 0 ? (-earlierDiffIndex - 1) : earlierDiffIndex;
			laterDiffIndex = laterDiffIndex < 0 ? (-laterDiffIndex - 1) : laterDiffIndex;
			return new int[] { earlierDiffIndex, laterDiffIndex };
		}

		/// <returns>
		/// the inode corresponding to the given snapshot.
		/// Note that the current inode is returned if there is no change
		/// between the given snapshot and the current state.
		/// </returns>
		public virtual A GetSnapshotINode(int snapshotId, A currentINode)
		{
			D diff = GetDiffById(snapshotId);
			A inode = diff == null ? null : diff.GetSnapshotINode();
			return inode == null ? currentINode : inode;
		}

		/// <summary>Check if the latest snapshot diff exists.</summary>
		/// <remarks>Check if the latest snapshot diff exists.  If not, add it.</remarks>
		/// <returns>the latest snapshot diff, which is never null.</returns>
		internal D CheckAndAddLatestSnapshotDiff(int latestSnapshotId, N currentINode)
		{
			D last = GetLast();
			return (last != null && Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.
				IdIntegerComparator.Compare(last.GetSnapshotId(), latestSnapshotId) >= 0) ? last
				 : AddDiff(latestSnapshotId, currentINode);
		}

		/// <summary>Save the snapshot copy to the latest snapshot.</summary>
		public virtual D SaveSelf2Snapshot(int latestSnapshotId, N currentINode, A snapshotCopy
			)
		{
			D diff = null;
			if (latestSnapshotId != Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.
				CurrentStateId)
			{
				diff = CheckAndAddLatestSnapshotDiff(latestSnapshotId, currentINode);
				if (diff.snapshotINode == null)
				{
					if (snapshotCopy == null)
					{
						snapshotCopy = CreateSnapshotCopy(currentINode);
					}
					diff.SaveSnapshotCopy(snapshotCopy);
				}
			}
			return diff;
		}

		public override IEnumerator<D> GetEnumerator()
		{
			return diffs.GetEnumerator();
		}

		public override string ToString()
		{
			return GetType().Name + ": " + diffs;
		}
	}
}
