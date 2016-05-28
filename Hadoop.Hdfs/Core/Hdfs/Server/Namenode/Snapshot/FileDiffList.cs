using System.Collections.Generic;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot
{
	/// <summary>A list of FileDiffs for storing snapshot data.</summary>
	public class FileDiffList : AbstractINodeDiffList<INodeFile, INodeFileAttributes, 
		FileDiff>
	{
		internal override FileDiff CreateDiff(int snapshotId, INodeFile file)
		{
			return new FileDiff(snapshotId, file);
		}

		internal override INodeFileAttributes CreateSnapshotCopy(INodeFile currentINode)
		{
			return new INodeFileAttributes.SnapshotCopy(currentINode);
		}

		public virtual void DestroyAndCollectSnapshotBlocks(INode.BlocksMapUpdateInfo collectedBlocks
			)
		{
			foreach (FileDiff d in AsList())
			{
				d.DestroyAndCollectSnapshotBlocks(collectedBlocks);
			}
		}

		public virtual void SaveSelf2Snapshot(int latestSnapshotId, INodeFile iNodeFile, 
			INodeFileAttributes snapshotCopy, bool withBlocks)
		{
			FileDiff diff = base.SaveSelf2Snapshot(latestSnapshotId, iNodeFile, snapshotCopy);
			if (withBlocks)
			{
				// Store blocks if this is the first update
				diff.SetBlocks(iNodeFile.GetBlocks());
			}
		}

		public virtual BlockInfoContiguous[] FindEarlierSnapshotBlocks(int snapshotId)
		{
			System.Diagnostics.Debug.Assert(snapshotId != Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
				.NoSnapshotId, "Wrong snapshot id");
			if (snapshotId == Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.CurrentStateId)
			{
				return null;
			}
			IList<FileDiff> diffs = this.AsList();
			int i = Sharpen.Collections.BinarySearch(diffs, snapshotId);
			BlockInfoContiguous[] blocks = null;
			for (i = i >= 0 ? i : -i - 2; i >= 0; i--)
			{
				blocks = diffs[i].GetBlocks();
				if (blocks != null)
				{
					break;
				}
			}
			return blocks;
		}

		public virtual BlockInfoContiguous[] FindLaterSnapshotBlocks(int snapshotId)
		{
			System.Diagnostics.Debug.Assert(snapshotId != Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
				.NoSnapshotId, "Wrong snapshot id");
			if (snapshotId == Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.CurrentStateId)
			{
				return null;
			}
			IList<FileDiff> diffs = this.AsList();
			int i = Sharpen.Collections.BinarySearch(diffs, snapshotId);
			BlockInfoContiguous[] blocks = null;
			for (i = i >= 0 ? i + 1 : -i - 1; i < diffs.Count; i++)
			{
				blocks = diffs[i].GetBlocks();
				if (blocks != null)
				{
					break;
				}
			}
			return blocks;
		}

		/// <summary>
		/// Copy blocks from the removed snapshot into the previous snapshot
		/// up to the file length of the latter.
		/// </summary>
		/// <remarks>
		/// Copy blocks from the removed snapshot into the previous snapshot
		/// up to the file length of the latter.
		/// Collect unused blocks of the removed snapshot.
		/// </remarks>
		internal virtual void CombineAndCollectSnapshotBlocks(BlockStoragePolicySuite bsps
			, INodeFile file, FileDiff removed, INode.BlocksMapUpdateInfo collectedBlocks, IList
			<INode> removedINodes)
		{
			BlockInfoContiguous[] removedBlocks = removed.GetBlocks();
			if (removedBlocks == null)
			{
				FileWithSnapshotFeature sf = file.GetFileWithSnapshotFeature();
				System.Diagnostics.Debug.Assert(sf != null, "FileWithSnapshotFeature is null");
				if (sf.IsCurrentFileDeleted())
				{
					sf.CollectBlocksAndClear(bsps, file, collectedBlocks, removedINodes);
				}
				return;
			}
			int p = GetPrior(removed.GetSnapshotId(), true);
			FileDiff earlierDiff = p == Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
				.NoSnapshotId ? null : GetDiffById(p);
			// Copy blocks to the previous snapshot if not set already
			if (earlierDiff != null)
			{
				earlierDiff.SetBlocks(removedBlocks);
			}
			BlockInfoContiguous[] earlierBlocks = (earlierDiff == null ? new BlockInfoContiguous
				[] {  } : earlierDiff.GetBlocks());
			// Find later snapshot (or file itself) with blocks
			BlockInfoContiguous[] laterBlocks = FindLaterSnapshotBlocks(removed.GetSnapshotId
				());
			laterBlocks = (laterBlocks == null) ? file.GetBlocks() : laterBlocks;
			// Skip blocks, which belong to either the earlier or the later lists
			int i = 0;
			for (; i < removedBlocks.Length; i++)
			{
				if (i < earlierBlocks.Length && removedBlocks[i] == earlierBlocks[i])
				{
					continue;
				}
				if (i < laterBlocks.Length && removedBlocks[i] == laterBlocks[i])
				{
					continue;
				}
				break;
			}
			// Check if last block is part of truncate recovery
			BlockInfoContiguous lastBlock = file.GetLastBlock();
			Block dontRemoveBlock = null;
			if (lastBlock != null && lastBlock.GetBlockUCState().Equals(HdfsServerConstants.BlockUCState
				.UnderRecovery))
			{
				dontRemoveBlock = ((BlockInfoContiguousUnderConstruction)lastBlock).GetTruncateBlock
					();
			}
			// Collect the remaining blocks of the file, ignoring truncate block
			for (; i < removedBlocks.Length; i++)
			{
				if (dontRemoveBlock == null || !removedBlocks[i].Equals(dontRemoveBlock))
				{
					collectedBlocks.AddDeleteBlock(removedBlocks[i]);
				}
			}
		}
	}
}
