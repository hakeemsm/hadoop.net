using System.Collections.Generic;
using System.IO;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot
{
	/// <summary>
	/// The difference of an
	/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.INodeFile"/>
	/// between two snapshots.
	/// </summary>
	public class FileDiff : AbstractINodeDiff<INodeFile, INodeFileAttributes, Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.FileDiff
		>
	{
		/// <summary>The file size at snapshot creation time.</summary>
		private readonly long fileSize;

		/// <summary>A copy of the INodeFile block list.</summary>
		/// <remarks>A copy of the INodeFile block list. Used in truncate.</remarks>
		private BlockInfoContiguous[] blocks;

		internal FileDiff(int snapshotId, INodeFile file)
			: base(snapshotId, null, null)
		{
			fileSize = file.ComputeFileSize();
			blocks = null;
		}

		/// <summary>Constructor used by FSImage loading</summary>
		internal FileDiff(int snapshotId, INodeFileAttributes snapshotINode, Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.FileDiff
			 posteriorDiff, long fileSize)
			: base(snapshotId, snapshotINode, posteriorDiff)
		{
			this.fileSize = fileSize;
			blocks = null;
		}

		/// <returns>the file size in the snapshot.</returns>
		public virtual long GetFileSize()
		{
			return fileSize;
		}

		/// <summary>
		/// Copy block references into the snapshot
		/// up to the current
		/// <see cref="fileSize"/>
		/// .
		/// Should be done only once.
		/// </summary>
		public virtual void SetBlocks(BlockInfoContiguous[] blocks)
		{
			if (this.blocks != null)
			{
				return;
			}
			int numBlocks = 0;
			for (long s = 0; numBlocks < blocks.Length && s < fileSize; numBlocks++)
			{
				s += blocks[numBlocks].GetNumBytes();
			}
			this.blocks = Arrays.CopyOf(blocks, numBlocks);
		}

		public virtual BlockInfoContiguous[] GetBlocks()
		{
			return blocks;
		}

		internal override QuotaCounts CombinePosteriorAndCollectBlocks(BlockStoragePolicySuite
			 bsps, INodeFile currentINode, Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.FileDiff
			 posterior, INode.BlocksMapUpdateInfo collectedBlocks, IList<INode> removedINodes
			)
		{
			FileWithSnapshotFeature sf = currentINode.GetFileWithSnapshotFeature();
			System.Diagnostics.Debug.Assert(sf != null, "FileWithSnapshotFeature is null");
			return sf.UpdateQuotaAndCollectBlocks(bsps, currentINode, posterior, collectedBlocks
				, removedINodes);
		}

		public override string ToString()
		{
			return base.ToString() + " fileSize=" + fileSize + ", rep=" + (snapshotINode == null
				 ? "?" : snapshotINode.GetFileReplication());
		}

		/// <exception cref="System.IO.IOException"/>
		internal override void Write(DataOutput @out, SnapshotFSImageFormat.ReferenceMap 
			referenceMap)
		{
			WriteSnapshot(@out);
			@out.WriteLong(fileSize);
			// write snapshotINode
			if (snapshotINode != null)
			{
				@out.WriteBoolean(true);
				FSImageSerialization.WriteINodeFileAttributes(snapshotINode, @out);
			}
			else
			{
				@out.WriteBoolean(false);
			}
		}

		internal override QuotaCounts DestroyDiffAndCollectBlocks(BlockStoragePolicySuite
			 bsps, INodeFile currentINode, INode.BlocksMapUpdateInfo collectedBlocks, IList<
			INode> removedINodes)
		{
			return currentINode.GetFileWithSnapshotFeature().UpdateQuotaAndCollectBlocks(bsps
				, currentINode, this, collectedBlocks, removedINodes);
		}

		public virtual void DestroyAndCollectSnapshotBlocks(INode.BlocksMapUpdateInfo collectedBlocks
			)
		{
			if (blocks == null || collectedBlocks == null)
			{
				return;
			}
			foreach (BlockInfoContiguous blk in blocks)
			{
				collectedBlocks.AddDeleteBlock(blk);
			}
			blocks = null;
		}
	}
}
