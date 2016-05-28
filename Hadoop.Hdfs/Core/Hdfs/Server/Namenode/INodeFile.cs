using System.Collections.Generic;
using System.IO;
using System.Text;
using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot;
using Org.Apache.Hadoop.Hdfs.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>I-node for closed file.</summary>
	public class INodeFile : INodeWithAdditionalFields, INodeFileAttributes, BlockCollection
	{
		/// <summary>The same as valueOf(inode, path, false).</summary>
		/// <exception cref="System.IO.FileNotFoundException"/>
		public static Org.Apache.Hadoop.Hdfs.Server.Namenode.INodeFile ValueOf(INode inode
			, string path)
		{
			return ValueOf(inode, path, false);
		}

		/// <summary>Cast INode to INodeFile.</summary>
		/// <exception cref="System.IO.FileNotFoundException"/>
		public static Org.Apache.Hadoop.Hdfs.Server.Namenode.INodeFile ValueOf(INode inode
			, string path, bool acceptNull)
		{
			if (inode == null)
			{
				if (acceptNull)
				{
					return null;
				}
				else
				{
					throw new FileNotFoundException("File does not exist: " + path);
				}
			}
			if (!inode.IsFile())
			{
				throw new FileNotFoundException("Path is not a file: " + path);
			}
			return inode.AsFile();
		}

		/// <summary>
		/// Bit format:
		/// [4-bit storagePolicyID][12-bit replication][48-bit preferredBlockSize]
		/// </summary>
		[System.Serializable]
		internal sealed class HeaderFormat
		{
			public static readonly INodeFile.HeaderFormat PreferredBlockSize = new INodeFile.HeaderFormat
				(null, 48, 1);

			public static readonly INodeFile.HeaderFormat Replication = new INodeFile.HeaderFormat
				(INodeFile.HeaderFormat.PreferredBlockSize.Bits, 12, 1);

			public static readonly INodeFile.HeaderFormat StoragePolicyId = new INodeFile.HeaderFormat
				(INodeFile.HeaderFormat.Replication.Bits, BlockStoragePolicySuite.IdBitLength, 0
				);

			private readonly LongBitFormat Bits;

			private HeaderFormat(LongBitFormat previous, int length, long min)
			{
				INodeFile.HeaderFormat.Bits = new LongBitFormat(Name(), previous, length, min);
			}

			internal static short GetReplication(long header)
			{
				return (short)INodeFile.HeaderFormat.Replication.Bits.Retrieve(header);
			}

			internal static long GetPreferredBlockSize(long header)
			{
				return INodeFile.HeaderFormat.PreferredBlockSize.Bits.Retrieve(header);
			}

			internal static byte GetStoragePolicyID(long header)
			{
				return unchecked((byte)INodeFile.HeaderFormat.StoragePolicyId.Bits.Retrieve(header
					));
			}

			internal static long ToLong(long preferredBlockSize, short replication, byte storagePolicyID
				)
			{
				long h = 0;
				if (preferredBlockSize == 0)
				{
					preferredBlockSize = INodeFile.HeaderFormat.PreferredBlockSize.Bits.GetMin();
				}
				h = INodeFile.HeaderFormat.PreferredBlockSize.Bits.Combine(preferredBlockSize, h);
				h = INodeFile.HeaderFormat.Replication.Bits.Combine(replication, h);
				h = INodeFile.HeaderFormat.StoragePolicyId.Bits.Combine(storagePolicyID, h);
				return h;
			}
		}

		private long header = 0L;

		private BlockInfoContiguous[] blocks;

		internal INodeFile(long id, byte[] name, PermissionStatus permissions, long mtime
			, long atime, BlockInfoContiguous[] blklist, short replication, long preferredBlockSize
			)
			: this(id, name, permissions, mtime, atime, blklist, replication, preferredBlockSize
				, unchecked((byte)0))
		{
		}

		internal INodeFile(long id, byte[] name, PermissionStatus permissions, long mtime
			, long atime, BlockInfoContiguous[] blklist, short replication, long preferredBlockSize
			, byte storagePolicyID)
			: base(id, name, permissions, mtime, atime)
		{
			header = INodeFile.HeaderFormat.ToLong(preferredBlockSize, replication, storagePolicyID
				);
			this.blocks = blklist;
		}

		public INodeFile(INodeFile that)
			: base(that)
		{
			this.header = that.header;
			this.blocks = that.blocks;
			this.features = that.features;
		}

		public INodeFile(INodeFile that, FileDiffList diffs)
			: this(that)
		{
			Preconditions.CheckArgument(!that.IsWithSnapshot());
			this.AddSnapshotFeature(diffs);
		}

		/// <returns>true unconditionally.</returns>
		public sealed override bool IsFile()
		{
			return true;
		}

		/// <returns>this object.</returns>
		public sealed override INodeFile AsFile()
		{
			return this;
		}

		public virtual bool MetadataEquals(INodeFileAttributes other)
		{
			return other != null && GetHeaderLong() == other.GetHeaderLong() && GetPermissionLong
				() == other.GetPermissionLong() && GetAclFeature() == other.GetAclFeature() && GetXAttrFeature
				() == other.GetXAttrFeature();
		}

		/* Start of Under-Construction Feature */
		/// <summary>
		/// If the inode contains a
		/// <see cref="FileUnderConstructionFeature"/>
		/// , return it;
		/// otherwise, return null.
		/// </summary>
		public FileUnderConstructionFeature GetFileUnderConstructionFeature()
		{
			return GetFeature(typeof(FileUnderConstructionFeature));
		}

		/// <summary>Is this file under construction?</summary>
		public virtual bool IsUnderConstruction()
		{
			// BlockCollection
			return GetFileUnderConstructionFeature() != null;
		}

		internal virtual INodeFile ToUnderConstruction(string clientName, string clientMachine
			)
		{
			Preconditions.CheckState(!IsUnderConstruction(), "file is already under construction"
				);
			FileUnderConstructionFeature uc = new FileUnderConstructionFeature(clientName, clientMachine
				);
			AddFeature(uc);
			return this;
		}

		/// <summary>
		/// Convert the file to a complete file, i.e., to remove the Under-Construction
		/// feature.
		/// </summary>
		public virtual INodeFile ToCompleteFile(long mtime)
		{
			Preconditions.CheckState(IsUnderConstruction(), "file is no longer under construction"
				);
			FileUnderConstructionFeature uc = GetFileUnderConstructionFeature();
			if (uc != null)
			{
				AssertAllBlocksComplete();
				RemoveFeature(uc);
				this.SetModificationTime(mtime);
			}
			return this;
		}

		/// <summary>Assert all blocks are complete.</summary>
		private void AssertAllBlocksComplete()
		{
			if (blocks == null)
			{
				return;
			}
			for (int i = 0; i < blocks.Length; i++)
			{
				Preconditions.CheckState(blocks[i].IsComplete(), "Failed to finalize" + " %s %s since blocks[%s] is non-complete, where blocks=%s."
					, GetType().Name, this, i, Arrays.AsList(blocks));
			}
		}

		public virtual void SetBlock(int index, BlockInfoContiguous blk)
		{
			// BlockCollection
			this.blocks[index] = blk;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual BlockInfoContiguousUnderConstruction SetLastBlock(BlockInfoContiguous
			 lastBlock, DatanodeStorageInfo[] locations)
		{
			// BlockCollection, the file should be under construction
			Preconditions.CheckState(IsUnderConstruction(), "file is no longer under construction"
				);
			if (NumBlocks() == 0)
			{
				throw new IOException("Failed to set last block: File is empty.");
			}
			BlockInfoContiguousUnderConstruction ucBlock = lastBlock.ConvertToBlockUnderConstruction
				(HdfsServerConstants.BlockUCState.UnderConstruction, locations);
			SetBlock(NumBlocks() - 1, ucBlock);
			return ucBlock;
		}

		/// <summary>Remove a block from the block list.</summary>
		/// <remarks>
		/// Remove a block from the block list. This block should be
		/// the last one on the list.
		/// </remarks>
		internal virtual bool RemoveLastBlock(Block oldblock)
		{
			Preconditions.CheckState(IsUnderConstruction(), "file is no longer under construction"
				);
			if (blocks == null || blocks.Length == 0)
			{
				return false;
			}
			int size_1 = blocks.Length - 1;
			if (!blocks[size_1].Equals(oldblock))
			{
				return false;
			}
			//copy to a new list
			BlockInfoContiguous[] newlist = new BlockInfoContiguous[size_1];
			System.Array.Copy(blocks, 0, newlist, 0, size_1);
			SetBlocks(newlist);
			return true;
		}

		/* End of Under-Construction Feature */
		/* Start of Snapshot Feature */
		public virtual FileWithSnapshotFeature AddSnapshotFeature(FileDiffList diffs)
		{
			Preconditions.CheckState(!IsWithSnapshot(), "File is already with snapshot");
			FileWithSnapshotFeature sf = new FileWithSnapshotFeature(diffs);
			this.AddFeature(sf);
			return sf;
		}

		/// <summary>
		/// If feature list contains a
		/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.FileWithSnapshotFeature
		/// 	"/>
		/// , return it;
		/// otherwise, return null.
		/// </summary>
		public FileWithSnapshotFeature GetFileWithSnapshotFeature()
		{
			return GetFeature(typeof(FileWithSnapshotFeature));
		}

		/// <summary>Is this file has the snapshot feature?</summary>
		public bool IsWithSnapshot()
		{
			return GetFileWithSnapshotFeature() != null;
		}

		public override string ToDetailString()
		{
			FileWithSnapshotFeature sf = this.GetFileWithSnapshotFeature();
			return base.ToDetailString() + (sf == null ? string.Empty : sf.GetDetailedString(
				));
		}

		public override INodeAttributes GetSnapshotINode(int snapshotId)
		{
			FileWithSnapshotFeature sf = this.GetFileWithSnapshotFeature();
			if (sf != null)
			{
				return sf.GetDiffs().GetSnapshotINode(snapshotId, this);
			}
			else
			{
				return this;
			}
		}

		internal override void RecordModification(int latestSnapshotId)
		{
			RecordModification(latestSnapshotId, false);
		}

		public virtual void RecordModification(int latestSnapshotId, bool withBlocks)
		{
			if (IsInLatestSnapshot(latestSnapshotId) && !ShouldRecordInSrcSnapshot(latestSnapshotId
				))
			{
				// the file is in snapshot, create a snapshot feature if it does not have
				FileWithSnapshotFeature sf = this.GetFileWithSnapshotFeature();
				if (sf == null)
				{
					sf = AddSnapshotFeature(null);
				}
				// record self in the diff list if necessary
				sf.GetDiffs().SaveSelf2Snapshot(latestSnapshotId, this, null, withBlocks);
			}
		}

		public virtual FileDiffList GetDiffs()
		{
			FileWithSnapshotFeature sf = this.GetFileWithSnapshotFeature();
			if (sf != null)
			{
				return sf.GetDiffs();
			}
			return null;
		}

		/* End of Snapshot Feature */
		/// <returns>the replication factor of the file.</returns>
		public short GetFileReplication(int snapshot)
		{
			if (snapshot != Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.CurrentStateId)
			{
				return ((INodeFileAttributes)GetSnapshotINode(snapshot)).GetFileReplication();
			}
			return INodeFile.HeaderFormat.GetReplication(header);
		}

		/// <summary>The same as getFileReplication(null).</summary>
		public short GetFileReplication()
		{
			// INodeFileAttributes
			return GetFileReplication(Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
				.CurrentStateId);
		}

		public virtual short GetBlockReplication()
		{
			// BlockCollection
			short max = GetFileReplication(Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
				.CurrentStateId);
			FileWithSnapshotFeature sf = this.GetFileWithSnapshotFeature();
			if (sf != null)
			{
				short maxInSnapshot = sf.GetMaxBlockRepInDiffs();
				if (sf.IsCurrentFileDeleted())
				{
					return maxInSnapshot;
				}
				max = maxInSnapshot > max ? maxInSnapshot : max;
			}
			return max;
		}

		/// <summary>Set the replication factor of this file.</summary>
		public void SetFileReplication(short replication)
		{
			header = INodeFile.HeaderFormat.Replication.Bits.Combine(replication, header);
		}

		/// <summary>Set the replication factor of this file.</summary>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.QuotaExceededException"/>
		public INodeFile SetFileReplication(short replication, int latestSnapshotId)
		{
			RecordModification(latestSnapshotId);
			SetFileReplication(replication);
			return this;
		}

		/// <returns>preferred block size (in bytes) of the file.</returns>
		public virtual long GetPreferredBlockSize()
		{
			return INodeFile.HeaderFormat.GetPreferredBlockSize(header);
		}

		public override byte GetLocalStoragePolicyID()
		{
			return INodeFile.HeaderFormat.GetStoragePolicyID(header);
		}

		public override byte GetStoragePolicyID()
		{
			byte id = GetLocalStoragePolicyID();
			if (id == BlockStoragePolicySuite.IdUnspecified)
			{
				return this.GetParent() != null ? this.GetParent().GetStoragePolicyID() : id;
			}
			return id;
		}

		private void SetStoragePolicyID(byte storagePolicyId)
		{
			header = INodeFile.HeaderFormat.StoragePolicyId.Bits.Combine(storagePolicyId, header
				);
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.QuotaExceededException"/>
		public void SetStoragePolicyID(byte storagePolicyId, int latestSnapshotId)
		{
			RecordModification(latestSnapshotId);
			SetStoragePolicyID(storagePolicyId);
		}

		public virtual long GetHeaderLong()
		{
			return header;
		}

		/// <returns>the storagespace required for a full block.</returns>
		internal long GetPreferredBlockStoragespace()
		{
			return GetPreferredBlockSize() * GetBlockReplication();
		}

		/// <returns>the blocks of the file.</returns>
		public virtual BlockInfoContiguous[] GetBlocks()
		{
			return this.blocks;
		}

		/// <returns>blocks of the file corresponding to the snapshot.</returns>
		public virtual BlockInfoContiguous[] GetBlocks(int snapshot)
		{
			if (snapshot == Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.CurrentStateId
				 || GetDiffs() == null)
			{
				return GetBlocks();
			}
			FileDiff diff = GetDiffs().GetDiffById(snapshot);
			BlockInfoContiguous[] snapshotBlocks = diff == null ? GetBlocks() : diff.GetBlocks
				();
			if (snapshotBlocks != null)
			{
				return snapshotBlocks;
			}
			// Blocks are not in the current snapshot
			// Find next snapshot with blocks present or return current file blocks
			snapshotBlocks = GetDiffs().FindLaterSnapshotBlocks(snapshot);
			return (snapshotBlocks == null) ? GetBlocks() : snapshotBlocks;
		}

		internal virtual void UpdateBlockCollection()
		{
			if (blocks != null)
			{
				foreach (BlockInfoContiguous b in blocks)
				{
					b.SetBlockCollection(this);
				}
			}
		}

		/// <summary>append array of blocks to this.blocks</summary>
		internal virtual void ConcatBlocks(INodeFile[] inodes)
		{
			int size = this.blocks.Length;
			int totalAddedBlocks = 0;
			foreach (INodeFile f in inodes)
			{
				totalAddedBlocks += f.blocks.Length;
			}
			BlockInfoContiguous[] newlist = new BlockInfoContiguous[size + totalAddedBlocks];
			System.Array.Copy(this.blocks, 0, newlist, 0, size);
			foreach (INodeFile @in in inodes)
			{
				System.Array.Copy(@in.blocks, 0, newlist, size, @in.blocks.Length);
				size += @in.blocks.Length;
			}
			SetBlocks(newlist);
			UpdateBlockCollection();
		}

		/// <summary>add a block to the block list</summary>
		internal virtual void AddBlock(BlockInfoContiguous newblock)
		{
			if (this.blocks == null)
			{
				this.SetBlocks(new BlockInfoContiguous[] { newblock });
			}
			else
			{
				int size = this.blocks.Length;
				BlockInfoContiguous[] newlist = new BlockInfoContiguous[size + 1];
				System.Array.Copy(this.blocks, 0, newlist, 0, size);
				newlist[size] = newblock;
				this.SetBlocks(newlist);
			}
		}

		/// <summary>Set the blocks.</summary>
		public virtual void SetBlocks(BlockInfoContiguous[] blocks)
		{
			this.blocks = blocks;
		}

		public override QuotaCounts CleanSubtree(BlockStoragePolicySuite bsps, int snapshot
			, int priorSnapshotId, INode.BlocksMapUpdateInfo collectedBlocks, IList<INode> removedINodes
			)
		{
			FileWithSnapshotFeature sf = GetFileWithSnapshotFeature();
			if (sf != null)
			{
				return sf.CleanFile(bsps, this, snapshot, priorSnapshotId, collectedBlocks, removedINodes
					);
			}
			QuotaCounts counts = new QuotaCounts.Builder().Build();
			if (snapshot == Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.CurrentStateId)
			{
				if (priorSnapshotId == Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.NoSnapshotId)
				{
					// this only happens when deleting the current file and the file is not
					// in any snapshot
					ComputeQuotaUsage(bsps, counts, false);
					DestroyAndCollectBlocks(bsps, collectedBlocks, removedINodes);
				}
				else
				{
					// when deleting the current file and the file is in snapshot, we should
					// clean the 0-sized block if the file is UC
					FileUnderConstructionFeature uc = GetFileUnderConstructionFeature();
					if (uc != null)
					{
						uc.CleanZeroSizeBlock(this, collectedBlocks);
					}
				}
			}
			return counts;
		}

		public override void DestroyAndCollectBlocks(BlockStoragePolicySuite bsps, INode.BlocksMapUpdateInfo
			 collectedBlocks, IList<INode> removedINodes)
		{
			if (blocks != null && collectedBlocks != null)
			{
				foreach (BlockInfoContiguous blk in blocks)
				{
					collectedBlocks.AddDeleteBlock(blk);
					blk.SetBlockCollection(null);
				}
			}
			SetBlocks(null);
			if (GetAclFeature() != null)
			{
				AclStorage.RemoveAclFeature(GetAclFeature());
			}
			Clear();
			removedINodes.AddItem(this);
			FileWithSnapshotFeature sf = GetFileWithSnapshotFeature();
			if (sf != null)
			{
				sf.GetDiffs().DestroyAndCollectSnapshotBlocks(collectedBlocks);
				sf.ClearDiffs();
			}
		}

		public virtual string GetName()
		{
			// Get the full path name of this inode.
			return GetFullPathName();
		}

		// This is the only place that needs to use the BlockStoragePolicySuite to
		// derive the intended storage type usage for quota by storage type
		public sealed override QuotaCounts ComputeQuotaUsage(BlockStoragePolicySuite bsps
			, byte blockStoragePolicyId, QuotaCounts counts, bool useCache, int lastSnapshotId
			)
		{
			long nsDelta = 1;
			long ssDeltaNoReplication;
			short replication;
			FileWithSnapshotFeature sf = GetFileWithSnapshotFeature();
			if (sf != null)
			{
				FileDiffList fileDiffList = sf.GetDiffs();
				int last = fileDiffList.GetLastSnapshotId();
				if (lastSnapshotId == Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.CurrentStateId
					 || last == Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.CurrentStateId)
				{
					ssDeltaNoReplication = StoragespaceConsumedNoReplication();
					replication = GetBlockReplication();
				}
				else
				{
					if (last < lastSnapshotId)
					{
						ssDeltaNoReplication = ComputeFileSize(true, false);
						replication = GetFileReplication();
					}
					else
					{
						int sid = fileDiffList.GetSnapshotById(lastSnapshotId);
						ssDeltaNoReplication = StoragespaceConsumedNoReplication(sid);
						replication = GetReplication(sid);
					}
				}
			}
			else
			{
				ssDeltaNoReplication = StoragespaceConsumedNoReplication();
				replication = GetBlockReplication();
			}
			counts.AddNameSpace(nsDelta);
			counts.AddStorageSpace(ssDeltaNoReplication * replication);
			if (blockStoragePolicyId != BlockStoragePolicySuite.IdUnspecified)
			{
				BlockStoragePolicy bsp = bsps.GetPolicy(blockStoragePolicyId);
				IList<StorageType> storageTypes = bsp.ChooseStorageTypes(replication);
				foreach (StorageType t in storageTypes)
				{
					if (!t.SupportTypeQuota())
					{
						continue;
					}
					counts.AddTypeSpace(t, ssDeltaNoReplication);
				}
			}
			return counts;
		}

		public sealed override ContentSummaryComputationContext ComputeContentSummary(ContentSummaryComputationContext
			 summary)
		{
			ContentCounts counts = summary.GetCounts();
			FileWithSnapshotFeature sf = GetFileWithSnapshotFeature();
			long fileLen = 0;
			if (sf == null)
			{
				fileLen = ComputeFileSize();
				counts.AddContent(Content.File, 1);
			}
			else
			{
				FileDiffList diffs = sf.GetDiffs();
				int n = diffs.AsList().Count;
				counts.AddContent(Content.File, n);
				if (n > 0 && sf.IsCurrentFileDeleted())
				{
					fileLen = diffs.GetLast().GetFileSize();
				}
				else
				{
					fileLen = ComputeFileSize();
				}
			}
			counts.AddContent(Content.Length, fileLen);
			counts.AddContent(Content.Diskspace, StoragespaceConsumed());
			if (GetStoragePolicyID() != BlockStoragePolicySuite.IdUnspecified)
			{
				BlockStoragePolicy bsp = summary.GetBlockStoragePolicySuite().GetPolicy(GetStoragePolicyID
					());
				IList<StorageType> storageTypes = bsp.ChooseStorageTypes(GetFileReplication());
				foreach (StorageType t in storageTypes)
				{
					if (!t.SupportTypeQuota())
					{
						continue;
					}
					counts.AddTypeSpace(t, fileLen);
				}
			}
			return summary;
		}

		/// <summary>The same as computeFileSize(null).</summary>
		public long ComputeFileSize()
		{
			return ComputeFileSize(Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.CurrentStateId
				);
		}

		/// <summary>
		/// Compute file size of the current file if the given snapshot is null;
		/// otherwise, get the file size from the given snapshot.
		/// </summary>
		public long ComputeFileSize(int snapshotId)
		{
			FileWithSnapshotFeature sf = this.GetFileWithSnapshotFeature();
			if (snapshotId != Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.CurrentStateId
				 && sf != null)
			{
				FileDiff d = sf.GetDiffs().GetDiffById(snapshotId);
				if (d != null)
				{
					return d.GetFileSize();
				}
			}
			return ComputeFileSize(true, false);
		}

		/// <summary>
		/// Compute file size of the current file size
		/// but not including the last block if it is under construction.
		/// </summary>
		public long ComputeFileSizeNotIncludingLastUcBlock()
		{
			return ComputeFileSize(false, false);
		}

		/// <summary>Compute file size of the current file.</summary>
		/// <param name="includesLastUcBlock">If the last block is under construction, should it be included?
		/// 	</param>
		/// <param name="usePreferredBlockSize4LastUcBlock">
		/// If the last block is under construction, should we use actual
		/// block size or preferred block size?
		/// Note that usePreferredBlockSize4LastUcBlock is ignored
		/// if includesLastUcBlock == false.
		/// </param>
		/// <returns>file size</returns>
		public long ComputeFileSize(bool includesLastUcBlock, bool usePreferredBlockSize4LastUcBlock
			)
		{
			if (blocks == null || blocks.Length == 0)
			{
				return 0;
			}
			int last = blocks.Length - 1;
			//check if the last block is BlockInfoUnderConstruction
			long size = blocks[last].GetNumBytes();
			if (blocks[last] is BlockInfoContiguousUnderConstruction)
			{
				if (!includesLastUcBlock)
				{
					size = 0;
				}
				else
				{
					if (usePreferredBlockSize4LastUcBlock)
					{
						size = GetPreferredBlockSize();
					}
				}
			}
			//sum other blocks
			for (int i = 0; i < last; i++)
			{
				size += blocks[i].GetNumBytes();
			}
			return size;
		}

		/// <summary>
		/// Compute size consumed by all blocks of the current file,
		/// including blocks in its snapshots.
		/// </summary>
		/// <remarks>
		/// Compute size consumed by all blocks of the current file,
		/// including blocks in its snapshots.
		/// Use preferred block size for the last block if it is under construction.
		/// </remarks>
		public long StoragespaceConsumed()
		{
			return StoragespaceConsumedNoReplication() * GetBlockReplication();
		}

		public long StoragespaceConsumedNoReplication()
		{
			FileWithSnapshotFeature sf = GetFileWithSnapshotFeature();
			if (sf == null)
			{
				return ComputeFileSize(true, true);
			}
			// Collect all distinct blocks
			long size = 0;
			ICollection<Block> allBlocks = new HashSet<Block>(Arrays.AsList(GetBlocks()));
			IList<FileDiff> diffs = sf.GetDiffs().AsList();
			foreach (FileDiff diff in diffs)
			{
				BlockInfoContiguous[] diffBlocks = diff.GetBlocks();
				if (diffBlocks != null)
				{
					Sharpen.Collections.AddAll(allBlocks, Arrays.AsList(diffBlocks));
				}
			}
			foreach (Block block in allBlocks)
			{
				size += block.GetNumBytes();
			}
			// check if the last block is under construction
			BlockInfoContiguous lastBlock = GetLastBlock();
			if (lastBlock != null && lastBlock is BlockInfoContiguousUnderConstruction)
			{
				size += GetPreferredBlockSize() - lastBlock.GetNumBytes();
			}
			return size;
		}

		public long StoragespaceConsumed(int lastSnapshotId)
		{
			if (lastSnapshotId != Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.CurrentStateId)
			{
				return ComputeFileSize(lastSnapshotId) * GetFileReplication(lastSnapshotId);
			}
			else
			{
				return StoragespaceConsumed();
			}
		}

		public short GetReplication(int lastSnapshotId)
		{
			if (lastSnapshotId != Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.CurrentStateId)
			{
				return GetFileReplication(lastSnapshotId);
			}
			else
			{
				return GetBlockReplication();
			}
		}

		public long StoragespaceConsumedNoReplication(int lastSnapshotId)
		{
			if (lastSnapshotId != Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.CurrentStateId)
			{
				return ComputeFileSize(lastSnapshotId);
			}
			else
			{
				return StoragespaceConsumedNoReplication();
			}
		}

		/// <summary>Return the penultimate allocated block for this file.</summary>
		internal virtual BlockInfoContiguous GetPenultimateBlock()
		{
			if (blocks == null || blocks.Length <= 1)
			{
				return null;
			}
			return blocks[blocks.Length - 2];
		}

		public virtual BlockInfoContiguous GetLastBlock()
		{
			return blocks == null || blocks.Length == 0 ? null : blocks[blocks.Length - 1];
		}

		public virtual int NumBlocks()
		{
			return blocks == null ? 0 : blocks.Length;
		}

		[VisibleForTesting]
		public override void DumpTreeRecursively(PrintWriter @out, StringBuilder prefix, 
			int snapshotId)
		{
			base.DumpTreeRecursively(@out, prefix, snapshotId);
			@out.Write(", fileSize=" + ComputeFileSize(snapshotId));
			// only compare the first block
			@out.Write(", blocks=");
			@out.Write(blocks == null || blocks.Length == 0 ? null : blocks[0]);
			@out.WriteLine();
		}

		/// <summary>Remove full blocks at the end file up to newLength</summary>
		/// <returns>sum of sizes of the remained blocks</returns>
		public virtual long CollectBlocksBeyondMax(long max, INode.BlocksMapUpdateInfo collectedBlocks
			)
		{
			BlockInfoContiguous[] oldBlocks = GetBlocks();
			if (oldBlocks == null)
			{
				return 0;
			}
			// find the minimum n such that the size of the first n blocks > max
			int n = 0;
			long size = 0;
			for (; n < oldBlocks.Length && max > size; n++)
			{
				size += oldBlocks[n].GetNumBytes();
			}
			if (n >= oldBlocks.Length)
			{
				return size;
			}
			// starting from block n, the data is beyond max.
			// resize the array.
			TruncateBlocksTo(n);
			// collect the blocks beyond max
			if (collectedBlocks != null)
			{
				for (; n < oldBlocks.Length; n++)
				{
					collectedBlocks.AddDeleteBlock(oldBlocks[n]);
				}
			}
			return size;
		}

		/// <summary>compute the quota usage change for a truncate op</summary>
		/// <param name="newLength">the length for truncation</param>
		/// <returns>the quota usage delta (not considering replication factor)</returns>
		internal virtual long ComputeQuotaDeltaForTruncate(long newLength)
		{
			BlockInfoContiguous[] blocks = GetBlocks();
			if (blocks == null || blocks.Length == 0)
			{
				return 0;
			}
			int n = 0;
			long size = 0;
			for (; n < blocks.Length && newLength > size; n++)
			{
				size += blocks[n].GetNumBytes();
			}
			bool onBoundary = size == newLength;
			long truncateSize = 0;
			for (int i = (onBoundary ? n : n - 1); i < blocks.Length; i++)
			{
				truncateSize += blocks[i].GetNumBytes();
			}
			FileWithSnapshotFeature sf = GetFileWithSnapshotFeature();
			if (sf != null)
			{
				FileDiff diff = sf.GetDiffs().GetLast();
				BlockInfoContiguous[] sblocks = diff != null ? diff.GetBlocks() : null;
				if (sblocks != null)
				{
					for (int i_1 = (onBoundary ? n : n - 1); i_1 < blocks.Length && i_1 < sblocks.Length
						 && blocks[i_1].Equals(sblocks[i_1]); i_1++)
					{
						truncateSize -= blocks[i_1].GetNumBytes();
					}
				}
			}
			return onBoundary ? -truncateSize : (GetPreferredBlockSize() - truncateSize);
		}

		internal virtual void TruncateBlocksTo(int n)
		{
			BlockInfoContiguous[] newBlocks;
			if (n == 0)
			{
				newBlocks = BlockInfoContiguous.EmptyArray;
			}
			else
			{
				newBlocks = new BlockInfoContiguous[n];
				System.Array.Copy(GetBlocks(), 0, newBlocks, 0, n);
			}
			// set new blocks
			SetBlocks(newBlocks);
		}

		public virtual void CollectBlocksBeyondSnapshot(BlockInfoContiguous[] snapshotBlocks
			, INode.BlocksMapUpdateInfo collectedBlocks)
		{
			BlockInfoContiguous[] oldBlocks = GetBlocks();
			if (snapshotBlocks == null || oldBlocks == null)
			{
				return;
			}
			// Skip blocks in common between the file and the snapshot
			int n = 0;
			while (n < oldBlocks.Length && n < snapshotBlocks.Length && oldBlocks[n] == snapshotBlocks
				[n])
			{
				n++;
			}
			TruncateBlocksTo(n);
			// Collect the remaining blocks of the file
			while (n < oldBlocks.Length)
			{
				collectedBlocks.AddDeleteBlock(oldBlocks[n++]);
			}
		}

		/// <summary>Exclude blocks collected for deletion that belong to a snapshot.</summary>
		internal virtual void ExcludeSnapshotBlocks(int snapshotId, INode.BlocksMapUpdateInfo
			 collectedBlocks)
		{
			if (collectedBlocks == null || collectedBlocks.GetToDeleteList().IsEmpty())
			{
				return;
			}
			FileWithSnapshotFeature sf = GetFileWithSnapshotFeature();
			if (sf == null)
			{
				return;
			}
			BlockInfoContiguous[] snapshotBlocks = GetDiffs().FindEarlierSnapshotBlocks(snapshotId
				);
			if (snapshotBlocks == null)
			{
				return;
			}
			IList<Block> toDelete = collectedBlocks.GetToDeleteList();
			foreach (Block blk in snapshotBlocks)
			{
				if (toDelete.Contains(blk))
				{
					collectedBlocks.RemoveDeleteBlock(blk);
				}
			}
		}

		/// <returns>true if the block is contained in a snapshot or false otherwise.</returns>
		internal virtual bool IsBlockInLatestSnapshot(BlockInfoContiguous block)
		{
			FileWithSnapshotFeature sf = this.GetFileWithSnapshotFeature();
			if (sf == null || sf.GetDiffs() == null)
			{
				return false;
			}
			BlockInfoContiguous[] snapshotBlocks = GetDiffs().FindEarlierSnapshotBlocks(GetDiffs
				().GetLastSnapshotId());
			return snapshotBlocks != null && Arrays.AsList(snapshotBlocks).Contains(block);
		}
	}
}
