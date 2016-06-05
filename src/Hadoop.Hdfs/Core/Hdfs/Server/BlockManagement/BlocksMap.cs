using System;
using System.Collections.Generic;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Blockmanagement
{
	/// <summary>This class maintains the map from a block to its metadata.</summary>
	/// <remarks>
	/// This class maintains the map from a block to its metadata.
	/// block's metadata currently includes blockCollection it belongs to and
	/// the datanodes that store the block.
	/// </remarks>
	internal class BlocksMap
	{
		private class StorageIterator : IEnumerator<DatanodeStorageInfo>
		{
			private readonly BlockInfoContiguous blockInfo;

			private int nextIdx = 0;

			internal StorageIterator(BlockInfoContiguous blkInfo)
			{
				this.blockInfo = blkInfo;
			}

			public override bool HasNext()
			{
				return blockInfo != null && nextIdx < blockInfo.GetCapacity() && blockInfo.GetDatanode
					(nextIdx) != null;
			}

			public override DatanodeStorageInfo Next()
			{
				return blockInfo.GetStorageInfo(nextIdx++);
			}

			public override void Remove()
			{
				throw new NotSupportedException("Sorry. can't remove.");
			}
		}

		/// <summary>
		/// Constant
		/// <see cref="Org.Apache.Hadoop.Util.LightWeightGSet{K, E}"/>
		/// capacity.
		/// </summary>
		private readonly int capacity;

		private GSet<Block, BlockInfoContiguous> blocks;

		internal BlocksMap(int capacity)
		{
			// Use 2% of total memory to size the GSet capacity
			this.capacity = capacity;
			this.blocks = new _LightWeightGSet_71(capacity);
		}

		private sealed class _LightWeightGSet_71 : LightWeightGSet<Block, BlockInfoContiguous
			>
		{
			public _LightWeightGSet_71(int baseArg1)
				: base(baseArg1)
			{
			}

			public override IEnumerator<BlockInfoContiguous> GetEnumerator()
			{
				LightWeightGSet.SetIterator iterator = new LightWeightGSet.SetIterator(this);
				/*
				* Not tracking any modifications to set. As this set will be used
				* always under FSNameSystem lock, modifications will not cause any
				* ConcurrentModificationExceptions. But there is a chance of missing
				* newly added elements during iteration.
				*/
				iterator.SetTrackModification(false);
				return iterator;
			}
		}

		internal virtual void Close()
		{
			Clear();
			blocks = null;
		}

		internal virtual void Clear()
		{
			if (blocks != null)
			{
				blocks.Clear();
			}
		}

		internal virtual BlockCollection GetBlockCollection(Block b)
		{
			BlockInfoContiguous info = blocks.Get(b);
			return (info != null) ? info.GetBlockCollection() : null;
		}

		/// <summary>Add block b belonging to the specified block collection to the map.</summary>
		internal virtual BlockInfoContiguous AddBlockCollection(BlockInfoContiguous b, BlockCollection
			 bc)
		{
			BlockInfoContiguous info = blocks.Get(b);
			if (info != b)
			{
				info = b;
				blocks.Put(info);
			}
			info.SetBlockCollection(bc);
			return info;
		}

		/// <summary>
		/// Remove the block from the block map;
		/// remove it from all data-node lists it belongs to;
		/// and remove all data-node locations associated with the block.
		/// </summary>
		internal virtual void RemoveBlock(Block block)
		{
			BlockInfoContiguous blockInfo = blocks.Remove(block);
			if (blockInfo == null)
			{
				return;
			}
			blockInfo.SetBlockCollection(null);
			for (int idx = blockInfo.NumNodes() - 1; idx >= 0; idx--)
			{
				DatanodeDescriptor dn = blockInfo.GetDatanode(idx);
				dn.RemoveBlock(blockInfo);
			}
		}

		// remove from the list and wipe the location
		/// <summary>Returns the block object it it exists in the map.</summary>
		internal virtual BlockInfoContiguous GetStoredBlock(Block b)
		{
			return blocks.Get(b);
		}

		/// <summary>
		/// Searches for the block in the BlocksMap and
		/// returns
		/// <see cref="System.Collections.IEnumerable{T}"/>
		/// of the storages the block belongs to.
		/// </summary>
		internal virtual IEnumerable<DatanodeStorageInfo> GetStorages(Block b)
		{
			return GetStorages(blocks.Get(b));
		}

		/// <summary>
		/// Searches for the block in the BlocksMap and
		/// returns
		/// <see cref="System.Collections.IEnumerable{T}"/>
		/// of the storages the block belongs to
		/// <i>that are of the given
		/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Protocol.DatanodeStorage.State">state</see>
		/// </i>.
		/// </summary>
		/// <param name="state">DatanodeStorage state by which to filter the returned Iterable
		/// 	</param>
		internal virtual IEnumerable<DatanodeStorageInfo> GetStorages(Block b, DatanodeStorage.State
			 state)
		{
			return Iterables.Filter(GetStorages(blocks.Get(b)), new _Predicate_155(state));
		}

		private sealed class _Predicate_155 : Predicate<DatanodeStorageInfo>
		{
			public _Predicate_155(DatanodeStorage.State state)
			{
				this.state = state;
			}

			public bool Apply(DatanodeStorageInfo storage)
			{
				return storage.GetState() == state;
			}

			private readonly DatanodeStorage.State state;
		}

		/// <summary>
		/// For a block that has already been retrieved from the BlocksMap
		/// returns
		/// <see cref="System.Collections.IEnumerable{T}"/>
		/// of the storages the block belongs to.
		/// </summary>
		internal virtual IEnumerable<DatanodeStorageInfo> GetStorages(BlockInfoContiguous
			 storedBlock)
		{
			return new _IEnumerable_168(storedBlock);
		}

		private sealed class _IEnumerable_168 : IEnumerable<DatanodeStorageInfo>
		{
			public _IEnumerable_168(BlockInfoContiguous storedBlock)
			{
				this.storedBlock = storedBlock;
			}

			public override IEnumerator<DatanodeStorageInfo> GetEnumerator()
			{
				return new BlocksMap.StorageIterator(storedBlock);
			}

			private readonly BlockInfoContiguous storedBlock;
		}

		/// <summary>counts number of containing nodes.</summary>
		/// <remarks>counts number of containing nodes. Better than using iterator.</remarks>
		internal virtual int NumNodes(Block b)
		{
			BlockInfoContiguous info = blocks.Get(b);
			return info == null ? 0 : info.NumNodes();
		}

		/// <summary>Remove data-node reference from the block.</summary>
		/// <remarks>
		/// Remove data-node reference from the block.
		/// Remove the block from the block map
		/// only if it does not belong to any file and data-nodes.
		/// </remarks>
		internal virtual bool RemoveNode(Block b, DatanodeDescriptor node)
		{
			BlockInfoContiguous info = blocks.Get(b);
			if (info == null)
			{
				return false;
			}
			// remove block from the data-node list and the node from the block info
			bool removed = node.RemoveBlock(info);
			if (info.GetDatanode(0) == null && info.GetBlockCollection() == null)
			{
				// no datanodes left
				// does not belong to a file
				blocks.Remove(b);
			}
			// remove block from the map
			return removed;
		}

		internal virtual int Size()
		{
			return blocks.Size();
		}

		internal virtual IEnumerable<BlockInfoContiguous> GetBlocks()
		{
			return blocks;
		}

		/// <summary>Get the capacity of the HashMap that stores blocks</summary>
		internal virtual int GetCapacity()
		{
			return capacity;
		}

		/// <summary>Replace a block in the block map by a new block.</summary>
		/// <remarks>
		/// Replace a block in the block map by a new block.
		/// The new block and the old one have the same key.
		/// </remarks>
		/// <param name="newBlock">- block for replacement</param>
		/// <returns>new block</returns>
		internal virtual BlockInfoContiguous ReplaceBlock(BlockInfoContiguous newBlock)
		{
			BlockInfoContiguous currentBlock = blocks.Get(newBlock);
			System.Diagnostics.Debug.Assert(currentBlock != null, "the block if not in blocksMap"
				);
			// replace block in data-node lists
			for (int i = currentBlock.NumNodes() - 1; i >= 0; i--)
			{
				DatanodeDescriptor dn = currentBlock.GetDatanode(i);
				DatanodeStorageInfo storage = currentBlock.FindStorageInfo(dn);
				bool removed = storage.RemoveBlock(currentBlock);
				Preconditions.CheckState(removed, "currentBlock not found.");
				DatanodeStorageInfo.AddBlockResult result = storage.AddBlock(newBlock);
				Preconditions.CheckState(result == DatanodeStorageInfo.AddBlockResult.Added, "newBlock already exists."
					);
			}
			// replace block in the map itself
			blocks.Put(newBlock);
			return newBlock;
		}
	}
}
