using System;
using System.Collections.Generic;
using Com.Google.Common.Annotations;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Blockmanagement
{
	/// <summary>A Datanode has one or more storages.</summary>
	/// <remarks>
	/// A Datanode has one or more storages. A storage in the Datanode is represented
	/// by this class.
	/// </remarks>
	public class DatanodeStorageInfo
	{
		public static readonly Org.Apache.Hadoop.Hdfs.Server.Blockmanagement.DatanodeStorageInfo
			[] EmptyArray = new Org.Apache.Hadoop.Hdfs.Server.Blockmanagement.DatanodeStorageInfo
			[] {  };

		public static DatanodeInfo[] ToDatanodeInfos(Org.Apache.Hadoop.Hdfs.Server.Blockmanagement.DatanodeStorageInfo
			[] storages)
		{
			return ToDatanodeInfos(Arrays.AsList(storages));
		}

		internal static DatanodeInfo[] ToDatanodeInfos(IList<Org.Apache.Hadoop.Hdfs.Server.Blockmanagement.DatanodeStorageInfo
			> storages)
		{
			DatanodeInfo[] datanodes = new DatanodeInfo[storages.Count];
			for (int i = 0; i < storages.Count; i++)
			{
				datanodes[i] = storages[i].GetDatanodeDescriptor();
			}
			return datanodes;
		}

		internal static DatanodeDescriptor[] ToDatanodeDescriptors(Org.Apache.Hadoop.Hdfs.Server.Blockmanagement.DatanodeStorageInfo
			[] storages)
		{
			DatanodeDescriptor[] datanodes = new DatanodeDescriptor[storages.Length];
			for (int i = 0; i < storages.Length; ++i)
			{
				datanodes[i] = storages[i].GetDatanodeDescriptor();
			}
			return datanodes;
		}

		public static string[] ToStorageIDs(Org.Apache.Hadoop.Hdfs.Server.Blockmanagement.DatanodeStorageInfo
			[] storages)
		{
			string[] storageIDs = new string[storages.Length];
			for (int i = 0; i < storageIDs.Length; i++)
			{
				storageIDs[i] = storages[i].GetStorageID();
			}
			return storageIDs;
		}

		public static StorageType[] ToStorageTypes(Org.Apache.Hadoop.Hdfs.Server.Blockmanagement.DatanodeStorageInfo
			[] storages)
		{
			StorageType[] storageTypes = new StorageType[storages.Length];
			for (int i = 0; i < storageTypes.Length; i++)
			{
				storageTypes[i] = storages[i].GetStorageType();
			}
			return storageTypes;
		}

		public virtual void UpdateFromStorage(DatanodeStorage storage)
		{
			state = storage.GetState();
			storageType = storage.GetStorageType();
		}

		/// <summary>Iterates over the list of blocks belonging to the data-node.</summary>
		internal class BlockIterator : IEnumerator<BlockInfoContiguous>
		{
			private BlockInfoContiguous current;

			internal BlockIterator(DatanodeStorageInfo _enclosing, BlockInfoContiguous head)
			{
				this._enclosing = _enclosing;
				this.current = head;
			}

			public override bool HasNext()
			{
				return this.current != null;
			}

			public override BlockInfoContiguous Next()
			{
				BlockInfoContiguous res = this.current;
				this.current = this.current.GetNext(this.current.FindStorageInfo(this._enclosing)
					);
				return res;
			}

			public override void Remove()
			{
				throw new NotSupportedException("Sorry. can't remove.");
			}

			private readonly DatanodeStorageInfo _enclosing;
		}

		private readonly DatanodeDescriptor dn;

		private readonly string storageID;

		private StorageType storageType;

		private DatanodeStorage.State state;

		private long capacity;

		private long dfsUsed;

		private volatile long remaining;

		private long blockPoolUsed;

		private volatile BlockInfoContiguous blockList = null;

		private int numBlocks = 0;

		private long lastBlockReportId = 0;

		/// <summary>The number of block reports received</summary>
		private int blockReportCount = 0;

		/// <summary>
		/// Set to false on any NN failover, and reset to true
		/// whenever a block report is received.
		/// </summary>
		private bool heartbeatedSinceFailover = false;

		/// <summary>
		/// At startup or at failover, the storages in the cluster may have pending
		/// block deletions from a previous incarnation of the NameNode.
		/// </summary>
		/// <remarks>
		/// At startup or at failover, the storages in the cluster may have pending
		/// block deletions from a previous incarnation of the NameNode. The block
		/// contents are considered as stale until a block report is received. When a
		/// storage is considered as stale, the replicas on it are also considered as
		/// stale. If any block has at least one stale replica, then no invalidations
		/// will be processed for this block. See HDFS-1972.
		/// </remarks>
		private bool blockContentsStale = true;

		internal DatanodeStorageInfo(DatanodeDescriptor dn, DatanodeStorage s)
		{
			// The ID of the last full block report which updated this storage.
			this.dn = dn;
			this.storageID = s.GetStorageID();
			this.storageType = s.GetStorageType();
			this.state = s.GetState();
		}

		internal virtual int GetBlockReportCount()
		{
			return blockReportCount;
		}

		internal virtual void SetBlockReportCount(int blockReportCount)
		{
			this.blockReportCount = blockReportCount;
		}

		internal virtual bool AreBlockContentsStale()
		{
			return blockContentsStale;
		}

		internal virtual void MarkStaleAfterFailover()
		{
			heartbeatedSinceFailover = false;
			blockContentsStale = true;
		}

		internal virtual void ReceivedHeartbeat(StorageReport report)
		{
			UpdateState(report);
			heartbeatedSinceFailover = true;
		}

		internal virtual void ReceivedBlockReport()
		{
			if (heartbeatedSinceFailover)
			{
				blockContentsStale = false;
			}
			blockReportCount++;
		}

		[VisibleForTesting]
		public virtual void SetUtilizationForTesting(long capacity, long dfsUsed, long remaining
			, long blockPoolUsed)
		{
			this.capacity = capacity;
			this.dfsUsed = dfsUsed;
			this.remaining = remaining;
			this.blockPoolUsed = blockPoolUsed;
		}

		internal virtual long GetLastBlockReportId()
		{
			return lastBlockReportId;
		}

		internal virtual void SetLastBlockReportId(long lastBlockReportId)
		{
			this.lastBlockReportId = lastBlockReportId;
		}

		internal virtual DatanodeStorage.State GetState()
		{
			return this.state;
		}

		internal virtual void SetState(DatanodeStorage.State state)
		{
			this.state = state;
		}

		internal virtual bool AreBlocksOnFailedStorage()
		{
			return GetState() == DatanodeStorage.State.Failed && numBlocks != 0;
		}

		internal virtual string GetStorageID()
		{
			return storageID;
		}

		public virtual StorageType GetStorageType()
		{
			return storageType;
		}

		internal virtual long GetCapacity()
		{
			return capacity;
		}

		internal virtual long GetDfsUsed()
		{
			return dfsUsed;
		}

		internal virtual long GetRemaining()
		{
			return remaining;
		}

		internal virtual long GetBlockPoolUsed()
		{
			return blockPoolUsed;
		}

		public virtual DatanodeStorageInfo.AddBlockResult AddBlock(BlockInfoContiguous b)
		{
			// First check whether the block belongs to a different storage
			// on the same DN.
			DatanodeStorageInfo.AddBlockResult result = DatanodeStorageInfo.AddBlockResult.Added;
			DatanodeStorageInfo otherStorage = b.FindStorageInfo(GetDatanodeDescriptor());
			if (otherStorage != null)
			{
				if (otherStorage != this)
				{
					// The block belongs to a different storage. Remove it first.
					otherStorage.RemoveBlock(b);
					result = DatanodeStorageInfo.AddBlockResult.Replaced;
				}
				else
				{
					// The block is already associated with this storage.
					return DatanodeStorageInfo.AddBlockResult.AlreadyExist;
				}
			}
			// add to the head of the data-node list
			b.AddStorage(this);
			blockList = b.ListInsert(blockList, this);
			numBlocks++;
			return result;
		}

		public virtual bool RemoveBlock(BlockInfoContiguous b)
		{
			blockList = b.ListRemove(blockList, this);
			if (b.RemoveStorage(this))
			{
				numBlocks--;
				return true;
			}
			else
			{
				return false;
			}
		}

		internal virtual int NumBlocks()
		{
			return numBlocks;
		}

		internal virtual IEnumerator<BlockInfoContiguous> GetBlockIterator()
		{
			return new DatanodeStorageInfo.BlockIterator(this, blockList);
		}

		/// <summary>Move block to the head of the list of blocks belonging to the data-node.
		/// 	</summary>
		/// <returns>the index of the head of the blockList</returns>
		internal virtual int MoveBlockToHead(BlockInfoContiguous b, int curIndex, int headIndex
			)
		{
			blockList = b.MoveBlockToHead(blockList, this, curIndex, headIndex);
			return curIndex;
		}

		/// <summary>Used for testing only</summary>
		/// <returns>the head of the blockList</returns>
		[VisibleForTesting]
		internal virtual BlockInfoContiguous GetBlockListHeadForTesting()
		{
			return blockList;
		}

		internal virtual void UpdateState(StorageReport r)
		{
			capacity = r.GetCapacity();
			dfsUsed = r.GetDfsUsed();
			remaining = r.GetRemaining();
			blockPoolUsed = r.GetBlockPoolUsed();
		}

		public virtual DatanodeDescriptor GetDatanodeDescriptor()
		{
			return dn;
		}

		/// <summary>Increment the number of blocks scheduled for each given storage</summary>
		public static void IncrementBlocksScheduled(params DatanodeStorageInfo[] storages
			)
		{
			foreach (DatanodeStorageInfo s in storages)
			{
				s.GetDatanodeDescriptor().IncrementBlocksScheduled(s.GetStorageType());
			}
		}

		public override bool Equals(object obj)
		{
			if (this == obj)
			{
				return true;
			}
			else
			{
				if (obj == null || !(obj is DatanodeStorageInfo))
				{
					return false;
				}
			}
			DatanodeStorageInfo that = (DatanodeStorageInfo)obj;
			return this.storageID.Equals(that.storageID);
		}

		public override int GetHashCode()
		{
			return storageID.GetHashCode();
		}

		public override string ToString()
		{
			return "[" + storageType + "]" + storageID + ":" + state + ":" + dn;
		}

		internal virtual StorageReport ToStorageReport()
		{
			return new StorageReport(new DatanodeStorage(storageID, state, storageType), false
				, capacity, dfsUsed, remaining, blockPoolUsed);
		}

		internal static IEnumerable<StorageType> ToStorageTypes(IEnumerable<DatanodeStorageInfo
			> infos)
		{
			return new _IEnumerable_338(infos);
		}

		private sealed class _IEnumerable_338 : IEnumerable<StorageType>
		{
			public _IEnumerable_338(IEnumerable<DatanodeStorageInfo> infos)
			{
				this.infos = infos;
			}

			public override IEnumerator<StorageType> GetEnumerator()
			{
				return new _IEnumerator_341(infos);
			}

			private sealed class _IEnumerator_341 : IEnumerator<StorageType>
			{
				public _IEnumerator_341(IEnumerable<DatanodeStorageInfo> infos)
				{
					this.infos = infos;
					this.i = infos.GetEnumerator();
				}

				internal readonly IEnumerator<DatanodeStorageInfo> i;

				public override bool HasNext()
				{
					return this.i.HasNext();
				}

				public override StorageType Next()
				{
					return this.i.Next().GetStorageType();
				}

				public override void Remove()
				{
					throw new NotSupportedException();
				}

				private readonly IEnumerable<DatanodeStorageInfo> infos;
			}

			private readonly IEnumerable<DatanodeStorageInfo> infos;
		}

		/// <returns>
		/// the first
		/// <see cref="DatanodeStorageInfo"/>
		/// corresponding to
		/// the given datanode
		/// </returns>
		internal static DatanodeStorageInfo GetDatanodeStorageInfo(IEnumerable<DatanodeStorageInfo
			> infos, DatanodeDescriptor datanode)
		{
			if (datanode == null)
			{
				return null;
			}
			foreach (DatanodeStorageInfo storage in infos)
			{
				if (storage.GetDatanodeDescriptor() == datanode)
				{
					return storage;
				}
			}
			return null;
		}

		internal enum AddBlockResult
		{
			Added,
			Replaced,
			AlreadyExist
		}
	}
}
