using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Blockmanagement
{
	/// <summary>
	/// BlockInfo class maintains for a given block
	/// the
	/// <see cref="INodeFile"/>
	/// it is part of and datanodes where the replicas of
	/// the block are stored.
	/// BlockInfo class maintains for a given block
	/// the
	/// <see cref="BlockCollection"/>
	/// it is part of and datanodes where the replicas of
	/// the block are stored.
	/// </summary>
	public class BlockInfoContiguous : Block, LightWeightGSet.LinkedElement
	{
		public static readonly Org.Apache.Hadoop.Hdfs.Server.Blockmanagement.BlockInfoContiguous
			[] EmptyArray = new Org.Apache.Hadoop.Hdfs.Server.Blockmanagement.BlockInfoContiguous
			[] {  };

		private BlockCollection bc;

		/// <summary>
		/// For implementing
		/// <see cref="Org.Apache.Hadoop.Util.LightWeightGSet.LinkedElement"/>
		/// interface
		/// </summary>
		private LightWeightGSet.LinkedElement nextLinkedElement;

		/// <summary>This array contains triplets of references.</summary>
		/// <remarks>
		/// This array contains triplets of references. For each i-th storage, the
		/// block belongs to triplets[3*i] is the reference to the
		/// <see cref="DatanodeStorageInfo"/>
		/// and triplets[3*i+1] and triplets[3*i+2] are
		/// references to the previous and the next blocks, respectively, in the list
		/// of blocks belonging to this storage.
		/// Using previous and next in Object triplets is done instead of a
		/// <see cref="System.Collections.ArrayList{E}"/>
		/// list to efficiently use memory. With LinkedList the cost
		/// per replica is 42 bytes (LinkedList#Entry object per replica) versus 16
		/// bytes using the triplets.
		/// </remarks>
		private object[] triplets;

		/// <summary>Construct an entry for blocksmap</summary>
		/// <param name="replication">the block's replication factor</param>
		public BlockInfoContiguous(short replication)
		{
			this.triplets = new object[3 * replication];
			this.bc = null;
		}

		public BlockInfoContiguous(Block blk, short replication)
			: base(blk)
		{
			this.triplets = new object[3 * replication];
			this.bc = null;
		}

		/// <summary>Copy construction.</summary>
		/// <remarks>
		/// Copy construction.
		/// This is used to convert BlockInfoUnderConstruction
		/// </remarks>
		/// <param name="from">BlockInfo to copy from.</param>
		protected internal BlockInfoContiguous(Org.Apache.Hadoop.Hdfs.Server.Blockmanagement.BlockInfoContiguous
			 from)
			: this(from, from.bc.GetBlockReplication())
		{
			this.bc = from.bc;
		}

		public virtual BlockCollection GetBlockCollection()
		{
			return bc;
		}

		public virtual void SetBlockCollection(BlockCollection bc)
		{
			this.bc = bc;
		}

		public virtual DatanodeDescriptor GetDatanode(int index)
		{
			DatanodeStorageInfo storage = GetStorageInfo(index);
			return storage == null ? null : storage.GetDatanodeDescriptor();
		}

		internal virtual DatanodeStorageInfo GetStorageInfo(int index)
		{
			System.Diagnostics.Debug.Assert(this.triplets != null, "BlockInfo is not initialized"
				);
			System.Diagnostics.Debug.Assert(index >= 0 && index * 3 < triplets.Length, "Index is out of bound"
				);
			return (DatanodeStorageInfo)triplets[index * 3];
		}

		private Org.Apache.Hadoop.Hdfs.Server.Blockmanagement.BlockInfoContiguous GetPrevious
			(int index)
		{
			System.Diagnostics.Debug.Assert(this.triplets != null, "BlockInfo is not initialized"
				);
			System.Diagnostics.Debug.Assert(index >= 0 && index * 3 + 1 < triplets.Length, "Index is out of bound"
				);
			Org.Apache.Hadoop.Hdfs.Server.Blockmanagement.BlockInfoContiguous info = (Org.Apache.Hadoop.Hdfs.Server.Blockmanagement.BlockInfoContiguous
				)triplets[index * 3 + 1];
			System.Diagnostics.Debug.Assert(info == null || info.GetType().FullName.StartsWith
				(typeof(Org.Apache.Hadoop.Hdfs.Server.Blockmanagement.BlockInfoContiguous).FullName
				), "BlockInfo is expected at " + index * 3);
			return info;
		}

		internal virtual Org.Apache.Hadoop.Hdfs.Server.Blockmanagement.BlockInfoContiguous
			 GetNext(int index)
		{
			System.Diagnostics.Debug.Assert(this.triplets != null, "BlockInfo is not initialized"
				);
			System.Diagnostics.Debug.Assert(index >= 0 && index * 3 + 2 < triplets.Length, "Index is out of bound"
				);
			Org.Apache.Hadoop.Hdfs.Server.Blockmanagement.BlockInfoContiguous info = (Org.Apache.Hadoop.Hdfs.Server.Blockmanagement.BlockInfoContiguous
				)triplets[index * 3 + 2];
			System.Diagnostics.Debug.Assert(info == null || info.GetType().FullName.StartsWith
				(typeof(Org.Apache.Hadoop.Hdfs.Server.Blockmanagement.BlockInfoContiguous).FullName
				), "BlockInfo is expected at " + index * 3);
			return info;
		}

		private void SetStorageInfo(int index, DatanodeStorageInfo storage)
		{
			System.Diagnostics.Debug.Assert(this.triplets != null, "BlockInfo is not initialized"
				);
			System.Diagnostics.Debug.Assert(index >= 0 && index * 3 < triplets.Length, "Index is out of bound"
				);
			triplets[index * 3] = storage;
		}

		/// <summary>
		/// Return the previous block on the block list for the datanode at
		/// position index.
		/// </summary>
		/// <remarks>
		/// Return the previous block on the block list for the datanode at
		/// position index. Set the previous block on the list to "to".
		/// </remarks>
		/// <param name="index">- the datanode index</param>
		/// <param name="to">- block to be set to previous on the list of blocks</param>
		/// <returns>current previous block on the list of blocks</returns>
		private Org.Apache.Hadoop.Hdfs.Server.Blockmanagement.BlockInfoContiguous SetPrevious
			(int index, Org.Apache.Hadoop.Hdfs.Server.Blockmanagement.BlockInfoContiguous to
			)
		{
			System.Diagnostics.Debug.Assert(this.triplets != null, "BlockInfo is not initialized"
				);
			System.Diagnostics.Debug.Assert(index >= 0 && index * 3 + 1 < triplets.Length, "Index is out of bound"
				);
			Org.Apache.Hadoop.Hdfs.Server.Blockmanagement.BlockInfoContiguous info = (Org.Apache.Hadoop.Hdfs.Server.Blockmanagement.BlockInfoContiguous
				)triplets[index * 3 + 1];
			triplets[index * 3 + 1] = to;
			return info;
		}

		/// <summary>
		/// Return the next block on the block list for the datanode at
		/// position index.
		/// </summary>
		/// <remarks>
		/// Return the next block on the block list for the datanode at
		/// position index. Set the next block on the list to "to".
		/// </remarks>
		/// <param name="index">- the datanode index</param>
		/// <param name="to">
		/// - block to be set to next on the list of blocks
		/// * @return current next block on the list of blocks
		/// </param>
		private Org.Apache.Hadoop.Hdfs.Server.Blockmanagement.BlockInfoContiguous SetNext
			(int index, Org.Apache.Hadoop.Hdfs.Server.Blockmanagement.BlockInfoContiguous to
			)
		{
			System.Diagnostics.Debug.Assert(this.triplets != null, "BlockInfo is not initialized"
				);
			System.Diagnostics.Debug.Assert(index >= 0 && index * 3 + 2 < triplets.Length, "Index is out of bound"
				);
			Org.Apache.Hadoop.Hdfs.Server.Blockmanagement.BlockInfoContiguous info = (Org.Apache.Hadoop.Hdfs.Server.Blockmanagement.BlockInfoContiguous
				)triplets[index * 3 + 2];
			triplets[index * 3 + 2] = to;
			return info;
		}

		public virtual int GetCapacity()
		{
			System.Diagnostics.Debug.Assert(this.triplets != null, "BlockInfo is not initialized"
				);
			System.Diagnostics.Debug.Assert(triplets.Length % 3 == 0, "Malformed BlockInfo");
			return triplets.Length / 3;
		}

		/// <summary>Ensure that there is enough  space to include num more triplets.</summary>
		/// <returns>first free triplet index.</returns>
		private int EnsureCapacity(int num)
		{
			System.Diagnostics.Debug.Assert(this.triplets != null, "BlockInfo is not initialized"
				);
			int last = NumNodes();
			if (triplets.Length >= (last + num) * 3)
			{
				return last;
			}
			/* Not enough space left. Create a new array. Should normally
			* happen only when replication is manually increased by the user. */
			object[] old = triplets;
			triplets = new object[(last + num) * 3];
			System.Array.Copy(old, 0, triplets, 0, last * 3);
			return last;
		}

		/// <summary>Count the number of data-nodes the block belongs to.</summary>
		public virtual int NumNodes()
		{
			System.Diagnostics.Debug.Assert(this.triplets != null, "BlockInfo is not initialized"
				);
			System.Diagnostics.Debug.Assert(triplets.Length % 3 == 0, "Malformed BlockInfo");
			for (int idx = GetCapacity() - 1; idx >= 0; idx--)
			{
				if (GetDatanode(idx) != null)
				{
					return idx + 1;
				}
			}
			return 0;
		}

		/// <summary>
		/// Add a
		/// <see cref="DatanodeStorageInfo"/>
		/// location for a block
		/// </summary>
		internal virtual bool AddStorage(DatanodeStorageInfo storage)
		{
			// find the last null node
			int lastNode = EnsureCapacity(1);
			SetStorageInfo(lastNode, storage);
			SetNext(lastNode, null);
			SetPrevious(lastNode, null);
			return true;
		}

		/// <summary>
		/// Remove
		/// <see cref="DatanodeStorageInfo"/>
		/// location for a block
		/// </summary>
		internal virtual bool RemoveStorage(DatanodeStorageInfo storage)
		{
			int dnIndex = FindStorageInfo(storage);
			if (dnIndex < 0)
			{
				// the node is not found
				return false;
			}
			System.Diagnostics.Debug.Assert(GetPrevious(dnIndex) == null && GetNext(dnIndex) 
				== null, "Block is still in the list and must be removed first.");
			// find the last not null node
			int lastNode = NumNodes() - 1;
			// replace current node triplet by the lastNode one 
			SetStorageInfo(dnIndex, GetStorageInfo(lastNode));
			SetNext(dnIndex, GetNext(lastNode));
			SetPrevious(dnIndex, GetPrevious(lastNode));
			// set the last triplet to null
			SetStorageInfo(lastNode, null);
			SetNext(lastNode, null);
			SetPrevious(lastNode, null);
			return true;
		}

		/// <summary>Find specified DatanodeDescriptor.</summary>
		/// <returns>index or -1 if not found.</returns>
		internal virtual bool FindDatanode(DatanodeDescriptor dn)
		{
			int len = GetCapacity();
			for (int idx = 0; idx < len; idx++)
			{
				DatanodeDescriptor cur = GetDatanode(idx);
				if (cur == dn)
				{
					return true;
				}
				if (cur == null)
				{
					break;
				}
			}
			return false;
		}

		/// <summary>Find specified DatanodeStorageInfo.</summary>
		/// <returns>DatanodeStorageInfo or null if not found.</returns>
		internal virtual DatanodeStorageInfo FindStorageInfo(DatanodeDescriptor dn)
		{
			int len = GetCapacity();
			for (int idx = 0; idx < len; idx++)
			{
				DatanodeStorageInfo cur = GetStorageInfo(idx);
				if (cur == null)
				{
					break;
				}
				if (cur.GetDatanodeDescriptor() == dn)
				{
					return cur;
				}
			}
			return null;
		}

		/// <summary>Find specified DatanodeStorageInfo.</summary>
		/// <returns>index or -1 if not found.</returns>
		internal virtual int FindStorageInfo(DatanodeStorageInfo storageInfo)
		{
			int len = GetCapacity();
			for (int idx = 0; idx < len; idx++)
			{
				DatanodeStorageInfo cur = GetStorageInfo(idx);
				if (cur == storageInfo)
				{
					return idx;
				}
				if (cur == null)
				{
					break;
				}
			}
			return -1;
		}

		/// <summary>
		/// Insert this block into the head of the list of blocks
		/// related to the specified DatanodeStorageInfo.
		/// </summary>
		/// <remarks>
		/// Insert this block into the head of the list of blocks
		/// related to the specified DatanodeStorageInfo.
		/// If the head is null then form a new list.
		/// </remarks>
		/// <returns>current block as the new head of the list.</returns>
		internal virtual Org.Apache.Hadoop.Hdfs.Server.Blockmanagement.BlockInfoContiguous
			 ListInsert(Org.Apache.Hadoop.Hdfs.Server.Blockmanagement.BlockInfoContiguous head
			, DatanodeStorageInfo storage)
		{
			int dnIndex = this.FindStorageInfo(storage);
			System.Diagnostics.Debug.Assert(dnIndex >= 0, "Data node is not found: current");
			System.Diagnostics.Debug.Assert(GetPrevious(dnIndex) == null && GetNext(dnIndex) 
				== null, "Block is already in the list and cannot be inserted.");
			this.SetPrevious(dnIndex, null);
			this.SetNext(dnIndex, head);
			if (head != null)
			{
				head.SetPrevious(head.FindStorageInfo(storage), this);
			}
			return this;
		}

		/// <summary>
		/// Remove this block from the list of blocks
		/// related to the specified DatanodeStorageInfo.
		/// </summary>
		/// <remarks>
		/// Remove this block from the list of blocks
		/// related to the specified DatanodeStorageInfo.
		/// If this block is the head of the list then return the next block as
		/// the new head.
		/// </remarks>
		/// <returns>
		/// the new head of the list or null if the list becomes
		/// empy after deletion.
		/// </returns>
		internal virtual Org.Apache.Hadoop.Hdfs.Server.Blockmanagement.BlockInfoContiguous
			 ListRemove(Org.Apache.Hadoop.Hdfs.Server.Blockmanagement.BlockInfoContiguous head
			, DatanodeStorageInfo storage)
		{
			if (head == null)
			{
				return null;
			}
			int dnIndex = this.FindStorageInfo(storage);
			if (dnIndex < 0)
			{
				// this block is not on the data-node list
				return head;
			}
			Org.Apache.Hadoop.Hdfs.Server.Blockmanagement.BlockInfoContiguous next = this.GetNext
				(dnIndex);
			Org.Apache.Hadoop.Hdfs.Server.Blockmanagement.BlockInfoContiguous prev = this.GetPrevious
				(dnIndex);
			this.SetNext(dnIndex, null);
			this.SetPrevious(dnIndex, null);
			if (prev != null)
			{
				prev.SetNext(prev.FindStorageInfo(storage), next);
			}
			if (next != null)
			{
				next.SetPrevious(next.FindStorageInfo(storage), prev);
			}
			if (this == head)
			{
				// removing the head
				head = next;
			}
			return head;
		}

		/// <summary>
		/// Remove this block from the list of blocks related to the specified
		/// DatanodeDescriptor.
		/// </summary>
		/// <remarks>
		/// Remove this block from the list of blocks related to the specified
		/// DatanodeDescriptor. Insert it into the head of the list of blocks.
		/// </remarks>
		/// <returns>the new head of the list.</returns>
		public virtual Org.Apache.Hadoop.Hdfs.Server.Blockmanagement.BlockInfoContiguous 
			MoveBlockToHead(Org.Apache.Hadoop.Hdfs.Server.Blockmanagement.BlockInfoContiguous
			 head, DatanodeStorageInfo storage, int curIndex, int headIndex)
		{
			if (head == this)
			{
				return this;
			}
			Org.Apache.Hadoop.Hdfs.Server.Blockmanagement.BlockInfoContiguous next = this.SetNext
				(curIndex, head);
			Org.Apache.Hadoop.Hdfs.Server.Blockmanagement.BlockInfoContiguous prev = this.SetPrevious
				(curIndex, null);
			head.SetPrevious(headIndex, this);
			prev.SetNext(prev.FindStorageInfo(storage), next);
			if (next != null)
			{
				next.SetPrevious(next.FindStorageInfo(storage), prev);
			}
			return this;
		}

		/// <summary>BlockInfo represents a block that is not being constructed.</summary>
		/// <remarks>
		/// BlockInfo represents a block that is not being constructed.
		/// In order to start modifying the block, the BlockInfo should be converted
		/// to
		/// <see cref="BlockInfoContiguousUnderConstruction"/>
		/// .
		/// </remarks>
		/// <returns>
		/// 
		/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Common.HdfsServerConstants.BlockUCState.Complete
		/// 	"/>
		/// </returns>
		public virtual HdfsServerConstants.BlockUCState GetBlockUCState()
		{
			return HdfsServerConstants.BlockUCState.Complete;
		}

		/// <summary>Is this block complete?</summary>
		/// <returns>
		/// true if the state of the block is
		/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Common.HdfsServerConstants.BlockUCState.Complete
		/// 	"/>
		/// </returns>
		public virtual bool IsComplete()
		{
			return GetBlockUCState().Equals(HdfsServerConstants.BlockUCState.Complete);
		}

		/// <summary>Convert a complete block to an under construction block.</summary>
		/// <returns>BlockInfoUnderConstruction -  an under construction block.</returns>
		public virtual BlockInfoContiguousUnderConstruction ConvertToBlockUnderConstruction
			(HdfsServerConstants.BlockUCState s, DatanodeStorageInfo[] targets)
		{
			if (IsComplete())
			{
				BlockInfoContiguousUnderConstruction ucBlock = new BlockInfoContiguousUnderConstruction
					(this, GetBlockCollection().GetBlockReplication(), s, targets);
				ucBlock.SetBlockCollection(GetBlockCollection());
				return ucBlock;
			}
			// the block is already under construction
			BlockInfoContiguousUnderConstruction ucBlock_1 = (BlockInfoContiguousUnderConstruction
				)this;
			ucBlock_1.SetBlockUCState(s);
			ucBlock_1.SetExpectedLocations(targets);
			ucBlock_1.SetBlockCollection(GetBlockCollection());
			return ucBlock_1;
		}

		public override int GetHashCode()
		{
			// Super implementation is sufficient
			return base.GetHashCode();
		}

		public override bool Equals(object obj)
		{
			// Sufficient to rely on super's implementation
			return (this == obj) || base.Equals(obj);
		}

		public virtual LightWeightGSet.LinkedElement GetNext()
		{
			return nextLinkedElement;
		}

		public virtual void SetNext(LightWeightGSet.LinkedElement next)
		{
			this.nextLinkedElement = next;
		}
	}
}
