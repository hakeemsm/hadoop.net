using System;
using System.Collections.Generic;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Blockmanagement
{
	/// <summary>Keep prioritized queues of under replicated blocks.</summary>
	/// <remarks>
	/// Keep prioritized queues of under replicated blocks.
	/// Blocks have replication priority, with priority
	/// <see cref="QueueHighestPriority"/>
	/// indicating the highest priority.
	/// </p>
	/// Having a prioritised queue allows the
	/// <see cref="BlockManager"/>
	/// to select
	/// which blocks to replicate first -it tries to give priority to data
	/// that is most at risk or considered most valuable.
	/// <p/>
	/// The policy for choosing which priority to give added blocks
	/// is implemented in
	/// <see cref="GetPriority(Org.Apache.Hadoop.Hdfs.Protocol.Block, int, int, int)"/>
	/// .
	/// </p>
	/// <p>The queue order is as follows:</p>
	/// <ol>
	/// <li>
	/// <see cref="QueueHighestPriority"/>
	/// : the blocks that must be replicated
	/// first. That is blocks with only one copy, or blocks with zero live
	/// copies but a copy in a node being decommissioned. These blocks
	/// are at risk of loss if the disk or server on which they
	/// remain fails.</li>
	/// <li>
	/// <see cref="QueueVeryUnderReplicated"/>
	/// : blocks that are very
	/// under-replicated compared to their expected values. Currently
	/// that means the ratio of the ratio of actual:expected means that
	/// there is <i>less than</i> 1:3.</li>. These blocks may not be at risk,
	/// but they are clearly considered "important".
	/// <li>
	/// <see cref="QueueUnderReplicated"/>
	/// : blocks that are also under
	/// replicated, and the ratio of actual:expected is good enough that
	/// they do not need to go into the
	/// <see cref="QueueVeryUnderReplicated"/>
	/// queue.</li>
	/// <li>
	/// <see cref="QueueReplicasBadlyDistributed"/>
	/// : there are as least as
	/// many copies of a block as required, but the blocks are not adequately
	/// distributed. Loss of a rack/switch could take all copies off-line.</li>
	/// <li>
	/// <see cref="QueueWithCorruptBlocks"/>
	/// This is for blocks that are corrupt
	/// and for which there are no-non-corrupt copies (currently) available.
	/// The policy here is to keep those corrupt blocks replicated, but give
	/// blocks that are not corrupt higher priority.</li>
	/// </ol>
	/// </remarks>
	internal class UnderReplicatedBlocks : IEnumerable<Block>
	{
		/// <summary>
		/// The total number of queues :
		/// <value/>
		/// 
		/// </summary>
		internal const int Level = 5;

		/// <summary>
		/// The queue with the highest priority:
		/// <value/>
		/// 
		/// </summary>
		internal const int QueueHighestPriority = 0;

		/// <summary>
		/// The queue for blocks that are way below their expected value :
		/// <value/>
		/// 
		/// </summary>
		internal const int QueueVeryUnderReplicated = 1;

		/// <summary>
		/// The queue for "normally" under-replicated blocks:
		/// <value/>
		/// 
		/// </summary>
		internal const int QueueUnderReplicated = 2;

		/// <summary>
		/// The queue for blocks that have the right number of replicas,
		/// but which the block manager felt were badly distributed:
		/// <value/>
		/// </summary>
		internal const int QueueReplicasBadlyDistributed = 3;

		/// <summary>
		/// The queue for corrupt blocks:
		/// <value/>
		/// 
		/// </summary>
		internal const int QueueWithCorruptBlocks = 4;

		/// <summary>the queues themselves</summary>
		private readonly IList<LightWeightLinkedSet<Block>> priorityQueues = new AList<LightWeightLinkedSet
			<Block>>();

		/// <summary>Stores the replication index for each priority</summary>
		private IDictionary<int, int> priorityToReplIdx = new Dictionary<int, int>(Level);

		/// <summary>The number of corrupt blocks with replication factor 1</summary>
		private int corruptReplOneBlocks = 0;

		/// <summary>Create an object.</summary>
		internal UnderReplicatedBlocks()
		{
			for (int i = 0; i < Level; i++)
			{
				priorityQueues.AddItem(new LightWeightLinkedSet<Block>());
				priorityToReplIdx[i] = 0;
			}
		}

		/// <summary>Empty the queues.</summary>
		internal virtual void Clear()
		{
			lock (this)
			{
				for (int i = 0; i < Level; i++)
				{
					priorityQueues[i].Clear();
				}
				corruptReplOneBlocks = 0;
			}
		}

		/// <summary>Return the total number of under replication blocks</summary>
		internal virtual int Size()
		{
			lock (this)
			{
				int size = 0;
				for (int i = 0; i < Level; i++)
				{
					size += priorityQueues[i].Count;
				}
				return size;
			}
		}

		/// <summary>Return the number of under replication blocks excluding corrupt blocks</summary>
		internal virtual int GetUnderReplicatedBlockCount()
		{
			lock (this)
			{
				int size = 0;
				for (int i = 0; i < Level; i++)
				{
					if (i != QueueWithCorruptBlocks)
					{
						size += priorityQueues[i].Count;
					}
				}
				return size;
			}
		}

		/// <summary>Return the number of corrupt blocks</summary>
		internal virtual int GetCorruptBlockSize()
		{
			lock (this)
			{
				return priorityQueues[QueueWithCorruptBlocks].Count;
			}
		}

		/// <summary>Return the number of corrupt blocks with replication factor 1</summary>
		internal virtual int GetCorruptReplOneBlockSize()
		{
			lock (this)
			{
				return corruptReplOneBlocks;
			}
		}

		/// <summary>Check if a block is in the neededReplication queue</summary>
		internal virtual bool Contains(Block block)
		{
			lock (this)
			{
				foreach (LightWeightLinkedSet<Block> set in priorityQueues)
				{
					if (set.Contains(block))
					{
						return true;
					}
				}
				return false;
			}
		}

		/// <summary>Return the priority of a block</summary>
		/// <param name="block">a under replicated block</param>
		/// <param name="curReplicas">current number of replicas of the block</param>
		/// <param name="expectedReplicas">expected number of replicas of the block</param>
		/// <returns>
		/// the priority for the blocks, between 0 and (
		/// <see cref="Level"/>
		/// -1)
		/// </returns>
		private int GetPriority(Block block, int curReplicas, int decommissionedReplicas, 
			int expectedReplicas)
		{
			System.Diagnostics.Debug.Assert(curReplicas >= 0, "Negative replicas!");
			if (curReplicas >= expectedReplicas)
			{
				// Block has enough copies, but not enough racks
				return QueueReplicasBadlyDistributed;
			}
			else
			{
				if (curReplicas == 0)
				{
					// If there are zero non-decommissioned replicas but there are
					// some decommissioned replicas, then assign them highest priority
					if (decommissionedReplicas > 0)
					{
						return QueueHighestPriority;
					}
					//all we have are corrupt blocks
					return QueueWithCorruptBlocks;
				}
				else
				{
					if (curReplicas == 1)
					{
						//only on replica -risk of loss
						// highest priority
						return QueueHighestPriority;
					}
					else
					{
						if ((curReplicas * 3) < expectedReplicas)
						{
							//there is less than a third as many blocks as requested;
							//this is considered very under-replicated
							return QueueVeryUnderReplicated;
						}
						else
						{
							//add to the normal queue for under replicated blocks
							return QueueUnderReplicated;
						}
					}
				}
			}
		}

		/// <summary>add a block to a under replication queue according to its priority</summary>
		/// <param name="block">a under replication block</param>
		/// <param name="curReplicas">current number of replicas of the block</param>
		/// <param name="decomissionedReplicas">the number of decommissioned replicas</param>
		/// <param name="expectedReplicas">expected number of replicas of the block</param>
		/// <returns>true if the block was added to a queue.</returns>
		internal virtual bool Add(Block block, int curReplicas, int decomissionedReplicas
			, int expectedReplicas)
		{
			lock (this)
			{
				System.Diagnostics.Debug.Assert(curReplicas >= 0, "Negative replicas!");
				int priLevel = GetPriority(block, curReplicas, decomissionedReplicas, expectedReplicas
					);
				if (priorityQueues[priLevel].AddItem(block))
				{
					if (priLevel == QueueWithCorruptBlocks && expectedReplicas == 1)
					{
						corruptReplOneBlocks++;
					}
					NameNode.blockStateChangeLog.Debug("BLOCK* NameSystem.UnderReplicationBlock.add: {}"
						 + " has only {} replicas and need {} replicas so is added to" + " neededReplications at priority level {}"
						, block, curReplicas, expectedReplicas, priLevel);
					return true;
				}
				return false;
			}
		}

		/// <summary>remove a block from a under replication queue</summary>
		internal virtual bool Remove(Block block, int oldReplicas, int decommissionedReplicas
			, int oldExpectedReplicas)
		{
			lock (this)
			{
				int priLevel = GetPriority(block, oldReplicas, decommissionedReplicas, oldExpectedReplicas
					);
				bool removedBlock = Remove(block, priLevel);
				if (priLevel == QueueWithCorruptBlocks && oldExpectedReplicas == 1 && removedBlock)
				{
					corruptReplOneBlocks--;
					System.Diagnostics.Debug.Assert(corruptReplOneBlocks >= 0, "Number of corrupt blocks with replication factor 1 "
						 + "should be non-negative");
				}
				return removedBlock;
			}
		}

		/// <summary>Remove a block from the under replication queues.</summary>
		/// <remarks>
		/// Remove a block from the under replication queues.
		/// The priLevel parameter is a hint of which queue to query
		/// first: if negative or &gt;=
		/// <see cref="Level"/>
		/// this shortcutting
		/// is not attmpted.
		/// If the block is not found in the nominated queue, an attempt is made to
		/// remove it from all queues.
		/// <i>Warning:</i> This is not a synchronized method.
		/// </remarks>
		/// <param name="block">block to remove</param>
		/// <param name="priLevel">expected privilege level</param>
		/// <returns>true if the block was found and removed from one of the priority queues</returns>
		internal virtual bool Remove(Block block, int priLevel)
		{
			if (priLevel >= 0 && priLevel < Level && priorityQueues[priLevel].Remove(block))
			{
				NameNode.blockStateChangeLog.Debug("BLOCK* NameSystem.UnderReplicationBlock.remove: Removing block {}"
					 + " from priority queue {}", block, priLevel);
				return true;
			}
			else
			{
				// Try to remove the block from all queues if the block was
				// not found in the queue for the given priority level.
				for (int i = 0; i < Level; i++)
				{
					if (priorityQueues[i].Remove(block))
					{
						NameNode.blockStateChangeLog.Debug("BLOCK* NameSystem.UnderReplicationBlock.remove: Removing block"
							 + " {} from priority queue {}", block, priLevel);
						return true;
					}
				}
			}
			return false;
		}

		/// <summary>Recalculate and potentially update the priority level of a block.</summary>
		/// <remarks>
		/// Recalculate and potentially update the priority level of a block.
		/// If the block priority has changed from before an attempt is made to
		/// remove it from the block queue. Regardless of whether or not the block
		/// is in the block queue of (recalculate) priority, an attempt is made
		/// to add it to that queue. This ensures that the block will be
		/// in its expected priority queue (and only that queue) by the end of the
		/// method call.
		/// </remarks>
		/// <param name="block">a under replicated block</param>
		/// <param name="curReplicas">current number of replicas of the block</param>
		/// <param name="decommissionedReplicas">the number of decommissioned replicas</param>
		/// <param name="curExpectedReplicas">expected number of replicas of the block</param>
		/// <param name="curReplicasDelta">the change in the replicate count from before</param>
		/// <param name="expectedReplicasDelta">the change in the expected replica count from before
		/// 	</param>
		internal virtual void Update(Block block, int curReplicas, int decommissionedReplicas
			, int curExpectedReplicas, int curReplicasDelta, int expectedReplicasDelta)
		{
			lock (this)
			{
				int oldReplicas = curReplicas - curReplicasDelta;
				int oldExpectedReplicas = curExpectedReplicas - expectedReplicasDelta;
				int curPri = GetPriority(block, curReplicas, decommissionedReplicas, curExpectedReplicas
					);
				int oldPri = GetPriority(block, oldReplicas, decommissionedReplicas, oldExpectedReplicas
					);
				if (NameNode.stateChangeLog.IsDebugEnabled())
				{
					NameNode.stateChangeLog.Debug("UnderReplicationBlocks.update " + block + " curReplicas "
						 + curReplicas + " curExpectedReplicas " + curExpectedReplicas + " oldReplicas "
						 + oldReplicas + " oldExpectedReplicas  " + oldExpectedReplicas + " curPri  " + 
						curPri + " oldPri  " + oldPri);
				}
				if (oldPri != curPri)
				{
					Remove(block, oldPri);
				}
				if (priorityQueues[curPri].AddItem(block))
				{
					NameNode.blockStateChangeLog.Debug("BLOCK* NameSystem.UnderReplicationBlock.update: {} has only {} "
						 + "replicas and needs {} replicas so is added to " + "neededReplications at priority level {}"
						, block, curReplicas, curExpectedReplicas, curPri);
				}
				if (oldPri != curPri || expectedReplicasDelta != 0)
				{
					// corruptReplOneBlocks could possibly change
					if (curPri == QueueWithCorruptBlocks && curExpectedReplicas == 1)
					{
						// add a new corrupt block with replication factor 1
						corruptReplOneBlocks++;
					}
					else
					{
						if (oldPri == QueueWithCorruptBlocks && curExpectedReplicas - expectedReplicasDelta
							 == 1)
						{
							// remove an existing corrupt block with replication factor 1
							corruptReplOneBlocks--;
						}
					}
				}
			}
		}

		/// <summary>Get a list of block lists to be replicated.</summary>
		/// <remarks>
		/// Get a list of block lists to be replicated. The index of block lists
		/// represents its replication priority. Replication index will be tracked for
		/// each priority list separately in priorityToReplIdx map. Iterates through
		/// all priority lists and find the elements after replication index. Once the
		/// last priority lists reaches to end, all replication indexes will be set to
		/// 0 and start from 1st priority list to fulfill the blockToProces count.
		/// </remarks>
		/// <param name="blocksToProcess">- number of blocks to fetch from underReplicated blocks.
		/// 	</param>
		/// <returns>
		/// Return a list of block lists to be replicated. The block list index
		/// represents its replication priority.
		/// </returns>
		public virtual IList<IList<Block>> ChooseUnderReplicatedBlocks(int blocksToProcess
			)
		{
			lock (this)
			{
				// initialize data structure for the return value
				IList<IList<Block>> blocksToReplicate = new AList<IList<Block>>(Level);
				for (int i = 0; i < Level; i++)
				{
					blocksToReplicate.AddItem(new AList<Block>());
				}
				if (Size() == 0)
				{
					// There are no blocks to collect.
					return blocksToReplicate;
				}
				int blockCount = 0;
				for (int priority = 0; priority < Level; priority++)
				{
					// Go through all blocks that need replications with current priority.
					UnderReplicatedBlocks.BlockIterator neededReplicationsIterator = Iterator(priority
						);
					int replIndex = priorityToReplIdx[priority];
					// skip to the first unprocessed block, which is at replIndex
					for (int i_1 = 0; i_1 < replIndex && neededReplicationsIterator.HasNext(); i_1++)
					{
						neededReplicationsIterator.Next();
					}
					blocksToProcess = Math.Min(blocksToProcess, Size());
					if (blockCount == blocksToProcess)
					{
						break;
					}
					// break if already expected blocks are obtained
					// Loop through all remaining blocks in the list.
					while (blockCount < blocksToProcess && neededReplicationsIterator.HasNext())
					{
						Block block = neededReplicationsIterator.Next();
						blocksToReplicate[priority].AddItem(block);
						replIndex++;
						blockCount++;
					}
					if (!neededReplicationsIterator.HasNext() && neededReplicationsIterator.GetPriority
						() == Level - 1)
					{
						// reset all priorities replication index to 0 because there is no
						// recently added blocks in any list.
						for (int i_2 = 0; i_2 < Level; i_2++)
						{
							priorityToReplIdx[i_2] = 0;
						}
						break;
					}
					priorityToReplIdx[priority] = replIndex;
				}
				return blocksToReplicate;
			}
		}

		/// <summary>returns an iterator of all blocks in a given priority queue</summary>
		internal virtual UnderReplicatedBlocks.BlockIterator Iterator(int level)
		{
			lock (this)
			{
				return new UnderReplicatedBlocks.BlockIterator(this, level);
			}
		}

		/// <summary>return an iterator of all the under replication blocks</summary>
		public override IEnumerator<Block> GetEnumerator()
		{
			lock (this)
			{
				return new UnderReplicatedBlocks.BlockIterator(this);
			}
		}

		/// <summary>An iterator over blocks.</summary>
		internal class BlockIterator : IEnumerator<Block>
		{
			private int level;

			private bool isIteratorForLevel = false;

			private readonly IList<IEnumerator<Block>> iterators = new AList<IEnumerator<Block
				>>();

			/// <summary>Construct an iterator over all queues.</summary>
			private BlockIterator(UnderReplicatedBlocks _enclosing)
			{
				this._enclosing = _enclosing;
				this.level = 0;
				for (int i = 0; i < UnderReplicatedBlocks.Level; i++)
				{
					this.iterators.AddItem(this._enclosing.priorityQueues[i].GetEnumerator());
				}
			}

			/// <summary>Constrict an iterator for a single queue level</summary>
			/// <param name="l">the priority level to iterate over</param>
			private BlockIterator(UnderReplicatedBlocks _enclosing, int l)
			{
				this._enclosing = _enclosing;
				this.level = l;
				this.isIteratorForLevel = true;
				this.iterators.AddItem(this._enclosing.priorityQueues[this.level].GetEnumerator()
					);
			}

			private void Update()
			{
				if (this.isIteratorForLevel)
				{
					return;
				}
				while (this.level < UnderReplicatedBlocks.Level - 1 && !this.iterators[this.level
					].HasNext())
				{
					this.level++;
				}
			}

			public override Block Next()
			{
				if (this.isIteratorForLevel)
				{
					return this.iterators[0].Next();
				}
				this.Update();
				return this.iterators[this.level].Next();
			}

			public override bool HasNext()
			{
				if (this.isIteratorForLevel)
				{
					return this.iterators[0].HasNext();
				}
				this.Update();
				return this.iterators[this.level].HasNext();
			}

			public override void Remove()
			{
				if (this.isIteratorForLevel)
				{
					this.iterators[0].Remove();
				}
				else
				{
					this.iterators[this.level].Remove();
				}
			}

			internal virtual int GetPriority()
			{
				return this.level;
			}

			private readonly UnderReplicatedBlocks _enclosing;
		}

		/// <summary>This method is to decrement the replication index for the given priority
		/// 	</summary>
		/// <param name="priority">- int priority level</param>
		public virtual void DecrementReplicationIndex(int priority)
		{
			int replIdx = priorityToReplIdx[priority];
			priorityToReplIdx[priority] = --replIdx;
		}
	}
}
