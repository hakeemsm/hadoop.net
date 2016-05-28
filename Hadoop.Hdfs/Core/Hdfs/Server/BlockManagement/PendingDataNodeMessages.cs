using System.Collections.Generic;
using System.Text;
using Com.Google.Common.Collect;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Blockmanagement
{
	/// <summary>
	/// In the Standby Node, we can receive messages about blocks
	/// before they are actually available in the namespace, or while
	/// they have an outdated state in the namespace.
	/// </summary>
	/// <remarks>
	/// In the Standby Node, we can receive messages about blocks
	/// before they are actually available in the namespace, or while
	/// they have an outdated state in the namespace. In those cases,
	/// we queue those block-related messages in this structure.
	/// </remarks>
	internal class PendingDataNodeMessages
	{
		internal readonly IDictionary<Block, Queue<PendingDataNodeMessages.ReportedBlockInfo
			>> queueByBlockId = Maps.NewHashMap();

		private int count = 0;

		internal class ReportedBlockInfo
		{
			private readonly Block block;

			private readonly DatanodeStorageInfo storageInfo;

			private readonly HdfsServerConstants.ReplicaState reportedState;

			internal ReportedBlockInfo(DatanodeStorageInfo storageInfo, Block block, HdfsServerConstants.ReplicaState
				 reportedState)
			{
				this.storageInfo = storageInfo;
				this.block = block;
				this.reportedState = reportedState;
			}

			internal virtual Block GetBlock()
			{
				return block;
			}

			internal virtual HdfsServerConstants.ReplicaState GetReportedState()
			{
				return reportedState;
			}

			internal virtual DatanodeStorageInfo GetStorageInfo()
			{
				return storageInfo;
			}

			public override string ToString()
			{
				return "ReportedBlockInfo [block=" + block + ", dn=" + storageInfo.GetDatanodeDescriptor
					() + ", reportedState=" + reportedState + "]";
			}
		}

		/// <summary>Remove all pending DN messages which reference the given DN.</summary>
		/// <param name="dn">the datanode whose messages we should remove.</param>
		internal virtual void RemoveAllMessagesForDatanode(DatanodeDescriptor dn)
		{
			foreach (KeyValuePair<Block, Queue<PendingDataNodeMessages.ReportedBlockInfo>> entry
				 in queueByBlockId)
			{
				Queue<PendingDataNodeMessages.ReportedBlockInfo> newQueue = Lists.NewLinkedList();
				Queue<PendingDataNodeMessages.ReportedBlockInfo> oldQueue = entry.Value;
				while (!oldQueue.IsEmpty())
				{
					PendingDataNodeMessages.ReportedBlockInfo rbi = oldQueue.Remove();
					if (!rbi.GetStorageInfo().GetDatanodeDescriptor().Equals(dn))
					{
						newQueue.AddItem(rbi);
					}
					else
					{
						count--;
					}
				}
				queueByBlockId[entry.Key] = newQueue;
			}
		}

		internal virtual void EnqueueReportedBlock(DatanodeStorageInfo storageInfo, Block
			 block, HdfsServerConstants.ReplicaState reportedState)
		{
			block = new Block(block);
			GetBlockQueue(block).AddItem(new PendingDataNodeMessages.ReportedBlockInfo(storageInfo
				, block, reportedState));
			count++;
		}

		/// <returns>
		/// any messages that were previously queued for the given block,
		/// or null if no messages were queued.
		/// </returns>
		internal virtual Queue<PendingDataNodeMessages.ReportedBlockInfo> TakeBlockQueue(
			Block block)
		{
			Queue<PendingDataNodeMessages.ReportedBlockInfo> queue = Sharpen.Collections.Remove
				(queueByBlockId, block);
			if (queue != null)
			{
				count -= queue.Count;
			}
			return queue;
		}

		private Queue<PendingDataNodeMessages.ReportedBlockInfo> GetBlockQueue(Block block
			)
		{
			Queue<PendingDataNodeMessages.ReportedBlockInfo> queue = queueByBlockId[block];
			if (queue == null)
			{
				queue = Lists.NewLinkedList();
				queueByBlockId[block] = queue;
			}
			return queue;
		}

		internal virtual int Count()
		{
			return count;
		}

		public override string ToString()
		{
			StringBuilder sb = new StringBuilder();
			foreach (KeyValuePair<Block, Queue<PendingDataNodeMessages.ReportedBlockInfo>> entry
				 in queueByBlockId)
			{
				sb.Append("Block " + entry.Key + ":\n");
				foreach (PendingDataNodeMessages.ReportedBlockInfo rbi in entry.Value)
				{
					sb.Append("  ").Append(rbi).Append("\n");
				}
			}
			return sb.ToString();
		}

		internal virtual IEnumerable<PendingDataNodeMessages.ReportedBlockInfo> TakeAll()
		{
			IList<PendingDataNodeMessages.ReportedBlockInfo> rbis = Lists.NewArrayListWithCapacity
				(count);
			foreach (Queue<PendingDataNodeMessages.ReportedBlockInfo> q in queueByBlockId.Values)
			{
				Sharpen.Collections.AddAll(rbis, q);
			}
			queueByBlockId.Clear();
			count = 0;
			return rbis;
		}
	}
}
