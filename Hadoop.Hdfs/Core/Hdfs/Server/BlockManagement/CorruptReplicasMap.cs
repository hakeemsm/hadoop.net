using System.Collections.Generic;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Blockmanagement
{
	/// <summary>Stores information about all corrupt blocks in the File System.</summary>
	/// <remarks>
	/// Stores information about all corrupt blocks in the File System.
	/// A Block is considered corrupt only if all of its replicas are
	/// corrupt. While reporting replicas of a Block, we hide any corrupt
	/// copies. These copies are removed once Block is found to have
	/// expected number of good replicas.
	/// Mapping: Block -&gt; TreeSet<DatanodeDescriptor>
	/// </remarks>
	public class CorruptReplicasMap
	{
		/// <summary>The corruption reason code</summary>
		public enum Reason
		{
			None,
			Any,
			GenstampMismatch,
			SizeMismatch,
			InvalidState,
			CorruptionReported
		}

		private readonly SortedDictionary<Block, IDictionary<DatanodeDescriptor, CorruptReplicasMap.Reason
			>> corruptReplicasMap = new SortedDictionary<Block, IDictionary<DatanodeDescriptor
			, CorruptReplicasMap.Reason>>();

		// not specified.
		// wildcard reason
		// mismatch in generation stamps
		// mismatch in sizes
		// invalid state
		// client or datanode reported the corruption
		/// <summary>Mark the block belonging to datanode as corrupt.</summary>
		/// <param name="blk">Block to be added to CorruptReplicasMap</param>
		/// <param name="dn">DatanodeDescriptor which holds the corrupt replica</param>
		/// <param name="reason">a textual reason (for logging purposes)</param>
		/// <param name="reasonCode">the enum representation of the reason</param>
		internal virtual void AddToCorruptReplicasMap(Block blk, DatanodeDescriptor dn, string
			 reason, CorruptReplicasMap.Reason reasonCode)
		{
			IDictionary<DatanodeDescriptor, CorruptReplicasMap.Reason> nodes = corruptReplicasMap
				[blk];
			if (nodes == null)
			{
				nodes = new Dictionary<DatanodeDescriptor, CorruptReplicasMap.Reason>();
				corruptReplicasMap[blk] = nodes;
			}
			string reasonText;
			if (reason != null)
			{
				reasonText = " because " + reason;
			}
			else
			{
				reasonText = string.Empty;
			}
			if (!nodes.Keys.Contains(dn))
			{
				NameNode.blockStateChangeLog.Info("BLOCK NameSystem.addToCorruptReplicasMap: {} added as corrupt on "
					 + "{} by {} {}", blk.GetBlockName(), dn, Org.Apache.Hadoop.Ipc.Server.GetRemoteIp
					(), reasonText);
			}
			else
			{
				NameNode.blockStateChangeLog.Info("BLOCK NameSystem.addToCorruptReplicasMap: duplicate requested for"
					 + " {} to add as corrupt on {} by {} {}", blk.GetBlockName(), dn, Org.Apache.Hadoop.Ipc.Server
					.GetRemoteIp(), reasonText);
			}
			// Add the node or update the reason.
			nodes[dn] = reasonCode;
		}

		/// <summary>Remove Block from CorruptBlocksMap</summary>
		/// <param name="blk">Block to be removed</param>
		internal virtual void RemoveFromCorruptReplicasMap(Block blk)
		{
			if (corruptReplicasMap != null)
			{
				Sharpen.Collections.Remove(corruptReplicasMap, blk);
			}
		}

		/// <summary>Remove the block at the given datanode from CorruptBlockMap</summary>
		/// <param name="blk">block to be removed</param>
		/// <param name="datanode">datanode where the block is located</param>
		/// <returns>
		/// true if the removal is successful;
		/// false if the replica is not in the map
		/// </returns>
		internal virtual bool RemoveFromCorruptReplicasMap(Block blk, DatanodeDescriptor 
			datanode)
		{
			return RemoveFromCorruptReplicasMap(blk, datanode, CorruptReplicasMap.Reason.Any);
		}

		internal virtual bool RemoveFromCorruptReplicasMap(Block blk, DatanodeDescriptor 
			datanode, CorruptReplicasMap.Reason reason)
		{
			IDictionary<DatanodeDescriptor, CorruptReplicasMap.Reason> datanodes = corruptReplicasMap
				[blk];
			if (datanodes == null)
			{
				return false;
			}
			// if reasons can be compared but don't match, return false.
			CorruptReplicasMap.Reason storedReason = datanodes[datanode];
			if (reason != CorruptReplicasMap.Reason.Any && storedReason != null && reason != 
				storedReason)
			{
				return false;
			}
			if (Sharpen.Collections.Remove(datanodes, datanode) != null)
			{
				// remove the replicas
				if (datanodes.IsEmpty())
				{
					// remove the block if there is no more corrupted replicas
					Sharpen.Collections.Remove(corruptReplicasMap, blk);
				}
				return true;
			}
			return false;
		}

		/// <summary>Get Nodes which have corrupt replicas of Block</summary>
		/// <param name="blk">Block for which nodes are requested</param>
		/// <returns>collection of nodes. Null if does not exists</returns>
		internal virtual ICollection<DatanodeDescriptor> GetNodes(Block blk)
		{
			IDictionary<DatanodeDescriptor, CorruptReplicasMap.Reason> nodes = corruptReplicasMap
				[blk];
			if (nodes == null)
			{
				return null;
			}
			return nodes.Keys;
		}

		/// <summary>Check if replica belonging to Datanode is corrupt</summary>
		/// <param name="blk">Block to check</param>
		/// <param name="node">DatanodeDescriptor which holds the replica</param>
		/// <returns>true if replica is corrupt, false if does not exists in this map</returns>
		internal virtual bool IsReplicaCorrupt(Block blk, DatanodeDescriptor node)
		{
			ICollection<DatanodeDescriptor> nodes = GetNodes(blk);
			return ((nodes != null) && (nodes.Contains(node)));
		}

		internal virtual int NumCorruptReplicas(Block blk)
		{
			ICollection<DatanodeDescriptor> nodes = GetNodes(blk);
			return (nodes == null) ? 0 : nodes.Count;
		}

		internal virtual int Size()
		{
			return corruptReplicasMap.Count;
		}

		/// <summary>Return a range of corrupt replica block ids.</summary>
		/// <remarks>
		/// Return a range of corrupt replica block ids. Up to numExpectedBlocks
		/// blocks starting at the next block after startingBlockId are returned
		/// (fewer if numExpectedBlocks blocks are unavailable). If startingBlockId
		/// is null, up to numExpectedBlocks blocks are returned from the beginning.
		/// If startingBlockId cannot be found, null is returned.
		/// </remarks>
		/// <param name="numExpectedBlocks">
		/// Number of block ids to return.
		/// 0 &lt;= numExpectedBlocks &lt;= 100
		/// </param>
		/// <param name="startingBlockId">
		/// Block id from which to start. If null, start at
		/// beginning.
		/// </param>
		/// <returns>Up to numExpectedBlocks blocks from startingBlockId if it exists</returns>
		internal virtual long[] GetCorruptReplicaBlockIds(int numExpectedBlocks, long startingBlockId
			)
		{
			if (numExpectedBlocks < 0 || numExpectedBlocks > 100)
			{
				return null;
			}
			IEnumerator<Block> blockIt = corruptReplicasMap.Keys.GetEnumerator();
			// if the starting block id was specified, iterate over keys until
			// we find the matching block. If we find a matching block, break
			// to leave the iterator on the next block after the specified block. 
			if (startingBlockId != null)
			{
				bool isBlockFound = false;
				while (blockIt.HasNext())
				{
					Block b = blockIt.Next();
					if (b.GetBlockId() == startingBlockId)
					{
						isBlockFound = true;
						break;
					}
				}
				if (!isBlockFound)
				{
					return null;
				}
			}
			AList<long> corruptReplicaBlockIds = new AList<long>();
			// append up to numExpectedBlocks blockIds to our list
			for (int i = 0; i < numExpectedBlocks && blockIt.HasNext(); i++)
			{
				corruptReplicaBlockIds.AddItem(blockIt.Next().GetBlockId());
			}
			long[] ret = new long[corruptReplicaBlockIds.Count];
			for (int i_1 = 0; i_1 < ret.Length; i_1++)
			{
				ret[i_1] = corruptReplicaBlockIds[i_1];
			}
			return ret;
		}

		/// <summary>
		/// return the reason about corrupted replica for a given block
		/// on a given dn
		/// </summary>
		/// <param name="block">block that has corrupted replica</param>
		/// <param name="node">datanode that contains this corrupted replica</param>
		/// <returns>reason</returns>
		internal virtual string GetCorruptReason(Block block, DatanodeDescriptor node)
		{
			CorruptReplicasMap.Reason reason = null;
			if (corruptReplicasMap.Contains(block))
			{
				if (corruptReplicasMap[block].Contains(node))
				{
					reason = corruptReplicasMap[block][node];
				}
			}
			if (reason != null)
			{
				return reason.ToString();
			}
			else
			{
				return null;
			}
		}
	}
}
