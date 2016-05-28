using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Annotations;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Util;
using Org.Apache.Hadoop.Util;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Blockmanagement
{
	/// <summary>
	/// Keeps a Collection for every named machine containing blocks
	/// that have recently been invalidated and are thought to live
	/// on the machine in question.
	/// </summary>
	internal class InvalidateBlocks
	{
		/// <summary>Mapping: DatanodeInfo -&gt; Collection of Blocks</summary>
		private readonly IDictionary<DatanodeInfo, LightWeightHashSet<Block>> node2blocks
			 = new SortedDictionary<DatanodeInfo, LightWeightHashSet<Block>>();

		/// <summary>The total number of blocks in the map.</summary>
		private long numBlocks = 0L;

		private readonly int blockInvalidateLimit;

		/// <summary>
		/// The period of pending time for block invalidation since the NameNode
		/// startup
		/// </summary>
		private readonly long pendingPeriodInMs;

		/// <summary>the startup time</summary>
		private readonly long startupTime = Time.MonotonicNow();

		internal InvalidateBlocks(int blockInvalidateLimit, long pendingPeriodInMs)
		{
			this.blockInvalidateLimit = blockInvalidateLimit;
			this.pendingPeriodInMs = pendingPeriodInMs;
			PrintBlockDeletionTime(BlockManager.Log);
		}

		private void PrintBlockDeletionTime(Logger log)
		{
			log.Info(DFSConfigKeys.DfsNamenodeStartupDelayBlockDeletionSecKey + " is set to "
				 + DFSUtil.DurationToString(pendingPeriodInMs));
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy MMM dd HH:mm:ss");
			Calendar calendar = new GregorianCalendar();
			calendar.Add(Calendar.Second, (int)(this.pendingPeriodInMs / 1000));
			log.Info("The block deletion will start around " + sdf.Format(calendar.GetTime())
				);
		}

		/// <returns>the number of blocks to be invalidated .</returns>
		internal virtual long NumBlocks()
		{
			lock (this)
			{
				return numBlocks;
			}
		}

		/// <returns>
		/// true if the given storage has the given block listed for
		/// invalidation. Blocks are compared including their generation stamps:
		/// if a block is pending invalidation but with a different generation stamp,
		/// returns false.
		/// </returns>
		internal virtual bool Contains(DatanodeInfo dn, Block block)
		{
			lock (this)
			{
				LightWeightHashSet<Block> s = node2blocks[dn];
				if (s == null)
				{
					return false;
				}
				// no invalidate blocks for this storage ID
				Block blockInSet = s.GetElement(block);
				return blockInSet != null && block.GetGenerationStamp() == blockInSet.GetGenerationStamp
					();
			}
		}

		/// <summary>
		/// Add a block to the block collection
		/// which will be invalidated on the specified datanode.
		/// </summary>
		internal virtual void Add(Block block, DatanodeInfo datanode, bool log)
		{
			lock (this)
			{
				LightWeightHashSet<Block> set = node2blocks[datanode];
				if (set == null)
				{
					set = new LightWeightHashSet<Block>();
					node2blocks[datanode] = set;
				}
				if (set.AddItem(block))
				{
					numBlocks++;
					if (log)
					{
						NameNode.blockStateChangeLog.Info("BLOCK* {}: add {} to {}", GetType().Name, block
							, datanode);
					}
				}
			}
		}

		/// <summary>Remove a storage from the invalidatesSet</summary>
		internal virtual void Remove(DatanodeInfo dn)
		{
			lock (this)
			{
				LightWeightHashSet<Block> blocks = Sharpen.Collections.Remove(node2blocks, dn);
				if (blocks != null)
				{
					numBlocks -= blocks.Count;
				}
			}
		}

		/// <summary>Remove the block from the specified storage.</summary>
		internal virtual void Remove(DatanodeInfo dn, Block block)
		{
			lock (this)
			{
				LightWeightHashSet<Block> v = node2blocks[dn];
				if (v != null && v.Remove(block))
				{
					numBlocks--;
					if (v.IsEmpty())
					{
						Sharpen.Collections.Remove(node2blocks, dn);
					}
				}
			}
		}

		/// <summary>Print the contents to out.</summary>
		internal virtual void Dump(PrintWriter @out)
		{
			lock (this)
			{
				int size = node2blocks.Values.Count;
				@out.WriteLine("Metasave: Blocks " + numBlocks + " waiting deletion from " + size
					 + " datanodes.");
				if (size == 0)
				{
					return;
				}
				foreach (KeyValuePair<DatanodeInfo, LightWeightHashSet<Block>> entry in node2blocks)
				{
					LightWeightHashSet<Block> blocks = entry.Value;
					if (blocks.Count > 0)
					{
						@out.WriteLine(entry.Key);
						@out.WriteLine(blocks);
					}
				}
			}
		}

		/// <returns>a list of the storage IDs.</returns>
		internal virtual IList<DatanodeInfo> GetDatanodes()
		{
			lock (this)
			{
				return new AList<DatanodeInfo>(node2blocks.Keys);
			}
		}

		/// <returns>the remianing pending time</returns>
		[VisibleForTesting]
		internal virtual long GetInvalidationDelay()
		{
			return pendingPeriodInMs - (Time.MonotonicNow() - startupTime);
		}

		internal virtual IList<Block> InvalidateWork(DatanodeDescriptor dn)
		{
			lock (this)
			{
				long delay = GetInvalidationDelay();
				if (delay > 0)
				{
					if (BlockManager.Log.IsDebugEnabled())
					{
						BlockManager.Log.Debug("Block deletion is delayed during NameNode startup. " + "The deletion will start after "
							 + delay + " ms.");
					}
					return null;
				}
				LightWeightHashSet<Block> set = node2blocks[dn];
				if (set == null)
				{
					return null;
				}
				// # blocks that can be sent in one message is limited
				int limit = blockInvalidateLimit;
				IList<Block> toInvalidate = set.PollN(limit);
				// If we send everything in this message, remove this node entry
				if (set.IsEmpty())
				{
					Remove(dn);
				}
				dn.AddBlocksToBeInvalidated(toInvalidate);
				numBlocks -= toInvalidate.Count;
				return toInvalidate;
			}
		}

		internal virtual void Clear()
		{
			lock (this)
			{
				node2blocks.Clear();
				numBlocks = 0;
			}
		}
	}
}
