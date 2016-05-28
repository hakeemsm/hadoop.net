using System;
using System.Collections.Generic;
using System.IO;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Util;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Blockmanagement
{
	/// <summary>
	/// PendingReplicationBlocks does the bookkeeping of all
	/// blocks that are getting replicated.
	/// </summary>
	/// <remarks>
	/// PendingReplicationBlocks does the bookkeeping of all
	/// blocks that are getting replicated.
	/// It does the following:
	/// 1)  record blocks that are getting replicated at this instant.
	/// 2)  a coarse grain timer to track age of replication request
	/// 3)  a thread that periodically identifies replication-requests
	/// that never made it.
	/// </remarks>
	internal class PendingReplicationBlocks
	{
		private static readonly Logger Log = BlockManager.Log;

		private readonly IDictionary<Block, PendingReplicationBlocks.PendingBlockInfo> pendingReplications;

		private readonly AList<Block> timedOutItems;

		internal Daemon timerThread = null;

		private volatile bool fsRunning = true;

		private long timeout = 5 * 60 * 1000;

		private const long DefaultRecheckInterval = 5 * 60 * 1000;

		internal PendingReplicationBlocks(long timeoutPeriod)
		{
			//
			// It might take anywhere between 5 to 10 minutes before
			// a request is timed out.
			//
			if (timeoutPeriod > 0)
			{
				this.timeout = timeoutPeriod;
			}
			pendingReplications = new Dictionary<Block, PendingReplicationBlocks.PendingBlockInfo
				>();
			timedOutItems = new AList<Block>();
		}

		internal virtual void Start()
		{
			timerThread = new Daemon(new PendingReplicationBlocks.PendingReplicationMonitor(this
				));
			timerThread.Start();
		}

		/// <summary>Add a block to the list of pending Replications</summary>
		/// <param name="block">The corresponding block</param>
		/// <param name="targets">The DataNodes where replicas of the block should be placed</param>
		internal virtual void Increment(Block block, DatanodeDescriptor[] targets)
		{
			lock (pendingReplications)
			{
				PendingReplicationBlocks.PendingBlockInfo found = pendingReplications[block];
				if (found == null)
				{
					pendingReplications[block] = new PendingReplicationBlocks.PendingBlockInfo(targets
						);
				}
				else
				{
					found.IncrementReplicas(targets);
					found.SetTimeStamp();
				}
			}
		}

		/// <summary>One replication request for this block has finished.</summary>
		/// <remarks>
		/// One replication request for this block has finished.
		/// Decrement the number of pending replication requests
		/// for this block.
		/// </remarks>
		/// <param name="The">DataNode that finishes the replication</param>
		internal virtual void Decrement(Block block, DatanodeDescriptor dn)
		{
			lock (pendingReplications)
			{
				PendingReplicationBlocks.PendingBlockInfo found = pendingReplications[block];
				if (found != null)
				{
					if (Log.IsDebugEnabled())
					{
						Log.Debug("Removing pending replication for " + block);
					}
					found.DecrementReplicas(dn);
					if (found.GetNumReplicas() <= 0)
					{
						Sharpen.Collections.Remove(pendingReplications, block);
					}
				}
			}
		}

		/// <summary>Remove the record about the given block from pendingReplications.</summary>
		/// <param name="block">
		/// The given block whose pending replication requests need to be
		/// removed
		/// </param>
		internal virtual void Remove(Block block)
		{
			lock (pendingReplications)
			{
				Sharpen.Collections.Remove(pendingReplications, block);
			}
		}

		public virtual void Clear()
		{
			lock (pendingReplications)
			{
				pendingReplications.Clear();
				timedOutItems.Clear();
			}
		}

		/// <summary>The total number of blocks that are undergoing replication</summary>
		internal virtual int Size()
		{
			return pendingReplications.Count;
		}

		/// <summary>How many copies of this block is pending replication?</summary>
		internal virtual int GetNumReplicas(Block block)
		{
			lock (pendingReplications)
			{
				PendingReplicationBlocks.PendingBlockInfo found = pendingReplications[block];
				if (found != null)
				{
					return found.GetNumReplicas();
				}
			}
			return 0;
		}

		/// <summary>
		/// Returns a list of blocks that have timed out their
		/// replication requests.
		/// </summary>
		/// <remarks>
		/// Returns a list of blocks that have timed out their
		/// replication requests. Returns null if no blocks have
		/// timed out.
		/// </remarks>
		internal virtual Block[] GetTimedOutBlocks()
		{
			lock (timedOutItems)
			{
				if (timedOutItems.Count <= 0)
				{
					return null;
				}
				Block[] blockList = Sharpen.Collections.ToArray(timedOutItems, new Block[timedOutItems
					.Count]);
				timedOutItems.Clear();
				return blockList;
			}
		}

		/// <summary>
		/// An object that contains information about a block that
		/// is being replicated.
		/// </summary>
		/// <remarks>
		/// An object that contains information about a block that
		/// is being replicated. It records the timestamp when the
		/// system started replicating the most recent copy of this
		/// block. It also records the list of Datanodes where the
		/// replication requests are in progress.
		/// </remarks>
		internal class PendingBlockInfo
		{
			private long timeStamp;

			private readonly IList<DatanodeDescriptor> targets;

			internal PendingBlockInfo(DatanodeDescriptor[] targets)
			{
				this.timeStamp = Time.MonotonicNow();
				this.targets = targets == null ? new AList<DatanodeDescriptor>() : new AList<DatanodeDescriptor
					>(Arrays.AsList(targets));
			}

			internal virtual long GetTimeStamp()
			{
				return timeStamp;
			}

			internal virtual void SetTimeStamp()
			{
				timeStamp = Time.MonotonicNow();
			}

			internal virtual void IncrementReplicas(params DatanodeDescriptor[] newTargets)
			{
				if (newTargets != null)
				{
					foreach (DatanodeDescriptor dn in newTargets)
					{
						targets.AddItem(dn);
					}
				}
			}

			internal virtual void DecrementReplicas(DatanodeDescriptor dn)
			{
				targets.Remove(dn);
			}

			internal virtual int GetNumReplicas()
			{
				return targets.Count;
			}
		}

		internal class PendingReplicationMonitor : Runnable
		{
			/*
			* A periodic thread that scans for blocks that never finished
			* their replication request.
			*/
			public virtual void Run()
			{
				while (this._enclosing.fsRunning)
				{
					long period = Math.Min(PendingReplicationBlocks.DefaultRecheckInterval, this._enclosing
						.timeout);
					try
					{
						this.PendingReplicationCheck();
						Sharpen.Thread.Sleep(period);
					}
					catch (Exception ie)
					{
						if (PendingReplicationBlocks.Log.IsDebugEnabled())
						{
							PendingReplicationBlocks.Log.Debug("PendingReplicationMonitor thread is interrupted."
								, ie);
						}
					}
				}
			}

			/// <summary>Iterate through all items and detect timed-out items</summary>
			internal virtual void PendingReplicationCheck()
			{
				lock (this._enclosing.pendingReplications)
				{
					IEnumerator<KeyValuePair<Block, PendingReplicationBlocks.PendingBlockInfo>> iter = 
						this._enclosing.pendingReplications.GetEnumerator();
					long now = Time.MonotonicNow();
					if (PendingReplicationBlocks.Log.IsDebugEnabled())
					{
						PendingReplicationBlocks.Log.Debug("PendingReplicationMonitor checking Q");
					}
					while (iter.HasNext())
					{
						KeyValuePair<Block, PendingReplicationBlocks.PendingBlockInfo> entry = iter.Next(
							);
						PendingReplicationBlocks.PendingBlockInfo pendingBlock = entry.Value;
						if (now > pendingBlock.GetTimeStamp() + this._enclosing.timeout)
						{
							Block block = entry.Key;
							lock (this._enclosing.timedOutItems)
							{
								this._enclosing.timedOutItems.AddItem(block);
							}
							PendingReplicationBlocks.Log.Warn("PendingReplicationMonitor timed out " + block);
							iter.Remove();
						}
					}
				}
			}

			internal PendingReplicationMonitor(PendingReplicationBlocks _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly PendingReplicationBlocks _enclosing;
		}

		/*
		* Shuts down the pending replication monitor thread.
		* Waits for the thread to exit.
		*/
		internal virtual void Stop()
		{
			fsRunning = false;
			if (timerThread == null)
			{
				return;
			}
			timerThread.Interrupt();
			try
			{
				timerThread.Join(3000);
			}
			catch (Exception)
			{
			}
		}

		/// <summary>Iterate through all items and print them.</summary>
		internal virtual void MetaSave(PrintWriter @out)
		{
			lock (pendingReplications)
			{
				@out.WriteLine("Metasave: Blocks being replicated: " + pendingReplications.Count);
				IEnumerator<KeyValuePair<Block, PendingReplicationBlocks.PendingBlockInfo>> iter = 
					pendingReplications.GetEnumerator();
				while (iter.HasNext())
				{
					KeyValuePair<Block, PendingReplicationBlocks.PendingBlockInfo> entry = iter.Next(
						);
					PendingReplicationBlocks.PendingBlockInfo pendingBlock = entry.Value;
					Block block = entry.Key;
					@out.WriteLine(block + " StartTime: " + Sharpen.Extensions.CreateDate(pendingBlock
						.timeStamp) + " NumReplicaInProgress: " + pendingBlock.GetNumReplicas());
				}
			}
		}
	}
}
