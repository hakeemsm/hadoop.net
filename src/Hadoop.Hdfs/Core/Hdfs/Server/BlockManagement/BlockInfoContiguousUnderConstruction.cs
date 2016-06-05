using System.Collections.Generic;
using System.IO;
using System.Text;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Blockmanagement
{
	/// <summary>
	/// Represents a block that is currently being constructed.<br />
	/// This is usually the last block of a file opened for write or append.
	/// </summary>
	public class BlockInfoContiguousUnderConstruction : BlockInfoContiguous
	{
		/// <summary>Block state.</summary>
		/// <remarks>
		/// Block state. See
		/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Common.HdfsServerConstants.BlockUCState"
		/// 	/>
		/// 
		/// </remarks>
		private HdfsServerConstants.BlockUCState blockUCState;

		/// <summary>Block replicas as assigned when the block was allocated.</summary>
		/// <remarks>
		/// Block replicas as assigned when the block was allocated.
		/// This defines the pipeline order.
		/// </remarks>
		private IList<BlockInfoContiguousUnderConstruction.ReplicaUnderConstruction> replicas;

		/// <summary>Index of the primary data node doing the recovery.</summary>
		/// <remarks>
		/// Index of the primary data node doing the recovery. Useful for log
		/// messages.
		/// </remarks>
		private int primaryNodeIndex = -1;

		/// <summary>
		/// The new generation stamp, which this block will have
		/// after the recovery succeeds.
		/// </summary>
		/// <remarks>
		/// The new generation stamp, which this block will have
		/// after the recovery succeeds. Also used as a recovery id to identify
		/// the right recovery if any of the abandoned recoveries re-appear.
		/// </remarks>
		private long blockRecoveryId = 0;

		/// <summary>The block source to use in the event of copy-on-write truncate.</summary>
		private Block truncateBlock;

		/// <summary>
		/// ReplicaUnderConstruction contains information about replicas while
		/// they are under construction.
		/// </summary>
		/// <remarks>
		/// ReplicaUnderConstruction contains information about replicas while
		/// they are under construction.
		/// The GS, the length and the state of the replica is as reported by
		/// the data-node.
		/// It is not guaranteed, but expected, that data-nodes actually have
		/// corresponding replicas.
		/// </remarks>
		internal class ReplicaUnderConstruction : Block
		{
			private readonly DatanodeStorageInfo expectedLocation;

			private HdfsServerConstants.ReplicaState state;

			private bool chosenAsPrimary;

			internal ReplicaUnderConstruction(Block block, DatanodeStorageInfo target, HdfsServerConstants.ReplicaState
				 state)
				: base(block)
			{
				this.expectedLocation = target;
				this.state = state;
				this.chosenAsPrimary = false;
			}

			/// <summary>Expected block replica location as assigned when the block was allocated.
			/// 	</summary>
			/// <remarks>
			/// Expected block replica location as assigned when the block was allocated.
			/// This defines the pipeline order.
			/// It is not guaranteed, but expected, that the data-node actually has
			/// the replica.
			/// </remarks>
			private DatanodeStorageInfo GetExpectedStorageLocation()
			{
				return expectedLocation;
			}

			/// <summary>Get replica state as reported by the data-node.</summary>
			internal virtual HdfsServerConstants.ReplicaState GetState()
			{
				return state;
			}

			/// <summary>Whether the replica was chosen for recovery.</summary>
			internal virtual bool GetChosenAsPrimary()
			{
				return chosenAsPrimary;
			}

			/// <summary>Set replica state.</summary>
			internal virtual void SetState(HdfsServerConstants.ReplicaState s)
			{
				state = s;
			}

			/// <summary>Set whether this replica was chosen for recovery.</summary>
			internal virtual void SetChosenAsPrimary(bool chosenAsPrimary)
			{
				this.chosenAsPrimary = chosenAsPrimary;
			}

			/// <summary>Is data-node the replica belongs to alive.</summary>
			internal virtual bool IsAlive()
			{
				return expectedLocation.GetDatanodeDescriptor().isAlive;
			}

			public override int GetHashCode()
			{
				// Block
				return base.GetHashCode();
			}

			public override bool Equals(object obj)
			{
				// Block
				// Sufficient to rely on super's implementation
				return (this == obj) || base.Equals(obj);
			}

			public override string ToString()
			{
				StringBuilder b = new StringBuilder(50);
				AppendStringTo(b);
				return b.ToString();
			}

			public override void AppendStringTo(StringBuilder sb)
			{
				sb.Append("ReplicaUC[").Append(expectedLocation).Append("|").Append(state).Append
					("]");
			}
		}

		/// <summary>
		/// Create block and set its state to
		/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Common.HdfsServerConstants.BlockUCState.UnderConstruction
		/// 	"/>
		/// .
		/// </summary>
		public BlockInfoContiguousUnderConstruction(Block blk, short replication)
			: this(blk, replication, HdfsServerConstants.BlockUCState.UnderConstruction, null
				)
		{
		}

		/// <summary>Create a block that is currently being constructed.</summary>
		public BlockInfoContiguousUnderConstruction(Block blk, short replication, HdfsServerConstants.BlockUCState
			 state, DatanodeStorageInfo[] targets)
			: base(blk, replication)
		{
			System.Diagnostics.Debug.Assert(GetBlockUCState() != HdfsServerConstants.BlockUCState
				.Complete, "BlockInfoUnderConstruction cannot be in COMPLETE state");
			this.blockUCState = state;
			SetExpectedLocations(targets);
		}

		/// <summary>Convert an under construction block to a complete block.</summary>
		/// <returns>BlockInfo - a complete block.</returns>
		/// <exception cref="System.IO.IOException">
		/// if the state of the block
		/// (the generation stamp and the length) has not been committed by
		/// the client or it does not have at least a minimal number of replicas
		/// reported from data-nodes.
		/// </exception>
		internal virtual BlockInfoContiguous ConvertToCompleteBlock()
		{
			System.Diagnostics.Debug.Assert(GetBlockUCState() != HdfsServerConstants.BlockUCState
				.Complete, "Trying to convert a COMPLETE block");
			return new BlockInfoContiguous(this);
		}

		/// <summary>Set expected locations</summary>
		public virtual void SetExpectedLocations(DatanodeStorageInfo[] targets)
		{
			int numLocations = targets == null ? 0 : targets.Length;
			this.replicas = new AList<BlockInfoContiguousUnderConstruction.ReplicaUnderConstruction
				>(numLocations);
			for (int i = 0; i < numLocations; i++)
			{
				replicas.AddItem(new BlockInfoContiguousUnderConstruction.ReplicaUnderConstruction
					(this, targets[i], HdfsServerConstants.ReplicaState.Rbw));
			}
		}

		/// <summary>
		/// Create array of expected replica locations
		/// (as has been assigned by chooseTargets()).
		/// </summary>
		public virtual DatanodeStorageInfo[] GetExpectedStorageLocations()
		{
			int numLocations = replicas == null ? 0 : replicas.Count;
			DatanodeStorageInfo[] storages = new DatanodeStorageInfo[numLocations];
			for (int i = 0; i < numLocations; i++)
			{
				storages[i] = replicas[i].GetExpectedStorageLocation();
			}
			return storages;
		}

		/// <summary>Get the number of expected locations</summary>
		public virtual int GetNumExpectedLocations()
		{
			return replicas == null ? 0 : replicas.Count;
		}

		/// <summary>Return the state of the block under construction.</summary>
		/// <seealso cref="Org.Apache.Hadoop.Hdfs.Server.Common.HdfsServerConstants.BlockUCState
		/// 	"/>
		public override HdfsServerConstants.BlockUCState GetBlockUCState()
		{
			// BlockInfo
			return blockUCState;
		}

		internal virtual void SetBlockUCState(HdfsServerConstants.BlockUCState s)
		{
			blockUCState = s;
		}

		/// <summary>Get block recovery ID</summary>
		public virtual long GetBlockRecoveryId()
		{
			return blockRecoveryId;
		}

		/// <summary>Get recover block</summary>
		public virtual Block GetTruncateBlock()
		{
			return truncateBlock;
		}

		public virtual void SetTruncateBlock(Block recoveryBlock)
		{
			this.truncateBlock = recoveryBlock;
		}

		/// <summary>Process the recorded replicas.</summary>
		/// <remarks>
		/// Process the recorded replicas. When about to commit or finish the
		/// pipeline recovery sort out bad replicas.
		/// </remarks>
		/// <param name="genStamp">The final generation stamp for the block.</param>
		public virtual void SetGenerationStampAndVerifyReplicas(long genStamp)
		{
			// Set the generation stamp for the block.
			SetGenerationStamp(genStamp);
			if (replicas == null)
			{
				return;
			}
			// Remove the replicas with wrong gen stamp.
			// The replica list is unchanged.
			foreach (BlockInfoContiguousUnderConstruction.ReplicaUnderConstruction r in replicas)
			{
				if (genStamp != r.GetGenerationStamp())
				{
					r.GetExpectedStorageLocation().RemoveBlock(this);
					NameNode.blockStateChangeLog.Info("BLOCK* Removing stale replica " + "from location: {}"
						, r.GetExpectedStorageLocation());
				}
			}
		}

		/// <summary>Commit block's length and generation stamp as reported by the client.</summary>
		/// <remarks>
		/// Commit block's length and generation stamp as reported by the client.
		/// Set block state to
		/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Common.HdfsServerConstants.BlockUCState.Committed
		/// 	"/>
		/// .
		/// </remarks>
		/// <param name="block">- contains client reported block length and generation</param>
		/// <exception cref="System.IO.IOException">if block ids are inconsistent.</exception>
		internal virtual void CommitBlock(Block block)
		{
			if (GetBlockId() != block.GetBlockId())
			{
				throw new IOException("Trying to commit inconsistent block: id = " + block.GetBlockId
					() + ", expected id = " + GetBlockId());
			}
			blockUCState = HdfsServerConstants.BlockUCState.Committed;
			this.SetNumBytes(block.GetNumBytes());
			// Sort out invalid replicas.
			SetGenerationStampAndVerifyReplicas(block.GetGenerationStamp());
		}

		/// <summary>Initialize lease recovery for this block.</summary>
		/// <remarks>
		/// Initialize lease recovery for this block.
		/// Find the first alive data-node starting from the previous primary and
		/// make it primary.
		/// </remarks>
		public virtual void InitializeBlockRecovery(long recoveryId)
		{
			SetBlockUCState(HdfsServerConstants.BlockUCState.UnderRecovery);
			blockRecoveryId = recoveryId;
			if (replicas.Count == 0)
			{
				NameNode.blockStateChangeLog.Warn("BLOCK*" + " BlockInfoUnderConstruction.initLeaseRecovery:"
					 + " No blocks found, lease removed.");
			}
			bool allLiveReplicasTriedAsPrimary = true;
			for (int i = 0; i < replicas.Count; i++)
			{
				// Check if all replicas have been tried or not.
				if (replicas[i].IsAlive())
				{
					allLiveReplicasTriedAsPrimary = (allLiveReplicasTriedAsPrimary && replicas[i].GetChosenAsPrimary
						());
				}
			}
			if (allLiveReplicasTriedAsPrimary)
			{
				// Just set all the replicas to be chosen whether they are alive or not.
				for (int i_1 = 0; i_1 < replicas.Count; i_1++)
				{
					replicas[i_1].SetChosenAsPrimary(false);
				}
			}
			long mostRecentLastUpdate = 0;
			BlockInfoContiguousUnderConstruction.ReplicaUnderConstruction primary = null;
			primaryNodeIndex = -1;
			for (int i_2 = 0; i_2 < replicas.Count; i_2++)
			{
				// Skip alive replicas which have been chosen for recovery.
				if (!(replicas[i_2].IsAlive() && !replicas[i_2].GetChosenAsPrimary()))
				{
					continue;
				}
				BlockInfoContiguousUnderConstruction.ReplicaUnderConstruction ruc = replicas[i_2];
				long lastUpdate = ruc.GetExpectedStorageLocation().GetDatanodeDescriptor().GetLastUpdateMonotonic
					();
				if (lastUpdate > mostRecentLastUpdate)
				{
					primaryNodeIndex = i_2;
					primary = ruc;
					mostRecentLastUpdate = lastUpdate;
				}
			}
			if (primary != null)
			{
				primary.GetExpectedStorageLocation().GetDatanodeDescriptor().AddBlockToBeRecovered
					(this);
				primary.SetChosenAsPrimary(true);
				NameNode.blockStateChangeLog.Info("BLOCK* {} recovery started, primary={}", this, 
					primary);
			}
		}

		internal virtual void AddReplicaIfNotPresent(DatanodeStorageInfo storage, Block block
			, HdfsServerConstants.ReplicaState rState)
		{
			IEnumerator<BlockInfoContiguousUnderConstruction.ReplicaUnderConstruction> it = replicas
				.GetEnumerator();
			while (it.HasNext())
			{
				BlockInfoContiguousUnderConstruction.ReplicaUnderConstruction r = it.Next();
				DatanodeStorageInfo expectedLocation = r.GetExpectedStorageLocation();
				if (expectedLocation == storage)
				{
					// Record the gen stamp from the report
					r.SetGenerationStamp(block.GetGenerationStamp());
					return;
				}
				else
				{
					if (expectedLocation != null && expectedLocation.GetDatanodeDescriptor() == storage
						.GetDatanodeDescriptor())
					{
						// The Datanode reported that the block is on a different storage
						// than the one chosen by BlockPlacementPolicy. This can occur as
						// we allow Datanodes to choose the target storage. Update our
						// state by removing the stale entry and adding a new one.
						it.Remove();
						break;
					}
				}
			}
			replicas.AddItem(new BlockInfoContiguousUnderConstruction.ReplicaUnderConstruction
				(block, storage, rState));
		}

		public override int GetHashCode()
		{
			// BlockInfo
			// BlockInfoUnderConstruction participates in maps the same way as BlockInfo
			return base.GetHashCode();
		}

		public override bool Equals(object obj)
		{
			// BlockInfo
			// Sufficient to rely on super's implementation
			return (this == obj) || base.Equals(obj);
		}

		public override string ToString()
		{
			StringBuilder b = new StringBuilder(100);
			AppendStringTo(b);
			return b.ToString();
		}

		public override void AppendStringTo(StringBuilder sb)
		{
			base.AppendStringTo(sb);
			AppendUCParts(sb);
		}

		private void AppendUCParts(StringBuilder sb)
		{
			sb.Append("{UCState=").Append(blockUCState).Append(", truncateBlock=" + truncateBlock
				).Append(", primaryNodeIndex=").Append(primaryNodeIndex).Append(", replicas=[");
			if (replicas != null)
			{
				IEnumerator<BlockInfoContiguousUnderConstruction.ReplicaUnderConstruction> iter = 
					replicas.GetEnumerator();
				if (iter.HasNext())
				{
					iter.Next().AppendStringTo(sb);
					while (iter.HasNext())
					{
						sb.Append(", ");
						iter.Next().AppendStringTo(sb);
					}
				}
			}
			sb.Append("]}");
		}
	}
}
