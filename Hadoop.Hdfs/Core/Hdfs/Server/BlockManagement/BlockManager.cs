using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Org.Apache.Hadoop;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Security.Token.Block;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Server.Namenode.Metrics;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.Hdfs.Util;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Util;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Blockmanagement
{
	/// <summary>Keeps information related to the blocks stored in the Hadoop cluster.</summary>
	public class BlockManager
	{
		public static readonly Logger Log = LoggerFactory.GetLogger(typeof(Org.Apache.Hadoop.Hdfs.Server.Blockmanagement.BlockManager
			));

		public static readonly Logger blockLog = NameNode.blockStateChangeLog;

		private const string QueueReasonCorruptState = "it has the wrong state or generation stamp";

		private const string QueueReasonFutureGenstamp = "generation stamp is in the future";

		private readonly Namesystem namesystem;

		private readonly DatanodeManager datanodeManager;

		private readonly HeartbeatManager heartbeatManager;

		private readonly BlockTokenSecretManager blockTokenSecretManager;

		private readonly PendingDataNodeMessages pendingDNMessages = new PendingDataNodeMessages
			();

		private volatile long pendingReplicationBlocksCount = 0L;

		private volatile long corruptReplicaBlocksCount = 0L;

		private volatile long underReplicatedBlocksCount = 0L;

		private volatile long scheduledReplicationBlocksCount = 0L;

		private readonly AtomicLong excessBlocksCount = new AtomicLong(0L);

		private readonly AtomicLong postponedMisreplicatedBlocksCount = new AtomicLong(0L
			);

		private readonly long startupDelayBlockDeletionInMs;

		/// <summary>Used by metrics</summary>
		public virtual long GetPendingReplicationBlocksCount()
		{
			return pendingReplicationBlocksCount;
		}

		/// <summary>Used by metrics</summary>
		public virtual long GetUnderReplicatedBlocksCount()
		{
			return underReplicatedBlocksCount;
		}

		/// <summary>Used by metrics</summary>
		public virtual long GetCorruptReplicaBlocksCount()
		{
			return corruptReplicaBlocksCount;
		}

		/// <summary>Used by metrics</summary>
		public virtual long GetScheduledReplicationBlocksCount()
		{
			return scheduledReplicationBlocksCount;
		}

		/// <summary>Used by metrics</summary>
		public virtual long GetPendingDeletionBlocksCount()
		{
			return invalidateBlocks.NumBlocks();
		}

		/// <summary>Used by metrics</summary>
		public virtual long GetStartupDelayBlockDeletionInMs()
		{
			return startupDelayBlockDeletionInMs;
		}

		/// <summary>Used by metrics</summary>
		public virtual long GetExcessBlocksCount()
		{
			return excessBlocksCount.Get();
		}

		/// <summary>Used by metrics</summary>
		public virtual long GetPostponedMisreplicatedBlocksCount()
		{
			return postponedMisreplicatedBlocksCount.Get();
		}

		/// <summary>Used by metrics</summary>
		public virtual int GetPendingDataNodeMessageCount()
		{
			return pendingDNMessages.Count();
		}

		/// <summary>replicationRecheckInterval is how often namenode checks for new replication work
		/// 	</summary>
		private readonly long replicationRecheckInterval;

		/// <summary>
		/// Mapping: Block -&gt; { BlockCollection, datanodes, self ref }
		/// Updated only in response to client-sent information.
		/// </summary>
		internal readonly BlocksMap blocksMap;

		/// <summary>Replication thread.</summary>
		internal readonly Daemon replicationThread;

		/// <summary>Store blocks -&gt; datanodedescriptor(s) map of corrupt replicas</summary>
		internal readonly CorruptReplicasMap corruptReplicas = new CorruptReplicasMap();

		/// <summary>Blocks to be invalidated.</summary>
		private readonly InvalidateBlocks invalidateBlocks;

		/// <summary>
		/// After a failover, over-replicated blocks may not be handled
		/// until all of the replicas have done a block report to the
		/// new active.
		/// </summary>
		/// <remarks>
		/// After a failover, over-replicated blocks may not be handled
		/// until all of the replicas have done a block report to the
		/// new active. This is to make sure that this NameNode has been
		/// notified of all block deletions that might have been pending
		/// when the failover happened.
		/// </remarks>
		private readonly ICollection<Org.Apache.Hadoop.Hdfs.Protocol.Block> postponedMisreplicatedBlocks
			 = Sets.NewHashSet();

		/// <summary>
		/// Maps a StorageID to the set of blocks that are "extra" for this
		/// DataNode.
		/// </summary>
		/// <remarks>
		/// Maps a StorageID to the set of blocks that are "extra" for this
		/// DataNode. We'll eventually remove these extras.
		/// </remarks>
		public readonly IDictionary<string, LightWeightLinkedSet<Org.Apache.Hadoop.Hdfs.Protocol.Block
			>> excessReplicateMap = new SortedDictionary<string, LightWeightLinkedSet<Org.Apache.Hadoop.Hdfs.Protocol.Block
			>>();

		/// <summary>Store set of Blocks that need to be replicated 1 or more times.</summary>
		/// <remarks>
		/// Store set of Blocks that need to be replicated 1 or more times.
		/// We also store pending replication-orders.
		/// </remarks>
		public readonly UnderReplicatedBlocks neededReplications = new UnderReplicatedBlocks
			();

		[VisibleForTesting]
		internal readonly PendingReplicationBlocks pendingReplications;

		/// <summary>The maximum number of replicas allowed for a block</summary>
		public readonly short maxReplication;

		/// <summary>
		/// The maximum number of outgoing replication streams a given node should have
		/// at one time considering all but the highest priority replications needed.
		/// </summary>
		internal int maxReplicationStreams;

		/// <summary>
		/// The maximum number of outgoing replication streams a given node should have
		/// at one time.
		/// </summary>
		internal int replicationStreamsHardLimit;

		/// <summary>Minimum copies needed or else write is disallowed</summary>
		public readonly short minReplication;

		/// <summary>Default number of replicas</summary>
		public readonly int defaultReplication;

		/// <summary>value returned by MAX_CORRUPT_FILES_RETURNED</summary>
		internal readonly int maxCorruptFilesReturned;

		internal readonly float blocksInvalidateWorkPct;

		internal readonly int blocksReplWorkMultiplier;

		internal readonly bool encryptDataTransfer;

		private readonly long maxNumBlocksToLog;

		/// <summary>
		/// When running inside a Standby node, the node may receive block reports
		/// from datanodes before receiving the corresponding namespace edits from
		/// the active NameNode.
		/// </summary>
		/// <remarks>
		/// When running inside a Standby node, the node may receive block reports
		/// from datanodes before receiving the corresponding namespace edits from
		/// the active NameNode. Thus, it will postpone them for later processing,
		/// instead of marking the blocks as corrupt.
		/// </remarks>
		private bool shouldPostponeBlocksFromFuture = false;

		/// <summary>
		/// Process replication queues asynchronously to allow namenode safemode exit
		/// and failover to be faster.
		/// </summary>
		/// <remarks>
		/// Process replication queues asynchronously to allow namenode safemode exit
		/// and failover to be faster. HDFS-5496
		/// </remarks>
		private Daemon replicationQueuesInitializer = null;

		/// <summary>
		/// Number of blocks to process asychronously for replication queues
		/// initialization once aquired the namesystem lock.
		/// </summary>
		/// <remarks>
		/// Number of blocks to process asychronously for replication queues
		/// initialization once aquired the namesystem lock. Remaining blocks will be
		/// processed again after aquiring lock again.
		/// </remarks>
		private int numBlocksPerIteration;

		/// <summary>Progress of the Replication queues initialisation.</summary>
		private double replicationQueuesInitProgress = 0.0;

		/// <summary>for block replicas placement</summary>
		private BlockPlacementPolicy blockplacement;

		private readonly BlockStoragePolicySuite storagePolicySuite;

		/// <summary>Check whether name system is running before terminating</summary>
		private bool checkNSRunning = true;

		/// <exception cref="System.IO.IOException"/>
		public BlockManager(Namesystem namesystem, Configuration conf)
		{
			replicationThread = new Daemon(new BlockManager.ReplicationMonitor(this));
			// whether or not to issue block encryption keys.
			// Max number of blocks to log info about during a block report.
			this.namesystem = namesystem;
			datanodeManager = new DatanodeManager(this, namesystem, conf);
			heartbeatManager = datanodeManager.GetHeartbeatManager();
			startupDelayBlockDeletionInMs = conf.GetLong(DFSConfigKeys.DfsNamenodeStartupDelayBlockDeletionSecKey
				, DFSConfigKeys.DfsNamenodeStartupDelayBlockDeletionSecDefault) * 1000L;
			invalidateBlocks = new InvalidateBlocks(datanodeManager.blockInvalidateLimit, startupDelayBlockDeletionInMs
				);
			// Compute the map capacity by allocating 2% of total memory
			blocksMap = new BlocksMap(LightWeightGSet.ComputeCapacity(2.0, "BlocksMap"));
			blockplacement = BlockPlacementPolicy.GetInstance(conf, datanodeManager.GetFSClusterStats
				(), datanodeManager.GetNetworkTopology(), datanodeManager.GetHost2DatanodeMap());
			storagePolicySuite = BlockStoragePolicySuite.CreateDefaultSuite();
			pendingReplications = new PendingReplicationBlocks(conf.GetInt(DFSConfigKeys.DfsNamenodeReplicationPendingTimeoutSecKey
				, DFSConfigKeys.DfsNamenodeReplicationPendingTimeoutSecDefault) * 1000L);
			blockTokenSecretManager = CreateBlockTokenSecretManager(conf);
			this.maxCorruptFilesReturned = conf.GetInt(DFSConfigKeys.DfsDefaultMaxCorruptFilesReturnedKey
				, DFSConfigKeys.DfsDefaultMaxCorruptFilesReturned);
			this.defaultReplication = conf.GetInt(DFSConfigKeys.DfsReplicationKey, DFSConfigKeys
				.DfsReplicationDefault);
			int maxR = conf.GetInt(DFSConfigKeys.DfsReplicationMaxKey, DFSConfigKeys.DfsReplicationMaxDefault
				);
			int minR = conf.GetInt(DFSConfigKeys.DfsNamenodeReplicationMinKey, DFSConfigKeys.
				DfsNamenodeReplicationMinDefault);
			if (minR <= 0)
			{
				throw new IOException("Unexpected configuration parameters: " + DFSConfigKeys.DfsNamenodeReplicationMinKey
					 + " = " + minR + " <= 0");
			}
			if (maxR > short.MaxValue)
			{
				throw new IOException("Unexpected configuration parameters: " + DFSConfigKeys.DfsReplicationMaxKey
					 + " = " + maxR + " > " + short.MaxValue);
			}
			if (minR > maxR)
			{
				throw new IOException("Unexpected configuration parameters: " + DFSConfigKeys.DfsNamenodeReplicationMinKey
					 + " = " + minR + " > " + DFSConfigKeys.DfsReplicationMaxKey + " = " + maxR);
			}
			this.minReplication = (short)minR;
			this.maxReplication = (short)maxR;
			this.maxReplicationStreams = conf.GetInt(DFSConfigKeys.DfsNamenodeReplicationMaxStreamsKey
				, DFSConfigKeys.DfsNamenodeReplicationMaxStreamsDefault);
			this.replicationStreamsHardLimit = conf.GetInt(DFSConfigKeys.DfsNamenodeReplicationStreamsHardLimitKey
				, DFSConfigKeys.DfsNamenodeReplicationStreamsHardLimitDefault);
			this.blocksInvalidateWorkPct = DFSUtil.GetInvalidateWorkPctPerIteration(conf);
			this.blocksReplWorkMultiplier = DFSUtil.GetReplWorkMultiplier(conf);
			this.replicationRecheckInterval = conf.GetInt(DFSConfigKeys.DfsNamenodeReplicationIntervalKey
				, DFSConfigKeys.DfsNamenodeReplicationIntervalDefault) * 1000L;
			this.encryptDataTransfer = conf.GetBoolean(DFSConfigKeys.DfsEncryptDataTransferKey
				, DFSConfigKeys.DfsEncryptDataTransferDefault);
			this.maxNumBlocksToLog = conf.GetLong(DFSConfigKeys.DfsMaxNumBlocksToLogKey, DFSConfigKeys
				.DfsMaxNumBlocksToLogDefault);
			this.numBlocksPerIteration = conf.GetInt(DFSConfigKeys.DfsBlockMisreplicationProcessingLimit
				, DFSConfigKeys.DfsBlockMisreplicationProcessingLimitDefault);
			Log.Info("defaultReplication         = " + defaultReplication);
			Log.Info("maxReplication             = " + maxReplication);
			Log.Info("minReplication             = " + minReplication);
			Log.Info("maxReplicationStreams      = " + maxReplicationStreams);
			Log.Info("replicationRecheckInterval = " + replicationRecheckInterval);
			Log.Info("encryptDataTransfer        = " + encryptDataTransfer);
			Log.Info("maxNumBlocksToLog          = " + maxNumBlocksToLog);
		}

		private static BlockTokenSecretManager CreateBlockTokenSecretManager(Configuration
			 conf)
		{
			bool isEnabled = conf.GetBoolean(DFSConfigKeys.DfsBlockAccessTokenEnableKey, DFSConfigKeys
				.DfsBlockAccessTokenEnableDefault);
			Log.Info(DFSConfigKeys.DfsBlockAccessTokenEnableKey + "=" + isEnabled);
			if (!isEnabled)
			{
				if (UserGroupInformation.IsSecurityEnabled())
				{
					Log.Error("Security is enabled but block access tokens " + "(via " + DFSConfigKeys
						.DfsBlockAccessTokenEnableKey + ") " + "aren't enabled. This may cause issues " 
						+ "when clients attempt to talk to a DataNode.");
				}
				return null;
			}
			long updateMin = conf.GetLong(DFSConfigKeys.DfsBlockAccessKeyUpdateIntervalKey, DFSConfigKeys
				.DfsBlockAccessKeyUpdateIntervalDefault);
			long lifetimeMin = conf.GetLong(DFSConfigKeys.DfsBlockAccessTokenLifetimeKey, DFSConfigKeys
				.DfsBlockAccessTokenLifetimeDefault);
			string encryptionAlgorithm = conf.Get(DFSConfigKeys.DfsDataEncryptionAlgorithmKey
				);
			Log.Info(DFSConfigKeys.DfsBlockAccessKeyUpdateIntervalKey + "=" + updateMin + " min(s), "
				 + DFSConfigKeys.DfsBlockAccessTokenLifetimeKey + "=" + lifetimeMin + " min(s), "
				 + DFSConfigKeys.DfsDataEncryptionAlgorithmKey + "=" + encryptionAlgorithm);
			string nsId = DFSUtil.GetNamenodeNameServiceId(conf);
			bool isHaEnabled = HAUtil.IsHAEnabled(conf, nsId);
			if (isHaEnabled)
			{
				string thisNnId = HAUtil.GetNameNodeId(conf, nsId);
				string otherNnId = HAUtil.GetNameNodeIdOfOtherNode(conf, nsId);
				return new BlockTokenSecretManager(updateMin * 60 * 1000L, lifetimeMin * 60 * 1000L
					, string.CompareOrdinal(thisNnId, otherNnId) < 0 ? 0 : 1, null, encryptionAlgorithm
					);
			}
			else
			{
				return new BlockTokenSecretManager(updateMin * 60 * 1000L, lifetimeMin * 60 * 1000L
					, 0, null, encryptionAlgorithm);
			}
		}

		public virtual BlockStoragePolicy GetStoragePolicy(string policyName)
		{
			return storagePolicySuite.GetPolicy(policyName);
		}

		public virtual BlockStoragePolicy GetStoragePolicy(byte policyId)
		{
			return storagePolicySuite.GetPolicy(policyId);
		}

		public virtual BlockStoragePolicy[] GetStoragePolicies()
		{
			return storagePolicySuite.GetAllPolicies();
		}

		public virtual void SetBlockPoolId(string blockPoolId)
		{
			if (IsBlockTokenEnabled())
			{
				blockTokenSecretManager.SetBlockPoolId(blockPoolId);
			}
		}

		public virtual BlockStoragePolicySuite GetStoragePolicySuite()
		{
			return storagePolicySuite;
		}

		/// <summary>get the BlockTokenSecretManager</summary>
		[VisibleForTesting]
		public virtual BlockTokenSecretManager GetBlockTokenSecretManager()
		{
			return blockTokenSecretManager;
		}

		/// <summary>Allow silent termination of replication monitor for testing</summary>
		[VisibleForTesting]
		internal virtual void EnableRMTerminationForTesting()
		{
			checkNSRunning = false;
		}

		private bool IsBlockTokenEnabled()
		{
			return blockTokenSecretManager != null;
		}

		/// <summary>Should the access keys be updated?</summary>
		/// <exception cref="System.IO.IOException"/>
		internal virtual bool ShouldUpdateBlockKey(long updateTime)
		{
			return IsBlockTokenEnabled() ? blockTokenSecretManager.UpdateKeys(updateTime) : false;
		}

		public virtual void Activate(Configuration conf)
		{
			pendingReplications.Start();
			datanodeManager.Activate(conf);
			this.replicationThread.Start();
		}

		public virtual void Close()
		{
			try
			{
				replicationThread.Interrupt();
				replicationThread.Join(3000);
			}
			catch (Exception)
			{
			}
			datanodeManager.Close();
			pendingReplications.Stop();
			blocksMap.Close();
		}

		/// <returns>the datanodeManager</returns>
		public virtual DatanodeManager GetDatanodeManager()
		{
			return datanodeManager;
		}

		[VisibleForTesting]
		public virtual BlockPlacementPolicy GetBlockPlacementPolicy()
		{
			return blockplacement;
		}

		/// <summary>Set BlockPlacementPolicy</summary>
		public virtual void SetBlockPlacementPolicy(BlockPlacementPolicy newpolicy)
		{
			if (newpolicy == null)
			{
				throw new HadoopIllegalArgumentException("newpolicy == null");
			}
			this.blockplacement = newpolicy;
		}

		/// <summary>Dump meta data to out.</summary>
		public virtual void MetaSave(PrintWriter @out)
		{
			System.Diagnostics.Debug.Assert(namesystem.HasWriteLock());
			IList<DatanodeDescriptor> live = new AList<DatanodeDescriptor>();
			IList<DatanodeDescriptor> dead = new AList<DatanodeDescriptor>();
			datanodeManager.FetchDatanodes(live, dead, false);
			@out.WriteLine("Live Datanodes: " + live.Count);
			@out.WriteLine("Dead Datanodes: " + dead.Count);
			//
			// Dump contents of neededReplication
			//
			lock (neededReplications)
			{
				@out.WriteLine("Metasave: Blocks waiting for replication: " + neededReplications.
					Size());
				foreach (Org.Apache.Hadoop.Hdfs.Protocol.Block block in neededReplications)
				{
					DumpBlockMeta(block, @out);
				}
			}
			// Dump any postponed over-replicated blocks
			@out.WriteLine("Mis-replicated blocks that have been postponed:");
			foreach (Org.Apache.Hadoop.Hdfs.Protocol.Block block_1 in postponedMisreplicatedBlocks)
			{
				DumpBlockMeta(block_1, @out);
			}
			// Dump blocks from pendingReplication
			pendingReplications.MetaSave(@out);
			// Dump blocks that are waiting to be deleted
			invalidateBlocks.Dump(@out);
			// Dump all datanodes
			GetDatanodeManager().DatanodeDump(@out);
		}

		/// <summary>
		/// Dump the metadata for the given block in a human-readable
		/// form.
		/// </summary>
		private void DumpBlockMeta(Org.Apache.Hadoop.Hdfs.Protocol.Block block, PrintWriter
			 @out)
		{
			IList<DatanodeDescriptor> containingNodes = new AList<DatanodeDescriptor>();
			IList<DatanodeStorageInfo> containingLiveReplicasNodes = new AList<DatanodeStorageInfo
				>();
			NumberReplicas numReplicas = new NumberReplicas();
			// source node returned is not used
			ChooseSourceDatanode(block, containingNodes, containingLiveReplicasNodes, numReplicas
				, UnderReplicatedBlocks.Level);
			// containingLiveReplicasNodes can include READ_ONLY_SHARED replicas which are 
			// not included in the numReplicas.liveReplicas() count
			System.Diagnostics.Debug.Assert(containingLiveReplicasNodes.Count >= numReplicas.
				LiveReplicas());
			int usableReplicas = numReplicas.LiveReplicas() + numReplicas.DecommissionedReplicas
				();
			if (block is BlockInfoContiguous)
			{
				BlockCollection bc = ((BlockInfoContiguous)block).GetBlockCollection();
				string fileName = (bc == null) ? "[orphaned]" : bc.GetName();
				@out.Write(fileName + ": ");
			}
			// l: == live:, d: == decommissioned c: == corrupt e: == excess
			@out.Write(block + ((usableReplicas > 0) ? string.Empty : " MISSING") + " (replicas:"
				 + " l: " + numReplicas.LiveReplicas() + " d: " + numReplicas.DecommissionedReplicas
				() + " c: " + numReplicas.CorruptReplicas() + " e: " + numReplicas.ExcessReplicas
				() + ") ");
			ICollection<DatanodeDescriptor> corruptNodes = corruptReplicas.GetNodes(block);
			foreach (DatanodeStorageInfo storage in blocksMap.GetStorages(block))
			{
				DatanodeDescriptor node = storage.GetDatanodeDescriptor();
				string state = string.Empty;
				if (corruptNodes != null && corruptNodes.Contains(node))
				{
					state = "(corrupt)";
				}
				else
				{
					if (node.IsDecommissioned() || node.IsDecommissionInProgress())
					{
						state = "(decommissioned)";
					}
				}
				if (storage.AreBlockContentsStale())
				{
					state += " (block deletions maybe out of date)";
				}
				@out.Write(" " + node + state + " : ");
			}
			@out.WriteLine(string.Empty);
		}

		/// <returns>maxReplicationStreams</returns>
		public virtual int GetMaxReplicationStreams()
		{
			return maxReplicationStreams;
		}

		/// <returns>true if the block has minimum replicas</returns>
		public virtual bool CheckMinReplication(Org.Apache.Hadoop.Hdfs.Protocol.Block block
			)
		{
			return (CountNodes(block).LiveReplicas() >= minReplication);
		}

		/// <summary>Commit a block of a file</summary>
		/// <param name="block">block to be committed</param>
		/// <param name="commitBlock">- contains client reported block length and generation</param>
		/// <returns>true if the block is changed to committed state.</returns>
		/// <exception cref="System.IO.IOException">
		/// if the block does not have at least a minimal number
		/// of replicas reported from data-nodes.
		/// </exception>
		private static bool CommitBlock(BlockInfoContiguousUnderConstruction block, Org.Apache.Hadoop.Hdfs.Protocol.Block
			 commitBlock)
		{
			if (block.GetBlockUCState() == HdfsServerConstants.BlockUCState.Committed)
			{
				return false;
			}
			System.Diagnostics.Debug.Assert(block.GetNumBytes() <= commitBlock.GetNumBytes(), 
				"commitBlock length is less than the stored one " + commitBlock.GetNumBytes() + 
				" vs. " + block.GetNumBytes());
			if (block.GetGenerationStamp() != commitBlock.GetGenerationStamp())
			{
				throw new IOException("Commit block with mismatching GS. NN has " + block + ", client submits "
					 + commitBlock);
			}
			block.CommitBlock(commitBlock);
			return true;
		}

		/// <summary>
		/// Commit the last block of the file and mark it as complete if it has
		/// meets the minimum replication requirement
		/// </summary>
		/// <param name="bc">block collection</param>
		/// <param name="commitBlock">- contains client reported block length and generation</param>
		/// <returns>true if the last block is changed to committed state.</returns>
		/// <exception cref="System.IO.IOException">
		/// if the block does not have at least a minimal number
		/// of replicas reported from data-nodes.
		/// </exception>
		public virtual bool CommitOrCompleteLastBlock(BlockCollection bc, Org.Apache.Hadoop.Hdfs.Protocol.Block
			 commitBlock)
		{
			if (commitBlock == null)
			{
				return false;
			}
			// not committing, this is a block allocation retry
			BlockInfoContiguous lastBlock = bc.GetLastBlock();
			if (lastBlock == null)
			{
				return false;
			}
			// no blocks in file yet
			if (lastBlock.IsComplete())
			{
				return false;
			}
			// already completed (e.g. by syncBlock)
			bool b = CommitBlock((BlockInfoContiguousUnderConstruction)lastBlock, commitBlock
				);
			if (CountNodes(lastBlock).LiveReplicas() >= minReplication)
			{
				CompleteBlock(bc, bc.NumBlocks() - 1, false);
			}
			return b;
		}

		/// <summary>Convert a specified block of the file to a complete block.</summary>
		/// <param name="bc">file</param>
		/// <param name="blkIndex">block index in the file</param>
		/// <exception cref="System.IO.IOException">
		/// if the block does not have at least a minimal number
		/// of replicas reported from data-nodes.
		/// </exception>
		private BlockInfoContiguous CompleteBlock(BlockCollection bc, int blkIndex, bool 
			force)
		{
			if (blkIndex < 0)
			{
				return null;
			}
			BlockInfoContiguous curBlock = bc.GetBlocks()[blkIndex];
			if (curBlock.IsComplete())
			{
				return curBlock;
			}
			BlockInfoContiguousUnderConstruction ucBlock = (BlockInfoContiguousUnderConstruction
				)curBlock;
			int numNodes = ucBlock.NumNodes();
			if (!force && numNodes < minReplication)
			{
				throw new IOException("Cannot complete block: " + "block does not satisfy minimal replication requirement."
					);
			}
			if (!force && ucBlock.GetBlockUCState() != HdfsServerConstants.BlockUCState.Committed)
			{
				throw new IOException("Cannot complete block: block has not been COMMITTED by the client"
					);
			}
			BlockInfoContiguous completeBlock = ucBlock.ConvertToCompleteBlock();
			// replace penultimate block in file
			bc.SetBlock(blkIndex, completeBlock);
			// Since safe-mode only counts complete blocks, and we now have
			// one more complete block, we need to adjust the total up, and
			// also count it as safe, if we have at least the minimum replica
			// count. (We may not have the minimum replica count yet if this is
			// a "forced" completion when a file is getting closed by an
			// OP_CLOSE edit on the standby).
			namesystem.AdjustSafeModeBlockTotals(0, 1);
			namesystem.IncrementSafeBlockCount(Math.Min(numNodes, minReplication));
			// replace block in the blocksMap
			return blocksMap.ReplaceBlock(completeBlock);
		}

		/// <exception cref="System.IO.IOException"/>
		private BlockInfoContiguous CompleteBlock(BlockCollection bc, BlockInfoContiguous
			 block, bool force)
		{
			BlockInfoContiguous[] fileBlocks = bc.GetBlocks();
			for (int idx = 0; idx < fileBlocks.Length; idx++)
			{
				if (fileBlocks[idx] == block)
				{
					return CompleteBlock(bc, idx, force);
				}
			}
			return block;
		}

		/// <summary>
		/// Force the given block in the given file to be marked as complete,
		/// regardless of whether enough replicas are present.
		/// </summary>
		/// <remarks>
		/// Force the given block in the given file to be marked as complete,
		/// regardless of whether enough replicas are present. This is necessary
		/// when tailing edit logs as a Standby.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public virtual BlockInfoContiguous ForceCompleteBlock(BlockCollection bc, BlockInfoContiguousUnderConstruction
			 block)
		{
			block.CommitBlock(block);
			return CompleteBlock(bc, block, true);
		}

		/// <summary>
		/// Convert the last block of the file to an under construction block.<p>
		/// The block is converted only if the file has blocks and the last one
		/// is a partial block (its size is less than the preferred block size).
		/// </summary>
		/// <remarks>
		/// Convert the last block of the file to an under construction block.<p>
		/// The block is converted only if the file has blocks and the last one
		/// is a partial block (its size is less than the preferred block size).
		/// The converted block is returned to the client.
		/// The client uses the returned block locations to form the data pipeline
		/// for this block.<br />
		/// The methods returns null if there is no partial block at the end.
		/// The client is supposed to allocate a new block with the next call.
		/// </remarks>
		/// <param name="bc">file</param>
		/// <param name="bytesToRemove">num of bytes to remove from block</param>
		/// <returns>the last block locations if the block is partial or null otherwise</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual LocatedBlock ConvertLastBlockToUnderConstruction(BlockCollection bc
			, long bytesToRemove)
		{
			BlockInfoContiguous oldBlock = bc.GetLastBlock();
			if (oldBlock == null || bc.GetPreferredBlockSize() == oldBlock.GetNumBytes() - bytesToRemove)
			{
				return null;
			}
			System.Diagnostics.Debug.Assert(oldBlock == GetStoredBlock(oldBlock), "last block of the file is not in blocksMap"
				);
			DatanodeStorageInfo[] targets = GetStorages(oldBlock);
			BlockInfoContiguousUnderConstruction ucBlock = bc.SetLastBlock(oldBlock, targets);
			blocksMap.ReplaceBlock(ucBlock);
			// Remove block from replication queue.
			NumberReplicas replicas = CountNodes(ucBlock);
			neededReplications.Remove(ucBlock, replicas.LiveReplicas(), replicas.DecommissionedReplicas
				(), GetReplication(ucBlock));
			pendingReplications.Remove(ucBlock);
			// remove this block from the list of pending blocks to be deleted. 
			foreach (DatanodeStorageInfo storage in targets)
			{
				invalidateBlocks.Remove(storage.GetDatanodeDescriptor(), oldBlock);
			}
			// Adjust safe-mode totals, since under-construction blocks don't
			// count in safe-mode.
			namesystem.AdjustSafeModeBlockTotals(targets.Length >= minReplication ? -1 : 0, -
				1);
			// decrement safe if we had enough
			// always decrement total blocks
			long fileLength = bc.ComputeContentSummary(GetStoragePolicySuite()).GetLength();
			long pos = fileLength - ucBlock.GetNumBytes();
			return CreateLocatedBlock(ucBlock, pos, BlockTokenSecretManager.AccessMode.Write);
		}

		/// <summary>Get all valid locations of the block</summary>
		private IList<DatanodeStorageInfo> GetValidLocations(Org.Apache.Hadoop.Hdfs.Protocol.Block
			 block)
		{
			IList<DatanodeStorageInfo> locations = new AList<DatanodeStorageInfo>(blocksMap.NumNodes
				(block));
			foreach (DatanodeStorageInfo storage in blocksMap.GetStorages(block))
			{
				// filter invalidate replicas
				if (!invalidateBlocks.Contains(storage.GetDatanodeDescriptor(), block))
				{
					locations.AddItem(storage);
				}
			}
			return locations;
		}

		/// <exception cref="System.IO.IOException"/>
		private IList<LocatedBlock> CreateLocatedBlockList(BlockInfoContiguous[] blocks, 
			long offset, long length, int nrBlocksToReturn, BlockTokenSecretManager.AccessMode
			 mode)
		{
			int curBlk = 0;
			long curPos = 0;
			long blkSize = 0;
			int nrBlocks = (blocks[0].GetNumBytes() == 0) ? 0 : blocks.Length;
			for (curBlk = 0; curBlk < nrBlocks; curBlk++)
			{
				blkSize = blocks[curBlk].GetNumBytes();
				System.Diagnostics.Debug.Assert(blkSize > 0, "Block of size 0");
				if (curPos + blkSize > offset)
				{
					break;
				}
				curPos += blkSize;
			}
			if (nrBlocks > 0 && curBlk == nrBlocks)
			{
				// offset >= end of file
				return Sharpen.Collections.EmptyList<LocatedBlock>();
			}
			long endOff = offset + length;
			IList<LocatedBlock> results = new AList<LocatedBlock>(blocks.Length);
			do
			{
				results.AddItem(CreateLocatedBlock(blocks[curBlk], curPos, mode));
				curPos += blocks[curBlk].GetNumBytes();
				curBlk++;
			}
			while (curPos < endOff && curBlk < blocks.Length && results.Count < nrBlocksToReturn);
			return results;
		}

		/// <exception cref="System.IO.IOException"/>
		private LocatedBlock CreateLocatedBlock(BlockInfoContiguous[] blocks, long endPos
			, BlockTokenSecretManager.AccessMode mode)
		{
			int curBlk = 0;
			long curPos = 0;
			int nrBlocks = (blocks[0].GetNumBytes() == 0) ? 0 : blocks.Length;
			for (curBlk = 0; curBlk < nrBlocks; curBlk++)
			{
				long blkSize = blocks[curBlk].GetNumBytes();
				if (curPos + blkSize >= endPos)
				{
					break;
				}
				curPos += blkSize;
			}
			return CreateLocatedBlock(blocks[curBlk], curPos, mode);
		}

		/// <exception cref="System.IO.IOException"/>
		private LocatedBlock CreateLocatedBlock(BlockInfoContiguous blk, long pos, BlockTokenSecretManager.AccessMode
			 mode)
		{
			LocatedBlock lb = CreateLocatedBlock(blk, pos);
			if (mode != null)
			{
				SetBlockToken(lb, mode);
			}
			return lb;
		}

		/// <returns>a LocatedBlock for the given block</returns>
		/// <exception cref="System.IO.IOException"/>
		private LocatedBlock CreateLocatedBlock(BlockInfoContiguous blk, long pos)
		{
			if (blk is BlockInfoContiguousUnderConstruction)
			{
				if (blk.IsComplete())
				{
					throw new IOException("blk instanceof BlockInfoUnderConstruction && blk.isComplete()"
						 + ", blk=" + blk);
				}
				BlockInfoContiguousUnderConstruction uc = (BlockInfoContiguousUnderConstruction)blk;
				DatanodeStorageInfo[] storages = uc.GetExpectedStorageLocations();
				ExtendedBlock eb = new ExtendedBlock(namesystem.GetBlockPoolId(), blk);
				return new LocatedBlock(eb, storages, pos, false);
			}
			// get block locations
			int numCorruptNodes = CountNodes(blk).CorruptReplicas();
			int numCorruptReplicas = corruptReplicas.NumCorruptReplicas(blk);
			if (numCorruptNodes != numCorruptReplicas)
			{
				Log.Warn("Inconsistent number of corrupt replicas for " + blk + " blockMap has " 
					+ numCorruptNodes + " but corrupt replicas map has " + numCorruptReplicas);
			}
			int numNodes = blocksMap.NumNodes(blk);
			bool isCorrupt = numCorruptNodes == numNodes;
			int numMachines = isCorrupt ? numNodes : numNodes - numCorruptNodes;
			DatanodeStorageInfo[] machines = new DatanodeStorageInfo[numMachines];
			int j = 0;
			if (numMachines > 0)
			{
				foreach (DatanodeStorageInfo storage in blocksMap.GetStorages(blk))
				{
					DatanodeDescriptor d = storage.GetDatanodeDescriptor();
					bool replicaCorrupt = corruptReplicas.IsReplicaCorrupt(blk, d);
					if (isCorrupt || (!replicaCorrupt))
					{
						machines[j++] = storage;
					}
				}
			}
			System.Diagnostics.Debug.Assert(j == machines.Length, "isCorrupt: " + isCorrupt +
				 " numMachines: " + numMachines + " numNodes: " + numNodes + " numCorrupt: " + numCorruptNodes
				 + " numCorruptRepls: " + numCorruptReplicas);
			ExtendedBlock eb_1 = new ExtendedBlock(namesystem.GetBlockPoolId(), blk);
			return new LocatedBlock(eb_1, machines, pos, isCorrupt);
		}

		/// <summary>Create a LocatedBlocks.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual LocatedBlocks CreateLocatedBlocks(BlockInfoContiguous[] blocks, long
			 fileSizeExcludeBlocksUnderConstruction, bool isFileUnderConstruction, long offset
			, long length, bool needBlockToken, bool inSnapshot, FileEncryptionInfo feInfo)
		{
			System.Diagnostics.Debug.Assert(namesystem.HasReadLock());
			if (blocks == null)
			{
				return null;
			}
			else
			{
				if (blocks.Length == 0)
				{
					return new LocatedBlocks(0, isFileUnderConstruction, Sharpen.Collections.EmptyList
						<LocatedBlock>(), null, false, feInfo);
				}
				else
				{
					if (Log.IsDebugEnabled())
					{
						Log.Debug("blocks = " + Arrays.AsList(blocks));
					}
					BlockTokenSecretManager.AccessMode mode = needBlockToken ? BlockTokenSecretManager.AccessMode
						.Read : null;
					IList<LocatedBlock> locatedblocks = CreateLocatedBlockList(blocks, offset, length
						, int.MaxValue, mode);
					LocatedBlock lastlb;
					bool isComplete;
					if (!inSnapshot)
					{
						BlockInfoContiguous last = blocks[blocks.Length - 1];
						long lastPos = last.IsComplete() ? fileSizeExcludeBlocksUnderConstruction - last.
							GetNumBytes() : fileSizeExcludeBlocksUnderConstruction;
						lastlb = CreateLocatedBlock(last, lastPos, mode);
						isComplete = last.IsComplete();
					}
					else
					{
						lastlb = CreateLocatedBlock(blocks, fileSizeExcludeBlocksUnderConstruction, mode);
						isComplete = true;
					}
					return new LocatedBlocks(fileSizeExcludeBlocksUnderConstruction, isFileUnderConstruction
						, locatedblocks, lastlb, isComplete, feInfo);
				}
			}
		}

		/// <returns>current access keys.</returns>
		public virtual ExportedBlockKeys GetBlockKeys()
		{
			return IsBlockTokenEnabled() ? blockTokenSecretManager.ExportKeys() : ExportedBlockKeys
				.DummyKeys;
		}

		/// <summary>Generate a block token for the located block.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void SetBlockToken(LocatedBlock b, BlockTokenSecretManager.AccessMode
			 mode)
		{
			if (IsBlockTokenEnabled())
			{
				// Use cached UGI if serving RPC calls.
				b.SetBlockToken(blockTokenSecretManager.GenerateToken(NameNode.GetRemoteUser().GetShortUserName
					(), b.GetBlock(), EnumSet.Of(mode)));
			}
		}

		internal virtual void AddKeyUpdateCommand(IList<DatanodeCommand> cmds, DatanodeDescriptor
			 nodeinfo)
		{
			// check access key update
			if (IsBlockTokenEnabled() && nodeinfo.needKeyUpdate)
			{
				cmds.AddItem(new KeyUpdateCommand(blockTokenSecretManager.ExportKeys()));
				nodeinfo.needKeyUpdate = false;
			}
		}

		public virtual DataEncryptionKey GenerateDataEncryptionKey()
		{
			if (IsBlockTokenEnabled() && encryptDataTransfer)
			{
				return blockTokenSecretManager.GenerateDataEncryptionKey();
			}
			else
			{
				return null;
			}
		}

		/// <summary>
		/// Clamp the specified replication between the minimum and the maximum
		/// replication levels.
		/// </summary>
		public virtual short AdjustReplication(short replication)
		{
			return replication < minReplication ? minReplication : replication > maxReplication
				 ? maxReplication : replication;
		}

		/// <summary>
		/// Check whether the replication parameter is within the range
		/// determined by system configuration.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void VerifyReplication(string src, short replication, string clientName
			)
		{
			if (replication >= minReplication && replication <= maxReplication)
			{
				//common case. avoid building 'text'
				return;
			}
			string text = "file " + src + ((clientName != null) ? " on client " + clientName : 
				string.Empty) + ".\n" + "Requested replication " + replication;
			if (replication > maxReplication)
			{
				throw new IOException(text + " exceeds maximum " + maxReplication);
			}
			if (replication < minReplication)
			{
				throw new IOException(text + " is less than the required minimum " + minReplication
					);
			}
		}

		/// <summary>Check if a block is replicated to at least the minimum replication.</summary>
		public virtual bool IsSufficientlyReplicated(BlockInfoContiguous b)
		{
			// Compare against the lesser of the minReplication and number of live DNs.
			int replication = Math.Min(minReplication, GetDatanodeManager().GetNumLiveDataNodes
				());
			return CountNodes(b).LiveReplicas() >= replication;
		}

		/// <summary>
		/// return a list of blocks & their locations on <code>datanode</code> whose
		/// total size is <code>size</code>
		/// </summary>
		/// <param name="datanode">on which blocks are located</param>
		/// <param name="size">total size of blocks</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual BlocksWithLocations GetBlocks(DatanodeID datanode, long size)
		{
			namesystem.CheckOperation(NameNode.OperationCategory.Read);
			namesystem.ReadLock();
			try
			{
				namesystem.CheckOperation(NameNode.OperationCategory.Read);
				return GetBlocksWithLocations(datanode, size);
			}
			finally
			{
				namesystem.ReadUnlock();
			}
		}

		/// <summary>Get all blocks with location information from a datanode.</summary>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.UnregisteredNodeException"/>
		private BlocksWithLocations GetBlocksWithLocations(DatanodeID datanode, long size
			)
		{
			DatanodeDescriptor node = GetDatanodeManager().GetDatanode(datanode);
			if (node == null)
			{
				blockLog.Warn("BLOCK* getBlocks: Asking for blocks from an" + " unrecorded node {}"
					, datanode);
				throw new HadoopIllegalArgumentException("Datanode " + datanode + " not found.");
			}
			int numBlocks = node.NumBlocks();
			if (numBlocks == 0)
			{
				return new BlocksWithLocations(new BlocksWithLocations.BlockWithLocations[0]);
			}
			IEnumerator<BlockInfoContiguous> iter = node.GetBlockIterator();
			int startBlock = DFSUtil.GetRandom().Next(numBlocks);
			// starting from a random block
			// skip blocks
			for (int i = 0; i < startBlock; i++)
			{
				iter.Next();
			}
			IList<BlocksWithLocations.BlockWithLocations> results = new AList<BlocksWithLocations.BlockWithLocations
				>();
			long totalSize = 0;
			BlockInfoContiguous curBlock;
			while (totalSize < size && iter.HasNext())
			{
				curBlock = iter.Next();
				if (!curBlock.IsComplete())
				{
					continue;
				}
				totalSize += AddBlock(curBlock, results);
			}
			if (totalSize < size)
			{
				iter = node.GetBlockIterator();
				// start from the beginning
				for (int i_1 = 0; i_1 < startBlock && totalSize < size; i_1++)
				{
					curBlock = iter.Next();
					if (!curBlock.IsComplete())
					{
						continue;
					}
					totalSize += AddBlock(curBlock, results);
				}
			}
			return new BlocksWithLocations(Sharpen.Collections.ToArray(results, new BlocksWithLocations.BlockWithLocations
				[results.Count]));
		}

		/// <summary>Remove the blocks associated to the given datanode.</summary>
		internal virtual void RemoveBlocksAssociatedTo(DatanodeDescriptor node)
		{
			IEnumerator<Org.Apache.Hadoop.Hdfs.Protocol.Block> it = node.GetBlockIterator();
			while (it.HasNext())
			{
				RemoveStoredBlock(it.Next(), node);
			}
			// Remove all pending DN messages referencing this DN.
			pendingDNMessages.RemoveAllMessagesForDatanode(node);
			node.ResetBlocks();
			invalidateBlocks.Remove(node);
		}

		/// <summary>Remove the blocks associated to the given DatanodeStorageInfo.</summary>
		internal virtual void RemoveBlocksAssociatedTo(DatanodeStorageInfo storageInfo)
		{
			System.Diagnostics.Debug.Assert(namesystem.HasWriteLock());
			IEnumerator<Org.Apache.Hadoop.Hdfs.Protocol.Block> it = storageInfo.GetBlockIterator
				();
			DatanodeDescriptor node = storageInfo.GetDatanodeDescriptor();
			while (it.HasNext())
			{
				Org.Apache.Hadoop.Hdfs.Protocol.Block block = it.Next();
				RemoveStoredBlock(block, node);
				invalidateBlocks.Remove(node, block);
			}
			namesystem.CheckSafeMode();
		}

		/// <summary>
		/// Adds block to list of blocks which will be invalidated on specified
		/// datanode and log the operation
		/// </summary>
		internal virtual void AddToInvalidates(Org.Apache.Hadoop.Hdfs.Protocol.Block block
			, DatanodeInfo datanode)
		{
			if (!namesystem.IsPopulatingReplQueues())
			{
				return;
			}
			invalidateBlocks.Add(block, datanode, true);
		}

		/// <summary>
		/// Adds block to list of blocks which will be invalidated on all its
		/// datanodes.
		/// </summary>
		private void AddToInvalidates(Org.Apache.Hadoop.Hdfs.Protocol.Block b)
		{
			if (!namesystem.IsPopulatingReplQueues())
			{
				return;
			}
			StringBuilder datanodes = new StringBuilder();
			foreach (DatanodeStorageInfo storage in blocksMap.GetStorages(b, DatanodeStorage.State
				.Normal))
			{
				DatanodeDescriptor node = storage.GetDatanodeDescriptor();
				invalidateBlocks.Add(b, node, false);
				datanodes.Append(node).Append(" ");
			}
			if (datanodes.Length != 0)
			{
				blockLog.Info("BLOCK* addToInvalidates: {} {}", b, datanodes.ToString());
			}
		}

		/// <summary>
		/// Remove all block invalidation tasks under this datanode UUID;
		/// used when a datanode registers with a new UUID and the old one
		/// is wiped.
		/// </summary>
		internal virtual void RemoveFromInvalidates(DatanodeInfo datanode)
		{
			if (!namesystem.IsPopulatingReplQueues())
			{
				return;
			}
			invalidateBlocks.Remove(datanode);
		}

		/// <summary>Mark the block belonging to datanode as corrupt</summary>
		/// <param name="blk">Block to be marked as corrupt</param>
		/// <param name="dn">Datanode which holds the corrupt replica</param>
		/// <param name="storageID">if known, null otherwise.</param>
		/// <param name="reason">
		/// a textual reason why the block should be marked corrupt,
		/// for logging purposes
		/// </param>
		/// <exception cref="System.IO.IOException"/>
		public virtual void FindAndMarkBlockAsCorrupt(ExtendedBlock blk, DatanodeInfo dn, 
			string storageID, string reason)
		{
			System.Diagnostics.Debug.Assert(namesystem.HasWriteLock());
			BlockInfoContiguous storedBlock = GetStoredBlock(blk.GetLocalBlock());
			if (storedBlock == null)
			{
				// Check if the replica is in the blockMap, if not
				// ignore the request for now. This could happen when BlockScanner
				// thread of Datanode reports bad block before Block reports are sent
				// by the Datanode on startup
				blockLog.Info("BLOCK* findAndMarkBlockAsCorrupt: {} not found", blk);
				return;
			}
			DatanodeDescriptor node = GetDatanodeManager().GetDatanode(dn);
			if (node == null)
			{
				throw new IOException("Cannot mark " + blk + " as corrupt because datanode " + dn
					 + " (" + dn.GetDatanodeUuid() + ") does not exist");
			}
			MarkBlockAsCorrupt(new BlockManager.BlockToMarkCorrupt(storedBlock, blk.GetGenerationStamp
				(), reason, CorruptReplicasMap.Reason.CorruptionReported), storageID == null ? null
				 : node.GetStorageInfo(storageID), node);
		}

		/// <param name="b"/>
		/// <param name="storageInfo">storage that contains the block, if known. null otherwise.
		/// 	</param>
		/// <exception cref="System.IO.IOException"/>
		private void MarkBlockAsCorrupt(BlockManager.BlockToMarkCorrupt b, DatanodeStorageInfo
			 storageInfo, DatanodeDescriptor node)
		{
			BlockCollection bc = b.corrupted.GetBlockCollection();
			if (bc == null)
			{
				blockLog.Info("BLOCK markBlockAsCorrupt: {} cannot be marked as" + " corrupt as it does not belong to any file"
					, b);
				AddToInvalidates(b.corrupted, node);
				return;
			}
			// Add replica to the data-node if it is not already there
			if (storageInfo != null)
			{
				storageInfo.AddBlock(b.stored);
			}
			// Add this replica to corruptReplicas Map
			corruptReplicas.AddToCorruptReplicasMap(b.corrupted, node, b.reason, b.reasonCode
				);
			NumberReplicas numberOfReplicas = CountNodes(b.stored);
			bool hasEnoughLiveReplicas = numberOfReplicas.LiveReplicas() >= bc.GetBlockReplication
				();
			bool minReplicationSatisfied = numberOfReplicas.LiveReplicas() >= minReplication;
			bool hasMoreCorruptReplicas = minReplicationSatisfied && (numberOfReplicas.LiveReplicas
				() + numberOfReplicas.CorruptReplicas()) > bc.GetBlockReplication();
			bool corruptedDuringWrite = minReplicationSatisfied && (b.stored.GetGenerationStamp
				() > b.corrupted.GetGenerationStamp());
			// case 1: have enough number of live replicas
			// case 2: corrupted replicas + live replicas > Replication factor
			// case 3: Block is marked corrupt due to failure while writing. In this
			//         case genstamp will be different than that of valid block.
			// In all these cases we can delete the replica.
			// In case of 3, rbw block will be deleted and valid block can be replicated
			if (hasEnoughLiveReplicas || hasMoreCorruptReplicas || corruptedDuringWrite)
			{
				// the block is over-replicated so invalidate the replicas immediately
				InvalidateBlock(b, node);
			}
			else
			{
				if (namesystem.IsPopulatingReplQueues())
				{
					// add the block to neededReplication
					UpdateNeededReplications(b.stored, -1, 0);
				}
			}
		}

		/// <summary>Invalidates the given block on the given datanode.</summary>
		/// <returns>
		/// true if the block was successfully invalidated and no longer
		/// present in the BlocksMap
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		private bool InvalidateBlock(BlockManager.BlockToMarkCorrupt b, DatanodeInfo dn)
		{
			blockLog.Info("BLOCK* invalidateBlock: {} on {}", b, dn);
			DatanodeDescriptor node = GetDatanodeManager().GetDatanode(dn);
			if (node == null)
			{
				throw new IOException("Cannot invalidate " + b + " because datanode " + dn + " does not exist."
					);
			}
			// Check how many copies we have of the block
			NumberReplicas nr = CountNodes(b.stored);
			if (nr.ReplicasOnStaleNodes() > 0)
			{
				blockLog.Info("BLOCK* invalidateBlocks: postponing " + "invalidation of {} on {} because {} replica(s) are located on "
					 + "nodes with potentially out-of-date block reports", b, dn, nr.ReplicasOnStaleNodes
					());
				PostponeBlock(b.corrupted);
				return false;
			}
			else
			{
				if (nr.LiveReplicas() >= 1)
				{
					// If we have at least one copy on a live node, then we can delete it.
					AddToInvalidates(b.corrupted, dn);
					RemoveStoredBlock(b.stored, node);
					blockLog.Debug("BLOCK* invalidateBlocks: {} on {} listed for deletion.", b, dn);
					return true;
				}
				else
				{
					blockLog.Info("BLOCK* invalidateBlocks: {} on {} is the only copy and" + " was not deleted"
						, b, dn);
					return false;
				}
			}
		}

		public virtual void SetPostponeBlocksFromFuture(bool postpone)
		{
			this.shouldPostponeBlocksFromFuture = postpone;
		}

		private void PostponeBlock(Org.Apache.Hadoop.Hdfs.Protocol.Block blk)
		{
			if (postponedMisreplicatedBlocks.AddItem(blk))
			{
				postponedMisreplicatedBlocksCount.IncrementAndGet();
			}
		}

		internal virtual void UpdateState()
		{
			pendingReplicationBlocksCount = pendingReplications.Size();
			underReplicatedBlocksCount = neededReplications.Size();
			corruptReplicaBlocksCount = corruptReplicas.Size();
		}

		/// <summary>Return number of under-replicated but not missing blocks</summary>
		public virtual int GetUnderReplicatedNotMissingBlocks()
		{
			return neededReplications.GetUnderReplicatedBlockCount();
		}

		/// <summary>Schedule blocks for deletion at datanodes</summary>
		/// <param name="nodesToProcess">number of datanodes to schedule deletion work</param>
		/// <returns>total number of block for deletion</returns>
		internal virtual int ComputeInvalidateWork(int nodesToProcess)
		{
			IList<DatanodeInfo> nodes = invalidateBlocks.GetDatanodes();
			Sharpen.Collections.Shuffle(nodes);
			nodesToProcess = Math.Min(nodes.Count, nodesToProcess);
			int blockCnt = 0;
			foreach (DatanodeInfo dnInfo in nodes)
			{
				int blocks = InvalidateWorkForOneNode(dnInfo);
				if (blocks > 0)
				{
					blockCnt += blocks;
					if (--nodesToProcess == 0)
					{
						break;
					}
				}
			}
			return blockCnt;
		}

		/// <summary>
		/// Scan blocks in
		/// <see cref="neededReplications"/>
		/// and assign replication
		/// work to data-nodes they belong to.
		/// The number of process blocks equals either twice the number of live
		/// data-nodes or the number of under-replicated blocks whichever is less.
		/// </summary>
		/// <returns>number of blocks scheduled for replication during this iteration.</returns>
		internal virtual int ComputeReplicationWork(int blocksToProcess)
		{
			IList<IList<Org.Apache.Hadoop.Hdfs.Protocol.Block>> blocksToReplicate = null;
			namesystem.WriteLock();
			try
			{
				// Choose the blocks to be replicated
				blocksToReplicate = neededReplications.ChooseUnderReplicatedBlocks(blocksToProcess
					);
			}
			finally
			{
				namesystem.WriteUnlock();
			}
			return ComputeReplicationWorkForBlocks(blocksToReplicate);
		}

		/// <summary>Replicate a set of blocks</summary>
		/// <param name="blocksToReplicate">blocks to be replicated, for each priority</param>
		/// <returns>the number of blocks scheduled for replication</returns>
		[VisibleForTesting]
		internal virtual int ComputeReplicationWorkForBlocks(IList<IList<Org.Apache.Hadoop.Hdfs.Protocol.Block
			>> blocksToReplicate)
		{
			int requiredReplication;
			int numEffectiveReplicas;
			IList<DatanodeDescriptor> containingNodes;
			DatanodeDescriptor srcNode;
			BlockCollection bc = null;
			int additionalReplRequired;
			int scheduledWork = 0;
			IList<BlockManager.ReplicationWork> work = new List<BlockManager.ReplicationWork>
				();
			namesystem.WriteLock();
			try
			{
				lock (neededReplications)
				{
					for (int priority = 0; priority < blocksToReplicate.Count; priority++)
					{
						foreach (Org.Apache.Hadoop.Hdfs.Protocol.Block block in blocksToReplicate[priority
							])
						{
							// block should belong to a file
							bc = blocksMap.GetBlockCollection(block);
							// abandoned block or block reopened for append
							if (bc == null || (bc.IsUnderConstruction() && block.Equals(bc.GetLastBlock())))
							{
								neededReplications.Remove(block, priority);
								// remove from neededReplications
								neededReplications.DecrementReplicationIndex(priority);
								continue;
							}
							requiredReplication = bc.GetBlockReplication();
							// get a source data-node
							containingNodes = new AList<DatanodeDescriptor>();
							IList<DatanodeStorageInfo> liveReplicaNodes = new AList<DatanodeStorageInfo>();
							NumberReplicas numReplicas = new NumberReplicas();
							srcNode = ChooseSourceDatanode(block, containingNodes, liveReplicaNodes, numReplicas
								, priority);
							if (srcNode == null)
							{
								// block can not be replicated from any node
								Log.Debug("Block " + block + " cannot be repl from any node");
								continue;
							}
							// liveReplicaNodes can include READ_ONLY_SHARED replicas which are 
							// not included in the numReplicas.liveReplicas() count
							System.Diagnostics.Debug.Assert(liveReplicaNodes.Count >= numReplicas.LiveReplicas
								());
							// do not schedule more if enough replicas is already pending
							numEffectiveReplicas = numReplicas.LiveReplicas() + pendingReplications.GetNumReplicas
								(block);
							if (numEffectiveReplicas >= requiredReplication)
							{
								if ((pendingReplications.GetNumReplicas(block) > 0) || (BlockHasEnoughRacks(block
									)))
								{
									neededReplications.Remove(block, priority);
									// remove from neededReplications
									neededReplications.DecrementReplicationIndex(priority);
									blockLog.Info("BLOCK* Removing {} from neededReplications as" + " it has enough replicas"
										, block);
									continue;
								}
							}
							if (numReplicas.LiveReplicas() < requiredReplication)
							{
								additionalReplRequired = requiredReplication - numEffectiveReplicas;
							}
							else
							{
								additionalReplRequired = 1;
							}
							// Needed on a new rack
							work.AddItem(new BlockManager.ReplicationWork(block, bc, srcNode, containingNodes
								, liveReplicaNodes, additionalReplRequired, priority));
						}
					}
				}
			}
			finally
			{
				namesystem.WriteUnlock();
			}
			ICollection<Node> excludedNodes = new HashSet<Node>();
			foreach (BlockManager.ReplicationWork rw in work)
			{
				// Exclude all of the containing nodes from being targets.
				// This list includes decommissioning or corrupt nodes.
				excludedNodes.Clear();
				foreach (DatanodeDescriptor dn in rw.containingNodes)
				{
					excludedNodes.AddItem(dn);
				}
				// choose replication targets: NOT HOLDING THE GLOBAL LOCK
				// It is costly to extract the filename for which chooseTargets is called,
				// so for now we pass in the block collection itself.
				rw.ChooseTargets(blockplacement, storagePolicySuite, excludedNodes);
			}
			namesystem.WriteLock();
			try
			{
				foreach (BlockManager.ReplicationWork rw_1 in work)
				{
					DatanodeStorageInfo[] targets = rw_1.targets;
					if (targets == null || targets.Length == 0)
					{
						rw_1.targets = null;
						continue;
					}
					lock (neededReplications)
					{
						Org.Apache.Hadoop.Hdfs.Protocol.Block block = rw_1.block;
						int priority = rw_1.priority;
						// Recheck since global lock was released
						// block should belong to a file
						bc = blocksMap.GetBlockCollection(block);
						// abandoned block or block reopened for append
						if (bc == null || (bc.IsUnderConstruction() && block.Equals(bc.GetLastBlock())))
						{
							neededReplications.Remove(block, priority);
							// remove from neededReplications
							rw_1.targets = null;
							neededReplications.DecrementReplicationIndex(priority);
							continue;
						}
						requiredReplication = bc.GetBlockReplication();
						// do not schedule more if enough replicas is already pending
						NumberReplicas numReplicas = CountNodes(block);
						numEffectiveReplicas = numReplicas.LiveReplicas() + pendingReplications.GetNumReplicas
							(block);
						if (numEffectiveReplicas >= requiredReplication)
						{
							if ((pendingReplications.GetNumReplicas(block) > 0) || (BlockHasEnoughRacks(block
								)))
							{
								neededReplications.Remove(block, priority);
								// remove from neededReplications
								neededReplications.DecrementReplicationIndex(priority);
								rw_1.targets = null;
								blockLog.Info("BLOCK* Removing {} from neededReplications as" + " it has enough replicas"
									, block);
								continue;
							}
						}
						if ((numReplicas.LiveReplicas() >= requiredReplication) && (!BlockHasEnoughRacks(
							block)))
						{
							if (rw_1.srcNode.GetNetworkLocation().Equals(targets[0].GetDatanodeDescriptor().GetNetworkLocation
								()))
							{
								//No use continuing, unless a new rack in this case
								continue;
							}
						}
						// Add block to the to be replicated list
						rw_1.srcNode.AddBlockToBeReplicated(block, targets);
						scheduledWork++;
						DatanodeStorageInfo.IncrementBlocksScheduled(targets);
						// Move the block-replication into a "pending" state.
						// The reason we use 'pending' is so we can retry
						// replications that fail after an appropriate amount of time.
						pendingReplications.Increment(block, DatanodeStorageInfo.ToDatanodeDescriptors(targets
							));
						blockLog.Debug("BLOCK* block {} is moved from neededReplications to " + "pendingReplications"
							, block);
						// remove from neededReplications
						if (numEffectiveReplicas + targets.Length >= requiredReplication)
						{
							neededReplications.Remove(block, priority);
							// remove from neededReplications
							neededReplications.DecrementReplicationIndex(priority);
						}
					}
				}
			}
			finally
			{
				namesystem.WriteUnlock();
			}
			if (blockLog.IsInfoEnabled())
			{
				// log which blocks have been scheduled for replication
				foreach (BlockManager.ReplicationWork rw_1 in work)
				{
					DatanodeStorageInfo[] targets = rw_1.targets;
					if (targets != null && targets.Length != 0)
					{
						StringBuilder targetList = new StringBuilder("datanode(s)");
						for (int k = 0; k < targets.Length; k++)
						{
							targetList.Append(' ');
							targetList.Append(targets[k].GetDatanodeDescriptor());
						}
						blockLog.Info("BLOCK* ask {} to replicate {} to {}", rw_1.srcNode, rw_1.block, targetList
							);
					}
				}
			}
			if (blockLog.IsDebugEnabled())
			{
				blockLog.Debug("BLOCK* neededReplications = {} pendingReplications = {}", neededReplications
					.Size(), pendingReplications.Size());
			}
			return scheduledWork;
		}

		/// <summary>Choose target for WebHDFS redirection.</summary>
		public virtual DatanodeStorageInfo[] ChooseTarget4WebHDFS(string src, DatanodeDescriptor
			 clientnode, ICollection<Node> excludes, long blocksize)
		{
			return blockplacement.ChooseTarget(src, 1, clientnode, Sharpen.Collections.EmptyList
				<DatanodeStorageInfo>(), false, excludes, blocksize, storagePolicySuite.GetDefaultPolicy
				());
		}

		/// <summary>Choose target for getting additional datanodes for an existing pipeline.
		/// 	</summary>
		public virtual DatanodeStorageInfo[] ChooseTarget4AdditionalDatanode(string src, 
			int numAdditionalNodes, Node clientnode, IList<DatanodeStorageInfo> chosen, ICollection
			<Node> excludes, long blocksize, byte storagePolicyID)
		{
			BlockStoragePolicy storagePolicy = storagePolicySuite.GetPolicy(storagePolicyID);
			return blockplacement.ChooseTarget(src, numAdditionalNodes, clientnode, chosen, true
				, excludes, blocksize, storagePolicy);
		}

		/// <summary>Choose target datanodes for creating a new block.</summary>
		/// <exception cref="System.IO.IOException">if the number of targets &lt; minimum replication.
		/// 	</exception>
		/// <seealso cref="BlockPlacementPolicy.ChooseTarget(string, int, Org.Apache.Hadoop.Net.Node, System.Collections.Generic.ICollection{E}, long, System.Collections.Generic.IList{E}, Org.Apache.Hadoop.Hdfs.Protocol.BlockStoragePolicy)
		/// 	"/>
		public virtual DatanodeStorageInfo[] ChooseTarget4NewBlock(string src, int numOfReplicas
			, Node client, ICollection<Node> excludedNodes, long blocksize, IList<string> favoredNodes
			, byte storagePolicyID)
		{
			IList<DatanodeDescriptor> favoredDatanodeDescriptors = GetDatanodeDescriptors(favoredNodes
				);
			BlockStoragePolicy storagePolicy = storagePolicySuite.GetPolicy(storagePolicyID);
			DatanodeStorageInfo[] targets = blockplacement.ChooseTarget(src, numOfReplicas, client
				, excludedNodes, blocksize, favoredDatanodeDescriptors, storagePolicy);
			if (targets.Length < minReplication)
			{
				throw new IOException("File " + src + " could only be replicated to " + targets.Length
					 + " nodes instead of minReplication (=" + minReplication + ").  There are " + GetDatanodeManager
					().GetNetworkTopology().GetNumOfLeaves() + " datanode(s) running and " + (excludedNodes
					 == null ? "no" : excludedNodes.Count) + " node(s) are excluded in this operation."
					);
			}
			return targets;
		}

		/// <summary>Get list of datanode descriptors for given list of nodes.</summary>
		/// <remarks>
		/// Get list of datanode descriptors for given list of nodes. Nodes are
		/// hostaddress:port or just hostaddress.
		/// </remarks>
		internal virtual IList<DatanodeDescriptor> GetDatanodeDescriptors(IList<string> nodes
			)
		{
			IList<DatanodeDescriptor> datanodeDescriptors = null;
			if (nodes != null)
			{
				datanodeDescriptors = new AList<DatanodeDescriptor>(nodes.Count);
				for (int i = 0; i < nodes.Count; i++)
				{
					DatanodeDescriptor node = datanodeManager.GetDatanodeDescriptor(nodes[i]);
					if (node != null)
					{
						datanodeDescriptors.AddItem(node);
					}
				}
			}
			return datanodeDescriptors;
		}

		/// <summary>
		/// Parse the data-nodes the block belongs to and choose one,
		/// which will be the replication source.
		/// </summary>
		/// <remarks>
		/// Parse the data-nodes the block belongs to and choose one,
		/// which will be the replication source.
		/// We prefer nodes that are in DECOMMISSION_INPROGRESS state to other nodes
		/// since the former do not have write traffic and hence are less busy.
		/// We do not use already decommissioned nodes as a source.
		/// Otherwise we choose a random node among those that did not reach their
		/// replication limits.  However, if the replication is of the highest priority
		/// and all nodes have reached their replication limits, we will choose a
		/// random node despite the replication limit.
		/// In addition form a list of all nodes containing the block
		/// and calculate its replication numbers.
		/// </remarks>
		/// <param name="block">Block for which a replication source is needed</param>
		/// <param name="containingNodes">
		/// List to be populated with nodes found to contain the
		/// given block
		/// </param>
		/// <param name="nodesContainingLiveReplicas">
		/// List to be populated with nodes found to
		/// contain live replicas of the given block
		/// </param>
		/// <param name="numReplicas">
		/// NumberReplicas instance to be initialized with the
		/// counts of live, corrupt, excess, and
		/// decommissioned replicas of the given
		/// block.
		/// </param>
		/// <param name="priority">
		/// integer representing replication priority of the given
		/// block
		/// </param>
		/// <returns>
		/// the DatanodeDescriptor of the chosen node from which to replicate
		/// the given block
		/// </returns>
		[VisibleForTesting]
		internal virtual DatanodeDescriptor ChooseSourceDatanode(Org.Apache.Hadoop.Hdfs.Protocol.Block
			 block, IList<DatanodeDescriptor> containingNodes, IList<DatanodeStorageInfo> nodesContainingLiveReplicas
			, NumberReplicas numReplicas, int priority)
		{
			containingNodes.Clear();
			nodesContainingLiveReplicas.Clear();
			DatanodeDescriptor srcNode = null;
			int live = 0;
			int decommissioned = 0;
			int corrupt = 0;
			int excess = 0;
			ICollection<DatanodeDescriptor> nodesCorrupt = corruptReplicas.GetNodes(block);
			foreach (DatanodeStorageInfo storage in blocksMap.GetStorages(block))
			{
				DatanodeDescriptor node = storage.GetDatanodeDescriptor();
				LightWeightLinkedSet<Org.Apache.Hadoop.Hdfs.Protocol.Block> excessBlocks = excessReplicateMap
					[node.GetDatanodeUuid()];
				int countableReplica = storage.GetState() == DatanodeStorage.State.Normal ? 1 : 0;
				if ((nodesCorrupt != null) && (nodesCorrupt.Contains(node)))
				{
					corrupt += countableReplica;
				}
				else
				{
					if (node.IsDecommissionInProgress() || node.IsDecommissioned())
					{
						decommissioned += countableReplica;
					}
					else
					{
						if (excessBlocks != null && excessBlocks.Contains(block))
						{
							excess += countableReplica;
						}
						else
						{
							nodesContainingLiveReplicas.AddItem(storage);
							live += countableReplica;
						}
					}
				}
				containingNodes.AddItem(node);
				// Check if this replica is corrupt
				// If so, do not select the node as src node
				if ((nodesCorrupt != null) && nodesCorrupt.Contains(node))
				{
					continue;
				}
				if (priority != UnderReplicatedBlocks.QueueHighestPriority && !node.IsDecommissionInProgress
					() && node.GetNumberOfBlocksToBeReplicated() >= maxReplicationStreams)
				{
					continue;
				}
				// already reached replication limit
				if (node.GetNumberOfBlocksToBeReplicated() >= replicationStreamsHardLimit)
				{
					continue;
				}
				// the block must not be scheduled for removal on srcNode
				if (excessBlocks != null && excessBlocks.Contains(block))
				{
					continue;
				}
				// never use already decommissioned nodes
				if (node.IsDecommissioned())
				{
					continue;
				}
				// We got this far, current node is a reasonable choice
				if (srcNode == null)
				{
					srcNode = node;
					continue;
				}
				// switch to a different node randomly
				// this to prevent from deterministically selecting the same node even
				// if the node failed to replicate the block on previous iterations
				if (DFSUtil.GetRandom().NextBoolean())
				{
					srcNode = node;
				}
			}
			if (numReplicas != null)
			{
				numReplicas.Initialize(live, decommissioned, corrupt, excess, 0);
			}
			return srcNode;
		}

		/// <summary>
		/// If there were any replication requests that timed out, reap them
		/// and put them back into the neededReplication queue
		/// </summary>
		private void ProcessPendingReplications()
		{
			Org.Apache.Hadoop.Hdfs.Protocol.Block[] timedOutItems = pendingReplications.GetTimedOutBlocks
				();
			if (timedOutItems != null)
			{
				namesystem.WriteLock();
				try
				{
					for (int i = 0; i < timedOutItems.Length; i++)
					{
						/*
						* Use the blockinfo from the blocksmap to be certain we're working
						* with the most up-to-date block information (e.g. genstamp).
						*/
						BlockInfoContiguous bi = blocksMap.GetStoredBlock(timedOutItems[i]);
						if (bi == null)
						{
							continue;
						}
						NumberReplicas num = CountNodes(timedOutItems[i]);
						if (IsNeededReplication(bi, GetReplication(bi), num.LiveReplicas()))
						{
							neededReplications.Add(bi, num.LiveReplicas(), num.DecommissionedReplicas(), GetReplication
								(bi));
						}
					}
				}
				finally
				{
					namesystem.WriteUnlock();
				}
			}
		}

		/// <summary>
		/// StatefulBlockInfo is used to build the "toUC" list, which is a list of
		/// updates to the information about under-construction blocks.
		/// </summary>
		/// <remarks>
		/// StatefulBlockInfo is used to build the "toUC" list, which is a list of
		/// updates to the information about under-construction blocks.
		/// Besides the block in question, it provides the ReplicaState
		/// reported by the datanode in the block report.
		/// </remarks>
		internal class StatefulBlockInfo
		{
			internal readonly BlockInfoContiguousUnderConstruction storedBlock;

			internal readonly Org.Apache.Hadoop.Hdfs.Protocol.Block reportedBlock;

			internal readonly HdfsServerConstants.ReplicaState reportedState;

			internal StatefulBlockInfo(BlockInfoContiguousUnderConstruction storedBlock, Org.Apache.Hadoop.Hdfs.Protocol.Block
				 reportedBlock, HdfsServerConstants.ReplicaState reportedState)
			{
				/* If we know the target datanodes where the replication timedout,
				* we could invoke decBlocksScheduled() on it. Its ok for now.
				*/
				this.storedBlock = storedBlock;
				this.reportedBlock = reportedBlock;
				this.reportedState = reportedState;
			}
		}

		/// <summary>
		/// BlockToMarkCorrupt is used to build the "toCorrupt" list, which is a
		/// list of blocks that should be considered corrupt due to a block report.
		/// </summary>
		private class BlockToMarkCorrupt
		{
			/// <summary>The corrupted block in a datanode.</summary>
			internal readonly BlockInfoContiguous corrupted;

			/// <summary>The corresponding block stored in the BlockManager.</summary>
			internal readonly BlockInfoContiguous stored;

			/// <summary>The reason to mark corrupt.</summary>
			internal readonly string reason;

			/// <summary>The reason code to be stored</summary>
			internal readonly CorruptReplicasMap.Reason reasonCode;

			internal BlockToMarkCorrupt(BlockInfoContiguous corrupted, BlockInfoContiguous stored
				, string reason, CorruptReplicasMap.Reason reasonCode)
			{
				Preconditions.CheckNotNull(corrupted, "corrupted is null");
				Preconditions.CheckNotNull(stored, "stored is null");
				this.corrupted = corrupted;
				this.stored = stored;
				this.reason = reason;
				this.reasonCode = reasonCode;
			}

			internal BlockToMarkCorrupt(BlockInfoContiguous stored, string reason, CorruptReplicasMap.Reason
				 reasonCode)
				: this(stored, stored, reason, reasonCode)
			{
			}

			internal BlockToMarkCorrupt(BlockInfoContiguous stored, long gs, string reason, CorruptReplicasMap.Reason
				 reasonCode)
				: this(new BlockInfoContiguous(stored), stored, reason, reasonCode)
			{
				//the corrupted block in datanode has a different generation stamp
				corrupted.SetGenerationStamp(gs);
			}

			public override string ToString()
			{
				return corrupted + "(" + (corrupted == stored ? "same as stored" : "stored=" + stored
					) + ")";
			}
		}

		/// <summary>The given storage is reporting all its blocks.</summary>
		/// <remarks>
		/// The given storage is reporting all its blocks.
		/// Update the (storage--&gt;block list) and (block--&gt;storage list) maps.
		/// </remarks>
		/// <returns>true if all known storages of the given DN have finished reporting.</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual bool ProcessReport(DatanodeID nodeID, DatanodeStorage storage, BlockListAsLongs
			 newReport, BlockReportContext context, bool lastStorageInRpc)
		{
			namesystem.WriteLock();
			long startTime = Time.MonotonicNow();
			//after acquiring write lock
			long endTime;
			DatanodeDescriptor node;
			ICollection<Org.Apache.Hadoop.Hdfs.Protocol.Block> invalidatedBlocks = null;
			try
			{
				node = datanodeManager.GetDatanode(nodeID);
				if (node == null || !node.isAlive)
				{
					throw new IOException("ProcessReport from dead or unregistered node: " + nodeID);
				}
				// To minimize startup time, we discard any second (or later) block reports
				// that we receive while still in startup phase.
				DatanodeStorageInfo storageInfo = node.GetStorageInfo(storage.GetStorageID());
				if (storageInfo == null)
				{
					// We handle this for backwards compatibility.
					storageInfo = node.UpdateStorage(storage);
				}
				if (namesystem.IsInStartupSafeMode() && storageInfo.GetBlockReportCount() > 0)
				{
					blockLog.Info("BLOCK* processReport: " + "discarded non-initial block report from {}"
						 + " because namenode still in startup phase", nodeID);
					return !node.HasStaleStorages();
				}
				if (storageInfo.GetBlockReportCount() == 0)
				{
					// The first block report can be processed a lot more efficiently than
					// ordinary block reports.  This shortens restart times.
					ProcessFirstBlockReport(storageInfo, newReport);
				}
				else
				{
					invalidatedBlocks = ProcessReport(storageInfo, newReport);
				}
				storageInfo.ReceivedBlockReport();
				if (context != null)
				{
					storageInfo.SetLastBlockReportId(context.GetReportId());
					if (lastStorageInRpc)
					{
						int rpcsSeen = node.UpdateBlockReportContext(context);
						if (rpcsSeen >= context.GetTotalRpcs())
						{
							IList<DatanodeStorageInfo> zombies = node.RemoveZombieStorages();
							if (zombies.IsEmpty())
							{
								Log.Debug("processReport 0x{}: no zombie storages found.", long.ToHexString(context
									.GetReportId()));
							}
							else
							{
								foreach (DatanodeStorageInfo zombie in zombies)
								{
									RemoveZombieReplicas(context, zombie);
								}
							}
							node.ClearBlockReportContext();
						}
						else
						{
							Log.Debug("processReport 0x{}: {} more RPCs remaining in this " + "report.", long
								.ToHexString(context.GetReportId()), (context.GetTotalRpcs() - rpcsSeen));
						}
					}
				}
			}
			finally
			{
				endTime = Time.MonotonicNow();
				namesystem.WriteUnlock();
			}
			if (invalidatedBlocks != null)
			{
				foreach (Org.Apache.Hadoop.Hdfs.Protocol.Block b in invalidatedBlocks)
				{
					blockLog.Info("BLOCK* processReport: {} on node {} size {} does not " + "belong to any file"
						, b, node, b.GetNumBytes());
				}
			}
			// Log the block report processing stats from Namenode perspective
			NameNodeMetrics metrics = NameNode.GetNameNodeMetrics();
			if (metrics != null)
			{
				metrics.AddBlockReport((int)(endTime - startTime));
			}
			blockLog.Info("BLOCK* processReport: from storage {} node {}, " + "blocks: {}, hasStaleStorage: {}, processing time: {} msecs"
				, storage.GetStorageID(), nodeID, newReport.GetNumberOfBlocks(), node.HasStaleStorages
				(), (endTime - startTime));
			return !node.HasStaleStorages();
		}

		private void RemoveZombieReplicas(BlockReportContext context, DatanodeStorageInfo
			 zombie)
		{
			Log.Warn("processReport 0x{}: removing zombie storage {}, which no " + "longer exists on the DataNode."
				, long.ToHexString(context.GetReportId()), zombie.GetStorageID());
			System.Diagnostics.Debug.Assert((namesystem.HasWriteLock()));
			IEnumerator<BlockInfoContiguous> iter = zombie.GetBlockIterator();
			int prevBlocks = zombie.NumBlocks();
			while (iter.HasNext())
			{
				BlockInfoContiguous block = iter.Next();
				// We assume that a block can be on only one storage in a DataNode.
				// That's why we pass in the DatanodeDescriptor rather than the
				// DatanodeStorageInfo.
				// TODO: remove this assumption in case we want to put a block on
				// more than one storage on a datanode (and because it's a difficult
				// assumption to really enforce)
				RemoveStoredBlock(block, zombie.GetDatanodeDescriptor());
				invalidateBlocks.Remove(zombie.GetDatanodeDescriptor(), block);
			}
			System.Diagnostics.Debug.Assert((zombie.NumBlocks() == 0));
			Log.Warn("processReport 0x{}: removed {} replicas from storage {}, " + "which no longer exists on the DataNode."
				, long.ToHexString(context.GetReportId()), prevBlocks, zombie.GetStorageID());
		}

		/// <summary>Rescan the list of blocks which were previously postponed.</summary>
		internal virtual void RescanPostponedMisreplicatedBlocks()
		{
			if (GetPostponedMisreplicatedBlocksCount() == 0)
			{
				return;
			}
			long startTimeRescanPostponedMisReplicatedBlocks = Time.MonotonicNow();
			long startPostponedMisReplicatedBlocksCount = GetPostponedMisreplicatedBlocksCount
				();
			namesystem.WriteLock();
			try
			{
				// blocksPerRescan is the configured number of blocks per rescan.
				// Randomly select blocksPerRescan consecutive blocks from the HashSet
				// when the number of blocks remaining is larger than blocksPerRescan.
				// The reason we don't always pick the first blocksPerRescan blocks is to
				// handle the case if for some reason some datanodes remain in
				// content stale state for a long time and only impact the first
				// blocksPerRescan blocks.
				int i = 0;
				long startIndex = 0;
				long blocksPerRescan = datanodeManager.GetBlocksPerPostponedMisreplicatedBlocksRescan
					();
				long @base = GetPostponedMisreplicatedBlocksCount() - blocksPerRescan;
				if (@base > 0)
				{
					startIndex = DFSUtil.GetRandom().NextLong() % (@base + 1);
					if (startIndex < 0)
					{
						startIndex += (@base + 1);
					}
				}
				IEnumerator<Org.Apache.Hadoop.Hdfs.Protocol.Block> it = postponedMisreplicatedBlocks
					.GetEnumerator();
				for (int tmp = 0; tmp < startIndex; tmp++)
				{
					it.Next();
				}
				for (; it.HasNext(); i++)
				{
					Org.Apache.Hadoop.Hdfs.Protocol.Block b = it.Next();
					if (i >= blocksPerRescan)
					{
						break;
					}
					BlockInfoContiguous bi = blocksMap.GetStoredBlock(b);
					if (bi == null)
					{
						if (Log.IsDebugEnabled())
						{
							Log.Debug("BLOCK* rescanPostponedMisreplicatedBlocks: " + "Postponed mis-replicated block "
								 + b + " no longer found " + "in block map.");
						}
						it.Remove();
						postponedMisreplicatedBlocksCount.DecrementAndGet();
						continue;
					}
					BlockManager.MisReplicationResult res = ProcessMisReplicatedBlock(bi);
					if (Log.IsDebugEnabled())
					{
						Log.Debug("BLOCK* rescanPostponedMisreplicatedBlocks: " + "Re-scanned block " + b
							 + ", result is " + res);
					}
					if (res != BlockManager.MisReplicationResult.Postpone)
					{
						it.Remove();
						postponedMisreplicatedBlocksCount.DecrementAndGet();
					}
				}
			}
			finally
			{
				namesystem.WriteUnlock();
				long endPostponedMisReplicatedBlocksCount = GetPostponedMisreplicatedBlocksCount(
					);
				Log.Info("Rescan of postponedMisreplicatedBlocks completed in " + (Time.MonotonicNow
					() - startTimeRescanPostponedMisReplicatedBlocks) + " msecs. " + endPostponedMisReplicatedBlocksCount
					 + " blocks are left. " + (startPostponedMisReplicatedBlocksCount - endPostponedMisReplicatedBlocksCount
					) + " blocks are removed.");
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private ICollection<Org.Apache.Hadoop.Hdfs.Protocol.Block> ProcessReport(DatanodeStorageInfo
			 storageInfo, BlockListAsLongs report)
		{
			// Normal case:
			// Modify the (block-->datanode) map, according to the difference
			// between the old and new block report.
			//
			ICollection<BlockInfoContiguous> toAdd = new List<BlockInfoContiguous>();
			ICollection<Org.Apache.Hadoop.Hdfs.Protocol.Block> toRemove = new TreeSet<Org.Apache.Hadoop.Hdfs.Protocol.Block
				>();
			ICollection<Org.Apache.Hadoop.Hdfs.Protocol.Block> toInvalidate = new List<Org.Apache.Hadoop.Hdfs.Protocol.Block
				>();
			ICollection<BlockManager.BlockToMarkCorrupt> toCorrupt = new List<BlockManager.BlockToMarkCorrupt
				>();
			ICollection<BlockManager.StatefulBlockInfo> toUC = new List<BlockManager.StatefulBlockInfo
				>();
			ReportDiff(storageInfo, report, toAdd, toRemove, toInvalidate, toCorrupt, toUC);
			DatanodeDescriptor node = storageInfo.GetDatanodeDescriptor();
			// Process the blocks on each queue
			foreach (BlockManager.StatefulBlockInfo b in toUC)
			{
				AddStoredBlockUnderConstruction(b, storageInfo);
			}
			foreach (Org.Apache.Hadoop.Hdfs.Protocol.Block b_1 in toRemove)
			{
				RemoveStoredBlock(b_1, node);
			}
			int numBlocksLogged = 0;
			foreach (BlockInfoContiguous b_2 in toAdd)
			{
				AddStoredBlock(b_2, storageInfo, null, numBlocksLogged < maxNumBlocksToLog);
				numBlocksLogged++;
			}
			if (numBlocksLogged > maxNumBlocksToLog)
			{
				blockLog.Info("BLOCK* processReport: logged info for {} of {} " + "reported.", maxNumBlocksToLog
					, numBlocksLogged);
			}
			foreach (Org.Apache.Hadoop.Hdfs.Protocol.Block b_3 in toInvalidate)
			{
				AddToInvalidates(b_3, node);
			}
			foreach (BlockManager.BlockToMarkCorrupt b_4 in toCorrupt)
			{
				MarkBlockAsCorrupt(b_4, storageInfo, node);
			}
			return toInvalidate;
		}

		/// <summary>
		/// Mark block replicas as corrupt except those on the storages in
		/// newStorages list.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void MarkBlockReplicasAsCorrupt(BlockInfoContiguous block, long oldGenerationStamp
			, long oldNumBytes, DatanodeStorageInfo[] newStorages)
		{
			System.Diagnostics.Debug.Assert(namesystem.HasWriteLock());
			BlockManager.BlockToMarkCorrupt b = null;
			if (block.GetGenerationStamp() != oldGenerationStamp)
			{
				b = new BlockManager.BlockToMarkCorrupt(block, oldGenerationStamp, "genstamp does not match "
					 + oldGenerationStamp + " : " + block.GetGenerationStamp(), CorruptReplicasMap.Reason
					.GenstampMismatch);
			}
			else
			{
				if (block.GetNumBytes() != oldNumBytes)
				{
					b = new BlockManager.BlockToMarkCorrupt(block, "length does not match " + oldNumBytes
						 + " : " + block.GetNumBytes(), CorruptReplicasMap.Reason.SizeMismatch);
				}
				else
				{
					return;
				}
			}
			foreach (DatanodeStorageInfo storage in GetStorages(block))
			{
				bool isCorrupt = true;
				if (newStorages != null)
				{
					foreach (DatanodeStorageInfo newStorage in newStorages)
					{
						if (newStorage != null && storage.Equals(newStorage))
						{
							isCorrupt = false;
							break;
						}
					}
				}
				if (isCorrupt)
				{
					blockLog.Info("BLOCK* markBlockReplicasAsCorrupt: mark block replica" + " {} on {} as corrupt because the dn is not in the new committed "
						 + "storage list.", b, storage.GetDatanodeDescriptor());
					MarkBlockAsCorrupt(b, storage, storage.GetDatanodeDescriptor());
				}
			}
		}

		/// <summary>
		/// processFirstBlockReport is intended only for processing "initial" block
		/// reports, the first block report received from a DN after it registers.
		/// </summary>
		/// <remarks>
		/// processFirstBlockReport is intended only for processing "initial" block
		/// reports, the first block report received from a DN after it registers.
		/// It just adds all the valid replicas to the datanode, without calculating
		/// a toRemove list (since there won't be any).  It also silently discards
		/// any invalid blocks, thereby deferring their processing until
		/// the next block report.
		/// </remarks>
		/// <param name="storageInfo">- DatanodeStorageInfo that sent the report</param>
		/// <param name="report">- the initial block report, to be processed</param>
		/// <exception cref="System.IO.IOException"></exception>
		private void ProcessFirstBlockReport(DatanodeStorageInfo storageInfo, BlockListAsLongs
			 report)
		{
			if (report == null)
			{
				return;
			}
			System.Diagnostics.Debug.Assert((namesystem.HasWriteLock()));
			System.Diagnostics.Debug.Assert((storageInfo.GetBlockReportCount() == 0));
			foreach (BlockListAsLongs.BlockReportReplica iblk in report)
			{
				HdfsServerConstants.ReplicaState reportedState = iblk.GetState();
				if (shouldPostponeBlocksFromFuture && namesystem.IsGenStampInFuture(iblk))
				{
					QueueReportedBlock(storageInfo, iblk, reportedState, QueueReasonFutureGenstamp);
					continue;
				}
				BlockInfoContiguous storedBlock = blocksMap.GetStoredBlock(iblk);
				// If block does not belong to any file, we are done.
				if (storedBlock == null)
				{
					continue;
				}
				// If block is corrupt, mark it and continue to next block.
				HdfsServerConstants.BlockUCState ucState = storedBlock.GetBlockUCState();
				BlockManager.BlockToMarkCorrupt c = CheckReplicaCorrupt(iblk, reportedState, storedBlock
					, ucState, storageInfo.GetDatanodeDescriptor());
				if (c != null)
				{
					if (shouldPostponeBlocksFromFuture)
					{
						// In the Standby, we may receive a block report for a file that we
						// just have an out-of-date gen-stamp or state for, for example.
						QueueReportedBlock(storageInfo, iblk, reportedState, QueueReasonCorruptState);
					}
					else
					{
						MarkBlockAsCorrupt(c, storageInfo, storageInfo.GetDatanodeDescriptor());
					}
					continue;
				}
				// If block is under construction, add this replica to its list
				if (IsBlockUnderConstruction(storedBlock, ucState, reportedState))
				{
					((BlockInfoContiguousUnderConstruction)storedBlock).AddReplicaIfNotPresent(storageInfo
						, iblk, reportedState);
					// OpenFileBlocks only inside snapshots also will be added to safemode
					// threshold. So we need to update such blocks to safemode
					// refer HDFS-5283
					BlockInfoContiguousUnderConstruction blockUC = (BlockInfoContiguousUnderConstruction
						)storedBlock;
					if (namesystem.IsInSnapshot(blockUC))
					{
						int numOfReplicas = blockUC.GetNumExpectedLocations();
						namesystem.IncrementSafeBlockCount(numOfReplicas);
					}
				}
				//and fall through to next clause
				//add replica if appropriate
				if (reportedState == HdfsServerConstants.ReplicaState.Finalized)
				{
					AddStoredBlockImmediate(storedBlock, storageInfo);
				}
			}
		}

		private void ReportDiff(DatanodeStorageInfo storageInfo, BlockListAsLongs newReport
			, ICollection<BlockInfoContiguous> toAdd, ICollection<Org.Apache.Hadoop.Hdfs.Protocol.Block
			> toRemove, ICollection<Org.Apache.Hadoop.Hdfs.Protocol.Block> toInvalidate, ICollection
			<BlockManager.BlockToMarkCorrupt> toCorrupt, ICollection<BlockManager.StatefulBlockInfo
			> toUC)
		{
			// add to DatanodeDescriptor
			// remove from DatanodeDescriptor
			// should be removed from DN
			// add to corrupt replicas list
			// add to under-construction list
			// place a delimiter in the list which separates blocks 
			// that have been reported from those that have not
			BlockInfoContiguous delimiter = new BlockInfoContiguous(new Org.Apache.Hadoop.Hdfs.Protocol.Block
				(), (short)1);
			DatanodeStorageInfo.AddBlockResult result = storageInfo.AddBlock(delimiter);
			System.Diagnostics.Debug.Assert(result == DatanodeStorageInfo.AddBlockResult.Added
				, "Delimiting block cannot be present in the node");
			int headIndex = 0;
			//currently the delimiter is in the head of the list
			int curIndex;
			if (newReport == null)
			{
				newReport = BlockListAsLongs.Empty;
			}
			// scan the report and process newly reported blocks
			foreach (BlockListAsLongs.BlockReportReplica iblk in newReport)
			{
				HdfsServerConstants.ReplicaState iState = iblk.GetState();
				BlockInfoContiguous storedBlock = ProcessReportedBlock(storageInfo, iblk, iState, 
					toAdd, toInvalidate, toCorrupt, toUC);
				// move block to the head of the list
				if (storedBlock != null && (curIndex = storedBlock.FindStorageInfo(storageInfo)) 
					>= 0)
				{
					headIndex = storageInfo.MoveBlockToHead(storedBlock, curIndex, headIndex);
				}
			}
			// collect blocks that have not been reported
			// all of them are next to the delimiter
			IEnumerator<BlockInfoContiguous> it = new DatanodeStorageInfo.BlockIterator(this, 
				delimiter.GetNext(0));
			while (it.HasNext())
			{
				toRemove.AddItem(it.Next());
			}
			storageInfo.RemoveBlock(delimiter);
		}

		/// <summary>Process a block replica reported by the data-node.</summary>
		/// <remarks>
		/// Process a block replica reported by the data-node.
		/// No side effects except adding to the passed-in Collections.
		/// <ol>
		/// <li>If the block is not known to the system (not in blocksMap) then the
		/// data-node should be notified to invalidate this block.</li>
		/// <li>If the reported replica is valid that is has the same generation stamp
		/// and length as recorded on the name-node, then the replica location should
		/// be added to the name-node.</li>
		/// <li>If the reported replica is not valid, then it is marked as corrupt,
		/// which triggers replication of the existing valid replicas.
		/// Corrupt replicas are removed from the system when the block
		/// is fully replicated.</li>
		/// <li>If the reported replica is for a block currently marked "under
		/// construction" in the NN, then it should be added to the
		/// BlockInfoUnderConstruction's list of replicas.</li>
		/// </ol>
		/// </remarks>
		/// <param name="storageInfo">DatanodeStorageInfo that sent the report.</param>
		/// <param name="block">reported block replica</param>
		/// <param name="reportedState">reported replica state</param>
		/// <param name="toAdd">add to DatanodeDescriptor</param>
		/// <param name="toInvalidate">
		/// missing blocks (not in the blocks map)
		/// should be removed from the data-node
		/// </param>
		/// <param name="toCorrupt">
		/// replicas with unexpected length or generation stamp;
		/// add to corrupt replicas
		/// </param>
		/// <param name="toUC">replicas of blocks currently under construction</param>
		/// <returns>
		/// the up-to-date stored block, if it should be kept.
		/// Otherwise, null.
		/// </returns>
		private BlockInfoContiguous ProcessReportedBlock(DatanodeStorageInfo storageInfo, 
			Org.Apache.Hadoop.Hdfs.Protocol.Block block, HdfsServerConstants.ReplicaState reportedState
			, ICollection<BlockInfoContiguous> toAdd, ICollection<Org.Apache.Hadoop.Hdfs.Protocol.Block
			> toInvalidate, ICollection<BlockManager.BlockToMarkCorrupt> toCorrupt, ICollection
			<BlockManager.StatefulBlockInfo> toUC)
		{
			DatanodeDescriptor dn = storageInfo.GetDatanodeDescriptor();
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Reported block " + block + " on " + dn + " size " + block.GetNumBytes(
					) + " replicaState = " + reportedState);
			}
			if (shouldPostponeBlocksFromFuture && namesystem.IsGenStampInFuture(block))
			{
				QueueReportedBlock(storageInfo, block, reportedState, QueueReasonFutureGenstamp);
				return null;
			}
			// find block by blockId
			BlockInfoContiguous storedBlock = blocksMap.GetStoredBlock(block);
			if (storedBlock == null)
			{
				// If blocksMap does not contain reported block id,
				// the replica should be removed from the data-node.
				toInvalidate.AddItem(new Org.Apache.Hadoop.Hdfs.Protocol.Block(block));
				return null;
			}
			HdfsServerConstants.BlockUCState ucState = storedBlock.GetBlockUCState();
			// Block is on the NN
			if (Log.IsDebugEnabled())
			{
				Log.Debug("In memory blockUCState = " + ucState);
			}
			// Ignore replicas already scheduled to be removed from the DN
			if (invalidateBlocks.Contains(dn, block))
			{
				/*
				* TODO: following assertion is incorrect, see HDFS-2668 assert
				* storedBlock.findDatanode(dn) < 0 : "Block " + block +
				* " in recentInvalidatesSet should not appear in DN " + dn;
				*/
				return storedBlock;
			}
			BlockManager.BlockToMarkCorrupt c = CheckReplicaCorrupt(block, reportedState, storedBlock
				, ucState, dn);
			if (c != null)
			{
				if (shouldPostponeBlocksFromFuture)
				{
					// If the block is an out-of-date generation stamp or state,
					// but we're the standby, we shouldn't treat it as corrupt,
					// but instead just queue it for later processing.
					// TODO: Pretty confident this should be s/storedBlock/block below,
					// since we should be postponing the info of the reported block, not
					// the stored block. See HDFS-6289 for more context.
					QueueReportedBlock(storageInfo, storedBlock, reportedState, QueueReasonCorruptState
						);
				}
				else
				{
					toCorrupt.AddItem(c);
				}
				return storedBlock;
			}
			if (IsBlockUnderConstruction(storedBlock, ucState, reportedState))
			{
				toUC.AddItem(new BlockManager.StatefulBlockInfo((BlockInfoContiguousUnderConstruction
					)storedBlock, new Org.Apache.Hadoop.Hdfs.Protocol.Block(block), reportedState));
				return storedBlock;
			}
			// Add replica if appropriate. If the replica was previously corrupt
			// but now okay, it might need to be updated.
			if (reportedState == HdfsServerConstants.ReplicaState.Finalized && (storedBlock.FindStorageInfo
				(storageInfo) == -1 || corruptReplicas.IsReplicaCorrupt(storedBlock, dn)))
			{
				toAdd.AddItem(storedBlock);
			}
			return storedBlock;
		}

		/// <summary>
		/// Queue the given reported block for later processing in the
		/// standby node.
		/// </summary>
		/// <remarks>
		/// Queue the given reported block for later processing in the
		/// standby node. @see PendingDataNodeMessages.
		/// </remarks>
		/// <param name="reason">a textual reason to report in the debug logs</param>
		private void QueueReportedBlock(DatanodeStorageInfo storageInfo, Org.Apache.Hadoop.Hdfs.Protocol.Block
			 block, HdfsServerConstants.ReplicaState reportedState, string reason)
		{
			System.Diagnostics.Debug.Assert(shouldPostponeBlocksFromFuture);
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Queueing reported block " + block + " in state " + reportedState + " from datanode "
					 + storageInfo.GetDatanodeDescriptor() + " for later processing because " + reason
					 + ".");
			}
			pendingDNMessages.EnqueueReportedBlock(storageInfo, block, reportedState);
		}

		/// <summary>
		/// Try to process any messages that were previously queued for the given
		/// block.
		/// </summary>
		/// <remarks>
		/// Try to process any messages that were previously queued for the given
		/// block. This is called from FSEditLogLoader whenever a block's state
		/// in the namespace has changed or a new block has been created.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public virtual void ProcessQueuedMessagesForBlock(Org.Apache.Hadoop.Hdfs.Protocol.Block
			 b)
		{
			Queue<PendingDataNodeMessages.ReportedBlockInfo> queue = pendingDNMessages.TakeBlockQueue
				(b);
			if (queue == null)
			{
				// Nothing to re-process
				return;
			}
			ProcessQueuedMessages(queue);
		}

		/// <exception cref="System.IO.IOException"/>
		private void ProcessQueuedMessages(IEnumerable<PendingDataNodeMessages.ReportedBlockInfo
			> rbis)
		{
			foreach (PendingDataNodeMessages.ReportedBlockInfo rbi in rbis)
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Processing previouly queued message " + rbi);
				}
				if (rbi.GetReportedState() == null)
				{
					// This is a DELETE_BLOCK request
					DatanodeStorageInfo storageInfo = rbi.GetStorageInfo();
					RemoveStoredBlock(rbi.GetBlock(), storageInfo.GetDatanodeDescriptor());
				}
				else
				{
					ProcessAndHandleReportedBlock(rbi.GetStorageInfo(), rbi.GetBlock(), rbi.GetReportedState
						(), null);
				}
			}
		}

		/// <summary>
		/// Process any remaining queued datanode messages after entering
		/// active state.
		/// </summary>
		/// <remarks>
		/// Process any remaining queued datanode messages after entering
		/// active state. At this point they will not be re-queued since
		/// we are the definitive master node and thus should be up-to-date
		/// with the namespace information.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public virtual void ProcessAllPendingDNMessages()
		{
			System.Diagnostics.Debug.Assert(!shouldPostponeBlocksFromFuture, "processAllPendingDNMessages() should be called after disabling "
				 + "block postponement.");
			int count = pendingDNMessages.Count();
			if (count > 0)
			{
				Log.Info("Processing " + count + " messages from DataNodes " + "that were previously queued during standby state"
					);
			}
			ProcessQueuedMessages(pendingDNMessages.TakeAll());
			System.Diagnostics.Debug.Assert(pendingDNMessages.Count() == 0);
		}

		/// <summary>
		/// The next two methods test the various cases under which we must conclude
		/// the replica is corrupt, or under construction.
		/// </summary>
		/// <remarks>
		/// The next two methods test the various cases under which we must conclude
		/// the replica is corrupt, or under construction.  These are laid out
		/// as switch statements, on the theory that it is easier to understand
		/// the combinatorics of reportedState and ucState that way.  It should be
		/// at least as efficient as boolean expressions.
		/// </remarks>
		/// <returns>a BlockToMarkCorrupt object, or null if the replica is not corrupt</returns>
		private BlockManager.BlockToMarkCorrupt CheckReplicaCorrupt(Org.Apache.Hadoop.Hdfs.Protocol.Block
			 reported, HdfsServerConstants.ReplicaState reportedState, BlockInfoContiguous storedBlock
			, HdfsServerConstants.BlockUCState ucState, DatanodeDescriptor dn)
		{
			switch (reportedState)
			{
				case HdfsServerConstants.ReplicaState.Finalized:
				{
					switch (ucState)
					{
						case HdfsServerConstants.BlockUCState.Complete:
						case HdfsServerConstants.BlockUCState.Committed:
						{
							if (storedBlock.GetGenerationStamp() != reported.GetGenerationStamp())
							{
								long reportedGS = reported.GetGenerationStamp();
								return new BlockManager.BlockToMarkCorrupt(storedBlock, reportedGS, "block is " +
									 ucState + " and reported genstamp " + reportedGS + " does not match genstamp in block map "
									 + storedBlock.GetGenerationStamp(), CorruptReplicasMap.Reason.GenstampMismatch);
							}
							else
							{
								if (storedBlock.GetNumBytes() != reported.GetNumBytes())
								{
									return new BlockManager.BlockToMarkCorrupt(storedBlock, "block is " + ucState + " and reported length "
										 + reported.GetNumBytes() + " does not match " + "length in block map " + storedBlock
										.GetNumBytes(), CorruptReplicasMap.Reason.SizeMismatch);
								}
								else
								{
									return null;
								}
							}
							goto case HdfsServerConstants.BlockUCState.UnderConstruction;
						}

						case HdfsServerConstants.BlockUCState.UnderConstruction:
						{
							// not corrupt
							if (storedBlock.GetGenerationStamp() > reported.GetGenerationStamp())
							{
								long reportedGS = reported.GetGenerationStamp();
								return new BlockManager.BlockToMarkCorrupt(storedBlock, reportedGS, "block is " +
									 ucState + " and reported state " + reportedState + ", But reported genstamp " +
									 reportedGS + " does not match genstamp in block map " + storedBlock.GetGenerationStamp
									(), CorruptReplicasMap.Reason.GenstampMismatch);
							}
							return null;
						}

						default:
						{
							return null;
						}
					}
					goto case HdfsServerConstants.ReplicaState.Rbw;
				}

				case HdfsServerConstants.ReplicaState.Rbw:
				case HdfsServerConstants.ReplicaState.Rwr:
				{
					if (!storedBlock.IsComplete())
					{
						return null;
					}
					else
					{
						// not corrupt
						if (storedBlock.GetGenerationStamp() != reported.GetGenerationStamp())
						{
							long reportedGS = reported.GetGenerationStamp();
							return new BlockManager.BlockToMarkCorrupt(storedBlock, reportedGS, "reported " +
								 reportedState + " replica with genstamp " + reportedGS + " does not match COMPLETE block's genstamp in block map "
								 + storedBlock.GetGenerationStamp(), CorruptReplicasMap.Reason.GenstampMismatch);
						}
						else
						{
							// COMPLETE block, same genstamp
							if (reportedState == HdfsServerConstants.ReplicaState.Rbw)
							{
								// If it's a RBW report for a COMPLETE block, it may just be that
								// the block report got a little bit delayed after the pipeline
								// closed. So, ignore this report, assuming we will get a
								// FINALIZED replica later. See HDFS-2791
								Log.Info("Received an RBW replica for " + storedBlock + " on " + dn + ": ignoring it, since it is "
									 + "complete with the same genstamp");
								return null;
							}
							else
							{
								return new BlockManager.BlockToMarkCorrupt(storedBlock, "reported replica has invalid state "
									 + reportedState, CorruptReplicasMap.Reason.InvalidState);
							}
						}
					}
					goto case HdfsServerConstants.ReplicaState.Rur;
				}

				case HdfsServerConstants.ReplicaState.Rur:
				case HdfsServerConstants.ReplicaState.Temporary:
				default:
				{
					// should not be reported
					// should not be reported
					string msg = "Unexpected replica state " + reportedState + " for block: " + storedBlock
						 + " on " + dn + " size " + storedBlock.GetNumBytes();
					// log here at WARN level since this is really a broken HDFS invariant
					Log.Warn(msg);
					return new BlockManager.BlockToMarkCorrupt(storedBlock, msg, CorruptReplicasMap.Reason
						.InvalidState);
				}
			}
		}

		private bool IsBlockUnderConstruction(BlockInfoContiguous storedBlock, HdfsServerConstants.BlockUCState
			 ucState, HdfsServerConstants.ReplicaState reportedState)
		{
			switch (reportedState)
			{
				case HdfsServerConstants.ReplicaState.Finalized:
				{
					switch (ucState)
					{
						case HdfsServerConstants.BlockUCState.UnderConstruction:
						case HdfsServerConstants.BlockUCState.UnderRecovery:
						{
							return true;
						}

						default:
						{
							return false;
						}
					}
					goto case HdfsServerConstants.ReplicaState.Rbw;
				}

				case HdfsServerConstants.ReplicaState.Rbw:
				case HdfsServerConstants.ReplicaState.Rwr:
				{
					return (!storedBlock.IsComplete());
				}

				case HdfsServerConstants.ReplicaState.Rur:
				case HdfsServerConstants.ReplicaState.Temporary:
				default:
				{
					// should not be reported                                                                                             
					// should not be reported                                                                                             
					return false;
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void AddStoredBlockUnderConstruction(BlockManager.StatefulBlockInfo
			 ucBlock, DatanodeStorageInfo storageInfo)
		{
			BlockInfoContiguousUnderConstruction block = ucBlock.storedBlock;
			block.AddReplicaIfNotPresent(storageInfo, ucBlock.reportedBlock, ucBlock.reportedState
				);
			if (ucBlock.reportedState == HdfsServerConstants.ReplicaState.Finalized && !block
				.FindDatanode(storageInfo.GetDatanodeDescriptor()))
			{
				AddStoredBlock(block, storageInfo, null, true);
			}
		}

		/// <summary>
		/// Faster version of
		/// <see cref="AddStoredBlock(BlockInfoContiguous, DatanodeStorageInfo, DatanodeDescriptor, bool)
		/// 	"/>
		/// ,
		/// intended for use with initial block report at startup. If not in startup
		/// safe mode, will call standard addStoredBlock(). Assumes this method is
		/// called "immediately" so there is no need to refresh the storedBlock from
		/// blocksMap. Doesn't handle underReplication/overReplication, or worry about
		/// pendingReplications or corruptReplicas, because it's in startup safe mode.
		/// Doesn't log every block, because there are typically millions of them.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		private void AddStoredBlockImmediate(BlockInfoContiguous storedBlock, DatanodeStorageInfo
			 storageInfo)
		{
			System.Diagnostics.Debug.Assert((storedBlock != null && namesystem.HasWriteLock()
				));
			if (!namesystem.IsInStartupSafeMode() || namesystem.IsPopulatingReplQueues())
			{
				AddStoredBlock(storedBlock, storageInfo, null, false);
				return;
			}
			// just add it
			DatanodeStorageInfo.AddBlockResult result = storageInfo.AddBlock(storedBlock);
			// Now check for completion of blocks and safe block count
			int numCurrentReplica = CountLiveNodes(storedBlock);
			if (storedBlock.GetBlockUCState() == HdfsServerConstants.BlockUCState.Committed &&
				 numCurrentReplica >= minReplication)
			{
				CompleteBlock(storedBlock.GetBlockCollection(), storedBlock, false);
			}
			else
			{
				if (storedBlock.IsComplete() && result == DatanodeStorageInfo.AddBlockResult.Added)
				{
					// check whether safe replication is reached for the block
					// only complete blocks are counted towards that.
					// In the case that the block just became complete above, completeBlock()
					// handles the safe block count maintenance.
					namesystem.IncrementSafeBlockCount(numCurrentReplica);
				}
			}
		}

		/// <summary>Modify (block--&gt;datanode) map.</summary>
		/// <remarks>
		/// Modify (block--&gt;datanode) map. Remove block from set of
		/// needed replications if this takes care of the problem.
		/// </remarks>
		/// <returns>the block that is stored in blockMap.</returns>
		/// <exception cref="System.IO.IOException"/>
		private Org.Apache.Hadoop.Hdfs.Protocol.Block AddStoredBlock(BlockInfoContiguous 
			block, DatanodeStorageInfo storageInfo, DatanodeDescriptor delNodeHint, bool logEveryBlock
			)
		{
			System.Diagnostics.Debug.Assert(block != null && namesystem.HasWriteLock());
			BlockInfoContiguous storedBlock;
			DatanodeDescriptor node = storageInfo.GetDatanodeDescriptor();
			if (block is BlockInfoContiguousUnderConstruction)
			{
				//refresh our copy in case the block got completed in another thread
				storedBlock = blocksMap.GetStoredBlock(block);
			}
			else
			{
				storedBlock = block;
			}
			if (storedBlock == null || storedBlock.GetBlockCollection() == null)
			{
				// If this block does not belong to anyfile, then we are done.
				blockLog.Info("BLOCK* addStoredBlock: {} on {} size {} but it does not" + " belong to any file"
					, block, node, block.GetNumBytes());
				// we could add this block to invalidate set of this datanode.
				// it will happen in next block report otherwise.
				return block;
			}
			BlockCollection bc = storedBlock.GetBlockCollection();
			System.Diagnostics.Debug.Assert(bc != null, "Block must belong to a file");
			// add block to the datanode
			DatanodeStorageInfo.AddBlockResult result = storageInfo.AddBlock(storedBlock);
			int curReplicaDelta;
			if (result == DatanodeStorageInfo.AddBlockResult.Added)
			{
				curReplicaDelta = 1;
				if (logEveryBlock)
				{
					LogAddStoredBlock(storedBlock, node);
				}
			}
			else
			{
				if (result == DatanodeStorageInfo.AddBlockResult.Replaced)
				{
					curReplicaDelta = 0;
					blockLog.Warn("BLOCK* addStoredBlock: block {} moved to storageType " + "{} on node {}"
						, storedBlock, storageInfo.GetStorageType(), node);
				}
				else
				{
					// if the same block is added again and the replica was corrupt
					// previously because of a wrong gen stamp, remove it from the
					// corrupt block list.
					corruptReplicas.RemoveFromCorruptReplicasMap(block, node, CorruptReplicasMap.Reason
						.GenstampMismatch);
					curReplicaDelta = 0;
					blockLog.Warn("BLOCK* addStoredBlock: Redundant addStoredBlock request" + " received for {} on node {} size {}"
						, storedBlock, node, storedBlock.GetNumBytes());
				}
			}
			// Now check for completion of blocks and safe block count
			NumberReplicas num = CountNodes(storedBlock);
			int numLiveReplicas = num.LiveReplicas();
			int numCurrentReplica = numLiveReplicas + pendingReplications.GetNumReplicas(storedBlock
				);
			if (storedBlock.GetBlockUCState() == HdfsServerConstants.BlockUCState.Committed &&
				 numLiveReplicas >= minReplication)
			{
				storedBlock = CompleteBlock(bc, storedBlock, false);
			}
			else
			{
				if (storedBlock.IsComplete() && result == DatanodeStorageInfo.AddBlockResult.Added)
				{
					// check whether safe replication is reached for the block
					// only complete blocks are counted towards that
					// Is no-op if not in safe mode.
					// In the case that the block just became complete above, completeBlock()
					// handles the safe block count maintenance.
					namesystem.IncrementSafeBlockCount(numCurrentReplica);
				}
			}
			// if file is under construction, then done for now
			if (bc.IsUnderConstruction())
			{
				return storedBlock;
			}
			// do not try to handle over/under-replicated blocks during first safe mode
			if (!namesystem.IsPopulatingReplQueues())
			{
				return storedBlock;
			}
			// handle underReplication/overReplication
			short fileReplication = bc.GetBlockReplication();
			if (!IsNeededReplication(storedBlock, fileReplication, numCurrentReplica))
			{
				neededReplications.Remove(storedBlock, numCurrentReplica, num.DecommissionedReplicas
					(), fileReplication);
			}
			else
			{
				UpdateNeededReplications(storedBlock, curReplicaDelta, 0);
			}
			if (numCurrentReplica > fileReplication)
			{
				ProcessOverReplicatedBlock(storedBlock, fileReplication, node, delNodeHint);
			}
			// If the file replication has reached desired value
			// we can remove any corrupt replicas the block may have
			int corruptReplicasCount = corruptReplicas.NumCorruptReplicas(storedBlock);
			int numCorruptNodes = num.CorruptReplicas();
			if (numCorruptNodes != corruptReplicasCount)
			{
				Log.Warn("Inconsistent number of corrupt replicas for " + storedBlock + "blockMap has "
					 + numCorruptNodes + " but corrupt replicas map has " + corruptReplicasCount);
			}
			if ((corruptReplicasCount > 0) && (numLiveReplicas >= fileReplication))
			{
				InvalidateCorruptReplicas(storedBlock);
			}
			return storedBlock;
		}

		private void LogAddStoredBlock(BlockInfoContiguous storedBlock, DatanodeDescriptor
			 node)
		{
			if (!blockLog.IsInfoEnabled())
			{
				return;
			}
			StringBuilder sb = new StringBuilder(500);
			sb.Append("BLOCK* addStoredBlock: blockMap updated: ").Append(node).Append(" is added to "
				);
			storedBlock.AppendStringTo(sb);
			sb.Append(" size ").Append(storedBlock.GetNumBytes());
			blockLog.Info(sb.ToString());
		}

		/// <summary>Invalidate corrupt replicas.</summary>
		/// <remarks>
		/// Invalidate corrupt replicas.
		/// <p>
		/// This will remove the replicas from the block's location list,
		/// add them to
		/// <see cref="invalidateBlocks"/>
		/// so that they could be further
		/// deleted from the respective data-nodes,
		/// and remove the block from corruptReplicasMap.
		/// <p>
		/// This method should be called when the block has sufficient
		/// number of live replicas.
		/// </remarks>
		/// <param name="blk">Block whose corrupt replicas need to be invalidated</param>
		private void InvalidateCorruptReplicas(BlockInfoContiguous blk)
		{
			ICollection<DatanodeDescriptor> nodes = corruptReplicas.GetNodes(blk);
			bool removedFromBlocksMap = true;
			if (nodes == null)
			{
				return;
			}
			// make a copy of the array of nodes in order to avoid
			// ConcurrentModificationException, when the block is removed from the node
			DatanodeDescriptor[] nodesCopy = Sharpen.Collections.ToArray(nodes, new DatanodeDescriptor
				[0]);
			foreach (DatanodeDescriptor node in nodesCopy)
			{
				try
				{
					if (!InvalidateBlock(new BlockManager.BlockToMarkCorrupt(blk, null, CorruptReplicasMap.Reason
						.Any), node))
					{
						removedFromBlocksMap = false;
					}
				}
				catch (IOException e)
				{
					blockLog.Info("invalidateCorruptReplicas error in deleting bad block" + " {} on {}"
						, blk, node, e);
					removedFromBlocksMap = false;
				}
			}
			// Remove the block from corruptReplicasMap
			if (removedFromBlocksMap)
			{
				corruptReplicas.RemoveFromCorruptReplicasMap(blk);
			}
		}

		/// <summary>
		/// For each block in the name-node verify whether it belongs to any file,
		/// over or under replicated.
		/// </summary>
		/// <remarks>
		/// For each block in the name-node verify whether it belongs to any file,
		/// over or under replicated. Place it into the respective queue.
		/// </remarks>
		public virtual void ProcessMisReplicatedBlocks()
		{
			System.Diagnostics.Debug.Assert(namesystem.HasWriteLock());
			StopReplicationInitializer();
			neededReplications.Clear();
			replicationQueuesInitializer = new _Daemon_2670(this);
			replicationQueuesInitializer.SetName("Replication Queue Initializer");
			replicationQueuesInitializer.Start();
		}

		private sealed class _Daemon_2670 : Daemon
		{
			public _Daemon_2670(BlockManager _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public override void Run()
			{
				try
				{
					this._enclosing.ProcessMisReplicatesAsync();
				}
				catch (Exception)
				{
					BlockManager.Log.Info("Interrupted while processing replication queues.");
				}
				catch (Exception e)
				{
					BlockManager.Log.Error("Error while processing replication queues async", e);
				}
			}

			private readonly BlockManager _enclosing;
		}

		/*
		* Stop the ongoing initialisation of replication queues
		*/
		private void StopReplicationInitializer()
		{
			if (replicationQueuesInitializer != null)
			{
				replicationQueuesInitializer.Interrupt();
				try
				{
					replicationQueuesInitializer.Join();
				}
				catch (Exception)
				{
					Log.Warn("Interrupted while waiting for replicationQueueInitializer. Returning.."
						);
					return;
				}
				finally
				{
					replicationQueuesInitializer = null;
				}
			}
		}

		/*
		* Since the BlocksMapGset does not throw the ConcurrentModificationException
		* and supports further iteration after modification to list, there is a
		* chance of missing the newly added block while iterating. Since every
		* addition to blocksMap will check for mis-replication, missing mis-replication
		* check for new blocks will not be a problem.
		*/
		/// <exception cref="System.Exception"/>
		private void ProcessMisReplicatesAsync()
		{
			long nrInvalid = 0;
			long nrOverReplicated = 0;
			long nrUnderReplicated = 0;
			long nrPostponed = 0;
			long nrUnderConstruction = 0;
			long startTimeMisReplicatedScan = Time.MonotonicNow();
			IEnumerator<BlockInfoContiguous> blocksItr = blocksMap.GetBlocks().GetEnumerator(
				);
			long totalBlocks = blocksMap.Size();
			replicationQueuesInitProgress = 0;
			long totalProcessed = 0;
			long sleepDuration = Math.Max(1, Math.Min(numBlocksPerIteration / 1000, 10000));
			while (namesystem.IsRunning() && !Sharpen.Thread.CurrentThread().IsInterrupted())
			{
				int processed = 0;
				namesystem.WriteLockInterruptibly();
				try
				{
					while (processed < numBlocksPerIteration && blocksItr.HasNext())
					{
						BlockInfoContiguous block = blocksItr.Next();
						BlockManager.MisReplicationResult res = ProcessMisReplicatedBlock(block);
						if (Log.IsTraceEnabled())
						{
							Log.Trace("block " + block + ": " + res);
						}
						switch (res)
						{
							case BlockManager.MisReplicationResult.UnderReplicated:
							{
								nrUnderReplicated++;
								break;
							}

							case BlockManager.MisReplicationResult.OverReplicated:
							{
								nrOverReplicated++;
								break;
							}

							case BlockManager.MisReplicationResult.Invalid:
							{
								nrInvalid++;
								break;
							}

							case BlockManager.MisReplicationResult.Postpone:
							{
								nrPostponed++;
								PostponeBlock(block);
								break;
							}

							case BlockManager.MisReplicationResult.UnderConstruction:
							{
								nrUnderConstruction++;
								break;
							}

							case BlockManager.MisReplicationResult.Ok:
							{
								break;
							}

							default:
							{
								throw new Exception("Invalid enum value: " + res);
							}
						}
						processed++;
					}
					totalProcessed += processed;
					// there is a possibility that if any of the blocks deleted/added during
					// initialisation, then progress might be different.
					replicationQueuesInitProgress = Math.Min((double)totalProcessed / totalBlocks, 1.0
						);
					if (!blocksItr.HasNext())
					{
						Log.Info("Total number of blocks            = " + blocksMap.Size());
						Log.Info("Number of invalid blocks          = " + nrInvalid);
						Log.Info("Number of under-replicated blocks = " + nrUnderReplicated);
						Log.Info("Number of  over-replicated blocks = " + nrOverReplicated + ((nrPostponed
							 > 0) ? (" (" + nrPostponed + " postponed)") : string.Empty));
						Log.Info("Number of blocks being written    = " + nrUnderConstruction);
						NameNode.stateChangeLog.Info("STATE* Replication Queue initialization " + "scan for invalid, over- and under-replicated blocks "
							 + "completed in " + (Time.MonotonicNow() - startTimeMisReplicatedScan) + " msec"
							);
						break;
					}
				}
				finally
				{
					namesystem.WriteUnlock();
					// Make sure it is out of the write lock for sufficiently long time.
					Sharpen.Thread.Sleep(sleepDuration);
				}
			}
			if (Sharpen.Thread.CurrentThread().IsInterrupted())
			{
				Log.Info("Interrupted while processing replication queues.");
			}
		}

		/// <summary>Get the progress of the Replication queues initialisation</summary>
		/// <returns>Returns values between 0 and 1 for the progress.</returns>
		public virtual double GetReplicationQueuesInitProgress()
		{
			return replicationQueuesInitProgress;
		}

		/// <summary>Process a single possibly misreplicated block.</summary>
		/// <remarks>
		/// Process a single possibly misreplicated block. This adds it to the
		/// appropriate queues if necessary, and returns a result code indicating
		/// what happened with it.
		/// </remarks>
		private BlockManager.MisReplicationResult ProcessMisReplicatedBlock(BlockInfoContiguous
			 block)
		{
			BlockCollection bc = block.GetBlockCollection();
			if (bc == null)
			{
				// block does not belong to any file
				AddToInvalidates(block);
				return BlockManager.MisReplicationResult.Invalid;
			}
			if (!block.IsComplete())
			{
				// Incomplete blocks are never considered mis-replicated --
				// they'll be reached when they are completed or recovered.
				return BlockManager.MisReplicationResult.UnderConstruction;
			}
			// calculate current replication
			short expectedReplication = bc.GetBlockReplication();
			NumberReplicas num = CountNodes(block);
			int numCurrentReplica = num.LiveReplicas();
			// add to under-replicated queue if need to be
			if (IsNeededReplication(block, expectedReplication, numCurrentReplica))
			{
				if (neededReplications.Add(block, numCurrentReplica, num.DecommissionedReplicas()
					, expectedReplication))
				{
					return BlockManager.MisReplicationResult.UnderReplicated;
				}
			}
			if (numCurrentReplica > expectedReplication)
			{
				if (num.ReplicasOnStaleNodes() > 0)
				{
					// If any of the replicas of this block are on nodes that are
					// considered "stale", then these replicas may in fact have
					// already been deleted. So, we cannot safely act on the
					// over-replication until a later point in time, when
					// the "stale" nodes have block reported.
					return BlockManager.MisReplicationResult.Postpone;
				}
				// over-replicated block
				ProcessOverReplicatedBlock(block, expectedReplication, null, null);
				return BlockManager.MisReplicationResult.OverReplicated;
			}
			return BlockManager.MisReplicationResult.Ok;
		}

		/// <summary>Set replication for the blocks.</summary>
		public virtual void SetReplication(short oldRepl, short newRepl, string src, params 
			Org.Apache.Hadoop.Hdfs.Protocol.Block[] blocks)
		{
			if (newRepl == oldRepl)
			{
				return;
			}
			// update needReplication priority queues
			foreach (Org.Apache.Hadoop.Hdfs.Protocol.Block b in blocks)
			{
				UpdateNeededReplications(b, 0, newRepl - oldRepl);
			}
			if (oldRepl > newRepl)
			{
				// old replication > the new one; need to remove copies
				Log.Info("Decreasing replication from " + oldRepl + " to " + newRepl + " for " + 
					src);
				foreach (Org.Apache.Hadoop.Hdfs.Protocol.Block b_1 in blocks)
				{
					ProcessOverReplicatedBlock(b_1, newRepl, null, null);
				}
			}
			else
			{
				// replication factor is increased
				Log.Info("Increasing replication from " + oldRepl + " to " + newRepl + " for " + 
					src);
			}
		}

		/// <summary>Find how many of the containing nodes are "extra", if any.</summary>
		/// <remarks>
		/// Find how many of the containing nodes are "extra", if any.
		/// If there are any extras, call chooseExcessReplicates() to
		/// mark them in the excessReplicateMap.
		/// </remarks>
		private void ProcessOverReplicatedBlock(Org.Apache.Hadoop.Hdfs.Protocol.Block block
			, short replication, DatanodeDescriptor addedNode, DatanodeDescriptor delNodeHint
			)
		{
			System.Diagnostics.Debug.Assert(namesystem.HasWriteLock());
			if (addedNode == delNodeHint)
			{
				delNodeHint = null;
			}
			ICollection<DatanodeStorageInfo> nonExcess = new AList<DatanodeStorageInfo>();
			ICollection<DatanodeDescriptor> corruptNodes = corruptReplicas.GetNodes(block);
			foreach (DatanodeStorageInfo storage in blocksMap.GetStorages(block, DatanodeStorage.State
				.Normal))
			{
				DatanodeDescriptor cur = storage.GetDatanodeDescriptor();
				if (storage.AreBlockContentsStale())
				{
					Log.Trace("BLOCK* processOverReplicatedBlock: Postponing {}" + " since storage {} does not yet have up-to-date information."
						, block, storage);
					PostponeBlock(block);
					return;
				}
				LightWeightLinkedSet<Org.Apache.Hadoop.Hdfs.Protocol.Block> excessBlocks = excessReplicateMap
					[cur.GetDatanodeUuid()];
				if (excessBlocks == null || !excessBlocks.Contains(block))
				{
					if (!cur.IsDecommissionInProgress() && !cur.IsDecommissioned())
					{
						// exclude corrupt replicas
						if (corruptNodes == null || !corruptNodes.Contains(cur))
						{
							nonExcess.AddItem(storage);
						}
					}
				}
			}
			ChooseExcessReplicates(nonExcess, block, replication, addedNode, delNodeHint, blockplacement
				);
		}

		/// <summary>We want "replication" replicates for the block, but we now have too many.
		/// 	</summary>
		/// <remarks>
		/// We want "replication" replicates for the block, but we now have too many.
		/// In this method, copy enough nodes from 'srcNodes' into 'dstNodes' such that:
		/// srcNodes.size() - dstNodes.size() == replication
		/// We pick node that make sure that replicas are spread across racks and
		/// also try hard to pick one with least free space.
		/// The algorithm is first to pick a node with least free space from nodes
		/// that are on a rack holding more than one replicas of the block.
		/// So removing such a replica won't remove a rack.
		/// If no such a node is available,
		/// then pick a node with least free space
		/// </remarks>
		private void ChooseExcessReplicates(ICollection<DatanodeStorageInfo> nonExcess, Org.Apache.Hadoop.Hdfs.Protocol.Block
			 b, short replication, DatanodeDescriptor addedNode, DatanodeDescriptor delNodeHint
			, BlockPlacementPolicy replicator)
		{
			System.Diagnostics.Debug.Assert(namesystem.HasWriteLock());
			// first form a rack to datanodes map and
			BlockCollection bc = GetBlockCollection(b);
			BlockStoragePolicy storagePolicy = storagePolicySuite.GetPolicy(bc.GetStoragePolicyID
				());
			IList<StorageType> excessTypes = storagePolicy.ChooseExcess(replication, DatanodeStorageInfo
				.ToStorageTypes(nonExcess));
			IDictionary<string, IList<DatanodeStorageInfo>> rackMap = new Dictionary<string, 
				IList<DatanodeStorageInfo>>();
			IList<DatanodeStorageInfo> moreThanOne = new AList<DatanodeStorageInfo>();
			IList<DatanodeStorageInfo> exactlyOne = new AList<DatanodeStorageInfo>();
			// split nodes into two sets
			// moreThanOne contains nodes on rack with more than one replica
			// exactlyOne contains the remaining nodes
			replicator.SplitNodesWithRack(nonExcess, rackMap, moreThanOne, exactlyOne);
			// pick one node to delete that favors the delete hint
			// otherwise pick one with least space from priSet if it is not empty
			// otherwise one node with least space from remains
			bool firstOne = true;
			DatanodeStorageInfo delNodeHintStorage = DatanodeStorageInfo.GetDatanodeStorageInfo
				(nonExcess, delNodeHint);
			DatanodeStorageInfo addedNodeStorage = DatanodeStorageInfo.GetDatanodeStorageInfo
				(nonExcess, addedNode);
			while (nonExcess.Count - replication > 0)
			{
				DatanodeStorageInfo cur;
				if (UseDelHint(firstOne, delNodeHintStorage, addedNodeStorage, moreThanOne, excessTypes
					))
				{
					cur = delNodeHintStorage;
				}
				else
				{
					// regular excessive replica removal
					cur = replicator.ChooseReplicaToDelete(bc, b, replication, moreThanOne, exactlyOne
						, excessTypes);
				}
				firstOne = false;
				// adjust rackmap, moreThanOne, and exactlyOne
				replicator.AdjustSetsWithChosenReplica(rackMap, moreThanOne, exactlyOne, cur);
				nonExcess.Remove(cur);
				AddToExcessReplicate(cur.GetDatanodeDescriptor(), b);
				//
				// The 'excessblocks' tracks blocks until we get confirmation
				// that the datanode has deleted them; the only way we remove them
				// is when we get a "removeBlock" message.  
				//
				// The 'invalidate' list is used to inform the datanode the block 
				// should be deleted.  Items are removed from the invalidate list
				// upon giving instructions to the namenode.
				//
				AddToInvalidates(b, cur.GetDatanodeDescriptor());
				blockLog.Info("BLOCK* chooseExcessReplicates: " + "({}, {}) is added to invalidated blocks set"
					, cur, b);
			}
		}

		/// <summary>Check if we can use delHint</summary>
		internal static bool UseDelHint(bool isFirst, DatanodeStorageInfo delHint, DatanodeStorageInfo
			 added, IList<DatanodeStorageInfo> moreThan1Racks, IList<StorageType> excessTypes
			)
		{
			if (!isFirst)
			{
				return false;
			}
			else
			{
				// only consider delHint for the first case
				if (delHint == null)
				{
					return false;
				}
				else
				{
					// no delHint
					if (!excessTypes.Contains(delHint.GetStorageType()))
					{
						return false;
					}
					else
					{
						// delHint storage type is not an excess type
						// check if removing delHint reduces the number of racks
						if (moreThan1Racks.Contains(delHint))
						{
							return true;
						}
						else
						{
							// delHint and some other nodes are under the same rack 
							if (added != null && !moreThan1Racks.Contains(added))
							{
								return true;
							}
						}
						// the added node adds a new rack
						return false;
					}
				}
			}
		}

		// removing delHint reduces the number of racks;
		private void AddToExcessReplicate(DatanodeInfo dn, Org.Apache.Hadoop.Hdfs.Protocol.Block
			 block)
		{
			System.Diagnostics.Debug.Assert(namesystem.HasWriteLock());
			LightWeightLinkedSet<Org.Apache.Hadoop.Hdfs.Protocol.Block> excessBlocks = excessReplicateMap
				[dn.GetDatanodeUuid()];
			if (excessBlocks == null)
			{
				excessBlocks = new LightWeightLinkedSet<Org.Apache.Hadoop.Hdfs.Protocol.Block>();
				excessReplicateMap[dn.GetDatanodeUuid()] = excessBlocks;
			}
			if (excessBlocks.AddItem(block))
			{
				excessBlocksCount.IncrementAndGet();
				blockLog.Debug("BLOCK* addToExcessReplicate: ({}, {}) is added to" + " excessReplicateMap"
					, dn, block);
			}
		}

		private void RemoveStoredBlock(DatanodeStorageInfo storageInfo, Org.Apache.Hadoop.Hdfs.Protocol.Block
			 block, DatanodeDescriptor node)
		{
			if (shouldPostponeBlocksFromFuture && namesystem.IsGenStampInFuture(block))
			{
				QueueReportedBlock(storageInfo, block, null, QueueReasonFutureGenstamp);
				return;
			}
			RemoveStoredBlock(block, node);
		}

		/// <summary>Modify (block--&gt;datanode) map.</summary>
		/// <remarks>
		/// Modify (block--&gt;datanode) map. Possibly generate replication tasks, if the
		/// removed block is still valid.
		/// </remarks>
		public virtual void RemoveStoredBlock(Org.Apache.Hadoop.Hdfs.Protocol.Block block
			, DatanodeDescriptor node)
		{
			blockLog.Debug("BLOCK* removeStoredBlock: {} from {}", block, node);
			System.Diagnostics.Debug.Assert((namesystem.HasWriteLock()));
			{
				if (!blocksMap.RemoveNode(block, node))
				{
					blockLog.Debug("BLOCK* removeStoredBlock: {} has already been" + " removed from node {}"
						, block, node);
					return;
				}
				//
				// It's possible that the block was removed because of a datanode
				// failure. If the block is still valid, check if replication is
				// necessary. In that case, put block on a possibly-will-
				// be-replicated list.
				//
				BlockCollection bc = blocksMap.GetBlockCollection(block);
				if (bc != null)
				{
					namesystem.DecrementSafeBlockCount(block);
					UpdateNeededReplications(block, -1, 0);
				}
				//
				// We've removed a block from a node, so it's definitely no longer
				// in "excess" there.
				//
				LightWeightLinkedSet<Org.Apache.Hadoop.Hdfs.Protocol.Block> excessBlocks = excessReplicateMap
					[node.GetDatanodeUuid()];
				if (excessBlocks != null)
				{
					if (excessBlocks.Remove(block))
					{
						excessBlocksCount.DecrementAndGet();
						blockLog.Debug("BLOCK* removeStoredBlock: {} is removed from " + "excessBlocks", 
							block);
						if (excessBlocks.Count == 0)
						{
							Sharpen.Collections.Remove(excessReplicateMap, node.GetDatanodeUuid());
						}
					}
				}
				// Remove the replica from corruptReplicas
				corruptReplicas.RemoveFromCorruptReplicasMap(block, node);
			}
		}

		/// <summary>
		/// Get all valid locations of the block & add the block to results
		/// return the length of the added block; 0 if the block is not added
		/// </summary>
		private long AddBlock(Org.Apache.Hadoop.Hdfs.Protocol.Block block, IList<BlocksWithLocations.BlockWithLocations
			> results)
		{
			IList<DatanodeStorageInfo> locations = GetValidLocations(block);
			if (locations.Count == 0)
			{
				return 0;
			}
			else
			{
				string[] datanodeUuids = new string[locations.Count];
				string[] storageIDs = new string[datanodeUuids.Length];
				StorageType[] storageTypes = new StorageType[datanodeUuids.Length];
				for (int i = 0; i < locations.Count; i++)
				{
					DatanodeStorageInfo s = locations[i];
					datanodeUuids[i] = s.GetDatanodeDescriptor().GetDatanodeUuid();
					storageIDs[i] = s.GetStorageID();
					storageTypes[i] = s.GetStorageType();
				}
				results.AddItem(new BlocksWithLocations.BlockWithLocations(block, datanodeUuids, 
					storageIDs, storageTypes));
				return block.GetNumBytes();
			}
		}

		/// <summary>The given node is reporting that it received a certain block.</summary>
		/// <exception cref="System.IO.IOException"/>
		[VisibleForTesting]
		internal virtual void AddBlock(DatanodeStorageInfo storageInfo, Org.Apache.Hadoop.Hdfs.Protocol.Block
			 block, string delHint)
		{
			DatanodeDescriptor node = storageInfo.GetDatanodeDescriptor();
			// Decrement number of blocks scheduled to this datanode.
			// for a retry request (of DatanodeProtocol#blockReceivedAndDeleted with 
			// RECEIVED_BLOCK), we currently also decrease the approximate number. 
			node.DecrementBlocksScheduled(storageInfo.GetStorageType());
			// get the deletion hint node
			DatanodeDescriptor delHintNode = null;
			if (delHint != null && delHint.Length != 0)
			{
				delHintNode = datanodeManager.GetDatanode(delHint);
				if (delHintNode == null)
				{
					blockLog.Warn("BLOCK* blockReceived: {} is expected to be removed " + "from an unrecorded node {}"
						, block, delHint);
				}
			}
			//
			// Modify the blocks->datanode map and node's map.
			//
			pendingReplications.Decrement(block, node);
			ProcessAndHandleReportedBlock(storageInfo, block, HdfsServerConstants.ReplicaState
				.Finalized, delHintNode);
		}

		/// <exception cref="System.IO.IOException"/>
		private void ProcessAndHandleReportedBlock(DatanodeStorageInfo storageInfo, Org.Apache.Hadoop.Hdfs.Protocol.Block
			 block, HdfsServerConstants.ReplicaState reportedState, DatanodeDescriptor delHintNode
			)
		{
			// blockReceived reports a finalized block
			ICollection<BlockInfoContiguous> toAdd = new List<BlockInfoContiguous>();
			ICollection<Org.Apache.Hadoop.Hdfs.Protocol.Block> toInvalidate = new List<Org.Apache.Hadoop.Hdfs.Protocol.Block
				>();
			ICollection<BlockManager.BlockToMarkCorrupt> toCorrupt = new List<BlockManager.BlockToMarkCorrupt
				>();
			ICollection<BlockManager.StatefulBlockInfo> toUC = new List<BlockManager.StatefulBlockInfo
				>();
			DatanodeDescriptor node = storageInfo.GetDatanodeDescriptor();
			ProcessReportedBlock(storageInfo, block, reportedState, toAdd, toInvalidate, toCorrupt
				, toUC);
			// the block is only in one of the to-do lists
			// if it is in none then data-node already has it
			System.Diagnostics.Debug.Assert(toUC.Count + toAdd.Count + toInvalidate.Count + toCorrupt
				.Count <= 1, "The block should be only in one of the lists.");
			foreach (BlockManager.StatefulBlockInfo b in toUC)
			{
				AddStoredBlockUnderConstruction(b, storageInfo);
			}
			long numBlocksLogged = 0;
			foreach (BlockInfoContiguous b_1 in toAdd)
			{
				AddStoredBlock(b_1, storageInfo, delHintNode, numBlocksLogged < maxNumBlocksToLog
					);
				numBlocksLogged++;
			}
			if (numBlocksLogged > maxNumBlocksToLog)
			{
				blockLog.Info("BLOCK* addBlock: logged info for {} of {} reported.", maxNumBlocksToLog
					, numBlocksLogged);
			}
			foreach (Org.Apache.Hadoop.Hdfs.Protocol.Block b_2 in toInvalidate)
			{
				blockLog.Info("BLOCK* addBlock: block {} on node {} size {} does not " + "belong to any file"
					, b_2, node, b_2.GetNumBytes());
				AddToInvalidates(b_2, node);
			}
			foreach (BlockManager.BlockToMarkCorrupt b_3 in toCorrupt)
			{
				MarkBlockAsCorrupt(b_3, storageInfo, node);
			}
		}

		/// <summary>The given node is reporting incremental information about some blocks.</summary>
		/// <remarks>
		/// The given node is reporting incremental information about some blocks.
		/// This includes blocks that are starting to be received, completed being
		/// received, or deleted.
		/// This method must be called with FSNamesystem lock held.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public virtual void ProcessIncrementalBlockReport(DatanodeID nodeID, StorageReceivedDeletedBlocks
			 srdb)
		{
			System.Diagnostics.Debug.Assert(namesystem.HasWriteLock());
			int received = 0;
			int deleted = 0;
			int receiving = 0;
			DatanodeDescriptor node = datanodeManager.GetDatanode(nodeID);
			if (node == null || !node.isAlive)
			{
				blockLog.Warn("BLOCK* processIncrementalBlockReport" + " is received from dead or unregistered node {}"
					, nodeID);
				throw new IOException("Got incremental block report from unregistered or dead node"
					);
			}
			DatanodeStorageInfo storageInfo = node.GetStorageInfo(srdb.GetStorage().GetStorageID
				());
			if (storageInfo == null)
			{
				// The DataNode is reporting an unknown storage. Usually the NN learns
				// about new storages from heartbeats but during NN restart we may
				// receive a block report or incremental report before the heartbeat.
				// We must handle this for protocol compatibility. This issue was
				// uncovered by HDFS-6094.
				storageInfo = node.UpdateStorage(srdb.GetStorage());
			}
			foreach (ReceivedDeletedBlockInfo rdbi in srdb.GetBlocks())
			{
				switch (rdbi.GetStatus())
				{
					case ReceivedDeletedBlockInfo.BlockStatus.DeletedBlock:
					{
						RemoveStoredBlock(storageInfo, rdbi.GetBlock(), node);
						deleted++;
						break;
					}

					case ReceivedDeletedBlockInfo.BlockStatus.ReceivedBlock:
					{
						AddBlock(storageInfo, rdbi.GetBlock(), rdbi.GetDelHints());
						received++;
						break;
					}

					case ReceivedDeletedBlockInfo.BlockStatus.ReceivingBlock:
					{
						receiving++;
						ProcessAndHandleReportedBlock(storageInfo, rdbi.GetBlock(), HdfsServerConstants.ReplicaState
							.Rbw, null);
						break;
					}

					default:
					{
						string msg = "Unknown block status code reported by " + nodeID + ": " + rdbi;
						blockLog.Warn(msg);
						System.Diagnostics.Debug.Assert(false, msg);
						// if assertions are enabled, throw.
						break;
					}
				}
				blockLog.Debug("BLOCK* block {}: {} is received from {}", rdbi.GetStatus(), rdbi.
					GetBlock(), nodeID);
			}
			blockLog.Debug("*BLOCK* NameNode.processIncrementalBlockReport: from " + "{} receiving: {}, received: {}, deleted: {}"
				, nodeID, receiving, received, deleted);
		}

		/// <summary>
		/// Return the number of nodes hosting a given block, grouped
		/// by the state of those replicas.
		/// </summary>
		public virtual NumberReplicas CountNodes(Org.Apache.Hadoop.Hdfs.Protocol.Block b)
		{
			int decommissioned = 0;
			int live = 0;
			int corrupt = 0;
			int excess = 0;
			int stale = 0;
			ICollection<DatanodeDescriptor> nodesCorrupt = corruptReplicas.GetNodes(b);
			foreach (DatanodeStorageInfo storage in blocksMap.GetStorages(b, DatanodeStorage.State
				.Normal))
			{
				DatanodeDescriptor node = storage.GetDatanodeDescriptor();
				if ((nodesCorrupt != null) && (nodesCorrupt.Contains(node)))
				{
					corrupt++;
				}
				else
				{
					if (node.IsDecommissionInProgress() || node.IsDecommissioned())
					{
						decommissioned++;
					}
					else
					{
						LightWeightLinkedSet<Org.Apache.Hadoop.Hdfs.Protocol.Block> blocksExcess = excessReplicateMap
							[node.GetDatanodeUuid()];
						if (blocksExcess != null && blocksExcess.Contains(b))
						{
							excess++;
						}
						else
						{
							live++;
						}
					}
				}
				if (storage.AreBlockContentsStale())
				{
					stale++;
				}
			}
			return new NumberReplicas(live, decommissioned, corrupt, excess, stale);
		}

		/// <summary>
		/// Simpler, faster form of
		/// <see cref="CountNodes(Org.Apache.Hadoop.Hdfs.Protocol.Block)"/>
		/// that only returns the number
		/// of live nodes.  If in startup safemode (or its 30-sec extension period),
		/// then it gains speed by ignoring issues of excess replicas or nodes
		/// that are decommissioned or in process of becoming decommissioned.
		/// If not in startup, then it calls
		/// <see cref="CountNodes(Org.Apache.Hadoop.Hdfs.Protocol.Block)"/>
		/// instead.
		/// </summary>
		/// <param name="b">- the block being tested</param>
		/// <returns>count of live nodes for this block</returns>
		internal virtual int CountLiveNodes(BlockInfoContiguous b)
		{
			if (!namesystem.IsInStartupSafeMode())
			{
				return CountNodes(b).LiveReplicas();
			}
			// else proceed with fast case
			int live = 0;
			ICollection<DatanodeDescriptor> nodesCorrupt = corruptReplicas.GetNodes(b);
			foreach (DatanodeStorageInfo storage in blocksMap.GetStorages(b, DatanodeStorage.State
				.Normal))
			{
				DatanodeDescriptor node = storage.GetDatanodeDescriptor();
				if ((nodesCorrupt == null) || (!nodesCorrupt.Contains(node)))
				{
					live++;
				}
			}
			return live;
		}

		/// <summary>On stopping decommission, check if the node has excess replicas.</summary>
		/// <remarks>
		/// On stopping decommission, check if the node has excess replicas.
		/// If there are any excess replicas, call processOverReplicatedBlock().
		/// Process over replicated blocks only when active NN is out of safe mode.
		/// </remarks>
		internal virtual void ProcessOverReplicatedBlocksOnReCommission(DatanodeDescriptor
			 srcNode)
		{
			if (!namesystem.IsPopulatingReplQueues())
			{
				return;
			}
			IEnumerator<Org.Apache.Hadoop.Hdfs.Protocol.Block> it = srcNode.GetBlockIterator(
				);
			int numOverReplicated = 0;
			while (it.HasNext())
			{
				Org.Apache.Hadoop.Hdfs.Protocol.Block block = it.Next();
				BlockCollection bc = blocksMap.GetBlockCollection(block);
				short expectedReplication = bc.GetBlockReplication();
				NumberReplicas num = CountNodes(block);
				int numCurrentReplica = num.LiveReplicas();
				if (numCurrentReplica > expectedReplication)
				{
					// over-replicated block 
					ProcessOverReplicatedBlock(block, expectedReplication, null, null);
					numOverReplicated++;
				}
			}
			Log.Info("Invalidated " + numOverReplicated + " over-replicated blocks on " + srcNode
				 + " during recommissioning");
		}

		/// <summary>
		/// Returns whether a node can be safely decommissioned based on its
		/// liveness.
		/// </summary>
		/// <remarks>
		/// Returns whether a node can be safely decommissioned based on its
		/// liveness. Dead nodes cannot always be safely decommissioned.
		/// </remarks>
		internal virtual bool IsNodeHealthyForDecommission(DatanodeDescriptor node)
		{
			if (!node.CheckBlockReportReceived())
			{
				Log.Info("Node {} hasn't sent its first block report.", node);
				return false;
			}
			if (node.isAlive)
			{
				return true;
			}
			UpdateState();
			if (pendingReplicationBlocksCount == 0 && underReplicatedBlocksCount == 0)
			{
				Log.Info("Node {} is dead and there are no under-replicated" + " blocks or blocks pending replication. Safe to decommission."
					, node);
				return true;
			}
			Log.Warn("Node {} is dead " + "while decommission is in progress. Cannot be safely "
				 + "decommissioned since there is risk of reduced " + "data durability or data loss. Either restart the failed node or"
				 + " force decommissioning by removing, calling refreshNodes, " + "then re-adding to the excludes files."
				, node);
			return false;
		}

		public virtual int GetActiveBlockCount()
		{
			return blocksMap.Size();
		}

		public virtual DatanodeStorageInfo[] GetStorages(BlockInfoContiguous block)
		{
			DatanodeStorageInfo[] storages = new DatanodeStorageInfo[block.NumNodes()];
			int i = 0;
			foreach (DatanodeStorageInfo s in blocksMap.GetStorages(block))
			{
				storages[i++] = s;
			}
			return storages;
		}

		public virtual int GetTotalBlocks()
		{
			return blocksMap.Size();
		}

		public virtual void RemoveBlock(Org.Apache.Hadoop.Hdfs.Protocol.Block block)
		{
			System.Diagnostics.Debug.Assert(namesystem.HasWriteLock());
			// No need to ACK blocks that are being removed entirely
			// from the namespace, since the removal of the associated
			// file already removes them from the block map below.
			block.SetNumBytes(BlockCommand.NoAck);
			AddToInvalidates(block);
			RemoveBlockFromMap(block);
			// Remove the block from pendingReplications and neededReplications
			pendingReplications.Remove(block);
			neededReplications.Remove(block, UnderReplicatedBlocks.Level);
			if (postponedMisreplicatedBlocks.Remove(block))
			{
				postponedMisreplicatedBlocksCount.DecrementAndGet();
			}
		}

		public virtual BlockInfoContiguous GetStoredBlock(Org.Apache.Hadoop.Hdfs.Protocol.Block
			 block)
		{
			return blocksMap.GetStoredBlock(block);
		}

		/// <summary>updates a block in under replication queue</summary>
		private void UpdateNeededReplications(Org.Apache.Hadoop.Hdfs.Protocol.Block block
			, int curReplicasDelta, int expectedReplicasDelta)
		{
			namesystem.WriteLock();
			try
			{
				if (!namesystem.IsPopulatingReplQueues())
				{
					return;
				}
				NumberReplicas repl = CountNodes(block);
				int curExpectedReplicas = GetReplication(block);
				if (IsNeededReplication(block, curExpectedReplicas, repl.LiveReplicas()))
				{
					neededReplications.Update(block, repl.LiveReplicas(), repl.DecommissionedReplicas
						(), curExpectedReplicas, curReplicasDelta, expectedReplicasDelta);
				}
				else
				{
					int oldReplicas = repl.LiveReplicas() - curReplicasDelta;
					int oldExpectedReplicas = curExpectedReplicas - expectedReplicasDelta;
					neededReplications.Remove(block, oldReplicas, repl.DecommissionedReplicas(), oldExpectedReplicas
						);
				}
			}
			finally
			{
				namesystem.WriteUnlock();
			}
		}

		/// <summary>Check replication of the blocks in the collection.</summary>
		/// <remarks>
		/// Check replication of the blocks in the collection.
		/// If any block is needed replication, insert it into the replication queue.
		/// Otherwise, if the block is more than the expected replication factor,
		/// process it as an over replicated block.
		/// </remarks>
		public virtual void CheckReplication(BlockCollection bc)
		{
			short expected = bc.GetBlockReplication();
			foreach (Org.Apache.Hadoop.Hdfs.Protocol.Block block in bc.GetBlocks())
			{
				NumberReplicas n = CountNodes(block);
				if (IsNeededReplication(block, expected, n.LiveReplicas()))
				{
					neededReplications.Add(block, n.LiveReplicas(), n.DecommissionedReplicas(), expected
						);
				}
				else
				{
					if (n.LiveReplicas() > expected)
					{
						ProcessOverReplicatedBlock(block, expected, null, null);
					}
				}
			}
		}

		/// <returns>
		/// 0 if the block is not found;
		/// otherwise, return the replication factor of the block.
		/// </returns>
		private int GetReplication(Org.Apache.Hadoop.Hdfs.Protocol.Block block)
		{
			BlockCollection bc = blocksMap.GetBlockCollection(block);
			return bc == null ? 0 : bc.GetBlockReplication();
		}

		/// <summary>
		/// Get blocks to invalidate for <i>nodeId</i>
		/// in
		/// <see cref="invalidateBlocks"/>
		/// .
		/// </summary>
		/// <returns>number of blocks scheduled for removal during this iteration.</returns>
		private int InvalidateWorkForOneNode(DatanodeInfo dn)
		{
			IList<Org.Apache.Hadoop.Hdfs.Protocol.Block> toInvalidate;
			namesystem.WriteLock();
			try
			{
				// blocks should not be replicated or removed if safe mode is on
				if (namesystem.IsInSafeMode())
				{
					Log.Debug("In safemode, not computing replication work");
					return 0;
				}
				try
				{
					DatanodeDescriptor dnDescriptor = datanodeManager.GetDatanode(dn);
					if (dnDescriptor == null)
					{
						Log.Warn("DataNode " + dn + " cannot be found with UUID " + dn.GetDatanodeUuid() 
							+ ", removing block invalidation work.");
						invalidateBlocks.Remove(dn);
						return 0;
					}
					toInvalidate = invalidateBlocks.InvalidateWork(dnDescriptor);
					if (toInvalidate == null)
					{
						return 0;
					}
				}
				catch (UnregisteredNodeException)
				{
					return 0;
				}
			}
			finally
			{
				namesystem.WriteUnlock();
			}
			blockLog.Info("BLOCK* {}: ask {} to delete {}", GetType().Name, dn, toInvalidate);
			return toInvalidate.Count;
		}

		internal virtual bool BlockHasEnoughRacks(Org.Apache.Hadoop.Hdfs.Protocol.Block b
			)
		{
			bool enoughRacks = false;
			ICollection<DatanodeDescriptor> corruptNodes = corruptReplicas.GetNodes(b);
			int numExpectedReplicas = GetReplication(b);
			string rackName = null;
			foreach (DatanodeStorageInfo storage in blocksMap.GetStorages(b))
			{
				DatanodeDescriptor cur = storage.GetDatanodeDescriptor();
				if (!cur.IsDecommissionInProgress() && !cur.IsDecommissioned())
				{
					if ((corruptNodes == null) || !corruptNodes.Contains(cur))
					{
						if (numExpectedReplicas == 1 || (numExpectedReplicas > 1 && !datanodeManager.HasClusterEverBeenMultiRack
							()))
						{
							enoughRacks = true;
							break;
						}
						string rackNameNew = cur.GetNetworkLocation();
						if (rackName == null)
						{
							rackName = rackNameNew;
						}
						else
						{
							if (!rackName.Equals(rackNameNew))
							{
								enoughRacks = true;
								break;
							}
						}
					}
				}
			}
			return enoughRacks;
		}

		/// <summary>
		/// A block needs replication if the number of replicas is less than expected
		/// or if it does not have enough racks.
		/// </summary>
		internal virtual bool IsNeededReplication(Org.Apache.Hadoop.Hdfs.Protocol.Block b
			, int expected, int current)
		{
			return current < expected || !BlockHasEnoughRacks(b);
		}

		public virtual long GetMissingBlocksCount()
		{
			// not locking
			return this.neededReplications.GetCorruptBlockSize();
		}

		public virtual long GetMissingReplOneBlocksCount()
		{
			// not locking
			return this.neededReplications.GetCorruptReplOneBlockSize();
		}

		public virtual BlockInfoContiguous AddBlockCollection(BlockInfoContiguous block, 
			BlockCollection bc)
		{
			return blocksMap.AddBlockCollection(block, bc);
		}

		public virtual BlockCollection GetBlockCollection(Org.Apache.Hadoop.Hdfs.Protocol.Block
			 b)
		{
			return blocksMap.GetBlockCollection(b);
		}

		/// <returns>an iterator of the datanodes.</returns>
		public virtual IEnumerable<DatanodeStorageInfo> GetStorages(Org.Apache.Hadoop.Hdfs.Protocol.Block
			 block)
		{
			return blocksMap.GetStorages(block);
		}

		public virtual int NumCorruptReplicas(Org.Apache.Hadoop.Hdfs.Protocol.Block block
			)
		{
			return corruptReplicas.NumCorruptReplicas(block);
		}

		public virtual void RemoveBlockFromMap(Org.Apache.Hadoop.Hdfs.Protocol.Block block
			)
		{
			RemoveFromExcessReplicateMap(block);
			blocksMap.RemoveBlock(block);
			// If block is removed from blocksMap remove it from corruptReplicasMap
			corruptReplicas.RemoveFromCorruptReplicasMap(block);
		}

		/// <summary>If a block is removed from blocksMap, remove it from excessReplicateMap.
		/// 	</summary>
		private void RemoveFromExcessReplicateMap(Org.Apache.Hadoop.Hdfs.Protocol.Block block
			)
		{
			foreach (DatanodeStorageInfo info in blocksMap.GetStorages(block))
			{
				string uuid = info.GetDatanodeDescriptor().GetDatanodeUuid();
				LightWeightLinkedSet<Org.Apache.Hadoop.Hdfs.Protocol.Block> excessReplicas = excessReplicateMap
					[uuid];
				if (excessReplicas != null)
				{
					if (excessReplicas.Remove(block))
					{
						excessBlocksCount.DecrementAndGet();
						if (excessReplicas.IsEmpty())
						{
							Sharpen.Collections.Remove(excessReplicateMap, uuid);
						}
					}
				}
			}
		}

		public virtual int GetCapacity()
		{
			return blocksMap.GetCapacity();
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
		public virtual long[] GetCorruptReplicaBlockIds(int numExpectedBlocks, long startingBlockId
			)
		{
			return corruptReplicas.GetCorruptReplicaBlockIds(numExpectedBlocks, startingBlockId
				);
		}

		/// <summary>Return an iterator over the set of blocks for which there are no replicas.
		/// 	</summary>
		public virtual IEnumerator<Org.Apache.Hadoop.Hdfs.Protocol.Block> GetCorruptReplicaBlockIterator
			()
		{
			return neededReplications.Iterator(UnderReplicatedBlocks.QueueWithCorruptBlocks);
		}

		/// <summary>Get the replicas which are corrupt for a given block.</summary>
		public virtual ICollection<DatanodeDescriptor> GetCorruptReplicas(Org.Apache.Hadoop.Hdfs.Protocol.Block
			 block)
		{
			return corruptReplicas.GetNodes(block);
		}

		/// <summary>Get reason for certain corrupted replicas for a given block and a given dn.
		/// 	</summary>
		public virtual string GetCorruptReason(Org.Apache.Hadoop.Hdfs.Protocol.Block block
			, DatanodeDescriptor node)
		{
			return corruptReplicas.GetCorruptReason(block, node);
		}

		/// <returns>the size of UnderReplicatedBlocks</returns>
		public virtual int NumOfUnderReplicatedBlocks()
		{
			return neededReplications.Size();
		}

		/// <summary>Periodically calls computeReplicationWork().</summary>
		private class ReplicationMonitor : Runnable
		{
			public virtual void Run()
			{
				while (this._enclosing.namesystem.IsRunning())
				{
					try
					{
						// Process replication work only when active NN is out of safe mode.
						if (this._enclosing.namesystem.IsPopulatingReplQueues())
						{
							this._enclosing.ComputeDatanodeWork();
							this._enclosing.ProcessPendingReplications();
							this._enclosing.RescanPostponedMisreplicatedBlocks();
						}
						Sharpen.Thread.Sleep(this._enclosing.replicationRecheckInterval);
					}
					catch (Exception t)
					{
						if (!this._enclosing.namesystem.IsRunning())
						{
							BlockManager.Log.Info("Stopping ReplicationMonitor.");
							if (!(t is Exception))
							{
								BlockManager.Log.Info("ReplicationMonitor received an exception" + " while shutting down."
									, t);
							}
							break;
						}
						else
						{
							if (!this._enclosing.checkNSRunning && t is Exception)
							{
								BlockManager.Log.Info("Stopping ReplicationMonitor for testing.");
								break;
							}
						}
						BlockManager.Log.Error("ReplicationMonitor thread received Runtime exception. ", 
							t);
						ExitUtil.Terminate(1, t);
					}
				}
			}

			internal ReplicationMonitor(BlockManager _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly BlockManager _enclosing;
		}

		/// <summary>
		/// Compute block replication and block invalidation work that can be scheduled
		/// on data-nodes.
		/// </summary>
		/// <remarks>
		/// Compute block replication and block invalidation work that can be scheduled
		/// on data-nodes. The datanode will be informed of this work at the next
		/// heartbeat.
		/// </remarks>
		/// <returns>number of blocks scheduled for replication or removal.</returns>
		internal virtual int ComputeDatanodeWork()
		{
			// Blocks should not be replicated or removed if in safe mode.
			// It's OK to check safe mode here w/o holding lock, in the worst
			// case extra replications will be scheduled, and these will get
			// fixed up later.
			if (namesystem.IsInSafeMode())
			{
				return 0;
			}
			int numlive = heartbeatManager.GetLiveDatanodeCount();
			int blocksToProcess = numlive * this.blocksReplWorkMultiplier;
			int nodesToProcess = (int)Math.Ceil(numlive * this.blocksInvalidateWorkPct);
			int workFound = this.ComputeReplicationWork(blocksToProcess);
			// Update counters
			namesystem.WriteLock();
			try
			{
				this.UpdateState();
				this.scheduledReplicationBlocksCount = workFound;
			}
			finally
			{
				namesystem.WriteUnlock();
			}
			workFound += this.ComputeInvalidateWork(nodesToProcess);
			return workFound;
		}

		/// <summary>
		/// Clear all queues that hold decisions previously made by
		/// this NameNode.
		/// </summary>
		public virtual void ClearQueues()
		{
			neededReplications.Clear();
			pendingReplications.Clear();
			excessReplicateMap.Clear();
			invalidateBlocks.Clear();
			datanodeManager.ClearPendingQueues();
			postponedMisreplicatedBlocks.Clear();
			postponedMisreplicatedBlocksCount.Set(0);
		}

		private class ReplicationWork
		{
			private readonly Org.Apache.Hadoop.Hdfs.Protocol.Block block;

			private readonly BlockCollection bc;

			private readonly DatanodeDescriptor srcNode;

			private readonly IList<DatanodeDescriptor> containingNodes;

			private readonly IList<DatanodeStorageInfo> liveReplicaStorages;

			private readonly int additionalReplRequired;

			private DatanodeStorageInfo[] targets;

			private readonly int priority;

			public ReplicationWork(Org.Apache.Hadoop.Hdfs.Protocol.Block block, BlockCollection
				 bc, DatanodeDescriptor srcNode, IList<DatanodeDescriptor> containingNodes, IList
				<DatanodeStorageInfo> liveReplicaStorages, int additionalReplRequired, int priority
				)
			{
				this.block = block;
				this.bc = bc;
				this.srcNode = srcNode;
				this.srcNode.IncrementPendingReplicationWithoutTargets();
				this.containingNodes = containingNodes;
				this.liveReplicaStorages = liveReplicaStorages;
				this.additionalReplRequired = additionalReplRequired;
				this.priority = priority;
				this.targets = null;
			}

			private void ChooseTargets(BlockPlacementPolicy blockplacement, BlockStoragePolicySuite
				 storagePolicySuite, ICollection<Node> excludedNodes)
			{
				try
				{
					targets = blockplacement.ChooseTarget(bc.GetName(), additionalReplRequired, srcNode
						, liveReplicaStorages, false, excludedNodes, block.GetNumBytes(), storagePolicySuite
						.GetPolicy(bc.GetStoragePolicyID()));
				}
				finally
				{
					srcNode.DecrementPendingReplicationWithoutTargets();
				}
			}
		}

		/// <summary>
		/// A simple result enum for the result of
		/// <see cref="ProcessMisReplicatedBlock(BlockInfoContiguous)"/>
		/// .
		/// </summary>
		internal enum MisReplicationResult
		{
			Invalid,
			UnderReplicated,
			OverReplicated,
			Postpone,
			UnderConstruction,
			Ok
		}

		public virtual void Shutdown()
		{
			StopReplicationInitializer();
			blocksMap.Close();
		}

		public virtual void Clear()
		{
			ClearQueues();
			blocksMap.Clear();
		}
	}
}
