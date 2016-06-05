using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Collect;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Security.Token.Block;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Htrace;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	internal class BlockStorageLocationUtil
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(BlockStorageLocationUtil
			));

		/// <summary>
		/// Create a list of
		/// <see cref="VolumeBlockLocationCallable"/>
		/// corresponding to a set
		/// of datanodes and blocks. The blocks must all correspond to the same
		/// block pool.
		/// </summary>
		/// <param name="datanodeBlocks">Map of datanodes to block replicas at each datanode</param>
		/// <returns>
		/// callables Used to query each datanode for location information on
		/// the block replicas at the datanode
		/// </returns>
		private static IList<BlockStorageLocationUtil.VolumeBlockLocationCallable> CreateVolumeBlockLocationCallables
			(Configuration conf, IDictionary<DatanodeInfo, IList<LocatedBlock>> datanodeBlocks
			, int timeout, bool connectToDnViaHostname, Span parent)
		{
			if (datanodeBlocks.IsEmpty())
			{
				return Lists.NewArrayList();
			}
			// Construct the callables, one per datanode
			IList<BlockStorageLocationUtil.VolumeBlockLocationCallable> callables = new AList
				<BlockStorageLocationUtil.VolumeBlockLocationCallable>();
			foreach (KeyValuePair<DatanodeInfo, IList<LocatedBlock>> entry in datanodeBlocks)
			{
				// Construct RPC parameters
				DatanodeInfo datanode = entry.Key;
				IList<LocatedBlock> locatedBlocks = entry.Value;
				if (locatedBlocks.IsEmpty())
				{
					continue;
				}
				// Ensure that the blocks all are from the same block pool.
				string poolId = locatedBlocks[0].GetBlock().GetBlockPoolId();
				foreach (LocatedBlock lb in locatedBlocks)
				{
					if (!poolId.Equals(lb.GetBlock().GetBlockPoolId()))
					{
						throw new ArgumentException("All blocks to be queried must be in the same block pool: "
							 + locatedBlocks[0].GetBlock() + " and " + lb + " are from different pools.");
					}
				}
				long[] blockIds = new long[locatedBlocks.Count];
				int i = 0;
				IList<Org.Apache.Hadoop.Security.Token.Token<BlockTokenIdentifier>> dnTokens = new 
					AList<Org.Apache.Hadoop.Security.Token.Token<BlockTokenIdentifier>>(locatedBlocks
					.Count);
				foreach (LocatedBlock b in locatedBlocks)
				{
					blockIds[i++] = b.GetBlock().GetBlockId();
					dnTokens.AddItem(b.GetBlockToken());
				}
				BlockStorageLocationUtil.VolumeBlockLocationCallable callable = new BlockStorageLocationUtil.VolumeBlockLocationCallable
					(conf, datanode, poolId, blockIds, dnTokens, timeout, connectToDnViaHostname, parent
					);
				callables.AddItem(callable);
			}
			return callables;
		}

		/// <summary>
		/// Queries datanodes for the blocks specified in <code>datanodeBlocks</code>,
		/// making one RPC to each datanode.
		/// </summary>
		/// <remarks>
		/// Queries datanodes for the blocks specified in <code>datanodeBlocks</code>,
		/// making one RPC to each datanode. These RPCs are made in parallel using a
		/// threadpool.
		/// </remarks>
		/// <param name="datanodeBlocks">Map of datanodes to the blocks present on the DN</param>
		/// <returns>metadatas Map of datanodes to block metadata of the DN</returns>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Security.Token.Block.InvalidBlockTokenException
		/// 	">if client does not have read access on a requested block</exception>
		internal static IDictionary<DatanodeInfo, HdfsBlocksMetadata> QueryDatanodesForHdfsBlocksMetadata
			(Configuration conf, IDictionary<DatanodeInfo, IList<LocatedBlock>> datanodeBlocks
			, int poolsize, int timeoutMs, bool connectToDnViaHostname)
		{
			IList<BlockStorageLocationUtil.VolumeBlockLocationCallable> callables = CreateVolumeBlockLocationCallables
				(conf, datanodeBlocks, timeoutMs, connectToDnViaHostname, Trace.CurrentSpan());
			// Use a thread pool to execute the Callables in parallel
			IList<Future<HdfsBlocksMetadata>> futures = new AList<Future<HdfsBlocksMetadata>>
				();
			ExecutorService executor = new ScheduledThreadPoolExecutor(poolsize);
			try
			{
				futures = executor.InvokeAll(callables, timeoutMs, TimeUnit.Milliseconds);
			}
			catch (Exception)
			{
			}
			// Swallow the exception here, because we can return partial results
			executor.Shutdown();
			IDictionary<DatanodeInfo, HdfsBlocksMetadata> metadatas = Maps.NewHashMapWithExpectedSize
				(datanodeBlocks.Count);
			// Fill in metadatas with results from DN RPCs, where possible
			for (int i = 0; i < futures.Count; i++)
			{
				BlockStorageLocationUtil.VolumeBlockLocationCallable callable = callables[i];
				DatanodeInfo datanode = callable.GetDatanodeInfo();
				Future<HdfsBlocksMetadata> future = futures[i];
				try
				{
					HdfsBlocksMetadata metadata = future.Get();
					metadatas[callable.GetDatanodeInfo()] = metadata;
				}
				catch (CancellationException e)
				{
					Log.Info("Cancelled while waiting for datanode " + datanode.GetIpcAddr(false) + ": "
						 + e.ToString());
				}
				catch (ExecutionException e)
				{
					Exception t = e.InnerException;
					if (t is InvalidBlockTokenException)
					{
						Log.Warn("Invalid access token when trying to retrieve " + "information from datanode "
							 + datanode.GetIpcAddr(false));
						throw (InvalidBlockTokenException)t;
					}
					else
					{
						if (t is NotSupportedException)
						{
							Log.Info("Datanode " + datanode.GetIpcAddr(false) + " does not support" + " required #getHdfsBlocksMetadata() API"
								);
							throw (NotSupportedException)t;
						}
						else
						{
							Log.Info("Failed to query block locations on datanode " + datanode.GetIpcAddr(false
								) + ": " + t);
						}
					}
					if (Log.IsDebugEnabled())
					{
						Log.Debug("Could not fetch information from datanode", t);
					}
				}
				catch (Exception)
				{
					// Shouldn't happen, because invokeAll waits for all Futures to be ready
					Log.Info("Interrupted while fetching HdfsBlocksMetadata");
				}
			}
			return metadatas;
		}

		/// <summary>
		/// Group the per-replica
		/// <see cref="Org.Apache.Hadoop.FS.VolumeId"/>
		/// info returned from
		/// <see cref="DFSClient#queryDatanodesForHdfsBlocksMetadata(Map)"/>
		/// to be
		/// associated
		/// with the corresponding
		/// <see cref="Org.Apache.Hadoop.Hdfs.Protocol.LocatedBlock"/>
		/// .
		/// </summary>
		/// <param name="blocks">Original LocatedBlock array</param>
		/// <param name="metadatas">VolumeId information for the replicas on each datanode</param>
		/// <returns>
		/// blockVolumeIds per-replica VolumeId information associated with the
		/// parent LocatedBlock
		/// </returns>
		internal static IDictionary<LocatedBlock, IList<VolumeId>> AssociateVolumeIdsWithBlocks
			(IList<LocatedBlock> blocks, IDictionary<DatanodeInfo, HdfsBlocksMetadata> metadatas
			)
		{
			// Initialize mapping of ExtendedBlock to LocatedBlock. 
			// Used to associate results from DN RPCs to the parent LocatedBlock
			IDictionary<long, LocatedBlock> blockIdToLocBlock = new Dictionary<long, LocatedBlock
				>();
			foreach (LocatedBlock b in blocks)
			{
				blockIdToLocBlock[b.GetBlock().GetBlockId()] = b;
			}
			// Initialize the mapping of blocks -> list of VolumeIds, one per replica
			// This is filled out with real values from the DN RPCs
			IDictionary<LocatedBlock, IList<VolumeId>> blockVolumeIds = new Dictionary<LocatedBlock
				, IList<VolumeId>>();
			foreach (LocatedBlock b_1 in blocks)
			{
				AList<VolumeId> l = new AList<VolumeId>(b_1.GetLocations().Length);
				for (int i = 0; i < b_1.GetLocations().Length; i++)
				{
					l.AddItem(null);
				}
				blockVolumeIds[b_1] = l;
			}
			// Iterate through the list of metadatas (one per datanode). 
			// For each metadata, if it's valid, insert its volume location information 
			// into the Map returned to the caller 
			foreach (KeyValuePair<DatanodeInfo, HdfsBlocksMetadata> entry in metadatas)
			{
				DatanodeInfo datanode = entry.Key;
				HdfsBlocksMetadata metadata = entry.Value;
				// Check if metadata is valid
				if (metadata == null)
				{
					continue;
				}
				long[] metaBlockIds = metadata.GetBlockIds();
				IList<byte[]> metaVolumeIds = metadata.GetVolumeIds();
				IList<int> metaVolumeIndexes = metadata.GetVolumeIndexes();
				// Add VolumeId for each replica in the HdfsBlocksMetadata
				for (int j = 0; j < metaBlockIds.Length; j++)
				{
					int volumeIndex = metaVolumeIndexes[j];
					long blockId = metaBlockIds[j];
					// Skip if block wasn't found, or not a valid index into metaVolumeIds
					// Also skip if the DN responded with a block we didn't ask for
					if (volumeIndex == int.MaxValue || volumeIndex >= metaVolumeIds.Count || !blockIdToLocBlock
						.Contains(blockId))
					{
						if (Log.IsDebugEnabled())
						{
							Log.Debug("No data for block " + blockId);
						}
						continue;
					}
					// Get the VolumeId by indexing into the list of VolumeIds
					// provided by the datanode
					byte[] volumeId = metaVolumeIds[volumeIndex];
					HdfsVolumeId id = new HdfsVolumeId(volumeId);
					// Find out which index we are in the LocatedBlock's replicas
					LocatedBlock locBlock = blockIdToLocBlock[blockId];
					DatanodeInfo[] dnInfos = locBlock.GetLocations();
					int index = -1;
					for (int k = 0; k < dnInfos.Length; k++)
					{
						if (dnInfos[k].Equals(datanode))
						{
							index = k;
							break;
						}
					}
					if (index < 0)
					{
						if (Log.IsDebugEnabled())
						{
							Log.Debug("Datanode responded with a block volume id we did" + " not request, omitting."
								);
						}
						continue;
					}
					// Place VolumeId at the same index as the DN's index in the list of
					// replicas
					IList<VolumeId> volumeIds = blockVolumeIds[locBlock];
					volumeIds.Set(index, id);
				}
			}
			return blockVolumeIds;
		}

		/// <summary>
		/// Helper method to combine a list of
		/// <see cref="Org.Apache.Hadoop.Hdfs.Protocol.LocatedBlock"/>
		/// with associated
		/// <see cref="Org.Apache.Hadoop.FS.VolumeId"/>
		/// information to form a list of
		/// <see cref="Org.Apache.Hadoop.FS.BlockStorageLocation"/>
		/// .
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		internal static BlockStorageLocation[] ConvertToVolumeBlockLocations(IList<LocatedBlock
			> blocks, IDictionary<LocatedBlock, IList<VolumeId>> blockVolumeIds)
		{
			// Construct the final return value of VolumeBlockLocation[]
			BlockLocation[] locations = DFSUtil.LocatedBlocks2Locations(blocks);
			IList<BlockStorageLocation> volumeBlockLocs = new AList<BlockStorageLocation>(locations
				.Length);
			for (int i = 0; i < locations.Length; i++)
			{
				LocatedBlock locBlock = blocks[i];
				IList<VolumeId> volumeIds = blockVolumeIds[locBlock];
				BlockStorageLocation bsLoc = new BlockStorageLocation(locations[i], Sharpen.Collections.ToArray
					(volumeIds, new VolumeId[0]));
				volumeBlockLocs.AddItem(bsLoc);
			}
			return Sharpen.Collections.ToArray(volumeBlockLocs, new BlockStorageLocation[] { 
				 });
		}

		/// <summary>
		/// Callable that sets up an RPC proxy to a datanode and queries it for
		/// volume location information for a list of ExtendedBlocks.
		/// </summary>
		private class VolumeBlockLocationCallable : Callable<HdfsBlocksMetadata>
		{
			private readonly Configuration configuration;

			private readonly int timeout;

			private readonly DatanodeInfo datanode;

			private readonly string poolId;

			private readonly long[] blockIds;

			private readonly IList<Org.Apache.Hadoop.Security.Token.Token<BlockTokenIdentifier
				>> dnTokens;

			private readonly bool connectToDnViaHostname;

			private readonly Span parentSpan;

			internal VolumeBlockLocationCallable(Configuration configuration, DatanodeInfo datanode
				, string poolId, long[] blockIds, IList<Org.Apache.Hadoop.Security.Token.Token<BlockTokenIdentifier
				>> dnTokens, int timeout, bool connectToDnViaHostname, Span parentSpan)
			{
				this.configuration = configuration;
				this.timeout = timeout;
				this.datanode = datanode;
				this.poolId = poolId;
				this.blockIds = blockIds;
				this.dnTokens = dnTokens;
				this.connectToDnViaHostname = connectToDnViaHostname;
				this.parentSpan = parentSpan;
			}

			public virtual DatanodeInfo GetDatanodeInfo()
			{
				return datanode;
			}

			/// <exception cref="System.Exception"/>
			public virtual HdfsBlocksMetadata Call()
			{
				HdfsBlocksMetadata metadata = null;
				// Create the RPC proxy and make the RPC
				ClientDatanodeProtocol cdp = null;
				TraceScope scope = Trace.StartSpan("getHdfsBlocksMetadata", parentSpan);
				try
				{
					cdp = DFSUtil.CreateClientDatanodeProtocolProxy(datanode, configuration, timeout, 
						connectToDnViaHostname);
					metadata = cdp.GetHdfsBlocksMetadata(poolId, blockIds, dnTokens);
				}
				catch (IOException e)
				{
					// Bubble this up to the caller, handle with the Future
					throw;
				}
				finally
				{
					scope.Close();
					if (cdp != null)
					{
						RPC.StopProxy(cdp);
					}
				}
				return metadata;
			}
		}
	}
}
