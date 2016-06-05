using System;
using System.Collections.Generic;
using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset.Impl;
using Org.Apache.Hadoop.Hdfs.Server.Datanode.Metrics;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset
{
	/// <summary>
	/// This is a service provider interface for the underlying storage that
	/// stores replicas for a data node.
	/// </summary>
	/// <remarks>
	/// This is a service provider interface for the underlying storage that
	/// stores replicas for a data node.
	/// The default implementation stores replicas on local drives.
	/// </remarks>
	public abstract class FsDatasetSpi<V> : FSDatasetMBean
		where V : FsVolumeSpi
	{
		/// <summary>
		/// A factory for creating
		/// <see cref="FsDatasetSpi{V}"/>
		/// objects.
		/// </summary>
		public abstract class Factory<D>
			where D : FsDatasetSpi<object>
		{
			/// <returns>the configured factory.</returns>
			public static FsDatasetSpi.Factory<object> GetFactory(Configuration conf)
			{
				Type clazz = conf.GetClass<FsDatasetSpi.Factory>(DFSConfigKeys.DfsDatanodeFsdatasetFactoryKey
					, typeof(FsDatasetFactory));
				return ReflectionUtils.NewInstance(clazz, conf);
			}

			/// <summary>Create a new object.</summary>
			/// <exception cref="System.IO.IOException"/>
			public abstract D NewInstance(DataNode datanode, DataStorage storage, Configuration
				 conf);

			/// <summary>Does the factory create simulated objects?</summary>
			public virtual bool IsSimulated()
			{
				return false;
			}
		}

		/// <returns>a list of volumes.</returns>
		public abstract IList<V> GetVolumes();

		/// <summary>
		/// Add a new volume to the FsDataset.<p/>
		/// If the FSDataset supports block scanning, this function registers
		/// the new volume with the block scanner.
		/// </summary>
		/// <param name="location">The storage location for the new volume.</param>
		/// <param name="nsInfos">Namespace information for the new volume.</param>
		/// <exception cref="System.IO.IOException"/>
		public abstract void AddVolume(StorageLocation location, IList<NamespaceInfo> nsInfos
			);

		/// <summary>Removes a collection of volumes from FsDataset.</summary>
		/// <remarks>
		/// Removes a collection of volumes from FsDataset.
		/// If the FSDataset supports block scanning, this function removes
		/// the volumes from the block scanner.
		/// </remarks>
		/// <param name="volumes">The paths of the volumes to be removed.</param>
		/// <param name="clearFailure">
		/// set true to clear the failure information about the
		/// volumes.
		/// </param>
		public abstract void RemoveVolumes(ICollection<FilePath> volumes, bool clearFailure
			);

		/// <returns>a storage with the given storage ID</returns>
		public abstract DatanodeStorage GetStorage(string storageUuid);

		/// <returns>one or more storage reports for attached volumes.</returns>
		/// <exception cref="System.IO.IOException"/>
		public abstract StorageReport[] GetStorageReports(string bpid);

		/// <returns>the volume that contains a replica of the block.</returns>
		public abstract V GetVolume(ExtendedBlock b);

		/// <returns>a volume information map (name =&gt; info).</returns>
		public abstract IDictionary<string, object> GetVolumeInfoMap();

		/// <summary>Returns info about volume failures.</summary>
		/// <returns>info about volume failures, possibly null</returns>
		public abstract VolumeFailureSummary GetVolumeFailureSummary();

		/// <returns>a list of finalized blocks for the given block pool.</returns>
		public abstract IList<FinalizedReplica> GetFinalizedBlocks(string bpid);

		/// <returns>a list of finalized blocks for the given block pool.</returns>
		public abstract IList<FinalizedReplica> GetFinalizedBlocksOnPersistentStorage(string
			 bpid);

		/// <summary>
		/// Check whether the in-memory block record matches the block on the disk,
		/// and, in case that they are not matched, update the record or mark it
		/// as corrupted.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public abstract void CheckAndUpdate(string bpid, long blockId, FilePath diskFile, 
			FilePath diskMetaFile, FsVolumeSpi vol);

		/// <param name="b">- the block</param>
		/// <returns>
		/// a stream if the meta-data of the block exists;
		/// otherwise, return null.
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		public abstract LengthInputStream GetMetaDataInputStream(ExtendedBlock b);

		/// <summary>Returns the specified block's on-disk length (excluding metadata)</summary>
		/// <returns>the specified block's on-disk length (excluding metadta)</returns>
		/// <exception cref="System.IO.IOException">on error</exception>
		public abstract long GetLength(ExtendedBlock b);

		/// <summary>Get reference to the replica meta info in the replicasMap.</summary>
		/// <remarks>
		/// Get reference to the replica meta info in the replicasMap.
		/// To be called from methods that are synchronized on
		/// <see cref="FSDataset"/>
		/// </remarks>
		/// <returns>replica from the replicas map</returns>
		[Obsolete]
		public abstract Replica GetReplica(string bpid, long blockId);

		/// <returns>replica meta information</returns>
		public abstract string GetReplicaString(string bpid, long blockId);

		/// <returns>the generation stamp stored with the block.</returns>
		/// <exception cref="System.IO.IOException"/>
		public abstract Block GetStoredBlock(string bpid, long blkid);

		/// <summary>Returns an input stream at specified offset of the specified block</summary>
		/// <param name="b">block</param>
		/// <param name="seekOffset">offset with in the block to seek to</param>
		/// <returns>
		/// an input stream to read the contents of the specified block,
		/// starting at the offset
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		public abstract InputStream GetBlockInputStream(ExtendedBlock b, long seekOffset);

		/// <summary>
		/// Returns an input stream at specified offset of the specified block
		/// The block is still in the tmp directory and is not finalized
		/// </summary>
		/// <returns>
		/// an input stream to read the contents of the specified block,
		/// starting at the offset
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		public abstract ReplicaInputStreams GetTmpInputStreams(ExtendedBlock b, long blkoff
			, long ckoff);

		/// <summary>Creates a temporary replica and returns the meta information of the replica
		/// 	</summary>
		/// <param name="b">block</param>
		/// <returns>the meta info of the replica which is being written to</returns>
		/// <exception cref="System.IO.IOException">if an error occurs</exception>
		public abstract ReplicaHandler CreateTemporary(StorageType storageType, ExtendedBlock
			 b);

		/// <summary>Creates a RBW replica and returns the meta info of the replica</summary>
		/// <param name="b">block</param>
		/// <returns>the meta info of the replica which is being written to</returns>
		/// <exception cref="System.IO.IOException">if an error occurs</exception>
		public abstract ReplicaHandler CreateRbw(StorageType storageType, ExtendedBlock b
			, bool allowLazyPersist);

		/// <summary>Recovers a RBW replica and returns the meta info of the replica</summary>
		/// <param name="b">block</param>
		/// <param name="newGS">the new generation stamp for the replica</param>
		/// <param name="minBytesRcvd">the minimum number of bytes that the replica could have
		/// 	</param>
		/// <param name="maxBytesRcvd">the maximum number of bytes that the replica could have
		/// 	</param>
		/// <returns>the meta info of the replica which is being written to</returns>
		/// <exception cref="System.IO.IOException">if an error occurs</exception>
		public abstract ReplicaHandler RecoverRbw(ExtendedBlock b, long newGS, long minBytesRcvd
			, long maxBytesRcvd);

		/// <summary>Covert a temporary replica to a RBW.</summary>
		/// <param name="temporary">the temporary replica being converted</param>
		/// <returns>the result RBW</returns>
		/// <exception cref="System.IO.IOException"/>
		public abstract ReplicaInPipelineInterface ConvertTemporaryToRbw(ExtendedBlock temporary
			);

		/// <summary>Append to a finalized replica and returns the meta info of the replica</summary>
		/// <param name="b">block</param>
		/// <param name="newGS">the new generation stamp for the replica</param>
		/// <param name="expectedBlockLen">the number of bytes the replica is expected to have
		/// 	</param>
		/// <returns>the meata info of the replica which is being written to</returns>
		/// <exception cref="System.IO.IOException"/>
		public abstract ReplicaHandler Append(ExtendedBlock b, long newGS, long expectedBlockLen
			);

		/// <summary>
		/// Recover a failed append to a finalized replica
		/// and returns the meta info of the replica
		/// </summary>
		/// <param name="b">block</param>
		/// <param name="newGS">the new generation stamp for the replica</param>
		/// <param name="expectedBlockLen">the number of bytes the replica is expected to have
		/// 	</param>
		/// <returns>the meta info of the replica which is being written to</returns>
		/// <exception cref="System.IO.IOException"/>
		public abstract ReplicaHandler RecoverAppend(ExtendedBlock b, long newGS, long expectedBlockLen
			);

		/// <summary>
		/// Recover a failed pipeline close
		/// It bumps the replica's generation stamp and finalize it if RBW replica
		/// </summary>
		/// <param name="b">block</param>
		/// <param name="newGS">the new generation stamp for the replica</param>
		/// <param name="expectedBlockLen">the number of bytes the replica is expected to have
		/// 	</param>
		/// <returns>the storage uuid of the replica.</returns>
		/// <exception cref="System.IO.IOException"/>
		public abstract string RecoverClose(ExtendedBlock b, long newGS, long expectedBlockLen
			);

		/// <summary>Finalizes the block previously opened for writing using writeToBlock.</summary>
		/// <remarks>
		/// Finalizes the block previously opened for writing using writeToBlock.
		/// The block size is what is in the parameter b and it must match the amount
		/// of data written
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Server.Datanode.ReplicaNotFoundException"
		/// 	>
		/// if the replica can not be found when the
		/// block is been finalized. For instance, the block resides on an HDFS volume
		/// that has been removed.
		/// </exception>
		public abstract void FinalizeBlock(ExtendedBlock b);

		/// <summary>Unfinalizes the block previously opened for writing using writeToBlock.</summary>
		/// <remarks>
		/// Unfinalizes the block previously opened for writing using writeToBlock.
		/// The temporary file associated with this block is deleted.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public abstract void UnfinalizeBlock(ExtendedBlock b);

		/// <summary>Returns one block report per volume.</summary>
		/// <param name="bpid">Block Pool Id</param>
		/// <returns>- a map of DatanodeStorage to block report for the volume.</returns>
		public abstract IDictionary<DatanodeStorage, BlockListAsLongs> GetBlockReports(string
			 bpid);

		/// <summary>
		/// Returns the cache report - the full list of cached block IDs of a
		/// block pool.
		/// </summary>
		/// <param name="bpid">Block Pool Id</param>
		/// <returns>the cache report - the full list of cached block IDs.</returns>
		public abstract IList<long> GetCacheReport(string bpid);

		/// <summary>Does the dataset contain the block?</summary>
		public abstract bool Contains(ExtendedBlock block);

		/// <summary>Check if a block is valid.</summary>
		/// <param name="b">The block to check.</param>
		/// <param name="minLength">The minimum length that the block must have.  May be 0.</param>
		/// <param name="state">
		/// If this is null, it is ignored.  If it is non-null, we
		/// will check that the replica has this state.
		/// </param>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Server.Datanode.ReplicaNotFoundException"
		/// 	>If the replica is not found</exception>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Server.Datanode.UnexpectedReplicaStateException
		/// 	">
		/// If the replica is not in the
		/// expected state.
		/// </exception>
		/// <exception cref="System.IO.FileNotFoundException">
		/// If the block file is not found or there
		/// was an error locating it.
		/// </exception>
		/// <exception cref="System.IO.EOFException">If the replica length is too short.</exception>
		/// <exception cref="System.IO.IOException">May be thrown from the methods called.</exception>
		public abstract void CheckBlock(ExtendedBlock b, long minLength, HdfsServerConstants.ReplicaState
			 state);

		/// <summary>Is the block valid?</summary>
		/// <returns>- true if the specified block is valid</returns>
		public abstract bool IsValidBlock(ExtendedBlock b);

		/// <summary>Is the block a valid RBW?</summary>
		/// <returns>- true if the specified block is a valid RBW</returns>
		public abstract bool IsValidRbw(ExtendedBlock b);

		/// <summary>Invalidates the specified blocks</summary>
		/// <param name="bpid">Block pool Id</param>
		/// <param name="invalidBlks">- the blocks to be invalidated</param>
		/// <exception cref="System.IO.IOException"/>
		public abstract void Invalidate(string bpid, Block[] invalidBlks);

		/// <summary>Caches the specified blocks</summary>
		/// <param name="bpid">Block pool id</param>
		/// <param name="blockIds">- block ids to cache</param>
		public abstract void Cache(string bpid, long[] blockIds);

		/// <summary>Uncaches the specified blocks</summary>
		/// <param name="bpid">Block pool id</param>
		/// <param name="blockIds">- blocks ids to uncache</param>
		public abstract void Uncache(string bpid, long[] blockIds);

		/// <summary>Determine if the specified block is cached.</summary>
		/// <param name="bpid">Block pool id</param>
		/// <param name="blockIds">- block id</param>
		/// <returns>true if the block is cached</returns>
		public abstract bool IsCached(string bpid, long blockId);

		/// <summary>Check if all the data directories are healthy</summary>
		/// <returns>A set of unhealthy data directories.</returns>
		public abstract ICollection<FilePath> CheckDataDir();

		/// <summary>Shutdown the FSDataset</summary>
		public abstract void Shutdown();

		/// <summary>
		/// Sets the file pointer of the checksum stream so that the last checksum
		/// will be overwritten
		/// </summary>
		/// <param name="b">block</param>
		/// <param name="outs">The streams for the data file and checksum file</param>
		/// <param name="checksumSize">number of bytes each checksum has</param>
		/// <exception cref="System.IO.IOException"/>
		public abstract void AdjustCrcChannelPosition(ExtendedBlock b, ReplicaOutputStreams
			 outs, int checksumSize);

		/// <summary>Checks how many valid storage volumes there are in the DataNode.</summary>
		/// <returns>
		/// true if more than the minimum number of valid volumes are left
		/// in the FSDataSet.
		/// </returns>
		public abstract bool HasEnoughResource();

		/// <summary>Get visible length of the specified replica.</summary>
		/// <exception cref="System.IO.IOException"/>
		public abstract long GetReplicaVisibleLength(ExtendedBlock block);

		/// <summary>Initialize a replica recovery.</summary>
		/// <returns>
		/// actual state of the replica on this data-node or
		/// null if data-node does not have the replica.
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		public abstract ReplicaRecoveryInfo InitReplicaRecovery(BlockRecoveryCommand.RecoveringBlock
			 rBlock);

		/// <summary>Update replica's generation stamp and length and finalize it.</summary>
		/// <returns>the ID of storage that stores the block</returns>
		/// <exception cref="System.IO.IOException"/>
		public abstract string UpdateReplicaUnderRecovery(ExtendedBlock oldBlock, long recoveryId
			, long newBlockId, long newLength);

		/// <summary>add new block pool ID</summary>
		/// <param name="bpid">Block pool Id</param>
		/// <param name="conf">Configuration</param>
		/// <exception cref="System.IO.IOException"/>
		public abstract void AddBlockPool(string bpid, Configuration conf);

		/// <summary>Shutdown and remove the block pool from underlying storage.</summary>
		/// <param name="bpid">Block pool Id to be removed</param>
		public abstract void ShutdownBlockPool(string bpid);

		/// <summary>Deletes the block pool directories.</summary>
		/// <remarks>
		/// Deletes the block pool directories. If force is false, directories are
		/// deleted only if no block files exist for the block pool. If force
		/// is true entire directory for the blockpool is deleted along with its
		/// contents.
		/// </remarks>
		/// <param name="bpid">BlockPool Id to be deleted.</param>
		/// <param name="force">
		/// If force is false, directories are deleted only if no
		/// block files exist for the block pool, otherwise entire
		/// directory for the blockpool is deleted along with its contents.
		/// </param>
		/// <exception cref="System.IO.IOException"/>
		public abstract void DeleteBlockPool(string bpid, bool force);

		/// <summary>
		/// Get
		/// <see cref="Org.Apache.Hadoop.Hdfs.Protocol.BlockLocalPathInfo"/>
		/// for the given block.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public abstract BlockLocalPathInfo GetBlockLocalPathInfo(ExtendedBlock b);

		/// <summary>
		/// Get a
		/// <see cref="Org.Apache.Hadoop.Hdfs.Protocol.HdfsBlocksMetadata"/>
		/// corresponding to the list of blocks in
		/// <code>blocks</code>.
		/// </summary>
		/// <param name="bpid">pool to query</param>
		/// <param name="blockIds">List of block ids for which to return metadata</param>
		/// <returns>metadata Metadata for the list of blocks</returns>
		/// <exception cref="System.IO.IOException"/>
		public abstract HdfsBlocksMetadata GetHdfsBlocksMetadata(string bpid, long[] blockIds
			);

		/// <summary>Enable 'trash' for the given dataset.</summary>
		/// <remarks>
		/// Enable 'trash' for the given dataset. When trash is enabled, files are
		/// moved to a separate trash directory instead of being deleted immediately.
		/// This can be useful for example during rolling upgrades.
		/// </remarks>
		public abstract void EnableTrash(string bpid);

		/// <summary>Clear trash</summary>
		public abstract void ClearTrash(string bpid);

		/// <returns>true when trash is enabled</returns>
		public abstract bool TrashEnabled(string bpid);

		/// <summary>Create a marker file indicating that a rolling upgrade is in progress.</summary>
		/// <exception cref="System.IO.IOException"/>
		public abstract void SetRollingUpgradeMarker(string bpid);

		/// <summary>Delete the rolling upgrade marker file if it exists.</summary>
		/// <param name="bpid"/>
		/// <exception cref="System.IO.IOException"/>
		public abstract void ClearRollingUpgradeMarker(string bpid);

		/// <summary>submit a sync_file_range request to AsyncDiskService</summary>
		public abstract void SubmitBackgroundSyncFileRangeRequest(ExtendedBlock block, FileDescriptor
			 fd, long offset, long nbytes, int flags);

		/// <summary>Callback from RamDiskAsyncLazyPersistService upon async lazy persist task end
		/// 	</summary>
		public abstract void OnCompleteLazyPersist(string bpId, long blockId, long creationTime
			, FilePath[] savedFiles, V targetVolume);

		/// <summary>Callback from RamDiskAsyncLazyPersistService upon async lazy persist task fail
		/// 	</summary>
		public abstract void OnFailLazyPersist(string bpId, long blockId);

		/// <summary>Move block from one storage to another storage</summary>
		/// <exception cref="System.IO.IOException"/>
		public abstract ReplicaInfo MoveBlockAcrossStorage(ExtendedBlock block, StorageType
			 targetStorageType);

		/// <summary>
		/// Set a block to be pinned on this datanode so that it cannot be moved
		/// by Balancer/Mover.
		/// </summary>
		/// <remarks>
		/// Set a block to be pinned on this datanode so that it cannot be moved
		/// by Balancer/Mover.
		/// It is a no-op when dfs.datanode.block-pinning.enabled is set to false.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public abstract void SetPinning(ExtendedBlock block);

		/// <summary>Check whether the block was pinned</summary>
		/// <exception cref="System.IO.IOException"/>
		public abstract bool GetPinning(ExtendedBlock block);

		/// <summary>Confirm whether the block is deleting</summary>
		public abstract bool IsDeletingBlock(string bpid, long blockId);
	}

	public static class FsDatasetSpiConstants
	{
	}
}
