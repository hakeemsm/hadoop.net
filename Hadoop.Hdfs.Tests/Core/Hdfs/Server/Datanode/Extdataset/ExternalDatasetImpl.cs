using System;
using System.Collections.Generic;
using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode.Extdataset
{
	public class ExternalDatasetImpl : FsDatasetSpi<ExternalVolumeImpl>
	{
		private readonly DatanodeStorage storage = new DatanodeStorage(DatanodeStorage.GenerateUuid
			(), DatanodeStorage.State.Normal, StorageType.Default);

		public override IList<ExternalVolumeImpl> GetVolumes()
		{
			return null;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void AddVolume(StorageLocation location, IList<NamespaceInfo> nsInfos
			)
		{
		}

		public override void RemoveVolumes(ICollection<FilePath> volumes, bool clearFailure
			)
		{
		}

		public override DatanodeStorage GetStorage(string storageUuid)
		{
			return null;
		}

		/// <exception cref="System.IO.IOException"/>
		public override StorageReport[] GetStorageReports(string bpid)
		{
			StorageReport[] result = new StorageReport[1];
			result[0] = new StorageReport(storage, false, 0, 0, 0, 0);
			return result;
		}

		public override ExternalVolumeImpl GetVolume(ExtendedBlock b)
		{
			return null;
		}

		public override IDictionary<string, object> GetVolumeInfoMap()
		{
			return null;
		}

		public override IList<FinalizedReplica> GetFinalizedBlocks(string bpid)
		{
			return null;
		}

		public override IList<FinalizedReplica> GetFinalizedBlocksOnPersistentStorage(string
			 bpid)
		{
			return null;
		}

		public override void CheckAndUpdate(string bpid, long blockId, FilePath diskFile, 
			FilePath diskMetaFile, FsVolumeSpi vol)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public override LengthInputStream GetMetaDataInputStream(ExtendedBlock b)
		{
			return new LengthInputStream(null, 0);
		}

		/// <exception cref="System.IO.IOException"/>
		public override long GetLength(ExtendedBlock b)
		{
			return 0;
		}

		[Obsolete]
		public override Replica GetReplica(string bpid, long blockId)
		{
			return new ExternalReplica();
		}

		public override string GetReplicaString(string bpid, long blockId)
		{
			return null;
		}

		/// <exception cref="System.IO.IOException"/>
		public override Block GetStoredBlock(string bpid, long blkid)
		{
			return new Block();
		}

		/// <exception cref="System.IO.IOException"/>
		public override InputStream GetBlockInputStream(ExtendedBlock b, long seekOffset)
		{
			return null;
		}

		/// <exception cref="System.IO.IOException"/>
		public override ReplicaInputStreams GetTmpInputStreams(ExtendedBlock b, long blkoff
			, long ckoff)
		{
			return new ReplicaInputStreams(null, null, null);
		}

		/// <exception cref="System.IO.IOException"/>
		public override ReplicaHandler CreateTemporary(StorageType t, ExtendedBlock b)
		{
			return new ReplicaHandler(new ExternalReplicaInPipeline(), null);
		}

		/// <exception cref="System.IO.IOException"/>
		public override ReplicaHandler CreateRbw(StorageType t, ExtendedBlock b, bool tf)
		{
			return new ReplicaHandler(new ExternalReplicaInPipeline(), null);
		}

		/// <exception cref="System.IO.IOException"/>
		public override ReplicaHandler RecoverRbw(ExtendedBlock b, long newGS, long minBytesRcvd
			, long maxBytesRcvd)
		{
			return new ReplicaHandler(new ExternalReplicaInPipeline(), null);
		}

		/// <exception cref="System.IO.IOException"/>
		public override ReplicaInPipelineInterface ConvertTemporaryToRbw(ExtendedBlock temporary
			)
		{
			return new ExternalReplicaInPipeline();
		}

		/// <exception cref="System.IO.IOException"/>
		public override ReplicaHandler Append(ExtendedBlock b, long newGS, long expectedBlockLen
			)
		{
			return new ReplicaHandler(new ExternalReplicaInPipeline(), null);
		}

		/// <exception cref="System.IO.IOException"/>
		public override ReplicaHandler RecoverAppend(ExtendedBlock b, long newGS, long expectedBlockLen
			)
		{
			return new ReplicaHandler(new ExternalReplicaInPipeline(), null);
		}

		/// <exception cref="System.IO.IOException"/>
		public override string RecoverClose(ExtendedBlock b, long newGS, long expectedBlockLen
			)
		{
			return null;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void FinalizeBlock(ExtendedBlock b)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public override void UnfinalizeBlock(ExtendedBlock b)
		{
		}

		public override IDictionary<DatanodeStorage, BlockListAsLongs> GetBlockReports(string
			 bpid)
		{
			IDictionary<DatanodeStorage, BlockListAsLongs> result = new Dictionary<DatanodeStorage
				, BlockListAsLongs>();
			result[storage] = BlockListAsLongs.Empty;
			return result;
		}

		public override IList<long> GetCacheReport(string bpid)
		{
			return null;
		}

		public override bool Contains(ExtendedBlock block)
		{
			return false;
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Server.Datanode.ReplicaNotFoundException"
		/// 	/>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Server.Datanode.UnexpectedReplicaStateException
		/// 	"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="System.IO.EOFException"/>
		/// <exception cref="System.IO.IOException"/>
		public override void CheckBlock(ExtendedBlock b, long minLength, HdfsServerConstants.ReplicaState
			 state)
		{
		}

		public override bool IsValidBlock(ExtendedBlock b)
		{
			return false;
		}

		public override bool IsValidRbw(ExtendedBlock b)
		{
			return false;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Invalidate(string bpid, Block[] invalidBlks)
		{
		}

		public override void Cache(string bpid, long[] blockIds)
		{
		}

		public override void Uncache(string bpid, long[] blockIds)
		{
		}

		public override bool IsCached(string bpid, long blockId)
		{
			return false;
		}

		public override ICollection<FilePath> CheckDataDir()
		{
			return null;
		}

		public override void Shutdown()
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public override void AdjustCrcChannelPosition(ExtendedBlock b, ReplicaOutputStreams
			 outs, int checksumSize)
		{
		}

		public override bool HasEnoughResource()
		{
			return false;
		}

		/// <exception cref="System.IO.IOException"/>
		public override long GetReplicaVisibleLength(ExtendedBlock block)
		{
			return 0;
		}

		/// <exception cref="System.IO.IOException"/>
		public override ReplicaRecoveryInfo InitReplicaRecovery(BlockRecoveryCommand.RecoveringBlock
			 rBlock)
		{
			return new ReplicaRecoveryInfo(0, 0, 0, HdfsServerConstants.ReplicaState.Finalized
				);
		}

		/// <exception cref="System.IO.IOException"/>
		public override string UpdateReplicaUnderRecovery(ExtendedBlock oldBlock, long recoveryId
			, long newBlockId, long newLength)
		{
			return null;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void AddBlockPool(string bpid, Configuration conf)
		{
		}

		public override void ShutdownBlockPool(string bpid)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public override void DeleteBlockPool(string bpid, bool force)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public override BlockLocalPathInfo GetBlockLocalPathInfo(ExtendedBlock b)
		{
			return new BlockLocalPathInfo(null, "file", "metafile");
		}

		/// <exception cref="System.IO.IOException"/>
		public override HdfsBlocksMetadata GetHdfsBlocksMetadata(string bpid, long[] blockIds
			)
		{
			return new HdfsBlocksMetadata(null, null, null, null);
		}

		public override void EnableTrash(string bpid)
		{
		}

		public override void ClearTrash(string bpid)
		{
		}

		public override bool TrashEnabled(string bpid)
		{
			return false;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void SetRollingUpgradeMarker(string bpid)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public override void ClearRollingUpgradeMarker(string bpid)
		{
		}

		public override void SubmitBackgroundSyncFileRangeRequest(ExtendedBlock block, FileDescriptor
			 fd, long offset, long nbytes, int flags)
		{
		}

		public override void OnCompleteLazyPersist(string bpId, long blockId, long creationTime
			, FilePath[] savedFiles, ExternalVolumeImpl targetVolume)
		{
		}

		public override void OnFailLazyPersist(string bpId, long blockId)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public override ReplicaInfo MoveBlockAcrossStorage(ExtendedBlock block, StorageType
			 targetStorageType)
		{
			return null;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual long GetBlockPoolUsed(string bpid)
		{
			return 0;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual long GetDfsUsed()
		{
			return 0;
		}

		public virtual long GetCapacity()
		{
			return 0;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual long GetRemaining()
		{
			return 0;
		}

		public virtual string GetStorageInfo()
		{
			return null;
		}

		public virtual int GetNumFailedVolumes()
		{
			return 0;
		}

		public virtual string[] GetFailedStorageLocations()
		{
			return null;
		}

		public virtual long GetLastVolumeFailureDate()
		{
			return 0;
		}

		public virtual long GetEstimatedCapacityLostTotal()
		{
			return 0;
		}

		public override VolumeFailureSummary GetVolumeFailureSummary()
		{
			return null;
		}

		public virtual long GetCacheUsed()
		{
			return 0;
		}

		public virtual long GetCacheCapacity()
		{
			return 0;
		}

		public virtual long GetNumBlocksCached()
		{
			return 0;
		}

		public virtual long GetNumBlocksFailedToCache()
		{
			return 0;
		}

		public virtual long GetNumBlocksFailedToUncache()
		{
			return 0;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void SetPinning(ExtendedBlock block)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool GetPinning(ExtendedBlock block)
		{
			return false;
		}

		public override bool IsDeletingBlock(string bpid, long blockId)
		{
			return false;
		}
	}
}
