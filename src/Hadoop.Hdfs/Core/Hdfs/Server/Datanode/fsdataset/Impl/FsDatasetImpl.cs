using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Javax.Management;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset;
using Org.Apache.Hadoop.Hdfs.Server.Datanode.Metrics;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.IO.Nativeio;
using Org.Apache.Hadoop.Metrics2.Util;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset.Impl
{
	/// <summary>FSDataset manages a set of data blocks.</summary>
	/// <remarks>
	/// FSDataset manages a set of data blocks.  Each block
	/// has a unique name and an extent on disk.
	/// </remarks>
	internal class FsDatasetImpl : FsDatasetSpi<FsVolumeImpl>
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset.Impl.FsDatasetImpl
			));

		private static readonly bool isNativeIOAvailable;

		static FsDatasetImpl()
		{
			isNativeIOAvailable = NativeIO.IsAvailable();
			if (Path.Windows && !isNativeIOAvailable)
			{
				Log.Warn("Data node cannot fully support concurrent reading" + " and writing without native code extensions on Windows."
					);
			}
		}

		public override IList<FsVolumeImpl> GetVolumes()
		{
			// FsDatasetSpi
			return volumes.GetVolumes();
		}

		public override DatanodeStorage GetStorage(string storageUuid)
		{
			return storageMap[storageUuid];
		}

		/// <exception cref="System.IO.IOException"/>
		public override StorageReport[] GetStorageReports(string bpid)
		{
			// FsDatasetSpi
			IList<StorageReport> reports;
			lock (statsLock)
			{
				IList<FsVolumeImpl> curVolumes = GetVolumes();
				reports = new AList<StorageReport>(curVolumes.Count);
				foreach (FsVolumeImpl volume in curVolumes)
				{
					try
					{
						using (FsVolumeReference @ref = volume.ObtainReference())
						{
							StorageReport sr = new StorageReport(volume.ToDatanodeStorage(), false, volume.GetCapacity
								(), volume.GetDfsUsed(), volume.GetAvailable(), volume.GetBlockPoolUsed(bpid));
							reports.AddItem(sr);
						}
					}
					catch (ClosedChannelException)
					{
						continue;
					}
				}
			}
			return Sharpen.Collections.ToArray(reports, new StorageReport[reports.Count]);
		}

		public override FsVolumeImpl GetVolume(ExtendedBlock b)
		{
			lock (this)
			{
				ReplicaInfo r = volumeMap.Get(b.GetBlockPoolId(), b.GetLocalBlock());
				return r != null ? (FsVolumeImpl)r.GetVolume() : null;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override Block GetStoredBlock(string bpid, long blkid)
		{
			lock (this)
			{
				// FsDatasetSpi
				FilePath blockfile = GetFile(bpid, blkid, false);
				if (blockfile == null)
				{
					return null;
				}
				FilePath metafile = FsDatasetUtil.FindMetaFile(blockfile);
				long gs = FsDatasetUtil.ParseGenerationStamp(blockfile, metafile);
				return new Block(blkid, blockfile.Length(), gs);
			}
		}

		/// <summary>This should be primarily used for testing.</summary>
		/// <returns>clone of replica store in datanode memory</returns>
		internal virtual ReplicaInfo FetchReplicaInfo(string bpid, long blockId)
		{
			ReplicaInfo r = volumeMap.Get(bpid, blockId);
			if (r == null)
			{
				return null;
			}
			switch (r.GetState())
			{
				case HdfsServerConstants.ReplicaState.Finalized:
				{
					return new FinalizedReplica((FinalizedReplica)r);
				}

				case HdfsServerConstants.ReplicaState.Rbw:
				{
					return new ReplicaBeingWritten((ReplicaBeingWritten)r);
				}

				case HdfsServerConstants.ReplicaState.Rwr:
				{
					return new ReplicaWaitingToBeRecovered((ReplicaWaitingToBeRecovered)r);
				}

				case HdfsServerConstants.ReplicaState.Rur:
				{
					return new ReplicaUnderRecovery((ReplicaUnderRecovery)r);
				}

				case HdfsServerConstants.ReplicaState.Temporary:
				{
					return new ReplicaInPipeline((ReplicaInPipeline)r);
				}
			}
			return null;
		}

		/// <exception cref="System.IO.IOException"/>
		public override LengthInputStream GetMetaDataInputStream(ExtendedBlock b)
		{
			// FsDatasetSpi
			FilePath meta = FsDatasetUtil.GetMetaFile(GetBlockFile(b), b.GetGenerationStamp()
				);
			if (meta == null || !meta.Exists())
			{
				return null;
			}
			if (isNativeIOAvailable)
			{
				return new LengthInputStream(NativeIO.GetShareDeleteFileInputStream(meta), meta.Length
					());
			}
			return new LengthInputStream(new FileInputStream(meta), meta.Length());
		}

		internal readonly DataNode datanode;

		internal readonly DataStorage dataStorage;

		internal readonly FsVolumeList volumes;

		internal readonly IDictionary<string, DatanodeStorage> storageMap;

		internal readonly FsDatasetAsyncDiskService asyncDiskService;

		internal readonly Daemon lazyWriter;

		internal readonly FsDatasetCache cacheManager;

		private readonly Configuration conf;

		private readonly int validVolsRequired;

		private volatile bool fsRunning;

		internal readonly ReplicaMap volumeMap;

		internal readonly IDictionary<string, ICollection<long>> deletingBlock;

		internal readonly RamDiskReplicaTracker ramDiskReplicaTracker;

		internal readonly RamDiskAsyncLazyPersistService asyncLazyPersistService;

		private const int MaxBlockEvictionsPerIteration = 3;

		private readonly object statsLock = new object();

		internal readonly LocalFileSystem localFS;

		private bool blockPinningEnabled;

		/// <summary>An FSDataset has a directory where it loads its data files.</summary>
		/// <exception cref="System.IO.IOException"/>
		internal FsDatasetImpl(DataNode datanode, DataStorage storage, Configuration conf
			)
		{
			// Used for synchronizing access to usage stats
			this.fsRunning = true;
			this.datanode = datanode;
			this.dataStorage = storage;
			this.conf = conf;
			// The number of volumes required for operation is the total number 
			// of volumes minus the number of failed volumes we can tolerate.
			int volFailuresTolerated = conf.GetInt(DFSConfigKeys.DfsDatanodeFailedVolumesToleratedKey
				, DFSConfigKeys.DfsDatanodeFailedVolumesToleratedDefault);
			string[] dataDirs = conf.GetTrimmedStrings(DFSConfigKeys.DfsDatanodeDataDirKey);
			ICollection<StorageLocation> dataLocations = DataNode.GetStorageLocations(conf);
			IList<VolumeFailureInfo> volumeFailureInfos = GetInitialVolumeFailureInfos(dataLocations
				, storage);
			int volsConfigured = (dataDirs == null) ? 0 : dataDirs.Length;
			int volsFailed = volumeFailureInfos.Count;
			this.validVolsRequired = volsConfigured - volFailuresTolerated;
			if (volFailuresTolerated < 0 || volFailuresTolerated >= volsConfigured)
			{
				throw new DiskChecker.DiskErrorException("Invalid volume failure " + " config value: "
					 + volFailuresTolerated);
			}
			if (volsFailed > volFailuresTolerated)
			{
				throw new DiskChecker.DiskErrorException("Too many failed volumes - " + "current valid volumes: "
					 + storage.GetNumStorageDirs() + ", volumes configured: " + volsConfigured + ", volumes failed: "
					 + volsFailed + ", volume failures tolerated: " + volFailuresTolerated);
			}
			storageMap = new ConcurrentHashMap<string, DatanodeStorage>();
			volumeMap = new ReplicaMap(this);
			ramDiskReplicaTracker = RamDiskReplicaTracker.GetInstance(conf, this);
			VolumeChoosingPolicy<FsVolumeImpl> blockChooserImpl = ReflectionUtils.NewInstance
				(conf.GetClass<VolumeChoosingPolicy>(DFSConfigKeys.DfsDatanodeFsdatasetVolumeChoosingPolicyKey
				, typeof(RoundRobinVolumeChoosingPolicy)), conf);
			volumes = new FsVolumeList(volumeFailureInfos, datanode.GetBlockScanner(), blockChooserImpl
				);
			asyncDiskService = new FsDatasetAsyncDiskService(datanode, this);
			asyncLazyPersistService = new RamDiskAsyncLazyPersistService(datanode);
			deletingBlock = new Dictionary<string, ICollection<long>>();
			for (int idx = 0; idx < storage.GetNumStorageDirs(); idx++)
			{
				AddVolume(dataLocations, storage.GetStorageDir(idx));
			}
			SetupAsyncLazyPersistThreads();
			cacheManager = new FsDatasetCache(this);
			// Start the lazy writer once we have built the replica maps.
			lazyWriter = new Daemon(new FsDatasetImpl.LazyWriter(this, conf));
			lazyWriter.Start();
			RegisterMBean(datanode.GetDatanodeUuid());
			localFS = FileSystem.GetLocal(conf);
			blockPinningEnabled = conf.GetBoolean(DFSConfigKeys.DfsDatanodeBlockPinningEnabled
				, DFSConfigKeys.DfsDatanodeBlockPinningEnabledDefault);
		}

		/// <summary>
		/// Gets initial volume failure information for all volumes that failed
		/// immediately at startup.
		/// </summary>
		/// <remarks>
		/// Gets initial volume failure information for all volumes that failed
		/// immediately at startup.  The method works by determining the set difference
		/// between all configured storage locations and the actual storage locations in
		/// use after attempting to put all of them into service.
		/// </remarks>
		/// <returns>each storage location that has failed</returns>
		private static IList<VolumeFailureInfo> GetInitialVolumeFailureInfos(ICollection<
			StorageLocation> dataLocations, DataStorage storage)
		{
			ICollection<string> failedLocationSet = Sets.NewHashSetWithExpectedSize(dataLocations
				.Count);
			foreach (StorageLocation sl in dataLocations)
			{
				failedLocationSet.AddItem(sl.GetFile().GetAbsolutePath());
			}
			for (IEnumerator<Storage.StorageDirectory> it = storage.DirIterator(); it.HasNext
				(); )
			{
				Storage.StorageDirectory sd = it.Next();
				failedLocationSet.Remove(sd.GetRoot().GetAbsolutePath());
			}
			IList<VolumeFailureInfo> volumeFailureInfos = Lists.NewArrayListWithCapacity(failedLocationSet
				.Count);
			long failureDate = Time.Now();
			foreach (string failedStorageLocation in failedLocationSet)
			{
				volumeFailureInfos.AddItem(new VolumeFailureInfo(failedStorageLocation, failureDate
					));
			}
			return volumeFailureInfos;
		}

		/// <exception cref="System.IO.IOException"/>
		private void AddVolume(ICollection<StorageLocation> dataLocations, Storage.StorageDirectory
			 sd)
		{
			FilePath dir = sd.GetCurrentDir();
			StorageType storageType = GetStorageTypeFromLocations(dataLocations, sd.GetRoot()
				);
			// If IOException raises from FsVolumeImpl() or getVolumeMap(), there is
			// nothing needed to be rolled back to make various data structures, e.g.,
			// storageMap and asyncDiskService, consistent.
			FsVolumeImpl fsVolume = new FsVolumeImpl(this, sd.GetStorageUuid(), dir, this.conf
				, storageType);
			FsVolumeReference @ref = fsVolume.ObtainReference();
			ReplicaMap tempVolumeMap = new ReplicaMap(this);
			fsVolume.GetVolumeMap(tempVolumeMap, ramDiskReplicaTracker);
			lock (this)
			{
				volumeMap.AddAll(tempVolumeMap);
				storageMap[sd.GetStorageUuid()] = new DatanodeStorage(sd.GetStorageUuid(), DatanodeStorage.State
					.Normal, storageType);
				asyncDiskService.AddVolume(sd.GetCurrentDir());
				volumes.AddVolume(@ref);
			}
			Log.Info("Added volume - " + dir + ", StorageType: " + storageType);
		}

		/// <exception cref="System.IO.IOException"/>
		[VisibleForTesting]
		public virtual FsVolumeImpl CreateFsVolume(string storageUuid, FilePath currentDir
			, StorageType storageType)
		{
			return new FsVolumeImpl(this, storageUuid, currentDir, conf, storageType);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void AddVolume(StorageLocation location, IList<NamespaceInfo> nsInfos
			)
		{
			FilePath dir = location.GetFile();
			// Prepare volume in DataStorage
			DataStorage.VolumeBuilder builder;
			try
			{
				builder = dataStorage.PrepareVolume(datanode, location.GetFile(), nsInfos);
			}
			catch (IOException e)
			{
				volumes.AddVolumeFailureInfo(new VolumeFailureInfo(location.GetFile().GetAbsolutePath
					(), Time.Now()));
				throw;
			}
			Storage.StorageDirectory sd = builder.GetStorageDirectory();
			StorageType storageType = location.GetStorageType();
			FsVolumeImpl fsVolume = CreateFsVolume(sd.GetStorageUuid(), sd.GetCurrentDir(), storageType
				);
			ReplicaMap tempVolumeMap = new ReplicaMap(fsVolume);
			AList<IOException> exceptions = Lists.NewArrayList();
			foreach (NamespaceInfo nsInfo in nsInfos)
			{
				string bpid = nsInfo.GetBlockPoolID();
				try
				{
					fsVolume.AddBlockPool(bpid, this.conf);
					fsVolume.GetVolumeMap(bpid, tempVolumeMap, ramDiskReplicaTracker);
				}
				catch (IOException e)
				{
					Log.Warn("Caught exception when adding " + fsVolume + ". Will throw later.", e);
					exceptions.AddItem(e);
				}
			}
			if (!exceptions.IsEmpty())
			{
				try
				{
					sd.Unlock();
				}
				catch (IOException e)
				{
					exceptions.AddItem(e);
				}
				throw MultipleIOException.CreateIOException(exceptions);
			}
			FsVolumeReference @ref = fsVolume.ObtainReference();
			SetupAsyncLazyPersistThread(fsVolume);
			builder.Build();
			lock (this)
			{
				volumeMap.AddAll(tempVolumeMap);
				storageMap[sd.GetStorageUuid()] = new DatanodeStorage(sd.GetStorageUuid(), DatanodeStorage.State
					.Normal, storageType);
				asyncDiskService.AddVolume(sd.GetCurrentDir());
				volumes.AddVolume(@ref);
			}
			Log.Info("Added volume - " + dir + ", StorageType: " + storageType);
		}

		/// <summary>Removes a set of volumes from FsDataset.</summary>
		/// <param name="volumesToRemove">a set of absolute root path of each volume.</param>
		/// <param name="clearFailure">set true to clear failure information.</param>
		public override void RemoveVolumes(ICollection<FilePath> volumesToRemove, bool clearFailure
			)
		{
			// Make sure that all volumes are absolute path.
			foreach (FilePath vol in volumesToRemove)
			{
				Preconditions.CheckArgument(vol.IsAbsolute(), string.Format("%s is not absolute path."
					, vol.GetPath()));
			}
			IDictionary<string, IList<ReplicaInfo>> blkToInvalidate = new Dictionary<string, 
				IList<ReplicaInfo>>();
			IList<string> storageToRemove = new AList<string>();
			lock (this)
			{
				for (int idx = 0; idx < dataStorage.GetNumStorageDirs(); idx++)
				{
					Storage.StorageDirectory sd = dataStorage.GetStorageDir(idx);
					FilePath absRoot = sd.GetRoot().GetAbsoluteFile();
					if (volumesToRemove.Contains(absRoot))
					{
						Log.Info("Removing " + absRoot + " from FsDataset.");
						// Disable the volume from the service.
						asyncDiskService.RemoveVolume(sd.GetCurrentDir());
						volumes.RemoveVolume(absRoot, clearFailure);
						// Removed all replica information for the blocks on the volume.
						// Unlike updating the volumeMap in addVolume(), this operation does
						// not scan disks.
						foreach (string bpid in volumeMap.GetBlockPoolList())
						{
							IList<ReplicaInfo> blocks = new AList<ReplicaInfo>();
							for (IEnumerator<ReplicaInfo> it = volumeMap.Replicas(bpid).GetEnumerator(); it.HasNext
								(); )
							{
								ReplicaInfo block = it.Next();
								FilePath absBasePath = new FilePath(block.GetVolume().GetBasePath()).GetAbsoluteFile
									();
								if (absBasePath.Equals(absRoot))
								{
									blocks.AddItem(block);
									it.Remove();
								}
							}
							blkToInvalidate[bpid] = blocks;
						}
						storageToRemove.AddItem(sd.GetStorageUuid());
					}
				}
				SetupAsyncLazyPersistThreads();
			}
			// Call this outside the lock.
			foreach (KeyValuePair<string, IList<ReplicaInfo>> entry in blkToInvalidate)
			{
				string bpid = entry.Key;
				IList<ReplicaInfo> blocks = entry.Value;
				foreach (ReplicaInfo block in blocks)
				{
					Invalidate(bpid, block);
				}
			}
			lock (this)
			{
				foreach (string storageUuid in storageToRemove)
				{
					Sharpen.Collections.Remove(storageMap, storageUuid);
				}
			}
		}

		private StorageType GetStorageTypeFromLocations(ICollection<StorageLocation> dataLocations
			, FilePath dir)
		{
			foreach (StorageLocation dataLocation in dataLocations)
			{
				if (dataLocation.GetFile().Equals(dir))
				{
					return dataLocation.GetStorageType();
				}
			}
			return StorageType.Default;
		}

		/// <summary>Return the total space used by dfs datanode</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual long GetDfsUsed()
		{
			// FSDatasetMBean
			lock (statsLock)
			{
				return volumes.GetDfsUsed();
			}
		}

		/// <summary>Return the total space used by dfs datanode</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual long GetBlockPoolUsed(string bpid)
		{
			// FSDatasetMBean
			lock (statsLock)
			{
				return volumes.GetBlockPoolUsed(bpid);
			}
		}

		/// <summary>Return true - if there are still valid volumes on the DataNode.</summary>
		public override bool HasEnoughResource()
		{
			// FsDatasetSpi
			return GetVolumes().Count >= validVolsRequired;
		}

		/// <summary>Return total capacity, used and unused</summary>
		public virtual long GetCapacity()
		{
			// FSDatasetMBean
			lock (statsLock)
			{
				return volumes.GetCapacity();
			}
		}

		/// <summary>Return how many bytes can still be stored in the FSDataset</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual long GetRemaining()
		{
			// FSDatasetMBean
			lock (statsLock)
			{
				return volumes.GetRemaining();
			}
		}

		/// <summary>Return the number of failed volumes in the FSDataset.</summary>
		public virtual int GetNumFailedVolumes()
		{
			// FSDatasetMBean
			return volumes.GetVolumeFailureInfos().Length;
		}

		public virtual string[] GetFailedStorageLocations()
		{
			// FSDatasetMBean
			VolumeFailureInfo[] infos = volumes.GetVolumeFailureInfos();
			IList<string> failedStorageLocations = Lists.NewArrayListWithCapacity(infos.Length
				);
			foreach (VolumeFailureInfo info in infos)
			{
				failedStorageLocations.AddItem(info.GetFailedStorageLocation());
			}
			return Sharpen.Collections.ToArray(failedStorageLocations, new string[failedStorageLocations
				.Count]);
		}

		public virtual long GetLastVolumeFailureDate()
		{
			// FSDatasetMBean
			long lastVolumeFailureDate = 0;
			foreach (VolumeFailureInfo info in volumes.GetVolumeFailureInfos())
			{
				long failureDate = info.GetFailureDate();
				if (failureDate > lastVolumeFailureDate)
				{
					lastVolumeFailureDate = failureDate;
				}
			}
			return lastVolumeFailureDate;
		}

		public virtual long GetEstimatedCapacityLostTotal()
		{
			// FSDatasetMBean
			long estimatedCapacityLostTotal = 0;
			foreach (VolumeFailureInfo info in volumes.GetVolumeFailureInfos())
			{
				estimatedCapacityLostTotal += info.GetEstimatedCapacityLost();
			}
			return estimatedCapacityLostTotal;
		}

		public override VolumeFailureSummary GetVolumeFailureSummary()
		{
			// FsDatasetSpi
			VolumeFailureInfo[] infos = volumes.GetVolumeFailureInfos();
			if (infos.Length == 0)
			{
				return null;
			}
			IList<string> failedStorageLocations = Lists.NewArrayListWithCapacity(infos.Length
				);
			long lastVolumeFailureDate = 0;
			long estimatedCapacityLostTotal = 0;
			foreach (VolumeFailureInfo info in infos)
			{
				failedStorageLocations.AddItem(info.GetFailedStorageLocation());
				long failureDate = info.GetFailureDate();
				if (failureDate > lastVolumeFailureDate)
				{
					lastVolumeFailureDate = failureDate;
				}
				estimatedCapacityLostTotal += info.GetEstimatedCapacityLost();
			}
			return new VolumeFailureSummary(Sharpen.Collections.ToArray(failedStorageLocations
				, new string[failedStorageLocations.Count]), lastVolumeFailureDate, estimatedCapacityLostTotal
				);
		}

		public virtual long GetCacheUsed()
		{
			// FSDatasetMBean
			return cacheManager.GetCacheUsed();
		}

		public virtual long GetCacheCapacity()
		{
			// FSDatasetMBean
			return cacheManager.GetCacheCapacity();
		}

		public virtual long GetNumBlocksFailedToCache()
		{
			// FSDatasetMBean
			return cacheManager.GetNumBlocksFailedToCache();
		}

		public virtual long GetNumBlocksFailedToUncache()
		{
			// FSDatasetMBean
			return cacheManager.GetNumBlocksFailedToUncache();
		}

		public virtual long GetNumBlocksCached()
		{
			// FSDatasetMBean
			return cacheManager.GetNumBlocksCached();
		}

		/// <summary>Find the block's on-disk length</summary>
		/// <exception cref="System.IO.IOException"/>
		public override long GetLength(ExtendedBlock b)
		{
			// FsDatasetSpi
			return GetBlockFile(b).Length();
		}

		/// <summary>Get File name for a given block.</summary>
		/// <exception cref="System.IO.IOException"/>
		private FilePath GetBlockFile(ExtendedBlock b)
		{
			return GetBlockFile(b.GetBlockPoolId(), b.GetBlockId());
		}

		/// <summary>Get File name for a given block.</summary>
		/// <exception cref="System.IO.IOException"/>
		internal virtual FilePath GetBlockFile(string bpid, long blockId)
		{
			FilePath f = ValidateBlockFile(bpid, blockId);
			if (f == null)
			{
				throw new IOException("BlockId " + blockId + " is not valid.");
			}
			return f;
		}

		/// <summary>
		/// Return the File associated with a block, without first
		/// checking that it exists.
		/// </summary>
		/// <remarks>
		/// Return the File associated with a block, without first
		/// checking that it exists. This should be used when the
		/// next operation is going to open the file for read anyway,
		/// and thus the exists check is redundant.
		/// </remarks>
		/// <param name="touch">
		/// if true then update the last access timestamp of the
		/// block. Currently used for blocks on transient storage.
		/// </param>
		/// <exception cref="System.IO.IOException"/>
		private FilePath GetBlockFileNoExistsCheck(ExtendedBlock b, bool touch)
		{
			FilePath f;
			lock (this)
			{
				f = GetFile(b.GetBlockPoolId(), b.GetLocalBlock().GetBlockId(), touch);
			}
			if (f == null)
			{
				throw new IOException("Block " + b + " is not valid");
			}
			return f;
		}

		/// <exception cref="System.IO.IOException"/>
		public override InputStream GetBlockInputStream(ExtendedBlock b, long seekOffset)
		{
			// FsDatasetSpi
			FilePath blockFile = GetBlockFileNoExistsCheck(b, true);
			if (isNativeIOAvailable)
			{
				return NativeIO.GetShareDeleteFileInputStream(blockFile, seekOffset);
			}
			else
			{
				try
				{
					return OpenAndSeek(blockFile, seekOffset);
				}
				catch (FileNotFoundException)
				{
					throw new IOException("Block " + b + " is not valid. " + "Expected block file at "
						 + blockFile + " does not exist.");
				}
			}
		}

		/// <summary>Get the meta info of a block stored in volumeMap.</summary>
		/// <remarks>
		/// Get the meta info of a block stored in volumeMap. To find a block,
		/// block pool Id, block Id and generation stamp must match.
		/// </remarks>
		/// <param name="b">extended block</param>
		/// <returns>the meta replica information</returns>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Server.Datanode.ReplicaNotFoundException"
		/// 	>
		/// if no entry is in the map or
		/// there is a generation stamp mismatch
		/// </exception>
		internal virtual ReplicaInfo GetReplicaInfo(ExtendedBlock b)
		{
			ReplicaInfo info = volumeMap.Get(b.GetBlockPoolId(), b.GetLocalBlock());
			if (info == null)
			{
				throw new ReplicaNotFoundException(ReplicaNotFoundException.NonExistentReplica + 
					b);
			}
			return info;
		}

		/// <summary>Get the meta info of a block stored in volumeMap.</summary>
		/// <remarks>
		/// Get the meta info of a block stored in volumeMap. Block is looked up
		/// without matching the generation stamp.
		/// </remarks>
		/// <param name="bpid">block pool Id</param>
		/// <param name="blkid">block Id</param>
		/// <returns>the meta replica information; null if block was not found</returns>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Server.Datanode.ReplicaNotFoundException"
		/// 	>
		/// if no entry is in the map or
		/// there is a generation stamp mismatch
		/// </exception>
		private ReplicaInfo GetReplicaInfo(string bpid, long blkid)
		{
			ReplicaInfo info = volumeMap.Get(bpid, blkid);
			if (info == null)
			{
				throw new ReplicaNotFoundException(ReplicaNotFoundException.NonExistentReplica + 
					bpid + ":" + blkid);
			}
			return info;
		}

		/// <summary>Returns handles to the block file and its metadata file</summary>
		/// <exception cref="System.IO.IOException"/>
		public override ReplicaInputStreams GetTmpInputStreams(ExtendedBlock b, long blkOffset
			, long metaOffset)
		{
			lock (this)
			{
				// FsDatasetSpi
				ReplicaInfo info = GetReplicaInfo(b);
				FsVolumeReference @ref = info.GetVolume().ObtainReference();
				try
				{
					InputStream blockInStream = OpenAndSeek(info.GetBlockFile(), blkOffset);
					try
					{
						InputStream metaInStream = OpenAndSeek(info.GetMetaFile(), metaOffset);
						return new ReplicaInputStreams(blockInStream, metaInStream, @ref);
					}
					catch (IOException e)
					{
						IOUtils.Cleanup(null, blockInStream);
						throw;
					}
				}
				catch (IOException e)
				{
					IOUtils.Cleanup(null, @ref);
					throw;
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static FileInputStream OpenAndSeek(FilePath file, long offset)
		{
			RandomAccessFile raf = null;
			try
			{
				raf = new RandomAccessFile(file, "r");
				if (offset > 0)
				{
					raf.Seek(offset);
				}
				return new FileInputStream(raf.GetFD());
			}
			catch (IOException ioe)
			{
				IOUtils.Cleanup(null, raf);
				throw;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal static FilePath MoveBlockFiles(Block b, FilePath srcfile, FilePath destdir
			)
		{
			FilePath dstfile = new FilePath(destdir, b.GetBlockName());
			FilePath srcmeta = FsDatasetUtil.GetMetaFile(srcfile, b.GetGenerationStamp());
			FilePath dstmeta = FsDatasetUtil.GetMetaFile(dstfile, b.GetGenerationStamp());
			try
			{
				NativeIO.RenameTo(srcmeta, dstmeta);
			}
			catch (IOException e)
			{
				throw new IOException("Failed to move meta file for " + b + " from " + srcmeta + 
					" to " + dstmeta, e);
			}
			try
			{
				NativeIO.RenameTo(srcfile, dstfile);
			}
			catch (IOException e)
			{
				throw new IOException("Failed to move block file for " + b + " from " + srcfile +
					 " to " + dstfile.GetAbsolutePath(), e);
			}
			if (Log.IsDebugEnabled())
			{
				Log.Debug("addFinalizedBlock: Moved " + srcmeta + " to " + dstmeta + " and " + srcfile
					 + " to " + dstfile);
			}
			return dstfile;
		}

		/// <summary>Copy the block and meta files for the given block to the given destination.
		/// 	</summary>
		/// <returns>the new meta and block files.</returns>
		/// <exception cref="System.IO.IOException"/>
		internal static FilePath[] CopyBlockFiles(long blockId, long genStamp, FilePath srcMeta
			, FilePath srcFile, FilePath destRoot, bool calculateChecksum)
		{
			FilePath destDir = DatanodeUtil.IdToBlockDir(destRoot, blockId);
			FilePath dstFile = new FilePath(destDir, srcFile.GetName());
			FilePath dstMeta = FsDatasetUtil.GetMetaFile(dstFile, genStamp);
			return CopyBlockFiles(srcMeta, srcFile, dstMeta, dstFile, calculateChecksum);
		}

		/// <exception cref="System.IO.IOException"/>
		internal static FilePath[] CopyBlockFiles(FilePath srcMeta, FilePath srcFile, FilePath
			 dstMeta, FilePath dstFile, bool calculateChecksum)
		{
			if (calculateChecksum)
			{
				ComputeChecksum(srcMeta, dstMeta, srcFile);
			}
			else
			{
				try
				{
					Storage.NativeCopyFileUnbuffered(srcMeta, dstMeta, true);
				}
				catch (IOException e)
				{
					throw new IOException("Failed to copy " + srcMeta + " to " + dstMeta, e);
				}
			}
			try
			{
				Storage.NativeCopyFileUnbuffered(srcFile, dstFile, true);
			}
			catch (IOException e)
			{
				throw new IOException("Failed to copy " + srcFile + " to " + dstFile, e);
			}
			if (Log.IsDebugEnabled())
			{
				if (calculateChecksum)
				{
					Log.Debug("Copied " + srcMeta + " to " + dstMeta + " and calculated checksum");
				}
				else
				{
					Log.Debug("Copied " + srcFile + " to " + dstFile);
				}
			}
			return new FilePath[] { dstMeta, dstFile };
		}

		/// <summary>Move block files from one storage to another storage.</summary>
		/// <returns>Returns the Old replicaInfo</returns>
		/// <exception cref="System.IO.IOException"/>
		public override ReplicaInfo MoveBlockAcrossStorage(ExtendedBlock block, StorageType
			 targetStorageType)
		{
			ReplicaInfo replicaInfo = GetReplicaInfo(block);
			if (replicaInfo.GetState() != HdfsServerConstants.ReplicaState.Finalized)
			{
				throw new ReplicaNotFoundException(ReplicaNotFoundException.UnfinalizedReplica + 
					block);
			}
			if (replicaInfo.GetNumBytes() != block.GetNumBytes())
			{
				throw new IOException("Corrupted replica " + replicaInfo + " with a length of " +
					 replicaInfo.GetNumBytes() + " expected length is " + block.GetNumBytes());
			}
			if (replicaInfo.GetVolume().GetStorageType() == targetStorageType)
			{
				throw new ReplicaAlreadyExistsException("Replica " + replicaInfo + " already exists on storage "
					 + targetStorageType);
			}
			if (replicaInfo.IsOnTransientStorage())
			{
				// Block movement from RAM_DISK will be done by LazyPersist mechanism
				throw new IOException("Replica " + replicaInfo + " cannot be moved from storageType : "
					 + replicaInfo.GetVolume().GetStorageType());
			}
			using (FsVolumeReference volumeRef = volumes.GetNextVolume(targetStorageType, block
				.GetNumBytes()))
			{
				FilePath oldBlockFile = replicaInfo.GetBlockFile();
				FilePath oldMetaFile = replicaInfo.GetMetaFile();
				FsVolumeImpl targetVolume = (FsVolumeImpl)volumeRef.GetVolume();
				// Copy files to temp dir first
				FilePath[] blockFiles = CopyBlockFiles(block.GetBlockId(), block.GetGenerationStamp
					(), oldMetaFile, oldBlockFile, targetVolume.GetTmpDir(block.GetBlockPoolId()), replicaInfo
					.IsOnTransientStorage());
				ReplicaInfo newReplicaInfo = new ReplicaInPipeline(replicaInfo.GetBlockId(), replicaInfo
					.GetGenerationStamp(), targetVolume, blockFiles[0].GetParentFile(), 0);
				newReplicaInfo.SetNumBytes(blockFiles[1].Length());
				// Finalize the copied files
				newReplicaInfo = FinalizeReplica(block.GetBlockPoolId(), newReplicaInfo);
				RemoveOldReplica(replicaInfo, newReplicaInfo, oldBlockFile, oldMetaFile, oldBlockFile
					.Length(), oldMetaFile.Length(), block.GetBlockPoolId());
			}
			// Replace the old block if any to reschedule the scanning.
			return replicaInfo;
		}

		/// <summary>
		/// Compute and store the checksum for a block file that does not already have
		/// its checksum computed.
		/// </summary>
		/// <param name="srcMeta">
		/// source meta file, containing only the checksum header, not a
		/// calculated checksum
		/// </param>
		/// <param name="dstMeta">
		/// destination meta file, into which this method will write a
		/// full computed checksum
		/// </param>
		/// <param name="blockFile">block file for which the checksum will be computed</param>
		/// <exception cref="System.IO.IOException"/>
		private static void ComputeChecksum(FilePath srcMeta, FilePath dstMeta, FilePath 
			blockFile)
		{
			DataChecksum checksum = BlockMetadataHeader.ReadDataChecksum(srcMeta);
			byte[] data = new byte[1 << 16];
			byte[] crcs = new byte[checksum.GetChecksumSize(data.Length)];
			DataOutputStream metaOut = null;
			try
			{
				FilePath parentFile = dstMeta.GetParentFile();
				if (parentFile != null)
				{
					if (!parentFile.Mkdirs() && !parentFile.IsDirectory())
					{
						throw new IOException("Destination '" + parentFile + "' directory cannot be created"
							);
					}
				}
				metaOut = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(dstMeta
					), HdfsConstants.SmallBufferSize));
				BlockMetadataHeader.WriteHeader(metaOut, checksum);
				int offset = 0;
				using (InputStream dataIn = isNativeIOAvailable ? NativeIO.GetShareDeleteFileInputStream
					(blockFile) : new FileInputStream(blockFile))
				{
					for (int n; (n = dataIn.Read(data, offset, data.Length - offset)) != -1; )
					{
						if (n > 0)
						{
							n += offset;
							offset = n % checksum.GetBytesPerChecksum();
							int length = n - offset;
							if (length > 0)
							{
								checksum.CalculateChunkedSums(data, 0, length, crcs, 0);
								metaOut.Write(crcs, 0, checksum.GetChecksumSize(length));
								System.Array.Copy(data, length, data, 0, offset);
							}
						}
					}
				}
				// calculate and write the last crc
				checksum.CalculateChunkedSums(data, 0, offset, crcs, 0);
				metaOut.Write(crcs, 0, 4);
			}
			finally
			{
				IOUtils.Cleanup(Log, metaOut);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static void TruncateBlock(FilePath blockFile, FilePath metaFile, long oldlen
			, long newlen)
		{
			Log.Info("truncateBlock: blockFile=" + blockFile + ", metaFile=" + metaFile + ", oldlen="
				 + oldlen + ", newlen=" + newlen);
			if (newlen == oldlen)
			{
				return;
			}
			if (newlen > oldlen)
			{
				throw new IOException("Cannot truncate block to from oldlen (=" + oldlen + ") to newlen (="
					 + newlen + ")");
			}
			DataChecksum dcs = BlockMetadataHeader.ReadHeader(metaFile).GetChecksum();
			int checksumsize = dcs.GetChecksumSize();
			int bpc = dcs.GetBytesPerChecksum();
			long n = (newlen - 1) / bpc + 1;
			long newmetalen = BlockMetadataHeader.GetHeaderSize() + n * checksumsize;
			long lastchunkoffset = (n - 1) * bpc;
			int lastchunksize = (int)(newlen - lastchunkoffset);
			byte[] b = new byte[Math.Max(lastchunksize, checksumsize)];
			RandomAccessFile blockRAF = new RandomAccessFile(blockFile, "rw");
			try
			{
				//truncate blockFile 
				blockRAF.SetLength(newlen);
				//read last chunk
				blockRAF.Seek(lastchunkoffset);
				blockRAF.ReadFully(b, 0, lastchunksize);
			}
			finally
			{
				blockRAF.Close();
			}
			//compute checksum
			dcs.Update(b, 0, lastchunksize);
			dcs.WriteValue(b, 0, false);
			//update metaFile 
			RandomAccessFile metaRAF = new RandomAccessFile(metaFile, "rw");
			try
			{
				metaRAF.SetLength(newmetalen);
				metaRAF.Seek(newmetalen - checksumsize);
				metaRAF.Write(b, 0, checksumsize);
			}
			finally
			{
				metaRAF.Close();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override ReplicaHandler Append(ExtendedBlock b, long newGS, long expectedBlockLen
			)
		{
			lock (this)
			{
				// FsDatasetSpi
				// If the block was successfully finalized because all packets
				// were successfully processed at the Datanode but the ack for
				// some of the packets were not received by the client. The client 
				// re-opens the connection and retries sending those packets.
				// The other reason is that an "append" is occurring to this block.
				// check the validity of the parameter
				if (newGS < b.GetGenerationStamp())
				{
					throw new IOException("The new generation stamp " + newGS + " should be greater than the replica "
						 + b + "'s generation stamp");
				}
				ReplicaInfo replicaInfo = GetReplicaInfo(b);
				Log.Info("Appending to " + replicaInfo);
				if (replicaInfo.GetState() != HdfsServerConstants.ReplicaState.Finalized)
				{
					throw new ReplicaNotFoundException(ReplicaNotFoundException.UnfinalizedReplica + 
						b);
				}
				if (replicaInfo.GetNumBytes() != expectedBlockLen)
				{
					throw new IOException("Corrupted replica " + replicaInfo + " with a length of " +
						 replicaInfo.GetNumBytes() + " expected length is " + expectedBlockLen);
				}
				FsVolumeReference @ref = replicaInfo.GetVolume().ObtainReference();
				ReplicaBeingWritten replica = null;
				try
				{
					replica = Append(b.GetBlockPoolId(), (FinalizedReplica)replicaInfo, newGS, b.GetNumBytes
						());
				}
				catch (IOException e)
				{
					IOUtils.Cleanup(null, @ref);
					throw;
				}
				return new ReplicaHandler(replica, @ref);
			}
		}

		/// <summary>
		/// Append to a finalized replica
		/// Change a finalized replica to be a RBW replica and
		/// bump its generation stamp to be the newGS
		/// </summary>
		/// <param name="bpid">block pool Id</param>
		/// <param name="replicaInfo">a finalized replica</param>
		/// <param name="newGS">new generation stamp</param>
		/// <param name="estimateBlockLen">estimate generation stamp</param>
		/// <returns>a RBW replica</returns>
		/// <exception cref="System.IO.IOException">
		/// if moving the replica from finalized directory
		/// to rbw directory fails
		/// </exception>
		private ReplicaBeingWritten Append(string bpid, FinalizedReplica replicaInfo, long
			 newGS, long estimateBlockLen)
		{
			lock (this)
			{
				// If the block is cached, start uncaching it.
				cacheManager.UncacheBlock(bpid, replicaInfo.GetBlockId());
				// unlink the finalized replica
				replicaInfo.UnlinkBlock(1);
				// construct a RBW replica with the new GS
				FilePath blkfile = replicaInfo.GetBlockFile();
				FsVolumeImpl v = (FsVolumeImpl)replicaInfo.GetVolume();
				if (v.GetAvailable() < estimateBlockLen - replicaInfo.GetNumBytes())
				{
					throw new DiskChecker.DiskOutOfSpaceException("Insufficient space for appending to "
						 + replicaInfo);
				}
				FilePath newBlkFile = new FilePath(v.GetRbwDir(bpid), replicaInfo.GetBlockName());
				FilePath oldmeta = replicaInfo.GetMetaFile();
				ReplicaBeingWritten newReplicaInfo = new ReplicaBeingWritten(replicaInfo.GetBlockId
					(), replicaInfo.GetNumBytes(), newGS, v, newBlkFile.GetParentFile(), Sharpen.Thread
					.CurrentThread(), estimateBlockLen);
				FilePath newmeta = newReplicaInfo.GetMetaFile();
				// rename meta file to rbw directory
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Renaming " + oldmeta + " to " + newmeta);
				}
				try
				{
					NativeIO.RenameTo(oldmeta, newmeta);
				}
				catch (IOException e)
				{
					throw new IOException("Block " + replicaInfo + " reopen failed. " + " Unable to move meta file  "
						 + oldmeta + " to rbw dir " + newmeta, e);
				}
				// rename block file to rbw directory
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Renaming " + blkfile + " to " + newBlkFile + ", file length=" + blkfile
						.Length());
				}
				try
				{
					NativeIO.RenameTo(blkfile, newBlkFile);
				}
				catch (IOException e)
				{
					try
					{
						NativeIO.RenameTo(newmeta, oldmeta);
					}
					catch (IOException ex)
					{
						Log.Warn("Cannot move meta file " + newmeta + "back to the finalized directory " 
							+ oldmeta, ex);
					}
					throw new IOException("Block " + replicaInfo + " reopen failed. " + " Unable to move block file "
						 + blkfile + " to rbw dir " + newBlkFile, e);
				}
				// Replace finalized replica by a RBW replica in replicas map
				volumeMap.Add(bpid, newReplicaInfo);
				v.ReserveSpaceForRbw(estimateBlockLen - replicaInfo.GetNumBytes());
				return newReplicaInfo;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private ReplicaInfo RecoverCheck(ExtendedBlock b, long newGS, long expectedBlockLen
			)
		{
			ReplicaInfo replicaInfo = GetReplicaInfo(b.GetBlockPoolId(), b.GetBlockId());
			// check state
			if (replicaInfo.GetState() != HdfsServerConstants.ReplicaState.Finalized && replicaInfo
				.GetState() != HdfsServerConstants.ReplicaState.Rbw)
			{
				throw new ReplicaNotFoundException(ReplicaNotFoundException.UnfinalizedAndNonrbwReplica
					 + replicaInfo);
			}
			// check generation stamp
			long replicaGenerationStamp = replicaInfo.GetGenerationStamp();
			if (replicaGenerationStamp < b.GetGenerationStamp() || replicaGenerationStamp > newGS)
			{
				throw new ReplicaNotFoundException(ReplicaNotFoundException.UnexpectedGsReplica +
					 replicaGenerationStamp + ". Expected GS range is [" + b.GetGenerationStamp() + 
					", " + newGS + "].");
			}
			// stop the previous writer before check a replica's length
			long replicaLen = replicaInfo.GetNumBytes();
			if (replicaInfo.GetState() == HdfsServerConstants.ReplicaState.Rbw)
			{
				ReplicaBeingWritten rbw = (ReplicaBeingWritten)replicaInfo;
				// kill the previous writer
				rbw.StopWriter(datanode.GetDnConf().GetXceiverStopTimeout());
				rbw.SetWriter(Sharpen.Thread.CurrentThread());
				// check length: bytesRcvd, bytesOnDisk, and bytesAcked should be the same
				if (replicaLen != rbw.GetBytesOnDisk() || replicaLen != rbw.GetBytesAcked())
				{
					throw new ReplicaAlreadyExistsException("RBW replica " + replicaInfo + "bytesRcvd("
						 + rbw.GetNumBytes() + "), bytesOnDisk(" + rbw.GetBytesOnDisk() + "), and bytesAcked("
						 + rbw.GetBytesAcked() + ") are not the same.");
				}
			}
			// check block length
			if (replicaLen != expectedBlockLen)
			{
				throw new IOException("Corrupted replica " + replicaInfo + " with a length of " +
					 replicaLen + " expected length is " + expectedBlockLen);
			}
			return replicaInfo;
		}

		/// <exception cref="System.IO.IOException"/>
		public override ReplicaHandler RecoverAppend(ExtendedBlock b, long newGS, long expectedBlockLen
			)
		{
			lock (this)
			{
				// FsDatasetSpi
				Log.Info("Recover failed append to " + b);
				ReplicaInfo replicaInfo = RecoverCheck(b, newGS, expectedBlockLen);
				FsVolumeReference @ref = replicaInfo.GetVolume().ObtainReference();
				ReplicaBeingWritten replica;
				try
				{
					// change the replica's state/gs etc.
					if (replicaInfo.GetState() == HdfsServerConstants.ReplicaState.Finalized)
					{
						replica = Append(b.GetBlockPoolId(), (FinalizedReplica)replicaInfo, newGS, b.GetNumBytes
							());
					}
					else
					{
						//RBW
						BumpReplicaGS(replicaInfo, newGS);
						replica = (ReplicaBeingWritten)replicaInfo;
					}
				}
				catch (IOException e)
				{
					IOUtils.Cleanup(null, @ref);
					throw;
				}
				return new ReplicaHandler(replica, @ref);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override string RecoverClose(ExtendedBlock b, long newGS, long expectedBlockLen
			)
		{
			lock (this)
			{
				// FsDatasetSpi
				Log.Info("Recover failed close " + b);
				// check replica's state
				ReplicaInfo replicaInfo = RecoverCheck(b, newGS, expectedBlockLen);
				// bump the replica's GS
				BumpReplicaGS(replicaInfo, newGS);
				// finalize the replica if RBW
				if (replicaInfo.GetState() == HdfsServerConstants.ReplicaState.Rbw)
				{
					FinalizeReplica(b.GetBlockPoolId(), replicaInfo);
				}
				return replicaInfo.GetStorageUuid();
			}
		}

		/// <summary>Bump a replica's generation stamp to a new one.</summary>
		/// <remarks>
		/// Bump a replica's generation stamp to a new one.
		/// Its on-disk meta file name is renamed to be the new one too.
		/// </remarks>
		/// <param name="replicaInfo">a replica</param>
		/// <param name="newGS">new generation stamp</param>
		/// <exception cref="System.IO.IOException">if rename fails</exception>
		private void BumpReplicaGS(ReplicaInfo replicaInfo, long newGS)
		{
			long oldGS = replicaInfo.GetGenerationStamp();
			FilePath oldmeta = replicaInfo.GetMetaFile();
			replicaInfo.SetGenerationStamp(newGS);
			FilePath newmeta = replicaInfo.GetMetaFile();
			// rename meta file to new GS
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Renaming " + oldmeta + " to " + newmeta);
			}
			try
			{
				NativeIO.RenameTo(oldmeta, newmeta);
			}
			catch (IOException e)
			{
				replicaInfo.SetGenerationStamp(oldGS);
				// restore old GS
				throw new IOException("Block " + replicaInfo + " reopen failed. " + " Unable to move meta file  "
					 + oldmeta + " to " + newmeta, e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override ReplicaHandler CreateRbw(StorageType storageType, ExtendedBlock b
			, bool allowLazyPersist)
		{
			lock (this)
			{
				// FsDatasetSpi
				ReplicaInfo replicaInfo = volumeMap.Get(b.GetBlockPoolId(), b.GetBlockId());
				if (replicaInfo != null)
				{
					throw new ReplicaAlreadyExistsException("Block " + b + " already exists in state "
						 + replicaInfo.GetState() + " and thus cannot be created.");
				}
				// create a new block
				FsVolumeReference @ref;
				while (true)
				{
					try
					{
						if (allowLazyPersist)
						{
							// First try to place the block on a transient volume.
							@ref = volumes.GetNextTransientVolume(b.GetNumBytes());
							datanode.GetMetrics().IncrRamDiskBlocksWrite();
						}
						else
						{
							@ref = volumes.GetNextVolume(storageType, b.GetNumBytes());
						}
					}
					catch (DiskChecker.DiskOutOfSpaceException de)
					{
						if (allowLazyPersist)
						{
							datanode.GetMetrics().IncrRamDiskBlocksWriteFallback();
							allowLazyPersist = false;
							continue;
						}
						throw;
					}
					break;
				}
				FsVolumeImpl v = (FsVolumeImpl)@ref.GetVolume();
				// create an rbw file to hold block in the designated volume
				FilePath f;
				try
				{
					f = v.CreateRbwFile(b.GetBlockPoolId(), b.GetLocalBlock());
				}
				catch (IOException e)
				{
					IOUtils.Cleanup(null, @ref);
					throw;
				}
				ReplicaBeingWritten newReplicaInfo = new ReplicaBeingWritten(b.GetBlockId(), b.GetGenerationStamp
					(), v, f.GetParentFile(), b.GetNumBytes());
				volumeMap.Add(b.GetBlockPoolId(), newReplicaInfo);
				return new ReplicaHandler(newReplicaInfo, @ref);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override ReplicaHandler RecoverRbw(ExtendedBlock b, long newGS, long minBytesRcvd
			, long maxBytesRcvd)
		{
			lock (this)
			{
				// FsDatasetSpi
				Log.Info("Recover RBW replica " + b);
				ReplicaInfo replicaInfo = GetReplicaInfo(b.GetBlockPoolId(), b.GetBlockId());
				// check the replica's state
				if (replicaInfo.GetState() != HdfsServerConstants.ReplicaState.Rbw)
				{
					throw new ReplicaNotFoundException(ReplicaNotFoundException.NonRbwReplica + replicaInfo
						);
				}
				ReplicaBeingWritten rbw = (ReplicaBeingWritten)replicaInfo;
				Log.Info("Recovering " + rbw);
				// Stop the previous writer
				rbw.StopWriter(datanode.GetDnConf().GetXceiverStopTimeout());
				rbw.SetWriter(Sharpen.Thread.CurrentThread());
				// check generation stamp
				long replicaGenerationStamp = rbw.GetGenerationStamp();
				if (replicaGenerationStamp < b.GetGenerationStamp() || replicaGenerationStamp > newGS)
				{
					throw new ReplicaNotFoundException(ReplicaNotFoundException.UnexpectedGsReplica +
						 b + ". Expected GS range is [" + b.GetGenerationStamp() + ", " + newGS + "].");
				}
				// check replica length
				long bytesAcked = rbw.GetBytesAcked();
				long numBytes = rbw.GetNumBytes();
				if (bytesAcked < minBytesRcvd || numBytes > maxBytesRcvd)
				{
					throw new ReplicaNotFoundException("Unmatched length replica " + replicaInfo + ": BytesAcked = "
						 + bytesAcked + " BytesRcvd = " + numBytes + " are not in the range of [" + minBytesRcvd
						 + ", " + maxBytesRcvd + "].");
				}
				FsVolumeReference @ref = rbw.GetVolume().ObtainReference();
				try
				{
					// Truncate the potentially corrupt portion.
					// If the source was client and the last node in the pipeline was lost,
					// any corrupt data written after the acked length can go unnoticed.
					if (numBytes > bytesAcked)
					{
						FilePath replicafile = rbw.GetBlockFile();
						TruncateBlock(replicafile, rbw.GetMetaFile(), numBytes, bytesAcked);
						rbw.SetNumBytes(bytesAcked);
						rbw.SetLastChecksumAndDataLen(bytesAcked, null);
					}
					// bump the replica's generation stamp to newGS
					BumpReplicaGS(rbw, newGS);
				}
				catch (IOException e)
				{
					IOUtils.Cleanup(null, @ref);
					throw;
				}
				return new ReplicaHandler(rbw, @ref);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override ReplicaInPipelineInterface ConvertTemporaryToRbw(ExtendedBlock b)
		{
			lock (this)
			{
				// FsDatasetSpi
				long blockId = b.GetBlockId();
				long expectedGs = b.GetGenerationStamp();
				long visible = b.GetNumBytes();
				Log.Info("Convert " + b + " from Temporary to RBW, visible length=" + visible);
				ReplicaInPipeline temp;
				{
					// get replica
					ReplicaInfo r = volumeMap.Get(b.GetBlockPoolId(), blockId);
					if (r == null)
					{
						throw new ReplicaNotFoundException(ReplicaNotFoundException.NonExistentReplica + 
							b);
					}
					// check the replica's state
					if (r.GetState() != HdfsServerConstants.ReplicaState.Temporary)
					{
						throw new ReplicaAlreadyExistsException("r.getState() != ReplicaState.TEMPORARY, r="
							 + r);
					}
					temp = (ReplicaInPipeline)r;
				}
				// check generation stamp
				if (temp.GetGenerationStamp() != expectedGs)
				{
					throw new ReplicaAlreadyExistsException("temp.getGenerationStamp() != expectedGs = "
						 + expectedGs + ", temp=" + temp);
				}
				// TODO: check writer?
				// set writer to the current thread
				// temp.setWriter(Thread.currentThread());
				// check length
				long numBytes = temp.GetNumBytes();
				if (numBytes < visible)
				{
					throw new IOException(numBytes + " = numBytes < visible = " + visible + ", temp="
						 + temp);
				}
				// check volume
				FsVolumeImpl v = (FsVolumeImpl)temp.GetVolume();
				if (v == null)
				{
					throw new IOException("r.getVolume() = null, temp=" + temp);
				}
				// move block files to the rbw directory
				BlockPoolSlice bpslice = v.GetBlockPoolSlice(b.GetBlockPoolId());
				FilePath dest = MoveBlockFiles(b.GetLocalBlock(), temp.GetBlockFile(), bpslice.GetRbwDir
					());
				// create RBW
				ReplicaBeingWritten rbw = new ReplicaBeingWritten(blockId, numBytes, expectedGs, 
					v, dest.GetParentFile(), Sharpen.Thread.CurrentThread(), 0);
				rbw.SetBytesAcked(visible);
				// overwrite the RBW in the volume map
				volumeMap.Add(b.GetBlockPoolId(), rbw);
				return rbw;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override ReplicaHandler CreateTemporary(StorageType storageType, ExtendedBlock
			 b)
		{
			// FsDatasetSpi
			long startTimeMs = Time.MonotonicNow();
			long writerStopTimeoutMs = datanode.GetDnConf().GetXceiverStopTimeout();
			ReplicaInfo lastFoundReplicaInfo = null;
			do
			{
				lock (this)
				{
					ReplicaInfo currentReplicaInfo = volumeMap.Get(b.GetBlockPoolId(), b.GetBlockId()
						);
					if (currentReplicaInfo == lastFoundReplicaInfo)
					{
						if (lastFoundReplicaInfo != null)
						{
							Invalidate(b.GetBlockPoolId(), new Block[] { lastFoundReplicaInfo });
						}
						FsVolumeReference @ref = volumes.GetNextVolume(storageType, b.GetNumBytes());
						FsVolumeImpl v = (FsVolumeImpl)@ref.GetVolume();
						// create a temporary file to hold block in the designated volume
						FilePath f;
						try
						{
							f = v.CreateTmpFile(b.GetBlockPoolId(), b.GetLocalBlock());
						}
						catch (IOException e)
						{
							IOUtils.Cleanup(null, @ref);
							throw;
						}
						ReplicaInPipeline newReplicaInfo = new ReplicaInPipeline(b.GetBlockId(), b.GetGenerationStamp
							(), v, f.GetParentFile(), 0);
						volumeMap.Add(b.GetBlockPoolId(), newReplicaInfo);
						return new ReplicaHandler(newReplicaInfo, @ref);
					}
					else
					{
						if (!(currentReplicaInfo.GetGenerationStamp() < b.GetGenerationStamp() && currentReplicaInfo
							 is ReplicaInPipeline))
						{
							throw new ReplicaAlreadyExistsException("Block " + b + " already exists in state "
								 + currentReplicaInfo.GetState() + " and thus cannot be created.");
						}
						lastFoundReplicaInfo = currentReplicaInfo;
					}
				}
				// Hang too long, just bail out. This is not supposed to happen.
				long writerStopMs = Time.MonotonicNow() - startTimeMs;
				if (writerStopMs > writerStopTimeoutMs)
				{
					Log.Warn("Unable to stop existing writer for block " + b + " after " + writerStopMs
						 + " miniseconds.");
					throw new IOException("Unable to stop existing writer for block " + b + " after "
						 + writerStopMs + " miniseconds.");
				}
				// Stop the previous writer
				((ReplicaInPipeline)lastFoundReplicaInfo).StopWriter(writerStopTimeoutMs);
			}
			while (true);
		}

		/// <summary>
		/// Sets the offset in the meta file so that the
		/// last checksum will be overwritten.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public override void AdjustCrcChannelPosition(ExtendedBlock b, ReplicaOutputStreams
			 streams, int checksumSize)
		{
			// FsDatasetSpi
			FileOutputStream file = (FileOutputStream)streams.GetChecksumOut();
			FileChannel channel = file.GetChannel();
			long oldPos = channel.Position();
			long newPos = oldPos - checksumSize;
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Changing meta file offset of block " + b + " from " + oldPos + " to " 
					+ newPos);
			}
			channel.Position(newPos);
		}

		//
		// REMIND - mjc - eventually we should have a timeout system
		// in place to clean up block files left by abandoned clients.
		// We should have some timer in place, so that if a blockfile
		// is created but non-valid, and has been idle for >48 hours,
		// we can GC it safely.
		//
		/// <summary>Complete the block write!</summary>
		/// <exception cref="System.IO.IOException"/>
		public override void FinalizeBlock(ExtendedBlock b)
		{
			lock (this)
			{
				// FsDatasetSpi
				if (Sharpen.Thread.Interrupted())
				{
					// Don't allow data modifications from interrupted threads
					throw new IOException("Cannot finalize block from Interrupted Thread");
				}
				ReplicaInfo replicaInfo = GetReplicaInfo(b);
				if (replicaInfo.GetState() == HdfsServerConstants.ReplicaState.Finalized)
				{
					// this is legal, when recovery happens on a file that has
					// been opened for append but never modified
					return;
				}
				FinalizeReplica(b.GetBlockPoolId(), replicaInfo);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private FinalizedReplica FinalizeReplica(string bpid, ReplicaInfo replicaInfo)
		{
			lock (this)
			{
				FinalizedReplica newReplicaInfo = null;
				if (replicaInfo.GetState() == HdfsServerConstants.ReplicaState.Rur && ((ReplicaUnderRecovery
					)replicaInfo).GetOriginalReplica().GetState() == HdfsServerConstants.ReplicaState
					.Finalized)
				{
					newReplicaInfo = (FinalizedReplica)((ReplicaUnderRecovery)replicaInfo).GetOriginalReplica
						();
				}
				else
				{
					FsVolumeImpl v = (FsVolumeImpl)replicaInfo.GetVolume();
					FilePath f = replicaInfo.GetBlockFile();
					if (v == null)
					{
						throw new IOException("No volume for temporary file " + f + " for block " + replicaInfo
							);
					}
					FilePath dest = v.AddFinalizedBlock(bpid, replicaInfo, f, replicaInfo.GetBytesReserved
						());
					newReplicaInfo = new FinalizedReplica(replicaInfo, v, dest.GetParentFile());
					if (v.IsTransientStorage())
					{
						ramDiskReplicaTracker.AddReplica(bpid, replicaInfo.GetBlockId(), v);
						datanode.GetMetrics().AddRamDiskBytesWrite(replicaInfo.GetNumBytes());
					}
				}
				volumeMap.Add(bpid, newReplicaInfo);
				return newReplicaInfo;
			}
		}

		/// <summary>Remove the temporary block file (if any)</summary>
		/// <exception cref="System.IO.IOException"/>
		public override void UnfinalizeBlock(ExtendedBlock b)
		{
			lock (this)
			{
				// FsDatasetSpi
				ReplicaInfo replicaInfo = volumeMap.Get(b.GetBlockPoolId(), b.GetLocalBlock());
				if (replicaInfo != null && replicaInfo.GetState() == HdfsServerConstants.ReplicaState
					.Temporary)
				{
					// remove from volumeMap
					volumeMap.Remove(b.GetBlockPoolId(), b.GetLocalBlock());
					// delete the on-disk temp file
					if (DelBlockFromDisk(replicaInfo.GetBlockFile(), replicaInfo.GetMetaFile(), b.GetLocalBlock
						()))
					{
						Log.Warn("Block " + b + " unfinalized and removed. ");
					}
					if (replicaInfo.GetVolume().IsTransientStorage())
					{
						ramDiskReplicaTracker.DiscardReplica(b.GetBlockPoolId(), b.GetBlockId(), true);
					}
				}
			}
		}

		/// <summary>Remove a block from disk</summary>
		/// <param name="blockFile">block file</param>
		/// <param name="metaFile">block meta file</param>
		/// <param name="b">a block</param>
		/// <returns>true if on-disk files are deleted; false otherwise</returns>
		private bool DelBlockFromDisk(FilePath blockFile, FilePath metaFile, Block b)
		{
			if (blockFile == null)
			{
				Log.Warn("No file exists for block: " + b);
				return true;
			}
			if (!blockFile.Delete())
			{
				Log.Warn("Not able to delete the block file: " + blockFile);
				return false;
			}
			else
			{
				// remove the meta file
				if (metaFile != null && !metaFile.Delete())
				{
					Log.Warn("Not able to delete the meta block file: " + metaFile);
					return false;
				}
			}
			return true;
		}

		public override IDictionary<DatanodeStorage, BlockListAsLongs> GetBlockReports(string
			 bpid)
		{
			IDictionary<DatanodeStorage, BlockListAsLongs> blockReportsMap = new Dictionary<DatanodeStorage
				, BlockListAsLongs>();
			IDictionary<string, BlockListAsLongs.Builder> builders = new Dictionary<string, BlockListAsLongs.Builder
				>();
			IList<FsVolumeImpl> curVolumes = GetVolumes();
			foreach (FsVolumeSpi v in curVolumes)
			{
				builders[v.GetStorageID()] = BlockListAsLongs.Builder();
			}
			lock (this)
			{
				foreach (ReplicaInfo b in volumeMap.Replicas(bpid))
				{
					switch (b.GetState())
					{
						case HdfsServerConstants.ReplicaState.Finalized:
						case HdfsServerConstants.ReplicaState.Rbw:
						case HdfsServerConstants.ReplicaState.Rwr:
						{
							builders[b.GetVolume().GetStorageID()].Add(b);
							break;
						}

						case HdfsServerConstants.ReplicaState.Rur:
						{
							ReplicaUnderRecovery rur = (ReplicaUnderRecovery)b;
							builders[rur.GetVolume().GetStorageID()].Add(rur.GetOriginalReplica());
							break;
						}

						case HdfsServerConstants.ReplicaState.Temporary:
						{
							break;
						}

						default:
						{
							System.Diagnostics.Debug.Assert(false, "Illegal ReplicaInfo state.");
							break;
						}
					}
				}
			}
			foreach (FsVolumeImpl v_1 in curVolumes)
			{
				blockReportsMap[v_1.ToDatanodeStorage()] = builders[v_1.GetStorageID()].Build();
			}
			return blockReportsMap;
		}

		public override IList<long> GetCacheReport(string bpid)
		{
			// FsDatasetSpi
			return cacheManager.GetCachedBlocks(bpid);
		}

		/// <summary>Get the list of finalized blocks from in-memory blockmap for a block pool.
		/// 	</summary>
		public override IList<FinalizedReplica> GetFinalizedBlocks(string bpid)
		{
			lock (this)
			{
				AList<FinalizedReplica> finalized = new AList<FinalizedReplica>(volumeMap.Size(bpid
					));
				foreach (ReplicaInfo b in volumeMap.Replicas(bpid))
				{
					if (b.GetState() == HdfsServerConstants.ReplicaState.Finalized)
					{
						finalized.AddItem(new FinalizedReplica((FinalizedReplica)b));
					}
				}
				return finalized;
			}
		}

		/// <summary>Get the list of finalized blocks from in-memory blockmap for a block pool.
		/// 	</summary>
		public override IList<FinalizedReplica> GetFinalizedBlocksOnPersistentStorage(string
			 bpid)
		{
			lock (this)
			{
				AList<FinalizedReplica> finalized = new AList<FinalizedReplica>(volumeMap.Size(bpid
					));
				foreach (ReplicaInfo b in volumeMap.Replicas(bpid))
				{
					if (!b.GetVolume().IsTransientStorage() && b.GetState() == HdfsServerConstants.ReplicaState
						.Finalized)
					{
						finalized.AddItem(new FinalizedReplica((FinalizedReplica)b));
					}
				}
				return finalized;
			}
		}

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
		public override void CheckBlock(ExtendedBlock b, long minLength, HdfsServerConstants.ReplicaState
			 state)
		{
			ReplicaInfo replicaInfo = volumeMap.Get(b.GetBlockPoolId(), b.GetLocalBlock());
			if (replicaInfo == null)
			{
				throw new ReplicaNotFoundException(b);
			}
			if (replicaInfo.GetState() != state)
			{
				throw new UnexpectedReplicaStateException(b, state);
			}
			if (!replicaInfo.GetBlockFile().Exists())
			{
				throw new FileNotFoundException(replicaInfo.GetBlockFile().GetPath());
			}
			long onDiskLength = GetLength(b);
			if (onDiskLength < minLength)
			{
				throw new EOFException(b + "'s on-disk length " + onDiskLength + " is shorter than minLength "
					 + minLength);
			}
		}

		/// <summary>Check whether the given block is a valid one.</summary>
		/// <remarks>
		/// Check whether the given block is a valid one.
		/// valid means finalized
		/// </remarks>
		public override bool IsValidBlock(ExtendedBlock b)
		{
			// FsDatasetSpi
			return IsValid(b, HdfsServerConstants.ReplicaState.Finalized);
		}

		/// <summary>Check whether the given block is a valid RBW.</summary>
		public override bool IsValidRbw(ExtendedBlock b)
		{
			// {@link FsDatasetSpi}
			return IsValid(b, HdfsServerConstants.ReplicaState.Rbw);
		}

		/// <summary>Does the block exist and have the given state?</summary>
		private bool IsValid(ExtendedBlock b, HdfsServerConstants.ReplicaState state)
		{
			try
			{
				CheckBlock(b, 0, state);
			}
			catch (IOException)
			{
				return false;
			}
			return true;
		}

		/// <summary>Find the file corresponding to the block and return it if it exists.</summary>
		internal virtual FilePath ValidateBlockFile(string bpid, long blockId)
		{
			//Should we check for metadata file too?
			FilePath f;
			lock (this)
			{
				f = GetFile(bpid, blockId, false);
			}
			if (f != null)
			{
				if (f.Exists())
				{
					return f;
				}
				// if file is not null, but doesn't exist - possibly disk failed
				datanode.CheckDiskErrorAsync();
			}
			if (Log.IsDebugEnabled())
			{
				Log.Debug("blockId=" + blockId + ", f=" + f);
			}
			return null;
		}

		/// <summary>Check the files of a replica.</summary>
		/// <exception cref="System.IO.IOException"/>
		internal static void CheckReplicaFiles(ReplicaInfo r)
		{
			//check replica's file
			FilePath f = r.GetBlockFile();
			if (!f.Exists())
			{
				throw new FileNotFoundException("File " + f + " not found, r=" + r);
			}
			if (r.GetBytesOnDisk() != f.Length())
			{
				throw new IOException("File length mismatched.  The length of " + f + " is " + f.
					Length() + " but r=" + r);
			}
			//check replica's meta file
			FilePath metafile = FsDatasetUtil.GetMetaFile(f, r.GetGenerationStamp());
			if (!metafile.Exists())
			{
				throw new IOException("Metafile " + metafile + " does not exist, r=" + r);
			}
			if (metafile.Length() == 0)
			{
				throw new IOException("Metafile " + metafile + " is empty, r=" + r);
			}
		}

		/// <summary>We're informed that a block is no longer valid.</summary>
		/// <remarks>
		/// We're informed that a block is no longer valid.  We
		/// could lazily garbage-collect the block, but why bother?
		/// just get rid of it.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public override void Invalidate(string bpid, Block[] invalidBlks)
		{
			// FsDatasetSpi
			IList<string> errors = new AList<string>();
			for (int i = 0; i < invalidBlks.Length; i++)
			{
				FilePath f;
				FsVolumeImpl v;
				lock (this)
				{
					ReplicaInfo info = volumeMap.Get(bpid, invalidBlks[i]);
					if (info == null)
					{
						// It is okay if the block is not found -- it may be deleted earlier.
						Log.Info("Failed to delete replica " + invalidBlks[i] + ": ReplicaInfo not found."
							);
						continue;
					}
					if (info.GetGenerationStamp() != invalidBlks[i].GetGenerationStamp())
					{
						errors.AddItem("Failed to delete replica " + invalidBlks[i] + ": GenerationStamp not matched, info="
							 + info);
						continue;
					}
					f = info.GetBlockFile();
					v = (FsVolumeImpl)info.GetVolume();
					if (v == null)
					{
						errors.AddItem("Failed to delete replica " + invalidBlks[i] + ". No volume for this replica, file="
							 + f);
						continue;
					}
					FilePath parent = f.GetParentFile();
					if (parent == null)
					{
						errors.AddItem("Failed to delete replica " + invalidBlks[i] + ". Parent not found for file "
							 + f);
						continue;
					}
					ReplicaInfo removing = volumeMap.Remove(bpid, invalidBlks[i]);
					AddDeletingBlock(bpid, removing.GetBlockId());
					if (Log.IsDebugEnabled())
					{
						Log.Debug("Block file " + removing.GetBlockFile().GetName() + " is to be deleted"
							);
					}
				}
				if (v.IsTransientStorage())
				{
					RamDiskReplicaTracker.RamDiskReplica replicaInfo = ramDiskReplicaTracker.GetReplica
						(bpid, invalidBlks[i].GetBlockId());
					if (replicaInfo != null)
					{
						if (!replicaInfo.GetIsPersisted())
						{
							datanode.GetMetrics().IncrRamDiskBlocksDeletedBeforeLazyPersisted();
						}
						ramDiskReplicaTracker.DiscardReplica(replicaInfo.GetBlockPoolId(), replicaInfo.GetBlockId
							(), true);
					}
				}
				// If a DFSClient has the replica in its cache of short-circuit file
				// descriptors (and the client is using ShortCircuitShm), invalidate it.
				datanode.GetShortCircuitRegistry().ProcessBlockInvalidation(new ExtendedBlockId(invalidBlks
					[i].GetBlockId(), bpid));
				// If the block is cached, start uncaching it.
				cacheManager.UncacheBlock(bpid, invalidBlks[i].GetBlockId());
				// Delete the block asynchronously to make sure we can do it fast enough.
				// It's ok to unlink the block file before the uncache operation
				// finishes.
				try
				{
					asyncDiskService.DeleteAsync(v.ObtainReference(), f, FsDatasetUtil.GetMetaFile(f, 
						invalidBlks[i].GetGenerationStamp()), new ExtendedBlock(bpid, invalidBlks[i]), dataStorage
						.GetTrashDirectoryForBlockFile(bpid, f));
				}
				catch (ClosedChannelException)
				{
					Log.Warn("Volume " + v + " is closed, ignore the deletion task for " + "block " +
						 invalidBlks[i]);
				}
			}
			if (!errors.IsEmpty())
			{
				StringBuilder b = new StringBuilder("Failed to delete ").Append(errors.Count).Append
					(" (out of ").Append(invalidBlks.Length).Append(") replica(s):");
				for (int i_1 = 0; i_1 < errors.Count; i_1++)
				{
					b.Append("\n").Append(i_1).Append(") ").Append(errors[i_1]);
				}
				throw new IOException(b.ToString());
			}
		}

		/// <summary>Invalidate a block but does not delete the actual on-disk block file.</summary>
		/// <remarks>
		/// Invalidate a block but does not delete the actual on-disk block file.
		/// It should only be used when deactivating disks.
		/// </remarks>
		/// <param name="bpid">the block pool ID.</param>
		/// <param name="block">The block to be invalidated.</param>
		public virtual void Invalidate(string bpid, ReplicaInfo block)
		{
			// If a DFSClient has the replica in its cache of short-circuit file
			// descriptors (and the client is using ShortCircuitShm), invalidate it.
			datanode.GetShortCircuitRegistry().ProcessBlockInvalidation(new ExtendedBlockId(block
				.GetBlockId(), bpid));
			// If the block is cached, start uncaching it.
			cacheManager.UncacheBlock(bpid, block.GetBlockId());
			datanode.NotifyNamenodeDeletedBlock(new ExtendedBlock(bpid, block), block.GetStorageUuid
				());
		}

		/// <summary>
		/// Asynchronously attempts to cache a single block via
		/// <see cref="FsDatasetCache"/>
		/// .
		/// </summary>
		private void CacheBlock(string bpid, long blockId)
		{
			FsVolumeImpl volume;
			string blockFileName;
			long length;
			long genstamp;
			Executor volumeExecutor;
			lock (this)
			{
				ReplicaInfo info = volumeMap.Get(bpid, blockId);
				bool success = false;
				try
				{
					if (info == null)
					{
						Log.Warn("Failed to cache block with id " + blockId + ", pool " + bpid + ": ReplicaInfo not found."
							);
						return;
					}
					if (info.GetState() != HdfsServerConstants.ReplicaState.Finalized)
					{
						Log.Warn("Failed to cache block with id " + blockId + ", pool " + bpid + ": replica is not finalized; it is in state "
							 + info.GetState());
						return;
					}
					try
					{
						volume = (FsVolumeImpl)info.GetVolume();
						if (volume == null)
						{
							Log.Warn("Failed to cache block with id " + blockId + ", pool " + bpid + ": volume not found."
								);
							return;
						}
					}
					catch (InvalidCastException)
					{
						Log.Warn("Failed to cache block with id " + blockId + ": volume was not an instance of FsVolumeImpl."
							);
						return;
					}
					if (volume.IsTransientStorage())
					{
						Log.Warn("Caching not supported on block with id " + blockId + " since the volume is backed by RAM."
							);
						return;
					}
					success = true;
				}
				finally
				{
					if (!success)
					{
						cacheManager.numBlocksFailedToCache.IncrementAndGet();
					}
				}
				blockFileName = info.GetBlockFile().GetAbsolutePath();
				length = info.GetVisibleLength();
				genstamp = info.GetGenerationStamp();
				volumeExecutor = volume.GetCacheExecutor();
			}
			cacheManager.CacheBlock(blockId, bpid, blockFileName, length, genstamp, volumeExecutor
				);
		}

		public override void Cache(string bpid, long[] blockIds)
		{
			// FsDatasetSpi
			for (int i = 0; i < blockIds.Length; i++)
			{
				CacheBlock(bpid, blockIds[i]);
			}
		}

		public override void Uncache(string bpid, long[] blockIds)
		{
			// FsDatasetSpi
			for (int i = 0; i < blockIds.Length; i++)
			{
				cacheManager.UncacheBlock(bpid, blockIds[i]);
			}
		}

		public override bool IsCached(string bpid, long blockId)
		{
			return cacheManager.IsCached(bpid, blockId);
		}

		public override bool Contains(ExtendedBlock block)
		{
			lock (this)
			{
				// FsDatasetSpi
				long blockId = block.GetLocalBlock().GetBlockId();
				return GetFile(block.GetBlockPoolId(), blockId, false) != null;
			}
		}

		/// <summary>Turn the block identifier into a filename</summary>
		/// <param name="bpid">Block pool Id</param>
		/// <param name="blockId">a block's id</param>
		/// <returns>on disk data file path; null if the replica does not exist</returns>
		internal virtual FilePath GetFile(string bpid, long blockId, bool touch)
		{
			ReplicaInfo info = volumeMap.Get(bpid, blockId);
			if (info != null)
			{
				if (touch && info.GetVolume().IsTransientStorage())
				{
					ramDiskReplicaTracker.Touch(bpid, blockId);
					datanode.GetMetrics().IncrRamDiskBlocksReadHits();
				}
				return info.GetBlockFile();
			}
			return null;
		}

		/// <summary>
		/// check if a data directory is healthy
		/// if some volumes failed - the caller must emove all the blocks that belong
		/// to these failed volumes.
		/// </summary>
		/// <returns>the failed volumes. Returns null if no volume failed.</returns>
		public override ICollection<FilePath> CheckDataDir()
		{
			// FsDatasetSpi
			return volumes.CheckDirs();
		}

		public override string ToString()
		{
			// FsDatasetSpi
			return "FSDataset{dirpath='" + volumes + "'}";
		}

		private ObjectName mbeanName;

		/// <summary>
		/// Register the FSDataset MBean using the name
		/// "hadoop:service=DataNode,name=FSDatasetState-<datanodeUuid>"
		/// </summary>
		internal virtual void RegisterMBean(string datanodeUuid)
		{
			// We wrap to bypass standard mbean naming convetion.
			// This wraping can be removed in java 6 as it is more flexible in 
			// package naming for mbeans and their impl.
			try
			{
				StandardMBean bean = new StandardMBean(this, typeof(FSDatasetMBean));
				mbeanName = MBeans.Register("DataNode", "FSDatasetState-" + datanodeUuid, bean);
			}
			catch (NotCompliantMBeanException e)
			{
				Log.Warn("Error registering FSDatasetState MBean", e);
			}
			Log.Info("Registered FSDatasetState MBean");
		}

		public override void Shutdown()
		{
			// FsDatasetSpi
			fsRunning = false;
			((FsDatasetImpl.LazyWriter)lazyWriter.GetRunnable()).Stop();
			lazyWriter.Interrupt();
			if (mbeanName != null)
			{
				MBeans.Unregister(mbeanName);
			}
			if (asyncDiskService != null)
			{
				asyncDiskService.Shutdown();
			}
			if (asyncLazyPersistService != null)
			{
				asyncLazyPersistService.Shutdown();
			}
			if (volumes != null)
			{
				volumes.Shutdown();
			}
			try
			{
				lazyWriter.Join();
			}
			catch (Exception)
			{
				Log.Warn("FsDatasetImpl.shutdown ignoring InterruptedException " + "from LazyWriter.join"
					);
			}
		}

		public virtual string GetStorageInfo()
		{
			// FSDatasetMBean
			return ToString();
		}

		/// <summary>
		/// Reconcile the difference between blocks on the disk and blocks in
		/// volumeMap
		/// Check the given block for inconsistencies.
		/// </summary>
		/// <remarks>
		/// Reconcile the difference between blocks on the disk and blocks in
		/// volumeMap
		/// Check the given block for inconsistencies. Look at the
		/// current state of the block and reconcile the differences as follows:
		/// <ul>
		/// <li>If the block file is missing, delete the block from volumeMap</li>
		/// <li>If the block file exists and the block is missing in volumeMap,
		/// add the block to volumeMap <li>
		/// <li>If generation stamp does not match, then update the block with right
		/// generation stamp</li>
		/// <li>If the block length in memory does not match the actual block file length
		/// then mark the block as corrupt and update the block length in memory</li>
		/// <li>If the file in
		/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Datanode.ReplicaInfo"/>
		/// does not match the file on
		/// the disk, update
		/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Datanode.ReplicaInfo"/>
		/// with the correct file</li>
		/// </ul>
		/// </remarks>
		/// <param name="blockId">Block that differs</param>
		/// <param name="diskFile">Block file on the disk</param>
		/// <param name="diskMetaFile">Metadata file from on the disk</param>
		/// <param name="vol">Volume of the block file</param>
		/// <exception cref="System.IO.IOException"/>
		public override void CheckAndUpdate(string bpid, long blockId, FilePath diskFile, 
			FilePath diskMetaFile, FsVolumeSpi vol)
		{
			Block corruptBlock = null;
			ReplicaInfo memBlockInfo;
			lock (this)
			{
				memBlockInfo = volumeMap.Get(bpid, blockId);
				if (memBlockInfo != null && memBlockInfo.GetState() != HdfsServerConstants.ReplicaState
					.Finalized)
				{
					// Block is not finalized - ignore the difference
					return;
				}
				long diskGS = diskMetaFile != null && diskMetaFile.Exists() ? Block.GetGenerationStamp
					(diskMetaFile.GetName()) : GenerationStamp.GrandfatherGenerationStamp;
				if (diskFile == null || !diskFile.Exists())
				{
					if (memBlockInfo == null)
					{
						// Block file does not exist and block does not exist in memory
						// If metadata file exists then delete it
						if (diskMetaFile != null && diskMetaFile.Exists() && diskMetaFile.Delete())
						{
							Log.Warn("Deleted a metadata file without a block " + diskMetaFile.GetAbsolutePath
								());
						}
						return;
					}
					if (!memBlockInfo.GetBlockFile().Exists())
					{
						// Block is in memory and not on the disk
						// Remove the block from volumeMap
						volumeMap.Remove(bpid, blockId);
						if (vol.IsTransientStorage())
						{
							ramDiskReplicaTracker.DiscardReplica(bpid, blockId, true);
						}
						Log.Warn("Removed block " + blockId + " from memory with missing block file on the disk"
							);
						// Finally remove the metadata file
						if (diskMetaFile != null && diskMetaFile.Exists() && diskMetaFile.Delete())
						{
							Log.Warn("Deleted a metadata file for the deleted block " + diskMetaFile.GetAbsolutePath
								());
						}
					}
					return;
				}
				/*
				* Block file exists on the disk
				*/
				if (memBlockInfo == null)
				{
					// Block is missing in memory - add the block to volumeMap
					ReplicaInfo diskBlockInfo = new FinalizedReplica(blockId, diskFile.Length(), diskGS
						, vol, diskFile.GetParentFile());
					volumeMap.Add(bpid, diskBlockInfo);
					if (vol.IsTransientStorage())
					{
						ramDiskReplicaTracker.AddReplica(bpid, blockId, (FsVolumeImpl)vol);
					}
					Log.Warn("Added missing block to memory " + diskBlockInfo);
					return;
				}
				/*
				* Block exists in volumeMap and the block file exists on the disk
				*/
				// Compare block files
				FilePath memFile = memBlockInfo.GetBlockFile();
				if (memFile.Exists())
				{
					if (memFile.CompareTo(diskFile) != 0)
					{
						if (diskMetaFile.Exists())
						{
							if (memBlockInfo.GetMetaFile().Exists())
							{
								// We have two sets of block+meta files. Decide which one to
								// keep.
								ReplicaInfo diskBlockInfo = new FinalizedReplica(blockId, diskFile.Length(), diskGS
									, vol, diskFile.GetParentFile());
								((FsVolumeImpl)vol).GetBlockPoolSlice(bpid).ResolveDuplicateReplicas(memBlockInfo
									, diskBlockInfo, volumeMap);
							}
						}
						else
						{
							if (!diskFile.Delete())
							{
								Log.Warn("Failed to delete " + diskFile + ". Will retry on next scan");
							}
						}
					}
				}
				else
				{
					// Block refers to a block file that does not exist.
					// Update the block with the file found on the disk. Since the block
					// file and metadata file are found as a pair on the disk, update
					// the block based on the metadata file found on the disk
					Log.Warn("Block file in volumeMap " + memFile.GetAbsolutePath() + " does not exist. Updating it to the file found during scan "
						 + diskFile.GetAbsolutePath());
					memBlockInfo.SetDir(diskFile.GetParentFile());
					memFile = diskFile;
					Log.Warn("Updating generation stamp for block " + blockId + " from " + memBlockInfo
						.GetGenerationStamp() + " to " + diskGS);
					memBlockInfo.SetGenerationStamp(diskGS);
				}
				// Compare generation stamp
				if (memBlockInfo.GetGenerationStamp() != diskGS)
				{
					FilePath memMetaFile = FsDatasetUtil.GetMetaFile(diskFile, memBlockInfo.GetGenerationStamp
						());
					if (memMetaFile.Exists())
					{
						if (memMetaFile.CompareTo(diskMetaFile) != 0)
						{
							Log.Warn("Metadata file in memory " + memMetaFile.GetAbsolutePath() + " does not match file found by scan "
								 + (diskMetaFile == null ? null : diskMetaFile.GetAbsolutePath()));
						}
					}
					else
					{
						// Metadata file corresponding to block in memory is missing
						// If metadata file found during the scan is on the same directory
						// as the block file, then use the generation stamp from it
						long gs = diskMetaFile != null && diskMetaFile.Exists() && diskMetaFile.GetParent
							().Equals(memFile.GetParent()) ? diskGS : GenerationStamp.GrandfatherGenerationStamp;
						Log.Warn("Updating generation stamp for block " + blockId + " from " + memBlockInfo
							.GetGenerationStamp() + " to " + gs);
						memBlockInfo.SetGenerationStamp(gs);
					}
				}
				// Compare block size
				if (memBlockInfo.GetNumBytes() != memFile.Length())
				{
					// Update the length based on the block file
					corruptBlock = new Block(memBlockInfo);
					Log.Warn("Updating size of block " + blockId + " from " + memBlockInfo.GetNumBytes
						() + " to " + memFile.Length());
					memBlockInfo.SetNumBytes(memFile.Length());
				}
			}
			// Send corrupt block report outside the lock
			if (corruptBlock != null)
			{
				Log.Warn("Reporting the block " + corruptBlock + " as corrupt due to length mismatch"
					);
				try
				{
					datanode.ReportBadBlocks(new ExtendedBlock(bpid, corruptBlock));
				}
				catch (IOException e)
				{
					Log.Warn("Failed to repot bad block " + corruptBlock, e);
				}
			}
		}

		[System.ObsoleteAttribute(@"use FetchReplicaInfo(string, long) instead.")]
		public override Replica GetReplica(string bpid, long blockId)
		{
			// FsDatasetSpi
			return volumeMap.Get(bpid, blockId);
		}

		public override string GetReplicaString(string bpid, long blockId)
		{
			lock (this)
			{
				Replica r = volumeMap.Get(bpid, blockId);
				return r == null ? "null" : r.ToString();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override ReplicaRecoveryInfo InitReplicaRecovery(BlockRecoveryCommand.RecoveringBlock
			 rBlock)
		{
			lock (this)
			{
				// FsDatasetSpi
				return InitReplicaRecovery(rBlock.GetBlock().GetBlockPoolId(), volumeMap, rBlock.
					GetBlock().GetLocalBlock(), rBlock.GetNewGenerationStamp(), datanode.GetDnConf()
					.GetXceiverStopTimeout());
			}
		}

		/// <summary>
		/// static version of
		/// <see cref="InitReplicaRecovery(Org.Apache.Hadoop.Hdfs.Server.Protocol.BlockRecoveryCommand.RecoveringBlock)
		/// 	"/>
		/// .
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		internal static ReplicaRecoveryInfo InitReplicaRecovery(string bpid, ReplicaMap map
			, Block block, long recoveryId, long xceiverStopTimeout)
		{
			ReplicaInfo replica = map.Get(bpid, block.GetBlockId());
			Log.Info("initReplicaRecovery: " + block + ", recoveryId=" + recoveryId + ", replica="
				 + replica);
			//check replica
			if (replica == null)
			{
				return null;
			}
			//stop writer if there is any
			if (replica is ReplicaInPipeline)
			{
				ReplicaInPipeline rip = (ReplicaInPipeline)replica;
				rip.StopWriter(xceiverStopTimeout);
				//check replica bytes on disk.
				if (rip.GetBytesOnDisk() < rip.GetVisibleLength())
				{
					throw new IOException("THIS IS NOT SUPPOSED TO HAPPEN:" + " getBytesOnDisk() < getVisibleLength(), rip="
						 + rip);
				}
				//check the replica's files
				CheckReplicaFiles(rip);
			}
			//check generation stamp
			if (replica.GetGenerationStamp() < block.GetGenerationStamp())
			{
				throw new IOException("replica.getGenerationStamp() < block.getGenerationStamp(), block="
					 + block + ", replica=" + replica);
			}
			//check recovery id
			if (replica.GetGenerationStamp() >= recoveryId)
			{
				throw new IOException("THIS IS NOT SUPPOSED TO HAPPEN:" + " replica.getGenerationStamp() >= recoveryId = "
					 + recoveryId + ", block=" + block + ", replica=" + replica);
			}
			//check RUR
			ReplicaUnderRecovery rur;
			if (replica.GetState() == HdfsServerConstants.ReplicaState.Rur)
			{
				rur = (ReplicaUnderRecovery)replica;
				if (rur.GetRecoveryID() >= recoveryId)
				{
					throw new RecoveryInProgressException("rur.getRecoveryID() >= recoveryId = " + recoveryId
						 + ", block=" + block + ", rur=" + rur);
				}
				long oldRecoveryID = rur.GetRecoveryID();
				rur.SetRecoveryID(recoveryId);
				Log.Info("initReplicaRecovery: update recovery id for " + block + " from " + oldRecoveryID
					 + " to " + recoveryId);
			}
			else
			{
				rur = new ReplicaUnderRecovery(replica, recoveryId);
				map.Add(bpid, rur);
				Log.Info("initReplicaRecovery: changing replica state for " + block + " from " + 
					replica.GetState() + " to " + rur.GetState());
			}
			return rur.CreateInfo();
		}

		/// <exception cref="System.IO.IOException"/>
		public override string UpdateReplicaUnderRecovery(ExtendedBlock oldBlock, long recoveryId
			, long newBlockId, long newlength)
		{
			lock (this)
			{
				// FsDatasetSpi
				//get replica
				string bpid = oldBlock.GetBlockPoolId();
				ReplicaInfo replica = volumeMap.Get(bpid, oldBlock.GetBlockId());
				Log.Info("updateReplica: " + oldBlock + ", recoveryId=" + recoveryId + ", length="
					 + newlength + ", replica=" + replica);
				//check replica
				if (replica == null)
				{
					throw new ReplicaNotFoundException(oldBlock);
				}
				//check replica state
				if (replica.GetState() != HdfsServerConstants.ReplicaState.Rur)
				{
					throw new IOException("replica.getState() != " + HdfsServerConstants.ReplicaState
						.Rur + ", replica=" + replica);
				}
				//check replica's byte on disk
				if (replica.GetBytesOnDisk() != oldBlock.GetNumBytes())
				{
					throw new IOException("THIS IS NOT SUPPOSED TO HAPPEN:" + " replica.getBytesOnDisk() != block.getNumBytes(), block="
						 + oldBlock + ", replica=" + replica);
				}
				//check replica files before update
				CheckReplicaFiles(replica);
				//update replica
				FinalizedReplica finalized = UpdateReplicaUnderRecovery(oldBlock.GetBlockPoolId()
					, (ReplicaUnderRecovery)replica, recoveryId, newBlockId, newlength);
				bool copyTruncate = newBlockId != oldBlock.GetBlockId();
				if (!copyTruncate)
				{
					System.Diagnostics.Debug.Assert(finalized.GetBlockId() == oldBlock.GetBlockId() &&
						 finalized.GetGenerationStamp() == recoveryId && finalized.GetNumBytes() == newlength
						, "Replica information mismatched: oldBlock=" + oldBlock + ", recoveryId=" + recoveryId
						 + ", newlength=" + newlength + ", newBlockId=" + newBlockId + ", finalized=" + 
						finalized);
				}
				else
				{
					System.Diagnostics.Debug.Assert(finalized.GetBlockId() == oldBlock.GetBlockId() &&
						 finalized.GetGenerationStamp() == oldBlock.GetGenerationStamp() && finalized.GetNumBytes
						() == oldBlock.GetNumBytes(), "Finalized and old information mismatched: oldBlock="
						 + oldBlock + ", genStamp=" + oldBlock.GetGenerationStamp() + ", len=" + oldBlock
						.GetNumBytes() + ", finalized=" + finalized);
				}
				//check replica files after update
				CheckReplicaFiles(finalized);
				//return storage ID
				return GetVolume(new ExtendedBlock(bpid, finalized)).GetStorageID();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private FinalizedReplica UpdateReplicaUnderRecovery(string bpid, ReplicaUnderRecovery
			 rur, long recoveryId, long newBlockId, long newlength)
		{
			//check recovery id
			if (rur.GetRecoveryID() != recoveryId)
			{
				throw new IOException("rur.getRecoveryID() != recoveryId = " + recoveryId + ", rur="
					 + rur);
			}
			bool copyOnTruncate = newBlockId > 0L && rur.GetBlockId() != newBlockId;
			FilePath blockFile;
			FilePath metaFile;
			// bump rur's GS to be recovery id
			if (!copyOnTruncate)
			{
				BumpReplicaGS(rur, recoveryId);
				blockFile = rur.GetBlockFile();
				metaFile = rur.GetMetaFile();
			}
			else
			{
				FilePath[] copiedReplicaFiles = CopyReplicaWithNewBlockIdAndGS(rur, bpid, newBlockId
					, recoveryId);
				blockFile = copiedReplicaFiles[1];
				metaFile = copiedReplicaFiles[0];
			}
			//update length
			if (rur.GetNumBytes() < newlength)
			{
				throw new IOException("rur.getNumBytes() < newlength = " + newlength + ", rur=" +
					 rur);
			}
			if (rur.GetNumBytes() > newlength)
			{
				rur.UnlinkBlock(1);
				TruncateBlock(blockFile, metaFile, rur.GetNumBytes(), newlength);
				if (!copyOnTruncate)
				{
					// update RUR with the new length
					rur.SetNumBytes(newlength);
				}
				else
				{
					// Copying block to a new block with new blockId.
					// Not truncating original block.
					ReplicaBeingWritten newReplicaInfo = new ReplicaBeingWritten(newBlockId, recoveryId
						, rur.GetVolume(), blockFile.GetParentFile(), newlength);
					newReplicaInfo.SetNumBytes(newlength);
					volumeMap.Add(bpid, newReplicaInfo);
					FinalizeReplica(bpid, newReplicaInfo);
				}
			}
			// finalize the block
			return FinalizeReplica(bpid, rur);
		}

		/// <exception cref="System.IO.IOException"/>
		private FilePath[] CopyReplicaWithNewBlockIdAndGS(ReplicaUnderRecovery replicaInfo
			, string bpid, long newBlkId, long newGS)
		{
			string blockFileName = Block.BlockFilePrefix + newBlkId;
			FsVolumeReference v = volumes.GetNextVolume(replicaInfo.GetVolume().GetStorageType
				(), replicaInfo.GetNumBytes());
			FilePath tmpDir = ((FsVolumeImpl)v.GetVolume()).GetBlockPoolSlice(bpid).GetTmpDir
				();
			FilePath destDir = DatanodeUtil.IdToBlockDir(tmpDir, newBlkId);
			FilePath dstBlockFile = new FilePath(destDir, blockFileName);
			FilePath dstMetaFile = FsDatasetUtil.GetMetaFile(dstBlockFile, newGS);
			return CopyBlockFiles(replicaInfo.GetMetaFile(), replicaInfo.GetBlockFile(), dstMetaFile
				, dstBlockFile, true);
		}

		/// <exception cref="System.IO.IOException"/>
		public override long GetReplicaVisibleLength(ExtendedBlock block)
		{
			lock (this)
			{
				// FsDatasetSpi
				Replica replica = GetReplicaInfo(block.GetBlockPoolId(), block.GetBlockId());
				if (replica.GetGenerationStamp() < block.GetGenerationStamp())
				{
					throw new IOException("replica.getGenerationStamp() < block.getGenerationStamp(), block="
						 + block + ", replica=" + replica);
				}
				return replica.GetVisibleLength();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void AddBlockPool(string bpid, Configuration conf)
		{
			Log.Info("Adding block pool " + bpid);
			lock (this)
			{
				volumes.AddBlockPool(bpid, conf);
				volumeMap.InitBlockPool(bpid);
			}
			volumes.GetAllVolumesMap(bpid, volumeMap, ramDiskReplicaTracker);
		}

		public override void ShutdownBlockPool(string bpid)
		{
			lock (this)
			{
				Log.Info("Removing block pool " + bpid);
				volumeMap.CleanUpBlockPool(bpid);
				volumes.RemoveBlockPool(bpid);
			}
		}

		/// <summary>Class for representing the Datanode volume information</summary>
		private class VolumeInfo
		{
			internal readonly string directory;

			internal readonly long usedSpace;

			internal readonly long freeSpace;

			internal readonly long reservedSpace;

			internal VolumeInfo(FsVolumeImpl v, long usedSpace, long freeSpace)
			{
				// size of space used by HDFS
				// size of free space excluding reserved space
				// size of space reserved for non-HDFS and RBW
				this.directory = v.ToString();
				this.usedSpace = usedSpace;
				this.freeSpace = freeSpace;
				this.reservedSpace = v.GetReserved();
			}
		}

		private ICollection<FsDatasetImpl.VolumeInfo> GetVolumeInfo()
		{
			ICollection<FsDatasetImpl.VolumeInfo> info = new AList<FsDatasetImpl.VolumeInfo>(
				);
			foreach (FsVolumeImpl volume in GetVolumes())
			{
				long used = 0;
				long free = 0;
				try
				{
					using (FsVolumeReference @ref = volume.ObtainReference())
					{
						used = volume.GetDfsUsed();
						free = volume.GetAvailable();
					}
				}
				catch (ClosedChannelException)
				{
					continue;
				}
				catch (IOException e)
				{
					Log.Warn(e.Message);
					used = 0;
					free = 0;
				}
				info.AddItem(new FsDatasetImpl.VolumeInfo(volume, used, free));
			}
			return info;
		}

		public override IDictionary<string, object> GetVolumeInfoMap()
		{
			IDictionary<string, object> info = new Dictionary<string, object>();
			ICollection<FsDatasetImpl.VolumeInfo> volumes = GetVolumeInfo();
			foreach (FsDatasetImpl.VolumeInfo v in volumes)
			{
				IDictionary<string, object> innerInfo = new Dictionary<string, object>();
				innerInfo["usedSpace"] = v.usedSpace;
				innerInfo["freeSpace"] = v.freeSpace;
				innerInfo["reservedSpace"] = v.reservedSpace;
				info[v.directory] = innerInfo;
			}
			return info;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void DeleteBlockPool(string bpid, bool force)
		{
			lock (this)
			{
				//FsDatasetSpi
				IList<FsVolumeImpl> curVolumes = GetVolumes();
				if (!force)
				{
					foreach (FsVolumeImpl volume in curVolumes)
					{
						try
						{
							using (FsVolumeReference @ref = volume.ObtainReference())
							{
								if (!volume.IsBPDirEmpty(bpid))
								{
									Log.Warn(bpid + " has some block files, cannot delete unless forced");
									throw new IOException("Cannot delete block pool, " + "it contains some block files"
										);
								}
							}
						}
						catch (ClosedChannelException)
						{
						}
					}
				}
				// ignore.
				foreach (FsVolumeImpl volume_1 in curVolumes)
				{
					try
					{
						using (FsVolumeReference @ref = volume_1.ObtainReference())
						{
							volume_1.DeleteBPDirectories(bpid, force);
						}
					}
					catch (ClosedChannelException)
					{
					}
				}
			}
		}

		// ignore.
		/// <exception cref="System.IO.IOException"/>
		public override BlockLocalPathInfo GetBlockLocalPathInfo(ExtendedBlock block)
		{
			// FsDatasetSpi
			lock (this)
			{
				Replica replica = volumeMap.Get(block.GetBlockPoolId(), block.GetBlockId());
				if (replica == null)
				{
					throw new ReplicaNotFoundException(block);
				}
				if (replica.GetGenerationStamp() < block.GetGenerationStamp())
				{
					throw new IOException("Replica generation stamp < block generation stamp, block="
						 + block + ", replica=" + replica);
				}
				else
				{
					if (replica.GetGenerationStamp() > block.GetGenerationStamp())
					{
						block.SetGenerationStamp(replica.GetGenerationStamp());
					}
				}
			}
			FilePath datafile = GetBlockFile(block);
			FilePath metafile = FsDatasetUtil.GetMetaFile(datafile, block.GetGenerationStamp(
				));
			BlockLocalPathInfo info = new BlockLocalPathInfo(block, datafile.GetAbsolutePath(
				), metafile.GetAbsolutePath());
			return info;
		}

		/// <exception cref="System.IO.IOException"/>
		public override HdfsBlocksMetadata GetHdfsBlocksMetadata(string poolId, long[] blockIds
			)
		{
			// FsDatasetSpi
			IList<FsVolumeImpl> curVolumes = GetVolumes();
			// List of VolumeIds, one per volume on the datanode
			IList<byte[]> blocksVolumeIds = new AList<byte[]>(curVolumes.Count);
			// List of indexes into the list of VolumeIds, pointing at the VolumeId of
			// the volume that the block is on
			IList<int> blocksVolumeIndexes = new AList<int>(blockIds.Length);
			// Initialize the list of VolumeIds simply by enumerating the volumes
			for (int i = 0; i < curVolumes.Count; i++)
			{
				blocksVolumeIds.AddItem(((byte[])ByteBuffer.Allocate(4).PutInt(i).Array()));
			}
			// Determine the index of the VolumeId of each block's volume, by comparing 
			// the block's volume against the enumerated volumes
			for (int i_1 = 0; i_1 < blockIds.Length; i_1++)
			{
				long blockId = blockIds[i_1];
				bool isValid = false;
				ReplicaInfo info = volumeMap.Get(poolId, blockId);
				int volumeIndex = 0;
				if (info != null)
				{
					FsVolumeSpi blockVolume = info.GetVolume();
					foreach (FsVolumeImpl volume in curVolumes)
					{
						// This comparison of references should be safe
						if (blockVolume == volume)
						{
							isValid = true;
							break;
						}
						volumeIndex++;
					}
				}
				// Indicates that the block is not present, or not found in a data dir
				if (!isValid)
				{
					volumeIndex = int.MaxValue;
				}
				blocksVolumeIndexes.AddItem(volumeIndex);
			}
			return new HdfsBlocksMetadata(poolId, blockIds, blocksVolumeIds, blocksVolumeIndexes
				);
		}

		public override void EnableTrash(string bpid)
		{
			dataStorage.EnableTrash(bpid);
		}

		public override void ClearTrash(string bpid)
		{
			dataStorage.ClearTrash(bpid);
		}

		public override bool TrashEnabled(string bpid)
		{
			return dataStorage.TrashEnabled(bpid);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void SetRollingUpgradeMarker(string bpid)
		{
			dataStorage.SetRollingUpgradeMarker(bpid);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void ClearRollingUpgradeMarker(string bpid)
		{
			dataStorage.ClearRollingUpgradeMarker(bpid);
		}

		public override void OnCompleteLazyPersist(string bpId, long blockId, long creationTime
			, FilePath[] savedFiles, FsVolumeImpl targetVolume)
		{
			lock (this)
			{
				ramDiskReplicaTracker.RecordEndLazyPersist(bpId, blockId, savedFiles);
				targetVolume.IncDfsUsed(bpId, savedFiles[0].Length() + savedFiles[1].Length());
				// Update metrics (ignore the metadata file size)
				datanode.GetMetrics().IncrRamDiskBlocksLazyPersisted();
				datanode.GetMetrics().IncrRamDiskBytesLazyPersisted(savedFiles[1].Length());
				datanode.GetMetrics().AddRamDiskBlocksLazyPersistWindowMs(Time.MonotonicNow() - creationTime
					);
				if (Log.IsDebugEnabled())
				{
					Log.Debug("LazyWriter: Finish persisting RamDisk block: " + " block pool Id: " + 
						bpId + " block id: " + blockId + " to block file " + savedFiles[1] + " and meta file "
						 + savedFiles[0] + " on target volume " + targetVolume);
				}
			}
		}

		public override void OnFailLazyPersist(string bpId, long blockId)
		{
			RamDiskReplicaTracker.RamDiskReplica block = null;
			block = ramDiskReplicaTracker.GetReplica(bpId, blockId);
			if (block != null)
			{
				Log.Warn("Failed to save replica " + block + ". re-enqueueing it.");
				ramDiskReplicaTracker.ReenqueueReplicaNotPersisted(block);
			}
		}

		public override void SubmitBackgroundSyncFileRangeRequest(ExtendedBlock block, FileDescriptor
			 fd, long offset, long nbytes, int flags)
		{
			FsVolumeImpl fsVolumeImpl = this.GetVolume(block);
			asyncDiskService.SubmitSyncFileRangeRequest(fsVolumeImpl, fd, offset, nbytes, flags
				);
		}

		private bool RamDiskConfigured()
		{
			foreach (FsVolumeImpl v in GetVolumes())
			{
				if (v.IsTransientStorage())
				{
					return true;
				}
			}
			return false;
		}

		// Add/Remove per DISK volume async lazy persist thread when RamDisk volume is
		// added or removed.
		// This should only be called when the FsDataSetImpl#volumes list is finalized.
		private void SetupAsyncLazyPersistThreads()
		{
			foreach (FsVolumeImpl v in GetVolumes())
			{
				SetupAsyncLazyPersistThread(v);
			}
		}

		private void SetupAsyncLazyPersistThread(FsVolumeImpl v)
		{
			// Skip transient volumes
			if (v.IsTransientStorage())
			{
				return;
			}
			bool ramDiskConfigured = RamDiskConfigured();
			// Add thread for DISK volume if RamDisk is configured
			if (ramDiskConfigured && !asyncLazyPersistService.QueryVolume(v.GetCurrentDir()))
			{
				asyncLazyPersistService.AddVolume(v.GetCurrentDir());
			}
			// Remove thread for DISK volume if RamDisk is not configured
			if (!ramDiskConfigured && asyncLazyPersistService.QueryVolume(v.GetCurrentDir()))
			{
				asyncLazyPersistService.RemoveVolume(v.GetCurrentDir());
			}
		}

		private void RemoveOldReplica(ReplicaInfo replicaInfo, ReplicaInfo newReplicaInfo
			, FilePath blockFile, FilePath metaFile, long blockFileUsed, long metaFileUsed, 
			string bpid)
		{
			// Before deleting the files from old storage we must notify the
			// NN that the files are on the new storage. Else a blockReport from
			// the transient storage might cause the NN to think the blocks are lost.
			// Replicas must be evicted from client short-circuit caches, because the
			// storage will no longer be same, and thus will require validating
			// checksum.  This also stops a client from holding file descriptors,
			// which would prevent the OS from reclaiming the memory.
			ExtendedBlock extendedBlock = new ExtendedBlock(bpid, newReplicaInfo);
			datanode.GetShortCircuitRegistry().ProcessBlockInvalidation(ExtendedBlockId.FromExtendedBlock
				(extendedBlock));
			datanode.NotifyNamenodeReceivedBlock(extendedBlock, null, newReplicaInfo.GetStorageUuid
				());
			// Remove the old replicas
			if (blockFile.Delete() || !blockFile.Exists())
			{
				((FsVolumeImpl)replicaInfo.GetVolume()).DecDfsUsed(bpid, blockFileUsed);
				if (metaFile.Delete() || !metaFile.Exists())
				{
					((FsVolumeImpl)replicaInfo.GetVolume()).DecDfsUsed(bpid, metaFileUsed);
				}
			}
		}

		internal class LazyWriter : Runnable
		{
			private volatile bool shouldRun = true;

			internal readonly int checkpointerInterval;

			internal readonly float lowWatermarkFreeSpacePercentage;

			internal readonly long lowWatermarkFreeSpaceBytes;

			public LazyWriter(FsDatasetImpl _enclosing, Configuration conf)
			{
				this._enclosing = _enclosing;
				// If deletion failed then the directory scanner will cleanup the blocks
				// eventually.
				this.checkpointerInterval = conf.GetInt(DFSConfigKeys.DfsDatanodeLazyWriterIntervalSec
					, DFSConfigKeys.DfsDatanodeLazyWriterIntervalDefaultSec);
				this.lowWatermarkFreeSpacePercentage = conf.GetFloat(DFSConfigKeys.DfsDatanodeRamDiskLowWatermarkPercent
					, DFSConfigKeys.DfsDatanodeRamDiskLowWatermarkPercentDefault);
				this.lowWatermarkFreeSpaceBytes = conf.GetLong(DFSConfigKeys.DfsDatanodeRamDiskLowWatermarkBytes
					, DFSConfigKeys.DfsDatanodeRamDiskLowWatermarkBytesDefault);
			}

			/// <summary>Checkpoint a pending replica to persistent storage now.</summary>
			/// <remarks>
			/// Checkpoint a pending replica to persistent storage now.
			/// If we fail then move the replica to the end of the queue.
			/// </remarks>
			/// <returns>true if there is more work to be done, false otherwise.</returns>
			private bool SaveNextReplica()
			{
				RamDiskReplicaTracker.RamDiskReplica block = null;
				FsVolumeReference targetReference;
				FsVolumeImpl targetVolume;
				ReplicaInfo replicaInfo;
				bool succeeded = false;
				try
				{
					block = this._enclosing.ramDiskReplicaTracker.DequeueNextReplicaToPersist();
					if (block != null)
					{
						lock (this._enclosing)
						{
							replicaInfo = this._enclosing.volumeMap.Get(block.GetBlockPoolId(), block.GetBlockId
								());
							// If replicaInfo is null, the block was either deleted before
							// it could be checkpointed or it is already on persistent storage.
							// This can occur if a second replica on persistent storage was found
							// after the lazy write was scheduled.
							if (replicaInfo != null && replicaInfo.GetVolume().IsTransientStorage())
							{
								// Pick a target volume to persist the block.
								targetReference = this._enclosing.volumes.GetNextVolume(StorageType.Default, replicaInfo
									.GetNumBytes());
								targetVolume = (FsVolumeImpl)targetReference.GetVolume();
								this._enclosing.ramDiskReplicaTracker.RecordStartLazyPersist(block.GetBlockPoolId
									(), block.GetBlockId(), targetVolume);
								if (FsDatasetImpl.Log.IsDebugEnabled())
								{
									FsDatasetImpl.Log.Debug("LazyWriter: Start persisting RamDisk block:" + " block pool Id: "
										 + block.GetBlockPoolId() + " block id: " + block.GetBlockId() + " on target volume "
										 + targetVolume);
								}
								this._enclosing.asyncLazyPersistService.SubmitLazyPersistTask(block.GetBlockPoolId
									(), block.GetBlockId(), replicaInfo.GetGenerationStamp(), block.GetCreationTime(
									), replicaInfo.GetMetaFile(), replicaInfo.GetBlockFile(), targetReference);
							}
						}
					}
					succeeded = true;
				}
				catch (IOException ioe)
				{
					FsDatasetImpl.Log.Warn("Exception saving replica " + block, ioe);
				}
				finally
				{
					if (!succeeded && block != null)
					{
						FsDatasetImpl.Log.Warn("Failed to save replica " + block + ". re-enqueueing it.");
						this._enclosing.OnFailLazyPersist(block.GetBlockPoolId(), block.GetBlockId());
					}
				}
				return succeeded;
			}

			/// <exception cref="System.IO.IOException"/>
			private bool TransientFreeSpaceBelowThreshold()
			{
				long free = 0;
				long capacity = 0;
				float percentFree = 0.0f;
				// Don't worry about fragmentation for now. We don't expect more than one
				// transient volume per DN.
				foreach (FsVolumeImpl v in this._enclosing.GetVolumes())
				{
					try
					{
						using (FsVolumeReference @ref = v.ObtainReference())
						{
							if (v.IsTransientStorage())
							{
								capacity += v.GetCapacity();
								free += v.GetAvailable();
							}
						}
					}
					catch (ClosedChannelException)
					{
					}
				}
				// ignore.
				if (capacity == 0)
				{
					return false;
				}
				percentFree = (float)((double)free * 100 / capacity);
				return (percentFree < this.lowWatermarkFreeSpacePercentage) || (free < this.lowWatermarkFreeSpaceBytes
					);
			}

			/// <summary>
			/// Attempt to evict one or more transient block replicas we have at least
			/// spaceNeeded bytes free.
			/// </summary>
			/// <exception cref="System.IO.IOException"/>
			private void EvictBlocks()
			{
				int iterations = 0;
				while (iterations++ < FsDatasetImpl.MaxBlockEvictionsPerIteration && this.TransientFreeSpaceBelowThreshold
					())
				{
					RamDiskReplicaTracker.RamDiskReplica replicaState = this._enclosing.ramDiskReplicaTracker
						.GetNextCandidateForEviction();
					if (replicaState == null)
					{
						break;
					}
					if (FsDatasetImpl.Log.IsDebugEnabled())
					{
						FsDatasetImpl.Log.Debug("Evicting block " + replicaState);
					}
					ReplicaInfo replicaInfo;
					ReplicaInfo newReplicaInfo;
					FilePath blockFile;
					FilePath metaFile;
					long blockFileUsed;
					long metaFileUsed;
					string bpid = replicaState.GetBlockPoolId();
					lock (this._enclosing)
					{
						replicaInfo = this._enclosing.GetReplicaInfo(replicaState.GetBlockPoolId(), replicaState
							.GetBlockId());
						Preconditions.CheckState(replicaInfo.GetVolume().IsTransientStorage());
						blockFile = replicaInfo.GetBlockFile();
						metaFile = replicaInfo.GetMetaFile();
						blockFileUsed = blockFile.Length();
						metaFileUsed = metaFile.Length();
						this._enclosing.ramDiskReplicaTracker.DiscardReplica(replicaState.GetBlockPoolId(
							), replicaState.GetBlockId(), false);
						// Move the replica from lazyPersist/ to finalized/ on target volume
						BlockPoolSlice bpSlice = replicaState.GetLazyPersistVolume().GetBlockPoolSlice(bpid
							);
						FilePath newBlockFile = bpSlice.ActivateSavedReplica(replicaInfo, replicaState.GetSavedMetaFile
							(), replicaState.GetSavedBlockFile());
						newReplicaInfo = new FinalizedReplica(replicaInfo.GetBlockId(), replicaInfo.GetBytesOnDisk
							(), replicaInfo.GetGenerationStamp(), replicaState.GetLazyPersistVolume(), newBlockFile
							.GetParentFile());
						// Update the volumeMap entry.
						this._enclosing.volumeMap.Add(bpid, newReplicaInfo);
						// Update metrics
						this._enclosing.datanode.GetMetrics().IncrRamDiskBlocksEvicted();
						this._enclosing.datanode.GetMetrics().AddRamDiskBlocksEvictionWindowMs(Time.MonotonicNow
							() - replicaState.GetCreationTime());
						if (replicaState.GetNumReads() == 0)
						{
							this._enclosing.datanode.GetMetrics().IncrRamDiskBlocksEvictedWithoutRead();
						}
					}
					this._enclosing.RemoveOldReplica(replicaInfo, newReplicaInfo, blockFile, metaFile
						, blockFileUsed, metaFileUsed, bpid);
				}
			}

			public virtual void Run()
			{
				int numSuccessiveFailures = 0;
				while (this._enclosing.fsRunning && this.shouldRun)
				{
					try
					{
						numSuccessiveFailures = this.SaveNextReplica() ? 0 : (numSuccessiveFailures + 1);
						this.EvictBlocks();
						// Sleep if we have no more work to do or if it looks like we are not
						// making any forward progress. This is to ensure that if all persist
						// operations are failing we don't keep retrying them in a tight loop.
						if (numSuccessiveFailures >= this._enclosing.ramDiskReplicaTracker.NumReplicasNotPersisted
							())
						{
							Sharpen.Thread.Sleep(this.checkpointerInterval * 1000);
							numSuccessiveFailures = 0;
						}
					}
					catch (Exception)
					{
						FsDatasetImpl.Log.Info("LazyWriter was interrupted, exiting");
						break;
					}
					catch (Exception e)
					{
						FsDatasetImpl.Log.Warn("Ignoring exception in LazyWriter:", e);
					}
				}
			}

			public virtual void Stop()
			{
				this.shouldRun = false;
			}

			private readonly FsDatasetImpl _enclosing;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void SetPinning(ExtendedBlock block)
		{
			if (!blockPinningEnabled)
			{
				return;
			}
			FilePath f = GetBlockFile(block);
			Path p = new Path(f.GetAbsolutePath());
			FsPermission oldPermission = localFS.GetFileStatus(new Path(f.GetAbsolutePath()))
				.GetPermission();
			//sticky bit is used for pinning purpose
			FsPermission permission = new FsPermission(oldPermission.GetUserAction(), oldPermission
				.GetGroupAction(), oldPermission.GetOtherAction(), true);
			localFS.SetPermission(p, permission);
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool GetPinning(ExtendedBlock block)
		{
			if (!blockPinningEnabled)
			{
				return false;
			}
			FilePath f = GetBlockFile(block);
			FileStatus fss = localFS.GetFileStatus(new Path(f.GetAbsolutePath()));
			return fss.GetPermission().GetStickyBit();
		}

		public override bool IsDeletingBlock(string bpid, long blockId)
		{
			lock (deletingBlock)
			{
				ICollection<long> s = deletingBlock[bpid];
				return s != null ? s.Contains(blockId) : false;
			}
		}

		public virtual void RemoveDeletedBlocks(string bpid, ICollection<long> blockIds)
		{
			lock (deletingBlock)
			{
				ICollection<long> s = deletingBlock[bpid];
				if (s != null)
				{
					foreach (long id in blockIds)
					{
						s.Remove(id);
					}
				}
			}
		}

		private void AddDeletingBlock(string bpid, long blockId)
		{
			lock (deletingBlock)
			{
				ICollection<long> s = deletingBlock[bpid];
				if (s == null)
				{
					s = new HashSet<long>();
					deletingBlock[bpid] = s;
				}
				s.AddItem(blockId);
			}
		}
	}
}
