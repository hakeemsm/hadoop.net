using System;
using System.IO;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.IO.Nativeio;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset.Impl
{
	/// <summary>A block pool slice represents a portion of a block pool stored on a volume.
	/// 	</summary>
	/// <remarks>
	/// A block pool slice represents a portion of a block pool stored on a volume.
	/// Taken together, all BlockPoolSlices sharing a block pool ID across a
	/// cluster represent a single block pool.
	/// This class is synchronized by
	/// <see cref="FsVolumeImpl"/>
	/// .
	/// </remarks>
	internal class BlockPoolSlice
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset.Impl.BlockPoolSlice
			));

		private readonly string bpid;

		private readonly FsVolumeImpl volume;

		private readonly FilePath currentDir;

		private readonly FilePath finalizedDir;

		private readonly FilePath lazypersistDir;

		private readonly FilePath rbwDir;

		private readonly FilePath tmpDir;

		private const string DuCacheFile = "dfsUsed";

		private volatile bool dfsUsedSaved = false;

		private const int ShutdownHookPriority = 30;

		private readonly bool deleteDuplicateReplicas;

		private readonly DU dfsUsage;

		/// <summary>Create a blook pool slice</summary>
		/// <param name="bpid">Block pool Id</param>
		/// <param name="volume">
		/// 
		/// <see cref="FsVolumeImpl"/>
		/// to which this BlockPool belongs to
		/// </param>
		/// <param name="bpDir">directory corresponding to the BlockPool</param>
		/// <param name="conf">configuration</param>
		/// <exception cref="System.IO.IOException"/>
		internal BlockPoolSlice(string bpid, FsVolumeImpl volume, FilePath bpDir, Configuration
			 conf)
		{
			// volume to which this BlockPool belongs to
			// StorageDirectory/current/bpid/current
			// directory where finalized replicas are stored
			// directory store RBW replica
			// directory store Temporary replica
			// TODO:FEDERATION scalability issue - a thread per DU is needed
			this.bpid = bpid;
			this.volume = volume;
			this.currentDir = new FilePath(bpDir, DataStorage.StorageDirCurrent);
			this.finalizedDir = new FilePath(currentDir, DataStorage.StorageDirFinalized);
			this.lazypersistDir = new FilePath(currentDir, DataStorage.StorageDirLazyPersist);
			if (!this.finalizedDir.Exists())
			{
				if (!this.finalizedDir.Mkdirs())
				{
					throw new IOException("Failed to mkdirs " + this.finalizedDir);
				}
			}
			this.deleteDuplicateReplicas = conf.GetBoolean(DFSConfigKeys.DfsDatanodeDuplicateReplicaDeletion
				, DFSConfigKeys.DfsDatanodeDuplicateReplicaDeletionDefault);
			// Files that were being written when the datanode was last shutdown
			// are now moved back to the data directory. It is possible that
			// in the future, we might want to do some sort of datanode-local
			// recovery for these blocks. For example, crc validation.
			//
			this.tmpDir = new FilePath(bpDir, DataStorage.StorageDirTmp);
			if (tmpDir.Exists())
			{
				FileUtil.FullyDelete(tmpDir);
			}
			this.rbwDir = new FilePath(currentDir, DataStorage.StorageDirRbw);
			bool supportAppends = conf.GetBoolean(DFSConfigKeys.DfsSupportAppendKey, DFSConfigKeys
				.DfsSupportAppendDefault);
			if (rbwDir.Exists() && !supportAppends)
			{
				FileUtil.FullyDelete(rbwDir);
			}
			if (!rbwDir.Mkdirs())
			{
				// create rbw directory if not exist
				if (!rbwDir.IsDirectory())
				{
					throw new IOException("Mkdirs failed to create " + rbwDir.ToString());
				}
			}
			if (!tmpDir.Mkdirs())
			{
				if (!tmpDir.IsDirectory())
				{
					throw new IOException("Mkdirs failed to create " + tmpDir.ToString());
				}
			}
			// Use cached value initially if available. Or the following call will
			// block until the initial du command completes.
			this.dfsUsage = new DU(bpDir, conf, LoadDfsUsed());
			this.dfsUsage.Start();
			// Make the dfs usage to be saved during shutdown.
			ShutdownHookManager.Get().AddShutdownHook(new _Runnable_145(this), ShutdownHookPriority
				);
		}

		private sealed class _Runnable_145 : Runnable
		{
			public _Runnable_145(BlockPoolSlice _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public void Run()
			{
				if (!this._enclosing.dfsUsedSaved)
				{
					this._enclosing.SaveDfsUsed();
				}
			}

			private readonly BlockPoolSlice _enclosing;
		}

		internal virtual FilePath GetDirectory()
		{
			return currentDir.GetParentFile();
		}

		internal virtual FilePath GetFinalizedDir()
		{
			return finalizedDir;
		}

		internal virtual FilePath GetLazypersistDir()
		{
			return lazypersistDir;
		}

		internal virtual FilePath GetRbwDir()
		{
			return rbwDir;
		}

		internal virtual FilePath GetTmpDir()
		{
			return tmpDir;
		}

		/// <summary>Run DU on local drives.</summary>
		/// <remarks>Run DU on local drives.  It must be synchronized from caller.</remarks>
		internal virtual void DecDfsUsed(long value)
		{
			dfsUsage.DecDfsUsed(value);
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual long GetDfsUsed()
		{
			return dfsUsage.GetUsed();
		}

		internal virtual void IncDfsUsed(long value)
		{
			dfsUsage.IncDfsUsed(value);
		}

		/// <summary>
		/// Read in the cached DU value and return it if it is less than 600 seconds
		/// old (DU update interval).
		/// </summary>
		/// <remarks>
		/// Read in the cached DU value and return it if it is less than 600 seconds
		/// old (DU update interval). Slight imprecision of dfsUsed is not critical
		/// and skipping DU can significantly shorten the startup time.
		/// If the cached value is not available or too old, -1 is returned.
		/// </remarks>
		internal virtual long LoadDfsUsed()
		{
			long cachedDfsUsed;
			long mtime;
			Scanner sc;
			try
			{
				sc = new Scanner(new FilePath(currentDir, DuCacheFile), "UTF-8");
			}
			catch (FileNotFoundException)
			{
				return -1;
			}
			try
			{
				// Get the recorded dfsUsed from the file.
				if (sc.HasNextLong())
				{
					cachedDfsUsed = sc.NextLong();
				}
				else
				{
					return -1;
				}
				// Get the recorded mtime from the file.
				if (sc.HasNextLong())
				{
					mtime = sc.NextLong();
				}
				else
				{
					return -1;
				}
				// Return the cached value if mtime is okay.
				if (mtime > 0 && (Time.Now() - mtime < 600000L))
				{
					FsDatasetImpl.Log.Info("Cached dfsUsed found for " + currentDir + ": " + cachedDfsUsed
						);
					return cachedDfsUsed;
				}
				return -1;
			}
			finally
			{
				sc.Close();
			}
		}

		/// <summary>Write the current dfsUsed to the cache file.</summary>
		internal virtual void SaveDfsUsed()
		{
			FilePath outFile = new FilePath(currentDir, DuCacheFile);
			if (outFile.Exists() && !outFile.Delete())
			{
				FsDatasetImpl.Log.Warn("Failed to delete old dfsUsed file in " + outFile.GetParent
					());
			}
			try
			{
				long used = GetDfsUsed();
				using (TextWriter @out = new OutputStreamWriter(new FileOutputStream(outFile), "UTF-8"
					))
				{
					// mtime is written last, so that truncated writes won't be valid.
					@out.Write(System.Convert.ToString(used) + " " + System.Convert.ToString(Time.Now
						()));
					@out.Flush();
				}
			}
			catch (IOException ioe)
			{
				// If write failed, the volume might be bad. Since the cache file is
				// not critical, log the error and continue.
				FsDatasetImpl.Log.Warn("Failed to write dfsUsed to " + outFile, ioe);
			}
		}

		/// <summary>Temporary files.</summary>
		/// <remarks>
		/// Temporary files. They get moved to the finalized block directory when
		/// the block is finalized.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		internal virtual FilePath CreateTmpFile(Block b)
		{
			FilePath f = new FilePath(tmpDir, b.GetBlockName());
			return DatanodeUtil.CreateTmpFile(b, f);
		}

		/// <summary>RBW files.</summary>
		/// <remarks>
		/// RBW files. They get moved to the finalized block directory when
		/// the block is finalized.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		internal virtual FilePath CreateRbwFile(Block b)
		{
			FilePath f = new FilePath(rbwDir, b.GetBlockName());
			return DatanodeUtil.CreateTmpFile(b, f);
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual FilePath AddBlock(Block b, FilePath f)
		{
			FilePath blockDir = DatanodeUtil.IdToBlockDir(finalizedDir, b.GetBlockId());
			if (!blockDir.Exists())
			{
				if (!blockDir.Mkdirs())
				{
					throw new IOException("Failed to mkdirs " + blockDir);
				}
			}
			FilePath blockFile = FsDatasetImpl.MoveBlockFiles(b, f, blockDir);
			FilePath metaFile = FsDatasetUtil.GetMetaFile(blockFile, b.GetGenerationStamp());
			dfsUsage.IncDfsUsed(b.GetNumBytes() + metaFile.Length());
			return blockFile;
		}

		/// <summary>
		/// Move a persisted replica from lazypersist directory to a subdirectory
		/// under finalized.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		internal virtual FilePath ActivateSavedReplica(Block b, FilePath metaFile, FilePath
			 blockFile)
		{
			FilePath blockDir = DatanodeUtil.IdToBlockDir(finalizedDir, b.GetBlockId());
			FilePath targetBlockFile = new FilePath(blockDir, blockFile.GetName());
			FilePath targetMetaFile = new FilePath(blockDir, metaFile.GetName());
			FileUtils.MoveFile(blockFile, targetBlockFile);
			FsDatasetImpl.Log.Info("Moved " + blockFile + " to " + targetBlockFile);
			FileUtils.MoveFile(metaFile, targetMetaFile);
			FsDatasetImpl.Log.Info("Moved " + metaFile + " to " + targetMetaFile);
			return targetBlockFile;
		}

		/// <exception cref="Org.Apache.Hadoop.Util.DiskChecker.DiskErrorException"/>
		internal virtual void CheckDirs()
		{
			DiskChecker.CheckDirs(finalizedDir);
			DiskChecker.CheckDir(tmpDir);
			DiskChecker.CheckDir(rbwDir);
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void GetVolumeMap(ReplicaMap volumeMap, RamDiskReplicaTracker lazyWriteReplicaMap
			)
		{
			// Recover lazy persist replicas, they will be added to the volumeMap
			// when we scan the finalized directory.
			if (lazypersistDir.Exists())
			{
				int numRecovered = MoveLazyPersistReplicasToFinalized(lazypersistDir);
				FsDatasetImpl.Log.Info("Recovered " + numRecovered + " replicas from " + lazypersistDir
					);
			}
			// add finalized replicas
			AddToReplicasMap(volumeMap, finalizedDir, lazyWriteReplicaMap, true);
			// add rbw replicas
			AddToReplicasMap(volumeMap, rbwDir, lazyWriteReplicaMap, false);
		}

		/// <summary>Recover an unlinked tmp file on datanode restart.</summary>
		/// <remarks>
		/// Recover an unlinked tmp file on datanode restart. If the original block
		/// does not exist, then the tmp file is renamed to be the
		/// original file name and the original name is returned; otherwise the tmp
		/// file is deleted and null is returned.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		internal virtual FilePath RecoverTempUnlinkedBlock(FilePath unlinkedTmp)
		{
			FilePath blockFile = FsDatasetUtil.GetOrigFile(unlinkedTmp);
			if (blockFile.Exists())
			{
				// If the original block file still exists, then no recovery is needed.
				if (!unlinkedTmp.Delete())
				{
					throw new IOException("Unable to cleanup unlinked tmp file " + unlinkedTmp);
				}
				return null;
			}
			else
			{
				if (!unlinkedTmp.RenameTo(blockFile))
				{
					throw new IOException("Unable to rename unlinked tmp file " + unlinkedTmp);
				}
				return blockFile;
			}
		}

		/// <summary>
		/// Move replicas in the lazy persist directory to their corresponding locations
		/// in the finalized directory.
		/// </summary>
		/// <returns>number of replicas recovered.</returns>
		/// <exception cref="System.IO.IOException"/>
		private int MoveLazyPersistReplicasToFinalized(FilePath source)
		{
			FilePath[] files = FileUtil.ListFiles(source);
			int numRecovered = 0;
			foreach (FilePath file in files)
			{
				if (file.IsDirectory())
				{
					numRecovered += MoveLazyPersistReplicasToFinalized(file);
				}
				if (Block.IsMetaFilename(file.GetName()))
				{
					FilePath metaFile = file;
					FilePath blockFile = Block.MetaToBlockFile(metaFile);
					long blockId = Block.Filename2id(blockFile.GetName());
					FilePath targetDir = DatanodeUtil.IdToBlockDir(finalizedDir, blockId);
					if (blockFile.Exists())
					{
						if (!targetDir.Exists() && !targetDir.Mkdirs())
						{
							Log.Warn("Failed to mkdirs " + targetDir);
							continue;
						}
						FilePath targetMetaFile = new FilePath(targetDir, metaFile.GetName());
						try
						{
							NativeIO.RenameTo(metaFile, targetMetaFile);
						}
						catch (IOException e)
						{
							Log.Warn("Failed to move meta file from " + metaFile + " to " + targetMetaFile, e
								);
							continue;
						}
						FilePath targetBlockFile = new FilePath(targetDir, blockFile.GetName());
						try
						{
							NativeIO.RenameTo(blockFile, targetBlockFile);
						}
						catch (IOException e)
						{
							Log.Warn("Failed to move block file from " + blockFile + " to " + targetBlockFile
								, e);
							continue;
						}
						if (targetBlockFile.Exists() && targetMetaFile.Exists())
						{
							++numRecovered;
						}
						else
						{
							// Failure should be rare.
							Log.Warn("Failed to move " + blockFile + " to " + targetDir);
						}
					}
				}
			}
			FileUtil.FullyDelete(source);
			return numRecovered;
		}

		/// <summary>Add replicas under the given directory to the volume map</summary>
		/// <param name="volumeMap">the replicas map</param>
		/// <param name="dir">an input directory</param>
		/// <param name="lazyWriteReplicaMap">
		/// Map of replicas on transient
		/// storage.
		/// </param>
		/// <param name="isFinalized">
		/// true if the directory has finalized replicas;
		/// false if the directory has rbw replicas
		/// </param>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void AddToReplicasMap(ReplicaMap volumeMap, FilePath dir, RamDiskReplicaTracker
			 lazyWriteReplicaMap, bool isFinalized)
		{
			FilePath[] files = FileUtil.ListFiles(dir);
			foreach (FilePath file in files)
			{
				if (file.IsDirectory())
				{
					AddToReplicasMap(volumeMap, file, lazyWriteReplicaMap, isFinalized);
				}
				if (isFinalized && FsDatasetUtil.IsUnlinkTmpFile(file))
				{
					file = RecoverTempUnlinkedBlock(file);
					if (file == null)
					{
						// the original block still exists, so we cover it
						// in another iteration and can continue here
						continue;
					}
				}
				if (!Block.IsBlockFilename(file))
				{
					continue;
				}
				long genStamp = FsDatasetUtil.GetGenerationStampFromFile(files, file);
				long blockId = Block.Filename2id(file.GetName());
				ReplicaInfo newReplica = null;
				if (isFinalized)
				{
					newReplica = new FinalizedReplica(blockId, file.Length(), genStamp, volume, file.
						GetParentFile());
				}
				else
				{
					bool loadRwr = true;
					FilePath restartMeta = new FilePath(file.GetParent() + FilePath.pathSeparator + "."
						 + file.GetName() + ".restart");
					Scanner sc = null;
					try
					{
						sc = new Scanner(restartMeta, "UTF-8");
						// The restart meta file exists
						if (sc.HasNextLong() && (sc.NextLong() > Time.Now()))
						{
							// It didn't expire. Load the replica as a RBW.
							// We don't know the expected block length, so just use 0
							// and don't reserve any more space for writes.
							newReplica = new ReplicaBeingWritten(blockId, ValidateIntegrityAndSetLength(file, 
								genStamp), genStamp, volume, file.GetParentFile(), null, 0);
							loadRwr = false;
						}
						sc.Close();
						if (!restartMeta.Delete())
						{
							FsDatasetImpl.Log.Warn("Failed to delete restart meta file: " + restartMeta.GetPath
								());
						}
					}
					catch (FileNotFoundException)
					{
					}
					finally
					{
						// nothing to do hereFile dir =
						if (sc != null)
						{
							sc.Close();
						}
					}
					// Restart meta doesn't exist or expired.
					if (loadRwr)
					{
						newReplica = new ReplicaWaitingToBeRecovered(blockId, ValidateIntegrityAndSetLength
							(file, genStamp), genStamp, volume, file.GetParentFile());
					}
				}
				ReplicaInfo oldReplica = volumeMap.Get(bpid, newReplica.GetBlockId());
				if (oldReplica == null)
				{
					volumeMap.Add(bpid, newReplica);
				}
				else
				{
					// We have multiple replicas of the same block so decide which one
					// to keep.
					newReplica = ResolveDuplicateReplicas(newReplica, oldReplica, volumeMap);
				}
				// If we are retaining a replica on transient storage make sure
				// it is in the lazyWriteReplicaMap so it can be persisted
				// eventually.
				if (newReplica.GetVolume().IsTransientStorage())
				{
					lazyWriteReplicaMap.AddReplica(bpid, blockId, (FsVolumeImpl)newReplica.GetVolume(
						));
				}
				else
				{
					lazyWriteReplicaMap.DiscardReplica(bpid, blockId, false);
				}
			}
		}

		/// <summary>
		/// This method is invoked during DN startup when volumes are scanned to
		/// build up the volumeMap.
		/// </summary>
		/// <remarks>
		/// This method is invoked during DN startup when volumes are scanned to
		/// build up the volumeMap.
		/// Given two replicas, decide which one to keep. The preference is as
		/// follows:
		/// 1. Prefer the replica with the higher generation stamp.
		/// 2. If generation stamps are equal, prefer the replica with the
		/// larger on-disk length.
		/// 3. If on-disk length is the same, prefer the replica on persistent
		/// storage volume.
		/// 4. All other factors being equal, keep replica1.
		/// The other replica is removed from the volumeMap and is deleted from
		/// its storage volume.
		/// </remarks>
		/// <param name="replica1"/>
		/// <param name="replica2"/>
		/// <param name="volumeMap"/>
		/// <returns>the replica that is retained.</returns>
		/// <exception cref="System.IO.IOException"/>
		internal virtual ReplicaInfo ResolveDuplicateReplicas(ReplicaInfo replica1, ReplicaInfo
			 replica2, ReplicaMap volumeMap)
		{
			if (!deleteDuplicateReplicas)
			{
				// Leave both block replicas in place.
				return replica1;
			}
			ReplicaInfo replicaToDelete = SelectReplicaToDelete(replica1, replica2);
			ReplicaInfo replicaToKeep = (replicaToDelete != replica1) ? replica1 : replica2;
			// Update volumeMap and delete the replica
			volumeMap.Add(bpid, replicaToKeep);
			if (replicaToDelete != null)
			{
				DeleteReplica(replicaToDelete);
			}
			return replicaToKeep;
		}

		[VisibleForTesting]
		internal static ReplicaInfo SelectReplicaToDelete(ReplicaInfo replica1, ReplicaInfo
			 replica2)
		{
			ReplicaInfo replicaToKeep;
			ReplicaInfo replicaToDelete;
			// it's the same block so don't ever delete it, even if GS or size
			// differs.  caller should keep the one it just discovered on disk
			if (replica1.GetBlockFile().Equals(replica2.GetBlockFile()))
			{
				return null;
			}
			if (replica1.GetGenerationStamp() != replica2.GetGenerationStamp())
			{
				replicaToKeep = replica1.GetGenerationStamp() > replica2.GetGenerationStamp() ? replica1
					 : replica2;
			}
			else
			{
				if (replica1.GetNumBytes() != replica2.GetNumBytes())
				{
					replicaToKeep = replica1.GetNumBytes() > replica2.GetNumBytes() ? replica1 : replica2;
				}
				else
				{
					if (replica1.GetVolume().IsTransientStorage() && !replica2.GetVolume().IsTransientStorage
						())
					{
						replicaToKeep = replica2;
					}
					else
					{
						replicaToKeep = replica1;
					}
				}
			}
			replicaToDelete = (replicaToKeep == replica1) ? replica2 : replica1;
			if (Log.IsDebugEnabled())
			{
				Log.Debug("resolveDuplicateReplicas decide to keep " + replicaToKeep + ".  Will try to delete "
					 + replicaToDelete);
			}
			return replicaToDelete;
		}

		private void DeleteReplica(ReplicaInfo replicaToDelete)
		{
			// Delete the files on disk. Failure here is okay.
			FilePath blockFile = replicaToDelete.GetBlockFile();
			if (!blockFile.Delete())
			{
				Log.Warn("Failed to delete block file " + blockFile);
			}
			FilePath metaFile = replicaToDelete.GetMetaFile();
			if (!metaFile.Delete())
			{
				Log.Warn("Failed to delete meta file " + metaFile);
			}
		}

		/// <summary>Find out the number of bytes in the block that match its crc.</summary>
		/// <remarks>
		/// Find out the number of bytes in the block that match its crc.
		/// This algorithm assumes that data corruption caused by unexpected
		/// datanode shutdown occurs only in the last crc chunk. So it checks
		/// only the last chunk.
		/// </remarks>
		/// <param name="blockFile">the block file</param>
		/// <param name="genStamp">generation stamp of the block</param>
		/// <returns>the number of valid bytes</returns>
		private long ValidateIntegrityAndSetLength(FilePath blockFile, long genStamp)
		{
			DataInputStream checksumIn = null;
			InputStream blockIn = null;
			try
			{
				FilePath metaFile = FsDatasetUtil.GetMetaFile(blockFile, genStamp);
				long blockFileLen = blockFile.Length();
				long metaFileLen = metaFile.Length();
				int crcHeaderLen = DataChecksum.GetChecksumHeaderSize();
				if (!blockFile.Exists() || blockFileLen == 0 || !metaFile.Exists() || metaFileLen
					 < crcHeaderLen)
				{
					return 0;
				}
				checksumIn = new DataInputStream(new BufferedInputStream(new FileInputStream(metaFile
					), HdfsConstants.IoFileBufferSize));
				// read and handle the common header here. For now just a version
				DataChecksum checksum = BlockMetadataHeader.ReadDataChecksum(checksumIn, metaFile
					);
				int bytesPerChecksum = checksum.GetBytesPerChecksum();
				int checksumSize = checksum.GetChecksumSize();
				long numChunks = Math.Min((blockFileLen + bytesPerChecksum - 1) / bytesPerChecksum
					, (metaFileLen - crcHeaderLen) / checksumSize);
				if (numChunks == 0)
				{
					return 0;
				}
				IOUtils.SkipFully(checksumIn, (numChunks - 1) * checksumSize);
				blockIn = new FileInputStream(blockFile);
				long lastChunkStartPos = (numChunks - 1) * bytesPerChecksum;
				IOUtils.SkipFully(blockIn, lastChunkStartPos);
				int lastChunkSize = (int)Math.Min(bytesPerChecksum, blockFileLen - lastChunkStartPos
					);
				byte[] buf = new byte[lastChunkSize + checksumSize];
				checksumIn.ReadFully(buf, lastChunkSize, checksumSize);
				IOUtils.ReadFully(blockIn, buf, 0, lastChunkSize);
				checksum.Update(buf, 0, lastChunkSize);
				long validFileLength;
				if (checksum.Compare(buf, lastChunkSize))
				{
					// last chunk matches crc
					validFileLength = lastChunkStartPos + lastChunkSize;
				}
				else
				{
					// last chunck is corrupt
					validFileLength = lastChunkStartPos;
				}
				// truncate if extra bytes are present without CRC
				if (blockFile.Length() > validFileLength)
				{
					RandomAccessFile blockRAF = new RandomAccessFile(blockFile, "rw");
					try
					{
						// truncate blockFile
						blockRAF.SetLength(validFileLength);
					}
					finally
					{
						blockRAF.Close();
					}
				}
				return validFileLength;
			}
			catch (IOException e)
			{
				FsDatasetImpl.Log.Warn(e);
				return 0;
			}
			finally
			{
				IOUtils.CloseStream(checksumIn);
				IOUtils.CloseStream(blockIn);
			}
		}

		public override string ToString()
		{
			return currentDir.GetAbsolutePath();
		}

		internal virtual void Shutdown()
		{
			SaveDfsUsed();
			dfsUsedSaved = true;
			dfsUsage.Shutdown();
		}
	}
}
