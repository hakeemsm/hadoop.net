using System;
using Com.Google.Common.Base;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset.Impl
{
	public abstract class RamDiskReplicaTracker
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(RamDiskReplicaTracker
			));

		internal FsDatasetImpl fsDataset;

		internal class RamDiskReplica : Comparable<RamDiskReplicaTracker.RamDiskReplica>
		{
			private readonly string bpid;

			private readonly long blockId;

			private FilePath savedBlockFile;

			private FilePath savedMetaFile;

			private long creationTime;

			protected internal AtomicLong numReads = new AtomicLong(0);

			protected internal bool isPersisted;

			/// <summary>RAM_DISK volume that holds the original replica.</summary>
			internal readonly FsVolumeSpi ramDiskVolume;

			/// <summary>Persistent volume that holds or will hold the saved replica.</summary>
			internal FsVolumeImpl lazyPersistVolume;

			internal RamDiskReplica(string bpid, long blockId, FsVolumeImpl ramDiskVolume)
			{
				this.bpid = bpid;
				this.blockId = blockId;
				this.ramDiskVolume = ramDiskVolume;
				lazyPersistVolume = null;
				savedMetaFile = null;
				savedBlockFile = null;
				creationTime = Time.MonotonicNow();
				isPersisted = false;
			}

			internal virtual long GetBlockId()
			{
				return blockId;
			}

			internal virtual string GetBlockPoolId()
			{
				return bpid;
			}

			internal virtual FsVolumeImpl GetLazyPersistVolume()
			{
				return lazyPersistVolume;
			}

			internal virtual void SetLazyPersistVolume(FsVolumeImpl volume)
			{
				Preconditions.CheckState(!volume.IsTransientStorage());
				this.lazyPersistVolume = volume;
			}

			internal virtual FilePath GetSavedBlockFile()
			{
				return savedBlockFile;
			}

			internal virtual FilePath GetSavedMetaFile()
			{
				return savedMetaFile;
			}

			internal virtual long GetNumReads()
			{
				return numReads.Get();
			}

			internal virtual long GetCreationTime()
			{
				return creationTime;
			}

			internal virtual bool GetIsPersisted()
			{
				return isPersisted;
			}

			/// <summary>Record the saved meta and block files on the given volume.</summary>
			/// <param name="files">Meta and block files, in that order.</param>
			internal virtual void RecordSavedBlockFiles(FilePath[] files)
			{
				this.savedMetaFile = files[0];
				this.savedBlockFile = files[1];
			}

			public override int GetHashCode()
			{
				return bpid.GetHashCode() ^ (int)blockId;
			}

			public override bool Equals(object other)
			{
				if (this == other)
				{
					return true;
				}
				if (other == null || GetType() != other.GetType())
				{
					return false;
				}
				RamDiskReplicaTracker.RamDiskReplica otherState = (RamDiskReplicaTracker.RamDiskReplica
					)other;
				return (otherState.bpid.Equals(bpid) && otherState.blockId == blockId);
			}

			// Delete the saved meta and block files. Failure to delete can be
			// ignored, the directory scanner will retry the deletion later.
			internal virtual void DeleteSavedFiles()
			{
				if (savedBlockFile != null)
				{
					if (!savedBlockFile.Delete())
					{
						Log.Warn("Failed to delete block file " + savedBlockFile);
					}
					savedBlockFile = null;
				}
				if (savedMetaFile != null)
				{
					if (!savedMetaFile.Delete())
					{
						Log.Warn("Failed to delete meta file " + savedMetaFile);
					}
					savedMetaFile = null;
				}
			}

			public virtual int CompareTo(RamDiskReplicaTracker.RamDiskReplica other)
			{
				int bpidResult = string.CompareOrdinal(bpid, other.bpid);
				if (bpidResult == 0)
				{
					if (blockId == other.blockId)
					{
						return 0;
					}
					else
					{
						if (blockId < other.blockId)
						{
							return -1;
						}
						else
						{
							return 1;
						}
					}
				}
				return bpidResult;
			}

			public override string ToString()
			{
				return "[BlockPoolID=" + bpid + "; BlockId=" + blockId + "]";
			}
		}

		/// <summary>
		/// Get an instance of the configured RamDiskReplicaTracker based on the
		/// the configuration property
		/// <see cref="Org.Apache.Hadoop.Hdfs.DFSConfigKeys.DfsDatanodeRamDiskReplicaTrackerKey
		/// 	"/>
		/// .
		/// </summary>
		/// <param name="conf">the configuration to be used</param>
		/// <param name="dataset">the FsDataset object.</param>
		/// <returns>an instance of RamDiskReplicaTracker</returns>
		internal static RamDiskReplicaTracker GetInstance(Configuration conf, FsDatasetImpl
			 fsDataset)
		{
			Type trackerClass = conf.GetClass<RamDiskReplicaTracker>(DFSConfigKeys.DfsDatanodeRamDiskReplicaTrackerKey
				, DFSConfigKeys.DfsDatanodeRamDiskReplicaTrackerDefault);
			RamDiskReplicaTracker tracker = ReflectionUtils.NewInstance(trackerClass, conf);
			tracker.Initialize(fsDataset);
			return tracker;
		}

		internal virtual void Initialize(FsDatasetImpl fsDataset)
		{
			this.fsDataset = fsDataset;
		}

		/// <summary>Start tracking a new finalized replica on RAM disk.</summary>
		/// <param name="transientVolume">RAM disk volume that stores the replica.</param>
		internal abstract void AddReplica(string bpid, long blockId, FsVolumeImpl transientVolume
			);

		/// <summary>Invoked when a replica is opened by a client.</summary>
		/// <remarks>
		/// Invoked when a replica is opened by a client. This may be used as
		/// a heuristic by the eviction scheme.
		/// </remarks>
		internal abstract void Touch(string bpid, long blockId);

		/// <summary>Get the next replica to write to persistent storage.</summary>
		internal abstract RamDiskReplicaTracker.RamDiskReplica DequeueNextReplicaToPersist
			();

		/// <summary>
		/// Invoked if a replica that was previously dequeued for persistence
		/// could not be successfully persisted.
		/// </summary>
		/// <remarks>
		/// Invoked if a replica that was previously dequeued for persistence
		/// could not be successfully persisted. Add it back so it can be retried
		/// later.
		/// </remarks>
		internal abstract void ReenqueueReplicaNotPersisted(RamDiskReplicaTracker.RamDiskReplica
			 ramDiskReplica);

		/// <summary>Invoked when the Lazy persist operation is started by the DataNode.</summary>
		/// <param name="checkpointVolume"/>
		internal abstract void RecordStartLazyPersist(string bpid, long blockId, FsVolumeImpl
			 checkpointVolume);

		/// <summary>Invoked when the Lazy persist operation is complete.</summary>
		/// <param name="savedFiles">The saved meta and block files, in that order.</param>
		internal abstract void RecordEndLazyPersist(string bpid, long blockId, FilePath[]
			 savedFiles);

		/// <summary>Return a candidate replica to remove from RAM Disk.</summary>
		/// <remarks>
		/// Return a candidate replica to remove from RAM Disk. The exact replica
		/// to be returned may depend on the eviction scheme utilized.
		/// </remarks>
		/// <returns/>
		internal abstract RamDiskReplicaTracker.RamDiskReplica GetNextCandidateForEviction
			();

		/// <summary>Return the number of replicas pending persistence to disk.</summary>
		internal abstract int NumReplicasNotPersisted();

		/// <summary>Discard all state we are tracking for the given replica.</summary>
		internal abstract void DiscardReplica(string bpid, long blockId, bool deleteSavedCopies
			);

		/// <summary>
		/// Return RamDiskReplica info given block pool id and block id
		/// Return null if it does not exist in RamDisk
		/// </summary>
		internal abstract RamDiskReplicaTracker.RamDiskReplica GetReplica(string bpid, long
			 blockId);
	}
}
