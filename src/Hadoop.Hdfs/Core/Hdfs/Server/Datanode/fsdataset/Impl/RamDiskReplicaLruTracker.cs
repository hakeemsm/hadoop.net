using System;
using System.Collections.Generic;
using Com.Google.Common.Collect;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset.Impl
{
	/// <summary>
	/// An implementation of RamDiskReplicaTracker that uses an LRU
	/// eviction scheme.
	/// </summary>
	public class RamDiskReplicaLruTracker : RamDiskReplicaTracker
	{
		private class RamDiskReplicaLru : RamDiskReplicaTracker.RamDiskReplica
		{
			internal long lastUsedTime;

			private RamDiskReplicaLru(RamDiskReplicaLruTracker _enclosing, string bpid, long 
				blockId, FsVolumeImpl ramDiskVolume)
				: base(bpid, blockId, ramDiskVolume)
			{
				this._enclosing = _enclosing;
			}

			public override int GetHashCode()
			{
				return base.GetHashCode();
			}

			public override bool Equals(object other)
			{
				return base.Equals(other);
			}

			private readonly RamDiskReplicaLruTracker _enclosing;
		}

		/// <summary>Map of blockpool ID to <map of blockID to ReplicaInfo>.</summary>
		internal IDictionary<string, IDictionary<long, RamDiskReplicaLruTracker.RamDiskReplicaLru
			>> replicaMaps;

		/// <summary>Queue of replicas that need to be written to disk.</summary>
		/// <remarks>
		/// Queue of replicas that need to be written to disk.
		/// Stale entries are GC'd by dequeueNextReplicaToPersist.
		/// </remarks>
		internal Queue<RamDiskReplicaLruTracker.RamDiskReplicaLru> replicasNotPersisted;

		/// <summary>Map of persisted replicas ordered by their last use times.</summary>
		internal TreeMultimap<long, RamDiskReplicaLruTracker.RamDiskReplicaLru> replicasPersisted;

		internal RamDiskReplicaLruTracker()
		{
			replicaMaps = new Dictionary<string, IDictionary<long, RamDiskReplicaLruTracker.RamDiskReplicaLru
				>>();
			replicasNotPersisted = new List<RamDiskReplicaLruTracker.RamDiskReplicaLru>();
			replicasPersisted = TreeMultimap.Create();
		}

		internal override void AddReplica(string bpid, long blockId, FsVolumeImpl transientVolume
			)
		{
			lock (this)
			{
				IDictionary<long, RamDiskReplicaLruTracker.RamDiskReplicaLru> map = replicaMaps[bpid
					];
				if (map == null)
				{
					map = new Dictionary<long, RamDiskReplicaLruTracker.RamDiskReplicaLru>();
					replicaMaps[bpid] = map;
				}
				RamDiskReplicaLruTracker.RamDiskReplicaLru ramDiskReplicaLru = new RamDiskReplicaLruTracker.RamDiskReplicaLru
					(this, bpid, blockId, transientVolume);
				map[blockId] = ramDiskReplicaLru;
				replicasNotPersisted.AddItem(ramDiskReplicaLru);
			}
		}

		internal override void Touch(string bpid, long blockId)
		{
			lock (this)
			{
				IDictionary<long, RamDiskReplicaLruTracker.RamDiskReplicaLru> map = replicaMaps[bpid
					];
				RamDiskReplicaLruTracker.RamDiskReplicaLru ramDiskReplicaLru = map[blockId];
				if (ramDiskReplicaLru == null)
				{
					return;
				}
				ramDiskReplicaLru.numReads.GetAndIncrement();
				// Reinsert the replica with its new timestamp.
				if (replicasPersisted.Remove(ramDiskReplicaLru.lastUsedTime, ramDiskReplicaLru))
				{
					ramDiskReplicaLru.lastUsedTime = Time.MonotonicNow();
					replicasPersisted.Put(ramDiskReplicaLru.lastUsedTime, ramDiskReplicaLru);
				}
			}
		}

		internal override void RecordStartLazyPersist(string bpid, long blockId, FsVolumeImpl
			 checkpointVolume)
		{
			lock (this)
			{
				IDictionary<long, RamDiskReplicaLruTracker.RamDiskReplicaLru> map = replicaMaps[bpid
					];
				RamDiskReplicaLruTracker.RamDiskReplicaLru ramDiskReplicaLru = map[blockId];
				ramDiskReplicaLru.SetLazyPersistVolume(checkpointVolume);
			}
		}

		internal override void RecordEndLazyPersist(string bpid, long blockId, FilePath[]
			 savedFiles)
		{
			lock (this)
			{
				IDictionary<long, RamDiskReplicaLruTracker.RamDiskReplicaLru> map = replicaMaps[bpid
					];
				RamDiskReplicaLruTracker.RamDiskReplicaLru ramDiskReplicaLru = map[blockId];
				if (ramDiskReplicaLru == null)
				{
					throw new InvalidOperationException("Unknown replica bpid=" + bpid + "; blockId="
						 + blockId);
				}
				ramDiskReplicaLru.RecordSavedBlockFiles(savedFiles);
				if (replicasNotPersisted.Peek() == ramDiskReplicaLru)
				{
					// Common case.
					replicasNotPersisted.Remove();
				}
				else
				{
					// Caller error? Fallback to O(n) removal.
					replicasNotPersisted.Remove(ramDiskReplicaLru);
				}
				ramDiskReplicaLru.lastUsedTime = Time.MonotonicNow();
				replicasPersisted.Put(ramDiskReplicaLru.lastUsedTime, ramDiskReplicaLru);
				ramDiskReplicaLru.isPersisted = true;
			}
		}

		internal override RamDiskReplicaTracker.RamDiskReplica DequeueNextReplicaToPersist
			()
		{
			lock (this)
			{
				while (replicasNotPersisted.Count != 0)
				{
					RamDiskReplicaLruTracker.RamDiskReplicaLru ramDiskReplicaLru = replicasNotPersisted
						.Remove();
					IDictionary<long, RamDiskReplicaLruTracker.RamDiskReplicaLru> replicaMap = replicaMaps
						[ramDiskReplicaLru.GetBlockPoolId()];
					if (replicaMap != null && replicaMap[ramDiskReplicaLru.GetBlockId()] != null)
					{
						return ramDiskReplicaLru;
					}
				}
				// The replica no longer exists, look for the next one.
				return null;
			}
		}

		internal override void ReenqueueReplicaNotPersisted(RamDiskReplicaTracker.RamDiskReplica
			 ramDiskReplicaLru)
		{
			lock (this)
			{
				replicasNotPersisted.AddItem((RamDiskReplicaLruTracker.RamDiskReplicaLru)ramDiskReplicaLru
					);
			}
		}

		internal override int NumReplicasNotPersisted()
		{
			lock (this)
			{
				return replicasNotPersisted.Count;
			}
		}

		internal override RamDiskReplicaTracker.RamDiskReplica GetNextCandidateForEviction
			()
		{
			lock (this)
			{
				IEnumerator<RamDiskReplicaLruTracker.RamDiskReplicaLru> it = replicasPersisted.Values
					().GetEnumerator();
				while (it.HasNext())
				{
					RamDiskReplicaLruTracker.RamDiskReplicaLru ramDiskReplicaLru = it.Next();
					it.Remove();
					IDictionary<long, RamDiskReplicaLruTracker.RamDiskReplicaLru> replicaMap = replicaMaps
						[ramDiskReplicaLru.GetBlockPoolId()];
					if (replicaMap != null && replicaMap[ramDiskReplicaLru.GetBlockId()] != null)
					{
						return ramDiskReplicaLru;
					}
				}
				// The replica no longer exists, look for the next one.
				return null;
			}
		}

		/// <summary>Discard any state we are tracking for the given replica.</summary>
		/// <remarks>
		/// Discard any state we are tracking for the given replica. This could mean
		/// the block is either deleted from the block space or the replica is no longer
		/// on transient storage.
		/// </remarks>
		/// <param name="deleteSavedCopies">
		/// true if we should delete the saved copies on
		/// persistent storage. This should be set by the
		/// caller when the block is no longer needed.
		/// </param>
		internal override void DiscardReplica(string bpid, long blockId, bool deleteSavedCopies
			)
		{
			lock (this)
			{
				IDictionary<long, RamDiskReplicaLruTracker.RamDiskReplicaLru> map = replicaMaps[bpid
					];
				if (map == null)
				{
					return;
				}
				RamDiskReplicaLruTracker.RamDiskReplicaLru ramDiskReplicaLru = map[blockId];
				if (ramDiskReplicaLru == null)
				{
					return;
				}
				if (deleteSavedCopies)
				{
					ramDiskReplicaLru.DeleteSavedFiles();
				}
				Sharpen.Collections.Remove(map, blockId);
				replicasPersisted.Remove(ramDiskReplicaLru.lastUsedTime, ramDiskReplicaLru);
			}
		}

		// replicasNotPersisted will be lazily GC'ed.
		internal override RamDiskReplicaTracker.RamDiskReplica GetReplica(string bpid, long
			 blockId)
		{
			lock (this)
			{
				IDictionary<long, RamDiskReplicaLruTracker.RamDiskReplicaLru> map = replicaMaps[bpid
					];
				if (map == null)
				{
					return null;
				}
				return map[blockId];
			}
		}
	}
}
