using System;
using System.IO;
using System.Text;
using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.IO.Nativeio;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Shortcircuit
{
	/// <summary>
	/// A ShortCircuitReplica object contains file descriptors for a block that
	/// we are reading via short-circuit local reads.
	/// </summary>
	/// <remarks>
	/// A ShortCircuitReplica object contains file descriptors for a block that
	/// we are reading via short-circuit local reads.
	/// The file descriptors can be shared between multiple threads because
	/// all the operations we perform are stateless-- i.e., we use pread
	/// instead of read, to avoid using the shared position state.
	/// </remarks>
	public class ShortCircuitReplica
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(ShortCircuitCache));

		/// <summary>Identifies this ShortCircuitReplica object.</summary>
		internal readonly ExtendedBlockId key;

		/// <summary>The block data input stream.</summary>
		private readonly FileInputStream dataStream;

		/// <summary>The block metadata input stream.</summary>
		/// <remarks>
		/// The block metadata input stream.
		/// TODO: make this nullable if the file has no checksums on disk.
		/// </remarks>
		private readonly FileInputStream metaStream;

		/// <summary>Block metadata header.</summary>
		private readonly BlockMetadataHeader metaHeader;

		/// <summary>The cache we belong to.</summary>
		private readonly ShortCircuitCache cache;

		/// <summary>Monotonic time at which the replica was created.</summary>
		private readonly long creationTimeMs;

		/// <summary>If non-null, the shared memory slot associated with this replica.</summary>
		private readonly ShortCircuitShm.Slot slot;

		/// <summary>Current mmap state.</summary>
		/// <remarks>
		/// Current mmap state.
		/// Protected by the cache lock.
		/// </remarks>
		internal object mmapData;

		/// <summary>True if this replica has been purged from the cache; false otherwise.</summary>
		/// <remarks>
		/// True if this replica has been purged from the cache; false otherwise.
		/// Protected by the cache lock.
		/// </remarks>
		internal bool purged = false;

		/// <summary>Number of external references to this replica.</summary>
		/// <remarks>
		/// Number of external references to this replica.  Replicas are referenced
		/// by the cache, BlockReaderLocal instances, and by ClientMmap instances.
		/// The number starts at 2 because when we create a replica, it is referenced
		/// by both the cache and the requester.
		/// Protected by the cache lock.
		/// </remarks>
		internal int refCount = 2;

		/// <summary>
		/// The monotonic time in nanoseconds at which the replica became evictable, or
		/// null if it is not evictable.
		/// </summary>
		/// <remarks>
		/// The monotonic time in nanoseconds at which the replica became evictable, or
		/// null if it is not evictable.
		/// Protected by the cache lock.
		/// </remarks>
		private long evictableTimeNs = null;

		/// <exception cref="System.IO.IOException"/>
		public ShortCircuitReplica(ExtendedBlockId key, FileInputStream dataStream, FileInputStream
			 metaStream, ShortCircuitCache cache, long creationTimeMs, ShortCircuitShm.Slot 
			slot)
		{
			this.key = key;
			this.dataStream = dataStream;
			this.metaStream = metaStream;
			this.metaHeader = BlockMetadataHeader.PreadHeader(metaStream.GetChannel());
			if (metaHeader.GetVersion() != 1)
			{
				throw new IOException("invalid metadata header version " + metaHeader.GetVersion(
					) + ".  Can only handle version 1.");
			}
			this.cache = cache;
			this.creationTimeMs = creationTimeMs;
			this.slot = slot;
		}

		/// <summary>Decrement the reference count.</summary>
		public virtual void Unref()
		{
			cache.Unref(this);
		}

		/// <summary>Check if the replica is stale.</summary>
		/// <remarks>
		/// Check if the replica is stale.
		/// Must be called with the cache lock held.
		/// </remarks>
		internal virtual bool IsStale()
		{
			if (slot != null)
			{
				// Check staleness by looking at the shared memory area we use to
				// communicate with the DataNode.
				bool stale = !slot.IsValid();
				if (Log.IsTraceEnabled())
				{
					Log.Trace(this + ": checked shared memory segment.  isStale=" + stale);
				}
				return stale;
			}
			else
			{
				// Fall back to old, time-based staleness method.
				long deltaMs = Time.MonotonicNow() - creationTimeMs;
				long staleThresholdMs = cache.GetStaleThresholdMs();
				if (deltaMs > staleThresholdMs)
				{
					if (Log.IsTraceEnabled())
					{
						Log.Trace(this + " is stale because it's " + deltaMs + " ms old, and staleThresholdMs = "
							 + staleThresholdMs);
					}
					return true;
				}
				else
				{
					if (Log.IsTraceEnabled())
					{
						Log.Trace(this + " is not stale because it's only " + deltaMs + " ms old, and staleThresholdMs = "
							 + staleThresholdMs);
					}
					return false;
				}
			}
		}

		/// <summary>Try to add a no-checksum anchor to our shared memory slot.</summary>
		/// <remarks>
		/// Try to add a no-checksum anchor to our shared memory slot.
		/// It is only possible to add this anchor when the block is mlocked on the Datanode.
		/// The DataNode will not munlock the block until the number of no-checksum anchors
		/// for the block reaches zero.
		/// This method does not require any synchronization.
		/// </remarks>
		/// <returns>True if we successfully added a no-checksum anchor.</returns>
		public virtual bool AddNoChecksumAnchor()
		{
			if (slot == null)
			{
				return false;
			}
			bool result = slot.AddAnchor();
			if (Log.IsTraceEnabled())
			{
				if (result)
				{
					Log.Trace(this + ": added no-checksum anchor to slot " + slot);
				}
				else
				{
					Log.Trace(this + ": could not add no-checksum anchor to slot " + slot);
				}
			}
			return result;
		}

		/// <summary>Remove a no-checksum anchor for our shared memory slot.</summary>
		/// <remarks>
		/// Remove a no-checksum anchor for our shared memory slot.
		/// This method does not require any synchronization.
		/// </remarks>
		public virtual void RemoveNoChecksumAnchor()
		{
			if (slot != null)
			{
				slot.RemoveAnchor();
			}
		}

		/// <summary>Check if the replica has an associated mmap that has been fully loaded.</summary>
		/// <remarks>
		/// Check if the replica has an associated mmap that has been fully loaded.
		/// Must be called with the cache lock held.
		/// </remarks>
		[VisibleForTesting]
		public virtual bool HasMmap()
		{
			return ((mmapData != null) && (mmapData is MappedByteBuffer));
		}

		/// <summary>Free the mmap associated with this replica.</summary>
		/// <remarks>
		/// Free the mmap associated with this replica.
		/// Must be called with the cache lock held.
		/// </remarks>
		internal virtual void Munmap()
		{
			MappedByteBuffer mmap = (MappedByteBuffer)mmapData;
			NativeIO.POSIX.Munmap(mmap);
			mmapData = null;
		}

		/// <summary>Close the replica.</summary>
		/// <remarks>
		/// Close the replica.
		/// Must be called after there are no more references to the replica in the
		/// cache or elsewhere.
		/// </remarks>
		internal virtual void Close()
		{
			string suffix = string.Empty;
			Preconditions.CheckState(refCount == 0, "tried to close replica with refCount %d: %s"
				, refCount, this);
			refCount = -1;
			Preconditions.CheckState(purged, "tried to close unpurged replica %s", this);
			if (HasMmap())
			{
				Munmap();
				if (Log.IsTraceEnabled())
				{
					suffix += "  munmapped.";
				}
			}
			IOUtils.Cleanup(Log, dataStream, metaStream);
			if (slot != null)
			{
				cache.ScheduleSlotReleaser(slot);
				if (Log.IsTraceEnabled())
				{
					suffix += "  scheduling " + slot + " for later release.";
				}
			}
			if (Log.IsTraceEnabled())
			{
				Log.Trace("closed " + this + suffix);
			}
		}

		public virtual FileInputStream GetDataStream()
		{
			return dataStream;
		}

		public virtual FileInputStream GetMetaStream()
		{
			return metaStream;
		}

		public virtual BlockMetadataHeader GetMetaHeader()
		{
			return metaHeader;
		}

		public virtual ExtendedBlockId GetKey()
		{
			return key;
		}

		public virtual ClientMmap GetOrCreateClientMmap(bool anchor)
		{
			return cache.GetOrCreateClientMmap(this, anchor);
		}

		internal virtual MappedByteBuffer LoadMmapInternal()
		{
			try
			{
				FileChannel channel = dataStream.GetChannel();
				MappedByteBuffer mmap = channel.Map(FileChannel.MapMode.ReadOnly, 0, Math.Min(int.MaxValue
					, channel.Size()));
				if (Log.IsTraceEnabled())
				{
					Log.Trace(this + ": created mmap of size " + channel.Size());
				}
				return mmap;
			}
			catch (IOException e)
			{
				Log.Warn(this + ": mmap error", e);
				return null;
			}
			catch (RuntimeException e)
			{
				Log.Warn(this + ": mmap error", e);
				return null;
			}
		}

		/// <summary>Get the evictable time in nanoseconds.</summary>
		/// <remarks>
		/// Get the evictable time in nanoseconds.
		/// Note: you must hold the cache lock to call this function.
		/// </remarks>
		/// <returns>the evictable time in nanoseconds.</returns>
		public virtual long GetEvictableTimeNs()
		{
			return evictableTimeNs;
		}

		/// <summary>Set the evictable time in nanoseconds.</summary>
		/// <remarks>
		/// Set the evictable time in nanoseconds.
		/// Note: you must hold the cache lock to call this function.
		/// </remarks>
		/// <param name="evictableTimeNs">
		/// The evictable time in nanoseconds, or null
		/// to set no evictable time.
		/// </param>
		internal virtual void SetEvictableTimeNs(long evictableTimeNs)
		{
			this.evictableTimeNs = evictableTimeNs;
		}

		[VisibleForTesting]
		public virtual ShortCircuitShm.Slot GetSlot()
		{
			return slot;
		}

		/// <summary>Convert the replica to a string for debugging purposes.</summary>
		/// <remarks>
		/// Convert the replica to a string for debugging purposes.
		/// Note that we can't take the lock here.
		/// </remarks>
		public override string ToString()
		{
			return new StringBuilder().Append("ShortCircuitReplica{").Append("key=").Append(key
				).Append(", metaHeader.version=").Append(metaHeader.GetVersion()).Append(", metaHeader.checksum="
				).Append(metaHeader.GetChecksum()).Append(", ident=").Append("0x").Append(Sharpen.Extensions.ToHexString
				(Runtime.IdentityHashCode(this))).Append(", creationTimeMs=").Append(creationTimeMs
				).Append("}").ToString();
		}
	}
}
