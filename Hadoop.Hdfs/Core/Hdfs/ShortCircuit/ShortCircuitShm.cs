using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Com.Google.Common.Primitives;
using Org.Apache.Commons.Lang.Builder;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.IO.Nativeio;
using Org.Apache.Hadoop.Util;
using Sharpen;
using Sun.Misc;

namespace Org.Apache.Hadoop.Hdfs.Shortcircuit
{
	/// <summary>A shared memory segment used to implement short-circuit reads.</summary>
	public class ShortCircuitShm
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.Shortcircuit.ShortCircuitShm
			));

		protected internal const int BytesPerSlot = 64;

		private static readonly Unsafe @unsafe = SafetyDance();

		private static Unsafe SafetyDance()
		{
			try
			{
				FieldInfo f = Sharpen.Runtime.GetDeclaredField(typeof(Unsafe), "theUnsafe");
				return (Unsafe)f.GetValue(null);
			}
			catch (Exception e)
			{
				Log.Error("failed to load misc.Unsafe", e);
			}
			return null;
		}

		/// <summary>Calculate the usable size of a shared memory segment.</summary>
		/// <remarks>
		/// Calculate the usable size of a shared memory segment.
		/// We round down to a multiple of the slot size and do some validation.
		/// </remarks>
		/// <param name="stream">The stream we're using.</param>
		/// <returns>The usable size of the shared memory segment.</returns>
		/// <exception cref="System.IO.IOException"/>
		private static int GetUsableLength(FileInputStream stream)
		{
			int intSize = Ints.CheckedCast(stream.GetChannel().Size());
			int slots = intSize / BytesPerSlot;
			if (slots == 0)
			{
				throw new IOException("size of shared memory segment was " + intSize + ", but that is not enough to hold even one slot."
					);
			}
			return slots * BytesPerSlot;
		}

		/// <summary>Identifies a DfsClientShm.</summary>
		public class ShmId : Comparable<ShortCircuitShm.ShmId>
		{
			private static readonly Random random = new Random();

			private readonly long hi;

			private readonly long lo;

			/// <summary>Generate a random ShmId.</summary>
			/// <remarks>
			/// Generate a random ShmId.
			/// We generate ShmIds randomly to prevent a malicious client from
			/// successfully guessing one and using that to interfere with another
			/// client.
			/// </remarks>
			public static ShortCircuitShm.ShmId CreateRandom()
			{
				return new ShortCircuitShm.ShmId(random.NextLong(), random.NextLong());
			}

			public ShmId(long hi, long lo)
			{
				this.hi = hi;
				this.lo = lo;
			}

			public virtual long GetHi()
			{
				return hi;
			}

			public virtual long GetLo()
			{
				return lo;
			}

			public override bool Equals(object o)
			{
				if ((o == null) || (o.GetType() != this.GetType()))
				{
					return false;
				}
				ShortCircuitShm.ShmId other = (ShortCircuitShm.ShmId)o;
				return new EqualsBuilder().Append(hi, other.hi).Append(lo, other.lo).IsEquals();
			}

			public override int GetHashCode()
			{
				return new HashCodeBuilder().Append(this.hi).Append(this.lo).ToHashCode();
			}

			public override string ToString()
			{
				return string.Format("%016x%016x", hi, lo);
			}

			public virtual int CompareTo(ShortCircuitShm.ShmId other)
			{
				return ComparisonChain.Start().Compare(hi, other.hi).Compare(lo, other.lo).Result
					();
			}
		}

		/// <summary>Uniquely identifies a slot.</summary>
		public class SlotId
		{
			private readonly ShortCircuitShm.ShmId shmId;

			private readonly int slotIdx;

			public SlotId(ShortCircuitShm.ShmId shmId, int slotIdx)
			{
				this.shmId = shmId;
				this.slotIdx = slotIdx;
			}

			public virtual ShortCircuitShm.ShmId GetShmId()
			{
				return shmId;
			}

			public virtual int GetSlotIdx()
			{
				return slotIdx;
			}

			public override bool Equals(object o)
			{
				if ((o == null) || (o.GetType() != this.GetType()))
				{
					return false;
				}
				ShortCircuitShm.SlotId other = (ShortCircuitShm.SlotId)o;
				return new EqualsBuilder().Append(shmId, other.shmId).Append(slotIdx, other.slotIdx
					).IsEquals();
			}

			public override int GetHashCode()
			{
				return new HashCodeBuilder().Append(this.shmId).Append(this.slotIdx).ToHashCode();
			}

			public override string ToString()
			{
				return string.Format("SlotId(%s:%d)", shmId.ToString(), slotIdx);
			}
		}

		public class SlotIterator : IEnumerator<ShortCircuitShm.Slot>
		{
			internal int slotIdx = -1;

			public override bool HasNext()
			{
				lock (this._enclosing)
				{
					return this._enclosing.allocatedSlots.NextSetBit(this.slotIdx + 1) != -1;
				}
			}

			public override ShortCircuitShm.Slot Next()
			{
				lock (this._enclosing)
				{
					int nextSlotIdx = this._enclosing.allocatedSlots.NextSetBit(this.slotIdx + 1);
					if (nextSlotIdx == -1)
					{
						throw new NoSuchElementException();
					}
					this.slotIdx = nextSlotIdx;
					return this._enclosing.slots[nextSlotIdx];
				}
			}

			public override void Remove()
			{
				throw new NotSupportedException("SlotIterator " + "doesn't support removal");
			}

			internal SlotIterator(ShortCircuitShm _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly ShortCircuitShm _enclosing;
		}

		/// <summary>A slot containing information about a replica.</summary>
		/// <remarks>
		/// A slot containing information about a replica.
		/// The format is:
		/// word 0
		/// bit 0:32   Slot flags (see below).
		/// bit 33:63  Anchor count.
		/// word 1:7
		/// Reserved for future use, such as statistics.
		/// Padding is also useful for avoiding false sharing.
		/// Little-endian versus big-endian is not relevant here since both the client
		/// and the server reside on the same computer and use the same orientation.
		/// </remarks>
		public class Slot
		{
			/// <summary>Flag indicating that the slot is valid.</summary>
			/// <remarks>
			/// Flag indicating that the slot is valid.
			/// The DFSClient sets this flag when it allocates a new slot within one of
			/// its shared memory regions.
			/// The DataNode clears this flag when the replica associated with this slot
			/// is no longer valid.  The client itself also clears this flag when it
			/// believes that the DataNode is no longer using this slot to communicate.
			/// </remarks>
			private const long ValidFlag = 1L << 63;

			/// <summary>Flag indicating that the slot can be anchored.</summary>
			private const long AnchorableFlag = 1L << 62;

			/// <summary>The slot address in memory.</summary>
			private readonly long slotAddress;

			/// <summary>BlockId of the block this slot is used for.</summary>
			private readonly ExtendedBlockId blockId;

			internal Slot(ShortCircuitShm _enclosing, long slotAddress, ExtendedBlockId blockId
				)
			{
				this._enclosing = _enclosing;
				this.slotAddress = slotAddress;
				this.blockId = blockId;
			}

			/// <summary>Get the short-circuit memory segment associated with this Slot.</summary>
			/// <returns>The enclosing short-circuit memory segment.</returns>
			public virtual ShortCircuitShm GetShm()
			{
				return this._enclosing;
			}

			/// <summary>Get the ExtendedBlockId associated with this slot.</summary>
			/// <returns>The ExtendedBlockId of this slot.</returns>
			public virtual ExtendedBlockId GetBlockId()
			{
				return this.blockId;
			}

			/// <summary>Get the SlotId of this slot, containing both shmId and slotIdx.</summary>
			/// <returns>The SlotId of this slot.</returns>
			public virtual ShortCircuitShm.SlotId GetSlotId()
			{
				return new ShortCircuitShm.SlotId(this._enclosing.GetShmId(), this.GetSlotIdx());
			}

			/// <summary>Get the Slot index.</summary>
			/// <returns>The index of this slot.</returns>
			public virtual int GetSlotIdx()
			{
				return Ints.CheckedCast((this.slotAddress - this._enclosing.baseAddress) / ShortCircuitShm
					.BytesPerSlot);
			}

			/// <summary>Clear the slot.</summary>
			internal virtual void Clear()
			{
				ShortCircuitShm.@unsafe.PutLongVolatile(null, this.slotAddress, 0);
			}

			private bool IsSet(long flag)
			{
				long prev = ShortCircuitShm.@unsafe.GetLongVolatile(null, this.slotAddress);
				return (prev & flag) != 0;
			}

			private void SetFlag(long flag)
			{
				long prev;
				do
				{
					prev = ShortCircuitShm.@unsafe.GetLongVolatile(null, this.slotAddress);
					if ((prev & flag) != 0)
					{
						return;
					}
				}
				while (!ShortCircuitShm.@unsafe.CompareAndSwapLong(null, this.slotAddress, prev, 
					prev | flag));
			}

			private void ClearFlag(long flag)
			{
				long prev;
				do
				{
					prev = ShortCircuitShm.@unsafe.GetLongVolatile(null, this.slotAddress);
					if ((prev & flag) == 0)
					{
						return;
					}
				}
				while (!ShortCircuitShm.@unsafe.CompareAndSwapLong(null, this.slotAddress, prev, 
					prev & (~flag)));
			}

			public virtual bool IsValid()
			{
				return this.IsSet(ShortCircuitShm.Slot.ValidFlag);
			}

			public virtual void MakeValid()
			{
				this.SetFlag(ShortCircuitShm.Slot.ValidFlag);
			}

			public virtual void MakeInvalid()
			{
				this.ClearFlag(ShortCircuitShm.Slot.ValidFlag);
			}

			public virtual bool IsAnchorable()
			{
				return this.IsSet(ShortCircuitShm.Slot.AnchorableFlag);
			}

			public virtual void MakeAnchorable()
			{
				this.SetFlag(ShortCircuitShm.Slot.AnchorableFlag);
			}

			public virtual void MakeUnanchorable()
			{
				this.ClearFlag(ShortCircuitShm.Slot.AnchorableFlag);
			}

			public virtual bool IsAnchored()
			{
				long prev = ShortCircuitShm.@unsafe.GetLongVolatile(null, this.slotAddress);
				if ((prev & ShortCircuitShm.Slot.ValidFlag) == 0)
				{
					// Slot is no longer valid.
					return false;
				}
				return ((prev & unchecked((int)(0x7fffffff))) != 0);
			}

			/// <summary>Try to add an anchor for a given slot.</summary>
			/// <remarks>
			/// Try to add an anchor for a given slot.
			/// When a slot is anchored, we know that the block it refers to is resident
			/// in memory.
			/// </remarks>
			/// <returns>True if the slot is anchored.</returns>
			public virtual bool AddAnchor()
			{
				long prev;
				do
				{
					prev = ShortCircuitShm.@unsafe.GetLongVolatile(null, this.slotAddress);
					if ((prev & ShortCircuitShm.Slot.ValidFlag) == 0)
					{
						// Slot is no longer valid.
						return false;
					}
					if ((prev & ShortCircuitShm.Slot.AnchorableFlag) == 0)
					{
						// Slot can't be anchored right now.
						return false;
					}
					if ((prev & unchecked((int)(0x7fffffff))) == unchecked((int)(0x7fffffff)))
					{
						// Too many other threads have anchored the slot (2 billion?)
						return false;
					}
				}
				while (!ShortCircuitShm.@unsafe.CompareAndSwapLong(null, this.slotAddress, prev, 
					prev + 1));
				return true;
			}

			/// <summary>Remove an anchor for a given slot.</summary>
			public virtual void RemoveAnchor()
			{
				long prev;
				do
				{
					prev = ShortCircuitShm.@unsafe.GetLongVolatile(null, this.slotAddress);
					Preconditions.CheckState((prev & unchecked((int)(0x7fffffff))) != 0, "Tried to remove anchor for slot "
						 + this.slotAddress + ", which was " + "not anchored.");
				}
				while (!ShortCircuitShm.@unsafe.CompareAndSwapLong(null, this.slotAddress, prev, 
					prev - 1));
			}

			public override string ToString()
			{
				return "Slot(slotIdx=" + this.GetSlotIdx() + ", shm=" + this.GetShm() + ")";
			}

			private readonly ShortCircuitShm _enclosing;
		}

		/// <summary>ID for this SharedMemorySegment.</summary>
		private readonly ShortCircuitShm.ShmId shmId;

		/// <summary>The base address of the memory-mapped file.</summary>
		private readonly long baseAddress;

		/// <summary>The mmapped length of the shared memory segment</summary>
		private readonly int mmappedLength;

		/// <summary>The slots associated with this shared memory segment.</summary>
		/// <remarks>
		/// The slots associated with this shared memory segment.
		/// slot[i] contains the slot at offset i * BYTES_PER_SLOT,
		/// or null if that slot is not allocated.
		/// </remarks>
		private readonly ShortCircuitShm.Slot[] slots;

		/// <summary>A bitset where each bit represents a slot which is in use.</summary>
		private readonly BitSet allocatedSlots;

		/// <summary>Create the ShortCircuitShm.</summary>
		/// <param name="shmId">The ID to use.</param>
		/// <param name="stream">
		/// The stream that we're going to use to create this
		/// shared memory segment.
		/// Although this is a FileInputStream, we are going to
		/// assume that the underlying file descriptor is writable
		/// as well as readable. It would be more appropriate to use
		/// a RandomAccessFile here, but that class does not have
		/// any public accessor which returns a FileDescriptor,
		/// unlike FileInputStream.
		/// </param>
		/// <exception cref="System.IO.IOException"/>
		public ShortCircuitShm(ShortCircuitShm.ShmId shmId, FileInputStream stream)
		{
			if (!NativeIO.IsAvailable())
			{
				throw new NotSupportedException("NativeIO is not available.");
			}
			if (Shell.Windows)
			{
				throw new NotSupportedException("DfsClientShm is not yet implemented for Windows."
					);
			}
			if (@unsafe == null)
			{
				throw new NotSupportedException("can't use DfsClientShm because we failed to " + 
					"load misc.Unsafe.");
			}
			this.shmId = shmId;
			this.mmappedLength = GetUsableLength(stream);
			this.baseAddress = NativeIO.POSIX.Mmap(stream.GetFD(), NativeIO.POSIX.MmapProtRead
				 | NativeIO.POSIX.MmapProtWrite, true, mmappedLength);
			this.slots = new ShortCircuitShm.Slot[mmappedLength / BytesPerSlot];
			this.allocatedSlots = new BitSet(slots.Length);
			if (Log.IsTraceEnabled())
			{
				Log.Trace("creating " + this.GetType().Name + "(shmId=" + shmId + ", mmappedLength="
					 + mmappedLength + ", baseAddress=" + string.Format("%x", baseAddress) + ", slots.length="
					 + slots.Length + ")");
			}
		}

		public ShortCircuitShm.ShmId GetShmId()
		{
			return shmId;
		}

		/// <summary>Determine if this shared memory object is empty.</summary>
		/// <returns>True if the shared memory object is empty.</returns>
		public bool IsEmpty()
		{
			lock (this)
			{
				return allocatedSlots.NextSetBit(0) == -1;
			}
		}

		/// <summary>Determine if this shared memory object is full.</summary>
		/// <returns>True if the shared memory object is full.</returns>
		public bool IsFull()
		{
			lock (this)
			{
				return allocatedSlots.NextClearBit(0) >= slots.Length;
			}
		}

		/// <summary>Calculate the base address of a slot.</summary>
		/// <param name="slotIdx">Index of the slot.</param>
		/// <returns>The base address of the slot.</returns>
		private long CalculateSlotAddress(int slotIdx)
		{
			long offset = slotIdx;
			offset *= BytesPerSlot;
			return this.baseAddress + offset;
		}

		/// <summary>Allocate a new slot and register it.</summary>
		/// <remarks>
		/// Allocate a new slot and register it.
		/// This function chooses an empty slot, initializes it, and then returns
		/// the relevant Slot object.
		/// </remarks>
		/// <returns>The new slot.</returns>
		public ShortCircuitShm.Slot AllocAndRegisterSlot(ExtendedBlockId blockId)
		{
			lock (this)
			{
				int idx = allocatedSlots.NextClearBit(0);
				if (idx >= slots.Length)
				{
					throw new RuntimeException(this + ": no more slots are available.");
				}
				allocatedSlots.Set(idx, true);
				ShortCircuitShm.Slot slot = new ShortCircuitShm.Slot(this, CalculateSlotAddress(idx
					), blockId);
				slot.Clear();
				slot.MakeValid();
				slots[idx] = slot;
				if (Log.IsTraceEnabled())
				{
					Log.Trace(this + ": allocAndRegisterSlot " + idx + ": allocatedSlots=" + allocatedSlots
						 + StringUtils.GetStackTrace(Sharpen.Thread.CurrentThread()));
				}
				return slot;
			}
		}

		/// <exception cref="Org.Apache.Hadoop.FS.InvalidRequestException"/>
		public ShortCircuitShm.Slot GetSlot(int slotIdx)
		{
			lock (this)
			{
				if (!allocatedSlots.Get(slotIdx))
				{
					throw new InvalidRequestException(this + ": slot " + slotIdx + " does not exist."
						);
				}
				return slots[slotIdx];
			}
		}

		/// <summary>Register a slot.</summary>
		/// <remarks>
		/// Register a slot.
		/// This function looks at a slot which has already been initialized (by
		/// another process), and registers it with us.  Then, it returns the
		/// relevant Slot object.
		/// </remarks>
		/// <returns>The slot.</returns>
		/// <exception cref="Org.Apache.Hadoop.FS.InvalidRequestException">
		/// If the slot index we're trying to allocate has not been
		/// initialized, or is already in use.
		/// </exception>
		public ShortCircuitShm.Slot RegisterSlot(int slotIdx, ExtendedBlockId blockId)
		{
			lock (this)
			{
				if (slotIdx < 0)
				{
					throw new InvalidRequestException(this + ": invalid negative slot " + "index " + 
						slotIdx);
				}
				if (slotIdx >= slots.Length)
				{
					throw new InvalidRequestException(this + ": invalid slot " + "index " + slotIdx);
				}
				if (allocatedSlots.Get(slotIdx))
				{
					throw new InvalidRequestException(this + ": slot " + slotIdx + " is already in use."
						);
				}
				ShortCircuitShm.Slot slot = new ShortCircuitShm.Slot(this, CalculateSlotAddress(slotIdx
					), blockId);
				if (!slot.IsValid())
				{
					throw new InvalidRequestException(this + ": slot " + slotIdx + " is not marked as valid."
						);
				}
				slots[slotIdx] = slot;
				allocatedSlots.Set(slotIdx, true);
				if (Log.IsTraceEnabled())
				{
					Log.Trace(this + ": registerSlot " + slotIdx + ": allocatedSlots=" + allocatedSlots
						 + StringUtils.GetStackTrace(Sharpen.Thread.CurrentThread()));
				}
				return slot;
			}
		}

		/// <summary>Unregisters a slot.</summary>
		/// <remarks>
		/// Unregisters a slot.
		/// This doesn't alter the contents of the slot.  It just means
		/// </remarks>
		/// <param name="slotIdx">Index of the slot to unregister.</param>
		public void UnregisterSlot(int slotIdx)
		{
			lock (this)
			{
				Preconditions.CheckState(allocatedSlots.Get(slotIdx), "tried to unregister slot "
					 + slotIdx + ", which was not registered.");
				allocatedSlots.Set(slotIdx, false);
				slots[slotIdx] = null;
				if (Log.IsTraceEnabled())
				{
					Log.Trace(this + ": unregisterSlot " + slotIdx);
				}
			}
		}

		/// <summary>Iterate over all allocated slots.</summary>
		/// <remarks>
		/// Iterate over all allocated slots.
		/// Note that this method isn't safe if
		/// </remarks>
		/// <returns>The slot iterator.</returns>
		public virtual ShortCircuitShm.SlotIterator SlotIterator()
		{
			return new ShortCircuitShm.SlotIterator(this);
		}

		public virtual void Free()
		{
			try
			{
				NativeIO.POSIX.Munmap(baseAddress, mmappedLength);
			}
			catch (IOException e)
			{
				Log.Warn(this + ": failed to munmap", e);
			}
			Log.Trace(this + ": freed");
		}

		public override string ToString()
		{
			return this.GetType().Name + "(" + shmId + ")";
		}
	}
}
