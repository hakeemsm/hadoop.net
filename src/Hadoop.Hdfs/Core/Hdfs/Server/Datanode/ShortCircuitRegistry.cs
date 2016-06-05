using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Org.Apache.Commons.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Shortcircuit;
using Org.Apache.Hadoop.IO.Nativeio;
using Org.Apache.Hadoop.Net.Unix;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode
{
	/// <summary>Manages client short-circuit memory segments on the DataNode.</summary>
	/// <remarks>
	/// Manages client short-circuit memory segments on the DataNode.
	/// DFSClients request shared memory segments from the DataNode.  The
	/// ShortCircuitRegistry generates and manages these segments.  Each segment
	/// has a randomly generated 128-bit ID which uniquely identifies it.  The
	/// segments each contain several "slots."
	/// Before performing a short-circuit read, DFSClients must request a pair of
	/// file descriptors from the DataNode via the REQUEST_SHORT_CIRCUIT_FDS
	/// operation.  As part of this operation, DFSClients pass the ID of the shared
	/// memory segment they would like to use to communicate information about this
	/// replica, as well as the slot number within that segment they would like to
	/// use.  Slot allocation is always done by the client.
	/// Slots are used to track the state of the block on the both the client and
	/// datanode. When this DataNode mlocks a block, the corresponding slots for the
	/// replicas are marked as "anchorable".  Anchorable blocks can be safely read
	/// without verifying the checksum.  This means that BlockReaderLocal objects
	/// using these replicas can skip checksumming.  It also means that we can do
	/// zero-copy reads on these replicas (the ZCR interface has no way of
	/// verifying checksums.)
	/// When a DN needs to munlock a block, it needs to first wait for the block to
	/// be unanchored by clients doing a no-checksum read or a zero-copy read. The
	/// DN also marks the block's slots as "unanchorable" to prevent additional
	/// clients from initiating these operations in the future.
	/// The counterpart of this class on the client is
	/// <see cref="DfsClientShmManager"/>
	/// .
	/// </remarks>
	public class ShortCircuitRegistry
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.Server.Datanode.ShortCircuitRegistry
			));

		private const int ShmLength = 8192;

		public class RegisteredShm : ShortCircuitShm, DomainSocketWatcher.Handler
		{
			private readonly string clientName;

			private readonly ShortCircuitRegistry registry;

			/// <exception cref="System.IO.IOException"/>
			internal RegisteredShm(string clientName, ShortCircuitShm.ShmId shmId, FileInputStream
				 stream, ShortCircuitRegistry registry)
				: base(shmId, stream)
			{
				this.clientName = clientName;
				this.registry = registry;
			}

			public virtual bool Handle(DomainSocket sock)
			{
				lock (registry)
				{
					lock (this)
					{
						registry.RemoveShm(this);
					}
				}
				return true;
			}

			internal virtual string GetClientName()
			{
				return clientName;
			}
		}

		public virtual void RemoveShm(ShortCircuitShm shm)
		{
			lock (this)
			{
				if (Log.IsTraceEnabled())
				{
					Log.Debug("removing shm " + shm);
				}
				// Stop tracking the shmId.
				ShortCircuitRegistry.RegisteredShm removedShm = Sharpen.Collections.Remove(segments
					, shm.GetShmId());
				Preconditions.CheckState(removedShm == shm, "failed to remove " + shm.GetShmId());
				// Stop tracking the slots.
				for (IEnumerator<ShortCircuitShm.Slot> iter = shm.SlotIterator(); iter.HasNext(); )
				{
					ShortCircuitShm.Slot slot = iter.Next();
					bool removed = slots.Remove(slot.GetBlockId(), slot);
					Preconditions.CheckState(removed);
					slot.MakeInvalid();
				}
				// De-allocate the memory map and close the shared file. 
				shm.Free();
			}
		}

		/// <summary>Whether or not the registry is enabled.</summary>
		private bool enabled;

		/// <summary>The factory which creates shared file descriptors.</summary>
		private readonly SharedFileDescriptorFactory shmFactory;

		/// <summary>
		/// A watcher which sends out callbacks when the UNIX domain socket
		/// associated with a shared memory segment closes.
		/// </summary>
		private readonly DomainSocketWatcher watcher;

		private readonly Dictionary<ShortCircuitShm.ShmId, ShortCircuitRegistry.RegisteredShm
			> segments = new Dictionary<ShortCircuitShm.ShmId, ShortCircuitRegistry.RegisteredShm
			>(0);

		private readonly HashMultimap<ExtendedBlockId, ShortCircuitShm.Slot> slots = HashMultimap
			.Create(0, 1);

		/// <exception cref="System.IO.IOException"/>
		public ShortCircuitRegistry(Configuration conf)
		{
			bool enabled = false;
			SharedFileDescriptorFactory shmFactory = null;
			DomainSocketWatcher watcher = null;
			try
			{
				int interruptCheck = conf.GetInt(DFSConfigKeys.DfsShortCircuitSharedMemoryWatcherInterruptCheckMs
					, DFSConfigKeys.DfsShortCircuitSharedMemoryWatcherInterruptCheckMsDefault);
				if (interruptCheck <= 0)
				{
					throw new IOException(DFSConfigKeys.DfsShortCircuitSharedMemoryWatcherInterruptCheckMs
						 + " was set to " + interruptCheck);
				}
				string[] shmPaths = conf.GetTrimmedStrings(DFSConfigKeys.DfsDatanodeSharedFileDescriptorPaths
					);
				if (shmPaths.Length == 0)
				{
					shmPaths = DFSConfigKeys.DfsDatanodeSharedFileDescriptorPathsDefault.Split(",");
				}
				shmFactory = SharedFileDescriptorFactory.Create("HadoopShortCircuitShm_", shmPaths
					);
				string dswLoadingFailure = DomainSocketWatcher.GetLoadingFailureReason();
				if (dswLoadingFailure != null)
				{
					throw new IOException(dswLoadingFailure);
				}
				watcher = new DomainSocketWatcher(interruptCheck, "datanode");
				enabled = true;
				if (Log.IsDebugEnabled())
				{
					Log.Debug("created new ShortCircuitRegistry with interruptCheck=" + interruptCheck
						 + ", shmPath=" + shmFactory.GetPath());
				}
			}
			catch (IOException e)
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Disabling ShortCircuitRegistry", e);
				}
			}
			finally
			{
				this.enabled = enabled;
				this.shmFactory = shmFactory;
				this.watcher = watcher;
			}
		}

		/// <summary>Process a block mlock event from the FsDatasetCache.</summary>
		/// <param name="blockId">The block that was mlocked.</param>
		public virtual void ProcessBlockMlockEvent(ExtendedBlockId blockId)
		{
			lock (this)
			{
				if (!enabled)
				{
					return;
				}
				ICollection<ShortCircuitShm.Slot> affectedSlots = ((ICollection<ShortCircuitShm.Slot
					>)slots.Get(blockId));
				foreach (ShortCircuitShm.Slot slot in affectedSlots)
				{
					slot.MakeAnchorable();
				}
			}
		}

		/// <summary>Mark any slots associated with this blockId as unanchorable.</summary>
		/// <param name="blockId">The block ID.</param>
		/// <returns>True if we should allow the munlock request.</returns>
		public virtual bool ProcessBlockMunlockRequest(ExtendedBlockId blockId)
		{
			lock (this)
			{
				if (!enabled)
				{
					return true;
				}
				bool allowMunlock = true;
				ICollection<ShortCircuitShm.Slot> affectedSlots = ((ICollection<ShortCircuitShm.Slot
					>)slots.Get(blockId));
				foreach (ShortCircuitShm.Slot slot in affectedSlots)
				{
					slot.MakeUnanchorable();
					if (slot.IsAnchored())
					{
						allowMunlock = false;
					}
				}
				return allowMunlock;
			}
		}

		/// <summary>
		/// Invalidate any slot associated with a blockId that we are invalidating
		/// (deleting) from this DataNode.
		/// </summary>
		/// <remarks>
		/// Invalidate any slot associated with a blockId that we are invalidating
		/// (deleting) from this DataNode.  When a slot is invalid, the DFSClient will
		/// not use the corresponding replica for new read or mmap operations (although
		/// existing, ongoing read or mmap operations will complete.)
		/// </remarks>
		/// <param name="blockId">The block ID.</param>
		public virtual void ProcessBlockInvalidation(ExtendedBlockId blockId)
		{
			lock (this)
			{
				if (!enabled)
				{
					return;
				}
				ICollection<ShortCircuitShm.Slot> affectedSlots = ((ICollection<ShortCircuitShm.Slot
					>)slots.Get(blockId));
				if (!affectedSlots.IsEmpty())
				{
					StringBuilder bld = new StringBuilder();
					string prefix = string.Empty;
					bld.Append("Block ").Append(blockId).Append(" has been invalidated.  ").Append("Marking short-circuit slots as invalid: "
						);
					foreach (ShortCircuitShm.Slot slot in affectedSlots)
					{
						slot.MakeInvalid();
						bld.Append(prefix).Append(slot.ToString());
						prefix = ", ";
					}
					Log.Info(bld.ToString());
				}
			}
		}

		public virtual string GetClientNames(ExtendedBlockId blockId)
		{
			lock (this)
			{
				if (!enabled)
				{
					return string.Empty;
				}
				HashSet<string> clientNames = new HashSet<string>();
				ICollection<ShortCircuitShm.Slot> affectedSlots = ((ICollection<ShortCircuitShm.Slot
					>)slots.Get(blockId));
				foreach (ShortCircuitShm.Slot slot in affectedSlots)
				{
					clientNames.AddItem(((ShortCircuitRegistry.RegisteredShm)slot.GetShm()).GetClientName
						());
				}
				return Joiner.On(",").Join(clientNames);
			}
		}

		public class NewShmInfo : IDisposable
		{
			public readonly ShortCircuitShm.ShmId shmId;

			public readonly FileInputStream stream;

			internal NewShmInfo(ShortCircuitShm.ShmId shmId, FileInputStream stream)
			{
				this.shmId = shmId;
				this.stream = stream;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Close()
			{
				stream.Close();
			}
		}

		/// <summary>Handle a DFSClient request to create a new memory segment.</summary>
		/// <param name="clientName">Client name as reported by the client.</param>
		/// <param name="sock">
		/// The DomainSocket to associate with this memory
		/// segment.  When this socket is closed, or the
		/// other side writes anything to the socket, the
		/// segment will be closed.  This can happen at any
		/// time, including right after this function returns.
		/// </param>
		/// <returns>
		/// A NewShmInfo object.  The caller must close the
		/// NewShmInfo object once they are done with it.
		/// </returns>
		/// <exception cref="System.IO.IOException">If the new memory segment could not be created.
		/// 	</exception>
		public virtual ShortCircuitRegistry.NewShmInfo CreateNewMemorySegment(string clientName
			, DomainSocket sock)
		{
			ShortCircuitRegistry.NewShmInfo info = null;
			ShortCircuitRegistry.RegisteredShm shm = null;
			ShortCircuitShm.ShmId shmId = null;
			lock (this)
			{
				if (!enabled)
				{
					if (Log.IsTraceEnabled())
					{
						Log.Trace("createNewMemorySegment: ShortCircuitRegistry is " + "not enabled.");
					}
					throw new NotSupportedException();
				}
				FileInputStream fis = null;
				try
				{
					do
					{
						shmId = ShortCircuitShm.ShmId.CreateRandom();
					}
					while (segments.Contains(shmId));
					fis = shmFactory.CreateDescriptor(clientName, ShmLength);
					shm = new ShortCircuitRegistry.RegisteredShm(clientName, shmId, fis, this);
				}
				finally
				{
					if (shm == null)
					{
						IOUtils.CloseQuietly(fis);
					}
				}
				info = new ShortCircuitRegistry.NewShmInfo(shmId, fis);
				segments[shmId] = shm;
			}
			// Drop the registry lock to prevent deadlock.
			// After this point, RegisteredShm#handle may be called at any time.
			watcher.Add(sock, shm);
			if (Log.IsTraceEnabled())
			{
				Log.Trace("createNewMemorySegment: created " + info.shmId);
			}
			return info;
		}

		/// <exception cref="Org.Apache.Hadoop.FS.InvalidRequestException"/>
		public virtual void RegisterSlot(ExtendedBlockId blockId, ShortCircuitShm.SlotId 
			slotId, bool isCached)
		{
			lock (this)
			{
				if (!enabled)
				{
					if (Log.IsTraceEnabled())
					{
						Log.Trace(this + " can't register a slot because the " + "ShortCircuitRegistry is not enabled."
							);
					}
					throw new NotSupportedException();
				}
				ShortCircuitShm.ShmId shmId = slotId.GetShmId();
				ShortCircuitRegistry.RegisteredShm shm = segments[shmId];
				if (shm == null)
				{
					throw new InvalidRequestException("there is no shared memory segment " + "registered with shmId "
						 + shmId);
				}
				ShortCircuitShm.Slot slot = shm.RegisterSlot(slotId.GetSlotIdx(), blockId);
				if (isCached)
				{
					slot.MakeAnchorable();
				}
				else
				{
					slot.MakeUnanchorable();
				}
				bool added = slots.Put(blockId, slot);
				Preconditions.CheckState(added);
				if (Log.IsTraceEnabled())
				{
					Log.Trace(this + ": registered " + blockId + " with slot " + slotId + " (isCached="
						 + isCached + ")");
				}
			}
		}

		/// <exception cref="Org.Apache.Hadoop.FS.InvalidRequestException"/>
		public virtual void UnregisterSlot(ShortCircuitShm.SlotId slotId)
		{
			lock (this)
			{
				if (!enabled)
				{
					if (Log.IsTraceEnabled())
					{
						Log.Trace("unregisterSlot: ShortCircuitRegistry is " + "not enabled.");
					}
					throw new NotSupportedException();
				}
				ShortCircuitShm.ShmId shmId = slotId.GetShmId();
				ShortCircuitRegistry.RegisteredShm shm = segments[shmId];
				if (shm == null)
				{
					throw new InvalidRequestException("there is no shared memory segment " + "registered with shmId "
						 + shmId);
				}
				ShortCircuitShm.Slot slot = shm.GetSlot(slotId.GetSlotIdx());
				slot.MakeInvalid();
				shm.UnregisterSlot(slotId.GetSlotIdx());
				slots.Remove(slot.GetBlockId(), slot);
			}
		}

		public virtual void Shutdown()
		{
			lock (this)
			{
				if (!enabled)
				{
					return;
				}
				enabled = false;
			}
			IOUtils.CloseQuietly(watcher);
		}

		public interface Visitor
		{
			void Accept(Dictionary<ShortCircuitShm.ShmId, ShortCircuitRegistry.RegisteredShm>
				 segments, HashMultimap<ExtendedBlockId, ShortCircuitShm.Slot> slots);
		}

		[VisibleForTesting]
		public virtual void Visit(ShortCircuitRegistry.Visitor visitor)
		{
			lock (this)
			{
				visitor.Accept(segments, slots);
			}
		}
	}
}
