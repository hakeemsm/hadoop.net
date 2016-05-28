using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Base;
using Org.Apache.Hadoop.Hdfs.Net;
using Org.Apache.Hadoop.Net.Unix;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Shortcircuit
{
	/// <summary>
	/// DfsClientShm is a subclass of ShortCircuitShm which is used by the
	/// DfsClient.
	/// </summary>
	/// <remarks>
	/// DfsClientShm is a subclass of ShortCircuitShm which is used by the
	/// DfsClient.
	/// When the UNIX domain socket associated with this shared memory segment
	/// closes unexpectedly, we mark the slots inside this segment as disconnected.
	/// ShortCircuitReplica objects that contain disconnected slots are stale,
	/// and will not be used to service new reads or mmap operations.
	/// However, in-progress read or mmap operations will continue to proceed.
	/// Once the last slot is deallocated, the segment can be safely munmapped.
	/// Slots may also become stale because the associated replica has been deleted
	/// on the DataNode.  In this case, the DataNode will clear the 'valid' bit.
	/// The client will then see these slots as stale (see
	/// #{ShortCircuitReplica#isStale}).
	/// </remarks>
	public class DfsClientShm : ShortCircuitShm, DomainSocketWatcher.Handler
	{
		/// <summary>The EndpointShmManager associated with this shared memory segment.</summary>
		private readonly DfsClientShmManager.EndpointShmManager manager;

		/// <summary>The UNIX domain socket associated with this DfsClientShm.</summary>
		/// <remarks>
		/// The UNIX domain socket associated with this DfsClientShm.
		/// We rely on the DomainSocketWatcher to close the socket associated with
		/// this DomainPeer when necessary.
		/// </remarks>
		private readonly DomainPeer peer;

		/// <summary>
		/// True if this shared memory segment has lost its connection to the
		/// DataNode.
		/// </summary>
		/// <remarks>
		/// True if this shared memory segment has lost its connection to the
		/// DataNode.
		/// <see cref="Handle(Org.Apache.Hadoop.Net.Unix.DomainSocket)"/>
		/// sets this to true.
		/// </remarks>
		private bool disconnected = false;

		/// <exception cref="System.IO.IOException"/>
		internal DfsClientShm(ShortCircuitShm.ShmId shmId, FileInputStream stream, DfsClientShmManager.EndpointShmManager
			 manager, DomainPeer peer)
			: base(shmId, stream)
		{
			this.manager = manager;
			this.peer = peer;
		}

		public virtual DfsClientShmManager.EndpointShmManager GetEndpointShmManager()
		{
			return manager;
		}

		public virtual DomainPeer GetPeer()
		{
			return peer;
		}

		/// <summary>Determine if the shared memory segment is disconnected from the DataNode.
		/// 	</summary>
		/// <remarks>
		/// Determine if the shared memory segment is disconnected from the DataNode.
		/// This must be called with the DfsClientShmManager lock held.
		/// </remarks>
		/// <returns>True if the shared memory segment is stale.</returns>
		public virtual bool IsDisconnected()
		{
			lock (this)
			{
				return disconnected;
			}
		}

		/// <summary>
		/// Handle the closure of the UNIX domain socket associated with this shared
		/// memory segment by marking this segment as stale.
		/// </summary>
		/// <remarks>
		/// Handle the closure of the UNIX domain socket associated with this shared
		/// memory segment by marking this segment as stale.
		/// If there are no slots associated with this shared memory segment, it will
		/// be freed immediately in this function.
		/// </remarks>
		public virtual bool Handle(DomainSocket sock)
		{
			manager.UnregisterShm(GetShmId());
			lock (this)
			{
				Preconditions.CheckState(!disconnected);
				disconnected = true;
				bool hadSlots = false;
				for (IEnumerator<ShortCircuitShm.Slot> iter = SlotIterator(); iter.HasNext(); )
				{
					ShortCircuitShm.Slot slot = iter.Next();
					slot.MakeInvalid();
					hadSlots = true;
				}
				if (!hadSlots)
				{
					Free();
				}
			}
			return true;
		}
	}
}
