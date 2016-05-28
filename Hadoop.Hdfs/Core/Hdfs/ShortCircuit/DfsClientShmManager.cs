using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Org.Apache.Commons.Lang.Mutable;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Net;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Protocol.Datatransfer;
using Org.Apache.Hadoop.Hdfs.Protocol.Proto;
using Org.Apache.Hadoop.Hdfs.ProtocolPB;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Net.Unix;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Shortcircuit
{
	/// <summary>Manages short-circuit memory segments for an HDFS client.</summary>
	/// <remarks>
	/// Manages short-circuit memory segments for an HDFS client.
	/// Clients are responsible for requesting and releasing shared memory segments used
	/// for communicating with the DataNode. The client will try to allocate new slots
	/// in the set of existing segments, falling back to getting a new segment from the
	/// DataNode via
	/// <see cref="Org.Apache.Hadoop.Hdfs.Protocol.Datatransfer.DataTransferProtocol.RequestShortCircuitFds(Org.Apache.Hadoop.Hdfs.Protocol.ExtendedBlock, Org.Apache.Hadoop.Security.Token.Token{T}, SlotId, int, bool)
	/// 	"/>
	/// .
	/// The counterpart to this class on the DataNode is
	/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Datanode.ShortCircuitRegistry"/>
	/// .
	/// See
	/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Datanode.ShortCircuitRegistry"/>
	/// for more information on the communication protocol.
	/// </remarks>
	public class DfsClientShmManager : IDisposable
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.Shortcircuit.DfsClientShmManager
			));

		/// <summary>Manages short-circuit memory segments that pertain to a given DataNode.</summary>
		internal class EndpointShmManager
		{
			/// <summary>The datanode we're managing.</summary>
			private readonly DatanodeInfo datanode;

			/// <summary>Shared memory segments which have no empty slots.</summary>
			/// <remarks>
			/// Shared memory segments which have no empty slots.
			/// Protected by the manager lock.
			/// </remarks>
			private readonly SortedDictionary<ShortCircuitShm.ShmId, DfsClientShm> full = new 
				SortedDictionary<ShortCircuitShm.ShmId, DfsClientShm>();

			/// <summary>Shared memory segments which have at least one empty slot.</summary>
			/// <remarks>
			/// Shared memory segments which have at least one empty slot.
			/// Protected by the manager lock.
			/// </remarks>
			private readonly SortedDictionary<ShortCircuitShm.ShmId, DfsClientShm> notFull = 
				new SortedDictionary<ShortCircuitShm.ShmId, DfsClientShm>();

			/// <summary>
			/// True if this datanode doesn't support short-circuit shared memory
			/// segments.
			/// </summary>
			/// <remarks>
			/// True if this datanode doesn't support short-circuit shared memory
			/// segments.
			/// Protected by the manager lock.
			/// </remarks>
			private bool disabled = false;

			/// <summary>
			/// True if we're in the process of loading a shared memory segment from
			/// this DataNode.
			/// </summary>
			/// <remarks>
			/// True if we're in the process of loading a shared memory segment from
			/// this DataNode.
			/// Protected by the manager lock.
			/// </remarks>
			private bool loading = false;

			internal EndpointShmManager(DfsClientShmManager _enclosing, DatanodeInfo datanode
				)
			{
				this._enclosing = _enclosing;
				this.datanode = datanode;
			}

			/// <summary>Pull a slot out of a preexisting shared memory segment.</summary>
			/// <remarks>
			/// Pull a slot out of a preexisting shared memory segment.
			/// Must be called with the manager lock held.
			/// </remarks>
			/// <param name="blockId">The blockId to put inside the Slot object.</param>
			/// <returns>
			/// null if none of our shared memory segments contain a
			/// free slot; the slot object otherwise.
			/// </returns>
			private ShortCircuitShm.Slot AllocSlotFromExistingShm(ExtendedBlockId blockId)
			{
				if (this.notFull.IsEmpty())
				{
					return null;
				}
				KeyValuePair<ShortCircuitShm.ShmId, DfsClientShm> entry = this.notFull.FirstEntry
					();
				DfsClientShm shm = entry.Value;
				ShortCircuitShm.ShmId shmId = shm.GetShmId();
				ShortCircuitShm.Slot slot = shm.AllocAndRegisterSlot(blockId);
				if (shm.IsFull())
				{
					if (DfsClientShmManager.Log.IsTraceEnabled())
					{
						DfsClientShmManager.Log.Trace(this + ": pulled the last slot " + slot.GetSlotIdx(
							) + " out of " + shm);
					}
					DfsClientShm removedShm = Sharpen.Collections.Remove(this.notFull, shmId);
					Preconditions.CheckState(removedShm == shm);
					this.full[shmId] = shm;
				}
				else
				{
					if (DfsClientShmManager.Log.IsTraceEnabled())
					{
						DfsClientShmManager.Log.Trace(this + ": pulled slot " + slot.GetSlotIdx() + " out of "
							 + shm);
					}
				}
				return slot;
			}

			/// <summary>Ask the DataNode for a new shared memory segment.</summary>
			/// <remarks>
			/// Ask the DataNode for a new shared memory segment.  This function must be
			/// called with the manager lock held.  We will release the lock while
			/// communicating with the DataNode.
			/// </remarks>
			/// <param name="clientName">The current client name.</param>
			/// <param name="peer">The peer to use to talk to the DataNode.</param>
			/// <returns>
			/// Null if the DataNode does not support shared memory
			/// segments, or experienced an error creating the
			/// shm.  The shared memory segment itself on success.
			/// </returns>
			/// <exception cref="System.IO.IOException">
			/// If there was an error communicating over the socket.
			/// We will not throw an IOException unless the socket
			/// itself (or the network) is the problem.
			/// </exception>
			private DfsClientShm RequestNewShm(string clientName, DomainPeer peer)
			{
				DataOutputStream @out = new DataOutputStream(new BufferedOutputStream(peer.GetOutputStream
					()));
				new Sender(@out).RequestShortCircuitShm(clientName);
				DataTransferProtos.ShortCircuitShmResponseProto resp = DataTransferProtos.ShortCircuitShmResponseProto
					.ParseFrom(PBHelper.VintPrefixed(peer.GetInputStream()));
				string error = resp.HasError() ? resp.GetError() : "(unknown)";
				switch (resp.GetStatus())
				{
					case DataTransferProtos.Status.Success:
					{
						DomainSocket sock = peer.GetDomainSocket();
						byte[] buf = new byte[1];
						FileInputStream[] fis = new FileInputStream[1];
						if (sock.RecvFileInputStreams(fis, buf, 0, buf.Length) < 0)
						{
							throw new EOFException("got EOF while trying to transfer the " + "file descriptor for the shared memory segment."
								);
						}
						if (fis[0] == null)
						{
							throw new IOException("the datanode " + this.datanode + " failed to " + "pass a file descriptor for the shared memory segment."
								);
						}
						try
						{
							DfsClientShm shm = new DfsClientShm(PBHelper.Convert(resp.GetId()), fis[0], this, 
								peer);
							if (DfsClientShmManager.Log.IsTraceEnabled())
							{
								DfsClientShmManager.Log.Trace(this + ": createNewShm: created " + shm);
							}
							return shm;
						}
						finally
						{
							IOUtils.Cleanup(DfsClientShmManager.Log, fis[0]);
						}
						goto case DataTransferProtos.Status.ErrorUnsupported;
					}

					case DataTransferProtos.Status.ErrorUnsupported:
					{
						// The DataNode just does not support short-circuit shared memory
						// access, and we should stop asking.
						DfsClientShmManager.Log.Info(this + ": datanode does not support short-circuit " 
							+ "shared memory access: " + error);
						this.disabled = true;
						return null;
					}

					default:
					{
						// The datanode experienced some kind of unexpected error when trying to
						// create the short-circuit shared memory segment.
						DfsClientShmManager.Log.Warn(this + ": error requesting short-circuit shared memory "
							 + "access: " + error);
						return null;
					}
				}
			}

			/// <summary>Allocate a new shared memory slot connected to this datanode.</summary>
			/// <remarks>
			/// Allocate a new shared memory slot connected to this datanode.
			/// Must be called with the EndpointShmManager lock held.
			/// </remarks>
			/// <param name="peer">The peer to use to talk to the DataNode.</param>
			/// <param name="usedPeer">
			/// (out param) Will be set to true if we used the peer.
			/// When a peer is used
			/// </param>
			/// <param name="clientName">The client name.</param>
			/// <param name="blockId">The block ID to use.</param>
			/// <returns>
			/// null if the DataNode does not support shared memory
			/// segments, or experienced an error creating the
			/// shm.  The shared memory segment itself on success.
			/// </returns>
			/// <exception cref="System.IO.IOException">If there was an error communicating over the socket.
			/// 	</exception>
			internal virtual ShortCircuitShm.Slot AllocSlot(DomainPeer peer, MutableBoolean usedPeer
				, string clientName, ExtendedBlockId blockId)
			{
				while (true)
				{
					if (this._enclosing.closed)
					{
						if (DfsClientShmManager.Log.IsTraceEnabled())
						{
							DfsClientShmManager.Log.Trace(this + ": the DfsClientShmManager has been closed."
								);
						}
						return null;
					}
					if (this.disabled)
					{
						if (DfsClientShmManager.Log.IsTraceEnabled())
						{
							DfsClientShmManager.Log.Trace(this + ": shared memory segment access is disabled."
								);
						}
						return null;
					}
					// Try to use an existing slot.
					ShortCircuitShm.Slot slot = this.AllocSlotFromExistingShm(blockId);
					if (slot != null)
					{
						return slot;
					}
					// There are no free slots.  If someone is loading more slots, wait
					// for that to finish.
					if (this.loading)
					{
						if (DfsClientShmManager.Log.IsTraceEnabled())
						{
							DfsClientShmManager.Log.Trace(this + ": waiting for loading to finish...");
						}
						this._enclosing.finishedLoading.AwaitUninterruptibly();
					}
					else
					{
						// Otherwise, load the slot ourselves.
						this.loading = true;
						this._enclosing.Lock.Unlock();
						DfsClientShm shm;
						try
						{
							shm = this.RequestNewShm(clientName, peer);
							if (shm == null)
							{
								continue;
							}
							// See #{DfsClientShmManager#domainSocketWatcher} for details
							// about why we do this before retaking the manager lock.
							this._enclosing.domainSocketWatcher.Add(peer.GetDomainSocket(), shm);
							// The DomainPeer is now our responsibility, and should not be
							// closed by the caller.
							usedPeer.SetValue(true);
						}
						finally
						{
							this._enclosing.Lock.Lock();
							this.loading = false;
							this._enclosing.finishedLoading.SignalAll();
						}
						if (shm.IsDisconnected())
						{
							// If the peer closed immediately after the shared memory segment
							// was created, the DomainSocketWatcher callback might already have
							// fired and marked the shm as disconnected.  In this case, we
							// obviously don't want to add the SharedMemorySegment to our list
							// of valid not-full segments.
							if (DfsClientShmManager.Log.IsDebugEnabled())
							{
								DfsClientShmManager.Log.Debug(this + ": the UNIX domain socket associated with " 
									+ "this short-circuit memory closed before we could make " + "use of the shm.");
							}
						}
						else
						{
							this.notFull[shm.GetShmId()] = shm;
						}
					}
				}
			}

			/// <summary>Stop tracking a slot.</summary>
			/// <remarks>
			/// Stop tracking a slot.
			/// Must be called with the EndpointShmManager lock held.
			/// </remarks>
			/// <param name="slot">The slot to release.</param>
			internal virtual void FreeSlot(ShortCircuitShm.Slot slot)
			{
				DfsClientShm shm = (DfsClientShm)slot.GetShm();
				shm.UnregisterSlot(slot.GetSlotIdx());
				if (shm.IsDisconnected())
				{
					// Stale shared memory segments should not be tracked here.
					Preconditions.CheckState(!this.full.Contains(shm.GetShmId()));
					Preconditions.CheckState(!this.notFull.Contains(shm.GetShmId()));
					if (shm.IsEmpty())
					{
						if (DfsClientShmManager.Log.IsTraceEnabled())
						{
							DfsClientShmManager.Log.Trace(this + ": freeing empty stale " + shm);
						}
						shm.Free();
					}
				}
				else
				{
					ShortCircuitShm.ShmId shmId = shm.GetShmId();
					Sharpen.Collections.Remove(this.full, shmId);
					// The shm can't be full if we just freed a slot.
					if (shm.IsEmpty())
					{
						Sharpen.Collections.Remove(this.notFull, shmId);
						// If the shared memory segment is now empty, we call shutdown(2) on
						// the UNIX domain socket associated with it.  The DomainSocketWatcher,
						// which is watching this socket, will call DfsClientShm#handle,
						// cleaning up this shared memory segment.
						//
						// See #{DfsClientShmManager#domainSocketWatcher} for details about why
						// we don't want to call DomainSocketWatcher#remove directly here.
						//
						// Note that we could experience 'fragmentation' here, where the
						// DFSClient allocates a bunch of slots in different shared memory
						// segments, and then frees most of them, but never fully empties out
						// any segment.  We make some attempt to avoid this fragmentation by
						// always allocating new slots out of the shared memory segment with the
						// lowest ID, but it could still occur.  In most workloads,
						// fragmentation should not be a major concern, since it doesn't impact
						// peak file descriptor usage or the speed of allocation.
						if (DfsClientShmManager.Log.IsTraceEnabled())
						{
							DfsClientShmManager.Log.Trace(this + ": shutting down UNIX domain socket for " + 
								"empty " + shm);
						}
						this.Shutdown(shm);
					}
					else
					{
						this.notFull[shmId] = shm;
					}
				}
			}

			/// <summary>Unregister a shared memory segment.</summary>
			/// <remarks>
			/// Unregister a shared memory segment.
			/// Once a segment is unregistered, we will not allocate any more slots
			/// inside that segment.
			/// The DomainSocketWatcher calls this while holding the DomainSocketWatcher
			/// lock.
			/// </remarks>
			/// <param name="shmId">The ID of the shared memory segment to unregister.</param>
			internal virtual void UnregisterShm(ShortCircuitShm.ShmId shmId)
			{
				this._enclosing.Lock.Lock();
				try
				{
					Sharpen.Collections.Remove(this.full, shmId);
					Sharpen.Collections.Remove(this.notFull, shmId);
				}
				finally
				{
					this._enclosing.Lock.Unlock();
				}
			}

			public override string ToString()
			{
				return string.Format("EndpointShmManager(%s, parent=%s)", this.datanode, this._enclosing
					);
			}

			internal virtual DfsClientShmManager.PerDatanodeVisitorInfo GetVisitorInfo()
			{
				return new DfsClientShmManager.PerDatanodeVisitorInfo(this.full, this.notFull, this
					.disabled);
			}

			internal void Shutdown(DfsClientShm shm)
			{
				try
				{
					shm.GetPeer().GetDomainSocket().Shutdown();
				}
				catch (IOException e)
				{
					DfsClientShmManager.Log.Warn(this + ": error shutting down shm: got IOException calling "
						 + "shutdown(SHUT_RDWR)", e);
				}
			}

			private readonly DfsClientShmManager _enclosing;
		}

		private bool closed = false;

		private readonly ReentrantLock Lock = new ReentrantLock();

		/// <summary>
		/// A condition variable which is signalled when we finish loading a segment
		/// from the Datanode.
		/// </summary>
		private readonly Condition finishedLoading = Lock.NewCondition();

		/// <summary>Information about each Datanode.</summary>
		private readonly Dictionary<DatanodeInfo, DfsClientShmManager.EndpointShmManager>
			 datanodes = new Dictionary<DatanodeInfo, DfsClientShmManager.EndpointShmManager
			>(1);

		/// <summary>
		/// The DomainSocketWatcher which keeps track of the UNIX domain socket
		/// associated with each shared memory segment.
		/// </summary>
		/// <remarks>
		/// The DomainSocketWatcher which keeps track of the UNIX domain socket
		/// associated with each shared memory segment.
		/// Note: because the DomainSocketWatcher makes callbacks into this
		/// DfsClientShmManager object, you must MUST NOT attempt to take the
		/// DomainSocketWatcher lock while holding the DfsClientShmManager lock,
		/// or else deadlock might result.   This means that most DomainSocketWatcher
		/// methods are off-limits unless you release the manager lock first.
		/// </remarks>
		private readonly DomainSocketWatcher domainSocketWatcher;

		/// <exception cref="System.IO.IOException"/>
		internal DfsClientShmManager(int interruptCheckPeriodMs)
		{
			this.domainSocketWatcher = new DomainSocketWatcher(interruptCheckPeriodMs, "client"
				);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual ShortCircuitShm.Slot AllocSlot(DatanodeInfo datanode, DomainPeer peer
			, MutableBoolean usedPeer, ExtendedBlockId blockId, string clientName)
		{
			Lock.Lock();
			try
			{
				if (closed)
				{
					Log.Trace(this + ": the DfsClientShmManager isclosed.");
					return null;
				}
				DfsClientShmManager.EndpointShmManager shmManager = datanodes[datanode];
				if (shmManager == null)
				{
					shmManager = new DfsClientShmManager.EndpointShmManager(this, datanode);
					datanodes[datanode] = shmManager;
				}
				return shmManager.AllocSlot(peer, usedPeer, clientName, blockId);
			}
			finally
			{
				Lock.Unlock();
			}
		}

		public virtual void FreeSlot(ShortCircuitShm.Slot slot)
		{
			Lock.Lock();
			try
			{
				DfsClientShm shm = (DfsClientShm)slot.GetShm();
				shm.GetEndpointShmManager().FreeSlot(slot);
			}
			finally
			{
				Lock.Unlock();
			}
		}

		public class PerDatanodeVisitorInfo
		{
			public readonly SortedDictionary<ShortCircuitShm.ShmId, DfsClientShm> full;

			public readonly SortedDictionary<ShortCircuitShm.ShmId, DfsClientShm> notFull;

			public readonly bool disabled;

			internal PerDatanodeVisitorInfo(SortedDictionary<ShortCircuitShm.ShmId, DfsClientShm
				> full, SortedDictionary<ShortCircuitShm.ShmId, DfsClientShm> notFull, bool disabled
				)
			{
				this.full = full;
				this.notFull = notFull;
				this.disabled = disabled;
			}
		}

		public interface Visitor
		{
			/// <exception cref="System.IO.IOException"/>
			void Visit(Dictionary<DatanodeInfo, DfsClientShmManager.PerDatanodeVisitorInfo> info
				);
		}

		/// <exception cref="System.IO.IOException"/>
		[VisibleForTesting]
		public virtual void Visit(DfsClientShmManager.Visitor visitor)
		{
			Lock.Lock();
			try
			{
				Dictionary<DatanodeInfo, DfsClientShmManager.PerDatanodeVisitorInfo> info = new Dictionary
					<DatanodeInfo, DfsClientShmManager.PerDatanodeVisitorInfo>();
				foreach (KeyValuePair<DatanodeInfo, DfsClientShmManager.EndpointShmManager> entry
					 in datanodes)
				{
					info[entry.Key] = entry.Value.GetVisitorInfo();
				}
				visitor.Visit(info);
			}
			finally
			{
				Lock.Unlock();
			}
		}

		/// <summary>Close the DfsClientShmManager.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void Close()
		{
			Lock.Lock();
			try
			{
				if (closed)
				{
					return;
				}
				closed = true;
			}
			finally
			{
				Lock.Unlock();
			}
			// When closed, the domainSocketWatcher will issue callbacks that mark
			// all the outstanding DfsClientShm segments as stale.
			IOUtils.Cleanup(Log, domainSocketWatcher);
		}

		public override string ToString()
		{
			return string.Format("ShortCircuitShmManager(%08x)", Runtime.IdentityHashCode(this
				));
		}

		[VisibleForTesting]
		public virtual DomainSocketWatcher GetDomainSocketWatcher()
		{
			return domainSocketWatcher;
		}
	}
}
