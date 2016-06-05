using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Sockets;
using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Protocol.Datatransfer;
using Org.Apache.Hadoop.Hdfs.Protocol.Datatransfer.Sasl;
using Org.Apache.Hadoop.Hdfs.Protocol.Proto;
using Org.Apache.Hadoop.Hdfs.ProtocolPB;
using Org.Apache.Hadoop.Hdfs.Security.Token.Block;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Balancer
{
	/// <summary>Dispatching block replica moves between datanodes.</summary>
	public class Dispatcher
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.Server.Balancer.Dispatcher
			));

		private const long Gb = 1L << 30;

		private const long MaxBlocksSizeToFetch = 2 * Gb;

		private const int MaxNoPendingMoveIterations = 5;

		/// <summary>
		/// the period of time to delay the usage of a DataNode after hitting
		/// errors when using it for migrating data
		/// </summary>
		private static long delayAfterErrors = 10 * 1000;

		private readonly NameNodeConnector nnc;

		private readonly SaslDataTransferClient saslClient;

		/// <summary>Set of datanodes to be excluded.</summary>
		private readonly ICollection<string> excludedNodes;

		/// <summary>Restrict to the following nodes.</summary>
		private readonly ICollection<string> includedNodes;

		private readonly ICollection<Dispatcher.Source> sources = new HashSet<Dispatcher.Source
			>();

		private readonly ICollection<Dispatcher.DDatanode.StorageGroup> targets = new HashSet
			<Dispatcher.DDatanode.StorageGroup>();

		private readonly Dispatcher.GlobalBlockMap globalBlocks = new Dispatcher.GlobalBlockMap
			();

		private readonly MovedBlocks<Dispatcher.DDatanode.StorageGroup> movedBlocks;

		/// <summary>Map (datanodeUuid,storageType -&gt; StorageGroup)</summary>
		private readonly Dispatcher.StorageGroupMap<Dispatcher.DDatanode.StorageGroup> storageGroupMap
			 = new Dispatcher.StorageGroupMap<Dispatcher.DDatanode.StorageGroup>();

		private NetworkTopology cluster;

		private readonly ExecutorService moveExecutor;

		private readonly ExecutorService dispatchExecutor;

		/// <summary>The maximum number of concurrent blocks moves at a datanode</summary>
		private readonly int maxConcurrentMovesPerNode;

		private class GlobalBlockMap
		{
			private readonly IDictionary<Block, Dispatcher.DBlock> map = new Dictionary<Block
				, Dispatcher.DBlock>();

			// 1GB
			/// <summary>
			/// Get the block from the map;
			/// if the block is not found, create a new block and put it in the map.
			/// </summary>
			private Dispatcher.DBlock Get(Block b)
			{
				Dispatcher.DBlock block = map[b];
				if (block == null)
				{
					block = new Dispatcher.DBlock(b);
					map[b] = block;
				}
				return block;
			}

			/// <summary>Remove all blocks except for the moved blocks.</summary>
			private void RemoveAllButRetain(MovedBlocks<Dispatcher.DDatanode.StorageGroup> movedBlocks
				)
			{
				for (IEnumerator<Block> i = map.Keys.GetEnumerator(); i.HasNext(); )
				{
					if (!movedBlocks.Contains(i.Next()))
					{
						i.Remove();
					}
				}
			}
		}

		public class StorageGroupMap<G>
			where G : Dispatcher.DDatanode.StorageGroup
		{
			private static string ToKey(string datanodeUuid, StorageType storageType)
			{
				return datanodeUuid + ":" + storageType;
			}

			private readonly IDictionary<string, G> map = new Dictionary<string, G>();

			public virtual G Get(string datanodeUuid, StorageType storageType)
			{
				return map[ToKey(datanodeUuid, storageType)];
			}

			public virtual void Put(G g)
			{
				string key = ToKey(g.GetDatanodeInfo().GetDatanodeUuid(), g.storageType);
				Dispatcher.DDatanode.StorageGroup existing = map[key] = g;
				Preconditions.CheckState(existing == null);
			}

			internal virtual int Size()
			{
				return map.Count;
			}

			internal virtual void Clear()
			{
				map.Clear();
			}

			public virtual ICollection<G> Values()
			{
				return map.Values;
			}
		}

		/// <summary>This class keeps track of a scheduled block move</summary>
		public class PendingMove
		{
			private Dispatcher.DBlock block;

			private Dispatcher.Source source;

			private Dispatcher.DDatanode proxySource;

			private Dispatcher.DDatanode.StorageGroup target;

			private PendingMove(Dispatcher _enclosing, Dispatcher.Source source, Dispatcher.DDatanode.StorageGroup
				 target)
			{
				this._enclosing = _enclosing;
				this.source = source;
				this.target = target;
			}

			public override string ToString()
			{
				Block b = this.block != null ? this.block.GetBlock() : null;
				string bStr = b != null ? (b + " with size=" + b.GetNumBytes() + " ") : " ";
				return bStr + "from " + this.source.GetDisplayName() + " to " + this.target.GetDisplayName
					() + " through " + (this.proxySource != null ? this.proxySource.datanode : string.Empty
					);
			}

			/// <summary>
			/// Choose a block & a proxy source for this pendingMove whose source &
			/// target have already been chosen.
			/// </summary>
			/// <returns>true if a block and its proxy are chosen; false otherwise</returns>
			private bool ChooseBlockAndProxy()
			{
				// source and target must have the same storage type
				StorageType t = this.source.GetStorageType();
				// iterate all source's blocks until find a good one
				for (IEnumerator<Dispatcher.DBlock> i = this.source.GetBlockIterator(); i.HasNext
					(); )
				{
					if (this.MarkMovedIfGoodBlock(i.Next(), t))
					{
						i.Remove();
						return true;
					}
				}
				return false;
			}

			/// <returns>true if the given block is good for the tentative move.</returns>
			private bool MarkMovedIfGoodBlock(Dispatcher.DBlock block, StorageType targetStorageType
				)
			{
				lock (block)
				{
					lock (this._enclosing.movedBlocks)
					{
						if (this._enclosing.IsGoodBlockCandidate(this.source, this.target, targetStorageType
							, block))
						{
							this.block = block;
							if (this.ChooseProxySource())
							{
								this._enclosing.movedBlocks.Put(block);
								if (Dispatcher.Log.IsDebugEnabled())
								{
									Dispatcher.Log.Debug("Decided to move " + this);
								}
								return true;
							}
						}
					}
				}
				return false;
			}

			/// <summary>Choose a proxy source.</summary>
			/// <returns>true if a proxy is found; otherwise false</returns>
			private bool ChooseProxySource()
			{
				DatanodeInfo targetDN = this.target.GetDatanodeInfo();
				// if source and target are same nodes then no need of proxy
				if (this.source.GetDatanodeInfo().Equals(targetDN) && this.AddTo(this.source))
				{
					return true;
				}
				// if node group is supported, first try add nodes in the same node group
				if (this._enclosing.cluster.IsNodeGroupAware())
				{
					foreach (Dispatcher.DDatanode.StorageGroup loc in this.block.GetLocations())
					{
						if (this._enclosing.cluster.IsOnSameNodeGroup(loc.GetDatanodeInfo(), targetDN) &&
							 this.AddTo(loc))
						{
							return true;
						}
					}
				}
				// check if there is replica which is on the same rack with the target
				foreach (Dispatcher.DDatanode.StorageGroup loc_1 in this.block.GetLocations())
				{
					if (this._enclosing.cluster.IsOnSameRack(loc_1.GetDatanodeInfo(), targetDN) && this
						.AddTo(loc_1))
					{
						return true;
					}
				}
				// find out a non-busy replica
				foreach (Dispatcher.DDatanode.StorageGroup loc_2 in this.block.GetLocations())
				{
					if (this.AddTo(loc_2))
					{
						return true;
					}
				}
				return false;
			}

			/// <summary>add to a proxy source for specific block movement</summary>
			private bool AddTo(Dispatcher.DDatanode.StorageGroup g)
			{
				Dispatcher.DDatanode dn = g.GetDDatanode();
				if (dn.AddPendingBlock(this))
				{
					this.proxySource = dn;
					return true;
				}
				return false;
			}

			/// <summary>Dispatch the move to the proxy source & wait for the response.</summary>
			private void Dispatch()
			{
				if (Dispatcher.Log.IsDebugEnabled())
				{
					Dispatcher.Log.Debug("Start moving " + this);
				}
				Socket sock = new Socket();
				DataOutputStream @out = null;
				DataInputStream @in = null;
				try
				{
					sock.Connect(NetUtils.CreateSocketAddr(this.target.GetDatanodeInfo().GetXferAddr(
						)), HdfsServerConstants.ReadTimeout);
					sock.SetKeepAlive(true);
					OutputStream unbufOut = sock.GetOutputStream();
					InputStream unbufIn = sock.GetInputStream();
					ExtendedBlock eb = new ExtendedBlock(this._enclosing.nnc.GetBlockpoolID(), this.block
						.GetBlock());
					KeyManager km = this._enclosing.nnc.GetKeyManager();
					Org.Apache.Hadoop.Security.Token.Token<BlockTokenIdentifier> accessToken = km.GetAccessToken
						(eb);
					IOStreamPair saslStreams = this._enclosing.saslClient.SocketSend(sock, unbufOut, 
						unbufIn, km, accessToken, this.target.GetDatanodeInfo());
					unbufOut = saslStreams.@out;
					unbufIn = saslStreams.@in;
					@out = new DataOutputStream(new BufferedOutputStream(unbufOut, HdfsConstants.IoFileBufferSize
						));
					@in = new DataInputStream(new BufferedInputStream(unbufIn, HdfsConstants.IoFileBufferSize
						));
					this.SendRequest(@out, eb, accessToken);
					this.ReceiveResponse(@in);
					this._enclosing.nnc.GetBytesMoved().AddAndGet(this.block.GetNumBytes());
					Dispatcher.Log.Info("Successfully moved " + this);
				}
				catch (IOException e)
				{
					Dispatcher.Log.Warn("Failed to move " + this + ": " + e.Message);
					this.target.GetDDatanode().SetHasFailure();
					// Proxy or target may have some issues, delay before using these nodes
					// further in order to avoid a potential storm of "threads quota
					// exceeded" warnings when the dispatcher gets out of sync with work
					// going on in datanodes.
					this.proxySource.ActivateDelay(Dispatcher.delayAfterErrors);
					this.target.GetDDatanode().ActivateDelay(Dispatcher.delayAfterErrors);
				}
				finally
				{
					IOUtils.CloseStream(@out);
					IOUtils.CloseStream(@in);
					IOUtils.CloseSocket(sock);
					this.proxySource.RemovePendingBlock(this);
					this.target.GetDDatanode().RemovePendingBlock(this);
					lock (this)
					{
						this.Reset();
					}
					lock (this._enclosing)
					{
						Sharpen.Runtime.NotifyAll(this._enclosing);
					}
				}
			}

			/// <summary>Send a block replace request to the output stream</summary>
			/// <exception cref="System.IO.IOException"/>
			private void SendRequest(DataOutputStream @out, ExtendedBlock eb, Org.Apache.Hadoop.Security.Token.Token
				<BlockTokenIdentifier> accessToken)
			{
				new Sender(@out).ReplaceBlock(eb, this.target.storageType, accessToken, this.source
					.GetDatanodeInfo().GetDatanodeUuid(), this.proxySource.datanode);
			}

			/// <summary>Receive a block copy response from the input stream</summary>
			/// <exception cref="System.IO.IOException"/>
			private void ReceiveResponse(DataInputStream @in)
			{
				DataTransferProtos.BlockOpResponseProto response = DataTransferProtos.BlockOpResponseProto
					.ParseFrom(PBHelper.VintPrefixed(@in));
				while (response.GetStatus() == DataTransferProtos.Status.InProgress)
				{
					// read intermediate responses
					response = DataTransferProtos.BlockOpResponseProto.ParseFrom(PBHelper.VintPrefixed
						(@in));
				}
				string logInfo = "block move is failed";
				DataTransferProtoUtil.CheckBlockOpStatus(response, logInfo);
			}

			/// <summary>reset the object</summary>
			private void Reset()
			{
				this.block = null;
				this.source = null;
				this.proxySource = null;
				this.target = null;
			}

			private readonly Dispatcher _enclosing;
		}

		/// <summary>A class for keeping track of block locations in the dispatcher.</summary>
		public class DBlock : MovedBlocks.Locations<Dispatcher.DDatanode.StorageGroup>
		{
			public DBlock(Org.Apache.Hadoop.Hdfs.Protocol.Block block)
				: base(block)
			{
			}
		}

		/// <summary>The class represents a desired move.</summary>
		internal class Task
		{
			private readonly Dispatcher.DDatanode.StorageGroup target;

			private long size;

			internal Task(Dispatcher.DDatanode.StorageGroup target, long size)
			{
				// bytes scheduled to move
				this.target = target;
				this.size = size;
			}

			internal virtual long GetSize()
			{
				return size;
			}
		}

		/// <summary>A class that keeps track of a datanode.</summary>
		public class DDatanode
		{
			/// <summary>A group of storages in a datanode with the same storage type.</summary>
			public class StorageGroup
			{
				internal readonly StorageType storageType;

				internal readonly long maxSize2Move;

				private long scheduledSize = 0L;

				private StorageGroup(DDatanode _enclosing, StorageType storageType, long maxSize2Move
					)
				{
					this._enclosing = _enclosing;
					this.storageType = storageType;
					this.maxSize2Move = maxSize2Move;
				}

				public virtual StorageType GetStorageType()
				{
					return this.storageType;
				}

				private Dispatcher.DDatanode GetDDatanode()
				{
					return this._enclosing;
				}

				public virtual DatanodeInfo GetDatanodeInfo()
				{
					return this._enclosing.datanode;
				}

				/// <summary>Decide if still need to move more bytes</summary>
				internal virtual bool HasSpaceForScheduling()
				{
					return this.HasSpaceForScheduling(0L);
				}

				internal virtual bool HasSpaceForScheduling(long size)
				{
					lock (this)
					{
						return this.AvailableSizeToMove() > size;
					}
				}

				/// <returns>the total number of bytes that need to be moved</returns>
				internal virtual long AvailableSizeToMove()
				{
					lock (this)
					{
						return this.maxSize2Move - this.scheduledSize;
					}
				}

				/// <summary>increment scheduled size</summary>
				public virtual void IncScheduledSize(long size)
				{
					lock (this)
					{
						this.scheduledSize += size;
					}
				}

				/// <returns>scheduled size</returns>
				internal virtual long GetScheduledSize()
				{
					lock (this)
					{
						return this.scheduledSize;
					}
				}

				/// <summary>Reset scheduled size to zero.</summary>
				internal virtual void ResetScheduledSize()
				{
					lock (this)
					{
						this.scheduledSize = 0L;
					}
				}

				private Dispatcher.PendingMove AddPendingMove(Dispatcher.DBlock block, Dispatcher.PendingMove
					 pm)
				{
					if (this.GetDDatanode().AddPendingBlock(pm))
					{
						if (pm.MarkMovedIfGoodBlock(block, this.GetStorageType()))
						{
							this.IncScheduledSize(pm.block.GetNumBytes());
							return pm;
						}
						else
						{
							this.GetDDatanode().RemovePendingBlock(pm);
						}
					}
					return null;
				}

				/// <returns>the name for display</returns>
				internal virtual string GetDisplayName()
				{
					return this._enclosing.datanode + ":" + this.storageType;
				}

				public override string ToString()
				{
					return this.GetDisplayName();
				}

				public override int GetHashCode()
				{
					return this.GetStorageType().GetHashCode() ^ this.GetDatanodeInfo().GetHashCode();
				}

				public override bool Equals(object obj)
				{
					if (this == obj)
					{
						return true;
					}
					else
					{
						if (obj == null || !(obj is Dispatcher.DDatanode.StorageGroup))
						{
							return false;
						}
						else
						{
							Dispatcher.DDatanode.StorageGroup that = (Dispatcher.DDatanode.StorageGroup)obj;
							return this.GetStorageType() == that.GetStorageType() && this.GetDatanodeInfo().Equals
								(that.GetDatanodeInfo());
						}
					}
				}

				private readonly DDatanode _enclosing;
			}

			internal readonly DatanodeInfo datanode;

			private readonly EnumMap<StorageType, Dispatcher.Source> sourceMap = new EnumMap<
				StorageType, Dispatcher.Source>(typeof(StorageType));

			private readonly EnumMap<StorageType, Dispatcher.DDatanode.StorageGroup> targetMap
				 = new EnumMap<StorageType, Dispatcher.DDatanode.StorageGroup>(typeof(StorageType
				));

			protected internal long delayUntil = 0L;

			/// <summary>blocks being moved but not confirmed yet</summary>
			private readonly IList<Dispatcher.PendingMove> pendings;

			private volatile bool hasFailure = false;

			private readonly int maxConcurrentMoves;

			public override string ToString()
			{
				return GetType().Name + ":" + datanode;
			}

			private DDatanode(DatanodeInfo datanode, int maxConcurrentMoves)
			{
				this.datanode = datanode;
				this.maxConcurrentMoves = maxConcurrentMoves;
				this.pendings = new AList<Dispatcher.PendingMove>(maxConcurrentMoves);
			}

			public virtual DatanodeInfo GetDatanodeInfo()
			{
				return datanode;
			}

			private static void Put<G>(StorageType storageType, G g, EnumMap<StorageType, G> 
				map)
				where G : Dispatcher.DDatanode.StorageGroup
			{
				Dispatcher.DDatanode.StorageGroup existing = map[storageType] = g;
				Preconditions.CheckState(existing == null);
			}

			public virtual Dispatcher.DDatanode.StorageGroup AddTarget(StorageType storageType
				, long maxSize2Move)
			{
				Dispatcher.DDatanode.StorageGroup g = new Dispatcher.DDatanode.StorageGroup(this, 
					storageType, maxSize2Move);
				Put(storageType, g, targetMap);
				return g;
			}

			public virtual Dispatcher.Source AddSource(StorageType storageType, long maxSize2Move
				, Dispatcher d)
			{
				Dispatcher.Source s = new Dispatcher.Source(this, storageType, maxSize2Move, this
					);
				Put(storageType, s, sourceMap);
				return s;
			}

			private void ActivateDelay(long delta)
			{
				lock (this)
				{
					delayUntil = Time.MonotonicNow() + delta;
				}
			}

			private bool IsDelayActive()
			{
				lock (this)
				{
					if (delayUntil == 0 || Time.MonotonicNow() > delayUntil)
					{
						delayUntil = 0;
						return false;
					}
					return true;
				}
			}

			/// <summary>Check if the node can schedule more blocks to move</summary>
			internal virtual bool IsPendingQNotFull()
			{
				lock (this)
				{
					return pendings.Count < maxConcurrentMoves;
				}
			}

			/// <summary>Check if all the dispatched moves are done</summary>
			internal virtual bool IsPendingQEmpty()
			{
				lock (this)
				{
					return pendings.IsEmpty();
				}
			}

			/// <summary>Add a scheduled block move to the node</summary>
			internal virtual bool AddPendingBlock(Dispatcher.PendingMove pendingBlock)
			{
				lock (this)
				{
					if (!IsDelayActive() && IsPendingQNotFull())
					{
						return pendings.AddItem(pendingBlock);
					}
					return false;
				}
			}

			/// <summary>Remove a scheduled block move from the node</summary>
			internal virtual bool RemovePendingBlock(Dispatcher.PendingMove pendingBlock)
			{
				lock (this)
				{
					return pendings.Remove(pendingBlock);
				}
			}

			internal virtual void SetHasFailure()
			{
				this.hasFailure = true;
			}
		}

		/// <summary>A node that can be the sources of a block move</summary>
		public class Source : Dispatcher.DDatanode.StorageGroup
		{
			private readonly IList<Dispatcher.Task> tasks = new AList<Dispatcher.Task>(2);

			private long blocksToReceive = 0L;

			/// <summary>
			/// Source blocks point to the objects in
			/// <see cref="Dispatcher.globalBlocks"/>
			/// because we want to keep one copy of a block and be aware that the
			/// locations are changing over time.
			/// </summary>
			private readonly IList<Dispatcher.DBlock> srcBlocks = new AList<Dispatcher.DBlock
				>();

			private Source(Dispatcher _enclosing, StorageType storageType, long maxSize2Move, 
				Dispatcher.DDatanode dn)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			//HM: hack put into Sharpen to fix this. Resolve
			/// <summary>Add a task</summary>
			internal virtual void AddTask(Dispatcher.Task task)
			{
				Preconditions.CheckState(task.target != this, "Source and target are the same storage group "
					 + this.GetDisplayName());
				this.IncScheduledSize(task.size);
				this.tasks.AddItem(task);
			}

			/// <returns>an iterator to this source's blocks</returns>
			internal virtual IEnumerator<Dispatcher.DBlock> GetBlockIterator()
			{
				return this.srcBlocks.GetEnumerator();
			}

			/// <summary>
			/// Fetch new blocks of this source from namenode and update this source's
			/// block list &
			/// <see cref="Dispatcher.globalBlocks"/>
			/// .
			/// </summary>
			/// <returns>the total size of the received blocks in the number of bytes.</returns>
			/// <exception cref="System.IO.IOException"/>
			private long GetBlockList()
			{
				long size = Math.Min(Dispatcher.MaxBlocksSizeToFetch, this.blocksToReceive);
				BlocksWithLocations newBlocks = this._enclosing.nnc.GetBlocks(this.GetDatanodeInfo
					(), size);
				long bytesReceived = 0;
				foreach (BlocksWithLocations.BlockWithLocations blk in newBlocks.GetBlocks())
				{
					bytesReceived += blk.GetBlock().GetNumBytes();
					lock (this._enclosing.globalBlocks)
					{
						Dispatcher.DBlock block = this._enclosing.globalBlocks.Get(blk.GetBlock());
						lock (block)
						{
							block.ClearLocations();
							// update locations
							string[] datanodeUuids = blk.GetDatanodeUuids();
							StorageType[] storageTypes = blk.GetStorageTypes();
							for (int i = 0; i < datanodeUuids.Length; i++)
							{
								Dispatcher.DDatanode.StorageGroup g = this._enclosing.storageGroupMap.Get(datanodeUuids
									[i], storageTypes[i]);
								if (g != null)
								{
									// not unknown
									block.AddLocation(g);
								}
							}
						}
						if (!this.srcBlocks.Contains(block) && this.IsGoodBlockCandidate(block))
						{
							// filter bad candidates
							this.srcBlocks.AddItem(block);
						}
					}
				}
				return bytesReceived;
			}

			/// <summary>Decide if the given block is a good candidate to move or not</summary>
			private bool IsGoodBlockCandidate(Dispatcher.DBlock block)
			{
				// source and target must have the same storage type
				StorageType sourceStorageType = this.GetStorageType();
				foreach (Dispatcher.Task t in this.tasks)
				{
					if (this._enclosing.IsGoodBlockCandidate(this, t.target, sourceStorageType, block
						))
					{
						return true;
					}
				}
				return false;
			}

			/// <summary>Choose a move for the source.</summary>
			/// <remarks>
			/// Choose a move for the source. The block's source, target, and proxy
			/// are determined too. When choosing proxy and target, source &
			/// target throttling has been considered. They are chosen only when they
			/// have the capacity to support this block move. The block should be
			/// dispatched immediately after this method is returned.
			/// </remarks>
			/// <returns>a move that's good for the source to dispatch immediately.</returns>
			private Dispatcher.PendingMove ChooseNextMove()
			{
				for (IEnumerator<Dispatcher.Task> i = this.tasks.GetEnumerator(); i.HasNext(); )
				{
					Dispatcher.Task task = i.Next();
					Dispatcher.DDatanode target = task.target.GetDDatanode();
					Dispatcher.PendingMove pendingBlock = new Dispatcher.PendingMove(this, this, task
						.target);
					if (target.AddPendingBlock(pendingBlock))
					{
						// target is not busy, so do a tentative block allocation
						if (pendingBlock.ChooseBlockAndProxy())
						{
							long blockSize = pendingBlock.block.GetNumBytes();
							this.IncScheduledSize(-blockSize);
							task.size -= blockSize;
							if (task.size == 0)
							{
								i.Remove();
							}
							return pendingBlock;
						}
						else
						{
							// cancel the tentative move
							target.RemovePendingBlock(pendingBlock);
						}
					}
				}
				return null;
			}

			/// <summary>Add a pending move</summary>
			public virtual Dispatcher.PendingMove AddPendingMove(Dispatcher.DBlock block, Dispatcher.DDatanode.StorageGroup
				 target)
			{
				return target.AddPendingMove(block, new Dispatcher.PendingMove(this, this, target
					));
			}

			/// <summary>Iterate all source's blocks to remove moved ones</summary>
			private void RemoveMovedBlocks()
			{
				for (IEnumerator<Dispatcher.DBlock> i = this.GetBlockIterator(); i.HasNext(); )
				{
					if (this._enclosing.movedBlocks.Contains(i.Next().GetBlock()))
					{
						i.Remove();
					}
				}
			}

			private const int SourceBlocksMinSize = 5;

			/// <returns>if should fetch more blocks from namenode</returns>
			private bool ShouldFetchMoreBlocks()
			{
				return this.srcBlocks.Count < Dispatcher.Source.SourceBlocksMinSize && this.blocksToReceive
					 > 0;
			}

			private const long MaxIterationTime = 20 * 60 * 1000L;

			// 20 mins
			/// <summary>
			/// This method iteratively does the following: it first selects a block to
			/// move, then sends a request to the proxy source to start the block move
			/// when the source's block list falls below a threshold, it asks the
			/// namenode for more blocks.
			/// </summary>
			/// <remarks>
			/// This method iteratively does the following: it first selects a block to
			/// move, then sends a request to the proxy source to start the block move
			/// when the source's block list falls below a threshold, it asks the
			/// namenode for more blocks. It terminates when it has dispatch enough block
			/// move tasks or it has received enough blocks from the namenode, or the
			/// elapsed time of the iteration has exceeded the max time limit.
			/// </remarks>
			private void DispatchBlocks()
			{
				long startTime = Time.MonotonicNow();
				this.blocksToReceive = 2 * this.GetScheduledSize();
				bool isTimeUp = false;
				int noPendingMoveIteration = 0;
				while (!isTimeUp && this.GetScheduledSize() > 0 && (!this.srcBlocks.IsEmpty() || 
					this.blocksToReceive > 0))
				{
					Dispatcher.PendingMove p = this.ChooseNextMove();
					if (p != null)
					{
						// Reset no pending move counter
						noPendingMoveIteration = 0;
						this._enclosing.ExecutePendingMove(p);
						continue;
					}
					// Since we cannot schedule any block to move,
					// remove any moved blocks from the source block list and
					this.RemoveMovedBlocks();
					// filter already moved blocks
					// check if we should fetch more blocks from the namenode
					if (this.ShouldFetchMoreBlocks())
					{
						// fetch new blocks
						try
						{
							this.blocksToReceive -= this.GetBlockList();
							continue;
						}
						catch (IOException e)
						{
							Dispatcher.Log.Warn("Exception while getting block list", e);
							return;
						}
					}
					else
					{
						// source node cannot find a pending block to move, iteration +1
						noPendingMoveIteration++;
						// in case no blocks can be moved for source node's task,
						// jump out of while-loop after 5 iterations.
						if (noPendingMoveIteration >= Dispatcher.MaxNoPendingMoveIterations)
						{
							this.ResetScheduledSize();
						}
					}
					// check if time is up or not
					if (Time.MonotonicNow() - startTime > Dispatcher.Source.MaxIterationTime)
					{
						isTimeUp = true;
						continue;
					}
					// Now we can not schedule any block to move and there are
					// no new blocks added to the source block list, so we wait.
					try
					{
						lock (this._enclosing)
						{
							Sharpen.Runtime.Wait(this._enclosing, 1000);
						}
					}
					catch (Exception)
					{
					}
				}
			}

			// wait for targets/sources to be idle
			public override int GetHashCode()
			{
				return base.GetHashCode();
			}

			public override bool Equals(object obj)
			{
				return base.Equals(obj);
			}

			private readonly Dispatcher _enclosing;
		}

		public Dispatcher(NameNodeConnector nnc, ICollection<string> includedNodes, ICollection
			<string> excludedNodes, long movedWinWidth, int moverThreads, int dispatcherThreads
			, int maxConcurrentMovesPerNode, Configuration conf)
		{
			this.nnc = nnc;
			this.excludedNodes = excludedNodes;
			this.includedNodes = includedNodes;
			this.movedBlocks = new MovedBlocks<Dispatcher.DDatanode.StorageGroup>(movedWinWidth
				);
			this.cluster = NetworkTopology.GetInstance(conf);
			this.moveExecutor = Executors.NewFixedThreadPool(moverThreads);
			this.dispatchExecutor = dispatcherThreads == 0 ? null : Executors.NewFixedThreadPool
				(dispatcherThreads);
			this.maxConcurrentMovesPerNode = maxConcurrentMovesPerNode;
			this.saslClient = new SaslDataTransferClient(conf, DataTransferSaslUtil.GetSaslPropertiesResolver
				(conf), TrustedChannelResolver.GetInstance(conf), nnc.fallbackToSimpleAuth);
		}

		public virtual DistributedFileSystem GetDistributedFileSystem()
		{
			return nnc.GetDistributedFileSystem();
		}

		public virtual Dispatcher.StorageGroupMap<Dispatcher.DDatanode.StorageGroup> GetStorageGroupMap
			()
		{
			return storageGroupMap;
		}

		public virtual NetworkTopology GetCluster()
		{
			return cluster;
		}

		internal virtual long GetBytesMoved()
		{
			return nnc.GetBytesMoved().Get();
		}

		internal virtual long BytesToMove()
		{
			Preconditions.CheckState(storageGroupMap.Size() >= sources.Count + targets.Count, 
				"Mismatched number of storage groups (" + storageGroupMap.Size() + " < " + sources
				.Count + " sources + " + targets.Count + " targets)");
			long b = 0L;
			foreach (Dispatcher.Source src in sources)
			{
				b += src.GetScheduledSize();
			}
			return b;
		}

		internal virtual void Add(Dispatcher.Source source, Dispatcher.DDatanode.StorageGroup
			 target)
		{
			sources.AddItem(source);
			targets.AddItem(target);
		}

		private bool ShouldIgnore(DatanodeInfo dn)
		{
			// ignore decommissioned nodes
			bool decommissioned = dn.IsDecommissioned();
			// ignore decommissioning nodes
			bool decommissioning = dn.IsDecommissionInProgress();
			// ignore nodes in exclude list
			bool excluded = Dispatcher.Util.IsExcluded(excludedNodes, dn);
			// ignore nodes not in the include list (if include list is not empty)
			bool notIncluded = !Dispatcher.Util.IsIncluded(includedNodes, dn);
			if (decommissioned || decommissioning || excluded || notIncluded)
			{
				if (Log.IsTraceEnabled())
				{
					Log.Trace("Excluding datanode " + dn + ": " + decommissioned + ", " + decommissioning
						 + ", " + excluded + ", " + notIncluded);
				}
				return true;
			}
			return false;
		}

		/// <summary>Get live datanode storage reports and then build the network topology.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual IList<DatanodeStorageReport> Init()
		{
			DatanodeStorageReport[] reports = nnc.GetLiveDatanodeStorageReport();
			IList<DatanodeStorageReport> trimmed = new AList<DatanodeStorageReport>();
			// create network topology and classify utilization collections:
			// over-utilized, above-average, below-average and under-utilized.
			foreach (DatanodeStorageReport r in DFSUtil.Shuffle(reports))
			{
				DatanodeInfo datanode = r.GetDatanodeInfo();
				if (ShouldIgnore(datanode))
				{
					continue;
				}
				trimmed.AddItem(r);
				cluster.Add(datanode);
			}
			return trimmed;
		}

		public virtual Dispatcher.DDatanode NewDatanode(DatanodeInfo datanode)
		{
			return new Dispatcher.DDatanode(datanode, maxConcurrentMovesPerNode);
		}

		public virtual void ExecutePendingMove(Dispatcher.PendingMove p)
		{
			// move the block
			moveExecutor.Execute(new _Runnable_888(p));
		}

		private sealed class _Runnable_888 : Runnable
		{
			public _Runnable_888(Dispatcher.PendingMove p)
			{
				this.p = p;
			}

			public void Run()
			{
				p.Dispatch();
			}

			private readonly Dispatcher.PendingMove p;
		}

		/// <exception cref="System.Exception"/>
		public virtual bool DispatchAndCheckContinue()
		{
			return nnc.ShouldContinue(DispatchBlockMoves());
		}

		/// <summary>Dispatch block moves for each source.</summary>
		/// <remarks>
		/// Dispatch block moves for each source. The thread selects blocks to move &
		/// sends request to proxy source to initiate block move. The process is flow
		/// controlled. Block selection is blocked if there are too many un-confirmed
		/// block moves.
		/// </remarks>
		/// <returns>the total number of bytes successfully moved in this iteration.</returns>
		/// <exception cref="System.Exception"/>
		private long DispatchBlockMoves()
		{
			long bytesLastMoved = GetBytesMoved();
			Future<object>[] futures = new Future<object>[sources.Count];
			IEnumerator<Dispatcher.Source> i = sources.GetEnumerator();
			for (int j = 0; j < futures.Length; j++)
			{
				Dispatcher.Source s = i.Next();
				futures[j] = dispatchExecutor.Submit(new _Runnable_915(s));
			}
			// wait for all dispatcher threads to finish
			foreach (Future<object> future in futures)
			{
				try
				{
					future.Get();
				}
				catch (ExecutionException e)
				{
					Log.Warn("Dispatcher thread failed", e.InnerException);
				}
			}
			// wait for all block moving to be done
			WaitForMoveCompletion(targets);
			return GetBytesMoved() - bytesLastMoved;
		}

		private sealed class _Runnable_915 : Runnable
		{
			public _Runnable_915(Dispatcher.Source s)
			{
				this.s = s;
			}

			public void Run()
			{
				s.DispatchBlocks();
			}

			private readonly Dispatcher.Source s;
		}

		/// <summary>The sleeping period before checking if block move is completed again</summary>
		private static long blockMoveWaitTime = 30000L;

		/// <summary>Wait for all block move confirmations.</summary>
		/// <returns>true if there is failed move execution</returns>
		public static bool WaitForMoveCompletion<_T0>(IEnumerable<_T0> targets)
			where _T0 : Dispatcher.DDatanode.StorageGroup
		{
			bool hasFailure = false;
			for (; ; )
			{
				bool empty = true;
				foreach (Dispatcher.DDatanode.StorageGroup t in targets)
				{
					if (!t.GetDDatanode().IsPendingQEmpty())
					{
						empty = false;
						break;
					}
					else
					{
						hasFailure |= t.GetDDatanode().hasFailure;
					}
				}
				if (empty)
				{
					return hasFailure;
				}
				// all pending queues are empty
				try
				{
					Sharpen.Thread.Sleep(blockMoveWaitTime);
				}
				catch (Exception)
				{
				}
			}
		}

		/// <summary>Decide if the block is a good candidate to be moved from source to target.
		/// 	</summary>
		/// <remarks>
		/// Decide if the block is a good candidate to be moved from source to target.
		/// A block is a good candidate if
		/// 1. the block is not in the process of being moved/has not been moved;
		/// 2. the block does not have a replica on the target;
		/// 3. doing the move does not reduce the number of racks that the block has
		/// </remarks>
		private bool IsGoodBlockCandidate(Dispatcher.DDatanode.StorageGroup source, Dispatcher.DDatanode.StorageGroup
			 target, StorageType targetStorageType, Dispatcher.DBlock block)
		{
			if (source.Equals(target))
			{
				return false;
			}
			if (target.storageType != targetStorageType)
			{
				return false;
			}
			// check if the block is moved or not
			if (movedBlocks.Contains(block.GetBlock()))
			{
				return false;
			}
			DatanodeInfo targetDatanode = target.GetDatanodeInfo();
			if (source.GetDatanodeInfo().Equals(targetDatanode))
			{
				// the block is moved inside same DN
				return true;
			}
			// check if block has replica in target node
			foreach (Dispatcher.DDatanode.StorageGroup blockLocation in block.GetLocations())
			{
				if (blockLocation.GetDatanodeInfo().Equals(targetDatanode))
				{
					return false;
				}
			}
			if (cluster.IsNodeGroupAware() && IsOnSameNodeGroupWithReplicas(source, target, block
				))
			{
				return false;
			}
			if (ReduceNumOfRacks(source, target, block))
			{
				return false;
			}
			return true;
		}

		/// <summary>
		/// Determine whether moving the given block replica from source to target
		/// would reduce the number of racks of the block replicas.
		/// </summary>
		private bool ReduceNumOfRacks(Dispatcher.DDatanode.StorageGroup source, Dispatcher.DDatanode.StorageGroup
			 target, Dispatcher.DBlock block)
		{
			DatanodeInfo sourceDn = source.GetDatanodeInfo();
			if (cluster.IsOnSameRack(sourceDn, target.GetDatanodeInfo()))
			{
				// source and target are on the same rack
				return false;
			}
			bool notOnSameRack = true;
			lock (block)
			{
				foreach (Dispatcher.DDatanode.StorageGroup loc in block.GetLocations())
				{
					if (cluster.IsOnSameRack(loc.GetDatanodeInfo(), target.GetDatanodeInfo()))
					{
						notOnSameRack = false;
						break;
					}
				}
			}
			if (notOnSameRack)
			{
				// target is not on the same rack as any replica
				return false;
			}
			foreach (Dispatcher.DDatanode.StorageGroup g in block.GetLocations())
			{
				if (g != source && cluster.IsOnSameRack(g.GetDatanodeInfo(), sourceDn))
				{
					// source is on the same rack of another replica
					return false;
				}
			}
			return true;
		}

		/// <summary>
		/// Check if there are any replica (other than source) on the same node group
		/// with target.
		/// </summary>
		/// <remarks>
		/// Check if there are any replica (other than source) on the same node group
		/// with target. If true, then target is not a good candidate for placing
		/// specific replica as we don't want 2 replicas under the same nodegroup.
		/// </remarks>
		/// <returns>
		/// true if there are any replica (other than source) on the same node
		/// group with target
		/// </returns>
		private bool IsOnSameNodeGroupWithReplicas(Dispatcher.DDatanode.StorageGroup source
			, Dispatcher.DDatanode.StorageGroup target, Dispatcher.DBlock block)
		{
			DatanodeInfo targetDn = target.GetDatanodeInfo();
			foreach (Dispatcher.DDatanode.StorageGroup g in block.GetLocations())
			{
				if (g != source && cluster.IsOnSameNodeGroup(g.GetDatanodeInfo(), targetDn))
				{
					return true;
				}
			}
			return false;
		}

		/// <summary>Reset all fields in order to prepare for the next iteration</summary>
		internal virtual void Reset(Configuration conf)
		{
			cluster = NetworkTopology.GetInstance(conf);
			storageGroupMap.Clear();
			sources.Clear();
			targets.Clear();
			globalBlocks.RemoveAllButRetain(movedBlocks);
			movedBlocks.Cleanup();
		}

		/// <summary>set the sleeping period for block move completion check</summary>
		[VisibleForTesting]
		public static void SetBlockMoveWaitTime(long time)
		{
			blockMoveWaitTime = time;
		}

		[VisibleForTesting]
		public static void SetDelayAfterErrors(long time)
		{
			delayAfterErrors = time;
		}

		/// <summary>shutdown thread pools</summary>
		public virtual void ShutdownNow()
		{
			if (dispatchExecutor != null)
			{
				dispatchExecutor.ShutdownNow();
			}
			moveExecutor.ShutdownNow();
		}

		internal class Util
		{
			/// <returns>true if data node is part of the excludedNodes.</returns>
			internal static bool IsExcluded(ICollection<string> excludedNodes, DatanodeInfo dn
				)
			{
				return IsIn(excludedNodes, dn);
			}

			/// <returns>
			/// true if includedNodes is empty or data node is part of the
			/// includedNodes.
			/// </returns>
			internal static bool IsIncluded(ICollection<string> includedNodes, DatanodeInfo dn
				)
			{
				return (includedNodes.IsEmpty() || IsIn(includedNodes, dn));
			}

			/// <summary>
			/// Match is checked using host name , ip address with and without port
			/// number.
			/// </summary>
			/// <returns>true if the datanode's transfer address matches the set of nodes.</returns>
			private static bool IsIn(ICollection<string> datanodes, DatanodeInfo dn)
			{
				return IsIn(datanodes, dn.GetPeerHostName(), dn.GetXferPort()) || IsIn(datanodes, 
					dn.GetIpAddr(), dn.GetXferPort()) || IsIn(datanodes, dn.GetHostName(), dn.GetXferPort
					());
			}

			/// <returns>true if nodes contains host or host:port</returns>
			private static bool IsIn(ICollection<string> nodes, string host, int port)
			{
				if (host == null)
				{
					return false;
				}
				return (nodes.Contains(host) || nodes.Contains(host + ":" + port));
			}

			/// <summary>Parse a comma separated string to obtain set of host names</summary>
			/// <returns>set of host names</returns>
			internal static ICollection<string> ParseHostList(string @string)
			{
				string[] addrs = StringUtils.GetTrimmedStrings(@string);
				return new HashSet<string>(Arrays.AsList(addrs));
			}

			/// <summary>Read set of host names from a file</summary>
			/// <returns>set of host names</returns>
			internal static ICollection<string> GetHostListFromFile(string fileName, string type
				)
			{
				ICollection<string> nodes = new HashSet<string>();
				try
				{
					HostsFileReader.ReadFileToSet(type, fileName, nodes);
					return StringUtils.GetTrimmedStrings(nodes);
				}
				catch (IOException)
				{
					throw new ArgumentException("Failed to read host list from file: " + fileName);
				}
			}
		}
	}
}
