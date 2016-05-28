using System;
using System.Collections.Generic;
using System.IO;
using Javax.Management;
using Org.Apache.Commons.Lang;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset;
using Org.Apache.Hadoop.Hdfs.Server.Datanode.Metrics;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Metrics2.Util;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode
{
	/// <summary>This class implements a simulated FSDataset.</summary>
	/// <remarks>
	/// This class implements a simulated FSDataset.
	/// Blocks that are created are recorded but their data (plus their CRCs) are
	/// discarded.
	/// Fixed data is returned when blocks are read; a null CRC meta file is
	/// created for such data.
	/// This FSDataset does not remember any block information across its
	/// restarts; it does however offer an operation to inject blocks
	/// (See the TestInectionForSImulatedStorage()
	/// for a usage example of injection.
	/// Note the synchronization is coarse grained - it is at each method.
	/// </remarks>
	public class SimulatedFSDataset : FsDatasetSpi<FsVolumeSpi>
	{
		internal class Factory : FsDatasetSpi.Factory<SimulatedFSDataset>
		{
			/// <exception cref="System.IO.IOException"/>
			public override SimulatedFSDataset NewInstance(DataNode datanode, DataStorage storage
				, Configuration conf)
			{
				return new SimulatedFSDataset(storage, conf);
			}

			public override bool IsSimulated()
			{
				return true;
			}
		}

		public static void SetFactory(Configuration conf)
		{
			conf.Set(DFSConfigKeys.DfsDatanodeFsdatasetFactoryKey, typeof(SimulatedFSDataset.Factory
				).FullName);
		}

		public const string ConfigPropertyCapacity = "dfs.datanode.simulateddatastorage.capacity";

		public const long DefaultCapacity = 2L << 40;

		public const byte DefaultDatabyte = 9;

		public const string ConfigPropertyState = "dfs.datanode.simulateddatastorage.state";

		private static readonly DatanodeStorage.State DefaultState = DatanodeStorage.State
			.Normal;

		internal static readonly byte[] nullCrcFileData;

		static SimulatedFSDataset()
		{
			// 1 terabyte
			DataChecksum checksum = DataChecksum.NewDataChecksum(DataChecksum.Type.Null, 16 *
				 1024);
			byte[] nullCrcHeader = checksum.GetHeader();
			nullCrcFileData = new byte[2 + nullCrcHeader.Length];
			nullCrcFileData[0] = unchecked((byte)(((short)(((ushort)BlockMetadataHeader.Version
				) >> 8)) & unchecked((int)(0xff))));
			nullCrcFileData[1] = unchecked((byte)(BlockMetadataHeader.Version & unchecked((int
				)(0xff))));
			for (int i = 0; i < nullCrcHeader.Length; i++)
			{
				nullCrcFileData[i + 2] = nullCrcHeader[i];
			}
		}

		private class BInfo : ReplicaInPipelineInterface
		{
			internal readonly Block theBlock;

			private bool finalized = false;

			internal SimulatedFSDataset.SimulatedOutputStream oStream = null;

			private long bytesAcked;

			private long bytesRcvd;

			private bool pinned = false;

			/// <exception cref="System.IO.IOException"/>
			internal BInfo(SimulatedFSDataset _enclosing, string bpid, Block b, bool forWriting
				)
			{
				this._enclosing = _enclosing;
				// information about a single block
				// if not finalized => ongoing creation
				this.theBlock = new Block(b);
				if (this.theBlock.GetNumBytes() < 0)
				{
					this.theBlock.SetNumBytes(0);
				}
				if (!this._enclosing.storage.Alloc(bpid, this.theBlock.GetNumBytes()))
				{
					// expected length - actual length may
					// be more - we find out at finalize
					DataNode.Log.Warn("Lack of free storage on a block alloc");
					throw new IOException("Creating block, no free space available");
				}
				if (forWriting)
				{
					this.finalized = false;
					this.oStream = new SimulatedFSDataset.SimulatedOutputStream();
				}
				else
				{
					this.finalized = true;
					this.oStream = null;
				}
			}

			public virtual string GetStorageUuid()
			{
				return this._enclosing.storage.GetStorageUuid();
			}

			public virtual long GetGenerationStamp()
			{
				lock (this)
				{
					return this.theBlock.GetGenerationStamp();
				}
			}

			public virtual long GetNumBytes()
			{
				lock (this)
				{
					if (!this.finalized)
					{
						return this.bytesRcvd;
					}
					else
					{
						return this.theBlock.GetNumBytes();
					}
				}
			}

			public virtual void SetNumBytes(long length)
			{
				lock (this)
				{
					if (!this.finalized)
					{
						this.bytesRcvd = length;
					}
					else
					{
						this.theBlock.SetNumBytes(length);
					}
				}
			}

			internal virtual SimulatedFSDataset.SimulatedInputStream GetIStream()
			{
				lock (this)
				{
					if (!this.finalized)
					{
						// throw new IOException("Trying to read an unfinalized block");
						return new SimulatedFSDataset.SimulatedInputStream(this.oStream.GetLength(), SimulatedFSDataset
							.DefaultDatabyte);
					}
					else
					{
						return new SimulatedFSDataset.SimulatedInputStream(this.theBlock.GetNumBytes(), SimulatedFSDataset
							.DefaultDatabyte);
					}
				}
			}

			/// <exception cref="System.IO.IOException"/>
			internal virtual void FinalizeBlock(string bpid, long finalSize)
			{
				lock (this)
				{
					if (this.finalized)
					{
						throw new IOException("Finalizing a block that has already been finalized" + this
							.theBlock.GetBlockId());
					}
					if (this.oStream == null)
					{
						DataNode.Log.Error("Null oStream on unfinalized block - bug");
						throw new IOException("Unexpected error on finalize");
					}
					if (this.oStream.GetLength() != finalSize)
					{
						DataNode.Log.Warn("Size passed to finalize (" + finalSize + ")does not match what was written:"
							 + this.oStream.GetLength());
						throw new IOException("Size passed to finalize does not match the amount of data written"
							);
					}
					// We had allocated the expected length when block was created; 
					// adjust if necessary
					long extraLen = finalSize - this.theBlock.GetNumBytes();
					if (extraLen > 0)
					{
						if (!this._enclosing.storage.Alloc(bpid, extraLen))
						{
							DataNode.Log.Warn("Lack of free storage on a block alloc");
							throw new IOException("Creating block, no free space available");
						}
					}
					else
					{
						this._enclosing.storage.Free(bpid, -extraLen);
					}
					this.theBlock.SetNumBytes(finalSize);
					this.finalized = true;
					this.oStream = null;
					return;
				}
			}

			/// <exception cref="System.IO.IOException"/>
			internal virtual void UnfinalizeBlock()
			{
				lock (this)
				{
					if (!this.finalized)
					{
						throw new IOException("Unfinalized a block that's not finalized " + this.theBlock
							);
					}
					this.finalized = false;
					this.oStream = new SimulatedFSDataset.SimulatedOutputStream();
					long blockLen = this.theBlock.GetNumBytes();
					this.oStream.SetLength(blockLen);
					this.bytesRcvd = blockLen;
					this.bytesAcked = blockLen;
				}
			}

			internal virtual SimulatedFSDataset.SimulatedInputStream GetMetaIStream()
			{
				return new SimulatedFSDataset.SimulatedInputStream(SimulatedFSDataset.nullCrcFileData
					);
			}

			internal virtual bool IsFinalized()
			{
				lock (this)
				{
					return this.finalized;
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual ReplicaOutputStreams CreateStreams(bool isCreate, DataChecksum requestedChecksum
				)
			{
				lock (this)
				{
					if (this.finalized)
					{
						throw new IOException("Trying to write to a finalized replica " + this.theBlock);
					}
					else
					{
						SimulatedFSDataset.SimulatedOutputStream crcStream = new SimulatedFSDataset.SimulatedOutputStream
							();
						return new ReplicaOutputStreams(this.oStream, crcStream, requestedChecksum, this.
							_enclosing.volume.IsTransientStorage());
					}
				}
			}

			public virtual long GetBlockId()
			{
				lock (this)
				{
					return this.theBlock.GetBlockId();
				}
			}

			public virtual long GetVisibleLength()
			{
				lock (this)
				{
					return this.GetBytesAcked();
				}
			}

			public virtual HdfsServerConstants.ReplicaState GetState()
			{
				return this.finalized ? HdfsServerConstants.ReplicaState.Finalized : HdfsServerConstants.ReplicaState
					.Rbw;
			}

			public virtual long GetBytesAcked()
			{
				lock (this)
				{
					if (this.finalized)
					{
						return this.theBlock.GetNumBytes();
					}
					else
					{
						return this.bytesAcked;
					}
				}
			}

			public virtual void SetBytesAcked(long bytesAcked)
			{
				lock (this)
				{
					if (!this.finalized)
					{
						this.bytesAcked = bytesAcked;
					}
				}
			}

			public virtual void ReleaseAllBytesReserved()
			{
			}

			public virtual long GetBytesOnDisk()
			{
				lock (this)
				{
					if (this.finalized)
					{
						return this.theBlock.GetNumBytes();
					}
					else
					{
						return this.oStream.GetLength();
					}
				}
			}

			public virtual void SetLastChecksumAndDataLen(long dataLength, byte[] lastChecksum
				)
			{
				this.oStream.SetLength(dataLength);
			}

			public virtual ChunkChecksum GetLastChecksumAndDataLen()
			{
				return new ChunkChecksum(this.oStream.GetLength(), null);
			}

			public virtual bool IsOnTransientStorage()
			{
				return false;
			}

			private readonly SimulatedFSDataset _enclosing;
		}

		/// <summary>
		/// Class is used for tracking block pool storage utilization similar
		/// to
		/// <see cref="BlockPoolSlice"/>
		/// </summary>
		private class SimulatedBPStorage
		{
			private long used;

			// in bytes
			internal virtual long GetUsed()
			{
				return used;
			}

			internal virtual void Alloc(long amount)
			{
				used += amount;
			}

			internal virtual void Free(long amount)
			{
				used -= amount;
			}

			internal SimulatedBPStorage()
			{
				used = 0;
			}
		}

		/// <summary>
		/// Class used for tracking datanode level storage utilization similar
		/// to
		/// <see cref="FSVolumeSet"/>
		/// </summary>
		private class SimulatedStorage
		{
			private readonly IDictionary<string, SimulatedFSDataset.SimulatedBPStorage> map = 
				new Dictionary<string, SimulatedFSDataset.SimulatedBPStorage>();

			private readonly long capacity;

			private readonly DatanodeStorage dnStorage;

			// in bytes
			internal virtual long GetFree()
			{
				lock (this)
				{
					return capacity - GetUsed();
				}
			}

			internal virtual long GetCapacity()
			{
				return capacity;
			}

			internal virtual long GetUsed()
			{
				lock (this)
				{
					long used = 0;
					foreach (SimulatedFSDataset.SimulatedBPStorage bpStorage in map.Values)
					{
						used += bpStorage.GetUsed();
					}
					return used;
				}
			}

			/// <exception cref="System.IO.IOException"/>
			internal virtual long GetBlockPoolUsed(string bpid)
			{
				lock (this)
				{
					return GetBPStorage(bpid).GetUsed();
				}
			}

			internal virtual int GetNumFailedVolumes()
			{
				return 0;
			}

			/// <exception cref="System.IO.IOException"/>
			internal virtual bool Alloc(string bpid, long amount)
			{
				lock (this)
				{
					if (GetFree() >= amount)
					{
						GetBPStorage(bpid).Alloc(amount);
						return true;
					}
					return false;
				}
			}

			/// <exception cref="System.IO.IOException"/>
			internal virtual void Free(string bpid, long amount)
			{
				lock (this)
				{
					GetBPStorage(bpid).Free(amount);
				}
			}

			internal SimulatedStorage(long cap, DatanodeStorage.State state)
			{
				capacity = cap;
				dnStorage = new DatanodeStorage("SimulatedStorage-" + DatanodeStorage.GenerateUuid
					(), state, StorageType.Default);
			}

			internal virtual void AddBlockPool(string bpid)
			{
				lock (this)
				{
					SimulatedFSDataset.SimulatedBPStorage bpStorage = map[bpid];
					if (bpStorage != null)
					{
						return;
					}
					map[bpid] = new SimulatedFSDataset.SimulatedBPStorage();
				}
			}

			internal virtual void RemoveBlockPool(string bpid)
			{
				lock (this)
				{
					Sharpen.Collections.Remove(map, bpid);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			private SimulatedFSDataset.SimulatedBPStorage GetBPStorage(string bpid)
			{
				SimulatedFSDataset.SimulatedBPStorage bpStorage = map[bpid];
				if (bpStorage == null)
				{
					throw new IOException("block pool " + bpid + " not found");
				}
				return bpStorage;
			}

			internal virtual string GetStorageUuid()
			{
				return dnStorage.GetStorageID();
			}

			internal virtual DatanodeStorage GetDnStorage()
			{
				return dnStorage;
			}

			internal virtual StorageReport GetStorageReport(string bpid)
			{
				lock (this)
				{
					return new StorageReport(dnStorage, false, GetCapacity(), GetUsed(), GetFree(), map
						[bpid].GetUsed());
				}
			}
		}

		internal class SimulatedVolume : FsVolumeSpi
		{
			private readonly SimulatedFSDataset.SimulatedStorage storage;

			internal SimulatedVolume(SimulatedFSDataset.SimulatedStorage storage)
			{
				this.storage = storage;
			}

			/// <exception cref="Sharpen.ClosedChannelException"/>
			public override FsVolumeReference ObtainReference()
			{
				return null;
			}

			public override string GetStorageID()
			{
				return storage.GetStorageUuid();
			}

			public override string[] GetBlockPoolList()
			{
				return new string[0];
			}

			/// <exception cref="System.IO.IOException"/>
			public override long GetAvailable()
			{
				return storage.GetCapacity() - storage.GetUsed();
			}

			public override string GetBasePath()
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public override string GetPath(string bpid)
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public override FilePath GetFinalizedDir(string bpid)
			{
				return null;
			}

			public override StorageType GetStorageType()
			{
				return null;
			}

			public override bool IsTransientStorage()
			{
				return false;
			}

			public override void ReserveSpaceForRbw(long bytesToReserve)
			{
			}

			public override void ReleaseReservedSpace(long bytesToRelease)
			{
			}

			public override FsVolumeSpi.BlockIterator NewBlockIterator(string bpid, string name
				)
			{
				throw new NotSupportedException();
			}

			/// <exception cref="System.IO.IOException"/>
			public override FsVolumeSpi.BlockIterator LoadBlockIterator(string bpid, string name
				)
			{
				throw new NotSupportedException();
			}

			public override FsDatasetSpi GetDataset()
			{
				throw new NotSupportedException();
			}
		}

		private readonly IDictionary<string, IDictionary<Block, SimulatedFSDataset.BInfo>
			> blockMap = new Dictionary<string, IDictionary<Block, SimulatedFSDataset.BInfo>
			>();

		private readonly SimulatedFSDataset.SimulatedStorage storage;

		private readonly SimulatedFSDataset.SimulatedVolume volume;

		private readonly string datanodeUuid;

		public SimulatedFSDataset(DataStorage storage, Configuration conf)
		{
			if (storage != null)
			{
				for (int i = 0; i < storage.GetNumStorageDirs(); ++i)
				{
					storage.CreateStorageID(storage.GetStorageDir(i), false);
				}
				this.datanodeUuid = storage.GetDatanodeUuid();
			}
			else
			{
				this.datanodeUuid = "SimulatedDatanode-" + DataNode.GenerateUuid();
			}
			RegisterMBean(datanodeUuid);
			this.storage = new SimulatedFSDataset.SimulatedStorage(conf.GetLong(ConfigPropertyCapacity
				, DefaultCapacity), conf.GetEnum(ConfigPropertyState, DefaultState));
			this.volume = new SimulatedFSDataset.SimulatedVolume(this.storage);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void InjectBlocks<_T0>(string bpid, IEnumerable<_T0> injectBlocks)
			where _T0 : Block
		{
			lock (this)
			{
				ExtendedBlock blk = new ExtendedBlock();
				if (injectBlocks != null)
				{
					foreach (Block b in injectBlocks)
					{
						// if any blocks in list is bad, reject list
						if (b == null)
						{
							throw new ArgumentNullException("Null blocks in block list");
						}
						blk.Set(bpid, b);
						if (IsValidBlock(blk))
						{
							throw new IOException("Block already exists in  block list");
						}
					}
					IDictionary<Block, SimulatedFSDataset.BInfo> map = blockMap[bpid];
					if (map == null)
					{
						map = new Dictionary<Block, SimulatedFSDataset.BInfo>();
						blockMap[bpid] = map;
					}
					foreach (Block b_1 in injectBlocks)
					{
						SimulatedFSDataset.BInfo binfo = new SimulatedFSDataset.BInfo(this, bpid, b_1, false
							);
						map[binfo.theBlock] = binfo;
					}
				}
			}
		}

		/// <summary>Get a map for a given block pool Id</summary>
		/// <exception cref="System.IO.IOException"/>
		private IDictionary<Block, SimulatedFSDataset.BInfo> GetMap(string bpid)
		{
			IDictionary<Block, SimulatedFSDataset.BInfo> map = blockMap[bpid];
			if (map == null)
			{
				throw new IOException("Non existent blockpool " + bpid);
			}
			return map;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void FinalizeBlock(ExtendedBlock b)
		{
			lock (this)
			{
				// FsDatasetSpi
				IDictionary<Block, SimulatedFSDataset.BInfo> map = GetMap(b.GetBlockPoolId());
				SimulatedFSDataset.BInfo binfo = map[b.GetLocalBlock()];
				if (binfo == null)
				{
					throw new IOException("Finalizing a non existing block " + b);
				}
				binfo.FinalizeBlock(b.GetBlockPoolId(), b.GetNumBytes());
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void UnfinalizeBlock(ExtendedBlock b)
		{
			lock (this)
			{
				// FsDatasetSpi
				if (IsValidRbw(b))
				{
					IDictionary<Block, SimulatedFSDataset.BInfo> map = GetMap(b.GetBlockPoolId());
					Sharpen.Collections.Remove(map, b.GetLocalBlock());
				}
			}
		}

		internal virtual BlockListAsLongs GetBlockReport(string bpid)
		{
			lock (this)
			{
				BlockListAsLongs.Builder report = BlockListAsLongs.Builder();
				IDictionary<Block, SimulatedFSDataset.BInfo> map = blockMap[bpid];
				if (map != null)
				{
					foreach (SimulatedFSDataset.BInfo b in map.Values)
					{
						if (b.IsFinalized())
						{
							report.Add(b);
						}
					}
				}
				return report.Build();
			}
		}

		public override IDictionary<DatanodeStorage, BlockListAsLongs> GetBlockReports(string
			 bpid)
		{
			lock (this)
			{
				return Sharpen.Collections.SingletonMap(storage.GetDnStorage(), GetBlockReport(bpid
					));
			}
		}

		public override IList<long> GetCacheReport(string bpid)
		{
			// FsDatasetSpi
			return new List<long>();
		}

		public virtual long GetCapacity()
		{
			// FSDatasetMBean
			return storage.GetCapacity();
		}

		public virtual long GetDfsUsed()
		{
			// FSDatasetMBean
			return storage.GetUsed();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual long GetBlockPoolUsed(string bpid)
		{
			// FSDatasetMBean
			return storage.GetBlockPoolUsed(bpid);
		}

		public virtual long GetRemaining()
		{
			// FSDatasetMBean
			return storage.GetFree();
		}

		public virtual int GetNumFailedVolumes()
		{
			// FSDatasetMBean
			return storage.GetNumFailedVolumes();
		}

		public virtual string[] GetFailedStorageLocations()
		{
			// FSDatasetMBean
			return null;
		}

		public virtual long GetLastVolumeFailureDate()
		{
			// FSDatasetMBean
			return 0;
		}

		public virtual long GetEstimatedCapacityLostTotal()
		{
			// FSDatasetMBean
			return 0;
		}

		public override VolumeFailureSummary GetVolumeFailureSummary()
		{
			// FsDatasetSpi
			return new VolumeFailureSummary(ArrayUtils.EmptyStringArray, 0, 0);
		}

		public virtual long GetCacheUsed()
		{
			// FSDatasetMBean
			return 0l;
		}

		public virtual long GetCacheCapacity()
		{
			// FSDatasetMBean
			return 0l;
		}

		public virtual long GetNumBlocksCached()
		{
			// FSDatasetMBean
			return 0l;
		}

		public virtual long GetNumBlocksFailedToCache()
		{
			return 0l;
		}

		public virtual long GetNumBlocksFailedToUncache()
		{
			return 0l;
		}

		/// <exception cref="System.IO.IOException"/>
		public override long GetLength(ExtendedBlock b)
		{
			lock (this)
			{
				// FsDatasetSpi
				IDictionary<Block, SimulatedFSDataset.BInfo> map = GetMap(b.GetBlockPoolId());
				SimulatedFSDataset.BInfo binfo = map[b.GetLocalBlock()];
				if (binfo == null)
				{
					throw new IOException("Finalizing a non existing block " + b);
				}
				return binfo.GetNumBytes();
			}
		}

		[Obsolete]
		public override Replica GetReplica(string bpid, long blockId)
		{
			IDictionary<Block, SimulatedFSDataset.BInfo> map = blockMap[bpid];
			if (map != null)
			{
				return map[new Block(blockId)];
			}
			return null;
		}

		public override string GetReplicaString(string bpid, long blockId)
		{
			lock (this)
			{
				Replica r = null;
				IDictionary<Block, SimulatedFSDataset.BInfo> map = blockMap[bpid];
				if (map != null)
				{
					r = map[new Block(blockId)];
				}
				return r == null ? "null" : r.ToString();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override Block GetStoredBlock(string bpid, long blkid)
		{
			// FsDatasetSpi
			IDictionary<Block, SimulatedFSDataset.BInfo> map = blockMap[bpid];
			if (map != null)
			{
				SimulatedFSDataset.BInfo binfo = map[new Block(blkid)];
				if (binfo == null)
				{
					return null;
				}
				return new Block(blkid, binfo.GetGenerationStamp(), binfo.GetNumBytes());
			}
			return null;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Invalidate(string bpid, Block[] invalidBlks)
		{
			lock (this)
			{
				// FsDatasetSpi
				bool error = false;
				if (invalidBlks == null)
				{
					return;
				}
				IDictionary<Block, SimulatedFSDataset.BInfo> map = GetMap(bpid);
				foreach (Block b in invalidBlks)
				{
					if (b == null)
					{
						continue;
					}
					SimulatedFSDataset.BInfo binfo = map[b];
					if (binfo == null)
					{
						error = true;
						DataNode.Log.Warn("Invalidate: Missing block");
						continue;
					}
					storage.Free(bpid, binfo.GetNumBytes());
					Sharpen.Collections.Remove(map, b);
				}
				if (error)
				{
					throw new IOException("Invalidate: Missing blocks.");
				}
			}
		}

		public override void Cache(string bpid, long[] cacheBlks)
		{
			// FSDatasetSpi
			throw new NotSupportedException("SimulatedFSDataset does not support cache operation!"
				);
		}

		public override void Uncache(string bpid, long[] uncacheBlks)
		{
			// FSDatasetSpi
			throw new NotSupportedException("SimulatedFSDataset does not support uncache operation!"
				);
		}

		public override bool IsCached(string bpid, long blockId)
		{
			// FSDatasetSpi
			return false;
		}

		private SimulatedFSDataset.BInfo GetBInfo(ExtendedBlock b)
		{
			IDictionary<Block, SimulatedFSDataset.BInfo> map = blockMap[b.GetBlockPoolId()];
			return map == null ? null : map[b.GetLocalBlock()];
		}

		public override bool Contains(ExtendedBlock block)
		{
			// {@link FsDatasetSpi}
			return GetBInfo(block) != null;
		}

		/// <summary>Check if a block is valid.</summary>
		/// <param name="b">The block to check.</param>
		/// <param name="minLength">The minimum length that the block must have.  May be 0.</param>
		/// <param name="state">
		/// If this is null, it is ignored.  If it is non-null, we
		/// will check that the replica has this state.
		/// </param>
		/// <exception cref="ReplicaNotFoundException">If the replica is not found</exception>
		/// <exception cref="UnexpectedReplicaStateException">
		/// If the replica is not in the
		/// expected state.
		/// </exception>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Server.Datanode.ReplicaNotFoundException"
		/// 	/>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Server.Datanode.UnexpectedReplicaStateException
		/// 	"/>
		public override void CheckBlock(ExtendedBlock b, long minLength, HdfsServerConstants.ReplicaState
			 state)
		{
			// {@link FsDatasetSpi}
			SimulatedFSDataset.BInfo binfo = GetBInfo(b);
			if (binfo == null)
			{
				throw new ReplicaNotFoundException(b);
			}
			if ((state == HdfsServerConstants.ReplicaState.Finalized && !binfo.IsFinalized())
				 || (state != HdfsServerConstants.ReplicaState.Finalized && binfo.IsFinalized()))
			{
				throw new UnexpectedReplicaStateException(b, state);
			}
		}

		public override bool IsValidBlock(ExtendedBlock b)
		{
			lock (this)
			{
				// FsDatasetSpi
				try
				{
					CheckBlock(b, 0, HdfsServerConstants.ReplicaState.Finalized);
				}
				catch (IOException)
				{
					return false;
				}
				return true;
			}
		}

		/* check if a block is created but not finalized */
		public override bool IsValidRbw(ExtendedBlock b)
		{
			lock (this)
			{
				try
				{
					CheckBlock(b, 0, HdfsServerConstants.ReplicaState.Rbw);
				}
				catch (IOException)
				{
					return false;
				}
				return true;
			}
		}

		public override string ToString()
		{
			return GetStorageInfo();
		}

		/// <exception cref="System.IO.IOException"/>
		public override ReplicaHandler Append(ExtendedBlock b, long newGS, long expectedBlockLen
			)
		{
			lock (this)
			{
				// FsDatasetSpi
				IDictionary<Block, SimulatedFSDataset.BInfo> map = GetMap(b.GetBlockPoolId());
				SimulatedFSDataset.BInfo binfo = map[b.GetLocalBlock()];
				if (binfo == null || !binfo.IsFinalized())
				{
					throw new ReplicaNotFoundException("Block " + b + " is not valid, and cannot be appended to."
						);
				}
				binfo.UnfinalizeBlock();
				return new ReplicaHandler(binfo, null);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override ReplicaHandler RecoverAppend(ExtendedBlock b, long newGS, long expectedBlockLen
			)
		{
			lock (this)
			{
				// FsDatasetSpi
				IDictionary<Block, SimulatedFSDataset.BInfo> map = GetMap(b.GetBlockPoolId());
				SimulatedFSDataset.BInfo binfo = map[b.GetLocalBlock()];
				if (binfo == null)
				{
					throw new ReplicaNotFoundException("Block " + b + " is not valid, and cannot be appended to."
						);
				}
				if (binfo.IsFinalized())
				{
					binfo.UnfinalizeBlock();
				}
				Sharpen.Collections.Remove(map, b);
				binfo.theBlock.SetGenerationStamp(newGS);
				map[binfo.theBlock] = binfo;
				return new ReplicaHandler(binfo, null);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override string RecoverClose(ExtendedBlock b, long newGS, long expectedBlockLen
			)
		{
			// FsDatasetSpi
			IDictionary<Block, SimulatedFSDataset.BInfo> map = GetMap(b.GetBlockPoolId());
			SimulatedFSDataset.BInfo binfo = map[b.GetLocalBlock()];
			if (binfo == null)
			{
				throw new ReplicaNotFoundException("Block " + b + " is not valid, and cannot be appended to."
					);
			}
			if (!binfo.IsFinalized())
			{
				binfo.FinalizeBlock(b.GetBlockPoolId(), binfo.GetNumBytes());
			}
			Sharpen.Collections.Remove(map, b.GetLocalBlock());
			binfo.theBlock.SetGenerationStamp(newGS);
			map[binfo.theBlock] = binfo;
			return binfo.GetStorageUuid();
		}

		/// <exception cref="System.IO.IOException"/>
		public override ReplicaHandler RecoverRbw(ExtendedBlock b, long newGS, long minBytesRcvd
			, long maxBytesRcvd)
		{
			lock (this)
			{
				// FsDatasetSpi
				IDictionary<Block, SimulatedFSDataset.BInfo> map = GetMap(b.GetBlockPoolId());
				SimulatedFSDataset.BInfo binfo = map[b.GetLocalBlock()];
				if (binfo == null)
				{
					throw new ReplicaNotFoundException("Block " + b + " does not exist, and cannot be appended to."
						);
				}
				if (binfo.IsFinalized())
				{
					throw new ReplicaAlreadyExistsException("Block " + b + " is valid, and cannot be written to."
						);
				}
				Sharpen.Collections.Remove(map, b);
				binfo.theBlock.SetGenerationStamp(newGS);
				map[binfo.theBlock] = binfo;
				return new ReplicaHandler(binfo, null);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override ReplicaHandler CreateRbw(StorageType storageType, ExtendedBlock b
			, bool allowLazyPersist)
		{
			lock (this)
			{
				// FsDatasetSpi
				return CreateTemporary(storageType, b);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override ReplicaHandler CreateTemporary(StorageType storageType, ExtendedBlock
			 b)
		{
			lock (this)
			{
				// FsDatasetSpi
				if (IsValidBlock(b))
				{
					throw new ReplicaAlreadyExistsException("Block " + b + " is valid, and cannot be written to."
						);
				}
				if (IsValidRbw(b))
				{
					throw new ReplicaAlreadyExistsException("Block " + b + " is being written, and cannot be written to."
						);
				}
				IDictionary<Block, SimulatedFSDataset.BInfo> map = GetMap(b.GetBlockPoolId());
				SimulatedFSDataset.BInfo binfo = new SimulatedFSDataset.BInfo(this, b.GetBlockPoolId
					(), b.GetLocalBlock(), true);
				map[binfo.theBlock] = binfo;
				return new ReplicaHandler(binfo, null);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual InputStream GetBlockInputStream(ExtendedBlock b)
		{
			lock (this)
			{
				IDictionary<Block, SimulatedFSDataset.BInfo> map = GetMap(b.GetBlockPoolId());
				SimulatedFSDataset.BInfo binfo = map[b.GetLocalBlock()];
				if (binfo == null)
				{
					throw new IOException("No such Block " + b);
				}
				return binfo.GetIStream();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override InputStream GetBlockInputStream(ExtendedBlock b, long seekOffset)
		{
			lock (this)
			{
				// FsDatasetSpi
				InputStream result = GetBlockInputStream(b);
				IOUtils.SkipFully(result, seekOffset);
				return result;
			}
		}

		/// <summary>Not supported</summary>
		/// <exception cref="System.IO.IOException"/>
		public override ReplicaInputStreams GetTmpInputStreams(ExtendedBlock b, long blkoff
			, long ckoff)
		{
			// FsDatasetSpi
			throw new IOException("Not supported");
		}

		/// <exception cref="System.IO.IOException"/>
		public override LengthInputStream GetMetaDataInputStream(ExtendedBlock b)
		{
			lock (this)
			{
				// FsDatasetSpi
				IDictionary<Block, SimulatedFSDataset.BInfo> map = GetMap(b.GetBlockPoolId());
				SimulatedFSDataset.BInfo binfo = map[b.GetLocalBlock()];
				if (binfo == null)
				{
					throw new IOException("No such Block " + b);
				}
				if (!binfo.finalized)
				{
					throw new IOException("Block " + b + " is being written, its meta cannot be read"
						);
				}
				SimulatedFSDataset.SimulatedInputStream sin = binfo.GetMetaIStream();
				return new LengthInputStream(sin, sin.GetLength());
			}
		}

		public override ICollection<FilePath> CheckDataDir()
		{
			// nothing to check for simulated data set
			return null;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void AdjustCrcChannelPosition(ExtendedBlock b, ReplicaOutputStreams
			 stream, int checksumSize)
		{
			lock (this)
			{
			}
		}

		/// <summary>Simulated input and output streams</summary>
		private class SimulatedInputStream : InputStream
		{
			internal byte theRepeatedData = 7;

			internal readonly long length;

			internal int currentPos = 0;

			internal byte[] data = null;

			/// <summary>An input stream of size l with repeated bytes</summary>
			/// <param name="l">size of the stream</param>
			/// <param name="iRepeatedData">byte that is repeated in the stream</param>
			internal SimulatedInputStream(long l, byte iRepeatedData)
			{
				// FsDatasetSpi
				// bytes
				length = l;
				theRepeatedData = iRepeatedData;
			}

			/// <summary>An input stream of of the supplied data</summary>
			/// <param name="iData">data to construct the stream</param>
			internal SimulatedInputStream(byte[] iData)
			{
				data = iData;
				length = data.Length;
			}

			/// <returns>the lenght of the input stream</returns>
			internal virtual long GetLength()
			{
				return length;
			}

			/// <exception cref="System.IO.IOException"/>
			public override int Read()
			{
				if (currentPos >= length)
				{
					return -1;
				}
				if (data != null)
				{
					return data[currentPos++];
				}
				else
				{
					currentPos++;
					return theRepeatedData;
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public override int Read(byte[] b)
			{
				if (b == null)
				{
					throw new ArgumentNullException();
				}
				if (b.Length == 0)
				{
					return 0;
				}
				if (currentPos >= length)
				{
					// EOF
					return -1;
				}
				int bytesRead = (int)Math.Min(b.Length, length - currentPos);
				if (data != null)
				{
					System.Array.Copy(data, currentPos, b, 0, bytesRead);
				}
				else
				{
					// all data is zero
					foreach (int i in b)
					{
						b[i] = theRepeatedData;
					}
				}
				currentPos += bytesRead;
				return bytesRead;
			}
		}

		/// <summary>
		/// This class implements an output stream that merely throws its data away, but records its
		/// length.
		/// </summary>
		private class SimulatedOutputStream : OutputStream
		{
			internal long length = 0;

			/// <summary>constructor for Simulated Output Steram</summary>
			internal SimulatedOutputStream()
			{
			}

			/// <returns>the length of the data created so far.</returns>
			internal virtual long GetLength()
			{
				return length;
			}

			internal virtual void SetLength(long length)
			{
				this.length = length;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Write(int arg0)
			{
				length++;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Write(byte[] b)
			{
				length += b.Length;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Write(byte[] b, int off, int len)
			{
				length += len;
			}
		}

		private ObjectName mbeanName;

		/// <summary>
		/// Register the FSDataset MBean using the name
		/// "hadoop:service=DataNode,name=FSDatasetState-<storageid>"
		/// We use storage id for MBean name since a minicluster within a single
		/// Java VM may have multiple Simulated Datanodes.
		/// </summary>
		internal virtual void RegisterMBean(string storageId)
		{
			// We wrap to bypass standard mbean naming convetion.
			// This wraping can be removed in java 6 as it is more flexible in 
			// package naming for mbeans and their impl.
			StandardMBean bean;
			try
			{
				bean = new StandardMBean(this, typeof(FSDatasetMBean));
				mbeanName = MBeans.Register("DataNode", "FSDatasetState-" + storageId, bean);
			}
			catch (NotCompliantMBeanException e)
			{
				DataNode.Log.Warn("Error registering FSDatasetState MBean", e);
			}
			DataNode.Log.Info("Registered FSDatasetState MBean");
		}

		public override void Shutdown()
		{
			if (mbeanName != null)
			{
				MBeans.Unregister(mbeanName);
			}
		}

		public virtual string GetStorageInfo()
		{
			return "Simulated FSDataset-" + datanodeUuid;
		}

		public override bool HasEnoughResource()
		{
			return true;
		}

		/// <exception cref="System.IO.IOException"/>
		public override ReplicaRecoveryInfo InitReplicaRecovery(BlockRecoveryCommand.RecoveringBlock
			 rBlock)
		{
			ExtendedBlock b = rBlock.GetBlock();
			IDictionary<Block, SimulatedFSDataset.BInfo> map = GetMap(b.GetBlockPoolId());
			SimulatedFSDataset.BInfo binfo = map[b.GetLocalBlock()];
			if (binfo == null)
			{
				throw new IOException("No such Block " + b);
			}
			return new ReplicaRecoveryInfo(binfo.GetBlockId(), binfo.GetBytesOnDisk(), binfo.
				GetGenerationStamp(), binfo.IsFinalized() ? HdfsServerConstants.ReplicaState.Finalized
				 : HdfsServerConstants.ReplicaState.Rbw);
		}

		public override string UpdateReplicaUnderRecovery(ExtendedBlock oldBlock, long recoveryId
			, long newBlockId, long newlength)
		{
			// FsDatasetSpi
			// Caller does not care about the exact Storage UUID returned.
			return datanodeUuid;
		}

		public override long GetReplicaVisibleLength(ExtendedBlock block)
		{
			// FsDatasetSpi
			return block.GetNumBytes();
		}

		public override void AddBlockPool(string bpid, Configuration conf)
		{
			// FsDatasetSpi
			IDictionary<Block, SimulatedFSDataset.BInfo> map = new Dictionary<Block, SimulatedFSDataset.BInfo
				>();
			blockMap[bpid] = map;
			storage.AddBlockPool(bpid);
		}

		public override void ShutdownBlockPool(string bpid)
		{
			// FsDatasetSpi
			Sharpen.Collections.Remove(blockMap, bpid);
			storage.RemoveBlockPool(bpid);
		}

		public override void DeleteBlockPool(string bpid, bool force)
		{
			// FsDatasetSpi
			return;
		}

		/// <exception cref="System.IO.IOException"/>
		public override ReplicaInPipelineInterface ConvertTemporaryToRbw(ExtendedBlock temporary
			)
		{
			IDictionary<Block, SimulatedFSDataset.BInfo> map = blockMap[temporary.GetBlockPoolId
				()];
			if (map == null)
			{
				throw new IOException("Block pool not found, temporary=" + temporary);
			}
			SimulatedFSDataset.BInfo r = map[temporary.GetLocalBlock()];
			if (r == null)
			{
				throw new IOException("Block not found, temporary=" + temporary);
			}
			else
			{
				if (r.IsFinalized())
				{
					throw new IOException("Replica already finalized, temporary=" + temporary + ", r="
						 + r);
				}
			}
			return r;
		}

		public override BlockLocalPathInfo GetBlockLocalPathInfo(ExtendedBlock b)
		{
			throw new NotSupportedException();
		}

		/// <exception cref="System.IO.IOException"/>
		public override HdfsBlocksMetadata GetHdfsBlocksMetadata(string bpid, long[] blockIds
			)
		{
			throw new NotSupportedException();
		}

		public override void EnableTrash(string bpid)
		{
			throw new NotSupportedException();
		}

		public override void ClearTrash(string bpid)
		{
		}

		public override bool TrashEnabled(string bpid)
		{
			return false;
		}

		public override void SetRollingUpgradeMarker(string bpid)
		{
		}

		public override void ClearRollingUpgradeMarker(string bpid)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public override void CheckAndUpdate(string bpid, long blockId, FilePath diskFile, 
			FilePath diskMetaFile, FsVolumeSpi vol)
		{
			throw new NotSupportedException();
		}

		public override IList<FsVolumeSpi> GetVolumes()
		{
			throw new NotSupportedException();
		}

		/// <exception cref="System.IO.IOException"/>
		public override void AddVolume(StorageLocation location, IList<NamespaceInfo> nsInfos
			)
		{
			throw new NotSupportedException();
		}

		public override DatanodeStorage GetStorage(string storageUuid)
		{
			return storageUuid.Equals(storage.GetStorageUuid()) ? storage.dnStorage : null;
		}

		public override StorageReport[] GetStorageReports(string bpid)
		{
			return new StorageReport[] { storage.GetStorageReport(bpid) };
		}

		public override IList<FinalizedReplica> GetFinalizedBlocks(string bpid)
		{
			throw new NotSupportedException();
		}

		public override IList<FinalizedReplica> GetFinalizedBlocksOnPersistentStorage(string
			 bpid)
		{
			throw new NotSupportedException();
		}

		public override IDictionary<string, object> GetVolumeInfoMap()
		{
			throw new NotSupportedException();
		}

		public override FsVolumeSpi GetVolume(ExtendedBlock b)
		{
			return volume;
		}

		public override void RemoveVolumes(ICollection<FilePath> volumes, bool clearFailure
			)
		{
			lock (this)
			{
				throw new NotSupportedException();
			}
		}

		public override void SubmitBackgroundSyncFileRangeRequest(ExtendedBlock block, FileDescriptor
			 fd, long offset, long nbytes, int flags)
		{
			throw new NotSupportedException();
		}

		public override void OnCompleteLazyPersist(string bpId, long blockId, long creationTime
			, FilePath[] savedFiles, FsVolumeSpi targetVolume)
		{
			throw new NotSupportedException();
		}

		public override void OnFailLazyPersist(string bpId, long blockId)
		{
			throw new NotSupportedException();
		}

		/// <exception cref="System.IO.IOException"/>
		public override ReplicaInfo MoveBlockAcrossStorage(ExtendedBlock block, StorageType
			 targetStorageType)
		{
			// TODO Auto-generated method stub
			return null;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void SetPinning(ExtendedBlock b)
		{
			blockMap[b.GetBlockPoolId()][b.GetLocalBlock()].pinned = true;
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool GetPinning(ExtendedBlock b)
		{
			return blockMap[b.GetBlockPoolId()][b.GetLocalBlock()].pinned;
		}

		public override bool IsDeletingBlock(string bpid, long blockId)
		{
			throw new NotSupportedException();
		}
	}
}
