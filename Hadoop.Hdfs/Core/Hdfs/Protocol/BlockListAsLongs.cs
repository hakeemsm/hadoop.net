using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Base;
using Com.Google.Protobuf;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Protocol
{
	public abstract class BlockListAsLongs : IEnumerable<BlockListAsLongs.BlockReportReplica
		>
	{
		private const int ChunkSize = 64 * 1024;

		private static long[] EmptyLongs = new long[] { 0, 0 };

		private sealed class _BlockListAsLongs_43 : BlockListAsLongs
		{
			public _BlockListAsLongs_43()
			{
			}

			// 64K
			public override int GetNumberOfBlocks()
			{
				return 0;
			}

			public override ByteString GetBlocksBuffer()
			{
				return ByteString.Empty;
			}

			public override long[] GetBlockListAsLongs()
			{
				return BlockListAsLongs.EmptyLongs;
			}

			public override IEnumerator<BlockListAsLongs.BlockReportReplica> GetEnumerator()
			{
				return Sharpen.Collections.EmptyIterator();
			}
		}

		public static BlockListAsLongs Empty = new _BlockListAsLongs_43();

		/// <summary>Prepare an instance to in-place decode the given ByteString buffer</summary>
		/// <param name="numBlocks">- blocks in the buffer</param>
		/// <param name="blocksBuf">- ByteString encoded varints</param>
		/// <returns>BlockListAsLongs</returns>
		public static BlockListAsLongs DecodeBuffer(int numBlocks, ByteString blocksBuf)
		{
			return new BlockListAsLongs.BufferDecoder(numBlocks, blocksBuf);
		}

		/// <summary>Prepare an instance to in-place decode the given ByteString buffers</summary>
		/// <param name="numBlocks">- blocks in the buffers</param>
		/// <param name="blocksBufs">- list of ByteString encoded varints</param>
		/// <returns>BlockListAsLongs</returns>
		public static BlockListAsLongs DecodeBuffers(int numBlocks, IList<ByteString> blocksBufs
			)
		{
			// this doesn't actually copy the data
			return DecodeBuffer(numBlocks, ByteString.CopyFrom(blocksBufs));
		}

		/// <summary>Prepare an instance to in-place decode the given list of Longs.</summary>
		/// <remarks>
		/// Prepare an instance to in-place decode the given list of Longs.  Note
		/// it's much more efficient to decode ByteString buffers and only exists
		/// for compatibility.
		/// </remarks>
		/// <param name="blocksList">- list of longs</param>
		/// <returns>BlockListAsLongs</returns>
		public static BlockListAsLongs DecodeLongs(IList<long> blocksList)
		{
			return blocksList.IsEmpty() ? Empty : new BlockListAsLongs.LongsDecoder(blocksList
				);
		}

		/// <summary>
		/// Prepare an instance to encode the collection of replicas into an
		/// efficient ByteString.
		/// </summary>
		/// <param name="replicas">- replicas to encode</param>
		/// <returns>BlockListAsLongs</returns>
		public static BlockListAsLongs Encode<_T0>(ICollection<_T0> replicas)
			where _T0 : Replica
		{
			BlockListAsLongs.Builder builder = Builder();
			foreach (Replica replica in replicas)
			{
				builder.Add(replica);
			}
			return builder.Build();
		}

		public static BlockListAsLongs.Builder Builder()
		{
			return new BlockListAsLongs.Builder();
		}

		/// <summary>The number of blocks</summary>
		/// <returns>- the number of blocks</returns>
		public abstract int GetNumberOfBlocks();

		/// <summary>
		/// Very efficient encoding of the block report into a ByteString to avoid
		/// the overhead of protobuf repeating fields.
		/// </summary>
		/// <remarks>
		/// Very efficient encoding of the block report into a ByteString to avoid
		/// the overhead of protobuf repeating fields.  Primitive repeating fields
		/// require re-allocs of an ArrayList<Long> and the associated (un)boxing
		/// overhead which puts pressure on GC.
		/// The structure of the buffer is as follows:
		/// - each replica is represented by 4 longs:
		/// blockId, block length, genstamp, replica state
		/// </remarks>
		/// <returns>ByteString encoded block report</returns>
		public abstract ByteString GetBlocksBuffer();

		/// <summary>List of ByteStrings that encode this block report</summary>
		/// <returns>ByteStrings</returns>
		public virtual IList<ByteString> GetBlocksBuffers()
		{
			ByteString blocksBuf = GetBlocksBuffer();
			IList<ByteString> buffers;
			int size = blocksBuf.Size();
			if (size <= ChunkSize)
			{
				buffers = Sharpen.Collections.SingletonList(blocksBuf);
			}
			else
			{
				buffers = new AList<ByteString>();
				for (int pos = 0; pos < size; pos += ChunkSize)
				{
					// this doesn't actually copy the data
					buffers.AddItem(blocksBuf.Substring(pos, Math.Min(pos + ChunkSize, size)));
				}
			}
			return buffers;
		}

		/// <summary>Convert block report to old-style list of longs.</summary>
		/// <remarks>
		/// Convert block report to old-style list of longs.  Only used to
		/// re-encode the block report when the DN detects an older NN. This is
		/// inefficient, but in practice a DN is unlikely to be upgraded first
		/// The structure of the array is as follows:
		/// 0: the length of the finalized replica list;
		/// 1: the length of the under-construction replica list;
		/// - followed by finalized replica list where each replica is represented by
		/// 3 longs: one for the blockId, one for the block length, and one for
		/// the generation stamp;
		/// - followed by the invalid replica represented with three -1s;
		/// - followed by the under-construction replica list where each replica is
		/// represented by 4 longs: three for the block id, length, generation
		/// stamp, and the fourth for the replica state.
		/// </remarks>
		/// <returns>list of longs</returns>
		public abstract long[] GetBlockListAsLongs();

		/// <summary>Returns a singleton iterator over blocks in the block report.</summary>
		/// <remarks>
		/// Returns a singleton iterator over blocks in the block report.  Do not
		/// add the returned blocks to a collection.
		/// </remarks>
		/// <returns>Iterator</returns>
		public abstract override IEnumerator<BlockListAsLongs.BlockReportReplica> GetEnumerator
			();

		public class Builder
		{
			private readonly ByteString.Output @out;

			private readonly CodedOutputStream cos;

			private int numBlocks = 0;

			private int numFinalized = 0;

			internal Builder()
			{
				@out = ByteString.NewOutput(64 * 1024);
				cos = CodedOutputStream.NewInstance(@out);
			}

			public virtual void Add(Replica replica)
			{
				try
				{
					// zig-zag to reduce size of legacy blocks
					cos.WriteSInt64NoTag(replica.GetBlockId());
					cos.WriteRawVarint64(replica.GetBytesOnDisk());
					cos.WriteRawVarint64(replica.GetGenerationStamp());
					HdfsServerConstants.ReplicaState state = replica.GetState();
					// although state is not a 64-bit value, using a long varint to
					// allow for future use of the upper bits
					cos.WriteRawVarint64(state.GetValue());
					if (state == HdfsServerConstants.ReplicaState.Finalized)
					{
						numFinalized++;
					}
					numBlocks++;
				}
				catch (IOException ioe)
				{
					// shouldn't happen, ByteString.Output doesn't throw IOE
					throw new InvalidOperationException(ioe);
				}
			}

			public virtual int GetNumberOfBlocks()
			{
				return numBlocks;
			}

			public virtual BlockListAsLongs Build()
			{
				try
				{
					cos.Flush();
				}
				catch (IOException ioe)
				{
					// shouldn't happen, ByteString.Output doesn't throw IOE
					throw new InvalidOperationException(ioe);
				}
				return new BlockListAsLongs.BufferDecoder(numBlocks, numFinalized, @out.ToByteString
					());
			}
		}

		private class BufferDecoder : BlockListAsLongs
		{
			private static long NumBytesMask = (long)(((ulong)(-1L)) >> (64 - 48));

			private static long ReplicaStateMask = (long)(((ulong)(-1L)) >> (64 - 4));

			private readonly ByteString buffer;

			private readonly int numBlocks;

			private int numFinalized;

			internal BufferDecoder(int numBlocks, ByteString buf)
				: this(numBlocks, -1, buf)
			{
			}

			internal BufferDecoder(int numBlocks, int numFinalized, ByteString buf)
			{
				// decode new-style ByteString buffer based block report
				// reserve upper bits for future use.  decoding masks off these bits to
				// allow compatibility for the current through future release that may
				// start using the bits
				this.numBlocks = numBlocks;
				this.numFinalized = numFinalized;
				this.buffer = buf;
			}

			public override int GetNumberOfBlocks()
			{
				return numBlocks;
			}

			public override ByteString GetBlocksBuffer()
			{
				return buffer;
			}

			public override long[] GetBlockListAsLongs()
			{
				// terribly inefficient but only occurs if server tries to transcode
				// an undecoded buffer into longs - ie. it will never happen but let's
				// handle it anyway
				if (numFinalized == -1)
				{
					int n = 0;
					foreach (Replica replica in this)
					{
						if (replica.GetState() == HdfsServerConstants.ReplicaState.Finalized)
						{
							n++;
						}
					}
					numFinalized = n;
				}
				int numUc = numBlocks - numFinalized;
				int size = 2 + 3 * (numFinalized + 1) + 4 * (numUc);
				long[] longs = new long[size];
				longs[0] = numFinalized;
				longs[1] = numUc;
				int idx = 2;
				int ucIdx = idx + 3 * numFinalized;
				// delimiter block
				longs[ucIdx++] = -1;
				longs[ucIdx++] = -1;
				longs[ucIdx++] = -1;
				foreach (BlockListAsLongs.BlockReportReplica block in this)
				{
					switch (block.GetState())
					{
						case HdfsServerConstants.ReplicaState.Finalized:
						{
							longs[idx++] = block.GetBlockId();
							longs[idx++] = block.GetNumBytes();
							longs[idx++] = block.GetGenerationStamp();
							break;
						}

						default:
						{
							longs[ucIdx++] = block.GetBlockId();
							longs[ucIdx++] = block.GetNumBytes();
							longs[ucIdx++] = block.GetGenerationStamp();
							longs[ucIdx++] = block.GetState().GetValue();
							break;
						}
					}
				}
				return longs;
			}

			public override IEnumerator<BlockListAsLongs.BlockReportReplica> GetEnumerator()
			{
				return new _IEnumerator_310(this);
			}

			private sealed class _IEnumerator_310 : IEnumerator<BlockListAsLongs.BlockReportReplica
				>
			{
				public _IEnumerator_310(BufferDecoder _enclosing)
				{
					this._enclosing = _enclosing;
					this.block = new BlockListAsLongs.BlockReportReplica();
					this.cis = this._enclosing.buffer.NewCodedInput();
					this.currentBlockIndex = 0;
				}

				internal readonly BlockListAsLongs.BlockReportReplica block;

				internal readonly CodedInputStream cis;

				private int currentBlockIndex;

				public override bool HasNext()
				{
					return this.currentBlockIndex < this._enclosing.numBlocks;
				}

				public override BlockListAsLongs.BlockReportReplica Next()
				{
					this.currentBlockIndex++;
					try
					{
						// zig-zag to reduce size of legacy blocks and mask off bits
						// we don't (yet) understand
						this.block.SetBlockId(this.cis.ReadSInt64());
						this.block.SetNumBytes(this.cis.ReadRawVarint64() & BlockListAsLongs.BufferDecoder
							.NumBytesMask);
						this.block.SetGenerationStamp(this.cis.ReadRawVarint64());
						long state = this.cis.ReadRawVarint64() & BlockListAsLongs.BufferDecoder.ReplicaStateMask;
						this.block.SetState(HdfsServerConstants.ReplicaState.GetState((int)state));
					}
					catch (IOException e)
					{
						throw new InvalidOperationException(e);
					}
					return this.block;
				}

				public override void Remove()
				{
					throw new NotSupportedException();
				}

				private readonly BufferDecoder _enclosing;
			}
		}

		private class LongsDecoder : BlockListAsLongs
		{
			private readonly IList<long> values;

			private readonly int finalizedBlocks;

			private readonly int numBlocks;

			internal LongsDecoder(IList<long> values)
			{
				// decode old style block report of longs
				// set the header
				this.values = values.SubList(2, values.Count);
				this.finalizedBlocks = values[0];
				this.numBlocks = finalizedBlocks + values[1];
			}

			public override int GetNumberOfBlocks()
			{
				return numBlocks;
			}

			public override ByteString GetBlocksBuffer()
			{
				BlockListAsLongs.Builder builder = Builder();
				foreach (Replica replica in this)
				{
					builder.Add(replica);
				}
				return builder.Build().GetBlocksBuffer();
			}

			public override long[] GetBlockListAsLongs()
			{
				long[] longs = new long[2 + values.Count];
				longs[0] = finalizedBlocks;
				longs[1] = numBlocks - finalizedBlocks;
				for (int i = 0; i < longs.Length; i++)
				{
					longs[i] = values[i];
				}
				return longs;
			}

			public override IEnumerator<BlockListAsLongs.BlockReportReplica> GetEnumerator()
			{
				return new _IEnumerator_385(this);
			}

			private sealed class _IEnumerator_385 : IEnumerator<BlockListAsLongs.BlockReportReplica
				>
			{
				public _IEnumerator_385(LongsDecoder _enclosing)
				{
					this._enclosing = _enclosing;
					this.block = new BlockListAsLongs.BlockReportReplica();
					this.iter = this._enclosing.values.GetEnumerator();
					this.currentBlockIndex = 0;
				}

				private readonly BlockListAsLongs.BlockReportReplica block;

				internal readonly IEnumerator<long> iter;

				private int currentBlockIndex;

				public override bool HasNext()
				{
					return this.currentBlockIndex < this._enclosing.numBlocks;
				}

				public override BlockListAsLongs.BlockReportReplica Next()
				{
					if (this.currentBlockIndex == this._enclosing.finalizedBlocks)
					{
						// verify the presence of the delimiter block
						this.ReadBlock();
						Preconditions.CheckArgument(this.block.GetBlockId() == -1 && this.block.GetNumBytes
							() == -1 && this.block.GetGenerationStamp() == -1, "Invalid delimiter block");
					}
					this.ReadBlock();
					if (this.currentBlockIndex++ < this._enclosing.finalizedBlocks)
					{
						this.block.SetState(HdfsServerConstants.ReplicaState.Finalized);
					}
					else
					{
						this.block.SetState(HdfsServerConstants.ReplicaState.GetState(this.iter.Next()));
					}
					return this.block;
				}

				private void ReadBlock()
				{
					this.block.SetBlockId(this.iter.Next());
					this.block.SetNumBytes(this.iter.Next());
					this.block.SetGenerationStamp(this.iter.Next());
				}

				public override void Remove()
				{
					throw new NotSupportedException();
				}

				private readonly LongsDecoder _enclosing;
			}
		}

		public class BlockReportReplica : Block, Replica
		{
			private HdfsServerConstants.ReplicaState state;

			private BlockReportReplica()
			{
			}

			public BlockReportReplica(Block block)
				: base(block)
			{
				if (block is BlockListAsLongs.BlockReportReplica)
				{
					this.state = ((BlockListAsLongs.BlockReportReplica)block).GetState();
				}
				else
				{
					this.state = HdfsServerConstants.ReplicaState.Finalized;
				}
			}

			public virtual void SetState(HdfsServerConstants.ReplicaState state)
			{
				this.state = state;
			}

			public virtual HdfsServerConstants.ReplicaState GetState()
			{
				return state;
			}

			public virtual long GetBytesOnDisk()
			{
				return GetNumBytes();
			}

			public virtual long GetVisibleLength()
			{
				throw new NotSupportedException();
			}

			public virtual string GetStorageUuid()
			{
				throw new NotSupportedException();
			}

			public virtual bool IsOnTransientStorage()
			{
				throw new NotSupportedException();
			}

			public override bool Equals(object o)
			{
				return base.Equals(o);
			}

			public override int GetHashCode()
			{
				return base.GetHashCode();
			}
		}
	}
}
