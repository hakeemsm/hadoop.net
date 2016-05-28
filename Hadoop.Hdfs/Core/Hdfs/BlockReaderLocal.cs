using System;
using System.IO;
using System.Text;
using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Hdfs.Shortcircuit;
using Org.Apache.Hadoop.Util;
using Org.Apache.Htrace;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>BlockReaderLocal enables local short circuited reads.</summary>
	/// <remarks>
	/// BlockReaderLocal enables local short circuited reads. If the DFS client is on
	/// the same machine as the datanode, then the client can read files directly
	/// from the local file system rather than going through the datanode for better
	/// performance. <br />
	/// <see cref="BlockReaderLocal"/>
	/// works as follows:
	/// <ul>
	/// <li>The client performing short circuit reads must be configured at the
	/// datanode.</li>
	/// <li>The client gets the file descriptors for the metadata file and the data
	/// file for the block using
	/// <see cref="org.apache.hadoop.hdfs.server.datanode.DataXceiver#requestShortCircuitFds
	/// 	"/>
	/// .
	/// </li>
	/// <li>The client reads the file descriptors.</li>
	/// </ul>
	/// </remarks>
	internal class BlockReaderLocal : BlockReader
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.BlockReaderLocal
			));

		private static readonly DirectBufferPool bufferPool = new DirectBufferPool();

		public class Builder
		{
			private readonly int bufferSize;

			private bool verifyChecksum;

			private int maxReadahead;

			private string filename;

			private ShortCircuitReplica replica;

			private long dataPos;

			private ExtendedBlock block;

			private StorageType storageType;

			public Builder(DFSClient.Conf conf)
			{
				this.maxReadahead = int.MaxValue;
				this.verifyChecksum = !conf.skipShortCircuitChecksums;
				this.bufferSize = conf.shortCircuitBufferSize;
			}

			public virtual BlockReaderLocal.Builder SetVerifyChecksum(bool verifyChecksum)
			{
				this.verifyChecksum = verifyChecksum;
				return this;
			}

			public virtual BlockReaderLocal.Builder SetCachingStrategy(CachingStrategy cachingStrategy
				)
			{
				long readahead = cachingStrategy.GetReadahead() != null ? cachingStrategy.GetReadahead
					() : DFSConfigKeys.DfsDatanodeReadaheadBytesDefault;
				this.maxReadahead = (int)Math.Min(int.MaxValue, readahead);
				return this;
			}

			public virtual BlockReaderLocal.Builder SetFilename(string filename)
			{
				this.filename = filename;
				return this;
			}

			public virtual BlockReaderLocal.Builder SetShortCircuitReplica(ShortCircuitReplica
				 replica)
			{
				this.replica = replica;
				return this;
			}

			public virtual BlockReaderLocal.Builder SetStartOffset(long startOffset)
			{
				this.dataPos = Math.Max(0, startOffset);
				return this;
			}

			public virtual BlockReaderLocal.Builder SetBlock(ExtendedBlock block)
			{
				this.block = block;
				return this;
			}

			public virtual BlockReaderLocal.Builder SetStorageType(StorageType storageType)
			{
				this.storageType = storageType;
				return this;
			}

			public virtual BlockReaderLocal Build()
			{
				Preconditions.CheckNotNull(replica);
				return new BlockReaderLocal(this);
			}
		}

		private bool closed = false;

		/// <summary>Pair of streams for this block.</summary>
		private readonly ShortCircuitReplica replica;

		/// <summary>The data FileChannel.</summary>
		private readonly FileChannel dataIn;

		/// <summary>The next place we'll read from in the block data FileChannel.</summary>
		/// <remarks>
		/// The next place we'll read from in the block data FileChannel.
		/// If data is buffered in dataBuf, this offset will be larger than the
		/// offset of the next byte which a read() operation will give us.
		/// </remarks>
		private long dataPos;

		/// <summary>The Checksum FileChannel.</summary>
		private readonly FileChannel checksumIn;

		/// <summary>Checksum type and size.</summary>
		private readonly DataChecksum checksum;

		/// <summary>If false, we will always skip the checksum.</summary>
		private readonly bool verifyChecksum;

		/// <summary>Name of the block, for logging purposes.</summary>
		private readonly string filename;

		/// <summary>Block ID and Block Pool ID.</summary>
		private readonly ExtendedBlock block;

		/// <summary>Cache of Checksum#bytesPerChecksum.</summary>
		private readonly int bytesPerChecksum;

		/// <summary>Cache of Checksum#checksumSize.</summary>
		private readonly int checksumSize;

		/// <summary>Maximum number of chunks to allocate.</summary>
		/// <remarks>
		/// Maximum number of chunks to allocate.
		/// This is used to allocate dataBuf and checksumBuf, in the event that
		/// we need them.
		/// </remarks>
		private readonly int maxAllocatedChunks;

		/// <summary>True if zero readahead was requested.</summary>
		private readonly bool zeroReadaheadRequested;

		/// <summary>Maximum amount of readahead we'll do.</summary>
		/// <remarks>
		/// Maximum amount of readahead we'll do.  This will always be at least the,
		/// size of a single chunk, even if
		/// <see cref="zeroReadaheadRequested"/>
		/// is true.
		/// The reason is because we need to do a certain amount of buffering in order
		/// to do checksumming.
		/// This determines how many bytes we'll use out of dataBuf and checksumBuf.
		/// Why do we allocate buffers, and then (potentially) only use part of them?
		/// The rationale is that allocating a lot of buffers of different sizes would
		/// make it very difficult for the DirectBufferPool to re-use buffers.
		/// </remarks>
		private readonly int maxReadaheadLength;

		/// <summary>
		/// Buffers data starting at the current dataPos and extending on
		/// for dataBuf.limit().
		/// </summary>
		/// <remarks>
		/// Buffers data starting at the current dataPos and extending on
		/// for dataBuf.limit().
		/// This may be null if we don't need it.
		/// </remarks>
		private ByteBuffer dataBuf;

		/// <summary>
		/// Buffers checksums starting at the current checksumPos and extending on
		/// for checksumBuf.limit().
		/// </summary>
		/// <remarks>
		/// Buffers checksums starting at the current checksumPos and extending on
		/// for checksumBuf.limit().
		/// This may be null if we don't need it.
		/// </remarks>
		private ByteBuffer checksumBuf;

		/// <summary>StorageType of replica on DataNode.</summary>
		private StorageType storageType;

		private BlockReaderLocal(BlockReaderLocal.Builder builder)
		{
			this.replica = builder.replica;
			this.dataIn = replica.GetDataStream().GetChannel();
			this.dataPos = builder.dataPos;
			this.checksumIn = replica.GetMetaStream().GetChannel();
			BlockMetadataHeader header = builder.replica.GetMetaHeader();
			this.checksum = header.GetChecksum();
			this.verifyChecksum = builder.verifyChecksum && (this.checksum.GetChecksumType().
				id != DataChecksum.ChecksumNull);
			this.filename = builder.filename;
			this.block = builder.block;
			this.bytesPerChecksum = checksum.GetBytesPerChecksum();
			this.checksumSize = checksum.GetChecksumSize();
			this.maxAllocatedChunks = (bytesPerChecksum == 0) ? 0 : ((builder.bufferSize + bytesPerChecksum
				 - 1) / bytesPerChecksum);
			// Calculate the effective maximum readahead.
			// We can't do more readahead than there is space in the buffer.
			int maxReadaheadChunks = (bytesPerChecksum == 0) ? 0 : ((Math.Min(builder.bufferSize
				, builder.maxReadahead) + bytesPerChecksum - 1) / bytesPerChecksum);
			if (maxReadaheadChunks == 0)
			{
				this.zeroReadaheadRequested = true;
				maxReadaheadChunks = 1;
			}
			else
			{
				this.zeroReadaheadRequested = false;
			}
			this.maxReadaheadLength = maxReadaheadChunks * bytesPerChecksum;
			this.storageType = builder.storageType;
		}

		private void CreateDataBufIfNeeded()
		{
			lock (this)
			{
				if (dataBuf == null)
				{
					dataBuf = bufferPool.GetBuffer(maxAllocatedChunks * bytesPerChecksum);
					dataBuf.Position(0);
					dataBuf.Limit(0);
				}
			}
		}

		private void FreeDataBufIfExists()
		{
			lock (this)
			{
				if (dataBuf != null)
				{
					// When disposing of a dataBuf, we have to move our stored file index
					// backwards.
					dataPos -= dataBuf.Remaining();
					dataBuf.Clear();
					bufferPool.ReturnBuffer(dataBuf);
					dataBuf = null;
				}
			}
		}

		private void CreateChecksumBufIfNeeded()
		{
			lock (this)
			{
				if (checksumBuf == null)
				{
					checksumBuf = bufferPool.GetBuffer(maxAllocatedChunks * checksumSize);
					checksumBuf.Position(0);
					checksumBuf.Limit(0);
				}
			}
		}

		private void FreeChecksumBufIfExists()
		{
			lock (this)
			{
				if (checksumBuf != null)
				{
					checksumBuf.Clear();
					bufferPool.ReturnBuffer(checksumBuf);
					checksumBuf = null;
				}
			}
		}

		private int DrainDataBuf(ByteBuffer buf)
		{
			lock (this)
			{
				if (dataBuf == null)
				{
					return -1;
				}
				int oldLimit = dataBuf.Limit();
				int nRead = Math.Min(dataBuf.Remaining(), buf.Remaining());
				if (nRead == 0)
				{
					return (dataBuf.Remaining() == 0) ? -1 : 0;
				}
				try
				{
					dataBuf.Limit(dataBuf.Position() + nRead);
					buf.Put(dataBuf);
				}
				finally
				{
					dataBuf.Limit(oldLimit);
				}
				return nRead;
			}
		}

		/// <summary>Read from the block file into a buffer.</summary>
		/// <remarks>
		/// Read from the block file into a buffer.
		/// This function overwrites checksumBuf.  It will increment dataPos.
		/// </remarks>
		/// <param name="buf">
		/// The buffer to read into.  May be dataBuf.
		/// The position and limit of this buffer should be set to
		/// multiples of the checksum size.
		/// </param>
		/// <param name="canSkipChecksum">True if we can skip checksumming.</param>
		/// <returns>Total bytes read.  0 on EOF.</returns>
		/// <exception cref="System.IO.IOException"/>
		private int FillBuffer(ByteBuffer buf, bool canSkipChecksum)
		{
			lock (this)
			{
				TraceScope scope = Trace.StartSpan("BlockReaderLocal#fillBuffer(" + block.GetBlockId
					() + ")", Sampler.Never);
				try
				{
					int total = 0;
					long startDataPos = dataPos;
					int startBufPos = buf.Position();
					while (buf.HasRemaining())
					{
						int nRead = dataIn.Read(buf, dataPos);
						if (nRead < 0)
						{
							break;
						}
						dataPos += nRead;
						total += nRead;
					}
					if (canSkipChecksum)
					{
						FreeChecksumBufIfExists();
						return total;
					}
					if (total > 0)
					{
						try
						{
							buf.Limit(buf.Position());
							buf.Position(startBufPos);
							CreateChecksumBufIfNeeded();
							int checksumsNeeded = (total + bytesPerChecksum - 1) / bytesPerChecksum;
							checksumBuf.Clear();
							checksumBuf.Limit(checksumsNeeded * checksumSize);
							long checksumPos = BlockMetadataHeader.GetHeaderSize() + ((startDataPos / bytesPerChecksum
								) * checksumSize);
							while (checksumBuf.HasRemaining())
							{
								int nRead = checksumIn.Read(checksumBuf, checksumPos);
								if (nRead < 0)
								{
									throw new IOException("Got unexpected checksum file EOF at " + checksumPos + ", block file position "
										 + startDataPos + " for " + "block " + block + " of file " + filename);
								}
								checksumPos += nRead;
							}
							checksumBuf.Flip();
							checksum.VerifyChunkedSums(buf, checksumBuf, filename, startDataPos);
						}
						finally
						{
							buf.Position(buf.Limit());
						}
					}
					return total;
				}
				finally
				{
					scope.Close();
				}
			}
		}

		private bool CreateNoChecksumContext()
		{
			if (verifyChecksum)
			{
				if (storageType != null && storageType.IsTransient())
				{
					// Checksums are not stored for replicas on transient storage.  We do not
					// anchor, because we do not intend for client activity to block eviction
					// from transient storage on the DataNode side.
					return true;
				}
				else
				{
					return replica.AddNoChecksumAnchor();
				}
			}
			else
			{
				return true;
			}
		}

		private void ReleaseNoChecksumContext()
		{
			if (verifyChecksum)
			{
				if (storageType == null || !storageType.IsTransient())
				{
					replica.RemoveNoChecksumAnchor();
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual int Read(ByteBuffer buf)
		{
			lock (this)
			{
				bool canSkipChecksum = CreateNoChecksumContext();
				try
				{
					string traceString = null;
					if (Log.IsTraceEnabled())
					{
						traceString = new StringBuilder().Append("read(").Append("buf.remaining=").Append
							(buf.Remaining()).Append(", block=").Append(block).Append(", filename=").Append(
							filename).Append(", canSkipChecksum=").Append(canSkipChecksum).Append(")").ToString
							();
						Log.Info(traceString + ": starting");
					}
					int nRead;
					try
					{
						if (canSkipChecksum && zeroReadaheadRequested)
						{
							nRead = ReadWithoutBounceBuffer(buf);
						}
						else
						{
							nRead = ReadWithBounceBuffer(buf, canSkipChecksum);
						}
					}
					catch (IOException e)
					{
						if (Log.IsTraceEnabled())
						{
							Log.Info(traceString + ": I/O error", e);
						}
						throw;
					}
					if (Log.IsTraceEnabled())
					{
						Log.Info(traceString + ": returning " + nRead);
					}
					return nRead;
				}
				finally
				{
					if (canSkipChecksum)
					{
						ReleaseNoChecksumContext();
					}
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private int ReadWithoutBounceBuffer(ByteBuffer buf)
		{
			lock (this)
			{
				FreeDataBufIfExists();
				FreeChecksumBufIfExists();
				int total = 0;
				while (buf.HasRemaining())
				{
					int nRead = dataIn.Read(buf, dataPos);
					if (nRead <= 0)
					{
						break;
					}
					dataPos += nRead;
					total += nRead;
				}
				return (total == 0 && (dataPos == dataIn.Size())) ? -1 : total;
			}
		}

		/// <summary>Fill the data buffer.</summary>
		/// <remarks>
		/// Fill the data buffer.  If necessary, validate the data against the
		/// checksums.
		/// We always want the offsets of the data contained in dataBuf to be
		/// aligned to the chunk boundary.  If we are validating checksums, we
		/// accomplish this by seeking backwards in the file until we're on a
		/// chunk boundary.  (This is necessary because we can't checksum a
		/// partial chunk.)  If we are not validating checksums, we simply only
		/// fill the latter part of dataBuf.
		/// </remarks>
		/// <param name="canSkipChecksum">true if we can skip checksumming.</param>
		/// <returns>true if we hit EOF.</returns>
		/// <exception cref="System.IO.IOException"/>
		private bool FillDataBuf(bool canSkipChecksum)
		{
			lock (this)
			{
				CreateDataBufIfNeeded();
				int slop = (int)(dataPos % bytesPerChecksum);
				long oldDataPos = dataPos;
				dataBuf.Limit(maxReadaheadLength);
				if (canSkipChecksum)
				{
					dataBuf.Position(slop);
					FillBuffer(dataBuf, canSkipChecksum);
				}
				else
				{
					dataPos -= slop;
					dataBuf.Position(0);
					FillBuffer(dataBuf, canSkipChecksum);
				}
				dataBuf.Limit(dataBuf.Position());
				dataBuf.Position(Math.Min(dataBuf.Position(), slop));
				if (Log.IsTraceEnabled())
				{
					Log.Trace("loaded " + dataBuf.Remaining() + " bytes into bounce " + "buffer from offset "
						 + oldDataPos + " of " + block);
				}
				return dataBuf.Limit() != maxReadaheadLength;
			}
		}

		/// <summary>Read using the bounce buffer.</summary>
		/// <remarks>
		/// Read using the bounce buffer.
		/// A 'direct' read actually has three phases. The first drains any
		/// remaining bytes from the slow read buffer. After this the read is
		/// guaranteed to be on a checksum chunk boundary. If there are still bytes
		/// to read, the fast direct path is used for as many remaining bytes as
		/// possible, up to a multiple of the checksum chunk size. Finally, any
		/// 'odd' bytes remaining at the end of the read cause another slow read to
		/// be issued, which involves an extra copy.
		/// Every 'slow' read tries to fill the slow read buffer in one go for
		/// efficiency's sake. As described above, all non-checksum-chunk-aligned
		/// reads will be served from the slower read path.
		/// </remarks>
		/// <param name="buf">The buffer to read into.</param>
		/// <param name="canSkipChecksum">True if we can skip checksums.</param>
		/// <exception cref="System.IO.IOException"/>
		private int ReadWithBounceBuffer(ByteBuffer buf, bool canSkipChecksum)
		{
			lock (this)
			{
				int total = 0;
				int bb = DrainDataBuf(buf);
				// drain bounce buffer if possible
				if (bb >= 0)
				{
					total += bb;
					if (buf.Remaining() == 0)
					{
						return total;
					}
				}
				bool eof = true;
				bool done = false;
				do
				{
					if (buf.IsDirect() && (buf.Remaining() >= maxReadaheadLength) && ((dataPos % bytesPerChecksum
						) == 0))
					{
						// Fast lane: try to read directly into user-supplied buffer, bypassing
						// bounce buffer.
						int oldLimit = buf.Limit();
						int nRead;
						try
						{
							buf.Limit(buf.Position() + maxReadaheadLength);
							nRead = FillBuffer(buf, canSkipChecksum);
						}
						finally
						{
							buf.Limit(oldLimit);
						}
						if (nRead < maxReadaheadLength)
						{
							done = true;
						}
						if (nRead > 0)
						{
							eof = false;
						}
						total += nRead;
					}
					else
					{
						// Slow lane: refill bounce buffer.
						if (FillDataBuf(canSkipChecksum))
						{
							done = true;
						}
						bb = DrainDataBuf(buf);
						// drain bounce buffer if possible
						if (bb >= 0)
						{
							eof = false;
							total += bb;
						}
					}
				}
				while ((!done) && (buf.Remaining() > 0));
				return (eof && total == 0) ? -1 : total;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual int Read(byte[] arr, int off, int len)
		{
			lock (this)
			{
				bool canSkipChecksum = CreateNoChecksumContext();
				int nRead;
				try
				{
					string traceString = null;
					if (Log.IsTraceEnabled())
					{
						traceString = new StringBuilder().Append("read(arr.length=").Append(arr.Length).Append
							(", off=").Append(off).Append(", len=").Append(len).Append(", filename=").Append
							(filename).Append(", block=").Append(block).Append(", canSkipChecksum=").Append(
							canSkipChecksum).Append(")").ToString();
						Log.Trace(traceString + ": starting");
					}
					try
					{
						if (canSkipChecksum && zeroReadaheadRequested)
						{
							nRead = ReadWithoutBounceBuffer(arr, off, len);
						}
						else
						{
							nRead = ReadWithBounceBuffer(arr, off, len, canSkipChecksum);
						}
					}
					catch (IOException e)
					{
						if (Log.IsTraceEnabled())
						{
							Log.Trace(traceString + ": I/O error", e);
						}
						throw;
					}
					if (Log.IsTraceEnabled())
					{
						Log.Trace(traceString + ": returning " + nRead);
					}
				}
				finally
				{
					if (canSkipChecksum)
					{
						ReleaseNoChecksumContext();
					}
				}
				return nRead;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private int ReadWithoutBounceBuffer(byte[] arr, int off, int len)
		{
			lock (this)
			{
				FreeDataBufIfExists();
				FreeChecksumBufIfExists();
				int nRead = dataIn.Read(ByteBuffer.Wrap(arr, off, len), dataPos);
				if (nRead > 0)
				{
					dataPos += nRead;
				}
				else
				{
					if ((nRead == 0) && (dataPos == dataIn.Size()))
					{
						return -1;
					}
				}
				return nRead;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private int ReadWithBounceBuffer(byte[] arr, int off, int len, bool canSkipChecksum
			)
		{
			lock (this)
			{
				CreateDataBufIfNeeded();
				if (!dataBuf.HasRemaining())
				{
					dataBuf.Position(0);
					dataBuf.Limit(maxReadaheadLength);
					FillDataBuf(canSkipChecksum);
				}
				if (dataBuf.Remaining() == 0)
				{
					return -1;
				}
				int toRead = Math.Min(dataBuf.Remaining(), len);
				dataBuf.Get(arr, off, toRead);
				return toRead;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual long Skip(long n)
		{
			lock (this)
			{
				int discardedFromBuf = 0;
				long remaining = n;
				if ((dataBuf != null) && dataBuf.HasRemaining())
				{
					discardedFromBuf = (int)Math.Min(dataBuf.Remaining(), n);
					dataBuf.Position(dataBuf.Position() + discardedFromBuf);
					remaining -= discardedFromBuf;
				}
				if (Log.IsTraceEnabled())
				{
					Log.Trace("skip(n=" + n + ", block=" + block + ", filename=" + filename + "): discarded "
						 + discardedFromBuf + " bytes from " + "dataBuf and advanced dataPos by " + remaining
						);
				}
				dataPos += remaining;
				return n;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual int Available()
		{
			// We never do network I/O in BlockReaderLocal.
			return int.MaxValue;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Close()
		{
			lock (this)
			{
				if (closed)
				{
					return;
				}
				closed = true;
				if (Log.IsTraceEnabled())
				{
					Log.Trace("close(filename=" + filename + ", block=" + block + ")");
				}
				replica.Unref();
				FreeDataBufIfExists();
				FreeChecksumBufIfExists();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadFully(byte[] arr, int off, int len)
		{
			lock (this)
			{
				BlockReaderUtil.ReadFully(this, arr, off, len);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual int ReadAll(byte[] buf, int off, int len)
		{
			lock (this)
			{
				return BlockReaderUtil.ReadAll(this, buf, off, len);
			}
		}

		public virtual bool IsLocal()
		{
			return true;
		}

		public virtual bool IsShortCircuit()
		{
			return true;
		}

		/// <summary>Get or create a memory map for this replica.</summary>
		/// <remarks>
		/// Get or create a memory map for this replica.
		/// There are two kinds of ClientMmap objects we could fetch here: one that
		/// will always read pre-checksummed data, and one that may read data that
		/// hasn't been checksummed.
		/// If we fetch the former, "safe" kind of ClientMmap, we have to increment
		/// the anchor count on the shared memory slot.  This will tell the DataNode
		/// not to munlock the block until this ClientMmap is closed.
		/// If we fetch the latter, we don't bother with anchoring.
		/// </remarks>
		/// <param name="opts">The options to use, such as SKIP_CHECKSUMS.</param>
		/// <returns>null on failure; the ClientMmap otherwise.</returns>
		public virtual ClientMmap GetClientMmap(EnumSet<ReadOption> opts)
		{
			bool anchor = verifyChecksum && (opts.Contains(ReadOption.SkipChecksums) == false
				);
			if (anchor)
			{
				if (!CreateNoChecksumContext())
				{
					if (Log.IsTraceEnabled())
					{
						Log.Trace("can't get an mmap for " + block + " of " + filename + " since SKIP_CHECKSUMS was not given, "
							 + "we aren't skipping checksums, and the block is not mlocked.");
					}
					return null;
				}
			}
			ClientMmap clientMmap = null;
			try
			{
				clientMmap = replica.GetOrCreateClientMmap(anchor);
			}
			finally
			{
				if ((clientMmap == null) && anchor)
				{
					ReleaseNoChecksumContext();
				}
			}
			return clientMmap;
		}

		[VisibleForTesting]
		internal virtual bool GetVerifyChecksum()
		{
			return this.verifyChecksum;
		}

		[VisibleForTesting]
		internal virtual int GetMaxReadaheadLength()
		{
			return this.maxReadaheadLength;
		}

		/// <summary>Make the replica anchorable.</summary>
		/// <remarks>
		/// Make the replica anchorable.  Normally this can only be done by the
		/// DataNode.  This method is only for testing.
		/// </remarks>
		[VisibleForTesting]
		internal virtual void ForceAnchorable()
		{
			replica.GetSlot().MakeAnchorable();
		}

		/// <summary>Make the replica unanchorable.</summary>
		/// <remarks>
		/// Make the replica unanchorable.  Normally this can only be done by the
		/// DataNode.  This method is only for testing.
		/// </remarks>
		[VisibleForTesting]
		internal virtual void ForceUnanchorable()
		{
			replica.GetSlot().MakeUnanchorable();
		}
	}
}
