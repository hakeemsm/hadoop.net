using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Protocol.Datatransfer;
using Org.Apache.Hadoop.Hdfs.Protocol.Proto;
using Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.Hdfs.Util;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.IO.Nativeio;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode
{
	/// <summary>
	/// A class that receives a block and writes to its own disk, meanwhile
	/// may copies it to another site.
	/// </summary>
	/// <remarks>
	/// A class that receives a block and writes to its own disk, meanwhile
	/// may copies it to another site. If a throttler is provided,
	/// streaming throttling is also supported.
	/// </remarks>
	internal class BlockReceiver : IDisposable
	{
		public static readonly Log Log = DataNode.Log;

		internal static readonly Log ClientTraceLog = DataNode.ClientTraceLog;

		[VisibleForTesting]
		internal static long CacheDropLagBytes = 8 * 1024 * 1024;

		private readonly long datanodeSlowLogThresholdMs;

		private DataInputStream @in = null;

		private DataChecksum clientChecksum;

		private DataChecksum diskChecksum;

		/// <summary>
		/// In the case that the client is writing with a different
		/// checksum polynomial than the block is stored with on disk,
		/// the DataNode needs to recalculate checksums before writing.
		/// </summary>
		private readonly bool needsChecksumTranslation;

		private OutputStream @out = null;

		private FileDescriptor outFd;

		private DataOutputStream checksumOut = null;

		private readonly int bytesPerChecksum;

		private readonly int checksumSize;

		private readonly PacketReceiver packetReceiver = new PacketReceiver(false);

		protected internal readonly string inAddr;

		protected internal readonly string myAddr;

		private string mirrorAddr;

		private DataOutputStream mirrorOut;

		private Daemon responder = null;

		private DataTransferThrottler throttler;

		private ReplicaOutputStreams streams;

		private DatanodeInfo srcDataNode = null;

		private readonly DataNode datanode;

		private volatile bool mirrorError;

		private bool dropCacheBehindWrites;

		private long lastCacheManagementOffset = 0;

		private bool syncBehindWrites;

		private bool syncBehindWritesInBackground;

		/// <summary>The client name.</summary>
		/// <remarks>The client name.  It is empty if a datanode is the client</remarks>
		private readonly string clientname;

		private readonly bool isClient;

		private readonly bool isDatanode;

		/// <summary>the block to receive</summary>
		private readonly ExtendedBlock block;

		/// <summary>the replica to write</summary>
		private readonly ReplicaInPipelineInterface replicaInfo;

		/// <summary>pipeline stage</summary>
		private readonly BlockConstructionStage stage;

		private readonly bool isTransfer;

		private bool syncOnClose;

		private long restartBudget;

		/// <summary>the reference of the volume where the block receiver writes to</summary>
		private ReplicaHandler replicaHandler;

		/// <summary>for replaceBlock response</summary>
		private readonly long responseInterval;

		private long lastResponseTime = 0;

		private bool isReplaceBlock = false;

		private DataOutputStream replyOut = null;

		private bool pinning;

		private long lastSentTime;

		private long maxSendIdleTime;

		/// <exception cref="System.IO.IOException"/>
		internal BlockReceiver(ExtendedBlock block, StorageType storageType, DataInputStream
			 @in, string inAddr, string myAddr, BlockConstructionStage stage, long newGs, long
			 minBytesRcvd, long maxBytesRcvd, string clientname, DatanodeInfo srcDataNode, DataNode
			 datanode, DataChecksum requestedChecksum, CachingStrategy cachingStrategy, bool
			 allowLazyPersist, bool pinning)
		{
			// from where data are read
			// checksum used by client
			// checksum we write to disk
			// to block file at local disk
			// to crc file at local disk
			// Cache management state
			try
			{
				this.block = block;
				this.@in = @in;
				this.inAddr = inAddr;
				this.myAddr = myAddr;
				this.srcDataNode = srcDataNode;
				this.datanode = datanode;
				this.clientname = clientname;
				this.isDatanode = clientname.Length == 0;
				this.isClient = !this.isDatanode;
				this.restartBudget = datanode.GetDnConf().restartReplicaExpiry;
				this.datanodeSlowLogThresholdMs = datanode.GetDnConf().datanodeSlowIoWarningThresholdMs;
				// For replaceBlock() calls response should be sent to avoid socketTimeout
				// at clients. So sending with the interval of 0.5 * socketTimeout
				long readTimeout = datanode.GetDnConf().socketTimeout;
				this.responseInterval = (long)(readTimeout * 0.5);
				//for datanode, we have
				//1: clientName.length() == 0, and
				//2: stage == null or PIPELINE_SETUP_CREATE
				this.stage = stage;
				this.isTransfer = stage == BlockConstructionStage.TransferRbw || stage == BlockConstructionStage
					.TransferFinalized;
				this.pinning = pinning;
				this.lastSentTime = Time.MonotonicNow();
				// Downstream will timeout in readTimeout on receiving the next packet.
				// If there is no data traffic, a heartbeat packet is sent at
				// the interval of 0.5*readTimeout. Here, we set 0.9*readTimeout to be
				// the threshold for detecting congestion.
				this.maxSendIdleTime = (long)(readTimeout * 0.9);
				if (Log.IsDebugEnabled())
				{
					Log.Debug(GetType().Name + ": " + block + "\n  isClient  =" + isClient + ", clientname="
						 + clientname + "\n  isDatanode=" + isDatanode + ", srcDataNode=" + srcDataNode 
						+ "\n  inAddr=" + inAddr + ", myAddr=" + myAddr + "\n  cachingStrategy = " + cachingStrategy
						 + "\n  pinning=" + pinning);
				}
				//
				// Open local disk out
				//
				if (isDatanode)
				{
					//replication or move
					replicaHandler = datanode.data.CreateTemporary(storageType, block);
				}
				else
				{
					switch (stage)
					{
						case BlockConstructionStage.PipelineSetupCreate:
						{
							replicaHandler = datanode.data.CreateRbw(storageType, block, allowLazyPersist);
							datanode.NotifyNamenodeReceivingBlock(block, replicaHandler.GetReplica().GetStorageUuid
								());
							break;
						}

						case BlockConstructionStage.PipelineSetupStreamingRecovery:
						{
							replicaHandler = datanode.data.RecoverRbw(block, newGs, minBytesRcvd, maxBytesRcvd
								);
							block.SetGenerationStamp(newGs);
							break;
						}

						case BlockConstructionStage.PipelineSetupAppend:
						{
							replicaHandler = datanode.data.Append(block, newGs, minBytesRcvd);
							block.SetGenerationStamp(newGs);
							datanode.NotifyNamenodeReceivingBlock(block, replicaHandler.GetReplica().GetStorageUuid
								());
							break;
						}

						case BlockConstructionStage.PipelineSetupAppendRecovery:
						{
							replicaHandler = datanode.data.RecoverAppend(block, newGs, minBytesRcvd);
							block.SetGenerationStamp(newGs);
							datanode.NotifyNamenodeReceivingBlock(block, replicaHandler.GetReplica().GetStorageUuid
								());
							break;
						}

						case BlockConstructionStage.TransferRbw:
						case BlockConstructionStage.TransferFinalized:
						{
							// this is a transfer destination
							replicaHandler = datanode.data.CreateTemporary(storageType, block);
							break;
						}

						default:
						{
							throw new IOException("Unsupported stage " + stage + " while receiving block " + 
								block + " from " + inAddr);
						}
					}
				}
				replicaInfo = replicaHandler.GetReplica();
				this.dropCacheBehindWrites = (cachingStrategy.GetDropBehind() == null) ? datanode
					.GetDnConf().dropCacheBehindWrites : cachingStrategy.GetDropBehind();
				this.syncBehindWrites = datanode.GetDnConf().syncBehindWrites;
				this.syncBehindWritesInBackground = datanode.GetDnConf().syncBehindWritesInBackground;
				bool isCreate = isDatanode || isTransfer || stage == BlockConstructionStage.PipelineSetupCreate;
				streams = replicaInfo.CreateStreams(isCreate, requestedChecksum);
				System.Diagnostics.Debug.Assert(streams != null, "null streams!");
				// read checksum meta information
				this.clientChecksum = requestedChecksum;
				this.diskChecksum = streams.GetChecksum();
				this.needsChecksumTranslation = !clientChecksum.Equals(diskChecksum);
				this.bytesPerChecksum = diskChecksum.GetBytesPerChecksum();
				this.checksumSize = diskChecksum.GetChecksumSize();
				this.@out = streams.GetDataOut();
				if (@out is FileOutputStream)
				{
					this.outFd = ((FileOutputStream)@out).GetFD();
				}
				else
				{
					Log.Warn("Could not get file descriptor for outputstream of class " + @out.GetType
						());
				}
				this.checksumOut = new DataOutputStream(new BufferedOutputStream(streams.GetChecksumOut
					(), HdfsConstants.SmallBufferSize));
				// write data chunk header if creating a new replica
				if (isCreate)
				{
					BlockMetadataHeader.WriteHeader(checksumOut, diskChecksum);
				}
			}
			catch (ReplicaAlreadyExistsException bae)
			{
				throw;
			}
			catch (ReplicaNotFoundException bne)
			{
				throw;
			}
			catch (IOException ioe)
			{
				IOUtils.CloseStream(this);
				CleanupBlock();
				// check if there is a disk error
				IOException cause = DatanodeUtil.GetCauseIfDiskError(ioe);
				DataNode.Log.Warn("IOException in BlockReceiver constructor. Cause is ", cause);
				if (cause != null)
				{
					// possible disk error
					ioe = cause;
					datanode.CheckDiskErrorAsync();
				}
				throw;
			}
		}

		/// <summary>Return the datanode object.</summary>
		internal virtual DataNode GetDataNode()
		{
			return datanode;
		}

		internal virtual string GetStorageUuid()
		{
			return replicaInfo.GetStorageUuid();
		}

		/// <summary>close files and release volume reference.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void Close()
		{
			packetReceiver.Close();
			IOException ioe = null;
			if (syncOnClose && (@out != null || checksumOut != null))
			{
				datanode.metrics.IncrFsyncCount();
			}
			long flushTotalNanos = 0;
			bool measuredFlushTime = false;
			// close checksum file
			try
			{
				if (checksumOut != null)
				{
					long flushStartNanos = Runtime.NanoTime();
					checksumOut.Flush();
					long flushEndNanos = Runtime.NanoTime();
					if (syncOnClose)
					{
						long fsyncStartNanos = flushEndNanos;
						streams.SyncChecksumOut();
						datanode.metrics.AddFsyncNanos(Runtime.NanoTime() - fsyncStartNanos);
					}
					flushTotalNanos += flushEndNanos - flushStartNanos;
					measuredFlushTime = true;
					checksumOut.Close();
					checksumOut = null;
				}
			}
			catch (IOException e)
			{
				ioe = e;
			}
			finally
			{
				IOUtils.CloseStream(checksumOut);
			}
			// close block file
			try
			{
				if (@out != null)
				{
					long flushStartNanos = Runtime.NanoTime();
					@out.Flush();
					long flushEndNanos = Runtime.NanoTime();
					if (syncOnClose)
					{
						long fsyncStartNanos = flushEndNanos;
						streams.SyncDataOut();
						datanode.metrics.AddFsyncNanos(Runtime.NanoTime() - fsyncStartNanos);
					}
					flushTotalNanos += flushEndNanos - flushStartNanos;
					measuredFlushTime = true;
					@out.Close();
					@out = null;
				}
			}
			catch (IOException e)
			{
				ioe = e;
			}
			finally
			{
				IOUtils.CloseStream(@out);
			}
			if (replicaHandler != null)
			{
				IOUtils.Cleanup(null, replicaHandler);
				replicaHandler = null;
			}
			if (measuredFlushTime)
			{
				datanode.metrics.AddFlushNanos(flushTotalNanos);
			}
			// disk check
			if (ioe != null)
			{
				datanode.CheckDiskErrorAsync();
				throw ioe;
			}
		}

		internal virtual void SetLastSentTime(long sentTime)
		{
			lock (this)
			{
				lastSentTime = sentTime;
			}
		}

		/// <summary>
		/// It can return false if
		/// - upstream did not send packet for a long time
		/// - a packet was received but got stuck in local disk I/O.
		/// </summary>
		/// <remarks>
		/// It can return false if
		/// - upstream did not send packet for a long time
		/// - a packet was received but got stuck in local disk I/O.
		/// - a packet was received but got stuck on send to mirror.
		/// </remarks>
		internal virtual bool PacketSentInTime()
		{
			lock (this)
			{
				long diff = Time.MonotonicNow() - lastSentTime;
				if (diff > maxSendIdleTime)
				{
					Log.Info("A packet was last sent " + diff + " milliseconds ago.");
					return false;
				}
				return true;
			}
		}

		/// <summary>Flush block data and metadata files to disk.</summary>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void FlushOrSync(bool isSync)
		{
			long flushTotalNanos = 0;
			long begin = Time.MonotonicNow();
			if (checksumOut != null)
			{
				long flushStartNanos = Runtime.NanoTime();
				checksumOut.Flush();
				long flushEndNanos = Runtime.NanoTime();
				if (isSync)
				{
					long fsyncStartNanos = flushEndNanos;
					streams.SyncChecksumOut();
					datanode.metrics.AddFsyncNanos(Runtime.NanoTime() - fsyncStartNanos);
				}
				flushTotalNanos += flushEndNanos - flushStartNanos;
			}
			if (@out != null)
			{
				long flushStartNanos = Runtime.NanoTime();
				@out.Flush();
				long flushEndNanos = Runtime.NanoTime();
				if (isSync)
				{
					long fsyncStartNanos = flushEndNanos;
					streams.SyncDataOut();
					datanode.metrics.AddFsyncNanos(Runtime.NanoTime() - fsyncStartNanos);
				}
				flushTotalNanos += flushEndNanos - flushStartNanos;
			}
			if (checksumOut != null || @out != null)
			{
				datanode.metrics.AddFlushNanos(flushTotalNanos);
				if (isSync)
				{
					datanode.metrics.IncrFsyncCount();
				}
			}
			long duration = Time.MonotonicNow() - begin;
			if (duration > datanodeSlowLogThresholdMs)
			{
				Log.Warn("Slow flushOrSync took " + duration + "ms (threshold=" + datanodeSlowLogThresholdMs
					 + "ms), isSync:" + isSync + ", flushTotalNanos=" + flushTotalNanos + "ns");
			}
		}

		/// <summary>
		/// While writing to mirrorOut, failure to write to mirror should not
		/// affect this datanode unless it is caused by interruption.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		private void HandleMirrorOutError(IOException ioe)
		{
			string bpid = block.GetBlockPoolId();
			Log.Info(datanode.GetDNRegistrationForBP(bpid) + ":Exception writing " + block + 
				" to mirror " + mirrorAddr, ioe);
			if (Sharpen.Thread.Interrupted())
			{
				// shut down if the thread is interrupted
				throw ioe;
			}
			else
			{
				// encounter an error while writing to mirror
				// continue to run even if can not write to mirror
				// notify client of the error
				// and wait for the client to shut down the pipeline
				mirrorError = true;
			}
		}

		/// <summary>Verify multiple CRC chunks.</summary>
		/// <exception cref="System.IO.IOException"/>
		private void VerifyChunks(ByteBuffer dataBuf, ByteBuffer checksumBuf)
		{
			try
			{
				clientChecksum.VerifyChunkedSums(dataBuf, checksumBuf, clientname, 0);
			}
			catch (ChecksumException ce)
			{
				Log.Warn("Checksum error in block " + block + " from " + inAddr, ce);
				// No need to report to namenode when client is writing.
				if (srcDataNode != null && isDatanode)
				{
					try
					{
						Log.Info("report corrupt " + block + " from datanode " + srcDataNode + " to namenode"
							);
						datanode.ReportRemoteBadBlock(srcDataNode, block);
					}
					catch (IOException)
					{
						Log.Warn("Failed to report bad " + block + " from datanode " + srcDataNode + " to namenode"
							);
					}
				}
				throw new IOException("Unexpected checksum mismatch while writing " + block + " from "
					 + inAddr);
			}
		}

		/// <summary>
		/// Translate CRC chunks from the client's checksum implementation
		/// to the disk checksum implementation.
		/// </summary>
		/// <remarks>
		/// Translate CRC chunks from the client's checksum implementation
		/// to the disk checksum implementation.
		/// This does not verify the original checksums, under the assumption
		/// that they have already been validated.
		/// </remarks>
		private void TranslateChunks(ByteBuffer dataBuf, ByteBuffer checksumBuf)
		{
			diskChecksum.CalculateChunkedSums(dataBuf, checksumBuf);
		}

		/// <summary>Check whether checksum needs to be verified.</summary>
		/// <remarks>
		/// Check whether checksum needs to be verified.
		/// Skip verifying checksum iff this is not the last one in the
		/// pipeline and clientName is non-null. i.e. Checksum is verified
		/// on all the datanodes when the data is being written by a
		/// datanode rather than a client. Whe client is writing the data,
		/// protocol includes acks and only the last datanode needs to verify
		/// checksum.
		/// </remarks>
		/// <returns>true if checksum verification is needed, otherwise false.</returns>
		private bool ShouldVerifyChecksum()
		{
			return (mirrorOut == null || isDatanode || needsChecksumTranslation);
		}

		/// <summary>Receives and processes a packet.</summary>
		/// <remarks>
		/// Receives and processes a packet. It can contain many chunks.
		/// returns the number of data bytes that the packet has.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		private int ReceivePacket()
		{
			// read the next packet
			packetReceiver.ReceiveNextPacket(@in);
			PacketHeader header = packetReceiver.GetHeader();
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Receiving one packet for block " + block + ": " + header);
			}
			// Sanity check the header
			if (header.GetOffsetInBlock() > replicaInfo.GetNumBytes())
			{
				throw new IOException("Received an out-of-sequence packet for " + block + "from "
					 + inAddr + " at offset " + header.GetOffsetInBlock() + ". Expecting packet starting at "
					 + replicaInfo.GetNumBytes());
			}
			if (header.GetDataLen() < 0)
			{
				throw new IOException("Got wrong length during writeBlock(" + block + ") from " +
					 inAddr + " at offset " + header.GetOffsetInBlock() + ": " + header.GetDataLen()
					);
			}
			long offsetInBlock = header.GetOffsetInBlock();
			long seqno = header.GetSeqno();
			bool lastPacketInBlock = header.IsLastPacketInBlock();
			int len = header.GetDataLen();
			bool syncBlock = header.GetSyncBlock();
			// avoid double sync'ing on close
			if (syncBlock && lastPacketInBlock)
			{
				this.syncOnClose = false;
			}
			// update received bytes
			long firstByteInBlock = offsetInBlock;
			offsetInBlock += len;
			if (replicaInfo.GetNumBytes() < offsetInBlock)
			{
				replicaInfo.SetNumBytes(offsetInBlock);
			}
			// put in queue for pending acks, unless sync was requested
			if (responder != null && !syncBlock && !ShouldVerifyChecksum())
			{
				((BlockReceiver.PacketResponder)responder.GetRunnable()).Enqueue(seqno, lastPacketInBlock
					, offsetInBlock, DataTransferProtos.Status.Success);
			}
			// Drop heartbeat for testing.
			if (seqno < 0 && len == 0 && DataNodeFaultInjector.Get().DropHeartbeatPacket())
			{
				return 0;
			}
			//First write the packet to the mirror:
			if (mirrorOut != null && !mirrorError)
			{
				try
				{
					long begin = Time.MonotonicNow();
					packetReceiver.MirrorPacketTo(mirrorOut);
					mirrorOut.Flush();
					long now = Time.MonotonicNow();
					SetLastSentTime(now);
					long duration = now - begin;
					if (duration > datanodeSlowLogThresholdMs)
					{
						Log.Warn("Slow BlockReceiver write packet to mirror took " + duration + "ms (threshold="
							 + datanodeSlowLogThresholdMs + "ms)");
					}
				}
				catch (IOException e)
				{
					HandleMirrorOutError(e);
				}
			}
			ByteBuffer dataBuf = packetReceiver.GetDataSlice();
			ByteBuffer checksumBuf = packetReceiver.GetChecksumSlice();
			if (lastPacketInBlock || len == 0)
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Receiving an empty packet or the end of the block " + block);
				}
				// sync block if requested
				if (syncBlock)
				{
					FlushOrSync(true);
				}
			}
			else
			{
				int checksumLen = diskChecksum.GetChecksumSize(len);
				int checksumReceivedLen = checksumBuf.Capacity();
				if (checksumReceivedLen > 0 && checksumReceivedLen != checksumLen)
				{
					throw new IOException("Invalid checksum length: received length is " + checksumReceivedLen
						 + " but expected length is " + checksumLen);
				}
				if (checksumReceivedLen > 0 && ShouldVerifyChecksum())
				{
					try
					{
						VerifyChunks(dataBuf, checksumBuf);
					}
					catch (IOException ioe)
					{
						// checksum error detected locally. there is no reason to continue.
						if (responder != null)
						{
							try
							{
								((BlockReceiver.PacketResponder)responder.GetRunnable()).Enqueue(seqno, lastPacketInBlock
									, offsetInBlock, DataTransferProtos.Status.ErrorChecksum);
								// Wait until the responder sends back the response
								// and interrupt this thread.
								Sharpen.Thread.Sleep(3000);
							}
							catch (Exception)
							{
							}
						}
						throw new IOException("Terminating due to a checksum error." + ioe);
					}
					if (needsChecksumTranslation)
					{
						// overwrite the checksums in the packet buffer with the
						// appropriate polynomial for the disk storage.
						TranslateChunks(dataBuf, checksumBuf);
					}
				}
				if (checksumReceivedLen == 0 && !streams.IsTransientStorage())
				{
					// checksum is missing, need to calculate it
					checksumBuf = ByteBuffer.Allocate(checksumLen);
					diskChecksum.CalculateChunkedSums(dataBuf, checksumBuf);
				}
				// by this point, the data in the buffer uses the disk checksum
				bool shouldNotWriteChecksum = checksumReceivedLen == 0 && streams.IsTransientStorage
					();
				try
				{
					long onDiskLen = replicaInfo.GetBytesOnDisk();
					if (onDiskLen < offsetInBlock)
					{
						// Normally the beginning of an incoming packet is aligned with the
						// existing data on disk. If the beginning packet data offset is not
						// checksum chunk aligned, the end of packet will not go beyond the
						// next chunk boundary.
						// When a failure-recovery is involved, the client state and the
						// the datanode state may not exactly agree. I.e. the client may
						// resend part of data that is already on disk. Correct number of
						// bytes should be skipped when writing the data and checksum
						// buffers out to disk.
						long partialChunkSizeOnDisk = onDiskLen % bytesPerChecksum;
						long lastChunkBoundary = onDiskLen - partialChunkSizeOnDisk;
						bool alignedOnDisk = partialChunkSizeOnDisk == 0;
						bool alignedInPacket = firstByteInBlock % bytesPerChecksum == 0;
						// If the end of the on-disk data is not chunk-aligned, the last
						// checksum needs to be overwritten.
						bool overwriteLastCrc = !alignedOnDisk && !shouldNotWriteChecksum;
						// If the starting offset of the packat data is at the last chunk
						// boundary of the data on disk, the partial checksum recalculation
						// can be skipped and the checksum supplied by the client can be used
						// instead. This reduces disk reads and cpu load.
						bool doCrcRecalc = overwriteLastCrc && (lastChunkBoundary != firstByteInBlock);
						// If this is a partial chunk, then verify that this is the only
						// chunk in the packet. If the starting offset is not chunk
						// aligned, the packet should terminate at or before the next
						// chunk boundary.
						if (!alignedInPacket && len > bytesPerChecksum)
						{
							throw new IOException("Unexpected packet data length for " + block + " from " + inAddr
								 + ": a partial chunk must be " + " sent in an individual packet (data length = "
								 + len + " > bytesPerChecksum = " + bytesPerChecksum + ")");
						}
						// If the last portion of the block file is not a full chunk,
						// then read in pre-existing partial data chunk and recalculate
						// the checksum so that the checksum calculation can continue
						// from the right state. If the client provided the checksum for
						// the whole chunk, this is not necessary.
						Checksum partialCrc = null;
						if (doCrcRecalc)
						{
							if (Log.IsDebugEnabled())
							{
								Log.Debug("receivePacket for " + block + ": previous write did not end at the chunk boundary."
									 + " onDiskLen=" + onDiskLen);
							}
							long offsetInChecksum = BlockMetadataHeader.GetHeaderSize() + onDiskLen / bytesPerChecksum
								 * checksumSize;
							partialCrc = ComputePartialChunkCrc(onDiskLen, offsetInChecksum);
						}
						// The data buffer position where write will begin. If the packet
						// data and on-disk data have no overlap, this will not be at the
						// beginning of the buffer.
						int startByteToDisk = (int)(onDiskLen - firstByteInBlock) + dataBuf.ArrayOffset()
							 + dataBuf.Position();
						// Actual number of data bytes to write.
						int numBytesToDisk = (int)(offsetInBlock - onDiskLen);
						// Write data to disk.
						long begin = Time.MonotonicNow();
						@out.Write(((byte[])dataBuf.Array()), startByteToDisk, numBytesToDisk);
						long duration = Time.MonotonicNow() - begin;
						if (duration > datanodeSlowLogThresholdMs)
						{
							Log.Warn("Slow BlockReceiver write data to disk cost:" + duration + "ms (threshold="
								 + datanodeSlowLogThresholdMs + "ms)");
						}
						byte[] lastCrc;
						if (shouldNotWriteChecksum)
						{
							lastCrc = null;
						}
						else
						{
							int skip = 0;
							byte[] crcBytes = null;
							// First, prepare to overwrite the partial crc at the end.
							if (overwriteLastCrc)
							{
								// not chunk-aligned on disk
								// prepare to overwrite last checksum
								AdjustCrcFilePosition();
							}
							// The CRC was recalculated for the last partial chunk. Update the
							// CRC by reading the rest of the chunk, then write it out.
							if (doCrcRecalc)
							{
								// Calculate new crc for this chunk.
								int bytesToReadForRecalc = (int)(bytesPerChecksum - partialChunkSizeOnDisk);
								if (numBytesToDisk < bytesToReadForRecalc)
								{
									bytesToReadForRecalc = numBytesToDisk;
								}
								partialCrc.Update(((byte[])dataBuf.Array()), startByteToDisk, bytesToReadForRecalc
									);
								byte[] buf = FSOutputSummer.ConvertToByteStream(partialCrc, checksumSize);
								crcBytes = CopyLastChunkChecksum(buf, checksumSize, buf.Length);
								checksumOut.Write(buf);
								if (Log.IsDebugEnabled())
								{
									Log.Debug("Writing out partial crc for data len " + len + ", skip=" + skip);
								}
								skip++;
							}
							//  For the partial chunk that was just read.
							// Determine how many checksums need to be skipped up to the last
							// boundary. The checksum after the boundary was already counted
							// above. Only count the number of checksums skipped up to the
							// boundary here.
							long skippedDataBytes = lastChunkBoundary - firstByteInBlock;
							if (skippedDataBytes > 0)
							{
								skip += (int)(skippedDataBytes / bytesPerChecksum) + ((skippedDataBytes % bytesPerChecksum
									 == 0) ? 0 : 1);
							}
							skip *= checksumSize;
							// Convert to number of bytes
							// write the rest of checksum
							int offset = checksumBuf.ArrayOffset() + checksumBuf.Position() + skip;
							int end = offset + checksumLen - skip;
							// If offset >= end, there is no more checksum to write.
							// I.e. a partial chunk checksum rewrite happened and there is no
							// more to write after that.
							if (offset >= end && doCrcRecalc)
							{
								lastCrc = crcBytes;
							}
							else
							{
								int remainingBytes = checksumLen - skip;
								lastCrc = CopyLastChunkChecksum(((byte[])checksumBuf.Array()), checksumSize, end);
								checksumOut.Write(((byte[])checksumBuf.Array()), offset, remainingBytes);
							}
						}
						/// flush entire packet, sync if requested
						FlushOrSync(syncBlock);
						replicaInfo.SetLastChecksumAndDataLen(offsetInBlock, lastCrc);
						datanode.metrics.IncrBytesWritten(len);
						datanode.metrics.IncrTotalWriteTime(duration);
						ManageWriterOsCache(offsetInBlock);
					}
				}
				catch (IOException iex)
				{
					datanode.CheckDiskErrorAsync();
					throw;
				}
			}
			// if sync was requested, put in queue for pending acks here
			// (after the fsync finished)
			if (responder != null && (syncBlock || ShouldVerifyChecksum()))
			{
				((BlockReceiver.PacketResponder)responder.GetRunnable()).Enqueue(seqno, lastPacketInBlock
					, offsetInBlock, DataTransferProtos.Status.Success);
			}
			/*
			* Send in-progress responses for the replaceBlock() calls back to caller to
			* avoid timeouts due to balancer throttling. HDFS-6247
			*/
			if (isReplaceBlock && (Time.MonotonicNow() - lastResponseTime > responseInterval))
			{
				DataTransferProtos.BlockOpResponseProto.Builder response = DataTransferProtos.BlockOpResponseProto
					.NewBuilder().SetStatus(DataTransferProtos.Status.InProgress);
				((DataTransferProtos.BlockOpResponseProto)response.Build()).WriteDelimitedTo(replyOut
					);
				replyOut.Flush();
				lastResponseTime = Time.MonotonicNow();
			}
			if (throttler != null)
			{
				// throttle I/O
				throttler.Throttle(len);
			}
			return lastPacketInBlock ? -1 : len;
		}

		private static byte[] CopyLastChunkChecksum(byte[] array, int size, int end)
		{
			return Arrays.CopyOfRange(array, end - size, end);
		}

		private void ManageWriterOsCache(long offsetInBlock)
		{
			try
			{
				if (outFd != null && offsetInBlock > lastCacheManagementOffset + CacheDropLagBytes)
				{
					long begin = Time.MonotonicNow();
					//
					// For SYNC_FILE_RANGE_WRITE, we want to sync from
					// lastCacheManagementOffset to a position "two windows ago"
					//
					//                         <========= sync ===========>
					// +-----------------------O--------------------------X
					// start                  last                      curPos
					// of file                 
					//
					if (syncBehindWrites)
					{
						if (syncBehindWritesInBackground)
						{
							this.datanode.GetFSDataset().SubmitBackgroundSyncFileRangeRequest(block, outFd, lastCacheManagementOffset
								, offsetInBlock - lastCacheManagementOffset, NativeIO.POSIX.SyncFileRangeWrite);
						}
						else
						{
							NativeIO.POSIX.SyncFileRangeIfPossible(outFd, lastCacheManagementOffset, offsetInBlock
								 - lastCacheManagementOffset, NativeIO.POSIX.SyncFileRangeWrite);
						}
					}
					//
					// For POSIX_FADV_DONTNEED, we want to drop from the beginning 
					// of the file to a position prior to the current position.
					//
					// <=== drop =====> 
					//                 <---W--->
					// +--------------+--------O--------------------------X
					// start        dropPos   last                      curPos
					// of file             
					//                     
					long dropPos = lastCacheManagementOffset - CacheDropLagBytes;
					if (dropPos > 0 && dropCacheBehindWrites)
					{
						NativeIO.POSIX.GetCacheManipulator().PosixFadviseIfPossible(block.GetBlockName(), 
							outFd, 0, dropPos, NativeIO.POSIX.PosixFadvDontneed);
					}
					lastCacheManagementOffset = offsetInBlock;
					long duration = Time.MonotonicNow() - begin;
					if (duration > datanodeSlowLogThresholdMs)
					{
						Log.Warn("Slow manageWriterOsCache took " + duration + "ms (threshold=" + datanodeSlowLogThresholdMs
							 + "ms)");
					}
				}
			}
			catch (Exception t)
			{
				Log.Warn("Error managing cache for writer of block " + block, t);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void SendOOB()
		{
			((BlockReceiver.PacketResponder)responder.GetRunnable()).SendOOBResponse(PipelineAck
				.GetRestartOOBStatus());
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void ReceiveBlock(DataOutputStream mirrOut, DataInputStream mirrIn
			, DataOutputStream replyOut, string mirrAddr, DataTransferThrottler throttlerArg
			, DatanodeInfo[] downstreams, bool isReplaceBlock)
		{
			// output to next datanode
			// input from next datanode
			// output to previous datanode
			syncOnClose = datanode.GetDnConf().syncOnClose;
			bool responderClosed = false;
			mirrorOut = mirrOut;
			mirrorAddr = mirrAddr;
			throttler = throttlerArg;
			this.replyOut = replyOut;
			this.isReplaceBlock = isReplaceBlock;
			try
			{
				if (isClient && !isTransfer)
				{
					responder = new Daemon(datanode.threadGroup, new BlockReceiver.PacketResponder(this
						, replyOut, mirrIn, downstreams));
					responder.Start();
				}
				// start thread to processes responses
				while (ReceivePacket() >= 0)
				{
				}
				/* Receive until the last packet */
				// wait for all outstanding packet responses. And then
				// indicate responder to gracefully shutdown.
				// Mark that responder has been closed for future processing
				if (responder != null)
				{
					((BlockReceiver.PacketResponder)responder.GetRunnable()).Close();
					responderClosed = true;
				}
				// If this write is for a replication or transfer-RBW/Finalized,
				// then finalize block or convert temporary to RBW.
				// For client-writes, the block is finalized in the PacketResponder.
				if (isDatanode || isTransfer)
				{
					// Hold a volume reference to finalize block.
					using (ReplicaHandler handler = ClaimReplicaHandler())
					{
						// close the block/crc files
						Close();
						block.SetNumBytes(replicaInfo.GetNumBytes());
						if (stage == BlockConstructionStage.TransferRbw)
						{
							// for TRANSFER_RBW, convert temporary to RBW
							datanode.data.ConvertTemporaryToRbw(block);
						}
						else
						{
							// for isDatnode or TRANSFER_FINALIZED
							// Finalize the block.
							datanode.data.FinalizeBlock(block);
						}
					}
					datanode.metrics.IncrBlocksWritten();
				}
			}
			catch (IOException ioe)
			{
				replicaInfo.ReleaseAllBytesReserved();
				if (datanode.IsRestarting())
				{
					// Do not throw if shutting down for restart. Otherwise, it will cause
					// premature termination of responder.
					Log.Info("Shutting down for restart (" + block + ").");
				}
				else
				{
					Log.Info("Exception for " + block, ioe);
					throw;
				}
			}
			finally
			{
				// Clear the previous interrupt state of this thread.
				Sharpen.Thread.Interrupted();
				// If a shutdown for restart was initiated, upstream needs to be notified.
				// There is no need to do anything special if the responder was closed
				// normally.
				if (!responderClosed)
				{
					// Data transfer was not complete.
					if (responder != null)
					{
						// In case this datanode is shutting down for quick restart,
						// send a special ack upstream.
						if (datanode.IsRestarting() && isClient && !isTransfer)
						{
							FilePath blockFile = ((ReplicaInPipeline)replicaInfo).GetBlockFile();
							FilePath restartMeta = new FilePath(blockFile.GetParent() + FilePath.pathSeparator
								 + "." + blockFile.GetName() + ".restart");
							if (restartMeta.Exists() && !restartMeta.Delete())
							{
								Log.Warn("Failed to delete restart meta file: " + restartMeta.GetPath());
							}
							try
							{
								using (TextWriter @out = new OutputStreamWriter(new FileOutputStream(restartMeta)
									, "UTF-8"))
								{
									// write out the current time.
									@out.Write(System.Convert.ToString(Time.Now() + restartBudget));
									@out.Flush();
								}
							}
							catch (IOException)
							{
							}
							finally
							{
								// The worst case is not recovering this RBW replica. 
								// Client will fall back to regular pipeline recovery.
								IOUtils.Cleanup(Log, @out);
							}
							try
							{
								// Even if the connection is closed after the ack packet is
								// flushed, the client can react to the connection closure 
								// first. Insert a delay to lower the chance of client 
								// missing the OOB ack.
								Sharpen.Thread.Sleep(1000);
							}
							catch (Exception)
							{
							}
						}
						// It is already going down. Ignore this.
						responder.Interrupt();
					}
					IOUtils.CloseStream(this);
					CleanupBlock();
				}
				if (responder != null)
				{
					try
					{
						responder.Interrupt();
						// join() on the responder should timeout a bit earlier than the
						// configured deadline. Otherwise, the join() on this thread will
						// likely timeout as well.
						long joinTimeout = datanode.GetDnConf().GetXceiverStopTimeout();
						joinTimeout = joinTimeout > 1 ? joinTimeout * 8 / 10 : joinTimeout;
						responder.Join(joinTimeout);
						if (responder.IsAlive())
						{
							string msg = "Join on responder thread " + responder + " timed out";
							Log.Warn(msg + "\n" + StringUtils.GetStackTrace(responder));
							throw new IOException(msg);
						}
					}
					catch (Exception)
					{
						responder.Interrupt();
						// do not throw if shutting down for restart.
						if (!datanode.IsRestarting())
						{
							throw new IOException("Interrupted receiveBlock");
						}
					}
					responder = null;
				}
			}
		}

		/// <summary>
		/// Cleanup a partial block
		/// if this write is for a replication request (and not from a client)
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		private void CleanupBlock()
		{
			if (isDatanode)
			{
				datanode.data.UnfinalizeBlock(block);
			}
		}

		/// <summary>
		/// Adjust the file pointer in the local meta file so that the last checksum
		/// will be overwritten.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		private void AdjustCrcFilePosition()
		{
			if (@out != null)
			{
				@out.Flush();
			}
			if (checksumOut != null)
			{
				checksumOut.Flush();
			}
			// rollback the position of the meta file
			datanode.data.AdjustCrcChannelPosition(block, streams, checksumSize);
		}

		/// <summary>Convert a checksum byte array to a long</summary>
		private static long Checksum2long(byte[] checksum)
		{
			long crc = 0L;
			for (int i = 0; i < checksum.Length; i++)
			{
				crc |= (unchecked((long)(0xffL)) & checksum[i]) << ((checksum.Length - i - 1) * 8
					);
			}
			return crc;
		}

		/// <summary>
		/// reads in the partial crc chunk and computes checksum
		/// of pre-existing data in partial chunk.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		private Checksum ComputePartialChunkCrc(long blkoff, long ckoff)
		{
			// find offset of the beginning of partial chunk.
			//
			int sizePartialChunk = (int)(blkoff % bytesPerChecksum);
			blkoff = blkoff - sizePartialChunk;
			if (Log.IsDebugEnabled())
			{
				Log.Debug("computePartialChunkCrc for " + block + ": sizePartialChunk=" + sizePartialChunk
					 + ", block offset=" + blkoff + ", metafile offset=" + ckoff);
			}
			// create an input stream from the block file
			// and read in partial crc chunk into temporary buffer
			//
			byte[] buf = new byte[sizePartialChunk];
			byte[] crcbuf = new byte[checksumSize];
			using (ReplicaInputStreams instr = datanode.data.GetTmpInputStreams(block, blkoff
				, ckoff))
			{
				IOUtils.ReadFully(instr.GetDataIn(), buf, 0, sizePartialChunk);
				// open meta file and read in crc value computer earlier
				IOUtils.ReadFully(instr.GetChecksumIn(), crcbuf, 0, crcbuf.Length);
			}
			// compute crc of partial chunk from data read in the block file.
			Checksum partialCrc = DataChecksum.NewDataChecksum(diskChecksum.GetChecksumType()
				, diskChecksum.GetBytesPerChecksum());
			partialCrc.Update(buf, 0, sizePartialChunk);
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Read in partial CRC chunk from disk for " + block);
			}
			// paranoia! verify that the pre-computed crc matches what we
			// recalculated just now
			if (partialCrc.GetValue() != Checksum2long(crcbuf))
			{
				string msg = "Partial CRC " + partialCrc.GetValue() + " does not match value computed the "
					 + " last time file was closed " + Checksum2long(crcbuf);
				throw new IOException(msg);
			}
			return partialCrc;
		}

		/// <summary>The caller claims the ownership of the replica handler.</summary>
		private ReplicaHandler ClaimReplicaHandler()
		{
			ReplicaHandler handler = replicaHandler;
			replicaHandler = null;
			return handler;
		}

		private enum PacketResponderType
		{
			NonPipeline,
			LastInPipeline,
			HasDownstreamInPipeline
		}

		/// <summary>
		/// Processes responses from downstream datanodes in the pipeline
		/// and sends back replies to the originator.
		/// </summary>
		internal class PacketResponder : Runnable, IDisposable
		{
			/// <summary>queue for packets waiting for ack - synchronization using monitor lock</summary>
			private readonly List<BlockReceiver.Packet> ackQueue = new List<BlockReceiver.Packet
				>();

			/// <summary>the thread that spawns this responder</summary>
			private readonly Sharpen.Thread receiverThread = Sharpen.Thread.CurrentThread();

			/// <summary>is this responder running? - synchronization using monitor lock</summary>
			private volatile bool running = true;

			/// <summary>input from the next downstream datanode</summary>
			private readonly DataInputStream downstreamIn;

			/// <summary>output to upstream datanode/client</summary>
			private readonly DataOutputStream upstreamOut;

			/// <summary>The type of this responder</summary>
			private readonly BlockReceiver.PacketResponderType type;

			/// <summary>for log and error messages</summary>
			private readonly string myString;

			private bool sending = false;

			public override string ToString()
			{
				return this.myString;
			}

			internal PacketResponder(BlockReceiver _enclosing, DataOutputStream upstreamOut, 
				DataInputStream downstreamIn, DatanodeInfo[] downstreams)
			{
				this._enclosing = _enclosing;
				this.downstreamIn = downstreamIn;
				this.upstreamOut = upstreamOut;
				this.type = downstreams == null ? BlockReceiver.PacketResponderType.NonPipeline : 
					downstreams.Length == 0 ? BlockReceiver.PacketResponderType.LastInPipeline : BlockReceiver.PacketResponderType
					.HasDownstreamInPipeline;
				StringBuilder b = new StringBuilder(this.GetType().Name).Append(": ").Append(this
					._enclosing.block).Append(", type=").Append(this.type);
				if (this.type != BlockReceiver.PacketResponderType.HasDownstreamInPipeline)
				{
					b.Append(", downstreams=").Append(downstreams.Length).Append(":").Append(Arrays.AsList
						(downstreams));
				}
				this.myString = b.ToString();
			}

			private bool IsRunning()
			{
				// When preparing for a restart, it should continue to run until
				// interrupted by the receiver thread.
				return this.running && (this._enclosing.datanode.shouldRun || this._enclosing.datanode
					.IsRestarting());
			}

			/// <summary>enqueue the seqno that is still be to acked by the downstream datanode.</summary>
			/// <param name="seqno">sequence number of the packet</param>
			/// <param name="lastPacketInBlock">if true, this is the last packet in block</param>
			/// <param name="offsetInBlock">offset of this packet in block</param>
			internal virtual void Enqueue(long seqno, bool lastPacketInBlock, long offsetInBlock
				, DataTransferProtos.Status ackStatus)
			{
				BlockReceiver.Packet p = new BlockReceiver.Packet(seqno, lastPacketInBlock, offsetInBlock
					, Runtime.NanoTime(), ackStatus);
				if (BlockReceiver.Log.IsDebugEnabled())
				{
					BlockReceiver.Log.Debug(this.myString + ": enqueue " + p);
				}
				lock (this.ackQueue)
				{
					if (this.running)
					{
						this.ackQueue.AddLast(p);
						Sharpen.Runtime.NotifyAll(this.ackQueue);
					}
				}
			}

			/// <summary>Send an OOB response.</summary>
			/// <remarks>
			/// Send an OOB response. If all acks have been sent already for the block
			/// and the responder is about to close, the delivery is not guaranteed.
			/// This is because the other end can close the connection independently.
			/// An OOB coming from downstream will be automatically relayed upstream
			/// by the responder. This method is used only by originating datanode.
			/// </remarks>
			/// <param name="ackStatus">the type of ack to be sent</param>
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			internal virtual void SendOOBResponse(DataTransferProtos.Status ackStatus)
			{
				if (!this.running)
				{
					BlockReceiver.Log.Info("Cannot send OOB response " + ackStatus + ". Responder not running."
						);
					return;
				}
				lock (this)
				{
					if (this.sending)
					{
						Sharpen.Runtime.Wait(this, PipelineAck.GetOOBTimeout(ackStatus));
						// Didn't get my turn in time. Give up.
						if (this.sending)
						{
							throw new IOException("Could not send OOB reponse in time: " + ackStatus);
						}
					}
					this.sending = true;
				}
				BlockReceiver.Log.Info("Sending an out of band ack of type " + ackStatus);
				try
				{
					this.SendAckUpstreamUnprotected(null, PipelineAck.UnkownSeqno, 0L, 0L, PipelineAck
						.CombineHeader(this._enclosing.datanode.GetECN(), ackStatus));
				}
				finally
				{
					// Let others send ack. Unless there are miltiple OOB send
					// calls, there can be only one waiter, the responder thread.
					// In any case, only one needs to be notified.
					lock (this)
					{
						this.sending = false;
						Sharpen.Runtime.Notify(this);
					}
				}
			}

			/// <summary>
			/// Wait for a packet with given
			/// <paramref name="seqno"/>
			/// to be enqueued to ackQueue
			/// </summary>
			/// <exception cref="System.Exception"/>
			internal virtual BlockReceiver.Packet WaitForAckHead(long seqno)
			{
				lock (this.ackQueue)
				{
					while (this.IsRunning() && this.ackQueue.Count == 0)
					{
						if (BlockReceiver.Log.IsDebugEnabled())
						{
							BlockReceiver.Log.Debug(this.myString + ": seqno=" + seqno + " waiting for local datanode to finish write."
								);
						}
						Sharpen.Runtime.Wait(this.ackQueue);
					}
					return this.IsRunning() ? this.ackQueue.GetFirst() : null;
				}
			}

			/// <summary>wait for all pending packets to be acked.</summary>
			/// <remarks>wait for all pending packets to be acked. Then shutdown thread.</remarks>
			public virtual void Close()
			{
				lock (this.ackQueue)
				{
					while (this.IsRunning() && this.ackQueue.Count != 0)
					{
						try
						{
							Sharpen.Runtime.Wait(this.ackQueue);
						}
						catch (Exception)
						{
							this.running = false;
							Sharpen.Thread.CurrentThread().Interrupt();
						}
					}
					if (BlockReceiver.Log.IsDebugEnabled())
					{
						BlockReceiver.Log.Debug(this.myString + ": closing");
					}
					this.running = false;
					Sharpen.Runtime.NotifyAll(this.ackQueue);
				}
				lock (this)
				{
					this.running = false;
					Sharpen.Runtime.NotifyAll(this);
				}
			}

			/// <summary>Thread to process incoming acks.</summary>
			/// <seealso cref="Sharpen.Runnable.Run()"/>
			public virtual void Run()
			{
				bool lastPacketInBlock = false;
				long startTime = BlockReceiver.ClientTraceLog.IsInfoEnabled() ? Runtime.NanoTime(
					) : 0;
				while (this.IsRunning() && !lastPacketInBlock)
				{
					long totalAckTimeNanos = 0;
					bool isInterrupted = false;
					try
					{
						BlockReceiver.Packet pkt = null;
						long expected = -2;
						PipelineAck ack = new PipelineAck();
						long seqno = PipelineAck.UnkownSeqno;
						long ackRecvNanoTime = 0;
						try
						{
							if (this.type != BlockReceiver.PacketResponderType.LastInPipeline && !this._enclosing
								.mirrorError)
							{
								// read an ack from downstream datanode
								ack.ReadFields(this.downstreamIn);
								ackRecvNanoTime = Runtime.NanoTime();
								if (BlockReceiver.Log.IsDebugEnabled())
								{
									BlockReceiver.Log.Debug(this.myString + " got " + ack);
								}
								// Process an OOB ACK.
								DataTransferProtos.Status oobStatus = ack.GetOOBStatus();
								if (oobStatus != null)
								{
									BlockReceiver.Log.Info("Relaying an out of band ack of type " + oobStatus);
									this.SendAckUpstream(ack, PipelineAck.UnkownSeqno, 0L, 0L, PipelineAck.CombineHeader
										(this._enclosing.datanode.GetECN(), DataTransferProtos.Status.Success));
									continue;
								}
								seqno = ack.GetSeqno();
							}
							if (seqno != PipelineAck.UnkownSeqno || this.type == BlockReceiver.PacketResponderType
								.LastInPipeline)
							{
								pkt = this.WaitForAckHead(seqno);
								if (!this.IsRunning())
								{
									break;
								}
								expected = pkt.seqno;
								if (this.type == BlockReceiver.PacketResponderType.HasDownstreamInPipeline && seqno
									 != expected)
								{
									throw new IOException(this.myString + "seqno: expected=" + expected + ", received="
										 + seqno);
								}
								if (this.type == BlockReceiver.PacketResponderType.HasDownstreamInPipeline)
								{
									// The total ack time includes the ack times of downstream
									// nodes.
									// The value is 0 if this responder doesn't have a downstream
									// DN in the pipeline.
									totalAckTimeNanos = ackRecvNanoTime - pkt.ackEnqueueNanoTime;
									// Report the elapsed time from ack send to ack receive minus
									// the downstream ack time.
									long ackTimeNanos = totalAckTimeNanos - ack.GetDownstreamAckTimeNanos();
									if (ackTimeNanos < 0)
									{
										if (BlockReceiver.Log.IsDebugEnabled())
										{
											BlockReceiver.Log.Debug("Calculated invalid ack time: " + ackTimeNanos + "ns.");
										}
									}
									else
									{
										this._enclosing.datanode.metrics.AddPacketAckRoundTripTimeNanos(ackTimeNanos);
									}
								}
								lastPacketInBlock = pkt.lastPacketInBlock;
							}
						}
						catch (Exception)
						{
							isInterrupted = true;
						}
						catch (IOException ioe)
						{
							if (Sharpen.Thread.Interrupted())
							{
								isInterrupted = true;
							}
							else
							{
								if (ioe is EOFException && !this._enclosing.PacketSentInTime())
								{
									// The downstream error was caused by upstream including this
									// node not sending packet in time. Let the upstream determine
									// who is at fault.  If the immediate upstream node thinks it
									// has sent a packet in time, this node will be reported as bad.
									// Otherwise, the upstream node will propagate the error up by
									// closing the connection.
									BlockReceiver.Log.Warn("The downstream error might be due to congestion in " + "upstream including this node. Propagating the error: "
										, ioe);
									throw;
								}
								else
								{
									// continue to run even if can not read from mirror
									// notify client of the error
									// and wait for the client to shut down the pipeline
									this._enclosing.mirrorError = true;
									BlockReceiver.Log.Info(this.myString, ioe);
								}
							}
						}
						if (Sharpen.Thread.Interrupted() || isInterrupted)
						{
							/*
							* The receiver thread cancelled this thread. We could also check
							* any other status updates from the receiver thread (e.g. if it is
							* ok to write to replyOut). It is prudent to not send any more
							* status back to the client because this datanode has a problem.
							* The upstream datanode will detect that this datanode is bad, and
							* rightly so.
							*
							* The receiver thread can also interrupt this thread for sending
							* an out-of-band response upstream.
							*/
							BlockReceiver.Log.Info(this.myString + ": Thread is interrupted.");
							this.running = false;
							continue;
						}
						if (lastPacketInBlock)
						{
							// Finalize the block and close the block file
							this.FinalizeBlock(startTime);
						}
						DataTransferProtos.Status myStatus = pkt != null ? pkt.ackStatus : DataTransferProtos.Status
							.Success;
						this.SendAckUpstream(ack, expected, totalAckTimeNanos, (pkt != null ? pkt.offsetInBlock
							 : 0), PipelineAck.CombineHeader(this._enclosing.datanode.GetECN(), myStatus));
						if (pkt != null)
						{
							// remove the packet from the ack queue
							this.RemoveAckHead();
						}
					}
					catch (IOException e)
					{
						BlockReceiver.Log.Warn("IOException in BlockReceiver.run(): ", e);
						if (this.running)
						{
							this._enclosing.datanode.CheckDiskErrorAsync();
							BlockReceiver.Log.Info(this.myString, e);
							this.running = false;
							if (!Sharpen.Thread.Interrupted())
							{
								// failure not caused by interruption
								this.receiverThread.Interrupt();
							}
						}
					}
					catch (Exception e)
					{
						if (this.running)
						{
							BlockReceiver.Log.Info(this.myString, e);
							this.running = false;
							this.receiverThread.Interrupt();
						}
					}
				}
				BlockReceiver.Log.Info(this.myString + " terminating");
			}

			/// <summary>Finalize the block and close the block file</summary>
			/// <param name="startTime">time when BlockReceiver started receiving the block</param>
			/// <exception cref="System.IO.IOException"/>
			private void FinalizeBlock(long startTime)
			{
				long endTime = 0;
				// Hold a volume reference to finalize block.
				using (ReplicaHandler handler = this._enclosing.ClaimReplicaHandler())
				{
					this._enclosing.Close();
					endTime = BlockReceiver.ClientTraceLog.IsInfoEnabled() ? Runtime.NanoTime() : 0;
					this._enclosing.block.SetNumBytes(this._enclosing.replicaInfo.GetNumBytes());
					this._enclosing.datanode.data.FinalizeBlock(this._enclosing.block);
				}
				if (this._enclosing.pinning)
				{
					this._enclosing.datanode.data.SetPinning(this._enclosing.block);
				}
				this._enclosing.datanode.CloseBlock(this._enclosing.block, DataNode.EmptyDelHint, 
					this._enclosing.replicaInfo.GetStorageUuid());
				if (BlockReceiver.ClientTraceLog.IsInfoEnabled() && this._enclosing.isClient)
				{
					long offset = 0;
					DatanodeRegistration dnR = this._enclosing.datanode.GetDNRegistrationForBP(this._enclosing
						.block.GetBlockPoolId());
					BlockReceiver.ClientTraceLog.Info(string.Format(DataNode.DnClienttraceFormat, this
						._enclosing.inAddr, this._enclosing.myAddr, this._enclosing.block.GetNumBytes(), 
						"HDFS_WRITE", this._enclosing.clientname, offset, dnR.GetDatanodeUuid(), this._enclosing
						.block, endTime - startTime));
				}
				else
				{
					BlockReceiver.Log.Info("Received " + this._enclosing.block + " size " + this._enclosing
						.block.GetNumBytes() + " from " + this._enclosing.inAddr);
				}
			}

			/// <summary>The wrapper for the unprotected version.</summary>
			/// <remarks>
			/// The wrapper for the unprotected version. This is only called by
			/// the responder's run() method.
			/// </remarks>
			/// <param name="ack">Ack received from downstream</param>
			/// <param name="seqno">sequence number of ack to be sent upstream</param>
			/// <param name="totalAckTimeNanos">
			/// total ack time including all the downstream
			/// nodes
			/// </param>
			/// <param name="offsetInBlock">offset in block for the data in packet</param>
			/// <param name="myHeader">the local ack header</param>
			/// <exception cref="System.IO.IOException"/>
			private void SendAckUpstream(PipelineAck ack, long seqno, long totalAckTimeNanos, 
				long offsetInBlock, int myHeader)
			{
				try
				{
					// Wait for other sender to finish. Unless there is an OOB being sent,
					// the responder won't have to wait.
					lock (this)
					{
						while (this.sending)
						{
							Sharpen.Runtime.Wait(this);
						}
						this.sending = true;
					}
					try
					{
						if (!this.running)
						{
							return;
						}
						this.SendAckUpstreamUnprotected(ack, seqno, totalAckTimeNanos, offsetInBlock, myHeader
							);
					}
					finally
					{
						lock (this)
						{
							this.sending = false;
							Sharpen.Runtime.Notify(this);
						}
					}
				}
				catch (Exception)
				{
					// The responder was interrupted. Make it go down without
					// interrupting the receiver(writer) thread.  
					this.running = false;
				}
			}

			/// <param name="ack">Ack received from downstream</param>
			/// <param name="seqno">sequence number of ack to be sent upstream</param>
			/// <param name="totalAckTimeNanos">
			/// total ack time including all the downstream
			/// nodes
			/// </param>
			/// <param name="offsetInBlock">offset in block for the data in packet</param>
			/// <param name="myHeader">the local ack header</param>
			/// <exception cref="System.IO.IOException"/>
			private void SendAckUpstreamUnprotected(PipelineAck ack, long seqno, long totalAckTimeNanos
				, long offsetInBlock, int myHeader)
			{
				int[] replies;
				if (ack == null)
				{
					// A new OOB response is being sent from this node. Regardless of
					// downstream nodes, reply should contain one reply.
					replies = new int[] { myHeader };
				}
				else
				{
					if (this._enclosing.mirrorError)
					{
						// ack read error
						int h = PipelineAck.CombineHeader(this._enclosing.datanode.GetECN(), DataTransferProtos.Status
							.Success);
						int h1 = PipelineAck.CombineHeader(this._enclosing.datanode.GetECN(), DataTransferProtos.Status
							.Error);
						replies = new int[] { h, h1 };
					}
					else
					{
						short ackLen = this.type == BlockReceiver.PacketResponderType.LastInPipeline ? 0 : 
							ack.GetNumOfReplies();
						replies = new int[ackLen + 1];
						replies[0] = myHeader;
						for (int i = 0; i < ackLen; ++i)
						{
							replies[i + 1] = ack.GetHeaderFlag(i);
						}
						// If the mirror has reported that it received a corrupt packet,
						// do self-destruct to mark myself bad, instead of making the
						// mirror node bad. The mirror is guaranteed to be good without
						// corrupt data on disk.
						if (ackLen > 0 && PipelineAck.GetStatusFromHeader(replies[1]) == DataTransferProtos.Status
							.ErrorChecksum)
						{
							throw new IOException("Shutting down writer and responder " + "since the down streams reported the data sent by this "
								 + "thread is corrupt");
						}
					}
				}
				PipelineAck replyAck = new PipelineAck(seqno, replies, totalAckTimeNanos);
				if (replyAck.IsSuccess() && offsetInBlock > this._enclosing.replicaInfo.GetBytesAcked
					())
				{
					this._enclosing.replicaInfo.SetBytesAcked(offsetInBlock);
				}
				// send my ack back to upstream datanode
				long begin = Time.MonotonicNow();
				replyAck.Write(this.upstreamOut);
				this.upstreamOut.Flush();
				long duration = Time.MonotonicNow() - begin;
				if (duration > this._enclosing.datanodeSlowLogThresholdMs)
				{
					BlockReceiver.Log.Warn("Slow PacketResponder send ack to upstream took " + duration
						 + "ms (threshold=" + this._enclosing.datanodeSlowLogThresholdMs + "ms), " + this
						.myString + ", replyAck=" + replyAck);
				}
				else
				{
					if (BlockReceiver.Log.IsDebugEnabled())
					{
						BlockReceiver.Log.Debug(this.myString + ", replyAck=" + replyAck);
					}
				}
				// If a corruption was detected in the received data, terminate after
				// sending ERROR_CHECKSUM back.
				DataTransferProtos.Status myStatus = PipelineAck.GetStatusFromHeader(myHeader);
				if (myStatus == DataTransferProtos.Status.ErrorChecksum)
				{
					throw new IOException("Shutting down writer and responder " + "due to a checksum error in received data. The error "
						 + "response has been sent upstream.");
				}
			}

			/// <summary>
			/// Remove a packet from the head of the ack queue
			/// This should be called only when the ack queue is not empty
			/// </summary>
			private void RemoveAckHead()
			{
				lock (this.ackQueue)
				{
					this.ackQueue.RemoveFirst();
					Sharpen.Runtime.NotifyAll(this.ackQueue);
				}
			}

			private readonly BlockReceiver _enclosing;
		}

		/// <summary>This information is cached by the Datanode in the ackQueue.</summary>
		private class Packet
		{
			internal readonly long seqno;

			internal readonly bool lastPacketInBlock;

			internal readonly long offsetInBlock;

			internal readonly long ackEnqueueNanoTime;

			internal readonly DataTransferProtos.Status ackStatus;

			internal Packet(long seqno, bool lastPacketInBlock, long offsetInBlock, long ackEnqueueNanoTime
				, DataTransferProtos.Status ackStatus)
			{
				this.seqno = seqno;
				this.lastPacketInBlock = lastPacketInBlock;
				this.offsetInBlock = offsetInBlock;
				this.ackEnqueueNanoTime = ackEnqueueNanoTime;
				this.ackStatus = ackStatus;
			}

			public override string ToString()
			{
				return GetType().Name + "(seqno=" + seqno + ", lastPacketInBlock=" + lastPacketInBlock
					 + ", offsetInBlock=" + offsetInBlock + ", ackEnqueueNanoTime=" + ackEnqueueNanoTime
					 + ", ackStatus=" + ackStatus + ")";
			}
		}
	}
}
