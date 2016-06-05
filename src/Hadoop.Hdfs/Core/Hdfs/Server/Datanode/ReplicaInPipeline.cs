using System;
using System.IO;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode
{
	/// <summary>
	/// This class defines a replica in a pipeline, which
	/// includes a persistent replica being written to by a dfs client or
	/// a temporary replica being replicated by a source datanode or
	/// being copied for the balancing purpose.
	/// </summary>
	/// <remarks>
	/// This class defines a replica in a pipeline, which
	/// includes a persistent replica being written to by a dfs client or
	/// a temporary replica being replicated by a source datanode or
	/// being copied for the balancing purpose.
	/// The base class implements a temporary replica
	/// </remarks>
	public class ReplicaInPipeline : ReplicaInfo, ReplicaInPipelineInterface
	{
		private long bytesAcked;

		private long bytesOnDisk;

		private byte[] lastChecksum;

		private Sharpen.Thread writer;

		/// <summary>Bytes reserved for this replica on the containing volume.</summary>
		/// <remarks>
		/// Bytes reserved for this replica on the containing volume.
		/// Based off difference between the estimated maximum block length and
		/// the bytes already written to this block.
		/// </remarks>
		private long bytesReserved;

		/// <summary>Constructor for a zero length replica</summary>
		/// <param name="blockId">block id</param>
		/// <param name="genStamp">replica generation stamp</param>
		/// <param name="vol">volume where replica is located</param>
		/// <param name="dir">directory path where block and meta files are located</param>
		/// <param name="bytesToReserve">
		/// disk space to reserve for this replica, based on
		/// the estimated maximum block length.
		/// </param>
		public ReplicaInPipeline(long blockId, long genStamp, FsVolumeSpi vol, FilePath dir
			, long bytesToReserve)
			: this(blockId, 0L, genStamp, vol, dir, Sharpen.Thread.CurrentThread(), bytesToReserve
				)
		{
		}

		/// <summary>Constructor</summary>
		/// <param name="block">a block</param>
		/// <param name="vol">volume where replica is located</param>
		/// <param name="dir">directory path where block and meta files are located</param>
		/// <param name="writer">a thread that is writing to this replica</param>
		internal ReplicaInPipeline(Block block, FsVolumeSpi vol, FilePath dir, Sharpen.Thread
			 writer)
			: this(block.GetBlockId(), block.GetNumBytes(), block.GetGenerationStamp(), vol, 
				dir, writer, 0L)
		{
		}

		/// <summary>Constructor</summary>
		/// <param name="blockId">block id</param>
		/// <param name="len">replica length</param>
		/// <param name="genStamp">replica generation stamp</param>
		/// <param name="vol">volume where replica is located</param>
		/// <param name="dir">directory path where block and meta files are located</param>
		/// <param name="writer">a thread that is writing to this replica</param>
		/// <param name="bytesToReserve">
		/// disk space to reserve for this replica, based on
		/// the estimated maximum block length.
		/// </param>
		internal ReplicaInPipeline(long blockId, long len, long genStamp, FsVolumeSpi vol
			, FilePath dir, Sharpen.Thread writer, long bytesToReserve)
			: base(blockId, len, genStamp, vol, dir)
		{
			this.bytesAcked = len;
			this.bytesOnDisk = len;
			this.writer = writer;
			this.bytesReserved = bytesToReserve;
		}

		/// <summary>Copy constructor.</summary>
		/// <param name="from">where to copy from</param>
		public ReplicaInPipeline(Org.Apache.Hadoop.Hdfs.Server.Datanode.ReplicaInPipeline
			 from)
			: base(from)
		{
			this.bytesAcked = from.GetBytesAcked();
			this.bytesOnDisk = from.GetBytesOnDisk();
			this.writer = from.writer;
			this.bytesReserved = from.bytesReserved;
		}

		public override long GetVisibleLength()
		{
			return -1;
		}

		public override HdfsServerConstants.ReplicaState GetState()
		{
			//ReplicaInfo
			return HdfsServerConstants.ReplicaState.Temporary;
		}

		public virtual long GetBytesAcked()
		{
			// ReplicaInPipelineInterface
			return bytesAcked;
		}

		public virtual void SetBytesAcked(long bytesAcked)
		{
			// ReplicaInPipelineInterface
			long newBytesAcked = bytesAcked - this.bytesAcked;
			this.bytesAcked = bytesAcked;
			// Once bytes are ACK'ed we can release equivalent space from the
			// volume's reservedForRbw count. We could have released it as soon
			// as the write-to-disk completed but that would be inefficient.
			GetVolume().ReleaseReservedSpace(newBytesAcked);
			bytesReserved -= newBytesAcked;
		}

		public override long GetBytesOnDisk()
		{
			// ReplicaInPipelineInterface
			return bytesOnDisk;
		}

		public override long GetBytesReserved()
		{
			return bytesReserved;
		}

		public virtual void ReleaseAllBytesReserved()
		{
			// ReplicaInPipelineInterface
			GetVolume().ReleaseReservedSpace(bytesReserved);
			bytesReserved = 0;
		}

		public virtual void SetLastChecksumAndDataLen(long dataLength, byte[] lastChecksum
			)
		{
			lock (this)
			{
				// ReplicaInPipelineInterface
				this.bytesOnDisk = dataLength;
				this.lastChecksum = lastChecksum;
			}
		}

		public virtual ChunkChecksum GetLastChecksumAndDataLen()
		{
			lock (this)
			{
				// ReplicaInPipelineInterface
				return new ChunkChecksum(GetBytesOnDisk(), lastChecksum);
			}
		}

		/// <summary>Set the thread that is writing to this replica</summary>
		/// <param name="writer">a thread writing to this replica</param>
		public virtual void SetWriter(Sharpen.Thread writer)
		{
			this.writer = writer;
		}

		public override bool Equals(object o)
		{
			// Object
			return base.Equals(o);
		}

		/// <summary>Interrupt the writing thread and wait until it dies</summary>
		/// <exception cref="System.IO.IOException">the waiting is interrupted</exception>
		public virtual void StopWriter(long xceiverStopTimeout)
		{
			if (writer != null && writer != Sharpen.Thread.CurrentThread() && writer.IsAlive(
				))
			{
				writer.Interrupt();
				try
				{
					writer.Join(xceiverStopTimeout);
					if (writer.IsAlive())
					{
						string msg = "Join on writer thread " + writer + " timed out";
						DataNode.Log.Warn(msg + "\n" + StringUtils.GetStackTrace(writer));
						throw new IOException(msg);
					}
				}
				catch (Exception)
				{
					throw new IOException("Waiting for writer thread is interrupted.");
				}
			}
		}

		public override int GetHashCode()
		{
			// Object
			return base.GetHashCode();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual ReplicaOutputStreams CreateStreams(bool isCreate, DataChecksum requestedChecksum
			)
		{
			// ReplicaInPipelineInterface
			FilePath blockFile = GetBlockFile();
			FilePath metaFile = GetMetaFile();
			if (DataNode.Log.IsDebugEnabled())
			{
				DataNode.Log.Debug("writeTo blockfile is " + blockFile + " of size " + blockFile.
					Length());
				DataNode.Log.Debug("writeTo metafile is " + metaFile + " of size " + metaFile.Length
					());
			}
			long blockDiskSize = 0L;
			long crcDiskSize = 0L;
			// the checksum that should actually be used -- this
			// may differ from requestedChecksum for appends.
			DataChecksum checksum;
			RandomAccessFile metaRAF = new RandomAccessFile(metaFile, "rw");
			if (!isCreate)
			{
				// For append or recovery, we must enforce the existing checksum.
				// Also, verify that the file has correct lengths, etc.
				bool checkedMeta = false;
				try
				{
					BlockMetadataHeader header = BlockMetadataHeader.ReadHeader(metaRAF);
					checksum = header.GetChecksum();
					if (checksum.GetBytesPerChecksum() != requestedChecksum.GetBytesPerChecksum())
					{
						throw new IOException("Client requested checksum " + requestedChecksum + " when appending to an existing block "
							 + "with different chunk size: " + checksum);
					}
					int bytesPerChunk = checksum.GetBytesPerChecksum();
					int checksumSize = checksum.GetChecksumSize();
					blockDiskSize = bytesOnDisk;
					crcDiskSize = BlockMetadataHeader.GetHeaderSize() + (blockDiskSize + bytesPerChunk
						 - 1) / bytesPerChunk * checksumSize;
					if (blockDiskSize > 0 && (blockDiskSize > blockFile.Length() || crcDiskSize > metaFile
						.Length()))
					{
						throw new IOException("Corrupted block: " + this);
					}
					checkedMeta = true;
				}
				finally
				{
					if (!checkedMeta)
					{
						// clean up in case of exceptions.
						IOUtils.CloseStream(metaRAF);
					}
				}
			}
			else
			{
				// for create, we can use the requested checksum
				checksum = requestedChecksum;
			}
			FileOutputStream blockOut = null;
			FileOutputStream crcOut = null;
			try
			{
				blockOut = new FileOutputStream(new RandomAccessFile(blockFile, "rw").GetFD());
				crcOut = new FileOutputStream(metaRAF.GetFD());
				if (!isCreate)
				{
					blockOut.GetChannel().Position(blockDiskSize);
					crcOut.GetChannel().Position(crcDiskSize);
				}
				return new ReplicaOutputStreams(blockOut, crcOut, checksum, GetVolume().IsTransientStorage
					());
			}
			catch (IOException e)
			{
				IOUtils.CloseStream(blockOut);
				IOUtils.CloseStream(metaRAF);
				throw;
			}
		}

		public override string ToString()
		{
			return base.ToString() + "\n  bytesAcked=" + bytesAcked + "\n  bytesOnDisk=" + bytesOnDisk;
		}
	}
}
