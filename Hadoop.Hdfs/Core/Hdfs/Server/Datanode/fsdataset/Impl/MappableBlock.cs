using System;
using System.IO;
using Com.Google.Common.Base;
using Org.Apache.Commons.IO;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.IO.Nativeio;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset.Impl
{
	/// <summary>Represents an HDFS block that is mmapped by the DataNode.</summary>
	public class MappableBlock : IDisposable
	{
		private MappedByteBuffer mmap;

		private readonly long length;

		internal MappableBlock(MappedByteBuffer mmap, long length)
		{
			this.mmap = mmap;
			this.length = length;
			System.Diagnostics.Debug.Assert(length > 0);
		}

		public virtual long GetLength()
		{
			return length;
		}

		/// <summary>Load the block.</summary>
		/// <remarks>
		/// Load the block.
		/// mmap and mlock the block, and then verify its checksum.
		/// </remarks>
		/// <param name="length">The current length of the block.</param>
		/// <param name="blockIn">
		/// The block input stream.  Should be positioned at the
		/// start.  The caller must close this.
		/// </param>
		/// <param name="metaIn">
		/// The meta file input stream.  Should be positioned at
		/// the start.  The caller must close this.
		/// </param>
		/// <param name="blockFileName">The block file name, for logging purposes.</param>
		/// <returns>The Mappable block.</returns>
		/// <exception cref="System.IO.IOException"/>
		public static Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset.Impl.MappableBlock
			 Load(long length, FileInputStream blockIn, FileInputStream metaIn, string blockFileName
			)
		{
			Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset.Impl.MappableBlock mappableBlock
				 = null;
			MappedByteBuffer mmap = null;
			FileChannel blockChannel = null;
			try
			{
				blockChannel = blockIn.GetChannel();
				if (blockChannel == null)
				{
					throw new IOException("Block InputStream has no FileChannel.");
				}
				mmap = blockChannel.Map(FileChannel.MapMode.ReadOnly, 0, length);
				NativeIO.POSIX.GetCacheManipulator().Mlock(blockFileName, mmap, length);
				VerifyChecksum(length, metaIn, blockChannel, blockFileName);
				mappableBlock = new Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset.Impl.MappableBlock
					(mmap, length);
			}
			finally
			{
				IOUtils.CloseQuietly(blockChannel);
				if (mappableBlock == null)
				{
					if (mmap != null)
					{
						NativeIO.POSIX.Munmap(mmap);
					}
				}
			}
			// unmapping also unlocks
			return mappableBlock;
		}

		/// <summary>Verifies the block's checksum.</summary>
		/// <remarks>Verifies the block's checksum. This is an I/O intensive operation.</remarks>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.ChecksumException"/>
		private static void VerifyChecksum(long length, FileInputStream metaIn, FileChannel
			 blockChannel, string blockFileName)
		{
			// Verify the checksum from the block's meta file
			// Get the DataChecksum from the meta file header
			BlockMetadataHeader header = BlockMetadataHeader.ReadHeader(new DataInputStream(new 
				BufferedInputStream(metaIn, BlockMetadataHeader.GetHeaderSize())));
			FileChannel metaChannel = null;
			try
			{
				metaChannel = metaIn.GetChannel();
				if (metaChannel == null)
				{
					throw new IOException("Block InputStream meta file has no FileChannel.");
				}
				DataChecksum checksum = header.GetChecksum();
				int bytesPerChecksum = checksum.GetBytesPerChecksum();
				int checksumSize = checksum.GetChecksumSize();
				int numChunks = (8 * 1024 * 1024) / bytesPerChecksum;
				ByteBuffer blockBuf = ByteBuffer.Allocate(numChunks * bytesPerChecksum);
				ByteBuffer checksumBuf = ByteBuffer.Allocate(numChunks * checksumSize);
				// Verify the checksum
				int bytesVerified = 0;
				while (bytesVerified < length)
				{
					Preconditions.CheckState(bytesVerified % bytesPerChecksum == 0, "Unexpected partial chunk before EOF"
						);
					System.Diagnostics.Debug.Assert(bytesVerified % bytesPerChecksum == 0);
					int bytesRead = FillBuffer(blockChannel, blockBuf);
					if (bytesRead == -1)
					{
						throw new IOException("checksum verification failed: premature EOF");
					}
					blockBuf.Flip();
					// Number of read chunks, including partial chunk at end
					int chunks = (bytesRead + bytesPerChecksum - 1) / bytesPerChecksum;
					checksumBuf.Limit(chunks * checksumSize);
					FillBuffer(metaChannel, checksumBuf);
					checksumBuf.Flip();
					checksum.VerifyChunkedSums(blockBuf, checksumBuf, blockFileName, bytesVerified);
					// Success
					bytesVerified += bytesRead;
					blockBuf.Clear();
					checksumBuf.Clear();
				}
			}
			finally
			{
				IOUtils.CloseQuietly(metaChannel);
			}
		}

		/// <summary>Reads bytes into a buffer until EOF or the buffer's limit is reached</summary>
		/// <exception cref="System.IO.IOException"/>
		private static int FillBuffer(FileChannel channel, ByteBuffer buf)
		{
			int bytesRead = channel.Read(buf);
			if (bytesRead < 0)
			{
				//EOF
				return bytesRead;
			}
			while (buf.Remaining() > 0)
			{
				int n = channel.Read(buf);
				if (n < 0)
				{
					//EOF
					return bytesRead;
				}
				bytesRead += n;
			}
			return bytesRead;
		}

		public virtual void Close()
		{
			if (mmap != null)
			{
				NativeIO.POSIX.Munmap(mmap);
				mmap = null;
			}
		}
	}
}
