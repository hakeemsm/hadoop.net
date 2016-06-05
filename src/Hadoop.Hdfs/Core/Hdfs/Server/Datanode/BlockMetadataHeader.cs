using System.IO;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode
{
	/// <summary>BlockMetadataHeader manages metadata for data blocks on Datanodes.</summary>
	/// <remarks>
	/// BlockMetadataHeader manages metadata for data blocks on Datanodes.
	/// This is not related to the Block related functionality in Namenode.
	/// The biggest part of data block metadata is CRC for the block.
	/// </remarks>
	public class BlockMetadataHeader
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.Server.Datanode.BlockMetadataHeader
			));

		public const short Version = 1;

		/// <summary>Header includes everything except the checksum(s) themselves.</summary>
		/// <remarks>
		/// Header includes everything except the checksum(s) themselves.
		/// Version is two bytes. Following it is the DataChecksum
		/// that occupies 5 bytes.
		/// </remarks>
		private readonly short version;

		private DataChecksum checksum = null;

		[VisibleForTesting]
		public BlockMetadataHeader(short version, DataChecksum checksum)
		{
			this.checksum = checksum;
			this.version = version;
		}

		/// <summary>Get the version</summary>
		public virtual short GetVersion()
		{
			return version;
		}

		/// <summary>Get the checksum</summary>
		public virtual DataChecksum GetChecksum()
		{
			return checksum;
		}

		/// <summary>Read the checksum header from the meta file.</summary>
		/// <returns>the data checksum obtained from the header.</returns>
		/// <exception cref="System.IO.IOException"/>
		public static DataChecksum ReadDataChecksum(FilePath metaFile)
		{
			DataInputStream @in = null;
			try
			{
				@in = new DataInputStream(new BufferedInputStream(new FileInputStream(metaFile), 
					HdfsConstants.IoFileBufferSize));
				return ReadDataChecksum(@in, metaFile);
			}
			finally
			{
				IOUtils.CloseStream(@in);
			}
		}

		/// <summary>Read the checksum header from the meta input stream.</summary>
		/// <returns>the data checksum obtained from the header.</returns>
		/// <exception cref="System.IO.IOException"/>
		public static DataChecksum ReadDataChecksum(DataInputStream metaIn, object name)
		{
			// read and handle the common header here. For now just a version
			Org.Apache.Hadoop.Hdfs.Server.Datanode.BlockMetadataHeader header = ReadHeader(metaIn
				);
			if (header.GetVersion() != Version)
			{
				Log.Warn("Unexpected meta-file version for " + name + ": version in file is " + header
					.GetVersion() + " but expected version is " + Version);
			}
			return header.GetChecksum();
		}

		/// <summary>Read the header without changing the position of the FileChannel.</summary>
		/// <param name="fc">The FileChannel to read.</param>
		/// <returns>the Metadata Header.</returns>
		/// <exception cref="System.IO.IOException">on error.</exception>
		public static Org.Apache.Hadoop.Hdfs.Server.Datanode.BlockMetadataHeader PreadHeader
			(FileChannel fc)
		{
			byte[] arr = new byte[GetHeaderSize()];
			ByteBuffer buf = ByteBuffer.Wrap(arr);
			while (buf.HasRemaining())
			{
				if (fc.Read(buf, 0) <= 0)
				{
					throw new EOFException("unexpected EOF while reading " + "metadata file header");
				}
			}
			short version = (short)((arr[0] << 8) | (arr[1] & unchecked((int)(0xff))));
			DataChecksum dataChecksum = DataChecksum.NewDataChecksum(arr, 2);
			return new Org.Apache.Hadoop.Hdfs.Server.Datanode.BlockMetadataHeader(version, dataChecksum
				);
		}

		/// <summary>This reads all the fields till the beginning of checksum.</summary>
		/// <returns>Metadata Header</returns>
		/// <exception cref="System.IO.IOException"/>
		public static Org.Apache.Hadoop.Hdfs.Server.Datanode.BlockMetadataHeader ReadHeader
			(DataInputStream @in)
		{
			return ReadHeader(@in.ReadShort(), @in);
		}

		/// <summary>Reads header at the top of metadata file and returns the header.</summary>
		/// <returns>metadata header for the block</returns>
		/// <exception cref="System.IO.IOException"/>
		public static Org.Apache.Hadoop.Hdfs.Server.Datanode.BlockMetadataHeader ReadHeader
			(FilePath file)
		{
			DataInputStream @in = null;
			try
			{
				@in = new DataInputStream(new BufferedInputStream(new FileInputStream(file)));
				return ReadHeader(@in);
			}
			finally
			{
				IOUtils.CloseStream(@in);
			}
		}

		/// <summary>Read the header at the beginning of the given block meta file.</summary>
		/// <remarks>
		/// Read the header at the beginning of the given block meta file.
		/// The current file position will be altered by this method.
		/// If an error occurs, the file is <em>not</em> closed.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public static Org.Apache.Hadoop.Hdfs.Server.Datanode.BlockMetadataHeader ReadHeader
			(RandomAccessFile raf)
		{
			byte[] buf = new byte[GetHeaderSize()];
			raf.Seek(0);
			raf.ReadFully(buf, 0, buf.Length);
			return ReadHeader(new DataInputStream(new ByteArrayInputStream(buf)));
		}

		// Version is already read.
		/// <exception cref="System.IO.IOException"/>
		private static Org.Apache.Hadoop.Hdfs.Server.Datanode.BlockMetadataHeader ReadHeader
			(short version, DataInputStream @in)
		{
			DataChecksum checksum = DataChecksum.NewDataChecksum(@in);
			return new Org.Apache.Hadoop.Hdfs.Server.Datanode.BlockMetadataHeader(version, checksum
				);
		}

		/// <summary>This writes all the fields till the beginning of checksum.</summary>
		/// <param name="out">DataOutputStream</param>
		/// <exception cref="System.IO.IOException"/>
		[VisibleForTesting]
		public static void WriteHeader(DataOutputStream @out, Org.Apache.Hadoop.Hdfs.Server.Datanode.BlockMetadataHeader
			 header)
		{
			@out.WriteShort(header.GetVersion());
			header.GetChecksum().WriteHeader(@out);
		}

		/// <summary>Writes all the fields till the beginning of checksum.</summary>
		/// <exception cref="System.IO.IOException">on error</exception>
		public static void WriteHeader(DataOutputStream @out, DataChecksum checksum)
		{
			WriteHeader(@out, new Org.Apache.Hadoop.Hdfs.Server.Datanode.BlockMetadataHeader(
				Version, checksum));
		}

		/// <summary>Returns the size of the header</summary>
		public static int GetHeaderSize()
		{
			return short.Size / byte.Size + DataChecksum.GetChecksumHeaderSize();
		}
	}
}
