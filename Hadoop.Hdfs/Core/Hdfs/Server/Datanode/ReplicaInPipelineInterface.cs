using Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode
{
	/// <summary>This defines the interface of a replica in Pipeline that's being written to
	/// 	</summary>
	public interface ReplicaInPipelineInterface : Replica
	{
		/// <summary>Set the number of bytes received</summary>
		/// <param name="bytesReceived">number of bytes received</param>
		void SetNumBytes(long bytesReceived);

		/// <summary>Get the number of bytes acked</summary>
		/// <returns>the number of bytes acked</returns>
		long GetBytesAcked();

		/// <summary>Set the number bytes that have acked</summary>
		/// <param name="bytesAcked">number bytes acked</param>
		void SetBytesAcked(long bytesAcked);

		/// <summary>Release any disk space reserved for this replica.</summary>
		void ReleaseAllBytesReserved();

		/// <summary>store the checksum for the last chunk along with the data length</summary>
		/// <param name="dataLength">number of bytes on disk</param>
		/// <param name="lastChecksum">- checksum bytes for the last chunk</param>
		void SetLastChecksumAndDataLen(long dataLength, byte[] lastChecksum);

		/// <summary>
		/// gets the last chunk checksum and the length of the block corresponding
		/// to that checksum
		/// </summary>
		ChunkChecksum GetLastChecksumAndDataLen();

		/// <summary>
		/// Create output streams for writing to this replica,
		/// one for block file and one for CRC file
		/// </summary>
		/// <param name="isCreate">if it is for creation</param>
		/// <param name="requestedChecksum">the checksum the writer would prefer to use</param>
		/// <returns>output streams for writing</returns>
		/// <exception cref="System.IO.IOException">if any error occurs</exception>
		ReplicaOutputStreams CreateStreams(bool isCreate, DataChecksum requestedChecksum);
	}
}
