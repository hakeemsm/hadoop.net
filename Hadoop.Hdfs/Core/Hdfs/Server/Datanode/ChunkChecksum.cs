using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode
{
	/// <summary>
	/// holder class that holds checksum bytes and the length in a block at which
	/// the checksum bytes end
	/// ex: length = 1023 and checksum is 4 bytes which is for 512 bytes, then
	/// the checksum applies for the last chunk, or bytes 512 - 1023
	/// </summary>
	public class ChunkChecksum
	{
		private readonly long dataLength;

		private readonly byte[] checksum;

		public ChunkChecksum(long dataLength, byte[] checksum)
		{
			// can be null if not available
			this.dataLength = dataLength;
			this.checksum = checksum;
		}

		public virtual long GetDataLength()
		{
			return dataLength;
		}

		public virtual byte[] GetChecksum()
		{
			return checksum;
		}
	}
}
