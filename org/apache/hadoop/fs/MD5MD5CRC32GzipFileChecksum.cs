using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>For CRC32 with the Gzip polynomial</summary>
	public class MD5MD5CRC32GzipFileChecksum : org.apache.hadoop.fs.MD5MD5CRC32FileChecksum
	{
		/// <summary>Same as this(0, 0, null)</summary>
		public MD5MD5CRC32GzipFileChecksum()
			: this(0, 0, null)
		{
		}

		/// <summary>Create a MD5FileChecksum</summary>
		public MD5MD5CRC32GzipFileChecksum(int bytesPerCRC, long crcPerBlock, org.apache.hadoop.io.MD5Hash
			 md5)
			: base(bytesPerCRC, crcPerBlock, md5)
		{
		}

		public override org.apache.hadoop.util.DataChecksum.Type getCrcType()
		{
			// default to the one that is understood by all releases.
			return org.apache.hadoop.util.DataChecksum.Type.CRC32;
		}
	}
}
