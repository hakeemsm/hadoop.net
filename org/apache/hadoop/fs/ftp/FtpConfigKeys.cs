using Sharpen;

namespace org.apache.hadoop.fs.ftp
{
	/// <summary>
	/// This class contains constants for configuration keys used
	/// in the ftp file system.
	/// </summary>
	/// <remarks>
	/// This class contains constants for configuration keys used
	/// in the ftp file system.
	/// Note that the settings for unimplemented features are ignored.
	/// E.g. checksum related settings are just place holders. Even when
	/// wrapped with
	/// <see cref="ChecksumFileSystem"/>
	/// , these settings are not
	/// used.
	/// </remarks>
	public class FtpConfigKeys : org.apache.hadoop.fs.CommonConfigurationKeys
	{
		public const string BLOCK_SIZE_KEY = "ftp.blocksize";

		public const long BLOCK_SIZE_DEFAULT = 4 * 1024;

		public const string REPLICATION_KEY = "ftp.replication";

		public const short REPLICATION_DEFAULT = 1;

		public const string STREAM_BUFFER_SIZE_KEY = "ftp.stream-buffer-size";

		public const int STREAM_BUFFER_SIZE_DEFAULT = 1024 * 1024;

		public const string BYTES_PER_CHECKSUM_KEY = "ftp.bytes-per-checksum";

		public const int BYTES_PER_CHECKSUM_DEFAULT = 512;

		public const string CLIENT_WRITE_PACKET_SIZE_KEY = "ftp.client-write-packet-size";

		public const int CLIENT_WRITE_PACKET_SIZE_DEFAULT = 64 * 1024;

		public const bool ENCRYPT_DATA_TRANSFER_DEFAULT = false;

		public const long FS_TRASH_INTERVAL_DEFAULT = 0;

		public static readonly org.apache.hadoop.util.DataChecksum.Type CHECKSUM_TYPE_DEFAULT
			 = org.apache.hadoop.util.DataChecksum.Type.CRC32;

		/// <exception cref="System.IO.IOException"/>
		protected internal static org.apache.hadoop.fs.FsServerDefaults getServerDefaults
			()
		{
			return new org.apache.hadoop.fs.FsServerDefaults(BLOCK_SIZE_DEFAULT, BYTES_PER_CHECKSUM_DEFAULT
				, CLIENT_WRITE_PACKET_SIZE_DEFAULT, REPLICATION_DEFAULT, STREAM_BUFFER_SIZE_DEFAULT
				, ENCRYPT_DATA_TRANSFER_DEFAULT, FS_TRASH_INTERVAL_DEFAULT, CHECKSUM_TYPE_DEFAULT
				);
		}
	}
}
