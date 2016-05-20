using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>
	/// This class contains constants for configuration keys used
	/// in the local file system, raw local fs and checksum fs.
	/// </summary>
	public class LocalFileSystemConfigKeys : org.apache.hadoop.fs.CommonConfigurationKeys
	{
		public const string LOCAL_FS_BLOCK_SIZE_KEY = "file.blocksize";

		public const long LOCAL_FS_BLOCK_SIZE_DEFAULT = 64 * 1024 * 1024;

		public const string LOCAL_FS_REPLICATION_KEY = "file.replication";

		public const short LOCAL_FS_REPLICATION_DEFAULT = 1;

		public const string LOCAL_FS_STREAM_BUFFER_SIZE_KEY = "file.stream-buffer-size";

		public const int LOCAL_FS_STREAM_BUFFER_SIZE_DEFAULT = 4096;

		public const string LOCAL_FS_BYTES_PER_CHECKSUM_KEY = "file.bytes-per-checksum";

		public const int LOCAL_FS_BYTES_PER_CHECKSUM_DEFAULT = 512;

		public const string LOCAL_FS_CLIENT_WRITE_PACKET_SIZE_KEY = "file.client-write-packet-size";

		public const int LOCAL_FS_CLIENT_WRITE_PACKET_SIZE_DEFAULT = 64 * 1024;
	}
}
