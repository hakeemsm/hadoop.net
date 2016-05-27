using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	/// <summary>
	/// This class contains constants for configuration keys used
	/// in the local file system, raw local fs and checksum fs.
	/// </summary>
	public class LocalFileSystemConfigKeys : CommonConfigurationKeys
	{
		public const string LocalFsBlockSizeKey = "file.blocksize";

		public const long LocalFsBlockSizeDefault = 64 * 1024 * 1024;

		public const string LocalFsReplicationKey = "file.replication";

		public const short LocalFsReplicationDefault = 1;

		public const string LocalFsStreamBufferSizeKey = "file.stream-buffer-size";

		public const int LocalFsStreamBufferSizeDefault = 4096;

		public const string LocalFsBytesPerChecksumKey = "file.bytes-per-checksum";

		public const int LocalFsBytesPerChecksumDefault = 512;

		public const string LocalFsClientWritePacketSizeKey = "file.client-write-packet-size";

		public const int LocalFsClientWritePacketSizeDefault = 64 * 1024;
	}
}
