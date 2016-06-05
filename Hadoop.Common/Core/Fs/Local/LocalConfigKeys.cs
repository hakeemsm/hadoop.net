using Hadoop.Common.Core.Fs;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Util;


namespace Org.Apache.Hadoop.FS.Local
{
	/// <summary>
	/// This class contains constants for configuration keys used
	/// in the local file system, raw local fs and checksum fs.
	/// </summary>
	/// <remarks>
	/// This class contains constants for configuration keys used
	/// in the local file system, raw local fs and checksum fs.
	/// Note that the settings for unimplemented features are ignored.
	/// E.g. checksum related settings are just place holders. Even when
	/// wrapped with
	/// <see cref="ChecksumFileSystem"/>
	/// , these settings are not
	/// used.
	/// </remarks>
	public class LocalConfigKeys : CommonConfigurationKeys
	{
		public const string BlockSizeKey = "file.blocksize";

		public const long BlockSizeDefault = 64 * 1024 * 1024;

		public const string ReplicationKey = "file.replication";

		public const short ReplicationDefault = 1;

		public const string StreamBufferSizeKey = "file.stream-buffer-size";

		public const int StreamBufferSizeDefault = 4096;

		public const string BytesPerChecksumKey = "file.bytes-per-checksum";

		public const int BytesPerChecksumDefault = 512;

		public const string ClientWritePacketSizeKey = "file.client-write-packet-size";

		public const int ClientWritePacketSizeDefault = 64 * 1024;

		public const bool EncryptDataTransferDefault = false;

		public const long FsTrashIntervalDefault = 0;

		public static readonly DataChecksum.Type ChecksumTypeDefault = DataChecksum.Type.
			Crc32;

		/// <exception cref="System.IO.IOException"/>
		public static FsServerDefaults GetServerDefaults()
		{
			return new FsServerDefaults(BlockSizeDefault, BytesPerChecksumDefault, ClientWritePacketSizeDefault
				, ReplicationDefault, StreamBufferSizeDefault, EncryptDataTransferDefault, FsTrashIntervalDefault
				, ChecksumTypeDefault);
		}
	}
}
