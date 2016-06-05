using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Util;

namespace Hadoop.Common.Core.Fs.Ftp
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
	public class FtpConfigKeys : CommonConfigurationKeys
	{
		public const string BlockSizeKey = "ftp.blocksize";

		public const long BlockSizeDefault = 4 * 1024;

		public const string ReplicationKey = "ftp.replication";

		public const short ReplicationDefault = 1;

		public const string StreamBufferSizeKey = "ftp.stream-buffer-size";

		public const int StreamBufferSizeDefault = 1024 * 1024;

		public const string BytesPerChecksumKey = "ftp.bytes-per-checksum";

		public const int BytesPerChecksumDefault = 512;

		public const string ClientWritePacketSizeKey = "ftp.client-write-packet-size";

		public const int ClientWritePacketSizeDefault = 64 * 1024;

		public const bool EncryptDataTransferDefault = false;

		public const long FsTrashIntervalDefault = 0;

		public static readonly DataChecksum.Type ChecksumTypeDefault = DataChecksum.Type.
			Crc32;

		/// <exception cref="System.IO.IOException"/>
		protected internal static FsServerDefaults GetServerDefaults()
		{
			return new FsServerDefaults(BlockSizeDefault, BytesPerChecksumDefault, ClientWritePacketSizeDefault
				, ReplicationDefault, StreamBufferSizeDefault, EncryptDataTransferDefault, FsTrashIntervalDefault
				, ChecksumTypeDefault);
		}
	}
}
