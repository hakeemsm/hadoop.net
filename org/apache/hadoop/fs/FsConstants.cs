using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>FileSystem related constants.</summary>
	public abstract class FsConstants
	{
		public const java.net.URI LOCAL_FS_URI = java.net.URI.create("file:///");

		public const string FTP_SCHEME = "ftp";

		public const int MAX_PATH_LINKS = 32;

		/// <summary>ViewFs: viewFs file system (ie the mount file system on client side)</summary>
		public const java.net.URI VIEWFS_URI = java.net.URI.create("viewfs:///");

		public const string VIEWFS_SCHEME = "viewfs";
		// URI for local filesystem
		// URI scheme for FTP
		// Maximum number of symlinks to recursively resolve in a path
	}

	public static class FsConstantsConstants
	{
	}
}
