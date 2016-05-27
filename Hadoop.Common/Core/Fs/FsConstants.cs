using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	/// <summary>FileSystem related constants.</summary>
	public abstract class FsConstants
	{
		public const URI LocalFsUri = URI.Create("file:///");

		public const string FtpScheme = "ftp";

		public const int MaxPathLinks = 32;

		/// <summary>ViewFs: viewFs file system (ie the mount file system on client side)</summary>
		public const URI ViewfsUri = URI.Create("viewfs:///");

		public const string ViewfsScheme = "viewfs";
		// URI for local filesystem
		// URI scheme for FTP
		// Maximum number of symlinks to recursively resolve in a path
	}

	public static class FsConstantsConstants
	{
	}
}
