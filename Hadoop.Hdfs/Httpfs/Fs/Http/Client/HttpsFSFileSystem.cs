using Sharpen;

namespace Org.Apache.Hadoop.FS.Http.Client
{
	/// <summary><p>HttpFSServer implementation of the FileSystemAccess FileSystem for SSL.
	/// 	</summary>
	/// <remarks>
	/// <p>HttpFSServer implementation of the FileSystemAccess FileSystem for SSL.
	/// </p>
	/// <p>This implementation allows a user to access HDFS over HTTPS via a
	/// HttpFSServer server.</p>
	/// </remarks>
	public class HttpsFSFileSystem : HttpFSFileSystem
	{
		public const string Scheme = "swebhdfs";

		public override string GetScheme()
		{
			return Scheme;
		}
	}
}
