using Org.Apache.Hadoop.Lib.Service;
using Org.Apache.Hadoop.Lib.Servlet;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Http.Server
{
	/// <summary>
	/// Filter that releases FileSystemAccess filesystem instances upon HTTP request
	/// completion.
	/// </summary>
	public class HttpFSReleaseFilter : FileSystemReleaseFilter
	{
		/// <summary>
		/// Returns the
		/// <see cref="Org.Apache.Hadoop.Lib.Service.FileSystemAccess"/>
		/// service to return the FileSystemAccess filesystem
		/// instance to.
		/// </summary>
		/// <returns>the FileSystemAccess service.</returns>
		protected internal override FileSystemAccess GetFileSystemAccess()
		{
			return HttpFSServerWebApp.Get().Get<FileSystemAccess>();
		}
	}
}
