using Javax.Servlet;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Lib.Service;
using Sharpen;

namespace Org.Apache.Hadoop.Lib.Servlet
{
	/// <summary>
	/// The <code>FileSystemReleaseFilter</code> releases back to the
	/// <see cref="Org.Apache.Hadoop.Lib.Service.FileSystemAccess"/>
	/// service a <code>FileSystem</code> instance.
	/// <p>
	/// This filter is useful in situations where a servlet request
	/// is streaming out HDFS data and the corresponding filesystem
	/// instance have to be closed after the streaming completes.
	/// </summary>
	public abstract class FileSystemReleaseFilter : Filter
	{
		private static readonly ThreadLocal<FileSystem> FileSystemTl = new ThreadLocal<FileSystem
			>();

		/// <summary>Initializes the filter.</summary>
		/// <remarks>
		/// Initializes the filter.
		/// <p>
		/// This implementation is a NOP.
		/// </remarks>
		/// <param name="filterConfig">filter configuration.</param>
		/// <exception cref="Javax.Servlet.ServletException">thrown if the filter could not be initialized.
		/// 	</exception>
		public virtual void Init(FilterConfig filterConfig)
		{
		}

		/// <summary>
		/// It delegates the incoming request to the <code>FilterChain</code>, and
		/// at its completion (in a finally block) releases the filesystem instance
		/// back to the
		/// <see cref="Org.Apache.Hadoop.Lib.Service.FileSystemAccess"/>
		/// service.
		/// </summary>
		/// <param name="servletRequest">servlet request.</param>
		/// <param name="servletResponse">servlet response.</param>
		/// <param name="filterChain">filter chain.</param>
		/// <exception cref="System.IO.IOException">thrown if an IO error occurrs.</exception>
		/// <exception cref="Javax.Servlet.ServletException">thrown if a servet error occurrs.
		/// 	</exception>
		public virtual void DoFilter(ServletRequest servletRequest, ServletResponse servletResponse
			, FilterChain filterChain)
		{
			try
			{
				filterChain.DoFilter(servletRequest, servletResponse);
			}
			finally
			{
				FileSystem fs = FileSystemTl.Get();
				if (fs != null)
				{
					FileSystemTl.Remove();
					GetFileSystemAccess().ReleaseFileSystem(fs);
				}
			}
		}

		/// <summary>Destroys the filter.</summary>
		/// <remarks>
		/// Destroys the filter.
		/// <p>
		/// This implementation is a NOP.
		/// </remarks>
		public virtual void Destroy()
		{
		}

		/// <summary>
		/// Static method that sets the <code>FileSystem</code> to release back to
		/// the
		/// <see cref="Org.Apache.Hadoop.Lib.Service.FileSystemAccess"/>
		/// service on servlet request completion.
		/// </summary>
		/// <param name="fs">fileystem instance.</param>
		public static void SetFileSystem(FileSystem fs)
		{
			FileSystemTl.Set(fs);
		}

		/// <summary>
		/// Abstract method to be implemetned by concrete implementations of the
		/// filter that return the
		/// <see cref="Org.Apache.Hadoop.Lib.Service.FileSystemAccess"/>
		/// service to which the filesystem
		/// will be returned to.
		/// </summary>
		/// <returns>the FileSystemAccess service.</returns>
		protected internal abstract FileSystemAccess GetFileSystemAccess();
	}
}
