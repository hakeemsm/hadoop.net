using System.IO;
using System.Text;
using Org.Apache.Commons.Lang;
using Org.Apache.Commons.Logging;


namespace Org.Apache.Hadoop.IO.Nativeio
{
	/// <summary>A factory for creating shared file descriptors inside a given directory.
	/// 	</summary>
	/// <remarks>
	/// A factory for creating shared file descriptors inside a given directory.
	/// Typically, the directory will be /dev/shm or /tmp.
	/// We will hand out file descriptors that correspond to unlinked files residing
	/// in that directory.  These file descriptors are suitable for sharing across
	/// multiple processes and are both readable and writable.
	/// Because we unlink the temporary files right after creating them, a JVM crash
	/// usually does not leave behind any temporary files in the directory.  However,
	/// it may happen that we crash right after creating the file and before
	/// unlinking it.  In the constructor, we attempt to clean up after any such
	/// remnants by trying to unlink any temporary files created by previous
	/// SharedFileDescriptorFactory instances that also used our prefix.
	/// </remarks>
	public class SharedFileDescriptorFactory
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.IO.Nativeio.SharedFileDescriptorFactory
			));

		private readonly string prefix;

		private readonly string path;

		public static string GetLoadingFailureReason()
		{
			if (!NativeIO.IsAvailable())
			{
				return "NativeIO is not available.";
			}
			if (!SystemUtils.IsOsUnix)
			{
				return "The OS is not UNIX.";
			}
			return null;
		}

		/// <summary>Create a new SharedFileDescriptorFactory.</summary>
		/// <param name="prefix">
		/// The prefix to prepend to all the file names created
		/// by this factory.
		/// </param>
		/// <param name="paths">
		/// An array of paths to use.  We will try each path in
		/// succession, and return a factory using the first
		/// usable path.
		/// </param>
		/// <returns>The factory.</returns>
		/// <exception cref="System.IO.IOException">If a factory could not be created for any reason.
		/// 	</exception>
		public static Org.Apache.Hadoop.IO.Nativeio.SharedFileDescriptorFactory Create(string
			 prefix, string[] paths)
		{
			string loadingFailureReason = GetLoadingFailureReason();
			if (loadingFailureReason != null)
			{
				throw new IOException(loadingFailureReason);
			}
			if (paths.Length == 0)
			{
				throw new IOException("no SharedFileDescriptorFactory paths were " + "configured."
					);
			}
			StringBuilder errors = new StringBuilder();
			string strPrefix = string.Empty;
			foreach (string path in paths)
			{
				try
				{
					FileInputStream fis = new FileInputStream(CreateDescriptor0(prefix + "test", path
						, 1));
					fis.Close();
					DeleteStaleTemporaryFiles0(prefix, path);
					return new Org.Apache.Hadoop.IO.Nativeio.SharedFileDescriptorFactory(prefix, path
						);
				}
				catch (IOException e)
				{
					errors.Append(strPrefix).Append("Error creating file descriptor in ").Append(path
						).Append(": ").Append(e.Message);
					strPrefix = ", ";
				}
			}
			throw new IOException(errors.ToString());
		}

		/// <summary>Create a SharedFileDescriptorFactory.</summary>
		/// <param name="prefix">Prefix to add to all file names we use.</param>
		/// <param name="path">Path to use.</param>
		private SharedFileDescriptorFactory(string prefix, string path)
		{
			this.prefix = prefix;
			this.path = path;
		}

		public virtual string GetPath()
		{
			return path;
		}

		/// <summary>Create a shared file descriptor which will be both readable and writable.
		/// 	</summary>
		/// <param name="info">
		/// Information to include in the path of the
		/// generated descriptor.
		/// </param>
		/// <param name="length">The starting file length.</param>
		/// <returns>The file descriptor, wrapped in a FileInputStream.</returns>
		/// <exception cref="System.IO.IOException">
		/// If there was an I/O or configuration error creating
		/// the descriptor.
		/// </exception>
		public virtual FileInputStream CreateDescriptor(string info, int length)
		{
			return new FileInputStream(CreateDescriptor0(prefix + info, path, length));
		}

		/// <summary>Delete temporary files in the directory, NOT following symlinks.</summary>
		/// <exception cref="System.IO.IOException"/>
		private static void DeleteStaleTemporaryFiles0(string prefix, string path)
		{
		}

		/// <summary>Create a file with O_EXCL, and then resize it to the desired size.</summary>
		/// <exception cref="System.IO.IOException"/>
		private static FileDescriptor CreateDescriptor0(string prefix, string path, int length
			)
		{
		}
	}
}
