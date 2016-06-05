using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.Lib.Service
{
	public abstract class FileSystemAccess
	{
		public interface FileSystemExecutor<T>
		{
			/// <exception cref="System.IO.IOException"/>
			T Execute(FileSystem fs);
		}

		/// <exception cref="Org.Apache.Hadoop.Lib.Service.FileSystemAccessException"/>
		public abstract T Execute<T>(string user, Configuration conf, FileSystemAccess.FileSystemExecutor
			<T> executor);

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Lib.Service.FileSystemAccessException"/>
		public abstract FileSystem CreateFileSystem(string user, Configuration conf);

		/// <exception cref="System.IO.IOException"/>
		public abstract void ReleaseFileSystem(FileSystem fs);

		public abstract Configuration GetFileSystemConfiguration();
	}

	public static class FileSystemAccessConstants
	{
	}
}
