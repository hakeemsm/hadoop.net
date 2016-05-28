using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>Provides an iterator interface for listCorruptFileBlocks.</summary>
	/// <remarks>
	/// Provides an iterator interface for listCorruptFileBlocks.
	/// This class is used by DistributedFileSystem and Hdfs.
	/// </remarks>
	public class CorruptFileBlockIterator : RemoteIterator<Path>
	{
		private readonly DFSClient dfs;

		private readonly string path;

		private string[] files = null;

		private int fileIdx = 0;

		private string cookie = null;

		private Path nextPath = null;

		private int callsMade = 0;

		/// <exception cref="System.IO.IOException"/>
		public CorruptFileBlockIterator(DFSClient dfs, Path path)
		{
			this.dfs = dfs;
			this.path = Path2String(path);
			LoadNext();
		}

		/// <returns>
		/// the number of calls made to the DFSClient.
		/// This is for debugging and testing purposes.
		/// </returns>
		public virtual int GetCallsMade()
		{
			return callsMade;
		}

		private string Path2String(Path path)
		{
			return path.ToUri().GetPath();
		}

		private Path String2Path(string @string)
		{
			return new Path(@string);
		}

		/// <exception cref="System.IO.IOException"/>
		private void LoadNext()
		{
			if (files == null || fileIdx >= files.Length)
			{
				CorruptFileBlocks cfb = dfs.ListCorruptFileBlocks(path, cookie);
				files = cfb.GetFiles();
				cookie = cfb.GetCookie();
				fileIdx = 0;
				callsMade++;
			}
			if (fileIdx >= files.Length)
			{
				// received an empty response
				// there are no more corrupt file blocks
				nextPath = null;
			}
			else
			{
				nextPath = String2Path(files[fileIdx]);
				fileIdx++;
			}
		}

		public virtual bool HasNext()
		{
			return nextPath != null;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual Path Next()
		{
			if (!HasNext())
			{
				throw new NoSuchElementException("No more corrupt file blocks");
			}
			Path result = nextPath;
			LoadNext();
			return result;
		}
	}
}
