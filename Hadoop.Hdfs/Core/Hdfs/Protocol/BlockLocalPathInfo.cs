using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Protocol
{
	/// <summary>
	/// A block and the full path information to the block data file and
	/// the metadata file stored on the local file system.
	/// </summary>
	public class BlockLocalPathInfo
	{
		private readonly ExtendedBlock block;

		private string localBlockPath = string.Empty;

		private string localMetaPath = string.Empty;

		/// <summary>Constructs BlockLocalPathInfo.</summary>
		/// <param name="b">The block corresponding to this lock path info.</param>
		/// <param name="file">Block data file.</param>
		/// <param name="metafile">Metadata file for the block.</param>
		public BlockLocalPathInfo(ExtendedBlock b, string file, string metafile)
		{
			// local file storing the data
			// local file storing the checksum
			block = b;
			localBlockPath = file;
			localMetaPath = metafile;
		}

		/// <summary>Get the Block data file.</summary>
		/// <returns>Block data file.</returns>
		public virtual string GetBlockPath()
		{
			return localBlockPath;
		}

		/// <returns>the Block</returns>
		public virtual ExtendedBlock GetBlock()
		{
			return block;
		}

		/// <summary>Get the Block metadata file.</summary>
		/// <returns>Block metadata file.</returns>
		public virtual string GetMetaPath()
		{
			return localMetaPath;
		}

		/// <summary>Get number of bytes in the block.</summary>
		/// <returns>Number of bytes in the block.</returns>
		public virtual long GetNumBytes()
		{
			return block.GetNumBytes();
		}
	}
}
