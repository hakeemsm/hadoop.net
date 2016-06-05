using Org.Apache.Hadoop.Hdfs.Protocol;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	/// <summary>
	/// Wrapper for
	/// <see cref="BlockLocation"/>
	/// that also includes a
	/// <see cref="Org.Apache.Hadoop.Hdfs.Protocol.LocatedBlock"/>
	/// ,
	/// allowing more detailed queries to the datanode about a block.
	/// </summary>
	public class HdfsBlockLocation : BlockLocation
	{
		private readonly LocatedBlock block;

		/// <exception cref="System.IO.IOException"/>
		public HdfsBlockLocation(BlockLocation loc, LocatedBlock block)
			: base(loc)
		{
			// Initialize with data from passed in BlockLocation
			this.block = block;
		}

		public virtual LocatedBlock GetLocatedBlock()
		{
			return block;
		}
	}
}
