using Org.Apache.Hadoop.Hdfs.Protocol;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Protocol
{
	/// <summary>Block report for a Datanode storage</summary>
	public class StorageBlockReport
	{
		private readonly DatanodeStorage storage;

		private readonly BlockListAsLongs blocks;

		public StorageBlockReport(DatanodeStorage storage, BlockListAsLongs blocks)
		{
			this.storage = storage;
			this.blocks = blocks;
		}

		public virtual DatanodeStorage GetStorage()
		{
			return storage;
		}

		public virtual BlockListAsLongs GetBlocks()
		{
			return blocks;
		}
	}
}
