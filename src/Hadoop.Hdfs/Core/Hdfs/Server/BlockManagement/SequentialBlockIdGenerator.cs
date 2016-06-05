using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Blockmanagement
{
	/// <summary>
	/// Generate the next valid block ID by incrementing the maximum block
	/// ID allocated so far, starting at 2^30+1.
	/// </summary>
	/// <remarks>
	/// Generate the next valid block ID by incrementing the maximum block
	/// ID allocated so far, starting at 2^30+1.
	/// Block IDs used to be allocated randomly in the past. Hence we may
	/// find some conflicts while stepping through the ID space sequentially.
	/// However given the sparsity of the ID space, conflicts should be rare
	/// and can be skipped over when detected.
	/// </remarks>
	public class SequentialBlockIdGenerator : SequentialNumber
	{
		/// <summary>The last reserved block ID.</summary>
		public const long LastReservedBlockId = 1024L * 1024 * 1024;

		private readonly BlockManager blockManager;

		internal SequentialBlockIdGenerator(BlockManager blockManagerRef)
			: base(LastReservedBlockId)
		{
			this.blockManager = blockManagerRef;
		}

		public override long NextValue()
		{
			// NumberGenerator
			Block b = new Block(base.NextValue());
			// There may be an occasional conflict with randomly generated
			// block IDs. Skip over the conflicts.
			while (IsValidBlock(b))
			{
				b.SetBlockId(base.NextValue());
			}
			return b.GetBlockId();
		}

		/// <summary>Returns whether the given block is one pointed-to by a file.</summary>
		private bool IsValidBlock(Block b)
		{
			return (blockManager.GetBlockCollection(b) != null);
		}
	}
}
