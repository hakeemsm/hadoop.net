using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Blockmanagement
{
	/// <summary>
	/// This interface is used by the block manager to expose a
	/// few characteristics of a collection of Block/BlockUnderConstruction.
	/// </summary>
	public interface BlockCollection
	{
		/// <summary>Get the last block of the collection.</summary>
		BlockInfoContiguous GetLastBlock();

		/// <summary>Get content summary.</summary>
		ContentSummary ComputeContentSummary(BlockStoragePolicySuite bsps);

		/// <returns>the number of blocks</returns>
		int NumBlocks();

		/// <summary>Get the blocks.</summary>
		BlockInfoContiguous[] GetBlocks();

		/// <summary>Get preferred block size for the collection</summary>
		/// <returns>preferred block size in bytes</returns>
		long GetPreferredBlockSize();

		/// <summary>Get block replication for the collection</summary>
		/// <returns>block replication value</returns>
		short GetBlockReplication();

		/// <returns>the storage policy ID.</returns>
		byte GetStoragePolicyID();

		/// <summary>Get the name of the collection.</summary>
		string GetName();

		/// <summary>Set the block at the given index.</summary>
		void SetBlock(int index, BlockInfoContiguous blk);

		/// <summary>
		/// Convert the last block of the collection to an under-construction block
		/// and set the locations.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		BlockInfoContiguousUnderConstruction SetLastBlock(BlockInfoContiguous lastBlock, 
			DatanodeStorageInfo[] targets);

		/// <returns>whether the block collection is under construction.</returns>
		bool IsUnderConstruction();
	}
}
