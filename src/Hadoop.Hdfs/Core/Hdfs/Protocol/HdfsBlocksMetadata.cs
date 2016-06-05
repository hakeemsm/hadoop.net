using System.Collections.Generic;
using Com.Google.Common.Base;
using Com.Google.Common.Primitives;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Protocol
{
	/// <summary>
	/// Augments an array of blocks on a datanode with additional information about
	/// where the block is stored.
	/// </summary>
	public class HdfsBlocksMetadata
	{
		/// <summary>The block pool that was queried</summary>
		private readonly string blockPoolId;

		/// <summary>List of blocks</summary>
		private readonly long[] blockIds;

		/// <summary>List of volumes</summary>
		private readonly IList<byte[]> volumeIds;

		/// <summary>
		/// List of indexes into <code>volumeIds</code>, one per block in
		/// <code>blocks</code>.
		/// </summary>
		/// <remarks>
		/// List of indexes into <code>volumeIds</code>, one per block in
		/// <code>blocks</code>. A value of Integer.MAX_VALUE indicates that the
		/// block was not found.
		/// </remarks>
		private readonly IList<int> volumeIndexes;

		/// <summary>Constructs HdfsBlocksMetadata.</summary>
		/// <param name="blockIds">List of blocks described</param>
		/// <param name="volumeIds">
		/// List of potential volume identifiers, specifying volumes where
		/// blocks may be stored
		/// </param>
		/// <param name="volumeIndexes">Indexes into the list of volume identifiers, one per block
		/// 	</param>
		public HdfsBlocksMetadata(string blockPoolId, long[] blockIds, IList<byte[]> volumeIds
			, IList<int> volumeIndexes)
		{
			Preconditions.CheckArgument(blockIds.Length == volumeIndexes.Count, "Argument lengths should match"
				);
			this.blockPoolId = blockPoolId;
			this.blockIds = blockIds;
			this.volumeIds = volumeIds;
			this.volumeIndexes = volumeIndexes;
		}

		/// <summary>Get the array of blocks.</summary>
		/// <returns>array of blocks</returns>
		public virtual long[] GetBlockIds()
		{
			return blockIds;
		}

		/// <summary>Get the list of volume identifiers in raw byte form.</summary>
		/// <returns>list of ids</returns>
		public virtual IList<byte[]> GetVolumeIds()
		{
			return volumeIds;
		}

		/// <summary>
		/// Get a list of indexes into the array of
		/// <see cref="VolumeId"/>
		/// s, one per block.
		/// </summary>
		/// <returns>list of indexes</returns>
		public virtual IList<int> GetVolumeIndexes()
		{
			return volumeIndexes;
		}

		public override string ToString()
		{
			return "Metadata for " + blockIds.Length + " blocks in " + blockPoolId + ": " + Joiner
				.On(",").Join(Longs.AsList(blockIds));
		}
	}
}
