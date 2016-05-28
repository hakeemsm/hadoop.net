using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	/// <summary>
	/// Wrapper for
	/// <see cref="BlockLocation"/>
	/// that also adds
	/// <see cref="VolumeId"/>
	/// volume
	/// location information for each replica.
	/// </summary>
	public class BlockStorageLocation : BlockLocation
	{
		private readonly VolumeId[] volumeIds;

		/// <exception cref="System.IO.IOException"/>
		public BlockStorageLocation(BlockLocation loc, VolumeId[] volumeIds)
			: base(loc.GetNames(), loc.GetHosts(), loc.GetTopologyPaths(), loc.GetOffset(), loc
				.GetLength(), loc.IsCorrupt())
		{
			// Initialize with data from passed in BlockLocation
			this.volumeIds = volumeIds;
		}

		/// <summary>
		/// Gets the list of
		/// <see cref="VolumeId"/>
		/// corresponding to the block's replicas.
		/// </summary>
		/// <returns>volumeIds list of VolumeId for the block's replicas</returns>
		public virtual VolumeId[] GetVolumeIds()
		{
			return volumeIds;
		}
	}
}
