using System.Collections.Generic;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset
{
	/// <summary>This interface specifies the policy for choosing volumes to store replicas.
	/// 	</summary>
	public interface VolumeChoosingPolicy<V>
		where V : FsVolumeSpi
	{
		/// <summary>
		/// Choose a volume to place a replica,
		/// given a list of volumes and the replica size sought for storage.
		/// </summary>
		/// <remarks>
		/// Choose a volume to place a replica,
		/// given a list of volumes and the replica size sought for storage.
		/// The implementations of this interface must be thread-safe.
		/// </remarks>
		/// <param name="volumes">- a list of available volumes.</param>
		/// <param name="replicaSize">- the size of the replica for which a volume is sought.
		/// 	</param>
		/// <returns>the chosen volume.</returns>
		/// <exception cref="System.IO.IOException">when disks are unavailable or are full.</exception>
		V ChooseVolume(IList<V> volumes, long replicaSize);
	}
}
