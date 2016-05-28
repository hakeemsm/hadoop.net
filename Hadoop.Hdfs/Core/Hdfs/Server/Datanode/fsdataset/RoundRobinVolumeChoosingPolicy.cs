using System.Collections.Generic;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset
{
	/// <summary>Choose volumes in round-robin order.</summary>
	public class RoundRobinVolumeChoosingPolicy<V> : VolumeChoosingPolicy<V>
		where V : FsVolumeSpi
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(RoundRobinVolumeChoosingPolicy
			));

		private int curVolume = 0;

		/// <exception cref="System.IO.IOException"/>
		public virtual V ChooseVolume(IList<V> volumes, long blockSize)
		{
			lock (this)
			{
				if (volumes.Count < 1)
				{
					throw new DiskChecker.DiskOutOfSpaceException("No more available volumes");
				}
				// since volumes could've been removed because of the failure
				// make sure we are not out of bounds
				if (curVolume >= volumes.Count)
				{
					curVolume = 0;
				}
				int startVolume = curVolume;
				long maxAvailable = 0;
				while (true)
				{
					V volume = volumes[curVolume];
					curVolume = (curVolume + 1) % volumes.Count;
					long availableVolumeSize = volume.GetAvailable();
					if (availableVolumeSize > blockSize)
					{
						return volume;
					}
					if (availableVolumeSize > maxAvailable)
					{
						maxAvailable = availableVolumeSize;
					}
					if (curVolume == startVolume)
					{
						throw new DiskChecker.DiskOutOfSpaceException("Out of space: " + "The volume with the most available space (="
							 + maxAvailable + " B) is less than the block size (=" + blockSize + " B).");
					}
				}
			}
		}
	}
}
