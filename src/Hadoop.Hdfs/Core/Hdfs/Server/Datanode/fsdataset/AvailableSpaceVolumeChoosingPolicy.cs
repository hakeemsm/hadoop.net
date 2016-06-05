using System;
using System.Collections.Generic;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset
{
	/// <summary>
	/// A DN volume choosing policy which takes into account the amount of free
	/// space on each of the available volumes when considering where to assign a
	/// new replica allocation.
	/// </summary>
	/// <remarks>
	/// A DN volume choosing policy which takes into account the amount of free
	/// space on each of the available volumes when considering where to assign a
	/// new replica allocation. By default this policy prefers assigning replicas to
	/// those volumes with more available free space, so as to over time balance the
	/// available space of all the volumes within a DN.
	/// </remarks>
	public class AvailableSpaceVolumeChoosingPolicy<V> : VolumeChoosingPolicy<V>, Configurable
		where V : FsVolumeSpi
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset.AvailableSpaceVolumeChoosingPolicy
			));

		private readonly Random random;

		private long balancedSpaceThreshold = DFSConfigKeys.DfsDatanodeAvailableSpaceVolumeChoosingPolicyBalancedSpaceThresholdDefault;

		private float balancedPreferencePercent = DFSConfigKeys.DfsDatanodeAvailableSpaceVolumeChoosingPolicyBalancedSpacePreferenceFractionDefault;

		internal AvailableSpaceVolumeChoosingPolicy(Random random)
		{
			this.random = random;
		}

		public AvailableSpaceVolumeChoosingPolicy()
			: this(new Random())
		{
		}

		public virtual void SetConf(Configuration conf)
		{
			lock (this)
			{
				balancedSpaceThreshold = conf.GetLong(DFSConfigKeys.DfsDatanodeAvailableSpaceVolumeChoosingPolicyBalancedSpaceThresholdKey
					, DFSConfigKeys.DfsDatanodeAvailableSpaceVolumeChoosingPolicyBalancedSpaceThresholdDefault
					);
				balancedPreferencePercent = conf.GetFloat(DFSConfigKeys.DfsDatanodeAvailableSpaceVolumeChoosingPolicyBalancedSpacePreferenceFractionKey
					, DFSConfigKeys.DfsDatanodeAvailableSpaceVolumeChoosingPolicyBalancedSpacePreferenceFractionDefault
					);
				Log.Info("Available space volume choosing policy initialized: " + DFSConfigKeys.DfsDatanodeAvailableSpaceVolumeChoosingPolicyBalancedSpaceThresholdKey
					 + " = " + balancedSpaceThreshold + ", " + DFSConfigKeys.DfsDatanodeAvailableSpaceVolumeChoosingPolicyBalancedSpacePreferenceFractionKey
					 + " = " + balancedPreferencePercent);
				if (balancedPreferencePercent > 1.0)
				{
					Log.Warn("The value of " + DFSConfigKeys.DfsDatanodeAvailableSpaceVolumeChoosingPolicyBalancedSpacePreferenceFractionKey
						 + " is greater than 1.0 but should be in the range 0.0 - 1.0");
				}
				if (balancedPreferencePercent < 0.5)
				{
					Log.Warn("The value of " + DFSConfigKeys.DfsDatanodeAvailableSpaceVolumeChoosingPolicyBalancedSpacePreferenceFractionKey
						 + " is less than 0.5 so volumes with less available disk space will receive more block allocations"
						);
				}
			}
		}

		public virtual Configuration GetConf()
		{
			lock (this)
			{
				// Nothing to do. Only added to fulfill the Configurable contract.
				return null;
			}
		}

		private readonly VolumeChoosingPolicy<V> roundRobinPolicyBalanced = new RoundRobinVolumeChoosingPolicy
			<V>();

		private readonly VolumeChoosingPolicy<V> roundRobinPolicyHighAvailable = new RoundRobinVolumeChoosingPolicy
			<V>();

		private readonly VolumeChoosingPolicy<V> roundRobinPolicyLowAvailable = new RoundRobinVolumeChoosingPolicy
			<V>();

		/// <exception cref="System.IO.IOException"/>
		public virtual V ChooseVolume(IList<V> volumes, long replicaSize)
		{
			lock (this)
			{
				if (volumes.Count < 1)
				{
					throw new DiskChecker.DiskOutOfSpaceException("No more available volumes");
				}
				AvailableSpaceVolumeChoosingPolicy.AvailableSpaceVolumeList volumesWithSpaces = new 
					AvailableSpaceVolumeChoosingPolicy.AvailableSpaceVolumeList(this, volumes);
				if (volumesWithSpaces.AreAllVolumesWithinFreeSpaceThreshold())
				{
					// If they're actually not too far out of whack, fall back on pure round
					// robin.
					V volume = roundRobinPolicyBalanced.ChooseVolume(volumes, replicaSize);
					if (Log.IsDebugEnabled())
					{
						Log.Debug("All volumes are within the configured free space balance " + "threshold. Selecting "
							 + volume + " for write of block size " + replicaSize);
					}
					return volume;
				}
				else
				{
					V volume = null;
					// If none of the volumes with low free space have enough space for the
					// replica, always try to choose a volume with a lot of free space.
					long mostAvailableAmongLowVolumes = volumesWithSpaces.GetMostAvailableSpaceAmongVolumesWithLowAvailableSpace
						();
					IList<V> highAvailableVolumes = ExtractVolumesFromPairs(volumesWithSpaces.GetVolumesWithHighAvailableSpace
						());
					IList<V> lowAvailableVolumes = ExtractVolumesFromPairs(volumesWithSpaces.GetVolumesWithLowAvailableSpace
						());
					float preferencePercentScaler = (highAvailableVolumes.Count * balancedPreferencePercent
						) + (lowAvailableVolumes.Count * (1 - balancedPreferencePercent));
					float scaledPreferencePercent = (highAvailableVolumes.Count * balancedPreferencePercent
						) / preferencePercentScaler;
					if (mostAvailableAmongLowVolumes < replicaSize || random.NextFloat() < scaledPreferencePercent)
					{
						volume = roundRobinPolicyHighAvailable.ChooseVolume(highAvailableVolumes, replicaSize
							);
						if (Log.IsDebugEnabled())
						{
							Log.Debug("Volumes are imbalanced. Selecting " + volume + " from high available space volumes for write of block size "
								 + replicaSize);
						}
					}
					else
					{
						volume = roundRobinPolicyLowAvailable.ChooseVolume(lowAvailableVolumes, replicaSize
							);
						if (Log.IsDebugEnabled())
						{
							Log.Debug("Volumes are imbalanced. Selecting " + volume + " from low available space volumes for write of block size "
								 + replicaSize);
						}
					}
					return volume;
				}
			}
		}

		/// <summary>Used to keep track of the list of volumes we're choosing from.</summary>
		private class AvailableSpaceVolumeList
		{
			private readonly IList<AvailableSpaceVolumeChoosingPolicy.AvailableSpaceVolumePair
				> volumes;

			/// <exception cref="System.IO.IOException"/>
			public AvailableSpaceVolumeList(AvailableSpaceVolumeChoosingPolicy<V> _enclosing, 
				IList<V> volumes)
			{
				this._enclosing = _enclosing;
				this.volumes = new AList<AvailableSpaceVolumeChoosingPolicy.AvailableSpaceVolumePair
					>();
				foreach (V volume in volumes)
				{
					this.volumes.AddItem(new AvailableSpaceVolumeChoosingPolicy.AvailableSpaceVolumePair
						(this, volume));
				}
			}

			/// <returns>
			/// true if all volumes' free space is within the
			/// configured threshold, false otherwise.
			/// </returns>
			public virtual bool AreAllVolumesWithinFreeSpaceThreshold()
			{
				long leastAvailable = long.MaxValue;
				long mostAvailable = 0;
				foreach (AvailableSpaceVolumeChoosingPolicy.AvailableSpaceVolumePair volume in this
					.volumes)
				{
					leastAvailable = Math.Min(leastAvailable, volume.GetAvailable());
					mostAvailable = Math.Max(mostAvailable, volume.GetAvailable());
				}
				return (mostAvailable - leastAvailable) < this._enclosing.balancedSpaceThreshold;
			}

			/// <returns>
			/// the minimum amount of space available on a single volume,
			/// across all volumes.
			/// </returns>
			private long GetLeastAvailableSpace()
			{
				long leastAvailable = long.MaxValue;
				foreach (AvailableSpaceVolumeChoosingPolicy.AvailableSpaceVolumePair volume in this
					.volumes)
				{
					leastAvailable = Math.Min(leastAvailable, volume.GetAvailable());
				}
				return leastAvailable;
			}

			/// <returns>the maximum amount of space available across volumes with low space.</returns>
			public virtual long GetMostAvailableSpaceAmongVolumesWithLowAvailableSpace()
			{
				long mostAvailable = long.MinValue;
				foreach (AvailableSpaceVolumeChoosingPolicy.AvailableSpaceVolumePair volume in this
					.GetVolumesWithLowAvailableSpace())
				{
					mostAvailable = Math.Max(mostAvailable, volume.GetAvailable());
				}
				return mostAvailable;
			}

			/// <returns>the list of volumes with relatively low available space.</returns>
			public virtual IList<AvailableSpaceVolumeChoosingPolicy.AvailableSpaceVolumePair>
				 GetVolumesWithLowAvailableSpace()
			{
				long leastAvailable = this.GetLeastAvailableSpace();
				IList<AvailableSpaceVolumeChoosingPolicy.AvailableSpaceVolumePair> ret = new AList
					<AvailableSpaceVolumeChoosingPolicy.AvailableSpaceVolumePair>();
				foreach (AvailableSpaceVolumeChoosingPolicy.AvailableSpaceVolumePair volume in this
					.volumes)
				{
					if (volume.GetAvailable() <= leastAvailable + this._enclosing.balancedSpaceThreshold)
					{
						ret.AddItem(volume);
					}
				}
				return ret;
			}

			/// <returns>the list of volumes with a lot of available space.</returns>
			public virtual IList<AvailableSpaceVolumeChoosingPolicy.AvailableSpaceVolumePair>
				 GetVolumesWithHighAvailableSpace()
			{
				long leastAvailable = this.GetLeastAvailableSpace();
				IList<AvailableSpaceVolumeChoosingPolicy.AvailableSpaceVolumePair> ret = new AList
					<AvailableSpaceVolumeChoosingPolicy.AvailableSpaceVolumePair>();
				foreach (AvailableSpaceVolumeChoosingPolicy.AvailableSpaceVolumePair volume in this
					.volumes)
				{
					if (volume.GetAvailable() > leastAvailable + this._enclosing.balancedSpaceThreshold)
					{
						ret.AddItem(volume);
					}
				}
				return ret;
			}

			private readonly AvailableSpaceVolumeChoosingPolicy<V> _enclosing;
		}

		/// <summary>
		/// Used so that we only check the available space on a given volume once, at
		/// the beginning of
		/// <see cref="AvailableSpaceVolumeChoosingPolicy{V}.ChooseVolume(System.Collections.IList{E}, long)
		/// 	"/>
		/// .
		/// </summary>
		private class AvailableSpaceVolumePair
		{
			private readonly V volume;

			private readonly long availableSpace;

			/// <exception cref="System.IO.IOException"/>
			public AvailableSpaceVolumePair(AvailableSpaceVolumeChoosingPolicy<V> _enclosing, 
				V volume)
			{
				this._enclosing = _enclosing;
				this.volume = volume;
				this.availableSpace = volume.GetAvailable();
			}

			public virtual long GetAvailable()
			{
				return this.availableSpace;
			}

			public virtual V GetVolume()
			{
				return this.volume;
			}

			private readonly AvailableSpaceVolumeChoosingPolicy<V> _enclosing;
		}

		private IList<V> ExtractVolumesFromPairs(IList<AvailableSpaceVolumeChoosingPolicy.AvailableSpaceVolumePair
			> volumes)
		{
			IList<V> ret = new AList<V>();
			foreach (AvailableSpaceVolumeChoosingPolicy.AvailableSpaceVolumePair volume in volumes)
			{
				ret.AddItem(volume.GetVolume());
			}
			return ret;
		}
	}
}
