using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Protocol
{
	/// <summary>Summarizes information about data volume failures on a DataNode.</summary>
	public class VolumeFailureSummary
	{
		private readonly string[] failedStorageLocations;

		private readonly long lastVolumeFailureDate;

		private readonly long estimatedCapacityLostTotal;

		/// <summary>Creates a new VolumeFailureSummary.</summary>
		/// <param name="failedStorageLocations">storage locations that have failed</param>
		/// <param name="lastVolumeFailureDate">
		/// date/time of last volume failure in
		/// milliseconds since epoch
		/// </param>
		/// <param name="estimatedCapacityLostTotal">estimate of capacity lost in bytes</param>
		public VolumeFailureSummary(string[] failedStorageLocations, long lastVolumeFailureDate
			, long estimatedCapacityLostTotal)
		{
			this.failedStorageLocations = failedStorageLocations;
			this.lastVolumeFailureDate = lastVolumeFailureDate;
			this.estimatedCapacityLostTotal = estimatedCapacityLostTotal;
		}

		/// <summary>Returns each storage location that has failed, sorted.</summary>
		/// <returns>each storage location that has failed, sorted</returns>
		public virtual string[] GetFailedStorageLocations()
		{
			return this.failedStorageLocations;
		}

		/// <summary>
		/// Returns the date/time of the last volume failure in milliseconds since
		/// epoch.
		/// </summary>
		/// <returns>date/time of last volume failure in milliseconds since epoch</returns>
		public virtual long GetLastVolumeFailureDate()
		{
			return this.lastVolumeFailureDate;
		}

		/// <summary>Returns estimate of capacity lost.</summary>
		/// <remarks>
		/// Returns estimate of capacity lost.  This is said to be an estimate, because
		/// in some cases it's impossible to know the capacity of the volume, such as if
		/// we never had a chance to query its capacity before the failure occurred.
		/// </remarks>
		/// <returns>estimate of capacity lost in bytes</returns>
		public virtual long GetEstimatedCapacityLostTotal()
		{
			return this.estimatedCapacityLostTotal;
		}
	}
}
