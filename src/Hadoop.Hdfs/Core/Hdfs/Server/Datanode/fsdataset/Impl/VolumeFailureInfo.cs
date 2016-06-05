using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset.Impl
{
	/// <summary>Tracks information about failure of a data volume.</summary>
	internal sealed class VolumeFailureInfo
	{
		private readonly string failedStorageLocation;

		private readonly long failureDate;

		private readonly long estimatedCapacityLost;

		/// <summary>
		/// Creates a new VolumeFailureInfo, when the capacity lost from this volume
		/// failure is unknown.
		/// </summary>
		/// <remarks>
		/// Creates a new VolumeFailureInfo, when the capacity lost from this volume
		/// failure is unknown.  Typically, this means the volume failed immediately at
		/// startup, so there was never a chance to query its capacity.
		/// </remarks>
		/// <param name="failedStorageLocation">storage location that has failed</param>
		/// <param name="failureDate">date/time of failure in milliseconds since epoch</param>
		public VolumeFailureInfo(string failedStorageLocation, long failureDate)
			: this(failedStorageLocation, failureDate, 0)
		{
		}

		/// <summary>Creates a new VolumeFailureInfo.</summary>
		/// <param name="failedStorageLocation">storage location that has failed</param>
		/// <param name="failureDate">date/time of failure in milliseconds since epoch</param>
		/// <param name="estimatedCapacityLost">estimate of capacity lost in bytes</param>
		public VolumeFailureInfo(string failedStorageLocation, long failureDate, long estimatedCapacityLost
			)
		{
			this.failedStorageLocation = failedStorageLocation;
			this.failureDate = failureDate;
			this.estimatedCapacityLost = estimatedCapacityLost;
		}

		/// <summary>Returns the storage location that has failed.</summary>
		/// <returns>storage location that has failed</returns>
		public string GetFailedStorageLocation()
		{
			return this.failedStorageLocation;
		}

		/// <summary>Returns date/time of failure</summary>
		/// <returns>date/time of failure in milliseconds since epoch</returns>
		public long GetFailureDate()
		{
			return this.failureDate;
		}

		/// <summary>Returns estimate of capacity lost.</summary>
		/// <remarks>
		/// Returns estimate of capacity lost.  This is said to be an estimate, because
		/// in some cases it's impossible to know the capacity of the volume, such as if
		/// we never had a chance to query its capacity before the failure occurred.
		/// </remarks>
		/// <returns>estimate of capacity lost in bytes</returns>
		public long GetEstimatedCapacityLost()
		{
			return this.estimatedCapacityLost;
		}
	}
}
