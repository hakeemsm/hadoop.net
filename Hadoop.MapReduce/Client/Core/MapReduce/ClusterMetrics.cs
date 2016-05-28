using System.IO;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce
{
	/// <summary>Status information on the current state of the Map-Reduce cluster.</summary>
	/// <remarks>
	/// Status information on the current state of the Map-Reduce cluster.
	/// <p><code>ClusterMetrics</code> provides clients with information such as:
	/// <ol>
	/// <li>
	/// Size of the cluster.
	/// </li>
	/// <li>
	/// Number of blacklisted and decommissioned trackers.
	/// </li>
	/// <li>
	/// Slot capacity of the cluster.
	/// </li>
	/// <li>
	/// The number of currently occupied/reserved map and reduce slots.
	/// </li>
	/// <li>
	/// The number of currently running map and reduce tasks.
	/// </li>
	/// <li>
	/// The number of job submissions.
	/// </li>
	/// </ol>
	/// <p>Clients can query for the latest <code>ClusterMetrics</code>, via
	/// <see cref="Cluster.GetClusterStatus()"/>
	/// .</p>
	/// </remarks>
	/// <seealso cref="Cluster"/>
	public class ClusterMetrics : Writable
	{
		private int runningMaps;

		private int runningReduces;

		private int occupiedMapSlots;

		private int occupiedReduceSlots;

		private int reservedMapSlots;

		private int reservedReduceSlots;

		private int totalMapSlots;

		private int totalReduceSlots;

		private int totalJobSubmissions;

		private int numTrackers;

		private int numBlacklistedTrackers;

		private int numGraylistedTrackers;

		private int numDecommissionedTrackers;

		public ClusterMetrics()
		{
		}

		public ClusterMetrics(int runningMaps, int runningReduces, int occupiedMapSlots, 
			int occupiedReduceSlots, int reservedMapSlots, int reservedReduceSlots, int mapSlots
			, int reduceSlots, int totalJobSubmissions, int numTrackers, int numBlacklistedTrackers
			, int numDecommissionedNodes)
			: this(runningMaps, runningReduces, occupiedMapSlots, occupiedReduceSlots, reservedMapSlots
				, reservedReduceSlots, mapSlots, reduceSlots, totalJobSubmissions, numTrackers, 
				numBlacklistedTrackers, 0, numDecommissionedNodes)
		{
		}

		public ClusterMetrics(int runningMaps, int runningReduces, int occupiedMapSlots, 
			int occupiedReduceSlots, int reservedMapSlots, int reservedReduceSlots, int mapSlots
			, int reduceSlots, int totalJobSubmissions, int numTrackers, int numBlacklistedTrackers
			, int numGraylistedTrackers, int numDecommissionedNodes)
		{
			this.runningMaps = runningMaps;
			this.runningReduces = runningReduces;
			this.occupiedMapSlots = occupiedMapSlots;
			this.occupiedReduceSlots = occupiedReduceSlots;
			this.reservedMapSlots = reservedMapSlots;
			this.reservedReduceSlots = reservedReduceSlots;
			this.totalMapSlots = mapSlots;
			this.totalReduceSlots = reduceSlots;
			this.totalJobSubmissions = totalJobSubmissions;
			this.numTrackers = numTrackers;
			this.numBlacklistedTrackers = numBlacklistedTrackers;
			this.numGraylistedTrackers = numGraylistedTrackers;
			this.numDecommissionedTrackers = numDecommissionedNodes;
		}

		/// <summary>Get the number of running map tasks in the cluster.</summary>
		/// <returns>running maps</returns>
		public virtual int GetRunningMaps()
		{
			return runningMaps;
		}

		/// <summary>Get the number of running reduce tasks in the cluster.</summary>
		/// <returns>running reduces</returns>
		public virtual int GetRunningReduces()
		{
			return runningReduces;
		}

		/// <summary>Get number of occupied map slots in the cluster.</summary>
		/// <returns>occupied map slot count</returns>
		public virtual int GetOccupiedMapSlots()
		{
			return occupiedMapSlots;
		}

		/// <summary>Get the number of occupied reduce slots in the cluster.</summary>
		/// <returns>occupied reduce slot count</returns>
		public virtual int GetOccupiedReduceSlots()
		{
			return occupiedReduceSlots;
		}

		/// <summary>Get number of reserved map slots in the cluster.</summary>
		/// <returns>reserved map slot count</returns>
		public virtual int GetReservedMapSlots()
		{
			return reservedMapSlots;
		}

		/// <summary>Get the number of reserved reduce slots in the cluster.</summary>
		/// <returns>reserved reduce slot count</returns>
		public virtual int GetReservedReduceSlots()
		{
			return reservedReduceSlots;
		}

		/// <summary>Get the total number of map slots in the cluster.</summary>
		/// <returns>map slot capacity</returns>
		public virtual int GetMapSlotCapacity()
		{
			return totalMapSlots;
		}

		/// <summary>Get the total number of reduce slots in the cluster.</summary>
		/// <returns>reduce slot capacity</returns>
		public virtual int GetReduceSlotCapacity()
		{
			return totalReduceSlots;
		}

		/// <summary>Get the total number of job submissions in the cluster.</summary>
		/// <returns>total number of job submissions</returns>
		public virtual int GetTotalJobSubmissions()
		{
			return totalJobSubmissions;
		}

		/// <summary>Get the number of active trackers in the cluster.</summary>
		/// <returns>active tracker count.</returns>
		public virtual int GetTaskTrackerCount()
		{
			return numTrackers;
		}

		/// <summary>Get the number of blacklisted trackers in the cluster.</summary>
		/// <returns>blacklisted tracker count</returns>
		public virtual int GetBlackListedTaskTrackerCount()
		{
			return numBlacklistedTrackers;
		}

		/// <summary>Get the number of graylisted trackers in the cluster.</summary>
		/// <returns>graylisted tracker count</returns>
		public virtual int GetGrayListedTaskTrackerCount()
		{
			return numGraylistedTrackers;
		}

		/// <summary>Get the number of decommissioned trackers in the cluster.</summary>
		/// <returns>decommissioned tracker count</returns>
		public virtual int GetDecommissionedTaskTrackerCount()
		{
			return numDecommissionedTrackers;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadFields(DataInput @in)
		{
			runningMaps = @in.ReadInt();
			runningReduces = @in.ReadInt();
			occupiedMapSlots = @in.ReadInt();
			occupiedReduceSlots = @in.ReadInt();
			reservedMapSlots = @in.ReadInt();
			reservedReduceSlots = @in.ReadInt();
			totalMapSlots = @in.ReadInt();
			totalReduceSlots = @in.ReadInt();
			totalJobSubmissions = @in.ReadInt();
			numTrackers = @in.ReadInt();
			numBlacklistedTrackers = @in.ReadInt();
			numGraylistedTrackers = @in.ReadInt();
			numDecommissionedTrackers = @in.ReadInt();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Write(DataOutput @out)
		{
			@out.WriteInt(runningMaps);
			@out.WriteInt(runningReduces);
			@out.WriteInt(occupiedMapSlots);
			@out.WriteInt(occupiedReduceSlots);
			@out.WriteInt(reservedMapSlots);
			@out.WriteInt(reservedReduceSlots);
			@out.WriteInt(totalMapSlots);
			@out.WriteInt(totalReduceSlots);
			@out.WriteInt(totalJobSubmissions);
			@out.WriteInt(numTrackers);
			@out.WriteInt(numBlacklistedTrackers);
			@out.WriteInt(numGraylistedTrackers);
			@out.WriteInt(numDecommissionedTrackers);
		}
	}
}
