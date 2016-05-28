using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>Status information on the current state of the Map-Reduce cluster.</summary>
	/// <remarks>
	/// Status information on the current state of the Map-Reduce cluster.
	/// <p><code>ClusterStatus</code> provides clients with information such as:
	/// <ol>
	/// <li>
	/// Size of the cluster.
	/// </li>
	/// <li>
	/// Name of the trackers.
	/// </li>
	/// <li>
	/// Task capacity of the cluster.
	/// </li>
	/// <li>
	/// The number of currently running map and reduce tasks.
	/// </li>
	/// <li>
	/// State of the <code>JobTracker</code>.
	/// </li>
	/// <li>
	/// Details regarding black listed trackers.
	/// </li>
	/// </ol>
	/// <p>Clients can query for the latest <code>ClusterStatus</code>, via
	/// <see cref="JobClient.GetClusterStatus()"/>
	/// .</p>
	/// </remarks>
	/// <seealso cref="JobClient"/>
	public class ClusterStatus : Writable
	{
		/// <summary>Class which encapsulates information about a blacklisted tasktracker.</summary>
		/// <remarks>
		/// Class which encapsulates information about a blacklisted tasktracker.
		/// The information includes the tasktracker's name and reasons for
		/// getting blacklisted. The toString method of the class will print
		/// the information in a whitespace separated fashion to enable parsing.
		/// </remarks>
		public class BlackListInfo : Writable
		{
			private string trackerName;

			private string reasonForBlackListing;

			private string blackListReport;

			internal BlackListInfo()
			{
			}

			/// <summary>Gets the blacklisted tasktracker's name.</summary>
			/// <returns>tracker's name.</returns>
			public virtual string GetTrackerName()
			{
				return trackerName;
			}

			/// <summary>Gets the reason for which the tasktracker was blacklisted.</summary>
			/// <returns>reason which tracker was blacklisted</returns>
			public virtual string GetReasonForBlackListing()
			{
				return reasonForBlackListing;
			}

			/// <summary>Sets the blacklisted tasktracker's name.</summary>
			/// <param name="trackerName">of the tracker.</param>
			internal virtual void SetTrackerName(string trackerName)
			{
				this.trackerName = trackerName;
			}

			/// <summary>Sets the reason for which the tasktracker was blacklisted.</summary>
			/// <param name="reasonForBlackListing"/>
			internal virtual void SetReasonForBlackListing(string reasonForBlackListing)
			{
				this.reasonForBlackListing = reasonForBlackListing;
			}

			/// <summary>Gets a descriptive report about why the tasktracker was blacklisted.</summary>
			/// <returns>report describing why the tasktracker was blacklisted.</returns>
			public virtual string GetBlackListReport()
			{
				return blackListReport;
			}

			/// <summary>Sets a descriptive report about why the tasktracker was blacklisted.</summary>
			/// <param name="blackListReport">
			/// report describing why the tasktracker
			/// was blacklisted.
			/// </param>
			internal virtual void SetBlackListReport(string blackListReport)
			{
				this.blackListReport = blackListReport;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void ReadFields(DataInput @in)
			{
				trackerName = StringInterner.WeakIntern(Text.ReadString(@in));
				reasonForBlackListing = StringInterner.WeakIntern(Text.ReadString(@in));
				blackListReport = StringInterner.WeakIntern(Text.ReadString(@in));
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Write(DataOutput @out)
			{
				Text.WriteString(@out, trackerName);
				Text.WriteString(@out, reasonForBlackListing);
				Text.WriteString(@out, blackListReport);
			}

			public override string ToString()
			{
				StringBuilder sb = new StringBuilder();
				sb.Append(trackerName);
				sb.Append("\t");
				sb.Append(reasonForBlackListing);
				sb.Append("\t");
				sb.Append(blackListReport.Replace("\n", ":"));
				return sb.ToString();
			}
		}

		public const long UninitializedMemoryValue = -1;

		private int numActiveTrackers;

		private ICollection<string> activeTrackers = new AList<string>();

		private int numBlacklistedTrackers;

		private int numExcludedNodes;

		private long ttExpiryInterval;

		private int map_tasks;

		private int reduce_tasks;

		private int max_map_tasks;

		private int max_reduce_tasks;

		private Cluster.JobTrackerStatus status;

		private ICollection<ClusterStatus.BlackListInfo> blacklistedTrackersInfo = new AList
			<ClusterStatus.BlackListInfo>();

		private int grayListedTrackers;

		internal ClusterStatus()
		{
		}

		/// <summary>Construct a new cluster status.</summary>
		/// <param name="trackers">no. of tasktrackers in the cluster</param>
		/// <param name="blacklists">no of blacklisted task trackers in the cluster</param>
		/// <param name="ttExpiryInterval">the tasktracker expiry interval</param>
		/// <param name="maps">no. of currently running map-tasks in the cluster</param>
		/// <param name="reduces">no. of currently running reduce-tasks in the cluster</param>
		/// <param name="maxMaps">the maximum no. of map tasks in the cluster</param>
		/// <param name="maxReduces">the maximum no. of reduce tasks in the cluster</param>
		/// <param name="status">
		/// the
		/// <see cref="Org.Apache.Hadoop.Mapreduce.Cluster.JobTrackerStatus"/>
		/// of the <code>JobTracker</code>
		/// </param>
		internal ClusterStatus(int trackers, int blacklists, long ttExpiryInterval, int maps
			, int reduces, int maxMaps, int maxReduces, Cluster.JobTrackerStatus status)
			: this(trackers, blacklists, ttExpiryInterval, maps, reduces, maxMaps, maxReduces
				, status, 0)
		{
		}

		/// <summary>Construct a new cluster status.</summary>
		/// <param name="trackers">no. of tasktrackers in the cluster</param>
		/// <param name="blacklists">no of blacklisted task trackers in the cluster</param>
		/// <param name="ttExpiryInterval">the tasktracker expiry interval</param>
		/// <param name="maps">no. of currently running map-tasks in the cluster</param>
		/// <param name="reduces">no. of currently running reduce-tasks in the cluster</param>
		/// <param name="maxMaps">the maximum no. of map tasks in the cluster</param>
		/// <param name="maxReduces">the maximum no. of reduce tasks in the cluster</param>
		/// <param name="status">
		/// the
		/// <see cref="Org.Apache.Hadoop.Mapreduce.Cluster.JobTrackerStatus"/>
		/// of the <code>JobTracker</code>
		/// </param>
		/// <param name="numDecommissionedNodes">number of decommission trackers</param>
		internal ClusterStatus(int trackers, int blacklists, long ttExpiryInterval, int maps
			, int reduces, int maxMaps, int maxReduces, Cluster.JobTrackerStatus status, int
			 numDecommissionedNodes)
			: this(trackers, blacklists, ttExpiryInterval, maps, reduces, maxMaps, maxReduces
				, status, numDecommissionedNodes, 0)
		{
		}

		/// <summary>Construct a new cluster status.</summary>
		/// <param name="trackers">no. of tasktrackers in the cluster</param>
		/// <param name="blacklists">no of blacklisted task trackers in the cluster</param>
		/// <param name="ttExpiryInterval">the tasktracker expiry interval</param>
		/// <param name="maps">no. of currently running map-tasks in the cluster</param>
		/// <param name="reduces">no. of currently running reduce-tasks in the cluster</param>
		/// <param name="maxMaps">the maximum no. of map tasks in the cluster</param>
		/// <param name="maxReduces">the maximum no. of reduce tasks in the cluster</param>
		/// <param name="status">
		/// the
		/// <see cref="Org.Apache.Hadoop.Mapreduce.Cluster.JobTrackerStatus"/>
		/// of the <code>JobTracker</code>
		/// </param>
		/// <param name="numDecommissionedNodes">number of decommission trackers</param>
		/// <param name="numGrayListedTrackers">number of graylisted trackers</param>
		internal ClusterStatus(int trackers, int blacklists, long ttExpiryInterval, int maps
			, int reduces, int maxMaps, int maxReduces, Cluster.JobTrackerStatus status, int
			 numDecommissionedNodes, int numGrayListedTrackers)
		{
			numActiveTrackers = trackers;
			numBlacklistedTrackers = blacklists;
			this.numExcludedNodes = numDecommissionedNodes;
			this.ttExpiryInterval = ttExpiryInterval;
			map_tasks = maps;
			reduce_tasks = reduces;
			max_map_tasks = maxMaps;
			max_reduce_tasks = maxReduces;
			this.status = status;
			this.grayListedTrackers = numGrayListedTrackers;
		}

		/// <summary>Construct a new cluster status.</summary>
		/// <param name="activeTrackers">active tasktrackers in the cluster</param>
		/// <param name="blacklistedTrackers">blacklisted tasktrackers in the cluster</param>
		/// <param name="ttExpiryInterval">the tasktracker expiry interval</param>
		/// <param name="maps">no. of currently running map-tasks in the cluster</param>
		/// <param name="reduces">no. of currently running reduce-tasks in the cluster</param>
		/// <param name="maxMaps">the maximum no. of map tasks in the cluster</param>
		/// <param name="maxReduces">the maximum no. of reduce tasks in the cluster</param>
		/// <param name="status">
		/// the
		/// <see cref="Org.Apache.Hadoop.Mapreduce.Cluster.JobTrackerStatus"/>
		/// of the <code>JobTracker</code>
		/// </param>
		internal ClusterStatus(ICollection<string> activeTrackers, ICollection<ClusterStatus.BlackListInfo
			> blacklistedTrackers, long ttExpiryInterval, int maps, int reduces, int maxMaps
			, int maxReduces, Cluster.JobTrackerStatus status)
			: this(activeTrackers, blacklistedTrackers, ttExpiryInterval, maps, reduces, maxMaps
				, maxReduces, status, 0)
		{
		}

		/// <summary>Construct a new cluster status.</summary>
		/// <param name="activeTrackers">active tasktrackers in the cluster</param>
		/// <param name="blackListedTrackerInfo">
		/// blacklisted tasktrackers information
		/// in the cluster
		/// </param>
		/// <param name="ttExpiryInterval">the tasktracker expiry interval</param>
		/// <param name="maps">no. of currently running map-tasks in the cluster</param>
		/// <param name="reduces">no. of currently running reduce-tasks in the cluster</param>
		/// <param name="maxMaps">the maximum no. of map tasks in the cluster</param>
		/// <param name="maxReduces">the maximum no. of reduce tasks in the cluster</param>
		/// <param name="status">
		/// the
		/// <see cref="Org.Apache.Hadoop.Mapreduce.Cluster.JobTrackerStatus"/>
		/// of the <code>JobTracker</code>
		/// </param>
		/// <param name="numDecommissionNodes">number of decommission trackers</param>
		internal ClusterStatus(ICollection<string> activeTrackers, ICollection<ClusterStatus.BlackListInfo
			> blackListedTrackerInfo, long ttExpiryInterval, int maps, int reduces, int maxMaps
			, int maxReduces, Cluster.JobTrackerStatus status, int numDecommissionNodes)
			: this(activeTrackers.Count, blackListedTrackerInfo.Count, ttExpiryInterval, maps
				, reduces, maxMaps, maxReduces, status, numDecommissionNodes)
		{
			this.activeTrackers = activeTrackers;
			this.blacklistedTrackersInfo = blackListedTrackerInfo;
		}

		/// <summary>Get the number of task trackers in the cluster.</summary>
		/// <returns>the number of task trackers in the cluster.</returns>
		public virtual int GetTaskTrackers()
		{
			return numActiveTrackers;
		}

		/// <summary>Get the names of task trackers in the cluster.</summary>
		/// <returns>the active task trackers in the cluster.</returns>
		public virtual ICollection<string> GetActiveTrackerNames()
		{
			return activeTrackers;
		}

		/// <summary>Get the names of task trackers in the cluster.</summary>
		/// <returns>the blacklisted task trackers in the cluster.</returns>
		public virtual ICollection<string> GetBlacklistedTrackerNames()
		{
			AList<string> blacklistedTrackers = new AList<string>();
			foreach (ClusterStatus.BlackListInfo bi in blacklistedTrackersInfo)
			{
				blacklistedTrackers.AddItem(bi.GetTrackerName());
			}
			return blacklistedTrackers;
		}

		/// <summary>Get the names of graylisted task trackers in the cluster.</summary>
		/// <remarks>
		/// Get the names of graylisted task trackers in the cluster.
		/// The gray list of trackers is no longer available on M/R 2.x. The function
		/// is kept to be compatible with M/R 1.x applications.
		/// </remarks>
		/// <returns>an empty graylisted task trackers in the cluster.</returns>
		[Obsolete]
		public virtual ICollection<string> GetGraylistedTrackerNames()
		{
			return Sharpen.Collections.EmptySet();
		}

		/// <summary>Get the number of graylisted task trackers in the cluster.</summary>
		/// <remarks>
		/// Get the number of graylisted task trackers in the cluster.
		/// The gray list of trackers is no longer available on M/R 2.x. The function
		/// is kept to be compatible with M/R 1.x applications.
		/// </remarks>
		/// <returns>0 graylisted task trackers in the cluster.</returns>
		[Obsolete]
		public virtual int GetGraylistedTrackers()
		{
			return grayListedTrackers;
		}

		/// <summary>Get the number of blacklisted task trackers in the cluster.</summary>
		/// <returns>the number of blacklisted task trackers in the cluster.</returns>
		public virtual int GetBlacklistedTrackers()
		{
			return numBlacklistedTrackers;
		}

		/// <summary>Get the number of excluded hosts in the cluster.</summary>
		/// <returns>the number of excluded hosts in the cluster.</returns>
		public virtual int GetNumExcludedNodes()
		{
			return numExcludedNodes;
		}

		/// <summary>Get the tasktracker expiry interval for the cluster</summary>
		/// <returns>the expiry interval in msec</returns>
		public virtual long GetTTExpiryInterval()
		{
			return ttExpiryInterval;
		}

		/// <summary>Get the number of currently running map tasks in the cluster.</summary>
		/// <returns>the number of currently running map tasks in the cluster.</returns>
		public virtual int GetMapTasks()
		{
			return map_tasks;
		}

		/// <summary>Get the number of currently running reduce tasks in the cluster.</summary>
		/// <returns>the number of currently running reduce tasks in the cluster.</returns>
		public virtual int GetReduceTasks()
		{
			return reduce_tasks;
		}

		/// <summary>Get the maximum capacity for running map tasks in the cluster.</summary>
		/// <returns>the maximum capacity for running map tasks in the cluster.</returns>
		public virtual int GetMaxMapTasks()
		{
			return max_map_tasks;
		}

		/// <summary>Get the maximum capacity for running reduce tasks in the cluster.</summary>
		/// <returns>the maximum capacity for running reduce tasks in the cluster.</returns>
		public virtual int GetMaxReduceTasks()
		{
			return max_reduce_tasks;
		}

		/// <summary>Get the JobTracker's status.</summary>
		/// <returns>
		/// 
		/// <see cref="Org.Apache.Hadoop.Mapreduce.Cluster.JobTrackerStatus"/>
		/// of the JobTracker
		/// </returns>
		public virtual Cluster.JobTrackerStatus GetJobTrackerStatus()
		{
			return status;
		}

		/// <summary>Returns UNINITIALIZED_MEMORY_VALUE (-1)</summary>
		[Obsolete]
		public virtual long GetMaxMemory()
		{
			return UninitializedMemoryValue;
		}

		/// <summary>Returns UNINITIALIZED_MEMORY_VALUE (-1)</summary>
		[Obsolete]
		public virtual long GetUsedMemory()
		{
			return UninitializedMemoryValue;
		}

		/// <summary>Gets the list of blacklisted trackers along with reasons for blacklisting.
		/// 	</summary>
		/// <returns>
		/// the collection of
		/// <see cref="BlackListInfo"/>
		/// objects.
		/// </returns>
		public virtual ICollection<ClusterStatus.BlackListInfo> GetBlackListedTrackersInfo
			()
		{
			return blacklistedTrackersInfo;
		}

		/// <summary>
		/// Get the current state of the <code>JobTracker</code>,
		/// as
		/// <see cref="State"/>
		/// <see cref="State"/>
		/// should no longer be used on M/R 2.x. The function
		/// is kept to be compatible with M/R 1.x applications.
		/// </summary>
		/// <returns>the invalid state of the <code>JobTracker</code>.</returns>
		[Obsolete]
		public virtual JobTracker.State GetJobTrackerState()
		{
			return JobTracker.State.Running;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Write(DataOutput @out)
		{
			if (activeTrackers.Count == 0)
			{
				@out.WriteInt(numActiveTrackers);
				@out.WriteInt(0);
			}
			else
			{
				@out.WriteInt(activeTrackers.Count);
				@out.WriteInt(activeTrackers.Count);
				foreach (string tracker in activeTrackers)
				{
					Org.Apache.Hadoop.IO.Text.WriteString(@out, tracker);
				}
			}
			if (blacklistedTrackersInfo.Count == 0)
			{
				@out.WriteInt(numBlacklistedTrackers);
				@out.WriteInt(blacklistedTrackersInfo.Count);
			}
			else
			{
				@out.WriteInt(blacklistedTrackersInfo.Count);
				@out.WriteInt(blacklistedTrackersInfo.Count);
				foreach (ClusterStatus.BlackListInfo tracker in blacklistedTrackersInfo)
				{
					tracker.Write(@out);
				}
			}
			@out.WriteInt(numExcludedNodes);
			@out.WriteLong(ttExpiryInterval);
			@out.WriteInt(map_tasks);
			@out.WriteInt(reduce_tasks);
			@out.WriteInt(max_map_tasks);
			@out.WriteInt(max_reduce_tasks);
			WritableUtils.WriteEnum(@out, status);
			@out.WriteInt(grayListedTrackers);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadFields(DataInput @in)
		{
			numActiveTrackers = @in.ReadInt();
			int numTrackerNames = @in.ReadInt();
			if (numTrackerNames > 0)
			{
				for (int i = 0; i < numTrackerNames; i++)
				{
					string name = StringInterner.WeakIntern(Org.Apache.Hadoop.IO.Text.ReadString(@in)
						);
					activeTrackers.AddItem(name);
				}
			}
			numBlacklistedTrackers = @in.ReadInt();
			int blackListTrackerInfoSize = @in.ReadInt();
			if (blackListTrackerInfoSize > 0)
			{
				for (int i = 0; i < blackListTrackerInfoSize; i++)
				{
					ClusterStatus.BlackListInfo info = new ClusterStatus.BlackListInfo();
					info.ReadFields(@in);
					blacklistedTrackersInfo.AddItem(info);
				}
			}
			numExcludedNodes = @in.ReadInt();
			ttExpiryInterval = @in.ReadLong();
			map_tasks = @in.ReadInt();
			reduce_tasks = @in.ReadInt();
			max_map_tasks = @in.ReadInt();
			max_reduce_tasks = @in.ReadInt();
			status = WritableUtils.ReadEnum<Cluster.JobTrackerStatus>(@in);
			grayListedTrackers = @in.ReadInt();
		}
	}
}
