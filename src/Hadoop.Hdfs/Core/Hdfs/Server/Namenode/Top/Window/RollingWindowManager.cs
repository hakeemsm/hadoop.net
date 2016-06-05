using System.Collections.Generic;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.Top.Window
{
	/// <summary>
	/// A class to manage the set of
	/// <see cref="RollingWindow"/>
	/// s. This class is the
	/// interface of metrics system to the
	/// <see cref="RollingWindow"/>
	/// s to retrieve the
	/// current top metrics.
	/// <p/>
	/// Thread-safety is provided by each
	/// <see cref="RollingWindow"/>
	/// being thread-safe as
	/// well as
	/// <see cref="Sharpen.ConcurrentHashMap{K, V}"/>
	/// for the collection of them.
	/// </summary>
	public class RollingWindowManager
	{
		public static readonly Logger Log = LoggerFactory.GetLogger(typeof(Org.Apache.Hadoop.Hdfs.Server.Namenode.Top.Window.RollingWindowManager
			));

		private readonly int windowLenMs;

		private readonly int bucketsPerWindow;

		private readonly int topUsersCnt;

		[System.Serializable]
		private class RollingWindowMap : ConcurrentHashMap<string, RollingWindow>
		{
			private const long serialVersionUID = -6785807073237052051L;
			// e.g., 10 buckets per minute
			// e.g., report top 10 metrics
		}

		/// <summary>Represents a snapshot of the rolling window.</summary>
		/// <remarks>
		/// Represents a snapshot of the rolling window. It contains one Op per
		/// operation in the window, with ranked users for each Op.
		/// </remarks>
		public class TopWindow
		{
			private readonly int windowMillis;

			private readonly IList<RollingWindowManager.OP> top;

			public TopWindow(int windowMillis)
			{
				this.windowMillis = windowMillis;
				this.top = Lists.NewArrayList();
			}

			public virtual void AddOp(RollingWindowManager.OP op)
			{
				top.AddItem(op);
			}

			public virtual int GetWindowLenMs()
			{
				return windowMillis;
			}

			public virtual IList<RollingWindowManager.OP> GetOps()
			{
				return top;
			}
		}

		/// <summary>Represents an operation within a TopWindow.</summary>
		/// <remarks>
		/// Represents an operation within a TopWindow. It contains a ranked
		/// set of the top users for the operation.
		/// </remarks>
		public class OP
		{
			private readonly string opType;

			private readonly IList<RollingWindowManager.User> topUsers;

			private readonly long totalCount;

			public OP(string opType, long totalCount)
			{
				this.opType = opType;
				this.topUsers = Lists.NewArrayList();
				this.totalCount = totalCount;
			}

			public virtual void AddUser(RollingWindowManager.User u)
			{
				topUsers.AddItem(u);
			}

			public virtual string GetOpType()
			{
				return opType;
			}

			public virtual IList<RollingWindowManager.User> GetTopUsers()
			{
				return topUsers;
			}

			public virtual long GetTotalCount()
			{
				return totalCount;
			}
		}

		/// <summary>Represents a user who called an Op within a TopWindow.</summary>
		/// <remarks>
		/// Represents a user who called an Op within a TopWindow. Specifies the
		/// user and the number of times the user called the operation.
		/// </remarks>
		public class User
		{
			private readonly string user;

			private readonly long count;

			public User(string user, long count)
			{
				this.user = user;
				this.count = count;
			}

			public virtual string GetUser()
			{
				return user;
			}

			public virtual long GetCount()
			{
				return count;
			}
		}

		/// <summary>
		/// A mapping from each reported metric to its
		/// <see cref="RollingWindowMap"/>
		/// that
		/// maintains the set of
		/// <see cref="RollingWindow"/>
		/// s for the users that have
		/// operated on that metric.
		/// </summary>
		public ConcurrentHashMap<string, RollingWindowManager.RollingWindowMap> metricMap
			 = new ConcurrentHashMap<string, RollingWindowManager.RollingWindowMap>();

		public RollingWindowManager(Configuration conf, int reportingPeriodMs)
		{
			windowLenMs = reportingPeriodMs;
			bucketsPerWindow = conf.GetInt(DFSConfigKeys.NntopBucketsPerWindowKey, DFSConfigKeys
				.NntopBucketsPerWindowDefault);
			Preconditions.CheckArgument(bucketsPerWindow > 0, "a window should have at least one bucket"
				);
			Preconditions.CheckArgument(bucketsPerWindow <= windowLenMs, "the minimum size of a bucket is 1 ms"
				);
			//same-size buckets
			Preconditions.CheckArgument(windowLenMs % bucketsPerWindow == 0, "window size must be a multiplication of number of buckets"
				);
			topUsersCnt = conf.GetInt(DFSConfigKeys.NntopNumUsersKey, DFSConfigKeys.NntopNumUsersDefault
				);
			Preconditions.CheckArgument(topUsersCnt > 0, "the number of requested top users must be at least 1"
				);
		}

		/// <summary>
		/// Called when the metric command is changed by "delta" units at time "time"
		/// via user "user"
		/// </summary>
		/// <param name="time">the time of the event</param>
		/// <param name="command">the metric that is updated, e.g., the operation name</param>
		/// <param name="user">the user that updated the metric</param>
		/// <param name="delta">the amount of change in the metric, e.g., +1</param>
		public virtual void RecordMetric(long time, string command, string user, long delta
			)
		{
			RollingWindow window = GetRollingWindow(command, user);
			window.IncAt(time, delta);
		}

		/// <summary>Take a snapshot of current top users in the past period.</summary>
		/// <param name="time">the current time</param>
		/// <returns>
		/// a TopWindow describing the top users for each metric in the
		/// window.
		/// </returns>
		public virtual RollingWindowManager.TopWindow Snapshot(long time)
		{
			RollingWindowManager.TopWindow window = new RollingWindowManager.TopWindow(windowLenMs
				);
			if (Log.IsDebugEnabled())
			{
				ICollection<string> metricNames = metricMap.Keys;
				Log.Debug("iterating in reported metrics, size={} values={}", metricNames.Count, 
					metricNames);
			}
			foreach (KeyValuePair<string, RollingWindowManager.RollingWindowMap> entry in metricMap)
			{
				string metricName = entry.Key;
				RollingWindowManager.RollingWindowMap rollingWindows = entry.Value;
				RollingWindowManager.TopN topN = GetTopUsersForMetric(time, metricName, rollingWindows
					);
				int size = topN.Count;
				if (size == 0)
				{
					continue;
				}
				RollingWindowManager.OP op = new RollingWindowManager.OP(metricName, topN.GetTotal
					());
				window.AddOp(op);
				// Reverse the users from the TopUsers using a stack, 
				// since we'd like them sorted in descending rather than ascending order
				Stack<RollingWindowManager.NameValuePair> reverse = new Stack<RollingWindowManager.NameValuePair
					>();
				for (int i = 0; i < size; i++)
				{
					reverse.Push(topN.Poll());
				}
				for (int i_1 = 0; i_1 < size; i_1++)
				{
					RollingWindowManager.NameValuePair userEntry = reverse.Pop();
					RollingWindowManager.User user = new RollingWindowManager.User(userEntry.name, Sharpen.Extensions.ValueOf
						(userEntry.value));
					op.AddUser(user);
				}
			}
			return window;
		}

		/// <summary>Calculates the top N users over a time interval.</summary>
		/// <param name="time">the current time</param>
		/// <param name="metricName">Name of metric</param>
		/// <returns/>
		private RollingWindowManager.TopN GetTopUsersForMetric(long time, string metricName
			, RollingWindowManager.RollingWindowMap rollingWindows)
		{
			RollingWindowManager.TopN topN = new RollingWindowManager.TopN(topUsersCnt);
			IEnumerator<KeyValuePair<string, RollingWindow>> iterator = rollingWindows.GetEnumerator
				();
			while (iterator.HasNext())
			{
				KeyValuePair<string, RollingWindow> entry = iterator.Next();
				string userName = entry.Key;
				RollingWindow aWindow = entry.Value;
				long windowSum = aWindow.GetSum(time);
				// do the gc here
				if (windowSum == 0)
				{
					Log.Debug("gc window of metric: {} userName: {}", metricName, userName);
					iterator.Remove();
					continue;
				}
				Log.Debug("offer window of metric: {} userName: {} sum: {}", metricName, userName
					, windowSum);
				topN.Offer(new RollingWindowManager.NameValuePair(userName, windowSum));
			}
			Log.Info("topN size for command {} is: {}", metricName, topN.Count);
			return topN;
		}

		/// <summary>Get the rolling window specified by metric and user.</summary>
		/// <param name="metric">the updated metric</param>
		/// <param name="user">the user that updated the metric</param>
		/// <returns>the rolling window</returns>
		private RollingWindow GetRollingWindow(string metric, string user)
		{
			RollingWindowManager.RollingWindowMap rwMap = metricMap[metric];
			if (rwMap == null)
			{
				rwMap = new RollingWindowManager.RollingWindowMap();
				RollingWindowManager.RollingWindowMap prevRwMap = metricMap.PutIfAbsent(metric, rwMap
					);
				if (prevRwMap != null)
				{
					rwMap = prevRwMap;
				}
			}
			RollingWindow window = rwMap[user];
			if (window != null)
			{
				return window;
			}
			window = new RollingWindow(windowLenMs, bucketsPerWindow);
			RollingWindow prevWindow = rwMap.PutIfAbsent(user, window);
			if (prevWindow != null)
			{
				window = prevWindow;
			}
			return window;
		}

		/// <summary>A pair of a name and its corresponding value.</summary>
		/// <remarks>
		/// A pair of a name and its corresponding value. Defines a custom
		/// comparator so the TopN PriorityQueue sorts based on the count.
		/// </remarks>
		private class NameValuePair : Comparable<RollingWindowManager.NameValuePair>
		{
			internal string name;

			internal long value;

			public NameValuePair(string metricName, long value)
			{
				this.name = metricName;
				this.value = value;
			}

			public virtual int CompareTo(RollingWindowManager.NameValuePair other)
			{
				return (int)(value - other.value);
			}

			public override bool Equals(object other)
			{
				if (other is RollingWindowManager.NameValuePair)
				{
					return CompareTo((RollingWindowManager.NameValuePair)other) == 0;
				}
				return false;
			}

			public override int GetHashCode()
			{
				return Sharpen.Extensions.ValueOf(value).GetHashCode();
			}
		}

		/// <summary>A fixed-size priority queue, used to retrieve top-n of offered entries.</summary>
		[System.Serializable]
		private class TopN : PriorityQueue<RollingWindowManager.NameValuePair>
		{
			private const long serialVersionUID = 5134028249611535803L;

			internal int n;

			private long total = 0;

			internal TopN(int n)
				: base(n)
			{
				// > 0
				this.n = n;
			}

			public override bool Offer(RollingWindowManager.NameValuePair entry)
			{
				UpdateTotal(entry.value);
				if (Count == n)
				{
					RollingWindowManager.NameValuePair smallest = Peek();
					if (smallest.value >= entry.value)
					{
						return false;
					}
					Poll();
				}
				// remove smallest
				return base.Offer(entry);
			}

			private void UpdateTotal(long value)
			{
				total += value;
			}

			public virtual long GetTotal()
			{
				return total;
			}
		}
	}
}
