using System.Collections.Generic;
using System.Net;
using Com.Google.Common.Collect;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Namenode.Top;
using Org.Apache.Hadoop.Hdfs.Server.Namenode.Top.Window;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Util;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.Top.Metrics
{
	/// <summary>The interface to the top metrics.</summary>
	/// <remarks>
	/// The interface to the top metrics.
	/// <p/>
	/// Metrics are collected by a custom audit logger,
	/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.Top.TopAuditLogger"/>
	/// , which calls TopMetrics to
	/// increment per-operation, per-user counts on every audit log call. These
	/// counts are used to show the top users by NameNode operation as well as
	/// across all operations.
	/// <p/>
	/// TopMetrics maintains these counts for a configurable number of time
	/// intervals, e.g. 1min, 5min, 25min. Each interval is tracked by a
	/// RollingWindowManager.
	/// <p/>
	/// These metrics are published as a JSON string via
	/// <see cref="org.apache.hadoop.hdfs.server.namenode.metrics.FSNamesystemMBean#getTopWindows
	/// 	"/>
	/// . This is
	/// done by calling
	/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.Top.Window.RollingWindowManager.Snapshot(long)
	/// 	"/>
	/// on each RollingWindowManager.
	/// <p/>
	/// Thread-safe: relies on thread-safety of RollingWindowManager
	/// </remarks>
	public class TopMetrics
	{
		public static readonly Logger Log = LoggerFactory.GetLogger(typeof(Org.Apache.Hadoop.Hdfs.Server.Namenode.Top.Metrics.TopMetrics
			));

		private static void LogConf(Configuration conf)
		{
			Log.Info("NNTop conf: " + DFSConfigKeys.NntopBucketsPerWindowKey + " = " + conf.Get
				(DFSConfigKeys.NntopBucketsPerWindowKey));
			Log.Info("NNTop conf: " + DFSConfigKeys.NntopNumUsersKey + " = " + conf.Get(DFSConfigKeys
				.NntopNumUsersKey));
			Log.Info("NNTop conf: " + DFSConfigKeys.NntopWindowsMinutesKey + " = " + conf.Get
				(DFSConfigKeys.NntopWindowsMinutesKey));
		}

		/// <summary>A map from reporting periods to WindowManager.</summary>
		/// <remarks>
		/// A map from reporting periods to WindowManager. Thread-safety is provided by
		/// the fact that the mapping is not changed after construction.
		/// </remarks>
		internal readonly IDictionary<int, RollingWindowManager> rollingWindowManagers = 
			new Dictionary<int, RollingWindowManager>();

		public TopMetrics(Configuration conf, int[] reportingPeriods)
		{
			LogConf(conf);
			for (int i = 0; i < reportingPeriods.Length; i++)
			{
				rollingWindowManagers[reportingPeriods[i]] = new RollingWindowManager(conf, reportingPeriods
					[i]);
			}
		}

		/// <summary>
		/// Get a list of the current TopWindow statistics, one TopWindow per tracked
		/// time interval.
		/// </summary>
		public virtual IList<RollingWindowManager.TopWindow> GetTopWindows()
		{
			long monoTime = Time.MonotonicNow();
			IList<RollingWindowManager.TopWindow> windows = Lists.NewArrayListWithCapacity(rollingWindowManagers
				.Count);
			foreach (KeyValuePair<int, RollingWindowManager> entry in rollingWindowManagers)
			{
				RollingWindowManager.TopWindow window = entry.Value.Snapshot(monoTime);
				windows.AddItem(window);
			}
			return windows;
		}

		/// <summary>
		/// Pick the same information that DefaultAuditLogger does before writing to a
		/// log file.
		/// </summary>
		/// <remarks>
		/// Pick the same information that DefaultAuditLogger does before writing to a
		/// log file. This is to be consistent when
		/// <see cref="TopMetrics"/>
		/// is charged with
		/// data read back from log files instead of being invoked directly by the
		/// FsNamesystem
		/// </remarks>
		public virtual void Report(bool succeeded, string userName, IPAddress addr, string
			 cmd, string src, string dst, FileStatus status)
		{
			// currently nntop only makes use of the username and the command
			Report(userName, cmd);
		}

		public virtual void Report(string userName, string cmd)
		{
			long currTime = Time.MonotonicNow();
			Report(currTime, userName, cmd);
		}

		public virtual void Report(long currTime, string userName, string cmd)
		{
			Log.Debug("a metric is reported: cmd: {} user: {}", cmd, userName);
			userName = UserGroupInformation.TrimLoginMethod(userName);
			foreach (RollingWindowManager rollingWindowManager in rollingWindowManagers.Values)
			{
				rollingWindowManager.RecordMetric(currTime, cmd, userName, 1);
				rollingWindowManager.RecordMetric(currTime, TopConf.AllCmds, userName, 1);
			}
		}
	}
}
