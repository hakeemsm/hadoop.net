using Com.Google.Common.Base;
using Com.Google.Common.Primitives;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.Top
{
	/// <summary>This class is a common place for NNTop configuration.</summary>
	public sealed class TopConf
	{
		/// <summary>Whether TopMetrics are enabled</summary>
		public readonly bool isEnabled;

		/// <summary>A meta command representing the total number of calls to all commands</summary>
		public const string AllCmds = "*";

		/// <summary>nntop reporting periods in milliseconds</summary>
		public readonly int[] nntopReportingPeriodsMs;

		public TopConf(Configuration conf)
		{
			isEnabled = conf.GetBoolean(DFSConfigKeys.NntopEnabledKey, DFSConfigKeys.NntopEnabledDefault
				);
			string[] periodsStr = conf.GetTrimmedStrings(DFSConfigKeys.NntopWindowsMinutesKey
				, DFSConfigKeys.NntopWindowsMinutesDefault);
			nntopReportingPeriodsMs = new int[periodsStr.Length];
			for (int i = 0; i < periodsStr.Length; i++)
			{
				nntopReportingPeriodsMs[i] = Ints.CheckedCast(TimeUnit.Minutes.ToMillis(System.Convert.ToInt32
					(periodsStr[i])));
			}
			foreach (int aPeriodMs in nntopReportingPeriodsMs)
			{
				Preconditions.CheckArgument(aPeriodMs >= TimeUnit.Minutes.ToMillis(1), "minimum reporting period is 1 min!"
					);
			}
		}
	}
}
