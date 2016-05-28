using Org.Apache.Hadoop.Mapreduce.V2.HS;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.HS.Webapp.Dao
{
	public class HistoryInfo
	{
		protected internal long startedOn;

		protected internal string hadoopVersion;

		protected internal string hadoopBuildVersion;

		protected internal string hadoopVersionBuiltOn;

		public HistoryInfo()
		{
			this.startedOn = JobHistoryServer.historyServerTimeStamp;
			this.hadoopVersion = VersionInfo.GetVersion();
			this.hadoopBuildVersion = VersionInfo.GetBuildVersion();
			this.hadoopVersionBuiltOn = VersionInfo.GetDate();
		}

		public virtual string GetHadoopVersion()
		{
			return this.hadoopVersion;
		}

		public virtual string GetHadoopBuildVersion()
		{
			return this.hadoopBuildVersion;
		}

		public virtual string GetHadoopVersionBuiltOn()
		{
			return this.hadoopVersionBuiltOn;
		}

		public virtual long GetStartedOn()
		{
			return this.startedOn;
		}
	}
}
