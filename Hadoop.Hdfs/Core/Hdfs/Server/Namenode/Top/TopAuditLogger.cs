using System.Net;
using System.Text;
using Com.Google.Common.Base;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Server.Namenode.Top.Metrics;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.Top
{
	/// <summary>
	/// An
	/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.AuditLogger"/>
	/// that sends logged data directly to the metrics
	/// systems. It is used when the top service is used directly by the name node
	/// </summary>
	public class TopAuditLogger : AuditLogger
	{
		public static readonly Logger Log = LoggerFactory.GetLogger(typeof(Org.Apache.Hadoop.Hdfs.Server.Namenode.Top.TopAuditLogger
			));

		private readonly TopMetrics topMetrics;

		public TopAuditLogger(TopMetrics topMetrics)
		{
			Preconditions.CheckNotNull(topMetrics, "Cannot init with a null " + "TopMetrics");
			this.topMetrics = topMetrics;
		}

		public virtual void Initialize(Configuration conf)
		{
		}

		public virtual void LogAuditEvent(bool succeeded, string userName, IPAddress addr
			, string cmd, string src, string dst, FileStatus status)
		{
			try
			{
				topMetrics.Report(succeeded, userName, addr, cmd, src, dst, status);
			}
			catch
			{
				Log.Error("An error occurred while reflecting the event in top service, " + "event: (cmd={},userName={})"
					, cmd, userName);
			}
			if (Log.IsDebugEnabled())
			{
				StringBuilder sb = new StringBuilder();
				sb.Append("allowed=").Append(succeeded).Append("\t");
				sb.Append("ugi=").Append(userName).Append("\t");
				sb.Append("ip=").Append(addr).Append("\t");
				sb.Append("cmd=").Append(cmd).Append("\t");
				sb.Append("src=").Append(src).Append("\t");
				sb.Append("dst=").Append(dst).Append("\t");
				if (null == status)
				{
					sb.Append("perm=null");
				}
				else
				{
					sb.Append("perm=");
					sb.Append(status.GetOwner()).Append(":");
					sb.Append(status.GetGroup()).Append(":");
					sb.Append(status.GetPermission());
				}
				Log.Debug("------------------- logged event for top service: " + sb);
			}
		}
	}
}
