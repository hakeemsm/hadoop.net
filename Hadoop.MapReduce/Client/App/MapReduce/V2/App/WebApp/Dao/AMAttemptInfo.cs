using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.Util;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Webapp.Dao
{
	public class AMAttemptInfo
	{
		protected internal string nodeHttpAddress;

		protected internal string nodeId;

		protected internal int id;

		protected internal long startTime;

		protected internal string containerId;

		protected internal string logsLink;

		public AMAttemptInfo()
		{
		}

		public AMAttemptInfo(AMInfo amInfo, string jobId, string user)
		{
			this.nodeHttpAddress = string.Empty;
			this.nodeId = string.Empty;
			string nmHost = amInfo.GetNodeManagerHost();
			int nmHttpPort = amInfo.GetNodeManagerHttpPort();
			int nmPort = amInfo.GetNodeManagerPort();
			if (nmHost != null)
			{
				this.nodeHttpAddress = nmHost + ":" + nmHttpPort;
				NodeId nodeId = NodeId.NewInstance(nmHost, nmPort);
				this.nodeId = nodeId.ToString();
			}
			this.id = amInfo.GetAppAttemptId().GetAttemptId();
			this.startTime = amInfo.GetStartTime();
			this.containerId = string.Empty;
			this.logsLink = string.Empty;
			ContainerId containerId = amInfo.GetContainerId();
			if (containerId != null)
			{
				this.containerId = containerId.ToString();
				this.logsLink = StringHelper.Join(MRWebAppUtil.GetYARNWebappScheme() + nodeHttpAddress
					, StringHelper.Ujoin("node", "containerlogs", this.containerId, user));
			}
		}

		public virtual string GetNodeHttpAddress()
		{
			return this.nodeHttpAddress;
		}

		public virtual string GetNodeId()
		{
			return this.nodeId;
		}

		public virtual int GetAttemptId()
		{
			return this.id;
		}

		public virtual long GetStartTime()
		{
			return this.startTime;
		}

		public virtual string GetContainerId()
		{
			return this.containerId;
		}

		public virtual string GetLogsLink()
		{
			return this.logsLink;
		}
	}
}
