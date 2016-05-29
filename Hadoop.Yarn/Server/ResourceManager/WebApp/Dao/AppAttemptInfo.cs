using Org.Apache.Commons.Lang;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Hadoop.Yarn.Webapp.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp.Dao
{
	public class AppAttemptInfo
	{
		protected internal int id;

		protected internal long startTime;

		protected internal string containerId;

		protected internal string nodeHttpAddress;

		protected internal string nodeId;

		protected internal string logsLink;

		protected internal string blacklistedNodes;

		public AppAttemptInfo()
		{
		}

		public AppAttemptInfo(ResourceManager rm, RMAppAttempt attempt, string user, string
			 schemePrefix)
		{
			this.startTime = 0;
			this.containerId = string.Empty;
			this.nodeHttpAddress = string.Empty;
			this.nodeId = string.Empty;
			this.logsLink = string.Empty;
			this.blacklistedNodes = string.Empty;
			if (attempt != null)
			{
				this.id = attempt.GetAppAttemptId().GetAttemptId();
				this.startTime = attempt.GetStartTime();
				Container masterContainer = attempt.GetMasterContainer();
				if (masterContainer != null)
				{
					this.containerId = masterContainer.GetId().ToString();
					this.nodeHttpAddress = masterContainer.GetNodeHttpAddress();
					this.nodeId = masterContainer.GetNodeId().ToString();
					this.logsLink = WebAppUtils.GetRunningLogURL(schemePrefix + masterContainer.GetNodeHttpAddress
						(), ConverterUtils.ToString(masterContainer.GetId()), user);
					if (rm.GetResourceScheduler() is AbstractYarnScheduler)
					{
						AbstractYarnScheduler ayScheduler = (AbstractYarnScheduler)rm.GetResourceScheduler
							();
						SchedulerApplicationAttempt sattempt = ayScheduler.GetApplicationAttempt(attempt.
							GetAppAttemptId());
						if (sattempt != null)
						{
							blacklistedNodes = StringUtils.Join(sattempt.GetBlacklistedNodes(), ", ");
						}
					}
				}
			}
		}

		public virtual int GetAttemptId()
		{
			return this.id;
		}

		public virtual long GetStartTime()
		{
			return this.startTime;
		}

		public virtual string GetNodeHttpAddress()
		{
			return this.nodeHttpAddress;
		}

		public virtual string GetLogsLink()
		{
			return this.logsLink;
		}
	}
}
