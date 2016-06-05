using Com.Google.Common.Annotations;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Security
{
	public class QueueACLsManager
	{
		private ResourceScheduler scheduler;

		private bool isACLsEnable;

		[VisibleForTesting]
		public QueueACLsManager()
			: this(null, new Configuration())
		{
		}

		public QueueACLsManager(ResourceScheduler scheduler, Configuration conf)
		{
			this.scheduler = scheduler;
			this.isACLsEnable = conf.GetBoolean(YarnConfiguration.YarnAclEnable, YarnConfiguration
				.DefaultYarnAclEnable);
		}

		public virtual bool CheckAccess(UserGroupInformation callerUGI, QueueACL acl, string
			 queueName)
		{
			if (!isACLsEnable)
			{
				return true;
			}
			return scheduler.CheckAccess(callerUGI, acl, queueName);
		}
	}
}
