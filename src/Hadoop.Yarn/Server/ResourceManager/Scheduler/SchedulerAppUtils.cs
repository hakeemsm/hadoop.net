using Org.Apache.Commons.Logging;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler
{
	public class SchedulerAppUtils
	{
		public static bool IsBlacklisted(SchedulerApplicationAttempt application, SchedulerNode
			 node, Log Log)
		{
			if (application.IsBlacklisted(node.GetNodeName()))
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Skipping 'host' " + node.GetNodeName() + " for " + application.GetApplicationId
						() + " since it has been blacklisted");
				}
				return true;
			}
			if (application.IsBlacklisted(node.GetRackName()))
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Skipping 'rack' " + node.GetRackName() + " for " + application.GetApplicationId
						() + " since it has been blacklisted");
				}
				return true;
			}
			return false;
		}
	}
}
