using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Monitor
{
	public interface SchedulingEditPolicy
	{
		void Init(Configuration config, RMContext context, PreemptableResourceScheduler scheduler
			);

		/// <summary>This method is invoked at regular intervals.</summary>
		/// <remarks>
		/// This method is invoked at regular intervals. Internally the policy is
		/// allowed to track containers and affect the scheduler. The "actions"
		/// performed are passed back through an EventHandler.
		/// </remarks>
		void EditSchedule();

		long GetMonitoringInterval();

		string GetPolicyName();
	}
}
