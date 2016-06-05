using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation
{
	public class FairReservationSystem : AbstractReservationSystem
	{
		private FairScheduler fairScheduler;

		public FairReservationSystem()
			: base(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation.FairReservationSystem
				).FullName)
		{
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		public override void Reinitialize(Configuration conf, RMContext rmContext)
		{
			// Validate if the scheduler is fair scheduler
			ResourceScheduler scheduler = rmContext.GetScheduler();
			if (!(scheduler is FairScheduler))
			{
				throw new YarnRuntimeException("Class " + scheduler.GetType().GetCanonicalName() 
					+ " not instance of " + typeof(FairScheduler).GetCanonicalName());
			}
			fairScheduler = (FairScheduler)scheduler;
			this.conf = conf;
			base.Reinitialize(conf, rmContext);
		}

		protected internal override ReservationSchedulerConfiguration GetReservationSchedulerConfiguration
			()
		{
			return fairScheduler.GetAllocationConfiguration();
		}

		protected internal override ResourceCalculator GetResourceCalculator()
		{
			return fairScheduler.GetResourceCalculator();
		}

		protected internal override QueueMetrics GetRootQueueMetrics()
		{
			return fairScheduler.GetRootQueueMetrics();
		}

		protected internal override Org.Apache.Hadoop.Yarn.Api.Records.Resource GetMinAllocation
			()
		{
			return fairScheduler.GetMinimumResourceCapability();
		}

		protected internal override Org.Apache.Hadoop.Yarn.Api.Records.Resource GetMaxAllocation
			()
		{
			return fairScheduler.GetMaximumResourceCapability();
		}

		protected internal override string GetPlanQueuePath(string planQueueName)
		{
			return planQueueName;
		}

		protected internal override Org.Apache.Hadoop.Yarn.Api.Records.Resource GetPlanQueueCapacity
			(string planQueueName)
		{
			return fairScheduler.GetQueueManager().GetParentQueue(planQueueName, false).GetSteadyFairShare
				();
		}
	}
}
