using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation
{
	/// <summary>
	/// This is the implementation of
	/// <see cref="ReservationSystem"/>
	/// based on the
	/// <see cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity.CapacityScheduler
	/// 	"/>
	/// </summary>
	public class CapacityReservationSystem : AbstractReservationSystem
	{
		private static readonly Logger Log = LoggerFactory.GetLogger(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation.CapacityReservationSystem
			));

		private CapacityScheduler capScheduler;

		public CapacityReservationSystem()
			: base(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation.CapacityReservationSystem
				).FullName)
		{
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		public override void Reinitialize(Configuration conf, RMContext rmContext)
		{
			// Validate if the scheduler is capacity based
			ResourceScheduler scheduler = rmContext.GetScheduler();
			if (!(scheduler is CapacityScheduler))
			{
				throw new YarnRuntimeException("Class " + scheduler.GetType().GetCanonicalName() 
					+ " not instance of " + typeof(CapacityScheduler).GetCanonicalName());
			}
			capScheduler = (CapacityScheduler)scheduler;
			this.conf = conf;
			base.Reinitialize(conf, rmContext);
		}

		protected internal override Resource GetMinAllocation()
		{
			return capScheduler.GetMinimumResourceCapability();
		}

		protected internal override Resource GetMaxAllocation()
		{
			return capScheduler.GetMaximumResourceCapability();
		}

		protected internal override ResourceCalculator GetResourceCalculator()
		{
			return capScheduler.GetResourceCalculator();
		}

		protected internal override QueueMetrics GetRootQueueMetrics()
		{
			return capScheduler.GetRootQueueMetrics();
		}

		protected internal override string GetPlanQueuePath(string planQueueName)
		{
			return capScheduler.GetQueue(planQueueName).GetQueuePath();
		}

		protected internal override Org.Apache.Hadoop.Yarn.Api.Records.Resource GetPlanQueueCapacity
			(string planQueueName)
		{
			Org.Apache.Hadoop.Yarn.Api.Records.Resource minAllocation = GetMinAllocation();
			ResourceCalculator rescCalc = GetResourceCalculator();
			CSQueue planQueue = capScheduler.GetQueue(planQueueName);
			return rescCalc.MultiplyAndNormalizeDown(capScheduler.GetClusterResource(), planQueue
				.GetAbsoluteCapacity(), minAllocation);
		}

		protected internal override ReservationSchedulerConfiguration GetReservationSchedulerConfiguration
			()
		{
			return capScheduler.GetConfiguration();
		}
	}
}
