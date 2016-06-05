using System.Collections.Generic;
using System.IO;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation
{
	/// <summary>
	/// This class implements a
	/// <see cref="PlanFollower"/>
	/// . This is invoked on a timer, and
	/// it is in charge to publish the state of the
	/// <see cref="Plan"/>
	/// s to the underlying
	/// <see cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity.CapacityScheduler
	/// 	"/>
	/// . This implementation does so, by
	/// adding/removing/resizing leaf queues in the scheduler, thus affecting the
	/// dynamic behavior of the scheduler in a way that is consistent with the
	/// content of the plan. It also updates the plan's view on how much resources
	/// are available in the cluster.
	/// This implementation of PlanFollower is relatively stateless, and it can
	/// synchronize schedulers and Plans that have arbitrary changes (performing set
	/// differences among existing queues). This makes it resilient to frequency of
	/// synchronization, and RM restart issues (no "catch up" is necessary).
	/// </summary>
	public class CapacitySchedulerPlanFollower : AbstractSchedulerPlanFollower
	{
		private static readonly Logger Log = LoggerFactory.GetLogger(typeof(CapacitySchedulerPlanFollower
			));

		private CapacityScheduler cs;

		public override void Init(Clock clock, ResourceScheduler sched, ICollection<Plan>
			 plans)
		{
			base.Init(clock, sched, plans);
			Log.Info("Initializing Plan Follower Policy:" + this.GetType().GetCanonicalName()
				);
			if (!(sched is CapacityScheduler))
			{
				throw new YarnRuntimeException("CapacitySchedulerPlanFollower can only work with CapacityScheduler"
					);
			}
			this.cs = (CapacityScheduler)sched;
		}

		protected internal override Queue GetPlanQueue(string planQueueName)
		{
			CSQueue queue = cs.GetQueue(planQueueName);
			if (!(queue is PlanQueue))
			{
				Log.Error("The Plan is not an PlanQueue!");
				return null;
			}
			return queue;
		}

		protected internal override float CalculateReservationToPlanRatio(Resource clusterResources
			, Resource planResources, Resource reservationResources)
		{
			return Resources.Divide(cs.GetResourceCalculator(), clusterResources, reservationResources
				, planResources);
		}

		protected internal override bool ArePlanResourcesLessThanReservations(Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 clusterResources, Org.Apache.Hadoop.Yarn.Api.Records.Resource planResources, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 reservedResources)
		{
			return Resources.GreaterThan(cs.GetResourceCalculator(), clusterResources, reservedResources
				, planResources);
		}

		protected internal override IList<Queue> GetChildReservationQueues(Queue queue)
		{
			PlanQueue planQueue = (PlanQueue)queue;
			IList<CSQueue> childQueues = planQueue.GetChildQueues();
			return childQueues;
		}

		protected internal override void AddReservationQueue(string planQueueName, Queue 
			queue, string currResId)
		{
			PlanQueue planQueue = (PlanQueue)queue;
			try
			{
				ReservationQueue resQueue = new ReservationQueue(cs, currResId, planQueue);
				cs.AddQueue(resQueue);
			}
			catch (SchedulerDynamicEditException e)
			{
				Log.Warn("Exception while trying to activate reservation: {} for plan: {}", currResId
					, planQueueName, e);
			}
			catch (IOException e)
			{
				Log.Warn("Exception while trying to activate reservation: {} for plan: {}", currResId
					, planQueueName, e);
			}
		}

		protected internal override void CreateDefaultReservationQueue(string planQueueName
			, Queue queue, string defReservationId)
		{
			PlanQueue planQueue = (PlanQueue)queue;
			if (cs.GetQueue(defReservationId) == null)
			{
				try
				{
					ReservationQueue defQueue = new ReservationQueue(cs, defReservationId, planQueue);
					cs.AddQueue(defQueue);
				}
				catch (SchedulerDynamicEditException e)
				{
					Log.Warn("Exception while trying to create default reservation queue for plan: {}"
						, planQueueName, e);
				}
				catch (IOException e)
				{
					Log.Warn("Exception while trying to create default reservation queue for " + "plan: {}"
						, planQueueName, e);
				}
			}
		}

		protected internal override Org.Apache.Hadoop.Yarn.Api.Records.Resource GetPlanResources
			(Plan plan, Queue queue, Org.Apache.Hadoop.Yarn.Api.Records.Resource clusterResources
			)
		{
			PlanQueue planQueue = (PlanQueue)queue;
			float planAbsCap = planQueue.GetAbsoluteCapacity();
			Org.Apache.Hadoop.Yarn.Api.Records.Resource planResources = Resources.Multiply(clusterResources
				, planAbsCap);
			plan.SetTotalCapacity(planResources);
			return planResources;
		}

		protected internal override Org.Apache.Hadoop.Yarn.Api.Records.Resource GetReservationQueueResourceIfExists
			(Plan plan, ReservationId reservationId)
		{
			CSQueue resQueue = cs.GetQueue(reservationId.ToString());
			Org.Apache.Hadoop.Yarn.Api.Records.Resource reservationResource = null;
			if (resQueue != null)
			{
				reservationResource = Resources.Multiply(cs.GetClusterResource(), resQueue.GetAbsoluteCapacity
					());
			}
			return reservationResource;
		}
	}
}
