using System.Collections.Generic;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation.Exceptions;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Common;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation
{
	public abstract class AbstractSchedulerPlanFollower : PlanFollower
	{
		private static readonly Logger Log = LoggerFactory.GetLogger(typeof(CapacitySchedulerPlanFollower
			));

		protected internal ICollection<Plan> plans = new AList<Plan>();

		protected internal YarnScheduler scheduler;

		protected internal Clock clock;

		public virtual void Init(Clock clock, ResourceScheduler sched, ICollection<Plan> 
			plans)
		{
			this.clock = clock;
			this.scheduler = sched;
			Sharpen.Collections.AddAll(this.plans, plans);
		}

		public virtual void Run()
		{
			lock (this)
			{
				foreach (Plan plan in plans)
				{
					SynchronizePlan(plan);
				}
			}
		}

		public virtual void SetPlans(ICollection<Plan> plans)
		{
			lock (this)
			{
				this.plans.Clear();
				Sharpen.Collections.AddAll(this.plans, plans);
			}
		}

		public virtual void SynchronizePlan(Plan plan)
		{
			lock (this)
			{
				string planQueueName = plan.GetQueueName();
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Running plan follower edit policy for plan: " + planQueueName);
				}
				// align with plan step
				long step = plan.GetStep();
				long now = clock.GetTime();
				if (now % step != 0)
				{
					now += step - (now % step);
				}
				Queue planQueue = GetPlanQueue(planQueueName);
				if (planQueue == null)
				{
					return;
				}
				// first we publish to the plan the current availability of resources
				Resource clusterResources = scheduler.GetClusterResource();
				Resource planResources = GetPlanResources(plan, planQueue, clusterResources);
				ICollection<ReservationAllocation> currentReservations = plan.GetReservationsAtTime
					(now);
				ICollection<string> curReservationNames = new HashSet<string>();
				Resource reservedResources = Resource.NewInstance(0, 0);
				int numRes = GetReservedResources(now, currentReservations, curReservationNames, 
					reservedResources);
				// create the default reservation queue if it doesnt exist
				string defReservationId = GetReservationIdFromQueueName(planQueueName) + ReservationConstants
					.DefaultQueueSuffix;
				string defReservationQueue = GetReservationQueueName(planQueueName, defReservationId
					);
				CreateDefaultReservationQueue(planQueueName, planQueue, defReservationId);
				curReservationNames.AddItem(defReservationId);
				// if the resources dedicated to this plan has shrunk invoke replanner
				if (ArePlanResourcesLessThanReservations(clusterResources, planResources, reservedResources
					))
				{
					try
					{
						plan.GetReplanner().Plan(plan, null);
					}
					catch (PlanningException e)
					{
						Log.Warn("Exception while trying to replan: {}", planQueueName, e);
					}
				}
				// identify the reservations that have expired and new reservations that
				// have to be activated
				IList<Queue> resQueues = GetChildReservationQueues(planQueue);
				ICollection<string> expired = new HashSet<string>();
				foreach (Queue resQueue in resQueues)
				{
					string resQueueName = resQueue.GetQueueName();
					string reservationId = GetReservationIdFromQueueName(resQueueName);
					if (curReservationNames.Contains(reservationId))
					{
						// it is already existing reservation, so needed not create new
						// reservation queue
						curReservationNames.Remove(reservationId);
					}
					else
					{
						// the reservation has termination, mark for cleanup
						expired.AddItem(reservationId);
					}
				}
				// garbage collect expired reservations
				CleanupExpiredQueues(planQueueName, plan.GetMoveOnExpiry(), expired, defReservationQueue
					);
				// Add new reservations and update existing ones
				float totalAssignedCapacity = 0f;
				if (currentReservations != null)
				{
					// first release all excess capacity in default queue
					try
					{
						SetQueueEntitlement(planQueueName, defReservationQueue, 0f, 1.0f);
					}
					catch (YarnException e)
					{
						Log.Warn("Exception while trying to release default queue capacity for plan: {}", 
							planQueueName, e);
					}
					// sort allocations from the one giving up the most resources, to the
					// one asking for the most
					// avoid order-of-operation errors that temporarily violate 100%
					// capacity bound
					IList<ReservationAllocation> sortedAllocations = SortByDelta(new AList<ReservationAllocation
						>(currentReservations), now, plan);
					foreach (ReservationAllocation res in sortedAllocations)
					{
						string currResId = res.GetReservationId().ToString();
						if (curReservationNames.Contains(currResId))
						{
							AddReservationQueue(planQueueName, planQueue, currResId);
						}
						Resource capToAssign = res.GetResourcesAtTime(now);
						float targetCapacity = 0f;
						if (planResources.GetMemory() > 0 && planResources.GetVirtualCores() > 0)
						{
							targetCapacity = CalculateReservationToPlanRatio(clusterResources, planResources, 
								capToAssign);
						}
						if (Log.IsDebugEnabled())
						{
							Log.Debug("Assigning capacity of {} to queue {} with target capacity {}", capToAssign
								, currResId, targetCapacity);
						}
						// set maxCapacity to 100% unless the job requires gang, in which
						// case we stick to capacity (as running early/before is likely a
						// waste of resources)
						float maxCapacity = 1.0f;
						if (res.ContainsGangs())
						{
							maxCapacity = targetCapacity;
						}
						try
						{
							SetQueueEntitlement(planQueueName, currResId, targetCapacity, maxCapacity);
						}
						catch (YarnException e)
						{
							Log.Warn("Exception while trying to size reservation for plan: {}", currResId, planQueueName
								, e);
						}
						totalAssignedCapacity += targetCapacity;
					}
				}
				// compute the default queue capacity
				float defQCap = 1.0f - totalAssignedCapacity;
				if (Log.IsDebugEnabled())
				{
					Log.Debug("PlanFollowerEditPolicyTask: total Plan Capacity: {} " + "currReservation: {} default-queue capacity: {}"
						, planResources, numRes, defQCap);
				}
				// set the default queue to eat-up all remaining capacity
				try
				{
					SetQueueEntitlement(planQueueName, defReservationQueue, defQCap, 1.0f);
				}
				catch (YarnException e)
				{
					Log.Warn("Exception while trying to reclaim default queue capacity for plan: {}", 
						planQueueName, e);
				}
				// garbage collect finished reservations from plan
				try
				{
					plan.ArchiveCompletedReservations(now);
				}
				catch (PlanningException e)
				{
					Log.Error("Exception in archiving completed reservations: ", e);
				}
				Log.Info("Finished iteration of plan follower edit policy for plan: " + planQueueName
					);
			}
		}

		// Extension: update plan with app states,
		// useful to support smart replanning
		protected internal virtual string GetReservationIdFromQueueName(string resQueueName
			)
		{
			return resQueueName;
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		protected internal virtual void SetQueueEntitlement(string planQueueName, string 
			currResId, float targetCapacity, float maxCapacity)
		{
			string reservationQueueName = GetReservationQueueName(planQueueName, currResId);
			scheduler.SetEntitlement(reservationQueueName, new QueueEntitlement(targetCapacity
				, maxCapacity));
		}

		// Schedulers have different ways of naming queues. See YARN-2773
		protected internal virtual string GetReservationQueueName(string planQueueName, string
			 reservationId)
		{
			return reservationId;
		}

		/// <summary>First sets entitlement of queues to zero to prevent new app submission.</summary>
		/// <remarks>
		/// First sets entitlement of queues to zero to prevent new app submission.
		/// Then move all apps in the set of queues to the parent plan queue's default
		/// reservation queue if move is enabled. Finally cleanups the queue by killing
		/// any apps (if move is disabled or move failed) and removing the queue
		/// </remarks>
		protected internal virtual void CleanupExpiredQueues(string planQueueName, bool shouldMove
			, ICollection<string> toRemove, string defReservationQueue)
		{
			foreach (string expiredReservationId in toRemove)
			{
				try
				{
					// reduce entitlement to 0
					string expiredReservation = GetReservationQueueName(planQueueName, expiredReservationId
						);
					SetQueueEntitlement(planQueueName, expiredReservation, 0.0f, 0.0f);
					if (shouldMove)
					{
						MoveAppsInQueueSync(expiredReservation, defReservationQueue);
					}
					if (scheduler.GetAppsInQueue(expiredReservation).Count > 0)
					{
						scheduler.KillAllAppsInQueue(expiredReservation);
						Log.Info("Killing applications in queue: {}", expiredReservation);
					}
					else
					{
						scheduler.RemoveQueue(expiredReservation);
						Log.Info("Queue: " + expiredReservation + " removed");
					}
				}
				catch (YarnException e)
				{
					Log.Warn("Exception while trying to expire reservation: {}", expiredReservationId
						, e);
				}
			}
		}

		/// <summary>
		/// Move all apps in the set of queues to the parent plan queue's default
		/// reservation queue in a synchronous fashion
		/// </summary>
		private void MoveAppsInQueueSync(string expiredReservation, string defReservationQueue
			)
		{
			IList<ApplicationAttemptId> activeApps = scheduler.GetAppsInQueue(expiredReservation
				);
			if (activeApps.IsEmpty())
			{
				return;
			}
			foreach (ApplicationAttemptId app in activeApps)
			{
				// fallback to parent's default queue
				try
				{
					scheduler.MoveApplication(app.GetApplicationId(), defReservationQueue);
				}
				catch (YarnException e)
				{
					Log.Warn("Encountered unexpected error during migration of application: {}" + " from reservation: {}"
						, app, expiredReservation, e);
				}
			}
		}

		protected internal virtual int GetReservedResources(long now, ICollection<ReservationAllocation
			> currentReservations, ICollection<string> curReservationNames, Resource reservedResources
			)
		{
			int numRes = 0;
			if (currentReservations != null)
			{
				numRes = currentReservations.Count;
				foreach (ReservationAllocation reservation in currentReservations)
				{
					curReservationNames.AddItem(reservation.GetReservationId().ToString());
					Resources.AddTo(reservedResources, reservation.GetResourcesAtTime(now));
				}
			}
			return numRes;
		}

		/// <summary>
		/// Sort in the order from the least new amount of resources asked (likely
		/// negative) to the highest.
		/// </summary>
		/// <remarks>
		/// Sort in the order from the least new amount of resources asked (likely
		/// negative) to the highest. This prevents "order-of-operation" errors related
		/// to exceeding 100% capacity temporarily.
		/// </remarks>
		protected internal virtual IList<ReservationAllocation> SortByDelta(IList<ReservationAllocation
			> currentReservations, long now, Plan plan)
		{
			currentReservations.Sort(new AbstractSchedulerPlanFollower.ReservationAllocationComparator
				(now, this, plan));
			return currentReservations;
		}

		/// <summary>Get queue associated with reservable queue named</summary>
		/// <param name="planQueueName">Name of the reservable queue</param>
		/// <returns>queue associated with the reservable queue</returns>
		protected internal abstract Queue GetPlanQueue(string planQueueName);

		/// <summary>Calculates ratio of reservationResources to planResources</summary>
		protected internal abstract float CalculateReservationToPlanRatio(Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 clusterResources, Org.Apache.Hadoop.Yarn.Api.Records.Resource planResources, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 reservationResources);

		/// <summary>Check if plan resources are less than expected reservation resources</summary>
		protected internal abstract bool ArePlanResourcesLessThanReservations(Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 clusterResources, Org.Apache.Hadoop.Yarn.Api.Records.Resource planResources, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 reservedResources);

		/// <summary>Get a list of reservation queues for this planQueue</summary>
		protected internal abstract IList<Queue> GetChildReservationQueues(Queue planQueue
			);

		/// <summary>Add a new reservation queue for reservation currResId for this planQueue
		/// 	</summary>
		protected internal abstract void AddReservationQueue(string planQueueName, Queue 
			queue, string currResId);

		/// <summary>
		/// Creates the default reservation queue for use when no reservation is
		/// used for applications submitted to this planQueue
		/// </summary>
		protected internal abstract void CreateDefaultReservationQueue(string planQueueName
			, Queue queue, string defReservationQueue);

		/// <summary>Get plan resources for this planQueue</summary>
		protected internal abstract Org.Apache.Hadoop.Yarn.Api.Records.Resource GetPlanResources
			(Plan plan, Queue queue, Org.Apache.Hadoop.Yarn.Api.Records.Resource clusterResources
			);

		/// <summary>Get reservation queue resources if it exists otherwise return null</summary>
		protected internal abstract Org.Apache.Hadoop.Yarn.Api.Records.Resource GetReservationQueueResourceIfExists
			(Plan plan, ReservationId reservationId);

		private class ReservationAllocationComparator : IComparer<ReservationAllocation>
		{
			internal AbstractSchedulerPlanFollower planFollower;

			internal long now;

			internal Plan plan;

			internal ReservationAllocationComparator(long now, AbstractSchedulerPlanFollower 
				planFollower, Plan plan)
			{
				this.now = now;
				this.planFollower = planFollower;
				this.plan = plan;
			}

			private Org.Apache.Hadoop.Yarn.Api.Records.Resource GetUnallocatedReservedResources
				(ReservationAllocation reservation)
			{
				Org.Apache.Hadoop.Yarn.Api.Records.Resource resResource;
				Org.Apache.Hadoop.Yarn.Api.Records.Resource reservationResource = planFollower.GetReservationQueueResourceIfExists
					(plan, reservation.GetReservationId());
				if (reservationResource != null)
				{
					resResource = Resources.Subtract(reservation.GetResourcesAtTime(now), reservationResource
						);
				}
				else
				{
					resResource = reservation.GetResourcesAtTime(now);
				}
				return resResource;
			}

			public virtual int Compare(ReservationAllocation lhs, ReservationAllocation rhs)
			{
				// compute delta between current and previous reservation, and compare
				// based on that
				Org.Apache.Hadoop.Yarn.Api.Records.Resource lhsRes = GetUnallocatedReservedResources
					(lhs);
				Org.Apache.Hadoop.Yarn.Api.Records.Resource rhsRes = GetUnallocatedReservedResources
					(rhs);
				return lhsRes.CompareTo(rhsRes);
			}
		}
	}
}
