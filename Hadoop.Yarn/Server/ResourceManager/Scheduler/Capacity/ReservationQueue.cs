using System.IO;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Common;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity
{
	/// <summary>
	/// This represents a dynamic
	/// <see cref="LeafQueue"/>
	/// managed by the
	/// <see cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation.ReservationSystem
	/// 	"/>
	/// </summary>
	public class ReservationQueue : LeafQueue
	{
		private static readonly Logger Log = LoggerFactory.GetLogger(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity.ReservationQueue
			));

		private PlanQueue parent;

		/// <exception cref="System.IO.IOException"/>
		public ReservationQueue(CapacitySchedulerContext cs, string queueName, PlanQueue 
			parent)
			: base(cs, queueName, parent, null)
		{
			// the following parameters are common to all reservation in the plan
			UpdateQuotas(parent.GetUserLimitForReservation(), parent.GetUserLimitFactor(), parent
				.GetMaxApplicationsForReservations(), parent.GetMaxApplicationsPerUserForReservation
				());
			this.parent = parent;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Reinitialize(CSQueue newlyParsedQueue, Resource clusterResource
			)
		{
			lock (this)
			{
				// Sanity check
				if (!(newlyParsedQueue is Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity.ReservationQueue
					) || !newlyParsedQueue.GetQueuePath().Equals(GetQueuePath()))
				{
					throw new IOException("Trying to reinitialize " + GetQueuePath() + " from " + newlyParsedQueue
						.GetQueuePath());
				}
				base.Reinitialize(newlyParsedQueue, clusterResource);
				CSQueueUtils.UpdateQueueStatistics(parent.schedulerContext.GetResourceCalculator(
					), newlyParsedQueue, parent, parent.schedulerContext.GetClusterResource(), parent
					.schedulerContext.GetMinimumResourceCapability());
				UpdateQuotas(parent.GetUserLimitForReservation(), parent.GetUserLimitFactor(), parent
					.GetMaxApplicationsForReservations(), parent.GetMaxApplicationsPerUserForReservation
					());
			}
		}

		/// <summary>
		/// This methods to change capacity for a queue and adjusts its
		/// absoluteCapacity
		/// </summary>
		/// <param name="entitlement">
		/// the new entitlement for the queue (capacity,
		/// maxCapacity, etc..)
		/// </param>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.SchedulerDynamicEditException
		/// 	"/>
		public virtual void SetEntitlement(QueueEntitlement entitlement)
		{
			lock (this)
			{
				float capacity = entitlement.GetCapacity();
				if (capacity < 0 || capacity > 1.0f)
				{
					throw new SchedulerDynamicEditException("Capacity demand is not in the [0,1] range: "
						 + capacity);
				}
				SetCapacity(capacity);
				SetAbsoluteCapacity(GetParent().GetAbsoluteCapacity() * GetCapacity());
				// note: we currently set maxCapacity to capacity
				// this might be revised later
				SetMaxCapacity(entitlement.GetMaxCapacity());
				if (Log.IsDebugEnabled())
				{
					Log.Debug("successfully changed to " + capacity + " for queue " + this.GetQueueName
						());
				}
			}
		}

		private void UpdateQuotas(int userLimit, float userLimitFactor, int maxAppsForReservation
			, int maxAppsPerUserForReservation)
		{
			SetUserLimit(userLimit);
			SetUserLimitFactor(userLimitFactor);
			SetMaxApplications(maxAppsForReservation);
			maxApplicationsPerUser = maxAppsPerUserForReservation;
		}

		protected internal override void SetupConfigurableCapacities()
		{
			CSQueueUtils.UpdateAndCheckCapacitiesByLabel(GetQueuePath(), queueCapacities, parent
				 == null ? null : parent.GetQueueCapacities());
		}
	}
}
