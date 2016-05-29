using System.Collections.Generic;
using System.IO;
using System.Text;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity
{
	/// <summary>
	/// This represents a dynamic queue managed by the
	/// <see cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation.ReservationSystem
	/// 	"/>
	/// .
	/// From the user perspective this is equivalent to a LeafQueue that respect
	/// reservations, but functionality wise is a sub-class of ParentQueue
	/// </summary>
	public class PlanQueue : ParentQueue
	{
		private static readonly Logger Log = LoggerFactory.GetLogger(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity.PlanQueue
			));

		private int maxAppsForReservation;

		private int maxAppsPerUserForReservation;

		private int userLimit;

		private float userLimitFactor;

		protected internal CapacitySchedulerContext schedulerContext;

		private bool showReservationsAsQueues;

		/// <exception cref="System.IO.IOException"/>
		public PlanQueue(CapacitySchedulerContext cs, string queueName, CSQueue parent, CSQueue
			 old)
			: base(cs, queueName, parent, old)
		{
			this.schedulerContext = cs;
			// Set the reservation queue attributes for the Plan
			CapacitySchedulerConfiguration conf = cs.GetConfiguration();
			string queuePath = base.GetQueuePath();
			int maxAppsForReservation = conf.GetMaximumApplicationsPerQueue(queuePath);
			showReservationsAsQueues = conf.GetShowReservationAsQueues(queuePath);
			if (maxAppsForReservation < 0)
			{
				maxAppsForReservation = (int)(CapacitySchedulerConfiguration.DefaultMaximumSystemApplicatiions
					 * base.GetAbsoluteCapacity());
			}
			int userLimit = conf.GetUserLimit(queuePath);
			float userLimitFactor = conf.GetUserLimitFactor(queuePath);
			int maxAppsPerUserForReservation = (int)(maxAppsForReservation * (userLimit / 100.0f
				) * userLimitFactor);
			UpdateQuotas(userLimit, userLimitFactor, maxAppsForReservation, maxAppsPerUserForReservation
				);
			StringBuilder queueInfo = new StringBuilder();
			queueInfo.Append("Created Plan Queue: ").Append(queueName).Append("\nwith capacity: ["
				).Append(base.GetCapacity()).Append("]\nwith max capacity: [").Append(base.GetMaximumCapacity
				()).Append("\nwith max reservation apps: [").Append(maxAppsForReservation).Append
				("]\nwith max reservation apps per user: [").Append(maxAppsPerUserForReservation
				).Append("]\nwith user limit: [").Append(userLimit).Append("]\nwith user limit factor: ["
				).Append(userLimitFactor).Append("].");
			Log.Info(queueInfo.ToString());
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Reinitialize(CSQueue newlyParsedQueue, Resource clusterResource
			)
		{
			lock (this)
			{
				// Sanity check
				if (!(newlyParsedQueue is Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity.PlanQueue
					) || !newlyParsedQueue.GetQueuePath().Equals(GetQueuePath()))
				{
					throw new IOException("Trying to reinitialize " + GetQueuePath() + " from " + newlyParsedQueue
						.GetQueuePath());
				}
				Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity.PlanQueue newlyParsedParentQueue
					 = (Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity.PlanQueue)newlyParsedQueue;
				if (newlyParsedParentQueue.GetChildQueues().Count > 0)
				{
					throw new IOException("Reservable Queue should not have sub-queues in the" + "configuration"
						);
				}
				// Set new configs
				SetupQueueConfigs(clusterResource);
				UpdateQuotas(newlyParsedParentQueue.userLimit, newlyParsedParentQueue.userLimitFactor
					, newlyParsedParentQueue.maxAppsForReservation, newlyParsedParentQueue.maxAppsPerUserForReservation
					);
				// run reinitialize on each existing queue, to trigger absolute cap
				// recomputations
				foreach (CSQueue res in this.GetChildQueues())
				{
					res.Reinitialize(res, clusterResource);
				}
				showReservationsAsQueues = newlyParsedParentQueue.showReservationsAsQueues;
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.SchedulerDynamicEditException
		/// 	"/>
		internal virtual void AddChildQueue(CSQueue newQueue)
		{
			lock (this)
			{
				if (newQueue.GetCapacity() > 0)
				{
					throw new SchedulerDynamicEditException("Queue " + newQueue + " being added has non zero capacity."
						);
				}
				bool added = this.childQueues.AddItem(newQueue);
				if (Log.IsDebugEnabled())
				{
					Log.Debug("updateChildQueues (action: add queue): " + added + " " + GetChildQueuesToPrint
						());
				}
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.SchedulerDynamicEditException
		/// 	"/>
		internal virtual void RemoveChildQueue(CSQueue remQueue)
		{
			lock (this)
			{
				if (remQueue.GetCapacity() > 0)
				{
					throw new SchedulerDynamicEditException("Queue " + remQueue + " being removed has non zero capacity."
						);
				}
				IEnumerator<CSQueue> qiter = childQueues.GetEnumerator();
				while (qiter.HasNext())
				{
					CSQueue cs = qiter.Next();
					if (cs.Equals(remQueue))
					{
						qiter.Remove();
						if (Log.IsDebugEnabled())
						{
							Log.Debug("Removed child queue: {}", cs.GetQueueName());
						}
					}
				}
			}
		}

		protected internal virtual float SumOfChildCapacities()
		{
			lock (this)
			{
				float ret = 0;
				foreach (CSQueue l in childQueues)
				{
					ret += l.GetCapacity();
				}
				return ret;
			}
		}

		private void UpdateQuotas(int userLimit, float userLimitFactor, int maxAppsForReservation
			, int maxAppsPerUserForReservation)
		{
			this.userLimit = userLimit;
			this.userLimitFactor = userLimitFactor;
			this.maxAppsForReservation = maxAppsForReservation;
			this.maxAppsPerUserForReservation = maxAppsPerUserForReservation;
		}

		/// <summary>Number of maximum applications for each of the reservations in this Plan.
		/// 	</summary>
		/// <returns>maxAppsForreservation</returns>
		public virtual int GetMaxApplicationsForReservations()
		{
			return maxAppsForReservation;
		}

		/// <summary>
		/// Number of maximum applications per user for each of the reservations in
		/// this Plan.
		/// </summary>
		/// <returns>maxAppsPerUserForreservation</returns>
		public virtual int GetMaxApplicationsPerUserForReservation()
		{
			return maxAppsPerUserForReservation;
		}

		/// <summary>User limit value for each of the reservations in this Plan.</summary>
		/// <returns>userLimit</returns>
		public virtual int GetUserLimitForReservation()
		{
			return userLimit;
		}

		/// <summary>User limit factor value for each of the reservations in this Plan.</summary>
		/// <returns>userLimitFactor</returns>
		public virtual float GetUserLimitFactor()
		{
			return userLimitFactor;
		}

		/// <summary>Determine whether to hide/show the ReservationQueues</summary>
		public virtual bool ShowReservationsAsQueues()
		{
			return showReservationsAsQueues;
		}
	}
}
