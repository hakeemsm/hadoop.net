using Com.Google.Common.Annotations;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair
{
	public class ReservationQueueConfiguration
	{
		private long reservationWindow;

		private long enforcementWindow;

		private string reservationAdmissionPolicy;

		private string reservationAgent;

		private string planner;

		private bool showReservationAsQueues;

		private bool moveOnExpiry;

		private float avgOverTimeMultiplier;

		private float maxOverTimeMultiplier;

		public ReservationQueueConfiguration()
		{
			this.reservationWindow = ReservationSchedulerConfiguration.DefaultReservationWindow;
			this.enforcementWindow = ReservationSchedulerConfiguration.DefaultReservationEnforcementWindow;
			this.reservationAdmissionPolicy = ReservationSchedulerConfiguration.DefaultReservationAdmissionPolicy;
			this.reservationAgent = ReservationSchedulerConfiguration.DefaultReservationAgentName;
			this.planner = ReservationSchedulerConfiguration.DefaultReservationPlannerName;
			this.showReservationAsQueues = ReservationSchedulerConfiguration.DefaultShowReservationsAsQueues;
			this.moveOnExpiry = ReservationSchedulerConfiguration.DefaultReservationMoveOnExpiry;
			this.avgOverTimeMultiplier = ReservationSchedulerConfiguration.DefaultCapacityOverTimeMultiplier;
			this.maxOverTimeMultiplier = ReservationSchedulerConfiguration.DefaultCapacityOverTimeMultiplier;
		}

		public virtual long GetReservationWindowMsec()
		{
			return reservationWindow;
		}

		public virtual long GetEnforcementWindowMsec()
		{
			return enforcementWindow;
		}

		public virtual bool ShouldShowReservationAsQueues()
		{
			return showReservationAsQueues;
		}

		public virtual bool ShouldMoveOnExpiry()
		{
			return moveOnExpiry;
		}

		public virtual string GetReservationAdmissionPolicy()
		{
			return reservationAdmissionPolicy;
		}

		public virtual string GetReservationAgent()
		{
			return reservationAgent;
		}

		public virtual string GetPlanner()
		{
			return planner;
		}

		public virtual float GetAvgOverTimeMultiplier()
		{
			return avgOverTimeMultiplier;
		}

		public virtual float GetMaxOverTimeMultiplier()
		{
			return maxOverTimeMultiplier;
		}

		public virtual void SetPlanner(string planner)
		{
			this.planner = planner;
		}

		public virtual void SetReservationAdmissionPolicy(string reservationAdmissionPolicy
			)
		{
			this.reservationAdmissionPolicy = reservationAdmissionPolicy;
		}

		public virtual void SetReservationAgent(string reservationAgent)
		{
			this.reservationAgent = reservationAgent;
		}

		[VisibleForTesting]
		public virtual void SetReservationWindow(long reservationWindow)
		{
			this.reservationWindow = reservationWindow;
		}

		[VisibleForTesting]
		public virtual void SetAverageCapacity(int averageCapacity)
		{
			this.avgOverTimeMultiplier = averageCapacity;
		}
	}
}
