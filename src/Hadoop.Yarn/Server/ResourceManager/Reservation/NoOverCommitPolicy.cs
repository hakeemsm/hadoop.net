using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation.Exceptions;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation
{
	/// <summary>
	/// This policy enforce a simple physical cluster capacity constraints, by
	/// validating that the allocation proposed fits in the current plan.
	/// </summary>
	/// <remarks>
	/// This policy enforce a simple physical cluster capacity constraints, by
	/// validating that the allocation proposed fits in the current plan. This
	/// validation is compatible with "updates" and in verifying the capacity
	/// constraints it conceptually remove the prior version of the reservation.
	/// </remarks>
	public class NoOverCommitPolicy : SharingPolicy
	{
		/// <exception cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation.Exceptions.PlanningException
		/// 	"/>
		public virtual void Validate(Plan plan, ReservationAllocation reservation)
		{
			ReservationAllocation oldReservation = plan.GetReservationById(reservation.GetReservationId
				());
			// check updates are using same name
			if (oldReservation != null && !oldReservation.GetUser().Equals(reservation.GetUser
				()))
			{
				throw new MismatchedUserException("Updating an existing reservation with mismatching user:"
					 + oldReservation.GetUser() + " != " + reservation.GetUser());
			}
			long startTime = reservation.GetStartTime();
			long endTime = reservation.GetEndTime();
			long step = plan.GetStep();
			// for every instant in time, check we are respecting cluster capacity
			for (long t = startTime; t < endTime; t += step)
			{
				Resource currExistingAllocTot = plan.GetTotalCommittedResources(t);
				Resource currNewAlloc = reservation.GetResourcesAtTime(t);
				Resource currOldAlloc = Resource.NewInstance(0, 0);
				if (oldReservation != null)
				{
					oldReservation.GetResourcesAtTime(t);
				}
				// check the cluster is never over committed
				// currExistingAllocTot + currNewAlloc - currOldAlloc >
				// capPlan.getTotalCapacity()
				if (Resources.GreaterThan(plan.GetResourceCalculator(), plan.GetTotalCapacity(), 
					Resources.Subtract(Resources.Add(currExistingAllocTot, currNewAlloc), currOldAlloc
					), plan.GetTotalCapacity()))
				{
					throw new ResourceOverCommitException("Resources at time " + t + " would be overcommitted by "
						 + "accepting reservation: " + reservation.GetReservationId());
				}
			}
		}

		public virtual long GetValidWindow()
		{
			// this policy has no "memory" so the valid window is set to zero
			return 0;
		}

		public virtual void Init(string planQueuePath, ReservationSchedulerConfiguration 
			conf)
		{
		}
		// nothing to do for this policy
	}
}
