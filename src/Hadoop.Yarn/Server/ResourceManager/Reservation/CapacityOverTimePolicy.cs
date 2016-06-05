using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation.Exceptions;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation
{
	/// <summary>This policy enforces a time-extended notion of Capacity.</summary>
	/// <remarks>
	/// This policy enforces a time-extended notion of Capacity. In particular it
	/// guarantees that the allocation received in input when combined with all
	/// previous allocation for the user does not violate an instantaneous max limit
	/// on the resources received, and that for every window of time of length
	/// validWindow, the integral of the allocations for a user (sum of the currently
	/// submitted allocation and all prior allocations for the user) does not exceed
	/// validWindow * maxAvg.
	/// This allows flexibility, in the sense that an allocation can instantaneously
	/// use large portions of the available capacity, but prevents abuses by bounding
	/// the average use over time.
	/// By controlling maxInst, maxAvg, validWindow the administrator configuring
	/// this policy can obtain a behavior ranging from instantaneously enforced
	/// capacity (akin to existing queues), or fully flexible allocations (likely
	/// reserved to super-users, or trusted systems).
	/// </remarks>
	public class CapacityOverTimePolicy : SharingPolicy
	{
		private ReservationSchedulerConfiguration conf;

		private long validWindow;

		private float maxInst;

		private float maxAvg;

		// For now this is CapacityScheduler specific, but given a hierarchy in the
		// configuration structure of the schedulers (e.g., SchedulerConfiguration)
		// it should be easy to remove this limitation
		public virtual void Init(string reservationQueuePath, ReservationSchedulerConfiguration
			 conf)
		{
			this.conf = conf;
			validWindow = this.conf.GetReservationWindow(reservationQueuePath);
			maxInst = this.conf.GetInstantaneousMaxCapacity(reservationQueuePath) / 100;
			maxAvg = this.conf.GetAverageCapacity(reservationQueuePath) / 100;
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation.Exceptions.PlanningException
		/// 	"/>
		public virtual void Validate(Plan plan, ReservationAllocation reservation)
		{
			// this is entire method invoked under a write-lock on the plan, no need
			// to synchronize accesses to the plan further
			// Try to verify whether there is already a reservation with this ID in
			// the system (remove its contribution during validation to simulate a
			// try-n-swap
			// update).
			ReservationAllocation oldReservation = plan.GetReservationById(reservation.GetReservationId
				());
			// sanity check that the update of a reservation is not changing username
			if (oldReservation != null && !oldReservation.GetUser().Equals(reservation.GetUser
				()))
			{
				throw new MismatchedUserException("Updating an existing reservation with mismatched user:"
					 + oldReservation.GetUser() + " != " + reservation.GetUser());
			}
			long startTime = reservation.GetStartTime();
			long endTime = reservation.GetEndTime();
			long step = plan.GetStep();
			Resource planTotalCapacity = plan.GetTotalCapacity();
			Org.Apache.Hadoop.Yarn.Api.Records.Resource maxAvgRes = Resources.Multiply(planTotalCapacity
				, maxAvg);
			Org.Apache.Hadoop.Yarn.Api.Records.Resource maxInsRes = Resources.Multiply(planTotalCapacity
				, maxInst);
			// define variable that will store integral of resources (need diff class to
			// avoid overflow issues for long/large allocations)
			CapacityOverTimePolicy.IntegralResource runningTot = new CapacityOverTimePolicy.IntegralResource
				(0L, 0L);
			CapacityOverTimePolicy.IntegralResource maxAllowed = new CapacityOverTimePolicy.IntegralResource
				(maxAvgRes);
			maxAllowed.MultiplyBy(validWindow / step);
			// check that the resources offered to the user during any window of length
			// "validWindow" overlapping this allocation are within maxAllowed
			// also enforce instantaneous and physical constraints during this pass
			for (long t = startTime - validWindow; t < endTime + validWindow; t += step)
			{
				Org.Apache.Hadoop.Yarn.Api.Records.Resource currExistingAllocTot = plan.GetTotalCommittedResources
					(t);
				Org.Apache.Hadoop.Yarn.Api.Records.Resource currExistingAllocForUser = plan.GetConsumptionForUser
					(reservation.GetUser(), t);
				Org.Apache.Hadoop.Yarn.Api.Records.Resource currNewAlloc = reservation.GetResourcesAtTime
					(t);
				Org.Apache.Hadoop.Yarn.Api.Records.Resource currOldAlloc = Resources.None();
				if (oldReservation != null)
				{
					currOldAlloc = oldReservation.GetResourcesAtTime(t);
				}
				// throw exception if the cluster is overcommitted
				// tot_allocated - old + new > capacity
				Org.Apache.Hadoop.Yarn.Api.Records.Resource inst = Resources.Subtract(Resources.Add
					(currExistingAllocTot, currNewAlloc), currOldAlloc);
				if (Resources.GreaterThan(plan.GetResourceCalculator(), planTotalCapacity, inst, 
					planTotalCapacity))
				{
					throw new ResourceOverCommitException(" Resources at time " + t + " would be overcommitted ("
						 + inst + " over " + plan.GetTotalCapacity() + ") by accepting reservation: " + 
						reservation.GetReservationId());
				}
				// throw exception if instantaneous limits are violated
				// tot_alloc_to_this_user - old + new > inst_limit
				if (Resources.GreaterThan(plan.GetResourceCalculator(), planTotalCapacity, Resources
					.Subtract(Resources.Add(currExistingAllocForUser, currNewAlloc), currOldAlloc), 
					maxInsRes))
				{
					throw new PlanningQuotaException("Instantaneous quota capacity " + maxInst + " would be passed at time "
						 + t + " by accepting reservation: " + reservation.GetReservationId());
				}
				// throw exception if the running integral of utilization over validWindow
				// is violated. We perform a delta check, adding/removing instants at the
				// boundary of the window from runningTot.
				// runningTot = previous_runningTot + currExistingAllocForUser +
				// currNewAlloc - currOldAlloc - pastNewAlloc - pastOldAlloc;
				// Where:
				// 1) currNewAlloc, currExistingAllocForUser represent the contribution of
				// the instant in time added in this pass.
				// 2) pastNewAlloc, pastOldAlloc are the contributions relative to time
				// instants that are being retired from the the window
				// 3) currOldAlloc is the contribution (if any) of the previous version of
				// this reservation (the one we are updating)
				runningTot.Add(currExistingAllocForUser);
				runningTot.Add(currNewAlloc);
				runningTot.Subtract(currOldAlloc);
				// expire contributions from instant in time before (t - validWindow)
				if (t > startTime)
				{
					Org.Apache.Hadoop.Yarn.Api.Records.Resource pastOldAlloc = plan.GetConsumptionForUser
						(reservation.GetUser(), t - validWindow);
					Org.Apache.Hadoop.Yarn.Api.Records.Resource pastNewAlloc = reservation.GetResourcesAtTime
						(t - validWindow);
					// runningTot = runningTot - pastExistingAlloc - pastNewAlloc;
					runningTot.Subtract(pastOldAlloc);
					runningTot.Subtract(pastNewAlloc);
				}
				// check integral
				// runningTot > maxAvg * validWindow
				// NOTE: we need to use comparator of IntegralResource directly, as
				// Resource and ResourceCalculator assume "int" amount of resources,
				// which is not sufficient when comparing integrals (out-of-bound)
				if (maxAllowed.CompareTo(runningTot) < 0)
				{
					throw new PlanningQuotaException("Integral (avg over time) quota capacity " + maxAvg
						 + " over a window of " + validWindow / 1000 + " seconds, " + " would be passed at time "
						 + t + "(" + Sharpen.Extensions.CreateDate(t) + ") by accepting reservation: " +
						 reservation.GetReservationId());
				}
			}
		}

		public virtual long GetValidWindow()
		{
			return validWindow;
		}

		/// <summary>
		/// This class provides support for Resource-like book-keeping, based on
		/// long(s), as using Resource to store the "integral" of the allocation over
		/// time leads to integer overflows for large allocations/clusters.
		/// </summary>
		/// <remarks>
		/// This class provides support for Resource-like book-keeping, based on
		/// long(s), as using Resource to store the "integral" of the allocation over
		/// time leads to integer overflows for large allocations/clusters. (Evolving
		/// Resource to use long is too disruptive at this point.)
		/// The comparison/multiplication behaviors of IntegralResource are consistent
		/// with the DefaultResourceCalculator.
		/// </remarks>
		private class IntegralResource
		{
			internal long memory;

			internal long vcores;

			public IntegralResource(Org.Apache.Hadoop.Yarn.Api.Records.Resource resource)
			{
				this.memory = resource.GetMemory();
				this.vcores = resource.GetVirtualCores();
			}

			public IntegralResource(long mem, long vcores)
			{
				this.memory = mem;
				this.vcores = vcores;
			}

			public virtual void Add(Org.Apache.Hadoop.Yarn.Api.Records.Resource r)
			{
				memory += r.GetMemory();
				vcores += r.GetVirtualCores();
			}

			public virtual void Subtract(Org.Apache.Hadoop.Yarn.Api.Records.Resource r)
			{
				memory -= r.GetMemory();
				vcores -= r.GetVirtualCores();
			}

			public virtual void MultiplyBy(long window)
			{
				memory = memory * window;
				vcores = vcores * window;
			}

			public virtual long CompareTo(CapacityOverTimePolicy.IntegralResource other)
			{
				long diff = memory - other.memory;
				if (diff == 0)
				{
					diff = vcores - other.vcores;
				}
				return diff;
			}

			public override string ToString()
			{
				return "<memory:" + memory + ", vCores:" + vcores + ">";
			}
		}
	}
}
