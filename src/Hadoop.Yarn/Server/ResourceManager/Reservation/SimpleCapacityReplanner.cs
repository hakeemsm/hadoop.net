using System.Collections.Generic;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation
{
	/// <summary>
	/// This (re)planner scan a period of time from now to a maximum time window (or
	/// the end of the last session, whichever comes first) checking the overall
	/// capacity is not violated.
	/// </summary>
	/// <remarks>
	/// This (re)planner scan a period of time from now to a maximum time window (or
	/// the end of the last session, whichever comes first) checking the overall
	/// capacity is not violated.
	/// It greedily removes sessions in reversed order of acceptance (latest accepted
	/// is the first removed).
	/// </remarks>
	public class SimpleCapacityReplanner : Planner
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation.SimpleCapacityReplanner
			));

		private static readonly Resource ZeroResource = Resource.NewInstance(0, 0);

		private readonly Clock clock;

		private long lengthOfCheckZone;

		public SimpleCapacityReplanner()
			: this(new UTCClock())
		{
		}

		[VisibleForTesting]
		internal SimpleCapacityReplanner(Clock clock)
		{
			// this allows to control to time-span of this replanning
			// far into the future time instants might be worth replanning for
			// later on
			this.clock = clock;
		}

		public virtual void Init(string planQueueName, ReservationSchedulerConfiguration 
			conf)
		{
			this.lengthOfCheckZone = conf.GetEnforcementWindow(planQueueName);
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation.Exceptions.PlanningException
		/// 	"/>
		public virtual void Plan(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation.Plan
			 plan, IList<ReservationDefinition> contracts)
		{
			if (contracts != null)
			{
				throw new RuntimeException("SimpleCapacityReplanner cannot handle new reservation contracts"
					);
			}
			ResourceCalculator resCalc = plan.GetResourceCalculator();
			Org.Apache.Hadoop.Yarn.Api.Records.Resource totCap = plan.GetTotalCapacity();
			long now = clock.GetTime();
			// loop on all moment in time from now to the end of the check Zone
			// or the end of the planned sessions whichever comes first
			for (long t = now; (t < plan.GetLastEndTime() && t < (now + lengthOfCheckZone)); 
				t += plan.GetStep())
			{
				Org.Apache.Hadoop.Yarn.Api.Records.Resource excessCap = Resources.Subtract(plan.GetTotalCommittedResources
					(t), totCap);
				// if we are violating
				if (Resources.GreaterThan(resCalc, totCap, excessCap, ZeroResource))
				{
					// sorted on reverse order of acceptance, so newest reservations first
					ICollection<ReservationAllocation> curReservations = new TreeSet<ReservationAllocation
						>(plan.GetReservationsAtTime(t));
					for (IEnumerator<ReservationAllocation> resIter = curReservations.GetEnumerator()
						; resIter.HasNext() && Resources.GreaterThan(resCalc, totCap, excessCap, ZeroResource
						); )
					{
						ReservationAllocation reservation = resIter.Next();
						plan.DeleteReservation(reservation.GetReservationId());
						excessCap = Resources.Subtract(excessCap, reservation.GetResourcesAtTime(t));
						Log.Info("Removing reservation " + reservation.GetReservationId() + " to repair physical-resource constraints in the plan: "
							 + plan.GetQueueName());
					}
				}
			}
		}
	}
}
