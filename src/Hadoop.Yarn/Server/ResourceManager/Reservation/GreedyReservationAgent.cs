using System;
using System.Collections.Generic;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation.Exceptions;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation
{
	/// <summary>
	/// This Agent employs a simple greedy placement strategy, placing the various
	/// stages of a
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ReservationRequest"/>
	/// from the deadline moving backward
	/// towards the arrival. This allows jobs with earlier deadline to be scheduled
	/// greedily as well. Combined with an opportunistic anticipation of work if the
	/// cluster is not fully utilized also seems to provide good latency for
	/// best-effort jobs (i.e., jobs running without a reservation).
	/// This agent does not account for locality and only consider container
	/// granularity for validation purposes (i.e., you can't exceed max-container
	/// size).
	/// </summary>
	public class GreedyReservationAgent : ReservationAgent
	{
		private static readonly Logger Log = LoggerFactory.GetLogger(typeof(GreedyReservationAgent
			));

		/// <exception cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation.Exceptions.PlanningException
		/// 	"/>
		public virtual bool CreateReservation(ReservationId reservationId, string user, Plan
			 plan, ReservationDefinition contract)
		{
			return ComputeAllocation(reservationId, user, plan, contract, null);
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation.Exceptions.PlanningException
		/// 	"/>
		public virtual bool UpdateReservation(ReservationId reservationId, string user, Plan
			 plan, ReservationDefinition contract)
		{
			return ComputeAllocation(reservationId, user, plan, contract, plan.GetReservationById
				(reservationId));
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation.Exceptions.PlanningException
		/// 	"/>
		public virtual bool DeleteReservation(ReservationId reservationId, string user, Plan
			 plan)
		{
			return plan.DeleteReservation(reservationId);
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation.Exceptions.PlanningException
		/// 	"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation.Exceptions.ContractValidationException
		/// 	"/>
		private bool ComputeAllocation(ReservationId reservationId, string user, Plan plan
			, ReservationDefinition contract, ReservationAllocation oldReservation)
		{
			Log.Info("placing the following ReservationRequest: " + contract);
			Resource totalCapacity = plan.GetTotalCapacity();
			// Here we can addd logic to adjust the ResourceDefinition to account for
			// system "imperfections" (e.g., scheduling delays for large containers).
			// Align with plan step conservatively (i.e., ceil arrival, and floor
			// deadline)
			long earliestStart = contract.GetArrival();
			long step = plan.GetStep();
			if (earliestStart % step != 0)
			{
				earliestStart = earliestStart + (step - (earliestStart % step));
			}
			long deadline = contract.GetDeadline() - contract.GetDeadline() % plan.GetStep();
			// setup temporary variables to handle time-relations between stages and
			// intermediate answers
			long curDeadline = deadline;
			long oldDeadline = -1;
			IDictionary<ReservationInterval, ReservationRequest> allocations = new Dictionary
				<ReservationInterval, ReservationRequest>();
			RLESparseResourceAllocation tempAssigned = new RLESparseResourceAllocation(plan.GetResourceCalculator
				(), plan.GetMinimumAllocation());
			IList<ReservationRequest> stages = contract.GetReservationRequests().GetReservationResources
				();
			ReservationRequestInterpreter type = contract.GetReservationRequests().GetInterpreter
				();
			// Iterate the stages in backward from deadline
			for (ListIterator<ReservationRequest> li = stages.ListIterator(stages.Count); li.
				HasPrevious(); )
			{
				ReservationRequest currentReservationStage = li.Previous();
				// validate the RR respect basic constraints
				ValidateInput(plan, currentReservationStage, totalCapacity);
				// run allocation for a single stage
				IDictionary<ReservationInterval, ReservationRequest> curAlloc = PlaceSingleStage(
					plan, tempAssigned, currentReservationStage, earliestStart, curDeadline, oldReservation
					, totalCapacity);
				if (curAlloc == null)
				{
					// if we did not find an allocation for the currentReservationStage
					// return null, unless the ReservationDefinition we are placing is of
					// type ANY
					if (type != ReservationRequestInterpreter.RAny)
					{
						throw new PlanningException("The GreedyAgent" + " couldn't find a valid allocation for your request"
							);
					}
					else
					{
						continue;
					}
				}
				else
				{
					// if we did find an allocation add it to the set of allocations
					allocations.PutAll(curAlloc);
					// if this request is of type ANY we are done searching (greedy)
					// and can return the current allocation (break-out of the search)
					if (type == ReservationRequestInterpreter.RAny)
					{
						break;
					}
					// if the request is of ORDER or ORDER_NO_GAP we constraint the next
					// round of allocation to precede the current allocation, by setting
					// curDeadline
					if (type == ReservationRequestInterpreter.ROrder || type == ReservationRequestInterpreter
						.ROrderNoGap)
					{
						curDeadline = FindEarliestTime(curAlloc.Keys);
						// for ORDER_NO_GAP verify that the allocation found so far has no
						// gap, return null otherwise (the greedy procedure failed to find a
						// no-gap
						// allocation)
						if (type == ReservationRequestInterpreter.ROrderNoGap && oldDeadline > 0)
						{
							if (oldDeadline - FindLatestTime(curAlloc.Keys) > plan.GetStep())
							{
								throw new PlanningException("The GreedyAgent" + " couldn't find a valid allocation for your request"
									);
							}
						}
						// keep the variable oldDeadline pointing to the last deadline we
						// found
						oldDeadline = curDeadline;
					}
				}
			}
			// / If we got here is because we failed to find an allocation for the
			// ReservationDefinition give-up and report failure to the user
			if (allocations.IsEmpty())
			{
				throw new PlanningException("The GreedyAgent" + " couldn't find a valid allocation for your request"
					);
			}
			// create reservation with above allocations if not null/empty
			ReservationRequest ZeroRes = ReservationRequest.NewInstance(Resource.NewInstance(
				0, 0), 0);
			long firstStartTime = FindEarliestTime(allocations.Keys);
			// add zero-padding from arrival up to the first non-null allocation
			// to guarantee that the reservation exists starting at arrival
			if (firstStartTime > earliestStart)
			{
				allocations[new ReservationInterval(earliestStart, firstStartTime)] = ZeroRes;
				firstStartTime = earliestStart;
			}
			// consider to add trailing zeros at the end for simmetry
			// Actually add/update the reservation in the plan.
			// This is subject to validation as other agents might be placing
			// in parallel and there might be sharing policies the agent is not
			// aware off.
			ReservationAllocation capReservation = new InMemoryReservationAllocation(reservationId
				, contract, user, plan.GetQueueName(), firstStartTime, FindLatestTime(allocations
				.Keys), allocations, plan.GetResourceCalculator(), plan.GetMinimumAllocation());
			if (oldReservation != null)
			{
				return plan.UpdateReservation(capReservation);
			}
			else
			{
				return plan.AddReservation(capReservation);
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation.Exceptions.ContractValidationException
		/// 	"/>
		private void ValidateInput(Plan plan, ReservationRequest rr, Resource totalCapacity
			)
		{
			if (rr.GetConcurrency() < 1)
			{
				throw new ContractValidationException("Gang Size should be >= 1");
			}
			if (rr.GetNumContainers() <= 0)
			{
				throw new ContractValidationException("Num containers should be >= 0");
			}
			// check that gangSize and numContainers are compatible
			if (rr.GetNumContainers() % rr.GetConcurrency() != 0)
			{
				throw new ContractValidationException("Parallelism must be an exact multiple of gang size"
					);
			}
			// check that the largest container request does not exceed
			// the cluster-wide limit for container sizes
			if (Resources.GreaterThan(plan.GetResourceCalculator(), totalCapacity, rr.GetCapability
				(), plan.GetMaximumAllocation()))
			{
				throw new ContractValidationException("Individual" + " capability requests should not exceed cluster's maxAlloc"
					);
			}
		}

		/// <summary>
		/// This method actually perform the placement of an atomic stage of the
		/// reservation.
		/// </summary>
		/// <remarks>
		/// This method actually perform the placement of an atomic stage of the
		/// reservation. The key idea is to traverse the plan backward for a
		/// "lease-duration" worth of time, and compute what is the maximum multiple of
		/// our concurrency (gang) parameter we can fit. We do this and move towards
		/// previous instant in time until the time-window is exhausted or we placed
		/// all the user request.
		/// </remarks>
		private IDictionary<ReservationInterval, ReservationRequest> PlaceSingleStage(Plan
			 plan, RLESparseResourceAllocation tempAssigned, ReservationRequest rr, long earliestStart
			, long curDeadline, ReservationAllocation oldResAllocation, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 totalCapacity)
		{
			IDictionary<ReservationInterval, ReservationRequest> allocationRequests = new Dictionary
				<ReservationInterval, ReservationRequest>();
			// compute the gang as a resource and get the duration
			Org.Apache.Hadoop.Yarn.Api.Records.Resource gang = Resources.Multiply(rr.GetCapability
				(), rr.GetConcurrency());
			long dur = rr.GetDuration();
			long step = plan.GetStep();
			// ceil the duration to the next multiple of the plan step
			if (dur % step != 0)
			{
				dur += (step - (dur % step));
			}
			// we know for sure that this division has no remainder (part of contract
			// with user, validate before
			int gangsToPlace = rr.GetNumContainers() / rr.GetConcurrency();
			int maxGang = 0;
			// loop trying to place until we are done, or we are considering
			// an invalid range of times
			while (gangsToPlace > 0 && curDeadline - dur >= earliestStart)
			{
				// as we run along we remember how many gangs we can fit, and what
				// was the most constraining moment in time (we will restart just
				// after that to place the next batch)
				maxGang = gangsToPlace;
				long minPoint = curDeadline;
				int curMaxGang = maxGang;
				// start placing at deadline (excluded due to [,) interval semantics and
				// move backward
				for (long t = curDeadline - plan.GetStep(); t >= curDeadline - dur && maxGang > 0
					; t = t - plan.GetStep())
				{
					// As we run along we will logically remove the previous allocation for
					// this reservation
					// if one existed
					Org.Apache.Hadoop.Yarn.Api.Records.Resource oldResCap = Org.Apache.Hadoop.Yarn.Api.Records.Resource
						.NewInstance(0, 0);
					if (oldResAllocation != null)
					{
						oldResCap = oldResAllocation.GetResourcesAtTime(t);
					}
					// compute net available resources
					Org.Apache.Hadoop.Yarn.Api.Records.Resource netAvailableRes = Resources.Clone(totalCapacity
						);
					Resources.AddTo(netAvailableRes, oldResCap);
					Resources.SubtractFrom(netAvailableRes, plan.GetTotalCommittedResources(t));
					Resources.SubtractFrom(netAvailableRes, tempAssigned.GetCapacityAtTime(t));
					// compute maximum number of gangs we could fit
					curMaxGang = (int)Math.Floor(Resources.Divide(plan.GetResourceCalculator(), totalCapacity
						, netAvailableRes, gang));
					// pick the minimum between available resources in this instant, and how
					// many gangs we have to place
					curMaxGang = Math.Min(gangsToPlace, curMaxGang);
					// compare with previous max, and set it. also remember *where* we found
					// the minimum (useful for next attempts)
					if (curMaxGang <= maxGang)
					{
						maxGang = curMaxGang;
						minPoint = t;
					}
				}
				// if we were able to place any gang, record this, and decrement
				// gangsToPlace
				if (maxGang > 0)
				{
					gangsToPlace -= maxGang;
					ReservationInterval reservationInt = new ReservationInterval(curDeadline - dur, curDeadline
						);
					ReservationRequest reservationRes = ReservationRequest.NewInstance(rr.GetCapability
						(), rr.GetConcurrency() * maxGang, rr.GetConcurrency(), rr.GetDuration());
					// remember occupied space (plan is read-only till we find a plausible
					// allocation for the entire request). This is needed since we might be
					// placing other ReservationRequest within the same
					// ReservationDefinition,
					// and we must avoid double-counting the available resources
					tempAssigned.AddInterval(reservationInt, reservationRes);
					allocationRequests[reservationInt] = reservationRes;
				}
				// reset our new starting point (curDeadline) to the most constraining
				// point so far, we will look "left" of that to find more places where
				// to schedule gangs (for sure nothing on the "right" of this point can
				// fit a full gang.
				curDeadline = minPoint;
			}
			// if no gangs are left to place we succeed and return the allocation
			if (gangsToPlace == 0)
			{
				return allocationRequests;
			}
			else
			{
				// If we are here is becasue we did not manage to satisfy this request.
				// So we need to remove unwanted side-effect from tempAssigned (needed
				// for ANY).
				foreach (KeyValuePair<ReservationInterval, ReservationRequest> tempAllocation in 
					allocationRequests)
				{
					tempAssigned.RemoveInterval(tempAllocation.Key, tempAllocation.Value);
				}
				// and return null to signal failure in this allocation
				return null;
			}
		}

		// finds the leftmost point of this set of ReservationInterval
		private long FindEarliestTime(ICollection<ReservationInterval> resInt)
		{
			long ret = long.MaxValue;
			foreach (ReservationInterval s in resInt)
			{
				if (s.GetStartTime() < ret)
				{
					ret = s.GetStartTime();
				}
			}
			return ret;
		}

		// finds the rightmost point of this set of ReservationIntervals
		private long FindLatestTime(ICollection<ReservationInterval> resInt)
		{
			long ret = long.MinValue;
			foreach (ReservationInterval s in resInt)
			{
				if (s.GetEndTime() > ret)
				{
					ret = s.GetEndTime();
				}
			}
			return ret;
		}
	}
}
