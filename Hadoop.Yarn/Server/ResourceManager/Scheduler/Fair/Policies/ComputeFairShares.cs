using System;
using System.Collections.Generic;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Resource;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair.Policies
{
	/// <summary>Contains logic for computing the fair shares.</summary>
	/// <remarks>
	/// Contains logic for computing the fair shares. A
	/// <see cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair.Schedulable
	/// 	"/>
	/// 's fair
	/// share is
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.Resource"/>
	/// it is entitled to, independent of the current
	/// demands and allocations on the cluster. A
	/// <see cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair.Schedulable
	/// 	"/>
	/// whose resource
	/// consumption lies at or below its fair share will never have its containers
	/// preempted.
	/// </remarks>
	public class ComputeFairShares
	{
		private const int ComputeFairSharesIterations = 25;

		/// <summary>
		/// Compute fair share of the given schedulables.Fair share is an allocation of
		/// shares considering only active schedulables ie schedulables which have
		/// running apps.
		/// </summary>
		/// <param name="schedulables"/>
		/// <param name="totalResources"/>
		/// <param name="type"/>
		public static void ComputeShares<_T0>(ICollection<_T0> schedulables, Resource totalResources
			, ResourceType type)
			where _T0 : Schedulable
		{
			ComputeSharesInternal(schedulables, totalResources, type, false);
		}

		/// <summary>Compute the steady fair share of the given queues.</summary>
		/// <remarks>
		/// Compute the steady fair share of the given queues. The steady fair
		/// share is an allocation of shares considering all queues, i.e.,
		/// active and inactive.
		/// </remarks>
		/// <param name="queues"/>
		/// <param name="totalResources"/>
		/// <param name="type"/>
		public static void ComputeSteadyShares<_T0>(ICollection<_T0> queues, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 totalResources, ResourceType type)
			where _T0 : FSQueue
		{
			ComputeSharesInternal(queues, totalResources, type, true);
		}

		/// <summary>
		/// Given a set of Schedulables and a number of slots, compute their weighted
		/// fair shares.
		/// </summary>
		/// <remarks>
		/// Given a set of Schedulables and a number of slots, compute their weighted
		/// fair shares. The min and max shares and of the Schedulables are assumed to
		/// be set beforehand. We compute the fairest possible allocation of shares to
		/// the Schedulables that respects their min and max shares.
		/// <p>
		/// To understand what this method does, we must first define what weighted
		/// fair sharing means in the presence of min and max shares. If there
		/// were no minimum or maximum shares, then weighted fair sharing would be
		/// achieved if the ratio of slotsAssigned / weight was equal for each
		/// Schedulable and all slots were assigned. Minimum and maximum shares add a
		/// further twist - Some Schedulables may have a min share higher than their
		/// assigned share or a max share lower than their assigned share.
		/// <p>
		/// To deal with these possibilities, we define an assignment of slots as being
		/// fair if there exists a ratio R such that: Schedulables S where S.minShare
		/// <literal>&gt;</literal>
		/// R * S.weight are given share S.minShare - Schedulables S
		/// where S.maxShare
		/// <literal>&lt;</literal>
		/// R * S.weight are given S.maxShare -
		/// All other Schedulables S are assigned share R * S.weight -
		/// The sum of all the shares is totalSlots.
		/// <p>
		/// We call R the weight-to-slots ratio because it converts a Schedulable's
		/// weight to the number of slots it is assigned.
		/// <p>
		/// We compute a fair allocation by finding a suitable weight-to-slot ratio R.
		/// To do this, we use binary search. Given a ratio R, we compute the number of
		/// slots that would be used in total with this ratio (the sum of the shares
		/// computed using the conditions above). If this number of slots is less than
		/// totalSlots, then R is too small and more slots could be assigned. If the
		/// number of slots is more than totalSlots, then R is too large.
		/// <p>
		/// We begin the binary search with a lower bound on R of 0 (which means that
		/// all Schedulables are only given their minShare) and an upper bound computed
		/// to be large enough that too many slots are given (by doubling R until we
		/// use more than totalResources resources). The helper method
		/// resourceUsedWithWeightToResourceRatio computes the total resources used with a
		/// given value of R.
		/// <p>
		/// The running time of this algorithm is linear in the number of Schedulables,
		/// because resourceUsedWithWeightToResourceRatio is linear-time and the number of
		/// iterations of binary search is a constant (dependent on desired precision).
		/// </remarks>
		private static void ComputeSharesInternal<_T0>(ICollection<_T0> allSchedulables, 
			Org.Apache.Hadoop.Yarn.Api.Records.Resource totalResources, ResourceType type, bool
			 isSteadyShare)
			where _T0 : Schedulable
		{
			ICollection<Schedulable> schedulables = new AList<Schedulable>();
			int takenResources = HandleFixedFairShares(allSchedulables, schedulables, isSteadyShare
				, type);
			if (schedulables.IsEmpty())
			{
				return;
			}
			// Find an upper bound on R that we can use in our binary search. We start
			// at R = 1 and double it until we have either used all the resources or we
			// have met all Schedulables' max shares.
			int totalMaxShare = 0;
			foreach (Schedulable sched in schedulables)
			{
				int maxShare = GetResourceValue(sched.GetMaxShare(), type);
				totalMaxShare = (int)Math.Min((long)maxShare + (long)totalMaxShare, int.MaxValue);
				if (totalMaxShare == int.MaxValue)
				{
					break;
				}
			}
			int totalResource = Math.Max((GetResourceValue(totalResources, type) - takenResources
				), 0);
			totalResource = Math.Min(totalMaxShare, totalResource);
			double rMax = 1.0;
			while (ResourceUsedWithWeightToResourceRatio(rMax, schedulables, type) < totalResource
				)
			{
				rMax *= 2.0;
			}
			// Perform the binary search for up to COMPUTE_FAIR_SHARES_ITERATIONS steps
			double left = 0;
			double right = rMax;
			for (int i = 0; i < ComputeFairSharesIterations; i++)
			{
				double mid = (left + right) / 2.0;
				int plannedResourceUsed = ResourceUsedWithWeightToResourceRatio(mid, schedulables
					, type);
				if (plannedResourceUsed == totalResource)
				{
					right = mid;
					break;
				}
				else
				{
					if (plannedResourceUsed < totalResource)
					{
						left = mid;
					}
					else
					{
						right = mid;
					}
				}
			}
			// Set the fair shares based on the value of R we've converged to
			foreach (Schedulable sched_1 in schedulables)
			{
				if (isSteadyShare)
				{
					SetResourceValue(ComputeShare(sched_1, right, type), ((FSQueue)sched_1).GetSteadyFairShare
						(), type);
				}
				else
				{
					SetResourceValue(ComputeShare(sched_1, right, type), sched_1.GetFairShare(), type
						);
				}
			}
		}

		/// <summary>
		/// Compute the resources that would be used given a weight-to-resource ratio
		/// w2rRatio, for use in the computeFairShares algorithm as described in #
		/// </summary>
		private static int ResourceUsedWithWeightToResourceRatio<_T0>(double w2rRatio, ICollection
			<_T0> schedulables, ResourceType type)
			where _T0 : Schedulable
		{
			int resourcesTaken = 0;
			foreach (Schedulable sched in schedulables)
			{
				int share = ComputeShare(sched, w2rRatio, type);
				resourcesTaken += share;
			}
			return resourcesTaken;
		}

		/// <summary>
		/// Compute the resources assigned to a Schedulable given a particular
		/// weight-to-resource ratio w2rRatio.
		/// </summary>
		private static int ComputeShare(Schedulable sched, double w2rRatio, ResourceType 
			type)
		{
			double share = sched.GetWeights().GetWeight(type) * w2rRatio;
			share = Math.Max(share, GetResourceValue(sched.GetMinShare(), type));
			share = Math.Min(share, GetResourceValue(sched.GetMaxShare(), type));
			return (int)share;
		}

		/// <summary>Helper method to handle Schedulabes with fixed fairshares.</summary>
		/// <remarks>
		/// Helper method to handle Schedulabes with fixed fairshares.
		/// Returns the resources taken by fixed fairshare schedulables,
		/// and adds the remaining to the passed nonFixedSchedulables.
		/// </remarks>
		private static int HandleFixedFairShares<_T0>(ICollection<_T0> schedulables, ICollection
			<Schedulable> nonFixedSchedulables, bool isSteadyShare, ResourceType type)
			where _T0 : Schedulable
		{
			int totalResource = 0;
			foreach (Schedulable sched in schedulables)
			{
				int fixedShare = GetFairShareIfFixed(sched, isSteadyShare, type);
				if (fixedShare < 0)
				{
					nonFixedSchedulables.AddItem(sched);
				}
				else
				{
					SetResourceValue(fixedShare, isSteadyShare ? ((FSQueue)sched).GetSteadyFairShare(
						) : sched.GetFairShare(), type);
					totalResource = (int)Math.Min((long)totalResource + (long)fixedShare, int.MaxValue
						);
				}
			}
			return totalResource;
		}

		/// <summary>
		/// Get the fairshare for the
		/// <see cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair.Schedulable
		/// 	"/>
		/// if it is fixed, -1 otherwise.
		/// The fairshare is fixed if either the maxShare is 0, weight is 0,
		/// or the Schedulable is not active for instantaneous fairshare.
		/// </summary>
		private static int GetFairShareIfFixed(Schedulable sched, bool isSteadyShare, ResourceType
			 type)
		{
			// Check if maxShare is 0
			if (GetResourceValue(sched.GetMaxShare(), type) <= 0)
			{
				return 0;
			}
			// For instantaneous fairshares, check if queue is active
			if (!isSteadyShare && (sched is FSQueue) && !((FSQueue)sched).IsActive())
			{
				return 0;
			}
			// Check if weight is 0
			if (sched.GetWeights().GetWeight(type) <= 0)
			{
				int minShare = GetResourceValue(sched.GetMinShare(), type);
				return (minShare <= 0) ? 0 : minShare;
			}
			return -1;
		}

		private static int GetResourceValue(Org.Apache.Hadoop.Yarn.Api.Records.Resource resource
			, ResourceType type)
		{
			switch (type)
			{
				case ResourceType.Memory:
				{
					return resource.GetMemory();
				}

				case ResourceType.Cpu:
				{
					return resource.GetVirtualCores();
				}

				default:
				{
					throw new ArgumentException("Invalid resource");
				}
			}
		}

		private static void SetResourceValue(int val, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 resource, ResourceType type)
		{
			switch (type)
			{
				case ResourceType.Memory:
				{
					resource.SetMemory(val);
					break;
				}

				case ResourceType.Cpu:
				{
					resource.SetVirtualCores(val);
					break;
				}

				default:
				{
					throw new ArgumentException("Invalid resource");
				}
			}
		}
	}
}
