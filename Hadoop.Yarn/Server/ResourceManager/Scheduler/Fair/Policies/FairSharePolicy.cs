using System;
using System.Collections.Generic;
using Com.Google.Common.Annotations;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Resource;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair.Policies
{
	/// <summary>Makes scheduling decisions by trying to equalize shares of memory.</summary>
	public class FairSharePolicy : SchedulingPolicy
	{
		[VisibleForTesting]
		public const string Name = "fair";

		private static readonly DefaultResourceCalculator ResourceCalculator = new DefaultResourceCalculator
			();

		private FairSharePolicy.FairShareComparator comparator = new FairSharePolicy.FairShareComparator
			();

		public override string GetName()
		{
			return Name;
		}

		/// <summary>Compare Schedulables via weighted fair sharing.</summary>
		/// <remarks>
		/// Compare Schedulables via weighted fair sharing. In addition, Schedulables
		/// below their min share get priority over those whose min share is met.
		/// Schedulables below their min share are compared by how far below it they
		/// are as a ratio. For example, if job A has 8 out of a min share of 10 tasks
		/// and job B has 50 out of a min share of 100, then job B is scheduled next,
		/// because B is at 50% of its min share and A is at 80% of its min share.
		/// Schedulables above their min share are compared by (runningTasks / weight).
		/// If all weights are equal, slots are given to the job with the fewest tasks;
		/// otherwise, jobs with more weight get proportionally more slots.
		/// </remarks>
		[System.Serializable]
		private class FairShareComparator : IComparer<Schedulable>
		{
			private const long serialVersionUID = 5564969375856699313L;

			private static readonly Org.Apache.Hadoop.Yarn.Api.Records.Resource One = Resources
				.CreateResource(1);

			public virtual int Compare(Schedulable s1, Schedulable s2)
			{
				double minShareRatio1;
				double minShareRatio2;
				double useToWeightRatio1;
				double useToWeightRatio2;
				Org.Apache.Hadoop.Yarn.Api.Records.Resource minShare1 = Resources.Min(ResourceCalculator
					, null, s1.GetMinShare(), s1.GetDemand());
				Org.Apache.Hadoop.Yarn.Api.Records.Resource minShare2 = Resources.Min(ResourceCalculator
					, null, s2.GetMinShare(), s2.GetDemand());
				bool s1Needy = Resources.LessThan(ResourceCalculator, null, s1.GetResourceUsage()
					, minShare1);
				bool s2Needy = Resources.LessThan(ResourceCalculator, null, s2.GetResourceUsage()
					, minShare2);
				minShareRatio1 = (double)s1.GetResourceUsage().GetMemory() / Resources.Max(ResourceCalculator
					, null, minShare1, One).GetMemory();
				minShareRatio2 = (double)s2.GetResourceUsage().GetMemory() / Resources.Max(ResourceCalculator
					, null, minShare2, One).GetMemory();
				useToWeightRatio1 = s1.GetResourceUsage().GetMemory() / s1.GetWeights().GetWeight
					(ResourceType.Memory);
				useToWeightRatio2 = s2.GetResourceUsage().GetMemory() / s2.GetWeights().GetWeight
					(ResourceType.Memory);
				int res = 0;
				if (s1Needy && !s2Needy)
				{
					res = -1;
				}
				else
				{
					if (s2Needy && !s1Needy)
					{
						res = 1;
					}
					else
					{
						if (s1Needy && s2Needy)
						{
							res = (int)Math.Signum(minShareRatio1 - minShareRatio2);
						}
						else
						{
							// Neither schedulable is needy
							res = (int)Math.Signum(useToWeightRatio1 - useToWeightRatio2);
						}
					}
				}
				if (res == 0)
				{
					// Apps are tied in fairness ratio. Break the tie by submit time and job
					// name to get a deterministic ordering, which is useful for unit tests.
					res = (int)Math.Signum(s1.GetStartTime() - s2.GetStartTime());
					if (res == 0)
					{
						res = string.CompareOrdinal(s1.GetName(), s2.GetName());
					}
				}
				return res;
			}
		}

		public override IComparer<Schedulable> GetComparator()
		{
			return comparator;
		}

		public override Org.Apache.Hadoop.Yarn.Api.Records.Resource GetHeadroom(Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 queueFairShare, Org.Apache.Hadoop.Yarn.Api.Records.Resource queueUsage, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 maxAvailable)
		{
			int queueAvailableMemory = Math.Max(queueFairShare.GetMemory() - queueUsage.GetMemory
				(), 0);
			Org.Apache.Hadoop.Yarn.Api.Records.Resource headroom = Resources.CreateResource(Math
				.Min(maxAvailable.GetMemory(), queueAvailableMemory), maxAvailable.GetVirtualCores
				());
			return headroom;
		}

		public override void ComputeShares<_T0>(ICollection<_T0> schedulables, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 totalResources)
		{
			ComputeFairShares.ComputeShares(schedulables, totalResources, ResourceType.Memory
				);
		}

		public override void ComputeSteadyShares<_T0>(ICollection<_T0> queues, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 totalResources)
		{
			ComputeFairShares.ComputeSteadyShares(queues, totalResources, ResourceType.Memory
				);
		}

		public override bool CheckIfUsageOverFairShare(Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 usage, Org.Apache.Hadoop.Yarn.Api.Records.Resource fairShare)
		{
			return Resources.GreaterThan(ResourceCalculator, null, usage, fairShare);
		}

		public override bool CheckIfAMResourceUsageOverLimit(Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 usage, Org.Apache.Hadoop.Yarn.Api.Records.Resource maxAMResource)
		{
			return usage.GetMemory() > maxAMResource.GetMemory();
		}

		public override byte GetApplicableDepth()
		{
			return SchedulingPolicy.DepthAny;
		}
	}
}
