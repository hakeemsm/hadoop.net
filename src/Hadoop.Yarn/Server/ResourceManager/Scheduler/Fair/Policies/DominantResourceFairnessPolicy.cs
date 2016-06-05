using System;
using System.Collections.Generic;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Resource;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair.Policies
{
	/// <summary>Makes scheduling decisions by trying to equalize dominant resource usage.
	/// 	</summary>
	/// <remarks>
	/// Makes scheduling decisions by trying to equalize dominant resource usage.
	/// A schedulable's dominant resource usage is the largest ratio of resource
	/// usage to capacity among the resource types it is using.
	/// </remarks>
	public class DominantResourceFairnessPolicy : SchedulingPolicy
	{
		public const string Name = "DRF";

		private DominantResourceFairnessPolicy.DominantResourceFairnessComparator comparator
			 = new DominantResourceFairnessPolicy.DominantResourceFairnessComparator();

		public override string GetName()
		{
			return Name;
		}

		public override byte GetApplicableDepth()
		{
			return SchedulingPolicy.DepthAny;
		}

		public override IComparer<Schedulable> GetComparator()
		{
			return comparator;
		}

		public override void ComputeShares<_T0>(ICollection<_T0> schedulables, Resource totalResources
			)
		{
			foreach (ResourceType type in ResourceType.Values())
			{
				ComputeFairShares.ComputeShares(schedulables, totalResources, type);
			}
		}

		public override void ComputeSteadyShares<_T0>(ICollection<_T0> queues, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 totalResources)
		{
			foreach (ResourceType type in ResourceType.Values())
			{
				ComputeFairShares.ComputeSteadyShares(queues, totalResources, type);
			}
		}

		public override bool CheckIfUsageOverFairShare(Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 usage, Org.Apache.Hadoop.Yarn.Api.Records.Resource fairShare)
		{
			return !Resources.FitsIn(usage, fairShare);
		}

		public override bool CheckIfAMResourceUsageOverLimit(Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 usage, Org.Apache.Hadoop.Yarn.Api.Records.Resource maxAMResource)
		{
			return !Resources.FitsIn(usage, maxAMResource);
		}

		public override Org.Apache.Hadoop.Yarn.Api.Records.Resource GetHeadroom(Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 queueFairShare, Org.Apache.Hadoop.Yarn.Api.Records.Resource queueUsage, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 maxAvailable)
		{
			int queueAvailableMemory = Math.Max(queueFairShare.GetMemory() - queueUsage.GetMemory
				(), 0);
			int queueAvailableCPU = Math.Max(queueFairShare.GetVirtualCores() - queueUsage.GetVirtualCores
				(), 0);
			Org.Apache.Hadoop.Yarn.Api.Records.Resource headroom = Resources.CreateResource(Math
				.Min(maxAvailable.GetMemory(), queueAvailableMemory), Math.Min(maxAvailable.GetVirtualCores
				(), queueAvailableCPU));
			return headroom;
		}

		public override void Initialize(Org.Apache.Hadoop.Yarn.Api.Records.Resource clusterCapacity
			)
		{
			comparator.SetClusterCapacity(clusterCapacity);
		}

		public class DominantResourceFairnessComparator : IComparer<Schedulable>
		{
			private static readonly int NumResources = ResourceType.Values().Length;

			private Org.Apache.Hadoop.Yarn.Api.Records.Resource clusterCapacity;

			public virtual void SetClusterCapacity(Org.Apache.Hadoop.Yarn.Api.Records.Resource
				 clusterCapacity)
			{
				this.clusterCapacity = clusterCapacity;
			}

			public virtual int Compare(Schedulable s1, Schedulable s2)
			{
				ResourceWeights sharesOfCluster1 = new ResourceWeights();
				ResourceWeights sharesOfCluster2 = new ResourceWeights();
				ResourceWeights sharesOfMinShare1 = new ResourceWeights();
				ResourceWeights sharesOfMinShare2 = new ResourceWeights();
				ResourceType[] resourceOrder1 = new ResourceType[NumResources];
				ResourceType[] resourceOrder2 = new ResourceType[NumResources];
				// Calculate shares of the cluster for each resource both schedulables.
				CalculateShares(s1.GetResourceUsage(), clusterCapacity, sharesOfCluster1, resourceOrder1
					, s1.GetWeights());
				CalculateShares(s1.GetResourceUsage(), s1.GetMinShare(), sharesOfMinShare1, null, 
					ResourceWeights.Neutral);
				CalculateShares(s2.GetResourceUsage(), clusterCapacity, sharesOfCluster2, resourceOrder2
					, s2.GetWeights());
				CalculateShares(s2.GetResourceUsage(), s2.GetMinShare(), sharesOfMinShare2, null, 
					ResourceWeights.Neutral);
				// A queue is needy for its min share if its dominant resource
				// (with respect to the cluster capacity) is below its configured min share
				// for that resource
				bool s1Needy = sharesOfMinShare1.GetWeight(resourceOrder1[0]) < 1.0f;
				bool s2Needy = sharesOfMinShare2.GetWeight(resourceOrder2[0]) < 1.0f;
				int res = 0;
				if (!s2Needy && !s1Needy)
				{
					res = CompareShares(sharesOfCluster1, sharesOfCluster2, resourceOrder1, resourceOrder2
						);
				}
				else
				{
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
							// both are needy below min share
							res = CompareShares(sharesOfMinShare1, sharesOfMinShare2, resourceOrder1, resourceOrder2
								);
						}
					}
				}
				if (res == 0)
				{
					// Apps are tied in fairness ratio. Break the tie by submit time.
					res = (int)(s1.GetStartTime() - s2.GetStartTime());
				}
				return res;
			}

			/// <summary>Calculates and orders a resource's share of a pool in terms of two vectors.
			/// 	</summary>
			/// <remarks>
			/// Calculates and orders a resource's share of a pool in terms of two vectors.
			/// The shares vector contains, for each resource, the fraction of the pool that
			/// it takes up.  The resourceOrder vector contains an ordering of resources
			/// by largest share.  So if resource=<10 MB, 5 CPU>, and pool=<100 MB, 10 CPU>,
			/// shares will be [.1, .5] and resourceOrder will be [CPU, MEMORY].
			/// </remarks>
			internal virtual void CalculateShares(Org.Apache.Hadoop.Yarn.Api.Records.Resource
				 resource, Org.Apache.Hadoop.Yarn.Api.Records.Resource pool, ResourceWeights shares
				, ResourceType[] resourceOrder, ResourceWeights weights)
			{
				shares.SetWeight(ResourceType.Memory, (float)resource.GetMemory() / (pool.GetMemory
					() * weights.GetWeight(ResourceType.Memory)));
				shares.SetWeight(ResourceType.Cpu, (float)resource.GetVirtualCores() / (pool.GetVirtualCores
					() * weights.GetWeight(ResourceType.Cpu)));
				// sort order vector by resource share
				if (resourceOrder != null)
				{
					if (shares.GetWeight(ResourceType.Memory) > shares.GetWeight(ResourceType.Cpu))
					{
						resourceOrder[0] = ResourceType.Memory;
						resourceOrder[1] = ResourceType.Cpu;
					}
					else
					{
						resourceOrder[0] = ResourceType.Cpu;
						resourceOrder[1] = ResourceType.Memory;
					}
				}
			}

			private int CompareShares(ResourceWeights shares1, ResourceWeights shares2, ResourceType
				[] resourceOrder1, ResourceType[] resourceOrder2)
			{
				for (int i = 0; i < resourceOrder1.Length; i++)
				{
					int ret = (int)Math.Signum(shares1.GetWeight(resourceOrder1[i]) - shares2.GetWeight
						(resourceOrder2[i]));
					if (ret != 0)
					{
						return ret;
					}
				}
				return 0;
			}
		}
	}
}
