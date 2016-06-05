using System;
using System.Collections.Generic;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair.Policies;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair
{
	public abstract class SchedulingPolicy
	{
		private static readonly ConcurrentHashMap<Type, SchedulingPolicy> instances = new 
			ConcurrentHashMap<Type, SchedulingPolicy>();

		public static readonly SchedulingPolicy DefaultPolicy = GetInstance(typeof(FairSharePolicy
			));

		public const byte DepthLeaf = unchecked((byte)1);

		public const byte DepthIntermediate = unchecked((byte)2);

		public const byte DepthRoot = unchecked((byte)4);

		public const byte DepthParent = unchecked((byte)6);

		public const byte DepthAny = unchecked((byte)7);

		// Root and Intermediate
		/// <summary>
		/// Returns a
		/// <see cref="SchedulingPolicy"/>
		/// instance corresponding to the passed clazz
		/// </summary>
		public static SchedulingPolicy GetInstance(Type clazz)
		{
			SchedulingPolicy policy = ReflectionUtils.NewInstance(clazz, null);
			SchedulingPolicy policyRet = instances.PutIfAbsent(clazz, policy);
			if (policyRet != null)
			{
				return policyRet;
			}
			return policy;
		}

		/// <summary>
		/// Returns
		/// <see cref="SchedulingPolicy"/>
		/// instance corresponding to the
		/// <see cref="SchedulingPolicy"/>
		/// passed as a string. The policy can be "fair" for
		/// FairSharePolicy, "fifo" for FifoPolicy, or "drf" for
		/// DominantResourceFairnessPolicy. For a custom
		/// <see cref="SchedulingPolicy"/>
		/// s in the RM classpath, the policy should be
		/// canonical class name of the
		/// <see cref="SchedulingPolicy"/>
		/// .
		/// </summary>
		/// <param name="policy">canonical class name or "drf" or "fair" or "fifo"</param>
		/// <exception cref="AllocationConfigurationException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair.AllocationConfigurationException
		/// 	"/>
		public static SchedulingPolicy Parse(string policy)
		{
			Type clazz;
			string text = StringUtils.ToLowerCase(policy);
			if (Sharpen.Runtime.EqualsIgnoreCase(text, FairSharePolicy.Name))
			{
				clazz = typeof(FairSharePolicy);
			}
			else
			{
				if (Sharpen.Runtime.EqualsIgnoreCase(text, FifoPolicy.Name))
				{
					clazz = typeof(FifoPolicy);
				}
				else
				{
					if (Sharpen.Runtime.EqualsIgnoreCase(text, DominantResourceFairnessPolicy.Name))
					{
						clazz = typeof(DominantResourceFairnessPolicy);
					}
					else
					{
						try
						{
							clazz = Sharpen.Runtime.GetType(policy);
						}
						catch (TypeLoadException)
						{
							throw new AllocationConfigurationException(policy + " SchedulingPolicy class not found!"
								);
						}
					}
				}
			}
			if (!typeof(SchedulingPolicy).IsAssignableFrom(clazz))
			{
				throw new AllocationConfigurationException(policy + " does not extend SchedulingPolicy"
					);
			}
			return GetInstance(clazz);
		}

		public virtual void Initialize(Resource clusterCapacity)
		{
		}

		/// <returns>
		/// returns the name of
		/// <see cref="SchedulingPolicy"/>
		/// </returns>
		public abstract string GetName();

		/// <summary>
		/// Specifies the depths in the hierarchy, this
		/// <see cref="SchedulingPolicy"/>
		/// applies to
		/// </summary>
		/// <returns>
		/// depth equal to one of fields
		/// <see cref="SchedulingPolicy"/>
		/// #DEPTH_
		/// </returns>
		public abstract byte GetApplicableDepth();

		/// <summary>
		/// Checks if the specified
		/// <see cref="SchedulingPolicy"/>
		/// can be used for a queue at
		/// the specified depth in the hierarchy
		/// </summary>
		/// <param name="policy">
		/// 
		/// <see cref="SchedulingPolicy"/>
		/// we are checking the
		/// depth-applicability for
		/// </param>
		/// <param name="depth">queue's depth in the hierarchy</param>
		/// <returns>true if policy is applicable to passed depth, false otherwise</returns>
		public static bool IsApplicableTo(SchedulingPolicy policy, byte depth)
		{
			return ((policy.GetApplicableDepth() & depth) == depth) ? true : false;
		}

		/// <summary>
		/// The comparator returned by this method is to be used for sorting the
		/// <see cref="Schedulable"/>
		/// s in that queue.
		/// </summary>
		/// <returns>the comparator to sort by</returns>
		public abstract IComparer<Schedulable> GetComparator();

		/// <summary>
		/// Computes and updates the shares of
		/// <see cref="Schedulable"/>
		/// s as per
		/// the
		/// <see cref="SchedulingPolicy"/>
		/// , to be used later for scheduling decisions.
		/// The shares computed are instantaneous and only consider queues with
		/// running applications.
		/// </summary>
		/// <param name="schedulables">
		/// 
		/// <see cref="Schedulable"/>
		/// s whose shares are to be updated
		/// </param>
		/// <param name="totalResources">
		/// Total
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.Resource"/>
		/// s in the cluster
		/// </param>
		public abstract void ComputeShares<_T0>(ICollection<_T0> schedulables, Resource totalResources
			)
			where _T0 : Schedulable;

		/// <summary>
		/// Computes and updates the steady shares of
		/// <see cref="FSQueue"/>
		/// s as per the
		/// <see cref="SchedulingPolicy"/>
		/// . The steady share does not differentiate
		/// between queues with and without running applications under them. The
		/// steady share is not used for scheduling, it is displayed on the Web UI
		/// for better visibility.
		/// </summary>
		/// <param name="queues">
		/// 
		/// <see cref="FSQueue"/>
		/// s whose shares are to be updated
		/// </param>
		/// <param name="totalResources">
		/// Total
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.Resource"/>
		/// s in the cluster
		/// </param>
		public abstract void ComputeSteadyShares<_T0>(ICollection<_T0> queues, Resource totalResources
			)
			where _T0 : FSQueue;

		/// <summary>Check if the resource usage is over the fair share under this policy</summary>
		/// <param name="usage">
		/// 
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.Resource"/>
		/// the resource usage
		/// </param>
		/// <param name="fairShare">
		/// 
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.Resource"/>
		/// the fair share
		/// </param>
		/// <returns>true if check passes (is over) or false otherwise</returns>
		public abstract bool CheckIfUsageOverFairShare(Resource usage, Resource fairShare
			);

		/// <summary>Check if a leaf queue's AM resource usage over its limit under this policy
		/// 	</summary>
		/// <param name="usage">
		/// 
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.Resource"/>
		/// the resource used by application masters
		/// </param>
		/// <param name="maxAMResource">
		/// 
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.Resource"/>
		/// the maximum allowed resource for
		/// application masters
		/// </param>
		/// <returns>true if AM resource usage is over the limit</returns>
		public abstract bool CheckIfAMResourceUsageOverLimit(Resource usage, Resource maxAMResource
			);

		/// <summary>
		/// Get headroom by calculating the min of <code>clusterAvailable</code> and
		/// (<code>queueFairShare</code> - <code>queueUsage</code>) resources that are
		/// applicable to this policy.
		/// </summary>
		/// <remarks>
		/// Get headroom by calculating the min of <code>clusterAvailable</code> and
		/// (<code>queueFairShare</code> - <code>queueUsage</code>) resources that are
		/// applicable to this policy. For eg if only memory then leave other
		/// resources such as CPU to same as clusterAvailable.
		/// </remarks>
		/// <param name="queueFairShare">fairshare in the queue</param>
		/// <param name="queueUsage">resources used in the queue</param>
		/// <param name="maxAvailable">available resource in cluster for this queue</param>
		/// <returns>calculated headroom</returns>
		public abstract Resource GetHeadroom(Resource queueFairShare, Resource queueUsage
			, Resource maxAvailable);
	}
}
