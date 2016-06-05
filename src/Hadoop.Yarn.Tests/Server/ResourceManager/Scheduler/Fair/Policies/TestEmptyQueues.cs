using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair.Policies
{
	public class TestEmptyQueues
	{
		private ICollection<Schedulable> schedulables;

		[SetUp]
		public virtual void Setup()
		{
			schedulables = new AList<Schedulable>();
		}

		private void TestComputeShares(SchedulingPolicy policy)
		{
			policy.ComputeShares(schedulables, Resources.None());
		}

		public virtual void TestFifoPolicy()
		{
			TestComputeShares(SchedulingPolicy.GetInstance(typeof(FifoPolicy)));
		}

		public virtual void TestFairSharePolicy()
		{
			TestComputeShares(SchedulingPolicy.GetInstance(typeof(FairSharePolicy)));
		}

		public virtual void TestDRFPolicy()
		{
			TestComputeShares(SchedulingPolicy.GetInstance(typeof(DominantResourceFairnessPolicy
				)));
		}
	}
}
