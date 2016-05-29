using System.Collections.Generic;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Resource;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair;
using Org.Apache.Hadoop.Yarn.Server.Utils;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair.Policies
{
	/// <summary>
	/// comparator.compare(sched1, sched2) &lt; 0 means that sched1 should get a
	/// container before sched2
	/// </summary>
	public class TestDominantResourceFairnessPolicy
	{
		private IComparer<Schedulable> CreateComparator(int clusterMem, int clusterCpu)
		{
			DominantResourceFairnessPolicy policy = new DominantResourceFairnessPolicy();
			policy.Initialize(BuilderUtils.NewResource(clusterMem, clusterCpu));
			return policy.GetComparator();
		}

		private Schedulable CreateSchedulable(int memUsage, int cpuUsage)
		{
			return CreateSchedulable(memUsage, cpuUsage, ResourceWeights.Neutral, 0, 0);
		}

		private Schedulable CreateSchedulable(int memUsage, int cpuUsage, int minMemShare
			, int minCpuShare)
		{
			return CreateSchedulable(memUsage, cpuUsage, ResourceWeights.Neutral, minMemShare
				, minCpuShare);
		}

		private Schedulable CreateSchedulable(int memUsage, int cpuUsage, ResourceWeights
			 weights)
		{
			return CreateSchedulable(memUsage, cpuUsage, weights, 0, 0);
		}

		private Schedulable CreateSchedulable(int memUsage, int cpuUsage, ResourceWeights
			 weights, int minMemShare, int minCpuShare)
		{
			Org.Apache.Hadoop.Yarn.Api.Records.Resource usage = BuilderUtils.NewResource(memUsage
				, cpuUsage);
			Org.Apache.Hadoop.Yarn.Api.Records.Resource minShare = BuilderUtils.NewResource(minMemShare
				, minCpuShare);
			return new FakeSchedulable(minShare, Resources.CreateResource(int.MaxValue, int.MaxValue
				), weights, Resources.None(), usage, 0l);
		}

		[NUnit.Framework.Test]
		public virtual void TestSameDominantResource()
		{
			NUnit.Framework.Assert.IsTrue(CreateComparator(8000, 4).Compare(CreateSchedulable
				(1000, 1), CreateSchedulable(2000, 1)) < 0);
		}

		[NUnit.Framework.Test]
		public virtual void TestDifferentDominantResource()
		{
			NUnit.Framework.Assert.IsTrue(CreateComparator(8000, 8).Compare(CreateSchedulable
				(4000, 3), CreateSchedulable(2000, 5)) < 0);
		}

		[NUnit.Framework.Test]
		public virtual void TestOneIsNeedy()
		{
			NUnit.Framework.Assert.IsTrue(CreateComparator(8000, 8).Compare(CreateSchedulable
				(2000, 5, 0, 6), CreateSchedulable(4000, 3, 0, 0)) < 0);
		}

		[NUnit.Framework.Test]
		public virtual void TestBothAreNeedy()
		{
			NUnit.Framework.Assert.IsTrue(CreateComparator(8000, 100).Compare(CreateSchedulable
				(2000, 5), CreateSchedulable(4000, 3)) < 0);
			// dominant share is 2000/8000
			// dominant share is 4000/8000
			NUnit.Framework.Assert.IsTrue(CreateComparator(8000, 100).Compare(CreateSchedulable
				(2000, 5, 3000, 6), CreateSchedulable(4000, 3, 5000, 4)) < 0);
		}

		// dominant min share is 2/3
		// dominant min share is 4/5
		[NUnit.Framework.Test]
		public virtual void TestEvenWeightsSameDominantResource()
		{
			NUnit.Framework.Assert.IsTrue(CreateComparator(8000, 8).Compare(CreateSchedulable
				(3000, 1, new ResourceWeights(2.0f)), CreateSchedulable(2000, 1)) < 0);
			NUnit.Framework.Assert.IsTrue(CreateComparator(8000, 8).Compare(CreateSchedulable
				(1000, 3, new ResourceWeights(2.0f)), CreateSchedulable(1000, 2)) < 0);
		}

		[NUnit.Framework.Test]
		public virtual void TestEvenWeightsDifferentDominantResource()
		{
			NUnit.Framework.Assert.IsTrue(CreateComparator(8000, 8).Compare(CreateSchedulable
				(1000, 3, new ResourceWeights(2.0f)), CreateSchedulable(2000, 1)) < 0);
			NUnit.Framework.Assert.IsTrue(CreateComparator(8000, 8).Compare(CreateSchedulable
				(3000, 1, new ResourceWeights(2.0f)), CreateSchedulable(1000, 2)) < 0);
		}

		[NUnit.Framework.Test]
		public virtual void TestUnevenWeightsSameDominantResource()
		{
			NUnit.Framework.Assert.IsTrue(CreateComparator(8000, 8).Compare(CreateSchedulable
				(3000, 1, new ResourceWeights(2.0f, 1.0f)), CreateSchedulable(2000, 1)) < 0);
			NUnit.Framework.Assert.IsTrue(CreateComparator(8000, 8).Compare(CreateSchedulable
				(1000, 3, new ResourceWeights(1.0f, 2.0f)), CreateSchedulable(1000, 2)) < 0);
		}

		[NUnit.Framework.Test]
		public virtual void TestUnevenWeightsDifferentDominantResource()
		{
			NUnit.Framework.Assert.IsTrue(CreateComparator(8000, 8).Compare(CreateSchedulable
				(1000, 3, new ResourceWeights(1.0f, 2.0f)), CreateSchedulable(2000, 1)) < 0);
			NUnit.Framework.Assert.IsTrue(CreateComparator(8000, 8).Compare(CreateSchedulable
				(3000, 1, new ResourceWeights(2.0f, 1.0f)), CreateSchedulable(1000, 2)) < 0);
		}

		[NUnit.Framework.Test]
		public virtual void TestCalculateShares()
		{
			Org.Apache.Hadoop.Yarn.Api.Records.Resource used = Resources.CreateResource(10, 5
				);
			Org.Apache.Hadoop.Yarn.Api.Records.Resource capacity = Resources.CreateResource(100
				, 10);
			ResourceType[] resourceOrder = new ResourceType[2];
			ResourceWeights shares = new ResourceWeights();
			DominantResourceFairnessPolicy.DominantResourceFairnessComparator comparator = new 
				DominantResourceFairnessPolicy.DominantResourceFairnessComparator();
			comparator.CalculateShares(used, capacity, shares, resourceOrder, ResourceWeights
				.Neutral);
			NUnit.Framework.Assert.AreEqual(.1, shares.GetWeight(ResourceType.Memory), .00001
				);
			NUnit.Framework.Assert.AreEqual(.5, shares.GetWeight(ResourceType.Cpu), .00001);
			NUnit.Framework.Assert.AreEqual(ResourceType.Cpu, resourceOrder[0]);
			NUnit.Framework.Assert.AreEqual(ResourceType.Memory, resourceOrder[1]);
		}
	}
}
