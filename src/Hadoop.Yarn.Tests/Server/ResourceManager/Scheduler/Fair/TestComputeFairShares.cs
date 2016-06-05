using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Resource;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair.Policies;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair
{
	/// <summary>Exercise the computeFairShares method in SchedulingAlgorithms.</summary>
	public class TestComputeFairShares
	{
		private IList<Schedulable> scheds;

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			scheds = new AList<Schedulable>();
		}

		/// <summary>
		/// Basic test - pools with different demands that are all higher than their
		/// fair share (of 10 slots) should each get their fair share.
		/// </summary>
		[NUnit.Framework.Test]
		public virtual void TestEqualSharing()
		{
			scheds.AddItem(new FakeSchedulable());
			scheds.AddItem(new FakeSchedulable());
			scheds.AddItem(new FakeSchedulable());
			scheds.AddItem(new FakeSchedulable());
			ComputeFairShares.ComputeShares(scheds, Resources.CreateResource(40), ResourceType
				.Memory);
			VerifyMemoryShares(10, 10, 10, 10);
		}

		/// <summary>
		/// In this test, pool 4 has a smaller demand than the 40 / 4 = 10 slots that
		/// it would be assigned with equal sharing.
		/// </summary>
		/// <remarks>
		/// In this test, pool 4 has a smaller demand than the 40 / 4 = 10 slots that
		/// it would be assigned with equal sharing. It should only get the 3 slots
		/// it demands. The other pools must then split the remaining 37 slots, but
		/// pool 3, with 11 slots demanded, is now below its share of 37/3 ~= 12.3,
		/// so it only gets 11 slots. Pools 1 and 2 split the rest and get 13 each.
		/// </remarks>
		[NUnit.Framework.Test]
		public virtual void TestLowMaxShares()
		{
			scheds.AddItem(new FakeSchedulable(0, 100));
			scheds.AddItem(new FakeSchedulable(0, 50));
			scheds.AddItem(new FakeSchedulable(0, 11));
			scheds.AddItem(new FakeSchedulable(0, 3));
			ComputeFairShares.ComputeShares(scheds, Resources.CreateResource(40), ResourceType
				.Memory);
			VerifyMemoryShares(13, 13, 11, 3);
		}

		/// <summary>In this test, some pools have minimum shares set.</summary>
		/// <remarks>
		/// In this test, some pools have minimum shares set. Pool 1 has a min share
		/// of 20 so it gets 20 slots. Pool 2 also has a min share of 20, but its
		/// demand is only 10 so it can only get 10 slots. The remaining pools have
		/// 10 slots to split between them. Pool 4 gets 3 slots because its demand is
		/// only 3, and pool 3 gets the remaining 7 slots. Pool 4 also had a min share
		/// of 2 slots but this should not affect the outcome.
		/// </remarks>
		[NUnit.Framework.Test]
		public virtual void TestMinShares()
		{
			scheds.AddItem(new FakeSchedulable(20));
			scheds.AddItem(new FakeSchedulable(18));
			scheds.AddItem(new FakeSchedulable(0));
			scheds.AddItem(new FakeSchedulable(2));
			ComputeFairShares.ComputeShares(scheds, Resources.CreateResource(40), ResourceType
				.Memory);
			VerifyMemoryShares(20, 18, 0, 2);
		}

		/// <summary>Basic test for weighted shares with no minimum shares and no low demands.
		/// 	</summary>
		/// <remarks>
		/// Basic test for weighted shares with no minimum shares and no low demands.
		/// Each pool should get slots in proportion to its weight.
		/// </remarks>
		[NUnit.Framework.Test]
		public virtual void TestWeightedSharing()
		{
			scheds.AddItem(new FakeSchedulable(0, 2.0));
			scheds.AddItem(new FakeSchedulable(0, 1.0));
			scheds.AddItem(new FakeSchedulable(0, 1.0));
			scheds.AddItem(new FakeSchedulable(0, 0.5));
			ComputeFairShares.ComputeShares(scheds, Resources.CreateResource(45), ResourceType
				.Memory);
			VerifyMemoryShares(20, 10, 10, 5);
		}

		/// <summary>
		/// Weighted sharing test where pools 1 and 2 are now given lower demands than
		/// above.
		/// </summary>
		/// <remarks>
		/// Weighted sharing test where pools 1 and 2 are now given lower demands than
		/// above. Pool 1 stops at 10 slots, leaving 35. If the remaining pools split
		/// this into a 1:1:0.5 ratio, they would get 14:14:7 slots respectively, but
		/// pool 2's demand is only 11, so it only gets 11. The remaining 2 pools split
		/// the 24 slots left into a 1:0.5 ratio, getting 16 and 8 slots respectively.
		/// </remarks>
		[NUnit.Framework.Test]
		public virtual void TestWeightedSharingWithMaxShares()
		{
			scheds.AddItem(new FakeSchedulable(0, 10, 2.0));
			scheds.AddItem(new FakeSchedulable(0, 11, 1.0));
			scheds.AddItem(new FakeSchedulable(0, 30, 1.0));
			scheds.AddItem(new FakeSchedulable(0, 20, 0.5));
			ComputeFairShares.ComputeShares(scheds, Resources.CreateResource(45), ResourceType
				.Memory);
			VerifyMemoryShares(10, 11, 16, 8);
		}

		/// <summary>Weighted fair sharing test with min shares.</summary>
		/// <remarks>
		/// Weighted fair sharing test with min shares. As in the min share test above,
		/// pool 1 has a min share greater than its demand so it only gets its demand.
		/// Pool 3 has a min share of 15 even though its weight is very small, so it
		/// gets 15 slots. The remaining pools share the remaining 20 slots equally,
		/// getting 10 each. Pool 3's min share of 5 slots doesn't affect this.
		/// </remarks>
		[NUnit.Framework.Test]
		public virtual void TestWeightedSharingWithMinShares()
		{
			scheds.AddItem(new FakeSchedulable(20, 2.0));
			scheds.AddItem(new FakeSchedulable(0, 1.0));
			scheds.AddItem(new FakeSchedulable(5, 1.0));
			scheds.AddItem(new FakeSchedulable(15, 0.5));
			ComputeFairShares.ComputeShares(scheds, Resources.CreateResource(45), ResourceType
				.Memory);
			VerifyMemoryShares(20, 5, 5, 15);
		}

		/// <summary>
		/// Test that shares are computed accurately even when the number of slots is
		/// very large.
		/// </summary>
		[NUnit.Framework.Test]
		public virtual void TestLargeShares()
		{
			int million = 1000 * 1000;
			scheds.AddItem(new FakeSchedulable());
			scheds.AddItem(new FakeSchedulable());
			scheds.AddItem(new FakeSchedulable());
			scheds.AddItem(new FakeSchedulable());
			ComputeFairShares.ComputeShares(scheds, Resources.CreateResource(40 * million), ResourceType
				.Memory);
			VerifyMemoryShares(10 * million, 10 * million, 10 * million, 10 * million);
		}

		/// <summary>Test that being called on an empty list doesn't confuse the algorithm.</summary>
		[NUnit.Framework.Test]
		public virtual void TestEmptyList()
		{
			ComputeFairShares.ComputeShares(scheds, Resources.CreateResource(40), ResourceType
				.Memory);
			VerifyMemoryShares();
		}

		/// <summary>Test that CPU works as well as memory</summary>
		[NUnit.Framework.Test]
		public virtual void TestCPU()
		{
			scheds.AddItem(new FakeSchedulable(Resources.CreateResource(0, 20), new ResourceWeights
				(2.0f)));
			scheds.AddItem(new FakeSchedulable(Resources.CreateResource(0, 0), new ResourceWeights
				(1.0f)));
			scheds.AddItem(new FakeSchedulable(Resources.CreateResource(0, 5), new ResourceWeights
				(1.0f)));
			scheds.AddItem(new FakeSchedulable(Resources.CreateResource(0, 15), new ResourceWeights
				(0.5f)));
			ComputeFairShares.ComputeShares(scheds, Resources.CreateResource(0, 45), ResourceType
				.Cpu);
			VerifyCPUShares(20, 5, 5, 15);
		}

		/// <summary>Check that a given list of shares have been assigned to this.scheds.</summary>
		private void VerifyMemoryShares(params int[] shares)
		{
			NUnit.Framework.Assert.AreEqual(scheds.Count, shares.Length);
			for (int i = 0; i < shares.Length; i++)
			{
				NUnit.Framework.Assert.AreEqual(shares[i], scheds[i].GetFairShare().GetMemory());
			}
		}

		/// <summary>Check that a given list of shares have been assigned to this.scheds.</summary>
		private void VerifyCPUShares(params int[] shares)
		{
			NUnit.Framework.Assert.AreEqual(scheds.Count, shares.Length);
			for (int i = 0; i < shares.Length; i++)
			{
				NUnit.Framework.Assert.AreEqual(shares[i], scheds[i].GetFairShare().GetVirtualCores
					());
			}
		}
	}
}
