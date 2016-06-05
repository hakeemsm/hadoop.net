using System.Collections.Generic;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Org.Mockito;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation
{
	public class TestSimpleCapacityReplanner
	{
		/// <exception cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation.Exceptions.PlanningException
		/// 	"/>
		[NUnit.Framework.Test]
		public virtual void TestReplanningPlanCapacityLoss()
		{
			Resource clusterCapacity = Resource.NewInstance(100 * 1024, 10);
			Resource minAlloc = Resource.NewInstance(1024, 1);
			Resource maxAlloc = Resource.NewInstance(1024 * 8, 8);
			ResourceCalculator res = new DefaultResourceCalculator();
			long step = 1L;
			Clock clock = Org.Mockito.Mockito.Mock<Clock>();
			ReservationAgent agent = Org.Mockito.Mockito.Mock<ReservationAgent>();
			SharingPolicy policy = new NoOverCommitPolicy();
			policy.Init("root.dedicated", null);
			QueueMetrics queueMetrics = Org.Mockito.Mockito.Mock<QueueMetrics>();
			Org.Mockito.Mockito.When(clock.GetTime()).ThenReturn(0L);
			SimpleCapacityReplanner enf = new SimpleCapacityReplanner(clock);
			ReservationSchedulerConfiguration conf = Org.Mockito.Mockito.Mock<ReservationSchedulerConfiguration
				>();
			Org.Mockito.Mockito.When(conf.GetEnforcementWindow(Matchers.Any<string>())).ThenReturn
				(6L);
			enf.Init("blah", conf);
			// Initialize the plan with more resources
			InMemoryPlan plan = new InMemoryPlan(queueMetrics, policy, agent, clusterCapacity
				, step, res, minAlloc, maxAlloc, "dedicated", enf, true, clock);
			// add reservation filling the plan (separating them 1ms, so we are sure
			// s2 follows s1 on acceptance
			long ts = Runtime.CurrentTimeMillis();
			ReservationId r1 = ReservationId.NewInstance(ts, 1);
			int[] f5 = new int[] { 20, 20, 20, 20, 20 };
			NUnit.Framework.Assert.IsTrue(plan.ToString(), plan.AddReservation(new InMemoryReservationAllocation
				(r1, null, "u3", "dedicated", 0, 0 + f5.Length, GenerateAllocation(0, f5), res, 
				minAlloc)));
			Org.Mockito.Mockito.When(clock.GetTime()).ThenReturn(1L);
			ReservationId r2 = ReservationId.NewInstance(ts, 2);
			NUnit.Framework.Assert.IsTrue(plan.ToString(), plan.AddReservation(new InMemoryReservationAllocation
				(r2, null, "u4", "dedicated", 0, 0 + f5.Length, GenerateAllocation(0, f5), res, 
				minAlloc)));
			Org.Mockito.Mockito.When(clock.GetTime()).ThenReturn(2L);
			ReservationId r3 = ReservationId.NewInstance(ts, 3);
			NUnit.Framework.Assert.IsTrue(plan.ToString(), plan.AddReservation(new InMemoryReservationAllocation
				(r3, null, "u5", "dedicated", 0, 0 + f5.Length, GenerateAllocation(0, f5), res, 
				minAlloc)));
			Org.Mockito.Mockito.When(clock.GetTime()).ThenReturn(3L);
			ReservationId r4 = ReservationId.NewInstance(ts, 4);
			NUnit.Framework.Assert.IsTrue(plan.ToString(), plan.AddReservation(new InMemoryReservationAllocation
				(r4, null, "u6", "dedicated", 0, 0 + f5.Length, GenerateAllocation(0, f5), res, 
				minAlloc)));
			Org.Mockito.Mockito.When(clock.GetTime()).ThenReturn(4L);
			ReservationId r5 = ReservationId.NewInstance(ts, 5);
			NUnit.Framework.Assert.IsTrue(plan.ToString(), plan.AddReservation(new InMemoryReservationAllocation
				(r5, null, "u7", "dedicated", 0, 0 + f5.Length, GenerateAllocation(0, f5), res, 
				minAlloc)));
			int[] f6 = new int[] { 50, 50, 50, 50, 50 };
			ReservationId r6 = ReservationId.NewInstance(ts, 6);
			NUnit.Framework.Assert.IsTrue(plan.ToString(), plan.AddReservation(new InMemoryReservationAllocation
				(r6, null, "u3", "dedicated", 10, 10 + f6.Length, GenerateAllocation(10, f6), res
				, minAlloc)));
			Org.Mockito.Mockito.When(clock.GetTime()).ThenReturn(6L);
			ReservationId r7 = ReservationId.NewInstance(ts, 7);
			NUnit.Framework.Assert.IsTrue(plan.ToString(), plan.AddReservation(new InMemoryReservationAllocation
				(r7, null, "u4", "dedicated", 10, 10 + f6.Length, GenerateAllocation(10, f6), res
				, minAlloc)));
			// remove some of the resources (requires replanning)
			plan.SetTotalCapacity(Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance(70 
				* 1024, 70));
			Org.Mockito.Mockito.When(clock.GetTime()).ThenReturn(0L);
			// run the replanner
			enf.Plan(plan, null);
			// check which reservation are still present
			NUnit.Framework.Assert.IsNotNull(plan.GetReservationById(r1));
			NUnit.Framework.Assert.IsNotNull(plan.GetReservationById(r2));
			NUnit.Framework.Assert.IsNotNull(plan.GetReservationById(r3));
			NUnit.Framework.Assert.IsNotNull(plan.GetReservationById(r6));
			NUnit.Framework.Assert.IsNotNull(plan.GetReservationById(r7));
			// and which ones are removed
			NUnit.Framework.Assert.IsNull(plan.GetReservationById(r4));
			NUnit.Framework.Assert.IsNull(plan.GetReservationById(r5));
			// check resources at each moment in time no more exceed capacity
			for (int i = 0; i < 20; i++)
			{
				int tot = 0;
				foreach (ReservationAllocation r in plan.GetReservationsAtTime(i))
				{
					tot = r.GetResourcesAtTime(i).GetMemory();
				}
				NUnit.Framework.Assert.IsTrue(tot <= 70 * 1024);
			}
		}

		private IDictionary<ReservationInterval, ReservationRequest> GenerateAllocation(int
			 startTime, int[] alloc)
		{
			IDictionary<ReservationInterval, ReservationRequest> req = new SortedDictionary<ReservationInterval
				, ReservationRequest>();
			for (int i = 0; i < alloc.Length; i++)
			{
				req[new ReservationInterval(startTime + i, startTime + i + 1)] = ReservationRequest
					.NewInstance(Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance(1024, 1), alloc
					[i]);
			}
			return req;
		}
	}
}
