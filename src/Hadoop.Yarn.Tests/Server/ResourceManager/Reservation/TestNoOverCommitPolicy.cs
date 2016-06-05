using System;
using NUnit.Framework;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation
{
	public class TestNoOverCommitPolicy
	{
		internal long step;

		internal long initTime;

		internal InMemoryPlan plan;

		internal ReservationAgent mAgent;

		internal Resource minAlloc;

		internal ResourceCalculator res;

		internal Org.Apache.Hadoop.Yarn.Api.Records.Resource maxAlloc;

		internal int totCont = 1000000;

		/// <exception cref="System.Exception"/>
		[SetUp]
		public virtual void Setup()
		{
			// 1 sec step
			step = 1000L;
			initTime = Runtime.CurrentTimeMillis();
			minAlloc = Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance(1024, 1);
			res = new DefaultResourceCalculator();
			maxAlloc = Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance(1024 * 8, 8);
			mAgent = Org.Mockito.Mockito.Mock<ReservationAgent>();
			ReservationSystemTestUtil testUtil = new ReservationSystemTestUtil();
			string reservationQ = testUtil.GetFullReservationQueueName();
			QueueMetrics rootQueueMetrics = Org.Mockito.Mockito.Mock<QueueMetrics>();
			Org.Apache.Hadoop.Yarn.Api.Records.Resource clusterResource = ReservationSystemTestUtil
				.CalculateClusterResource(totCont);
			ReservationSchedulerConfiguration conf = Org.Mockito.Mockito.Mock<ReservationSchedulerConfiguration
				>();
			NoOverCommitPolicy policy = new NoOverCommitPolicy();
			policy.Init(reservationQ, conf);
			plan = new InMemoryPlan(rootQueueMetrics, policy, mAgent, clusterResource, step, 
				res, minAlloc, maxAlloc, "dedicated", null, true);
		}

		public virtual int[] GenerateData(int length, int val)
		{
			int[] data = new int[length];
			for (int i = 0; i < length; i++)
			{
				data[i] = val;
			}
			return data;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation.Exceptions.PlanningException
		/// 	"/>
		[NUnit.Framework.Test]
		public virtual void TestSingleUserEasyFitPass()
		{
			// generate allocation that easily fit within resource constraints
			int[] f = GenerateData(3600, (int)Math.Ceil(0.2 * totCont));
			NUnit.Framework.Assert.IsTrue(plan.ToString(), plan.AddReservation(new InMemoryReservationAllocation
				(ReservationSystemTestUtil.GetNewReservationId(), null, "u1", "dedicated", initTime
				, initTime + f.Length, ReservationSystemTestUtil.GenerateAllocation(initTime, step
				, f), res, minAlloc)));
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation.Exceptions.PlanningException
		/// 	"/>
		[NUnit.Framework.Test]
		public virtual void TestSingleUserBarelyFitPass()
		{
			// generate allocation from single tenant that barely fit
			int[] f = GenerateData(3600, totCont);
			NUnit.Framework.Assert.IsTrue(plan.ToString(), plan.AddReservation(new InMemoryReservationAllocation
				(ReservationSystemTestUtil.GetNewReservationId(), null, "u1", "dedicated", initTime
				, initTime + f.Length, ReservationSystemTestUtil.GenerateAllocation(initTime, step
				, f), res, minAlloc)));
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation.Exceptions.PlanningException
		/// 	"/>
		public virtual void TestSingleFail()
		{
			// generate allocation from single tenant that exceed capacity
			int[] f = GenerateData(3600, (int)(1.1 * totCont));
			plan.AddReservation(new InMemoryReservationAllocation(ReservationSystemTestUtil.GetNewReservationId
				(), null, "u1", "dedicated", initTime, initTime + f.Length, ReservationSystemTestUtil
				.GenerateAllocation(initTime, step, f), res, minAlloc));
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation.Exceptions.PlanningException
		/// 	"/>
		public virtual void TestUserMismatch()
		{
			// generate allocation from single tenant that exceed capacity
			int[] f = GenerateData(3600, (int)(0.5 * totCont));
			ReservationId rid = ReservationSystemTestUtil.GetNewReservationId();
			plan.AddReservation(new InMemoryReservationAllocation(rid, null, "u1", "dedicated"
				, initTime, initTime + f.Length, ReservationSystemTestUtil.GenerateAllocation(initTime
				, step, f), res, minAlloc));
			// trying to update a reservation with a mismatching user
			plan.UpdateReservation(new InMemoryReservationAllocation(rid, null, "u2", "dedicated"
				, initTime, initTime + f.Length, ReservationSystemTestUtil.GenerateAllocation(initTime
				, step, f), res, minAlloc));
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation.Exceptions.PlanningException
		/// 	"/>
		[NUnit.Framework.Test]
		public virtual void TestMultiTenantPass()
		{
			// generate allocation from multiple tenants that barely fit in tot capacity
			int[] f = GenerateData(3600, (int)Math.Ceil(0.25 * totCont));
			for (int i = 0; i < 4; i++)
			{
				NUnit.Framework.Assert.IsTrue(plan.ToString(), plan.AddReservation(new InMemoryReservationAllocation
					(ReservationSystemTestUtil.GetNewReservationId(), null, "u" + i, "dedicated", initTime
					, initTime + f.Length, ReservationSystemTestUtil.GenerateAllocation(initTime, step
					, f), res, minAlloc)));
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation.Exceptions.PlanningException
		/// 	"/>
		public virtual void TestMultiTenantFail()
		{
			// generate allocation from multiple tenants that exceed tot capacity
			int[] f = GenerateData(3600, (int)Math.Ceil(0.25 * totCont));
			for (int i = 0; i < 5; i++)
			{
				NUnit.Framework.Assert.IsTrue(plan.ToString(), plan.AddReservation(new InMemoryReservationAllocation
					(ReservationSystemTestUtil.GetNewReservationId(), null, "u" + i, "dedicated", initTime
					, initTime + f.Length, ReservationSystemTestUtil.GenerateAllocation(initTime, step
					, f), res, minAlloc)));
			}
		}
	}
}
