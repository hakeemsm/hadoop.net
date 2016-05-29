using System;
using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation.Exceptions;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation
{
	public class TestCapacityOverTimePolicy
	{
		internal long timeWindow;

		internal long step;

		internal float avgConstraint;

		internal float instConstraint;

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
			// 24h window
			timeWindow = 86400000L;
			// 1 sec step
			step = 1000L;
			// 25% avg cap on capacity
			avgConstraint = 25;
			// 70% instantaneous cap on capacity
			instConstraint = 70;
			initTime = Runtime.CurrentTimeMillis();
			minAlloc = Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance(1024, 1);
			res = new DefaultResourceCalculator();
			maxAlloc = Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance(1024 * 8, 8);
			mAgent = Org.Mockito.Mockito.Mock<ReservationAgent>();
			ReservationSystemTestUtil testUtil = new ReservationSystemTestUtil();
			QueueMetrics rootQueueMetrics = Org.Mockito.Mockito.Mock<QueueMetrics>();
			string reservationQ = testUtil.GetFullReservationQueueName();
			Org.Apache.Hadoop.Yarn.Api.Records.Resource clusterResource = ReservationSystemTestUtil
				.CalculateClusterResource(totCont);
			ReservationSchedulerConfiguration conf = ReservationSystemTestUtil.CreateConf(reservationQ
				, timeWindow, instConstraint, avgConstraint);
			CapacityOverTimePolicy policy = new CapacityOverTimePolicy();
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
		public virtual void TestSimplePass()
		{
			// generate allocation that simply fit within all constraints
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
		public virtual void TestSimplePass2()
		{
			// generate allocation from single tenant that exceed avg momentarily but
			// fit within
			// max instantanesou
			int[] f = GenerateData(3600, (int)Math.Ceil(0.69 * totCont));
			NUnit.Framework.Assert.IsTrue(plan.ToString(), plan.AddReservation(new InMemoryReservationAllocation
				(ReservationSystemTestUtil.GetNewReservationId(), null, "u1", "dedicated", initTime
				, initTime + f.Length, ReservationSystemTestUtil.GenerateAllocation(initTime, step
				, f), res, minAlloc)));
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

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation.Exceptions.PlanningException
		/// 	"/>
		public virtual void TestInstFail()
		{
			// generate allocation that exceed the instantaneous cap single-show
			int[] f = GenerateData(3600, (int)Math.Ceil(0.71 * totCont));
			NUnit.Framework.Assert.IsTrue(plan.ToString(), plan.AddReservation(new InMemoryReservationAllocation
				(ReservationSystemTestUtil.GetNewReservationId(), null, "u1", "dedicated", initTime
				, initTime + f.Length, ReservationSystemTestUtil.GenerateAllocation(initTime, step
				, f), res, minAlloc)));
			NUnit.Framework.Assert.Fail("should not have accepted this");
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation.Exceptions.PlanningException
		/// 	"/>
		[NUnit.Framework.Test]
		public virtual void TestInstFailBySum()
		{
			// generate allocation that exceed the instantaneous cap by sum
			int[] f = GenerateData(3600, (int)Math.Ceil(0.3 * totCont));
			NUnit.Framework.Assert.IsTrue(plan.ToString(), plan.AddReservation(new InMemoryReservationAllocation
				(ReservationSystemTestUtil.GetNewReservationId(), null, "u1", "dedicated", initTime
				, initTime + f.Length, ReservationSystemTestUtil.GenerateAllocation(initTime, step
				, f), res, minAlloc)));
			NUnit.Framework.Assert.IsTrue(plan.ToString(), plan.AddReservation(new InMemoryReservationAllocation
				(ReservationSystemTestUtil.GetNewReservationId(), null, "u1", "dedicated", initTime
				, initTime + f.Length, ReservationSystemTestUtil.GenerateAllocation(initTime, step
				, f), res, minAlloc)));
			try
			{
				NUnit.Framework.Assert.IsTrue(plan.ToString(), plan.AddReservation(new InMemoryReservationAllocation
					(ReservationSystemTestUtil.GetNewReservationId(), null, "u1", "dedicated", initTime
					, initTime + f.Length, ReservationSystemTestUtil.GenerateAllocation(initTime, step
					, f), res, minAlloc)));
				NUnit.Framework.Assert.Fail();
			}
			catch (PlanningQuotaException)
			{
			}
		}

		// expected
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation.Exceptions.PlanningException
		/// 	"/>
		public virtual void TestFailAvg()
		{
			// generate an allocation which violates the 25% average single-shot
			IDictionary<ReservationInterval, ReservationRequest> req = new SortedDictionary<ReservationInterval
				, ReservationRequest>();
			long win = timeWindow / 2 + 100;
			int cont = (int)Math.Ceil(0.5 * totCont);
			req[new ReservationInterval(initTime, initTime + win)] = ReservationRequest.NewInstance
				(Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance(1024, 1), cont);
			NUnit.Framework.Assert.IsTrue(plan.ToString(), plan.AddReservation(new InMemoryReservationAllocation
				(ReservationSystemTestUtil.GetNewReservationId(), null, "u1", "dedicated", initTime
				, initTime + win, req, res, minAlloc)));
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation.Exceptions.PlanningException
		/// 	"/>
		[NUnit.Framework.Test]
		public virtual void TestFailAvgBySum()
		{
			// generate an allocation which violates the 25% average by sum
			IDictionary<ReservationInterval, ReservationRequest> req = new SortedDictionary<ReservationInterval
				, ReservationRequest>();
			long win = 86400000 / 4 + 1;
			int cont = (int)Math.Ceil(0.5 * totCont);
			req[new ReservationInterval(initTime, initTime + win)] = ReservationRequest.NewInstance
				(Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance(1024, 1), cont);
			NUnit.Framework.Assert.IsTrue(plan.ToString(), plan.AddReservation(new InMemoryReservationAllocation
				(ReservationSystemTestUtil.GetNewReservationId(), null, "u1", "dedicated", initTime
				, initTime + win, req, res, minAlloc)));
			try
			{
				NUnit.Framework.Assert.IsTrue(plan.ToString(), plan.AddReservation(new InMemoryReservationAllocation
					(ReservationSystemTestUtil.GetNewReservationId(), null, "u1", "dedicated", initTime
					, initTime + win, req, res, minAlloc)));
				NUnit.Framework.Assert.Fail("should not have accepted this");
			}
			catch (PlanningQuotaException)
			{
			}
		}
		// expected
	}
}
