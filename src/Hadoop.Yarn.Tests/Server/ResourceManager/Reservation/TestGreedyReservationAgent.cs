using System;
using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation.Exceptions;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation
{
	public class TestGreedyReservationAgent
	{
		internal ReservationAgent agent;

		internal InMemoryPlan plan;

		internal Resource minAlloc = Resource.NewInstance(1024, 1);

		internal ResourceCalculator res = new DefaultResourceCalculator();

		internal Org.Apache.Hadoop.Yarn.Api.Records.Resource maxAlloc = Org.Apache.Hadoop.Yarn.Api.Records.Resource
			.NewInstance(1024 * 8, 8);

		internal Random rand = new Random();

		internal long step;

		/// <exception cref="System.Exception"/>
		[SetUp]
		public virtual void Setup()
		{
			long seed = rand.NextLong();
			rand.SetSeed(seed);
			Org.Mortbay.Log.Log.Info("Running with seed: " + seed);
			// setting completely loose quotas
			long timeWindow = 1000000L;
			Org.Apache.Hadoop.Yarn.Api.Records.Resource clusterCapacity = Org.Apache.Hadoop.Yarn.Api.Records.Resource
				.NewInstance(100 * 1024, 100);
			step = 1000L;
			ReservationSystemTestUtil testUtil = new ReservationSystemTestUtil();
			string reservationQ = testUtil.GetFullReservationQueueName();
			float instConstraint = 100;
			float avgConstraint = 100;
			ReservationSchedulerConfiguration conf = ReservationSystemTestUtil.CreateConf(reservationQ
				, timeWindow, instConstraint, avgConstraint);
			CapacityOverTimePolicy policy = new CapacityOverTimePolicy();
			policy.Init(reservationQ, conf);
			agent = new GreedyReservationAgent();
			QueueMetrics queueMetrics = Org.Mockito.Mockito.Mock<QueueMetrics>();
			plan = new InMemoryPlan(queueMetrics, policy, agent, clusterCapacity, step, res, 
				minAlloc, maxAlloc, "dedicated", null, true);
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation.Exceptions.PlanningException
		/// 	"/>
		[NUnit.Framework.Test]
		public virtual void TestSimple()
		{
			PrepareBasicPlan();
			// create a request with a single atomic ask
			ReservationDefinition rr = new ReservationDefinitionPBImpl();
			rr.SetArrival(5 * step);
			rr.SetDeadline(20 * step);
			ReservationRequest r = ReservationRequest.NewInstance(Org.Apache.Hadoop.Yarn.Api.Records.Resource
				.NewInstance(2048, 2), 10, 5, 10 * step);
			ReservationRequests reqs = new ReservationRequestsPBImpl();
			reqs.SetReservationResources(Collections.SingletonList(r));
			rr.SetReservationRequests(reqs);
			ReservationId reservationID = ReservationSystemTestUtil.GetNewReservationId();
			agent.CreateReservation(reservationID, "u1", plan, rr);
			NUnit.Framework.Assert.IsTrue("Agent-based allocation failed", reservationID != null
				);
			NUnit.Framework.Assert.IsTrue("Agent-based allocation failed", plan.GetAllReservations
				().Count == 3);
			ReservationAllocation cs = plan.GetReservationById(reservationID);
			System.Console.Out.WriteLine("--------AFTER SIMPLE ALLOCATION (queue: " + reservationID
				 + ")----------");
			System.Console.Out.WriteLine(plan.ToString());
			System.Console.Out.WriteLine(plan.ToCumulativeString());
			for (long i = 10 * step; i < 20 * step; i++)
			{
				NUnit.Framework.Assert.IsTrue("Agent-based allocation unexpected", Resources.Equals
					(cs.GetResourcesAtTime(i), Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance
					(2048 * 10, 2 * 10)));
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation.Exceptions.PlanningException
		/// 	"/>
		[NUnit.Framework.Test]
		public virtual void TestOrder()
		{
			PrepareBasicPlan();
			// create a completely utilized segment around time 30
			int[] f = new int[] { 100, 100 };
			NUnit.Framework.Assert.IsTrue(plan.ToString(), plan.AddReservation(new InMemoryReservationAllocation
				(ReservationSystemTestUtil.GetNewReservationId(), null, "u1", "dedicated", 30 * 
				step, 30 * step + f.Length * step, ReservationSystemTestUtil.GenerateAllocation(
				30 * step, step, f), res, minAlloc)));
			// create a chain of 4 RR, mixing gang and non-gang
			ReservationDefinition rr = new ReservationDefinitionPBImpl();
			rr.SetArrival(0 * step);
			rr.SetDeadline(70 * step);
			ReservationRequests reqs = new ReservationRequestsPBImpl();
			reqs.SetInterpreter(ReservationRequestInterpreter.ROrder);
			ReservationRequest r = ReservationRequest.NewInstance(Org.Apache.Hadoop.Yarn.Api.Records.Resource
				.NewInstance(2048, 2), 10, 1, 10 * step);
			ReservationRequest r2 = ReservationRequest.NewInstance(Org.Apache.Hadoop.Yarn.Api.Records.Resource
				.NewInstance(1024, 1), 10, 10, 20 * step);
			IList<ReservationRequest> list = new AList<ReservationRequest>();
			list.AddItem(r);
			list.AddItem(r2);
			list.AddItem(r);
			list.AddItem(r2);
			reqs.SetReservationResources(list);
			rr.SetReservationRequests(reqs);
			// submit to agent
			ReservationId reservationID = ReservationSystemTestUtil.GetNewReservationId();
			agent.CreateReservation(reservationID, "u1", plan, rr);
			// validate
			NUnit.Framework.Assert.IsTrue("Agent-based allocation failed", reservationID != null
				);
			NUnit.Framework.Assert.IsTrue("Agent-based allocation failed", plan.GetAllReservations
				().Count == 4);
			ReservationAllocation cs = plan.GetReservationById(reservationID);
			NUnit.Framework.Assert.IsTrue(cs.ToString(), Check(cs, 0 * step, 10 * step, 20, 1024
				, 1));
			NUnit.Framework.Assert.IsTrue(cs.ToString(), Check(cs, 10 * step, 30 * step, 10, 
				1024, 1));
			NUnit.Framework.Assert.IsTrue(cs.ToString(), Check(cs, 40 * step, 50 * step, 20, 
				1024, 1));
			NUnit.Framework.Assert.IsTrue(cs.ToString(), Check(cs, 50 * step, 70 * step, 10, 
				1024, 1));
			System.Console.Out.WriteLine("--------AFTER ORDER ALLOCATION (queue: " + reservationID
				 + ")----------");
			System.Console.Out.WriteLine(plan.ToString());
			System.Console.Out.WriteLine(plan.ToCumulativeString());
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation.Exceptions.PlanningException
		/// 	"/>
		[NUnit.Framework.Test]
		public virtual void TestOrderNoGapImpossible()
		{
			PrepareBasicPlan();
			// create a completely utilized segment at time 30
			int[] f = new int[] { 100, 100 };
			NUnit.Framework.Assert.IsTrue(plan.ToString(), plan.AddReservation(new InMemoryReservationAllocation
				(ReservationSystemTestUtil.GetNewReservationId(), null, "u1", "dedicated", 30 * 
				step, 30 * step + f.Length * step, ReservationSystemTestUtil.GenerateAllocation(
				30 * step, step, f), res, minAlloc)));
			// create a chain of 4 RR, mixing gang and non-gang
			ReservationDefinition rr = new ReservationDefinitionPBImpl();
			rr.SetArrival(0L);
			rr.SetDeadline(70L);
			ReservationRequests reqs = new ReservationRequestsPBImpl();
			reqs.SetInterpreter(ReservationRequestInterpreter.ROrderNoGap);
			ReservationRequest r = ReservationRequest.NewInstance(Org.Apache.Hadoop.Yarn.Api.Records.Resource
				.NewInstance(2048, 2), 10, 1, 10);
			ReservationRequest r2 = ReservationRequest.NewInstance(Org.Apache.Hadoop.Yarn.Api.Records.Resource
				.NewInstance(1024, 1), 10, 10, 20);
			IList<ReservationRequest> list = new AList<ReservationRequest>();
			list.AddItem(r);
			list.AddItem(r2);
			list.AddItem(r);
			list.AddItem(r2);
			reqs.SetReservationResources(list);
			rr.SetReservationRequests(reqs);
			ReservationId reservationID = ReservationSystemTestUtil.GetNewReservationId();
			bool result = false;
			try
			{
				// submit to agent
				result = agent.CreateReservation(reservationID, "u1", plan, rr);
				NUnit.Framework.Assert.Fail();
			}
			catch (PlanningException)
			{
			}
			// expected
			// validate
			NUnit.Framework.Assert.IsFalse("Agent-based allocation should have failed", result
				);
			NUnit.Framework.Assert.IsTrue("Agent-based allocation should have failed", plan.GetAllReservations
				().Count == 3);
			System.Console.Out.WriteLine("--------AFTER ORDER_NO_GAP IMPOSSIBLE ALLOCATION (queue: "
				 + reservationID + ")----------");
			System.Console.Out.WriteLine(plan.ToString());
			System.Console.Out.WriteLine(plan.ToCumulativeString());
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation.Exceptions.PlanningException
		/// 	"/>
		[NUnit.Framework.Test]
		public virtual void TestOrderNoGap()
		{
			PrepareBasicPlan();
			// create a chain of 4 RR, mixing gang and non-gang
			ReservationDefinition rr = new ReservationDefinitionPBImpl();
			rr.SetArrival(0 * step);
			rr.SetDeadline(60 * step);
			ReservationRequests reqs = new ReservationRequestsPBImpl();
			reqs.SetInterpreter(ReservationRequestInterpreter.ROrderNoGap);
			ReservationRequest r = ReservationRequest.NewInstance(Org.Apache.Hadoop.Yarn.Api.Records.Resource
				.NewInstance(2048, 2), 10, 1, 10 * step);
			ReservationRequest r2 = ReservationRequest.NewInstance(Org.Apache.Hadoop.Yarn.Api.Records.Resource
				.NewInstance(1024, 1), 10, 10, 20 * step);
			IList<ReservationRequest> list = new AList<ReservationRequest>();
			list.AddItem(r);
			list.AddItem(r2);
			list.AddItem(r);
			list.AddItem(r2);
			reqs.SetReservationResources(list);
			rr.SetReservationRequests(reqs);
			rr.SetReservationRequests(reqs);
			// submit to agent
			ReservationId reservationID = ReservationSystemTestUtil.GetNewReservationId();
			agent.CreateReservation(reservationID, "u1", plan, rr);
			System.Console.Out.WriteLine("--------AFTER ORDER ALLOCATION (queue: " + reservationID
				 + ")----------");
			System.Console.Out.WriteLine(plan.ToString());
			System.Console.Out.WriteLine(plan.ToCumulativeString());
			// validate
			NUnit.Framework.Assert.IsTrue("Agent-based allocation failed", reservationID != null
				);
			NUnit.Framework.Assert.IsTrue("Agent-based allocation failed", plan.GetAllReservations
				().Count == 3);
			ReservationAllocation cs = plan.GetReservationById(reservationID);
			NUnit.Framework.Assert.IsTrue(cs.ToString(), Check(cs, 0 * step, 10 * step, 20, 1024
				, 1));
			NUnit.Framework.Assert.IsTrue(cs.ToString(), Check(cs, 10 * step, 30 * step, 10, 
				1024, 1));
			NUnit.Framework.Assert.IsTrue(cs.ToString(), Check(cs, 30 * step, 40 * step, 20, 
				1024, 1));
			NUnit.Framework.Assert.IsTrue(cs.ToString(), Check(cs, 40 * step, 60 * step, 10, 
				1024, 1));
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation.Exceptions.PlanningException
		/// 	"/>
		[NUnit.Framework.Test]
		public virtual void TestSingleSliding()
		{
			PrepareBasicPlan();
			// create a single request for which we need subsequent (tight) packing.
			ReservationDefinition rr = new ReservationDefinitionPBImpl();
			rr.SetArrival(100 * step);
			rr.SetDeadline(120 * step);
			ReservationRequests reqs = new ReservationRequestsPBImpl();
			reqs.SetInterpreter(ReservationRequestInterpreter.RAll);
			ReservationRequest r = ReservationRequest.NewInstance(Org.Apache.Hadoop.Yarn.Api.Records.Resource
				.NewInstance(1024, 1), 200, 10, 10 * step);
			IList<ReservationRequest> list = new AList<ReservationRequest>();
			list.AddItem(r);
			reqs.SetReservationResources(list);
			rr.SetReservationRequests(reqs);
			// submit to agent
			ReservationId reservationID = ReservationSystemTestUtil.GetNewReservationId();
			agent.CreateReservation(reservationID, "u1", plan, rr);
			// validate results, we expect the second one to be accepted
			NUnit.Framework.Assert.IsTrue("Agent-based allocation failed", reservationID != null
				);
			NUnit.Framework.Assert.IsTrue("Agent-based allocation failed", plan.GetAllReservations
				().Count == 3);
			ReservationAllocation cs = plan.GetReservationById(reservationID);
			NUnit.Framework.Assert.IsTrue(cs.ToString(), Check(cs, 100 * step, 120 * step, 100
				, 1024, 1));
			System.Console.Out.WriteLine("--------AFTER packed ALLOCATION (queue: " + reservationID
				 + ")----------");
			System.Console.Out.WriteLine(plan.ToString());
			System.Console.Out.WriteLine(plan.ToCumulativeString());
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation.Exceptions.PlanningException
		/// 	"/>
		[NUnit.Framework.Test]
		public virtual void TestAny()
		{
			PrepareBasicPlan();
			// create an ANY request, with an impossible step (last in list, first
			// considered),
			// and two satisfiable ones. We expect the second one to be returned.
			ReservationDefinition rr = new ReservationDefinitionPBImpl();
			rr.SetArrival(100 * step);
			rr.SetDeadline(120 * step);
			ReservationRequests reqs = new ReservationRequestsPBImpl();
			reqs.SetInterpreter(ReservationRequestInterpreter.RAny);
			ReservationRequest r = ReservationRequest.NewInstance(Org.Apache.Hadoop.Yarn.Api.Records.Resource
				.NewInstance(1024, 1), 5, 5, 10 * step);
			ReservationRequest r2 = ReservationRequest.NewInstance(Org.Apache.Hadoop.Yarn.Api.Records.Resource
				.NewInstance(2048, 2), 10, 5, 10 * step);
			ReservationRequest r3 = ReservationRequest.NewInstance(Org.Apache.Hadoop.Yarn.Api.Records.Resource
				.NewInstance(1024, 1), 110, 110, 10 * step);
			IList<ReservationRequest> list = new AList<ReservationRequest>();
			list.AddItem(r);
			list.AddItem(r2);
			list.AddItem(r3);
			reqs.SetReservationResources(list);
			rr.SetReservationRequests(reqs);
			// submit to agent
			ReservationId reservationID = ReservationSystemTestUtil.GetNewReservationId();
			bool res = agent.CreateReservation(reservationID, "u1", plan, rr);
			// validate results, we expect the second one to be accepted
			NUnit.Framework.Assert.IsTrue("Agent-based allocation failed", res);
			NUnit.Framework.Assert.IsTrue("Agent-based allocation failed", plan.GetAllReservations
				().Count == 3);
			ReservationAllocation cs = plan.GetReservationById(reservationID);
			NUnit.Framework.Assert.IsTrue(cs.ToString(), Check(cs, 110 * step, 120 * step, 20
				, 1024, 1));
			System.Console.Out.WriteLine("--------AFTER ANY ALLOCATION (queue: " + reservationID
				 + ")----------");
			System.Console.Out.WriteLine(plan.ToString());
			System.Console.Out.WriteLine(plan.ToCumulativeString());
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation.Exceptions.PlanningException
		/// 	"/>
		[NUnit.Framework.Test]
		public virtual void TestAnyImpossible()
		{
			PrepareBasicPlan();
			// create an ANY request, with all impossible alternatives
			ReservationDefinition rr = new ReservationDefinitionPBImpl();
			rr.SetArrival(100L);
			rr.SetDeadline(120L);
			ReservationRequests reqs = new ReservationRequestsPBImpl();
			reqs.SetInterpreter(ReservationRequestInterpreter.RAny);
			// longer than arrival-deadline
			ReservationRequest r1 = ReservationRequest.NewInstance(Org.Apache.Hadoop.Yarn.Api.Records.Resource
				.NewInstance(1024, 1), 35, 5, 30);
			// above max cluster size
			ReservationRequest r2 = ReservationRequest.NewInstance(Org.Apache.Hadoop.Yarn.Api.Records.Resource
				.NewInstance(1024, 1), 110, 110, 10);
			IList<ReservationRequest> list = new AList<ReservationRequest>();
			list.AddItem(r1);
			list.AddItem(r2);
			reqs.SetReservationResources(list);
			rr.SetReservationRequests(reqs);
			ReservationId reservationID = ReservationSystemTestUtil.GetNewReservationId();
			bool result = false;
			try
			{
				// submit to agent
				result = agent.CreateReservation(reservationID, "u1", plan, rr);
				NUnit.Framework.Assert.Fail();
			}
			catch (PlanningException)
			{
			}
			// expected
			// validate results, we expect the second one to be accepted
			NUnit.Framework.Assert.IsFalse("Agent-based allocation should have failed", result
				);
			NUnit.Framework.Assert.IsTrue("Agent-based allocation should have failed", plan.GetAllReservations
				().Count == 2);
			System.Console.Out.WriteLine("--------AFTER ANY IMPOSSIBLE ALLOCATION (queue: " +
				 reservationID + ")----------");
			System.Console.Out.WriteLine(plan.ToString());
			System.Console.Out.WriteLine(plan.ToCumulativeString());
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation.Exceptions.PlanningException
		/// 	"/>
		[NUnit.Framework.Test]
		public virtual void TestAll()
		{
			PrepareBasicPlan();
			// create an ALL request
			ReservationDefinition rr = new ReservationDefinitionPBImpl();
			rr.SetArrival(100 * step);
			rr.SetDeadline(120 * step);
			ReservationRequests reqs = new ReservationRequestsPBImpl();
			reqs.SetInterpreter(ReservationRequestInterpreter.RAll);
			ReservationRequest r = ReservationRequest.NewInstance(Org.Apache.Hadoop.Yarn.Api.Records.Resource
				.NewInstance(1024, 1), 5, 5, 10 * step);
			ReservationRequest r2 = ReservationRequest.NewInstance(Org.Apache.Hadoop.Yarn.Api.Records.Resource
				.NewInstance(2048, 2), 10, 10, 20 * step);
			IList<ReservationRequest> list = new AList<ReservationRequest>();
			list.AddItem(r);
			list.AddItem(r2);
			reqs.SetReservationResources(list);
			rr.SetReservationRequests(reqs);
			// submit to agent
			ReservationId reservationID = ReservationSystemTestUtil.GetNewReservationId();
			agent.CreateReservation(reservationID, "u1", plan, rr);
			// validate results, we expect the second one to be accepted
			NUnit.Framework.Assert.IsTrue("Agent-based allocation failed", reservationID != null
				);
			NUnit.Framework.Assert.IsTrue("Agent-based allocation failed", plan.GetAllReservations
				().Count == 3);
			ReservationAllocation cs = plan.GetReservationById(reservationID);
			NUnit.Framework.Assert.IsTrue(cs.ToString(), Check(cs, 100 * step, 110 * step, 20
				, 1024, 1));
			NUnit.Framework.Assert.IsTrue(cs.ToString(), Check(cs, 110 * step, 120 * step, 25
				, 1024, 1));
			System.Console.Out.WriteLine("--------AFTER ALL ALLOCATION (queue: " + reservationID
				 + ")----------");
			System.Console.Out.WriteLine(plan.ToString());
			System.Console.Out.WriteLine(plan.ToCumulativeString());
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation.Exceptions.PlanningException
		/// 	"/>
		[NUnit.Framework.Test]
		public virtual void TestAllImpossible()
		{
			PrepareBasicPlan();
			// create an ALL request, with an impossible combination, it should be
			// rejected, and allocation remain unchanged
			ReservationDefinition rr = new ReservationDefinitionPBImpl();
			rr.SetArrival(100L);
			rr.SetDeadline(120L);
			ReservationRequests reqs = new ReservationRequestsPBImpl();
			reqs.SetInterpreter(ReservationRequestInterpreter.RAll);
			ReservationRequest r = ReservationRequest.NewInstance(Org.Apache.Hadoop.Yarn.Api.Records.Resource
				.NewInstance(1024, 1), 55, 5, 10);
			ReservationRequest r2 = ReservationRequest.NewInstance(Org.Apache.Hadoop.Yarn.Api.Records.Resource
				.NewInstance(2048, 2), 55, 5, 20);
			IList<ReservationRequest> list = new AList<ReservationRequest>();
			list.AddItem(r);
			list.AddItem(r2);
			reqs.SetReservationResources(list);
			rr.SetReservationRequests(reqs);
			ReservationId reservationID = ReservationSystemTestUtil.GetNewReservationId();
			bool result = false;
			try
			{
				// submit to agent
				result = agent.CreateReservation(reservationID, "u1", plan, rr);
				NUnit.Framework.Assert.Fail();
			}
			catch (PlanningException)
			{
			}
			// expected
			// validate results, we expect the second one to be accepted
			NUnit.Framework.Assert.IsFalse("Agent-based allocation failed", result);
			NUnit.Framework.Assert.IsTrue("Agent-based allocation failed", plan.GetAllReservations
				().Count == 2);
			System.Console.Out.WriteLine("--------AFTER ALL IMPOSSIBLE ALLOCATION (queue: " +
				 reservationID + ")----------");
			System.Console.Out.WriteLine(plan.ToString());
			System.Console.Out.WriteLine(plan.ToCumulativeString());
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation.Exceptions.PlanningException
		/// 	"/>
		private void PrepareBasicPlan()
		{
			// insert in the reservation a couple of controlled reservations, to create
			// conditions for assignment that are non-empty
			int[] f = new int[] { 10, 10, 20, 20, 20, 10, 10 };
			NUnit.Framework.Assert.IsTrue(plan.ToString(), plan.AddReservation(new InMemoryReservationAllocation
				(ReservationSystemTestUtil.GetNewReservationId(), null, "u1", "dedicated", 0L, 0L
				 + f.Length * step, ReservationSystemTestUtil.GenerateAllocation(0, step, f), res
				, minAlloc)));
			int[] f2 = new int[] { 5, 5, 5, 5, 5, 5, 5 };
			IDictionary<ReservationInterval, ReservationRequest> alloc = ReservationSystemTestUtil
				.GenerateAllocation(5000, step, f2);
			NUnit.Framework.Assert.IsTrue(plan.ToString(), plan.AddReservation(new InMemoryReservationAllocation
				(ReservationSystemTestUtil.GetNewReservationId(), null, "u1", "dedicated", 5000, 
				5000 + f2.Length * step, alloc, res, minAlloc)));
			System.Console.Out.WriteLine("--------BEFORE AGENT----------");
			System.Console.Out.WriteLine(plan.ToString());
			System.Console.Out.WriteLine(plan.ToCumulativeString());
		}

		private bool Check(ReservationAllocation cs, long start, long end, int containers
			, int mem, int cores)
		{
			bool res = true;
			for (long i = start; i < end; i++)
			{
				res = res && Resources.Equals(cs.GetResourcesAtTime(i), Org.Apache.Hadoop.Yarn.Api.Records.Resource
					.NewInstance(mem * containers, cores * containers));
			}
			return res;
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation.Exceptions.PlanningException
		/// 	"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestStress(int numJobs)
		{
			long timeWindow = 1000000L;
			Org.Apache.Hadoop.Yarn.Api.Records.Resource clusterCapacity = Org.Apache.Hadoop.Yarn.Api.Records.Resource
				.NewInstance(500 * 100 * 1024, 500 * 32);
			step = 1000L;
			ReservationSystemTestUtil testUtil = new ReservationSystemTestUtil();
			CapacityScheduler scheduler = testUtil.MockCapacityScheduler(500 * 100);
			string reservationQ = testUtil.GetFullReservationQueueName();
			float instConstraint = 100;
			float avgConstraint = 100;
			ReservationSchedulerConfiguration conf = ReservationSystemTestUtil.CreateConf(reservationQ
				, timeWindow, instConstraint, avgConstraint);
			CapacityOverTimePolicy policy = new CapacityOverTimePolicy();
			policy.Init(reservationQ, conf);
			plan = new InMemoryPlan(scheduler.GetRootQueueMetrics(), policy, agent, clusterCapacity
				, step, res, minAlloc, maxAlloc, "dedicated", null, true);
			int acc = 0;
			IList<ReservationDefinition> list = new AList<ReservationDefinition>();
			for (long i = 0; i < numJobs; i++)
			{
				list.AddItem(ReservationSystemTestUtil.GenerateRandomRR(rand, i));
			}
			long start = Runtime.CurrentTimeMillis();
			for (int i_1 = 0; i_1 < numJobs; i_1++)
			{
				try
				{
					if (agent.CreateReservation(ReservationSystemTestUtil.GetNewReservationId(), "u" 
						+ i_1 % 100, plan, list[i_1]))
					{
						acc++;
					}
				}
				catch (PlanningException)
				{
				}
			}
			// ignore exceptions
			long end = Runtime.CurrentTimeMillis();
			System.Console.Out.WriteLine("Submitted " + numJobs + " jobs " + " accepted " + acc
				 + " in " + (end - start) + "ms");
		}

		public static void Main(string[] arg)
		{
			// run a stress test with by default 1000 random jobs
			int numJobs = 1000;
			if (arg.Length > 0)
			{
				numJobs = System.Convert.ToInt32(arg[0]);
			}
			try
			{
				TestGreedyReservationAgent test = new TestGreedyReservationAgent();
				test.Setup();
				test.TestStress(numJobs);
			}
			catch (Exception e)
			{
				Sharpen.Runtime.PrintStackTrace(e);
			}
		}
	}
}
