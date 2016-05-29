using System;
using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation.Exceptions;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation
{
	public class TestInMemoryPlan
	{
		private string user = "yarn";

		private string planName = "test-reservation";

		private ResourceCalculator resCalc;

		private Org.Apache.Hadoop.Yarn.Api.Records.Resource minAlloc;

		private Org.Apache.Hadoop.Yarn.Api.Records.Resource maxAlloc;

		private Org.Apache.Hadoop.Yarn.Api.Records.Resource totalCapacity;

		private Clock clock;

		private QueueMetrics queueMetrics;

		private SharingPolicy policy;

		private ReservationAgent agent;

		private Planner replanner;

		/// <exception cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation.Exceptions.PlanningException
		/// 	"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			resCalc = new DefaultResourceCalculator();
			minAlloc = Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance(1024, 1);
			maxAlloc = Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance(64 * 1024, 20);
			totalCapacity = Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance(100 * 1024
				, 100);
			clock = Org.Mockito.Mockito.Mock<Clock>();
			queueMetrics = Org.Mockito.Mockito.Mock<QueueMetrics>();
			policy = Org.Mockito.Mockito.Mock<SharingPolicy>();
			replanner = Org.Mockito.Mockito.Mock<Planner>();
			Org.Mockito.Mockito.When(clock.GetTime()).ThenReturn(1L);
		}

		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			resCalc = null;
			minAlloc = null;
			maxAlloc = null;
			totalCapacity = null;
			clock = null;
			queueMetrics = null;
			policy = null;
			replanner = null;
		}

		[NUnit.Framework.Test]
		public virtual void TestAddReservation()
		{
			Plan plan = new InMemoryPlan(queueMetrics, policy, agent, totalCapacity, 1L, resCalc
				, minAlloc, maxAlloc, planName, replanner, true);
			ReservationId reservationID = ReservationSystemTestUtil.GetNewReservationId();
			int[] alloc = new int[] { 10, 10, 10, 10, 10, 10 };
			int start = 100;
			IDictionary<ReservationInterval, ReservationRequest> allocations = GenerateAllocation
				(start, alloc, false);
			ReservationDefinition rDef = CreateSimpleReservationDefinition(start, start + alloc
				.Length, alloc.Length, allocations.Values);
			ReservationAllocation rAllocation = new InMemoryReservationAllocation(reservationID
				, rDef, user, planName, start, start + alloc.Length, allocations, resCalc, minAlloc
				);
			NUnit.Framework.Assert.IsNull(plan.GetReservationById(reservationID));
			try
			{
				plan.AddReservation(rAllocation);
			}
			catch (PlanningException e)
			{
				NUnit.Framework.Assert.Fail(e.Message);
			}
			DoAssertions(plan, rAllocation);
			for (int i = 0; i < alloc.Length; i++)
			{
				NUnit.Framework.Assert.AreEqual(Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance
					(1024 * (alloc[i]), (alloc[i])), plan.GetTotalCommittedResources(start + i));
				NUnit.Framework.Assert.AreEqual(Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance
					(1024 * (alloc[i]), (alloc[i])), plan.GetConsumptionForUser(user, start + i));
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestAddEmptyReservation()
		{
			Plan plan = new InMemoryPlan(queueMetrics, policy, agent, totalCapacity, 1L, resCalc
				, minAlloc, maxAlloc, planName, replanner, true);
			ReservationId reservationID = ReservationSystemTestUtil.GetNewReservationId();
			int[] alloc = new int[] {  };
			int start = 100;
			IDictionary<ReservationInterval, ReservationRequest> allocations = new Dictionary
				<ReservationInterval, ReservationRequest>();
			ReservationDefinition rDef = CreateSimpleReservationDefinition(start, start + alloc
				.Length, alloc.Length, allocations.Values);
			ReservationAllocation rAllocation = new InMemoryReservationAllocation(reservationID
				, rDef, user, planName, start, start + alloc.Length, allocations, resCalc, minAlloc
				);
			NUnit.Framework.Assert.IsNull(plan.GetReservationById(reservationID));
			try
			{
				plan.AddReservation(rAllocation);
			}
			catch (PlanningException e)
			{
				NUnit.Framework.Assert.Fail(e.Message);
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestAddReservationAlreadyExists()
		{
			// First add a reservation
			Plan plan = new InMemoryPlan(queueMetrics, policy, agent, totalCapacity, 1L, resCalc
				, minAlloc, maxAlloc, planName, replanner, true);
			ReservationId reservationID = ReservationSystemTestUtil.GetNewReservationId();
			int[] alloc = new int[] { 10, 10, 10, 10, 10, 10 };
			int start = 100;
			IDictionary<ReservationInterval, ReservationRequest> allocations = GenerateAllocation
				(start, alloc, false);
			ReservationDefinition rDef = CreateSimpleReservationDefinition(start, start + alloc
				.Length, alloc.Length, allocations.Values);
			ReservationAllocation rAllocation = new InMemoryReservationAllocation(reservationID
				, rDef, user, planName, start, start + alloc.Length, allocations, resCalc, minAlloc
				);
			NUnit.Framework.Assert.IsNull(plan.GetReservationById(reservationID));
			try
			{
				plan.AddReservation(rAllocation);
			}
			catch (PlanningException e)
			{
				NUnit.Framework.Assert.Fail(e.Message);
			}
			DoAssertions(plan, rAllocation);
			for (int i = 0; i < alloc.Length; i++)
			{
				NUnit.Framework.Assert.AreEqual(Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance
					(1024 * (alloc[i]), (alloc[i])), plan.GetTotalCommittedResources(start + i));
				NUnit.Framework.Assert.AreEqual(Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance
					(1024 * (alloc[i]), (alloc[i])), plan.GetConsumptionForUser(user, start + i));
			}
			// Try to add it again
			try
			{
				plan.AddReservation(rAllocation);
				NUnit.Framework.Assert.Fail("Add should fail as it already exists");
			}
			catch (ArgumentException e)
			{
				NUnit.Framework.Assert.IsTrue(e.Message.EndsWith("already exists"));
			}
			catch (PlanningException e)
			{
				NUnit.Framework.Assert.Fail(e.Message);
			}
			DoAssertions(plan, rAllocation);
		}

		[NUnit.Framework.Test]
		public virtual void TestUpdateReservation()
		{
			Plan plan = new InMemoryPlan(queueMetrics, policy, agent, totalCapacity, 1L, resCalc
				, minAlloc, maxAlloc, planName, replanner, true);
			ReservationId reservationID = ReservationSystemTestUtil.GetNewReservationId();
			// First add a reservation
			int[] alloc = new int[] { 10, 10, 10, 10, 10, 10 };
			int start = 100;
			IDictionary<ReservationInterval, ReservationRequest> allocations = GenerateAllocation
				(start, alloc, false);
			ReservationDefinition rDef = CreateSimpleReservationDefinition(start, start + alloc
				.Length, alloc.Length, allocations.Values);
			ReservationAllocation rAllocation = new InMemoryReservationAllocation(reservationID
				, rDef, user, planName, start, start + alloc.Length, allocations, resCalc, minAlloc
				);
			NUnit.Framework.Assert.IsNull(plan.GetReservationById(reservationID));
			try
			{
				plan.AddReservation(rAllocation);
			}
			catch (PlanningException e)
			{
				NUnit.Framework.Assert.Fail(e.Message);
			}
			DoAssertions(plan, rAllocation);
			for (int i = 0; i < alloc.Length; i++)
			{
				NUnit.Framework.Assert.AreEqual(Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance
					(1024 * (alloc[i]), (alloc[i])), plan.GetTotalCommittedResources(start + i));
				NUnit.Framework.Assert.AreEqual(Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance
					(1024 * (alloc[i]), (alloc[i])), plan.GetConsumptionForUser(user, start + i));
			}
			// Now update it
			start = 110;
			int[] updatedAlloc = new int[] { 0, 5, 10, 10, 5, 0 };
			allocations = GenerateAllocation(start, updatedAlloc, true);
			rDef = CreateSimpleReservationDefinition(start, start + updatedAlloc.Length, updatedAlloc
				.Length, allocations.Values);
			rAllocation = new InMemoryReservationAllocation(reservationID, rDef, user, planName
				, start, start + updatedAlloc.Length, allocations, resCalc, minAlloc);
			try
			{
				plan.UpdateReservation(rAllocation);
			}
			catch (PlanningException e)
			{
				NUnit.Framework.Assert.Fail(e.Message);
			}
			DoAssertions(plan, rAllocation);
			for (int i_1 = 0; i_1 < updatedAlloc.Length; i_1++)
			{
				NUnit.Framework.Assert.AreEqual(Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance
					(1024 * (updatedAlloc[i_1] + i_1), updatedAlloc[i_1] + i_1), plan.GetTotalCommittedResources
					(start + i_1));
				NUnit.Framework.Assert.AreEqual(Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance
					(1024 * (updatedAlloc[i_1] + i_1), updatedAlloc[i_1] + i_1), plan.GetConsumptionForUser
					(user, start + i_1));
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestUpdateNonExistingReservation()
		{
			Plan plan = new InMemoryPlan(queueMetrics, policy, agent, totalCapacity, 1L, resCalc
				, minAlloc, maxAlloc, planName, replanner, true);
			ReservationId reservationID = ReservationSystemTestUtil.GetNewReservationId();
			// Try to update a reservation without adding
			int[] alloc = new int[] { 10, 10, 10, 10, 10, 10 };
			int start = 100;
			IDictionary<ReservationInterval, ReservationRequest> allocations = GenerateAllocation
				(start, alloc, false);
			ReservationDefinition rDef = CreateSimpleReservationDefinition(start, start + alloc
				.Length, alloc.Length, allocations.Values);
			ReservationAllocation rAllocation = new InMemoryReservationAllocation(reservationID
				, rDef, user, planName, start, start + alloc.Length, allocations, resCalc, minAlloc
				);
			NUnit.Framework.Assert.IsNull(plan.GetReservationById(reservationID));
			try
			{
				plan.UpdateReservation(rAllocation);
				NUnit.Framework.Assert.Fail("Update should fail as it does not exist in the plan"
					);
			}
			catch (ArgumentException e)
			{
				NUnit.Framework.Assert.IsTrue(e.Message.EndsWith("does not exist in the plan"));
			}
			catch (PlanningException e)
			{
				NUnit.Framework.Assert.Fail(e.Message);
			}
			NUnit.Framework.Assert.IsNull(plan.GetReservationById(reservationID));
		}

		[NUnit.Framework.Test]
		public virtual void TestDeleteReservation()
		{
			// First add a reservation
			Plan plan = new InMemoryPlan(queueMetrics, policy, agent, totalCapacity, 1L, resCalc
				, minAlloc, maxAlloc, planName, replanner, true);
			ReservationId reservationID = ReservationSystemTestUtil.GetNewReservationId();
			int[] alloc = new int[] { 10, 10, 10, 10, 10, 10 };
			int start = 100;
			IDictionary<ReservationInterval, ReservationRequest> allocations = GenerateAllocation
				(start, alloc, true);
			ReservationDefinition rDef = CreateSimpleReservationDefinition(start, start + alloc
				.Length, alloc.Length, allocations.Values);
			ReservationAllocation rAllocation = new InMemoryReservationAllocation(reservationID
				, rDef, user, planName, start, start + alloc.Length, allocations, resCalc, minAlloc
				);
			NUnit.Framework.Assert.IsNull(plan.GetReservationById(reservationID));
			try
			{
				plan.AddReservation(rAllocation);
			}
			catch (PlanningException e)
			{
				NUnit.Framework.Assert.Fail(e.Message);
			}
			DoAssertions(plan, rAllocation);
			for (int i = 0; i < alloc.Length; i++)
			{
				NUnit.Framework.Assert.AreEqual(Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance
					(1024 * (alloc[i] + i), (alloc[i] + i)), plan.GetTotalCommittedResources(start +
					 i));
				NUnit.Framework.Assert.AreEqual(Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance
					(1024 * (alloc[i] + i), (alloc[i] + i)), plan.GetConsumptionForUser(user, start 
					+ i));
			}
			// Now delete it
			try
			{
				plan.DeleteReservation(reservationID);
			}
			catch (PlanningException e)
			{
				NUnit.Framework.Assert.Fail(e.Message);
			}
			NUnit.Framework.Assert.IsNull(plan.GetReservationById(reservationID));
			for (int i_1 = 0; i_1 < alloc.Length; i_1++)
			{
				NUnit.Framework.Assert.AreEqual(Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance
					(0, 0), plan.GetTotalCommittedResources(start + i_1));
				NUnit.Framework.Assert.AreEqual(Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance
					(0, 0), plan.GetConsumptionForUser(user, start + i_1));
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestDeleteNonExistingReservation()
		{
			Plan plan = new InMemoryPlan(queueMetrics, policy, agent, totalCapacity, 1L, resCalc
				, minAlloc, maxAlloc, planName, replanner, true);
			ReservationId reservationID = ReservationSystemTestUtil.GetNewReservationId();
			// Try to delete a reservation without adding
			NUnit.Framework.Assert.IsNull(plan.GetReservationById(reservationID));
			try
			{
				plan.DeleteReservation(reservationID);
				NUnit.Framework.Assert.Fail("Delete should fail as it does not exist in the plan"
					);
			}
			catch (ArgumentException e)
			{
				NUnit.Framework.Assert.IsTrue(e.Message.EndsWith("does not exist in the plan"));
			}
			catch (PlanningException e)
			{
				NUnit.Framework.Assert.Fail(e.Message);
			}
			NUnit.Framework.Assert.IsNull(plan.GetReservationById(reservationID));
		}

		[NUnit.Framework.Test]
		public virtual void TestArchiveCompletedReservations()
		{
			Plan plan = new InMemoryPlan(queueMetrics, policy, agent, totalCapacity, 1L, resCalc
				, minAlloc, maxAlloc, planName, replanner, true);
			ReservationId reservationID1 = ReservationSystemTestUtil.GetNewReservationId();
			// First add a reservation
			int[] alloc1 = new int[] { 10, 10, 10, 10, 10, 10 };
			int start = 100;
			IDictionary<ReservationInterval, ReservationRequest> allocations1 = GenerateAllocation
				(start, alloc1, false);
			ReservationDefinition rDef1 = CreateSimpleReservationDefinition(start, start + alloc1
				.Length, alloc1.Length, allocations1.Values);
			ReservationAllocation rAllocation = new InMemoryReservationAllocation(reservationID1
				, rDef1, user, planName, start, start + alloc1.Length, allocations1, resCalc, minAlloc
				);
			NUnit.Framework.Assert.IsNull(plan.GetReservationById(reservationID1));
			try
			{
				plan.AddReservation(rAllocation);
			}
			catch (PlanningException e)
			{
				NUnit.Framework.Assert.Fail(e.Message);
			}
			DoAssertions(plan, rAllocation);
			for (int i = 0; i < alloc1.Length; i++)
			{
				NUnit.Framework.Assert.AreEqual(Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance
					(1024 * (alloc1[i]), (alloc1[i])), plan.GetTotalCommittedResources(start + i));
				NUnit.Framework.Assert.AreEqual(Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance
					(1024 * (alloc1[i]), (alloc1[i])), plan.GetConsumptionForUser(user, start + i));
			}
			// Now add another one
			ReservationId reservationID2 = ReservationSystemTestUtil.GetNewReservationId();
			int[] alloc2 = new int[] { 0, 5, 10, 5, 0 };
			IDictionary<ReservationInterval, ReservationRequest> allocations2 = GenerateAllocation
				(start, alloc2, true);
			ReservationDefinition rDef2 = CreateSimpleReservationDefinition(start, start + alloc2
				.Length, alloc2.Length, allocations2.Values);
			rAllocation = new InMemoryReservationAllocation(reservationID2, rDef2, user, planName
				, start, start + alloc2.Length, allocations2, resCalc, minAlloc);
			NUnit.Framework.Assert.IsNull(plan.GetReservationById(reservationID2));
			try
			{
				plan.AddReservation(rAllocation);
			}
			catch (PlanningException e)
			{
				NUnit.Framework.Assert.Fail(e.Message);
			}
			NUnit.Framework.Assert.IsNotNull(plan.GetReservationById(reservationID2));
			for (int i_1 = 0; i_1 < alloc2.Length; i_1++)
			{
				NUnit.Framework.Assert.AreEqual(Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance
					(1024 * (alloc1[i_1] + alloc2[i_1] + i_1), alloc1[i_1] + alloc2[i_1] + i_1), plan
					.GetTotalCommittedResources(start + i_1));
				NUnit.Framework.Assert.AreEqual(Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance
					(1024 * (alloc1[i_1] + alloc2[i_1] + i_1), alloc1[i_1] + alloc2[i_1] + i_1), plan
					.GetConsumptionForUser(user, start + i_1));
			}
			// Now archive completed reservations
			Org.Mockito.Mockito.When(clock.GetTime()).ThenReturn(106L);
			Org.Mockito.Mockito.When(policy.GetValidWindow()).ThenReturn(1L);
			try
			{
				// will only remove 2nd reservation as only that has fallen out of the
				// archival window
				plan.ArchiveCompletedReservations(clock.GetTime());
			}
			catch (PlanningException e)
			{
				NUnit.Framework.Assert.Fail(e.Message);
			}
			NUnit.Framework.Assert.IsNotNull(plan.GetReservationById(reservationID1));
			NUnit.Framework.Assert.IsNull(plan.GetReservationById(reservationID2));
			for (int i_2 = 0; i_2 < alloc1.Length; i_2++)
			{
				NUnit.Framework.Assert.AreEqual(Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance
					(1024 * (alloc1[i_2]), (alloc1[i_2])), plan.GetTotalCommittedResources(start + i_2
					));
				NUnit.Framework.Assert.AreEqual(Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance
					(1024 * (alloc1[i_2]), (alloc1[i_2])), plan.GetConsumptionForUser(user, start + 
					i_2));
			}
			Org.Mockito.Mockito.When(clock.GetTime()).ThenReturn(107L);
			try
			{
				// will remove 1st reservation also as it has fallen out of the archival
				// window
				plan.ArchiveCompletedReservations(clock.GetTime());
			}
			catch (PlanningException e)
			{
				NUnit.Framework.Assert.Fail(e.Message);
			}
			NUnit.Framework.Assert.IsNull(plan.GetReservationById(reservationID1));
			for (int i_3 = 0; i_3 < alloc1.Length; i_3++)
			{
				NUnit.Framework.Assert.AreEqual(Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance
					(0, 0), plan.GetTotalCommittedResources(start + i_3));
				NUnit.Framework.Assert.AreEqual(Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance
					(0, 0), plan.GetConsumptionForUser(user, start + i_3));
			}
		}

		private void DoAssertions(Plan plan, ReservationAllocation rAllocation)
		{
			ReservationId reservationID = rAllocation.GetReservationId();
			NUnit.Framework.Assert.IsNotNull(plan.GetReservationById(reservationID));
			NUnit.Framework.Assert.AreEqual(rAllocation, plan.GetReservationById(reservationID
				));
			NUnit.Framework.Assert.IsTrue(((InMemoryPlan)plan).GetAllReservations().Count == 
				1);
			NUnit.Framework.Assert.AreEqual(rAllocation.GetEndTime(), plan.GetLastEndTime());
			NUnit.Framework.Assert.AreEqual(totalCapacity, plan.GetTotalCapacity());
			NUnit.Framework.Assert.AreEqual(minAlloc, plan.GetMinimumAllocation());
			NUnit.Framework.Assert.AreEqual(maxAlloc, plan.GetMaximumAllocation());
			NUnit.Framework.Assert.AreEqual(resCalc, plan.GetResourceCalculator());
			NUnit.Framework.Assert.AreEqual(planName, plan.GetQueueName());
			NUnit.Framework.Assert.IsTrue(plan.GetMoveOnExpiry());
		}

		private ReservationDefinition CreateSimpleReservationDefinition(long arrival, long
			 deadline, long duration, ICollection<ReservationRequest> resources)
		{
			// create a request with a single atomic ask
			ReservationDefinition rDef = new ReservationDefinitionPBImpl();
			ReservationRequests reqs = new ReservationRequestsPBImpl();
			reqs.SetReservationResources(new AList<ReservationRequest>(resources));
			reqs.SetInterpreter(ReservationRequestInterpreter.RAll);
			rDef.SetReservationRequests(reqs);
			rDef.SetArrival(arrival);
			rDef.SetDeadline(deadline);
			return rDef;
		}

		private IDictionary<ReservationInterval, ReservationRequest> GenerateAllocation(int
			 startTime, int[] alloc, bool isStep)
		{
			IDictionary<ReservationInterval, ReservationRequest> req = new Dictionary<ReservationInterval
				, ReservationRequest>();
			int numContainers = 0;
			for (int i = 0; i < alloc.Length; i++)
			{
				if (isStep)
				{
					numContainers = alloc[i] + i;
				}
				else
				{
					numContainers = alloc[i];
				}
				ReservationRequest rr = ReservationRequest.NewInstance(Org.Apache.Hadoop.Yarn.Api.Records.Resource
					.NewInstance(1024, 1), (numContainers));
				req[new ReservationInterval(startTime + i, startTime + i + 1)] = rr;
			}
			return req;
		}
	}
}
