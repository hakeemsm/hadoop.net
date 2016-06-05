using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation
{
	public class TestInMemoryReservationAllocation
	{
		private string user = "yarn";

		private string planName = "test-reservation";

		private ResourceCalculator resCalc;

		private Org.Apache.Hadoop.Yarn.Api.Records.Resource minAlloc;

		private Random rand = new Random();

		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			resCalc = new DefaultResourceCalculator();
			minAlloc = Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance(1, 1);
		}

		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			user = null;
			planName = null;
			resCalc = null;
			minAlloc = null;
		}

		[NUnit.Framework.Test]
		public virtual void TestBlocks()
		{
			ReservationId reservationID = ReservationId.NewInstance(rand.NextLong(), rand.NextLong
				());
			int[] alloc = new int[] { 10, 10, 10, 10, 10, 10 };
			int start = 100;
			ReservationDefinition rDef = CreateSimpleReservationDefinition(start, start + alloc
				.Length + 1, alloc.Length);
			IDictionary<ReservationInterval, ReservationRequest> allocations = GenerateAllocation
				(start, alloc, false, false);
			ReservationAllocation rAllocation = new InMemoryReservationAllocation(reservationID
				, rDef, user, planName, start, start + alloc.Length + 1, allocations, resCalc, minAlloc
				);
			DoAssertions(rAllocation, reservationID, rDef, allocations, start, alloc);
			NUnit.Framework.Assert.IsFalse(rAllocation.ContainsGangs());
			for (int i = 0; i < alloc.Length; i++)
			{
				NUnit.Framework.Assert.AreEqual(Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance
					(1024 * (alloc[i]), (alloc[i])), rAllocation.GetResourcesAtTime(start + i));
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestSteps()
		{
			ReservationId reservationID = ReservationId.NewInstance(rand.NextLong(), rand.NextLong
				());
			int[] alloc = new int[] { 10, 10, 10, 10, 10, 10 };
			int start = 100;
			ReservationDefinition rDef = CreateSimpleReservationDefinition(start, start + alloc
				.Length + 1, alloc.Length);
			IDictionary<ReservationInterval, ReservationRequest> allocations = GenerateAllocation
				(start, alloc, true, false);
			ReservationAllocation rAllocation = new InMemoryReservationAllocation(reservationID
				, rDef, user, planName, start, start + alloc.Length + 1, allocations, resCalc, minAlloc
				);
			DoAssertions(rAllocation, reservationID, rDef, allocations, start, alloc);
			NUnit.Framework.Assert.IsFalse(rAllocation.ContainsGangs());
			for (int i = 0; i < alloc.Length; i++)
			{
				NUnit.Framework.Assert.AreEqual(Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance
					(1024 * (alloc[i] + i), (alloc[i] + i)), rAllocation.GetResourcesAtTime(start + 
					i));
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestSkyline()
		{
			ReservationId reservationID = ReservationId.NewInstance(rand.NextLong(), rand.NextLong
				());
			int[] alloc = new int[] { 0, 5, 10, 10, 5, 0 };
			int start = 100;
			ReservationDefinition rDef = CreateSimpleReservationDefinition(start, start + alloc
				.Length + 1, alloc.Length);
			IDictionary<ReservationInterval, ReservationRequest> allocations = GenerateAllocation
				(start, alloc, true, false);
			ReservationAllocation rAllocation = new InMemoryReservationAllocation(reservationID
				, rDef, user, planName, start, start + alloc.Length + 1, allocations, resCalc, minAlloc
				);
			DoAssertions(rAllocation, reservationID, rDef, allocations, start, alloc);
			NUnit.Framework.Assert.IsFalse(rAllocation.ContainsGangs());
			for (int i = 0; i < alloc.Length; i++)
			{
				NUnit.Framework.Assert.AreEqual(Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance
					(1024 * (alloc[i] + i), (alloc[i] + i)), rAllocation.GetResourcesAtTime(start + 
					i));
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestZeroAlloaction()
		{
			ReservationId reservationID = ReservationId.NewInstance(rand.NextLong(), rand.NextLong
				());
			int[] alloc = new int[] {  };
			long start = 0;
			ReservationDefinition rDef = CreateSimpleReservationDefinition(start, start + alloc
				.Length + 1, alloc.Length);
			IDictionary<ReservationInterval, ReservationRequest> allocations = new Dictionary
				<ReservationInterval, ReservationRequest>();
			ReservationAllocation rAllocation = new InMemoryReservationAllocation(reservationID
				, rDef, user, planName, start, start + alloc.Length + 1, allocations, resCalc, minAlloc
				);
			DoAssertions(rAllocation, reservationID, rDef, allocations, (int)start, alloc);
			NUnit.Framework.Assert.IsFalse(rAllocation.ContainsGangs());
		}

		[NUnit.Framework.Test]
		public virtual void TestGangAlloaction()
		{
			ReservationId reservationID = ReservationId.NewInstance(rand.NextLong(), rand.NextLong
				());
			int[] alloc = new int[] { 10, 10, 10, 10, 10, 10 };
			int start = 100;
			ReservationDefinition rDef = CreateSimpleReservationDefinition(start, start + alloc
				.Length + 1, alloc.Length);
			IDictionary<ReservationInterval, ReservationRequest> allocations = GenerateAllocation
				(start, alloc, false, true);
			ReservationAllocation rAllocation = new InMemoryReservationAllocation(reservationID
				, rDef, user, planName, start, start + alloc.Length + 1, allocations, resCalc, minAlloc
				);
			DoAssertions(rAllocation, reservationID, rDef, allocations, start, alloc);
			NUnit.Framework.Assert.IsTrue(rAllocation.ContainsGangs());
			for (int i = 0; i < alloc.Length; i++)
			{
				NUnit.Framework.Assert.AreEqual(Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance
					(1024 * (alloc[i]), (alloc[i])), rAllocation.GetResourcesAtTime(start + i));
			}
		}

		private void DoAssertions(ReservationAllocation rAllocation, ReservationId reservationID
			, ReservationDefinition rDef, IDictionary<ReservationInterval, ReservationRequest
			> allocations, int start, int[] alloc)
		{
			NUnit.Framework.Assert.AreEqual(reservationID, rAllocation.GetReservationId());
			NUnit.Framework.Assert.AreEqual(rDef, rAllocation.GetReservationDefinition());
			NUnit.Framework.Assert.AreEqual(allocations, rAllocation.GetAllocationRequests());
			NUnit.Framework.Assert.AreEqual(user, rAllocation.GetUser());
			NUnit.Framework.Assert.AreEqual(planName, rAllocation.GetPlanName());
			NUnit.Framework.Assert.AreEqual(start, rAllocation.GetStartTime());
			NUnit.Framework.Assert.AreEqual(start + alloc.Length + 1, rAllocation.GetEndTime(
				));
		}

		private ReservationDefinition CreateSimpleReservationDefinition(long arrival, long
			 deadline, long duration)
		{
			// create a request with a single atomic ask
			ReservationRequest r = ReservationRequest.NewInstance(Org.Apache.Hadoop.Yarn.Api.Records.Resource
				.NewInstance(1024, 1), 1, 1, duration);
			ReservationDefinition rDef = new ReservationDefinitionPBImpl();
			ReservationRequests reqs = new ReservationRequestsPBImpl();
			reqs.SetReservationResources(Sharpen.Collections.SingletonList(r));
			reqs.SetInterpreter(ReservationRequestInterpreter.RAll);
			rDef.SetReservationRequests(reqs);
			rDef.SetArrival(arrival);
			rDef.SetDeadline(deadline);
			return rDef;
		}

		private IDictionary<ReservationInterval, ReservationRequest> GenerateAllocation(int
			 startTime, int[] alloc, bool isStep, bool isGang)
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
				if (isGang)
				{
					rr.SetConcurrency(numContainers);
				}
				req[new ReservationInterval(startTime + i, startTime + i + 1)] = rr;
			}
			return req;
		}
	}
}
