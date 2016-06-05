using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords.Impl.PB;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Org.Mockito;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation
{
	public class TestReservationInputValidator
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestReservationInputValidator
			));

		private const string PlanName = "test-reservation";

		private Clock clock;

		private IDictionary<string, Plan> plans = new Dictionary<string, Plan>(1);

		private ReservationSystem rSystem;

		private Plan plan;

		private ReservationInputValidator rrValidator;

		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			clock = Org.Mockito.Mockito.Mock<Clock>();
			plan = Org.Mockito.Mockito.Mock<Plan>();
			rSystem = Org.Mockito.Mockito.Mock<ReservationSystem>();
			plans[PlanName] = plan;
			rrValidator = new ReservationInputValidator(clock);
			Org.Mockito.Mockito.When(clock.GetTime()).ThenReturn(1L);
			ResourceCalculator rCalc = new DefaultResourceCalculator();
			Org.Apache.Hadoop.Yarn.Api.Records.Resource resource = Org.Apache.Hadoop.Yarn.Api.Records.Resource
				.NewInstance(10240, 10);
			Org.Mockito.Mockito.When(plan.GetResourceCalculator()).ThenReturn(rCalc);
			Org.Mockito.Mockito.When(plan.GetTotalCapacity()).ThenReturn(resource);
			Org.Mockito.Mockito.When(rSystem.GetQueueForReservation(Matchers.Any<ReservationId
				>())).ThenReturn(PlanName);
			Org.Mockito.Mockito.When(rSystem.GetPlan(PlanName)).ThenReturn(plan);
		}

		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			rrValidator = null;
			clock = null;
			plan = null;
		}

		[NUnit.Framework.Test]
		public virtual void TestSubmitReservationNormal()
		{
			ReservationSubmissionRequest request = CreateSimpleReservationSubmissionRequest(1
				, 1, 1, 5, 3);
			Plan plan = null;
			try
			{
				plan = rrValidator.ValidateReservationSubmissionRequest(rSystem, request, ReservationSystemTestUtil
					.GetNewReservationId());
			}
			catch (YarnException e)
			{
				NUnit.Framework.Assert.Fail(e.Message);
			}
			NUnit.Framework.Assert.IsNotNull(plan);
		}

		[NUnit.Framework.Test]
		public virtual void TestSubmitReservationDoesnotExist()
		{
			ReservationSubmissionRequest request = new ReservationSubmissionRequestPBImpl();
			Plan plan = null;
			try
			{
				plan = rrValidator.ValidateReservationSubmissionRequest(rSystem, request, ReservationSystemTestUtil
					.GetNewReservationId());
				NUnit.Framework.Assert.Fail();
			}
			catch (YarnException e)
			{
				NUnit.Framework.Assert.IsNull(plan);
				string message = e.Message;
				NUnit.Framework.Assert.IsTrue(message.Equals("The queue to submit is not specified. Please try again with a valid reservable queue."
					));
				Log.Info(message);
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestSubmitReservationInvalidPlan()
		{
			ReservationSubmissionRequest request = CreateSimpleReservationSubmissionRequest(1
				, 1, 1, 5, 3);
			Org.Mockito.Mockito.When(rSystem.GetPlan(PlanName)).ThenReturn(null);
			Plan plan = null;
			try
			{
				plan = rrValidator.ValidateReservationSubmissionRequest(rSystem, request, ReservationSystemTestUtil
					.GetNewReservationId());
				NUnit.Framework.Assert.Fail();
			}
			catch (YarnException e)
			{
				NUnit.Framework.Assert.IsNull(plan);
				string message = e.Message;
				NUnit.Framework.Assert.IsTrue(message.EndsWith(" is not managed by reservation system. Please try again with a valid reservable queue."
					));
				Log.Info(message);
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestSubmitReservationNoDefinition()
		{
			ReservationSubmissionRequest request = new ReservationSubmissionRequestPBImpl();
			request.SetQueue(PlanName);
			Plan plan = null;
			try
			{
				plan = rrValidator.ValidateReservationSubmissionRequest(rSystem, request, ReservationSystemTestUtil
					.GetNewReservationId());
				NUnit.Framework.Assert.Fail();
			}
			catch (YarnException e)
			{
				NUnit.Framework.Assert.IsNull(plan);
				string message = e.Message;
				NUnit.Framework.Assert.IsTrue(message.Equals("Missing reservation definition. Please try again by specifying a reservation definition."
					));
				Log.Info(message);
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestSubmitReservationInvalidDeadline()
		{
			ReservationSubmissionRequest request = CreateSimpleReservationSubmissionRequest(1
				, 1, 1, 0, 3);
			Plan plan = null;
			try
			{
				plan = rrValidator.ValidateReservationSubmissionRequest(rSystem, request, ReservationSystemTestUtil
					.GetNewReservationId());
				NUnit.Framework.Assert.Fail();
			}
			catch (YarnException e)
			{
				NUnit.Framework.Assert.IsNull(plan);
				string message = e.Message;
				NUnit.Framework.Assert.IsTrue(message.StartsWith("The specified deadline: 0 is the past"
					));
				Log.Info(message);
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestSubmitReservationInvalidRR()
		{
			ReservationSubmissionRequest request = CreateSimpleReservationSubmissionRequest(0
				, 0, 1, 5, 3);
			Plan plan = null;
			try
			{
				plan = rrValidator.ValidateReservationSubmissionRequest(rSystem, request, ReservationSystemTestUtil
					.GetNewReservationId());
				NUnit.Framework.Assert.Fail();
			}
			catch (YarnException e)
			{
				NUnit.Framework.Assert.IsNull(plan);
				string message = e.Message;
				NUnit.Framework.Assert.IsTrue(message.StartsWith("No resources have been specified to reserve"
					));
				Log.Info(message);
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestSubmitReservationEmptyRR()
		{
			ReservationSubmissionRequest request = CreateSimpleReservationSubmissionRequest(1
				, 0, 1, 5, 3);
			Plan plan = null;
			try
			{
				plan = rrValidator.ValidateReservationSubmissionRequest(rSystem, request, ReservationSystemTestUtil
					.GetNewReservationId());
				NUnit.Framework.Assert.Fail();
			}
			catch (YarnException e)
			{
				NUnit.Framework.Assert.IsNull(plan);
				string message = e.Message;
				NUnit.Framework.Assert.IsTrue(message.StartsWith("No resources have been specified to reserve"
					));
				Log.Info(message);
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestSubmitReservationInvalidDuration()
		{
			ReservationSubmissionRequest request = CreateSimpleReservationSubmissionRequest(1
				, 1, 1, 3, 4);
			Plan plan = null;
			try
			{
				plan = rrValidator.ValidateReservationSubmissionRequest(rSystem, request, ReservationSystemTestUtil
					.GetNewReservationId());
				NUnit.Framework.Assert.Fail();
			}
			catch (YarnException e)
			{
				NUnit.Framework.Assert.IsNull(plan);
				string message = e.Message;
				NUnit.Framework.Assert.IsTrue(message.StartsWith("The time difference"));
				NUnit.Framework.Assert.IsTrue(message.Contains("must  be greater or equal to the minimum resource duration"
					));
				Log.Info(message);
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestSubmitReservationExceedsGangSize()
		{
			ReservationSubmissionRequest request = CreateSimpleReservationSubmissionRequest(1
				, 1, 1, 5, 4);
			Org.Apache.Hadoop.Yarn.Api.Records.Resource resource = Org.Apache.Hadoop.Yarn.Api.Records.Resource
				.NewInstance(512, 1);
			Org.Mockito.Mockito.When(plan.GetTotalCapacity()).ThenReturn(resource);
			Plan plan = null;
			try
			{
				plan = rrValidator.ValidateReservationSubmissionRequest(rSystem, request, ReservationSystemTestUtil
					.GetNewReservationId());
				NUnit.Framework.Assert.Fail();
			}
			catch (YarnException e)
			{
				NUnit.Framework.Assert.IsNull(plan);
				string message = e.Message;
				NUnit.Framework.Assert.IsTrue(message.StartsWith("The size of the largest gang in the reservation refinition"
					));
				NUnit.Framework.Assert.IsTrue(message.Contains("exceed the capacity available "));
				Log.Info(message);
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestUpdateReservationNormal()
		{
			ReservationUpdateRequest request = CreateSimpleReservationUpdateRequest(1, 1, 1, 
				5, 3);
			Plan plan = null;
			try
			{
				plan = rrValidator.ValidateReservationUpdateRequest(rSystem, request);
			}
			catch (YarnException e)
			{
				NUnit.Framework.Assert.Fail(e.Message);
			}
			NUnit.Framework.Assert.IsNotNull(plan);
		}

		[NUnit.Framework.Test]
		public virtual void TestUpdateReservationNoID()
		{
			ReservationUpdateRequest request = new ReservationUpdateRequestPBImpl();
			Plan plan = null;
			try
			{
				plan = rrValidator.ValidateReservationUpdateRequest(rSystem, request);
				NUnit.Framework.Assert.Fail();
			}
			catch (YarnException e)
			{
				NUnit.Framework.Assert.IsNull(plan);
				string message = e.Message;
				NUnit.Framework.Assert.IsTrue(message.StartsWith("Missing reservation id. Please try again by specifying a reservation id."
					));
				Log.Info(message);
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestUpdateReservationDoesnotExist()
		{
			ReservationUpdateRequest request = CreateSimpleReservationUpdateRequest(1, 1, 1, 
				5, 4);
			ReservationId rId = request.GetReservationId();
			Org.Mockito.Mockito.When(rSystem.GetQueueForReservation(rId)).ThenReturn(null);
			Plan plan = null;
			try
			{
				plan = rrValidator.ValidateReservationUpdateRequest(rSystem, request);
				NUnit.Framework.Assert.Fail();
			}
			catch (YarnException e)
			{
				NUnit.Framework.Assert.IsNull(plan);
				string message = e.Message;
				NUnit.Framework.Assert.IsTrue(message.Equals(MessageFormat.Format("The specified reservation with ID: {0} is unknown. Please try again with a valid reservation."
					, rId)));
				Log.Info(message);
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestUpdateReservationInvalidPlan()
		{
			ReservationUpdateRequest request = CreateSimpleReservationUpdateRequest(1, 1, 1, 
				5, 4);
			Org.Mockito.Mockito.When(rSystem.GetPlan(PlanName)).ThenReturn(null);
			Plan plan = null;
			try
			{
				plan = rrValidator.ValidateReservationUpdateRequest(rSystem, request);
				NUnit.Framework.Assert.Fail();
			}
			catch (YarnException e)
			{
				NUnit.Framework.Assert.IsNull(plan);
				string message = e.Message;
				NUnit.Framework.Assert.IsTrue(message.EndsWith(" is not associated with any valid plan. Please try again with a valid reservation."
					));
				Log.Info(message);
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestUpdateReservationNoDefinition()
		{
			ReservationUpdateRequest request = new ReservationUpdateRequestPBImpl();
			request.SetReservationId(ReservationSystemTestUtil.GetNewReservationId());
			Plan plan = null;
			try
			{
				plan = rrValidator.ValidateReservationUpdateRequest(rSystem, request);
				NUnit.Framework.Assert.Fail();
			}
			catch (YarnException e)
			{
				NUnit.Framework.Assert.IsNull(plan);
				string message = e.Message;
				NUnit.Framework.Assert.IsTrue(message.StartsWith("Missing reservation definition. Please try again by specifying a reservation definition."
					));
				Log.Info(message);
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestUpdateReservationInvalidDeadline()
		{
			ReservationUpdateRequest request = CreateSimpleReservationUpdateRequest(1, 1, 1, 
				0, 3);
			Plan plan = null;
			try
			{
				plan = rrValidator.ValidateReservationUpdateRequest(rSystem, request);
				NUnit.Framework.Assert.Fail();
			}
			catch (YarnException e)
			{
				NUnit.Framework.Assert.IsNull(plan);
				string message = e.Message;
				NUnit.Framework.Assert.IsTrue(message.StartsWith("The specified deadline: 0 is the past"
					));
				Log.Info(message);
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestUpdateReservationInvalidRR()
		{
			ReservationUpdateRequest request = CreateSimpleReservationUpdateRequest(0, 0, 1, 
				5, 3);
			Plan plan = null;
			try
			{
				plan = rrValidator.ValidateReservationUpdateRequest(rSystem, request);
				NUnit.Framework.Assert.Fail();
			}
			catch (YarnException e)
			{
				NUnit.Framework.Assert.IsNull(plan);
				string message = e.Message;
				NUnit.Framework.Assert.IsTrue(message.StartsWith("No resources have been specified to reserve"
					));
				Log.Info(message);
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestUpdateReservationEmptyRR()
		{
			ReservationUpdateRequest request = CreateSimpleReservationUpdateRequest(1, 0, 1, 
				5, 3);
			Plan plan = null;
			try
			{
				plan = rrValidator.ValidateReservationUpdateRequest(rSystem, request);
				NUnit.Framework.Assert.Fail();
			}
			catch (YarnException e)
			{
				NUnit.Framework.Assert.IsNull(plan);
				string message = e.Message;
				NUnit.Framework.Assert.IsTrue(message.StartsWith("No resources have been specified to reserve"
					));
				Log.Info(message);
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestUpdateReservationInvalidDuration()
		{
			ReservationUpdateRequest request = CreateSimpleReservationUpdateRequest(1, 1, 1, 
				3, 4);
			Plan plan = null;
			try
			{
				plan = rrValidator.ValidateReservationUpdateRequest(rSystem, request);
				NUnit.Framework.Assert.Fail();
			}
			catch (YarnException e)
			{
				NUnit.Framework.Assert.IsNull(plan);
				string message = e.Message;
				NUnit.Framework.Assert.IsTrue(message.Contains("must  be greater or equal to the minimum resource duration"
					));
				Log.Info(message);
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestUpdateReservationExceedsGangSize()
		{
			ReservationUpdateRequest request = CreateSimpleReservationUpdateRequest(1, 1, 1, 
				5, 4);
			Org.Apache.Hadoop.Yarn.Api.Records.Resource resource = Org.Apache.Hadoop.Yarn.Api.Records.Resource
				.NewInstance(512, 1);
			Org.Mockito.Mockito.When(plan.GetTotalCapacity()).ThenReturn(resource);
			Plan plan = null;
			try
			{
				plan = rrValidator.ValidateReservationUpdateRequest(rSystem, request);
				NUnit.Framework.Assert.Fail();
			}
			catch (YarnException e)
			{
				NUnit.Framework.Assert.IsNull(plan);
				string message = e.Message;
				NUnit.Framework.Assert.IsTrue(message.StartsWith("The size of the largest gang in the reservation refinition"
					));
				NUnit.Framework.Assert.IsTrue(message.Contains("exceed the capacity available "));
				Log.Info(message);
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestDeleteReservationNormal()
		{
			ReservationDeleteRequest request = new ReservationDeleteRequestPBImpl();
			ReservationId reservationID = ReservationSystemTestUtil.GetNewReservationId();
			request.SetReservationId(reservationID);
			ReservationAllocation reservation = Org.Mockito.Mockito.Mock<ReservationAllocation
				>();
			Org.Mockito.Mockito.When(plan.GetReservationById(reservationID)).ThenReturn(reservation
				);
			Plan plan = null;
			try
			{
				plan = rrValidator.ValidateReservationDeleteRequest(rSystem, request);
			}
			catch (YarnException e)
			{
				NUnit.Framework.Assert.Fail(e.Message);
			}
			NUnit.Framework.Assert.IsNotNull(plan);
		}

		[NUnit.Framework.Test]
		public virtual void TestDeleteReservationNoID()
		{
			ReservationDeleteRequest request = new ReservationDeleteRequestPBImpl();
			Plan plan = null;
			try
			{
				plan = rrValidator.ValidateReservationDeleteRequest(rSystem, request);
				NUnit.Framework.Assert.Fail();
			}
			catch (YarnException e)
			{
				NUnit.Framework.Assert.IsNull(plan);
				string message = e.Message;
				NUnit.Framework.Assert.IsTrue(message.StartsWith("Missing reservation id. Please try again by specifying a reservation id."
					));
				Log.Info(message);
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestDeleteReservationDoesnotExist()
		{
			ReservationDeleteRequest request = new ReservationDeleteRequestPBImpl();
			ReservationId rId = ReservationSystemTestUtil.GetNewReservationId();
			request.SetReservationId(rId);
			Org.Mockito.Mockito.When(rSystem.GetQueueForReservation(rId)).ThenReturn(null);
			Plan plan = null;
			try
			{
				plan = rrValidator.ValidateReservationDeleteRequest(rSystem, request);
				NUnit.Framework.Assert.Fail();
			}
			catch (YarnException e)
			{
				NUnit.Framework.Assert.IsNull(plan);
				string message = e.Message;
				NUnit.Framework.Assert.IsTrue(message.Equals(MessageFormat.Format("The specified reservation with ID: {0} is unknown. Please try again with a valid reservation."
					, rId)));
				Log.Info(message);
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestDeleteReservationInvalidPlan()
		{
			ReservationDeleteRequest request = new ReservationDeleteRequestPBImpl();
			ReservationId reservationID = ReservationSystemTestUtil.GetNewReservationId();
			request.SetReservationId(reservationID);
			Org.Mockito.Mockito.When(rSystem.GetPlan(PlanName)).ThenReturn(null);
			Plan plan = null;
			try
			{
				plan = rrValidator.ValidateReservationDeleteRequest(rSystem, request);
				NUnit.Framework.Assert.Fail();
			}
			catch (YarnException e)
			{
				NUnit.Framework.Assert.IsNull(plan);
				string message = e.Message;
				NUnit.Framework.Assert.IsTrue(message.EndsWith(" is not associated with any valid plan. Please try again with a valid reservation."
					));
				Log.Info(message);
			}
		}

		private ReservationSubmissionRequest CreateSimpleReservationSubmissionRequest(int
			 numRequests, int numContainers, long arrival, long deadline, long duration)
		{
			// create a request with a single atomic ask
			ReservationSubmissionRequest request = new ReservationSubmissionRequestPBImpl();
			ReservationDefinition rDef = new ReservationDefinitionPBImpl();
			rDef.SetArrival(arrival);
			rDef.SetDeadline(deadline);
			if (numRequests > 0)
			{
				ReservationRequests reqs = new ReservationRequestsPBImpl();
				rDef.SetReservationRequests(reqs);
				if (numContainers > 0)
				{
					ReservationRequest r = ReservationRequest.NewInstance(Org.Apache.Hadoop.Yarn.Api.Records.Resource
						.NewInstance(1024, 1), numContainers, 1, duration);
					reqs.SetReservationResources(Sharpen.Collections.SingletonList(r));
					reqs.SetInterpreter(ReservationRequestInterpreter.RAll);
				}
			}
			request.SetQueue(PlanName);
			request.SetReservationDefinition(rDef);
			return request;
		}

		private ReservationUpdateRequest CreateSimpleReservationUpdateRequest(int numRequests
			, int numContainers, long arrival, long deadline, long duration)
		{
			// create a request with a single atomic ask
			ReservationUpdateRequest request = new ReservationUpdateRequestPBImpl();
			ReservationDefinition rDef = new ReservationDefinitionPBImpl();
			rDef.SetArrival(arrival);
			rDef.SetDeadline(deadline);
			if (numRequests > 0)
			{
				ReservationRequests reqs = new ReservationRequestsPBImpl();
				rDef.SetReservationRequests(reqs);
				if (numContainers > 0)
				{
					ReservationRequest r = ReservationRequest.NewInstance(Org.Apache.Hadoop.Yarn.Api.Records.Resource
						.NewInstance(1024, 1), numContainers, 1, duration);
					reqs.SetReservationResources(Sharpen.Collections.SingletonList(r));
					reqs.SetInterpreter(ReservationRequestInterpreter.RAll);
				}
			}
			request.SetReservationDefinition(rDef);
			request.SetReservationId(ReservationSystemTestUtil.GetNewReservationId());
			return request;
		}
	}
}
