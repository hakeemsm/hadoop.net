/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
using NUnit.Framework;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Event;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation
{
	public abstract class TestSchedulerPlanFollowerBase
	{
		internal const int Gb = 1024;

		protected internal Clock mClock = null;

		protected internal ResourceScheduler scheduler = null;

		protected internal ReservationAgent mAgent;

		protected internal Resource minAlloc = Resource.NewInstance(Gb, 1);

		protected internal Resource maxAlloc = Resource.NewInstance(Gb * 8, 8);

		protected internal CapacityOverTimePolicy policy = new CapacityOverTimePolicy();

		protected internal Plan plan;

		private ResourceCalculator res = new DefaultResourceCalculator();

		/// <exception cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation.Exceptions.PlanningException
		/// 	"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		protected internal virtual void TestPlanFollower(bool isMove)
		{
			// Initialize plan based on move flag
			plan = new InMemoryPlan(scheduler.GetRootQueueMetrics(), policy, mAgent, scheduler
				.GetClusterResource(), 1L, res, scheduler.GetMinimumResourceCapability(), maxAlloc
				, "dedicated", null, isMove);
			// add a few reservations to the plan
			long ts = Runtime.CurrentTimeMillis();
			ReservationId r1 = ReservationId.NewInstance(ts, 1);
			int[] f1 = new int[] { 10, 10, 10, 10, 10 };
			NUnit.Framework.Assert.IsTrue(plan.ToString(), plan.AddReservation(new InMemoryReservationAllocation
				(r1, null, "u3", "dedicated", 0, 0 + f1.Length, ReservationSystemTestUtil.GenerateAllocation
				(0L, 1L, f1), res, minAlloc)));
			ReservationId r2 = ReservationId.NewInstance(ts, 2);
			NUnit.Framework.Assert.IsTrue(plan.ToString(), plan.AddReservation(new InMemoryReservationAllocation
				(r2, null, "u3", "dedicated", 3, 3 + f1.Length, ReservationSystemTestUtil.GenerateAllocation
				(3L, 1L, f1), res, minAlloc)));
			ReservationId r3 = ReservationId.NewInstance(ts, 3);
			int[] f2 = new int[] { 0, 10, 20, 10, 0 };
			NUnit.Framework.Assert.IsTrue(plan.ToString(), plan.AddReservation(new InMemoryReservationAllocation
				(r3, null, "u4", "dedicated", 10, 10 + f2.Length, ReservationSystemTestUtil.GenerateAllocation
				(10L, 1L, f2), res, minAlloc)));
			AbstractSchedulerPlanFollower planFollower = CreatePlanFollower();
			Org.Mockito.Mockito.When(mClock.GetTime()).ThenReturn(0L);
			planFollower.Run();
			Queue q = GetReservationQueue(r1.ToString());
			AssertReservationQueueExists(r1);
			// submit an app to r1
			string user_0 = "test-user";
			ApplicationId appId = ApplicationId.NewInstance(0, 1);
			ApplicationAttemptId appAttemptId_0 = ApplicationAttemptId.NewInstance(appId, 0);
			AppAddedSchedulerEvent addAppEvent = new AppAddedSchedulerEvent(appId, q.GetQueueName
				(), user_0);
			scheduler.Handle(addAppEvent);
			AppAttemptAddedSchedulerEvent appAttemptAddedEvent = new AppAttemptAddedSchedulerEvent
				(appAttemptId_0, false);
			scheduler.Handle(appAttemptAddedEvent);
			// initial default reservation queue should have no apps
			Queue defQ = GetDefaultQueue();
			NUnit.Framework.Assert.AreEqual(0, GetNumberOfApplications(defQ));
			AssertReservationQueueExists(r1, 0.1, 0.1);
			NUnit.Framework.Assert.AreEqual(1, GetNumberOfApplications(q));
			AssertReservationQueueDoesNotExist(r2);
			AssertReservationQueueDoesNotExist(r3);
			Org.Mockito.Mockito.When(mClock.GetTime()).ThenReturn(3L);
			planFollower.Run();
			NUnit.Framework.Assert.AreEqual(0, GetNumberOfApplications(defQ));
			AssertReservationQueueExists(r1, 0.1, 0.1);
			NUnit.Framework.Assert.AreEqual(1, GetNumberOfApplications(q));
			AssertReservationQueueExists(r2, 0.1, 0.1);
			AssertReservationQueueDoesNotExist(r3);
			Org.Mockito.Mockito.When(mClock.GetTime()).ThenReturn(10L);
			planFollower.Run();
			q = GetReservationQueue(r1.ToString());
			if (isMove)
			{
				// app should have been moved to default reservation queue
				NUnit.Framework.Assert.AreEqual(1, GetNumberOfApplications(defQ));
				NUnit.Framework.Assert.IsNull(q);
			}
			else
			{
				// app should be killed
				NUnit.Framework.Assert.AreEqual(0, GetNumberOfApplications(defQ));
				NUnit.Framework.Assert.IsNotNull(q);
				AppAttemptRemovedSchedulerEvent appAttemptRemovedEvent = new AppAttemptRemovedSchedulerEvent
					(appAttemptId_0, RMAppAttemptState.Killed, false);
				scheduler.Handle(appAttemptRemovedEvent);
			}
			AssertReservationQueueDoesNotExist(r2);
			AssertReservationQueueExists(r3, 0, 1.0);
			Org.Mockito.Mockito.When(mClock.GetTime()).ThenReturn(11L);
			planFollower.Run();
			if (isMove)
			{
				// app should have been moved to default reservation queue
				NUnit.Framework.Assert.AreEqual(1, GetNumberOfApplications(defQ));
			}
			else
			{
				// app should be killed
				NUnit.Framework.Assert.AreEqual(0, GetNumberOfApplications(defQ));
			}
			AssertReservationQueueDoesNotExist(r1);
			AssertReservationQueueDoesNotExist(r2);
			AssertReservationQueueExists(r3, 0.1, 0.1);
			Org.Mockito.Mockito.When(mClock.GetTime()).ThenReturn(12L);
			planFollower.Run();
			AssertReservationQueueDoesNotExist(r1);
			AssertReservationQueueDoesNotExist(r2);
			AssertReservationQueueExists(r3, 0.2, 0.2);
			Org.Mockito.Mockito.When(mClock.GetTime()).ThenReturn(16L);
			planFollower.Run();
			AssertReservationQueueDoesNotExist(r1);
			AssertReservationQueueDoesNotExist(r2);
			AssertReservationQueueDoesNotExist(r3);
			VerifyCapacity(defQ);
		}

		protected internal abstract Queue GetReservationQueue(string reservationId);

		protected internal abstract void VerifyCapacity(Queue defQ);

		protected internal abstract Queue GetDefaultQueue();

		protected internal abstract int GetNumberOfApplications(Queue queue);

		protected internal abstract AbstractSchedulerPlanFollower CreatePlanFollower();

		protected internal abstract void AssertReservationQueueExists(ReservationId r);

		protected internal abstract void AssertReservationQueueExists(ReservationId r2, double
			 expectedCapacity, double expectedMaxCapacity);

		protected internal abstract void AssertReservationQueueDoesNotExist(ReservationId
			 r2);
	}
}
