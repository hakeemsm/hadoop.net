using System;
using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Common;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity
{
	public class TestCapacitySchedulerDynamicBehavior
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestCapacitySchedulerDynamicBehavior
			));

		private const string A = CapacitySchedulerConfiguration.Root + ".a";

		private const string B = CapacitySchedulerConfiguration.Root + ".b";

		private const string B1 = B + ".b1";

		private const string B2 = B + ".b2";

		private const string B3 = B + ".b3";

		private static float ACapacity = 10.5f;

		private static float BCapacity = 89.5f;

		private static float A1Capacity = 30;

		private static float A2Capacity = 70;

		private static float B1Capacity = 79.2f;

		private static float B2Capacity = 0.8f;

		private static float B3Capacity = 20;

		private readonly TestCapacityScheduler tcs = new TestCapacityScheduler();

		private int Gb = 1024;

		private MockRM rm;

		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
			SetupPlanQueueConfiguration(conf);
			conf.SetClass(YarnConfiguration.RmScheduler, typeof(CapacityScheduler), typeof(ResourceScheduler
				));
			conf.SetBoolean(YarnConfiguration.RmReservationSystemEnable, false);
			rm = new MockRM(conf);
			rm.Start();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRefreshQueuesWithReservations()
		{
			CapacityScheduler cs = (CapacityScheduler)rm.GetResourceScheduler();
			// Test add one reservation dynamically and manually modify capacity
			ReservationQueue a1 = new ReservationQueue(cs, "a1", (PlanQueue)cs.GetQueue("a"));
			cs.AddQueue(a1);
			a1.SetEntitlement(new QueueEntitlement(A1Capacity / 100, 1f));
			// Test add another reservation queue and use setEntitlement to modify
			// capacity
			ReservationQueue a2 = new ReservationQueue(cs, "a2", (PlanQueue)cs.GetQueue("a"));
			cs.AddQueue(a2);
			cs.SetEntitlement("a2", new QueueEntitlement(A2Capacity / 100, 1.0f));
			// Verify all allocations match
			tcs.CheckQueueCapacities(cs, ACapacity, BCapacity);
			// Reinitialize and verify all dynamic queued survived
			CapacitySchedulerConfiguration conf = cs.GetConfiguration();
			conf.SetCapacity(A, 80f);
			conf.SetCapacity(B, 20f);
			cs.Reinitialize(conf, rm.GetRMContext());
			tcs.CheckQueueCapacities(cs, 80f, 20f);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAddQueueFailCases()
		{
			CapacityScheduler cs = (CapacityScheduler)rm.GetResourceScheduler();
			try
			{
				// Test invalid addition (adding non-zero size queue)
				ReservationQueue a1 = new ReservationQueue(cs, "a1", (PlanQueue)cs.GetQueue("a"));
				a1.SetEntitlement(new QueueEntitlement(A1Capacity / 100, 1f));
				cs.AddQueue(a1);
				NUnit.Framework.Assert.Fail();
			}
			catch (Exception)
			{
			}
			// expected
			// Test add one reservation dynamically and manually modify capacity
			ReservationQueue a1_1 = new ReservationQueue(cs, "a1", (PlanQueue)cs.GetQueue("a"
				));
			cs.AddQueue(a1_1);
			a1_1.SetEntitlement(new QueueEntitlement(A1Capacity / 100, 1f));
			// Test add another reservation queue and use setEntitlement to modify
			// capacity
			ReservationQueue a2 = new ReservationQueue(cs, "a2", (PlanQueue)cs.GetQueue("a"));
			cs.AddQueue(a2);
			try
			{
				// Test invalid entitlement (sum of queues exceed 100%)
				cs.SetEntitlement("a2", new QueueEntitlement(A2Capacity / 100 + 0.1f, 1.0f));
				NUnit.Framework.Assert.Fail();
			}
			catch (Exception)
			{
			}
			// expected
			cs.SetEntitlement("a2", new QueueEntitlement(A2Capacity / 100, 1.0f));
			// Verify all allocations match
			tcs.CheckQueueCapacities(cs, ACapacity, BCapacity);
			cs.Stop();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRemoveQueue()
		{
			CapacityScheduler cs = (CapacityScheduler)rm.GetResourceScheduler();
			// Test add one reservation dynamically and manually modify capacity
			ReservationQueue a1 = new ReservationQueue(cs, "a1", (PlanQueue)cs.GetQueue("a"));
			cs.AddQueue(a1);
			a1.SetEntitlement(new QueueEntitlement(A1Capacity / 100, 1f));
			// submit an app
			RMApp app = rm.SubmitApp(Gb, "test-move-1", "user_0", null, "a1");
			// check preconditions
			IList<ApplicationAttemptId> appsInA1 = cs.GetAppsInQueue("a1");
			NUnit.Framework.Assert.AreEqual(1, appsInA1.Count);
			try
			{
				cs.RemoveQueue("a1");
				NUnit.Framework.Assert.Fail();
			}
			catch (SchedulerDynamicEditException)
			{
			}
			// expected a1 contains applications
			// clear queue by killling all apps
			cs.KillAllAppsInQueue("a1");
			// wait for events of move to propagate
			rm.WaitForState(app.GetApplicationId(), RMAppState.Killed);
			try
			{
				cs.RemoveQueue("a1");
				NUnit.Framework.Assert.Fail();
			}
			catch (SchedulerDynamicEditException)
			{
			}
			// expected a1 is not zero capacity
			// set capacity to zero
			cs.SetEntitlement("a1", new QueueEntitlement(0f, 0f));
			cs.RemoveQueue("a1");
			NUnit.Framework.Assert.IsTrue(cs.GetQueue("a1") == null);
			rm.Stop();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMoveAppToPlanQueue()
		{
			CapacityScheduler scheduler = (CapacityScheduler)rm.GetResourceScheduler();
			// submit an app
			RMApp app = rm.SubmitApp(Gb, "test-move-1", "user_0", null, "b1");
			ApplicationAttemptId appAttemptId = rm.GetApplicationReport(app.GetApplicationId(
				)).GetCurrentApplicationAttemptId();
			// check preconditions
			IList<ApplicationAttemptId> appsInB1 = scheduler.GetAppsInQueue("b1");
			NUnit.Framework.Assert.AreEqual(1, appsInB1.Count);
			IList<ApplicationAttemptId> appsInB = scheduler.GetAppsInQueue("b");
			NUnit.Framework.Assert.AreEqual(1, appsInB.Count);
			NUnit.Framework.Assert.IsTrue(appsInB.Contains(appAttemptId));
			IList<ApplicationAttemptId> appsInA = scheduler.GetAppsInQueue("a");
			NUnit.Framework.Assert.IsTrue(appsInA.IsEmpty());
			string queue = scheduler.GetApplicationAttempt(appsInB1[0]).GetQueue().GetQueueName
				();
			NUnit.Framework.Assert.IsTrue(queue.Equals("b1"));
			IList<ApplicationAttemptId> appsInRoot = scheduler.GetAppsInQueue("root");
			NUnit.Framework.Assert.IsTrue(appsInRoot.Contains(appAttemptId));
			NUnit.Framework.Assert.AreEqual(1, appsInRoot.Count);
			// create the default reservation queue
			string defQName = "a" + ReservationConstants.DefaultQueueSuffix;
			ReservationQueue defQ = new ReservationQueue(scheduler, defQName, (PlanQueue)scheduler
				.GetQueue("a"));
			scheduler.AddQueue(defQ);
			defQ.SetEntitlement(new QueueEntitlement(1f, 1f));
			IList<ApplicationAttemptId> appsInDefQ = scheduler.GetAppsInQueue(defQName);
			NUnit.Framework.Assert.IsTrue(appsInDefQ.IsEmpty());
			// now move the app to plan queue
			scheduler.MoveApplication(app.GetApplicationId(), "a");
			// check postconditions
			appsInDefQ = scheduler.GetAppsInQueue(defQName);
			NUnit.Framework.Assert.AreEqual(1, appsInDefQ.Count);
			queue = scheduler.GetApplicationAttempt(appsInDefQ[0]).GetQueue().GetQueueName();
			NUnit.Framework.Assert.IsTrue(queue.Equals(defQName));
			appsInA = scheduler.GetAppsInQueue("a");
			NUnit.Framework.Assert.IsTrue(appsInA.Contains(appAttemptId));
			NUnit.Framework.Assert.AreEqual(1, appsInA.Count);
			appsInRoot = scheduler.GetAppsInQueue("root");
			NUnit.Framework.Assert.IsTrue(appsInRoot.Contains(appAttemptId));
			NUnit.Framework.Assert.AreEqual(1, appsInRoot.Count);
			appsInB1 = scheduler.GetAppsInQueue("b1");
			NUnit.Framework.Assert.IsTrue(appsInB1.IsEmpty());
			appsInB = scheduler.GetAppsInQueue("b");
			NUnit.Framework.Assert.IsTrue(appsInB.IsEmpty());
			rm.Stop();
		}

		private void SetupPlanQueueConfiguration(CapacitySchedulerConfiguration conf)
		{
			conf.SetQueues(CapacitySchedulerConfiguration.Root, new string[] { "a", "b" });
			conf.SetCapacity(A, ACapacity);
			conf.SetCapacity(B, BCapacity);
			// Define 2nd-level queues
			conf.SetQueues(B, new string[] { "b1", "b2", "b3" });
			conf.SetCapacity(B1, B1Capacity);
			conf.SetUserLimitFactor(B1, 100.0f);
			conf.SetCapacity(B2, B2Capacity);
			conf.SetUserLimitFactor(B2, 100.0f);
			conf.SetCapacity(B3, B3Capacity);
			conf.SetUserLimitFactor(B3, 100.0f);
			conf.SetReservable(A, true);
			conf.SetReservationWindow(A, 86400 * 1000);
			conf.SetAverageCapacity(A, 1.0f);
			Log.Info("Setup a as a plan queue");
		}
	}
}
