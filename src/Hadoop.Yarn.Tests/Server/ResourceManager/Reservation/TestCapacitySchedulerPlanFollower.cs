using NUnit.Framework;
using NUnit.Framework.Rules;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Security;
using Org.Apache.Hadoop.Yarn.Server.Security;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Org.Mockito;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation
{
	public class TestCapacitySchedulerPlanFollower : TestSchedulerPlanFollowerBase
	{
		private RMContext rmContext;

		private RMContext spyRMContext;

		private CapacitySchedulerContext csContext;

		private CapacityScheduler cs;

		[Rule]
		public TestName name = new TestName();

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			CapacityScheduler spyCs = new CapacityScheduler();
			cs = Org.Mockito.Mockito.Spy(spyCs);
			scheduler = cs;
			rmContext = TestUtils.GetMockRMContext();
			spyRMContext = Org.Mockito.Mockito.Spy(rmContext);
			ConcurrentMap<ApplicationId, RMApp> spyApps = Org.Mockito.Mockito.Spy(new ConcurrentHashMap
				<ApplicationId, RMApp>());
			RMApp rmApp = Org.Mockito.Mockito.Mock<RMApp>();
			Org.Mockito.Mockito.When(rmApp.GetRMAppAttempt((ApplicationAttemptId)Matchers.Any
				())).ThenReturn(null);
			Org.Mockito.Mockito.DoReturn(rmApp).When(spyApps)[(ApplicationId)Matchers.Any()];
			Org.Mockito.Mockito.When(spyRMContext.GetRMApps()).ThenReturn(spyApps);
			Org.Mockito.Mockito.When(spyRMContext.GetScheduler()).ThenReturn(scheduler);
			CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration();
			ReservationSystemTestUtil.SetupQueueConfiguration(csConf);
			cs.SetConf(csConf);
			csContext = Org.Mockito.Mockito.Mock<CapacitySchedulerContext>();
			Org.Mockito.Mockito.When(csContext.GetConfiguration()).ThenReturn(csConf);
			Org.Mockito.Mockito.When(csContext.GetConf()).ThenReturn(csConf);
			Org.Mockito.Mockito.When(csContext.GetMinimumResourceCapability()).ThenReturn(minAlloc
				);
			Org.Mockito.Mockito.When(csContext.GetMaximumResourceCapability()).ThenReturn(maxAlloc
				);
			Org.Mockito.Mockito.When(csContext.GetClusterResource()).ThenReturn(Resources.CreateResource
				(100 * 16 * Gb, 100 * 32));
			Org.Mockito.Mockito.When(scheduler.GetClusterResource()).ThenReturn(Resources.CreateResource
				(125 * Gb, 125));
			Org.Mockito.Mockito.When(csContext.GetResourceCalculator()).ThenReturn(new DefaultResourceCalculator
				());
			RMContainerTokenSecretManager containerTokenSecretManager = new RMContainerTokenSecretManager
				(csConf);
			containerTokenSecretManager.RollMasterKey();
			Org.Mockito.Mockito.When(csContext.GetContainerTokenSecretManager()).ThenReturn(containerTokenSecretManager
				);
			cs.SetRMContext(spyRMContext);
			cs.Init(csConf);
			cs.Start();
			SetupPlanFollower();
		}

		/// <exception cref="System.Exception"/>
		private void SetupPlanFollower()
		{
			ReservationSystemTestUtil testUtil = new ReservationSystemTestUtil();
			mClock = Org.Mockito.Mockito.Mock<Clock>();
			mAgent = Org.Mockito.Mockito.Mock<ReservationAgent>();
			string reservationQ = testUtil.GetFullReservationQueueName();
			CapacitySchedulerConfiguration csConf = cs.GetConfiguration();
			csConf.SetReservationWindow(reservationQ, 20L);
			csConf.SetMaximumCapacity(reservationQ, 40);
			csConf.SetAverageCapacity(reservationQ, 20);
			policy.Init(reservationQ, csConf);
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation.Exceptions.PlanningException
		/// 	"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		[NUnit.Framework.Test]
		public virtual void TestWithMoveOnExpiry()
		{
			// invoke plan follower test with move
			TestPlanFollower(true);
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation.Exceptions.PlanningException
		/// 	"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		[NUnit.Framework.Test]
		public virtual void TestWithKillOnExpiry()
		{
			// invoke plan follower test with kill
			TestPlanFollower(false);
		}

		protected internal override void VerifyCapacity(Queue defQ)
		{
			CSQueue csQueue = (CSQueue)defQ;
			NUnit.Framework.Assert.IsTrue(csQueue.GetCapacity() > 0.9);
		}

		protected internal override Queue GetDefaultQueue()
		{
			return cs.GetQueue("dedicated" + ReservationConstants.DefaultQueueSuffix);
		}

		protected internal override int GetNumberOfApplications(Queue queue)
		{
			CSQueue csQueue = (CSQueue)queue;
			int numberOfApplications = csQueue.GetNumApplications();
			return numberOfApplications;
		}

		protected internal override AbstractSchedulerPlanFollower CreatePlanFollower()
		{
			CapacitySchedulerPlanFollower planFollower = new CapacitySchedulerPlanFollower();
			planFollower.Init(mClock, scheduler, Collections.SingletonList(plan));
			return planFollower;
		}

		protected internal override void AssertReservationQueueExists(ReservationId r)
		{
			CSQueue q = cs.GetQueue(r.ToString());
			NUnit.Framework.Assert.IsNotNull(q);
		}

		protected internal override void AssertReservationQueueExists(ReservationId r2, double
			 expectedCapacity, double expectedMaxCapacity)
		{
			CSQueue q = cs.GetQueue(r2.ToString());
			NUnit.Framework.Assert.IsNotNull(q);
			NUnit.Framework.Assert.AreEqual(expectedCapacity, q.GetCapacity(), 0.01);
			NUnit.Framework.Assert.AreEqual(expectedMaxCapacity, q.GetMaximumCapacity(), 1.0);
		}

		protected internal override void AssertReservationQueueDoesNotExist(ReservationId
			 r2)
		{
			CSQueue q2 = cs.GetQueue(r2.ToString());
			NUnit.Framework.Assert.IsNull(q2);
		}

		public static ApplicationACLsManager MockAppACLsManager()
		{
			Configuration conf = new Configuration();
			return new ApplicationACLsManager(conf);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			if (scheduler != null)
			{
				cs.Stop();
			}
		}

		protected internal override Queue GetReservationQueue(string reservationId)
		{
			return cs.GetQueue(reservationId);
		}
	}
}
