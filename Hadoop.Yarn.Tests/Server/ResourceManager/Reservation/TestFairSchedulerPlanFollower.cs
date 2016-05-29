using NUnit.Framework;
using NUnit.Framework.Rules;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Resource;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair;
using Org.Apache.Hadoop.Yarn.Server.Security;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Mockito;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation
{
	public class TestFairSchedulerPlanFollower : TestSchedulerPlanFollowerBase
	{
		private static readonly string AllocFile = new FilePath(FairSchedulerTestBase.TestDir
			, typeof(TestFairReservationSystem).FullName + ".xml").GetAbsolutePath();

		private RMContext rmContext;

		private RMContext spyRMContext;

		private FairScheduler fs;

		private Configuration conf;

		private FairSchedulerTestBase testHelper = new FairSchedulerTestBase();

		[Rule]
		public TestName name = new TestName();

		protected internal virtual Configuration CreateConfiguration()
		{
			Configuration conf = testHelper.CreateConfiguration();
			conf.SetClass(YarnConfiguration.RmScheduler, typeof(FairScheduler), typeof(ResourceScheduler
				));
			conf.Set(FairSchedulerConfiguration.AllocationFile, AllocFile);
			return conf;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			conf = CreateConfiguration();
			ReservationSystemTestUtil.SetupFSAllocationFile(AllocFile);
			ReservationSystemTestUtil testUtil = new ReservationSystemTestUtil();
			// Setup
			rmContext = TestUtils.GetMockRMContext();
			spyRMContext = Org.Mockito.Mockito.Spy(rmContext);
			fs = ReservationSystemTestUtil.SetupFairScheduler(testUtil, spyRMContext, conf, 125
				);
			scheduler = fs;
			ConcurrentMap<ApplicationId, RMApp> spyApps = Org.Mockito.Mockito.Spy(new ConcurrentHashMap
				<ApplicationId, RMApp>());
			RMApp rmApp = Org.Mockito.Mockito.Mock<RMApp>();
			Org.Mockito.Mockito.When(rmApp.GetRMAppAttempt((ApplicationAttemptId)Matchers.Any
				())).ThenReturn(null);
			Org.Mockito.Mockito.DoReturn(rmApp).When(spyApps)[(ApplicationId)Matchers.Any()];
			Org.Mockito.Mockito.When(spyRMContext.GetRMApps()).ThenReturn(spyApps);
			ReservationSystemTestUtil.SetupFSAllocationFile(AllocFile);
			SetupPlanFollower();
		}

		/// <exception cref="System.Exception"/>
		private void SetupPlanFollower()
		{
			ReservationSystemTestUtil testUtil = new ReservationSystemTestUtil();
			mClock = Org.Mockito.Mockito.Mock<Clock>();
			mAgent = Org.Mockito.Mockito.Mock<ReservationAgent>();
			string reservationQ = testUtil.GetFullReservationQueueName();
			AllocationConfiguration allocConf = fs.GetAllocationConfiguration();
			allocConf.SetReservationWindow(20L);
			allocConf.SetAverageCapacity(20);
			policy.Init(reservationQ, allocConf);
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
			NUnit.Framework.Assert.IsTrue(((FSQueue)defQ).GetWeights().GetWeight(ResourceType
				.Memory) > 0.9);
		}

		protected internal override Queue GetDefaultQueue()
		{
			return GetReservationQueue("dedicated" + ReservationConstants.DefaultQueueSuffix);
		}

		protected internal override int GetNumberOfApplications(Queue queue)
		{
			int numberOfApplications = fs.GetAppsInQueue(queue.GetQueueName()).Count;
			return numberOfApplications;
		}

		protected internal override AbstractSchedulerPlanFollower CreatePlanFollower()
		{
			FairSchedulerPlanFollower planFollower = new FairSchedulerPlanFollower();
			planFollower.Init(mClock, scheduler, Collections.SingletonList(plan));
			return planFollower;
		}

		protected internal override void AssertReservationQueueExists(ReservationId r)
		{
			Queue q = GetReservationQueue(r.ToString());
			NUnit.Framework.Assert.IsNotNull(q);
		}

		protected internal override void AssertReservationQueueExists(ReservationId r, double
			 expectedCapacity, double expectedMaxCapacity)
		{
			FSLeafQueue q = fs.GetQueueManager().GetLeafQueue(plan.GetQueueName() + string.Empty
				 + "." + r, false);
			NUnit.Framework.Assert.IsNotNull(q);
			// For now we are setting both to same weight
			NUnit.Framework.Assert.AreEqual(expectedCapacity, q.GetWeights().GetWeight(ResourceType
				.Memory), 0.01);
		}

		protected internal override void AssertReservationQueueDoesNotExist(ReservationId
			 r)
		{
			Queue q = GetReservationQueue(r.ToString());
			NUnit.Framework.Assert.IsNull(q);
		}

		protected internal override Queue GetReservationQueue(string r)
		{
			return fs.GetQueueManager().GetLeafQueue(plan.GetQueueName() + string.Empty + "."
				 + r, false);
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
				fs.Stop();
			}
		}
	}
}
