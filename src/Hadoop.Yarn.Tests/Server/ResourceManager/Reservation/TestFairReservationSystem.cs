using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation
{
	public class TestFairReservationSystem
	{
		private static readonly string AllocFile = new FilePath(FairSchedulerTestBase.TestDir
			, typeof(TestFairReservationSystem).FullName + ".xml").GetAbsolutePath();

		private Configuration conf;

		private FairScheduler scheduler;

		private FairSchedulerTestBase testHelper = new FairSchedulerTestBase();

		protected internal virtual Configuration CreateConfiguration()
		{
			Configuration conf = testHelper.CreateConfiguration();
			conf.SetClass(YarnConfiguration.RmScheduler, typeof(FairScheduler), typeof(ResourceScheduler
				));
			conf.Set(FairSchedulerConfiguration.AllocationFile, AllocFile);
			return conf;
		}

		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public virtual void Setup()
		{
			conf = CreateConfiguration();
		}

		[TearDown]
		public virtual void Teardown()
		{
			conf = null;
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestFairReservationSystemInitialize()
		{
			ReservationSystemTestUtil.SetupFSAllocationFile(AllocFile);
			ReservationSystemTestUtil testUtil = new ReservationSystemTestUtil();
			// Setup
			RMContext mockRMContext = ReservationSystemTestUtil.CreateRMContext(conf);
			scheduler = ReservationSystemTestUtil.SetupFairScheduler(testUtil, mockRMContext, 
				conf, 10);
			FairReservationSystem reservationSystem = new FairReservationSystem();
			reservationSystem.SetRMContext(mockRMContext);
			try
			{
				reservationSystem.Reinitialize(scheduler.GetConf(), mockRMContext);
			}
			catch (YarnException e)
			{
				NUnit.Framework.Assert.Fail(e.Message);
			}
			ReservationSystemTestUtil.ValidateReservationQueue(reservationSystem, testUtil.GetFullReservationQueueName
				());
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestFairReservationSystemReinitialize()
		{
			ReservationSystemTestUtil.SetupFSAllocationFile(AllocFile);
			ReservationSystemTestUtil testUtil = new ReservationSystemTestUtil();
			// Setup
			RMContext mockRMContext = ReservationSystemTestUtil.CreateRMContext(conf);
			scheduler = ReservationSystemTestUtil.SetupFairScheduler(testUtil, mockRMContext, 
				conf, 10);
			FairReservationSystem reservationSystem = new FairReservationSystem();
			reservationSystem.SetRMContext(mockRMContext);
			try
			{
				reservationSystem.Reinitialize(scheduler.GetConf(), mockRMContext);
			}
			catch (YarnException e)
			{
				NUnit.Framework.Assert.Fail(e.Message);
			}
			// Assert queue in original config
			string planQNam = testUtil.GetFullReservationQueueName();
			ReservationSystemTestUtil.ValidateReservationQueue(reservationSystem, planQNam);
			// Dynamically add a plan
			ReservationSystemTestUtil.UpdateFSAllocationFile(AllocFile);
			scheduler.Reinitialize(conf, mockRMContext);
			try
			{
				reservationSystem.Reinitialize(conf, mockRMContext);
			}
			catch (YarnException e)
			{
				NUnit.Framework.Assert.Fail(e.Message);
			}
			string newQueue = "root.reservation";
			ReservationSystemTestUtil.ValidateNewReservationQueue(reservationSystem, newQueue
				);
		}
	}
}
