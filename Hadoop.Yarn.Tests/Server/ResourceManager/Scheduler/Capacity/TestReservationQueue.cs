using NUnit.Framework;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Common;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity
{
	public class TestReservationQueue
	{
		internal CapacitySchedulerConfiguration csConf;

		internal CapacitySchedulerContext csContext;

		internal const int DefMaxApps = 10000;

		internal const int Gb = 1024;

		private readonly ResourceCalculator resourceCalculator = new DefaultResourceCalculator
			();

		internal ReservationQueue reservationQueue;

		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public virtual void Setup()
		{
			// setup a context / conf
			csConf = new CapacitySchedulerConfiguration();
			YarnConfiguration conf = new YarnConfiguration();
			csContext = Org.Mockito.Mockito.Mock<CapacitySchedulerContext>();
			Org.Mockito.Mockito.When(csContext.GetConfiguration()).ThenReturn(csConf);
			Org.Mockito.Mockito.When(csContext.GetConf()).ThenReturn(conf);
			Org.Mockito.Mockito.When(csContext.GetMinimumResourceCapability()).ThenReturn(Resources
				.CreateResource(Gb, 1));
			Org.Mockito.Mockito.When(csContext.GetMaximumResourceCapability()).ThenReturn(Resources
				.CreateResource(16 * Gb, 32));
			Org.Mockito.Mockito.When(csContext.GetClusterResource()).ThenReturn(Resources.CreateResource
				(100 * 16 * Gb, 100 * 32));
			Org.Mockito.Mockito.When(csContext.GetResourceCalculator()).ThenReturn(resourceCalculator
				);
			RMContext mockRMContext = TestUtils.GetMockRMContext();
			Org.Mockito.Mockito.When(csContext.GetRMContext()).ThenReturn(mockRMContext);
			// create a queue
			PlanQueue pq = new PlanQueue(csContext, "root", null, null);
			reservationQueue = new ReservationQueue(csContext, "a", pq);
		}

		private void ValidateReservationQueue(double capacity)
		{
			NUnit.Framework.Assert.IsTrue(" actual capacity: " + reservationQueue.GetCapacity
				(), reservationQueue.GetCapacity() - capacity < CSQueueUtils.Epsilon);
			NUnit.Framework.Assert.AreEqual(reservationQueue.maxApplications, DefMaxApps);
			NUnit.Framework.Assert.AreEqual(reservationQueue.maxApplicationsPerUser, DefMaxApps
				);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAddSubtractCapacity()
		{
			// verify that setting, adding, subtracting capacity works
			reservationQueue.SetCapacity(1.0F);
			ValidateReservationQueue(1);
			reservationQueue.SetEntitlement(new QueueEntitlement(0.9f, 1f));
			ValidateReservationQueue(0.9);
			reservationQueue.SetEntitlement(new QueueEntitlement(1f, 1f));
			ValidateReservationQueue(1);
			reservationQueue.SetEntitlement(new QueueEntitlement(0f, 1f));
			ValidateReservationQueue(0);
			try
			{
				reservationQueue.SetEntitlement(new QueueEntitlement(1.1f, 1f));
				NUnit.Framework.Assert.Fail();
			}
			catch (SchedulerDynamicEditException)
			{
				// expected
				ValidateReservationQueue(1);
			}
			try
			{
				reservationQueue.SetEntitlement(new QueueEntitlement(-0.1f, 1f));
				NUnit.Framework.Assert.Fail();
			}
			catch (SchedulerDynamicEditException)
			{
				// expected
				ValidateReservationQueue(1);
			}
		}
	}
}
