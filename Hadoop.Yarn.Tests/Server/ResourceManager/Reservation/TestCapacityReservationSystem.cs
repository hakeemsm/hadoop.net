using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation
{
	public class TestCapacityReservationSystem
	{
		[NUnit.Framework.Test]
		public virtual void TestInitialize()
		{
			ReservationSystemTestUtil testUtil = new ReservationSystemTestUtil();
			CapacityScheduler capScheduler = null;
			try
			{
				capScheduler = testUtil.MockCapacityScheduler(10);
			}
			catch (IOException e)
			{
				NUnit.Framework.Assert.Fail(e.Message);
			}
			CapacityReservationSystem reservationSystem = new CapacityReservationSystem();
			reservationSystem.SetRMContext(capScheduler.GetRMContext());
			try
			{
				reservationSystem.Reinitialize(capScheduler.GetConf(), capScheduler.GetRMContext(
					));
			}
			catch (YarnException e)
			{
				NUnit.Framework.Assert.Fail(e.Message);
			}
			string planQName = testUtil.GetreservationQueueName();
			ReservationSystemTestUtil.ValidateReservationQueue(reservationSystem, planQName);
		}

		[NUnit.Framework.Test]
		public virtual void TestReinitialize()
		{
			ReservationSystemTestUtil testUtil = new ReservationSystemTestUtil();
			CapacityScheduler capScheduler = null;
			try
			{
				capScheduler = testUtil.MockCapacityScheduler(10);
			}
			catch (IOException e)
			{
				NUnit.Framework.Assert.Fail(e.Message);
			}
			CapacityReservationSystem reservationSystem = new CapacityReservationSystem();
			CapacitySchedulerConfiguration conf = capScheduler.GetConfiguration();
			RMContext mockContext = capScheduler.GetRMContext();
			reservationSystem.SetRMContext(mockContext);
			try
			{
				reservationSystem.Reinitialize(capScheduler.GetConfiguration(), mockContext);
			}
			catch (YarnException e)
			{
				NUnit.Framework.Assert.Fail(e.Message);
			}
			// Assert queue in original config
			string planQName = testUtil.GetreservationQueueName();
			ReservationSystemTestUtil.ValidateReservationQueue(reservationSystem, planQName);
			// Dynamically add a plan
			string newQ = "reservation";
			NUnit.Framework.Assert.IsNull(reservationSystem.GetPlan(newQ));
			testUtil.UpdateQueueConfiguration(conf, newQ);
			try
			{
				capScheduler.Reinitialize(conf, mockContext);
			}
			catch (IOException e)
			{
				NUnit.Framework.Assert.Fail(e.Message);
			}
			try
			{
				reservationSystem.Reinitialize(conf, mockContext);
			}
			catch (YarnException e)
			{
				NUnit.Framework.Assert.Fail(e.Message);
			}
			ReservationSystemTestUtil.ValidateNewReservationQueue(reservationSystem, newQ);
		}
	}
}
