using System;
using NUnit.Framework;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmcontainer;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Event;
using Org.Mockito;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager
{
	public class TestRMDispatcher
	{
		public virtual void TestSchedulerEventDispatcherForPreemptionEvents()
		{
			AsyncDispatcher rmDispatcher = new AsyncDispatcher();
			CapacityScheduler sched = Org.Mockito.Mockito.Spy(new CapacityScheduler());
			YarnConfiguration conf = new YarnConfiguration();
			ResourceManager.SchedulerEventDispatcher schedulerDispatcher = new ResourceManager.SchedulerEventDispatcher
				(sched);
			rmDispatcher.Register(typeof(SchedulerEventType), schedulerDispatcher);
			rmDispatcher.Init(conf);
			rmDispatcher.Start();
			schedulerDispatcher.Init(conf);
			schedulerDispatcher.Start();
			try
			{
				ApplicationAttemptId appAttemptId = Org.Mockito.Mockito.Mock<ApplicationAttemptId
					>();
				RMContainer container = Org.Mockito.Mockito.Mock<RMContainer>();
				ContainerPreemptEvent event1 = new ContainerPreemptEvent(appAttemptId, container, 
					SchedulerEventType.DropReservation);
				rmDispatcher.GetEventHandler().Handle(event1);
				ContainerPreemptEvent event2 = new ContainerPreemptEvent(appAttemptId, container, 
					SchedulerEventType.KillContainer);
				rmDispatcher.GetEventHandler().Handle(event2);
				ContainerPreemptEvent event3 = new ContainerPreemptEvent(appAttemptId, container, 
					SchedulerEventType.PreemptContainer);
				rmDispatcher.GetEventHandler().Handle(event3);
				// Wait for events to be processed by scheduler dispatcher.
				Sharpen.Thread.Sleep(1000);
				Org.Mockito.Mockito.Verify(sched, Org.Mockito.Mockito.Times(3)).Handle(Matchers.Any
					<SchedulerEvent>());
				Org.Mockito.Mockito.Verify(sched).DropContainerReservation(container);
				Org.Mockito.Mockito.Verify(sched).PreemptContainer(appAttemptId, container);
				Org.Mockito.Mockito.Verify(sched).KillContainer(container);
			}
			catch (Exception)
			{
				NUnit.Framework.Assert.Fail();
			}
			finally
			{
				schedulerDispatcher.Stop();
				rmDispatcher.Stop();
			}
		}
	}
}
