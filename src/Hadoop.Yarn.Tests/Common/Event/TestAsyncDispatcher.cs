using System;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Event
{
	public class TestAsyncDispatcher
	{
		/* This test checks whether dispatcher hangs on close if following two things
		* happen :
		* 1. A thread which was putting event to event queue is interrupted.
		* 2. Event queue is empty on close.
		*/
		/// <exception cref="System.Exception"/>
		public virtual void TestDispatcherOnCloseIfQueueEmpty()
		{
			BlockingQueue<Org.Apache.Hadoop.Yarn.Event.Event> eventQueue = Org.Mockito.Mockito.Spy
				(new LinkedBlockingQueue<Org.Apache.Hadoop.Yarn.Event.Event>());
			Org.Apache.Hadoop.Yarn.Event.Event @event = Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Yarn.Event.Event
				>();
			Org.Mockito.Mockito.DoThrow(new Exception()).When(eventQueue).Put(@event);
			DrainDispatcher disp = new DrainDispatcher(eventQueue);
			disp.Init(new Configuration());
			disp.SetDrainEventsOnStop();
			disp.Start();
			// Wait for event handler thread to start and begin waiting for events.
			disp.WaitForEventThreadToWait();
			try
			{
				disp.GetEventHandler().Handle(@event);
				NUnit.Framework.Assert.Fail("Expected YarnRuntimeException");
			}
			catch (YarnRuntimeException e)
			{
				NUnit.Framework.Assert.IsTrue(e.InnerException is Exception);
			}
			// Queue should be empty and dispatcher should not hang on close
			NUnit.Framework.Assert.IsTrue("Event Queue should have been empty", eventQueue.IsEmpty
				());
			disp.Close();
		}

		// Test dispatcher should timeout on draining events.
		/// <exception cref="System.Exception"/>
		public virtual void TestDispatchStopOnTimeout()
		{
			BlockingQueue<Org.Apache.Hadoop.Yarn.Event.Event> eventQueue = new LinkedBlockingQueue
				<Org.Apache.Hadoop.Yarn.Event.Event>();
			eventQueue = Org.Mockito.Mockito.Spy(eventQueue);
			// simulate dispatcher is not drained.
			Org.Mockito.Mockito.When(eventQueue.IsEmpty()).ThenReturn(false);
			YarnConfiguration conf = new YarnConfiguration();
			conf.SetInt(YarnConfiguration.DispatcherDrainEventsTimeout, 2000);
			DrainDispatcher disp = new DrainDispatcher(eventQueue);
			disp.Init(conf);
			disp.SetDrainEventsOnStop();
			disp.Start();
			disp.WaitForEventThreadToWait();
			disp.Close();
		}
	}
}
