using System;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;


namespace Org.Apache.Hadoop.Service
{
	public class TestServiceLifecycle : ServiceAssert
	{
		private static Log Log = LogFactory.GetLog(typeof(TestServiceLifecycle));

		/// <summary>
		/// Walk the
		/// <see cref="BreakableService"/>
		/// through it's lifecycle,
		/// more to verify that service's counters work than anything else
		/// </summary>
		/// <exception cref="System.Exception">if necessary</exception>
		[Fact]
		public virtual void TestWalkthrough()
		{
			BreakableService svc = new BreakableService();
			AssertServiceStateCreated(svc);
			AssertStateCount(svc, Service.STATE.Notinited, 1);
			AssertStateCount(svc, Service.STATE.Inited, 0);
			AssertStateCount(svc, Service.STATE.Started, 0);
			AssertStateCount(svc, Service.STATE.Stopped, 0);
			svc.Init(new Configuration());
			AssertServiceStateInited(svc);
			AssertStateCount(svc, Service.STATE.Inited, 1);
			svc.Start();
			AssertServiceStateStarted(svc);
			AssertStateCount(svc, Service.STATE.Started, 1);
			svc.Stop();
			AssertServiceStateStopped(svc);
			AssertStateCount(svc, Service.STATE.Stopped, 1);
		}

		/// <summary>call init twice</summary>
		/// <exception cref="System.Exception">if necessary</exception>
		[Fact]
		public virtual void TestInitTwice()
		{
			BreakableService svc = new BreakableService();
			Configuration conf = new Configuration();
			conf.Set("test.init", "t");
			svc.Init(conf);
			svc.Init(new Configuration());
			AssertStateCount(svc, Service.STATE.Inited, 1);
			AssertServiceConfigurationContains(svc, "test.init");
		}

		/// <summary>Call start twice</summary>
		/// <exception cref="System.Exception">if necessary</exception>
		[Fact]
		public virtual void TestStartTwice()
		{
			BreakableService svc = new BreakableService();
			svc.Init(new Configuration());
			svc.Start();
			svc.Start();
			AssertStateCount(svc, Service.STATE.Started, 1);
		}

		/// <summary>
		/// Verify that when a service is stopped more than once, no exception
		/// is thrown.
		/// </summary>
		/// <exception cref="System.Exception">if necessary</exception>
		[Fact]
		public virtual void TestStopTwice()
		{
			BreakableService svc = new BreakableService();
			svc.Init(new Configuration());
			svc.Start();
			svc.Stop();
			AssertStateCount(svc, Service.STATE.Stopped, 1);
			svc.Stop();
			AssertStateCount(svc, Service.STATE.Stopped, 1);
		}

		/// <summary>
		/// Show that if the service failed during an init
		/// operation, it stays in the created state, even after stopping it
		/// </summary>
		/// <exception cref="System.Exception">if necessary</exception>
		[Fact]
		public virtual void TestStopFailedInit()
		{
			BreakableService svc = new BreakableService(true, false, false);
			AssertServiceStateCreated(svc);
			try
			{
				svc.Init(new Configuration());
				NUnit.Framework.Assert.Fail("Expected a failure, got " + svc);
			}
			catch (BreakableService.BrokenLifecycleEvent)
			{
			}
			//expected
			//the service state wasn't passed
			AssertServiceStateStopped(svc);
			AssertStateCount(svc, Service.STATE.Inited, 1);
			AssertStateCount(svc, Service.STATE.Stopped, 1);
			//now try to stop
			svc.Stop();
			AssertStateCount(svc, Service.STATE.Stopped, 1);
		}

		/// <summary>
		/// Show that if the service failed during an init
		/// operation, it stays in the created state, even after stopping it
		/// </summary>
		/// <exception cref="System.Exception">if necessary</exception>
		[Fact]
		public virtual void TestStopFailedStart()
		{
			BreakableService svc = new BreakableService(false, true, false);
			svc.Init(new Configuration());
			AssertServiceStateInited(svc);
			try
			{
				svc.Start();
				NUnit.Framework.Assert.Fail("Expected a failure, got " + svc);
			}
			catch (BreakableService.BrokenLifecycleEvent)
			{
			}
			//expected
			//the service state wasn't passed
			AssertServiceStateStopped(svc);
		}

		/// <summary>
		/// verify that when a service fails during its stop operation,
		/// its state does not change.
		/// </summary>
		/// <exception cref="System.Exception">if necessary</exception>
		[Fact]
		public virtual void TestFailingStop()
		{
			BreakableService svc = new BreakableService(false, false, true);
			svc.Init(new Configuration());
			svc.Start();
			try
			{
				svc.Stop();
				NUnit.Framework.Assert.Fail("Expected a failure, got " + svc);
			}
			catch (BreakableService.BrokenLifecycleEvent)
			{
			}
			//expected
			AssertStateCount(svc, Service.STATE.Stopped, 1);
		}

		/// <summary>
		/// verify that when a service that is not started is stopped, the
		/// service enters the stopped state
		/// </summary>
		/// <exception cref="System.Exception">on a failure</exception>
		[Fact]
		public virtual void TestStopUnstarted()
		{
			BreakableService svc = new BreakableService();
			svc.Stop();
			AssertServiceStateStopped(svc);
			AssertStateCount(svc, Service.STATE.Inited, 0);
			AssertStateCount(svc, Service.STATE.Stopped, 1);
		}

		/// <summary>
		/// Show that if the service failed during an init
		/// operation, stop was called.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestStopFailingInitAndStop()
		{
			BreakableService svc = new BreakableService(true, false, true);
			svc.RegisterServiceListener(new LoggingStateChangeListener());
			try
			{
				svc.Init(new Configuration());
				NUnit.Framework.Assert.Fail("Expected a failure, got " + svc);
			}
			catch (BreakableService.BrokenLifecycleEvent e)
			{
				Assert.Equal(Service.STATE.Inited, e.state);
			}
			//the service state is stopped
			AssertServiceStateStopped(svc);
			Assert.Equal(Service.STATE.Inited, svc.GetFailureState());
			Exception failureCause = svc.GetFailureCause();
			NUnit.Framework.Assert.IsNotNull("Null failure cause in " + svc, failureCause);
			BreakableService.BrokenLifecycleEvent cause = (BreakableService.BrokenLifecycleEvent
				)failureCause;
			NUnit.Framework.Assert.IsNotNull("null state in " + cause + " raised by " + svc, 
				cause.state);
			Assert.Equal(Service.STATE.Inited, cause.state);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestInitNullConf()
		{
			BreakableService svc = new BreakableService(false, false, false);
			try
			{
				svc.Init(null);
				Log.Warn("Null Configurations are permitted ");
			}
			catch (ServiceStateException)
			{
			}
		}

		//expected
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestServiceNotifications()
		{
			BreakableService svc = new BreakableService(false, false, false);
			BreakableStateChangeListener listener = new BreakableStateChangeListener();
			svc.RegisterServiceListener(listener);
			svc.Init(new Configuration());
			AssertEventCount(listener, 1);
			svc.Start();
			AssertEventCount(listener, 2);
			svc.Stop();
			AssertEventCount(listener, 3);
			svc.Stop();
			AssertEventCount(listener, 3);
		}

		/// <summary>Test that when a service listener is unregistered, it stops being invoked
		/// 	</summary>
		/// <exception cref="System.Exception">on a failure</exception>
		[Fact]
		public virtual void TestServiceNotificationsStopOnceUnregistered()
		{
			BreakableService svc = new BreakableService(false, false, false);
			BreakableStateChangeListener listener = new BreakableStateChangeListener();
			svc.RegisterServiceListener(listener);
			svc.Init(new Configuration());
			AssertEventCount(listener, 1);
			svc.UnregisterServiceListener(listener);
			svc.Start();
			AssertEventCount(listener, 1);
			svc.Stop();
			AssertEventCount(listener, 1);
			svc.Stop();
		}

		/// <summary>This test uses a service listener that unregisters itself during the callbacks.
		/// 	</summary>
		/// <remarks>
		/// This test uses a service listener that unregisters itself during the callbacks.
		/// This a test that verifies the concurrency logic on the listener management
		/// code, that it doesn't throw any immutable state change exceptions
		/// if you change list membership during the notifications.
		/// The standard <code>AbstractService</code> implementation copies the list
		/// to an array in a <code>synchronized</code> block then iterates through
		/// the copy precisely to prevent this problem.
		/// </remarks>
		/// <exception cref="System.Exception">on a failure</exception>
		[Fact]
		public virtual void TestServiceNotificationsUnregisterDuringCallback()
		{
			BreakableService svc = new BreakableService(false, false, false);
			BreakableStateChangeListener listener = new TestServiceLifecycle.SelfUnregisteringBreakableStateChangeListener
				();
			BreakableStateChangeListener l2 = new BreakableStateChangeListener();
			svc.RegisterServiceListener(listener);
			svc.RegisterServiceListener(l2);
			svc.Init(new Configuration());
			AssertEventCount(listener, 1);
			AssertEventCount(l2, 1);
			svc.UnregisterServiceListener(listener);
			svc.Start();
			AssertEventCount(listener, 1);
			AssertEventCount(l2, 2);
			svc.Stop();
			AssertEventCount(listener, 1);
			svc.Stop();
		}

		private class SelfUnregisteringBreakableStateChangeListener : BreakableStateChangeListener
		{
			public override void StateChanged(Org.Apache.Hadoop.Service.Service service)
			{
				lock (this)
				{
					base.StateChanged(service);
					service.UnregisterServiceListener(this);
				}
			}
		}

		private void AssertEventCount(BreakableStateChangeListener listener, int expected
			)
		{
			Assert.Equal(listener.ToString(), expected, listener.GetEventCount
				());
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestServiceFailingNotifications()
		{
			BreakableService svc = new BreakableService(false, false, false);
			BreakableStateChangeListener listener = new BreakableStateChangeListener();
			listener.SetFailingState(Service.STATE.Started);
			svc.RegisterServiceListener(listener);
			svc.Init(new Configuration());
			AssertEventCount(listener, 1);
			//start this; the listener failed but this won't show
			svc.Start();
			//counter went up
			AssertEventCount(listener, 2);
			Assert.Equal(1, listener.GetFailureCount());
			//stop the service -this doesn't fail
			svc.Stop();
			AssertEventCount(listener, 3);
			Assert.Equal(1, listener.GetFailureCount());
			svc.Stop();
		}

		/// <summary>
		/// This test verifies that you can block waiting for something to happen
		/// and use notifications to manage it
		/// </summary>
		/// <exception cref="System.Exception">on a failure</exception>
		[Fact]
		public virtual void TestListenerWithNotifications()
		{
			//this tests that a listener can get notified when a service is stopped
			TestServiceLifecycle.AsyncSelfTerminatingService service = new TestServiceLifecycle.AsyncSelfTerminatingService
				(2000);
			TestServiceLifecycle.NotifyingListener listener = new TestServiceLifecycle.NotifyingListener
				();
			service.RegisterServiceListener(listener);
			service.Init(new Configuration());
			service.Start();
			AssertServiceInState(service, Service.STATE.Started);
			long start = Runtime.CurrentTimeMillis();
			lock (listener)
			{
				Runtime.Wait(listener, 20000);
			}
			long duration = Runtime.CurrentTimeMillis() - start;
			Assert.Equal(Service.STATE.Stopped, listener.notifyingState);
			AssertServiceInState(service, Service.STATE.Stopped);
			Assert.True("Duration of " + duration + " too long", duration <
				 10000);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestSelfTerminatingService()
		{
			TestServiceLifecycle.SelfTerminatingService service = new TestServiceLifecycle.SelfTerminatingService
				();
			BreakableStateChangeListener listener = new BreakableStateChangeListener();
			service.RegisterServiceListener(listener);
			service.Init(new Configuration());
			AssertEventCount(listener, 1);
			//start the service
			service.Start();
			//and expect an event count of exactly two
			AssertEventCount(listener, 2);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestStartInInitService()
		{
			Org.Apache.Hadoop.Service.Service service = new TestServiceLifecycle.StartInInitService
				();
			BreakableStateChangeListener listener = new BreakableStateChangeListener();
			service.RegisterServiceListener(listener);
			service.Init(new Configuration());
			AssertServiceInState(service, Service.STATE.Started);
			AssertEventCount(listener, 1);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestStopInInitService()
		{
			Org.Apache.Hadoop.Service.Service service = new TestServiceLifecycle.StopInInitService
				();
			BreakableStateChangeListener listener = new BreakableStateChangeListener();
			service.RegisterServiceListener(listener);
			service.Init(new Configuration());
			AssertServiceInState(service, Service.STATE.Stopped);
			AssertEventCount(listener, 1);
		}

		/// <summary>Listener that wakes up all threads waiting on it</summary>
		private class NotifyingListener : ServiceStateChangeListener
		{
			public Service.STATE notifyingState = Service.STATE.Notinited;

			public virtual void StateChanged(Org.Apache.Hadoop.Service.Service service)
			{
				lock (this)
				{
					notifyingState = service.GetServiceState();
					Runtime.NotifyAll(this);
				}
			}
		}

		/// <summary>Service that terminates itself after starting and sleeping for a while</summary>
		private class AsyncSelfTerminatingService : AbstractService, Runnable
		{
			internal readonly int timeout;

			private AsyncSelfTerminatingService(int timeout)
				: base("AsyncSelfTerminatingService")
			{
				this.timeout = timeout;
			}

			/// <exception cref="System.Exception"/>
			protected internal override void ServiceStart()
			{
				new Thread(this).Start();
				base.ServiceStart();
			}

			public virtual void Run()
			{
				try
				{
					Thread.Sleep(timeout);
				}
				catch (Exception)
				{
				}
				this.Stop();
			}
		}

		/// <summary>Service that terminates itself in startup</summary>
		private class SelfTerminatingService : AbstractService
		{
			private SelfTerminatingService()
				: base("SelfTerminatingService")
			{
			}

			/// <exception cref="System.Exception"/>
			protected internal override void ServiceStart()
			{
				//start
				base.ServiceStart();
				//then stop
				Stop();
			}
		}

		/// <summary>Service that starts itself in init</summary>
		private class StartInInitService : AbstractService
		{
			private StartInInitService()
				: base("StartInInitService")
			{
			}

			/// <exception cref="System.Exception"/>
			protected internal override void ServiceInit(Configuration conf)
			{
				base.ServiceInit(conf);
				Start();
			}
		}

		/// <summary>Service that starts itself in init</summary>
		private class StopInInitService : AbstractService
		{
			private StopInInitService()
				: base("StopInInitService")
			{
			}

			/// <exception cref="System.Exception"/>
			protected internal override void ServiceInit(Configuration conf)
			{
				base.ServiceInit(conf);
				Stop();
			}
		}
	}
}
