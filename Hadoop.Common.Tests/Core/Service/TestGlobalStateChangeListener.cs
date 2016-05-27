/*
* Licensed to the Apache Software Foundation (ASF) under one
*  or more contributor license agreements.  See the NOTICE file
*  distributed with this work for additional information
*  regarding copyright ownership.  The ASF licenses this file
*  to you under the Apache License, Version 2.0 (the
*  "License"); you may not use this file except in compliance
*  with the License.  You may obtain a copy of the License at
*
*       http://www.apache.org/licenses/LICENSE-2.0
*
*  Unless required by applicable law or agreed to in writing, software
*  distributed under the License is distributed on an "AS IS" BASIS,
*  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*  See the License for the specific language governing permissions and
*  limitations under the License.
*/
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Service
{
	/// <summary>Test global state changes.</summary>
	/// <remarks>
	/// Test global state changes. It is critical for all tests to clean up the
	/// global listener afterwards to avoid interfering with follow-on tests.
	/// One listener,
	/// <see cref="listener"/>
	/// is defined which is automatically
	/// unregistered on cleanup. All other listeners must be unregistered in the
	/// finally clauses of the tests.
	/// </remarks>
	public class TestGlobalStateChangeListener : ServiceAssert
	{
		internal BreakableStateChangeListener listener = new BreakableStateChangeListener
			("listener");

		private void Register()
		{
			Register(listener);
		}

		private bool Unregister()
		{
			return Unregister(listener);
		}

		private void Register(ServiceStateChangeListener l)
		{
			AbstractService.RegisterGlobalListener(l);
		}

		private bool Unregister(ServiceStateChangeListener l)
		{
			return AbstractService.UnregisterGlobalListener(l);
		}

		/// <summary>After every test case reset the list of global listeners.</summary>
		[TearDown]
		public virtual void Cleanup()
		{
			AbstractService.ResetGlobalListeners();
		}

		/// <summary>Assert that the last state of the listener is that the test expected.</summary>
		/// <param name="breakable">a breakable listener</param>
		/// <param name="state">the expected state</param>
		public virtual void AssertListenerState(BreakableStateChangeListener breakable, Service.STATE
			 state)
		{
			NUnit.Framework.Assert.AreEqual("Wrong state in " + breakable, state, breakable.GetLastState
				());
		}

		/// <summary>Assert that the number of state change notifications matches expectations.
		/// 	</summary>
		/// <param name="breakable">the listener</param>
		/// <param name="count">the expected count.</param>
		public virtual void AssertListenerEventCount(BreakableStateChangeListener breakable
			, int count)
		{
			NUnit.Framework.Assert.AreEqual("Wrong event count in " + breakable, count, breakable
				.GetEventCount());
		}

		/// <summary>Test that register/unregister works</summary>
		[NUnit.Framework.Test]
		public virtual void TestRegisterListener()
		{
			Register();
			NUnit.Framework.Assert.IsTrue("listener not registered", Unregister());
		}

		/// <summary>Test that double registration results in one registration only.</summary>
		[NUnit.Framework.Test]
		public virtual void TestRegisterListenerTwice()
		{
			Register();
			Register();
			NUnit.Framework.Assert.IsTrue("listener not registered", Unregister());
			//there should be no listener to unregister the second time
			NUnit.Framework.Assert.IsFalse("listener double registered", Unregister());
		}

		/// <summary>
		/// Test that the
		/// <see cref="BreakableStateChangeListener"/>
		/// is picking up
		/// the state changes and that its last event field is as expected.
		/// </summary>
		[NUnit.Framework.Test]
		public virtual void TestEventHistory()
		{
			Register();
			BreakableService service = new BreakableService();
			AssertListenerState(listener, Service.STATE.Notinited);
			NUnit.Framework.Assert.AreEqual(0, listener.GetEventCount());
			service.Init(new Configuration());
			AssertListenerState(listener, Service.STATE.Inited);
			NUnit.Framework.Assert.AreSame(service, listener.GetLastService());
			AssertListenerEventCount(listener, 1);
			service.Start();
			AssertListenerState(listener, Service.STATE.Started);
			AssertListenerEventCount(listener, 2);
			service.Stop();
			AssertListenerState(listener, Service.STATE.Stopped);
			AssertListenerEventCount(listener, 3);
		}

		/// <summary>
		/// This test triggers a failure in the listener - the expectation is that the
		/// service has already reached it's desired state, purely because the
		/// notifications take place afterwards.
		/// </summary>
		[NUnit.Framework.Test]
		public virtual void TestListenerFailure()
		{
			listener.SetFailingState(Service.STATE.Inited);
			Register();
			BreakableStateChangeListener l2 = new BreakableStateChangeListener();
			Register(l2);
			BreakableService service = new BreakableService();
			service.Init(new Configuration());
			//expected notifications to fail
			//still should record its invocation
			AssertListenerState(listener, Service.STATE.Inited);
			AssertListenerEventCount(listener, 1);
			//and second listener didn't get notified of anything
			AssertListenerEventCount(l2, 0);
			//service should still consider itself started
			AssertServiceStateInited(service);
			service.Start();
			service.Stop();
		}

		/// <summary>
		/// Create a chain of listeners and set one in the middle to fail; verify that
		/// those in front got called, and those after did not.
		/// </summary>
		[NUnit.Framework.Test]
		public virtual void TestListenerChain()
		{
			//create and register the listeners
			LoggingStateChangeListener logListener = new LoggingStateChangeListener();
			Register(logListener);
			BreakableStateChangeListener l0 = new BreakableStateChangeListener("l0");
			Register(l0);
			listener.SetFailingState(Service.STATE.Started);
			Register();
			BreakableStateChangeListener l3 = new BreakableStateChangeListener("l3");
			Register(l3);
			//create and init a service.
			BreakableService service = new BreakableService();
			service.Init(new Configuration());
			AssertServiceStateInited(service);
			AssertListenerState(l0, Service.STATE.Inited);
			AssertListenerState(listener, Service.STATE.Inited);
			AssertListenerState(l3, Service.STATE.Inited);
			service.Start();
			//expect that listener l1 and the failing listener are in start, but
			//not the final one
			AssertServiceStateStarted(service);
			AssertListenerState(l0, Service.STATE.Started);
			AssertListenerEventCount(l0, 2);
			AssertListenerState(listener, Service.STATE.Started);
			AssertListenerEventCount(listener, 2);
			//this is the listener that is not expected to have been invoked
			AssertListenerState(l3, Service.STATE.Inited);
			AssertListenerEventCount(l3, 1);
			//stop the service
			service.Stop();
			//listeners are all updated
			AssertListenerEventCount(l0, 3);
			AssertListenerEventCount(listener, 3);
			AssertListenerEventCount(l3, 2);
			//can all be unregistered in any order
			Unregister(logListener);
			Unregister(l0);
			Unregister(l3);
			//check that the listeners are all unregistered, even
			//though they were registered in a different order.
			//rather than do this by doing unregister checks, a new service is created
			service = new BreakableService();
			//this service is initialized
			service.Init(new Configuration());
			//it is asserted that the event count has not changed for the unregistered
			//listeners
			AssertListenerEventCount(l0, 3);
			AssertListenerEventCount(l3, 2);
			//except for the one listener that was not unregistered, which
			//has incremented by one
			AssertListenerEventCount(listener, 4);
		}
	}
}
