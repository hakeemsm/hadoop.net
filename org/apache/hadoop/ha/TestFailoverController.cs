using Sharpen;

namespace org.apache.hadoop.ha
{
	public class TestFailoverController
	{
		private java.net.InetSocketAddress svc1Addr = new java.net.InetSocketAddress("svc1"
			, 1234);

		private java.net.InetSocketAddress svc2Addr = new java.net.InetSocketAddress("svc2"
			, 5678);

		private org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
			();

		internal org.apache.hadoop.ha.HAServiceStatus STATE_NOT_READY = new org.apache.hadoop.ha.HAServiceStatus
			(org.apache.hadoop.ha.HAServiceProtocol.HAServiceState.STANDBY).setNotReadyToBecomeActive
			("injected not ready");

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testFailoverAndFailback()
		{
			org.apache.hadoop.ha.DummyHAService svc1 = new org.apache.hadoop.ha.DummyHAService
				(org.apache.hadoop.ha.HAServiceProtocol.HAServiceState.ACTIVE, svc1Addr);
			org.apache.hadoop.ha.DummyHAService svc2 = new org.apache.hadoop.ha.DummyHAService
				(org.apache.hadoop.ha.HAServiceProtocol.HAServiceState.STANDBY, svc2Addr);
			svc1.fencer = svc2.fencer = org.apache.hadoop.ha.TestNodeFencer.setupFencer(Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.ha.TestNodeFencer.AlwaysSucceedFencer)).getName());
			org.apache.hadoop.ha.TestNodeFencer.AlwaysSucceedFencer.fenceCalled = 0;
			doFailover(svc1, svc2, false, false);
			NUnit.Framework.Assert.AreEqual(0, org.apache.hadoop.ha.TestNodeFencer.AlwaysSucceedFencer
				.fenceCalled);
			NUnit.Framework.Assert.AreEqual(org.apache.hadoop.ha.HAServiceProtocol.HAServiceState
				.STANDBY, svc1.state);
			NUnit.Framework.Assert.AreEqual(org.apache.hadoop.ha.HAServiceProtocol.HAServiceState
				.ACTIVE, svc2.state);
			org.apache.hadoop.ha.TestNodeFencer.AlwaysSucceedFencer.fenceCalled = 0;
			doFailover(svc2, svc1, false, false);
			NUnit.Framework.Assert.AreEqual(0, org.apache.hadoop.ha.TestNodeFencer.AlwaysSucceedFencer
				.fenceCalled);
			NUnit.Framework.Assert.AreEqual(org.apache.hadoop.ha.HAServiceProtocol.HAServiceState
				.ACTIVE, svc1.state);
			NUnit.Framework.Assert.AreEqual(org.apache.hadoop.ha.HAServiceProtocol.HAServiceState
				.STANDBY, svc2.state);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testFailoverFromStandbyToStandby()
		{
			org.apache.hadoop.ha.DummyHAService svc1 = new org.apache.hadoop.ha.DummyHAService
				(org.apache.hadoop.ha.HAServiceProtocol.HAServiceState.STANDBY, svc1Addr);
			org.apache.hadoop.ha.DummyHAService svc2 = new org.apache.hadoop.ha.DummyHAService
				(org.apache.hadoop.ha.HAServiceProtocol.HAServiceState.STANDBY, svc2Addr);
			svc1.fencer = svc2.fencer = org.apache.hadoop.ha.TestNodeFencer.setupFencer(Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.ha.TestNodeFencer.AlwaysSucceedFencer)).getName());
			doFailover(svc1, svc2, false, false);
			NUnit.Framework.Assert.AreEqual(org.apache.hadoop.ha.HAServiceProtocol.HAServiceState
				.STANDBY, svc1.state);
			NUnit.Framework.Assert.AreEqual(org.apache.hadoop.ha.HAServiceProtocol.HAServiceState
				.ACTIVE, svc2.state);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testFailoverFromActiveToActive()
		{
			org.apache.hadoop.ha.DummyHAService svc1 = new org.apache.hadoop.ha.DummyHAService
				(org.apache.hadoop.ha.HAServiceProtocol.HAServiceState.ACTIVE, svc1Addr);
			org.apache.hadoop.ha.DummyHAService svc2 = new org.apache.hadoop.ha.DummyHAService
				(org.apache.hadoop.ha.HAServiceProtocol.HAServiceState.ACTIVE, svc2Addr);
			svc1.fencer = svc2.fencer = org.apache.hadoop.ha.TestNodeFencer.setupFencer(Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.ha.TestNodeFencer.AlwaysSucceedFencer)).getName());
			try
			{
				doFailover(svc1, svc2, false, false);
				NUnit.Framework.Assert.Fail("Can't failover to an already active service");
			}
			catch (org.apache.hadoop.ha.FailoverFailedException)
			{
			}
			// Expected
			NUnit.Framework.Assert.AreEqual(org.apache.hadoop.ha.HAServiceProtocol.HAServiceState
				.ACTIVE, svc1.state);
			NUnit.Framework.Assert.AreEqual(org.apache.hadoop.ha.HAServiceProtocol.HAServiceState
				.ACTIVE, svc2.state);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testFailoverWithoutPermission()
		{
			org.apache.hadoop.ha.DummyHAService svc1 = new org.apache.hadoop.ha.DummyHAService
				(org.apache.hadoop.ha.HAServiceProtocol.HAServiceState.ACTIVE, svc1Addr);
			org.mockito.Mockito.doThrow(new org.apache.hadoop.security.AccessControlException
				("Access denied")).when(svc1.proxy).getServiceStatus();
			org.apache.hadoop.ha.DummyHAService svc2 = new org.apache.hadoop.ha.DummyHAService
				(org.apache.hadoop.ha.HAServiceProtocol.HAServiceState.STANDBY, svc2Addr);
			org.mockito.Mockito.doThrow(new org.apache.hadoop.security.AccessControlException
				("Access denied")).when(svc2.proxy).getServiceStatus();
			svc1.fencer = svc2.fencer = org.apache.hadoop.ha.TestNodeFencer.setupFencer(Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.ha.TestNodeFencer.AlwaysSucceedFencer)).getName());
			try
			{
				doFailover(svc1, svc2, false, false);
				NUnit.Framework.Assert.Fail("Can't failover when access is denied");
			}
			catch (org.apache.hadoop.ha.FailoverFailedException ffe)
			{
				NUnit.Framework.Assert.IsTrue(ffe.InnerException.Message.contains("Access denied"
					));
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testFailoverToUnreadyService()
		{
			org.apache.hadoop.ha.DummyHAService svc1 = new org.apache.hadoop.ha.DummyHAService
				(org.apache.hadoop.ha.HAServiceProtocol.HAServiceState.ACTIVE, svc1Addr);
			org.apache.hadoop.ha.DummyHAService svc2 = new org.apache.hadoop.ha.DummyHAService
				(org.apache.hadoop.ha.HAServiceProtocol.HAServiceState.STANDBY, svc2Addr);
			org.mockito.Mockito.doReturn(STATE_NOT_READY).when(svc2.proxy).getServiceStatus();
			svc1.fencer = svc2.fencer = org.apache.hadoop.ha.TestNodeFencer.setupFencer(Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.ha.TestNodeFencer.AlwaysSucceedFencer)).getName());
			try
			{
				doFailover(svc1, svc2, false, false);
				NUnit.Framework.Assert.Fail("Can't failover to a service that's not ready");
			}
			catch (org.apache.hadoop.ha.FailoverFailedException ffe)
			{
				// Expected
				if (!ffe.Message.contains("injected not ready"))
				{
					throw;
				}
			}
			NUnit.Framework.Assert.AreEqual(org.apache.hadoop.ha.HAServiceProtocol.HAServiceState
				.ACTIVE, svc1.state);
			NUnit.Framework.Assert.AreEqual(org.apache.hadoop.ha.HAServiceProtocol.HAServiceState
				.STANDBY, svc2.state);
			// Forcing it means we ignore readyToBecomeActive
			doFailover(svc1, svc2, false, true);
			NUnit.Framework.Assert.AreEqual(org.apache.hadoop.ha.HAServiceProtocol.HAServiceState
				.STANDBY, svc1.state);
			NUnit.Framework.Assert.AreEqual(org.apache.hadoop.ha.HAServiceProtocol.HAServiceState
				.ACTIVE, svc2.state);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testFailoverToUnhealthyServiceFailsAndFailsback()
		{
			org.apache.hadoop.ha.DummyHAService svc1 = new org.apache.hadoop.ha.DummyHAService
				(org.apache.hadoop.ha.HAServiceProtocol.HAServiceState.ACTIVE, svc1Addr);
			org.apache.hadoop.ha.DummyHAService svc2 = new org.apache.hadoop.ha.DummyHAService
				(org.apache.hadoop.ha.HAServiceProtocol.HAServiceState.STANDBY, svc2Addr);
			org.mockito.Mockito.doThrow(new org.apache.hadoop.ha.HealthCheckFailedException("Failed!"
				)).when(svc2.proxy).monitorHealth();
			svc1.fencer = svc2.fencer = org.apache.hadoop.ha.TestNodeFencer.setupFencer(Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.ha.TestNodeFencer.AlwaysSucceedFencer)).getName());
			try
			{
				doFailover(svc1, svc2, false, false);
				NUnit.Framework.Assert.Fail("Failover to unhealthy service");
			}
			catch (org.apache.hadoop.ha.FailoverFailedException)
			{
			}
			// Expected
			NUnit.Framework.Assert.AreEqual(org.apache.hadoop.ha.HAServiceProtocol.HAServiceState
				.ACTIVE, svc1.state);
			NUnit.Framework.Assert.AreEqual(org.apache.hadoop.ha.HAServiceProtocol.HAServiceState
				.STANDBY, svc2.state);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testFailoverFromFaultyServiceSucceeds()
		{
			org.apache.hadoop.ha.DummyHAService svc1 = new org.apache.hadoop.ha.DummyHAService
				(org.apache.hadoop.ha.HAServiceProtocol.HAServiceState.ACTIVE, svc1Addr);
			org.mockito.Mockito.doThrow(new org.apache.hadoop.ha.ServiceFailedException("Failed!"
				)).when(svc1.proxy).transitionToStandby(anyReqInfo());
			org.apache.hadoop.ha.DummyHAService svc2 = new org.apache.hadoop.ha.DummyHAService
				(org.apache.hadoop.ha.HAServiceProtocol.HAServiceState.STANDBY, svc2Addr);
			svc1.fencer = svc2.fencer = org.apache.hadoop.ha.TestNodeFencer.setupFencer(Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.ha.TestNodeFencer.AlwaysSucceedFencer)).getName());
			org.apache.hadoop.ha.TestNodeFencer.AlwaysSucceedFencer.fenceCalled = 0;
			try
			{
				doFailover(svc1, svc2, false, false);
			}
			catch (org.apache.hadoop.ha.FailoverFailedException)
			{
				NUnit.Framework.Assert.Fail("Faulty active prevented failover");
			}
			// svc1 still thinks it's active, that's OK, it was fenced
			NUnit.Framework.Assert.AreEqual(1, org.apache.hadoop.ha.TestNodeFencer.AlwaysSucceedFencer
				.fenceCalled);
			NUnit.Framework.Assert.AreSame(svc1, org.apache.hadoop.ha.TestNodeFencer.AlwaysSucceedFencer
				.fencedSvc);
			NUnit.Framework.Assert.AreEqual(org.apache.hadoop.ha.HAServiceProtocol.HAServiceState
				.ACTIVE, svc1.state);
			NUnit.Framework.Assert.AreEqual(org.apache.hadoop.ha.HAServiceProtocol.HAServiceState
				.ACTIVE, svc2.state);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testFailoverFromFaultyServiceFencingFailure()
		{
			org.apache.hadoop.ha.DummyHAService svc1 = new org.apache.hadoop.ha.DummyHAService
				(org.apache.hadoop.ha.HAServiceProtocol.HAServiceState.ACTIVE, svc1Addr);
			org.mockito.Mockito.doThrow(new org.apache.hadoop.ha.ServiceFailedException("Failed!"
				)).when(svc1.proxy).transitionToStandby(anyReqInfo());
			org.apache.hadoop.ha.DummyHAService svc2 = new org.apache.hadoop.ha.DummyHAService
				(org.apache.hadoop.ha.HAServiceProtocol.HAServiceState.STANDBY, svc2Addr);
			svc1.fencer = svc2.fencer = org.apache.hadoop.ha.TestNodeFencer.setupFencer(Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.ha.TestNodeFencer.AlwaysFailFencer)).getName());
			org.apache.hadoop.ha.TestNodeFencer.AlwaysFailFencer.fenceCalled = 0;
			try
			{
				doFailover(svc1, svc2, false, false);
				NUnit.Framework.Assert.Fail("Failed over even though fencing failed");
			}
			catch (org.apache.hadoop.ha.FailoverFailedException)
			{
			}
			// Expected
			NUnit.Framework.Assert.AreEqual(1, org.apache.hadoop.ha.TestNodeFencer.AlwaysFailFencer
				.fenceCalled);
			NUnit.Framework.Assert.AreSame(svc1, org.apache.hadoop.ha.TestNodeFencer.AlwaysFailFencer
				.fencedSvc);
			NUnit.Framework.Assert.AreEqual(org.apache.hadoop.ha.HAServiceProtocol.HAServiceState
				.ACTIVE, svc1.state);
			NUnit.Framework.Assert.AreEqual(org.apache.hadoop.ha.HAServiceProtocol.HAServiceState
				.STANDBY, svc2.state);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testFencingFailureDuringFailover()
		{
			org.apache.hadoop.ha.DummyHAService svc1 = new org.apache.hadoop.ha.DummyHAService
				(org.apache.hadoop.ha.HAServiceProtocol.HAServiceState.ACTIVE, svc1Addr);
			org.apache.hadoop.ha.DummyHAService svc2 = new org.apache.hadoop.ha.DummyHAService
				(org.apache.hadoop.ha.HAServiceProtocol.HAServiceState.STANDBY, svc2Addr);
			svc1.fencer = svc2.fencer = org.apache.hadoop.ha.TestNodeFencer.setupFencer(Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.ha.TestNodeFencer.AlwaysFailFencer)).getName());
			org.apache.hadoop.ha.TestNodeFencer.AlwaysFailFencer.fenceCalled = 0;
			try
			{
				doFailover(svc1, svc2, true, false);
				NUnit.Framework.Assert.Fail("Failed over even though fencing requested and failed"
					);
			}
			catch (org.apache.hadoop.ha.FailoverFailedException)
			{
			}
			// Expected
			// If fencing was requested and it failed we don't try to make
			// svc2 active anyway, and we don't failback to svc1.
			NUnit.Framework.Assert.AreEqual(1, org.apache.hadoop.ha.TestNodeFencer.AlwaysFailFencer
				.fenceCalled);
			NUnit.Framework.Assert.AreSame(svc1, org.apache.hadoop.ha.TestNodeFencer.AlwaysFailFencer
				.fencedSvc);
			NUnit.Framework.Assert.AreEqual(org.apache.hadoop.ha.HAServiceProtocol.HAServiceState
				.STANDBY, svc1.state);
			NUnit.Framework.Assert.AreEqual(org.apache.hadoop.ha.HAServiceProtocol.HAServiceState
				.STANDBY, svc2.state);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testFailoverFromNonExistantServiceWithFencer()
		{
			org.apache.hadoop.ha.DummyHAService svc1 = org.mockito.Mockito.spy(new org.apache.hadoop.ha.DummyHAService
				(null, svc1Addr));
			// Getting a proxy to a dead server will throw IOException on call,
			// not on creation of the proxy.
			org.apache.hadoop.ha.HAServiceProtocol errorThrowingProxy = org.mockito.Mockito.mock
				<org.apache.hadoop.ha.HAServiceProtocol>(org.mockito.Mockito.withSettings().defaultAnswer
				(new org.mockito.@internal.stubbing.answers.ThrowsException(new System.IO.IOException
				("Could not connect to host"))).extraInterfaces(Sharpen.Runtime.getClassForType(
				typeof(java.io.Closeable))));
			org.mockito.Mockito.doNothing().when((java.io.Closeable)errorThrowingProxy).close
				();
			org.mockito.Mockito.doReturn(errorThrowingProxy).when(svc1).getProxy(org.mockito.Mockito
				.any<org.apache.hadoop.conf.Configuration>(), org.mockito.Mockito.anyInt());
			org.apache.hadoop.ha.DummyHAService svc2 = new org.apache.hadoop.ha.DummyHAService
				(org.apache.hadoop.ha.HAServiceProtocol.HAServiceState.STANDBY, svc2Addr);
			svc1.fencer = svc2.fencer = org.apache.hadoop.ha.TestNodeFencer.setupFencer(Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.ha.TestNodeFencer.AlwaysSucceedFencer)).getName());
			try
			{
				doFailover(svc1, svc2, false, false);
			}
			catch (org.apache.hadoop.ha.FailoverFailedException)
			{
				NUnit.Framework.Assert.Fail("Non-existant active prevented failover");
			}
			// Verify that the proxy created to try to make it go to standby
			// gracefully used the right rpc timeout
			org.mockito.Mockito.verify(svc1).getProxy(org.mockito.Mockito.any<org.apache.hadoop.conf.Configuration
				>(), org.mockito.Mockito.eq(org.apache.hadoop.fs.CommonConfigurationKeys.HA_FC_GRACEFUL_FENCE_TIMEOUT_DEFAULT
				));
			// Don't check svc1 because we can't reach it, but that's OK, it's been fenced.
			NUnit.Framework.Assert.AreEqual(org.apache.hadoop.ha.HAServiceProtocol.HAServiceState
				.ACTIVE, svc2.state);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testFailoverToNonExistantServiceFails()
		{
			org.apache.hadoop.ha.DummyHAService svc1 = new org.apache.hadoop.ha.DummyHAService
				(org.apache.hadoop.ha.HAServiceProtocol.HAServiceState.ACTIVE, svc1Addr);
			org.apache.hadoop.ha.DummyHAService svc2 = org.mockito.Mockito.spy(new org.apache.hadoop.ha.DummyHAService
				(null, svc2Addr));
			org.mockito.Mockito.doThrow(new System.IO.IOException("Failed to connect")).when(
				svc2).getProxy(org.mockito.Mockito.any<org.apache.hadoop.conf.Configuration>(), 
				org.mockito.Mockito.anyInt());
			svc1.fencer = svc2.fencer = org.apache.hadoop.ha.TestNodeFencer.setupFencer(Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.ha.TestNodeFencer.AlwaysSucceedFencer)).getName());
			try
			{
				doFailover(svc1, svc2, false, false);
				NUnit.Framework.Assert.Fail("Failed over to a non-existant standby");
			}
			catch (org.apache.hadoop.ha.FailoverFailedException)
			{
			}
			// Expected
			NUnit.Framework.Assert.AreEqual(org.apache.hadoop.ha.HAServiceProtocol.HAServiceState
				.ACTIVE, svc1.state);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testFailoverToFaultyServiceFailsbackOK()
		{
			org.apache.hadoop.ha.DummyHAService svc1 = org.mockito.Mockito.spy(new org.apache.hadoop.ha.DummyHAService
				(org.apache.hadoop.ha.HAServiceProtocol.HAServiceState.ACTIVE, svc1Addr));
			org.apache.hadoop.ha.DummyHAService svc2 = new org.apache.hadoop.ha.DummyHAService
				(org.apache.hadoop.ha.HAServiceProtocol.HAServiceState.STANDBY, svc2Addr);
			org.mockito.Mockito.doThrow(new org.apache.hadoop.ha.ServiceFailedException("Failed!"
				)).when(svc2.proxy).transitionToActive(anyReqInfo());
			svc1.fencer = svc2.fencer = org.apache.hadoop.ha.TestNodeFencer.setupFencer(Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.ha.TestNodeFencer.AlwaysSucceedFencer)).getName());
			try
			{
				doFailover(svc1, svc2, false, false);
				NUnit.Framework.Assert.Fail("Failover to already active service");
			}
			catch (org.apache.hadoop.ha.FailoverFailedException)
			{
			}
			// Expected
			// svc1 went standby then back to active
			org.mockito.Mockito.verify(svc1.proxy).transitionToStandby(anyReqInfo());
			org.mockito.Mockito.verify(svc1.proxy).transitionToActive(anyReqInfo());
			NUnit.Framework.Assert.AreEqual(org.apache.hadoop.ha.HAServiceProtocol.HAServiceState
				.ACTIVE, svc1.state);
			NUnit.Framework.Assert.AreEqual(org.apache.hadoop.ha.HAServiceProtocol.HAServiceState
				.STANDBY, svc2.state);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testWeDontFailbackIfActiveWasFenced()
		{
			org.apache.hadoop.ha.DummyHAService svc1 = new org.apache.hadoop.ha.DummyHAService
				(org.apache.hadoop.ha.HAServiceProtocol.HAServiceState.ACTIVE, svc1Addr);
			org.apache.hadoop.ha.DummyHAService svc2 = new org.apache.hadoop.ha.DummyHAService
				(org.apache.hadoop.ha.HAServiceProtocol.HAServiceState.STANDBY, svc2Addr);
			org.mockito.Mockito.doThrow(new org.apache.hadoop.ha.ServiceFailedException("Failed!"
				)).when(svc2.proxy).transitionToActive(anyReqInfo());
			svc1.fencer = svc2.fencer = org.apache.hadoop.ha.TestNodeFencer.setupFencer(Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.ha.TestNodeFencer.AlwaysSucceedFencer)).getName());
			try
			{
				doFailover(svc1, svc2, true, false);
				NUnit.Framework.Assert.Fail("Failed over to service that won't transition to active"
					);
			}
			catch (org.apache.hadoop.ha.FailoverFailedException)
			{
			}
			// Expected
			// We failed to failover and did not failback because we fenced
			// svc1 (we forced it), therefore svc1 and svc2 should be standby.
			NUnit.Framework.Assert.AreEqual(org.apache.hadoop.ha.HAServiceProtocol.HAServiceState
				.STANDBY, svc1.state);
			NUnit.Framework.Assert.AreEqual(org.apache.hadoop.ha.HAServiceProtocol.HAServiceState
				.STANDBY, svc2.state);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testWeFenceOnFailbackIfTransitionToActiveFails()
		{
			org.apache.hadoop.ha.DummyHAService svc1 = new org.apache.hadoop.ha.DummyHAService
				(org.apache.hadoop.ha.HAServiceProtocol.HAServiceState.ACTIVE, svc1Addr);
			org.apache.hadoop.ha.DummyHAService svc2 = new org.apache.hadoop.ha.DummyHAService
				(org.apache.hadoop.ha.HAServiceProtocol.HAServiceState.STANDBY, svc2Addr);
			org.mockito.Mockito.doThrow(new org.apache.hadoop.ha.ServiceFailedException("Failed!"
				)).when(svc2.proxy).transitionToActive(anyReqInfo());
			svc1.fencer = svc2.fencer = org.apache.hadoop.ha.TestNodeFencer.setupFencer(Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.ha.TestNodeFencer.AlwaysSucceedFencer)).getName());
			org.apache.hadoop.ha.TestNodeFencer.AlwaysSucceedFencer.fenceCalled = 0;
			try
			{
				doFailover(svc1, svc2, false, false);
				NUnit.Framework.Assert.Fail("Failed over to service that won't transition to active"
					);
			}
			catch (org.apache.hadoop.ha.FailoverFailedException)
			{
			}
			// Expected
			// We failed to failover. We did not fence svc1 because it cooperated
			// and we didn't force it, so we failed back to svc1 and fenced svc2.
			// Note svc2 still thinks it's active, that's OK, we fenced it.
			NUnit.Framework.Assert.AreEqual(org.apache.hadoop.ha.HAServiceProtocol.HAServiceState
				.ACTIVE, svc1.state);
			NUnit.Framework.Assert.AreEqual(1, org.apache.hadoop.ha.TestNodeFencer.AlwaysSucceedFencer
				.fenceCalled);
			NUnit.Framework.Assert.AreSame(svc2, org.apache.hadoop.ha.TestNodeFencer.AlwaysSucceedFencer
				.fencedSvc);
		}

		private org.apache.hadoop.ha.HAServiceProtocol.StateChangeRequestInfo anyReqInfo(
			)
		{
			return org.mockito.Mockito.any<org.apache.hadoop.ha.HAServiceProtocol.StateChangeRequestInfo
				>();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testFailureToFenceOnFailbackFailsTheFailback()
		{
			org.apache.hadoop.ha.DummyHAService svc1 = new org.apache.hadoop.ha.DummyHAService
				(org.apache.hadoop.ha.HAServiceProtocol.HAServiceState.ACTIVE, svc1Addr);
			org.apache.hadoop.ha.DummyHAService svc2 = new org.apache.hadoop.ha.DummyHAService
				(org.apache.hadoop.ha.HAServiceProtocol.HAServiceState.STANDBY, svc2Addr);
			org.mockito.Mockito.doThrow(new System.IO.IOException("Failed!")).when(svc2.proxy
				).transitionToActive(anyReqInfo());
			svc1.fencer = svc2.fencer = org.apache.hadoop.ha.TestNodeFencer.setupFencer(Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.ha.TestNodeFencer.AlwaysFailFencer)).getName());
			org.apache.hadoop.ha.TestNodeFencer.AlwaysFailFencer.fenceCalled = 0;
			try
			{
				doFailover(svc1, svc2, false, false);
				NUnit.Framework.Assert.Fail("Failed over to service that won't transition to active"
					);
			}
			catch (org.apache.hadoop.ha.FailoverFailedException)
			{
			}
			// Expected
			// We did not fence svc1 because it cooperated and we didn't force it, 
			// we failed to failover so we fenced svc2, we failed to fence svc2
			// so we did not failback to svc1, ie it's still standby.
			NUnit.Framework.Assert.AreEqual(org.apache.hadoop.ha.HAServiceProtocol.HAServiceState
				.STANDBY, svc1.state);
			NUnit.Framework.Assert.AreEqual(1, org.apache.hadoop.ha.TestNodeFencer.AlwaysFailFencer
				.fenceCalled);
			NUnit.Framework.Assert.AreSame(svc2, org.apache.hadoop.ha.TestNodeFencer.AlwaysFailFencer
				.fencedSvc);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testFailbackToFaultyServiceFails()
		{
			org.apache.hadoop.ha.DummyHAService svc1 = new org.apache.hadoop.ha.DummyHAService
				(org.apache.hadoop.ha.HAServiceProtocol.HAServiceState.ACTIVE, svc1Addr);
			org.mockito.Mockito.doThrow(new org.apache.hadoop.ha.ServiceFailedException("Failed!"
				)).when(svc1.proxy).transitionToActive(anyReqInfo());
			org.apache.hadoop.ha.DummyHAService svc2 = new org.apache.hadoop.ha.DummyHAService
				(org.apache.hadoop.ha.HAServiceProtocol.HAServiceState.STANDBY, svc2Addr);
			org.mockito.Mockito.doThrow(new org.apache.hadoop.ha.ServiceFailedException("Failed!"
				)).when(svc2.proxy).transitionToActive(anyReqInfo());
			svc1.fencer = svc2.fencer = org.apache.hadoop.ha.TestNodeFencer.setupFencer(Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.ha.TestNodeFencer.AlwaysSucceedFencer)).getName());
			try
			{
				doFailover(svc1, svc2, false, false);
				NUnit.Framework.Assert.Fail("Failover to already active service");
			}
			catch (org.apache.hadoop.ha.FailoverFailedException)
			{
			}
			// Expected
			NUnit.Framework.Assert.AreEqual(org.apache.hadoop.ha.HAServiceProtocol.HAServiceState
				.STANDBY, svc1.state);
			NUnit.Framework.Assert.AreEqual(org.apache.hadoop.ha.HAServiceProtocol.HAServiceState
				.STANDBY, svc2.state);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testSelfFailoverFails()
		{
			org.apache.hadoop.ha.DummyHAService svc1 = new org.apache.hadoop.ha.DummyHAService
				(org.apache.hadoop.ha.HAServiceProtocol.HAServiceState.ACTIVE, svc1Addr);
			org.apache.hadoop.ha.DummyHAService svc2 = new org.apache.hadoop.ha.DummyHAService
				(org.apache.hadoop.ha.HAServiceProtocol.HAServiceState.STANDBY, svc2Addr);
			svc1.fencer = svc2.fencer = org.apache.hadoop.ha.TestNodeFencer.setupFencer(Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.ha.TestNodeFencer.AlwaysSucceedFencer)).getName());
			org.apache.hadoop.ha.TestNodeFencer.AlwaysSucceedFencer.fenceCalled = 0;
			try
			{
				doFailover(svc1, svc1, false, false);
				NUnit.Framework.Assert.Fail("Can't failover to yourself");
			}
			catch (org.apache.hadoop.ha.FailoverFailedException)
			{
			}
			// Expected
			NUnit.Framework.Assert.AreEqual(0, org.apache.hadoop.ha.TestNodeFencer.AlwaysSucceedFencer
				.fenceCalled);
			NUnit.Framework.Assert.AreEqual(org.apache.hadoop.ha.HAServiceProtocol.HAServiceState
				.ACTIVE, svc1.state);
			try
			{
				doFailover(svc2, svc2, false, false);
				NUnit.Framework.Assert.Fail("Can't failover to yourself");
			}
			catch (org.apache.hadoop.ha.FailoverFailedException)
			{
			}
			// Expected
			NUnit.Framework.Assert.AreEqual(0, org.apache.hadoop.ha.TestNodeFencer.AlwaysSucceedFencer
				.fenceCalled);
			NUnit.Framework.Assert.AreEqual(org.apache.hadoop.ha.HAServiceProtocol.HAServiceState
				.STANDBY, svc2.state);
		}

		/// <exception cref="org.apache.hadoop.ha.FailoverFailedException"/>
		private void doFailover(org.apache.hadoop.ha.HAServiceTarget tgt1, org.apache.hadoop.ha.HAServiceTarget
			 tgt2, bool forceFence, bool forceActive)
		{
			org.apache.hadoop.ha.FailoverController fc = new org.apache.hadoop.ha.FailoverController
				(conf, org.apache.hadoop.ha.HAServiceProtocol.RequestSource.REQUEST_BY_USER);
			fc.failover(tgt1, tgt2, forceFence, forceActive);
		}
	}
}
