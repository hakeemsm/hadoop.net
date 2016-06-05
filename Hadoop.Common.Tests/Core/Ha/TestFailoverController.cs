using System;
using System.IO;
using System.Net;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Security;
using Org.Mockito.Internal.Stubbing.Answers;
using Sharpen;

namespace Org.Apache.Hadoop.HA
{
	public class TestFailoverController
	{
		private IPEndPoint svc1Addr = new IPEndPoint("svc1", 1234);

		private IPEndPoint svc2Addr = new IPEndPoint("svc2", 5678);

		private Configuration conf = new Configuration();

		internal HAServiceStatus StateNotReady = new HAServiceStatus(HAServiceProtocol.HAServiceState
			.Standby).SetNotReadyToBecomeActive("injected not ready");

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestFailoverAndFailback()
		{
			DummyHAService svc1 = new DummyHAService(HAServiceProtocol.HAServiceState.Active, 
				svc1Addr);
			DummyHAService svc2 = new DummyHAService(HAServiceProtocol.HAServiceState.Standby
				, svc2Addr);
			svc1.fencer = svc2.fencer = TestNodeFencer.SetupFencer(typeof(TestNodeFencer.AlwaysSucceedFencer
				).FullName);
			TestNodeFencer.AlwaysSucceedFencer.fenceCalled = 0;
			DoFailover(svc1, svc2, false, false);
			Assert.Equal(0, TestNodeFencer.AlwaysSucceedFencer.fenceCalled
				);
			Assert.Equal(HAServiceProtocol.HAServiceState.Standby, svc1.state
				);
			Assert.Equal(HAServiceProtocol.HAServiceState.Active, svc2.state
				);
			TestNodeFencer.AlwaysSucceedFencer.fenceCalled = 0;
			DoFailover(svc2, svc1, false, false);
			Assert.Equal(0, TestNodeFencer.AlwaysSucceedFencer.fenceCalled
				);
			Assert.Equal(HAServiceProtocol.HAServiceState.Active, svc1.state
				);
			Assert.Equal(HAServiceProtocol.HAServiceState.Standby, svc2.state
				);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestFailoverFromStandbyToStandby()
		{
			DummyHAService svc1 = new DummyHAService(HAServiceProtocol.HAServiceState.Standby
				, svc1Addr);
			DummyHAService svc2 = new DummyHAService(HAServiceProtocol.HAServiceState.Standby
				, svc2Addr);
			svc1.fencer = svc2.fencer = TestNodeFencer.SetupFencer(typeof(TestNodeFencer.AlwaysSucceedFencer
				).FullName);
			DoFailover(svc1, svc2, false, false);
			Assert.Equal(HAServiceProtocol.HAServiceState.Standby, svc1.state
				);
			Assert.Equal(HAServiceProtocol.HAServiceState.Active, svc2.state
				);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestFailoverFromActiveToActive()
		{
			DummyHAService svc1 = new DummyHAService(HAServiceProtocol.HAServiceState.Active, 
				svc1Addr);
			DummyHAService svc2 = new DummyHAService(HAServiceProtocol.HAServiceState.Active, 
				svc2Addr);
			svc1.fencer = svc2.fencer = TestNodeFencer.SetupFencer(typeof(TestNodeFencer.AlwaysSucceedFencer
				).FullName);
			try
			{
				DoFailover(svc1, svc2, false, false);
				NUnit.Framework.Assert.Fail("Can't failover to an already active service");
			}
			catch (FailoverFailedException)
			{
			}
			// Expected
			Assert.Equal(HAServiceProtocol.HAServiceState.Active, svc1.state
				);
			Assert.Equal(HAServiceProtocol.HAServiceState.Active, svc2.state
				);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestFailoverWithoutPermission()
		{
			DummyHAService svc1 = new DummyHAService(HAServiceProtocol.HAServiceState.Active, 
				svc1Addr);
			Org.Mockito.Mockito.DoThrow(new AccessControlException("Access denied")).When(svc1
				.proxy).GetServiceStatus();
			DummyHAService svc2 = new DummyHAService(HAServiceProtocol.HAServiceState.Standby
				, svc2Addr);
			Org.Mockito.Mockito.DoThrow(new AccessControlException("Access denied")).When(svc2
				.proxy).GetServiceStatus();
			svc1.fencer = svc2.fencer = TestNodeFencer.SetupFencer(typeof(TestNodeFencer.AlwaysSucceedFencer
				).FullName);
			try
			{
				DoFailover(svc1, svc2, false, false);
				NUnit.Framework.Assert.Fail("Can't failover when access is denied");
			}
			catch (FailoverFailedException ffe)
			{
				Assert.True(ffe.InnerException.Message.Contains("Access denied"
					));
			}
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestFailoverToUnreadyService()
		{
			DummyHAService svc1 = new DummyHAService(HAServiceProtocol.HAServiceState.Active, 
				svc1Addr);
			DummyHAService svc2 = new DummyHAService(HAServiceProtocol.HAServiceState.Standby
				, svc2Addr);
			Org.Mockito.Mockito.DoReturn(StateNotReady).When(svc2.proxy).GetServiceStatus();
			svc1.fencer = svc2.fencer = TestNodeFencer.SetupFencer(typeof(TestNodeFencer.AlwaysSucceedFencer
				).FullName);
			try
			{
				DoFailover(svc1, svc2, false, false);
				NUnit.Framework.Assert.Fail("Can't failover to a service that's not ready");
			}
			catch (FailoverFailedException ffe)
			{
				// Expected
				if (!ffe.Message.Contains("injected not ready"))
				{
					throw;
				}
			}
			Assert.Equal(HAServiceProtocol.HAServiceState.Active, svc1.state
				);
			Assert.Equal(HAServiceProtocol.HAServiceState.Standby, svc2.state
				);
			// Forcing it means we ignore readyToBecomeActive
			DoFailover(svc1, svc2, false, true);
			Assert.Equal(HAServiceProtocol.HAServiceState.Standby, svc1.state
				);
			Assert.Equal(HAServiceProtocol.HAServiceState.Active, svc2.state
				);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestFailoverToUnhealthyServiceFailsAndFailsback()
		{
			DummyHAService svc1 = new DummyHAService(HAServiceProtocol.HAServiceState.Active, 
				svc1Addr);
			DummyHAService svc2 = new DummyHAService(HAServiceProtocol.HAServiceState.Standby
				, svc2Addr);
			Org.Mockito.Mockito.DoThrow(new HealthCheckFailedException("Failed!")).When(svc2.
				proxy).MonitorHealth();
			svc1.fencer = svc2.fencer = TestNodeFencer.SetupFencer(typeof(TestNodeFencer.AlwaysSucceedFencer
				).FullName);
			try
			{
				DoFailover(svc1, svc2, false, false);
				NUnit.Framework.Assert.Fail("Failover to unhealthy service");
			}
			catch (FailoverFailedException)
			{
			}
			// Expected
			Assert.Equal(HAServiceProtocol.HAServiceState.Active, svc1.state
				);
			Assert.Equal(HAServiceProtocol.HAServiceState.Standby, svc2.state
				);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestFailoverFromFaultyServiceSucceeds()
		{
			DummyHAService svc1 = new DummyHAService(HAServiceProtocol.HAServiceState.Active, 
				svc1Addr);
			Org.Mockito.Mockito.DoThrow(new ServiceFailedException("Failed!")).When(svc1.proxy
				).TransitionToStandby(AnyReqInfo());
			DummyHAService svc2 = new DummyHAService(HAServiceProtocol.HAServiceState.Standby
				, svc2Addr);
			svc1.fencer = svc2.fencer = TestNodeFencer.SetupFencer(typeof(TestNodeFencer.AlwaysSucceedFencer
				).FullName);
			TestNodeFencer.AlwaysSucceedFencer.fenceCalled = 0;
			try
			{
				DoFailover(svc1, svc2, false, false);
			}
			catch (FailoverFailedException)
			{
				NUnit.Framework.Assert.Fail("Faulty active prevented failover");
			}
			// svc1 still thinks it's active, that's OK, it was fenced
			Assert.Equal(1, TestNodeFencer.AlwaysSucceedFencer.fenceCalled
				);
			NUnit.Framework.Assert.AreSame(svc1, TestNodeFencer.AlwaysSucceedFencer.fencedSvc
				);
			Assert.Equal(HAServiceProtocol.HAServiceState.Active, svc1.state
				);
			Assert.Equal(HAServiceProtocol.HAServiceState.Active, svc2.state
				);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestFailoverFromFaultyServiceFencingFailure()
		{
			DummyHAService svc1 = new DummyHAService(HAServiceProtocol.HAServiceState.Active, 
				svc1Addr);
			Org.Mockito.Mockito.DoThrow(new ServiceFailedException("Failed!")).When(svc1.proxy
				).TransitionToStandby(AnyReqInfo());
			DummyHAService svc2 = new DummyHAService(HAServiceProtocol.HAServiceState.Standby
				, svc2Addr);
			svc1.fencer = svc2.fencer = TestNodeFencer.SetupFencer(typeof(TestNodeFencer.AlwaysFailFencer
				).FullName);
			TestNodeFencer.AlwaysFailFencer.fenceCalled = 0;
			try
			{
				DoFailover(svc1, svc2, false, false);
				NUnit.Framework.Assert.Fail("Failed over even though fencing failed");
			}
			catch (FailoverFailedException)
			{
			}
			// Expected
			Assert.Equal(1, TestNodeFencer.AlwaysFailFencer.fenceCalled);
			NUnit.Framework.Assert.AreSame(svc1, TestNodeFencer.AlwaysFailFencer.fencedSvc);
			Assert.Equal(HAServiceProtocol.HAServiceState.Active, svc1.state
				);
			Assert.Equal(HAServiceProtocol.HAServiceState.Standby, svc2.state
				);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestFencingFailureDuringFailover()
		{
			DummyHAService svc1 = new DummyHAService(HAServiceProtocol.HAServiceState.Active, 
				svc1Addr);
			DummyHAService svc2 = new DummyHAService(HAServiceProtocol.HAServiceState.Standby
				, svc2Addr);
			svc1.fencer = svc2.fencer = TestNodeFencer.SetupFencer(typeof(TestNodeFencer.AlwaysFailFencer
				).FullName);
			TestNodeFencer.AlwaysFailFencer.fenceCalled = 0;
			try
			{
				DoFailover(svc1, svc2, true, false);
				NUnit.Framework.Assert.Fail("Failed over even though fencing requested and failed"
					);
			}
			catch (FailoverFailedException)
			{
			}
			// Expected
			// If fencing was requested and it failed we don't try to make
			// svc2 active anyway, and we don't failback to svc1.
			Assert.Equal(1, TestNodeFencer.AlwaysFailFencer.fenceCalled);
			NUnit.Framework.Assert.AreSame(svc1, TestNodeFencer.AlwaysFailFencer.fencedSvc);
			Assert.Equal(HAServiceProtocol.HAServiceState.Standby, svc1.state
				);
			Assert.Equal(HAServiceProtocol.HAServiceState.Standby, svc2.state
				);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestFailoverFromNonExistantServiceWithFencer()
		{
			DummyHAService svc1 = Org.Mockito.Mockito.Spy(new DummyHAService(null, svc1Addr));
			// Getting a proxy to a dead server will throw IOException on call,
			// not on creation of the proxy.
			HAServiceProtocol errorThrowingProxy = Org.Mockito.Mockito.Mock<HAServiceProtocol
				>(Org.Mockito.Mockito.WithSettings().DefaultAnswer(new ThrowsException(new IOException
				("Could not connect to host"))).ExtraInterfaces(typeof(IDisposable)));
			Org.Mockito.Mockito.DoNothing().When((IDisposable)errorThrowingProxy).Close();
			Org.Mockito.Mockito.DoReturn(errorThrowingProxy).When(svc1).GetProxy(Org.Mockito.Mockito
				.Any<Configuration>(), Org.Mockito.Mockito.AnyInt());
			DummyHAService svc2 = new DummyHAService(HAServiceProtocol.HAServiceState.Standby
				, svc2Addr);
			svc1.fencer = svc2.fencer = TestNodeFencer.SetupFencer(typeof(TestNodeFencer.AlwaysSucceedFencer
				).FullName);
			try
			{
				DoFailover(svc1, svc2, false, false);
			}
			catch (FailoverFailedException)
			{
				NUnit.Framework.Assert.Fail("Non-existant active prevented failover");
			}
			// Verify that the proxy created to try to make it go to standby
			// gracefully used the right rpc timeout
			Org.Mockito.Mockito.Verify(svc1).GetProxy(Org.Mockito.Mockito.Any<Configuration>(
				), Org.Mockito.Mockito.Eq(CommonConfigurationKeys.HaFcGracefulFenceTimeoutDefault
				));
			// Don't check svc1 because we can't reach it, but that's OK, it's been fenced.
			Assert.Equal(HAServiceProtocol.HAServiceState.Active, svc2.state
				);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestFailoverToNonExistantServiceFails()
		{
			DummyHAService svc1 = new DummyHAService(HAServiceProtocol.HAServiceState.Active, 
				svc1Addr);
			DummyHAService svc2 = Org.Mockito.Mockito.Spy(new DummyHAService(null, svc2Addr));
			Org.Mockito.Mockito.DoThrow(new IOException("Failed to connect")).When(svc2).GetProxy
				(Org.Mockito.Mockito.Any<Configuration>(), Org.Mockito.Mockito.AnyInt());
			svc1.fencer = svc2.fencer = TestNodeFencer.SetupFencer(typeof(TestNodeFencer.AlwaysSucceedFencer
				).FullName);
			try
			{
				DoFailover(svc1, svc2, false, false);
				NUnit.Framework.Assert.Fail("Failed over to a non-existant standby");
			}
			catch (FailoverFailedException)
			{
			}
			// Expected
			Assert.Equal(HAServiceProtocol.HAServiceState.Active, svc1.state
				);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestFailoverToFaultyServiceFailsbackOK()
		{
			DummyHAService svc1 = Org.Mockito.Mockito.Spy(new DummyHAService(HAServiceProtocol.HAServiceState
				.Active, svc1Addr));
			DummyHAService svc2 = new DummyHAService(HAServiceProtocol.HAServiceState.Standby
				, svc2Addr);
			Org.Mockito.Mockito.DoThrow(new ServiceFailedException("Failed!")).When(svc2.proxy
				).TransitionToActive(AnyReqInfo());
			svc1.fencer = svc2.fencer = TestNodeFencer.SetupFencer(typeof(TestNodeFencer.AlwaysSucceedFencer
				).FullName);
			try
			{
				DoFailover(svc1, svc2, false, false);
				NUnit.Framework.Assert.Fail("Failover to already active service");
			}
			catch (FailoverFailedException)
			{
			}
			// Expected
			// svc1 went standby then back to active
			Org.Mockito.Mockito.Verify(svc1.proxy).TransitionToStandby(AnyReqInfo());
			Org.Mockito.Mockito.Verify(svc1.proxy).TransitionToActive(AnyReqInfo());
			Assert.Equal(HAServiceProtocol.HAServiceState.Active, svc1.state
				);
			Assert.Equal(HAServiceProtocol.HAServiceState.Standby, svc2.state
				);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestWeDontFailbackIfActiveWasFenced()
		{
			DummyHAService svc1 = new DummyHAService(HAServiceProtocol.HAServiceState.Active, 
				svc1Addr);
			DummyHAService svc2 = new DummyHAService(HAServiceProtocol.HAServiceState.Standby
				, svc2Addr);
			Org.Mockito.Mockito.DoThrow(new ServiceFailedException("Failed!")).When(svc2.proxy
				).TransitionToActive(AnyReqInfo());
			svc1.fencer = svc2.fencer = TestNodeFencer.SetupFencer(typeof(TestNodeFencer.AlwaysSucceedFencer
				).FullName);
			try
			{
				DoFailover(svc1, svc2, true, false);
				NUnit.Framework.Assert.Fail("Failed over to service that won't transition to active"
					);
			}
			catch (FailoverFailedException)
			{
			}
			// Expected
			// We failed to failover and did not failback because we fenced
			// svc1 (we forced it), therefore svc1 and svc2 should be standby.
			Assert.Equal(HAServiceProtocol.HAServiceState.Standby, svc1.state
				);
			Assert.Equal(HAServiceProtocol.HAServiceState.Standby, svc2.state
				);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestWeFenceOnFailbackIfTransitionToActiveFails()
		{
			DummyHAService svc1 = new DummyHAService(HAServiceProtocol.HAServiceState.Active, 
				svc1Addr);
			DummyHAService svc2 = new DummyHAService(HAServiceProtocol.HAServiceState.Standby
				, svc2Addr);
			Org.Mockito.Mockito.DoThrow(new ServiceFailedException("Failed!")).When(svc2.proxy
				).TransitionToActive(AnyReqInfo());
			svc1.fencer = svc2.fencer = TestNodeFencer.SetupFencer(typeof(TestNodeFencer.AlwaysSucceedFencer
				).FullName);
			TestNodeFencer.AlwaysSucceedFencer.fenceCalled = 0;
			try
			{
				DoFailover(svc1, svc2, false, false);
				NUnit.Framework.Assert.Fail("Failed over to service that won't transition to active"
					);
			}
			catch (FailoverFailedException)
			{
			}
			// Expected
			// We failed to failover. We did not fence svc1 because it cooperated
			// and we didn't force it, so we failed back to svc1 and fenced svc2.
			// Note svc2 still thinks it's active, that's OK, we fenced it.
			Assert.Equal(HAServiceProtocol.HAServiceState.Active, svc1.state
				);
			Assert.Equal(1, TestNodeFencer.AlwaysSucceedFencer.fenceCalled
				);
			NUnit.Framework.Assert.AreSame(svc2, TestNodeFencer.AlwaysSucceedFencer.fencedSvc
				);
		}

		private HAServiceProtocol.StateChangeRequestInfo AnyReqInfo()
		{
			return Org.Mockito.Mockito.Any<HAServiceProtocol.StateChangeRequestInfo>();
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestFailureToFenceOnFailbackFailsTheFailback()
		{
			DummyHAService svc1 = new DummyHAService(HAServiceProtocol.HAServiceState.Active, 
				svc1Addr);
			DummyHAService svc2 = new DummyHAService(HAServiceProtocol.HAServiceState.Standby
				, svc2Addr);
			Org.Mockito.Mockito.DoThrow(new IOException("Failed!")).When(svc2.proxy).TransitionToActive
				(AnyReqInfo());
			svc1.fencer = svc2.fencer = TestNodeFencer.SetupFencer(typeof(TestNodeFencer.AlwaysFailFencer
				).FullName);
			TestNodeFencer.AlwaysFailFencer.fenceCalled = 0;
			try
			{
				DoFailover(svc1, svc2, false, false);
				NUnit.Framework.Assert.Fail("Failed over to service that won't transition to active"
					);
			}
			catch (FailoverFailedException)
			{
			}
			// Expected
			// We did not fence svc1 because it cooperated and we didn't force it, 
			// we failed to failover so we fenced svc2, we failed to fence svc2
			// so we did not failback to svc1, ie it's still standby.
			Assert.Equal(HAServiceProtocol.HAServiceState.Standby, svc1.state
				);
			Assert.Equal(1, TestNodeFencer.AlwaysFailFencer.fenceCalled);
			NUnit.Framework.Assert.AreSame(svc2, TestNodeFencer.AlwaysFailFencer.fencedSvc);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestFailbackToFaultyServiceFails()
		{
			DummyHAService svc1 = new DummyHAService(HAServiceProtocol.HAServiceState.Active, 
				svc1Addr);
			Org.Mockito.Mockito.DoThrow(new ServiceFailedException("Failed!")).When(svc1.proxy
				).TransitionToActive(AnyReqInfo());
			DummyHAService svc2 = new DummyHAService(HAServiceProtocol.HAServiceState.Standby
				, svc2Addr);
			Org.Mockito.Mockito.DoThrow(new ServiceFailedException("Failed!")).When(svc2.proxy
				).TransitionToActive(AnyReqInfo());
			svc1.fencer = svc2.fencer = TestNodeFencer.SetupFencer(typeof(TestNodeFencer.AlwaysSucceedFencer
				).FullName);
			try
			{
				DoFailover(svc1, svc2, false, false);
				NUnit.Framework.Assert.Fail("Failover to already active service");
			}
			catch (FailoverFailedException)
			{
			}
			// Expected
			Assert.Equal(HAServiceProtocol.HAServiceState.Standby, svc1.state
				);
			Assert.Equal(HAServiceProtocol.HAServiceState.Standby, svc2.state
				);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestSelfFailoverFails()
		{
			DummyHAService svc1 = new DummyHAService(HAServiceProtocol.HAServiceState.Active, 
				svc1Addr);
			DummyHAService svc2 = new DummyHAService(HAServiceProtocol.HAServiceState.Standby
				, svc2Addr);
			svc1.fencer = svc2.fencer = TestNodeFencer.SetupFencer(typeof(TestNodeFencer.AlwaysSucceedFencer
				).FullName);
			TestNodeFencer.AlwaysSucceedFencer.fenceCalled = 0;
			try
			{
				DoFailover(svc1, svc1, false, false);
				NUnit.Framework.Assert.Fail("Can't failover to yourself");
			}
			catch (FailoverFailedException)
			{
			}
			// Expected
			Assert.Equal(0, TestNodeFencer.AlwaysSucceedFencer.fenceCalled
				);
			Assert.Equal(HAServiceProtocol.HAServiceState.Active, svc1.state
				);
			try
			{
				DoFailover(svc2, svc2, false, false);
				NUnit.Framework.Assert.Fail("Can't failover to yourself");
			}
			catch (FailoverFailedException)
			{
			}
			// Expected
			Assert.Equal(0, TestNodeFencer.AlwaysSucceedFencer.fenceCalled
				);
			Assert.Equal(HAServiceProtocol.HAServiceState.Standby, svc2.state
				);
		}

		/// <exception cref="Org.Apache.Hadoop.HA.FailoverFailedException"/>
		private void DoFailover(HAServiceTarget tgt1, HAServiceTarget tgt2, bool forceFence
			, bool forceActive)
		{
			FailoverController fc = new FailoverController(conf, HAServiceProtocol.RequestSource
				.RequestByUser);
			fc.Failover(tgt1, tgt2, forceFence, forceActive);
		}
	}
}
