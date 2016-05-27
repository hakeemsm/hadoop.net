using NUnit.Framework;
using Org.Apache.Commons.Logging.Impl;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Test;
using Org.Apache.Hadoop.Util;
using Org.Apache.Log4j;
using Org.Apache.Zookeeper;
using Org.Apache.Zookeeper.Data;
using Org.Apache.Zookeeper.Server.Auth;
using Sharpen;

namespace Org.Apache.Hadoop.HA
{
	public class TestZKFailoverController : ClientBaseWithFixes
	{
		private Configuration conf;

		private MiniZKFCCluster cluster;

		private const string DigestUserPass = "test-user:test-password";

		private const string TestAuthGood = "digest:" + DigestUserPass;

		private static readonly string DigestUserHash;

		static TestZKFailoverController()
		{
			// Set up ZK digest-based credentials for the purposes of the tests,
			// to make sure all of our functionality works with auth and ACLs
			// present.
			try
			{
				DigestUserHash = DigestAuthenticationProvider.GenerateDigest(DigestUserPass);
			}
			catch (NoSuchAlgorithmException e)
			{
				throw new RuntimeException(e);
			}
		}

		private static readonly string TestAcl = "digest:" + DigestUserHash + ":rwcda";

		static TestZKFailoverController()
		{
			((Log4JLogger)ActiveStandbyElector.Log).GetLogger().SetLevel(Level.All);
		}

		[SetUp]
		public virtual void SetupConfAndServices()
		{
			conf = new Configuration();
			conf.Set(ZKFailoverController.ZkAclKey, TestAcl);
			conf.Set(ZKFailoverController.ZkAuthKey, TestAuthGood);
			conf.Set(ZKFailoverController.ZkQuorumKey, hostPort);
			this.cluster = new MiniZKFCCluster(conf, GetServer(serverFactory));
		}

		/// <summary>
		/// Test that the various command lines for formatting the ZK directory
		/// function correctly.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestFormatZK()
		{
			DummyHAService svc = cluster.GetService(1);
			// Run without formatting the base dir,
			// should barf
			NUnit.Framework.Assert.AreEqual(ZKFailoverController.ErrCodeNoParentZnode, RunFC(
				svc));
			// Format the base dir, should succeed
			NUnit.Framework.Assert.AreEqual(0, RunFC(svc, "-formatZK"));
			// Should fail to format if already formatted
			NUnit.Framework.Assert.AreEqual(ZKFailoverController.ErrCodeFormatDenied, RunFC(svc
				, "-formatZK", "-nonInteractive"));
			// Unless '-force' is on
			NUnit.Framework.Assert.AreEqual(0, RunFC(svc, "-formatZK", "-force"));
		}

		/// <summary>
		/// Test that if ZooKeeper is not running, the correct error
		/// code is returned.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestNoZK()
		{
			StopServer();
			DummyHAService svc = cluster.GetService(1);
			NUnit.Framework.Assert.AreEqual(ZKFailoverController.ErrCodeNoZk, RunFC(svc));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFormatOneClusterLeavesOtherClustersAlone()
		{
			DummyHAService svc = cluster.GetService(1);
			MiniZKFCCluster.DummyZKFC zkfcInOtherCluster = new _DummyZKFC_116(conf, cluster.GetService
				(1));
			// Run without formatting the base dir,
			// should barf
			NUnit.Framework.Assert.AreEqual(ZKFailoverController.ErrCodeNoParentZnode, RunFC(
				svc));
			// Format the base dir, should succeed
			NUnit.Framework.Assert.AreEqual(0, RunFC(svc, "-formatZK"));
			// Run the other cluster without formatting, should barf because
			// it uses a different parent znode
			NUnit.Framework.Assert.AreEqual(ZKFailoverController.ErrCodeNoParentZnode, zkfcInOtherCluster
				.Run(new string[] {  }));
			// Should succeed in formatting the second cluster
			NUnit.Framework.Assert.AreEqual(0, zkfcInOtherCluster.Run(new string[] { "-formatZK"
				 }));
			// But should not have deleted the original base node from the first
			// cluster
			NUnit.Framework.Assert.AreEqual(ZKFailoverController.ErrCodeFormatDenied, RunFC(svc
				, "-formatZK", "-nonInteractive"));
		}

		private sealed class _DummyZKFC_116 : MiniZKFCCluster.DummyZKFC
		{
			public _DummyZKFC_116(Configuration baseArg1, DummyHAService baseArg2)
				: base(baseArg1, baseArg2)
			{
			}

			protected internal override string GetScopeInsideParentNode()
			{
				return "other-scope";
			}
		}

		/// <summary>
		/// Test that automatic failover won't run against a target that hasn't
		/// explicitly enabled the feature.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestWontRunWhenAutoFailoverDisabled()
		{
			DummyHAService svc = cluster.GetService(1);
			svc = Org.Mockito.Mockito.Spy(svc);
			Org.Mockito.Mockito.DoReturn(false).When(svc).IsAutoFailoverEnabled();
			NUnit.Framework.Assert.AreEqual(ZKFailoverController.ErrCodeAutoFailoverNotEnabled
				, RunFC(svc, "-formatZK"));
			NUnit.Framework.Assert.AreEqual(ZKFailoverController.ErrCodeAutoFailoverNotEnabled
				, RunFC(svc));
		}

		/// <summary>
		/// Test that, if ACLs are specified in the configuration, that
		/// it sets the ACLs when formatting the parent node.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestFormatSetsAcls()
		{
			// Format the base dir, should succeed
			DummyHAService svc = cluster.GetService(1);
			NUnit.Framework.Assert.AreEqual(0, RunFC(svc, "-formatZK"));
			ZooKeeper otherClient = CreateClient();
			try
			{
				// client without auth should not be able to read it
				Stat stat = new Stat();
				otherClient.GetData(ZKFailoverController.ZkParentZnodeDefault, false, stat);
				NUnit.Framework.Assert.Fail("Was able to read data without authenticating!");
			}
			catch (KeeperException.NoAuthException)
			{
			}
		}

		// expected
		/// <summary>
		/// Test that the ZKFC won't run if fencing is not configured for the
		/// local service.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestFencingMustBeConfigured()
		{
			DummyHAService svc = Org.Mockito.Mockito.Spy(cluster.GetService(0));
			Org.Mockito.Mockito.DoThrow(new BadFencingConfigurationException("no fencing")).When
				(svc).CheckFencingConfigured();
			// Format the base dir, should succeed
			NUnit.Framework.Assert.AreEqual(0, RunFC(svc, "-formatZK"));
			// Try to run the actual FC, should fail without a fencer
			NUnit.Framework.Assert.AreEqual(ZKFailoverController.ErrCodeNoFencer, RunFC(svc));
		}

		/// <summary>
		/// Test that, when the health monitor indicates bad health status,
		/// failover is triggered.
		/// </summary>
		/// <remarks>
		/// Test that, when the health monitor indicates bad health status,
		/// failover is triggered. Also ensures that graceful active-&gt;standby
		/// transition is used when possible, falling back to fencing when
		/// the graceful approach fails.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestAutoFailoverOnBadHealth()
		{
			try
			{
				cluster.Start();
				DummyHAService svc1 = cluster.GetService(1);
				Log.Info("Faking svc0 unhealthy, should failover to svc1");
				cluster.SetHealthy(0, false);
				Log.Info("Waiting for svc0 to enter initializing state");
				cluster.WaitForHAState(0, HAServiceProtocol.HAServiceState.Initializing);
				cluster.WaitForHAState(1, HAServiceProtocol.HAServiceState.Active);
				Log.Info("Allowing svc0 to be healthy again, making svc1 unreachable " + "and fail to gracefully go to standby"
					);
				cluster.SetUnreachable(1, true);
				cluster.SetHealthy(0, true);
				// Should fail back to svc0 at this point
				cluster.WaitForHAState(0, HAServiceProtocol.HAServiceState.Active);
				// and fence svc1
				Org.Mockito.Mockito.Verify(svc1.fencer).Fence(Org.Mockito.Mockito.Same(svc1));
			}
			finally
			{
				cluster.Stop();
			}
		}

		/// <summary>
		/// Test that, when the health monitor indicates bad health status,
		/// failover is triggered.
		/// </summary>
		/// <remarks>
		/// Test that, when the health monitor indicates bad health status,
		/// failover is triggered. Also ensures that graceful active-&gt;standby
		/// transition is used when possible, falling back to fencing when
		/// the graceful approach fails.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestAutoFailoverOnBadState()
		{
			try
			{
				cluster.Start();
				DummyHAService svc0 = cluster.GetService(0);
				Log.Info("Faking svc0 to change the state, should failover to svc1");
				svc0.state = HAServiceProtocol.HAServiceState.Standby;
				// Should fail back to svc0 at this point
				cluster.WaitForHAState(1, HAServiceProtocol.HAServiceState.Active);
			}
			finally
			{
				cluster.Stop();
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestAutoFailoverOnLostZKSession()
		{
			try
			{
				cluster.Start();
				// Expire svc0, it should fail over to svc1
				cluster.ExpireAndVerifyFailover(0, 1);
				// Expire svc1, it should fail back to svc0
				cluster.ExpireAndVerifyFailover(1, 0);
				Log.Info("======= Running test cases second time to test " + "re-establishment ========="
					);
				// Expire svc0, it should fail over to svc1
				cluster.ExpireAndVerifyFailover(0, 1);
				// Expire svc1, it should fail back to svc0
				cluster.ExpireAndVerifyFailover(1, 0);
			}
			finally
			{
				cluster.Stop();
			}
		}

		/// <summary>
		/// Test that, if the standby node is unhealthy, it doesn't try to become
		/// active
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestDontFailoverToUnhealthyNode()
		{
			try
			{
				cluster.Start();
				// Make svc1 unhealthy, and wait for its FC to notice the bad health.
				cluster.SetHealthy(1, false);
				cluster.WaitForHealthState(1, HealthMonitor.State.ServiceUnhealthy);
				// Expire svc0
				cluster.GetElector(0).PreventSessionReestablishmentForTests();
				try
				{
					cluster.ExpireActiveLockHolder(0);
					Log.Info("Expired svc0's ZK session. Waiting a second to give svc1" + " a chance to take the lock, if it is ever going to."
						);
					Sharpen.Thread.Sleep(1000);
					// Ensure that no one holds the lock.
					cluster.WaitForActiveLockHolder(null);
				}
				finally
				{
					Log.Info("Allowing svc0's elector to re-establish its connection");
					cluster.GetElector(0).AllowSessionReestablishmentForTests();
				}
				// svc0 should get the lock again
				cluster.WaitForActiveLockHolder(0);
			}
			finally
			{
				cluster.Stop();
			}
		}

		/// <summary>
		/// Test that the ZKFC successfully quits the election when it fails to
		/// become active.
		/// </summary>
		/// <remarks>
		/// Test that the ZKFC successfully quits the election when it fails to
		/// become active. This allows the old node to successfully fail back.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestBecomingActiveFails()
		{
			try
			{
				cluster.Start();
				DummyHAService svc1 = cluster.GetService(1);
				Log.Info("Making svc1 fail to become active");
				cluster.SetFailToBecomeActive(1, true);
				Log.Info("Faking svc0 unhealthy, should NOT successfully " + "failover to svc1");
				cluster.SetHealthy(0, false);
				cluster.WaitForHealthState(0, HealthMonitor.State.ServiceUnhealthy);
				cluster.WaitForActiveLockHolder(null);
				Org.Mockito.Mockito.Verify(svc1.proxy, Org.Mockito.Mockito.Timeout(2000).AtLeastOnce
					()).TransitionToActive(Org.Mockito.Mockito.Any<HAServiceProtocol.StateChangeRequestInfo
					>());
				cluster.WaitForHAState(0, HAServiceProtocol.HAServiceState.Initializing);
				cluster.WaitForHAState(1, HAServiceProtocol.HAServiceState.Standby);
				Log.Info("Faking svc0 healthy again, should go back to svc0");
				cluster.SetHealthy(0, true);
				cluster.WaitForHAState(0, HAServiceProtocol.HAServiceState.Active);
				cluster.WaitForHAState(1, HAServiceProtocol.HAServiceState.Standby);
				cluster.WaitForActiveLockHolder(0);
				// Ensure that we can fail back to svc1  once it it is able
				// to become active (e.g the admin has restarted it)
				Log.Info("Allowing svc1 to become active, expiring svc0");
				svc1.failToBecomeActive = false;
				cluster.ExpireAndVerifyFailover(0, 1);
			}
			finally
			{
				cluster.Stop();
			}
		}

		/// <summary>
		/// Test that, when ZooKeeper fails, the system remains in its
		/// current state, without triggering any failovers, and without
		/// causing the active node to enter standby state.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestZooKeeperFailure()
		{
			try
			{
				cluster.Start();
				// Record initial ZK sessions
				long session0 = cluster.GetElector(0).GetZKSessionIdForTests();
				long session1 = cluster.GetElector(1).GetZKSessionIdForTests();
				Log.Info("====== Stopping ZK server");
				StopServer();
				WaitForServerDown(hostPort, ConnectionTimeout);
				Log.Info("====== Waiting for services to enter NEUTRAL mode");
				cluster.WaitForElectorState(0, ActiveStandbyElector.State.Neutral);
				cluster.WaitForElectorState(1, ActiveStandbyElector.State.Neutral);
				Log.Info("====== Checking that the services didn't change HA state");
				NUnit.Framework.Assert.AreEqual(HAServiceProtocol.HAServiceState.Active, cluster.
					GetService(0).state);
				NUnit.Framework.Assert.AreEqual(HAServiceProtocol.HAServiceState.Standby, cluster
					.GetService(1).state);
				Log.Info("====== Restarting server");
				StartServer();
				WaitForServerUp(hostPort, ConnectionTimeout);
				// Nodes should go back to their original states, since they re-obtain
				// the same sessions.
				cluster.WaitForElectorState(0, ActiveStandbyElector.State.Active);
				cluster.WaitForElectorState(1, ActiveStandbyElector.State.Standby);
				// Check HA states didn't change.
				cluster.WaitForHAState(0, HAServiceProtocol.HAServiceState.Active);
				cluster.WaitForHAState(1, HAServiceProtocol.HAServiceState.Standby);
				// Check they re-used the same sessions and didn't spuriously reconnect
				NUnit.Framework.Assert.AreEqual(session0, cluster.GetElector(0).GetZKSessionIdForTests
					());
				NUnit.Framework.Assert.AreEqual(session1, cluster.GetElector(1).GetZKSessionIdForTests
					());
			}
			finally
			{
				cluster.Stop();
			}
		}

		/// <summary>Test that the ZKFC can gracefully cede its active status.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestCedeActive()
		{
			try
			{
				cluster.Start();
				MiniZKFCCluster.DummyZKFC zkfc = cluster.GetZkfc(0);
				// It should be in active to start.
				NUnit.Framework.Assert.AreEqual(ActiveStandbyElector.State.Active, zkfc.GetElectorForTests
					().GetStateForTests());
				// Ask it to cede active for 3 seconds. It should respond promptly
				// (i.e. the RPC itself should not take 3 seconds!)
				ZKFCProtocol proxy = zkfc.GetLocalTarget().GetZKFCProxy(conf, 5000);
				long st = Time.Now();
				proxy.CedeActive(3000);
				long et = Time.Now();
				NUnit.Framework.Assert.IsTrue("RPC to cedeActive took " + (et - st) + " ms", et -
					 st < 1000);
				// Should be in "INIT" state since it's not in the election
				// at this point.
				NUnit.Framework.Assert.AreEqual(ActiveStandbyElector.State.Init, zkfc.GetElectorForTests
					().GetStateForTests());
				// After the prescribed 3 seconds, should go into STANDBY state,
				// since the other node in the cluster would have taken ACTIVE.
				cluster.WaitForElectorState(0, ActiveStandbyElector.State.Standby);
				long et2 = Time.Now();
				NUnit.Framework.Assert.IsTrue("Should take ~3 seconds to rejoin. Only took " + (et2
					 - et) + "ms before rejoining.", et2 - et > 2800);
			}
			finally
			{
				cluster.Stop();
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestGracefulFailover()
		{
			try
			{
				cluster.Start();
				cluster.WaitForActiveLockHolder(0);
				cluster.GetService(1).GetZKFCProxy(conf, 5000).GracefulFailover();
				cluster.WaitForActiveLockHolder(1);
				cluster.GetService(0).GetZKFCProxy(conf, 5000).GracefulFailover();
				cluster.WaitForActiveLockHolder(0);
				Sharpen.Thread.Sleep(10000);
				// allow to quiesce
				NUnit.Framework.Assert.AreEqual(0, cluster.GetService(0).fenceCount);
				NUnit.Framework.Assert.AreEqual(0, cluster.GetService(1).fenceCount);
				NUnit.Framework.Assert.AreEqual(2, cluster.GetService(0).activeTransitionCount);
				NUnit.Framework.Assert.AreEqual(1, cluster.GetService(1).activeTransitionCount);
			}
			finally
			{
				cluster.Stop();
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestGracefulFailoverToUnhealthy()
		{
			try
			{
				cluster.Start();
				cluster.WaitForActiveLockHolder(0);
				// Mark it unhealthy, wait for it to exit election
				cluster.SetHealthy(1, false);
				cluster.WaitForElectorState(1, ActiveStandbyElector.State.Init);
				// Ask for failover, it should fail, because it's unhealthy
				try
				{
					cluster.GetService(1).GetZKFCProxy(conf, 5000).GracefulFailover();
					NUnit.Framework.Assert.Fail("Did not fail to graceful failover to unhealthy service!"
						);
				}
				catch (ServiceFailedException sfe)
				{
					GenericTestUtils.AssertExceptionContains(cluster.GetService(1).ToString() + " is not currently healthy."
						, sfe);
				}
			}
			finally
			{
				cluster.Stop();
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestGracefulFailoverFailBecomingActive()
		{
			try
			{
				cluster.Start();
				cluster.WaitForActiveLockHolder(0);
				cluster.SetFailToBecomeActive(1, true);
				// Ask for failover, it should fail and report back to user.
				try
				{
					cluster.GetService(1).GetZKFCProxy(conf, 5000).GracefulFailover();
					NUnit.Framework.Assert.Fail("Did not fail to graceful failover when target failed "
						 + "to become active!");
				}
				catch (ServiceFailedException sfe)
				{
					GenericTestUtils.AssertExceptionContains("Couldn't make " + cluster.GetService(1)
						 + " active", sfe);
					GenericTestUtils.AssertExceptionContains("injected failure", sfe);
				}
				// No fencing
				NUnit.Framework.Assert.AreEqual(0, cluster.GetService(0).fenceCount);
				NUnit.Framework.Assert.AreEqual(0, cluster.GetService(1).fenceCount);
				// Service 0 should go back to being active after the failed failover
				cluster.WaitForActiveLockHolder(0);
			}
			finally
			{
				cluster.Stop();
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestGracefulFailoverFailBecomingStandby()
		{
			try
			{
				cluster.Start();
				cluster.WaitForActiveLockHolder(0);
				// Ask for failover when old node fails to transition to standby.
				// This should trigger fencing, since the cedeActive() command
				// still works, but leaves the breadcrumb in place.
				cluster.SetFailToBecomeStandby(0, true);
				cluster.GetService(1).GetZKFCProxy(conf, 5000).GracefulFailover();
				// Check that the old node was fenced
				NUnit.Framework.Assert.AreEqual(1, cluster.GetService(0).fenceCount);
			}
			finally
			{
				cluster.Stop();
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestGracefulFailoverFailBecomingStandbyAndFailFence()
		{
			try
			{
				cluster.Start();
				cluster.WaitForActiveLockHolder(0);
				// Ask for failover when old node fails to transition to standby.
				// This should trigger fencing, since the cedeActive() command
				// still works, but leaves the breadcrumb in place.
				cluster.SetFailToBecomeStandby(0, true);
				cluster.SetFailToFence(0, true);
				try
				{
					cluster.GetService(1).GetZKFCProxy(conf, 5000).GracefulFailover();
					NUnit.Framework.Assert.Fail("Failover should have failed when old node wont fence"
						);
				}
				catch (ServiceFailedException sfe)
				{
					GenericTestUtils.AssertExceptionContains("Unable to fence " + cluster.GetService(
						0), sfe);
				}
			}
			finally
			{
				cluster.Stop();
			}
		}

		/// <summary>Test which exercises all of the inputs into ZKFC.</summary>
		/// <remarks>
		/// Test which exercises all of the inputs into ZKFC. This is particularly
		/// useful for running under jcarder to check for lock order violations.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestOneOfEverything()
		{
			try
			{
				cluster.Start();
				// Failover by session expiration
				Log.Info("====== Failing over by session expiration");
				cluster.ExpireAndVerifyFailover(0, 1);
				cluster.ExpireAndVerifyFailover(1, 0);
				// Restart ZK
				Log.Info("====== Restarting server");
				StopServer();
				WaitForServerDown(hostPort, ConnectionTimeout);
				StartServer();
				WaitForServerUp(hostPort, ConnectionTimeout);
				// Failover by bad health
				cluster.SetHealthy(0, false);
				cluster.WaitForHAState(0, HAServiceProtocol.HAServiceState.Initializing);
				cluster.WaitForHAState(1, HAServiceProtocol.HAServiceState.Active);
				cluster.SetHealthy(1, true);
				cluster.SetHealthy(0, false);
				cluster.WaitForHAState(1, HAServiceProtocol.HAServiceState.Active);
				cluster.WaitForHAState(0, HAServiceProtocol.HAServiceState.Initializing);
				cluster.SetHealthy(0, true);
				cluster.WaitForHealthState(0, HealthMonitor.State.ServiceHealthy);
				// Graceful failovers
				cluster.GetZkfc(1).GracefulFailoverToYou();
				cluster.GetZkfc(0).GracefulFailoverToYou();
			}
			finally
			{
				cluster.Stop();
			}
		}

		/// <exception cref="System.Exception"/>
		private int RunFC(DummyHAService target, params string[] args)
		{
			MiniZKFCCluster.DummyZKFC zkfc = new MiniZKFCCluster.DummyZKFC(conf, target);
			return zkfc.Run(args);
		}
	}
}
