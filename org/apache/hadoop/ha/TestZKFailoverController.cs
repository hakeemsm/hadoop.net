using Sharpen;

namespace org.apache.hadoop.ha
{
	public class TestZKFailoverController : org.apache.hadoop.ha.ClientBaseWithFixes
	{
		private org.apache.hadoop.conf.Configuration conf;

		private org.apache.hadoop.ha.MiniZKFCCluster cluster;

		private const string DIGEST_USER_PASS = "test-user:test-password";

		private const string TEST_AUTH_GOOD = "digest:" + DIGEST_USER_PASS;

		private static readonly string DIGEST_USER_HASH;

		static TestZKFailoverController()
		{
			// Set up ZK digest-based credentials for the purposes of the tests,
			// to make sure all of our functionality works with auth and ACLs
			// present.
			try
			{
				DIGEST_USER_HASH = org.apache.zookeeper.server.auth.DigestAuthenticationProvider.
					generateDigest(DIGEST_USER_PASS);
			}
			catch (java.security.NoSuchAlgorithmException e)
			{
				throw new System.Exception(e);
			}
		}

		private static readonly string TEST_ACL = "digest:" + DIGEST_USER_HASH + ":rwcda";

		static TestZKFailoverController()
		{
			((org.apache.commons.logging.impl.Log4JLogger)org.apache.hadoop.ha.ActiveStandbyElector
				.LOG).getLogger().setLevel(org.apache.log4j.Level.ALL);
		}

		[NUnit.Framework.SetUp]
		public virtual void setupConfAndServices()
		{
			conf = new org.apache.hadoop.conf.Configuration();
			conf.set(org.apache.hadoop.ha.ZKFailoverController.ZK_ACL_KEY, TEST_ACL);
			conf.set(org.apache.hadoop.ha.ZKFailoverController.ZK_AUTH_KEY, TEST_AUTH_GOOD);
			conf.set(org.apache.hadoop.ha.ZKFailoverController.ZK_QUORUM_KEY, hostPort);
			this.cluster = new org.apache.hadoop.ha.MiniZKFCCluster(conf, getServer(serverFactory
				));
		}

		/// <summary>
		/// Test that the various command lines for formatting the ZK directory
		/// function correctly.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void testFormatZK()
		{
			org.apache.hadoop.ha.DummyHAService svc = cluster.getService(1);
			// Run without formatting the base dir,
			// should barf
			NUnit.Framework.Assert.AreEqual(org.apache.hadoop.ha.ZKFailoverController.ERR_CODE_NO_PARENT_ZNODE
				, runFC(svc));
			// Format the base dir, should succeed
			NUnit.Framework.Assert.AreEqual(0, runFC(svc, "-formatZK"));
			// Should fail to format if already formatted
			NUnit.Framework.Assert.AreEqual(org.apache.hadoop.ha.ZKFailoverController.ERR_CODE_FORMAT_DENIED
				, runFC(svc, "-formatZK", "-nonInteractive"));
			// Unless '-force' is on
			NUnit.Framework.Assert.AreEqual(0, runFC(svc, "-formatZK", "-force"));
		}

		/// <summary>
		/// Test that if ZooKeeper is not running, the correct error
		/// code is returned.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void testNoZK()
		{
			stopServer();
			org.apache.hadoop.ha.DummyHAService svc = cluster.getService(1);
			NUnit.Framework.Assert.AreEqual(org.apache.hadoop.ha.ZKFailoverController.ERR_CODE_NO_ZK
				, runFC(svc));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testFormatOneClusterLeavesOtherClustersAlone()
		{
			org.apache.hadoop.ha.DummyHAService svc = cluster.getService(1);
			org.apache.hadoop.ha.MiniZKFCCluster.DummyZKFC zkfcInOtherCluster = new _DummyZKFC_116
				(conf, cluster.getService(1));
			// Run without formatting the base dir,
			// should barf
			NUnit.Framework.Assert.AreEqual(org.apache.hadoop.ha.ZKFailoverController.ERR_CODE_NO_PARENT_ZNODE
				, runFC(svc));
			// Format the base dir, should succeed
			NUnit.Framework.Assert.AreEqual(0, runFC(svc, "-formatZK"));
			// Run the other cluster without formatting, should barf because
			// it uses a different parent znode
			NUnit.Framework.Assert.AreEqual(org.apache.hadoop.ha.ZKFailoverController.ERR_CODE_NO_PARENT_ZNODE
				, zkfcInOtherCluster.run(new string[] {  }));
			// Should succeed in formatting the second cluster
			NUnit.Framework.Assert.AreEqual(0, zkfcInOtherCluster.run(new string[] { "-formatZK"
				 }));
			// But should not have deleted the original base node from the first
			// cluster
			NUnit.Framework.Assert.AreEqual(org.apache.hadoop.ha.ZKFailoverController.ERR_CODE_FORMAT_DENIED
				, runFC(svc, "-formatZK", "-nonInteractive"));
		}

		private sealed class _DummyZKFC_116 : org.apache.hadoop.ha.MiniZKFCCluster.DummyZKFC
		{
			public _DummyZKFC_116(org.apache.hadoop.conf.Configuration baseArg1, org.apache.hadoop.ha.DummyHAService
				 baseArg2)
				: base(baseArg1, baseArg2)
			{
			}

			protected internal override string getScopeInsideParentNode()
			{
				return "other-scope";
			}
		}

		/// <summary>
		/// Test that automatic failover won't run against a target that hasn't
		/// explicitly enabled the feature.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void testWontRunWhenAutoFailoverDisabled()
		{
			org.apache.hadoop.ha.DummyHAService svc = cluster.getService(1);
			svc = org.mockito.Mockito.spy(svc);
			org.mockito.Mockito.doReturn(false).when(svc).isAutoFailoverEnabled();
			NUnit.Framework.Assert.AreEqual(org.apache.hadoop.ha.ZKFailoverController.ERR_CODE_AUTO_FAILOVER_NOT_ENABLED
				, runFC(svc, "-formatZK"));
			NUnit.Framework.Assert.AreEqual(org.apache.hadoop.ha.ZKFailoverController.ERR_CODE_AUTO_FAILOVER_NOT_ENABLED
				, runFC(svc));
		}

		/// <summary>
		/// Test that, if ACLs are specified in the configuration, that
		/// it sets the ACLs when formatting the parent node.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void testFormatSetsAcls()
		{
			// Format the base dir, should succeed
			org.apache.hadoop.ha.DummyHAService svc = cluster.getService(1);
			NUnit.Framework.Assert.AreEqual(0, runFC(svc, "-formatZK"));
			org.apache.zookeeper.ZooKeeper otherClient = createClient();
			try
			{
				// client without auth should not be able to read it
				org.apache.zookeeper.data.Stat stat = new org.apache.zookeeper.data.Stat();
				otherClient.getData(org.apache.hadoop.ha.ZKFailoverController.ZK_PARENT_ZNODE_DEFAULT
					, false, stat);
				NUnit.Framework.Assert.Fail("Was able to read data without authenticating!");
			}
			catch (org.apache.zookeeper.KeeperException.NoAuthException)
			{
			}
		}

		// expected
		/// <summary>
		/// Test that the ZKFC won't run if fencing is not configured for the
		/// local service.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void testFencingMustBeConfigured()
		{
			org.apache.hadoop.ha.DummyHAService svc = org.mockito.Mockito.spy(cluster.getService
				(0));
			org.mockito.Mockito.doThrow(new org.apache.hadoop.ha.BadFencingConfigurationException
				("no fencing")).when(svc).checkFencingConfigured();
			// Format the base dir, should succeed
			NUnit.Framework.Assert.AreEqual(0, runFC(svc, "-formatZK"));
			// Try to run the actual FC, should fail without a fencer
			NUnit.Framework.Assert.AreEqual(org.apache.hadoop.ha.ZKFailoverController.ERR_CODE_NO_FENCER
				, runFC(svc));
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
		public virtual void testAutoFailoverOnBadHealth()
		{
			try
			{
				cluster.start();
				org.apache.hadoop.ha.DummyHAService svc1 = cluster.getService(1);
				LOG.info("Faking svc0 unhealthy, should failover to svc1");
				cluster.setHealthy(0, false);
				LOG.info("Waiting for svc0 to enter initializing state");
				cluster.waitForHAState(0, org.apache.hadoop.ha.HAServiceProtocol.HAServiceState.INITIALIZING
					);
				cluster.waitForHAState(1, org.apache.hadoop.ha.HAServiceProtocol.HAServiceState.ACTIVE
					);
				LOG.info("Allowing svc0 to be healthy again, making svc1 unreachable " + "and fail to gracefully go to standby"
					);
				cluster.setUnreachable(1, true);
				cluster.setHealthy(0, true);
				// Should fail back to svc0 at this point
				cluster.waitForHAState(0, org.apache.hadoop.ha.HAServiceProtocol.HAServiceState.ACTIVE
					);
				// and fence svc1
				org.mockito.Mockito.verify(svc1.fencer).fence(org.mockito.Mockito.same(svc1));
			}
			finally
			{
				cluster.stop();
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
		public virtual void testAutoFailoverOnBadState()
		{
			try
			{
				cluster.start();
				org.apache.hadoop.ha.DummyHAService svc0 = cluster.getService(0);
				LOG.info("Faking svc0 to change the state, should failover to svc1");
				svc0.state = org.apache.hadoop.ha.HAServiceProtocol.HAServiceState.STANDBY;
				// Should fail back to svc0 at this point
				cluster.waitForHAState(1, org.apache.hadoop.ha.HAServiceProtocol.HAServiceState.ACTIVE
					);
			}
			finally
			{
				cluster.stop();
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void testAutoFailoverOnLostZKSession()
		{
			try
			{
				cluster.start();
				// Expire svc0, it should fail over to svc1
				cluster.expireAndVerifyFailover(0, 1);
				// Expire svc1, it should fail back to svc0
				cluster.expireAndVerifyFailover(1, 0);
				LOG.info("======= Running test cases second time to test " + "re-establishment ========="
					);
				// Expire svc0, it should fail over to svc1
				cluster.expireAndVerifyFailover(0, 1);
				// Expire svc1, it should fail back to svc0
				cluster.expireAndVerifyFailover(1, 0);
			}
			finally
			{
				cluster.stop();
			}
		}

		/// <summary>
		/// Test that, if the standby node is unhealthy, it doesn't try to become
		/// active
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void testDontFailoverToUnhealthyNode()
		{
			try
			{
				cluster.start();
				// Make svc1 unhealthy, and wait for its FC to notice the bad health.
				cluster.setHealthy(1, false);
				cluster.waitForHealthState(1, org.apache.hadoop.ha.HealthMonitor.State.SERVICE_UNHEALTHY
					);
				// Expire svc0
				cluster.getElector(0).preventSessionReestablishmentForTests();
				try
				{
					cluster.expireActiveLockHolder(0);
					LOG.info("Expired svc0's ZK session. Waiting a second to give svc1" + " a chance to take the lock, if it is ever going to."
						);
					java.lang.Thread.sleep(1000);
					// Ensure that no one holds the lock.
					cluster.waitForActiveLockHolder(null);
				}
				finally
				{
					LOG.info("Allowing svc0's elector to re-establish its connection");
					cluster.getElector(0).allowSessionReestablishmentForTests();
				}
				// svc0 should get the lock again
				cluster.waitForActiveLockHolder(0);
			}
			finally
			{
				cluster.stop();
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
		public virtual void testBecomingActiveFails()
		{
			try
			{
				cluster.start();
				org.apache.hadoop.ha.DummyHAService svc1 = cluster.getService(1);
				LOG.info("Making svc1 fail to become active");
				cluster.setFailToBecomeActive(1, true);
				LOG.info("Faking svc0 unhealthy, should NOT successfully " + "failover to svc1");
				cluster.setHealthy(0, false);
				cluster.waitForHealthState(0, org.apache.hadoop.ha.HealthMonitor.State.SERVICE_UNHEALTHY
					);
				cluster.waitForActiveLockHolder(null);
				org.mockito.Mockito.verify(svc1.proxy, org.mockito.Mockito.timeout(2000).atLeastOnce
					()).transitionToActive(org.mockito.Mockito.any<org.apache.hadoop.ha.HAServiceProtocol.StateChangeRequestInfo
					>());
				cluster.waitForHAState(0, org.apache.hadoop.ha.HAServiceProtocol.HAServiceState.INITIALIZING
					);
				cluster.waitForHAState(1, org.apache.hadoop.ha.HAServiceProtocol.HAServiceState.STANDBY
					);
				LOG.info("Faking svc0 healthy again, should go back to svc0");
				cluster.setHealthy(0, true);
				cluster.waitForHAState(0, org.apache.hadoop.ha.HAServiceProtocol.HAServiceState.ACTIVE
					);
				cluster.waitForHAState(1, org.apache.hadoop.ha.HAServiceProtocol.HAServiceState.STANDBY
					);
				cluster.waitForActiveLockHolder(0);
				// Ensure that we can fail back to svc1  once it it is able
				// to become active (e.g the admin has restarted it)
				LOG.info("Allowing svc1 to become active, expiring svc0");
				svc1.failToBecomeActive = false;
				cluster.expireAndVerifyFailover(0, 1);
			}
			finally
			{
				cluster.stop();
			}
		}

		/// <summary>
		/// Test that, when ZooKeeper fails, the system remains in its
		/// current state, without triggering any failovers, and without
		/// causing the active node to enter standby state.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void testZooKeeperFailure()
		{
			try
			{
				cluster.start();
				// Record initial ZK sessions
				long session0 = cluster.getElector(0).getZKSessionIdForTests();
				long session1 = cluster.getElector(1).getZKSessionIdForTests();
				LOG.info("====== Stopping ZK server");
				stopServer();
				waitForServerDown(hostPort, CONNECTION_TIMEOUT);
				LOG.info("====== Waiting for services to enter NEUTRAL mode");
				cluster.waitForElectorState(0, org.apache.hadoop.ha.ActiveStandbyElector.State.NEUTRAL
					);
				cluster.waitForElectorState(1, org.apache.hadoop.ha.ActiveStandbyElector.State.NEUTRAL
					);
				LOG.info("====== Checking that the services didn't change HA state");
				NUnit.Framework.Assert.AreEqual(org.apache.hadoop.ha.HAServiceProtocol.HAServiceState
					.ACTIVE, cluster.getService(0).state);
				NUnit.Framework.Assert.AreEqual(org.apache.hadoop.ha.HAServiceProtocol.HAServiceState
					.STANDBY, cluster.getService(1).state);
				LOG.info("====== Restarting server");
				startServer();
				waitForServerUp(hostPort, CONNECTION_TIMEOUT);
				// Nodes should go back to their original states, since they re-obtain
				// the same sessions.
				cluster.waitForElectorState(0, org.apache.hadoop.ha.ActiveStandbyElector.State.ACTIVE
					);
				cluster.waitForElectorState(1, org.apache.hadoop.ha.ActiveStandbyElector.State.STANDBY
					);
				// Check HA states didn't change.
				cluster.waitForHAState(0, org.apache.hadoop.ha.HAServiceProtocol.HAServiceState.ACTIVE
					);
				cluster.waitForHAState(1, org.apache.hadoop.ha.HAServiceProtocol.HAServiceState.STANDBY
					);
				// Check they re-used the same sessions and didn't spuriously reconnect
				NUnit.Framework.Assert.AreEqual(session0, cluster.getElector(0).getZKSessionIdForTests
					());
				NUnit.Framework.Assert.AreEqual(session1, cluster.getElector(1).getZKSessionIdForTests
					());
			}
			finally
			{
				cluster.stop();
			}
		}

		/// <summary>Test that the ZKFC can gracefully cede its active status.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void testCedeActive()
		{
			try
			{
				cluster.start();
				org.apache.hadoop.ha.MiniZKFCCluster.DummyZKFC zkfc = cluster.getZkfc(0);
				// It should be in active to start.
				NUnit.Framework.Assert.AreEqual(org.apache.hadoop.ha.ActiveStandbyElector.State.ACTIVE
					, zkfc.getElectorForTests().getStateForTests());
				// Ask it to cede active for 3 seconds. It should respond promptly
				// (i.e. the RPC itself should not take 3 seconds!)
				org.apache.hadoop.ha.ZKFCProtocol proxy = zkfc.getLocalTarget().getZKFCProxy(conf
					, 5000);
				long st = org.apache.hadoop.util.Time.now();
				proxy.cedeActive(3000);
				long et = org.apache.hadoop.util.Time.now();
				NUnit.Framework.Assert.IsTrue("RPC to cedeActive took " + (et - st) + " ms", et -
					 st < 1000);
				// Should be in "INIT" state since it's not in the election
				// at this point.
				NUnit.Framework.Assert.AreEqual(org.apache.hadoop.ha.ActiveStandbyElector.State.INIT
					, zkfc.getElectorForTests().getStateForTests());
				// After the prescribed 3 seconds, should go into STANDBY state,
				// since the other node in the cluster would have taken ACTIVE.
				cluster.waitForElectorState(0, org.apache.hadoop.ha.ActiveStandbyElector.State.STANDBY
					);
				long et2 = org.apache.hadoop.util.Time.now();
				NUnit.Framework.Assert.IsTrue("Should take ~3 seconds to rejoin. Only took " + (et2
					 - et) + "ms before rejoining.", et2 - et > 2800);
			}
			finally
			{
				cluster.stop();
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void testGracefulFailover()
		{
			try
			{
				cluster.start();
				cluster.waitForActiveLockHolder(0);
				cluster.getService(1).getZKFCProxy(conf, 5000).gracefulFailover();
				cluster.waitForActiveLockHolder(1);
				cluster.getService(0).getZKFCProxy(conf, 5000).gracefulFailover();
				cluster.waitForActiveLockHolder(0);
				java.lang.Thread.sleep(10000);
				// allow to quiesce
				NUnit.Framework.Assert.AreEqual(0, cluster.getService(0).fenceCount);
				NUnit.Framework.Assert.AreEqual(0, cluster.getService(1).fenceCount);
				NUnit.Framework.Assert.AreEqual(2, cluster.getService(0).activeTransitionCount);
				NUnit.Framework.Assert.AreEqual(1, cluster.getService(1).activeTransitionCount);
			}
			finally
			{
				cluster.stop();
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void testGracefulFailoverToUnhealthy()
		{
			try
			{
				cluster.start();
				cluster.waitForActiveLockHolder(0);
				// Mark it unhealthy, wait for it to exit election
				cluster.setHealthy(1, false);
				cluster.waitForElectorState(1, org.apache.hadoop.ha.ActiveStandbyElector.State.INIT
					);
				// Ask for failover, it should fail, because it's unhealthy
				try
				{
					cluster.getService(1).getZKFCProxy(conf, 5000).gracefulFailover();
					NUnit.Framework.Assert.Fail("Did not fail to graceful failover to unhealthy service!"
						);
				}
				catch (org.apache.hadoop.ha.ServiceFailedException sfe)
				{
					org.apache.hadoop.test.GenericTestUtils.assertExceptionContains(cluster.getService
						(1).ToString() + " is not currently healthy.", sfe);
				}
			}
			finally
			{
				cluster.stop();
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void testGracefulFailoverFailBecomingActive()
		{
			try
			{
				cluster.start();
				cluster.waitForActiveLockHolder(0);
				cluster.setFailToBecomeActive(1, true);
				// Ask for failover, it should fail and report back to user.
				try
				{
					cluster.getService(1).getZKFCProxy(conf, 5000).gracefulFailover();
					NUnit.Framework.Assert.Fail("Did not fail to graceful failover when target failed "
						 + "to become active!");
				}
				catch (org.apache.hadoop.ha.ServiceFailedException sfe)
				{
					org.apache.hadoop.test.GenericTestUtils.assertExceptionContains("Couldn't make " 
						+ cluster.getService(1) + " active", sfe);
					org.apache.hadoop.test.GenericTestUtils.assertExceptionContains("injected failure"
						, sfe);
				}
				// No fencing
				NUnit.Framework.Assert.AreEqual(0, cluster.getService(0).fenceCount);
				NUnit.Framework.Assert.AreEqual(0, cluster.getService(1).fenceCount);
				// Service 0 should go back to being active after the failed failover
				cluster.waitForActiveLockHolder(0);
			}
			finally
			{
				cluster.stop();
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void testGracefulFailoverFailBecomingStandby()
		{
			try
			{
				cluster.start();
				cluster.waitForActiveLockHolder(0);
				// Ask for failover when old node fails to transition to standby.
				// This should trigger fencing, since the cedeActive() command
				// still works, but leaves the breadcrumb in place.
				cluster.setFailToBecomeStandby(0, true);
				cluster.getService(1).getZKFCProxy(conf, 5000).gracefulFailover();
				// Check that the old node was fenced
				NUnit.Framework.Assert.AreEqual(1, cluster.getService(0).fenceCount);
			}
			finally
			{
				cluster.stop();
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void testGracefulFailoverFailBecomingStandbyAndFailFence()
		{
			try
			{
				cluster.start();
				cluster.waitForActiveLockHolder(0);
				// Ask for failover when old node fails to transition to standby.
				// This should trigger fencing, since the cedeActive() command
				// still works, but leaves the breadcrumb in place.
				cluster.setFailToBecomeStandby(0, true);
				cluster.setFailToFence(0, true);
				try
				{
					cluster.getService(1).getZKFCProxy(conf, 5000).gracefulFailover();
					NUnit.Framework.Assert.Fail("Failover should have failed when old node wont fence"
						);
				}
				catch (org.apache.hadoop.ha.ServiceFailedException sfe)
				{
					org.apache.hadoop.test.GenericTestUtils.assertExceptionContains("Unable to fence "
						 + cluster.getService(0), sfe);
				}
			}
			finally
			{
				cluster.stop();
			}
		}

		/// <summary>Test which exercises all of the inputs into ZKFC.</summary>
		/// <remarks>
		/// Test which exercises all of the inputs into ZKFC. This is particularly
		/// useful for running under jcarder to check for lock order violations.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void testOneOfEverything()
		{
			try
			{
				cluster.start();
				// Failover by session expiration
				LOG.info("====== Failing over by session expiration");
				cluster.expireAndVerifyFailover(0, 1);
				cluster.expireAndVerifyFailover(1, 0);
				// Restart ZK
				LOG.info("====== Restarting server");
				stopServer();
				waitForServerDown(hostPort, CONNECTION_TIMEOUT);
				startServer();
				waitForServerUp(hostPort, CONNECTION_TIMEOUT);
				// Failover by bad health
				cluster.setHealthy(0, false);
				cluster.waitForHAState(0, org.apache.hadoop.ha.HAServiceProtocol.HAServiceState.INITIALIZING
					);
				cluster.waitForHAState(1, org.apache.hadoop.ha.HAServiceProtocol.HAServiceState.ACTIVE
					);
				cluster.setHealthy(1, true);
				cluster.setHealthy(0, false);
				cluster.waitForHAState(1, org.apache.hadoop.ha.HAServiceProtocol.HAServiceState.ACTIVE
					);
				cluster.waitForHAState(0, org.apache.hadoop.ha.HAServiceProtocol.HAServiceState.INITIALIZING
					);
				cluster.setHealthy(0, true);
				cluster.waitForHealthState(0, org.apache.hadoop.ha.HealthMonitor.State.SERVICE_HEALTHY
					);
				// Graceful failovers
				cluster.getZkfc(1).gracefulFailoverToYou();
				cluster.getZkfc(0).gracefulFailoverToYou();
			}
			finally
			{
				cluster.stop();
			}
		}

		/// <exception cref="System.Exception"/>
		private int runFC(org.apache.hadoop.ha.DummyHAService target, params string[] args
			)
		{
			org.apache.hadoop.ha.MiniZKFCCluster.DummyZKFC zkfc = new org.apache.hadoop.ha.MiniZKFCCluster.DummyZKFC
				(conf, target);
			return zkfc.run(args);
		}
	}
}
