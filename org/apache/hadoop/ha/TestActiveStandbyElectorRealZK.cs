using Sharpen;

namespace org.apache.hadoop.ha
{
	/// <summary>
	/// Test for
	/// <see cref="ActiveStandbyElector"/>
	/// using real zookeeper.
	/// </summary>
	public class TestActiveStandbyElectorRealZK : org.apache.hadoop.ha.ClientBaseWithFixes
	{
		internal const int NUM_ELECTORS = 2;

		static TestActiveStandbyElectorRealZK()
		{
			((org.apache.commons.logging.impl.Log4JLogger)org.apache.hadoop.ha.ActiveStandbyElector
				.LOG).getLogger().setLevel(org.apache.log4j.Level.ALL);
		}

		internal static readonly string PARENT_DIR = "/" + java.util.UUID.randomUUID();

		internal org.apache.hadoop.ha.ActiveStandbyElector[] electors = new org.apache.hadoop.ha.ActiveStandbyElector
			[NUM_ELECTORS];

		private byte[][] appDatas = new byte[NUM_ELECTORS][];

		private org.apache.hadoop.ha.ActiveStandbyElector.ActiveStandbyElectorCallback[] 
			cbs = new org.apache.hadoop.ha.ActiveStandbyElector.ActiveStandbyElectorCallback
			[NUM_ELECTORS];

		private org.apache.zookeeper.server.ZooKeeperServer zkServer;

		/// <exception cref="System.Exception"/>
		public override void setUp()
		{
			base.setUp();
			zkServer = getServer(serverFactory);
			for (int i = 0; i < NUM_ELECTORS; i++)
			{
				cbs[i] = org.mockito.Mockito.mock<org.apache.hadoop.ha.ActiveStandbyElector.ActiveStandbyElectorCallback
					>();
				appDatas[i] = com.google.common.primitives.Ints.toByteArray(i);
				electors[i] = new org.apache.hadoop.ha.ActiveStandbyElector(hostPort, 5000, PARENT_DIR
					, org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE, java.util.Collections.emptyList
					<org.apache.hadoop.util.ZKUtil.ZKAuthInfo>(), cbs[i], org.apache.hadoop.fs.CommonConfigurationKeys
					.HA_FC_ELECTOR_ZK_OP_RETRIES_DEFAULT);
			}
		}

		/// <exception cref="System.Exception"/>
		private void checkFatalsAndReset()
		{
			for (int i = 0; i < NUM_ELECTORS; i++)
			{
				org.mockito.Mockito.verify(cbs[i], org.mockito.Mockito.never()).notifyFatalError(
					org.mockito.Mockito.anyString());
				org.mockito.Mockito.reset(cbs[i]);
			}
		}

		/// <summary>
		/// the test creates 2 electors which try to become active using a real
		/// zookeeper server.
		/// </summary>
		/// <remarks>
		/// the test creates 2 electors which try to become active using a real
		/// zookeeper server. It verifies that 1 becomes active and 1 becomes standby.
		/// Upon becoming active the leader quits election and the test verifies that
		/// the standby now becomes active.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void testActiveStandbyTransition()
		{
			LOG.info("starting test with parentDir:" + PARENT_DIR);
			NUnit.Framework.Assert.IsFalse(electors[0].parentZNodeExists());
			electors[0].ensureParentZNode();
			NUnit.Framework.Assert.IsTrue(electors[0].parentZNodeExists());
			// First elector joins election, becomes active.
			electors[0].joinElection(appDatas[0]);
			org.apache.hadoop.ha.ActiveStandbyElectorTestUtil.waitForActiveLockData(null, zkServer
				, PARENT_DIR, appDatas[0]);
			org.mockito.Mockito.verify(cbs[0], org.mockito.Mockito.timeout(1000)).becomeActive
				();
			checkFatalsAndReset();
			// Second elector joins election, becomes standby.
			electors[1].joinElection(appDatas[1]);
			org.mockito.Mockito.verify(cbs[1], org.mockito.Mockito.timeout(1000)).becomeStandby
				();
			checkFatalsAndReset();
			// First elector quits, second one should become active
			electors[0].quitElection(true);
			org.apache.hadoop.ha.ActiveStandbyElectorTestUtil.waitForActiveLockData(null, zkServer
				, PARENT_DIR, appDatas[1]);
			org.mockito.Mockito.verify(cbs[1], org.mockito.Mockito.timeout(1000)).becomeActive
				();
			checkFatalsAndReset();
			// First one rejoins, becomes standby, second one stays active
			electors[0].joinElection(appDatas[0]);
			org.mockito.Mockito.verify(cbs[0], org.mockito.Mockito.timeout(1000)).becomeStandby
				();
			checkFatalsAndReset();
			// Second one expires, first one becomes active
			electors[1].preventSessionReestablishmentForTests();
			try
			{
				zkServer.closeSession(electors[1].getZKSessionIdForTests());
				org.apache.hadoop.ha.ActiveStandbyElectorTestUtil.waitForActiveLockData(null, zkServer
					, PARENT_DIR, appDatas[0]);
				org.mockito.Mockito.verify(cbs[1], org.mockito.Mockito.timeout(1000)).enterNeutralMode
					();
				org.mockito.Mockito.verify(cbs[0], org.mockito.Mockito.timeout(1000)).fenceOldActive
					(org.mockito.AdditionalMatchers.aryEq(appDatas[1]));
				org.mockito.Mockito.verify(cbs[0], org.mockito.Mockito.timeout(1000)).becomeActive
					();
			}
			finally
			{
				electors[1].allowSessionReestablishmentForTests();
			}
			// Second one eventually reconnects and becomes standby
			org.mockito.Mockito.verify(cbs[1], org.mockito.Mockito.timeout(5000)).becomeStandby
				();
			checkFatalsAndReset();
			// First one expires, second one should become active
			electors[0].preventSessionReestablishmentForTests();
			try
			{
				zkServer.closeSession(electors[0].getZKSessionIdForTests());
				org.apache.hadoop.ha.ActiveStandbyElectorTestUtil.waitForActiveLockData(null, zkServer
					, PARENT_DIR, appDatas[1]);
				org.mockito.Mockito.verify(cbs[0], org.mockito.Mockito.timeout(1000)).enterNeutralMode
					();
				org.mockito.Mockito.verify(cbs[1], org.mockito.Mockito.timeout(1000)).fenceOldActive
					(org.mockito.AdditionalMatchers.aryEq(appDatas[0]));
				org.mockito.Mockito.verify(cbs[1], org.mockito.Mockito.timeout(1000)).becomeActive
					();
			}
			finally
			{
				electors[0].allowSessionReestablishmentForTests();
			}
			checkFatalsAndReset();
		}

		/// <exception cref="System.Exception"/>
		public virtual void testHandleSessionExpiration()
		{
			org.apache.hadoop.ha.ActiveStandbyElector.ActiveStandbyElectorCallback cb = cbs[0
				];
			byte[] appData = appDatas[0];
			org.apache.hadoop.ha.ActiveStandbyElector elector = electors[0];
			// Let the first elector become active
			elector.ensureParentZNode();
			elector.joinElection(appData);
			org.apache.zookeeper.server.ZooKeeperServer zks = getServer(serverFactory);
			org.apache.hadoop.ha.ActiveStandbyElectorTestUtil.waitForActiveLockData(null, zks
				, PARENT_DIR, appData);
			org.mockito.Mockito.verify(cb, org.mockito.Mockito.timeout(1000)).becomeActive();
			checkFatalsAndReset();
			LOG.info("========================== Expiring session");
			zks.closeSession(elector.getZKSessionIdForTests());
			// Should enter neutral mode when disconnected
			org.mockito.Mockito.verify(cb, org.mockito.Mockito.timeout(1000)).enterNeutralMode
				();
			// Should re-join the election and regain active
			org.apache.hadoop.ha.ActiveStandbyElectorTestUtil.waitForActiveLockData(null, zks
				, PARENT_DIR, appData);
			org.mockito.Mockito.verify(cb, org.mockito.Mockito.timeout(1000)).becomeActive();
			checkFatalsAndReset();
			LOG.info("========================== Quitting election");
			elector.quitElection(false);
			org.apache.hadoop.ha.ActiveStandbyElectorTestUtil.waitForActiveLockData(null, zks
				, PARENT_DIR, null);
			// Double check that we don't accidentally re-join the election
			// due to receiving the "expired" event.
			java.lang.Thread.sleep(1000);
			org.mockito.Mockito.verify(cb, org.mockito.Mockito.never()).becomeActive();
			org.apache.hadoop.ha.ActiveStandbyElectorTestUtil.waitForActiveLockData(null, zks
				, PARENT_DIR, null);
			checkFatalsAndReset();
		}

		/// <exception cref="System.Exception"/>
		public virtual void testHandleSessionExpirationOfStandby()
		{
			// Let elector 0 be active
			electors[0].ensureParentZNode();
			electors[0].joinElection(appDatas[0]);
			org.apache.zookeeper.server.ZooKeeperServer zks = getServer(serverFactory);
			org.apache.hadoop.ha.ActiveStandbyElectorTestUtil.waitForActiveLockData(null, zks
				, PARENT_DIR, appDatas[0]);
			org.mockito.Mockito.verify(cbs[0], org.mockito.Mockito.timeout(1000)).becomeActive
				();
			checkFatalsAndReset();
			// Let elector 1 be standby
			electors[1].joinElection(appDatas[1]);
			org.apache.hadoop.ha.ActiveStandbyElectorTestUtil.waitForElectorState(null, electors
				[1], org.apache.hadoop.ha.ActiveStandbyElector.State.STANDBY);
			LOG.info("========================== Expiring standby's session");
			zks.closeSession(electors[1].getZKSessionIdForTests());
			// Should enter neutral mode when disconnected
			org.mockito.Mockito.verify(cbs[1], org.mockito.Mockito.timeout(1000)).enterNeutralMode
				();
			// Should re-join the election and go back to STANDBY
			org.apache.hadoop.ha.ActiveStandbyElectorTestUtil.waitForElectorState(null, electors
				[1], org.apache.hadoop.ha.ActiveStandbyElector.State.STANDBY);
			checkFatalsAndReset();
			LOG.info("========================== Quitting election");
			electors[1].quitElection(false);
			// Double check that we don't accidentally re-join the election
			// by quitting elector 0 and ensuring elector 1 doesn't become active
			electors[0].quitElection(false);
			// due to receiving the "expired" event.
			java.lang.Thread.sleep(1000);
			org.mockito.Mockito.verify(cbs[1], org.mockito.Mockito.never()).becomeActive();
			org.apache.hadoop.ha.ActiveStandbyElectorTestUtil.waitForActiveLockData(null, zks
				, PARENT_DIR, null);
			checkFatalsAndReset();
		}

		/// <exception cref="System.Exception"/>
		public virtual void testDontJoinElectionOnDisconnectAndReconnect()
		{
			electors[0].ensureParentZNode();
			stopServer();
			org.apache.hadoop.ha.ActiveStandbyElectorTestUtil.waitForElectorState(null, electors
				[0], org.apache.hadoop.ha.ActiveStandbyElector.State.NEUTRAL);
			startServer();
			waitForServerUp(hostPort, CONNECTION_TIMEOUT);
			// Have to sleep to allow time for the clients to reconnect.
			java.lang.Thread.sleep(2000);
			org.mockito.Mockito.verify(cbs[0], org.mockito.Mockito.never()).becomeActive();
			org.mockito.Mockito.verify(cbs[1], org.mockito.Mockito.never()).becomeActive();
			checkFatalsAndReset();
		}
	}
}
