using Sharpen;

namespace org.apache.hadoop.ha
{
	public class TestActiveStandbyElector
	{
		private org.apache.zookeeper.ZooKeeper mockZK;

		private int count;

		private org.apache.hadoop.ha.ActiveStandbyElector.ActiveStandbyElectorCallback mockApp;

		private readonly byte[] data = new byte[8];

		private org.apache.hadoop.ha.TestActiveStandbyElector.ActiveStandbyElectorTester 
			elector;

		internal class ActiveStandbyElectorTester : org.apache.hadoop.ha.ActiveStandbyElector
		{
			private int sleptFor = 0;

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="org.apache.zookeeper.KeeperException"/>
			internal ActiveStandbyElectorTester(TestActiveStandbyElector _enclosing, string hostPort
				, int timeout, string parent, System.Collections.Generic.IList<org.apache.zookeeper.data.ACL
				> acl, org.apache.hadoop.ha.ActiveStandbyElector.ActiveStandbyElectorCallback app
				)
				: base(hostPort, timeout, parent, acl, java.util.Collections.emptyList<org.apache.hadoop.util.ZKUtil.ZKAuthInfo
					>(), app, org.apache.hadoop.fs.CommonConfigurationKeys.HA_FC_ELECTOR_ZK_OP_RETRIES_DEFAULT
					)
			{
				this._enclosing = _enclosing;
			}

			protected internal override org.apache.zookeeper.ZooKeeper getNewZooKeeper()
			{
				++this._enclosing.count;
				return this._enclosing.mockZK;
			}

			protected internal override void sleepFor(int ms)
			{
				// don't sleep in unit tests! Instead, just record the amount of
				// time slept
				org.apache.hadoop.ha.ActiveStandbyElector.LOG.info("Would have slept for " + ms +
					 "ms");
				this.sleptFor += ms;
			}

			private readonly TestActiveStandbyElector _enclosing;
		}

		private const string ZK_PARENT_NAME = "/parent/node";

		private const string ZK_LOCK_NAME = ZK_PARENT_NAME + "/" + org.apache.hadoop.ha.ActiveStandbyElector
			.LOCK_FILENAME;

		private const string ZK_BREADCRUMB_NAME = ZK_PARENT_NAME + "/" + org.apache.hadoop.ha.ActiveStandbyElector
			.BREADCRUMB_FILENAME;

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="org.apache.zookeeper.KeeperException"/>
		[NUnit.Framework.SetUp]
		public virtual void init()
		{
			count = 0;
			mockZK = org.mockito.Mockito.mock<org.apache.zookeeper.ZooKeeper>();
			mockApp = org.mockito.Mockito.mock<org.apache.hadoop.ha.ActiveStandbyElector.ActiveStandbyElectorCallback
				>();
			elector = new org.apache.hadoop.ha.TestActiveStandbyElector.ActiveStandbyElectorTester
				(this, "hostPort", 1000, ZK_PARENT_NAME, org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE
				, mockApp);
		}

		/// <summary>Set up the mock ZK to return no info for a prior active in ZK.</summary>
		/// <exception cref="System.Exception"/>
		private void mockNoPriorActive()
		{
			org.mockito.Mockito.doThrow(new org.apache.zookeeper.KeeperException.NoNodeException
				()).when(mockZK).getData(org.mockito.Mockito.eq(ZK_BREADCRUMB_NAME), org.mockito.Mockito
				.anyBoolean(), org.mockito.Mockito.any<org.apache.zookeeper.data.Stat>());
		}

		/// <summary>Set up the mock to return info for some prior active node in ZK./</summary>
		/// <exception cref="System.Exception"/>
		private void mockPriorActive(byte[] data)
		{
			org.mockito.Mockito.doReturn(data).when(mockZK).getData(org.mockito.Mockito.eq(ZK_BREADCRUMB_NAME
				), org.mockito.Mockito.anyBoolean(), org.mockito.Mockito.any<org.apache.zookeeper.data.Stat
				>());
		}

		/// <summary>verify that joinElection checks for null data</summary>
		public virtual void testJoinElectionException()
		{
			elector.joinElection(null);
		}

		/// <summary>verify that joinElection tries to create ephemeral lock znode</summary>
		[NUnit.Framework.Test]
		public virtual void testJoinElection()
		{
			elector.joinElection(data);
			org.mockito.Mockito.verify(mockZK, org.mockito.Mockito.times(1)).create(ZK_LOCK_NAME
				, data, org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE, org.apache.zookeeper.CreateMode
				.EPHEMERAL, elector, mockZK);
		}

		/// <summary>
		/// verify that successful znode create result becomes active and monitoring is
		/// started
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testCreateNodeResultBecomeActive()
		{
			mockNoPriorActive();
			elector.joinElection(data);
			elector.processResult(org.apache.zookeeper.KeeperException.Code.OK.intValue(), ZK_LOCK_NAME
				, mockZK, ZK_LOCK_NAME);
			org.mockito.Mockito.verify(mockApp, org.mockito.Mockito.times(1)).becomeActive();
			verifyExistCall(1);
			// monitor callback verifies the leader is ephemeral owner of lock but does
			// not call becomeActive since its already active
			org.apache.zookeeper.data.Stat stat = new org.apache.zookeeper.data.Stat();
			stat.setEphemeralOwner(1L);
			org.mockito.Mockito.when(mockZK.getSessionId()).thenReturn(1L);
			elector.processResult(org.apache.zookeeper.KeeperException.Code.OK.intValue(), ZK_LOCK_NAME
				, mockZK, stat);
			// should not call neutral mode/standby/active
			org.mockito.Mockito.verify(mockApp, org.mockito.Mockito.times(0)).enterNeutralMode
				();
			org.mockito.Mockito.verify(mockApp, org.mockito.Mockito.times(0)).becomeStandby();
			org.mockito.Mockito.verify(mockApp, org.mockito.Mockito.times(1)).becomeActive();
			// another joinElection not called.
			org.mockito.Mockito.verify(mockZK, org.mockito.Mockito.times(1)).create(ZK_LOCK_NAME
				, data, org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE, org.apache.zookeeper.CreateMode
				.EPHEMERAL, elector, mockZK);
			// no new monitor called
			verifyExistCall(1);
		}

		/// <summary>
		/// Verify that, when the callback fails to enter active state,
		/// the elector rejoins the election after sleeping for a short period.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testFailToBecomeActive()
		{
			mockNoPriorActive();
			elector.joinElection(data);
			NUnit.Framework.Assert.AreEqual(0, elector.sleptFor);
			org.mockito.Mockito.doThrow(new org.apache.hadoop.ha.ServiceFailedException("failed to become active"
				)).when(mockApp).becomeActive();
			elector.processResult(org.apache.zookeeper.KeeperException.Code.OK.intValue(), ZK_LOCK_NAME
				, mockZK, ZK_LOCK_NAME);
			// Should have tried to become active
			org.mockito.Mockito.verify(mockApp).becomeActive();
			// should re-join
			org.mockito.Mockito.verify(mockZK, org.mockito.Mockito.times(2)).create(ZK_LOCK_NAME
				, data, org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE, org.apache.zookeeper.CreateMode
				.EPHEMERAL, elector, mockZK);
			NUnit.Framework.Assert.AreEqual(2, count);
			NUnit.Framework.Assert.IsTrue(elector.sleptFor > 0);
		}

		/// <summary>
		/// Verify that, when the callback fails to enter active state, after
		/// a ZK disconnect (i.e from the StatCallback), that the elector rejoins
		/// the election after sleeping for a short period.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testFailToBecomeActiveAfterZKDisconnect()
		{
			mockNoPriorActive();
			elector.joinElection(data);
			NUnit.Framework.Assert.AreEqual(0, elector.sleptFor);
			elector.processResult(org.apache.zookeeper.KeeperException.Code.CONNECTIONLOSS.intValue
				(), ZK_LOCK_NAME, mockZK, ZK_LOCK_NAME);
			org.mockito.Mockito.verify(mockZK, org.mockito.Mockito.times(2)).create(ZK_LOCK_NAME
				, data, org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE, org.apache.zookeeper.CreateMode
				.EPHEMERAL, elector, mockZK);
			elector.processResult(org.apache.zookeeper.KeeperException.Code.NODEEXISTS.intValue
				(), ZK_LOCK_NAME, mockZK, ZK_LOCK_NAME);
			verifyExistCall(1);
			org.apache.zookeeper.data.Stat stat = new org.apache.zookeeper.data.Stat();
			stat.setEphemeralOwner(1L);
			org.mockito.Mockito.when(mockZK.getSessionId()).thenReturn(1L);
			// Fake failure to become active from within the stat callback
			org.mockito.Mockito.doThrow(new org.apache.hadoop.ha.ServiceFailedException("fail to become active"
				)).when(mockApp).becomeActive();
			elector.processResult(org.apache.zookeeper.KeeperException.Code.OK.intValue(), ZK_LOCK_NAME
				, mockZK, stat);
			org.mockito.Mockito.verify(mockApp, org.mockito.Mockito.times(1)).becomeActive();
			// should re-join
			org.mockito.Mockito.verify(mockZK, org.mockito.Mockito.times(3)).create(ZK_LOCK_NAME
				, data, org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE, org.apache.zookeeper.CreateMode
				.EPHEMERAL, elector, mockZK);
			NUnit.Framework.Assert.AreEqual(2, count);
			NUnit.Framework.Assert.IsTrue(elector.sleptFor > 0);
		}

		/// <summary>
		/// Verify that, if there is a record of a prior active node, the
		/// elector asks the application to fence it before becoming active.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testFencesOldActive()
		{
			byte[] fakeOldActiveData = new byte[0];
			mockPriorActive(fakeOldActiveData);
			elector.joinElection(data);
			elector.processResult(org.apache.zookeeper.KeeperException.Code.OK.intValue(), ZK_LOCK_NAME
				, mockZK, ZK_LOCK_NAME);
			// Application fences active.
			org.mockito.Mockito.verify(mockApp, org.mockito.Mockito.times(1)).fenceOldActive(
				fakeOldActiveData);
			// Updates breadcrumb node to new data
			org.mockito.Mockito.verify(mockZK, org.mockito.Mockito.times(1)).setData(org.mockito.Mockito
				.eq(ZK_BREADCRUMB_NAME), org.mockito.Mockito.eq(data), org.mockito.Mockito.eq(0)
				);
			// Then it becomes active itself
			org.mockito.Mockito.verify(mockApp, org.mockito.Mockito.times(1)).becomeActive();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testQuitElectionRemovesBreadcrumbNode()
		{
			mockNoPriorActive();
			elector.joinElection(data);
			elector.processResult(org.apache.zookeeper.KeeperException.Code.OK.intValue(), ZK_LOCK_NAME
				, mockZK, ZK_LOCK_NAME);
			// Writes its own active info
			org.mockito.Mockito.verify(mockZK, org.mockito.Mockito.times(1)).create(org.mockito.Mockito
				.eq(ZK_BREADCRUMB_NAME), org.mockito.Mockito.eq(data), org.mockito.Mockito.eq(org.apache.zookeeper.ZooDefs.Ids
				.OPEN_ACL_UNSAFE), org.mockito.Mockito.eq(org.apache.zookeeper.CreateMode.PERSISTENT
				));
			mockPriorActive(data);
			elector.quitElection(false);
			// Deletes its own active data
			org.mockito.Mockito.verify(mockZK, org.mockito.Mockito.times(1)).delete(org.mockito.Mockito
				.eq(ZK_BREADCRUMB_NAME), org.mockito.Mockito.eq(0));
		}

		/// <summary>
		/// verify that znode create for existing node and no retry becomes standby and
		/// monitoring is started
		/// </summary>
		[NUnit.Framework.Test]
		public virtual void testCreateNodeResultBecomeStandby()
		{
			elector.joinElection(data);
			elector.processResult(org.apache.zookeeper.KeeperException.Code.NODEEXISTS.intValue
				(), ZK_LOCK_NAME, mockZK, ZK_LOCK_NAME);
			org.mockito.Mockito.verify(mockApp, org.mockito.Mockito.times(1)).becomeStandby();
			verifyExistCall(1);
		}

		/// <summary>verify that znode create error result in fatal error</summary>
		[NUnit.Framework.Test]
		public virtual void testCreateNodeResultError()
		{
			elector.joinElection(data);
			elector.processResult(org.apache.zookeeper.KeeperException.Code.APIERROR.intValue
				(), ZK_LOCK_NAME, mockZK, ZK_LOCK_NAME);
			org.mockito.Mockito.verify(mockApp, org.mockito.Mockito.times(1)).notifyFatalError
				("Received create error from Zookeeper. code:APIERROR " + "for path " + ZK_LOCK_NAME
				);
		}

		/// <summary>
		/// verify that retry of network errors verifies master by session id and
		/// becomes active if they match.
		/// </summary>
		/// <remarks>
		/// verify that retry of network errors verifies master by session id and
		/// becomes active if they match. monitoring is started.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testCreateNodeResultRetryBecomeActive()
		{
			mockNoPriorActive();
			elector.joinElection(data);
			elector.processResult(org.apache.zookeeper.KeeperException.Code.CONNECTIONLOSS.intValue
				(), ZK_LOCK_NAME, mockZK, ZK_LOCK_NAME);
			elector.processResult(org.apache.zookeeper.KeeperException.Code.CONNECTIONLOSS.intValue
				(), ZK_LOCK_NAME, mockZK, ZK_LOCK_NAME);
			elector.processResult(org.apache.zookeeper.KeeperException.Code.CONNECTIONLOSS.intValue
				(), ZK_LOCK_NAME, mockZK, ZK_LOCK_NAME);
			elector.processResult(org.apache.zookeeper.KeeperException.Code.CONNECTIONLOSS.intValue
				(), ZK_LOCK_NAME, mockZK, ZK_LOCK_NAME);
			// 4 errors results in fatalError
			org.mockito.Mockito.verify(mockApp, org.mockito.Mockito.times(1)).notifyFatalError
				("Received create error from Zookeeper. code:CONNECTIONLOSS " + "for path " + ZK_LOCK_NAME
				 + ". " + "Not retrying further znode create connection errors.");
			elector.joinElection(data);
			// recreate connection via getNewZooKeeper
			NUnit.Framework.Assert.AreEqual(2, count);
			elector.processResult(org.apache.zookeeper.KeeperException.Code.CONNECTIONLOSS.intValue
				(), ZK_LOCK_NAME, mockZK, ZK_LOCK_NAME);
			elector.processResult(org.apache.zookeeper.KeeperException.Code.NODEEXISTS.intValue
				(), ZK_LOCK_NAME, mockZK, ZK_LOCK_NAME);
			verifyExistCall(1);
			org.apache.zookeeper.data.Stat stat = new org.apache.zookeeper.data.Stat();
			stat.setEphemeralOwner(1L);
			org.mockito.Mockito.when(mockZK.getSessionId()).thenReturn(1L);
			elector.processResult(org.apache.zookeeper.KeeperException.Code.OK.intValue(), ZK_LOCK_NAME
				, mockZK, stat);
			org.mockito.Mockito.verify(mockApp, org.mockito.Mockito.times(1)).becomeActive();
			verifyExistCall(1);
			org.mockito.Mockito.verify(mockZK, org.mockito.Mockito.times(6)).create(ZK_LOCK_NAME
				, data, org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE, org.apache.zookeeper.CreateMode
				.EPHEMERAL, elector, mockZK);
		}

		/// <summary>
		/// verify that retry of network errors verifies active by session id and
		/// becomes standby if they dont match.
		/// </summary>
		/// <remarks>
		/// verify that retry of network errors verifies active by session id and
		/// becomes standby if they dont match. monitoring is started.
		/// </remarks>
		[NUnit.Framework.Test]
		public virtual void testCreateNodeResultRetryBecomeStandby()
		{
			elector.joinElection(data);
			elector.processResult(org.apache.zookeeper.KeeperException.Code.CONNECTIONLOSS.intValue
				(), ZK_LOCK_NAME, mockZK, ZK_LOCK_NAME);
			elector.processResult(org.apache.zookeeper.KeeperException.Code.NODEEXISTS.intValue
				(), ZK_LOCK_NAME, mockZK, ZK_LOCK_NAME);
			verifyExistCall(1);
			org.apache.zookeeper.data.Stat stat = new org.apache.zookeeper.data.Stat();
			stat.setEphemeralOwner(0);
			org.mockito.Mockito.when(mockZK.getSessionId()).thenReturn(1L);
			elector.processResult(org.apache.zookeeper.KeeperException.Code.OK.intValue(), ZK_LOCK_NAME
				, mockZK, stat);
			org.mockito.Mockito.verify(mockApp, org.mockito.Mockito.times(1)).becomeStandby();
			verifyExistCall(1);
		}

		/// <summary>
		/// verify that if create znode results in nodeexists and that znode is deleted
		/// before exists() watch is set then the return of the exists() method results
		/// in attempt to re-create the znode and become active
		/// </summary>
		[NUnit.Framework.Test]
		public virtual void testCreateNodeResultRetryNoNode()
		{
			elector.joinElection(data);
			elector.processResult(org.apache.zookeeper.KeeperException.Code.CONNECTIONLOSS.intValue
				(), ZK_LOCK_NAME, mockZK, ZK_LOCK_NAME);
			elector.processResult(org.apache.zookeeper.KeeperException.Code.CONNECTIONLOSS.intValue
				(), ZK_LOCK_NAME, mockZK, ZK_LOCK_NAME);
			elector.processResult(org.apache.zookeeper.KeeperException.Code.NODEEXISTS.intValue
				(), ZK_LOCK_NAME, mockZK, ZK_LOCK_NAME);
			verifyExistCall(1);
			elector.processResult(org.apache.zookeeper.KeeperException.Code.NONODE.intValue()
				, ZK_LOCK_NAME, mockZK, (org.apache.zookeeper.data.Stat)null);
			org.mockito.Mockito.verify(mockApp, org.mockito.Mockito.times(1)).enterNeutralMode
				();
			org.mockito.Mockito.verify(mockZK, org.mockito.Mockito.times(4)).create(ZK_LOCK_NAME
				, data, org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE, org.apache.zookeeper.CreateMode
				.EPHEMERAL, elector, mockZK);
		}

		/// <summary>verify that more than 3 network error retries result fatalError</summary>
		[NUnit.Framework.Test]
		public virtual void testStatNodeRetry()
		{
			elector.joinElection(data);
			elector.processResult(org.apache.zookeeper.KeeperException.Code.CONNECTIONLOSS.intValue
				(), ZK_LOCK_NAME, mockZK, (org.apache.zookeeper.data.Stat)null);
			elector.processResult(org.apache.zookeeper.KeeperException.Code.CONNECTIONLOSS.intValue
				(), ZK_LOCK_NAME, mockZK, (org.apache.zookeeper.data.Stat)null);
			elector.processResult(org.apache.zookeeper.KeeperException.Code.CONNECTIONLOSS.intValue
				(), ZK_LOCK_NAME, mockZK, (org.apache.zookeeper.data.Stat)null);
			elector.processResult(org.apache.zookeeper.KeeperException.Code.CONNECTIONLOSS.intValue
				(), ZK_LOCK_NAME, mockZK, (org.apache.zookeeper.data.Stat)null);
			org.mockito.Mockito.verify(mockApp, org.mockito.Mockito.times(1)).notifyFatalError
				("Received stat error from Zookeeper. code:CONNECTIONLOSS. " + "Not retrying further znode monitoring connection errors."
				);
		}

		/// <summary>verify error in exists() callback results in fatal error</summary>
		[NUnit.Framework.Test]
		public virtual void testStatNodeError()
		{
			elector.joinElection(data);
			elector.processResult(org.apache.zookeeper.KeeperException.Code.RUNTIMEINCONSISTENCY
				.intValue(), ZK_LOCK_NAME, mockZK, (org.apache.zookeeper.data.Stat)null);
			org.mockito.Mockito.verify(mockApp, org.mockito.Mockito.times(0)).enterNeutralMode
				();
			org.mockito.Mockito.verify(mockApp, org.mockito.Mockito.times(1)).notifyFatalError
				("Received stat error from Zookeeper. code:RUNTIMEINCONSISTENCY");
		}

		/// <summary>verify behavior of watcher.process callback with non-node event</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testProcessCallbackEventNone()
		{
			mockNoPriorActive();
			elector.joinElection(data);
			org.apache.zookeeper.WatchedEvent mockEvent = org.mockito.Mockito.mock<org.apache.zookeeper.WatchedEvent
				>();
			org.mockito.Mockito.when(mockEvent.getType()).thenReturn(org.apache.zookeeper.Watcher.Event.EventType
				.None);
			// first SyncConnected should not do anything
			org.mockito.Mockito.when(mockEvent.getState()).thenReturn(org.apache.zookeeper.Watcher.Event.KeeperState
				.SyncConnected);
			elector.processWatchEvent(mockZK, mockEvent);
			org.mockito.Mockito.verify(mockZK, org.mockito.Mockito.times(0)).exists(org.mockito.Mockito
				.anyString(), org.mockito.Mockito.anyBoolean(), org.mockito.Mockito.anyObject<org.apache.zookeeper.AsyncCallback.StatCallback
				>(), org.mockito.Mockito.anyObject<object>());
			// disconnection should enter safe mode
			org.mockito.Mockito.when(mockEvent.getState()).thenReturn(org.apache.zookeeper.Watcher.Event.KeeperState
				.Disconnected);
			elector.processWatchEvent(mockZK, mockEvent);
			org.mockito.Mockito.verify(mockApp, org.mockito.Mockito.times(1)).enterNeutralMode
				();
			// re-connection should monitor master status
			org.mockito.Mockito.when(mockEvent.getState()).thenReturn(org.apache.zookeeper.Watcher.Event.KeeperState
				.SyncConnected);
			elector.processWatchEvent(mockZK, mockEvent);
			verifyExistCall(1);
			NUnit.Framework.Assert.IsTrue(elector.isMonitorLockNodePending());
			elector.processResult(org.apache.zookeeper.KeeperException.Code.SESSIONEXPIRED.intValue
				(), ZK_LOCK_NAME, mockZK, new org.apache.zookeeper.data.Stat());
			NUnit.Framework.Assert.IsFalse(elector.isMonitorLockNodePending());
			// session expired should enter safe mode and initiate re-election
			// re-election checked via checking re-creation of new zookeeper and
			// call to create lock znode
			org.mockito.Mockito.when(mockEvent.getState()).thenReturn(org.apache.zookeeper.Watcher.Event.KeeperState
				.Expired);
			elector.processWatchEvent(mockZK, mockEvent);
			// already in safe mode above. should not enter safe mode again
			org.mockito.Mockito.verify(mockApp, org.mockito.Mockito.times(1)).enterNeutralMode
				();
			// called getNewZooKeeper to create new session. first call was in
			// constructor
			NUnit.Framework.Assert.AreEqual(2, count);
			// once in initial joinElection and one now
			org.mockito.Mockito.verify(mockZK, org.mockito.Mockito.times(2)).create(ZK_LOCK_NAME
				, data, org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE, org.apache.zookeeper.CreateMode
				.EPHEMERAL, elector, mockZK);
			// create znode success. become master and monitor
			elector.processResult(org.apache.zookeeper.KeeperException.Code.OK.intValue(), ZK_LOCK_NAME
				, mockZK, ZK_LOCK_NAME);
			org.mockito.Mockito.verify(mockApp, org.mockito.Mockito.times(1)).becomeActive();
			verifyExistCall(2);
			// error event results in fatal error
			org.mockito.Mockito.when(mockEvent.getState()).thenReturn(org.apache.zookeeper.Watcher.Event.KeeperState
				.AuthFailed);
			elector.processWatchEvent(mockZK, mockEvent);
			org.mockito.Mockito.verify(mockApp, org.mockito.Mockito.times(1)).notifyFatalError
				("Unexpected Zookeeper watch event state: AuthFailed");
			// only 1 state change callback is called at a time
			org.mockito.Mockito.verify(mockApp, org.mockito.Mockito.times(1)).enterNeutralMode
				();
		}

		/// <summary>verify behavior of watcher.process with node event</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testProcessCallbackEventNode()
		{
			mockNoPriorActive();
			elector.joinElection(data);
			// make the object go into the monitoring state
			elector.processResult(org.apache.zookeeper.KeeperException.Code.NODEEXISTS.intValue
				(), ZK_LOCK_NAME, mockZK, ZK_LOCK_NAME);
			org.mockito.Mockito.verify(mockApp, org.mockito.Mockito.times(1)).becomeStandby();
			verifyExistCall(1);
			NUnit.Framework.Assert.IsTrue(elector.isMonitorLockNodePending());
			org.apache.zookeeper.data.Stat stat = new org.apache.zookeeper.data.Stat();
			stat.setEphemeralOwner(0L);
			org.mockito.Mockito.when(mockZK.getSessionId()).thenReturn(1L);
			elector.processResult(org.apache.zookeeper.KeeperException.Code.OK.intValue(), ZK_LOCK_NAME
				, mockZK, stat);
			NUnit.Framework.Assert.IsFalse(elector.isMonitorLockNodePending());
			org.apache.zookeeper.WatchedEvent mockEvent = org.mockito.Mockito.mock<org.apache.zookeeper.WatchedEvent
				>();
			org.mockito.Mockito.when(mockEvent.getPath()).thenReturn(ZK_LOCK_NAME);
			// monitoring should be setup again after event is received
			org.mockito.Mockito.when(mockEvent.getType()).thenReturn(org.apache.zookeeper.Watcher.Event.EventType
				.NodeDataChanged);
			elector.processWatchEvent(mockZK, mockEvent);
			verifyExistCall(2);
			NUnit.Framework.Assert.IsTrue(elector.isMonitorLockNodePending());
			elector.processResult(org.apache.zookeeper.KeeperException.Code.OK.intValue(), ZK_LOCK_NAME
				, mockZK, stat);
			NUnit.Framework.Assert.IsFalse(elector.isMonitorLockNodePending());
			// monitoring should be setup again after event is received
			org.mockito.Mockito.when(mockEvent.getType()).thenReturn(org.apache.zookeeper.Watcher.Event.EventType
				.NodeChildrenChanged);
			elector.processWatchEvent(mockZK, mockEvent);
			verifyExistCall(3);
			NUnit.Framework.Assert.IsTrue(elector.isMonitorLockNodePending());
			elector.processResult(org.apache.zookeeper.KeeperException.Code.OK.intValue(), ZK_LOCK_NAME
				, mockZK, stat);
			NUnit.Framework.Assert.IsFalse(elector.isMonitorLockNodePending());
			// lock node deletion when in standby mode should create znode again
			// successful znode creation enters active state and sets monitor
			org.mockito.Mockito.when(mockEvent.getType()).thenReturn(org.apache.zookeeper.Watcher.Event.EventType
				.NodeDeleted);
			elector.processWatchEvent(mockZK, mockEvent);
			// enterNeutralMode not called when app is standby and leader is lost
			org.mockito.Mockito.verify(mockApp, org.mockito.Mockito.times(0)).enterNeutralMode
				();
			// once in initial joinElection() and one now
			org.mockito.Mockito.verify(mockZK, org.mockito.Mockito.times(2)).create(ZK_LOCK_NAME
				, data, org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE, org.apache.zookeeper.CreateMode
				.EPHEMERAL, elector, mockZK);
			elector.processResult(org.apache.zookeeper.KeeperException.Code.OK.intValue(), ZK_LOCK_NAME
				, mockZK, ZK_LOCK_NAME);
			org.mockito.Mockito.verify(mockApp, org.mockito.Mockito.times(1)).becomeActive();
			verifyExistCall(4);
			NUnit.Framework.Assert.IsTrue(elector.isMonitorLockNodePending());
			stat.setEphemeralOwner(1L);
			elector.processResult(org.apache.zookeeper.KeeperException.Code.OK.intValue(), ZK_LOCK_NAME
				, mockZK, stat);
			NUnit.Framework.Assert.IsFalse(elector.isMonitorLockNodePending());
			// lock node deletion in active mode should enter neutral mode and create
			// znode again successful znode creation enters active state and sets
			// monitor
			org.mockito.Mockito.when(mockEvent.getType()).thenReturn(org.apache.zookeeper.Watcher.Event.EventType
				.NodeDeleted);
			elector.processWatchEvent(mockZK, mockEvent);
			org.mockito.Mockito.verify(mockApp, org.mockito.Mockito.times(1)).enterNeutralMode
				();
			// another joinElection called
			org.mockito.Mockito.verify(mockZK, org.mockito.Mockito.times(3)).create(ZK_LOCK_NAME
				, data, org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE, org.apache.zookeeper.CreateMode
				.EPHEMERAL, elector, mockZK);
			elector.processResult(org.apache.zookeeper.KeeperException.Code.OK.intValue(), ZK_LOCK_NAME
				, mockZK, ZK_LOCK_NAME);
			org.mockito.Mockito.verify(mockApp, org.mockito.Mockito.times(2)).becomeActive();
			verifyExistCall(5);
			NUnit.Framework.Assert.IsTrue(elector.isMonitorLockNodePending());
			elector.processResult(org.apache.zookeeper.KeeperException.Code.OK.intValue(), ZK_LOCK_NAME
				, mockZK, stat);
			NUnit.Framework.Assert.IsFalse(elector.isMonitorLockNodePending());
			// bad path name results in fatal error
			org.mockito.Mockito.when(mockEvent.getPath()).thenReturn(null);
			elector.processWatchEvent(mockZK, mockEvent);
			org.mockito.Mockito.verify(mockApp, org.mockito.Mockito.times(1)).notifyFatalError
				("Unexpected watch error from Zookeeper");
			// fatal error means no new connection other than one from constructor
			NUnit.Framework.Assert.AreEqual(1, count);
			// no new watches after fatal error
			verifyExistCall(5);
		}

		private void verifyExistCall(int times)
		{
			org.mockito.Mockito.verify(mockZK, org.mockito.Mockito.times(times)).exists(org.mockito.Mockito
				.eq(ZK_LOCK_NAME), org.mockito.Mockito.any<org.apache.zookeeper.Watcher>(), org.mockito.Mockito
				.same(elector), org.mockito.Mockito.same(mockZK));
		}

		/// <summary>verify becomeStandby is not called if already in standby</summary>
		[NUnit.Framework.Test]
		public virtual void testSuccessiveStandbyCalls()
		{
			elector.joinElection(data);
			// make the object go into the monitoring standby state
			elector.processResult(org.apache.zookeeper.KeeperException.Code.NODEEXISTS.intValue
				(), ZK_LOCK_NAME, mockZK, ZK_LOCK_NAME);
			org.mockito.Mockito.verify(mockApp, org.mockito.Mockito.times(1)).becomeStandby();
			verifyExistCall(1);
			NUnit.Framework.Assert.IsTrue(elector.isMonitorLockNodePending());
			org.apache.zookeeper.data.Stat stat = new org.apache.zookeeper.data.Stat();
			stat.setEphemeralOwner(0L);
			org.mockito.Mockito.when(mockZK.getSessionId()).thenReturn(1L);
			elector.processResult(org.apache.zookeeper.KeeperException.Code.OK.intValue(), ZK_LOCK_NAME
				, mockZK, stat);
			NUnit.Framework.Assert.IsFalse(elector.isMonitorLockNodePending());
			org.apache.zookeeper.WatchedEvent mockEvent = org.mockito.Mockito.mock<org.apache.zookeeper.WatchedEvent
				>();
			org.mockito.Mockito.when(mockEvent.getPath()).thenReturn(ZK_LOCK_NAME);
			// notify node deletion
			// monitoring should be setup again after event is received
			org.mockito.Mockito.when(mockEvent.getType()).thenReturn(org.apache.zookeeper.Watcher.Event.EventType
				.NodeDeleted);
			elector.processWatchEvent(mockZK, mockEvent);
			// is standby. no need to notify anything now
			org.mockito.Mockito.verify(mockApp, org.mockito.Mockito.times(0)).enterNeutralMode
				();
			// another joinElection called.
			org.mockito.Mockito.verify(mockZK, org.mockito.Mockito.times(2)).create(ZK_LOCK_NAME
				, data, org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE, org.apache.zookeeper.CreateMode
				.EPHEMERAL, elector, mockZK);
			// lost election
			elector.processResult(org.apache.zookeeper.KeeperException.Code.NODEEXISTS.intValue
				(), ZK_LOCK_NAME, mockZK, ZK_LOCK_NAME);
			// still standby. so no need to notify again
			org.mockito.Mockito.verify(mockApp, org.mockito.Mockito.times(1)).becomeStandby();
			// monitor is set again
			verifyExistCall(2);
		}

		/// <summary>verify quit election terminates connection and there are no new watches.
		/// 	</summary>
		/// <remarks>
		/// verify quit election terminates connection and there are no new watches.
		/// next call to joinElection creates new connection and performs election
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testQuitElection()
		{
			elector.joinElection(data);
			org.mockito.Mockito.verify(mockZK, org.mockito.Mockito.times(0)).close();
			elector.quitElection(true);
			org.mockito.Mockito.verify(mockZK, org.mockito.Mockito.times(1)).close();
			// no watches added
			verifyExistCall(0);
			byte[] data = new byte[8];
			elector.joinElection(data);
			// getNewZooKeeper called 2 times. once in constructor and once now
			NUnit.Framework.Assert.AreEqual(2, count);
			elector.processResult(org.apache.zookeeper.KeeperException.Code.NODEEXISTS.intValue
				(), ZK_LOCK_NAME, mockZK, ZK_LOCK_NAME);
			org.mockito.Mockito.verify(mockApp, org.mockito.Mockito.times(1)).becomeStandby();
			verifyExistCall(1);
		}

		/// <summary>
		/// verify that receiveActiveData gives data when active exists, tells that
		/// active does not exist and reports error in getting active information
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="org.apache.zookeeper.KeeperException"/>
		/// <exception cref="ActiveNotFoundException"/>
		/// <exception cref="org.apache.hadoop.ha.ActiveStandbyElector.ActiveNotFoundException
		/// 	"/>
		[NUnit.Framework.Test]
		public virtual void testGetActiveData()
		{
			// get valid active data
			byte[] data = new byte[8];
			org.mockito.Mockito.when(mockZK.getData(org.mockito.Mockito.eq(ZK_LOCK_NAME), org.mockito.Mockito
				.eq(false), org.mockito.Mockito.anyObject<org.apache.zookeeper.data.Stat>())).thenReturn
				(data);
			NUnit.Framework.Assert.AreEqual(data, elector.getActiveData());
			org.mockito.Mockito.verify(mockZK, org.mockito.Mockito.times(1)).getData(org.mockito.Mockito
				.eq(ZK_LOCK_NAME), org.mockito.Mockito.eq(false), org.mockito.Mockito.anyObject<
				org.apache.zookeeper.data.Stat>());
			// active does not exist
			org.mockito.Mockito.when(mockZK.getData(org.mockito.Mockito.eq(ZK_LOCK_NAME), org.mockito.Mockito
				.eq(false), org.mockito.Mockito.anyObject<org.apache.zookeeper.data.Stat>())).thenThrow
				(new org.apache.zookeeper.KeeperException.NoNodeException());
			try
			{
				elector.getActiveData();
				NUnit.Framework.Assert.Fail("ActiveNotFoundException expected");
			}
			catch (org.apache.hadoop.ha.ActiveStandbyElector.ActiveNotFoundException)
			{
				org.mockito.Mockito.verify(mockZK, org.mockito.Mockito.times(2)).getData(org.mockito.Mockito
					.eq(ZK_LOCK_NAME), org.mockito.Mockito.eq(false), org.mockito.Mockito.anyObject<
					org.apache.zookeeper.data.Stat>());
			}
			// error getting active data rethrows keeperexception
			try
			{
				org.mockito.Mockito.when(mockZK.getData(org.mockito.Mockito.eq(ZK_LOCK_NAME), org.mockito.Mockito
					.eq(false), org.mockito.Mockito.anyObject<org.apache.zookeeper.data.Stat>())).thenThrow
					(new org.apache.zookeeper.KeeperException.AuthFailedException());
				elector.getActiveData();
				NUnit.Framework.Assert.Fail("KeeperException.AuthFailedException expected");
			}
			catch (org.apache.zookeeper.KeeperException.AuthFailedException)
			{
				org.mockito.Mockito.verify(mockZK, org.mockito.Mockito.times(3)).getData(org.mockito.Mockito
					.eq(ZK_LOCK_NAME), org.mockito.Mockito.eq(false), org.mockito.Mockito.anyObject<
					org.apache.zookeeper.data.Stat>());
			}
		}

		/// <summary>Test that ensureBaseNode() recursively creates the specified dir</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testEnsureBaseNode()
		{
			elector.ensureParentZNode();
			java.lang.StringBuilder prefix = new java.lang.StringBuilder();
			foreach (string part in ZK_PARENT_NAME.split("/"))
			{
				if (part.isEmpty())
				{
					continue;
				}
				prefix.Append("/").Append(part);
				if (!"/".Equals(prefix.ToString()))
				{
					org.mockito.Mockito.verify(mockZK).create(org.mockito.Mockito.eq(prefix.ToString(
						)), org.mockito.Mockito.any<byte[]>(), org.mockito.Mockito.eq(org.apache.zookeeper.ZooDefs.Ids
						.OPEN_ACL_UNSAFE), org.mockito.Mockito.eq(org.apache.zookeeper.CreateMode.PERSISTENT
						));
				}
			}
		}

		/// <summary>
		/// Test for a bug encountered during development of HADOOP-8163:
		/// ensureBaseNode() should throw an exception if it has to retry
		/// more than 3 times to create any part of the path.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testEnsureBaseNodeFails()
		{
			org.mockito.Mockito.doThrow(new org.apache.zookeeper.KeeperException.ConnectionLossException
				()).when(mockZK).create(org.mockito.Mockito.eq(ZK_PARENT_NAME), org.mockito.Mockito
				.any<byte[]>(), org.mockito.Mockito.eq(org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE
				), org.mockito.Mockito.eq(org.apache.zookeeper.CreateMode.PERSISTENT));
			try
			{
				elector.ensureParentZNode();
				NUnit.Framework.Assert.Fail("Did not throw!");
			}
			catch (System.IO.IOException ioe)
			{
				if (!(ioe.InnerException is org.apache.zookeeper.KeeperException.ConnectionLossException
					))
				{
					throw;
				}
			}
			// Should have tried three times
			org.mockito.Mockito.verify(mockZK, org.mockito.Mockito.times(3)).create(org.mockito.Mockito
				.eq(ZK_PARENT_NAME), org.mockito.Mockito.any<byte[]>(), org.mockito.Mockito.eq(org.apache.zookeeper.ZooDefs.Ids
				.OPEN_ACL_UNSAFE), org.mockito.Mockito.eq(org.apache.zookeeper.CreateMode.PERSISTENT
				));
		}

		/// <summary>verify the zookeeper connection establishment</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testWithoutZKServer()
		{
			try
			{
				new org.apache.hadoop.ha.ActiveStandbyElector("127.0.0.1", 2000, ZK_PARENT_NAME, 
					org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE, java.util.Collections.emptyList
					<org.apache.hadoop.util.ZKUtil.ZKAuthInfo>(), mockApp, org.apache.hadoop.fs.CommonConfigurationKeys
					.HA_FC_ELECTOR_ZK_OP_RETRIES_DEFAULT);
				NUnit.Framework.Assert.Fail("Did not throw zookeeper connection loss exceptions!"
					);
			}
			catch (org.apache.zookeeper.KeeperException ke)
			{
				org.apache.hadoop.test.GenericTestUtils.assertExceptionContains("ConnectionLoss", 
					ke);
			}
		}

		/// <summary>joinElection(..) should happen only after SERVICE_HEALTHY.</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testBecomeActiveBeforeServiceHealthy()
		{
			mockNoPriorActive();
			org.apache.zookeeper.WatchedEvent mockEvent = org.mockito.Mockito.mock<org.apache.zookeeper.WatchedEvent
				>();
			org.mockito.Mockito.when(mockEvent.getType()).thenReturn(org.apache.zookeeper.Watcher.Event.EventType
				.None);
			// session expired should enter safe mode
			// But for first time, before the SERVICE_HEALTY i.e. appData is set,
			// should not enter the election.
			org.mockito.Mockito.when(mockEvent.getState()).thenReturn(org.apache.zookeeper.Watcher.Event.KeeperState
				.Expired);
			elector.processWatchEvent(mockZK, mockEvent);
			// joinElection should not be called.
			org.mockito.Mockito.verify(mockZK, org.mockito.Mockito.times(0)).create(ZK_LOCK_NAME
				, null, org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE, org.apache.zookeeper.CreateMode
				.EPHEMERAL, elector, mockZK);
		}
	}
}
