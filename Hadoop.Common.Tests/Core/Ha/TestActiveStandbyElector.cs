using System.Collections.Generic;
using System.IO;
using System.Text;
using NUnit.Framework;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Test;
using Org.Apache.Hadoop.Util;
using Org.Apache.Zookeeper;
using Org.Apache.Zookeeper.Data;
using Sharpen;

namespace Org.Apache.Hadoop.HA
{
	public class TestActiveStandbyElector
	{
		private ZooKeeper mockZK;

		private int count;

		private ActiveStandbyElector.ActiveStandbyElectorCallback mockApp;

		private readonly byte[] data = new byte[8];

		private TestActiveStandbyElector.ActiveStandbyElectorTester elector;

		internal class ActiveStandbyElectorTester : ActiveStandbyElector
		{
			private int sleptFor = 0;

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Org.Apache.Zookeeper.KeeperException"/>
			internal ActiveStandbyElectorTester(TestActiveStandbyElector _enclosing, string hostPort
				, int timeout, string parent, IList<ACL> acl, ActiveStandbyElector.ActiveStandbyElectorCallback
				 app)
				: base(hostPort, timeout, parent, acl, Sharpen.Collections.EmptyList<ZKUtil.ZKAuthInfo
					>(), app, CommonConfigurationKeys.HaFcElectorZkOpRetriesDefault)
			{
				this._enclosing = _enclosing;
			}

			protected internal override ZooKeeper GetNewZooKeeper()
			{
				++this._enclosing.count;
				return this._enclosing.mockZK;
			}

			protected internal override void SleepFor(int ms)
			{
				// don't sleep in unit tests! Instead, just record the amount of
				// time slept
				ActiveStandbyElector.Log.Info("Would have slept for " + ms + "ms");
				this.sleptFor += ms;
			}

			private readonly TestActiveStandbyElector _enclosing;
		}

		private const string ZkParentName = "/parent/node";

		private const string ZkLockName = ZkParentName + "/" + ActiveStandbyElector.LockFilename;

		private const string ZkBreadcrumbName = ZkParentName + "/" + ActiveStandbyElector
			.BreadcrumbFilename;

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Zookeeper.KeeperException"/>
		[SetUp]
		public virtual void Init()
		{
			count = 0;
			mockZK = Org.Mockito.Mockito.Mock<ZooKeeper>();
			mockApp = Org.Mockito.Mockito.Mock<ActiveStandbyElector.ActiveStandbyElectorCallback
				>();
			elector = new TestActiveStandbyElector.ActiveStandbyElectorTester(this, "hostPort"
				, 1000, ZkParentName, ZooDefs.Ids.OpenAclUnsafe, mockApp);
		}

		/// <summary>Set up the mock ZK to return no info for a prior active in ZK.</summary>
		/// <exception cref="System.Exception"/>
		private void MockNoPriorActive()
		{
			Org.Mockito.Mockito.DoThrow(new KeeperException.NoNodeException()).When(mockZK).GetData
				(Org.Mockito.Mockito.Eq(ZkBreadcrumbName), Org.Mockito.Mockito.AnyBoolean(), Org.Mockito.Mockito
				.Any<Stat>());
		}

		/// <summary>Set up the mock to return info for some prior active node in ZK./</summary>
		/// <exception cref="System.Exception"/>
		private void MockPriorActive(byte[] data)
		{
			Org.Mockito.Mockito.DoReturn(data).When(mockZK).GetData(Org.Mockito.Mockito.Eq(ZkBreadcrumbName
				), Org.Mockito.Mockito.AnyBoolean(), Org.Mockito.Mockito.Any<Stat>());
		}

		/// <summary>verify that joinElection checks for null data</summary>
		public virtual void TestJoinElectionException()
		{
			elector.JoinElection(null);
		}

		/// <summary>verify that joinElection tries to create ephemeral lock znode</summary>
		[NUnit.Framework.Test]
		public virtual void TestJoinElection()
		{
			elector.JoinElection(data);
			Org.Mockito.Mockito.Verify(mockZK, Org.Mockito.Mockito.Times(1)).Create(ZkLockName
				, data, ZooDefs.Ids.OpenAclUnsafe, CreateMode.Ephemeral, elector, mockZK);
		}

		/// <summary>
		/// verify that successful znode create result becomes active and monitoring is
		/// started
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCreateNodeResultBecomeActive()
		{
			MockNoPriorActive();
			elector.JoinElection(data);
			elector.ProcessResult(KeeperException.Code.Ok.IntValue(), ZkLockName, mockZK, ZkLockName
				);
			Org.Mockito.Mockito.Verify(mockApp, Org.Mockito.Mockito.Times(1)).BecomeActive();
			VerifyExistCall(1);
			// monitor callback verifies the leader is ephemeral owner of lock but does
			// not call becomeActive since its already active
			Stat stat = new Stat();
			stat.SetEphemeralOwner(1L);
			Org.Mockito.Mockito.When(mockZK.GetSessionId()).ThenReturn(1L);
			elector.ProcessResult(KeeperException.Code.Ok.IntValue(), ZkLockName, mockZK, stat
				);
			// should not call neutral mode/standby/active
			Org.Mockito.Mockito.Verify(mockApp, Org.Mockito.Mockito.Times(0)).EnterNeutralMode
				();
			Org.Mockito.Mockito.Verify(mockApp, Org.Mockito.Mockito.Times(0)).BecomeStandby();
			Org.Mockito.Mockito.Verify(mockApp, Org.Mockito.Mockito.Times(1)).BecomeActive();
			// another joinElection not called.
			Org.Mockito.Mockito.Verify(mockZK, Org.Mockito.Mockito.Times(1)).Create(ZkLockName
				, data, ZooDefs.Ids.OpenAclUnsafe, CreateMode.Ephemeral, elector, mockZK);
			// no new monitor called
			VerifyExistCall(1);
		}

		/// <summary>
		/// Verify that, when the callback fails to enter active state,
		/// the elector rejoins the election after sleeping for a short period.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFailToBecomeActive()
		{
			MockNoPriorActive();
			elector.JoinElection(data);
			NUnit.Framework.Assert.AreEqual(0, elector.sleptFor);
			Org.Mockito.Mockito.DoThrow(new ServiceFailedException("failed to become active")
				).When(mockApp).BecomeActive();
			elector.ProcessResult(KeeperException.Code.Ok.IntValue(), ZkLockName, mockZK, ZkLockName
				);
			// Should have tried to become active
			Org.Mockito.Mockito.Verify(mockApp).BecomeActive();
			// should re-join
			Org.Mockito.Mockito.Verify(mockZK, Org.Mockito.Mockito.Times(2)).Create(ZkLockName
				, data, ZooDefs.Ids.OpenAclUnsafe, CreateMode.Ephemeral, elector, mockZK);
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
		public virtual void TestFailToBecomeActiveAfterZKDisconnect()
		{
			MockNoPriorActive();
			elector.JoinElection(data);
			NUnit.Framework.Assert.AreEqual(0, elector.sleptFor);
			elector.ProcessResult(KeeperException.Code.Connectionloss.IntValue(), ZkLockName, 
				mockZK, ZkLockName);
			Org.Mockito.Mockito.Verify(mockZK, Org.Mockito.Mockito.Times(2)).Create(ZkLockName
				, data, ZooDefs.Ids.OpenAclUnsafe, CreateMode.Ephemeral, elector, mockZK);
			elector.ProcessResult(KeeperException.Code.Nodeexists.IntValue(), ZkLockName, mockZK
				, ZkLockName);
			VerifyExistCall(1);
			Stat stat = new Stat();
			stat.SetEphemeralOwner(1L);
			Org.Mockito.Mockito.When(mockZK.GetSessionId()).ThenReturn(1L);
			// Fake failure to become active from within the stat callback
			Org.Mockito.Mockito.DoThrow(new ServiceFailedException("fail to become active")).
				When(mockApp).BecomeActive();
			elector.ProcessResult(KeeperException.Code.Ok.IntValue(), ZkLockName, mockZK, stat
				);
			Org.Mockito.Mockito.Verify(mockApp, Org.Mockito.Mockito.Times(1)).BecomeActive();
			// should re-join
			Org.Mockito.Mockito.Verify(mockZK, Org.Mockito.Mockito.Times(3)).Create(ZkLockName
				, data, ZooDefs.Ids.OpenAclUnsafe, CreateMode.Ephemeral, elector, mockZK);
			NUnit.Framework.Assert.AreEqual(2, count);
			NUnit.Framework.Assert.IsTrue(elector.sleptFor > 0);
		}

		/// <summary>
		/// Verify that, if there is a record of a prior active node, the
		/// elector asks the application to fence it before becoming active.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFencesOldActive()
		{
			byte[] fakeOldActiveData = new byte[0];
			MockPriorActive(fakeOldActiveData);
			elector.JoinElection(data);
			elector.ProcessResult(KeeperException.Code.Ok.IntValue(), ZkLockName, mockZK, ZkLockName
				);
			// Application fences active.
			Org.Mockito.Mockito.Verify(mockApp, Org.Mockito.Mockito.Times(1)).FenceOldActive(
				fakeOldActiveData);
			// Updates breadcrumb node to new data
			Org.Mockito.Mockito.Verify(mockZK, Org.Mockito.Mockito.Times(1)).SetData(Org.Mockito.Mockito
				.Eq(ZkBreadcrumbName), Org.Mockito.Mockito.Eq(data), Org.Mockito.Mockito.Eq(0));
			// Then it becomes active itself
			Org.Mockito.Mockito.Verify(mockApp, Org.Mockito.Mockito.Times(1)).BecomeActive();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestQuitElectionRemovesBreadcrumbNode()
		{
			MockNoPriorActive();
			elector.JoinElection(data);
			elector.ProcessResult(KeeperException.Code.Ok.IntValue(), ZkLockName, mockZK, ZkLockName
				);
			// Writes its own active info
			Org.Mockito.Mockito.Verify(mockZK, Org.Mockito.Mockito.Times(1)).Create(Org.Mockito.Mockito
				.Eq(ZkBreadcrumbName), Org.Mockito.Mockito.Eq(data), Org.Mockito.Mockito.Eq(ZooDefs.Ids
				.OpenAclUnsafe), Org.Mockito.Mockito.Eq(CreateMode.Persistent));
			MockPriorActive(data);
			elector.QuitElection(false);
			// Deletes its own active data
			Org.Mockito.Mockito.Verify(mockZK, Org.Mockito.Mockito.Times(1)).Delete(Org.Mockito.Mockito
				.Eq(ZkBreadcrumbName), Org.Mockito.Mockito.Eq(0));
		}

		/// <summary>
		/// verify that znode create for existing node and no retry becomes standby and
		/// monitoring is started
		/// </summary>
		[NUnit.Framework.Test]
		public virtual void TestCreateNodeResultBecomeStandby()
		{
			elector.JoinElection(data);
			elector.ProcessResult(KeeperException.Code.Nodeexists.IntValue(), ZkLockName, mockZK
				, ZkLockName);
			Org.Mockito.Mockito.Verify(mockApp, Org.Mockito.Mockito.Times(1)).BecomeStandby();
			VerifyExistCall(1);
		}

		/// <summary>verify that znode create error result in fatal error</summary>
		[NUnit.Framework.Test]
		public virtual void TestCreateNodeResultError()
		{
			elector.JoinElection(data);
			elector.ProcessResult(KeeperException.Code.Apierror.IntValue(), ZkLockName, mockZK
				, ZkLockName);
			Org.Mockito.Mockito.Verify(mockApp, Org.Mockito.Mockito.Times(1)).NotifyFatalError
				("Received create error from Zookeeper. code:APIERROR " + "for path " + ZkLockName
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
		public virtual void TestCreateNodeResultRetryBecomeActive()
		{
			MockNoPriorActive();
			elector.JoinElection(data);
			elector.ProcessResult(KeeperException.Code.Connectionloss.IntValue(), ZkLockName, 
				mockZK, ZkLockName);
			elector.ProcessResult(KeeperException.Code.Connectionloss.IntValue(), ZkLockName, 
				mockZK, ZkLockName);
			elector.ProcessResult(KeeperException.Code.Connectionloss.IntValue(), ZkLockName, 
				mockZK, ZkLockName);
			elector.ProcessResult(KeeperException.Code.Connectionloss.IntValue(), ZkLockName, 
				mockZK, ZkLockName);
			// 4 errors results in fatalError
			Org.Mockito.Mockito.Verify(mockApp, Org.Mockito.Mockito.Times(1)).NotifyFatalError
				("Received create error from Zookeeper. code:CONNECTIONLOSS " + "for path " + ZkLockName
				 + ". " + "Not retrying further znode create connection errors.");
			elector.JoinElection(data);
			// recreate connection via getNewZooKeeper
			NUnit.Framework.Assert.AreEqual(2, count);
			elector.ProcessResult(KeeperException.Code.Connectionloss.IntValue(), ZkLockName, 
				mockZK, ZkLockName);
			elector.ProcessResult(KeeperException.Code.Nodeexists.IntValue(), ZkLockName, mockZK
				, ZkLockName);
			VerifyExistCall(1);
			Stat stat = new Stat();
			stat.SetEphemeralOwner(1L);
			Org.Mockito.Mockito.When(mockZK.GetSessionId()).ThenReturn(1L);
			elector.ProcessResult(KeeperException.Code.Ok.IntValue(), ZkLockName, mockZK, stat
				);
			Org.Mockito.Mockito.Verify(mockApp, Org.Mockito.Mockito.Times(1)).BecomeActive();
			VerifyExistCall(1);
			Org.Mockito.Mockito.Verify(mockZK, Org.Mockito.Mockito.Times(6)).Create(ZkLockName
				, data, ZooDefs.Ids.OpenAclUnsafe, CreateMode.Ephemeral, elector, mockZK);
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
		public virtual void TestCreateNodeResultRetryBecomeStandby()
		{
			elector.JoinElection(data);
			elector.ProcessResult(KeeperException.Code.Connectionloss.IntValue(), ZkLockName, 
				mockZK, ZkLockName);
			elector.ProcessResult(KeeperException.Code.Nodeexists.IntValue(), ZkLockName, mockZK
				, ZkLockName);
			VerifyExistCall(1);
			Stat stat = new Stat();
			stat.SetEphemeralOwner(0);
			Org.Mockito.Mockito.When(mockZK.GetSessionId()).ThenReturn(1L);
			elector.ProcessResult(KeeperException.Code.Ok.IntValue(), ZkLockName, mockZK, stat
				);
			Org.Mockito.Mockito.Verify(mockApp, Org.Mockito.Mockito.Times(1)).BecomeStandby();
			VerifyExistCall(1);
		}

		/// <summary>
		/// verify that if create znode results in nodeexists and that znode is deleted
		/// before exists() watch is set then the return of the exists() method results
		/// in attempt to re-create the znode and become active
		/// </summary>
		[NUnit.Framework.Test]
		public virtual void TestCreateNodeResultRetryNoNode()
		{
			elector.JoinElection(data);
			elector.ProcessResult(KeeperException.Code.Connectionloss.IntValue(), ZkLockName, 
				mockZK, ZkLockName);
			elector.ProcessResult(KeeperException.Code.Connectionloss.IntValue(), ZkLockName, 
				mockZK, ZkLockName);
			elector.ProcessResult(KeeperException.Code.Nodeexists.IntValue(), ZkLockName, mockZK
				, ZkLockName);
			VerifyExistCall(1);
			elector.ProcessResult(KeeperException.Code.Nonode.IntValue(), ZkLockName, mockZK, 
				(Stat)null);
			Org.Mockito.Mockito.Verify(mockApp, Org.Mockito.Mockito.Times(1)).EnterNeutralMode
				();
			Org.Mockito.Mockito.Verify(mockZK, Org.Mockito.Mockito.Times(4)).Create(ZkLockName
				, data, ZooDefs.Ids.OpenAclUnsafe, CreateMode.Ephemeral, elector, mockZK);
		}

		/// <summary>verify that more than 3 network error retries result fatalError</summary>
		[NUnit.Framework.Test]
		public virtual void TestStatNodeRetry()
		{
			elector.JoinElection(data);
			elector.ProcessResult(KeeperException.Code.Connectionloss.IntValue(), ZkLockName, 
				mockZK, (Stat)null);
			elector.ProcessResult(KeeperException.Code.Connectionloss.IntValue(), ZkLockName, 
				mockZK, (Stat)null);
			elector.ProcessResult(KeeperException.Code.Connectionloss.IntValue(), ZkLockName, 
				mockZK, (Stat)null);
			elector.ProcessResult(KeeperException.Code.Connectionloss.IntValue(), ZkLockName, 
				mockZK, (Stat)null);
			Org.Mockito.Mockito.Verify(mockApp, Org.Mockito.Mockito.Times(1)).NotifyFatalError
				("Received stat error from Zookeeper. code:CONNECTIONLOSS. " + "Not retrying further znode monitoring connection errors."
				);
		}

		/// <summary>verify error in exists() callback results in fatal error</summary>
		[NUnit.Framework.Test]
		public virtual void TestStatNodeError()
		{
			elector.JoinElection(data);
			elector.ProcessResult(KeeperException.Code.Runtimeinconsistency.IntValue(), ZkLockName
				, mockZK, (Stat)null);
			Org.Mockito.Mockito.Verify(mockApp, Org.Mockito.Mockito.Times(0)).EnterNeutralMode
				();
			Org.Mockito.Mockito.Verify(mockApp, Org.Mockito.Mockito.Times(1)).NotifyFatalError
				("Received stat error from Zookeeper. code:RUNTIMEINCONSISTENCY");
		}

		/// <summary>verify behavior of watcher.process callback with non-node event</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestProcessCallbackEventNone()
		{
			MockNoPriorActive();
			elector.JoinElection(data);
			WatchedEvent mockEvent = Org.Mockito.Mockito.Mock<WatchedEvent>();
			Org.Mockito.Mockito.When(mockEvent.GetType()).ThenReturn(Watcher.Event.EventType.
				None);
			// first SyncConnected should not do anything
			Org.Mockito.Mockito.When(mockEvent.GetState()).ThenReturn(Watcher.Event.KeeperState
				.SyncConnected);
			elector.ProcessWatchEvent(mockZK, mockEvent);
			Org.Mockito.Mockito.Verify(mockZK, Org.Mockito.Mockito.Times(0)).Exists(Org.Mockito.Mockito
				.AnyString(), Org.Mockito.Mockito.AnyBoolean(), Org.Mockito.Mockito.AnyObject<AsyncCallback.StatCallback
				>(), Org.Mockito.Mockito.AnyObject<object>());
			// disconnection should enter safe mode
			Org.Mockito.Mockito.When(mockEvent.GetState()).ThenReturn(Watcher.Event.KeeperState
				.Disconnected);
			elector.ProcessWatchEvent(mockZK, mockEvent);
			Org.Mockito.Mockito.Verify(mockApp, Org.Mockito.Mockito.Times(1)).EnterNeutralMode
				();
			// re-connection should monitor master status
			Org.Mockito.Mockito.When(mockEvent.GetState()).ThenReturn(Watcher.Event.KeeperState
				.SyncConnected);
			elector.ProcessWatchEvent(mockZK, mockEvent);
			VerifyExistCall(1);
			NUnit.Framework.Assert.IsTrue(elector.IsMonitorLockNodePending());
			elector.ProcessResult(KeeperException.Code.Sessionexpired.IntValue(), ZkLockName, 
				mockZK, new Stat());
			NUnit.Framework.Assert.IsFalse(elector.IsMonitorLockNodePending());
			// session expired should enter safe mode and initiate re-election
			// re-election checked via checking re-creation of new zookeeper and
			// call to create lock znode
			Org.Mockito.Mockito.When(mockEvent.GetState()).ThenReturn(Watcher.Event.KeeperState
				.Expired);
			elector.ProcessWatchEvent(mockZK, mockEvent);
			// already in safe mode above. should not enter safe mode again
			Org.Mockito.Mockito.Verify(mockApp, Org.Mockito.Mockito.Times(1)).EnterNeutralMode
				();
			// called getNewZooKeeper to create new session. first call was in
			// constructor
			NUnit.Framework.Assert.AreEqual(2, count);
			// once in initial joinElection and one now
			Org.Mockito.Mockito.Verify(mockZK, Org.Mockito.Mockito.Times(2)).Create(ZkLockName
				, data, ZooDefs.Ids.OpenAclUnsafe, CreateMode.Ephemeral, elector, mockZK);
			// create znode success. become master and monitor
			elector.ProcessResult(KeeperException.Code.Ok.IntValue(), ZkLockName, mockZK, ZkLockName
				);
			Org.Mockito.Mockito.Verify(mockApp, Org.Mockito.Mockito.Times(1)).BecomeActive();
			VerifyExistCall(2);
			// error event results in fatal error
			Org.Mockito.Mockito.When(mockEvent.GetState()).ThenReturn(Watcher.Event.KeeperState
				.AuthFailed);
			elector.ProcessWatchEvent(mockZK, mockEvent);
			Org.Mockito.Mockito.Verify(mockApp, Org.Mockito.Mockito.Times(1)).NotifyFatalError
				("Unexpected Zookeeper watch event state: AuthFailed");
			// only 1 state change callback is called at a time
			Org.Mockito.Mockito.Verify(mockApp, Org.Mockito.Mockito.Times(1)).EnterNeutralMode
				();
		}

		/// <summary>verify behavior of watcher.process with node event</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestProcessCallbackEventNode()
		{
			MockNoPriorActive();
			elector.JoinElection(data);
			// make the object go into the monitoring state
			elector.ProcessResult(KeeperException.Code.Nodeexists.IntValue(), ZkLockName, mockZK
				, ZkLockName);
			Org.Mockito.Mockito.Verify(mockApp, Org.Mockito.Mockito.Times(1)).BecomeStandby();
			VerifyExistCall(1);
			NUnit.Framework.Assert.IsTrue(elector.IsMonitorLockNodePending());
			Stat stat = new Stat();
			stat.SetEphemeralOwner(0L);
			Org.Mockito.Mockito.When(mockZK.GetSessionId()).ThenReturn(1L);
			elector.ProcessResult(KeeperException.Code.Ok.IntValue(), ZkLockName, mockZK, stat
				);
			NUnit.Framework.Assert.IsFalse(elector.IsMonitorLockNodePending());
			WatchedEvent mockEvent = Org.Mockito.Mockito.Mock<WatchedEvent>();
			Org.Mockito.Mockito.When(mockEvent.GetPath()).ThenReturn(ZkLockName);
			// monitoring should be setup again after event is received
			Org.Mockito.Mockito.When(mockEvent.GetType()).ThenReturn(Watcher.Event.EventType.
				NodeDataChanged);
			elector.ProcessWatchEvent(mockZK, mockEvent);
			VerifyExistCall(2);
			NUnit.Framework.Assert.IsTrue(elector.IsMonitorLockNodePending());
			elector.ProcessResult(KeeperException.Code.Ok.IntValue(), ZkLockName, mockZK, stat
				);
			NUnit.Framework.Assert.IsFalse(elector.IsMonitorLockNodePending());
			// monitoring should be setup again after event is received
			Org.Mockito.Mockito.When(mockEvent.GetType()).ThenReturn(Watcher.Event.EventType.
				NodeChildrenChanged);
			elector.ProcessWatchEvent(mockZK, mockEvent);
			VerifyExistCall(3);
			NUnit.Framework.Assert.IsTrue(elector.IsMonitorLockNodePending());
			elector.ProcessResult(KeeperException.Code.Ok.IntValue(), ZkLockName, mockZK, stat
				);
			NUnit.Framework.Assert.IsFalse(elector.IsMonitorLockNodePending());
			// lock node deletion when in standby mode should create znode again
			// successful znode creation enters active state and sets monitor
			Org.Mockito.Mockito.When(mockEvent.GetType()).ThenReturn(Watcher.Event.EventType.
				NodeDeleted);
			elector.ProcessWatchEvent(mockZK, mockEvent);
			// enterNeutralMode not called when app is standby and leader is lost
			Org.Mockito.Mockito.Verify(mockApp, Org.Mockito.Mockito.Times(0)).EnterNeutralMode
				();
			// once in initial joinElection() and one now
			Org.Mockito.Mockito.Verify(mockZK, Org.Mockito.Mockito.Times(2)).Create(ZkLockName
				, data, ZooDefs.Ids.OpenAclUnsafe, CreateMode.Ephemeral, elector, mockZK);
			elector.ProcessResult(KeeperException.Code.Ok.IntValue(), ZkLockName, mockZK, ZkLockName
				);
			Org.Mockito.Mockito.Verify(mockApp, Org.Mockito.Mockito.Times(1)).BecomeActive();
			VerifyExistCall(4);
			NUnit.Framework.Assert.IsTrue(elector.IsMonitorLockNodePending());
			stat.SetEphemeralOwner(1L);
			elector.ProcessResult(KeeperException.Code.Ok.IntValue(), ZkLockName, mockZK, stat
				);
			NUnit.Framework.Assert.IsFalse(elector.IsMonitorLockNodePending());
			// lock node deletion in active mode should enter neutral mode and create
			// znode again successful znode creation enters active state and sets
			// monitor
			Org.Mockito.Mockito.When(mockEvent.GetType()).ThenReturn(Watcher.Event.EventType.
				NodeDeleted);
			elector.ProcessWatchEvent(mockZK, mockEvent);
			Org.Mockito.Mockito.Verify(mockApp, Org.Mockito.Mockito.Times(1)).EnterNeutralMode
				();
			// another joinElection called
			Org.Mockito.Mockito.Verify(mockZK, Org.Mockito.Mockito.Times(3)).Create(ZkLockName
				, data, ZooDefs.Ids.OpenAclUnsafe, CreateMode.Ephemeral, elector, mockZK);
			elector.ProcessResult(KeeperException.Code.Ok.IntValue(), ZkLockName, mockZK, ZkLockName
				);
			Org.Mockito.Mockito.Verify(mockApp, Org.Mockito.Mockito.Times(2)).BecomeActive();
			VerifyExistCall(5);
			NUnit.Framework.Assert.IsTrue(elector.IsMonitorLockNodePending());
			elector.ProcessResult(KeeperException.Code.Ok.IntValue(), ZkLockName, mockZK, stat
				);
			NUnit.Framework.Assert.IsFalse(elector.IsMonitorLockNodePending());
			// bad path name results in fatal error
			Org.Mockito.Mockito.When(mockEvent.GetPath()).ThenReturn(null);
			elector.ProcessWatchEvent(mockZK, mockEvent);
			Org.Mockito.Mockito.Verify(mockApp, Org.Mockito.Mockito.Times(1)).NotifyFatalError
				("Unexpected watch error from Zookeeper");
			// fatal error means no new connection other than one from constructor
			NUnit.Framework.Assert.AreEqual(1, count);
			// no new watches after fatal error
			VerifyExistCall(5);
		}

		private void VerifyExistCall(int times)
		{
			Org.Mockito.Mockito.Verify(mockZK, Org.Mockito.Mockito.Times(times)).Exists(Org.Mockito.Mockito
				.Eq(ZkLockName), Org.Mockito.Mockito.Any<Watcher>(), Org.Mockito.Mockito.Same(elector
				), Org.Mockito.Mockito.Same(mockZK));
		}

		/// <summary>verify becomeStandby is not called if already in standby</summary>
		[NUnit.Framework.Test]
		public virtual void TestSuccessiveStandbyCalls()
		{
			elector.JoinElection(data);
			// make the object go into the monitoring standby state
			elector.ProcessResult(KeeperException.Code.Nodeexists.IntValue(), ZkLockName, mockZK
				, ZkLockName);
			Org.Mockito.Mockito.Verify(mockApp, Org.Mockito.Mockito.Times(1)).BecomeStandby();
			VerifyExistCall(1);
			NUnit.Framework.Assert.IsTrue(elector.IsMonitorLockNodePending());
			Stat stat = new Stat();
			stat.SetEphemeralOwner(0L);
			Org.Mockito.Mockito.When(mockZK.GetSessionId()).ThenReturn(1L);
			elector.ProcessResult(KeeperException.Code.Ok.IntValue(), ZkLockName, mockZK, stat
				);
			NUnit.Framework.Assert.IsFalse(elector.IsMonitorLockNodePending());
			WatchedEvent mockEvent = Org.Mockito.Mockito.Mock<WatchedEvent>();
			Org.Mockito.Mockito.When(mockEvent.GetPath()).ThenReturn(ZkLockName);
			// notify node deletion
			// monitoring should be setup again after event is received
			Org.Mockito.Mockito.When(mockEvent.GetType()).ThenReturn(Watcher.Event.EventType.
				NodeDeleted);
			elector.ProcessWatchEvent(mockZK, mockEvent);
			// is standby. no need to notify anything now
			Org.Mockito.Mockito.Verify(mockApp, Org.Mockito.Mockito.Times(0)).EnterNeutralMode
				();
			// another joinElection called.
			Org.Mockito.Mockito.Verify(mockZK, Org.Mockito.Mockito.Times(2)).Create(ZkLockName
				, data, ZooDefs.Ids.OpenAclUnsafe, CreateMode.Ephemeral, elector, mockZK);
			// lost election
			elector.ProcessResult(KeeperException.Code.Nodeexists.IntValue(), ZkLockName, mockZK
				, ZkLockName);
			// still standby. so no need to notify again
			Org.Mockito.Mockito.Verify(mockApp, Org.Mockito.Mockito.Times(1)).BecomeStandby();
			// monitor is set again
			VerifyExistCall(2);
		}

		/// <summary>verify quit election terminates connection and there are no new watches.
		/// 	</summary>
		/// <remarks>
		/// verify quit election terminates connection and there are no new watches.
		/// next call to joinElection creates new connection and performs election
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestQuitElection()
		{
			elector.JoinElection(data);
			Org.Mockito.Mockito.Verify(mockZK, Org.Mockito.Mockito.Times(0)).Close();
			elector.QuitElection(true);
			Org.Mockito.Mockito.Verify(mockZK, Org.Mockito.Mockito.Times(1)).Close();
			// no watches added
			VerifyExistCall(0);
			byte[] data = new byte[8];
			elector.JoinElection(data);
			// getNewZooKeeper called 2 times. once in constructor and once now
			NUnit.Framework.Assert.AreEqual(2, count);
			elector.ProcessResult(KeeperException.Code.Nodeexists.IntValue(), ZkLockName, mockZK
				, ZkLockName);
			Org.Mockito.Mockito.Verify(mockApp, Org.Mockito.Mockito.Times(1)).BecomeStandby();
			VerifyExistCall(1);
		}

		/// <summary>
		/// verify that receiveActiveData gives data when active exists, tells that
		/// active does not exist and reports error in getting active information
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="Org.Apache.Zookeeper.KeeperException"/>
		/// <exception cref="ActiveNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.HA.ActiveStandbyElector.ActiveNotFoundException
		/// 	"/>
		[NUnit.Framework.Test]
		public virtual void TestGetActiveData()
		{
			// get valid active data
			byte[] data = new byte[8];
			Org.Mockito.Mockito.When(mockZK.GetData(Org.Mockito.Mockito.Eq(ZkLockName), Org.Mockito.Mockito
				.Eq(false), Org.Mockito.Mockito.AnyObject<Stat>())).ThenReturn(data);
			NUnit.Framework.Assert.AreEqual(data, elector.GetActiveData());
			Org.Mockito.Mockito.Verify(mockZK, Org.Mockito.Mockito.Times(1)).GetData(Org.Mockito.Mockito
				.Eq(ZkLockName), Org.Mockito.Mockito.Eq(false), Org.Mockito.Mockito.AnyObject<Stat
				>());
			// active does not exist
			Org.Mockito.Mockito.When(mockZK.GetData(Org.Mockito.Mockito.Eq(ZkLockName), Org.Mockito.Mockito
				.Eq(false), Org.Mockito.Mockito.AnyObject<Stat>())).ThenThrow(new KeeperException.NoNodeException
				());
			try
			{
				elector.GetActiveData();
				NUnit.Framework.Assert.Fail("ActiveNotFoundException expected");
			}
			catch (ActiveStandbyElector.ActiveNotFoundException)
			{
				Org.Mockito.Mockito.Verify(mockZK, Org.Mockito.Mockito.Times(2)).GetData(Org.Mockito.Mockito
					.Eq(ZkLockName), Org.Mockito.Mockito.Eq(false), Org.Mockito.Mockito.AnyObject<Stat
					>());
			}
			// error getting active data rethrows keeperexception
			try
			{
				Org.Mockito.Mockito.When(mockZK.GetData(Org.Mockito.Mockito.Eq(ZkLockName), Org.Mockito.Mockito
					.Eq(false), Org.Mockito.Mockito.AnyObject<Stat>())).ThenThrow(new KeeperException.AuthFailedException
					());
				elector.GetActiveData();
				NUnit.Framework.Assert.Fail("KeeperException.AuthFailedException expected");
			}
			catch (KeeperException.AuthFailedException)
			{
				Org.Mockito.Mockito.Verify(mockZK, Org.Mockito.Mockito.Times(3)).GetData(Org.Mockito.Mockito
					.Eq(ZkLockName), Org.Mockito.Mockito.Eq(false), Org.Mockito.Mockito.AnyObject<Stat
					>());
			}
		}

		/// <summary>Test that ensureBaseNode() recursively creates the specified dir</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestEnsureBaseNode()
		{
			elector.EnsureParentZNode();
			StringBuilder prefix = new StringBuilder();
			foreach (string part in ZkParentName.Split("/"))
			{
				if (part.IsEmpty())
				{
					continue;
				}
				prefix.Append("/").Append(part);
				if (!"/".Equals(prefix.ToString()))
				{
					Org.Mockito.Mockito.Verify(mockZK).Create(Org.Mockito.Mockito.Eq(prefix.ToString(
						)), Org.Mockito.Mockito.Any<byte[]>(), Org.Mockito.Mockito.Eq(ZooDefs.Ids.OpenAclUnsafe
						), Org.Mockito.Mockito.Eq(CreateMode.Persistent));
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
		public virtual void TestEnsureBaseNodeFails()
		{
			Org.Mockito.Mockito.DoThrow(new KeeperException.ConnectionLossException()).When(mockZK
				).Create(Org.Mockito.Mockito.Eq(ZkParentName), Org.Mockito.Mockito.Any<byte[]>()
				, Org.Mockito.Mockito.Eq(ZooDefs.Ids.OpenAclUnsafe), Org.Mockito.Mockito.Eq(CreateMode
				.Persistent));
			try
			{
				elector.EnsureParentZNode();
				NUnit.Framework.Assert.Fail("Did not throw!");
			}
			catch (IOException ioe)
			{
				if (!(ioe.InnerException is KeeperException.ConnectionLossException))
				{
					throw;
				}
			}
			// Should have tried three times
			Org.Mockito.Mockito.Verify(mockZK, Org.Mockito.Mockito.Times(3)).Create(Org.Mockito.Mockito
				.Eq(ZkParentName), Org.Mockito.Mockito.Any<byte[]>(), Org.Mockito.Mockito.Eq(ZooDefs.Ids
				.OpenAclUnsafe), Org.Mockito.Mockito.Eq(CreateMode.Persistent));
		}

		/// <summary>verify the zookeeper connection establishment</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestWithoutZKServer()
		{
			try
			{
				new ActiveStandbyElector("127.0.0.1", 2000, ZkParentName, ZooDefs.Ids.OpenAclUnsafe
					, Sharpen.Collections.EmptyList<ZKUtil.ZKAuthInfo>(), mockApp, CommonConfigurationKeys
					.HaFcElectorZkOpRetriesDefault);
				NUnit.Framework.Assert.Fail("Did not throw zookeeper connection loss exceptions!"
					);
			}
			catch (KeeperException ke)
			{
				GenericTestUtils.AssertExceptionContains("ConnectionLoss", ke);
			}
		}

		/// <summary>joinElection(..) should happen only after SERVICE_HEALTHY.</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestBecomeActiveBeforeServiceHealthy()
		{
			MockNoPriorActive();
			WatchedEvent mockEvent = Org.Mockito.Mockito.Mock<WatchedEvent>();
			Org.Mockito.Mockito.When(mockEvent.GetType()).ThenReturn(Watcher.Event.EventType.
				None);
			// session expired should enter safe mode
			// But for first time, before the SERVICE_HEALTY i.e. appData is set,
			// should not enter the election.
			Org.Mockito.Mockito.When(mockEvent.GetState()).ThenReturn(Watcher.Event.KeeperState
				.Expired);
			elector.ProcessWatchEvent(mockZK, mockEvent);
			// joinElection should not be called.
			Org.Mockito.Mockito.Verify(mockZK, Org.Mockito.Mockito.Times(0)).Create(ZkLockName
				, null, ZooDefs.Ids.OpenAclUnsafe, CreateMode.Ephemeral, elector, mockZK);
		}
	}
}
