using Com.Google.Common.Primitives;
using Org.Apache.Commons.Logging.Impl;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Util;
using Org.Apache.Log4j;
using Org.Apache.Zookeeper;
using Org.Apache.Zookeeper.Server;
using Org.Mockito;


namespace Org.Apache.Hadoop.HA
{
	/// <summary>
	/// Test for
	/// <see cref="ActiveStandbyElector"/>
	/// using real zookeeper.
	/// </summary>
	public class TestActiveStandbyElectorRealZK : ClientBaseWithFixes
	{
		internal const int NumElectors = 2;

		static TestActiveStandbyElectorRealZK()
		{
			((Log4JLogger)ActiveStandbyElector.Log).GetLogger().SetLevel(Level.All);
		}

		internal static readonly string ParentDir = "/" + UUID.RandomUUID();

		internal ActiveStandbyElector[] electors = new ActiveStandbyElector[NumElectors];

		private byte[][] appDatas = new byte[NumElectors][];

		private ActiveStandbyElector.ActiveStandbyElectorCallback[] cbs = new ActiveStandbyElector.ActiveStandbyElectorCallback
			[NumElectors];

		private ZooKeeperServer zkServer;

		/// <exception cref="System.Exception"/>
		public override void SetUp()
		{
			base.SetUp();
			zkServer = GetServer(serverFactory);
			for (int i = 0; i < NumElectors; i++)
			{
				cbs[i] = Org.Mockito.Mockito.Mock<ActiveStandbyElector.ActiveStandbyElectorCallback
					>();
				appDatas[i] = Ints.ToByteArray(i);
				electors[i] = new ActiveStandbyElector(hostPort, 5000, ParentDir, ZooDefs.Ids.OpenAclUnsafe
					, Collections.EmptyList<ZKUtil.ZKAuthInfo>(), cbs[i], CommonConfigurationKeys.HaFcElectorZkOpRetriesDefault
					);
			}
		}

		/// <exception cref="System.Exception"/>
		private void CheckFatalsAndReset()
		{
			for (int i = 0; i < NumElectors; i++)
			{
				Org.Mockito.Mockito.Verify(cbs[i], Org.Mockito.Mockito.Never()).NotifyFatalError(
					Org.Mockito.Mockito.AnyString());
				Org.Mockito.Mockito.Reset(cbs[i]);
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
		public virtual void TestActiveStandbyTransition()
		{
			Log.Info("starting test with parentDir:" + ParentDir);
			NUnit.Framework.Assert.IsFalse(electors[0].ParentZNodeExists());
			electors[0].EnsureParentZNode();
			Assert.True(electors[0].ParentZNodeExists());
			// First elector joins election, becomes active.
			electors[0].JoinElection(appDatas[0]);
			ActiveStandbyElectorTestUtil.WaitForActiveLockData(null, zkServer, ParentDir, appDatas
				[0]);
			Org.Mockito.Mockito.Verify(cbs[0], Org.Mockito.Mockito.Timeout(1000)).BecomeActive
				();
			CheckFatalsAndReset();
			// Second elector joins election, becomes standby.
			electors[1].JoinElection(appDatas[1]);
			Org.Mockito.Mockito.Verify(cbs[1], Org.Mockito.Mockito.Timeout(1000)).BecomeStandby
				();
			CheckFatalsAndReset();
			// First elector quits, second one should become active
			electors[0].QuitElection(true);
			ActiveStandbyElectorTestUtil.WaitForActiveLockData(null, zkServer, ParentDir, appDatas
				[1]);
			Org.Mockito.Mockito.Verify(cbs[1], Org.Mockito.Mockito.Timeout(1000)).BecomeActive
				();
			CheckFatalsAndReset();
			// First one rejoins, becomes standby, second one stays active
			electors[0].JoinElection(appDatas[0]);
			Org.Mockito.Mockito.Verify(cbs[0], Org.Mockito.Mockito.Timeout(1000)).BecomeStandby
				();
			CheckFatalsAndReset();
			// Second one expires, first one becomes active
			electors[1].PreventSessionReestablishmentForTests();
			try
			{
				zkServer.CloseSession(electors[1].GetZKSessionIdForTests());
				ActiveStandbyElectorTestUtil.WaitForActiveLockData(null, zkServer, ParentDir, appDatas
					[0]);
				Org.Mockito.Mockito.Verify(cbs[1], Org.Mockito.Mockito.Timeout(1000)).EnterNeutralMode
					();
				Org.Mockito.Mockito.Verify(cbs[0], Org.Mockito.Mockito.Timeout(1000)).FenceOldActive
					(AdditionalMatchers.AryEq(appDatas[1]));
				Org.Mockito.Mockito.Verify(cbs[0], Org.Mockito.Mockito.Timeout(1000)).BecomeActive
					();
			}
			finally
			{
				electors[1].AllowSessionReestablishmentForTests();
			}
			// Second one eventually reconnects and becomes standby
			Org.Mockito.Mockito.Verify(cbs[1], Org.Mockito.Mockito.Timeout(5000)).BecomeStandby
				();
			CheckFatalsAndReset();
			// First one expires, second one should become active
			electors[0].PreventSessionReestablishmentForTests();
			try
			{
				zkServer.CloseSession(electors[0].GetZKSessionIdForTests());
				ActiveStandbyElectorTestUtil.WaitForActiveLockData(null, zkServer, ParentDir, appDatas
					[1]);
				Org.Mockito.Mockito.Verify(cbs[0], Org.Mockito.Mockito.Timeout(1000)).EnterNeutralMode
					();
				Org.Mockito.Mockito.Verify(cbs[1], Org.Mockito.Mockito.Timeout(1000)).FenceOldActive
					(AdditionalMatchers.AryEq(appDatas[0]));
				Org.Mockito.Mockito.Verify(cbs[1], Org.Mockito.Mockito.Timeout(1000)).BecomeActive
					();
			}
			finally
			{
				electors[0].AllowSessionReestablishmentForTests();
			}
			CheckFatalsAndReset();
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestHandleSessionExpiration()
		{
			ActiveStandbyElector.ActiveStandbyElectorCallback cb = cbs[0];
			byte[] appData = appDatas[0];
			ActiveStandbyElector elector = electors[0];
			// Let the first elector become active
			elector.EnsureParentZNode();
			elector.JoinElection(appData);
			ZooKeeperServer zks = GetServer(serverFactory);
			ActiveStandbyElectorTestUtil.WaitForActiveLockData(null, zks, ParentDir, appData);
			Org.Mockito.Mockito.Verify(cb, Org.Mockito.Mockito.Timeout(1000)).BecomeActive();
			CheckFatalsAndReset();
			Log.Info("========================== Expiring session");
			zks.CloseSession(elector.GetZKSessionIdForTests());
			// Should enter neutral mode when disconnected
			Org.Mockito.Mockito.Verify(cb, Org.Mockito.Mockito.Timeout(1000)).EnterNeutralMode
				();
			// Should re-join the election and regain active
			ActiveStandbyElectorTestUtil.WaitForActiveLockData(null, zks, ParentDir, appData);
			Org.Mockito.Mockito.Verify(cb, Org.Mockito.Mockito.Timeout(1000)).BecomeActive();
			CheckFatalsAndReset();
			Log.Info("========================== Quitting election");
			elector.QuitElection(false);
			ActiveStandbyElectorTestUtil.WaitForActiveLockData(null, zks, ParentDir, null);
			// Double check that we don't accidentally re-join the election
			// due to receiving the "expired" event.
			Thread.Sleep(1000);
			Org.Mockito.Mockito.Verify(cb, Org.Mockito.Mockito.Never()).BecomeActive();
			ActiveStandbyElectorTestUtil.WaitForActiveLockData(null, zks, ParentDir, null);
			CheckFatalsAndReset();
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestHandleSessionExpirationOfStandby()
		{
			// Let elector 0 be active
			electors[0].EnsureParentZNode();
			electors[0].JoinElection(appDatas[0]);
			ZooKeeperServer zks = GetServer(serverFactory);
			ActiveStandbyElectorTestUtil.WaitForActiveLockData(null, zks, ParentDir, appDatas
				[0]);
			Org.Mockito.Mockito.Verify(cbs[0], Org.Mockito.Mockito.Timeout(1000)).BecomeActive
				();
			CheckFatalsAndReset();
			// Let elector 1 be standby
			electors[1].JoinElection(appDatas[1]);
			ActiveStandbyElectorTestUtil.WaitForElectorState(null, electors[1], ActiveStandbyElector.State
				.Standby);
			Log.Info("========================== Expiring standby's session");
			zks.CloseSession(electors[1].GetZKSessionIdForTests());
			// Should enter neutral mode when disconnected
			Org.Mockito.Mockito.Verify(cbs[1], Org.Mockito.Mockito.Timeout(1000)).EnterNeutralMode
				();
			// Should re-join the election and go back to STANDBY
			ActiveStandbyElectorTestUtil.WaitForElectorState(null, electors[1], ActiveStandbyElector.State
				.Standby);
			CheckFatalsAndReset();
			Log.Info("========================== Quitting election");
			electors[1].QuitElection(false);
			// Double check that we don't accidentally re-join the election
			// by quitting elector 0 and ensuring elector 1 doesn't become active
			electors[0].QuitElection(false);
			// due to receiving the "expired" event.
			Thread.Sleep(1000);
			Org.Mockito.Mockito.Verify(cbs[1], Org.Mockito.Mockito.Never()).BecomeActive();
			ActiveStandbyElectorTestUtil.WaitForActiveLockData(null, zks, ParentDir, null);
			CheckFatalsAndReset();
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestDontJoinElectionOnDisconnectAndReconnect()
		{
			electors[0].EnsureParentZNode();
			StopServer();
			ActiveStandbyElectorTestUtil.WaitForElectorState(null, electors[0], ActiveStandbyElector.State
				.Neutral);
			StartServer();
			WaitForServerUp(hostPort, ConnectionTimeout);
			// Have to sleep to allow time for the clients to reconnect.
			Thread.Sleep(2000);
			Org.Mockito.Mockito.Verify(cbs[0], Org.Mockito.Mockito.Never()).BecomeActive();
			Org.Mockito.Mockito.Verify(cbs[1], Org.Mockito.Mockito.Never()).BecomeActive();
			CheckFatalsAndReset();
		}
	}
}
