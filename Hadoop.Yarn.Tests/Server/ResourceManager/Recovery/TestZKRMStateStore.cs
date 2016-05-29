using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.HA;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security.Token.Delegation;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Security.Client;
using Org.Apache.Hadoop.Yarn.Server.Records;
using Org.Apache.Hadoop.Yarn.Server.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Security;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Zookeeper;
using Org.Apache.Zookeeper.Data;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery
{
	public class TestZKRMStateStore : RMStateStoreTestBase
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(TestZKRMStateStore));

		private const int ZkTimeoutMs = 1000;

		internal class TestZKRMStateStoreTester : RMStateStoreTestBase.RMStateStoreHelper
		{
			internal ZooKeeper client;

			internal TestZKRMStateStore.TestZKRMStateStoreTester.TestZKRMStateStoreInternal store;

			internal string workingZnode = "/jira/issue/3077/rmstore";

			internal class TestZKRMStateStoreInternal : ZKRMStateStore
			{
				/// <exception cref="System.Exception"/>
				public TestZKRMStateStoreInternal(TestZKRMStateStoreTester _enclosing, Configuration
					 conf, string workingZnode)
				{
					this._enclosing = _enclosing;
					this.Init(conf);
					this.Start();
					NUnit.Framework.Assert.IsTrue(this.znodeWorkingPath.Equals(workingZnode));
				}

				/// <exception cref="System.IO.IOException"/>
				protected internal override ZooKeeper GetNewZooKeeper()
				{
					return this._enclosing.client;
				}

				public virtual string GetVersionNode()
				{
					return this.znodeWorkingPath + "/" + ZKRMStateStore.RootZnodeName + "/" + RMStateStore
						.VersionNode;
				}

				protected internal override Version GetCurrentVersion()
				{
					return ZKRMStateStore.CurrentVersionInfo;
				}

				public virtual string GetAppNode(string appId)
				{
					return this._enclosing.workingZnode + "/" + ZKRMStateStore.RootZnodeName + "/" + 
						RMStateStore.RmAppRoot + "/" + appId;
				}

				private readonly TestZKRMStateStoreTester _enclosing;
			}

			/// <exception cref="System.Exception"/>
			public virtual RMStateStore GetRMStateStore(ZooKeeper zk)
			{
				YarnConfiguration conf = new YarnConfiguration();
				conf.Set(YarnConfiguration.RmZkAddress, this._enclosing.hostPort);
				conf.Set(YarnConfiguration.ZkRmStateStoreParentPath, this.workingZnode);
				if (null == zk)
				{
					this.client = this._enclosing.CreateClient();
				}
				else
				{
					this.client = zk;
				}
				this.store = new TestZKRMStateStore.TestZKRMStateStoreTester.TestZKRMStateStoreInternal
					(this, conf, this.workingZnode);
				return this.store;
			}

			public virtual string GetWorkingZNode()
			{
				return this.workingZnode;
			}

			/// <exception cref="System.Exception"/>
			public virtual RMStateStore GetRMStateStore()
			{
				return this.GetRMStateStore(null);
			}

			/// <exception cref="System.Exception"/>
			public virtual bool IsFinalStateValid()
			{
				IList<string> nodes = this.client.GetChildren(this.store.znodeWorkingPath, false);
				return nodes.Count == 1;
			}

			/// <exception cref="System.Exception"/>
			public virtual void WriteVersion(Version version)
			{
				this.client.SetData(this.store.GetVersionNode(), ((VersionPBImpl)version).GetProto
					().ToByteArray(), -1);
			}

			/// <exception cref="System.Exception"/>
			public virtual Version GetCurrentVersion()
			{
				return this.store.GetCurrentVersion();
			}

			/// <exception cref="System.Exception"/>
			public virtual bool AppExists(RMApp app)
			{
				Stat node = this.client.Exists(this.store.GetAppNode(app.GetApplicationId().ToString
					()), false);
				return node != null;
			}

			internal TestZKRMStateStoreTester(TestZKRMStateStore _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestZKRMStateStore _enclosing;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestZKRMStateStoreRealZK()
		{
			TestZKRMStateStore.TestZKRMStateStoreTester zkTester = new TestZKRMStateStore.TestZKRMStateStoreTester
				(this);
			TestRMAppStateStore(zkTester);
			TestRMDTSecretManagerStateStore(zkTester);
			TestCheckVersion(zkTester);
			TestEpoch(zkTester);
			TestAppDeletion(zkTester);
			TestDeleteStore(zkTester);
			TestAMRMTokenSecretManagerStateStore(zkTester);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestCheckMajorVersionChange()
		{
			TestZKRMStateStore.TestZKRMStateStoreTester zkTester = new _TestZKRMStateStoreTester_176
				(this);
			// default version
			RMStateStore store = zkTester.GetRMStateStore();
			Version defaultVersion = zkTester.GetCurrentVersion();
			store.CheckVersion();
			NUnit.Framework.Assert.AreEqual(defaultVersion, store.LoadVersion());
		}

		private sealed class _TestZKRMStateStoreTester_176 : TestZKRMStateStore.TestZKRMStateStoreTester
		{
			public _TestZKRMStateStoreTester_176(TestZKRMStateStore _enclosing)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
				this.VersionInfo = Version.NewInstance(int.MaxValue, 0);
			}

			internal Version VersionInfo;

			/// <exception cref="System.Exception"/>
			public override Version GetCurrentVersion()
			{
				return this.VersionInfo;
			}

			/// <exception cref="System.Exception"/>
			public override RMStateStore GetRMStateStore()
			{
				YarnConfiguration conf = new YarnConfiguration();
				this.workingZnode = "/jira/issue/3077/rmstore";
				conf.Set(YarnConfiguration.RmZkAddress, this._enclosing.hostPort);
				conf.Set(YarnConfiguration.ZkRmStateStoreParentPath, this.workingZnode);
				this.client = this._enclosing.CreateClient();
				this.store = new _TestZKRMStateStoreInternal_191(this, conf, this.workingZnode);
				return this.store;
			}

			private sealed class _TestZKRMStateStoreInternal_191 : TestZKRMStateStore.TestZKRMStateStoreTester.TestZKRMStateStoreInternal
			{
				public _TestZKRMStateStoreInternal_191(_TestZKRMStateStoreTester_176 _enclosing, 
					Configuration baseArg1, string baseArg2)
					: base(_enclosing, baseArg1, baseArg2)
				{
					this._enclosing = _enclosing;
					this.storedVersion = null;
				}

				internal Version storedVersion;

				protected internal override Version GetCurrentVersion()
				{
					return this._enclosing.VersionInfo;
				}

				/// <exception cref="System.Exception"/>
				protected internal override Version LoadVersion()
				{
					lock (this)
					{
						return this.storedVersion;
					}
				}

				/// <exception cref="System.Exception"/>
				protected internal override void StoreVersion()
				{
					lock (this)
					{
						this.storedVersion = this._enclosing.VersionInfo;
					}
				}

				private readonly _TestZKRMStateStoreTester_176 _enclosing;
			}

			private readonly TestZKRMStateStore _enclosing;
		}

		private Configuration CreateHARMConf(string rmIds, string rmId, int adminPort)
		{
			Configuration conf = new YarnConfiguration();
			conf.SetBoolean(YarnConfiguration.RmHaEnabled, true);
			conf.Set(YarnConfiguration.RmHaIds, rmIds);
			conf.SetBoolean(YarnConfiguration.RecoveryEnabled, true);
			conf.Set(YarnConfiguration.RmStore, typeof(ZKRMStateStore).FullName);
			conf.Set(YarnConfiguration.RmZkAddress, hostPort);
			conf.SetInt(YarnConfiguration.RmZkTimeoutMs, ZkTimeoutMs);
			conf.Set(YarnConfiguration.RmHaId, rmId);
			conf.Set(YarnConfiguration.RmWebappAddress, "localhost:0");
			foreach (string rpcAddress in YarnConfiguration.GetServiceAddressConfKeys(conf))
			{
				foreach (string id in HAUtil.GetRMHAIds(conf))
				{
					conf.Set(HAUtil.AddSuffix(rpcAddress, id), "localhost:0");
				}
			}
			conf.Set(HAUtil.AddSuffix(YarnConfiguration.RmAdminAddress, rmId), "localhost:" +
				 adminPort);
			return conf;
		}

		private static bool VerifyZKACL(string id, string scheme, int perm, IList<ACL> acls
			)
		{
			foreach (ACL acl in acls)
			{
				if (acl.GetId().GetScheme().Equals(scheme) && acl.GetId().GetId().StartsWith(id) 
					&& acl.GetPerms() == perm)
				{
					return true;
				}
			}
			return false;
		}

		/// <summary>
		/// Test if RM can successfully start in HA disabled mode if it was previously
		/// running in HA enabled mode.
		/// </summary>
		/// <remarks>
		/// Test if RM can successfully start in HA disabled mode if it was previously
		/// running in HA enabled mode. And then start it in HA mode after running it
		/// with HA disabled. NoAuth Exception should not be sent by zookeeper and RM
		/// should start successfully.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestZKRootPathAcls()
		{
			HAServiceProtocol.StateChangeRequestInfo req = new HAServiceProtocol.StateChangeRequestInfo
				(HAServiceProtocol.RequestSource.RequestByUser);
			string rootPath = YarnConfiguration.DefaultZkRmStateStoreParentPath + "/" + ZKRMStateStore
				.RootZnodeName;
			// Start RM with HA enabled
			Configuration conf = CreateHARMConf("rm1,rm2", "rm1", 1234);
			conf.SetBoolean(YarnConfiguration.AutoFailoverEnabled, false);
			ResourceManager rm = new MockRM(conf);
			rm.Start();
			rm.GetRMContext().GetRMAdminService().TransitionToActive(req);
			Stat stat = new Stat();
			IList<ACL> acls = ((ZKRMStateStore)rm.GetRMContext().GetStateStore()).GetACLWithRetries
				(rootPath, stat);
			NUnit.Framework.Assert.AreEqual(acls.Count, 2);
			// CREATE and DELETE permissions for root node based on RM ID
			VerifyZKACL("digest", "localhost", ZooDefs.Perms.Create | ZooDefs.Perms.Delete, acls
				);
			VerifyZKACL("world", "anyone", ZooDefs.Perms.All ^ (ZooDefs.Perms.Create | ZooDefs.Perms
				.Delete), acls);
			rm.Close();
			// Now start RM with HA disabled. NoAuth Exception should not be thrown.
			conf.SetBoolean(YarnConfiguration.RmHaEnabled, false);
			rm = new MockRM(conf);
			rm.Start();
			rm.GetRMContext().GetRMAdminService().TransitionToActive(req);
			acls = ((ZKRMStateStore)rm.GetRMContext().GetStateStore()).GetACLWithRetries(rootPath
				, stat);
			NUnit.Framework.Assert.AreEqual(acls.Count, 1);
			VerifyZKACL("world", "anyone", ZooDefs.Perms.All, acls);
			rm.Close();
			// Start RM with HA enabled.
			conf.SetBoolean(YarnConfiguration.RmHaEnabled, true);
			rm = new MockRM(conf);
			rm.Start();
			rm.GetRMContext().GetRMAdminService().TransitionToActive(req);
			acls = ((ZKRMStateStore)rm.GetRMContext().GetStateStore()).GetACLWithRetries(rootPath
				, stat);
			NUnit.Framework.Assert.AreEqual(acls.Count, 2);
			VerifyZKACL("digest", "localhost", ZooDefs.Perms.Create | ZooDefs.Perms.Delete, acls
				);
			VerifyZKACL("world", "anyone", ZooDefs.Perms.All ^ (ZooDefs.Perms.Create | ZooDefs.Perms
				.Delete), acls);
			rm.Close();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFencing()
		{
			HAServiceProtocol.StateChangeRequestInfo req = new HAServiceProtocol.StateChangeRequestInfo
				(HAServiceProtocol.RequestSource.RequestByUser);
			Configuration conf1 = CreateHARMConf("rm1,rm2", "rm1", 1234);
			conf1.SetBoolean(YarnConfiguration.AutoFailoverEnabled, false);
			ResourceManager rm1 = new ResourceManager();
			rm1.Init(conf1);
			rm1.Start();
			rm1.GetRMContext().GetRMAdminService().TransitionToActive(req);
			NUnit.Framework.Assert.AreEqual("RM with ZKStore didn't start", Service.STATE.Started
				, rm1.GetServiceState());
			NUnit.Framework.Assert.AreEqual("RM should be Active", HAServiceProtocol.HAServiceState
				.Active, rm1.GetRMContext().GetRMAdminService().GetServiceStatus().GetState());
			Configuration conf2 = CreateHARMConf("rm1,rm2", "rm2", 5678);
			conf2.SetBoolean(YarnConfiguration.AutoFailoverEnabled, false);
			ResourceManager rm2 = new ResourceManager();
			rm2.Init(conf2);
			rm2.Start();
			rm2.GetRMContext().GetRMAdminService().TransitionToActive(req);
			NUnit.Framework.Assert.AreEqual("RM with ZKStore didn't start", Service.STATE.Started
				, rm2.GetServiceState());
			NUnit.Framework.Assert.AreEqual("RM should be Active", HAServiceProtocol.HAServiceState
				.Active, rm2.GetRMContext().GetRMAdminService().GetServiceStatus().GetState());
			for (int i = 0; i < ZkTimeoutMs / 50; i++)
			{
				if (HAServiceProtocol.HAServiceState.Active == rm1.GetRMContext().GetRMAdminService
					().GetServiceStatus().GetState())
				{
					Sharpen.Thread.Sleep(100);
				}
			}
			NUnit.Framework.Assert.AreEqual("RM should have been fenced", HAServiceProtocol.HAServiceState
				.Standby, rm1.GetRMContext().GetRMAdminService().GetServiceStatus().GetState());
			NUnit.Framework.Assert.AreEqual("RM should be Active", HAServiceProtocol.HAServiceState
				.Active, rm2.GetRMContext().GetRMAdminService().GetServiceStatus().GetState());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNoAuthExceptionInNonHAMode()
		{
			TestZKRMStateStore.TestZKRMStateStoreTester zkTester = new TestZKRMStateStore.TestZKRMStateStoreTester
				(this);
			string appRoot = zkTester.GetWorkingZNode() + "/ZKRMStateRoot/RMAppRoot";
			ZooKeeper zk = Org.Mockito.Mockito.Spy(CreateClient());
			Org.Mockito.Mockito.DoThrow(new KeeperException.NoAuthException()).When(zk).Create
				(appRoot, null, RMZKUtils.GetZKAcls(new Configuration()), CreateMode.Persistent);
			try
			{
				zkTester.GetRMStateStore(zk);
				NUnit.Framework.Assert.Fail("Expected exception to be thrown");
			}
			catch (ServiceStateException e)
			{
				NUnit.Framework.Assert.IsNotNull(e.InnerException);
				NUnit.Framework.Assert.IsTrue("Expected NoAuthException", e.InnerException is KeeperException.NoAuthException
					);
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFencedState()
		{
			TestZKRMStateStore.TestZKRMStateStoreTester zkTester = new TestZKRMStateStore.TestZKRMStateStoreTester
				(this);
			RMStateStore store = zkTester.GetRMStateStore();
			// Move state to FENCED from ACTIVE
			store.UpdateFencedState();
			NUnit.Framework.Assert.AreEqual("RMStateStore should have been in fenced state", 
				true, store.IsFencedState());
			long submitTime = Runtime.CurrentTimeMillis();
			long startTime = submitTime + 1000;
			// Add a new app
			RMApp mockApp = Org.Mockito.Mockito.Mock<RMApp>();
			ApplicationSubmissionContext context = new ApplicationSubmissionContextPBImpl();
			Org.Mockito.Mockito.When(mockApp.GetSubmitTime()).ThenReturn(submitTime);
			Org.Mockito.Mockito.When(mockApp.GetStartTime()).ThenReturn(startTime);
			Org.Mockito.Mockito.When(mockApp.GetApplicationSubmissionContext()).ThenReturn(context
				);
			Org.Mockito.Mockito.When(mockApp.GetUser()).ThenReturn("test");
			store.StoreNewApplication(mockApp);
			NUnit.Framework.Assert.AreEqual("RMStateStore should have been in fenced state", 
				true, store.IsFencedState());
			// Add a new attempt
			ClientToAMTokenSecretManagerInRM clientToAMTokenMgr = new ClientToAMTokenSecretManagerInRM
				();
			ApplicationAttemptId attemptId = ConverterUtils.ToApplicationAttemptId("appattempt_1234567894321_0001_000001"
				);
			SecretKey clientTokenMasterKey = clientToAMTokenMgr.CreateMasterKey(attemptId);
			RMAppAttemptMetrics mockRmAppAttemptMetrics = Org.Mockito.Mockito.Mock<RMAppAttemptMetrics
				>();
			Container container = new ContainerPBImpl();
			container.SetId(ConverterUtils.ToContainerId("container_1234567891234_0001_01_000001"
				));
			RMAppAttempt mockAttempt = Org.Mockito.Mockito.Mock<RMAppAttempt>();
			Org.Mockito.Mockito.When(mockAttempt.GetAppAttemptId()).ThenReturn(attemptId);
			Org.Mockito.Mockito.When(mockAttempt.GetMasterContainer()).ThenReturn(container);
			Org.Mockito.Mockito.When(mockAttempt.GetClientTokenMasterKey()).ThenReturn(clientTokenMasterKey
				);
			Org.Mockito.Mockito.When(mockAttempt.GetRMAppAttemptMetrics()).ThenReturn(mockRmAppAttemptMetrics
				);
			Org.Mockito.Mockito.When(mockRmAppAttemptMetrics.GetAggregateAppResourceUsage()).
				ThenReturn(new AggregateAppResourceUsage(0, 0));
			store.StoreNewApplicationAttempt(mockAttempt);
			NUnit.Framework.Assert.AreEqual("RMStateStore should have been in fenced state", 
				true, store.IsFencedState());
			long finishTime = submitTime + 1000;
			// Update attempt
			ApplicationAttemptStateData newAttemptState = ApplicationAttemptStateData.NewInstance
				(attemptId, container, store.GetCredentialsFromAppAttempt(mockAttempt), startTime
				, RMAppAttemptState.Finished, "testUrl", "test", FinalApplicationStatus.Succeeded
				, 100, finishTime, 0, 0);
			store.UpdateApplicationAttemptState(newAttemptState);
			NUnit.Framework.Assert.AreEqual("RMStateStore should have been in fenced state", 
				true, store.IsFencedState());
			// Update app
			ApplicationStateData appState = ApplicationStateData.NewInstance(submitTime, startTime
				, context, "test");
			store.UpdateApplicationState(appState);
			NUnit.Framework.Assert.AreEqual("RMStateStore should have been in fenced state", 
				true, store.IsFencedState());
			// Remove app
			store.RemoveApplication(mockApp);
			NUnit.Framework.Assert.AreEqual("RMStateStore should have been in fenced state", 
				true, store.IsFencedState());
			// store RM delegation token;
			RMDelegationTokenIdentifier dtId1 = new RMDelegationTokenIdentifier(new Text("owner1"
				), new Text("renewer1"), new Text("realuser1"));
			long renewDate1 = Runtime.CurrentTimeMillis();
			dtId1.SetSequenceNumber(1111);
			store.StoreRMDelegationToken(dtId1, renewDate1);
			NUnit.Framework.Assert.AreEqual("RMStateStore should have been in fenced state", 
				true, store.IsFencedState());
			store.UpdateRMDelegationToken(dtId1, renewDate1);
			NUnit.Framework.Assert.AreEqual("RMStateStore should have been in fenced state", 
				true, store.IsFencedState());
			// remove delegation key;
			store.RemoveRMDelegationToken(dtId1);
			NUnit.Framework.Assert.AreEqual("RMStateStore should have been in fenced state", 
				true, store.IsFencedState());
			// store delegation master key;
			DelegationKey key = new DelegationKey(1234, 4321, Sharpen.Runtime.GetBytesForString
				("keyBytes"));
			store.StoreRMDTMasterKey(key);
			NUnit.Framework.Assert.AreEqual("RMStateStore should have been in fenced state", 
				true, store.IsFencedState());
			// remove delegation master key;
			store.RemoveRMDTMasterKey(key);
			NUnit.Framework.Assert.AreEqual("RMStateStore should have been in fenced state", 
				true, store.IsFencedState());
			// store or update AMRMToken;
			store.StoreOrUpdateAMRMTokenSecretManager(null, false);
			NUnit.Framework.Assert.AreEqual("RMStateStore should have been in fenced state", 
				true, store.IsFencedState());
			store.Close();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDuplicateRMAppDeletion()
		{
			TestZKRMStateStore.TestZKRMStateStoreTester zkTester = new TestZKRMStateStore.TestZKRMStateStoreTester
				(this);
			long submitTime = Runtime.CurrentTimeMillis();
			long startTime = Runtime.CurrentTimeMillis() + 1234;
			RMStateStore store = zkTester.GetRMStateStore();
			RMStateStoreTestBase.TestDispatcher dispatcher = new RMStateStoreTestBase.TestDispatcher
				();
			store.SetRMDispatcher(dispatcher);
			ApplicationAttemptId attemptIdRemoved = ConverterUtils.ToApplicationAttemptId("appattempt_1352994193343_0002_000001"
				);
			ApplicationId appIdRemoved = attemptIdRemoved.GetApplicationId();
			StoreApp(store, appIdRemoved, submitTime, startTime);
			StoreAttempt(store, attemptIdRemoved, "container_1352994193343_0002_01_000001", null
				, null, dispatcher);
			ApplicationSubmissionContext context = new ApplicationSubmissionContextPBImpl();
			context.SetApplicationId(appIdRemoved);
			ApplicationStateData appStateRemoved = ApplicationStateData.NewInstance(submitTime
				, startTime, context, "user1");
			appStateRemoved.attempts[attemptIdRemoved] = null;
			store.RemoveApplicationStateInternal(appStateRemoved);
			try
			{
				store.RemoveApplicationStateInternal(appStateRemoved);
			}
			catch (KeeperException.NoNodeException)
			{
				NUnit.Framework.Assert.Fail("NoNodeException should not happen.");
			}
			store.Close();
		}
	}
}
