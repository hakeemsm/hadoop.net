using System;
using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Client.Api;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Ipc;
using Org.Apache.Hadoop.Yarn.Security;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Server.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Event;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fifo;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Client.Api.Impl
{
	public class TestAMRMClientOnRMRestart
	{
		internal static Configuration conf = null;

		internal const int rolling_interval_sec = 13;

		internal const long am_expire_ms = 4000;

		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void Setup()
		{
			conf = new Configuration();
			conf.Set(YarnConfiguration.RecoveryEnabled, "true");
			conf.Set(YarnConfiguration.RmStore, typeof(MemoryRMStateStore).FullName);
			conf.SetInt(YarnConfiguration.RmAmMaxAttempts, YarnConfiguration.DefaultRmAmMaxAttempts
				);
			conf.SetBoolean(YarnConfiguration.RmWorkPreservingRecoveryEnabled, true);
			conf.SetLong(YarnConfiguration.RmWorkPreservingRecoverySchedulingWaitMs, 0);
		}

		// Test does major 6 steps verification.
		// Step-1 : AMRMClient send allocate request for 2 container requests
		// Step-2 : 2 containers are allocated by RM.
		// Step-3 : AM Send 1 containerRequest(cRequest3) and 1 releaseRequests to
		// RM
		// Step-4 : On RM restart, AM(does not know RM is restarted) sends additional
		// containerRequest(cRequest4) and blacklisted nodes.
		// Intern RM send resync command
		// Step-5 : Allocater after resync command & new containerRequest(cRequest5)
		// Step-6 : RM allocates containers i.e cRequest3,cRequest4 and cRequest5
		/// <exception cref="System.Exception"/>
		public virtual void TestAMRMClientResendsRequestsOnRMRestart()
		{
			UserGroupInformation.SetLoginUser(null);
			MemoryRMStateStore memStore = new MemoryRMStateStore();
			memStore.Init(conf);
			// Phase-1 Start 1st RM
			TestAMRMClientOnRMRestart.MyResourceManager rm1 = new TestAMRMClientOnRMRestart.MyResourceManager
				(conf, memStore);
			rm1.Start();
			DrainDispatcher dispatcher = (DrainDispatcher)rm1.GetRMContext().GetDispatcher();
			// Submit the application
			RMApp app = rm1.SubmitApp(1024);
			dispatcher.Await();
			MockNM nm1 = new MockNM("h1:1234", 15120, rm1.GetResourceTrackerService());
			nm1.RegisterNode();
			nm1.NodeHeartbeat(true);
			// Node heartbeat
			dispatcher.Await();
			ApplicationAttemptId appAttemptId = app.GetCurrentAppAttempt().GetAppAttemptId();
			rm1.SendAMLaunched(appAttemptId);
			dispatcher.Await();
			Org.Apache.Hadoop.Security.Token.Token<AMRMTokenIdentifier> token = rm1.GetRMContext
				().GetRMApps()[appAttemptId.GetApplicationId()].GetRMAppAttempt(appAttemptId).GetAMRMToken
				();
			UserGroupInformation ugi = UserGroupInformation.GetCurrentUser();
			ugi.AddTokenIdentifier(token.DecodeIdentifier());
			// Step-1 : AMRMClient send allocate request for 2 ContainerRequest
			// cRequest1 = h1 and cRequest2 = h1,h2
			// blacklisted nodes = h2
			AMRMClient<AMRMClient.ContainerRequest> amClient = new TestAMRMClientOnRMRestart.MyAMRMClientImpl
				(rm1);
			amClient.Init(conf);
			amClient.Start();
			amClient.RegisterApplicationMaster("Host", 10000, string.Empty);
			AMRMClient.ContainerRequest cRequest1 = CreateReq(1, 1024, new string[] { "h1" });
			amClient.AddContainerRequest(cRequest1);
			AMRMClient.ContainerRequest cRequest2 = CreateReq(1, 1024, new string[] { "h1", "h2"
				 });
			amClient.AddContainerRequest(cRequest2);
			IList<string> blacklistAdditions = new AList<string>();
			IList<string> blacklistRemoval = new AList<string>();
			blacklistAdditions.AddItem("h2");
			blacklistRemoval.AddItem("h10");
			amClient.UpdateBlacklist(blacklistAdditions, blacklistRemoval);
			blacklistAdditions.Remove("h2");
			// remove from local list
			AllocateResponse allocateResponse = amClient.Allocate(0.1f);
			dispatcher.Await();
			NUnit.Framework.Assert.AreEqual("No of assignments must be 0", 0, allocateResponse
				.GetAllocatedContainers().Count);
			// Why 4 ask, why not 3 ask even h2 is blacklisted?
			// On blacklisting host,applicationmaster has to remove ask request from
			// remoterequest table.Here,test does not remove explicitely
			AssertAsksAndReleases(4, 0, rm1);
			AssertBlacklistAdditionsAndRemovals(1, 1, rm1);
			// Step-2 : NM heart beat is sent.
			// On 2nd AM allocate request, RM allocates 2 containers to AM
			nm1.NodeHeartbeat(true);
			// Node heartbeat
			dispatcher.Await();
			allocateResponse = amClient.Allocate(0.2f);
			dispatcher.Await();
			// 2 containers are allocated i.e for cRequest1 and cRequest2.
			NUnit.Framework.Assert.AreEqual("No of assignments must be 0", 2, allocateResponse
				.GetAllocatedContainers().Count);
			AssertAsksAndReleases(0, 0, rm1);
			AssertBlacklistAdditionsAndRemovals(0, 0, rm1);
			IList<Container> allocatedContainers = allocateResponse.GetAllocatedContainers();
			// removed allocated container requests
			amClient.RemoveContainerRequest(cRequest1);
			amClient.RemoveContainerRequest(cRequest2);
			allocateResponse = amClient.Allocate(0.2f);
			dispatcher.Await();
			NUnit.Framework.Assert.AreEqual("No of assignments must be 0", 0, allocateResponse
				.GetAllocatedContainers().Count);
			AssertAsksAndReleases(4, 0, rm1);
			AssertBlacklistAdditionsAndRemovals(0, 0, rm1);
			// Step-3 : Send 1 containerRequest and 1 releaseRequests to RM
			AMRMClient.ContainerRequest cRequest3 = CreateReq(1, 1024, new string[] { "h1" });
			amClient.AddContainerRequest(cRequest3);
			int pendingRelease = 0;
			IEnumerator<Container> it = allocatedContainers.GetEnumerator();
			while (it.HasNext())
			{
				amClient.ReleaseAssignedContainer(it.Next().GetId());
				pendingRelease++;
				it.Remove();
				break;
			}
			// remove one container
			allocateResponse = amClient.Allocate(0.3f);
			dispatcher.Await();
			NUnit.Framework.Assert.AreEqual("No of assignments must be 0", 0, allocateResponse
				.GetAllocatedContainers().Count);
			AssertAsksAndReleases(3, pendingRelease, rm1);
			AssertBlacklistAdditionsAndRemovals(0, 0, rm1);
			int completedContainer = allocateResponse.GetCompletedContainersStatuses().Count;
			pendingRelease -= completedContainer;
			// Phase-2 start 2nd RM is up
			TestAMRMClientOnRMRestart.MyResourceManager rm2 = new TestAMRMClientOnRMRestart.MyResourceManager
				(conf, memStore);
			rm2.Start();
			nm1.SetResourceTrackerService(rm2.GetResourceTrackerService());
			((TestAMRMClientOnRMRestart.MyAMRMClientImpl)amClient).UpdateRMProxy(rm2);
			dispatcher = (DrainDispatcher)rm2.GetRMContext().GetDispatcher();
			// NM should be rebooted on heartbeat, even first heartbeat for nm2
			NodeHeartbeatResponse hbResponse = nm1.NodeHeartbeat(true);
			NUnit.Framework.Assert.AreEqual(NodeAction.Resync, hbResponse.GetNodeAction());
			// new NM to represent NM re-register
			nm1 = new MockNM("h1:1234", 10240, rm2.GetResourceTrackerService());
			nm1.RegisterNode();
			nm1.NodeHeartbeat(true);
			dispatcher.Await();
			blacklistAdditions.AddItem("h3");
			amClient.UpdateBlacklist(blacklistAdditions, null);
			blacklistAdditions.Remove("h3");
			it = allocatedContainers.GetEnumerator();
			while (it.HasNext())
			{
				amClient.ReleaseAssignedContainer(it.Next().GetId());
				pendingRelease++;
				it.Remove();
			}
			AMRMClient.ContainerRequest cRequest4 = CreateReq(1, 1024, new string[] { "h1", "h2"
				 });
			amClient.AddContainerRequest(cRequest4);
			// Step-4 : On RM restart, AM(does not know RM is restarted) sends
			// additional
			// containerRequest and blacklisted nodes.
			// Intern RM send resync command,AMRMClient resend allocate request
			allocateResponse = amClient.Allocate(0.3f);
			dispatcher.Await();
			completedContainer = allocateResponse.GetCompletedContainersStatuses().Count;
			pendingRelease -= completedContainer;
			AssertAsksAndReleases(4, pendingRelease, rm2);
			AssertBlacklistAdditionsAndRemovals(2, 0, rm2);
			AMRMClient.ContainerRequest cRequest5 = CreateReq(1, 1024, new string[] { "h1", "h2"
				, "h3" });
			amClient.AddContainerRequest(cRequest5);
			// Step-5 : Allocater after resync command
			allocateResponse = amClient.Allocate(0.5f);
			dispatcher.Await();
			NUnit.Framework.Assert.AreEqual("No of assignments must be 0", 0, allocateResponse
				.GetAllocatedContainers().Count);
			AssertAsksAndReleases(5, 0, rm2);
			AssertBlacklistAdditionsAndRemovals(0, 0, rm2);
			int noAssignedContainer = 0;
			int count = 5;
			while (count-- > 0)
			{
				nm1.NodeHeartbeat(true);
				dispatcher.Await();
				allocateResponse = amClient.Allocate(0.5f);
				dispatcher.Await();
				noAssignedContainer += allocateResponse.GetAllocatedContainers().Count;
				if (noAssignedContainer == 3)
				{
					break;
				}
				Sharpen.Thread.Sleep(1000);
			}
			// Step-6 : RM allocates containers i.e cRequest3,cRequest4 and cRequest5
			NUnit.Framework.Assert.AreEqual("Number of container should be 3", 3, noAssignedContainer
				);
			amClient.Stop();
			rm1.Stop();
			rm2.Stop();
		}

		// Test verify for
		// 1. AM try to unregister without registering
		// 2. AM register to RM, and try to unregister immediately after RM restart
		/// <exception cref="System.Exception"/>
		public virtual void TestAMRMClientForUnregisterAMOnRMRestart()
		{
			MemoryRMStateStore memStore = new MemoryRMStateStore();
			memStore.Init(conf);
			// Phase-1 Start 1st RM
			TestAMRMClientOnRMRestart.MyResourceManager rm1 = new TestAMRMClientOnRMRestart.MyResourceManager
				(conf, memStore);
			rm1.Start();
			DrainDispatcher dispatcher = (DrainDispatcher)rm1.GetRMContext().GetDispatcher();
			// Submit the application
			RMApp app = rm1.SubmitApp(1024);
			dispatcher.Await();
			MockNM nm1 = new MockNM("h1:1234", 15120, rm1.GetResourceTrackerService());
			nm1.RegisterNode();
			nm1.NodeHeartbeat(true);
			// Node heartbeat
			dispatcher.Await();
			ApplicationAttemptId appAttemptId = app.GetCurrentAppAttempt().GetAppAttemptId();
			rm1.SendAMLaunched(appAttemptId);
			dispatcher.Await();
			Org.Apache.Hadoop.Security.Token.Token<AMRMTokenIdentifier> token = rm1.GetRMContext
				().GetRMApps()[appAttemptId.GetApplicationId()].GetRMAppAttempt(appAttemptId).GetAMRMToken
				();
			UserGroupInformation ugi = UserGroupInformation.GetCurrentUser();
			ugi.AddTokenIdentifier(token.DecodeIdentifier());
			AMRMClient<AMRMClient.ContainerRequest> amClient = new TestAMRMClientOnRMRestart.MyAMRMClientImpl
				(rm1);
			amClient.Init(conf);
			amClient.Start();
			amClient.RegisterApplicationMaster("h1", 10000, string.Empty);
			amClient.Allocate(0.1f);
			// Phase-2 start 2nd RM is up
			TestAMRMClientOnRMRestart.MyResourceManager rm2 = new TestAMRMClientOnRMRestart.MyResourceManager
				(conf, memStore);
			rm2.Start();
			nm1.SetResourceTrackerService(rm2.GetResourceTrackerService());
			((TestAMRMClientOnRMRestart.MyAMRMClientImpl)amClient).UpdateRMProxy(rm2);
			dispatcher = (DrainDispatcher)rm2.GetRMContext().GetDispatcher();
			// NM should be rebooted on heartbeat, even first heartbeat for nm2
			NodeHeartbeatResponse hbResponse = nm1.NodeHeartbeat(true);
			NUnit.Framework.Assert.AreEqual(NodeAction.Resync, hbResponse.GetNodeAction());
			// new NM to represent NM re-register
			nm1 = new MockNM("h1:1234", 10240, rm2.GetResourceTrackerService());
			ContainerId containerId = ContainerId.NewContainerId(appAttemptId, 1);
			NMContainerStatus containerReport = NMContainerStatus.NewInstance(containerId, ContainerState
				.Running, Resource.NewInstance(1024, 1), "recover container", 0, Priority.NewInstance
				(0), 0);
			nm1.RegisterNode(Arrays.AsList(containerReport), null);
			nm1.NodeHeartbeat(true);
			dispatcher.Await();
			amClient.UnregisterApplicationMaster(FinalApplicationStatus.Succeeded, null, null
				);
			rm2.WaitForState(appAttemptId, RMAppAttemptState.Finishing);
			nm1.NodeHeartbeat(appAttemptId, 1, ContainerState.Complete);
			rm2.WaitForState(appAttemptId, RMAppAttemptState.Finished);
			rm2.WaitForState(app.GetApplicationId(), RMAppState.Finished);
			amClient.Stop();
			rm1.Stop();
			rm2.Stop();
		}

		// Test verify for AM issued with rolled-over AMRMToken
		// is still able to communicate with restarted RM.
		/// <exception cref="System.Exception"/>
		public virtual void TestAMRMClientOnAMRMTokenRollOverOnRMRestart()
		{
			conf.SetLong(YarnConfiguration.RmAmrmTokenMasterKeyRollingIntervalSecs, rolling_interval_sec
				);
			conf.SetLong(YarnConfiguration.RmAmExpiryIntervalMs, am_expire_ms);
			MemoryRMStateStore memStore = new MemoryRMStateStore();
			memStore.Init(conf);
			// start first RM
			TestAMRMClientOnRMRestart.MyResourceManager2 rm1 = new TestAMRMClientOnRMRestart.MyResourceManager2
				(conf, memStore);
			rm1.Start();
			DrainDispatcher dispatcher = (DrainDispatcher)rm1.GetRMContext().GetDispatcher();
			long startTime = Runtime.CurrentTimeMillis();
			// Submit the application
			RMApp app = rm1.SubmitApp(1024);
			dispatcher.Await();
			MockNM nm1 = new MockNM("h1:1234", 15120, rm1.GetResourceTrackerService());
			nm1.RegisterNode();
			nm1.NodeHeartbeat(true);
			// Node heartbeat
			dispatcher.Await();
			ApplicationAttemptId appAttemptId = app.GetCurrentAppAttempt().GetAppAttemptId();
			rm1.SendAMLaunched(appAttemptId);
			dispatcher.Await();
			AMRMTokenSecretManager amrmTokenSecretManagerForRM1 = rm1.GetRMContext().GetAMRMTokenSecretManager
				();
			Org.Apache.Hadoop.Security.Token.Token<AMRMTokenIdentifier> token = amrmTokenSecretManagerForRM1
				.CreateAndGetAMRMToken(appAttemptId);
			UserGroupInformation ugi = UserGroupInformation.GetCurrentUser();
			ugi.AddTokenIdentifier(token.DecodeIdentifier());
			AMRMClient<AMRMClient.ContainerRequest> amClient = new TestAMRMClientOnRMRestart.MyAMRMClientImpl
				(rm1);
			amClient.Init(conf);
			amClient.Start();
			amClient.RegisterApplicationMaster("h1", 10000, string.Empty);
			amClient.Allocate(0.1f);
			// Wait for enough time and make sure the roll_over happens
			// At mean time, the old AMRMToken should continue to work
			while (Runtime.CurrentTimeMillis() - startTime < rolling_interval_sec * 1000)
			{
				amClient.Allocate(0.1f);
				try
				{
					Sharpen.Thread.Sleep(1000);
				}
				catch (Exception)
				{
				}
			}
			// DO NOTHING
			NUnit.Framework.Assert.IsTrue(amrmTokenSecretManagerForRM1.GetMasterKey().GetMasterKey
				().GetKeyId() != token.DecodeIdentifier().GetKeyId());
			amClient.Allocate(0.1f);
			// active the nextMasterKey, and replace the currentMasterKey
			Org.Apache.Hadoop.Security.Token.Token<AMRMTokenIdentifier> newToken = amrmTokenSecretManagerForRM1
				.CreateAndGetAMRMToken(appAttemptId);
			int waitCount = 0;
			while (waitCount++ <= 50)
			{
				if (amrmTokenSecretManagerForRM1.GetCurrnetMasterKeyData().GetMasterKey().GetKeyId
					() != token.DecodeIdentifier().GetKeyId())
				{
					break;
				}
				try
				{
					amClient.Allocate(0.1f);
				}
				catch (Exception)
				{
					break;
				}
				Sharpen.Thread.Sleep(500);
			}
			NUnit.Framework.Assert.IsTrue(amrmTokenSecretManagerForRM1.GetNextMasterKeyData()
				 == null);
			NUnit.Framework.Assert.IsTrue(amrmTokenSecretManagerForRM1.GetCurrnetMasterKeyData
				().GetMasterKey().GetKeyId() == newToken.DecodeIdentifier().GetKeyId());
			// start 2nd RM
			conf.Set(YarnConfiguration.RmSchedulerAddress, "0.0.0.0:9030");
			TestAMRMClientOnRMRestart.MyResourceManager2 rm2 = new TestAMRMClientOnRMRestart.MyResourceManager2
				(conf, memStore);
			rm2.Start();
			nm1.SetResourceTrackerService(rm2.GetResourceTrackerService());
			((TestAMRMClientOnRMRestart.MyAMRMClientImpl)amClient).UpdateRMProxy(rm2);
			dispatcher = (DrainDispatcher)rm2.GetRMContext().GetDispatcher();
			AMRMTokenSecretManager amrmTokenSecretManagerForRM2 = rm2.GetRMContext().GetAMRMTokenSecretManager
				();
			NUnit.Framework.Assert.IsTrue(amrmTokenSecretManagerForRM2.GetCurrnetMasterKeyData
				().GetMasterKey().GetKeyId() == newToken.DecodeIdentifier().GetKeyId());
			NUnit.Framework.Assert.IsTrue(amrmTokenSecretManagerForRM2.GetNextMasterKeyData()
				 == null);
			try
			{
				UserGroupInformation testUser = UserGroupInformation.CreateRemoteUser("testUser");
				SecurityUtil.SetTokenService(token, rm2.GetApplicationMasterService().GetBindAddress
					());
				testUser.AddToken(token);
				testUser.DoAs(new _PrivilegedAction_480(rm2)).Allocate(Org.Apache.Hadoop.Yarn.Util.Records
					.NewRecord<AllocateRequest>());
				NUnit.Framework.Assert.Fail("The old Token should not work");
			}
			catch (Exception ex)
			{
				NUnit.Framework.Assert.IsTrue(ex is SecretManager.InvalidToken);
				NUnit.Framework.Assert.IsTrue(ex.Message.Contains("Invalid AMRMToken from " + token
					.DecodeIdentifier().GetApplicationAttemptId()));
			}
			// make sure the recovered AMRMToken works for new RM
			amClient.Allocate(0.1f);
			amClient.UnregisterApplicationMaster(FinalApplicationStatus.Succeeded, null, null
				);
			amClient.Stop();
			rm1.Stop();
			rm2.Stop();
		}

		private sealed class _PrivilegedAction_480 : PrivilegedAction<ApplicationMasterProtocol
			>
		{
			public _PrivilegedAction_480(TestAMRMClientOnRMRestart.MyResourceManager2 rm2)
			{
				this.rm2 = rm2;
			}

			public ApplicationMasterProtocol Run()
			{
				return (ApplicationMasterProtocol)YarnRPC.Create(TestAMRMClientOnRMRestart.conf).
					GetProxy(typeof(ApplicationMasterProtocol), rm2.GetApplicationMasterService().GetBindAddress
					(), TestAMRMClientOnRMRestart.conf);
			}

			private readonly TestAMRMClientOnRMRestart.MyResourceManager2 rm2;
		}

		private class MyFifoScheduler : FifoScheduler
		{
			public MyFifoScheduler(RMContext rmContext)
				: base()
			{
				try
				{
					Configuration conf = new Configuration();
					Reinitialize(conf, rmContext);
				}
				catch (IOException)
				{
					System.Diagnostics.Debug.Assert((false));
				}
			}

			internal IList<ResourceRequest> lastAsk = null;

			internal IList<ContainerId> lastRelease = null;

			internal IList<string> lastBlacklistAdditions;

			internal IList<string> lastBlacklistRemovals;

			// override this to copy the objects otherwise FifoScheduler updates the
			// numContainers in same objects as kept by RMContainerAllocator
			public override Allocation Allocate(ApplicationAttemptId applicationAttemptId, IList
				<ResourceRequest> ask, IList<ContainerId> release, IList<string> blacklistAdditions
				, IList<string> blacklistRemovals)
			{
				lock (this)
				{
					IList<ResourceRequest> askCopy = new AList<ResourceRequest>();
					foreach (ResourceRequest req in ask)
					{
						ResourceRequest reqCopy = ResourceRequest.NewInstance(req.GetPriority(), req.GetResourceName
							(), req.GetCapability(), req.GetNumContainers(), req.GetRelaxLocality());
						askCopy.AddItem(reqCopy);
					}
					lastAsk = ask;
					lastRelease = release;
					lastBlacklistAdditions = blacklistAdditions;
					lastBlacklistRemovals = blacklistRemovals;
					return base.Allocate(applicationAttemptId, askCopy, release, blacklistAdditions, 
						blacklistRemovals);
				}
			}
		}

		private class MyResourceManager : MockRM
		{
			private static long fakeClusterTimeStamp = Runtime.CurrentTimeMillis();

			public MyResourceManager(Configuration conf, RMStateStore store)
				: base(conf, store)
			{
			}

			/// <exception cref="System.Exception"/>
			protected override void ServiceStart()
			{
				base.ServiceStart();
				// Ensure that the application attempt IDs for all the tests are the same
				// The application attempt IDs will be used as the login user names
				TestAMRMClientOnRMRestart.MyResourceManager.SetClusterTimeStamp(fakeClusterTimeStamp
					);
			}

			protected override Dispatcher CreateDispatcher()
			{
				return new DrainDispatcher();
			}

			protected override EventHandler<SchedulerEvent> CreateSchedulerEventDispatcher()
			{
				// Dispatch inline for test sanity
				return new _EventHandler_570(this);
			}

			private sealed class _EventHandler_570 : EventHandler<SchedulerEvent>
			{
				public _EventHandler_570(MyResourceManager _enclosing)
				{
					this._enclosing = _enclosing;
				}

				public void Handle(SchedulerEvent @event)
				{
					this._enclosing.scheduler.Handle(@event);
				}

				private readonly MyResourceManager _enclosing;
			}

			protected override ResourceScheduler CreateScheduler()
			{
				return new TestAMRMClientOnRMRestart.MyFifoScheduler(this.GetRMContext());
			}

			internal virtual TestAMRMClientOnRMRestart.MyFifoScheduler GetMyFifoScheduler()
			{
				return (TestAMRMClientOnRMRestart.MyFifoScheduler)scheduler;
			}
		}

		private class MyResourceManager2 : TestAMRMClientOnRMRestart.MyResourceManager
		{
			public MyResourceManager2(Configuration conf, RMStateStore store)
				: base(conf, store)
			{
			}

			protected override ApplicationMasterService CreateApplicationMasterService()
			{
				return new ApplicationMasterService(GetRMContext(), scheduler);
			}
		}

		private class MyAMRMClientImpl : AMRMClientImpl<AMRMClient.ContainerRequest>
		{
			private TestAMRMClientOnRMRestart.MyResourceManager rm;

			public MyAMRMClientImpl(TestAMRMClientOnRMRestart.MyResourceManager rm)
			{
				this.rm = rm;
			}

			/// <exception cref="System.Exception"/>
			protected override void ServiceInit(Configuration conf)
			{
				base.ServiceInit(conf);
			}

			/// <exception cref="System.Exception"/>
			protected override void ServiceStart()
			{
				this.rmClient = this.rm.GetApplicationMasterService();
			}

			/// <exception cref="System.Exception"/>
			protected override void ServiceStop()
			{
				rmClient = null;
				base.ServiceStop();
			}

			public virtual void UpdateRMProxy(TestAMRMClientOnRMRestart.MyResourceManager rm)
			{
				rmClient = rm.GetApplicationMasterService();
			}
		}

		private static void AssertBlacklistAdditionsAndRemovals(int expectedAdditions, int
			 expectedRemovals, TestAMRMClientOnRMRestart.MyResourceManager rm)
		{
			NUnit.Framework.Assert.AreEqual(expectedAdditions, rm.GetMyFifoScheduler().lastBlacklistAdditions
				.Count);
			NUnit.Framework.Assert.AreEqual(expectedRemovals, rm.GetMyFifoScheduler().lastBlacklistRemovals
				.Count);
		}

		private static void AssertAsksAndReleases(int expectedAsk, int expectedRelease, TestAMRMClientOnRMRestart.MyResourceManager
			 rm)
		{
			NUnit.Framework.Assert.AreEqual(expectedAsk, rm.GetMyFifoScheduler().lastAsk.Count
				);
			NUnit.Framework.Assert.AreEqual(expectedRelease, rm.GetMyFifoScheduler().lastRelease
				.Count);
		}

		private AMRMClient.ContainerRequest CreateReq(int priority, int memory, string[] 
			hosts)
		{
			Resource capability = Resource.NewInstance(memory, 1);
			Priority priorityOfContainer = Priority.NewInstance(priority);
			return new AMRMClient.ContainerRequest(capability, hosts, new string[] { NetworkTopology
				.DefaultRack }, priorityOfContainer);
		}
	}
}
