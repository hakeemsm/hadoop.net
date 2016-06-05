using System;
using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.HA;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security.Token.Delegation;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Security;
using Org.Apache.Hadoop.Yarn.Security.Client;
using Org.Apache.Hadoop.Yarn.Server.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Security;
using Org.Apache.Hadoop.Yarn.Server.Security;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery
{
	public class RMStateStoreTestBase : ClientBaseWithFixes
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(RMStateStoreTestBase));

		internal class TestDispatcher : Dispatcher, EventHandler<Org.Apache.Hadoop.Yarn.Event.Event
			>
		{
			internal ApplicationAttemptId attemptId;

			internal bool notified = false;

			public virtual void Register(Type eventType, EventHandler handler)
			{
			}

			public virtual void Handle(Org.Apache.Hadoop.Yarn.Event.Event @event)
			{
				if (@event is RMAppAttemptEvent)
				{
					RMAppAttemptEvent rmAppAttemptEvent = (RMAppAttemptEvent)@event;
					NUnit.Framework.Assert.AreEqual(attemptId, rmAppAttemptEvent.GetApplicationAttemptId
						());
				}
				notified = true;
				lock (this)
				{
					Sharpen.Runtime.NotifyAll(this);
				}
			}

			public virtual EventHandler GetEventHandler()
			{
				return this;
			}
		}

		public class StoreStateVerifier
		{
			internal virtual void AfterStoreApp(RMStateStore store, ApplicationId appId)
			{
			}

			internal virtual void AfterStoreAppAttempt(RMStateStore store, ApplicationAttemptId
				 appAttId)
			{
			}
		}

		internal interface RMStateStoreHelper
		{
			/// <exception cref="System.Exception"/>
			RMStateStore GetRMStateStore();

			/// <exception cref="System.Exception"/>
			bool IsFinalStateValid();

			/// <exception cref="System.Exception"/>
			void WriteVersion(Version version);

			/// <exception cref="System.Exception"/>
			Version GetCurrentVersion();

			/// <exception cref="System.Exception"/>
			bool AppExists(RMApp app);
		}

		internal virtual void WaitNotify(RMStateStoreTestBase.TestDispatcher dispatcher)
		{
			long startTime = Runtime.CurrentTimeMillis();
			while (!dispatcher.notified)
			{
				lock (dispatcher)
				{
					try
					{
						Sharpen.Runtime.Wait(dispatcher, 1000);
					}
					catch (Exception e)
					{
						Sharpen.Runtime.PrintStackTrace(e);
					}
				}
				if (Runtime.CurrentTimeMillis() - startTime > 1000 * 60)
				{
					NUnit.Framework.Assert.Fail("Timed out attempt store notification");
				}
			}
			dispatcher.notified = false;
		}

		/// <exception cref="System.Exception"/>
		protected internal virtual RMApp StoreApp(RMStateStore store, ApplicationId appId
			, long submitTime, long startTime)
		{
			ApplicationSubmissionContext context = new ApplicationSubmissionContextPBImpl();
			context.SetApplicationId(appId);
			RMApp mockApp = Org.Mockito.Mockito.Mock<RMApp>();
			Org.Mockito.Mockito.When(mockApp.GetApplicationId()).ThenReturn(appId);
			Org.Mockito.Mockito.When(mockApp.GetSubmitTime()).ThenReturn(submitTime);
			Org.Mockito.Mockito.When(mockApp.GetStartTime()).ThenReturn(startTime);
			Org.Mockito.Mockito.When(mockApp.GetApplicationSubmissionContext()).ThenReturn(context
				);
			Org.Mockito.Mockito.When(mockApp.GetUser()).ThenReturn("test");
			store.StoreNewApplication(mockApp);
			return mockApp;
		}

		/// <exception cref="System.Exception"/>
		protected internal virtual ContainerId StoreAttempt(RMStateStore store, ApplicationAttemptId
			 attemptId, string containerIdStr, Org.Apache.Hadoop.Security.Token.Token<AMRMTokenIdentifier
			> appToken, SecretKey clientTokenMasterKey, RMStateStoreTestBase.TestDispatcher 
			dispatcher)
		{
			RMAppAttemptMetrics mockRmAppAttemptMetrics = Org.Mockito.Mockito.Mock<RMAppAttemptMetrics
				>();
			Container container = new ContainerPBImpl();
			container.SetId(ConverterUtils.ToContainerId(containerIdStr));
			RMAppAttempt mockAttempt = Org.Mockito.Mockito.Mock<RMAppAttempt>();
			Org.Mockito.Mockito.When(mockAttempt.GetAppAttemptId()).ThenReturn(attemptId);
			Org.Mockito.Mockito.When(mockAttempt.GetMasterContainer()).ThenReturn(container);
			Org.Mockito.Mockito.When(mockAttempt.GetAMRMToken()).ThenReturn(appToken);
			Org.Mockito.Mockito.When(mockAttempt.GetClientTokenMasterKey()).ThenReturn(clientTokenMasterKey
				);
			Org.Mockito.Mockito.When(mockAttempt.GetRMAppAttemptMetrics()).ThenReturn(mockRmAppAttemptMetrics
				);
			Org.Mockito.Mockito.When(mockRmAppAttemptMetrics.GetAggregateAppResourceUsage()).
				ThenReturn(new AggregateAppResourceUsage(0, 0));
			dispatcher.attemptId = attemptId;
			store.StoreNewApplicationAttempt(mockAttempt);
			WaitNotify(dispatcher);
			return container.GetId();
		}

		/// <exception cref="System.Exception"/>
		internal virtual void TestRMAppStateStore(RMStateStoreTestBase.RMStateStoreHelper
			 stateStoreHelper)
		{
			TestRMAppStateStore(stateStoreHelper, new RMStateStoreTestBase.StoreStateVerifier
				());
		}

		/// <exception cref="System.Exception"/>
		internal virtual void TestRMAppStateStore(RMStateStoreTestBase.RMStateStoreHelper
			 stateStoreHelper, RMStateStoreTestBase.StoreStateVerifier verifier)
		{
			long submitTime = Runtime.CurrentTimeMillis();
			long startTime = Runtime.CurrentTimeMillis() + 1234;
			Configuration conf = new YarnConfiguration();
			RMStateStore store = stateStoreHelper.GetRMStateStore();
			RMStateStoreTestBase.TestDispatcher dispatcher = new RMStateStoreTestBase.TestDispatcher
				();
			store.SetRMDispatcher(dispatcher);
			RMContext rmContext = Org.Mockito.Mockito.Mock<RMContext>();
			Org.Mockito.Mockito.When(rmContext.GetStateStore()).ThenReturn(store);
			AMRMTokenSecretManager appTokenMgr = Org.Mockito.Mockito.Spy(new AMRMTokenSecretManager
				(conf, rmContext));
			MasterKeyData masterKeyData = appTokenMgr.CreateNewMasterKey();
			Org.Mockito.Mockito.When(appTokenMgr.GetMasterKey()).ThenReturn(masterKeyData);
			ClientToAMTokenSecretManagerInRM clientToAMTokenMgr = new ClientToAMTokenSecretManagerInRM
				();
			ApplicationAttemptId attemptId1 = ConverterUtils.ToApplicationAttemptId("appattempt_1352994193343_0001_000001"
				);
			ApplicationId appId1 = attemptId1.GetApplicationId();
			StoreApp(store, appId1, submitTime, startTime);
			verifier.AfterStoreApp(store, appId1);
			// create application token and client token key for attempt1
			Org.Apache.Hadoop.Security.Token.Token<AMRMTokenIdentifier> appAttemptToken1 = GenerateAMRMToken
				(attemptId1, appTokenMgr);
			SecretKey clientTokenKey1 = clientToAMTokenMgr.CreateMasterKey(attemptId1);
			ContainerId containerId1 = StoreAttempt(store, attemptId1, "container_1352994193343_0001_01_000001"
				, appAttemptToken1, clientTokenKey1, dispatcher);
			string appAttemptIdStr2 = "appattempt_1352994193343_0001_000002";
			ApplicationAttemptId attemptId2 = ConverterUtils.ToApplicationAttemptId(appAttemptIdStr2
				);
			// create application token and client token key for attempt2
			Org.Apache.Hadoop.Security.Token.Token<AMRMTokenIdentifier> appAttemptToken2 = GenerateAMRMToken
				(attemptId2, appTokenMgr);
			SecretKey clientTokenKey2 = clientToAMTokenMgr.CreateMasterKey(attemptId2);
			ContainerId containerId2 = StoreAttempt(store, attemptId2, "container_1352994193343_0001_02_000001"
				, appAttemptToken2, clientTokenKey2, dispatcher);
			ApplicationAttemptId attemptIdRemoved = ConverterUtils.ToApplicationAttemptId("appattempt_1352994193343_0002_000001"
				);
			ApplicationId appIdRemoved = attemptIdRemoved.GetApplicationId();
			StoreApp(store, appIdRemoved, submitTime, startTime);
			StoreAttempt(store, attemptIdRemoved, "container_1352994193343_0002_01_000001", null
				, null, dispatcher);
			verifier.AfterStoreAppAttempt(store, attemptIdRemoved);
			RMApp mockRemovedApp = Org.Mockito.Mockito.Mock<RMApp>();
			RMAppAttemptMetrics mockRmAppAttemptMetrics = Org.Mockito.Mockito.Mock<RMAppAttemptMetrics
				>();
			Dictionary<ApplicationAttemptId, RMAppAttempt> attempts = new Dictionary<ApplicationAttemptId
				, RMAppAttempt>();
			ApplicationSubmissionContext context = new ApplicationSubmissionContextPBImpl();
			context.SetApplicationId(appIdRemoved);
			Org.Mockito.Mockito.When(mockRemovedApp.GetSubmitTime()).ThenReturn(submitTime);
			Org.Mockito.Mockito.When(mockRemovedApp.GetApplicationSubmissionContext()).ThenReturn
				(context);
			Org.Mockito.Mockito.When(mockRemovedApp.GetAppAttempts()).ThenReturn(attempts);
			Org.Mockito.Mockito.When(mockRemovedApp.GetUser()).ThenReturn("user1");
			RMAppAttempt mockRemovedAttempt = Org.Mockito.Mockito.Mock<RMAppAttempt>();
			Org.Mockito.Mockito.When(mockRemovedAttempt.GetAppAttemptId()).ThenReturn(attemptIdRemoved
				);
			Org.Mockito.Mockito.When(mockRemovedAttempt.GetRMAppAttemptMetrics()).ThenReturn(
				mockRmAppAttemptMetrics);
			Org.Mockito.Mockito.When(mockRmAppAttemptMetrics.GetAggregateAppResourceUsage()).
				ThenReturn(new AggregateAppResourceUsage(0, 0));
			attempts[attemptIdRemoved] = mockRemovedAttempt;
			store.RemoveApplication(mockRemovedApp);
			// remove application directory recursively.
			StoreApp(store, appIdRemoved, submitTime, startTime);
			StoreAttempt(store, attemptIdRemoved, "container_1352994193343_0002_01_000001", null
				, null, dispatcher);
			store.RemoveApplication(mockRemovedApp);
			// let things settle down
			Sharpen.Thread.Sleep(1000);
			store.Close();
			// give tester a chance to modify app state in the store
			ModifyAppState();
			// load state
			store = stateStoreHelper.GetRMStateStore();
			store.SetRMDispatcher(dispatcher);
			RMStateStore.RMState state = store.LoadState();
			IDictionary<ApplicationId, ApplicationStateData> rmAppState = state.GetApplicationState
				();
			ApplicationStateData appState = rmAppState[appId1];
			// app is loaded
			NUnit.Framework.Assert.IsNotNull(appState);
			// app is loaded correctly
			NUnit.Framework.Assert.AreEqual(submitTime, appState.GetSubmitTime());
			NUnit.Framework.Assert.AreEqual(startTime, appState.GetStartTime());
			// submission context is loaded correctly
			NUnit.Framework.Assert.AreEqual(appId1, appState.GetApplicationSubmissionContext(
				).GetApplicationId());
			ApplicationAttemptStateData attemptState = appState.GetAttempt(attemptId1);
			// attempt1 is loaded correctly
			NUnit.Framework.Assert.IsNotNull(attemptState);
			NUnit.Framework.Assert.AreEqual(attemptId1, attemptState.GetAttemptId());
			NUnit.Framework.Assert.AreEqual(-1000, attemptState.GetAMContainerExitStatus());
			// attempt1 container is loaded correctly
			NUnit.Framework.Assert.AreEqual(containerId1, attemptState.GetMasterContainer().GetId
				());
			// attempt1 client token master key is loaded correctly
			Assert.AssertArrayEquals(clientTokenKey1.GetEncoded(), attemptState.GetAppAttemptTokens
				().GetSecretKey(RMStateStore.AmClientTokenMasterKeyName));
			attemptState = appState.GetAttempt(attemptId2);
			// attempt2 is loaded correctly
			NUnit.Framework.Assert.IsNotNull(attemptState);
			NUnit.Framework.Assert.AreEqual(attemptId2, attemptState.GetAttemptId());
			// attempt2 container is loaded correctly
			NUnit.Framework.Assert.AreEqual(containerId2, attemptState.GetMasterContainer().GetId
				());
			// attempt2 client token master key is loaded correctly
			Assert.AssertArrayEquals(clientTokenKey2.GetEncoded(), attemptState.GetAppAttemptTokens
				().GetSecretKey(RMStateStore.AmClientTokenMasterKeyName));
			//******* update application/attempt state *******//
			ApplicationStateData appState2 = ApplicationStateData.NewInstance(appState.GetSubmitTime
				(), appState.GetStartTime(), appState.GetUser(), appState.GetApplicationSubmissionContext
				(), RMAppState.Finished, "appDiagnostics", 1234);
			appState2.attempts.PutAll(appState.attempts);
			store.UpdateApplicationState(appState2);
			ApplicationAttemptStateData oldAttemptState = attemptState;
			ApplicationAttemptStateData newAttemptState = ApplicationAttemptStateData.NewInstance
				(oldAttemptState.GetAttemptId(), oldAttemptState.GetMasterContainer(), oldAttemptState
				.GetAppAttemptTokens(), oldAttemptState.GetStartTime(), RMAppAttemptState.Finished
				, "myTrackingUrl", "attemptDiagnostics", FinalApplicationStatus.Succeeded, 100, 
				oldAttemptState.GetFinishTime(), 0, 0);
			store.UpdateApplicationAttemptState(newAttemptState);
			// test updating the state of an app/attempt whose initial state was not
			// saved.
			ApplicationId dummyAppId = ApplicationId.NewInstance(1234, 10);
			ApplicationSubmissionContext dummyContext = new ApplicationSubmissionContextPBImpl
				();
			dummyContext.SetApplicationId(dummyAppId);
			ApplicationStateData dummyApp = ApplicationStateData.NewInstance(appState.GetSubmitTime
				(), appState.GetStartTime(), appState.GetUser(), dummyContext, RMAppState.Finished
				, "appDiagnostics", 1234);
			store.UpdateApplicationState(dummyApp);
			ApplicationAttemptId dummyAttemptId = ApplicationAttemptId.NewInstance(dummyAppId
				, 6);
			ApplicationAttemptStateData dummyAttempt = ApplicationAttemptStateData.NewInstance
				(dummyAttemptId, oldAttemptState.GetMasterContainer(), oldAttemptState.GetAppAttemptTokens
				(), oldAttemptState.GetStartTime(), RMAppAttemptState.Finished, "myTrackingUrl", 
				"attemptDiagnostics", FinalApplicationStatus.Succeeded, 111, oldAttemptState.GetFinishTime
				(), 0, 0);
			store.UpdateApplicationAttemptState(dummyAttempt);
			// let things settle down
			Sharpen.Thread.Sleep(1000);
			store.Close();
			// check updated application state.
			store = stateStoreHelper.GetRMStateStore();
			store.SetRMDispatcher(dispatcher);
			RMStateStore.RMState newRMState = store.LoadState();
			IDictionary<ApplicationId, ApplicationStateData> newRMAppState = newRMState.GetApplicationState
				();
			NUnit.Framework.Assert.IsNotNull(newRMAppState[dummyApp.GetApplicationSubmissionContext
				().GetApplicationId()]);
			ApplicationStateData updatedAppState = newRMAppState[appId1];
			NUnit.Framework.Assert.AreEqual(appState.GetApplicationSubmissionContext().GetApplicationId
				(), updatedAppState.GetApplicationSubmissionContext().GetApplicationId());
			NUnit.Framework.Assert.AreEqual(appState.GetSubmitTime(), updatedAppState.GetSubmitTime
				());
			NUnit.Framework.Assert.AreEqual(appState.GetStartTime(), updatedAppState.GetStartTime
				());
			NUnit.Framework.Assert.AreEqual(appState.GetUser(), updatedAppState.GetUser());
			// new app state fields
			NUnit.Framework.Assert.AreEqual(RMAppState.Finished, updatedAppState.GetState());
			NUnit.Framework.Assert.AreEqual("appDiagnostics", updatedAppState.GetDiagnostics(
				));
			NUnit.Framework.Assert.AreEqual(1234, updatedAppState.GetFinishTime());
			// check updated attempt state
			NUnit.Framework.Assert.IsNotNull(newRMAppState[dummyApp.GetApplicationSubmissionContext
				().GetApplicationId()].GetAttempt(dummyAttemptId));
			ApplicationAttemptStateData updatedAttemptState = updatedAppState.GetAttempt(newAttemptState
				.GetAttemptId());
			NUnit.Framework.Assert.AreEqual(oldAttemptState.GetAttemptId(), updatedAttemptState
				.GetAttemptId());
			NUnit.Framework.Assert.AreEqual(containerId2, updatedAttemptState.GetMasterContainer
				().GetId());
			Assert.AssertArrayEquals(clientTokenKey2.GetEncoded(), attemptState.GetAppAttemptTokens
				().GetSecretKey(RMStateStore.AmClientTokenMasterKeyName));
			// new attempt state fields
			NUnit.Framework.Assert.AreEqual(RMAppAttemptState.Finished, updatedAttemptState.GetState
				());
			NUnit.Framework.Assert.AreEqual("myTrackingUrl", updatedAttemptState.GetFinalTrackingUrl
				());
			NUnit.Framework.Assert.AreEqual("attemptDiagnostics", updatedAttemptState.GetDiagnostics
				());
			NUnit.Framework.Assert.AreEqual(100, updatedAttemptState.GetAMContainerExitStatus
				());
			NUnit.Framework.Assert.AreEqual(FinalApplicationStatus.Succeeded, updatedAttemptState
				.GetFinalApplicationStatus());
			// assert store is in expected state after everything is cleaned
			NUnit.Framework.Assert.IsTrue(stateStoreHelper.IsFinalStateValid());
			store.Close();
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRMDTSecretManagerStateStore(RMStateStoreTestBase.RMStateStoreHelper
			 stateStoreHelper)
		{
			RMStateStore store = stateStoreHelper.GetRMStateStore();
			RMStateStoreTestBase.TestDispatcher dispatcher = new RMStateStoreTestBase.TestDispatcher
				();
			store.SetRMDispatcher(dispatcher);
			// store RM delegation token;
			RMDelegationTokenIdentifier dtId1 = new RMDelegationTokenIdentifier(new Text("owner1"
				), new Text("renewer1"), new Text("realuser1"));
			int sequenceNumber = 1111;
			dtId1.SetSequenceNumber(sequenceNumber);
			byte[] tokenBeforeStore = dtId1.GetBytes();
			long renewDate1 = Runtime.CurrentTimeMillis();
			store.StoreRMDelegationToken(dtId1, renewDate1);
			ModifyRMDelegationTokenState();
			IDictionary<RMDelegationTokenIdentifier, long> token1 = new Dictionary<RMDelegationTokenIdentifier
				, long>();
			token1[dtId1] = renewDate1;
			// store delegation key;
			DelegationKey key = new DelegationKey(1234, 4321, Sharpen.Runtime.GetBytesForString
				("keyBytes"));
			HashSet<DelegationKey> keySet = new HashSet<DelegationKey>();
			keySet.AddItem(key);
			store.StoreRMDTMasterKey(key);
			RMStateStore.RMDTSecretManagerState secretManagerState = store.LoadState().GetRMDTSecretManagerState
				();
			NUnit.Framework.Assert.AreEqual(token1, secretManagerState.GetTokenState());
			NUnit.Framework.Assert.AreEqual(keySet, secretManagerState.GetMasterKeyState());
			NUnit.Framework.Assert.AreEqual(sequenceNumber, secretManagerState.GetDTSequenceNumber
				());
			RMDelegationTokenIdentifier tokenAfterStore = secretManagerState.GetTokenState().
				Keys.GetEnumerator().Next();
			NUnit.Framework.Assert.IsTrue(Arrays.Equals(tokenBeforeStore, tokenAfterStore.GetBytes
				()));
			// update RM delegation token;
			renewDate1 = Runtime.CurrentTimeMillis();
			store.UpdateRMDelegationToken(dtId1, renewDate1);
			token1[dtId1] = renewDate1;
			RMStateStore.RMDTSecretManagerState updateSecretManagerState = store.LoadState().
				GetRMDTSecretManagerState();
			NUnit.Framework.Assert.AreEqual(token1, updateSecretManagerState.GetTokenState());
			NUnit.Framework.Assert.AreEqual(keySet, updateSecretManagerState.GetMasterKeyState
				());
			NUnit.Framework.Assert.AreEqual(sequenceNumber, updateSecretManagerState.GetDTSequenceNumber
				());
			// check to delete delegationKey
			store.RemoveRMDTMasterKey(key);
			keySet.Clear();
			RMStateStore.RMDTSecretManagerState noKeySecretManagerState = store.LoadState().GetRMDTSecretManagerState
				();
			NUnit.Framework.Assert.AreEqual(token1, noKeySecretManagerState.GetTokenState());
			NUnit.Framework.Assert.AreEqual(keySet, noKeySecretManagerState.GetMasterKeyState
				());
			NUnit.Framework.Assert.AreEqual(sequenceNumber, noKeySecretManagerState.GetDTSequenceNumber
				());
			// check to delete delegationToken
			store.RemoveRMDelegationToken(dtId1);
			RMStateStore.RMDTSecretManagerState noKeyAndTokenSecretManagerState = store.LoadState
				().GetRMDTSecretManagerState();
			token1.Clear();
			NUnit.Framework.Assert.AreEqual(token1, noKeyAndTokenSecretManagerState.GetTokenState
				());
			NUnit.Framework.Assert.AreEqual(keySet, noKeyAndTokenSecretManagerState.GetMasterKeyState
				());
			NUnit.Framework.Assert.AreEqual(sequenceNumber, noKeySecretManagerState.GetDTSequenceNumber
				());
			store.Close();
		}

		protected internal virtual Org.Apache.Hadoop.Security.Token.Token<AMRMTokenIdentifier
			> GenerateAMRMToken(ApplicationAttemptId attemptId, AMRMTokenSecretManager appTokenMgr
			)
		{
			Org.Apache.Hadoop.Security.Token.Token<AMRMTokenIdentifier> appToken = appTokenMgr
				.CreateAndGetAMRMToken(attemptId);
			appToken.SetService(new Text("appToken service"));
			return appToken;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestCheckVersion(RMStateStoreTestBase.RMStateStoreHelper stateStoreHelper
			)
		{
			RMStateStore store = stateStoreHelper.GetRMStateStore();
			store.SetRMDispatcher(new RMStateStoreTestBase.TestDispatcher());
			// default version
			Version defaultVersion = stateStoreHelper.GetCurrentVersion();
			store.CheckVersion();
			NUnit.Framework.Assert.AreEqual(defaultVersion, store.LoadVersion());
			// compatible version
			Version compatibleVersion = Version.NewInstance(defaultVersion.GetMajorVersion(), 
				defaultVersion.GetMinorVersion() + 2);
			stateStoreHelper.WriteVersion(compatibleVersion);
			NUnit.Framework.Assert.AreEqual(compatibleVersion, store.LoadVersion());
			store.CheckVersion();
			// overwrite the compatible version
			NUnit.Framework.Assert.AreEqual(defaultVersion, store.LoadVersion());
			// incompatible version
			Version incompatibleVersion = Version.NewInstance(defaultVersion.GetMajorVersion(
				) + 2, defaultVersion.GetMinorVersion());
			stateStoreHelper.WriteVersion(incompatibleVersion);
			try
			{
				store.CheckVersion();
				NUnit.Framework.Assert.Fail("Invalid version, should fail.");
			}
			catch (Exception t)
			{
				NUnit.Framework.Assert.IsTrue(t is RMStateVersionIncompatibleException);
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestEpoch(RMStateStoreTestBase.RMStateStoreHelper stateStoreHelper
			)
		{
			RMStateStore store = stateStoreHelper.GetRMStateStore();
			store.SetRMDispatcher(new RMStateStoreTestBase.TestDispatcher());
			long firstTimeEpoch = store.GetAndIncrementEpoch();
			NUnit.Framework.Assert.AreEqual(0, firstTimeEpoch);
			long secondTimeEpoch = store.GetAndIncrementEpoch();
			NUnit.Framework.Assert.AreEqual(1, secondTimeEpoch);
			long thirdTimeEpoch = store.GetAndIncrementEpoch();
			NUnit.Framework.Assert.AreEqual(2, thirdTimeEpoch);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestAppDeletion(RMStateStoreTestBase.RMStateStoreHelper stateStoreHelper
			)
		{
			RMStateStore store = stateStoreHelper.GetRMStateStore();
			store.SetRMDispatcher(new RMStateStoreTestBase.TestDispatcher());
			AList<RMApp> appList = CreateAndStoreApps(stateStoreHelper, store, 5);
			foreach (RMApp app in appList)
			{
				// remove the app
				store.RemoveApplication(app);
				// wait for app to be removed.
				while (true)
				{
					if (!stateStoreHelper.AppExists(app))
					{
						break;
					}
					else
					{
						Sharpen.Thread.Sleep(100);
					}
				}
			}
		}

		/// <exception cref="System.Exception"/>
		private AList<RMApp> CreateAndStoreApps(RMStateStoreTestBase.RMStateStoreHelper stateStoreHelper
			, RMStateStore store, int numApps)
		{
			AList<RMApp> appList = new AList<RMApp>();
			for (int i = 0; i < numApps; i++)
			{
				ApplicationId appId = ApplicationId.NewInstance(1383183338, i);
				RMApp app = StoreApp(store, appId, 123456789, 987654321);
				appList.AddItem(app);
			}
			NUnit.Framework.Assert.AreEqual(numApps, appList.Count);
			foreach (RMApp app_1 in appList)
			{
				// wait for app to be stored.
				while (true)
				{
					if (stateStoreHelper.AppExists(app_1))
					{
						break;
					}
					else
					{
						Sharpen.Thread.Sleep(100);
					}
				}
			}
			return appList;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestDeleteStore(RMStateStoreTestBase.RMStateStoreHelper stateStoreHelper
			)
		{
			RMStateStore store = stateStoreHelper.GetRMStateStore();
			AList<RMApp> appList = CreateAndStoreApps(stateStoreHelper, store, 5);
			store.DeleteStore();
			// verify apps deleted
			foreach (RMApp app in appList)
			{
				NUnit.Framework.Assert.IsFalse(stateStoreHelper.AppExists(app));
			}
		}

		/// <exception cref="System.Exception"/>
		protected internal virtual void ModifyAppState()
		{
		}

		/// <exception cref="System.Exception"/>
		protected internal virtual void ModifyRMDelegationTokenState()
		{
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestAMRMTokenSecretManagerStateStore(RMStateStoreTestBase.RMStateStoreHelper
			 stateStoreHelper)
		{
			System.Console.Out.WriteLine("Start testing");
			RMStateStore store = stateStoreHelper.GetRMStateStore();
			RMStateStoreTestBase.TestDispatcher dispatcher = new RMStateStoreTestBase.TestDispatcher
				();
			store.SetRMDispatcher(dispatcher);
			RMContext rmContext = Org.Mockito.Mockito.Mock<RMContext>();
			Org.Mockito.Mockito.When(rmContext.GetStateStore()).ThenReturn(store);
			Configuration conf = new YarnConfiguration();
			AMRMTokenSecretManager appTokenMgr = new AMRMTokenSecretManager(conf, rmContext);
			//create and save the first masterkey
			MasterKeyData firstMasterKeyData = appTokenMgr.CreateNewMasterKey();
			AMRMTokenSecretManagerState state1 = AMRMTokenSecretManagerState.NewInstance(firstMasterKeyData
				.GetMasterKey(), null);
			rmContext.GetStateStore().StoreOrUpdateAMRMTokenSecretManager(state1, false);
			// load state
			store = stateStoreHelper.GetRMStateStore();
			Org.Mockito.Mockito.When(rmContext.GetStateStore()).ThenReturn(store);
			store.SetRMDispatcher(dispatcher);
			RMStateStore.RMState state = store.LoadState();
			NUnit.Framework.Assert.IsNotNull(state.GetAMRMTokenSecretManagerState());
			NUnit.Framework.Assert.AreEqual(firstMasterKeyData.GetMasterKey(), state.GetAMRMTokenSecretManagerState
				().GetCurrentMasterKey());
			NUnit.Framework.Assert.IsNull(state.GetAMRMTokenSecretManagerState().GetNextMasterKey
				());
			//create and save the second masterkey
			MasterKeyData secondMasterKeyData = appTokenMgr.CreateNewMasterKey();
			AMRMTokenSecretManagerState state2 = AMRMTokenSecretManagerState.NewInstance(firstMasterKeyData
				.GetMasterKey(), secondMasterKeyData.GetMasterKey());
			rmContext.GetStateStore().StoreOrUpdateAMRMTokenSecretManager(state2, true);
			// load state
			store = stateStoreHelper.GetRMStateStore();
			Org.Mockito.Mockito.When(rmContext.GetStateStore()).ThenReturn(store);
			store.SetRMDispatcher(dispatcher);
			RMStateStore.RMState state_2 = store.LoadState();
			NUnit.Framework.Assert.IsNotNull(state_2.GetAMRMTokenSecretManagerState());
			NUnit.Framework.Assert.AreEqual(firstMasterKeyData.GetMasterKey(), state_2.GetAMRMTokenSecretManagerState
				().GetCurrentMasterKey());
			NUnit.Framework.Assert.AreEqual(secondMasterKeyData.GetMasterKey(), state_2.GetAMRMTokenSecretManagerState
				().GetNextMasterKey());
			// re-create the masterKeyData based on the recovered masterkey
			// should have the same secretKey
			appTokenMgr.Recover(state_2);
			NUnit.Framework.Assert.AreEqual(appTokenMgr.GetCurrnetMasterKeyData().GetSecretKey
				(), firstMasterKeyData.GetSecretKey());
			NUnit.Framework.Assert.AreEqual(appTokenMgr.GetNextMasterKeyData().GetSecretKey()
				, secondMasterKeyData.GetSecretKey());
			store.Close();
		}
	}
}
