using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Token.Delegation;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Security.Client;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Log4j;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Security
{
	public class TestRMDelegationTokens
	{
		private YarnConfiguration conf;

		[SetUp]
		public virtual void Setup()
		{
			Logger rootLogger = LogManager.GetRootLogger();
			rootLogger.SetLevel(Level.Debug);
			ExitUtil.DisableSystemExit();
			conf = new YarnConfiguration();
			UserGroupInformation.SetConfiguration(conf);
			conf.Set(YarnConfiguration.RmStore, typeof(MemoryRMStateStore).FullName);
			conf.Set(YarnConfiguration.RmScheduler, typeof(FairScheduler).FullName);
		}

		// Test the DT mast key in the state-store when the mast key is being rolled.
		/// <exception cref="System.Exception"/>
		public virtual void TestRMDTMasterKeyStateOnRollingMasterKey()
		{
			MemoryRMStateStore memStore = new MemoryRMStateStore();
			memStore.Init(conf);
			RMStateStore.RMState rmState = memStore.GetState();
			IDictionary<RMDelegationTokenIdentifier, long> rmDTState = rmState.GetRMDTSecretManagerState
				().GetTokenState();
			ICollection<DelegationKey> rmDTMasterKeyState = rmState.GetRMDTSecretManagerState
				().GetMasterKeyState();
			MockRM rm1 = new TestRMDelegationTokens.MyMockRM(this, conf, memStore);
			rm1.Start();
			// on rm start, two master keys are created.
			// One is created at RMDTSecretMgr.startThreads.updateCurrentKey();
			// the other is created on the first run of
			// tokenRemoverThread.rollMasterKey()
			RMDelegationTokenSecretManager dtSecretManager = rm1.GetRMContext().GetRMDelegationTokenSecretManager
				();
			// assert all master keys are saved
			NUnit.Framework.Assert.AreEqual(dtSecretManager.GetAllMasterKeys(), rmDTMasterKeyState
				);
			ICollection<DelegationKey> expiringKeys = new HashSet<DelegationKey>();
			Sharpen.Collections.AddAll(expiringKeys, dtSecretManager.GetAllMasterKeys());
			// request to generate a RMDelegationToken
			GetDelegationTokenRequest request = Org.Mockito.Mockito.Mock<GetDelegationTokenRequest
				>();
			Org.Mockito.Mockito.When(request.GetRenewer()).ThenReturn("renewer1");
			GetDelegationTokenResponse response = rm1.GetClientRMService().GetDelegationToken
				(request);
			Org.Apache.Hadoop.Yarn.Api.Records.Token delegationToken = response.GetRMDelegationToken
				();
			Org.Apache.Hadoop.Security.Token.Token<RMDelegationTokenIdentifier> token1 = ConverterUtils
				.ConvertFromYarn(delegationToken, (Text)null);
			RMDelegationTokenIdentifier dtId1 = token1.DecodeIdentifier();
			// For all keys that still remain in memory, we should have them stored
			// in state-store also.
			while (((TestRMDelegationTokens.TestRMDelegationTokenSecretManager)dtSecretManager
				).numUpdatedKeys.Get() < 3)
			{
				((TestRMDelegationTokens.TestRMDelegationTokenSecretManager)dtSecretManager).CheckCurrentKeyInStateStore
					(rmDTMasterKeyState);
				Sharpen.Thread.Sleep(100);
			}
			// wait for token to expire and remove from state-store
			// rollMasterKey is called every 1 second.
			int count = 0;
			while (rmDTState.Contains(dtId1) && count < 100)
			{
				Sharpen.Thread.Sleep(100);
				count++;
			}
			rm1.Stop();
		}

		// Test all expired keys are removed from state-store.
		/// <exception cref="System.Exception"/>
		public virtual void TestRemoveExpiredMasterKeyInRMStateStore()
		{
			MemoryRMStateStore memStore = new MemoryRMStateStore();
			memStore.Init(conf);
			RMStateStore.RMState rmState = memStore.GetState();
			ICollection<DelegationKey> rmDTMasterKeyState = rmState.GetRMDTSecretManagerState
				().GetMasterKeyState();
			MockRM rm1 = new TestRMDelegationTokens.MyMockRM(this, conf, memStore);
			rm1.Start();
			RMDelegationTokenSecretManager dtSecretManager = rm1.GetRMContext().GetRMDelegationTokenSecretManager
				();
			// assert all master keys are saved
			NUnit.Framework.Assert.AreEqual(dtSecretManager.GetAllMasterKeys(), rmDTMasterKeyState
				);
			ICollection<DelegationKey> expiringKeys = new HashSet<DelegationKey>();
			Sharpen.Collections.AddAll(expiringKeys, dtSecretManager.GetAllMasterKeys());
			// wait for expiringKeys to expire
			while (true)
			{
				bool allExpired = true;
				foreach (DelegationKey key in expiringKeys)
				{
					if (rmDTMasterKeyState.Contains(key))
					{
						allExpired = false;
					}
				}
				if (allExpired)
				{
					break;
				}
				Sharpen.Thread.Sleep(500);
			}
		}

		internal class MyMockRM : TestRMRestart.TestSecurityMockRM
		{
			public MyMockRM(TestRMDelegationTokens _enclosing, Configuration conf, RMStateStore
				 store)
				: base(conf, store)
			{
				this._enclosing = _enclosing;
			}

			protected internal override RMSecretManagerService CreateRMSecretManagerService()
			{
				return new _RMSecretManagerService_169(this._enclosing.conf, this.rmContext);
			}

			private sealed class _RMSecretManagerService_169 : RMSecretManagerService
			{
				public _RMSecretManagerService_169(Configuration baseArg1, RMContextImpl baseArg2
					)
					: base(baseArg1, baseArg2)
				{
				}

				protected internal override RMDelegationTokenSecretManager CreateRMDelegationTokenSecretManager
					(Configuration conf, RMContext rmContext)
				{
					// KeyUpdateInterval-> 1 seconds
					// TokenMaxLifetime-> 2 seconds.
					return new TestRMDelegationTokens.TestRMDelegationTokenSecretManager(this, 1000, 
						1000, 2000, 1000, rmContext);
				}
			}

			private readonly TestRMDelegationTokens _enclosing;
		}

		public class TestRMDelegationTokenSecretManager : RMDelegationTokenSecretManager
		{
			public AtomicInteger numUpdatedKeys = new AtomicInteger(0);

			public TestRMDelegationTokenSecretManager(TestRMDelegationTokens _enclosing, long
				 delegationKeyUpdateInterval, long delegationTokenMaxLifetime, long delegationTokenRenewInterval
				, long delegationTokenRemoverScanInterval, RMContext rmContext)
				: base(delegationKeyUpdateInterval, delegationTokenMaxLifetime, delegationTokenRenewInterval
					, delegationTokenRemoverScanInterval, rmContext)
			{
				this._enclosing = _enclosing;
			}

			protected override void StoreNewMasterKey(DelegationKey newKey)
			{
				base.StoreNewMasterKey(newKey);
				this.numUpdatedKeys.IncrementAndGet();
			}

			public virtual DelegationKey CheckCurrentKeyInStateStore(ICollection<DelegationKey
				> rmDTMasterKeyState)
			{
				lock (this)
				{
					foreach (int keyId in this._enclosing._enclosing.allKeys.Keys)
					{
						if (keyId == this._enclosing._enclosing.currentId)
						{
							DelegationKey currentKey = this._enclosing._enclosing.allKeys[keyId];
							NUnit.Framework.Assert.IsTrue(rmDTMasterKeyState.Contains(currentKey));
							return currentKey;
						}
					}
					return null;
				}
			}

			private readonly TestRMDelegationTokens _enclosing;
		}
	}
}
