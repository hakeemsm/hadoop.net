using System;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Security.Client;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery
{
	public class TestMemoryRMStateStore
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNotifyStoreOperationFailed()
		{
			RMStateStore store = new _MemoryRMStateStore_34();
			Configuration conf = new Configuration();
			conf.SetBoolean(YarnConfiguration.RmHaEnabled, true);
			store.Init(conf);
			ResourceManager mockRM = Org.Mockito.Mockito.Mock<ResourceManager>();
			store.SetResourceManager(mockRM);
			RMDelegationTokenIdentifier mockTokenId = Org.Mockito.Mockito.Mock<RMDelegationTokenIdentifier
				>();
			store.RemoveRMDelegationToken(mockTokenId);
			NUnit.Framework.Assert.IsTrue("RMStateStore should have been in fenced state", store
				.IsFencedState());
			store = new _MemoryRMStateStore_51();
			store.Init(conf);
			store.SetResourceManager(mockRM);
			store.RemoveRMDelegationToken(mockTokenId);
			NUnit.Framework.Assert.IsTrue("RMStateStore should have been in fenced state", store
				.IsFencedState());
		}

		private sealed class _MemoryRMStateStore_34 : MemoryRMStateStore
		{
			public _MemoryRMStateStore_34()
			{
			}

			/// <exception cref="System.Exception"/>
			protected internal override void RemoveRMDelegationTokenState(RMDelegationTokenIdentifier
				 rmDTIdentifier)
			{
				lock (this)
				{
					throw new Exception("testNotifyStoreOperationFailed");
				}
			}
		}

		private sealed class _MemoryRMStateStore_51 : MemoryRMStateStore
		{
			public _MemoryRMStateStore_51()
			{
			}

			public override void RemoveRMDelegationToken(RMDelegationTokenIdentifier rmDTIdentifier
				)
			{
				lock (this)
				{
					this.NotifyStoreOperationFailed(new Exception("testNotifyStoreOperationFailed"));
				}
			}
		}
	}
}
