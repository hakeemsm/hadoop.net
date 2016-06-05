using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce.V2.Api;
using Org.Apache.Hadoop.Mapreduce.V2.Jobhistory;
using Org.Apache.Hadoop.Security.Token.Delegation;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Yarn.Server.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.HS
{
	public class TestHistoryServerLeveldbStateStoreService
	{
		private static readonly FilePath testDir = new FilePath(Runtime.GetProperty("test.build.data"
			, Runtime.GetProperty("java.io.tmpdir")), "TestHistoryServerLeveldbSystemStateStoreService"
			);

		private Configuration conf;

		[SetUp]
		public virtual void Setup()
		{
			FileUtil.FullyDelete(testDir);
			testDir.Mkdirs();
			conf = new Configuration();
			conf.SetBoolean(JHAdminConfig.MrHsRecoveryEnable, true);
			conf.SetClass(JHAdminConfig.MrHsStateStore, typeof(HistoryServerLeveldbStateStoreService
				), typeof(HistoryServerStateStoreService));
			conf.Set(JHAdminConfig.MrHsLeveldbStateStorePath, testDir.GetAbsoluteFile().ToString
				());
		}

		[TearDown]
		public virtual void Cleanup()
		{
			FileUtil.FullyDelete(testDir);
		}

		/// <exception cref="System.IO.IOException"/>
		private HistoryServerStateStoreService CreateAndStartStore()
		{
			HistoryServerStateStoreService store = HistoryServerStateStoreServiceFactory.GetStore
				(conf);
			NUnit.Framework.Assert.IsTrue("Factory did not create a leveldb store", store is 
				HistoryServerLeveldbStateStoreService);
			store.Init(conf);
			store.Start();
			return store;
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestCheckVersion()
		{
			HistoryServerLeveldbStateStoreService store = new HistoryServerLeveldbStateStoreService
				();
			store.Init(conf);
			store.Start();
			// default version
			Version defaultVersion = store.GetCurrentVersion();
			NUnit.Framework.Assert.AreEqual(defaultVersion, store.LoadVersion());
			// compatible version
			Version compatibleVersion = Version.NewInstance(defaultVersion.GetMajorVersion(), 
				defaultVersion.GetMinorVersion() + 2);
			store.DbStoreVersion(compatibleVersion);
			NUnit.Framework.Assert.AreEqual(compatibleVersion, store.LoadVersion());
			store.Close();
			store = new HistoryServerLeveldbStateStoreService();
			store.Init(conf);
			store.Start();
			// overwrite the compatible version
			NUnit.Framework.Assert.AreEqual(defaultVersion, store.LoadVersion());
			// incompatible version
			Version incompatibleVersion = Version.NewInstance(defaultVersion.GetMajorVersion(
				) + 1, defaultVersion.GetMinorVersion());
			store.DbStoreVersion(incompatibleVersion);
			store.Close();
			store = new HistoryServerLeveldbStateStoreService();
			try
			{
				store.Init(conf);
				store.Start();
				NUnit.Framework.Assert.Fail("Incompatible version, should have thrown before here."
					);
			}
			catch (ServiceStateException e)
			{
				NUnit.Framework.Assert.IsTrue("Exception message mismatch", e.Message.Contains("Incompatible version for state:"
					));
			}
			store.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestTokenStore()
		{
			HistoryServerStateStoreService store = CreateAndStartStore();
			// verify initially the store is empty
			HistoryServerStateStoreService.HistoryServerState state = store.LoadState();
			NUnit.Framework.Assert.IsTrue("token state not empty", state.tokenState.IsEmpty()
				);
			NUnit.Framework.Assert.IsTrue("key state not empty", state.tokenMasterKeyState.IsEmpty
				());
			// store a key and some tokens
			DelegationKey key1 = new DelegationKey(1, 2, Sharpen.Runtime.GetBytesForString("keyData1"
				));
			MRDelegationTokenIdentifier token1 = new MRDelegationTokenIdentifier(new Text("tokenOwner1"
				), new Text("tokenRenewer1"), new Text("tokenUser1"));
			token1.SetSequenceNumber(1);
			long tokenDate1 = 1L;
			MRDelegationTokenIdentifier token2 = new MRDelegationTokenIdentifier(new Text("tokenOwner2"
				), new Text("tokenRenewer2"), new Text("tokenUser2"));
			token2.SetSequenceNumber(12345678);
			long tokenDate2 = 87654321L;
			store.StoreTokenMasterKey(key1);
			store.StoreToken(token1, tokenDate1);
			store.StoreToken(token2, tokenDate2);
			store.Close();
			// verify the key and tokens can be recovered
			store = CreateAndStartStore();
			state = store.LoadState();
			NUnit.Framework.Assert.AreEqual("incorrect loaded token count", 2, state.tokenState
				.Count);
			NUnit.Framework.Assert.IsTrue("missing token 1", state.tokenState.Contains(token1
				));
			NUnit.Framework.Assert.AreEqual("incorrect token 1 date", tokenDate1, state.tokenState
				[token1]);
			NUnit.Framework.Assert.IsTrue("missing token 2", state.tokenState.Contains(token2
				));
			NUnit.Framework.Assert.AreEqual("incorrect token 2 date", tokenDate2, state.tokenState
				[token2]);
			NUnit.Framework.Assert.AreEqual("incorrect master key count", 1, state.tokenMasterKeyState
				.Count);
			NUnit.Framework.Assert.IsTrue("missing master key 1", state.tokenMasterKeyState.Contains
				(key1));
			// store some more keys and tokens, remove the previous key and one
			// of the tokens, and renew a previous token
			DelegationKey key2 = new DelegationKey(3, 4, Sharpen.Runtime.GetBytesForString("keyData2"
				));
			DelegationKey key3 = new DelegationKey(5, 6, Sharpen.Runtime.GetBytesForString("keyData3"
				));
			MRDelegationTokenIdentifier token3 = new MRDelegationTokenIdentifier(new Text("tokenOwner3"
				), new Text("tokenRenewer3"), new Text("tokenUser3"));
			token3.SetSequenceNumber(12345679);
			long tokenDate3 = 87654321L;
			store.RemoveToken(token1);
			store.StoreTokenMasterKey(key2);
			long newTokenDate2 = 975318642L;
			store.UpdateToken(token2, newTokenDate2);
			store.RemoveTokenMasterKey(key1);
			store.StoreTokenMasterKey(key3);
			store.StoreToken(token3, tokenDate3);
			store.Close();
			// verify the new keys and tokens are recovered, the removed key and
			// token are no longer present, and the renewed token has the updated
			// expiration date
			store = CreateAndStartStore();
			state = store.LoadState();
			NUnit.Framework.Assert.AreEqual("incorrect loaded token count", 2, state.tokenState
				.Count);
			NUnit.Framework.Assert.IsFalse("token 1 not removed", state.tokenState.Contains(token1
				));
			NUnit.Framework.Assert.IsTrue("missing token 2", state.tokenState.Contains(token2
				));
			NUnit.Framework.Assert.AreEqual("incorrect token 2 date", newTokenDate2, state.tokenState
				[token2]);
			NUnit.Framework.Assert.IsTrue("missing token 3", state.tokenState.Contains(token3
				));
			NUnit.Framework.Assert.AreEqual("incorrect token 3 date", tokenDate3, state.tokenState
				[token3]);
			NUnit.Framework.Assert.AreEqual("incorrect master key count", 2, state.tokenMasterKeyState
				.Count);
			NUnit.Framework.Assert.IsFalse("master key 1 not removed", state.tokenMasterKeyState
				.Contains(key1));
			NUnit.Framework.Assert.IsTrue("missing master key 2", state.tokenMasterKeyState.Contains
				(key2));
			NUnit.Framework.Assert.IsTrue("missing master key 3", state.tokenMasterKeyState.Contains
				(key3));
			store.Close();
		}
	}
}
