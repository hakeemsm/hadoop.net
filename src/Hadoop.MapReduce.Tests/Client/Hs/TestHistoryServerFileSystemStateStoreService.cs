using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce.V2.Api;
using Org.Apache.Hadoop.Mapreduce.V2.Jobhistory;
using Org.Apache.Hadoop.Security.Token.Delegation;
using Org.Mockito;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.HS
{
	public class TestHistoryServerFileSystemStateStoreService
	{
		private static readonly FilePath testDir = new FilePath(Runtime.GetProperty("test.build.data"
			, Runtime.GetProperty("java.io.tmpdir")), "TestHistoryServerFileSystemStateStoreService"
			);

		private Configuration conf;

		[SetUp]
		public virtual void Setup()
		{
			FileUtil.FullyDelete(testDir);
			testDir.Mkdirs();
			conf = new Configuration();
			conf.SetBoolean(JHAdminConfig.MrHsRecoveryEnable, true);
			conf.SetClass(JHAdminConfig.MrHsStateStore, typeof(HistoryServerFileSystemStateStoreService
				), typeof(HistoryServerStateStoreService));
			conf.Set(JHAdminConfig.MrHsFsStateStoreUri, testDir.GetAbsoluteFile().ToURI().ToString
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
			NUnit.Framework.Assert.IsTrue("Factory did not create a filesystem store", store 
				is HistoryServerFileSystemStateStoreService);
			store.Init(conf);
			store.Start();
			return store;
		}

		/// <exception cref="System.IO.IOException"/>
		private void TestTokenStore(string stateStoreUri)
		{
			conf.Set(JHAdminConfig.MrHsFsStateStoreUri, stateStoreUri);
			HistoryServerStateStoreService store = CreateAndStartStore();
			HistoryServerStateStoreService.HistoryServerState state = store.LoadState();
			NUnit.Framework.Assert.IsTrue("token state not empty", state.tokenState.IsEmpty()
				);
			NUnit.Framework.Assert.IsTrue("key state not empty", state.tokenMasterKeyState.IsEmpty
				());
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
			try
			{
				store.StoreTokenMasterKey(key1);
				NUnit.Framework.Assert.Fail("redundant store of key undetected");
			}
			catch (IOException)
			{
			}
			// expected
			store.StoreToken(token1, tokenDate1);
			store.StoreToken(token2, tokenDate2);
			try
			{
				store.StoreToken(token1, tokenDate1);
				NUnit.Framework.Assert.Fail("redundant store of token undetected");
			}
			catch (IOException)
			{
			}
			// expected
			store.Close();
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
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestTokenStore()
		{
			TestTokenStore(testDir.GetAbsoluteFile().ToURI().ToString());
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestTokenStoreHdfs()
		{
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).Build();
			conf = cluster.GetConfiguration(0);
			try
			{
				TestTokenStore("/tmp/historystore");
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestUpdatedTokenRecovery()
		{
			IOException intentionalErr = new IOException("intentional error");
			FileSystem fs = FileSystem.GetLocal(conf);
			FileSystem spyfs = Org.Mockito.Mockito.Spy(fs);
			// make the update token process fail halfway through where we're left
			// with just the temporary update file and no token file
			ArgumentMatcher<Path> updateTmpMatcher = new _ArgumentMatcher_196();
			Org.Mockito.Mockito.DoThrow(intentionalErr).When(spyfs).Rename(Matchers.ArgThat(updateTmpMatcher
				), Matchers.IsA<Path>());
			conf.Set(JHAdminConfig.MrHsFsStateStoreUri, testDir.GetAbsoluteFile().ToURI().ToString
				());
			HistoryServerStateStoreService store = new _HistoryServerFileSystemStateStoreService_211
				(spyfs);
			store.Init(conf);
			store.Start();
			MRDelegationTokenIdentifier token1 = new MRDelegationTokenIdentifier(new Text("tokenOwner1"
				), new Text("tokenRenewer1"), new Text("tokenUser1"));
			token1.SetSequenceNumber(1);
			long tokenDate1 = 1L;
			store.StoreToken(token1, tokenDate1);
			long newTokenDate1 = 975318642L;
			try
			{
				store.UpdateToken(token1, newTokenDate1);
				NUnit.Framework.Assert.Fail("intentional error not thrown");
			}
			catch (IOException e)
			{
				NUnit.Framework.Assert.AreEqual(intentionalErr, e);
			}
			store.Close();
			// verify the update file is seen and parsed upon recovery when
			// original token file is missing
			store = CreateAndStartStore();
			HistoryServerStateStoreService.HistoryServerState state = store.LoadState();
			NUnit.Framework.Assert.AreEqual("incorrect loaded token count", 1, state.tokenState
				.Count);
			NUnit.Framework.Assert.IsTrue("missing token 1", state.tokenState.Contains(token1
				));
			NUnit.Framework.Assert.AreEqual("incorrect token 1 date", newTokenDate1, state.tokenState
				[token1]);
			store.Close();
		}

		private sealed class _ArgumentMatcher_196 : ArgumentMatcher<Path>
		{
			public _ArgumentMatcher_196()
			{
			}

			public override bool Matches(object argument)
			{
				if (argument is Path)
				{
					return ((Path)argument).GetName().StartsWith("update");
				}
				return false;
			}
		}

		private sealed class _HistoryServerFileSystemStateStoreService_211 : HistoryServerFileSystemStateStoreService
		{
			public _HistoryServerFileSystemStateStoreService_211(FileSystem spyfs)
			{
				this.spyfs = spyfs;
			}

			/// <exception cref="System.IO.IOException"/>
			internal override FileSystem CreateFileSystem()
			{
				return spyfs;
			}

			private readonly FileSystem spyfs;
		}
	}
}
