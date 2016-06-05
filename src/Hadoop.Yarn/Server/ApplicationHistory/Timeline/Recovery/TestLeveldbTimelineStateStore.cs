using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security.Token.Delegation;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Security.Client;
using Org.Apache.Hadoop.Yarn.Server.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Timeline.Recovery
{
	public class TestLeveldbTimelineStateStore
	{
		private FileContext fsContext;

		private FilePath fsPath;

		private Configuration conf;

		private TimelineStateStore store;

		/// <exception cref="System.Exception"/>
		[SetUp]
		public virtual void Setup()
		{
			fsPath = new FilePath("target", GetType().Name + "-tmpDir").GetAbsoluteFile();
			fsContext = FileContext.GetLocalFSFileContext();
			fsContext.Delete(new Path(fsPath.GetAbsolutePath()), true);
			conf = new YarnConfiguration();
			conf.SetBoolean(YarnConfiguration.TimelineServiceRecoveryEnabled, true);
			conf.SetClass(YarnConfiguration.TimelineServiceStateStoreClass, typeof(LeveldbTimelineStateStore
				), typeof(TimelineStateStore));
			conf.Set(YarnConfiguration.TimelineServiceLeveldbStateStorePath, fsPath.GetAbsolutePath
				());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			if (store != null)
			{
				store.Stop();
			}
			if (fsContext != null)
			{
				fsContext.Delete(new Path(fsPath.GetAbsolutePath()), true);
			}
		}

		private LeveldbTimelineStateStore InitAndStartTimelineServiceStateStoreService()
		{
			store = new LeveldbTimelineStateStore();
			store.Init(conf);
			store.Start();
			return (LeveldbTimelineStateStore)store;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestTokenStore()
		{
			InitAndStartTimelineServiceStateStoreService();
			TimelineStateStore.TimelineServiceState state = store.LoadState();
			NUnit.Framework.Assert.IsTrue("token state not empty", state.tokenState.IsEmpty()
				);
			NUnit.Framework.Assert.IsTrue("key state not empty", state.tokenMasterKeyState.IsEmpty
				());
			DelegationKey key1 = new DelegationKey(1, 2, Sharpen.Runtime.GetBytesForString("keyData1"
				));
			TimelineDelegationTokenIdentifier token1 = new TimelineDelegationTokenIdentifier(
				new Text("tokenOwner1"), new Text("tokenRenewer1"), new Text("tokenUser1"));
			token1.SetSequenceNumber(1);
			token1.GetBytes();
			long tokenDate1 = 1L;
			TimelineDelegationTokenIdentifier token2 = new TimelineDelegationTokenIdentifier(
				new Text("tokenOwner2"), new Text("tokenRenewer2"), new Text("tokenUser2"));
			token2.SetSequenceNumber(12345678);
			token2.GetBytes();
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
			InitAndStartTimelineServiceStateStoreService();
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
			NUnit.Framework.Assert.AreEqual("incorrect latest sequence number", 12345678, state
				.GetLatestSequenceNumber());
			DelegationKey key2 = new DelegationKey(3, 4, Sharpen.Runtime.GetBytesForString("keyData2"
				));
			DelegationKey key3 = new DelegationKey(5, 6, Sharpen.Runtime.GetBytesForString("keyData3"
				));
			TimelineDelegationTokenIdentifier token3 = new TimelineDelegationTokenIdentifier(
				new Text("tokenOwner3"), new Text("tokenRenewer3"), new Text("tokenUser3"));
			token3.SetSequenceNumber(12345679);
			token3.GetBytes();
			long tokenDate3 = 87654321L;
			store.RemoveToken(token1);
			store.StoreTokenMasterKey(key2);
			long newTokenDate2 = 975318642L;
			store.UpdateToken(token2, newTokenDate2);
			store.RemoveTokenMasterKey(key1);
			store.StoreTokenMasterKey(key3);
			store.StoreToken(token3, tokenDate3);
			store.Close();
			InitAndStartTimelineServiceStateStoreService();
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
			NUnit.Framework.Assert.AreEqual("incorrect latest sequence number", 12345679, state
				.GetLatestSequenceNumber());
			store.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestCheckVersion()
		{
			LeveldbTimelineStateStore store = InitAndStartTimelineServiceStateStoreService();
			// default version
			Version defaultVersion = store.GetCurrentVersion();
			NUnit.Framework.Assert.AreEqual(defaultVersion, store.LoadVersion());
			// compatible version
			Version compatibleVersion = Version.NewInstance(defaultVersion.GetMajorVersion(), 
				defaultVersion.GetMinorVersion() + 2);
			store.StoreVersion(compatibleVersion);
			NUnit.Framework.Assert.AreEqual(compatibleVersion, store.LoadVersion());
			store.Stop();
			// overwrite the compatible version
			store = InitAndStartTimelineServiceStateStoreService();
			NUnit.Framework.Assert.AreEqual(defaultVersion, store.LoadVersion());
			// incompatible version
			Version incompatibleVersion = Version.NewInstance(defaultVersion.GetMajorVersion(
				) + 1, defaultVersion.GetMinorVersion());
			store.StoreVersion(incompatibleVersion);
			store.Stop();
			try
			{
				InitAndStartTimelineServiceStateStoreService();
				NUnit.Framework.Assert.Fail("Incompatible version, should expect fail here.");
			}
			catch (ServiceStateException e)
			{
				NUnit.Framework.Assert.IsTrue("Exception message mismatch", e.Message.Contains("Incompatible version for timeline state store"
					));
			}
		}
	}
}
