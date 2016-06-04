using System;
using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Security.Token.Delegation
{
	public class TestDelegationToken
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestDelegationToken));

		private static readonly Text Kind = new Text("MY KIND");

		public class TestDelegationTokenIdentifier : AbstractDelegationTokenIdentifier, Writable
		{
			public TestDelegationTokenIdentifier()
			{
			}

			public TestDelegationTokenIdentifier(Text owner, Text renewer, Text realUser)
				: base(owner, renewer, realUser)
			{
			}

			public override Text GetKind()
			{
				return Kind;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Write(DataOutput @out)
			{
				base.Write(@out);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void ReadFields(BinaryReader @in)
			{
				base.ReadFields(@in);
			}
		}

		public class TestDelegationTokenSecretManager : AbstractDelegationTokenSecretManager
			<TestDelegationToken.TestDelegationTokenIdentifier>
		{
			public bool isStoreNewMasterKeyCalled = false;

			public bool isRemoveStoredMasterKeyCalled = false;

			public bool isStoreNewTokenCalled = false;

			public bool isRemoveStoredTokenCalled = false;

			public bool isUpdateStoredTokenCalled = false;

			public TestDelegationTokenSecretManager(long delegationKeyUpdateInterval, long delegationTokenMaxLifetime
				, long delegationTokenRenewInterval, long delegationTokenRemoverScanInterval)
				: base(delegationKeyUpdateInterval, delegationTokenMaxLifetime, delegationTokenRenewInterval
					, delegationTokenRemoverScanInterval)
			{
			}

			public override TestDelegationToken.TestDelegationTokenIdentifier CreateIdentifier
				()
			{
				return new TestDelegationToken.TestDelegationTokenIdentifier();
			}

			protected internal override byte[] CreatePassword(TestDelegationToken.TestDelegationTokenIdentifier
				 t)
			{
				return base.CreatePassword(t);
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override void StoreNewMasterKey(DelegationKey key)
			{
				isStoreNewMasterKeyCalled = true;
				base.StoreNewMasterKey(key);
			}

			protected internal override void RemoveStoredMasterKey(DelegationKey key)
			{
				isRemoveStoredMasterKeyCalled = true;
				NUnit.Framework.Assert.IsFalse(key.Equals(allKeys[currentId]));
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override void StoreNewToken(TestDelegationToken.TestDelegationTokenIdentifier
				 ident, long renewDate)
			{
				base.StoreNewToken(ident, renewDate);
				isStoreNewTokenCalled = true;
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override void RemoveStoredToken(TestDelegationToken.TestDelegationTokenIdentifier
				 ident)
			{
				base.RemoveStoredToken(ident);
				isRemoveStoredTokenCalled = true;
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override void UpdateStoredToken(TestDelegationToken.TestDelegationTokenIdentifier
				 ident, long renewDate)
			{
				base.UpdateStoredToken(ident, renewDate);
				isUpdateStoredTokenCalled = true;
			}

			public virtual byte[] CreatePassword(TestDelegationToken.TestDelegationTokenIdentifier
				 t, DelegationKey key)
			{
				return SecretManager.CreatePassword(t.GetBytes(), key.GetKey());
			}

			public virtual IDictionary<TestDelegationToken.TestDelegationTokenIdentifier, AbstractDelegationTokenSecretManager.DelegationTokenInformation
				> GetAllTokens()
			{
				return currentTokens;
			}

			public virtual DelegationKey GetKey(TestDelegationToken.TestDelegationTokenIdentifier
				 id)
			{
				return allKeys[id.GetMasterKeyId()];
			}
		}

		public class TokenSelector : AbstractDelegationTokenSelector<TestDelegationToken.TestDelegationTokenIdentifier
			>
		{
			protected internal TokenSelector()
				: base(Kind)
			{
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSerialization()
		{
			TestDelegationToken.TestDelegationTokenIdentifier origToken = new TestDelegationToken.TestDelegationTokenIdentifier
				(new Text("alice"), new Text("bob"), new Text("colin"));
			TestDelegationToken.TestDelegationTokenIdentifier newToken = new TestDelegationToken.TestDelegationTokenIdentifier
				();
			origToken.SetIssueDate(123);
			origToken.SetMasterKeyId(321);
			origToken.SetMaxDate(314);
			origToken.SetSequenceNumber(12345);
			// clone origToken into newToken
			DataInputBuffer inBuf = new DataInputBuffer();
			DataOutputBuffer outBuf = new DataOutputBuffer();
			origToken.Write(outBuf);
			inBuf.Reset(outBuf.GetData(), 0, outBuf.GetLength());
			newToken.ReadFields(inBuf);
			// now test the fields
			NUnit.Framework.Assert.AreEqual("alice", newToken.GetUser().GetUserName());
			NUnit.Framework.Assert.AreEqual(new Text("bob"), newToken.GetRenewer());
			NUnit.Framework.Assert.AreEqual("colin", newToken.GetUser().GetRealUser().GetUserName
				());
			NUnit.Framework.Assert.AreEqual(123, newToken.GetIssueDate());
			NUnit.Framework.Assert.AreEqual(321, newToken.GetMasterKeyId());
			NUnit.Framework.Assert.AreEqual(314, newToken.GetMaxDate());
			NUnit.Framework.Assert.AreEqual(12345, newToken.GetSequenceNumber());
			NUnit.Framework.Assert.AreEqual(origToken, newToken);
		}

		private Org.Apache.Hadoop.Security.Token.Token<TestDelegationToken.TestDelegationTokenIdentifier
			> GenerateDelegationToken(TestDelegationToken.TestDelegationTokenSecretManager dtSecretManager
			, string owner, string renewer)
		{
			TestDelegationToken.TestDelegationTokenIdentifier dtId = new TestDelegationToken.TestDelegationTokenIdentifier
				(new Text(owner), new Text(renewer), null);
			return new Org.Apache.Hadoop.Security.Token.Token<TestDelegationToken.TestDelegationTokenIdentifier
				>(dtId, dtSecretManager);
		}

		private void ShouldThrow(PrivilegedExceptionAction<object> action, Type except)
		{
			try
			{
				action.Run();
				NUnit.Framework.Assert.Fail("action did not throw " + except);
			}
			catch (Exception th)
			{
				Log.Info("Caught an exception: ", th);
				NUnit.Framework.Assert.AreEqual("action threw wrong exception", except, th.GetType
					());
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestGetUserNullOwner()
		{
			TestDelegationToken.TestDelegationTokenIdentifier ident = new TestDelegationToken.TestDelegationTokenIdentifier
				(null, null, null);
			UserGroupInformation ugi = ident.GetUser();
			NUnit.Framework.Assert.IsNull(ugi);
		}

		[NUnit.Framework.Test]
		public virtual void TestGetUserWithOwner()
		{
			TestDelegationToken.TestDelegationTokenIdentifier ident = new TestDelegationToken.TestDelegationTokenIdentifier
				(new Text("owner"), null, null);
			UserGroupInformation ugi = ident.GetUser();
			NUnit.Framework.Assert.IsNull(ugi.GetRealUser());
			NUnit.Framework.Assert.AreEqual("owner", ugi.GetUserName());
			NUnit.Framework.Assert.AreEqual(UserGroupInformation.AuthenticationMethod.Token, 
				ugi.GetAuthenticationMethod());
		}

		[NUnit.Framework.Test]
		public virtual void TestGetUserWithOwnerEqualsReal()
		{
			Text owner = new Text("owner");
			TestDelegationToken.TestDelegationTokenIdentifier ident = new TestDelegationToken.TestDelegationTokenIdentifier
				(owner, null, owner);
			UserGroupInformation ugi = ident.GetUser();
			NUnit.Framework.Assert.IsNull(ugi.GetRealUser());
			NUnit.Framework.Assert.AreEqual("owner", ugi.GetUserName());
			NUnit.Framework.Assert.AreEqual(UserGroupInformation.AuthenticationMethod.Token, 
				ugi.GetAuthenticationMethod());
		}

		[NUnit.Framework.Test]
		public virtual void TestGetUserWithOwnerAndReal()
		{
			Text owner = new Text("owner");
			Text realUser = new Text("realUser");
			TestDelegationToken.TestDelegationTokenIdentifier ident = new TestDelegationToken.TestDelegationTokenIdentifier
				(owner, null, realUser);
			UserGroupInformation ugi = ident.GetUser();
			NUnit.Framework.Assert.IsNotNull(ugi.GetRealUser());
			NUnit.Framework.Assert.IsNull(ugi.GetRealUser().GetRealUser());
			NUnit.Framework.Assert.AreEqual("owner", ugi.GetUserName());
			NUnit.Framework.Assert.AreEqual("realUser", ugi.GetRealUser().GetUserName());
			NUnit.Framework.Assert.AreEqual(UserGroupInformation.AuthenticationMethod.Proxy, 
				ugi.GetAuthenticationMethod());
			NUnit.Framework.Assert.AreEqual(UserGroupInformation.AuthenticationMethod.Token, 
				ugi.GetRealUser().GetAuthenticationMethod());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDelegationTokenSecretManager()
		{
			TestDelegationToken.TestDelegationTokenSecretManager dtSecretManager = new TestDelegationToken.TestDelegationTokenSecretManager
				(24 * 60 * 60 * 1000, 3 * 1000, 1 * 1000, 3600000);
			try
			{
				dtSecretManager.StartThreads();
				Org.Apache.Hadoop.Security.Token.Token<TestDelegationToken.TestDelegationTokenIdentifier
					> token = GenerateDelegationToken(dtSecretManager, "SomeUser", "JobTracker");
				NUnit.Framework.Assert.IsTrue(dtSecretManager.isStoreNewTokenCalled);
				// Fake renewer should not be able to renew
				ShouldThrow(new _PrivilegedExceptionAction_272(dtSecretManager, token), typeof(AccessControlException
					));
				long time = dtSecretManager.RenewToken(token, "JobTracker");
				NUnit.Framework.Assert.IsTrue(dtSecretManager.isUpdateStoredTokenCalled);
				NUnit.Framework.Assert.IsTrue("renew time is in future", time > Time.Now());
				TestDelegationToken.TestDelegationTokenIdentifier identifier = new TestDelegationToken.TestDelegationTokenIdentifier
					();
				byte[] tokenId = token.GetIdentifier();
				identifier.ReadFields(new DataInputStream(new ByteArrayInputStream(tokenId)));
				NUnit.Framework.Assert.IsTrue(null != dtSecretManager.RetrievePassword(identifier
					));
				Log.Info("Sleep to expire the token");
				Sharpen.Thread.Sleep(2000);
				//Token should be expired
				try
				{
					dtSecretManager.RetrievePassword(identifier);
					//Should not come here
					NUnit.Framework.Assert.Fail("Token should have expired");
				}
				catch (SecretManager.InvalidToken)
				{
				}
				//Success
				dtSecretManager.RenewToken(token, "JobTracker");
				Log.Info("Sleep beyond the max lifetime");
				Sharpen.Thread.Sleep(2000);
				ShouldThrow(new _PrivilegedExceptionAction_302(dtSecretManager, token), typeof(SecretManager.InvalidToken
					));
			}
			finally
			{
				dtSecretManager.StopThreads();
			}
		}

		private sealed class _PrivilegedExceptionAction_272 : PrivilegedExceptionAction<object
			>
		{
			public _PrivilegedExceptionAction_272(TestDelegationToken.TestDelegationTokenSecretManager
				 dtSecretManager, Org.Apache.Hadoop.Security.Token.Token<TestDelegationToken.TestDelegationTokenIdentifier
				> token)
			{
				this.dtSecretManager = dtSecretManager;
				this.token = token;
			}

			/// <exception cref="System.Exception"/>
			public object Run()
			{
				dtSecretManager.RenewToken(token, "FakeRenewer");
				return null;
			}

			private readonly TestDelegationToken.TestDelegationTokenSecretManager dtSecretManager;

			private readonly Org.Apache.Hadoop.Security.Token.Token<TestDelegationToken.TestDelegationTokenIdentifier
				> token;
		}

		private sealed class _PrivilegedExceptionAction_302 : PrivilegedExceptionAction<object
			>
		{
			public _PrivilegedExceptionAction_302(TestDelegationToken.TestDelegationTokenSecretManager
				 dtSecretManager, Org.Apache.Hadoop.Security.Token.Token<TestDelegationToken.TestDelegationTokenIdentifier
				> token)
			{
				this.dtSecretManager = dtSecretManager;
				this.token = token;
			}

			/// <exception cref="System.Exception"/>
			public object Run()
			{
				dtSecretManager.RenewToken(token, "JobTracker");
				return null;
			}

			private readonly TestDelegationToken.TestDelegationTokenSecretManager dtSecretManager;

			private readonly Org.Apache.Hadoop.Security.Token.Token<TestDelegationToken.TestDelegationTokenIdentifier
				> token;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCancelDelegationToken()
		{
			TestDelegationToken.TestDelegationTokenSecretManager dtSecretManager = new TestDelegationToken.TestDelegationTokenSecretManager
				(24 * 60 * 60 * 1000, 10 * 1000, 1 * 1000, 3600000);
			try
			{
				dtSecretManager.StartThreads();
				Org.Apache.Hadoop.Security.Token.Token<TestDelegationToken.TestDelegationTokenIdentifier
					> token = GenerateDelegationToken(dtSecretManager, "SomeUser", "JobTracker");
				//Fake renewer should not be able to renew
				ShouldThrow(new _PrivilegedExceptionAction_324(dtSecretManager, token), typeof(AccessControlException
					));
				dtSecretManager.CancelToken(token, "JobTracker");
				NUnit.Framework.Assert.IsTrue(dtSecretManager.isRemoveStoredTokenCalled);
				ShouldThrow(new _PrivilegedExceptionAction_333(dtSecretManager, token), typeof(SecretManager.InvalidToken
					));
			}
			finally
			{
				dtSecretManager.StopThreads();
			}
		}

		private sealed class _PrivilegedExceptionAction_324 : PrivilegedExceptionAction<object
			>
		{
			public _PrivilegedExceptionAction_324(TestDelegationToken.TestDelegationTokenSecretManager
				 dtSecretManager, Org.Apache.Hadoop.Security.Token.Token<TestDelegationToken.TestDelegationTokenIdentifier
				> token)
			{
				this.dtSecretManager = dtSecretManager;
				this.token = token;
			}

			/// <exception cref="System.Exception"/>
			public object Run()
			{
				dtSecretManager.RenewToken(token, "FakeCanceller");
				return null;
			}

			private readonly TestDelegationToken.TestDelegationTokenSecretManager dtSecretManager;

			private readonly Org.Apache.Hadoop.Security.Token.Token<TestDelegationToken.TestDelegationTokenIdentifier
				> token;
		}

		private sealed class _PrivilegedExceptionAction_333 : PrivilegedExceptionAction<object
			>
		{
			public _PrivilegedExceptionAction_333(TestDelegationToken.TestDelegationTokenSecretManager
				 dtSecretManager, Org.Apache.Hadoop.Security.Token.Token<TestDelegationToken.TestDelegationTokenIdentifier
				> token)
			{
				this.dtSecretManager = dtSecretManager;
				this.token = token;
			}

			/// <exception cref="System.Exception"/>
			public object Run()
			{
				dtSecretManager.RenewToken(token, "JobTracker");
				return null;
			}

			private readonly TestDelegationToken.TestDelegationTokenSecretManager dtSecretManager;

			private readonly Org.Apache.Hadoop.Security.Token.Token<TestDelegationToken.TestDelegationTokenIdentifier
				> token;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRollMasterKey()
		{
			TestDelegationToken.TestDelegationTokenSecretManager dtSecretManager = new TestDelegationToken.TestDelegationTokenSecretManager
				(800, 800, 1 * 1000, 3600000);
			try
			{
				dtSecretManager.StartThreads();
				//generate a token and store the password
				Org.Apache.Hadoop.Security.Token.Token<TestDelegationToken.TestDelegationTokenIdentifier
					> token = GenerateDelegationToken(dtSecretManager, "SomeUser", "JobTracker");
				byte[] oldPasswd = token.GetPassword();
				//store the length of the keys list
				int prevNumKeys = dtSecretManager.GetAllKeys().Length;
				dtSecretManager.RollMasterKey();
				NUnit.Framework.Assert.IsTrue(dtSecretManager.isStoreNewMasterKeyCalled);
				//after rolling, the length of the keys list must increase
				int currNumKeys = dtSecretManager.GetAllKeys().Length;
				NUnit.Framework.Assert.AreEqual((currNumKeys - prevNumKeys) >= 1, true);
				//after rolling, the token that was generated earlier must
				//still be valid (retrievePassword will fail if the token
				//is not valid)
				ByteArrayInputStream bi = new ByteArrayInputStream(token.GetIdentifier());
				TestDelegationToken.TestDelegationTokenIdentifier identifier = dtSecretManager.CreateIdentifier
					();
				identifier.ReadFields(new DataInputStream(bi));
				byte[] newPasswd = dtSecretManager.RetrievePassword(identifier);
				//compare the passwords
				NUnit.Framework.Assert.AreEqual(oldPasswd, newPasswd);
				// wait for keys to expire
				while (!dtSecretManager.isRemoveStoredMasterKeyCalled)
				{
					Sharpen.Thread.Sleep(200);
				}
			}
			finally
			{
				dtSecretManager.StopThreads();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDelegationTokenSelector()
		{
			TestDelegationToken.TestDelegationTokenSecretManager dtSecretManager = new TestDelegationToken.TestDelegationTokenSecretManager
				(24 * 60 * 60 * 1000, 10 * 1000, 1 * 1000, 3600000);
			try
			{
				dtSecretManager.StartThreads();
				AbstractDelegationTokenSelector ds = new AbstractDelegationTokenSelector<TestDelegationToken.TestDelegationTokenIdentifier
					>(Kind);
				//Creates a collection of tokens
				Org.Apache.Hadoop.Security.Token.Token<TestDelegationToken.TestDelegationTokenIdentifier
					> token1 = GenerateDelegationToken(dtSecretManager, "SomeUser1", "JobTracker");
				token1.SetService(new Text("MY-SERVICE1"));
				Org.Apache.Hadoop.Security.Token.Token<TestDelegationToken.TestDelegationTokenIdentifier
					> token2 = GenerateDelegationToken(dtSecretManager, "SomeUser2", "JobTracker");
				token2.SetService(new Text("MY-SERVICE2"));
				IList<Org.Apache.Hadoop.Security.Token.Token<TestDelegationToken.TestDelegationTokenIdentifier
					>> tokens = new AList<Org.Apache.Hadoop.Security.Token.Token<TestDelegationToken.TestDelegationTokenIdentifier
					>>();
				tokens.AddItem(token1);
				tokens.AddItem(token2);
				//try to select a token with a given service name (created earlier)
				Org.Apache.Hadoop.Security.Token.Token<TestDelegationToken.TestDelegationTokenIdentifier
					> t = ds.SelectToken(new Text("MY-SERVICE1"), tokens);
				NUnit.Framework.Assert.AreEqual(t, token1);
			}
			finally
			{
				dtSecretManager.StopThreads();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestParallelDelegationTokenCreation()
		{
			TestDelegationToken.TestDelegationTokenSecretManager dtSecretManager = new TestDelegationToken.TestDelegationTokenSecretManager
				(2000, 24 * 60 * 60 * 1000, 7 * 24 * 60 * 60 * 1000, 2000);
			try
			{
				dtSecretManager.StartThreads();
				int numThreads = 100;
				int numTokensPerThread = 100;
				Sharpen.Thread[] issuers = new Sharpen.Thread[numThreads];
				for (int i = 0; i < numThreads; i++)
				{
					issuers[i] = new Daemon(new _T1720540651(this));
					issuers[i].Start();
				}
				for (int i_1 = 0; i_1 < numThreads; i_1++)
				{
					issuers[i_1].Join();
				}
				IDictionary<TestDelegationToken.TestDelegationTokenIdentifier, AbstractDelegationTokenSecretManager.DelegationTokenInformation
					> tokenCache = dtSecretManager.GetAllTokens();
				NUnit.Framework.Assert.AreEqual(numTokensPerThread * numThreads, tokenCache.Count
					);
				IEnumerator<TestDelegationToken.TestDelegationTokenIdentifier> iter = tokenCache.
					Keys.GetEnumerator();
				while (iter.HasNext())
				{
					TestDelegationToken.TestDelegationTokenIdentifier id = iter.Next();
					AbstractDelegationTokenSecretManager.DelegationTokenInformation info = tokenCache
						[id];
					NUnit.Framework.Assert.IsTrue(info != null);
					DelegationKey key = dtSecretManager.GetKey(id);
					NUnit.Framework.Assert.IsTrue(key != null);
					byte[] storedPassword = dtSecretManager.RetrievePassword(id);
					byte[] password = dtSecretManager.CreatePassword(id, key);
					NUnit.Framework.Assert.IsTrue(Arrays.Equals(password, storedPassword));
					//verify by secret manager api
					dtSecretManager.VerifyToken(id, password);
				}
			}
			finally
			{
				dtSecretManager.StopThreads();
			}
		}

		internal class _T1720540651 : Runnable
		{
			public virtual void Run()
			{
				for (int i = 0; i < numTokensPerThread; i++)
				{
					this._enclosing.GenerateDelegationToken(dtSecretManager, "auser", "arenewer");
					try
					{
						Sharpen.Thread.Sleep(250);
					}
					catch (Exception)
					{
					}
				}
			}

			internal _T1720540651(TestDelegationToken _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestDelegationToken _enclosing;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDelegationTokenNullRenewer()
		{
			TestDelegationToken.TestDelegationTokenSecretManager dtSecretManager = new TestDelegationToken.TestDelegationTokenSecretManager
				(24 * 60 * 60 * 1000, 10 * 1000, 1 * 1000, 3600000);
			dtSecretManager.StartThreads();
			TestDelegationToken.TestDelegationTokenIdentifier dtId = new TestDelegationToken.TestDelegationTokenIdentifier
				(new Text("theuser"), null, null);
			Org.Apache.Hadoop.Security.Token.Token<TestDelegationToken.TestDelegationTokenIdentifier
				> token = new Org.Apache.Hadoop.Security.Token.Token<TestDelegationToken.TestDelegationTokenIdentifier
				>(dtId, dtSecretManager);
			NUnit.Framework.Assert.IsTrue(token != null);
			try
			{
				dtSecretManager.RenewToken(token, string.Empty);
				NUnit.Framework.Assert.Fail("Renewal must not succeed");
			}
			catch (IOException)
			{
			}
		}

		//PASS
		/// <exception cref="System.IO.IOException"/>
		private bool TestDelegationTokenIdentiferSerializationRoundTrip(Text owner, Text 
			renewer, Text realUser)
		{
			TestDelegationToken.TestDelegationTokenIdentifier dtid = new TestDelegationToken.TestDelegationTokenIdentifier
				(owner, renewer, realUser);
			DataOutputBuffer @out = new DataOutputBuffer();
			dtid.WriteImpl(@out);
			DataInputBuffer @in = new DataInputBuffer();
			@in.Reset(@out.GetData(), @out.GetLength());
			try
			{
				TestDelegationToken.TestDelegationTokenIdentifier dtid2 = new TestDelegationToken.TestDelegationTokenIdentifier
					();
				dtid2.ReadFields(@in);
				NUnit.Framework.Assert.IsTrue(dtid.Equals(dtid2));
				return true;
			}
			catch (IOException)
			{
				return false;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestSimpleDtidSerialization()
		{
			NUnit.Framework.Assert.IsTrue(TestDelegationTokenIdentiferSerializationRoundTrip(
				new Text("owner"), new Text("renewer"), new Text("realUser")));
			NUnit.Framework.Assert.IsTrue(TestDelegationTokenIdentiferSerializationRoundTrip(
				new Text(string.Empty), new Text(string.Empty), new Text(string.Empty)));
			NUnit.Framework.Assert.IsTrue(TestDelegationTokenIdentiferSerializationRoundTrip(
				new Text(string.Empty), new Text("b"), new Text(string.Empty)));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestOverlongDtidSerialization()
		{
			byte[] bigBuf = new byte[Text.DefaultMaxLen + 1];
			for (int i = 0; i < bigBuf.Length; i++)
			{
				bigBuf[i] = 0;
			}
			NUnit.Framework.Assert.IsFalse(TestDelegationTokenIdentiferSerializationRoundTrip
				(new Text(bigBuf), new Text("renewer"), new Text("realUser")));
			NUnit.Framework.Assert.IsFalse(TestDelegationTokenIdentiferSerializationRoundTrip
				(new Text("owner"), new Text(bigBuf), new Text("realUser")));
			NUnit.Framework.Assert.IsFalse(TestDelegationTokenIdentiferSerializationRoundTrip
				(new Text("owner"), new Text("renewer"), new Text(bigBuf)));
		}

		[NUnit.Framework.Test]
		public virtual void TestDelegationKeyEqualAndHash()
		{
			DelegationKey key1 = new DelegationKey(1111, 2222, Sharpen.Runtime.GetBytesForString
				("keyBytes"));
			DelegationKey key2 = new DelegationKey(1111, 2222, Sharpen.Runtime.GetBytesForString
				("keyBytes"));
			DelegationKey key3 = new DelegationKey(3333, 2222, Sharpen.Runtime.GetBytesForString
				("keyBytes"));
			NUnit.Framework.Assert.AreEqual(key1, key2);
			NUnit.Framework.Assert.IsFalse(key2.Equals(key3));
		}
	}
}
