using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Curator;
using Org.Apache.Curator.Framework;
using Org.Apache.Curator.Framework.Api;
using Org.Apache.Curator.Retry;
using Org.Apache.Curator.Test;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Security.Token.Delegation.Web;
using Org.Apache.Zookeeper;
using Org.Apache.Zookeeper.Data;
using Org.Apache.Zookeeper.Server.Auth;


namespace Org.Apache.Hadoop.Security.Token.Delegation
{
	public class TestZKDelegationTokenSecretManager
	{
		private const int TestRetries = 2;

		private const int RetryCount = 5;

		private const int RetryWait = 1000;

		private const long DayInSecs = 86400;

		private TestingServer zkServer;

		/// <exception cref="System.Exception"/>
		[SetUp]
		public virtual void Setup()
		{
			zkServer = new TestingServer();
			zkServer.Start();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			if (zkServer != null)
			{
				zkServer.Close();
			}
		}

		protected internal virtual Configuration GetSecretConf(string connectString)
		{
			Configuration conf = new Configuration();
			conf.SetBoolean(DelegationTokenManager.EnableZkKey, true);
			conf.Set(ZKDelegationTokenSecretManager.ZkDtsmZkConnectionString, connectString);
			conf.Set(ZKDelegationTokenSecretManager.ZkDtsmZnodeWorkingPath, "testPath");
			conf.Set(ZKDelegationTokenSecretManager.ZkDtsmZkAuthType, "none");
			conf.SetLong(ZKDelegationTokenSecretManager.ZkDtsmZkShutdownTimeout, 100);
			conf.SetLong(DelegationTokenManager.UpdateInterval, DayInSecs);
			conf.SetLong(DelegationTokenManager.MaxLifetime, DayInSecs);
			conf.SetLong(DelegationTokenManager.RenewInterval, DayInSecs);
			conf.SetLong(DelegationTokenManager.RemovalScanInterval, DayInSecs);
			return conf;
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestMultiNodeOperations()
		{
			for (int i = 0; i < TestRetries; i++)
			{
				DelegationTokenManager tm1;
				DelegationTokenManager tm2 = null;
				string connectString = zkServer.GetConnectString();
				Configuration conf = GetSecretConf(connectString);
				tm1 = new DelegationTokenManager(conf, new Text("bla"));
				tm1.Init();
				tm2 = new DelegationTokenManager(conf, new Text("bla"));
				tm2.Init();
				Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> token = (Org.Apache.Hadoop.Security.Token.Token
					<DelegationTokenIdentifier>)tm1.CreateToken(UserGroupInformation.GetCurrentUser(
					), "foo");
				NUnit.Framework.Assert.IsNotNull(token);
				tm2.VerifyToken(token);
				tm2.RenewToken(token, "foo");
				tm1.VerifyToken(token);
				tm1.CancelToken(token, "foo");
				try
				{
					VerifyTokenFail(tm2, token);
					NUnit.Framework.Assert.Fail("Expected InvalidToken");
				}
				catch (SecretManager.InvalidToken)
				{
				}
				// Ignore
				token = (Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier>)tm2.CreateToken
					(UserGroupInformation.GetCurrentUser(), "bar");
				NUnit.Framework.Assert.IsNotNull(token);
				tm1.VerifyToken(token);
				tm1.RenewToken(token, "bar");
				tm2.VerifyToken(token);
				tm2.CancelToken(token, "bar");
				try
				{
					VerifyTokenFail(tm1, token);
					NUnit.Framework.Assert.Fail("Expected InvalidToken");
				}
				catch (SecretManager.InvalidToken)
				{
				}
				// Ignore
				VerifyDestroy(tm1, conf);
				VerifyDestroy(tm2, conf);
			}
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestNodeUpAferAWhile()
		{
			for (int i = 0; i < TestRetries; i++)
			{
				string connectString = zkServer.GetConnectString();
				Configuration conf = GetSecretConf(connectString);
				DelegationTokenManager tm1 = new DelegationTokenManager(conf, new Text("bla"));
				tm1.Init();
				Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> token1 = (Org.Apache.Hadoop.Security.Token.Token
					<DelegationTokenIdentifier>)tm1.CreateToken(UserGroupInformation.GetCurrentUser(
					), "foo");
				NUnit.Framework.Assert.IsNotNull(token1);
				Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> token2 = (Org.Apache.Hadoop.Security.Token.Token
					<DelegationTokenIdentifier>)tm1.CreateToken(UserGroupInformation.GetCurrentUser(
					), "bar");
				NUnit.Framework.Assert.IsNotNull(token2);
				Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> token3 = (Org.Apache.Hadoop.Security.Token.Token
					<DelegationTokenIdentifier>)tm1.CreateToken(UserGroupInformation.GetCurrentUser(
					), "boo");
				NUnit.Framework.Assert.IsNotNull(token3);
				tm1.VerifyToken(token1);
				tm1.VerifyToken(token2);
				tm1.VerifyToken(token3);
				// Cancel one token
				tm1.CancelToken(token1, "foo");
				// Start second node after some time..
				Thread.Sleep(1000);
				DelegationTokenManager tm2 = new DelegationTokenManager(conf, new Text("bla"));
				tm2.Init();
				tm2.VerifyToken(token2);
				tm2.VerifyToken(token3);
				try
				{
					VerifyTokenFail(tm2, token1);
					NUnit.Framework.Assert.Fail("Expected InvalidToken");
				}
				catch (SecretManager.InvalidToken)
				{
				}
				// Ignore
				// Create a new token thru the new ZKDTSM
				Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> token4 = (Org.Apache.Hadoop.Security.Token.Token
					<DelegationTokenIdentifier>)tm2.CreateToken(UserGroupInformation.GetCurrentUser(
					), "xyz");
				NUnit.Framework.Assert.IsNotNull(token4);
				tm2.VerifyToken(token4);
				tm1.VerifyToken(token4);
				// Bring down tm2
				VerifyDestroy(tm2, conf);
				// Start third node after some time..
				Thread.Sleep(1000);
				DelegationTokenManager tm3 = new DelegationTokenManager(conf, new Text("bla"));
				tm3.Init();
				tm3.VerifyToken(token2);
				tm3.VerifyToken(token3);
				tm3.VerifyToken(token4);
				try
				{
					VerifyTokenFail(tm3, token1);
					NUnit.Framework.Assert.Fail("Expected InvalidToken");
				}
				catch (SecretManager.InvalidToken)
				{
				}
				// Ignore
				VerifyDestroy(tm3, conf);
				VerifyDestroy(tm1, conf);
			}
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestRenewTokenSingleManager()
		{
			for (int i = 0; i < TestRetries; i++)
			{
				DelegationTokenManager tm1 = null;
				string connectString = zkServer.GetConnectString();
				Configuration conf = GetSecretConf(connectString);
				tm1 = new DelegationTokenManager(conf, new Text("foo"));
				tm1.Init();
				Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> token = (Org.Apache.Hadoop.Security.Token.Token
					<DelegationTokenIdentifier>)tm1.CreateToken(UserGroupInformation.GetCurrentUser(
					), "foo");
				NUnit.Framework.Assert.IsNotNull(token);
				tm1.RenewToken(token, "foo");
				tm1.VerifyToken(token);
				VerifyDestroy(tm1, conf);
			}
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestCancelTokenSingleManager()
		{
			for (int i = 0; i < TestRetries; i++)
			{
				DelegationTokenManager tm1 = null;
				string connectString = zkServer.GetConnectString();
				Configuration conf = GetSecretConf(connectString);
				tm1 = new DelegationTokenManager(conf, new Text("foo"));
				tm1.Init();
				Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> token = (Org.Apache.Hadoop.Security.Token.Token
					<DelegationTokenIdentifier>)tm1.CreateToken(UserGroupInformation.GetCurrentUser(
					), "foo");
				NUnit.Framework.Assert.IsNotNull(token);
				tm1.CancelToken(token, "foo");
				try
				{
					VerifyTokenFail(tm1, token);
					NUnit.Framework.Assert.Fail("Expected InvalidToken");
				}
				catch (SecretManager.InvalidToken it)
				{
					Runtime.PrintStackTrace(it);
				}
				VerifyDestroy(tm1, conf);
			}
		}

		/// <exception cref="System.Exception"/>
		protected internal virtual void VerifyDestroy(DelegationTokenManager tm, Configuration
			 conf)
		{
			AbstractDelegationTokenSecretManager sm = tm.GetDelegationTokenSecretManager();
			ZKDelegationTokenSecretManager zksm = (ZKDelegationTokenSecretManager)sm;
			ExecutorService es = zksm.GetListenerThreadPool();
			tm.Destroy();
			Assert.True(es.IsShutdown());
			// wait for the pool to terminate
			long timeout = conf.GetLong(ZKDelegationTokenSecretManager.ZkDtsmZkShutdownTimeout
				, ZKDelegationTokenSecretManager.ZkDtsmZkShutdownTimeoutDefault);
			Thread.Sleep(timeout * 3);
			Assert.True(es.IsTerminated());
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestStopThreads()
		{
			DelegationTokenManager tm1 = null;
			string connectString = zkServer.GetConnectString();
			// let's make the update interval short and the shutdown interval
			// comparatively longer, so if the update thread runs after shutdown,
			// it will cause an error.
			long updateIntervalSeconds = 1;
			long shutdownTimeoutMillis = updateIntervalSeconds * 1000 * 5;
			Configuration conf = GetSecretConf(connectString);
			conf.SetLong(DelegationTokenManager.UpdateInterval, updateIntervalSeconds);
			conf.SetLong(DelegationTokenManager.RemovalScanInterval, updateIntervalSeconds);
			conf.SetLong(DelegationTokenManager.RenewInterval, updateIntervalSeconds);
			conf.SetLong(ZKDelegationTokenSecretManager.ZkDtsmZkShutdownTimeout, shutdownTimeoutMillis
				);
			tm1 = new DelegationTokenManager(conf, new Text("foo"));
			tm1.Init();
			Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> token = (Org.Apache.Hadoop.Security.Token.Token
				<DelegationTokenIdentifier>)tm1.CreateToken(UserGroupInformation.GetCurrentUser(
				), "foo");
			NUnit.Framework.Assert.IsNotNull(token);
			AbstractDelegationTokenSecretManager sm = tm1.GetDelegationTokenSecretManager();
			ZKDelegationTokenSecretManager zksm = (ZKDelegationTokenSecretManager)sm;
			ExecutorService es = zksm.GetListenerThreadPool();
			es.Submit(new _Callable_300(shutdownTimeoutMillis));
			// force this to be shutdownNow
			tm1.Destroy();
		}

		private sealed class _Callable_300 : Callable<Void>
		{
			public _Callable_300(long shutdownTimeoutMillis)
			{
				this.shutdownTimeoutMillis = shutdownTimeoutMillis;
			}

			/// <exception cref="System.Exception"/>
			public Void Call()
			{
				Thread.Sleep(shutdownTimeoutMillis * 2);
				return null;
			}

			private readonly long shutdownTimeoutMillis;
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestACLs()
		{
			DelegationTokenManager tm1;
			string connectString = zkServer.GetConnectString();
			Configuration conf = GetSecretConf(connectString);
			RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
			string userPass = "myuser:mypass";
			ACL digestACL = new ACL(ZooDefs.Perms.All, new ID("digest", DigestAuthenticationProvider
				.GenerateDigest(userPass)));
			ACLProvider digestAclProvider = new _ACLProvider_319(digestACL);
			CuratorFramework curatorFramework = CuratorFrameworkFactory.Builder().ConnectString
				(connectString).RetryPolicy(retryPolicy).AclProvider(digestAclProvider).Authorization
				("digest", Runtime.GetBytesForString(userPass, "UTF-8")).Build();
			curatorFramework.Start();
			ZKDelegationTokenSecretManager.SetCurator(curatorFramework);
			tm1 = new DelegationTokenManager(conf, new Text("bla"));
			tm1.Init();
			// check ACL
			string workingPath = conf.Get(ZKDelegationTokenSecretManager.ZkDtsmZnodeWorkingPath
				);
			VerifyACL(curatorFramework, "/" + workingPath, digestACL);
			tm1.Destroy();
			ZKDelegationTokenSecretManager.SetCurator(null);
			curatorFramework.Close();
		}

		private sealed class _ACLProvider_319 : ACLProvider
		{
			public _ACLProvider_319(ACL digestACL)
			{
				this.digestACL = digestACL;
			}

			public IList<ACL> GetAclForPath(string path)
			{
				return this.GetDefaultAcl();
			}

			public IList<ACL> GetDefaultAcl()
			{
				IList<ACL> ret = new AList<ACL>();
				ret.AddItem(digestACL);
				return ret;
			}

			private readonly ACL digestACL;
		}

		/// <exception cref="System.Exception"/>
		private void VerifyACL(CuratorFramework curatorFramework, string path, ACL expectedACL
			)
		{
			IList<ACL> acls = curatorFramework.GetACL().ForPath(path);
			Assert.Equal(1, acls.Count);
			Assert.Equal(expectedACL, acls[0]);
		}

		// Since it is possible that there can be a delay for the cancel token message
		// initiated by one node to reach another node.. The second node can ofcourse
		// verify with ZK directly if the token that needs verification has been
		// cancelled but.. that would mean having to make an RPC call for every
		// verification request.
		// Thus, the eventual consistency tradef-off should be acceptable here...
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		private void VerifyTokenFail(DelegationTokenManager tm, Org.Apache.Hadoop.Security.Token.Token
			<DelegationTokenIdentifier> token)
		{
			VerifyTokenFailWithRetry(tm, token, RetryCount);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		private void VerifyTokenFailWithRetry(DelegationTokenManager tm, Org.Apache.Hadoop.Security.Token.Token
			<DelegationTokenIdentifier> token, int retryCount)
		{
			try
			{
				tm.VerifyToken(token);
			}
			catch (SecretManager.InvalidToken er)
			{
				throw;
			}
			if (retryCount > 0)
			{
				Thread.Sleep(RetryWait);
				VerifyTokenFailWithRetry(tm, token, retryCount - 1);
			}
		}
	}
}
