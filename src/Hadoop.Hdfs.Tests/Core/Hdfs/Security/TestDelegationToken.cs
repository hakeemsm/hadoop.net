using System;
using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Commons.Logging.Impl;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Security.Token.Delegation;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Server.Namenode.Web.Resources;
using Org.Apache.Hadoop.Hdfs.Web;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Log4j;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Security
{
	public class TestDelegationToken
	{
		private MiniDFSCluster cluster;

		private DelegationTokenSecretManager dtSecretManager;

		private Configuration config;

		private static readonly Log Log = LogFactory.GetLog(typeof(TestDelegationToken));

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			config = new HdfsConfiguration();
			config.SetBoolean(DFSConfigKeys.DfsWebhdfsEnabledKey, true);
			config.SetLong(DFSConfigKeys.DfsNamenodeDelegationTokenMaxLifetimeKey, 10000);
			config.SetLong(DFSConfigKeys.DfsNamenodeDelegationTokenRenewIntervalKey, 5000);
			config.SetBoolean(DFSConfigKeys.DfsNamenodeDelegationTokenAlwaysUseKey, true);
			config.Set("hadoop.security.auth_to_local", "RULE:[2:$1@$0](JobTracker@.*FOO.COM)s/@.*//"
				 + "DEFAULT");
			FileSystem.SetDefaultUri(config, "hdfs://localhost:" + "0");
			cluster = new MiniDFSCluster.Builder(config).NumDataNodes(0).Build();
			cluster.WaitActive();
			dtSecretManager = NameNodeAdapter.GetDtSecretManager(cluster.GetNamesystem());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			if (cluster != null)
			{
				cluster.Shutdown();
			}
		}

		private Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> GenerateDelegationToken
			(string owner, string renewer)
		{
			DelegationTokenIdentifier dtId = new DelegationTokenIdentifier(new Text(owner), new 
				Text(renewer), null);
			return new Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier>(dtId
				, dtSecretManager);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDelegationTokenSecretManager()
		{
			Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> token = GenerateDelegationToken
				("SomeUser", "JobTracker");
			// Fake renewer should not be able to renew
			try
			{
				dtSecretManager.RenewToken(token, "FakeRenewer");
				NUnit.Framework.Assert.Fail("should have failed");
			}
			catch (AccessControlException)
			{
			}
			// PASS
			dtSecretManager.RenewToken(token, "JobTracker");
			DelegationTokenIdentifier identifier = new DelegationTokenIdentifier();
			byte[] tokenId = token.GetIdentifier();
			identifier.ReadFields(new DataInputStream(new ByteArrayInputStream(tokenId)));
			NUnit.Framework.Assert.IsTrue(null != dtSecretManager.RetrievePassword(identifier
				));
			Log.Info("Sleep to expire the token");
			Sharpen.Thread.Sleep(6000);
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
			Sharpen.Thread.Sleep(5000);
			try
			{
				dtSecretManager.RenewToken(token, "JobTracker");
				NUnit.Framework.Assert.Fail("should have been expired");
			}
			catch (SecretManager.InvalidToken)
			{
			}
		}

		// PASS
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCancelDelegationToken()
		{
			Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> token = GenerateDelegationToken
				("SomeUser", "JobTracker");
			//Fake renewer should not be able to renew
			try
			{
				dtSecretManager.CancelToken(token, "FakeCanceller");
				NUnit.Framework.Assert.Fail("should have failed");
			}
			catch (AccessControlException)
			{
			}
			// PASS
			dtSecretManager.CancelToken(token, "JobTracker");
			try
			{
				dtSecretManager.RenewToken(token, "JobTracker");
				NUnit.Framework.Assert.Fail("should have failed");
			}
			catch (SecretManager.InvalidToken)
			{
			}
		}

		// PASS
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAddDelegationTokensDFSApi()
		{
			UserGroupInformation ugi = UserGroupInformation.CreateRemoteUser("JobTracker");
			DistributedFileSystem dfs = cluster.GetFileSystem();
			Credentials creds = new Credentials();
			Org.Apache.Hadoop.Security.Token.Token<object>[] tokens = dfs.AddDelegationTokens
				("JobTracker", creds);
			NUnit.Framework.Assert.AreEqual(1, tokens.Length);
			NUnit.Framework.Assert.AreEqual(1, creds.NumberOfTokens());
			CheckTokenIdentifier(ugi, tokens[0]);
			Org.Apache.Hadoop.Security.Token.Token<object>[] tokens2 = dfs.AddDelegationTokens
				("JobTracker", creds);
			NUnit.Framework.Assert.AreEqual(0, tokens2.Length);
			// already have token
			NUnit.Framework.Assert.AreEqual(1, creds.NumberOfTokens());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDelegationTokenWebHdfsApi()
		{
			((Log4JLogger)NamenodeWebHdfsMethods.Log).GetLogger().SetLevel(Level.All);
			string uri = WebHdfsFileSystem.Scheme + "://" + config.Get(DFSConfigKeys.DfsNamenodeHttpAddressKey
				);
			//get file system as JobTracker
			UserGroupInformation ugi = UserGroupInformation.CreateUserForTesting("JobTracker"
				, new string[] { "user" });
			WebHdfsFileSystem webhdfs = ugi.DoAs(new _PrivilegedExceptionAction_180(this, uri
				));
			{
				//test addDelegationTokens(..)
				Credentials creds = new Credentials();
				Org.Apache.Hadoop.Security.Token.Token<object>[] tokens = webhdfs.AddDelegationTokens
					("JobTracker", creds);
				NUnit.Framework.Assert.AreEqual(1, tokens.Length);
				NUnit.Framework.Assert.AreEqual(1, creds.NumberOfTokens());
				NUnit.Framework.Assert.AreSame(tokens[0], creds.GetAllTokens().GetEnumerator().Next
					());
				CheckTokenIdentifier(ugi, tokens[0]);
				Org.Apache.Hadoop.Security.Token.Token<object>[] tokens2 = webhdfs.AddDelegationTokens
					("JobTracker", creds);
				NUnit.Framework.Assert.AreEqual(0, tokens2.Length);
			}
		}

		private sealed class _PrivilegedExceptionAction_180 : PrivilegedExceptionAction<WebHdfsFileSystem
			>
		{
			public _PrivilegedExceptionAction_180(TestDelegationToken _enclosing, string uri)
			{
				this._enclosing = _enclosing;
				this.uri = uri;
			}

			/// <exception cref="System.Exception"/>
			public WebHdfsFileSystem Run()
			{
				return (WebHdfsFileSystem)FileSystem.Get(new URI(uri), this._enclosing.config);
			}

			private readonly TestDelegationToken _enclosing;

			private readonly string uri;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDelegationTokenWithDoAs()
		{
			DistributedFileSystem dfs = cluster.GetFileSystem();
			Credentials creds = new Credentials();
			Org.Apache.Hadoop.Security.Token.Token<object>[] tokens = dfs.AddDelegationTokens
				("JobTracker", creds);
			NUnit.Framework.Assert.AreEqual(1, tokens.Length);
			Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> token = (Org.Apache.Hadoop.Security.Token.Token
				<DelegationTokenIdentifier>)tokens[0];
			UserGroupInformation longUgi = UserGroupInformation.CreateRemoteUser("JobTracker/foo.com@FOO.COM"
				);
			UserGroupInformation shortUgi = UserGroupInformation.CreateRemoteUser("JobTracker"
				);
			longUgi.DoAs(new _PrivilegedExceptionAction_212(this, token, longUgi));
			shortUgi.DoAs(new _PrivilegedExceptionAction_223(this, token));
			longUgi.DoAs(new _PrivilegedExceptionAction_230(this, token, longUgi));
		}

		private sealed class _PrivilegedExceptionAction_212 : PrivilegedExceptionAction<object
			>
		{
			public _PrivilegedExceptionAction_212(TestDelegationToken _enclosing, Org.Apache.Hadoop.Security.Token.Token
				<DelegationTokenIdentifier> token, UserGroupInformation longUgi)
			{
				this._enclosing = _enclosing;
				this.token = token;
				this.longUgi = longUgi;
			}

			/// <exception cref="System.IO.IOException"/>
			public object Run()
			{
				try
				{
					token.Renew(this._enclosing.config);
				}
				catch (Exception)
				{
					NUnit.Framework.Assert.Fail("Could not renew delegation token for user " + longUgi
						);
				}
				return null;
			}

			private readonly TestDelegationToken _enclosing;

			private readonly Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier
				> token;

			private readonly UserGroupInformation longUgi;
		}

		private sealed class _PrivilegedExceptionAction_223 : PrivilegedExceptionAction<object
			>
		{
			public _PrivilegedExceptionAction_223(TestDelegationToken _enclosing, Org.Apache.Hadoop.Security.Token.Token
				<DelegationTokenIdentifier> token)
			{
				this._enclosing = _enclosing;
				this.token = token;
			}

			/// <exception cref="System.Exception"/>
			public object Run()
			{
				token.Renew(this._enclosing.config);
				return null;
			}

			private readonly TestDelegationToken _enclosing;

			private readonly Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier
				> token;
		}

		private sealed class _PrivilegedExceptionAction_230 : PrivilegedExceptionAction<object
			>
		{
			public _PrivilegedExceptionAction_230(TestDelegationToken _enclosing, Org.Apache.Hadoop.Security.Token.Token
				<DelegationTokenIdentifier> token, UserGroupInformation longUgi)
			{
				this._enclosing = _enclosing;
				this.token = token;
				this.longUgi = longUgi;
			}

			/// <exception cref="System.IO.IOException"/>
			public object Run()
			{
				try
				{
					token.Cancel(this._enclosing.config);
				}
				catch (Exception)
				{
					NUnit.Framework.Assert.Fail("Could not cancel delegation token for user " + longUgi
						);
				}
				return null;
			}

			private readonly TestDelegationToken _enclosing;

			private readonly Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier
				> token;

			private readonly UserGroupInformation longUgi;
		}

		/// <summary>
		/// Test that the delegation token secret manager only runs when the
		/// NN is out of safe mode.
		/// </summary>
		/// <remarks>
		/// Test that the delegation token secret manager only runs when the
		/// NN is out of safe mode. This is because the secret manager
		/// has to log to the edit log, which should not be written in
		/// safe mode. Regression test for HDFS-2579.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDTManagerInSafeMode()
		{
			cluster.StartDataNodes(config, 1, true, HdfsServerConstants.StartupOption.Regular
				, null);
			FileSystem fs = cluster.GetFileSystem();
			for (int i = 0; i < 5; i++)
			{
				DFSTestUtil.CreateFile(fs, new Path("/test-" + i), 100, (short)1, 1L);
			}
			cluster.GetConfiguration(0).SetInt(DFSConfigKeys.DfsNamenodeDelegationKeyUpdateIntervalKey
				, 500);
			cluster.GetConfiguration(0).SetInt(DFSConfigKeys.DfsNamenodeSafemodeExtensionKey, 
				30000);
			cluster.SetWaitSafeMode(false);
			cluster.RestartNameNode();
			NameNode nn = cluster.GetNameNode();
			NUnit.Framework.Assert.IsTrue(nn.IsInSafeMode());
			DelegationTokenSecretManager sm = NameNodeAdapter.GetDtSecretManager(nn.GetNamesystem
				());
			NUnit.Framework.Assert.IsFalse("Secret manager should not run in safe mode", sm.IsRunning
				());
			NameNodeAdapter.LeaveSafeMode(nn);
			NUnit.Framework.Assert.IsTrue("Secret manager should start when safe mode is exited"
				, sm.IsRunning());
			Log.Info("========= entering safemode again");
			NameNodeAdapter.EnterSafeMode(nn, false);
			NUnit.Framework.Assert.IsFalse("Secret manager should stop again when safe mode "
				 + "is manually entered", sm.IsRunning());
			// Set the cluster to leave safemode quickly on its own.
			cluster.GetConfiguration(0).SetInt(DFSConfigKeys.DfsNamenodeSafemodeExtensionKey, 
				0);
			cluster.SetWaitSafeMode(true);
			cluster.RestartNameNode();
			nn = cluster.GetNameNode();
			sm = NameNodeAdapter.GetDtSecretManager(nn.GetNamesystem());
			NUnit.Framework.Assert.IsFalse(nn.IsInSafeMode());
			NUnit.Framework.Assert.IsTrue(sm.IsRunning());
		}

		/// <exception cref="System.Exception"/>
		private void CheckTokenIdentifier<_T0>(UserGroupInformation ugi, Org.Apache.Hadoop.Security.Token.Token
			<_T0> token)
			where _T0 : TokenIdentifier
		{
			NUnit.Framework.Assert.IsNotNull(token);
			// should be able to use token.decodeIdentifier() but webhdfs isn't
			// registered with the service loader for token decoding
			DelegationTokenIdentifier identifier = new DelegationTokenIdentifier();
			byte[] tokenId = token.GetIdentifier();
			DataInputStream @in = new DataInputStream(new ByteArrayInputStream(tokenId));
			try
			{
				identifier.ReadFields(@in);
			}
			finally
			{
				@in.Close();
			}
			NUnit.Framework.Assert.IsNotNull(identifier);
			Log.Info("A valid token should have non-null password, and should be renewed successfully"
				);
			NUnit.Framework.Assert.IsTrue(null != dtSecretManager.RetrievePassword(identifier
				));
			dtSecretManager.RenewToken((Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier
				>)token, "JobTracker");
			ugi.DoAs(new _PrivilegedExceptionAction_309(this, token));
		}

		private sealed class _PrivilegedExceptionAction_309 : PrivilegedExceptionAction<object
			>
		{
			public _PrivilegedExceptionAction_309(TestDelegationToken _enclosing, Org.Apache.Hadoop.Security.Token.Token
				<object> token)
			{
				this._enclosing = _enclosing;
				this.token = token;
			}

			/// <exception cref="System.Exception"/>
			public object Run()
			{
				token.Renew(this._enclosing.config);
				token.Cancel(this._enclosing.config);
				return null;
			}

			private readonly TestDelegationToken _enclosing;

			private readonly Org.Apache.Hadoop.Security.Token.Token<object> token;
		}
	}
}
