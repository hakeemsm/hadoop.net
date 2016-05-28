using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Security.Token.Delegation;
using Org.Apache.Hadoop.Hdfs.Web.Resources;
using Org.Apache.Hadoop.Http;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authentication.Client;
using Org.Apache.Hadoop.Security.Ssl;
using Org.Apache.Hadoop.Security.Token;
using Org.Mockito;
using Org.Mockito.Internal.Util.Reflection;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Web
{
	public class TestWebHdfsTokens
	{
		private static Configuration conf;

		internal URI uri = null;

		[BeforeClass]
		public static void SetUp()
		{
			conf = new Configuration();
			SecurityUtil.SetAuthenticationMethod(UserGroupInformation.AuthenticationMethod.Kerberos
				, conf);
			UserGroupInformation.SetConfiguration(conf);
			UserGroupInformation.SetLoginUser(UserGroupInformation.CreateUserForTesting("LoginUser"
				, new string[] { "supergroup" }));
		}

		/// <exception cref="System.IO.IOException"/>
		private WebHdfsFileSystem SpyWebhdfsInSecureSetup()
		{
			WebHdfsFileSystem fsOrig = new WebHdfsFileSystem();
			fsOrig.Initialize(URI.Create("webhdfs://127.0.0.1:0"), conf);
			WebHdfsFileSystem fs = Org.Mockito.Mockito.Spy(fsOrig);
			return fs;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestTokenForNonTokenOp()
		{
			WebHdfsFileSystem fs = SpyWebhdfsInSecureSetup();
			Org.Apache.Hadoop.Security.Token.Token<object> token = Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Security.Token.Token
				>();
			Org.Mockito.Mockito.DoReturn(token).When(fs).GetDelegationToken(null);
			// should get/set/renew token
			fs.ToUrl(GetOpParam.OP.Open, null);
			Org.Mockito.Mockito.Verify(fs).GetDelegationToken();
			Org.Mockito.Mockito.Verify(fs).GetDelegationToken(null);
			Org.Mockito.Mockito.Verify(fs).SetDelegationToken(token);
			Org.Mockito.Mockito.Reset(fs);
			// should return prior token
			fs.ToUrl(GetOpParam.OP.Open, null);
			Org.Mockito.Mockito.Verify(fs).GetDelegationToken();
			Org.Mockito.Mockito.Verify(fs, Org.Mockito.Mockito.Never()).GetDelegationToken(null
				);
			Org.Mockito.Mockito.Verify(fs, Org.Mockito.Mockito.Never()).SetDelegationToken(token
				);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestNoTokenForGetToken()
		{
			CheckNoTokenForOperation(GetOpParam.OP.Getdelegationtoken);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestNoTokenForRenewToken()
		{
			CheckNoTokenForOperation(PutOpParam.OP.Renewdelegationtoken);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestNoTokenForCancelToken()
		{
			CheckNoTokenForOperation(PutOpParam.OP.Canceldelegationtoken);
		}

		/// <exception cref="System.IO.IOException"/>
		private void CheckNoTokenForOperation(HttpOpParam.OP op)
		{
			WebHdfsFileSystem fs = SpyWebhdfsInSecureSetup();
			Org.Mockito.Mockito.DoReturn(null).When(fs).GetDelegationToken(null);
			fs.Initialize(URI.Create("webhdfs://127.0.0.1:0"), conf);
			// do not get a token!
			fs.ToUrl(op, null);
			Org.Mockito.Mockito.Verify(fs, Org.Mockito.Mockito.Never()).GetDelegationToken();
			Org.Mockito.Mockito.Verify(fs, Org.Mockito.Mockito.Never()).GetDelegationToken(null
				);
			Org.Mockito.Mockito.Verify(fs, Org.Mockito.Mockito.Never()).SetDelegationToken((Org.Apache.Hadoop.Security.Token.Token
				<object>)Matchers.Any<Org.Apache.Hadoop.Security.Token.Token>());
		}

		public virtual void TestGetOpRequireAuth()
		{
			foreach (HttpOpParam.OP op in GetOpParam.OP.Values())
			{
				bool expect = (op == GetOpParam.OP.Getdelegationtoken);
				NUnit.Framework.Assert.AreEqual(expect, op.GetRequireAuth());
			}
		}

		public virtual void TestPutOpRequireAuth()
		{
			foreach (HttpOpParam.OP op in PutOpParam.OP.Values())
			{
				bool expect = (op == PutOpParam.OP.Renewdelegationtoken || op == PutOpParam.OP.Canceldelegationtoken
					);
				NUnit.Framework.Assert.AreEqual(expect, op.GetRequireAuth());
			}
		}

		public virtual void TestPostOpRequireAuth()
		{
			foreach (HttpOpParam.OP op in PostOpParam.OP.Values())
			{
				NUnit.Framework.Assert.IsFalse(op.GetRequireAuth());
			}
		}

		public virtual void TestDeleteOpRequireAuth()
		{
			foreach (HttpOpParam.OP op in DeleteOpParam.OP.Values())
			{
				NUnit.Framework.Assert.IsFalse(op.GetRequireAuth());
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestLazyTokenFetchForWebhdfs()
		{
			// for any(Token.class)
			MiniDFSCluster cluster = null;
			WebHdfsFileSystem fs = null;
			try
			{
				Configuration clusterConf = new HdfsConfiguration(conf);
				SecurityUtil.SetAuthenticationMethod(UserGroupInformation.AuthenticationMethod.Simple
					, clusterConf);
				clusterConf.SetBoolean(DFSConfigKeys.DfsNamenodeDelegationTokenAlwaysUseKey, true
					);
				// trick the NN into thinking security is enabled w/o it trying
				// to login from a keytab
				UserGroupInformation.SetConfiguration(clusterConf);
				cluster = new MiniDFSCluster.Builder(clusterConf).NumDataNodes(1).Build();
				cluster.WaitActive();
				SecurityUtil.SetAuthenticationMethod(UserGroupInformation.AuthenticationMethod.Kerberos
					, clusterConf);
				UserGroupInformation.SetConfiguration(clusterConf);
				uri = DFSUtil.CreateUri("webhdfs", cluster.GetNameNode().GetHttpAddress());
				ValidateLazyTokenFetch(clusterConf);
			}
			finally
			{
				IOUtils.Cleanup(null, fs);
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestLazyTokenFetchForSWebhdfs()
		{
			// for any(Token.class)
			MiniDFSCluster cluster = null;
			SWebHdfsFileSystem fs = null;
			try
			{
				Configuration clusterConf = new HdfsConfiguration(conf);
				SecurityUtil.SetAuthenticationMethod(UserGroupInformation.AuthenticationMethod.Simple
					, clusterConf);
				clusterConf.SetBoolean(DFSConfigKeys.DfsNamenodeDelegationTokenAlwaysUseKey, true
					);
				string Basedir = Runtime.GetProperty("test.build.dir", "target/test-dir") + "/" +
					 typeof(TestWebHdfsTokens).Name;
				string keystoresDir;
				string sslConfDir;
				clusterConf.SetBoolean(DFSConfigKeys.DfsWebhdfsEnabledKey, true);
				clusterConf.Set(DFSConfigKeys.DfsHttpPolicyKey, HttpConfig.Policy.HttpsOnly.ToString
					());
				clusterConf.Set(DFSConfigKeys.DfsNamenodeHttpsAddressKey, "localhost:0");
				clusterConf.Set(DFSConfigKeys.DfsDatanodeHttpsAddressKey, "localhost:0");
				FilePath @base = new FilePath(Basedir);
				FileUtil.FullyDelete(@base);
				@base.Mkdirs();
				keystoresDir = new FilePath(Basedir).GetAbsolutePath();
				sslConfDir = KeyStoreTestUtil.GetClasspathDir(typeof(TestWebHdfsTokens));
				KeyStoreTestUtil.SetupSSLConfig(keystoresDir, sslConfDir, clusterConf, false);
				// trick the NN into thinking security is enabled w/o it trying
				// to login from a keytab
				UserGroupInformation.SetConfiguration(clusterConf);
				cluster = new MiniDFSCluster.Builder(clusterConf).NumDataNodes(1).Build();
				cluster.WaitActive();
				IPEndPoint addr = cluster.GetNameNode().GetHttpsAddress();
				string nnAddr = NetUtils.GetHostPortString(addr);
				clusterConf.Set(DFSConfigKeys.DfsNamenodeHttpsAddressKey, nnAddr);
				SecurityUtil.SetAuthenticationMethod(UserGroupInformation.AuthenticationMethod.Kerberos
					, clusterConf);
				UserGroupInformation.SetConfiguration(clusterConf);
				uri = DFSUtil.CreateUri("swebhdfs", cluster.GetNameNode().GetHttpsAddress());
				ValidateLazyTokenFetch(clusterConf);
			}
			finally
			{
				IOUtils.Cleanup(null, fs);
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSetTokenServiceAndKind()
		{
			MiniDFSCluster cluster = null;
			try
			{
				Configuration clusterConf = new HdfsConfiguration(conf);
				SecurityUtil.SetAuthenticationMethod(UserGroupInformation.AuthenticationMethod.Simple
					, clusterConf);
				clusterConf.SetBoolean(DFSConfigKeys.DfsNamenodeDelegationTokenAlwaysUseKey, true
					);
				// trick the NN into thinking s[ecurity is enabled w/o it trying
				// to login from a keytab
				UserGroupInformation.SetConfiguration(clusterConf);
				cluster = new MiniDFSCluster.Builder(clusterConf).NumDataNodes(0).Build();
				cluster.WaitActive();
				SecurityUtil.SetAuthenticationMethod(UserGroupInformation.AuthenticationMethod.Kerberos
					, clusterConf);
				WebHdfsFileSystem fs = WebHdfsTestUtil.GetWebHdfsFileSystem(clusterConf, "webhdfs"
					);
				Whitebox.SetInternalState(fs, "canRefreshDelegationToken", true);
				URLConnectionFactory factory = new _URLConnectionFactory_268(new _ConnectionConfigurator_262
					());
				Whitebox.SetInternalState(fs, "connectionFactory", factory);
				Org.Apache.Hadoop.Security.Token.Token<object> token1 = fs.GetDelegationToken();
				NUnit.Framework.Assert.AreEqual(new Text("bar"), token1.GetKind());
				HttpOpParam.OP op = GetOpParam.OP.Getdelegationtoken;
				Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> token2 = new _FsPathResponseRunner_281
					(op, null, new RenewerParam(null)).Run();
				NUnit.Framework.Assert.AreEqual(new Text("bar"), token2.GetKind());
				NUnit.Framework.Assert.AreEqual(new Text("foo"), token2.GetService());
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		private sealed class _URLConnectionFactory_268 : URLConnectionFactory
		{
			public _URLConnectionFactory_268(ConnectionConfigurator baseArg1)
				: base(baseArg1)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override URLConnection OpenConnection(Uri url)
			{
				return base.OpenConnection(new Uri(url + "&service=foo&kind=bar"));
			}
		}

		private sealed class _ConnectionConfigurator_262 : ConnectionConfigurator
		{
			public _ConnectionConfigurator_262()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public HttpURLConnection Configure(HttpURLConnection conn)
			{
				return conn;
			}
		}

		private sealed class _FsPathResponseRunner_281 : WebHdfsFileSystem.FsPathResponseRunner
			<Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier>>
		{
			public _FsPathResponseRunner_281(HttpOpParam.OP baseArg1, Path baseArg2, Param<object
				, object>[] baseArg3)
				: base(baseArg1, baseArg2, baseArg3)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			internal override Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier
				> DecodeResponse<_T0>(IDictionary<_T0> json)
			{
				return JsonUtil.ToDelegationToken(json);
			}
		}

		/// <exception cref="System.Exception"/>
		private void ValidateLazyTokenFetch(Configuration clusterConf)
		{
			string testUser = "DummyUser";
			UserGroupInformation ugi = UserGroupInformation.CreateUserForTesting(testUser, new 
				string[] { "supergroup" });
			WebHdfsFileSystem fs = ugi.DoAs(new _PrivilegedExceptionAction_304(this, clusterConf
				));
			// verify token ops don't get a token
			NUnit.Framework.Assert.IsNull(fs.GetRenewToken());
			Org.Apache.Hadoop.Security.Token.Token<object> token = ((Org.Apache.Hadoop.Security.Token.Token
				<DelegationTokenIdentifier>)fs.GetDelegationToken(null));
			fs.RenewDelegationToken(token);
			fs.CancelDelegationToken(token);
			Org.Mockito.Mockito.Verify(fs, Org.Mockito.Mockito.Never()).GetDelegationToken();
			Org.Mockito.Mockito.Verify(fs, Org.Mockito.Mockito.Never()).ReplaceExpiredDelegationToken
				();
			Org.Mockito.Mockito.Verify(fs, Org.Mockito.Mockito.Never()).SetDelegationToken(Matchers.Any
				<Org.Apache.Hadoop.Security.Token.Token>());
			NUnit.Framework.Assert.IsNull(fs.GetRenewToken());
			Org.Mockito.Mockito.Reset(fs);
			// verify first non-token op gets a token
			Path p = new Path("/f");
			fs.Create(p, (short)1).Close();
			Org.Mockito.Mockito.Verify(fs, Org.Mockito.Mockito.Times(1)).GetDelegationToken();
			Org.Mockito.Mockito.Verify(fs, Org.Mockito.Mockito.Never()).ReplaceExpiredDelegationToken
				();
			Org.Mockito.Mockito.Verify(fs, Org.Mockito.Mockito.Times(1)).GetDelegationToken(Matchers.AnyString
				());
			Org.Mockito.Mockito.Verify(fs, Org.Mockito.Mockito.Times(1)).SetDelegationToken(Matchers.Any
				<Org.Apache.Hadoop.Security.Token.Token>());
			token = fs.GetRenewToken();
			NUnit.Framework.Assert.IsNotNull(token);
			NUnit.Framework.Assert.AreEqual(testUser, GetTokenOwner(token));
			NUnit.Framework.Assert.AreEqual(fs.GetTokenKind(), token.GetKind());
			Org.Mockito.Mockito.Reset(fs);
			// verify prior token is reused
			fs.GetFileStatus(p);
			Org.Mockito.Mockito.Verify(fs, Org.Mockito.Mockito.Times(1)).GetDelegationToken();
			Org.Mockito.Mockito.Verify(fs, Org.Mockito.Mockito.Never()).ReplaceExpiredDelegationToken
				();
			Org.Mockito.Mockito.Verify(fs, Org.Mockito.Mockito.Never()).GetDelegationToken(Matchers.AnyString
				());
			Org.Mockito.Mockito.Verify(fs, Org.Mockito.Mockito.Never()).SetDelegationToken(Matchers.Any
				<Org.Apache.Hadoop.Security.Token.Token>());
			Org.Apache.Hadoop.Security.Token.Token<object> token2 = fs.GetRenewToken();
			NUnit.Framework.Assert.IsNotNull(token2);
			NUnit.Framework.Assert.AreEqual(fs.GetTokenKind(), token.GetKind());
			NUnit.Framework.Assert.AreSame(token, token2);
			Org.Mockito.Mockito.Reset(fs);
			// verify renew of expired token fails w/o getting a new token
			token = fs.GetRenewToken();
			fs.CancelDelegationToken(token);
			try
			{
				fs.RenewDelegationToken(token);
				NUnit.Framework.Assert.Fail("should have failed");
			}
			catch (SecretManager.InvalidToken)
			{
			}
			catch (Exception ex)
			{
				NUnit.Framework.Assert.Fail("wrong exception:" + ex);
			}
			Org.Mockito.Mockito.Verify(fs, Org.Mockito.Mockito.Never()).GetDelegationToken();
			Org.Mockito.Mockito.Verify(fs, Org.Mockito.Mockito.Never()).ReplaceExpiredDelegationToken
				();
			Org.Mockito.Mockito.Verify(fs, Org.Mockito.Mockito.Never()).GetDelegationToken(Matchers.AnyString
				());
			Org.Mockito.Mockito.Verify(fs, Org.Mockito.Mockito.Never()).SetDelegationToken(Matchers.Any
				<Org.Apache.Hadoop.Security.Token.Token>());
			token2 = fs.GetRenewToken();
			NUnit.Framework.Assert.IsNotNull(token2);
			NUnit.Framework.Assert.AreEqual(fs.GetTokenKind(), token.GetKind());
			NUnit.Framework.Assert.AreSame(token, token2);
			Org.Mockito.Mockito.Reset(fs);
			// verify cancel of expired token fails w/o getting a new token
			try
			{
				fs.CancelDelegationToken(token);
				NUnit.Framework.Assert.Fail("should have failed");
			}
			catch (SecretManager.InvalidToken)
			{
			}
			catch (Exception ex)
			{
				NUnit.Framework.Assert.Fail("wrong exception:" + ex);
			}
			Org.Mockito.Mockito.Verify(fs, Org.Mockito.Mockito.Never()).GetDelegationToken();
			Org.Mockito.Mockito.Verify(fs, Org.Mockito.Mockito.Never()).ReplaceExpiredDelegationToken
				();
			Org.Mockito.Mockito.Verify(fs, Org.Mockito.Mockito.Never()).GetDelegationToken(Matchers.AnyString
				());
			Org.Mockito.Mockito.Verify(fs, Org.Mockito.Mockito.Never()).SetDelegationToken(Matchers.Any
				<Org.Apache.Hadoop.Security.Token.Token>());
			token2 = fs.GetRenewToken();
			NUnit.Framework.Assert.IsNotNull(token2);
			NUnit.Framework.Assert.AreEqual(fs.GetTokenKind(), token.GetKind());
			NUnit.Framework.Assert.AreSame(token, token2);
			Org.Mockito.Mockito.Reset(fs);
			// verify an expired token is replaced with a new token
			fs.Open(p).Close();
			Org.Mockito.Mockito.Verify(fs, Org.Mockito.Mockito.Times(2)).GetDelegationToken();
			// first bad, then good
			Org.Mockito.Mockito.Verify(fs, Org.Mockito.Mockito.Times(1)).ReplaceExpiredDelegationToken
				();
			Org.Mockito.Mockito.Verify(fs, Org.Mockito.Mockito.Times(1)).GetDelegationToken(null
				);
			Org.Mockito.Mockito.Verify(fs, Org.Mockito.Mockito.Times(1)).SetDelegationToken(Matchers.Any
				<Org.Apache.Hadoop.Security.Token.Token>());
			token2 = fs.GetRenewToken();
			NUnit.Framework.Assert.IsNotNull(token2);
			NUnit.Framework.Assert.AreEqual(fs.GetTokenKind(), token.GetKind());
			NUnit.Framework.Assert.AreNotSame(token, token2);
			NUnit.Framework.Assert.AreEqual(testUser, GetTokenOwner(token2));
			Org.Mockito.Mockito.Reset(fs);
			// verify with open because it's a little different in how it
			// opens connections
			fs.CancelDelegationToken(fs.GetRenewToken());
			InputStream @is = fs.Open(p);
			@is.Read();
			@is.Close();
			Org.Mockito.Mockito.Verify(fs, Org.Mockito.Mockito.Times(2)).GetDelegationToken();
			// first bad, then good
			Org.Mockito.Mockito.Verify(fs, Org.Mockito.Mockito.Times(1)).ReplaceExpiredDelegationToken
				();
			Org.Mockito.Mockito.Verify(fs, Org.Mockito.Mockito.Times(1)).GetDelegationToken(null
				);
			Org.Mockito.Mockito.Verify(fs, Org.Mockito.Mockito.Times(1)).SetDelegationToken(Matchers.Any
				<Org.Apache.Hadoop.Security.Token.Token>());
			token2 = fs.GetRenewToken();
			NUnit.Framework.Assert.IsNotNull(token2);
			NUnit.Framework.Assert.AreEqual(fs.GetTokenKind(), token.GetKind());
			NUnit.Framework.Assert.AreNotSame(token, token2);
			NUnit.Framework.Assert.AreEqual(testUser, GetTokenOwner(token2));
			Org.Mockito.Mockito.Reset(fs);
			// verify fs close cancels the token
			fs.Close();
			Org.Mockito.Mockito.Verify(fs, Org.Mockito.Mockito.Never()).GetDelegationToken();
			Org.Mockito.Mockito.Verify(fs, Org.Mockito.Mockito.Never()).ReplaceExpiredDelegationToken
				();
			Org.Mockito.Mockito.Verify(fs, Org.Mockito.Mockito.Never()).GetDelegationToken(Matchers.AnyString
				());
			Org.Mockito.Mockito.Verify(fs, Org.Mockito.Mockito.Never()).SetDelegationToken(Matchers.Any
				<Org.Apache.Hadoop.Security.Token.Token>());
			Org.Mockito.Mockito.Verify(fs, Org.Mockito.Mockito.Times(1)).CancelDelegationToken
				(Matchers.Eq(token2));
			// add a token to ugi for a new fs, verify it uses that token
			token = ((Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier>)fs.GetDelegationToken
				(null));
			ugi.AddToken(token);
			fs = ugi.DoAs(new _PrivilegedExceptionAction_426(this, clusterConf));
			NUnit.Framework.Assert.IsNull(fs.GetRenewToken());
			fs.GetFileStatus(new Path("/"));
			Org.Mockito.Mockito.Verify(fs, Org.Mockito.Mockito.Times(1)).GetDelegationToken();
			Org.Mockito.Mockito.Verify(fs, Org.Mockito.Mockito.Never()).ReplaceExpiredDelegationToken
				();
			Org.Mockito.Mockito.Verify(fs, Org.Mockito.Mockito.Never()).GetDelegationToken(Matchers.AnyString
				());
			Org.Mockito.Mockito.Verify(fs, Org.Mockito.Mockito.Times(1)).SetDelegationToken(Matchers.Eq
				(token));
			token2 = fs.GetRenewToken();
			NUnit.Framework.Assert.IsNotNull(token2);
			NUnit.Framework.Assert.AreEqual(fs.GetTokenKind(), token.GetKind());
			NUnit.Framework.Assert.AreSame(token, token2);
			Org.Mockito.Mockito.Reset(fs);
			// verify it reuses the prior ugi token
			fs.GetFileStatus(new Path("/"));
			Org.Mockito.Mockito.Verify(fs, Org.Mockito.Mockito.Times(1)).GetDelegationToken();
			Org.Mockito.Mockito.Verify(fs, Org.Mockito.Mockito.Never()).ReplaceExpiredDelegationToken
				();
			Org.Mockito.Mockito.Verify(fs, Org.Mockito.Mockito.Never()).GetDelegationToken(Matchers.AnyString
				());
			Org.Mockito.Mockito.Verify(fs, Org.Mockito.Mockito.Never()).SetDelegationToken(Matchers.Any
				<Org.Apache.Hadoop.Security.Token.Token>());
			token2 = fs.GetRenewToken();
			NUnit.Framework.Assert.IsNotNull(token2);
			NUnit.Framework.Assert.AreEqual(fs.GetTokenKind(), token.GetKind());
			NUnit.Framework.Assert.AreSame(token, token2);
			Org.Mockito.Mockito.Reset(fs);
			// verify an expired ugi token is NOT replaced with a new token
			fs.CancelDelegationToken(token);
			for (int i = 0; i < 2; i++)
			{
				try
				{
					fs.GetFileStatus(new Path("/"));
					NUnit.Framework.Assert.Fail("didn't fail");
				}
				catch (SecretManager.InvalidToken)
				{
				}
				catch (Exception ex)
				{
					NUnit.Framework.Assert.Fail("wrong exception:" + ex);
				}
				Org.Mockito.Mockito.Verify(fs, Org.Mockito.Mockito.Times(1)).GetDelegationToken();
				Org.Mockito.Mockito.Verify(fs, Org.Mockito.Mockito.Times(1)).ReplaceExpiredDelegationToken
					();
				Org.Mockito.Mockito.Verify(fs, Org.Mockito.Mockito.Never()).GetDelegationToken(Matchers.AnyString
					());
				Org.Mockito.Mockito.Verify(fs, Org.Mockito.Mockito.Never()).SetDelegationToken(Matchers.Any
					<Org.Apache.Hadoop.Security.Token.Token>());
				token2 = fs.GetRenewToken();
				NUnit.Framework.Assert.IsNotNull(token2);
				NUnit.Framework.Assert.AreEqual(fs.GetTokenKind(), token.GetKind());
				NUnit.Framework.Assert.AreSame(token, token2);
				Org.Mockito.Mockito.Reset(fs);
			}
			// verify fs close does NOT cancel the ugi token
			fs.Close();
			Org.Mockito.Mockito.Verify(fs, Org.Mockito.Mockito.Never()).GetDelegationToken();
			Org.Mockito.Mockito.Verify(fs, Org.Mockito.Mockito.Never()).ReplaceExpiredDelegationToken
				();
			Org.Mockito.Mockito.Verify(fs, Org.Mockito.Mockito.Never()).GetDelegationToken(Matchers.AnyString
				());
			Org.Mockito.Mockito.Verify(fs, Org.Mockito.Mockito.Never()).SetDelegationToken(Matchers.Any
				<Org.Apache.Hadoop.Security.Token.Token>());
			Org.Mockito.Mockito.Verify(fs, Org.Mockito.Mockito.Never()).CancelDelegationToken
				(Matchers.Any<Org.Apache.Hadoop.Security.Token.Token>());
		}

		private sealed class _PrivilegedExceptionAction_304 : PrivilegedExceptionAction<WebHdfsFileSystem
			>
		{
			public _PrivilegedExceptionAction_304(TestWebHdfsTokens _enclosing, Configuration
				 clusterConf)
			{
				this._enclosing = _enclosing;
				this.clusterConf = clusterConf;
			}

			/// <exception cref="System.IO.IOException"/>
			public WebHdfsFileSystem Run()
			{
				return Org.Mockito.Mockito.Spy((WebHdfsFileSystem)FileSystem.NewInstance(this._enclosing
					.uri, clusterConf));
			}

			private readonly TestWebHdfsTokens _enclosing;

			private readonly Configuration clusterConf;
		}

		private sealed class _PrivilegedExceptionAction_426 : PrivilegedExceptionAction<WebHdfsFileSystem
			>
		{
			public _PrivilegedExceptionAction_426(TestWebHdfsTokens _enclosing, Configuration
				 clusterConf)
			{
				this._enclosing = _enclosing;
				this.clusterConf = clusterConf;
			}

			/// <exception cref="System.IO.IOException"/>
			public WebHdfsFileSystem Run()
			{
				return Org.Mockito.Mockito.Spy((WebHdfsFileSystem)FileSystem.NewInstance(this._enclosing
					.uri, clusterConf));
			}

			private readonly TestWebHdfsTokens _enclosing;

			private readonly Configuration clusterConf;
		}

		/// <exception cref="System.IO.IOException"/>
		private string GetTokenOwner<_T0>(Org.Apache.Hadoop.Security.Token.Token<_T0> token
			)
			where _T0 : TokenIdentifier
		{
			// webhdfs doesn't register properly with the class loader
			Org.Apache.Hadoop.Security.Token.Token<object> clone = new Org.Apache.Hadoop.Security.Token.Token
				(token);
			clone.SetKind(DelegationTokenIdentifier.HdfsDelegationKind);
			return clone.DecodeIdentifier().GetUser().GetUserName();
		}
	}
}
