using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Util;
using Org.Mockito;
using Org.Mockito.Internal.Util.Reflection;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Web
{
	public class TestTokenAspect
	{
		private class DummyFs : FileSystem, DelegationTokenRenewer.Renewable, TokenAspect.TokenManagementDelegator
		{
			private static readonly Text TokenKind = new Text("DummyFS Token");

			private bool emulateSecurityEnabled;

			private TokenAspect<TestTokenAspect.DummyFs> tokenAspect;

			private readonly UserGroupInformation ugi = UserGroupInformation.CreateUserForTesting
				("foo", new string[] { "bar" });

			private URI uri;

			/// <exception cref="System.IO.IOException"/>
			public override FSDataOutputStream Append(Path f, int bufferSize, Progressable progress
				)
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void CancelDelegationToken<_T0>(Org.Apache.Hadoop.Security.Token.Token
				<_T0> token)
				where _T0 : TokenIdentifier
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override FSDataOutputStream Create(Path f, FsPermission permission, bool overwrite
				, int bufferSize, short replication, long blockSize, Progressable progress)
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public override bool Delete(Path f, bool recursive)
			{
				return false;
			}

			protected override URI GetCanonicalUri()
			{
				return base.GetCanonicalUri();
			}

			/// <exception cref="System.IO.IOException"/>
			public override FileStatus GetFileStatus(Path f)
			{
				return null;
			}

			public virtual Org.Apache.Hadoop.Security.Token.Token<object> GetRenewToken()
			{
				return null;
			}

			public override URI GetUri()
			{
				return uri;
			}

			public override Path GetWorkingDirectory()
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Initialize(URI name, Configuration conf)
			{
				base.Initialize(name, conf);
				SetConf(conf);
				this.uri = URI.Create(name.GetScheme() + "://" + name.GetAuthority());
				tokenAspect = new TokenAspect<TestTokenAspect.DummyFs>(this, SecurityUtil.BuildTokenService
					(uri), TokenKind);
				if (emulateSecurityEnabled || UserGroupInformation.IsSecurityEnabled())
				{
					tokenAspect.InitDelegationToken(ugi);
				}
			}

			/// <exception cref="System.IO.FileNotFoundException"/>
			/// <exception cref="System.IO.IOException"/>
			public override FileStatus[] ListStatus(Path f)
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public override bool Mkdirs(Path f, FsPermission permission)
			{
				return false;
			}

			/// <exception cref="System.IO.IOException"/>
			public override FSDataInputStream Open(Path f, int bufferSize)
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public override bool Rename(Path src, Path dst)
			{
				return false;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual long RenewDelegationToken<_T0>(Org.Apache.Hadoop.Security.Token.Token
				<_T0> token)
				where _T0 : TokenIdentifier
			{
				return 0;
			}

			public virtual void SetDelegationToken<T>(Org.Apache.Hadoop.Security.Token.Token<
				T> token)
				where T : TokenIdentifier
			{
			}

			public override void SetWorkingDirectory(Path new_dir)
			{
			}
		}

		private static DelegationTokenRenewer.RenewAction<object> GetActionFromTokenAspect
			(TokenAspect<TestTokenAspect.DummyFs> tokenAspect)
		{
			return (DelegationTokenRenewer.RenewAction<object>)Whitebox.GetInternalState(tokenAspect
				, "action");
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.URISyntaxException"/>
		[NUnit.Framework.Test]
		public virtual void TestCachedInitialization()
		{
			Configuration conf = new Configuration();
			TestTokenAspect.DummyFs fs = Org.Mockito.Mockito.Spy(new TestTokenAspect.DummyFs(
				));
			Org.Apache.Hadoop.Security.Token.Token<TokenIdentifier> token = new Org.Apache.Hadoop.Security.Token.Token
				<TokenIdentifier>(new byte[0], new byte[0], TestTokenAspect.DummyFs.TokenKind, new 
				Text("127.0.0.1:1234"));
			Org.Mockito.Mockito.DoReturn(token).When(fs).GetDelegationToken(Matchers.AnyString
				());
			Org.Mockito.Mockito.DoReturn(token).When(fs).GetRenewToken();
			fs.emulateSecurityEnabled = true;
			fs.Initialize(new URI("dummyfs://127.0.0.1:1234"), conf);
			fs.tokenAspect.EnsureTokenInitialized();
			Org.Mockito.Mockito.Verify(fs, Org.Mockito.Mockito.Times(1)).GetDelegationToken(null
				);
			Org.Mockito.Mockito.Verify(fs, Org.Mockito.Mockito.Times(1)).SetDelegationToken(token
				);
			// For the second iteration, the token should be cached.
			fs.tokenAspect.EnsureTokenInitialized();
			Org.Mockito.Mockito.Verify(fs, Org.Mockito.Mockito.Times(1)).GetDelegationToken(null
				);
			Org.Mockito.Mockito.Verify(fs, Org.Mockito.Mockito.Times(1)).SetDelegationToken(token
				);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.URISyntaxException"/>
		[NUnit.Framework.Test]
		public virtual void TestGetRemoteToken()
		{
			Configuration conf = new Configuration();
			TestTokenAspect.DummyFs fs = Org.Mockito.Mockito.Spy(new TestTokenAspect.DummyFs(
				));
			Org.Apache.Hadoop.Security.Token.Token<TokenIdentifier> token = new Org.Apache.Hadoop.Security.Token.Token
				<TokenIdentifier>(new byte[0], new byte[0], TestTokenAspect.DummyFs.TokenKind, new 
				Text("127.0.0.1:1234"));
			Org.Mockito.Mockito.DoReturn(token).When(fs).GetDelegationToken(Matchers.AnyString
				());
			Org.Mockito.Mockito.DoReturn(token).When(fs).GetRenewToken();
			fs.Initialize(new URI("dummyfs://127.0.0.1:1234"), conf);
			fs.tokenAspect.EnsureTokenInitialized();
			// Select a token, store and renew it
			Org.Mockito.Mockito.Verify(fs).SetDelegationToken(token);
			NUnit.Framework.Assert.IsNotNull(Whitebox.GetInternalState(fs.tokenAspect, "dtRenewer"
				));
			NUnit.Framework.Assert.IsNotNull(Whitebox.GetInternalState(fs.tokenAspect, "action"
				));
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.URISyntaxException"/>
		[NUnit.Framework.Test]
		public virtual void TestGetRemoteTokenFailure()
		{
			Configuration conf = new Configuration();
			TestTokenAspect.DummyFs fs = Org.Mockito.Mockito.Spy(new TestTokenAspect.DummyFs(
				));
			IOException e = new IOException();
			Org.Mockito.Mockito.DoThrow(e).When(fs).GetDelegationToken(Matchers.AnyString());
			fs.emulateSecurityEnabled = true;
			fs.Initialize(new URI("dummyfs://127.0.0.1:1234"), conf);
			try
			{
				fs.tokenAspect.EnsureTokenInitialized();
			}
			catch (IOException exc)
			{
				NUnit.Framework.Assert.AreEqual(e, exc);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.URISyntaxException"/>
		[NUnit.Framework.Test]
		public virtual void TestInitWithNoTokens()
		{
			Configuration conf = new Configuration();
			TestTokenAspect.DummyFs fs = Org.Mockito.Mockito.Spy(new TestTokenAspect.DummyFs(
				));
			Org.Mockito.Mockito.DoReturn(null).When(fs).GetDelegationToken(Matchers.AnyString
				());
			fs.Initialize(new URI("dummyfs://127.0.0.1:1234"), conf);
			fs.tokenAspect.EnsureTokenInitialized();
			// No token will be selected.
			Org.Mockito.Mockito.Verify(fs, Org.Mockito.Mockito.Never()).SetDelegationToken(Org.Mockito.Mockito
				.Any<Org.Apache.Hadoop.Security.Token.Token<TokenIdentifier>>());
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.URISyntaxException"/>
		[NUnit.Framework.Test]
		public virtual void TestInitWithUGIToken()
		{
			Configuration conf = new Configuration();
			TestTokenAspect.DummyFs fs = Org.Mockito.Mockito.Spy(new TestTokenAspect.DummyFs(
				));
			Org.Mockito.Mockito.DoReturn(null).When(fs).GetDelegationToken(Matchers.AnyString
				());
			Org.Apache.Hadoop.Security.Token.Token<TokenIdentifier> token = new Org.Apache.Hadoop.Security.Token.Token
				<TokenIdentifier>(new byte[0], new byte[0], TestTokenAspect.DummyFs.TokenKind, new 
				Text("127.0.0.1:1234"));
			fs.ugi.AddToken(token);
			fs.ugi.AddToken(new Org.Apache.Hadoop.Security.Token.Token<TokenIdentifier>(new byte
				[0], new byte[0], new Text("Other token"), new Text("127.0.0.1:8021")));
			NUnit.Framework.Assert.AreEqual("wrong tokens in user", 2, fs.ugi.GetTokens().Count
				);
			fs.emulateSecurityEnabled = true;
			fs.Initialize(new URI("dummyfs://127.0.0.1:1234"), conf);
			fs.tokenAspect.EnsureTokenInitialized();
			// Select a token from ugi (not from the remote host), store it but don't
			// renew it
			Org.Mockito.Mockito.Verify(fs).SetDelegationToken(token);
			Org.Mockito.Mockito.Verify(fs, Org.Mockito.Mockito.Never()).GetDelegationToken(Matchers.AnyString
				());
			NUnit.Framework.Assert.IsNull(Whitebox.GetInternalState(fs.tokenAspect, "dtRenewer"
				));
			NUnit.Framework.Assert.IsNull(Whitebox.GetInternalState(fs.tokenAspect, "action")
				);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRenewal()
		{
			Configuration conf = new Configuration();
			Org.Apache.Hadoop.Security.Token.Token<object> token1 = Org.Mockito.Mockito.Mock<
				Org.Apache.Hadoop.Security.Token.Token>();
			Org.Apache.Hadoop.Security.Token.Token<object> token2 = Org.Mockito.Mockito.Mock<
				Org.Apache.Hadoop.Security.Token.Token>();
			long renewCycle = 100;
			DelegationTokenRenewer.renewCycle = renewCycle;
			UserGroupInformation ugi = UserGroupInformation.CreateUserForTesting("foo", new string
				[] { "bar" });
			TestTokenAspect.DummyFs fs = Org.Mockito.Mockito.Spy(new TestTokenAspect.DummyFs(
				));
			Org.Mockito.Mockito.DoReturn(token1).DoReturn(token2).When(fs).GetDelegationToken
				(null);
			Org.Mockito.Mockito.DoReturn(token1).When(fs).GetRenewToken();
			// cause token renewer to abandon the token
			Org.Mockito.Mockito.DoThrow(new IOException("renew failed")).When(token1).Renew(conf
				);
			Org.Mockito.Mockito.DoThrow(new IOException("get failed")).When(fs).AddDelegationTokens
				(null, null);
			URI uri = new URI("dummyfs://127.0.0.1:1234");
			TokenAspect<TestTokenAspect.DummyFs> tokenAspect = new TokenAspect<TestTokenAspect.DummyFs
				>(fs, SecurityUtil.BuildTokenService(uri), TestTokenAspect.DummyFs.TokenKind);
			fs.Initialize(uri, conf);
			tokenAspect.InitDelegationToken(ugi);
			// trigger token acquisition
			tokenAspect.EnsureTokenInitialized();
			DelegationTokenRenewer.RenewAction<object> action = GetActionFromTokenAspect(tokenAspect
				);
			Org.Mockito.Mockito.Verify(fs).SetDelegationToken(token1);
			NUnit.Framework.Assert.IsTrue(action.IsValid());
			// upon renewal, token will go bad based on above stubbing
			Sharpen.Thread.Sleep(renewCycle * 2);
			NUnit.Framework.Assert.AreSame(action, GetActionFromTokenAspect(tokenAspect));
			NUnit.Framework.Assert.IsFalse(action.IsValid());
			// now that token is invalid, should get a new one
			tokenAspect.EnsureTokenInitialized();
			Org.Mockito.Mockito.Verify(fs, Org.Mockito.Mockito.Times(2)).GetDelegationToken(Matchers.AnyString
				());
			Org.Mockito.Mockito.Verify(fs).SetDelegationToken(token2);
			NUnit.Framework.Assert.AreNotSame(action, GetActionFromTokenAspect(tokenAspect));
			action = GetActionFromTokenAspect(tokenAspect);
			NUnit.Framework.Assert.IsTrue(action.IsValid());
		}
	}
}
