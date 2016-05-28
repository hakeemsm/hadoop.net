using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Mockito;
using Org.Mockito.Invocation;
using Org.Mockito.Stubbing;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Security
{
	public class TestTokenCache
	{
		private static Configuration conf;

		private static string renewer;

		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void Setup()
		{
			conf = new Configuration();
			conf.Set(YarnConfiguration.RmPrincipal, "mapred/host@REALM");
			renewer = Master.GetMasterPrincipal(conf);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestObtainTokens()
		{
			Credentials credentials = new Credentials();
			FileSystem fs = Org.Mockito.Mockito.Mock<FileSystem>();
			TokenCache.ObtainTokensForNamenodesInternal(fs, credentials, conf);
			Org.Mockito.Mockito.Verify(fs).AddDelegationTokens(Matchers.Eq(renewer), Matchers.Eq
				(credentials));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestBinaryCredentialsWithoutScheme()
		{
			TestBinaryCredentials(false);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestBinaryCredentialsWithScheme()
		{
			TestBinaryCredentials(true);
		}

		/// <exception cref="System.Exception"/>
		private void TestBinaryCredentials(bool hasScheme)
		{
			Path TestRootDir = new Path(Runtime.GetProperty("test.build.data", "test/build/data"
				));
			// ick, but need fq path minus file:/
			string binaryTokenFile = hasScheme ? FileSystem.GetLocal(conf).MakeQualified(new 
				Path(TestRootDir, "tokenFile")).ToString() : FileSystem.GetLocal(conf).MakeQualified
				(new Path(TestRootDir, "tokenFile")).ToUri().GetPath();
			FileSystemTestHelper.MockFileSystem fs1 = CreateFileSystemForServiceName("service1"
				);
			FileSystemTestHelper.MockFileSystem fs2 = CreateFileSystemForServiceName("service2"
				);
			FileSystemTestHelper.MockFileSystem fs3 = CreateFileSystemForServiceName("service3"
				);
			// get the tokens for fs1 & fs2 and write out to binary creds file
			Credentials creds = new Credentials();
			Org.Apache.Hadoop.Security.Token.Token<object> token1 = fs1.GetDelegationToken(renewer
				);
			Org.Apache.Hadoop.Security.Token.Token<object> token2 = fs2.GetDelegationToken(renewer
				);
			creds.AddToken(token1.GetService(), token1);
			creds.AddToken(token2.GetService(), token2);
			// wait to set, else the obtain tokens call above will fail with FNF
			conf.Set(MRJobConfig.MapreduceJobCredentialsBinary, binaryTokenFile);
			creds.WriteTokenStorageFile(new Path(binaryTokenFile), conf);
			// re-init creds and add a newer token for fs1
			creds = new Credentials();
			Org.Apache.Hadoop.Security.Token.Token<object> newerToken1 = fs1.GetDelegationToken
				(renewer);
			NUnit.Framework.Assert.AreNotSame(newerToken1, token1);
			creds.AddToken(newerToken1.GetService(), newerToken1);
			CheckToken(creds, newerToken1);
			// get token for fs1, see that fs2's token was loaded 
			TokenCache.ObtainTokensForNamenodesInternal(fs1, creds, conf);
			CheckToken(creds, newerToken1, token2);
			// get token for fs2, nothing should change since already present
			TokenCache.ObtainTokensForNamenodesInternal(fs2, creds, conf);
			CheckToken(creds, newerToken1, token2);
			// get token for fs3, should only add token for fs3
			TokenCache.ObtainTokensForNamenodesInternal(fs3, creds, conf);
			Org.Apache.Hadoop.Security.Token.Token<object> token3 = creds.GetToken(new Text(fs3
				.GetCanonicalServiceName()));
			NUnit.Framework.Assert.IsTrue(token3 != null);
			CheckToken(creds, newerToken1, token2, token3);
			// be paranoid, check one last time that nothing changes
			TokenCache.ObtainTokensForNamenodesInternal(fs1, creds, conf);
			TokenCache.ObtainTokensForNamenodesInternal(fs2, creds, conf);
			TokenCache.ObtainTokensForNamenodesInternal(fs3, creds, conf);
			CheckToken(creds, newerToken1, token2, token3);
		}

		private void CheckToken(Credentials creds, params Org.Apache.Hadoop.Security.Token.Token
			<object>[] tokens)
		{
			NUnit.Framework.Assert.AreEqual(tokens.Length, creds.GetAllTokens().Count);
			foreach (Org.Apache.Hadoop.Security.Token.Token<object> token in tokens)
			{
				Org.Apache.Hadoop.Security.Token.Token<object> credsToken = creds.GetToken(token.
					GetService());
				NUnit.Framework.Assert.IsTrue(credsToken != null);
				NUnit.Framework.Assert.AreEqual(token, credsToken);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private FileSystemTestHelper.MockFileSystem CreateFileSystemForServiceName(string
			 service)
		{
			FileSystemTestHelper.MockFileSystem mockFs = new FileSystemTestHelper.MockFileSystem
				();
			Org.Mockito.Mockito.When(mockFs.GetCanonicalServiceName()).ThenReturn(service);
			Org.Mockito.Mockito.When(mockFs.GetDelegationToken(Any<string>())).ThenAnswer(new 
				_Answer_142(service));
			// use unique value so when we restore from token storage, we can
			// tell if it's really the same token
			return mockFs;
		}

		private sealed class _Answer_142 : Answer<Org.Apache.Hadoop.Security.Token.Token<
			object>>
		{
			public _Answer_142(string service)
			{
				this.service = service;
				this.unique = 0;
			}

			internal int unique;

			/// <exception cref="System.Exception"/>
			public Org.Apache.Hadoop.Security.Token.Token<object> Answer(InvocationOnMock invocation
				)
			{
				Org.Apache.Hadoop.Security.Token.Token<object> token = new Org.Apache.Hadoop.Security.Token.Token
					<TokenIdentifier>();
				token.SetService(new Text(service));
				token.SetKind(new Text("token" + this.unique++));
				return token;
			}

			private readonly string service;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSingleTokenFetch()
		{
			Configuration conf = new Configuration();
			conf.Set(YarnConfiguration.RmPrincipal, "mapred/host@REALM");
			string renewer = Master.GetMasterPrincipal(conf);
			Credentials credentials = new Credentials();
			FileSystemTestHelper.MockFileSystem fs = new FileSystemTestHelper.MockFileSystem(
				);
			FileSystemTestHelper.MockFileSystem mockFs = (FileSystemTestHelper.MockFileSystem
				)((FileSystemTestHelper.MockFileSystem)fs.GetRawFileSystem());
			Org.Mockito.Mockito.When(mockFs.GetCanonicalServiceName()).ThenReturn("host:0");
			Org.Mockito.Mockito.When(mockFs.GetUri()).ThenReturn(new URI("mockfs://host:0"));
			Path mockPath = Org.Mockito.Mockito.Mock<Path>();
			Org.Mockito.Mockito.When(mockPath.GetFileSystem(conf)).ThenReturn(mockFs);
			Path[] paths = new Path[] { mockPath, mockPath };
			Org.Mockito.Mockito.When(mockFs.AddDelegationTokens("me", credentials)).ThenReturn
				(null);
			TokenCache.ObtainTokensForNamenodesInternal(credentials, paths, conf);
			Org.Mockito.Mockito.Verify(mockFs, Org.Mockito.Mockito.Times(1)).AddDelegationTokens
				(renewer, credentials);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCleanUpTokenReferral()
		{
			Configuration conf = new Configuration();
			conf.Set(MRJobConfig.MapreduceJobCredentialsBinary, "foo");
			TokenCache.CleanUpTokenReferral(conf);
			NUnit.Framework.Assert.IsNull(conf.Get(MRJobConfig.MapreduceJobCredentialsBinary)
				);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.URISyntaxException"/>
		[NUnit.Framework.Test]
		public virtual void TestGetTokensForNamenodes()
		{
			Path TestRootDir = new Path(Runtime.GetProperty("test.build.data", "test/build/data"
				));
			// ick, but need fq path minus file:/
			string binaryTokenFile = FileSystem.GetLocal(conf).MakeQualified(new Path(TestRootDir
				, "tokenFile")).ToUri().GetPath();
			FileSystemTestHelper.MockFileSystem fs1 = CreateFileSystemForServiceName("service1"
				);
			Credentials creds = new Credentials();
			Org.Apache.Hadoop.Security.Token.Token<object> token1 = fs1.GetDelegationToken(renewer
				);
			creds.AddToken(token1.GetService(), token1);
			// wait to set, else the obtain tokens call above will fail with FNF
			conf.Set(MRJobConfig.MapreduceJobCredentialsBinary, binaryTokenFile);
			creds.WriteTokenStorageFile(new Path(binaryTokenFile), conf);
			TokenCache.ObtainTokensForNamenodesInternal(fs1, creds, conf);
			string fs_addr = fs1.GetCanonicalServiceName();
			Org.Apache.Hadoop.Security.Token.Token<object> nnt = TokenCache.GetDelegationToken
				(creds, fs_addr);
			NUnit.Framework.Assert.IsNotNull("Token for nn is null", nnt);
		}
	}
}
