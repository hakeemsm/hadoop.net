using System;
using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Token;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Viewfs
{
	/// <summary>
	/// Test ViewFileSystem's support for having delegation tokens fetched and cached
	/// for the file system.
	/// </summary>
	/// <remarks>
	/// Test ViewFileSystem's support for having delegation tokens fetched and cached
	/// for the file system.
	/// Currently this class just ensures that getCanonicalServiceName() always
	/// returns <code>null</code> for ViewFileSystem instances.
	/// </remarks>
	public class TestViewFileSystemDelegationTokenSupport
	{
		private const string MountTableName = "vfs-cluster";

		internal static Configuration conf;

		internal static FileSystem viewFs;

		internal static TestViewFileSystemDelegationTokenSupport.FakeFileSystem fs1;

		internal static TestViewFileSystemDelegationTokenSupport.FakeFileSystem fs2;

		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void Setup()
		{
			conf = ViewFileSystemTestSetup.CreateConfig();
			fs1 = SetupFileSystem(new URI("fs1:///"), typeof(TestViewFileSystemDelegationTokenSupport.FakeFileSystem
				));
			fs2 = SetupFileSystem(new URI("fs2:///"), typeof(TestViewFileSystemDelegationTokenSupport.FakeFileSystem
				));
			viewFs = FileSystem.Get(FsConstants.ViewfsUri, conf);
		}

		/// <exception cref="System.Exception"/>
		internal static TestViewFileSystemDelegationTokenSupport.FakeFileSystem SetupFileSystem
			(URI uri, Type clazz)
		{
			string scheme = uri.GetScheme();
			conf.Set("fs." + scheme + ".impl", clazz.FullName);
			TestViewFileSystemDelegationTokenSupport.FakeFileSystem fs = (TestViewFileSystemDelegationTokenSupport.FakeFileSystem
				)FileSystem.Get(uri, conf);
			// mount each fs twice, will later ensure 1 token/fs
			ConfigUtil.AddLink(conf, "/mounts/" + scheme + "-one", fs.GetUri());
			ConfigUtil.AddLink(conf, "/mounts/" + scheme + "-two", fs.GetUri());
			return fs;
		}

		/// <summary>Regression test for HADOOP-8408.</summary>
		/// <exception cref="Sharpen.URISyntaxException"/>
		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestGetCanonicalServiceNameWithNonDefaultMountTable()
		{
			Configuration conf = new Configuration();
			ConfigUtil.AddLink(conf, MountTableName, "/user", new URI("file:///"));
			FileSystem viewFs = FileSystem.Get(new URI(FsConstants.ViewfsScheme + "://" + MountTableName
				), conf);
			string serviceName = viewFs.GetCanonicalServiceName();
			NUnit.Framework.Assert.IsNull(serviceName);
		}

		/// <exception cref="Sharpen.URISyntaxException"/>
		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestGetCanonicalServiceNameWithDefaultMountTable()
		{
			Configuration conf = new Configuration();
			ConfigUtil.AddLink(conf, "/user", new URI("file:///"));
			FileSystem viewFs = FileSystem.Get(FsConstants.ViewfsUri, conf);
			string serviceName = viewFs.GetCanonicalServiceName();
			NUnit.Framework.Assert.IsNull(serviceName);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestGetChildFileSystems()
		{
			NUnit.Framework.Assert.IsNull(fs1.GetChildFileSystems());
			NUnit.Framework.Assert.IsNull(fs2.GetChildFileSystems());
			IList<FileSystem> children = Arrays.AsList(viewFs.GetChildFileSystems());
			Assert.Equal(2, children.Count);
			Assert.True(children.Contains(fs1));
			Assert.True(children.Contains(fs2));
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestAddDelegationTokens()
		{
			Credentials creds = new Credentials();
			Org.Apache.Hadoop.Security.Token.Token<object>[] fs1Tokens = AddTokensWithCreds(fs1
				, creds);
			Assert.Equal(1, fs1Tokens.Length);
			Assert.Equal(1, creds.NumberOfTokens());
			Org.Apache.Hadoop.Security.Token.Token<object>[] fs2Tokens = AddTokensWithCreds(fs2
				, creds);
			Assert.Equal(1, fs2Tokens.Length);
			Assert.Equal(2, creds.NumberOfTokens());
			Credentials savedCreds = creds;
			creds = new Credentials();
			// should get the same set of tokens as explicitly fetched above
			Org.Apache.Hadoop.Security.Token.Token<object>[] viewFsTokens = viewFs.AddDelegationTokens
				("me", creds);
			Assert.Equal(2, viewFsTokens.Length);
			Assert.True(creds.GetAllTokens().ContainsAll(savedCreds.GetAllTokens
				()));
			Assert.Equal(savedCreds.NumberOfTokens(), creds.NumberOfTokens
				());
			// should get none, already have all tokens
			viewFsTokens = viewFs.AddDelegationTokens("me", creds);
			Assert.Equal(0, viewFsTokens.Length);
			Assert.True(creds.GetAllTokens().ContainsAll(savedCreds.GetAllTokens
				()));
			Assert.Equal(savedCreds.NumberOfTokens(), creds.NumberOfTokens
				());
		}

		/// <exception cref="System.Exception"/>
		internal virtual Org.Apache.Hadoop.Security.Token.Token<object>[] AddTokensWithCreds
			(FileSystem fs, Credentials creds)
		{
			Credentials savedCreds;
			savedCreds = new Credentials(creds);
			Org.Apache.Hadoop.Security.Token.Token<object>[] tokens = fs.AddDelegationTokens(
				"me", creds);
			// test that we got the token we wanted, and that creds were modified
			Assert.Equal(1, tokens.Length);
			Assert.Equal(fs.GetCanonicalServiceName(), tokens[0].GetService
				().ToString());
			Assert.True(creds.GetAllTokens().Contains(tokens[0]));
			Assert.True(creds.GetAllTokens().ContainsAll(savedCreds.GetAllTokens
				()));
			Assert.Equal(savedCreds.NumberOfTokens() + 1, creds.NumberOfTokens
				());
			// shouldn't get any new tokens since already in creds
			savedCreds = new Credentials(creds);
			Org.Apache.Hadoop.Security.Token.Token<object>[] tokenRefetch = fs.AddDelegationTokens
				("me", creds);
			Assert.Equal(0, tokenRefetch.Length);
			Assert.True(creds.GetAllTokens().ContainsAll(savedCreds.GetAllTokens
				()));
			Assert.Equal(savedCreds.NumberOfTokens(), creds.NumberOfTokens
				());
			return tokens;
		}

		internal class FakeFileSystem : RawLocalFileSystem
		{
			internal URI uri;

			/// <exception cref="System.IO.IOException"/>
			public override void Initialize(URI name, Configuration conf)
			{
				this.uri = name;
			}

			protected internal override Path GetInitialWorkingDirectory()
			{
				return new Path("/");
			}

			// ctor calls getUri before the uri is inited...
			public override URI GetUri()
			{
				return uri;
			}

			public override string GetCanonicalServiceName()
			{
				return (this.GetUri() + "/" + this.GetHashCode()).ToString();
			}

			/// <exception cref="System.IO.IOException"/>
			public override Org.Apache.Hadoop.Security.Token.Token<object> GetDelegationToken
				(string renewer)
			{
				Org.Apache.Hadoop.Security.Token.Token<object> token = new Org.Apache.Hadoop.Security.Token.Token
					<TokenIdentifier>();
				token.SetService(new Text(GetCanonicalServiceName()));
				return token;
			}

			public override void Close()
			{
			}
		}
	}
}
