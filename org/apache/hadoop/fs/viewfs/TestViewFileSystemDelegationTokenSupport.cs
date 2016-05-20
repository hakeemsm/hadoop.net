using Sharpen;

namespace org.apache.hadoop.fs.viewfs
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
		private const string MOUNT_TABLE_NAME = "vfs-cluster";

		internal static org.apache.hadoop.conf.Configuration conf;

		internal static org.apache.hadoop.fs.FileSystem viewFs;

		internal static org.apache.hadoop.fs.viewfs.TestViewFileSystemDelegationTokenSupport.FakeFileSystem
			 fs1;

		internal static org.apache.hadoop.fs.viewfs.TestViewFileSystemDelegationTokenSupport.FakeFileSystem
			 fs2;

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.BeforeClass]
		public static void setup()
		{
			conf = org.apache.hadoop.fs.viewfs.ViewFileSystemTestSetup.createConfig();
			fs1 = setupFileSystem(new java.net.URI("fs1:///"), Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.fs.viewfs.TestViewFileSystemDelegationTokenSupport.FakeFileSystem
				)));
			fs2 = setupFileSystem(new java.net.URI("fs2:///"), Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.fs.viewfs.TestViewFileSystemDelegationTokenSupport.FakeFileSystem
				)));
			viewFs = org.apache.hadoop.fs.FileSystem.get(org.apache.hadoop.fs.FsConstants.VIEWFS_URI
				, conf);
		}

		/// <exception cref="System.Exception"/>
		internal static org.apache.hadoop.fs.viewfs.TestViewFileSystemDelegationTokenSupport.FakeFileSystem
			 setupFileSystem(java.net.URI uri, java.lang.Class clazz)
		{
			string scheme = uri.getScheme();
			conf.set("fs." + scheme + ".impl", clazz.getName());
			org.apache.hadoop.fs.viewfs.TestViewFileSystemDelegationTokenSupport.FakeFileSystem
				 fs = (org.apache.hadoop.fs.viewfs.TestViewFileSystemDelegationTokenSupport.FakeFileSystem
				)org.apache.hadoop.fs.FileSystem.get(uri, conf);
			// mount each fs twice, will later ensure 1 token/fs
			org.apache.hadoop.fs.viewfs.ConfigUtil.addLink(conf, "/mounts/" + scheme + "-one"
				, fs.getUri());
			org.apache.hadoop.fs.viewfs.ConfigUtil.addLink(conf, "/mounts/" + scheme + "-two"
				, fs.getUri());
			return fs;
		}

		/// <summary>Regression test for HADOOP-8408.</summary>
		/// <exception cref="java.net.URISyntaxException"/>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testGetCanonicalServiceNameWithNonDefaultMountTable()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			org.apache.hadoop.fs.viewfs.ConfigUtil.addLink(conf, MOUNT_TABLE_NAME, "/user", new 
				java.net.URI("file:///"));
			org.apache.hadoop.fs.FileSystem viewFs = org.apache.hadoop.fs.FileSystem.get(new 
				java.net.URI(org.apache.hadoop.fs.FsConstants.VIEWFS_SCHEME + "://" + MOUNT_TABLE_NAME
				), conf);
			string serviceName = viewFs.getCanonicalServiceName();
			NUnit.Framework.Assert.IsNull(serviceName);
		}

		/// <exception cref="java.net.URISyntaxException"/>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testGetCanonicalServiceNameWithDefaultMountTable()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			org.apache.hadoop.fs.viewfs.ConfigUtil.addLink(conf, "/user", new java.net.URI("file:///"
				));
			org.apache.hadoop.fs.FileSystem viewFs = org.apache.hadoop.fs.FileSystem.get(org.apache.hadoop.fs.FsConstants
				.VIEWFS_URI, conf);
			string serviceName = viewFs.getCanonicalServiceName();
			NUnit.Framework.Assert.IsNull(serviceName);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testGetChildFileSystems()
		{
			NUnit.Framework.Assert.IsNull(fs1.getChildFileSystems());
			NUnit.Framework.Assert.IsNull(fs2.getChildFileSystems());
			System.Collections.Generic.IList<org.apache.hadoop.fs.FileSystem> children = java.util.Arrays
				.asList(viewFs.getChildFileSystems());
			NUnit.Framework.Assert.AreEqual(2, children.Count);
			NUnit.Framework.Assert.IsTrue(children.contains(fs1));
			NUnit.Framework.Assert.IsTrue(children.contains(fs2));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testAddDelegationTokens()
		{
			org.apache.hadoop.security.Credentials creds = new org.apache.hadoop.security.Credentials
				();
			org.apache.hadoop.security.token.Token<object>[] fs1Tokens = addTokensWithCreds(fs1
				, creds);
			NUnit.Framework.Assert.AreEqual(1, fs1Tokens.Length);
			NUnit.Framework.Assert.AreEqual(1, creds.numberOfTokens());
			org.apache.hadoop.security.token.Token<object>[] fs2Tokens = addTokensWithCreds(fs2
				, creds);
			NUnit.Framework.Assert.AreEqual(1, fs2Tokens.Length);
			NUnit.Framework.Assert.AreEqual(2, creds.numberOfTokens());
			org.apache.hadoop.security.Credentials savedCreds = creds;
			creds = new org.apache.hadoop.security.Credentials();
			// should get the same set of tokens as explicitly fetched above
			org.apache.hadoop.security.token.Token<object>[] viewFsTokens = viewFs.addDelegationTokens
				("me", creds);
			NUnit.Framework.Assert.AreEqual(2, viewFsTokens.Length);
			NUnit.Framework.Assert.IsTrue(creds.getAllTokens().containsAll(savedCreds.getAllTokens
				()));
			NUnit.Framework.Assert.AreEqual(savedCreds.numberOfTokens(), creds.numberOfTokens
				());
			// should get none, already have all tokens
			viewFsTokens = viewFs.addDelegationTokens("me", creds);
			NUnit.Framework.Assert.AreEqual(0, viewFsTokens.Length);
			NUnit.Framework.Assert.IsTrue(creds.getAllTokens().containsAll(savedCreds.getAllTokens
				()));
			NUnit.Framework.Assert.AreEqual(savedCreds.numberOfTokens(), creds.numberOfTokens
				());
		}

		/// <exception cref="System.Exception"/>
		internal virtual org.apache.hadoop.security.token.Token<object>[] addTokensWithCreds
			(org.apache.hadoop.fs.FileSystem fs, org.apache.hadoop.security.Credentials creds
			)
		{
			org.apache.hadoop.security.Credentials savedCreds;
			savedCreds = new org.apache.hadoop.security.Credentials(creds);
			org.apache.hadoop.security.token.Token<object>[] tokens = fs.addDelegationTokens(
				"me", creds);
			// test that we got the token we wanted, and that creds were modified
			NUnit.Framework.Assert.AreEqual(1, tokens.Length);
			NUnit.Framework.Assert.AreEqual(fs.getCanonicalServiceName(), tokens[0].getService
				().ToString());
			NUnit.Framework.Assert.IsTrue(creds.getAllTokens().contains(tokens[0]));
			NUnit.Framework.Assert.IsTrue(creds.getAllTokens().containsAll(savedCreds.getAllTokens
				()));
			NUnit.Framework.Assert.AreEqual(savedCreds.numberOfTokens() + 1, creds.numberOfTokens
				());
			// shouldn't get any new tokens since already in creds
			savedCreds = new org.apache.hadoop.security.Credentials(creds);
			org.apache.hadoop.security.token.Token<object>[] tokenRefetch = fs.addDelegationTokens
				("me", creds);
			NUnit.Framework.Assert.AreEqual(0, tokenRefetch.Length);
			NUnit.Framework.Assert.IsTrue(creds.getAllTokens().containsAll(savedCreds.getAllTokens
				()));
			NUnit.Framework.Assert.AreEqual(savedCreds.numberOfTokens(), creds.numberOfTokens
				());
			return tokens;
		}

		internal class FakeFileSystem : org.apache.hadoop.fs.RawLocalFileSystem
		{
			internal java.net.URI uri;

			/// <exception cref="System.IO.IOException"/>
			public override void initialize(java.net.URI name, org.apache.hadoop.conf.Configuration
				 conf)
			{
				this.uri = name;
			}

			protected internal override org.apache.hadoop.fs.Path getInitialWorkingDirectory(
				)
			{
				return new org.apache.hadoop.fs.Path("/");
			}

			// ctor calls getUri before the uri is inited...
			public override java.net.URI getUri()
			{
				return uri;
			}

			public override string getCanonicalServiceName()
			{
				return Sharpen.Runtime.getStringValueOf(this.getUri() + "/" + this.GetHashCode());
			}

			/// <exception cref="System.IO.IOException"/>
			public override org.apache.hadoop.security.token.Token<object> getDelegationToken
				(string renewer)
			{
				org.apache.hadoop.security.token.Token<object> token = new org.apache.hadoop.security.token.Token
					<org.apache.hadoop.security.token.TokenIdentifier>();
				token.setService(new org.apache.hadoop.io.Text(getCanonicalServiceName()));
				return token;
			}

			public override void close()
			{
			}
		}
	}
}
