using Sharpen;

namespace org.apache.hadoop.fs
{
	public class TestFileSystemCanonicalization
	{
		internal static string[] authorities = new string[] { "myfs://host", "myfs://host.a"
			, "myfs://host.a.b" };

		internal static string[] ips = new string[] { "myfs://127.0.0.1" };

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.BeforeClass]
		public static void initialize()
		{
			org.apache.hadoop.security.NetUtilsTestResolver.install();
		}

		// no ports
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testShortAuthority()
		{
			org.apache.hadoop.fs.FileSystem fs = getVerifiedFS("myfs://host", "myfs://host.a.b:123"
				);
			verifyPaths(fs, authorities, -1, true);
			verifyPaths(fs, authorities, 123, true);
			verifyPaths(fs, authorities, 456, false);
			verifyPaths(fs, ips, -1, false);
			verifyPaths(fs, ips, 123, false);
			verifyPaths(fs, ips, 456, false);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testPartialAuthority()
		{
			org.apache.hadoop.fs.FileSystem fs = getVerifiedFS("myfs://host.a", "myfs://host.a.b:123"
				);
			verifyPaths(fs, authorities, -1, true);
			verifyPaths(fs, authorities, 123, true);
			verifyPaths(fs, authorities, 456, false);
			verifyPaths(fs, ips, -1, false);
			verifyPaths(fs, ips, 123, false);
			verifyPaths(fs, ips, 456, false);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testFullAuthority()
		{
			org.apache.hadoop.fs.FileSystem fs = getVerifiedFS("myfs://host.a.b", "myfs://host.a.b:123"
				);
			verifyPaths(fs, authorities, -1, true);
			verifyPaths(fs, authorities, 123, true);
			verifyPaths(fs, authorities, 456, false);
			verifyPaths(fs, ips, -1, false);
			verifyPaths(fs, ips, 123, false);
			verifyPaths(fs, ips, 456, false);
		}

		// with default ports
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testShortAuthorityWithDefaultPort()
		{
			org.apache.hadoop.fs.FileSystem fs = getVerifiedFS("myfs://host:123", "myfs://host.a.b:123"
				);
			verifyPaths(fs, authorities, -1, true);
			verifyPaths(fs, authorities, 123, true);
			verifyPaths(fs, authorities, 456, false);
			verifyPaths(fs, ips, -1, false);
			verifyPaths(fs, ips, 123, false);
			verifyPaths(fs, ips, 456, false);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testPartialAuthorityWithDefaultPort()
		{
			org.apache.hadoop.fs.FileSystem fs = getVerifiedFS("myfs://host.a:123", "myfs://host.a.b:123"
				);
			verifyPaths(fs, authorities, -1, true);
			verifyPaths(fs, authorities, 123, true);
			verifyPaths(fs, authorities, 456, false);
			verifyPaths(fs, ips, -1, false);
			verifyPaths(fs, ips, 123, false);
			verifyPaths(fs, ips, 456, false);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testFullAuthorityWithDefaultPort()
		{
			org.apache.hadoop.fs.FileSystem fs = getVerifiedFS("myfs://host.a.b:123", "myfs://host.a.b:123"
				);
			verifyPaths(fs, authorities, -1, true);
			verifyPaths(fs, authorities, 123, true);
			verifyPaths(fs, authorities, 456, false);
			verifyPaths(fs, ips, -1, false);
			verifyPaths(fs, ips, 123, false);
			verifyPaths(fs, ips, 456, false);
		}

		// with non-standard ports
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testShortAuthorityWithOtherPort()
		{
			org.apache.hadoop.fs.FileSystem fs = getVerifiedFS("myfs://host:456", "myfs://host.a.b:456"
				);
			verifyPaths(fs, authorities, -1, false);
			verifyPaths(fs, authorities, 123, false);
			verifyPaths(fs, authorities, 456, true);
			verifyPaths(fs, ips, -1, false);
			verifyPaths(fs, ips, 123, false);
			verifyPaths(fs, ips, 456, false);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testPartialAuthorityWithOtherPort()
		{
			org.apache.hadoop.fs.FileSystem fs = getVerifiedFS("myfs://host.a:456", "myfs://host.a.b:456"
				);
			verifyPaths(fs, authorities, -1, false);
			verifyPaths(fs, authorities, 123, false);
			verifyPaths(fs, authorities, 456, true);
			verifyPaths(fs, ips, -1, false);
			verifyPaths(fs, ips, 123, false);
			verifyPaths(fs, ips, 456, false);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testFullAuthorityWithOtherPort()
		{
			org.apache.hadoop.fs.FileSystem fs = getVerifiedFS("myfs://host.a.b:456", "myfs://host.a.b:456"
				);
			verifyPaths(fs, authorities, -1, false);
			verifyPaths(fs, authorities, 123, false);
			verifyPaths(fs, authorities, 456, true);
			verifyPaths(fs, ips, -1, false);
			verifyPaths(fs, ips, 123, false);
			verifyPaths(fs, ips, 456, false);
		}

		// ips
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testIpAuthority()
		{
			org.apache.hadoop.fs.FileSystem fs = getVerifiedFS("myfs://127.0.0.1", "myfs://127.0.0.1:123"
				);
			verifyPaths(fs, authorities, -1, false);
			verifyPaths(fs, authorities, 123, false);
			verifyPaths(fs, authorities, 456, false);
			verifyPaths(fs, ips, -1, true);
			verifyPaths(fs, ips, 123, true);
			verifyPaths(fs, ips, 456, false);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testIpAuthorityWithDefaultPort()
		{
			org.apache.hadoop.fs.FileSystem fs = getVerifiedFS("myfs://127.0.0.1:123", "myfs://127.0.0.1:123"
				);
			verifyPaths(fs, authorities, -1, false);
			verifyPaths(fs, authorities, 123, false);
			verifyPaths(fs, authorities, 456, false);
			verifyPaths(fs, ips, -1, true);
			verifyPaths(fs, ips, 123, true);
			verifyPaths(fs, ips, 456, false);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testIpAuthorityWithOtherPort()
		{
			org.apache.hadoop.fs.FileSystem fs = getVerifiedFS("myfs://127.0.0.1:456", "myfs://127.0.0.1:456"
				);
			verifyPaths(fs, authorities, -1, false);
			verifyPaths(fs, authorities, 123, false);
			verifyPaths(fs, authorities, 456, false);
			verifyPaths(fs, ips, -1, false);
			verifyPaths(fs, ips, 123, false);
			verifyPaths(fs, ips, 456, true);
		}

		// bad stuff
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testMismatchedSchemes()
		{
			org.apache.hadoop.fs.FileSystem fs = getVerifiedFS("myfs2://simple", "myfs2://simple:123"
				);
			verifyPaths(fs, authorities, -1, false);
			verifyPaths(fs, authorities, 123, false);
			verifyPaths(fs, authorities, 456, false);
			verifyPaths(fs, ips, -1, false);
			verifyPaths(fs, ips, 123, false);
			verifyPaths(fs, ips, 456, false);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testMismatchedHosts()
		{
			org.apache.hadoop.fs.FileSystem fs = getVerifiedFS("myfs://simple", "myfs://simple:123"
				);
			verifyPaths(fs, authorities, -1, false);
			verifyPaths(fs, authorities, 123, false);
			verifyPaths(fs, authorities, 456, false);
			verifyPaths(fs, ips, -1, false);
			verifyPaths(fs, ips, 123, false);
			verifyPaths(fs, ips, 456, false);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testNullAuthority()
		{
			org.apache.hadoop.fs.FileSystem fs = getVerifiedFS("myfs:///", "myfs:///");
			verifyPaths(fs, new string[] { "myfs://" }, -1, true);
			verifyPaths(fs, authorities, -1, false);
			verifyPaths(fs, authorities, 123, false);
			verifyPaths(fs, authorities, 456, false);
			verifyPaths(fs, ips, -1, false);
			verifyPaths(fs, ips, 123, false);
			verifyPaths(fs, ips, 456, false);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testAuthorityFromDefaultFS()
		{
			org.apache.hadoop.conf.Configuration config = new org.apache.hadoop.conf.Configuration
				();
			string defaultFsKey = org.apache.hadoop.fs.CommonConfigurationKeys.FS_DEFAULT_NAME_KEY;
			org.apache.hadoop.fs.FileSystem fs = getVerifiedFS("myfs://host", "myfs://host.a.b:123"
				, config);
			verifyPaths(fs, new string[] { "myfs://" }, -1, false);
			config.set(defaultFsKey, "myfs://host");
			verifyPaths(fs, new string[] { "myfs://" }, -1, true);
			config.set(defaultFsKey, "myfs2://host");
			verifyPaths(fs, new string[] { "myfs://" }, -1, false);
			config.set(defaultFsKey, "myfs://host:123");
			verifyPaths(fs, new string[] { "myfs://" }, -1, true);
			config.set(defaultFsKey, "myfs://host:456");
			verifyPaths(fs, new string[] { "myfs://" }, -1, false);
		}

		/// <exception cref="System.Exception"/>
		internal virtual org.apache.hadoop.fs.FileSystem getVerifiedFS(string authority, 
			string canonical)
		{
			return getVerifiedFS(authority, canonical, new org.apache.hadoop.conf.Configuration
				());
		}

		// create a fs from the authority, then check its uri against the given uri
		// and the canonical.  then try to fetch paths using the canonical
		/// <exception cref="System.Exception"/>
		internal virtual org.apache.hadoop.fs.FileSystem getVerifiedFS(string authority, 
			string canonical, org.apache.hadoop.conf.Configuration conf)
		{
			java.net.URI uri = java.net.URI.create(authority);
			java.net.URI canonicalUri = java.net.URI.create(canonical);
			org.apache.hadoop.fs.FileSystem fs = new org.apache.hadoop.fs.TestFileSystemCanonicalization.DummyFileSystem
				(uri, conf);
			NUnit.Framework.Assert.AreEqual(uri, fs.getUri());
			NUnit.Framework.Assert.AreEqual(canonicalUri, fs.getCanonicalUri());
			verifyCheckPath(fs, "/file", true);
			return fs;
		}

		internal virtual void verifyPaths(org.apache.hadoop.fs.FileSystem fs, string[] uris
			, int port, bool shouldPass)
		{
			foreach (string uri in uris)
			{
				if (port != -1)
				{
					uri += ":" + port;
				}
				verifyCheckPath(fs, uri + "/file", shouldPass);
			}
		}

		internal virtual void verifyCheckPath(org.apache.hadoop.fs.FileSystem fs, string 
			path, bool shouldPass)
		{
			org.apache.hadoop.fs.Path rawPath = new org.apache.hadoop.fs.Path(path);
			org.apache.hadoop.fs.Path fqPath = null;
			System.Exception e = null;
			try
			{
				fqPath = fs.makeQualified(rawPath);
			}
			catch (System.ArgumentException iae)
			{
				e = iae;
			}
			if (shouldPass)
			{
				NUnit.Framework.Assert.AreEqual(null, e);
				string pathAuthority = rawPath.toUri().getAuthority();
				if (pathAuthority == null)
				{
					pathAuthority = fs.getUri().getAuthority();
				}
				NUnit.Framework.Assert.AreEqual(pathAuthority, fqPath.toUri().getAuthority());
			}
			else
			{
				NUnit.Framework.Assert.IsNotNull("did not fail", e);
				NUnit.Framework.Assert.AreEqual("Wrong FS: " + rawPath + ", expected: " + fs.getUri
					(), e.Message);
			}
		}

		internal class DummyFileSystem : org.apache.hadoop.fs.FileSystem
		{
			internal java.net.URI uri;

			internal static int defaultPort = 123;

			/// <exception cref="System.IO.IOException"/>
			internal DummyFileSystem(java.net.URI uri, org.apache.hadoop.conf.Configuration conf
				)
			{
				this.uri = uri;
				setConf(conf);
			}

			public override java.net.URI getUri()
			{
				return uri;
			}

			protected internal override int getDefaultPort()
			{
				return defaultPort;
			}

			protected internal override java.net.URI canonicalizeUri(java.net.URI uri)
			{
				return org.apache.hadoop.net.NetUtils.getCanonicalUri(uri, getDefaultPort());
			}

			/// <exception cref="System.IO.IOException"/>
			public override org.apache.hadoop.fs.FSDataInputStream open(org.apache.hadoop.fs.Path
				 f, int bufferSize)
			{
				throw new System.IO.IOException("not supposed to be here");
			}

			/// <exception cref="System.IO.IOException"/>
			public override org.apache.hadoop.fs.FSDataOutputStream create(org.apache.hadoop.fs.Path
				 f, org.apache.hadoop.fs.permission.FsPermission permission, bool overwrite, int
				 bufferSize, short replication, long blockSize, org.apache.hadoop.util.Progressable
				 progress)
			{
				throw new System.IO.IOException("not supposed to be here");
			}

			/// <exception cref="System.IO.IOException"/>
			public override org.apache.hadoop.fs.FSDataOutputStream append(org.apache.hadoop.fs.Path
				 f, int bufferSize, org.apache.hadoop.util.Progressable progress)
			{
				throw new System.IO.IOException("not supposed to be here");
			}

			/// <exception cref="System.IO.IOException"/>
			public override bool rename(org.apache.hadoop.fs.Path src, org.apache.hadoop.fs.Path
				 dst)
			{
				throw new System.IO.IOException("not supposed to be here");
			}

			/// <exception cref="System.IO.IOException"/>
			public override bool delete(org.apache.hadoop.fs.Path f, bool recursive)
			{
				throw new System.IO.IOException("not supposed to be here");
			}

			/// <exception cref="System.IO.IOException"/>
			public override org.apache.hadoop.fs.FileStatus[] listStatus(org.apache.hadoop.fs.Path
				 f)
			{
				throw new System.IO.IOException("not supposed to be here");
			}

			public override void setWorkingDirectory(org.apache.hadoop.fs.Path new_dir)
			{
			}

			public override org.apache.hadoop.fs.Path getWorkingDirectory()
			{
				return new org.apache.hadoop.fs.Path("/");
			}

			/// <exception cref="System.IO.IOException"/>
			public override bool mkdirs(org.apache.hadoop.fs.Path f, org.apache.hadoop.fs.permission.FsPermission
				 permission)
			{
				throw new System.IO.IOException("not supposed to be here");
			}

			/// <exception cref="System.IO.IOException"/>
			public override org.apache.hadoop.fs.FileStatus getFileStatus(org.apache.hadoop.fs.Path
				 f)
			{
				throw new System.IO.IOException("not supposed to be here");
			}
		}
	}
}
