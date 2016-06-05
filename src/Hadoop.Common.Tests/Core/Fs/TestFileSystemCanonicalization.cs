using System;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Util;


namespace Org.Apache.Hadoop.FS
{
	public class TestFileSystemCanonicalization
	{
		internal static string[] authorities = new string[] { "myfs://host", "myfs://host.a"
			, "myfs://host.a.b" };

		internal static string[] ips = new string[] { "myfs://127.0.0.1" };

		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void Initialize()
		{
			NetUtilsTestResolver.Install();
		}

		// no ports
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestShortAuthority()
		{
			FileSystem fs = GetVerifiedFS("myfs://host", "myfs://host.a.b:123");
			VerifyPaths(fs, authorities, -1, true);
			VerifyPaths(fs, authorities, 123, true);
			VerifyPaths(fs, authorities, 456, false);
			VerifyPaths(fs, ips, -1, false);
			VerifyPaths(fs, ips, 123, false);
			VerifyPaths(fs, ips, 456, false);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestPartialAuthority()
		{
			FileSystem fs = GetVerifiedFS("myfs://host.a", "myfs://host.a.b:123");
			VerifyPaths(fs, authorities, -1, true);
			VerifyPaths(fs, authorities, 123, true);
			VerifyPaths(fs, authorities, 456, false);
			VerifyPaths(fs, ips, -1, false);
			VerifyPaths(fs, ips, 123, false);
			VerifyPaths(fs, ips, 456, false);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestFullAuthority()
		{
			FileSystem fs = GetVerifiedFS("myfs://host.a.b", "myfs://host.a.b:123");
			VerifyPaths(fs, authorities, -1, true);
			VerifyPaths(fs, authorities, 123, true);
			VerifyPaths(fs, authorities, 456, false);
			VerifyPaths(fs, ips, -1, false);
			VerifyPaths(fs, ips, 123, false);
			VerifyPaths(fs, ips, 456, false);
		}

		// with default ports
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestShortAuthorityWithDefaultPort()
		{
			FileSystem fs = GetVerifiedFS("myfs://host:123", "myfs://host.a.b:123");
			VerifyPaths(fs, authorities, -1, true);
			VerifyPaths(fs, authorities, 123, true);
			VerifyPaths(fs, authorities, 456, false);
			VerifyPaths(fs, ips, -1, false);
			VerifyPaths(fs, ips, 123, false);
			VerifyPaths(fs, ips, 456, false);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestPartialAuthorityWithDefaultPort()
		{
			FileSystem fs = GetVerifiedFS("myfs://host.a:123", "myfs://host.a.b:123");
			VerifyPaths(fs, authorities, -1, true);
			VerifyPaths(fs, authorities, 123, true);
			VerifyPaths(fs, authorities, 456, false);
			VerifyPaths(fs, ips, -1, false);
			VerifyPaths(fs, ips, 123, false);
			VerifyPaths(fs, ips, 456, false);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestFullAuthorityWithDefaultPort()
		{
			FileSystem fs = GetVerifiedFS("myfs://host.a.b:123", "myfs://host.a.b:123");
			VerifyPaths(fs, authorities, -1, true);
			VerifyPaths(fs, authorities, 123, true);
			VerifyPaths(fs, authorities, 456, false);
			VerifyPaths(fs, ips, -1, false);
			VerifyPaths(fs, ips, 123, false);
			VerifyPaths(fs, ips, 456, false);
		}

		// with non-standard ports
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestShortAuthorityWithOtherPort()
		{
			FileSystem fs = GetVerifiedFS("myfs://host:456", "myfs://host.a.b:456");
			VerifyPaths(fs, authorities, -1, false);
			VerifyPaths(fs, authorities, 123, false);
			VerifyPaths(fs, authorities, 456, true);
			VerifyPaths(fs, ips, -1, false);
			VerifyPaths(fs, ips, 123, false);
			VerifyPaths(fs, ips, 456, false);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestPartialAuthorityWithOtherPort()
		{
			FileSystem fs = GetVerifiedFS("myfs://host.a:456", "myfs://host.a.b:456");
			VerifyPaths(fs, authorities, -1, false);
			VerifyPaths(fs, authorities, 123, false);
			VerifyPaths(fs, authorities, 456, true);
			VerifyPaths(fs, ips, -1, false);
			VerifyPaths(fs, ips, 123, false);
			VerifyPaths(fs, ips, 456, false);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestFullAuthorityWithOtherPort()
		{
			FileSystem fs = GetVerifiedFS("myfs://host.a.b:456", "myfs://host.a.b:456");
			VerifyPaths(fs, authorities, -1, false);
			VerifyPaths(fs, authorities, 123, false);
			VerifyPaths(fs, authorities, 456, true);
			VerifyPaths(fs, ips, -1, false);
			VerifyPaths(fs, ips, 123, false);
			VerifyPaths(fs, ips, 456, false);
		}

		// ips
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestIpAuthority()
		{
			FileSystem fs = GetVerifiedFS("myfs://127.0.0.1", "myfs://127.0.0.1:123");
			VerifyPaths(fs, authorities, -1, false);
			VerifyPaths(fs, authorities, 123, false);
			VerifyPaths(fs, authorities, 456, false);
			VerifyPaths(fs, ips, -1, true);
			VerifyPaths(fs, ips, 123, true);
			VerifyPaths(fs, ips, 456, false);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestIpAuthorityWithDefaultPort()
		{
			FileSystem fs = GetVerifiedFS("myfs://127.0.0.1:123", "myfs://127.0.0.1:123");
			VerifyPaths(fs, authorities, -1, false);
			VerifyPaths(fs, authorities, 123, false);
			VerifyPaths(fs, authorities, 456, false);
			VerifyPaths(fs, ips, -1, true);
			VerifyPaths(fs, ips, 123, true);
			VerifyPaths(fs, ips, 456, false);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestIpAuthorityWithOtherPort()
		{
			FileSystem fs = GetVerifiedFS("myfs://127.0.0.1:456", "myfs://127.0.0.1:456");
			VerifyPaths(fs, authorities, -1, false);
			VerifyPaths(fs, authorities, 123, false);
			VerifyPaths(fs, authorities, 456, false);
			VerifyPaths(fs, ips, -1, false);
			VerifyPaths(fs, ips, 123, false);
			VerifyPaths(fs, ips, 456, true);
		}

		// bad stuff
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestMismatchedSchemes()
		{
			FileSystem fs = GetVerifiedFS("myfs2://simple", "myfs2://simple:123");
			VerifyPaths(fs, authorities, -1, false);
			VerifyPaths(fs, authorities, 123, false);
			VerifyPaths(fs, authorities, 456, false);
			VerifyPaths(fs, ips, -1, false);
			VerifyPaths(fs, ips, 123, false);
			VerifyPaths(fs, ips, 456, false);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestMismatchedHosts()
		{
			FileSystem fs = GetVerifiedFS("myfs://simple", "myfs://simple:123");
			VerifyPaths(fs, authorities, -1, false);
			VerifyPaths(fs, authorities, 123, false);
			VerifyPaths(fs, authorities, 456, false);
			VerifyPaths(fs, ips, -1, false);
			VerifyPaths(fs, ips, 123, false);
			VerifyPaths(fs, ips, 456, false);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestNullAuthority()
		{
			FileSystem fs = GetVerifiedFS("myfs:///", "myfs:///");
			VerifyPaths(fs, new string[] { "myfs://" }, -1, true);
			VerifyPaths(fs, authorities, -1, false);
			VerifyPaths(fs, authorities, 123, false);
			VerifyPaths(fs, authorities, 456, false);
			VerifyPaths(fs, ips, -1, false);
			VerifyPaths(fs, ips, 123, false);
			VerifyPaths(fs, ips, 456, false);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestAuthorityFromDefaultFS()
		{
			Configuration config = new Configuration();
			string defaultFsKey = CommonConfigurationKeys.FsDefaultNameKey;
			FileSystem fs = GetVerifiedFS("myfs://host", "myfs://host.a.b:123", config);
			VerifyPaths(fs, new string[] { "myfs://" }, -1, false);
			config.Set(defaultFsKey, "myfs://host");
			VerifyPaths(fs, new string[] { "myfs://" }, -1, true);
			config.Set(defaultFsKey, "myfs2://host");
			VerifyPaths(fs, new string[] { "myfs://" }, -1, false);
			config.Set(defaultFsKey, "myfs://host:123");
			VerifyPaths(fs, new string[] { "myfs://" }, -1, true);
			config.Set(defaultFsKey, "myfs://host:456");
			VerifyPaths(fs, new string[] { "myfs://" }, -1, false);
		}

		/// <exception cref="System.Exception"/>
		internal virtual FileSystem GetVerifiedFS(string authority, string canonical)
		{
			return GetVerifiedFS(authority, canonical, new Configuration());
		}

		// create a fs from the authority, then check its uri against the given uri
		// and the canonical.  then try to fetch paths using the canonical
		/// <exception cref="System.Exception"/>
		internal virtual FileSystem GetVerifiedFS(string authority, string canonical, Configuration
			 conf)
		{
			URI uri = URI.Create(authority);
			URI canonicalUri = URI.Create(canonical);
			FileSystem fs = new TestFileSystemCanonicalization.DummyFileSystem(uri, conf);
			Assert.Equal(uri, fs.GetUri());
			Assert.Equal(canonicalUri, fs.GetCanonicalUri());
			VerifyCheckPath(fs, "/file", true);
			return fs;
		}

		internal virtual void VerifyPaths(FileSystem fs, string[] uris, int port, bool shouldPass
			)
		{
			foreach (string uri in uris)
			{
				if (port != -1)
				{
					uri += ":" + port;
				}
				VerifyCheckPath(fs, uri + "/file", shouldPass);
			}
		}

		internal virtual void VerifyCheckPath(FileSystem fs, string path, bool shouldPass
			)
		{
			Path rawPath = new Path(path);
			Path fqPath = null;
			Exception e = null;
			try
			{
				fqPath = fs.MakeQualified(rawPath);
			}
			catch (ArgumentException iae)
			{
				e = iae;
			}
			if (shouldPass)
			{
				Assert.Equal(null, e);
				string pathAuthority = rawPath.ToUri().GetAuthority();
				if (pathAuthority == null)
				{
					pathAuthority = fs.GetUri().GetAuthority();
				}
				Assert.Equal(pathAuthority, fqPath.ToUri().GetAuthority());
			}
			else
			{
				NUnit.Framework.Assert.IsNotNull("did not fail", e);
				Assert.Equal("Wrong FS: " + rawPath + ", expected: " + fs.GetUri
					(), e.Message);
			}
		}

		internal class DummyFileSystem : FileSystem
		{
			internal URI uri;

			internal static int defaultPort = 123;

			/// <exception cref="System.IO.IOException"/>
			internal DummyFileSystem(URI uri, Configuration conf)
			{
				this.uri = uri;
				SetConf(conf);
			}

			public override URI GetUri()
			{
				return uri;
			}

			protected internal override int GetDefaultPort()
			{
				return defaultPort;
			}

			protected internal override URI CanonicalizeUri(URI uri)
			{
				return NetUtils.GetCanonicalUri(uri, GetDefaultPort());
			}

			/// <exception cref="System.IO.IOException"/>
			public override FSDataInputStream Open(Path f, int bufferSize)
			{
				throw new IOException("not supposed to be here");
			}

			/// <exception cref="System.IO.IOException"/>
			public override FSDataOutputStream Create(Path f, FsPermission permission, bool overwrite
				, int bufferSize, short replication, long blockSize, Progressable progress)
			{
				throw new IOException("not supposed to be here");
			}

			/// <exception cref="System.IO.IOException"/>
			public override FSDataOutputStream Append(Path f, int bufferSize, Progressable progress
				)
			{
				throw new IOException("not supposed to be here");
			}

			/// <exception cref="System.IO.IOException"/>
			public override bool Rename(Path src, Path dst)
			{
				throw new IOException("not supposed to be here");
			}

			/// <exception cref="System.IO.IOException"/>
			public override bool Delete(Path f, bool recursive)
			{
				throw new IOException("not supposed to be here");
			}

			/// <exception cref="System.IO.IOException"/>
			public override FileStatus[] ListStatus(Path f)
			{
				throw new IOException("not supposed to be here");
			}

			public override void SetWorkingDirectory(Path new_dir)
			{
			}

			public override Path GetWorkingDirectory()
			{
				return new Path("/");
			}

			/// <exception cref="System.IO.IOException"/>
			public override bool Mkdirs(Path f, FsPermission permission)
			{
				throw new IOException("not supposed to be here");
			}

			/// <exception cref="System.IO.IOException"/>
			public override FileStatus GetFileStatus(Path f)
			{
				throw new IOException("not supposed to be here");
			}
		}
	}
}
