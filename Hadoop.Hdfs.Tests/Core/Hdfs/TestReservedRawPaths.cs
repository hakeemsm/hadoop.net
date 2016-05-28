using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Crypto.Key;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs.Client;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Test;
using Org.Apache.Log4j;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	public class TestReservedRawPaths
	{
		private Configuration conf;

		private FileSystemTestHelper fsHelper;

		private MiniDFSCluster cluster;

		private HdfsAdmin dfsAdmin;

		private DistributedFileSystem fs;

		private readonly string TestKey = "test_key";

		protected internal FileSystemTestWrapper fsWrapper;

		protected internal FileContextTestWrapper fcWrapper;

		/// <exception cref="System.Exception"/>
		[SetUp]
		public virtual void Setup()
		{
			conf = new HdfsConfiguration();
			fsHelper = new FileSystemTestHelper();
			// Set up java key store
			string testRoot = fsHelper.GetTestRootDir();
			FilePath testRootDir = new FilePath(testRoot).GetAbsoluteFile();
			Path jksPath = new Path(testRootDir.ToString(), "test.jks");
			conf.Set(DFSConfigKeys.DfsEncryptionKeyProviderUri, JavaKeyStoreProvider.SchemeName
				 + "://file" + jksPath.ToUri());
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
			Logger.GetLogger(typeof(EncryptionZoneManager)).SetLevel(Level.Trace);
			fs = cluster.GetFileSystem();
			fsWrapper = new FileSystemTestWrapper(cluster.GetFileSystem());
			fcWrapper = new FileContextTestWrapper(FileContext.GetFileContext(cluster.GetURI(
				), conf));
			dfsAdmin = new HdfsAdmin(cluster.GetURI(), conf);
			// Need to set the client's KeyProvider to the NN's for JKS,
			// else the updates do not get flushed properly
			fs.GetClient().SetKeyProvider(cluster.GetNameNode().GetNamesystem().GetProvider()
				);
			DFSTestUtil.CreateKey(TestKey, cluster, conf);
		}

		[TearDown]
		public virtual void Teardown()
		{
			if (cluster != null)
			{
				cluster.Shutdown();
			}
		}

		/// <summary>Basic read/write tests of raw files.</summary>
		/// <remarks>
		/// Basic read/write tests of raw files.
		/// Create a non-encrypted file
		/// Create an encryption zone
		/// Verify that non-encrypted file contents and decrypted file in EZ are equal
		/// Compare the raw encrypted bytes of the file with the decrypted version to
		/// ensure they're different
		/// Compare the raw and non-raw versions of the non-encrypted file to ensure
		/// they're the same.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestReadWriteRaw()
		{
			// Create a base file for comparison
			Path baseFile = new Path("/base");
			int len = 8192;
			DFSTestUtil.CreateFile(fs, baseFile, len, (short)1, unchecked((int)(0xFEED)));
			// Create the first enc file
			Path zone = new Path("/zone");
			fs.Mkdirs(zone);
			dfsAdmin.CreateEncryptionZone(zone, TestKey);
			Path encFile1 = new Path(zone, "myfile");
			DFSTestUtil.CreateFile(fs, encFile1, len, (short)1, unchecked((int)(0xFEED)));
			// Read them back in and compare byte-by-byte
			DFSTestUtil.VerifyFilesEqual(fs, baseFile, encFile1, len);
			// Raw file should be different from encrypted file
			Path encFile1Raw = new Path(zone, "/.reserved/raw/zone/myfile");
			DFSTestUtil.VerifyFilesNotEqual(fs, encFile1Raw, encFile1, len);
			// Raw file should be same as /base which is not in an EZ
			Path baseFileRaw = new Path(zone, "/.reserved/raw/base");
			DFSTestUtil.VerifyFilesEqual(fs, baseFile, baseFileRaw, len);
		}

		/// <exception cref="System.IO.IOException"/>
		private void AssertPathEquals(Path p1, Path p2)
		{
			FileStatus p1Stat = fs.GetFileStatus(p1);
			FileStatus p2Stat = fs.GetFileStatus(p2);
			/*
			* Use accessTime and modificationTime as substitutes for INode to check
			* for resolution to the same underlying file.
			*/
			NUnit.Framework.Assert.AreEqual("Access times not equal", p1Stat.GetAccessTime(), 
				p2Stat.GetAccessTime());
			NUnit.Framework.Assert.AreEqual("Modification times not equal", p1Stat.GetModificationTime
				(), p2Stat.GetModificationTime());
			NUnit.Framework.Assert.AreEqual("pathname1 not equal", p1, Path.GetPathWithoutSchemeAndAuthority
				(p1Stat.GetPath()));
			NUnit.Framework.Assert.AreEqual("pathname1 not equal", p2, Path.GetPathWithoutSchemeAndAuthority
				(p2Stat.GetPath()));
		}

		/// <summary>
		/// Tests that getFileStatus on raw and non raw resolve to the same
		/// file.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestGetFileStatus()
		{
			Path zone = new Path("zone");
			Path slashZone = new Path("/", zone);
			fs.Mkdirs(slashZone);
			dfsAdmin.CreateEncryptionZone(slashZone, TestKey);
			Path @base = new Path("base");
			Path reservedRaw = new Path("/.reserved/raw");
			Path baseRaw = new Path(reservedRaw, @base);
			int len = 8192;
			DFSTestUtil.CreateFile(fs, baseRaw, len, (short)1, unchecked((int)(0xFEED)));
			AssertPathEquals(new Path("/", @base), baseRaw);
			/* Repeat the test for a file in an ez. */
			Path ezEncFile = new Path(slashZone, @base);
			Path ezRawEncFile = new Path(new Path(reservedRaw, zone), @base);
			DFSTestUtil.CreateFile(fs, ezEncFile, len, (short)1, unchecked((int)(0xFEED)));
			AssertPathEquals(ezEncFile, ezRawEncFile);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestReservedRoot()
		{
			Path root = new Path("/");
			Path rawRoot = new Path("/.reserved/raw");
			Path rawRootSlash = new Path("/.reserved/raw/");
			AssertPathEquals(root, rawRoot);
			AssertPathEquals(root, rawRootSlash);
		}

		/* Verify mkdir works ok in .reserved/raw directory. */
		/// <exception cref="System.Exception"/>
		public virtual void TestReservedRawMkdir()
		{
			Path zone = new Path("zone");
			Path slashZone = new Path("/", zone);
			fs.Mkdirs(slashZone);
			dfsAdmin.CreateEncryptionZone(slashZone, TestKey);
			Path rawRoot = new Path("/.reserved/raw");
			Path dir1 = new Path("dir1");
			Path rawDir1 = new Path(rawRoot, dir1);
			fs.Mkdirs(rawDir1);
			AssertPathEquals(rawDir1, new Path("/", dir1));
			fs.Delete(rawDir1, true);
			Path rawZone = new Path(rawRoot, zone);
			Path rawDir1EZ = new Path(rawZone, dir1);
			fs.Mkdirs(rawDir1EZ);
			AssertPathEquals(rawDir1EZ, new Path(slashZone, dir1));
			fs.Delete(rawDir1EZ, true);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRelativePathnames()
		{
			Path baseFileRaw = new Path("/.reserved/raw/base");
			int len = 8192;
			DFSTestUtil.CreateFile(fs, baseFileRaw, len, (short)1, unchecked((int)(0xFEED)));
			Path root = new Path("/");
			Path rawRoot = new Path("/.reserved/raw");
			AssertPathEquals(root, new Path(rawRoot, "../raw"));
			AssertPathEquals(root, new Path(rawRoot, "../../.reserved/raw"));
			AssertPathEquals(baseFileRaw, new Path(rawRoot, "../raw/base"));
			AssertPathEquals(baseFileRaw, new Path(rawRoot, "../../.reserved/raw/base"));
			AssertPathEquals(baseFileRaw, new Path(rawRoot, "../../.reserved/raw/base/../base"
				));
			AssertPathEquals(baseFileRaw, new Path("/.reserved/../.reserved/raw/../raw/base")
				);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestAdminAccessOnly()
		{
			Path zone = new Path("zone");
			Path slashZone = new Path("/", zone);
			fs.Mkdirs(slashZone);
			dfsAdmin.CreateEncryptionZone(slashZone, TestKey);
			Path @base = new Path("base");
			Path reservedRaw = new Path("/.reserved/raw");
			int len = 8192;
			/* Test failure of create file in reserved/raw as non admin */
			UserGroupInformation user = UserGroupInformation.CreateUserForTesting("user", new 
				string[] { "mygroup" });
			user.DoAs(new _PrivilegedExceptionAction_233(this, reservedRaw, zone, @base, len)
				);
			/* Test failure of getFileStatus in reserved/raw as non admin */
			Path ezRawEncFile = new Path(new Path(reservedRaw, zone), @base);
			DFSTestUtil.CreateFile(fs, ezRawEncFile, len, (short)1, unchecked((int)(0xFEED)));
			user.DoAs(new _PrivilegedExceptionAction_252(this, ezRawEncFile));
			/* Test failure of listStatus in reserved/raw as non admin */
			user.DoAs(new _PrivilegedExceptionAction_267(this, ezRawEncFile));
			fs.SetPermission(new Path("/"), new FsPermission((short)0x1ff));
			/* Test failure of mkdir in reserved/raw as non admin */
			user.DoAs(new _PrivilegedExceptionAction_283(this, reservedRaw));
		}

		private sealed class _PrivilegedExceptionAction_233 : PrivilegedExceptionAction<object
			>
		{
			public _PrivilegedExceptionAction_233(TestReservedRawPaths _enclosing, Path reservedRaw
				, Path zone, Path @base, int len)
			{
				this._enclosing = _enclosing;
				this.reservedRaw = reservedRaw;
				this.zone = zone;
				this.@base = @base;
				this.len = len;
			}

			/// <exception cref="System.Exception"/>
			public object Run()
			{
				DistributedFileSystem fs = this._enclosing.cluster.GetFileSystem();
				try
				{
					Path ezRawEncFile = new Path(new Path(reservedRaw, zone), @base);
					DFSTestUtil.CreateFile(fs, ezRawEncFile, len, (short)1, unchecked((int)(0xFEED)));
					NUnit.Framework.Assert.Fail("access to /.reserved/raw is superuser-only operation"
						);
				}
				catch (AccessControlException e)
				{
					GenericTestUtils.AssertExceptionContains("Superuser privilege is required", e);
				}
				return null;
			}

			private readonly TestReservedRawPaths _enclosing;

			private readonly Path reservedRaw;

			private readonly Path zone;

			private readonly Path @base;

			private readonly int len;
		}

		private sealed class _PrivilegedExceptionAction_252 : PrivilegedExceptionAction<object
			>
		{
			public _PrivilegedExceptionAction_252(TestReservedRawPaths _enclosing, Path ezRawEncFile
				)
			{
				this._enclosing = _enclosing;
				this.ezRawEncFile = ezRawEncFile;
			}

			/// <exception cref="System.Exception"/>
			public object Run()
			{
				DistributedFileSystem fs = this._enclosing.cluster.GetFileSystem();
				try
				{
					fs.GetFileStatus(ezRawEncFile);
					NUnit.Framework.Assert.Fail("access to /.reserved/raw is superuser-only operation"
						);
				}
				catch (AccessControlException e)
				{
					GenericTestUtils.AssertExceptionContains("Superuser privilege is required", e);
				}
				return null;
			}

			private readonly TestReservedRawPaths _enclosing;

			private readonly Path ezRawEncFile;
		}

		private sealed class _PrivilegedExceptionAction_267 : PrivilegedExceptionAction<object
			>
		{
			public _PrivilegedExceptionAction_267(TestReservedRawPaths _enclosing, Path ezRawEncFile
				)
			{
				this._enclosing = _enclosing;
				this.ezRawEncFile = ezRawEncFile;
			}

			/// <exception cref="System.Exception"/>
			public object Run()
			{
				DistributedFileSystem fs = this._enclosing.cluster.GetFileSystem();
				try
				{
					fs.ListStatus(ezRawEncFile);
					NUnit.Framework.Assert.Fail("access to /.reserved/raw is superuser-only operation"
						);
				}
				catch (AccessControlException e)
				{
					GenericTestUtils.AssertExceptionContains("Superuser privilege is required", e);
				}
				return null;
			}

			private readonly TestReservedRawPaths _enclosing;

			private readonly Path ezRawEncFile;
		}

		private sealed class _PrivilegedExceptionAction_283 : PrivilegedExceptionAction<object
			>
		{
			public _PrivilegedExceptionAction_283(TestReservedRawPaths _enclosing, Path reservedRaw
				)
			{
				this._enclosing = _enclosing;
				this.reservedRaw = reservedRaw;
			}

			/// <exception cref="System.Exception"/>
			public object Run()
			{
				DistributedFileSystem fs = this._enclosing.cluster.GetFileSystem();
				Path d1 = new Path(reservedRaw, "dir1");
				try
				{
					fs.Mkdirs(d1);
					NUnit.Framework.Assert.Fail("access to /.reserved/raw is superuser-only operation"
						);
				}
				catch (AccessControlException e)
				{
					GenericTestUtils.AssertExceptionContains("Superuser privilege is required", e);
				}
				return null;
			}

			private readonly TestReservedRawPaths _enclosing;

			private readonly Path reservedRaw;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestListDotReserved()
		{
			// Create a base file for comparison
			Path baseFileRaw = new Path("/.reserved/raw/base");
			int len = 8192;
			DFSTestUtil.CreateFile(fs, baseFileRaw, len, (short)1, unchecked((int)(0xFEED)));
			/*
			* Ensure that you can't list /.reserved. Ever.
			*/
			try
			{
				fs.ListStatus(new Path("/.reserved"));
				NUnit.Framework.Assert.Fail("expected FNFE");
			}
			catch (FileNotFoundException e)
			{
				GenericTestUtils.AssertExceptionContains("/.reserved does not exist", e);
			}
			try
			{
				fs.ListStatus(new Path("/.reserved/.inodes"));
				NUnit.Framework.Assert.Fail("expected FNFE");
			}
			catch (FileNotFoundException e)
			{
				GenericTestUtils.AssertExceptionContains("/.reserved/.inodes does not exist", e);
			}
			FileStatus[] fileStatuses = fs.ListStatus(new Path("/.reserved/raw"));
			NUnit.Framework.Assert.AreEqual("expected 1 entry", fileStatuses.Length, 1);
			GenericTestUtils.AssertMatches(fileStatuses[0].GetPath().ToString(), "/.reserved/raw/base"
				);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestListRecursive()
		{
			Path rootPath = new Path("/");
			Path p = rootPath;
			for (int i = 0; i < 3; i++)
			{
				p = new Path(p, "dir" + i);
				fs.Mkdirs(p);
			}
			Path curPath = new Path("/.reserved/raw");
			int cnt = 0;
			FileStatus[] fileStatuses = fs.ListStatus(curPath);
			while (fileStatuses != null && fileStatuses.Length > 0)
			{
				FileStatus f = fileStatuses[0];
				GenericTestUtils.AssertMatches(f.GetPath().ToString(), "/.reserved/raw");
				curPath = Path.GetPathWithoutSchemeAndAuthority(f.GetPath());
				cnt++;
				fileStatuses = fs.ListStatus(curPath);
			}
			NUnit.Framework.Assert.AreEqual(3, cnt);
		}
	}
}
