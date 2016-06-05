using System;
using NUnit.Framework;
using Org.Apache.Commons.Logging.Impl;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Util;
using Org.Apache.Log4j;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	public class TestFcHdfsSetUMask
	{
		private static readonly FileContextTestHelper fileContextTestHelper = new FileContextTestHelper
			("/tmp/TestFcHdfsSetUMask");

		private static MiniDFSCluster cluster;

		private static Path defaultWorkingDirectory;

		private static FileContext fc;

		private static readonly FsPermission UserGroupOpenPermissions = FsPermission.CreateImmutable
			((short)0x1f8);

		private static readonly FsPermission UserGroupOpenFilePermissions = FsPermission.
			CreateImmutable((short)0x1b0);

		private static readonly FsPermission UserGroupOpenTestUmask = FsPermission.CreateImmutable
			((short)(0x1f8 ^ 0x1ff));

		private static readonly FsPermission BlankPermissions = FsPermission.CreateImmutable
			((short)0000);

		private static readonly FsPermission ParentPermsForBlankPermissions = FsPermission
			.CreateImmutable((short)0xc0);

		private static readonly FsPermission BlankTestUmask = FsPermission.CreateImmutable
			((short)(0000 ^ 0x1ff));

		private static readonly FsPermission WideOpenPermissions = FsPermission.CreateImmutable
			((short)0x1ff);

		private static readonly FsPermission WideOpenFilePermissions = FsPermission.CreateImmutable
			((short)0x1b6);

		private static readonly FsPermission WideOpenTestUmask = FsPermission.CreateImmutable
			((short)(0x1ff ^ 0x1ff));

		// rwxrwx---
		// ---------
		// parent directory permissions when creating a directory with blank (000)
		// permissions - it always add the -wx------ bits to the parent so that
		// it can create the child
		// rwxrwxrwx
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Javax.Security.Auth.Login.LoginException"/>
		/// <exception cref="Sharpen.URISyntaxException"/>
		[BeforeClass]
		public static void ClusterSetupAtBegining()
		{
			Configuration conf = new HdfsConfiguration();
			// set permissions very restrictive
			conf.Set(CommonConfigurationKeys.FsPermissionsUmaskKey, "077");
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(2).Build();
			fc = FileContext.GetFileContext(cluster.GetURI(0), conf);
			defaultWorkingDirectory = fc.MakeQualified(new Path("/user/" + UserGroupInformation
				.GetCurrentUser().GetShortUserName()));
			fc.Mkdir(defaultWorkingDirectory, FileContext.DefaultPerm, true);
		}

		/// <exception cref="System.Exception"/>
		[AfterClass]
		public static void ClusterShutdownAtEnd()
		{
			cluster.Shutdown();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			fc.SetUMask(WideOpenTestUmask);
			fc.Mkdir(fileContextTestHelper.GetTestRootPath(fc), FileContext.DefaultPerm, true
				);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			fc.Delete(fileContextTestHelper.GetTestRootPath(fc), true);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestMkdirWithExistingDirClear()
		{
			TestMkdirWithExistingDir(BlankTestUmask, BlankPermissions);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestMkdirWithExistingDirOpen()
		{
			TestMkdirWithExistingDir(WideOpenTestUmask, WideOpenPermissions);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestMkdirWithExistingDirMiddle()
		{
			TestMkdirWithExistingDir(UserGroupOpenTestUmask, UserGroupOpenPermissions);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestMkdirRecursiveWithNonExistingDirClear()
		{
			// by default parent directories have -wx------ bits set
			TestMkdirRecursiveWithNonExistingDir(BlankTestUmask, BlankPermissions, ParentPermsForBlankPermissions
				);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestMkdirRecursiveWithNonExistingDirOpen()
		{
			TestMkdirRecursiveWithNonExistingDir(WideOpenTestUmask, WideOpenPermissions, WideOpenPermissions
				);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestMkdirRecursiveWithNonExistingDirMiddle()
		{
			TestMkdirRecursiveWithNonExistingDir(UserGroupOpenTestUmask, UserGroupOpenPermissions
				, UserGroupOpenPermissions);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestCreateRecursiveWithExistingDirClear()
		{
			TestCreateRecursiveWithExistingDir(BlankTestUmask, BlankPermissions);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestCreateRecursiveWithExistingDirOpen()
		{
			TestCreateRecursiveWithExistingDir(WideOpenTestUmask, WideOpenFilePermissions);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestCreateRecursiveWithExistingDirMiddle()
		{
			TestCreateRecursiveWithExistingDir(UserGroupOpenTestUmask, UserGroupOpenFilePermissions
				);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestCreateRecursiveWithNonExistingDirClear()
		{
			// directory permission inherited from parent so this must match the @Before
			// set of umask
			TestCreateRecursiveWithNonExistingDir(BlankTestUmask, WideOpenPermissions, BlankPermissions
				);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestCreateRecursiveWithNonExistingDirOpen()
		{
			// directory permission inherited from parent so this must match the @Before
			// set of umask
			TestCreateRecursiveWithNonExistingDir(WideOpenTestUmask, WideOpenPermissions, WideOpenFilePermissions
				);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestCreateRecursiveWithNonExistingDirMiddle()
		{
			// directory permission inherited from parent so this must match the @Before
			// set of umask
			TestCreateRecursiveWithNonExistingDir(UserGroupOpenTestUmask, WideOpenPermissions
				, UserGroupOpenFilePermissions);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestMkdirWithExistingDir(FsPermission umask, FsPermission expectedPerms
			)
		{
			Path f = fileContextTestHelper.GetTestRootPath(fc, "aDir");
			fc.SetUMask(umask);
			fc.Mkdir(f, FileContext.DefaultPerm, true);
			NUnit.Framework.Assert.IsTrue(FileContextTestHelper.IsDir(fc, f));
			NUnit.Framework.Assert.AreEqual("permissions on directory are wrong", expectedPerms
				, fc.GetFileStatus(f).GetPermission());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestMkdirRecursiveWithNonExistingDir(FsPermission umask, FsPermission
			 expectedPerms, FsPermission expectedParentPerms)
		{
			Path f = fileContextTestHelper.GetTestRootPath(fc, "NonExistant2/aDir");
			fc.SetUMask(umask);
			fc.Mkdir(f, FileContext.DefaultPerm, true);
			NUnit.Framework.Assert.IsTrue(FileContextTestHelper.IsDir(fc, f));
			NUnit.Framework.Assert.AreEqual("permissions on directory are wrong", expectedPerms
				, fc.GetFileStatus(f).GetPermission());
			Path fParent = fileContextTestHelper.GetTestRootPath(fc, "NonExistant2");
			NUnit.Framework.Assert.AreEqual("permissions on parent directory are wrong", expectedParentPerms
				, fc.GetFileStatus(fParent).GetPermission());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestCreateRecursiveWithExistingDir(FsPermission umask, FsPermission
			 expectedPerms)
		{
			Path f = fileContextTestHelper.GetTestRootPath(fc, "foo");
			fc.SetUMask(umask);
			FileContextTestHelper.CreateFile(fc, f);
			NUnit.Framework.Assert.IsTrue(FileContextTestHelper.IsFile(fc, f));
			NUnit.Framework.Assert.AreEqual("permissions on file are wrong", expectedPerms, fc
				.GetFileStatus(f).GetPermission());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestCreateRecursiveWithNonExistingDir(FsPermission umask, FsPermission
			 expectedDirPerms, FsPermission expectedFilePerms)
		{
			Path f = fileContextTestHelper.GetTestRootPath(fc, "NonExisting/foo");
			Path fParent = fileContextTestHelper.GetTestRootPath(fc, "NonExisting");
			NUnit.Framework.Assert.IsFalse(FileContextTestHelper.Exists(fc, fParent));
			fc.SetUMask(umask);
			FileContextTestHelper.CreateFile(fc, f);
			NUnit.Framework.Assert.IsTrue(FileContextTestHelper.IsFile(fc, f));
			NUnit.Framework.Assert.AreEqual("permissions on file are wrong", expectedFilePerms
				, fc.GetFileStatus(f).GetPermission());
			NUnit.Framework.Assert.AreEqual("permissions on parent directory are wrong", expectedDirPerms
				, fc.GetFileStatus(fParent).GetPermission());
		}

		public TestFcHdfsSetUMask()
		{
			{
				try
				{
					((Log4JLogger)FileSystem.Log).GetLogger().SetLevel(Level.Debug);
				}
				catch (Exception e)
				{
					System.Console.Out.WriteLine("Cannot change log level\n" + StringUtils.StringifyException
						(e));
				}
			}
		}
	}
}
