using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Test;
using Sharpen;

namespace Org.Apache.Hadoop.Security
{
	public class TestPermissionSymlinks
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestPermissionSymlinks
			));

		private static readonly Configuration conf = new HdfsConfiguration();

		private static readonly UserGroupInformation user = UserGroupInformation.CreateRemoteUser
			("myuser");

		private static readonly Path linkParent = new Path("/symtest1");

		private static readonly Path targetParent = new Path("/symtest2");

		private static readonly Path link = new Path(linkParent, "link");

		private static readonly Path target = new Path(targetParent, "target");

		private static MiniDFSCluster cluster;

		private static FileSystem fs;

		private static FileSystemTestWrapper wrapper;

		// Non-super user to run commands with
		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void BeforeClassSetUp()
		{
			conf.SetBoolean(DFSConfigKeys.DfsPermissionsEnabledKey, true);
			conf.SetBoolean(DFSConfigKeys.DfsNamenodeAclsEnabledKey, true);
			conf.Set(FsPermission.UmaskLabel, "000");
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(3).Build();
			cluster.WaitActive();
			fs = cluster.GetFileSystem();
			wrapper = new FileSystemTestWrapper(fs);
		}

		/// <exception cref="System.Exception"/>
		[AfterClass]
		public static void AfterClassTearDown()
		{
			if (fs != null)
			{
				fs.Close();
			}
			if (cluster != null)
			{
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			// Create initial test files
			fs.Mkdirs(linkParent);
			fs.Mkdirs(targetParent);
			DFSTestUtil.CreateFile(fs, target, 1024, (short)3, unchecked((long)(0xBEEFl)));
			wrapper.CreateSymlink(target, link, false);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			// Wipe out everything
			fs.Delete(linkParent, true);
			fs.Delete(targetParent, true);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestDelete()
		{
			fs.SetPermission(linkParent, new FsPermission((short)0x16d));
			DoDeleteLinkParentNotWritable();
			fs.SetPermission(linkParent, new FsPermission((short)0x1ff));
			fs.SetPermission(targetParent, new FsPermission((short)0x16d));
			fs.SetPermission(target, new FsPermission((short)0x16d));
			DoDeleteTargetParentAndTargetNotWritable();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAclDelete()
		{
			fs.SetAcl(linkParent, Arrays.AsList(AclTestHelpers.AclEntry(AclEntryScope.Access, 
				AclEntryType.User, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope.Access, 
				AclEntryType.User, user.GetUserName(), FsAction.ReadExecute), AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Group, FsAction.All), AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Other, FsAction.All)));
			DoDeleteLinkParentNotWritable();
			fs.SetAcl(linkParent, Arrays.AsList(AclTestHelpers.AclEntry(AclEntryScope.Access, 
				AclEntryType.User, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope.Access, 
				AclEntryType.Group, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope.Access, 
				AclEntryType.Other, FsAction.All)));
			fs.SetAcl(targetParent, Arrays.AsList(AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.User, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.User, user.GetUserName(), FsAction.ReadExecute), AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Group, FsAction.All), AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Other, FsAction.All)));
			fs.SetAcl(target, Arrays.AsList(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.User, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.User, user.GetUserName(), FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Other, FsAction.All)));
			DoDeleteTargetParentAndTargetNotWritable();
		}

		/// <exception cref="System.Exception"/>
		private void DoDeleteLinkParentNotWritable()
		{
			// Try to delete where the symlink's parent dir is not writable
			try
			{
				user.DoAs(new _PrivilegedExceptionAction_150());
				NUnit.Framework.Assert.Fail("Deleted symlink without write permissions on parent!"
					);
			}
			catch (AccessControlException e)
			{
				GenericTestUtils.AssertExceptionContains("Permission denied", e);
			}
		}

		private sealed class _PrivilegedExceptionAction_150 : PrivilegedExceptionAction<object
			>
		{
			public _PrivilegedExceptionAction_150()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public object Run()
			{
				FileContext myfc = FileContext.GetFileContext(TestPermissionSymlinks.conf);
				myfc.Delete(TestPermissionSymlinks.link, false);
				return null;
			}
		}

		/// <exception cref="System.Exception"/>
		private void DoDeleteTargetParentAndTargetNotWritable()
		{
			// Try a delete where the symlink parent dir is writable,
			// but the target's parent and target are not
			user.DoAs(new _PrivilegedExceptionAction_167());
			// Make sure only the link was deleted
			NUnit.Framework.Assert.IsTrue("Target should not have been deleted!", wrapper.Exists
				(target));
			NUnit.Framework.Assert.IsFalse("Link should have been deleted!", wrapper.Exists(link
				));
		}

		private sealed class _PrivilegedExceptionAction_167 : PrivilegedExceptionAction<object
			>
		{
			public _PrivilegedExceptionAction_167()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public object Run()
			{
				FileContext myfc = FileContext.GetFileContext(TestPermissionSymlinks.conf);
				myfc.Delete(TestPermissionSymlinks.link, false);
				return null;
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestReadWhenTargetNotReadable()
		{
			fs.SetPermission(target, new FsPermission((short)0000));
			DoReadTargetNotReadable();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAclReadTargetNotReadable()
		{
			fs.SetAcl(target, Arrays.AsList(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.User, FsAction.ReadWrite), AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.User, user.GetUserName(), FsAction.None), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, FsAction.Read), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Other, FsAction.Read)));
			DoReadTargetNotReadable();
		}

		/// <exception cref="System.Exception"/>
		private void DoReadTargetNotReadable()
		{
			try
			{
				user.DoAs(new _PrivilegedExceptionAction_200());
				NUnit.Framework.Assert.Fail("Read link target even though target does not have" +
					 " read permissions!");
			}
			catch (IOException e)
			{
				GenericTestUtils.AssertExceptionContains("Permission denied", e);
			}
		}

		private sealed class _PrivilegedExceptionAction_200 : PrivilegedExceptionAction<object
			>
		{
			public _PrivilegedExceptionAction_200()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public object Run()
			{
				FileContext myfc = FileContext.GetFileContext(TestPermissionSymlinks.conf);
				myfc.Open(TestPermissionSymlinks.link).Read();
				return null;
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestFileStatus()
		{
			fs.SetPermission(target, new FsPermission((short)0000));
			DoGetFileLinkStatusTargetNotReadable();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAclGetFileLinkStatusTargetNotReadable()
		{
			fs.SetAcl(target, Arrays.AsList(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.User, FsAction.ReadWrite), AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.User, user.GetUserName(), FsAction.None), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, FsAction.Read), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Other, FsAction.Read)));
			DoGetFileLinkStatusTargetNotReadable();
		}

		/// <exception cref="System.Exception"/>
		private void DoGetFileLinkStatusTargetNotReadable()
		{
			// Try to getFileLinkStatus the link when the target is not readable
			user.DoAs(new _PrivilegedExceptionAction_233());
		}

		private sealed class _PrivilegedExceptionAction_233 : PrivilegedExceptionAction<object
			>
		{
			public _PrivilegedExceptionAction_233()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public object Run()
			{
				FileContext myfc = FileContext.GetFileContext(TestPermissionSymlinks.conf);
				FileStatus stat = myfc.GetFileLinkStatus(TestPermissionSymlinks.link);
				NUnit.Framework.Assert.AreEqual("Expected link's FileStatus path to match link!", 
					TestPermissionSymlinks.link.MakeQualified(TestPermissionSymlinks.fs.GetUri(), TestPermissionSymlinks
					.fs.GetWorkingDirectory()), stat.GetPath());
				Path linkTarget = myfc.GetLinkTarget(TestPermissionSymlinks.link);
				NUnit.Framework.Assert.AreEqual("Expected link's target to match target!", TestPermissionSymlinks
					.target, linkTarget);
				return null;
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRenameLinkTargetNotWritableFC()
		{
			fs.SetPermission(target, new FsPermission((short)0x16d));
			fs.SetPermission(targetParent, new FsPermission((short)0x16d));
			DoRenameLinkTargetNotWritableFC();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAclRenameTargetNotWritableFC()
		{
			fs.SetAcl(target, Arrays.AsList(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.User, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.User, user.GetUserName(), FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Other, FsAction.All)));
			fs.SetAcl(targetParent, Arrays.AsList(AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.User, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.User, user.GetUserName(), FsAction.ReadExecute), AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Group, FsAction.All), AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Other, FsAction.All)));
			DoRenameLinkTargetNotWritableFC();
		}

		/// <exception cref="System.Exception"/>
		private void DoRenameLinkTargetNotWritableFC()
		{
			// Rename the link when the target and parent are not writable
			user.DoAs(new _PrivilegedExceptionAction_272());
			// First FileContext
			NUnit.Framework.Assert.IsTrue("Expected target to exist", wrapper.Exists(target));
		}

		private sealed class _PrivilegedExceptionAction_272 : PrivilegedExceptionAction<object
			>
		{
			public _PrivilegedExceptionAction_272()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public object Run()
			{
				FileContext myfc = FileContext.GetFileContext(TestPermissionSymlinks.conf);
				Path newlink = new Path(TestPermissionSymlinks.linkParent, "newlink");
				myfc.Rename(TestPermissionSymlinks.link, newlink, Options.Rename.None);
				Path linkTarget = myfc.GetLinkTarget(newlink);
				NUnit.Framework.Assert.AreEqual("Expected link's target to match target!", TestPermissionSymlinks
					.target, linkTarget);
				return null;
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRenameSrcNotWritableFC()
		{
			fs.SetPermission(linkParent, new FsPermission((short)0x16d));
			DoRenameSrcNotWritableFC();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAclRenameSrcNotWritableFC()
		{
			fs.SetAcl(linkParent, Arrays.AsList(AclTestHelpers.AclEntry(AclEntryScope.Access, 
				AclEntryType.User, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope.Access, 
				AclEntryType.User, user.GetUserName(), FsAction.ReadExecute), AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Group, FsAction.All), AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Other, FsAction.All)));
			DoRenameSrcNotWritableFC();
		}

		/// <exception cref="System.Exception"/>
		private void DoRenameSrcNotWritableFC()
		{
			// Rename the link when the target and parent are not writable
			try
			{
				user.DoAs(new _PrivilegedExceptionAction_307());
				NUnit.Framework.Assert.Fail("Renamed link even though link's parent is not writable!"
					);
			}
			catch (IOException e)
			{
				GenericTestUtils.AssertExceptionContains("Permission denied", e);
			}
		}

		private sealed class _PrivilegedExceptionAction_307 : PrivilegedExceptionAction<object
			>
		{
			public _PrivilegedExceptionAction_307()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public object Run()
			{
				FileContext myfc = FileContext.GetFileContext(TestPermissionSymlinks.conf);
				Path newlink = new Path(TestPermissionSymlinks.targetParent, "newlink");
				myfc.Rename(TestPermissionSymlinks.link, newlink, Options.Rename.None);
				return null;
			}
		}

		// Need separate FileSystem tests since the server-side impl is different
		// See {@link ClientProtocol#rename} and {@link ClientProtocol#rename2}.
		/// <exception cref="System.Exception"/>
		public virtual void TestRenameLinkTargetNotWritableFS()
		{
			fs.SetPermission(target, new FsPermission((short)0x16d));
			fs.SetPermission(targetParent, new FsPermission((short)0x16d));
			DoRenameLinkTargetNotWritableFS();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAclRenameTargetNotWritableFS()
		{
			fs.SetAcl(target, Arrays.AsList(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.User, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.User, user.GetUserName(), FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Group, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Other, FsAction.All)));
			fs.SetAcl(targetParent, Arrays.AsList(AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.User, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope.Access
				, AclEntryType.User, user.GetUserName(), FsAction.ReadExecute), AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Group, FsAction.All), AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Other, FsAction.All)));
			DoRenameLinkTargetNotWritableFS();
		}

		/// <exception cref="System.Exception"/>
		private void DoRenameLinkTargetNotWritableFS()
		{
			// Rename the link when the target and parent are not writable
			user.DoAs(new _PrivilegedExceptionAction_349());
			// First FileContext
			NUnit.Framework.Assert.IsTrue("Expected target to exist", wrapper.Exists(target));
		}

		private sealed class _PrivilegedExceptionAction_349 : PrivilegedExceptionAction<object
			>
		{
			public _PrivilegedExceptionAction_349()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public object Run()
			{
				FileSystem myfs = FileSystem.Get(TestPermissionSymlinks.conf);
				Path newlink = new Path(TestPermissionSymlinks.linkParent, "newlink");
				myfs.Rename(TestPermissionSymlinks.link, newlink);
				Path linkTarget = myfs.GetLinkTarget(newlink);
				NUnit.Framework.Assert.AreEqual("Expected link's target to match target!", TestPermissionSymlinks
					.target, linkTarget);
				return null;
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRenameSrcNotWritableFS()
		{
			fs.SetPermission(linkParent, new FsPermission((short)0x16d));
			DoRenameSrcNotWritableFS();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAclRenameSrcNotWritableFS()
		{
			fs.SetAcl(linkParent, Arrays.AsList(AclTestHelpers.AclEntry(AclEntryScope.Access, 
				AclEntryType.User, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope.Access, 
				AclEntryType.User, user.GetUserName(), FsAction.ReadExecute), AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Group, FsAction.All), AclTestHelpers.AclEntry
				(AclEntryScope.Access, AclEntryType.Other, FsAction.All)));
			DoRenameSrcNotWritableFS();
		}

		/// <exception cref="System.Exception"/>
		private void DoRenameSrcNotWritableFS()
		{
			// Rename the link when the target and parent are not writable
			try
			{
				user.DoAs(new _PrivilegedExceptionAction_384());
				NUnit.Framework.Assert.Fail("Renamed link even though link's parent is not writable!"
					);
			}
			catch (IOException e)
			{
				GenericTestUtils.AssertExceptionContains("Permission denied", e);
			}
		}

		private sealed class _PrivilegedExceptionAction_384 : PrivilegedExceptionAction<object
			>
		{
			public _PrivilegedExceptionAction_384()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public object Run()
			{
				FileSystem myfs = FileSystem.Get(TestPermissionSymlinks.conf);
				Path newlink = new Path(TestPermissionSymlinks.targetParent, "newlink");
				myfs.Rename(TestPermissionSymlinks.link, newlink);
				return null;
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAccess()
		{
			fs.SetPermission(target, new FsPermission((short)0x2));
			fs.SetAcl(target, Arrays.AsList(AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.User, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.Group, FsAction.None), AclTestHelpers.AclEntry(AclEntryScope.Access, AclEntryType
				.User, user.GetShortUserName(), FsAction.Write), AclTestHelpers.AclEntry(AclEntryScope
				.Access, AclEntryType.Other, FsAction.Write)));
			FileContext myfc = user.DoAs(new _PrivilegedExceptionAction_407());
			// Path to targetChild via symlink
			myfc.Access(link, FsAction.Write);
			try
			{
				myfc.Access(link, FsAction.All);
				NUnit.Framework.Assert.Fail("The access call should have failed.");
			}
			catch (AccessControlException)
			{
			}
			// expected
			Path badPath = new Path(link, "bad");
			try
			{
				myfc.Access(badPath, FsAction.Read);
				NUnit.Framework.Assert.Fail("The access call should have failed");
			}
			catch (FileNotFoundException)
			{
			}
		}

		private sealed class _PrivilegedExceptionAction_407 : PrivilegedExceptionAction<FileContext
			>
		{
			public _PrivilegedExceptionAction_407()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public FileContext Run()
			{
				return FileContext.GetFileContext(TestPermissionSymlinks.conf);
			}
		}
		// expected
	}
}
