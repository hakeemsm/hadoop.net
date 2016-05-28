using System;
using System.IO;
using System.Text;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Web;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Test;
using Org.Apache.Log4j;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	/// <summary>Test symbolic links in Hdfs.</summary>
	public abstract class TestSymlinkHdfs : SymlinkBaseTest
	{
		protected internal static MiniDFSCluster cluster;

		protected internal static WebHdfsFileSystem webhdfs;

		protected internal static DistributedFileSystem dfs;

		protected override string GetScheme()
		{
			return "hdfs";
		}

		/// <exception cref="System.IO.IOException"/>
		protected override string TestBaseDir1()
		{
			return "/test1";
		}

		/// <exception cref="System.IO.IOException"/>
		protected override string TestBaseDir2()
		{
			return "/test2";
		}

		protected override URI TestURI()
		{
			return cluster.GetURI(0);
		}

		protected override IOException UnwrapException(IOException e)
		{
			if (e is RemoteException)
			{
				return ((RemoteException)e).UnwrapRemoteException();
			}
			return e;
		}

		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void BeforeClassSetup()
		{
			Configuration conf = new HdfsConfiguration();
			conf.SetBoolean(DFSConfigKeys.DfsWebhdfsEnabledKey, true);
			conf.Set(FsPermission.UmaskLabel, "000");
			conf.SetInt(DFSConfigKeys.DfsNamenodeMaxComponentLengthKey, 0);
			cluster = new MiniDFSCluster.Builder(conf).Build();
			webhdfs = WebHdfsTestUtil.GetWebHdfsFileSystem(conf, WebHdfsFileSystem.Scheme);
			dfs = cluster.GetFileSystem();
		}

		/// <exception cref="System.Exception"/>
		[AfterClass]
		public static void AfterClassTeardown()
		{
			cluster.Shutdown();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestLinkAcrossFileSystems()
		{
			Path localDir = new Path("file://" + wrapper.GetAbsoluteTestRootDir() + "/test");
			Path localFile = new Path("file://" + wrapper.GetAbsoluteTestRootDir() + "/test/file"
				);
			Path link = new Path(TestBaseDir1(), "linkToFile");
			FSTestWrapper localWrapper = wrapper.GetLocalFSWrapper();
			localWrapper.Delete(localDir, true);
			localWrapper.Mkdir(localDir, FileContext.DefaultPerm, true);
			localWrapper.SetWorkingDirectory(localDir);
			NUnit.Framework.Assert.AreEqual(localDir, localWrapper.GetWorkingDirectory());
			CreateAndWriteFile(localWrapper, localFile);
			wrapper.CreateSymlink(localFile, link, false);
			ReadFile(link);
			NUnit.Framework.Assert.AreEqual(fileSize, wrapper.GetFileStatus(link).GetLen());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestRenameAcrossFileSystemsViaLink()
		{
			Path localDir = new Path("file://" + wrapper.GetAbsoluteTestRootDir() + "/test");
			Path hdfsFile = new Path(TestBaseDir1(), "file");
			Path link = new Path(TestBaseDir1(), "link");
			Path hdfsFileNew = new Path(TestBaseDir1(), "fileNew");
			Path hdfsFileNewViaLink = new Path(link, "fileNew");
			FSTestWrapper localWrapper = wrapper.GetLocalFSWrapper();
			localWrapper.Delete(localDir, true);
			localWrapper.Mkdir(localDir, FileContext.DefaultPerm, true);
			localWrapper.SetWorkingDirectory(localDir);
			CreateAndWriteFile(hdfsFile);
			wrapper.CreateSymlink(localDir, link, false);
			// Rename hdfs://test1/file to hdfs://test1/link/fileNew
			// which renames to file://TEST_ROOT/test/fileNew which
			// spans AbstractFileSystems and therefore fails.
			try
			{
				wrapper.Rename(hdfsFile, hdfsFileNewViaLink);
				NUnit.Framework.Assert.Fail("Renamed across file systems");
			}
			catch (InvalidPathException)
			{
			}
			catch (ArgumentException e)
			{
				// Expected from FileContext
				// Expected from Filesystem
				GenericTestUtils.AssertExceptionContains("Wrong FS: ", e);
			}
			// Now rename hdfs://test1/link/fileNew to hdfs://test1/fileNew
			// which renames file://TEST_ROOT/test/fileNew to hdfs://test1/fileNew
			// which spans AbstractFileSystems and therefore fails.
			CreateAndWriteFile(hdfsFileNewViaLink);
			try
			{
				wrapper.Rename(hdfsFileNewViaLink, hdfsFileNew);
				NUnit.Framework.Assert.Fail("Renamed across file systems");
			}
			catch (InvalidPathException)
			{
			}
			catch (ArgumentException e)
			{
				// Expected from FileContext
				// Expected from Filesystem
				GenericTestUtils.AssertExceptionContains("Wrong FS: ", e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestCreateLinkToSlash()
		{
			Path dir = new Path(TestBaseDir1());
			Path file = new Path(TestBaseDir1(), "file");
			Path link = new Path(TestBaseDir1(), "linkToSlash");
			Path fileViaLink = new Path(TestBaseDir1() + "/linkToSlash" + TestBaseDir1() + "/file"
				);
			CreateAndWriteFile(file);
			wrapper.SetWorkingDirectory(dir);
			wrapper.CreateSymlink(new Path("/"), link, false);
			ReadFile(fileViaLink);
			NUnit.Framework.Assert.AreEqual(fileSize, wrapper.GetFileStatus(fileViaLink).GetLen
				());
			// Ditto when using another file context since the file system
			// for the slash is resolved according to the link's parent.
			if (wrapper is FileContextTestWrapper)
			{
				FSTestWrapper localWrapper = wrapper.GetLocalFSWrapper();
				Path linkQual = new Path(cluster.GetURI(0).ToString(), fileViaLink);
				NUnit.Framework.Assert.AreEqual(fileSize, localWrapper.GetFileStatus(linkQual).GetLen
					());
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestSetPermissionAffectsTarget()
		{
			Path file = new Path(TestBaseDir1(), "file");
			Path dir = new Path(TestBaseDir2());
			Path linkToFile = new Path(TestBaseDir1(), "linkToFile");
			Path linkToDir = new Path(TestBaseDir1(), "linkToDir");
			CreateAndWriteFile(file);
			wrapper.CreateSymlink(file, linkToFile, false);
			wrapper.CreateSymlink(dir, linkToDir, false);
			// Changing the permissions using the link does not modify
			// the permissions of the link..
			FsPermission perms = wrapper.GetFileLinkStatus(linkToFile).GetPermission();
			wrapper.SetPermission(linkToFile, new FsPermission((short)0x1b4));
			wrapper.SetOwner(linkToFile, "user", "group");
			NUnit.Framework.Assert.AreEqual(perms, wrapper.GetFileLinkStatus(linkToFile).GetPermission
				());
			// but the file's permissions were adjusted appropriately
			FileStatus stat = wrapper.GetFileStatus(file);
			NUnit.Framework.Assert.AreEqual(0x1b4, stat.GetPermission().ToShort());
			NUnit.Framework.Assert.AreEqual("user", stat.GetOwner());
			NUnit.Framework.Assert.AreEqual("group", stat.GetGroup());
			// Getting the file's permissions via the link is the same
			// as getting the permissions directly.
			NUnit.Framework.Assert.AreEqual(stat.GetPermission(), wrapper.GetFileStatus(linkToFile
				).GetPermission());
			// Ditto for a link to a directory
			perms = wrapper.GetFileLinkStatus(linkToDir).GetPermission();
			wrapper.SetPermission(linkToDir, new FsPermission((short)0x1b4));
			wrapper.SetOwner(linkToDir, "user", "group");
			NUnit.Framework.Assert.AreEqual(perms, wrapper.GetFileLinkStatus(linkToDir).GetPermission
				());
			stat = wrapper.GetFileStatus(dir);
			NUnit.Framework.Assert.AreEqual(0x1b4, stat.GetPermission().ToShort());
			NUnit.Framework.Assert.AreEqual("user", stat.GetOwner());
			NUnit.Framework.Assert.AreEqual("group", stat.GetGroup());
			NUnit.Framework.Assert.AreEqual(stat.GetPermission(), wrapper.GetFileStatus(linkToDir
				).GetPermission());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestCreateWithPartQualPathFails()
		{
			Path fileWoAuth = new Path("hdfs:///test/file");
			Path linkWoAuth = new Path("hdfs:///test/link");
			try
			{
				CreateAndWriteFile(fileWoAuth);
				NUnit.Framework.Assert.Fail("HDFS requires URIs with schemes have an authority");
			}
			catch (RuntimeException)
			{
			}
			// Expected
			try
			{
				wrapper.CreateSymlink(new Path("foo"), linkWoAuth, false);
				NUnit.Framework.Assert.Fail("HDFS requires URIs with schemes have an authority");
			}
			catch (RuntimeException)
			{
			}
		}

		// Expected
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestSetReplication()
		{
			Path file = new Path(TestBaseDir1(), "file");
			Path link = new Path(TestBaseDir1(), "linkToFile");
			CreateAndWriteFile(file);
			wrapper.CreateSymlink(file, link, false);
			wrapper.SetReplication(link, (short)2);
			NUnit.Framework.Assert.AreEqual(0, wrapper.GetFileLinkStatus(link).GetReplication
				());
			NUnit.Framework.Assert.AreEqual(2, wrapper.GetFileStatus(link).GetReplication());
			NUnit.Framework.Assert.AreEqual(2, wrapper.GetFileStatus(file).GetReplication());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestCreateLinkMaxPathLink()
		{
			Path dir = new Path(TestBaseDir1());
			Path file = new Path(TestBaseDir1(), "file");
			int maxPathLen = HdfsConstants.MaxPathLength;
			int dirLen = dir.ToString().Length + 1;
			int len = maxPathLen - dirLen;
			// Build a MAX_PATH_LENGTH path
			StringBuilder sb = new StringBuilder(string.Empty);
			for (int i = 0; i < (len / 10); i++)
			{
				sb.Append("0123456789");
			}
			for (int i_1 = 0; i_1 < (len % 10); i_1++)
			{
				sb.Append("x");
			}
			Path link = new Path(sb.ToString());
			NUnit.Framework.Assert.AreEqual(maxPathLen, dirLen + link.ToString().Length);
			// Check that it works
			CreateAndWriteFile(file);
			wrapper.SetWorkingDirectory(dir);
			wrapper.CreateSymlink(file, link, false);
			ReadFile(link);
			// Now modify the path so it's too large
			link = new Path(sb.ToString() + "x");
			try
			{
				wrapper.CreateSymlink(file, link, false);
				NUnit.Framework.Assert.Fail("Path name should be too long");
			}
			catch (IOException)
			{
			}
		}

		// Expected
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestLinkOwner()
		{
			Path file = new Path(TestBaseDir1(), "file");
			Path link = new Path(TestBaseDir1(), "symlinkToFile");
			CreateAndWriteFile(file);
			wrapper.CreateSymlink(file, link, false);
			FileStatus statFile = wrapper.GetFileStatus(file);
			FileStatus statLink = wrapper.GetFileStatus(link);
			NUnit.Framework.Assert.AreEqual(statLink.GetOwner(), statFile.GetOwner());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestWebHDFS()
		{
			Path file = new Path(TestBaseDir1(), "file");
			Path link = new Path(TestBaseDir1(), "linkToFile");
			CreateAndWriteFile(file);
			webhdfs.CreateSymlink(file, link, false);
			wrapper.SetReplication(link, (short)2);
			NUnit.Framework.Assert.AreEqual(0, wrapper.GetFileLinkStatus(link).GetReplication
				());
			NUnit.Framework.Assert.AreEqual(2, wrapper.GetFileStatus(link).GetReplication());
			NUnit.Framework.Assert.AreEqual(2, wrapper.GetFileStatus(file).GetReplication());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestQuota()
		{
			Path dir = new Path(TestBaseDir1());
			dfs.SetQuota(dir, 3, HdfsConstants.QuotaDontSet);
			Path file = new Path(dir, "file");
			CreateAndWriteFile(file);
			//creating the first link should succeed
			Path link1 = new Path(dir, "link1");
			wrapper.CreateSymlink(file, link1, false);
			try
			{
				//creating the second link should fail with QuotaExceededException.
				Path link2 = new Path(dir, "link2");
				wrapper.CreateSymlink(file, link2, false);
				NUnit.Framework.Assert.Fail("Created symlink despite quota violation");
			}
			catch (QuotaExceededException)
			{
			}
		}

		public TestSymlinkHdfs()
		{
			{
				GenericTestUtils.SetLogLevel(NameNode.stateChangeLog, Level.All);
			}
		}
		//expected
	}
}
