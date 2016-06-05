using System;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Security;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	public class TestHDFSFileContextMainOperations : FileContextMainOperationsBaseTest
	{
		private static MiniDFSCluster cluster;

		private static Path defaultWorkingDirectory;

		private static readonly HdfsConfiguration Conf = new HdfsConfiguration();

		protected override FileContextTestHelper CreateFileContextHelper()
		{
			return new FileContextTestHelper("/tmp/TestHDFSFileContextMainOperations");
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Javax.Security.Auth.Login.LoginException"/>
		/// <exception cref="Sharpen.URISyntaxException"/>
		[BeforeClass]
		public static void ClusterSetupAtBegining()
		{
			cluster = new MiniDFSCluster.Builder(Conf).NumDataNodes(2).Build();
			cluster.WaitClusterUp();
			URI uri0 = cluster.GetURI(0);
			fc = FileContext.GetFileContext(uri0, Conf);
			defaultWorkingDirectory = fc.MakeQualified(new Path("/user/" + UserGroupInformation
				.GetCurrentUser().GetShortUserName()));
			fc.Mkdir(defaultWorkingDirectory, FileContext.DefaultPerm, true);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Javax.Security.Auth.Login.LoginException"/>
		private static void RestartCluster()
		{
			if (cluster != null)
			{
				cluster.Shutdown();
				cluster = null;
			}
			cluster = new MiniDFSCluster.Builder(Conf).NumDataNodes(1).Format(false).Build();
			cluster.WaitClusterUp();
			fc = FileContext.GetFileContext(cluster.GetURI(0), Conf);
			defaultWorkingDirectory = fc.MakeQualified(new Path("/user/" + UserGroupInformation
				.GetCurrentUser().GetShortUserName()));
			fc.Mkdir(defaultWorkingDirectory, FileContext.DefaultPerm, true);
		}

		/// <exception cref="System.Exception"/>
		[AfterClass]
		public static void ClusterShutdownAtEnd()
		{
			if (cluster != null)
			{
				cluster.Shutdown();
				cluster = null;
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public override void SetUp()
		{
			base.SetUp();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public override void TearDown()
		{
			base.TearDown();
		}

		protected override Path GetDefaultWorkingDirectory()
		{
			return defaultWorkingDirectory;
		}

		protected override IOException UnwrapException(IOException e)
		{
			if (e is RemoteException)
			{
				return ((RemoteException)e).UnwrapRemoteException();
			}
			return e;
		}

		private Path GetTestRootPath(FileContext fc, string path)
		{
			return fileContextTestHelper.GetTestRootPath(fc, path);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestTruncate()
		{
			short repl = 3;
			int blockSize = 1024;
			int numOfBlocks = 2;
			DistributedFileSystem fs = cluster.GetFileSystem();
			Path dir = GetTestRootPath(fc, "test/hadoop");
			Path file = GetTestRootPath(fc, "test/hadoop/file");
			byte[] data = FileSystemTestHelper.GetFileData(numOfBlocks, blockSize);
			FileSystemTestHelper.CreateFile(fs, file, data, blockSize, repl);
			int newLength = blockSize;
			bool isReady = fc.Truncate(file, newLength);
			NUnit.Framework.Assert.IsTrue("Recovery is not expected.", isReady);
			FileStatus fileStatus = fc.GetFileStatus(file);
			NUnit.Framework.Assert.AreEqual(fileStatus.GetLen(), newLength);
			AppendTestUtil.CheckFullFile(fs, file, newLength, data, file.ToString());
			ContentSummary cs = fs.GetContentSummary(dir);
			NUnit.Framework.Assert.AreEqual("Bad disk space usage", cs.GetSpaceConsumed(), newLength
				 * repl);
			NUnit.Framework.Assert.IsTrue(fs.Delete(dir, true));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestOldRenameWithQuota()
		{
			DistributedFileSystem fs = cluster.GetFileSystem();
			Path src1 = GetTestRootPath(fc, "test/testOldRenameWithQuota/srcdir/src1");
			Path src2 = GetTestRootPath(fc, "test/testOldRenameWithQuota/srcdir/src2");
			Path dst1 = GetTestRootPath(fc, "test/testOldRenameWithQuota/dstdir/dst1");
			Path dst2 = GetTestRootPath(fc, "test/testOldRenameWithQuota/dstdir/dst2");
			CreateFile(src1);
			CreateFile(src2);
			fs.SetQuota(src1.GetParent(), HdfsConstants.QuotaDontSet, HdfsConstants.QuotaDontSet
				);
			fc.Mkdir(dst1.GetParent(), FileContext.DefaultPerm, true);
			fs.SetQuota(dst1.GetParent(), 2, HdfsConstants.QuotaDontSet);
			/*
			* Test1: src does not exceed quota and dst has no quota check and hence
			* accommodates rename
			*/
			OldRename(src1, dst1, true, false);
			/*
			* Test2: src does not exceed quota and dst has *no* quota to accommodate
			* rename.
			*/
			// dstDir quota = 1 and dst1 already uses it
			OldRename(src2, dst2, false, true);
			/*
			* Test3: src exceeds quota and dst has *no* quota to accommodate rename
			*/
			// src1 has no quota to accommodate new rename node
			fs.SetQuota(src1.GetParent(), 1, HdfsConstants.QuotaDontSet);
			OldRename(dst1, src1, false, true);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRenameWithQuota()
		{
			DistributedFileSystem fs = cluster.GetFileSystem();
			Path src1 = GetTestRootPath(fc, "test/testRenameWithQuota/srcdir/src1");
			Path src2 = GetTestRootPath(fc, "test/testRenameWithQuota/srcdir/src2");
			Path dst1 = GetTestRootPath(fc, "test/testRenameWithQuota/dstdir/dst1");
			Path dst2 = GetTestRootPath(fc, "test/testRenameWithQuota/dstdir/dst2");
			CreateFile(src1);
			CreateFile(src2);
			fs.SetQuota(src1.GetParent(), HdfsConstants.QuotaDontSet, HdfsConstants.QuotaDontSet
				);
			fc.Mkdir(dst1.GetParent(), FileContext.DefaultPerm, true);
			fs.SetQuota(dst1.GetParent(), 2, HdfsConstants.QuotaDontSet);
			/*
			* Test1: src does not exceed quota and dst has no quota check and hence
			* accommodates rename
			*/
			// rename uses dstdir quota=1
			Rename(src1, dst1, false, true, false, Options.Rename.None);
			// rename reuses dstdir quota=1
			Rename(src2, dst1, true, true, false, Options.Rename.Overwrite);
			/*
			* Test2: src does not exceed quota and dst has *no* quota to accommodate
			* rename.
			*/
			// dstDir quota = 1 and dst1 already uses it
			CreateFile(src2);
			Rename(src2, dst2, false, false, true, Options.Rename.None);
			/*
			* Test3: src exceeds quota and dst has *no* quota to accommodate rename
			* rename to a destination that does not exist
			*/
			// src1 has no quota to accommodate new rename node
			fs.SetQuota(src1.GetParent(), 1, HdfsConstants.QuotaDontSet);
			Rename(dst1, src1, false, false, true, Options.Rename.None);
			/*
			* Test4: src exceeds quota and dst has *no* quota to accommodate rename
			* rename to a destination that exists and quota freed by deletion of dst
			* is same as quota needed by src.
			*/
			// src1 has no quota to accommodate new rename node
			fs.SetQuota(src1.GetParent(), 100, HdfsConstants.QuotaDontSet);
			CreateFile(src1);
			fs.SetQuota(src1.GetParent(), 1, HdfsConstants.QuotaDontSet);
			Rename(dst1, src1, true, true, false, Options.Rename.Overwrite);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRenameRoot()
		{
			Path src = GetTestRootPath(fc, "test/testRenameRoot/srcdir/src1");
			Path dst = new Path("/");
			CreateFile(src);
			Rename(src, dst, true, false, true, Options.Rename.Overwrite);
			Rename(dst, src, true, false, true, Options.Rename.Overwrite);
		}

		/// <summary>
		/// Perform operations such as setting quota, deletion of files, rename and
		/// ensure system can apply edits log during startup.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestEditsLogOldRename()
		{
			DistributedFileSystem fs = cluster.GetFileSystem();
			Path src1 = GetTestRootPath(fc, "testEditsLogOldRename/srcdir/src1");
			Path dst1 = GetTestRootPath(fc, "testEditsLogOldRename/dstdir/dst1");
			CreateFile(src1);
			fs.Mkdirs(dst1.GetParent());
			CreateFile(dst1);
			// Set quota so that dst1 parent cannot allow under it new files/directories 
			fs.SetQuota(dst1.GetParent(), 2, HdfsConstants.QuotaDontSet);
			// Free up quota for a subsequent rename
			fs.Delete(dst1, true);
			OldRename(src1, dst1, true, false);
			// Restart the cluster and ensure the above operations can be
			// loaded from the edits log
			RestartCluster();
			fs = cluster.GetFileSystem();
			src1 = GetTestRootPath(fc, "testEditsLogOldRename/srcdir/src1");
			dst1 = GetTestRootPath(fc, "testEditsLogOldRename/dstdir/dst1");
			NUnit.Framework.Assert.IsFalse(fs.Exists(src1));
			// ensure src1 is already renamed
			NUnit.Framework.Assert.IsTrue(fs.Exists(dst1));
		}

		// ensure rename dst exists
		/// <summary>
		/// Perform operations such as setting quota, deletion of files, rename and
		/// ensure system can apply edits log during startup.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestEditsLogRename()
		{
			DistributedFileSystem fs = cluster.GetFileSystem();
			Path src1 = GetTestRootPath(fc, "testEditsLogRename/srcdir/src1");
			Path dst1 = GetTestRootPath(fc, "testEditsLogRename/dstdir/dst1");
			CreateFile(src1);
			fs.Mkdirs(dst1.GetParent());
			CreateFile(dst1);
			// Set quota so that dst1 parent cannot allow under it new files/directories 
			fs.SetQuota(dst1.GetParent(), 2, HdfsConstants.QuotaDontSet);
			// Free up quota for a subsequent rename
			fs.Delete(dst1, true);
			Rename(src1, dst1, true, true, false, Options.Rename.Overwrite);
			// Restart the cluster and ensure the above operations can be
			// loaded from the edits log
			RestartCluster();
			fs = cluster.GetFileSystem();
			src1 = GetTestRootPath(fc, "testEditsLogRename/srcdir/src1");
			dst1 = GetTestRootPath(fc, "testEditsLogRename/dstdir/dst1");
			NUnit.Framework.Assert.IsFalse(fs.Exists(src1));
			// ensure src1 is already renamed
			NUnit.Framework.Assert.IsTrue(fs.Exists(dst1));
		}

		// ensure rename dst exists
		[NUnit.Framework.Test]
		public virtual void TestIsValidNameInvalidNames()
		{
			string[] invalidNames = new string[] { "/foo/../bar", "/foo/./bar", "/foo/:/bar", 
				"/foo:bar" };
			foreach (string invalidName in invalidNames)
			{
				NUnit.Framework.Assert.IsFalse(invalidName + " is not valid", fc.GetDefaultFileSystem
					().IsValidName(invalidName));
			}
		}

		/// <exception cref="System.Exception"/>
		private void OldRename(Path src, Path dst, bool renameSucceeds, bool exception)
		{
			DistributedFileSystem fs = cluster.GetFileSystem();
			try
			{
				NUnit.Framework.Assert.AreEqual(renameSucceeds, fs.Rename(src, dst));
			}
			catch (Exception)
			{
				NUnit.Framework.Assert.IsTrue(exception);
			}
			NUnit.Framework.Assert.AreEqual(renameSucceeds, !FileContextTestHelper.Exists(fc, 
				src));
			NUnit.Framework.Assert.AreEqual(renameSucceeds, FileContextTestHelper.Exists(fc, 
				dst));
		}

		/// <exception cref="System.Exception"/>
		private void Rename(Path src, Path dst, bool dstExists, bool renameSucceeds, bool
			 exception, params Options.Rename[] options)
		{
			try
			{
				fc.Rename(src, dst, options);
				NUnit.Framework.Assert.IsTrue(renameSucceeds);
			}
			catch (Exception)
			{
				NUnit.Framework.Assert.IsTrue(exception);
			}
			NUnit.Framework.Assert.AreEqual(renameSucceeds, !FileContextTestHelper.Exists(fc, 
				src));
			NUnit.Framework.Assert.AreEqual((dstExists || renameSucceeds), FileContextTestHelper.Exists
				(fc, dst));
		}

		protected override bool ListCorruptedBlocksSupported()
		{
			return true;
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestCrossFileSystemRename()
		{
			try
			{
				fc.Rename(new Path("hdfs://127.0.0.1/aaa/bbb/Foo"), new Path("file://aaa/bbb/Moo"
					), Options.Rename.Overwrite);
				NUnit.Framework.Assert.Fail("IOexception expected.");
			}
			catch (IOException)
			{
			}
		}
		// okay
	}
}
