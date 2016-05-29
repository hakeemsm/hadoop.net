using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Yarn.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager
{
	public class TestDirectoryCollection
	{
		private static readonly FilePath testDir = new FilePath("target", typeof(TestDirectoryCollection
			).FullName).GetAbsoluteFile();

		private static readonly FilePath testFile = new FilePath(testDir, "testfile");

		private Configuration conf;

		private FileContext localFs;

		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public virtual void SetupForTests()
		{
			conf = new Configuration();
			localFs = FileContext.GetLocalFSFileContext(conf);
			testDir.Mkdirs();
			testFile.CreateNewFile();
		}

		[TearDown]
		public virtual void Teardown()
		{
			FileUtil.FullyDelete(testDir);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestConcurrentAccess()
		{
			// Initialize DirectoryCollection with a file instead of a directory
			string[] dirs = new string[] { testFile.GetPath() };
			DirectoryCollection dc = new DirectoryCollection(dirs, conf.GetFloat(YarnConfiguration
				.NmMaxPerDiskUtilizationPercentage, YarnConfiguration.DefaultNmMaxPerDiskUtilizationPercentage
				));
			// Create an iterator before checkDirs is called to reliable test case
			IList<string> list = dc.GetGoodDirs();
			ListIterator<string> li = list.ListIterator();
			// DiskErrorException will invalidate iterator of non-concurrent
			// collections. ConcurrentModificationException will be thrown upon next
			// use of the iterator.
			NUnit.Framework.Assert.IsTrue("checkDirs did not remove test file from directory list"
				, dc.CheckDirs());
			// Verify no ConcurrentModification is thrown
			li.Next();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestCreateDirectories()
		{
			conf.Set(CommonConfigurationKeys.FsPermissionsUmaskKey, "077");
			string dirA = new FilePath(testDir, "dirA").GetPath();
			string dirB = new FilePath(dirA, "dirB").GetPath();
			string dirC = new FilePath(testDir, "dirC").GetPath();
			Path pathC = new Path(dirC);
			FsPermission permDirC = new FsPermission((short)0x1c8);
			localFs.Mkdir(pathC, null, true);
			localFs.SetPermission(pathC, permDirC);
			string[] dirs = new string[] { dirA, dirB, dirC };
			DirectoryCollection dc = new DirectoryCollection(dirs, conf.GetFloat(YarnConfiguration
				.NmMaxPerDiskUtilizationPercentage, YarnConfiguration.DefaultNmMaxPerDiskUtilizationPercentage
				));
			FsPermission defaultPerm = FsPermission.GetDefault().ApplyUMask(new FsPermission(
				(short)FsPermission.DefaultUmask));
			bool createResult = dc.CreateNonExistentDirs(localFs, defaultPerm);
			NUnit.Framework.Assert.IsTrue(createResult);
			FileStatus status = localFs.GetFileStatus(new Path(dirA));
			NUnit.Framework.Assert.AreEqual("local dir parent not created with proper permissions"
				, defaultPerm, status.GetPermission());
			status = localFs.GetFileStatus(new Path(dirB));
			NUnit.Framework.Assert.AreEqual("local dir not created with proper permissions", 
				defaultPerm, status.GetPermission());
			status = localFs.GetFileStatus(pathC);
			NUnit.Framework.Assert.AreEqual("existing local directory permissions modified", 
				permDirC, status.GetPermission());
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestDiskSpaceUtilizationLimit()
		{
			string dirA = new FilePath(testDir, "dirA").GetPath();
			string[] dirs = new string[] { dirA };
			DirectoryCollection dc = new DirectoryCollection(dirs, 0.0F);
			dc.CheckDirs();
			NUnit.Framework.Assert.AreEqual(0, dc.GetGoodDirs().Count);
			NUnit.Framework.Assert.AreEqual(1, dc.GetFailedDirs().Count);
			NUnit.Framework.Assert.AreEqual(1, dc.GetFullDirs().Count);
			dc = new DirectoryCollection(dirs, 100.0F);
			dc.CheckDirs();
			NUnit.Framework.Assert.AreEqual(1, dc.GetGoodDirs().Count);
			NUnit.Framework.Assert.AreEqual(0, dc.GetFailedDirs().Count);
			NUnit.Framework.Assert.AreEqual(0, dc.GetFullDirs().Count);
			dc = new DirectoryCollection(dirs, testDir.GetTotalSpace() / (1024 * 1024));
			dc.CheckDirs();
			NUnit.Framework.Assert.AreEqual(0, dc.GetGoodDirs().Count);
			NUnit.Framework.Assert.AreEqual(1, dc.GetFailedDirs().Count);
			NUnit.Framework.Assert.AreEqual(1, dc.GetFullDirs().Count);
			dc = new DirectoryCollection(dirs, 100.0F, 0);
			dc.CheckDirs();
			NUnit.Framework.Assert.AreEqual(1, dc.GetGoodDirs().Count);
			NUnit.Framework.Assert.AreEqual(0, dc.GetFailedDirs().Count);
			NUnit.Framework.Assert.AreEqual(0, dc.GetFullDirs().Count);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestDiskLimitsCutoffSetters()
		{
			string[] dirs = new string[] { "dir" };
			DirectoryCollection dc = new DirectoryCollection(dirs, 0.0F, 100);
			float testValue = 57.5F;
			float delta = 0.1F;
			dc.SetDiskUtilizationPercentageCutoff(testValue);
			NUnit.Framework.Assert.AreEqual(testValue, dc.GetDiskUtilizationPercentageCutoff(
				), delta);
			testValue = -57.5F;
			dc.SetDiskUtilizationPercentageCutoff(testValue);
			NUnit.Framework.Assert.AreEqual(0.0F, dc.GetDiskUtilizationPercentageCutoff(), delta
				);
			testValue = 157.5F;
			dc.SetDiskUtilizationPercentageCutoff(testValue);
			NUnit.Framework.Assert.AreEqual(100.0F, dc.GetDiskUtilizationPercentageCutoff(), 
				delta);
			long spaceValue = 57;
			dc.SetDiskUtilizationSpaceCutoff(spaceValue);
			NUnit.Framework.Assert.AreEqual(spaceValue, dc.GetDiskUtilizationSpaceCutoff());
			spaceValue = -57;
			dc.SetDiskUtilizationSpaceCutoff(spaceValue);
			NUnit.Framework.Assert.AreEqual(0, dc.GetDiskUtilizationSpaceCutoff());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFailedDisksBecomingGoodAgain()
		{
			string dirA = new FilePath(testDir, "dirA").GetPath();
			string[] dirs = new string[] { dirA };
			DirectoryCollection dc = new DirectoryCollection(dirs, 0.0F);
			dc.CheckDirs();
			NUnit.Framework.Assert.AreEqual(0, dc.GetGoodDirs().Count);
			NUnit.Framework.Assert.AreEqual(1, dc.GetFailedDirs().Count);
			NUnit.Framework.Assert.AreEqual(1, dc.GetFullDirs().Count);
			dc.SetDiskUtilizationPercentageCutoff(100.0F);
			dc.CheckDirs();
			NUnit.Framework.Assert.AreEqual(1, dc.GetGoodDirs().Count);
			NUnit.Framework.Assert.AreEqual(0, dc.GetFailedDirs().Count);
			NUnit.Framework.Assert.AreEqual(0, dc.GetFullDirs().Count);
			conf.Set(CommonConfigurationKeys.FsPermissionsUmaskKey, "077");
			string dirB = new FilePath(testDir, "dirB").GetPath();
			Path pathB = new Path(dirB);
			FsPermission permDirB = new FsPermission((short)0x100);
			localFs.Mkdir(pathB, null, true);
			localFs.SetPermission(pathB, permDirB);
			string[] dirs2 = new string[] { dirB };
			dc = new DirectoryCollection(dirs2, 100.0F);
			dc.CheckDirs();
			NUnit.Framework.Assert.AreEqual(0, dc.GetGoodDirs().Count);
			NUnit.Framework.Assert.AreEqual(1, dc.GetFailedDirs().Count);
			NUnit.Framework.Assert.AreEqual(0, dc.GetFullDirs().Count);
			permDirB = new FsPermission((short)0x1c0);
			localFs.SetPermission(pathB, permDirB);
			dc.CheckDirs();
			NUnit.Framework.Assert.AreEqual(1, dc.GetGoodDirs().Count);
			NUnit.Framework.Assert.AreEqual(0, dc.GetFailedDirs().Count);
			NUnit.Framework.Assert.AreEqual(0, dc.GetFullDirs().Count);
		}

		[NUnit.Framework.Test]
		public virtual void TestConstructors()
		{
			string[] dirs = new string[] { "dir" };
			float delta = 0.1F;
			DirectoryCollection dc = new DirectoryCollection(dirs);
			NUnit.Framework.Assert.AreEqual(100.0F, dc.GetDiskUtilizationPercentageCutoff(), 
				delta);
			NUnit.Framework.Assert.AreEqual(0, dc.GetDiskUtilizationSpaceCutoff());
			dc = new DirectoryCollection(dirs, 57.5F);
			NUnit.Framework.Assert.AreEqual(57.5F, dc.GetDiskUtilizationPercentageCutoff(), delta
				);
			NUnit.Framework.Assert.AreEqual(0, dc.GetDiskUtilizationSpaceCutoff());
			dc = new DirectoryCollection(dirs, 57);
			NUnit.Framework.Assert.AreEqual(100.0F, dc.GetDiskUtilizationPercentageCutoff(), 
				delta);
			NUnit.Framework.Assert.AreEqual(57, dc.GetDiskUtilizationSpaceCutoff());
			dc = new DirectoryCollection(dirs, 57.5F, 67);
			NUnit.Framework.Assert.AreEqual(57.5F, dc.GetDiskUtilizationPercentageCutoff(), delta
				);
			NUnit.Framework.Assert.AreEqual(67, dc.GetDiskUtilizationSpaceCutoff());
			dc = new DirectoryCollection(dirs, -57.5F, -67);
			NUnit.Framework.Assert.AreEqual(0.0F, dc.GetDiskUtilizationPercentageCutoff(), delta
				);
			NUnit.Framework.Assert.AreEqual(0, dc.GetDiskUtilizationSpaceCutoff());
			dc = new DirectoryCollection(dirs, 157.5F, -67);
			NUnit.Framework.Assert.AreEqual(100.0F, dc.GetDiskUtilizationPercentageCutoff(), 
				delta);
			NUnit.Framework.Assert.AreEqual(0, dc.GetDiskUtilizationSpaceCutoff());
		}
	}
}
