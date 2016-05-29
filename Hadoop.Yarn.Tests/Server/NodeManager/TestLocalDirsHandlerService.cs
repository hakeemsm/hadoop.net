using NUnit.Framework;
using Org.Apache.Commons.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager
{
	public class TestLocalDirsHandlerService
	{
		private static readonly FilePath testDir = new FilePath("target", typeof(TestDirectoryCollection
			).FullName).GetAbsoluteFile();

		private static readonly FilePath testFile = new FilePath(testDir, "testfile");

		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public virtual void Setup()
		{
			testDir.Mkdirs();
			testFile.CreateNewFile();
		}

		[TearDown]
		public virtual void Teardown()
		{
			FileUtil.FullyDelete(testDir);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDirStructure()
		{
			Configuration conf = new YarnConfiguration();
			string localDir1 = new FilePath("file:///" + testDir, "localDir1").GetPath();
			conf.Set(YarnConfiguration.NmLocalDirs, localDir1);
			string logDir1 = new FilePath("file:///" + testDir, "logDir1").GetPath();
			conf.Set(YarnConfiguration.NmLogDirs, logDir1);
			LocalDirsHandlerService dirSvc = new LocalDirsHandlerService();
			dirSvc.Init(conf);
			NUnit.Framework.Assert.AreEqual(1, dirSvc.GetLocalDirs().Count);
			dirSvc.Close();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestValidPathsDirHandlerService()
		{
			Configuration conf = new YarnConfiguration();
			string localDir1 = new FilePath("file:///" + testDir, "localDir1").GetPath();
			string localDir2 = new FilePath("hdfs:///" + testDir, "localDir2").GetPath();
			conf.Set(YarnConfiguration.NmLocalDirs, localDir1 + "," + localDir2);
			string logDir1 = new FilePath("file:///" + testDir, "logDir1").GetPath();
			conf.Set(YarnConfiguration.NmLogDirs, logDir1);
			LocalDirsHandlerService dirSvc = new LocalDirsHandlerService();
			try
			{
				dirSvc.Init(conf);
				NUnit.Framework.Assert.Fail("Service should have thrown an exception due to wrong URI"
					);
			}
			catch (YarnRuntimeException)
			{
			}
			NUnit.Framework.Assert.AreEqual("Service should not be inited", Service.STATE.Stopped
				, dirSvc.GetServiceState());
			dirSvc.Close();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetFullDirs()
		{
			Configuration conf = new YarnConfiguration();
			conf.Set(CommonConfigurationKeys.FsPermissionsUmaskKey, "077");
			FileContext localFs = FileContext.GetLocalFSFileContext(conf);
			string localDir1 = new FilePath(testDir, "localDir1").GetPath();
			string localDir2 = new FilePath(testDir, "localDir2").GetPath();
			string logDir1 = new FilePath(testDir, "logDir1").GetPath();
			string logDir2 = new FilePath(testDir, "logDir2").GetPath();
			Path localDir1Path = new Path(localDir1);
			Path logDir1Path = new Path(logDir1);
			FsPermission dirPermissions = new FsPermission((short)0x108);
			localFs.Mkdir(localDir1Path, dirPermissions, true);
			localFs.Mkdir(logDir1Path, dirPermissions, true);
			conf.Set(YarnConfiguration.NmLocalDirs, localDir1 + "," + localDir2);
			conf.Set(YarnConfiguration.NmLogDirs, logDir1 + "," + logDir2);
			conf.SetFloat(YarnConfiguration.NmMaxPerDiskUtilizationPercentage, 0.0f);
			LocalDirsHandlerService dirSvc = new LocalDirsHandlerService();
			dirSvc.Init(conf);
			NUnit.Framework.Assert.AreEqual(0, dirSvc.GetLocalDirs().Count);
			NUnit.Framework.Assert.AreEqual(0, dirSvc.GetLogDirs().Count);
			NUnit.Framework.Assert.AreEqual(1, dirSvc.GetDiskFullLocalDirs().Count);
			NUnit.Framework.Assert.AreEqual(1, dirSvc.GetDiskFullLogDirs().Count);
			FileUtils.DeleteDirectory(new FilePath(localDir1));
			FileUtils.DeleteDirectory(new FilePath(localDir2));
			FileUtils.DeleteDirectory(new FilePath(logDir1));
			FileUtils.DeleteDirectory(new FilePath(logDir1));
			dirSvc.Close();
		}
	}
}
