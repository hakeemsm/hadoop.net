using System;
using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using NUnit.Framework.Rules;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.V2.Jobhistory;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.HS
{
	public class TestHistoryFileManager
	{
		private static MiniDFSCluster dfsCluster = null;

		private static MiniDFSCluster dfsCluster2 = null;

		private static string coreSitePath;

		[Rule]
		public TestName name = new TestName();

		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void SetUpClass()
		{
			coreSitePath = "." + FilePath.separator + "target" + FilePath.separator + "test-classes"
				 + FilePath.separator + "core-site.xml";
			Configuration conf = new HdfsConfiguration();
			Configuration conf2 = new HdfsConfiguration();
			dfsCluster = new MiniDFSCluster.Builder(conf).Build();
			conf2.Set(MiniDFSCluster.HdfsMinidfsBasedir, conf.Get(MiniDFSCluster.HdfsMinidfsBasedir
				) + "_2");
			dfsCluster2 = new MiniDFSCluster.Builder(conf2).Build();
		}

		/// <exception cref="System.Exception"/>
		[AfterClass]
		public static void CleanUpClass()
		{
			dfsCluster.Shutdown();
			dfsCluster2.Shutdown();
		}

		/// <exception cref="System.Exception"/>
		[TearDown]
		public virtual void CleanTest()
		{
			new FilePath(coreSitePath).Delete();
		}

		private string GetDoneDirNameForTest()
		{
			return "/" + name.GetMethodName();
		}

		private string GetIntermediateDoneDirNameForTest()
		{
			return "/intermediate_" + name.GetMethodName();
		}

		/// <exception cref="System.Exception"/>
		private void TestTryCreateHistoryDirs(Configuration conf, bool expected)
		{
			conf.Set(JHAdminConfig.MrHistoryDoneDir, GetDoneDirNameForTest());
			conf.Set(JHAdminConfig.MrHistoryIntermediateDoneDir, GetIntermediateDoneDirNameForTest
				());
			HistoryFileManager hfm = new HistoryFileManager();
			hfm.conf = conf;
			NUnit.Framework.Assert.AreEqual(expected, hfm.TryCreatingHistoryDirs(false));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCreateDirsWithoutFileSystem()
		{
			Configuration conf = new YarnConfiguration();
			conf.Set(FileSystem.FsDefaultNameKey, "hdfs://localhost:1");
			TestTryCreateHistoryDirs(conf, false);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCreateDirsWithFileSystem()
		{
			dfsCluster.GetFileSystem().SetSafeMode(HdfsConstants.SafeModeAction.SafemodeLeave
				);
			NUnit.Framework.Assert.IsFalse(dfsCluster.GetFileSystem().IsInSafeMode());
			TestTryCreateHistoryDirs(dfsCluster.GetConfiguration(0), true);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCreateDirsWithAdditionalFileSystem()
		{
			dfsCluster.GetFileSystem().SetSafeMode(HdfsConstants.SafeModeAction.SafemodeLeave
				);
			dfsCluster2.GetFileSystem().SetSafeMode(HdfsConstants.SafeModeAction.SafemodeLeave
				);
			NUnit.Framework.Assert.IsFalse(dfsCluster.GetFileSystem().IsInSafeMode());
			NUnit.Framework.Assert.IsFalse(dfsCluster2.GetFileSystem().IsInSafeMode());
			// Set default configuration to the first cluster
			Configuration conf = new Configuration(false);
			conf.Set(CommonConfigurationKeysPublic.FsDefaultNameKey, dfsCluster.GetURI().ToString
				());
			FileOutputStream os = new FileOutputStream(coreSitePath);
			conf.WriteXml(os);
			os.Close();
			TestTryCreateHistoryDirs(dfsCluster2.GetConfiguration(0), true);
			// Directories should be created only in the default file system (dfsCluster)
			NUnit.Framework.Assert.IsTrue(dfsCluster.GetFileSystem().Exists(new Path(GetDoneDirNameForTest
				())));
			NUnit.Framework.Assert.IsTrue(dfsCluster.GetFileSystem().Exists(new Path(GetIntermediateDoneDirNameForTest
				())));
			NUnit.Framework.Assert.IsFalse(dfsCluster2.GetFileSystem().Exists(new Path(GetDoneDirNameForTest
				())));
			NUnit.Framework.Assert.IsFalse(dfsCluster2.GetFileSystem().Exists(new Path(GetIntermediateDoneDirNameForTest
				())));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCreateDirsWithFileSystemInSafeMode()
		{
			dfsCluster.GetFileSystem().SetSafeMode(HdfsConstants.SafeModeAction.SafemodeEnter
				);
			NUnit.Framework.Assert.IsTrue(dfsCluster.GetFileSystem().IsInSafeMode());
			TestTryCreateHistoryDirs(dfsCluster.GetConfiguration(0), false);
		}

		/// <exception cref="System.Exception"/>
		private void TestCreateHistoryDirs(Configuration conf, Clock clock)
		{
			conf.Set(JHAdminConfig.MrHistoryDoneDir, "/" + UUID.RandomUUID());
			conf.Set(JHAdminConfig.MrHistoryIntermediateDoneDir, "/" + UUID.RandomUUID());
			HistoryFileManager hfm = new HistoryFileManager();
			hfm.conf = conf;
			hfm.CreateHistoryDirs(clock, 500, 2000);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCreateDirsWithFileSystemBecomingAvailBeforeTimeout()
		{
			dfsCluster.GetFileSystem().SetSafeMode(HdfsConstants.SafeModeAction.SafemodeEnter
				);
			NUnit.Framework.Assert.IsTrue(dfsCluster.GetFileSystem().IsInSafeMode());
			new _Thread_175().Start();
			TestCreateHistoryDirs(dfsCluster.GetConfiguration(0), new SystemClock());
		}

		private sealed class _Thread_175 : Sharpen.Thread
		{
			public _Thread_175()
			{
			}

			public override void Run()
			{
				try
				{
					Sharpen.Thread.Sleep(500);
					TestHistoryFileManager.dfsCluster.GetFileSystem().SetSafeMode(HdfsConstants.SafeModeAction
						.SafemodeLeave);
					NUnit.Framework.Assert.IsTrue(TestHistoryFileManager.dfsCluster.GetFileSystem().IsInSafeMode
						());
				}
				catch (Exception ex)
				{
					NUnit.Framework.Assert.Fail(ex.ToString());
				}
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestCreateDirsWithFileSystemNotBecomingAvailBeforeTimeout()
		{
			dfsCluster.GetFileSystem().SetSafeMode(HdfsConstants.SafeModeAction.SafemodeEnter
				);
			NUnit.Framework.Assert.IsTrue(dfsCluster.GetFileSystem().IsInSafeMode());
			ControlledClock clock = new ControlledClock(new SystemClock());
			clock.SetTime(1);
			new _Thread_199(clock).Start();
			TestCreateHistoryDirs(dfsCluster.GetConfiguration(0), clock);
		}

		private sealed class _Thread_199 : Sharpen.Thread
		{
			public _Thread_199(ControlledClock clock)
			{
				this.clock = clock;
			}

			public override void Run()
			{
				try
				{
					Sharpen.Thread.Sleep(500);
					clock.SetTime(3000);
				}
				catch (Exception ex)
				{
					NUnit.Framework.Assert.Fail(ex.ToString());
				}
			}

			private readonly ControlledClock clock;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestScanDirectory()
		{
			Path p = new Path("any");
			FileContext fc = Org.Mockito.Mockito.Mock<FileContext>();
			Org.Mockito.Mockito.When(fc.MakeQualified(p)).ThenReturn(p);
			Org.Mockito.Mockito.When(fc.ListStatus(p)).ThenThrow(new FileNotFoundException());
			IList<FileStatus> lfs = HistoryFileManager.ScanDirectory(p, fc, null);
			//primarily, succcess is that an exception was not thrown.  Also nice to
			//check this
			NUnit.Framework.Assert.IsNotNull(lfs);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestHistoryFileInfoSummaryFileNotExist()
		{
			TestHistoryFileManager.HistoryFileManagerTest hmTest = new TestHistoryFileManager.HistoryFileManagerTest
				();
			string job = "job_1410889000000_123456";
			Path summaryFile = new Path(job + ".summary");
			JobIndexInfo jobIndexInfo = new JobIndexInfo();
			jobIndexInfo.SetJobId(TypeConverter.ToYarn(JobID.ForName(job)));
			Configuration conf = dfsCluster.GetConfiguration(0);
			conf.Set(JHAdminConfig.MrHistoryDoneDir, "/" + UUID.RandomUUID());
			conf.Set(JHAdminConfig.MrHistoryIntermediateDoneDir, "/" + UUID.RandomUUID());
			hmTest.ServiceInit(conf);
			HistoryFileManager.HistoryFileInfo info = hmTest.GetHistoryFileInfo(null, null, summaryFile
				, jobIndexInfo, false);
			info.MoveToDone();
			NUnit.Framework.Assert.IsFalse(info.DidMoveFail());
		}

		internal class HistoryFileManagerTest : HistoryFileManager
		{
			public HistoryFileManagerTest()
				: base()
			{
			}

			public virtual HistoryFileManager.HistoryFileInfo GetHistoryFileInfo(Path historyFile
				, Path confFile, Path summaryFile, JobIndexInfo jobIndexInfo, bool isInDone)
			{
				return new HistoryFileManager.HistoryFileInfo(this, historyFile, confFile, summaryFile
					, jobIndexInfo, isInDone);
			}
		}
	}
}
