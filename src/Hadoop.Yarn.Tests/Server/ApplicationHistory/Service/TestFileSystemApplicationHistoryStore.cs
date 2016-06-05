using System;
using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice.Records;
using Org.Mockito;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice
{
	public class TestFileSystemApplicationHistoryStore : ApplicationHistoryStoreTestUtils
	{
		private static Log Log = LogFactory.GetLog(typeof(TestFileSystemApplicationHistoryStore
			).FullName);

		private FileSystem fs;

		private Path fsWorkingPath;

		/// <exception cref="System.Exception"/>
		[SetUp]
		public virtual void Setup()
		{
			fs = new RawLocalFileSystem();
			InitAndStartStore(fs);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.URISyntaxException"/>
		private void InitAndStartStore(FileSystem fs)
		{
			Configuration conf = new Configuration();
			fs.Initialize(new URI("/"), conf);
			fsWorkingPath = new Path("target", typeof(TestFileSystemApplicationHistoryStore).
				Name);
			fs.Delete(fsWorkingPath, true);
			conf.Set(YarnConfiguration.FsApplicationHistoryStoreUri, fsWorkingPath.ToString()
				);
			store = new _FileSystemApplicationHistoryStore_77(fs);
			store.Init(conf);
			store.Start();
		}

		private sealed class _FileSystemApplicationHistoryStore_77 : FileSystemApplicationHistoryStore
		{
			public _FileSystemApplicationHistoryStore_77(FileSystem fs)
			{
				this.fs = fs;
			}

			protected internal override FileSystem GetFileSystem(Path path, Configuration conf
				)
			{
				return fs;
			}

			private readonly FileSystem fs;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			store.Stop();
			fs.Delete(fsWorkingPath, true);
			fs.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestReadWriteHistoryData()
		{
			Log.Info("Starting testReadWriteHistoryData");
			TestWriteHistoryData(5);
			TestReadHistoryData(5);
		}

		/// <exception cref="System.IO.IOException"/>
		private void TestWriteHistoryData(int num)
		{
			TestWriteHistoryData(num, false, false);
		}

		/// <exception cref="System.IO.IOException"/>
		private void TestWriteHistoryData(int num, bool missingContainer, bool missingApplicationAttempt
			)
		{
			// write application history data
			for (int i = 1; i <= num; ++i)
			{
				ApplicationId appId = ApplicationId.NewInstance(0, i);
				WriteApplicationStartData(appId);
				// write application attempt history data
				for (int j = 1; j <= num; ++j)
				{
					ApplicationAttemptId appAttemptId = ApplicationAttemptId.NewInstance(appId, j);
					WriteApplicationAttemptStartData(appAttemptId);
					if (missingApplicationAttempt && j == num)
					{
						continue;
					}
					// write container history data
					for (int k = 1; k <= num; ++k)
					{
						ContainerId containerId = ContainerId.NewContainerId(appAttemptId, k);
						WriteContainerStartData(containerId);
						if (missingContainer && k == num)
						{
							continue;
						}
						WriteContainerFinishData(containerId);
					}
					WriteApplicationAttemptFinishData(appAttemptId);
				}
				WriteApplicationFinishData(appId);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void TestReadHistoryData(int num)
		{
			TestReadHistoryData(num, false, false);
		}

		/// <exception cref="System.IO.IOException"/>
		private void TestReadHistoryData(int num, bool missingContainer, bool missingApplicationAttempt
			)
		{
			// read application history data
			NUnit.Framework.Assert.AreEqual(num, store.GetAllApplications().Count);
			for (int i = 1; i <= num; ++i)
			{
				ApplicationId appId = ApplicationId.NewInstance(0, i);
				ApplicationHistoryData appData = store.GetApplication(appId);
				NUnit.Framework.Assert.IsNotNull(appData);
				NUnit.Framework.Assert.AreEqual(appId.ToString(), appData.GetApplicationName());
				NUnit.Framework.Assert.AreEqual(appId.ToString(), appData.GetDiagnosticsInfo());
				// read application attempt history data
				NUnit.Framework.Assert.AreEqual(num, store.GetApplicationAttempts(appId).Count);
				for (int j = 1; j <= num; ++j)
				{
					ApplicationAttemptId appAttemptId = ApplicationAttemptId.NewInstance(appId, j);
					ApplicationAttemptHistoryData attemptData = store.GetApplicationAttempt(appAttemptId
						);
					NUnit.Framework.Assert.IsNotNull(attemptData);
					NUnit.Framework.Assert.AreEqual(appAttemptId.ToString(), attemptData.GetHost());
					if (missingApplicationAttempt && j == num)
					{
						NUnit.Framework.Assert.IsNull(attemptData.GetDiagnosticsInfo());
						continue;
					}
					else
					{
						NUnit.Framework.Assert.AreEqual(appAttemptId.ToString(), attemptData.GetDiagnosticsInfo
							());
					}
					// read container history data
					NUnit.Framework.Assert.AreEqual(num, store.GetContainers(appAttemptId).Count);
					for (int k = 1; k <= num; ++k)
					{
						ContainerId containerId = ContainerId.NewContainerId(appAttemptId, k);
						ContainerHistoryData containerData = store.GetContainer(containerId);
						NUnit.Framework.Assert.IsNotNull(containerData);
						NUnit.Framework.Assert.AreEqual(Priority.NewInstance(containerId.GetId()), containerData
							.GetPriority());
						if (missingContainer && k == num)
						{
							NUnit.Framework.Assert.IsNull(containerData.GetDiagnosticsInfo());
						}
						else
						{
							NUnit.Framework.Assert.AreEqual(containerId.ToString(), containerData.GetDiagnosticsInfo
								());
						}
					}
					ContainerHistoryData masterContainer = store.GetAMContainer(appAttemptId);
					NUnit.Framework.Assert.IsNotNull(masterContainer);
					NUnit.Framework.Assert.AreEqual(ContainerId.NewContainerId(appAttemptId, 1), masterContainer
						.GetContainerId());
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestWriteAfterApplicationFinish()
		{
			Log.Info("Starting testWriteAfterApplicationFinish");
			ApplicationId appId = ApplicationId.NewInstance(0, 1);
			WriteApplicationStartData(appId);
			WriteApplicationFinishData(appId);
			// write application attempt history data
			ApplicationAttemptId appAttemptId = ApplicationAttemptId.NewInstance(appId, 1);
			try
			{
				WriteApplicationAttemptStartData(appAttemptId);
				NUnit.Framework.Assert.Fail();
			}
			catch (IOException e)
			{
				NUnit.Framework.Assert.IsTrue(e.Message.Contains("is not opened"));
			}
			try
			{
				WriteApplicationAttemptFinishData(appAttemptId);
				NUnit.Framework.Assert.Fail();
			}
			catch (IOException e)
			{
				NUnit.Framework.Assert.IsTrue(e.Message.Contains("is not opened"));
			}
			// write container history data
			ContainerId containerId = ContainerId.NewContainerId(appAttemptId, 1);
			try
			{
				WriteContainerStartData(containerId);
				NUnit.Framework.Assert.Fail();
			}
			catch (IOException e)
			{
				NUnit.Framework.Assert.IsTrue(e.Message.Contains("is not opened"));
			}
			try
			{
				WriteContainerFinishData(containerId);
				NUnit.Framework.Assert.Fail();
			}
			catch (IOException e)
			{
				NUnit.Framework.Assert.IsTrue(e.Message.Contains("is not opened"));
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestMassiveWriteContainerHistoryData()
		{
			Log.Info("Starting testMassiveWriteContainerHistoryData");
			long mb = 1024 * 1024;
			long usedDiskBefore = fs.GetContentSummary(fsWorkingPath).GetLength() / mb;
			ApplicationId appId = ApplicationId.NewInstance(0, 1);
			WriteApplicationStartData(appId);
			ApplicationAttemptId appAttemptId = ApplicationAttemptId.NewInstance(appId, 1);
			for (int i = 1; i <= 100000; ++i)
			{
				ContainerId containerId = ContainerId.NewContainerId(appAttemptId, i);
				WriteContainerStartData(containerId);
				WriteContainerFinishData(containerId);
			}
			WriteApplicationFinishData(appId);
			long usedDiskAfter = fs.GetContentSummary(fsWorkingPath).GetLength() / mb;
			NUnit.Framework.Assert.IsTrue((usedDiskAfter - usedDiskBefore) < 20);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestMissingContainerHistoryData()
		{
			Log.Info("Starting testMissingContainerHistoryData");
			TestWriteHistoryData(3, true, false);
			TestReadHistoryData(3, true, false);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestMissingApplicationAttemptHistoryData()
		{
			Log.Info("Starting testMissingApplicationAttemptHistoryData");
			TestWriteHistoryData(3, false, true);
			TestReadHistoryData(3, false, true);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestInitExistingWorkingDirectoryInSafeMode()
		{
			Log.Info("Starting testInitExistingWorkingDirectoryInSafeMode");
			TearDown();
			// Setup file system to inject startup conditions
			FileSystem fs = Org.Mockito.Mockito.Spy(new RawLocalFileSystem());
			Org.Mockito.Mockito.DoReturn(true).When(fs).IsDirectory(Matchers.Any<Path>());
			try
			{
				InitAndStartStore(fs);
			}
			catch (Exception e)
			{
				NUnit.Framework.Assert.Fail("Exception should not be thrown: " + e);
			}
			// Make sure that directory creation was not attempted
			Org.Mockito.Mockito.Verify(fs, Org.Mockito.Mockito.Times(1)).IsDirectory(Matchers.Any
				<Path>());
			Org.Mockito.Mockito.Verify(fs, Org.Mockito.Mockito.Times(0)).Mkdirs(Matchers.Any<
				Path>());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestInitNonExistingWorkingDirectoryInSafeMode()
		{
			Log.Info("Starting testInitNonExistingWorkingDirectoryInSafeMode");
			TearDown();
			// Setup file system to inject startup conditions
			FileSystem fs = Org.Mockito.Mockito.Spy(new RawLocalFileSystem());
			Org.Mockito.Mockito.DoReturn(false).When(fs).IsDirectory(Matchers.Any<Path>());
			Org.Mockito.Mockito.DoThrow(new IOException()).When(fs).Mkdirs(Matchers.Any<Path>
				());
			try
			{
				InitAndStartStore(fs);
				NUnit.Framework.Assert.Fail("Exception should have been thrown");
			}
			catch (Exception)
			{
			}
			// Expected failure
			// Make sure that directory creation was attempted
			Org.Mockito.Mockito.Verify(fs, Org.Mockito.Mockito.Times(1)).IsDirectory(Matchers.Any
				<Path>());
			Org.Mockito.Mockito.Verify(fs, Org.Mockito.Mockito.Times(1)).Mkdirs(Matchers.Any<
				Path>());
		}
	}
}
