using System;
using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Logaggregation
{
	public class TestAggregatedLogDeletionService
	{
		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public virtual void CloseFilesystems()
		{
			// prevent the same mockfs instance from being reused due to FS cache
			FileSystem.CloseAll();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDeletion()
		{
			long now = Runtime.CurrentTimeMillis();
			long toDeleteTime = now - (2000 * 1000);
			long toKeepTime = now - (1500 * 1000);
			string root = "mockfs://foo/";
			string remoteRootLogDir = root + "tmp/logs";
			string suffix = "logs";
			Configuration conf = new Configuration();
			conf.SetClass("fs.mockfs.impl", typeof(TestAggregatedLogDeletionService.MockFileSystem
				), typeof(FileSystem));
			conf.Set(YarnConfiguration.LogAggregationEnabled, "true");
			conf.Set(YarnConfiguration.LogAggregationRetainSeconds, "1800");
			conf.Set(YarnConfiguration.NmRemoteAppLogDir, remoteRootLogDir);
			conf.Set(YarnConfiguration.NmRemoteAppLogDirSuffix, suffix);
			Path rootPath = new Path(root);
			FileSystem rootFs = rootPath.GetFileSystem(conf);
			FileSystem mockFs = ((FilterFileSystem)rootFs).GetRawFileSystem();
			Path remoteRootLogPath = new Path(remoteRootLogDir);
			Path userDir = new Path(remoteRootLogPath, "me");
			FileStatus userDirStatus = new FileStatus(0, true, 0, 0, toKeepTime, userDir);
			Org.Mockito.Mockito.When(mockFs.ListStatus(remoteRootLogPath)).ThenReturn(new FileStatus
				[] { userDirStatus });
			ApplicationId appId1 = ApplicationId.NewInstance(Runtime.CurrentTimeMillis(), 1);
			Path userLogDir = new Path(userDir, suffix);
			Path app1Dir = new Path(userLogDir, appId1.ToString());
			FileStatus app1DirStatus = new FileStatus(0, true, 0, 0, toDeleteTime, app1Dir);
			ApplicationId appId2 = ApplicationId.NewInstance(Runtime.CurrentTimeMillis(), 2);
			Path app2Dir = new Path(userLogDir, appId2.ToString());
			FileStatus app2DirStatus = new FileStatus(0, true, 0, 0, toDeleteTime, app2Dir);
			ApplicationId appId3 = ApplicationId.NewInstance(Runtime.CurrentTimeMillis(), 3);
			Path app3Dir = new Path(userLogDir, appId3.ToString());
			FileStatus app3DirStatus = new FileStatus(0, true, 0, 0, toDeleteTime, app3Dir);
			ApplicationId appId4 = ApplicationId.NewInstance(Runtime.CurrentTimeMillis(), 4);
			Path app4Dir = new Path(userLogDir, appId4.ToString());
			FileStatus app4DirStatus = new FileStatus(0, true, 0, 0, toDeleteTime, app4Dir);
			ApplicationId appId5 = ApplicationId.NewInstance(Runtime.CurrentTimeMillis(), 5);
			Path app5Dir = new Path(userLogDir, appId5.ToString());
			FileStatus app5DirStatus = new FileStatus(0, true, 0, 0, toDeleteTime, app5Dir);
			Org.Mockito.Mockito.When(mockFs.ListStatus(userLogDir)).ThenReturn(new FileStatus
				[] { app1DirStatus, app2DirStatus, app3DirStatus, app4DirStatus, app5DirStatus }
				);
			Org.Mockito.Mockito.When(mockFs.ListStatus(app1Dir)).ThenReturn(new FileStatus[] 
				{  });
			Path app2Log1 = new Path(app2Dir, "host1");
			FileStatus app2Log1Status = new FileStatus(10, false, 1, 1, toDeleteTime, app2Log1
				);
			Path app2Log2 = new Path(app2Dir, "host2");
			FileStatus app2Log2Status = new FileStatus(10, false, 1, 1, toKeepTime, app2Log2);
			Org.Mockito.Mockito.When(mockFs.ListStatus(app2Dir)).ThenReturn(new FileStatus[] 
				{ app2Log1Status, app2Log2Status });
			Path app3Log1 = new Path(app3Dir, "host1");
			FileStatus app3Log1Status = new FileStatus(10, false, 1, 1, toDeleteTime, app3Log1
				);
			Path app3Log2 = new Path(app3Dir, "host2");
			FileStatus app3Log2Status = new FileStatus(10, false, 1, 1, toDeleteTime, app3Log2
				);
			Org.Mockito.Mockito.When(mockFs.Delete(app3Dir, true)).ThenThrow(new AccessControlException
				("Injected Error\nStack Trace :("));
			Org.Mockito.Mockito.When(mockFs.ListStatus(app3Dir)).ThenReturn(new FileStatus[] 
				{ app3Log1Status, app3Log2Status });
			Path app4Log1 = new Path(app4Dir, "host1");
			FileStatus app4Log1Status = new FileStatus(10, false, 1, 1, toDeleteTime, app4Log1
				);
			Path app4Log2 = new Path(app4Dir, "host2");
			FileStatus app4Log2Status = new FileStatus(10, false, 1, 1, toDeleteTime, app4Log2
				);
			Org.Mockito.Mockito.When(mockFs.ListStatus(app4Dir)).ThenReturn(new FileStatus[] 
				{ app4Log1Status, app4Log2Status });
			Path app5Log1 = new Path(app5Dir, "host1");
			FileStatus app5Log1Status = new FileStatus(10, false, 1, 1, toDeleteTime, app5Log1
				);
			Path app5Log2 = new Path(app5Dir, "host2");
			FileStatus app5Log2Status = new FileStatus(10, false, 1, 1, toKeepTime, app5Log2);
			Org.Mockito.Mockito.When(mockFs.ListStatus(app5Dir)).ThenReturn(new FileStatus[] 
				{ app5Log1Status, app5Log2Status });
			IList<ApplicationId> finishedApplications = Collections.UnmodifiableList(Arrays.AsList
				(appId1, appId2, appId3, appId4));
			IList<ApplicationId> runningApplications = Sharpen.Collections.UnmodifiableList(Arrays
				.AsList(appId5));
			AggregatedLogDeletionService deletionService = new _AggregatedLogDeletionService_161
				(finishedApplications, runningApplications);
			// DO NOTHING
			deletionService.Init(conf);
			deletionService.Start();
			Org.Mockito.Mockito.Verify(mockFs, Org.Mockito.Mockito.Timeout(2000)).Delete(app1Dir
				, true);
			Org.Mockito.Mockito.Verify(mockFs, Org.Mockito.Mockito.Timeout(2000).Times(0)).Delete
				(app2Dir, true);
			Org.Mockito.Mockito.Verify(mockFs, Org.Mockito.Mockito.Timeout(2000)).Delete(app3Dir
				, true);
			Org.Mockito.Mockito.Verify(mockFs, Org.Mockito.Mockito.Timeout(2000)).Delete(app4Dir
				, true);
			Org.Mockito.Mockito.Verify(mockFs, Org.Mockito.Mockito.Timeout(2000).Times(0)).Delete
				(app5Dir, true);
			Org.Mockito.Mockito.Verify(mockFs, Org.Mockito.Mockito.Timeout(2000)).Delete(app5Log1
				, true);
			Org.Mockito.Mockito.Verify(mockFs, Org.Mockito.Mockito.Timeout(2000).Times(0)).Delete
				(app5Log2, true);
			deletionService.Stop();
		}

		private sealed class _AggregatedLogDeletionService_161 : AggregatedLogDeletionService
		{
			public _AggregatedLogDeletionService_161(IList<ApplicationId> finishedApplications
				, IList<ApplicationId> runningApplications)
			{
				this.finishedApplications = finishedApplications;
				this.runningApplications = runningApplications;
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override ApplicationClientProtocol CreatRMClient()
			{
				try
				{
					return TestAggregatedLogDeletionService.CreateMockRMClient(finishedApplications, 
						runningApplications);
				}
				catch (Exception e)
				{
					throw new IOException(e);
				}
			}

			protected internal override void StopRMClient()
			{
			}

			private readonly IList<ApplicationId> finishedApplications;

			private readonly IList<ApplicationId> runningApplications;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRefreshLogRetentionSettings()
		{
			long now = Runtime.CurrentTimeMillis();
			//time before 2000 sec
			long before2000Secs = now - (2000 * 1000);
			//time before 50 sec
			long before50Secs = now - (50 * 1000);
			string root = "mockfs://foo/";
			string remoteRootLogDir = root + "tmp/logs";
			string suffix = "logs";
			Configuration conf = new Configuration();
			conf.SetClass("fs.mockfs.impl", typeof(TestAggregatedLogDeletionService.MockFileSystem
				), typeof(FileSystem));
			conf.Set(YarnConfiguration.LogAggregationEnabled, "true");
			conf.Set(YarnConfiguration.LogAggregationRetainSeconds, "1800");
			conf.Set(YarnConfiguration.LogAggregationRetainCheckIntervalSeconds, "1");
			conf.Set(YarnConfiguration.NmRemoteAppLogDir, remoteRootLogDir);
			conf.Set(YarnConfiguration.NmRemoteAppLogDirSuffix, suffix);
			Path rootPath = new Path(root);
			FileSystem rootFs = rootPath.GetFileSystem(conf);
			FileSystem mockFs = ((FilterFileSystem)rootFs).GetRawFileSystem();
			Path remoteRootLogPath = new Path(remoteRootLogDir);
			Path userDir = new Path(remoteRootLogPath, "me");
			FileStatus userDirStatus = new FileStatus(0, true, 0, 0, before50Secs, userDir);
			Org.Mockito.Mockito.When(mockFs.ListStatus(remoteRootLogPath)).ThenReturn(new FileStatus
				[] { userDirStatus });
			Path userLogDir = new Path(userDir, suffix);
			ApplicationId appId1 = ApplicationId.NewInstance(Runtime.CurrentTimeMillis(), 1);
			//Set time last modified of app1Dir directory and its files to before2000Secs 
			Path app1Dir = new Path(userLogDir, appId1.ToString());
			FileStatus app1DirStatus = new FileStatus(0, true, 0, 0, before2000Secs, app1Dir);
			ApplicationId appId2 = ApplicationId.NewInstance(Runtime.CurrentTimeMillis(), 2);
			//Set time last modified of app1Dir directory and its files to before50Secs 
			Path app2Dir = new Path(userLogDir, appId2.ToString());
			FileStatus app2DirStatus = new FileStatus(0, true, 0, 0, before50Secs, app2Dir);
			Org.Mockito.Mockito.When(mockFs.ListStatus(userLogDir)).ThenReturn(new FileStatus
				[] { app1DirStatus, app2DirStatus });
			Path app1Log1 = new Path(app1Dir, "host1");
			FileStatus app1Log1Status = new FileStatus(10, false, 1, 1, before2000Secs, app1Log1
				);
			Org.Mockito.Mockito.When(mockFs.ListStatus(app1Dir)).ThenReturn(new FileStatus[] 
				{ app1Log1Status });
			Path app2Log1 = new Path(app2Dir, "host1");
			FileStatus app2Log1Status = new FileStatus(10, false, 1, 1, before50Secs, app2Log1
				);
			Org.Mockito.Mockito.When(mockFs.ListStatus(app2Dir)).ThenReturn(new FileStatus[] 
				{ app2Log1Status });
			IList<ApplicationId> finishedApplications = Sharpen.Collections.UnmodifiableList(
				Arrays.AsList(appId1, appId2));
			AggregatedLogDeletionService deletionSvc = new _AggregatedLogDeletionService_259(
				conf, finishedApplications);
			// DO NOTHING
			deletionSvc.Init(conf);
			deletionSvc.Start();
			//app1Dir would be deleted since its done above log retention period
			Org.Mockito.Mockito.Verify(mockFs, Org.Mockito.Mockito.Timeout(10000)).Delete(app1Dir
				, true);
			//app2Dir is not expected to be deleted since its below the threshold
			Org.Mockito.Mockito.Verify(mockFs, Org.Mockito.Mockito.Timeout(3000).Times(0)).Delete
				(app2Dir, true);
			//Now,lets change the confs
			conf.Set(YarnConfiguration.LogAggregationRetainSeconds, "50");
			conf.Set(YarnConfiguration.LogAggregationRetainCheckIntervalSeconds, "2");
			//We have not called refreshLogSettings,hence don't expect to see the changed conf values
			NUnit.Framework.Assert.IsTrue(2000l != deletionSvc.GetCheckIntervalMsecs());
			//refresh the log settings
			deletionSvc.RefreshLogRetentionSettings();
			//Check interval time should reflect the new value
			NUnit.Framework.Assert.IsTrue(2000l == deletionSvc.GetCheckIntervalMsecs());
			//app2Dir should be deleted since it falls above the threshold
			Org.Mockito.Mockito.Verify(mockFs, Org.Mockito.Mockito.Timeout(10000)).Delete(app2Dir
				, true);
			deletionSvc.Stop();
		}

		private sealed class _AggregatedLogDeletionService_259 : AggregatedLogDeletionService
		{
			public _AggregatedLogDeletionService_259(Configuration conf, IList<ApplicationId>
				 finishedApplications)
			{
				this.conf = conf;
				this.finishedApplications = finishedApplications;
			}

			protected internal override Configuration CreateConf()
			{
				return conf;
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override ApplicationClientProtocol CreatRMClient()
			{
				try
				{
					return TestAggregatedLogDeletionService.CreateMockRMClient(finishedApplications, 
						null);
				}
				catch (Exception e)
				{
					throw new IOException(e);
				}
			}

			protected internal override void StopRMClient()
			{
			}

			private readonly Configuration conf;

			private readonly IList<ApplicationId> finishedApplications;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCheckInterval()
		{
			long RetentionSecs = 10 * 24 * 3600;
			long now = Runtime.CurrentTimeMillis();
			long toDeleteTime = now - RetentionSecs * 1000;
			string root = "mockfs://foo/";
			string remoteRootLogDir = root + "tmp/logs";
			string suffix = "logs";
			Configuration conf = new Configuration();
			conf.SetClass("fs.mockfs.impl", typeof(TestAggregatedLogDeletionService.MockFileSystem
				), typeof(FileSystem));
			conf.Set(YarnConfiguration.LogAggregationEnabled, "true");
			conf.Set(YarnConfiguration.LogAggregationRetainSeconds, "864000");
			conf.Set(YarnConfiguration.LogAggregationRetainCheckIntervalSeconds, "1");
			conf.Set(YarnConfiguration.NmRemoteAppLogDir, remoteRootLogDir);
			conf.Set(YarnConfiguration.NmRemoteAppLogDirSuffix, suffix);
			// prevent us from picking up the same mockfs instance from another test
			FileSystem.CloseAll();
			Path rootPath = new Path(root);
			FileSystem rootFs = rootPath.GetFileSystem(conf);
			FileSystem mockFs = ((FilterFileSystem)rootFs).GetRawFileSystem();
			Path remoteRootLogPath = new Path(remoteRootLogDir);
			Path userDir = new Path(remoteRootLogPath, "me");
			FileStatus userDirStatus = new FileStatus(0, true, 0, 0, now, userDir);
			Org.Mockito.Mockito.When(mockFs.ListStatus(remoteRootLogPath)).ThenReturn(new FileStatus
				[] { userDirStatus });
			ApplicationId appId1 = ApplicationId.NewInstance(Runtime.CurrentTimeMillis(), 1);
			Path userLogDir = new Path(userDir, suffix);
			Path app1Dir = new Path(userLogDir, appId1.ToString());
			FileStatus app1DirStatus = new FileStatus(0, true, 0, 0, now, app1Dir);
			Org.Mockito.Mockito.When(mockFs.ListStatus(userLogDir)).ThenReturn(new FileStatus
				[] { app1DirStatus });
			Path app1Log1 = new Path(app1Dir, "host1");
			FileStatus app1Log1Status = new FileStatus(10, false, 1, 1, now, app1Log1);
			Org.Mockito.Mockito.When(mockFs.ListStatus(app1Dir)).ThenReturn(new FileStatus[] 
				{ app1Log1Status });
			IList<ApplicationId> finishedApplications = Sharpen.Collections.UnmodifiableList(
				Arrays.AsList(appId1));
			AggregatedLogDeletionService deletionSvc = new _AggregatedLogDeletionService_354(
				finishedApplications);
			// DO NOTHING
			deletionSvc.Init(conf);
			deletionSvc.Start();
			Org.Mockito.Mockito.Verify(mockFs, Org.Mockito.Mockito.Timeout(10000).AtLeast(4))
				.ListStatus(Any<Path>());
			Org.Mockito.Mockito.Verify(mockFs, Org.Mockito.Mockito.Never()).Delete(app1Dir, true
				);
			// modify the timestamp of the logs and verify it's picked up quickly
			app1DirStatus = new FileStatus(0, true, 0, 0, toDeleteTime, app1Dir);
			app1Log1Status = new FileStatus(10, false, 1, 1, toDeleteTime, app1Log1);
			Org.Mockito.Mockito.When(mockFs.ListStatus(userLogDir)).ThenReturn(new FileStatus
				[] { app1DirStatus });
			Org.Mockito.Mockito.When(mockFs.ListStatus(app1Dir)).ThenReturn(new FileStatus[] 
				{ app1Log1Status });
			Org.Mockito.Mockito.Verify(mockFs, Org.Mockito.Mockito.Timeout(10000)).Delete(app1Dir
				, true);
			deletionSvc.Stop();
		}

		private sealed class _AggregatedLogDeletionService_354 : AggregatedLogDeletionService
		{
			public _AggregatedLogDeletionService_354(IList<ApplicationId> finishedApplications
				)
			{
				this.finishedApplications = finishedApplications;
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override ApplicationClientProtocol CreatRMClient()
			{
				try
				{
					return TestAggregatedLogDeletionService.CreateMockRMClient(finishedApplications, 
						null);
				}
				catch (Exception e)
				{
					throw new IOException(e);
				}
			}

			protected internal override void StopRMClient()
			{
			}

			private readonly IList<ApplicationId> finishedApplications;
		}

		internal class MockFileSystem : FilterFileSystem
		{
			internal MockFileSystem()
				: base(Org.Mockito.Mockito.Mock<FileSystem>())
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Initialize(URI name, Configuration conf)
			{
			}
		}

		/// <exception cref="System.Exception"/>
		private static ApplicationClientProtocol CreateMockRMClient(IList<ApplicationId> 
			finishedApplicaitons, IList<ApplicationId> runningApplications)
		{
			ApplicationClientProtocol mockProtocol = Org.Mockito.Mockito.Mock<ApplicationClientProtocol
				>();
			if (finishedApplicaitons != null && !finishedApplicaitons.IsEmpty())
			{
				foreach (ApplicationId appId in finishedApplicaitons)
				{
					GetApplicationReportRequest request = GetApplicationReportRequest.NewInstance(appId
						);
					GetApplicationReportResponse response = CreateApplicationReportWithFinishedApplication
						();
					Org.Mockito.Mockito.When(mockProtocol.GetApplicationReport(request)).ThenReturn(response
						);
				}
			}
			if (runningApplications != null && !runningApplications.IsEmpty())
			{
				foreach (ApplicationId appId in runningApplications)
				{
					GetApplicationReportRequest request = GetApplicationReportRequest.NewInstance(appId
						);
					GetApplicationReportResponse response = CreateApplicationReportWithRunningApplication
						();
					Org.Mockito.Mockito.When(mockProtocol.GetApplicationReport(request)).ThenReturn(response
						);
				}
			}
			return mockProtocol;
		}

		private static GetApplicationReportResponse CreateApplicationReportWithRunningApplication
			()
		{
			ApplicationReport report = Org.Mockito.Mockito.Mock<ApplicationReport>();
			Org.Mockito.Mockito.When(report.GetYarnApplicationState()).ThenReturn(YarnApplicationState
				.Running);
			GetApplicationReportResponse response = Org.Mockito.Mockito.Mock<GetApplicationReportResponse
				>();
			Org.Mockito.Mockito.When(response.GetApplicationReport()).ThenReturn(report);
			return response;
		}

		private static GetApplicationReportResponse CreateApplicationReportWithFinishedApplication
			()
		{
			ApplicationReport report = Org.Mockito.Mockito.Mock<ApplicationReport>();
			Org.Mockito.Mockito.When(report.GetYarnApplicationState()).ThenReturn(YarnApplicationState
				.Finished);
			GetApplicationReportResponse response = Org.Mockito.Mockito.Mock<GetApplicationReportResponse
				>();
			Org.Mockito.Mockito.When(response.GetApplicationReport()).ThenReturn(report);
			return response;
		}
	}
}
