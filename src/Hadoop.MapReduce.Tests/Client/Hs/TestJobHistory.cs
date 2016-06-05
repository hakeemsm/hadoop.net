using System;
using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.HS.Webapp.Dao;
using Org.Apache.Hadoop.Mapreduce.V2.Jobhistory;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.HS
{
	public class TestJobHistory
	{
		internal JobHistory jobHistory = null;

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRefreshLoadedJobCache()
		{
			HistoryFileManager historyManager = Org.Mockito.Mockito.Mock<HistoryFileManager>(
				);
			jobHistory = Org.Mockito.Mockito.Spy(new JobHistory());
			Org.Mockito.Mockito.DoReturn(historyManager).When(jobHistory).CreateHistoryFileManager
				();
			Configuration conf = new Configuration();
			// Set the cache size to 2
			conf.Set(JHAdminConfig.MrHistoryLoadedJobCacheSize, "2");
			jobHistory.Init(conf);
			jobHistory.Start();
			CachedHistoryStorage storage = Org.Mockito.Mockito.Spy((CachedHistoryStorage)jobHistory
				.GetHistoryStorage());
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job[] jobs = new Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job
				[3];
			JobId[] jobIds = new JobId[3];
			for (int i = 0; i < 3; i++)
			{
				jobs[i] = Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job>();
				jobIds[i] = Org.Mockito.Mockito.Mock<JobId>();
				Org.Mockito.Mockito.When(jobs[i].GetID()).ThenReturn(jobIds[i]);
			}
			HistoryFileManager.HistoryFileInfo fileInfo = Org.Mockito.Mockito.Mock<HistoryFileManager.HistoryFileInfo
				>();
			Org.Mockito.Mockito.When(historyManager.GetFileInfo(Any<JobId>())).ThenReturn(fileInfo
				);
			Org.Mockito.Mockito.When(fileInfo.LoadJob()).ThenReturn(jobs[0]).ThenReturn(jobs[
				1]).ThenReturn(jobs[2]);
			// getFullJob will put the job in the cache if it isn't there
			for (int i_1 = 0; i_1 < 3; i_1++)
			{
				storage.GetFullJob(jobs[i_1].GetID());
			}
			IDictionary<JobId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job> jobCache = storage
				.GetLoadedJobCache();
			// job0 should have been purged since cache size is 2
			NUnit.Framework.Assert.IsFalse(jobCache.Contains(jobs[0].GetID()));
			NUnit.Framework.Assert.IsTrue(jobCache.Contains(jobs[1].GetID()) && jobCache.Contains
				(jobs[2].GetID()));
			// Setting cache size to 3
			conf.Set(JHAdminConfig.MrHistoryLoadedJobCacheSize, "3");
			Org.Mockito.Mockito.DoReturn(conf).When(storage).CreateConf();
			Org.Mockito.Mockito.When(fileInfo.LoadJob()).ThenReturn(jobs[0]).ThenReturn(jobs[
				1]).ThenReturn(jobs[2]);
			jobHistory.RefreshLoadedJobCache();
			for (int i_2 = 0; i_2 < 3; i_2++)
			{
				storage.GetFullJob(jobs[i_2].GetID());
			}
			jobCache = storage.GetLoadedJobCache();
			// All three jobs should be in cache since its size is now 3
			for (int i_3 = 0; i_3 < 3; i_3++)
			{
				NUnit.Framework.Assert.IsTrue(jobCache.Contains(jobs[i_3].GetID()));
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRefreshJobRetentionSettings()
		{
			string root = "mockfs://foo/";
			string historyDoneDir = root + "mapred/history/done";
			long now = Runtime.CurrentTimeMillis();
			long someTimeYesterday = now - (25l * 3600 * 1000);
			long timeBefore200Secs = now - (200l * 1000);
			// Get yesterday's date in YY/MM/DD format
			string timestampComponent = JobHistoryUtils.TimestampDirectoryComponent(someTimeYesterday
				);
			// Create a folder under yesterday's done dir
			Path donePathYesterday = new Path(historyDoneDir, timestampComponent + "/" + "000000"
				);
			FileStatus dirCreatedYesterdayStatus = new FileStatus(0, true, 0, 0, someTimeYesterday
				, donePathYesterday);
			// Get today's date in YY/MM/DD format
			timestampComponent = JobHistoryUtils.TimestampDirectoryComponent(timeBefore200Secs
				);
			// Create a folder under today's done dir
			Path donePathToday = new Path(historyDoneDir, timestampComponent + "/" + "000000"
				);
			FileStatus dirCreatedTodayStatus = new FileStatus(0, true, 0, 0, timeBefore200Secs
				, donePathToday);
			// Create a jhist file with yesterday's timestamp under yesterday's done dir
			Path fileUnderYesterdayDir = new Path(donePathYesterday.ToString(), "job_1372363578825_0015-"
				 + someTimeYesterday + "-user-Sleep+job-" + someTimeYesterday + "-1-1-SUCCEEDED-default.jhist"
				);
			FileStatus fileUnderYesterdayDirStatus = new FileStatus(10, false, 0, 0, someTimeYesterday
				, fileUnderYesterdayDir);
			// Create a jhist file with today's timestamp under today's done dir
			Path fileUnderTodayDir = new Path(donePathYesterday.ToString(), "job_1372363578825_0016-"
				 + timeBefore200Secs + "-user-Sleep+job-" + timeBefore200Secs + "-1-1-SUCCEEDED-default.jhist"
				);
			FileStatus fileUnderTodayDirStatus = new FileStatus(10, false, 0, 0, timeBefore200Secs
				, fileUnderTodayDir);
			HistoryFileManager historyManager = Org.Mockito.Mockito.Spy(new HistoryFileManager
				());
			jobHistory = Org.Mockito.Mockito.Spy(new JobHistory());
			IList<FileStatus> fileStatusList = new List<FileStatus>();
			fileStatusList.AddItem(dirCreatedYesterdayStatus);
			fileStatusList.AddItem(dirCreatedTodayStatus);
			// Make the initial delay of history job cleaner as 4 secs
			Org.Mockito.Mockito.DoReturn(4).When(jobHistory).GetInitDelaySecs();
			Org.Mockito.Mockito.DoReturn(historyManager).When(jobHistory).CreateHistoryFileManager
				();
			IList<FileStatus> list1 = new List<FileStatus>();
			list1.AddItem(fileUnderYesterdayDirStatus);
			Org.Mockito.Mockito.DoReturn(list1).When(historyManager).ScanDirectoryForHistoryFiles
				(Eq(donePathYesterday), Any<FileContext>());
			IList<FileStatus> list2 = new List<FileStatus>();
			list2.AddItem(fileUnderTodayDirStatus);
			Org.Mockito.Mockito.DoReturn(list2).When(historyManager).ScanDirectoryForHistoryFiles
				(Eq(donePathToday), Any<FileContext>());
			Org.Mockito.Mockito.DoReturn(fileStatusList).When(historyManager).GetHistoryDirsForCleaning
				(Org.Mockito.Mockito.AnyLong());
			Org.Mockito.Mockito.DoReturn(true).When(historyManager).DeleteDir(Any<FileStatus>
				());
			HistoryFileManager.JobListCache jobListCache = Org.Mockito.Mockito.Mock<HistoryFileManager.JobListCache
				>();
			HistoryFileManager.HistoryFileInfo fileInfo = Org.Mockito.Mockito.Mock<HistoryFileManager.HistoryFileInfo
				>();
			Org.Mockito.Mockito.DoReturn(jobListCache).When(historyManager).CreateJobListCache
				();
			Org.Mockito.Mockito.When(jobListCache.Get(Any<JobId>())).ThenReturn(fileInfo);
			Org.Mockito.Mockito.DoNothing().When(fileInfo).Delete();
			// Set job retention time to 24 hrs and cleaner interval to 2 secs
			Configuration conf = new Configuration();
			conf.SetLong(JHAdminConfig.MrHistoryMaxAgeMs, 24l * 3600 * 1000);
			conf.SetLong(JHAdminConfig.MrHistoryCleanerIntervalMs, 2 * 1000);
			jobHistory.Init(conf);
			jobHistory.Start();
			NUnit.Framework.Assert.AreEqual(2 * 1000l, jobHistory.GetCleanerInterval());
			// Only yesterday's jhist file should get deleted
			Org.Mockito.Mockito.Verify(fileInfo, Org.Mockito.Mockito.Timeout(20000).Times(1))
				.Delete();
			fileStatusList.Remove(dirCreatedYesterdayStatus);
			// Now reset job retention time to 10 secs
			conf.SetLong(JHAdminConfig.MrHistoryMaxAgeMs, 10 * 1000);
			// Set cleaner interval to 1 sec
			conf.SetLong(JHAdminConfig.MrHistoryCleanerIntervalMs, 1 * 1000);
			Org.Mockito.Mockito.DoReturn(conf).When(jobHistory).CreateConf();
			// Do refresh job retention settings
			jobHistory.RefreshJobRetentionSettings();
			// Cleaner interval should be updated
			NUnit.Framework.Assert.AreEqual(1 * 1000l, jobHistory.GetCleanerInterval());
			// Today's jhist file will also be deleted now since it falls below the
			// retention threshold
			Org.Mockito.Mockito.Verify(fileInfo, Org.Mockito.Mockito.Timeout(20000).Times(2))
				.Delete();
		}

		[NUnit.Framework.Test]
		public virtual void TestRefreshLoadedJobCacheUnSupportedOperation()
		{
			jobHistory = Org.Mockito.Mockito.Spy(new JobHistory());
			HistoryStorage storage = new _HistoryStorage_224();
			// TODO Auto-generated method stub
			// TODO Auto-generated method stub
			// TODO Auto-generated method stub
			// TODO Auto-generated method stub
			Org.Mockito.Mockito.DoReturn(storage).When(jobHistory).CreateHistoryStorage();
			jobHistory.Init(new Configuration());
			jobHistory.Start();
			Exception th = null;
			try
			{
				jobHistory.RefreshLoadedJobCache();
			}
			catch (Exception e)
			{
				th = e;
			}
			NUnit.Framework.Assert.IsTrue(th is NotSupportedException);
		}

		private sealed class _HistoryStorage_224 : HistoryStorage
		{
			public _HistoryStorage_224()
			{
			}

			public void SetHistoryFileManager(HistoryFileManager hsManager)
			{
			}

			public JobsInfo GetPartialJobs(long offset, long count, string user, string queue
				, long sBegin, long sEnd, long fBegin, long fEnd, JobState jobState)
			{
				return null;
			}

			public Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job GetFullJob(JobId jobId)
			{
				return null;
			}

			public IDictionary<JobId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job> GetAllPartialJobs
				()
			{
				return null;
			}
		}

		[TearDown]
		public virtual void CleanUp()
		{
			if (jobHistory != null)
			{
				jobHistory.Stop();
			}
		}
	}
}
