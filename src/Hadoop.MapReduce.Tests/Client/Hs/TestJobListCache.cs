using System.Collections.Generic;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.HS
{
	public class TestJobListCache
	{
		public virtual void TestAddExisting()
		{
			HistoryFileManager.JobListCache cache = new HistoryFileManager.JobListCache(2, 1000
				);
			JobId jobId = MRBuilderUtils.NewJobId(1, 1, 1);
			HistoryFileManager.HistoryFileInfo fileInfo = Org.Mockito.Mockito.Mock<HistoryFileManager.HistoryFileInfo
				>();
			Org.Mockito.Mockito.When(fileInfo.GetJobId()).ThenReturn(jobId);
			cache.AddIfAbsent(fileInfo);
			cache.AddIfAbsent(fileInfo);
			NUnit.Framework.Assert.AreEqual("Incorrect number of cache entries", 1, cache.Values
				().Count);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestEviction()
		{
			int maxSize = 2;
			HistoryFileManager.JobListCache cache = new HistoryFileManager.JobListCache(maxSize
				, 1000);
			JobId jobId1 = MRBuilderUtils.NewJobId(1, 1, 1);
			HistoryFileManager.HistoryFileInfo fileInfo1 = Org.Mockito.Mockito.Mock<HistoryFileManager.HistoryFileInfo
				>();
			Org.Mockito.Mockito.When(fileInfo1.GetJobId()).ThenReturn(jobId1);
			JobId jobId2 = MRBuilderUtils.NewJobId(2, 2, 2);
			HistoryFileManager.HistoryFileInfo fileInfo2 = Org.Mockito.Mockito.Mock<HistoryFileManager.HistoryFileInfo
				>();
			Org.Mockito.Mockito.When(fileInfo2.GetJobId()).ThenReturn(jobId2);
			JobId jobId3 = MRBuilderUtils.NewJobId(3, 3, 3);
			HistoryFileManager.HistoryFileInfo fileInfo3 = Org.Mockito.Mockito.Mock<HistoryFileManager.HistoryFileInfo
				>();
			Org.Mockito.Mockito.When(fileInfo3.GetJobId()).ThenReturn(jobId3);
			cache.AddIfAbsent(fileInfo1);
			cache.AddIfAbsent(fileInfo2);
			cache.AddIfAbsent(fileInfo3);
			ICollection<HistoryFileManager.HistoryFileInfo> values;
			for (int i = 0; i < 9; i++)
			{
				values = cache.Values();
				if (values.Count > maxSize)
				{
					Sharpen.Thread.Sleep(100);
				}
				else
				{
					NUnit.Framework.Assert.IsFalse("fileInfo1 should have been evicted", values.Contains
						(fileInfo1));
					return;
				}
			}
			NUnit.Framework.Assert.Fail("JobListCache didn't delete the extra entry");
		}
	}
}
