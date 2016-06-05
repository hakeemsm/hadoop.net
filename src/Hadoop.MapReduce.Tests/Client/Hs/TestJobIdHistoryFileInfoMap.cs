using System.Collections.Generic;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.HS
{
	public class TestJobIdHistoryFileInfoMap
	{
		/// <exception cref="System.Exception"/>
		private bool CheckSize(HistoryFileManager.JobIdHistoryFileInfoMap map, int size)
		{
			for (int i = 0; i < 100; i++)
			{
				if (map.Size() != size)
				{
					Sharpen.Thread.Sleep(20);
				}
				else
				{
					return true;
				}
			}
			return false;
		}

		/// <summary>
		/// Trivial test case that verifies basic functionality of
		/// <see cref="JobIdHistoryFileInfoMap"/>
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestWithSingleElement()
		{
			HistoryFileManager.JobIdHistoryFileInfoMap mapWithSize = new HistoryFileManager.JobIdHistoryFileInfoMap
				();
			JobId jobId = MRBuilderUtils.NewJobId(1, 1, 1);
			HistoryFileManager.HistoryFileInfo fileInfo1 = Org.Mockito.Mockito.Mock<HistoryFileManager.HistoryFileInfo
				>();
			Org.Mockito.Mockito.When(fileInfo1.GetJobId()).ThenReturn(jobId);
			// add it twice
			NUnit.Framework.Assert.AreEqual("Incorrect return on putIfAbsent()", null, mapWithSize
				.PutIfAbsent(jobId, fileInfo1));
			NUnit.Framework.Assert.AreEqual("Incorrect return on putIfAbsent()", fileInfo1, mapWithSize
				.PutIfAbsent(jobId, fileInfo1));
			// check get()
			NUnit.Framework.Assert.AreEqual("Incorrect get()", fileInfo1, mapWithSize.Get(jobId
				));
			NUnit.Framework.Assert.IsTrue("Incorrect size()", CheckSize(mapWithSize, 1));
			// check navigableKeySet()
			NavigableSet<JobId> set = mapWithSize.NavigableKeySet();
			NUnit.Framework.Assert.AreEqual("Incorrect navigableKeySet()", 1, set.Count);
			NUnit.Framework.Assert.IsTrue("Incorrect navigableKeySet()", set.Contains(jobId));
			// check values()
			ICollection<HistoryFileManager.HistoryFileInfo> values = mapWithSize.Values();
			NUnit.Framework.Assert.AreEqual("Incorrect values()", 1, values.Count);
			NUnit.Framework.Assert.IsTrue("Incorrect values()", values.Contains(fileInfo1));
		}
	}
}
