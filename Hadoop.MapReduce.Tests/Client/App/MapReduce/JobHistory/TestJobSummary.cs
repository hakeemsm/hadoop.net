using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Jobhistory
{
	public class TestJobSummary
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestJobSummary));

		private JobSummary summary = new JobSummary();

		[SetUp]
		public virtual void Before()
		{
			JobId mockJobId = Org.Mockito.Mockito.Mock<JobId>();
			Org.Mockito.Mockito.When(mockJobId.ToString()).ThenReturn("testJobId");
			summary.SetJobId(mockJobId);
			summary.SetJobSubmitTime(2);
			summary.SetJobLaunchTime(3);
			summary.SetFirstMapTaskLaunchTime(4);
			summary.SetFirstReduceTaskLaunchTime(5);
			summary.SetJobFinishTime(6);
			summary.SetNumFinishedMaps(1);
			summary.SetNumFailedMaps(0);
			summary.SetNumFinishedReduces(1);
			summary.SetNumFailedReduces(0);
			summary.SetUser("testUser");
			summary.SetQueue("testQueue");
			summary.SetJobStatus("testJobStatus");
			summary.SetMapSlotSeconds(7);
			summary.SetReduceSlotSeconds(8);
			summary.SetJobName("testName");
		}

		[NUnit.Framework.Test]
		public virtual void TestEscapeJobSummary()
		{
			// verify newlines are escaped
			summary.SetJobName("aa\rbb\ncc\r\ndd");
			string @out = summary.GetJobSummaryString();
			Log.Info("summary: " + @out);
			NUnit.Framework.Assert.IsFalse(@out.Contains("\r"));
			NUnit.Framework.Assert.IsFalse(@out.Contains("\n"));
			NUnit.Framework.Assert.IsTrue(@out.Contains("aa\\rbb\\ncc\\r\\ndd"));
		}
	}
}
