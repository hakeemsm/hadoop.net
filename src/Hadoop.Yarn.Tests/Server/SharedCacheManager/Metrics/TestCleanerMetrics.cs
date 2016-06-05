using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Sharedcachemanager.Metrics
{
	public class TestCleanerMetrics
	{
		internal Configuration conf = new Configuration();

		internal CleanerMetrics cleanerMetrics;

		[SetUp]
		public virtual void Init()
		{
			cleanerMetrics = CleanerMetrics.GetInstance();
		}

		[NUnit.Framework.Test]
		public virtual void TestMetricsOverMultiplePeriods()
		{
			SimulateACleanerRun();
			AssertMetrics(4, 4, 1, 1);
			SimulateACleanerRun();
			AssertMetrics(4, 8, 1, 2);
		}

		public virtual void SimulateACleanerRun()
		{
			cleanerMetrics.ReportCleaningStart();
			cleanerMetrics.ReportAFileProcess();
			cleanerMetrics.ReportAFileDelete();
			cleanerMetrics.ReportAFileProcess();
			cleanerMetrics.ReportAFileProcess();
		}

		internal virtual void AssertMetrics(int proc, int totalProc, int del, int totalDel
			)
		{
			NUnit.Framework.Assert.AreEqual("Processed files in the last period are not measured correctly"
				, proc, cleanerMetrics.GetProcessedFiles());
			NUnit.Framework.Assert.AreEqual("Total processed files are not measured correctly"
				, totalProc, cleanerMetrics.GetTotalProcessedFiles());
			NUnit.Framework.Assert.AreEqual("Deleted files in the last period are not measured correctly"
				, del, cleanerMetrics.GetDeletedFiles());
			NUnit.Framework.Assert.AreEqual("Total deleted files are not measured correctly", 
				totalDel, cleanerMetrics.GetTotalDeletedFiles());
		}
	}
}
