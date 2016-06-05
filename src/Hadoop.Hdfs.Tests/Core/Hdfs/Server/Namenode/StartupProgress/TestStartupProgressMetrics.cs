using Org.Apache.Hadoop.Metrics2;
using Org.Apache.Hadoop.Test;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.Startupprogress
{
	public class TestStartupProgressMetrics
	{
		private StartupProgress startupProgress;

		private StartupProgressMetrics metrics;

		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			MetricsAsserts.MockMetricsSystem();
			startupProgress = new StartupProgress();
			metrics = new StartupProgressMetrics(startupProgress);
		}

		[NUnit.Framework.Test]
		public virtual void TestInitialState()
		{
			MetricsRecordBuilder builder = MetricsAsserts.GetMetrics(metrics, true);
			MetricsAsserts.AssertCounter("ElapsedTime", 0L, builder);
			MetricsAsserts.AssertGauge("PercentComplete", 0.0f, builder);
			MetricsAsserts.AssertCounter("LoadingFsImageCount", 0L, builder);
			MetricsAsserts.AssertCounter("LoadingFsImageElapsedTime", 0L, builder);
			MetricsAsserts.AssertCounter("LoadingFsImageTotal", 0L, builder);
			MetricsAsserts.AssertGauge("LoadingFsImagePercentComplete", 0.0f, builder);
			MetricsAsserts.AssertCounter("LoadingEditsCount", 0L, builder);
			MetricsAsserts.AssertCounter("LoadingEditsElapsedTime", 0L, builder);
			MetricsAsserts.AssertCounter("LoadingEditsTotal", 0L, builder);
			MetricsAsserts.AssertGauge("LoadingEditsPercentComplete", 0.0f, builder);
			MetricsAsserts.AssertCounter("SavingCheckpointCount", 0L, builder);
			MetricsAsserts.AssertCounter("SavingCheckpointElapsedTime", 0L, builder);
			MetricsAsserts.AssertCounter("SavingCheckpointTotal", 0L, builder);
			MetricsAsserts.AssertGauge("SavingCheckpointPercentComplete", 0.0f, builder);
			MetricsAsserts.AssertCounter("SafeModeCount", 0L, builder);
			MetricsAsserts.AssertCounter("SafeModeElapsedTime", 0L, builder);
			MetricsAsserts.AssertCounter("SafeModeTotal", 0L, builder);
			MetricsAsserts.AssertGauge("SafeModePercentComplete", 0.0f, builder);
		}

		[NUnit.Framework.Test]
		public virtual void TestRunningState()
		{
			StartupProgressTestHelper.SetStartupProgressForRunningState(startupProgress);
			MetricsRecordBuilder builder = MetricsAsserts.GetMetrics(metrics, true);
			NUnit.Framework.Assert.IsTrue(MetricsAsserts.GetLongCounter("ElapsedTime", builder
				) >= 0L);
			MetricsAsserts.AssertGauge("PercentComplete", 0.375f, builder);
			MetricsAsserts.AssertCounter("LoadingFsImageCount", 100L, builder);
			NUnit.Framework.Assert.IsTrue(MetricsAsserts.GetLongCounter("LoadingFsImageElapsedTime"
				, builder) >= 0L);
			MetricsAsserts.AssertCounter("LoadingFsImageTotal", 100L, builder);
			MetricsAsserts.AssertGauge("LoadingFsImagePercentComplete", 1.0f, builder);
			MetricsAsserts.AssertCounter("LoadingEditsCount", 100L, builder);
			NUnit.Framework.Assert.IsTrue(MetricsAsserts.GetLongCounter("LoadingEditsElapsedTime"
				, builder) >= 0L);
			MetricsAsserts.AssertCounter("LoadingEditsTotal", 200L, builder);
			MetricsAsserts.AssertGauge("LoadingEditsPercentComplete", 0.5f, builder);
			MetricsAsserts.AssertCounter("SavingCheckpointCount", 0L, builder);
			MetricsAsserts.AssertCounter("SavingCheckpointElapsedTime", 0L, builder);
			MetricsAsserts.AssertCounter("SavingCheckpointTotal", 0L, builder);
			MetricsAsserts.AssertGauge("SavingCheckpointPercentComplete", 0.0f, builder);
			MetricsAsserts.AssertCounter("SafeModeCount", 0L, builder);
			MetricsAsserts.AssertCounter("SafeModeElapsedTime", 0L, builder);
			MetricsAsserts.AssertCounter("SafeModeTotal", 0L, builder);
			MetricsAsserts.AssertGauge("SafeModePercentComplete", 0.0f, builder);
		}

		[NUnit.Framework.Test]
		public virtual void TestFinalState()
		{
			StartupProgressTestHelper.SetStartupProgressForFinalState(startupProgress);
			MetricsRecordBuilder builder = MetricsAsserts.GetMetrics(metrics, true);
			NUnit.Framework.Assert.IsTrue(MetricsAsserts.GetLongCounter("ElapsedTime", builder
				) >= 0L);
			MetricsAsserts.AssertGauge("PercentComplete", 1.0f, builder);
			MetricsAsserts.AssertCounter("LoadingFsImageCount", 100L, builder);
			NUnit.Framework.Assert.IsTrue(MetricsAsserts.GetLongCounter("LoadingFsImageElapsedTime"
				, builder) >= 0L);
			MetricsAsserts.AssertCounter("LoadingFsImageTotal", 100L, builder);
			MetricsAsserts.AssertGauge("LoadingFsImagePercentComplete", 1.0f, builder);
			MetricsAsserts.AssertCounter("LoadingEditsCount", 200L, builder);
			NUnit.Framework.Assert.IsTrue(MetricsAsserts.GetLongCounter("LoadingEditsElapsedTime"
				, builder) >= 0L);
			MetricsAsserts.AssertCounter("LoadingEditsTotal", 200L, builder);
			MetricsAsserts.AssertGauge("LoadingEditsPercentComplete", 1.0f, builder);
			MetricsAsserts.AssertCounter("SavingCheckpointCount", 300L, builder);
			NUnit.Framework.Assert.IsTrue(MetricsAsserts.GetLongCounter("SavingCheckpointElapsedTime"
				, builder) >= 0L);
			MetricsAsserts.AssertCounter("SavingCheckpointTotal", 300L, builder);
			MetricsAsserts.AssertGauge("SavingCheckpointPercentComplete", 1.0f, builder);
			MetricsAsserts.AssertCounter("SafeModeCount", 400L, builder);
			NUnit.Framework.Assert.IsTrue(MetricsAsserts.GetLongCounter("SafeModeElapsedTime"
				, builder) >= 0L);
			MetricsAsserts.AssertCounter("SafeModeTotal", 400L, builder);
			MetricsAsserts.AssertGauge("SafeModePercentComplete", 1.0f, builder);
		}
	}
}
