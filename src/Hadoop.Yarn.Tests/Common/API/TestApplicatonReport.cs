using NUnit.Framework;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api
{
	public class TestApplicatonReport
	{
		[NUnit.Framework.Test]
		public virtual void TestApplicationReport()
		{
			long timestamp = Runtime.CurrentTimeMillis();
			ApplicationReport appReport1 = CreateApplicationReport(1, 1, timestamp);
			ApplicationReport appReport2 = CreateApplicationReport(1, 1, timestamp);
			ApplicationReport appReport3 = CreateApplicationReport(1, 1, timestamp);
			NUnit.Framework.Assert.AreEqual(appReport1, appReport2);
			NUnit.Framework.Assert.AreEqual(appReport2, appReport3);
			appReport1.SetApplicationId(null);
			NUnit.Framework.Assert.IsNull(appReport1.GetApplicationId());
			NUnit.Framework.Assert.AreNotSame(appReport1, appReport2);
			appReport2.SetCurrentApplicationAttemptId(null);
			NUnit.Framework.Assert.IsNull(appReport2.GetCurrentApplicationAttemptId());
			NUnit.Framework.Assert.AreNotSame(appReport2, appReport3);
			NUnit.Framework.Assert.IsNull(appReport1.GetAMRMToken());
		}

		protected internal static ApplicationReport CreateApplicationReport(int appIdInt, 
			int appAttemptIdInt, long timestamp)
		{
			ApplicationId appId = ApplicationId.NewInstance(timestamp, appIdInt);
			ApplicationAttemptId appAttemptId = ApplicationAttemptId.NewInstance(appId, appAttemptIdInt
				);
			ApplicationReport appReport = ApplicationReport.NewInstance(appId, appAttemptId, 
				"user", "queue", "appname", "host", 124, null, YarnApplicationState.Finished, "diagnostics"
				, "url", 0, 0, FinalApplicationStatus.Succeeded, null, "N/A", 0.53789f, YarnConfiguration
				.DefaultApplicationType, null);
			return appReport;
		}
	}
}
