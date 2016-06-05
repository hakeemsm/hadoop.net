using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice
{
	public class TestApplicationHistoryManagerImpl : ApplicationHistoryStoreTestUtils
	{
		internal ApplicationHistoryManagerImpl applicationHistoryManagerImpl = null;

		/// <exception cref="System.Exception"/>
		[SetUp]
		public virtual void Setup()
		{
			Configuration config = new Configuration();
			config.SetClass(YarnConfiguration.ApplicationHistoryStore, typeof(MemoryApplicationHistoryStore
				), typeof(ApplicationHistoryStore));
			applicationHistoryManagerImpl = new ApplicationHistoryManagerImpl();
			applicationHistoryManagerImpl.Init(config);
			applicationHistoryManagerImpl.Start();
			store = applicationHistoryManagerImpl.GetHistoryStore();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			applicationHistoryManagerImpl.Stop();
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		[NUnit.Framework.Test]
		public virtual void TestApplicationReport()
		{
			ApplicationId appId = null;
			appId = ApplicationId.NewInstance(0, 1);
			WriteApplicationStartData(appId);
			WriteApplicationFinishData(appId);
			ApplicationAttemptId appAttemptId = ApplicationAttemptId.NewInstance(appId, 1);
			WriteApplicationAttemptStartData(appAttemptId);
			WriteApplicationAttemptFinishData(appAttemptId);
			ApplicationReport appReport = applicationHistoryManagerImpl.GetApplication(appId);
			NUnit.Framework.Assert.IsNotNull(appReport);
			NUnit.Framework.Assert.AreEqual(appId, appReport.GetApplicationId());
			NUnit.Framework.Assert.AreEqual(appAttemptId, appReport.GetCurrentApplicationAttemptId
				());
			NUnit.Framework.Assert.AreEqual(appAttemptId.ToString(), appReport.GetHost());
			NUnit.Framework.Assert.AreEqual("test type", appReport.GetApplicationType().ToString
				());
			NUnit.Framework.Assert.AreEqual("test queue", appReport.GetQueue().ToString());
		}
	}
}
