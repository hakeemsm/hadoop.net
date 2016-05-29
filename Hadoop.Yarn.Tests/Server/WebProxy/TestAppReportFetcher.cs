using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Webproxy
{
	public class TestAppReportFetcher
	{
		internal static ApplicationHistoryProtocol historyManager;

		internal static Configuration conf = new Configuration();

		private static ApplicationClientProtocol appManager;

		private static AppReportFetcher fetcher;

		private readonly string appNotFoundExceptionMsg = "APP NOT FOUND";

		[TearDown]
		public virtual void CleanUp()
		{
			historyManager = null;
			appManager = null;
			fetcher = null;
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestHelper(bool isAHSEnabled)
		{
			conf.SetBoolean(YarnConfiguration.ApplicationHistoryEnabled, isAHSEnabled);
			appManager = Org.Mockito.Mockito.Mock<ApplicationClientProtocol>();
			Org.Mockito.Mockito.When(appManager.GetApplicationReport(Org.Mockito.Mockito.Any<
				GetApplicationReportRequest>())).ThenThrow(new ApplicationNotFoundException(appNotFoundExceptionMsg
				));
			fetcher = new TestAppReportFetcher.AppReportFetcherForTest(conf, appManager);
			ApplicationId appId = ApplicationId.NewInstance(0, 0);
			fetcher.GetApplicationReport(appId);
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestFetchReportAHSEnabled()
		{
			TestHelper(true);
			Org.Mockito.Mockito.Verify(historyManager, Org.Mockito.Mockito.Times(1)).GetApplicationReport
				(Org.Mockito.Mockito.Any<GetApplicationReportRequest>());
			Org.Mockito.Mockito.Verify(appManager, Org.Mockito.Mockito.Times(1)).GetApplicationReport
				(Org.Mockito.Mockito.Any<GetApplicationReportRequest>());
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestFetchReportAHSDisabled()
		{
			try
			{
				TestHelper(false);
			}
			catch (ApplicationNotFoundException e)
			{
				NUnit.Framework.Assert.IsTrue(e.Message == appNotFoundExceptionMsg);
			}
			/* RM will not know of the app and Application History Service is disabled
			* So we will not try to get the report from AHS and RM will throw
			* ApplicationNotFoundException
			*/
			Org.Mockito.Mockito.Verify(appManager, Org.Mockito.Mockito.Times(1)).GetApplicationReport
				(Org.Mockito.Mockito.Any<GetApplicationReportRequest>());
			if (historyManager != null)
			{
				NUnit.Framework.Assert.Fail("HistoryManager should be null as AHS is disabled");
			}
		}

		internal class AppReportFetcherForTest : AppReportFetcher
		{
			public AppReportFetcherForTest(Configuration conf, ApplicationClientProtocol acp)
				: base(conf, acp)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override ApplicationHistoryProtocol GetAHSProxy(Configuration 
				conf)
			{
				GetApplicationReportResponse resp = Org.Mockito.Mockito.Mock<GetApplicationReportResponse
					>();
				historyManager = Org.Mockito.Mockito.Mock<ApplicationHistoryProtocol>();
				try
				{
					Org.Mockito.Mockito.When(historyManager.GetApplicationReport(Org.Mockito.Mockito.
						Any<GetApplicationReportRequest>())).ThenReturn(resp);
				}
				catch (YarnException e)
				{
					// TODO Auto-generated catch block
					Sharpen.Runtime.PrintStackTrace(e);
				}
				return historyManager;
			}
		}
	}
}
