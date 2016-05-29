using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Client;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Webproxy
{
	/// <summary>This class abstracts away how ApplicationReports are fetched.</summary>
	public class AppReportFetcher
	{
		internal enum AppReportSource
		{
			Rm,
			Ahs
		}

		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Webproxy.AppReportFetcher
			));

		private readonly Configuration conf;

		private readonly ApplicationClientProtocol applicationsManager;

		private readonly ApplicationHistoryProtocol historyManager;

		private readonly RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory
			(null);

		private bool isAHSEnabled;

		/// <summary>
		/// Create a new Connection to the RM/Application History Server
		/// to fetch Application reports.
		/// </summary>
		/// <param name="conf">the conf to use to know where the RM is.</param>
		public AppReportFetcher(Configuration conf)
		{
			if (conf.GetBoolean(YarnConfiguration.ApplicationHistoryEnabled, YarnConfiguration
				.DefaultApplicationHistoryEnabled))
			{
				isAHSEnabled = true;
			}
			this.conf = conf;
			try
			{
				applicationsManager = ClientRMProxy.CreateRMProxy<ApplicationClientProtocol>(conf
					);
				if (isAHSEnabled)
				{
					historyManager = GetAHSProxy(conf);
				}
				else
				{
					this.historyManager = null;
				}
			}
			catch (IOException e)
			{
				throw new YarnRuntimeException(e);
			}
		}

		/// <summary>
		/// Create a direct connection to RM instead of a remote connection when
		/// the proxy is running as part of the RM.
		/// </summary>
		/// <remarks>
		/// Create a direct connection to RM instead of a remote connection when
		/// the proxy is running as part of the RM. Also create a remote connection to
		/// Application History Server if it is enabled.
		/// </remarks>
		/// <param name="conf">the configuration to use</param>
		/// <param name="applicationsManager">what to use to get the RM reports.</param>
		public AppReportFetcher(Configuration conf, ApplicationClientProtocol applicationsManager
			)
		{
			if (conf.GetBoolean(YarnConfiguration.ApplicationHistoryEnabled, YarnConfiguration
				.DefaultApplicationHistoryEnabled))
			{
				isAHSEnabled = true;
			}
			this.conf = conf;
			this.applicationsManager = applicationsManager;
			if (isAHSEnabled)
			{
				try
				{
					historyManager = GetAHSProxy(conf);
				}
				catch (IOException e)
				{
					throw new YarnRuntimeException(e);
				}
			}
			else
			{
				this.historyManager = null;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual ApplicationHistoryProtocol GetAHSProxy(Configuration configuration
			)
		{
			return AHSProxy.CreateAHSProxy<ApplicationHistoryProtocol>(configuration, configuration
				.GetSocketAddr(YarnConfiguration.TimelineServiceAddress, YarnConfiguration.DefaultTimelineServiceAddress
				, YarnConfiguration.DefaultTimelineServicePort));
		}

		/// <summary>
		/// Get an application report for the specified application id from the RM and
		/// fall back to the Application History Server if not found in RM.
		/// </summary>
		/// <param name="appId">id of the application to get.</param>
		/// <returns>the ApplicationReport for the appId.</returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException">on any error.</exception>
		/// <exception cref="System.IO.IOException"/>
		public virtual AppReportFetcher.FetchedAppReport GetApplicationReport(ApplicationId
			 appId)
		{
			GetApplicationReportRequest request = recordFactory.NewRecordInstance<GetApplicationReportRequest
				>();
			request.SetApplicationId(appId);
			ApplicationReport appReport;
			AppReportFetcher.FetchedAppReport fetchedAppReport;
			try
			{
				appReport = applicationsManager.GetApplicationReport(request).GetApplicationReport
					();
				fetchedAppReport = new AppReportFetcher.FetchedAppReport(appReport, AppReportFetcher.AppReportSource
					.Rm);
			}
			catch (ApplicationNotFoundException e)
			{
				if (!isAHSEnabled)
				{
					// Just throw it as usual if historyService is not enabled.
					throw;
				}
				//Fetch the application report from AHS
				appReport = historyManager.GetApplicationReport(request).GetApplicationReport();
				fetchedAppReport = new AppReportFetcher.FetchedAppReport(appReport, AppReportFetcher.AppReportSource
					.Ahs);
			}
			return fetchedAppReport;
		}

		public virtual void Stop()
		{
			if (this.applicationsManager != null)
			{
				RPC.StopProxy(this.applicationsManager);
			}
			if (this.historyManager != null)
			{
				RPC.StopProxy(this.historyManager);
			}
		}

		internal class FetchedAppReport
		{
			private ApplicationReport appReport;

			private AppReportFetcher.AppReportSource appReportSource;

			public FetchedAppReport(ApplicationReport appReport, AppReportFetcher.AppReportSource
				 appReportSource)
			{
				/*
				* This class creates a bundle of the application report and the source from
				* where the the report was fetched. This allows the WebAppProxyServlet
				* to make decisions for the application report based on the source.
				*/
				this.appReport = appReport;
				this.appReportSource = appReportSource;
			}

			public virtual AppReportFetcher.AppReportSource GetAppReportSource()
			{
				return this.appReportSource;
			}

			public virtual ApplicationReport GetApplicationReport()
			{
				return this.appReport;
			}
		}
	}
}
