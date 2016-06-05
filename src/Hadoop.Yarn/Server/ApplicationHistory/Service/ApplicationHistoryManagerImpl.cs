using System.Collections.Generic;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice.Records;
using Org.Apache.Hadoop.Yarn.Webapp.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice
{
	public class ApplicationHistoryManagerImpl : AbstractService, ApplicationHistoryManager
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice.ApplicationHistoryManagerImpl
			));

		private const string Unavailable = "N/A";

		private ApplicationHistoryStore historyStore;

		private string serverHttpAddress;

		public ApplicationHistoryManagerImpl()
			: base(typeof(Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice.ApplicationHistoryManagerImpl
				).FullName)
		{
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceInit(Configuration conf)
		{
			Log.Info("ApplicationHistory Init");
			historyStore = CreateApplicationHistoryStore(conf);
			historyStore.Init(conf);
			serverHttpAddress = WebAppUtils.GetHttpSchemePrefix(conf) + WebAppUtils.GetAHSWebAppURLWithoutScheme
				(conf);
			base.ServiceInit(conf);
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStart()
		{
			Log.Info("Starting ApplicationHistory");
			historyStore.Start();
			base.ServiceStart();
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStop()
		{
			Log.Info("Stopping ApplicationHistory");
			historyStore.Stop();
			base.ServiceStop();
		}

		protected internal virtual ApplicationHistoryStore CreateApplicationHistoryStore(
			Configuration conf)
		{
			return ReflectionUtils.NewInstance(conf.GetClass<ApplicationHistoryStore>(YarnConfiguration
				.ApplicationHistoryStore, typeof(FileSystemApplicationHistoryStore)), conf);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual ContainerReport GetAMContainer(ApplicationAttemptId appAttemptId)
		{
			ApplicationReport app = GetApplication(appAttemptId.GetApplicationId());
			return ConvertToContainerReport(historyStore.GetAMContainer(appAttemptId), app ==
				 null ? null : app.GetUser());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual IDictionary<ApplicationId, ApplicationReport> GetApplications(long
			 appsNum)
		{
			IDictionary<ApplicationId, ApplicationHistoryData> histData = historyStore.GetAllApplications
				();
			Dictionary<ApplicationId, ApplicationReport> applicationsReport = new Dictionary<
				ApplicationId, ApplicationReport>();
			foreach (KeyValuePair<ApplicationId, ApplicationHistoryData> entry in histData)
			{
				applicationsReport[entry.Key] = ConvertToApplicationReport(entry.Value);
			}
			return applicationsReport;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual ApplicationReport GetApplication(ApplicationId appId)
		{
			return ConvertToApplicationReport(historyStore.GetApplication(appId));
		}

		/// <exception cref="System.IO.IOException"/>
		private ApplicationReport ConvertToApplicationReport(ApplicationHistoryData appHistory
			)
		{
			ApplicationAttemptId currentApplicationAttemptId = null;
			string trackingUrl = Unavailable;
			string host = Unavailable;
			int rpcPort = -1;
			ApplicationAttemptHistoryData lastAttempt = GetLastAttempt(appHistory.GetApplicationId
				());
			if (lastAttempt != null)
			{
				currentApplicationAttemptId = lastAttempt.GetApplicationAttemptId();
				trackingUrl = lastAttempt.GetTrackingURL();
				host = lastAttempt.GetHost();
				rpcPort = lastAttempt.GetRPCPort();
			}
			return ApplicationReport.NewInstance(appHistory.GetApplicationId(), currentApplicationAttemptId
				, appHistory.GetUser(), appHistory.GetQueue(), appHistory.GetApplicationName(), 
				host, rpcPort, null, appHistory.GetYarnApplicationState(), appHistory.GetDiagnosticsInfo
				(), trackingUrl, appHistory.GetStartTime(), appHistory.GetFinishTime(), appHistory
				.GetFinalApplicationStatus(), null, string.Empty, 100, appHistory.GetApplicationType
				(), null);
		}

		/// <exception cref="System.IO.IOException"/>
		private ApplicationAttemptHistoryData GetLastAttempt(ApplicationId appId)
		{
			IDictionary<ApplicationAttemptId, ApplicationAttemptHistoryData> attempts = historyStore
				.GetApplicationAttempts(appId);
			ApplicationAttemptId prevMaxAttemptId = null;
			foreach (ApplicationAttemptId attemptId in attempts.Keys)
			{
				if (prevMaxAttemptId == null)
				{
					prevMaxAttemptId = attemptId;
				}
				else
				{
					if (prevMaxAttemptId.GetAttemptId() < attemptId.GetAttemptId())
					{
						prevMaxAttemptId = attemptId;
					}
				}
			}
			return attempts[prevMaxAttemptId];
		}

		private ApplicationAttemptReport ConvertToApplicationAttemptReport(ApplicationAttemptHistoryData
			 appAttemptHistory)
		{
			return ApplicationAttemptReport.NewInstance(appAttemptHistory.GetApplicationAttemptId
				(), appAttemptHistory.GetHost(), appAttemptHistory.GetRPCPort(), appAttemptHistory
				.GetTrackingURL(), null, appAttemptHistory.GetDiagnosticsInfo(), appAttemptHistory
				.GetYarnApplicationAttemptState(), appAttemptHistory.GetMasterContainerId());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual ApplicationAttemptReport GetApplicationAttempt(ApplicationAttemptId
			 appAttemptId)
		{
			return ConvertToApplicationAttemptReport(historyStore.GetApplicationAttempt(appAttemptId
				));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual IDictionary<ApplicationAttemptId, ApplicationAttemptReport> GetApplicationAttempts
			(ApplicationId appId)
		{
			IDictionary<ApplicationAttemptId, ApplicationAttemptHistoryData> histData = historyStore
				.GetApplicationAttempts(appId);
			Dictionary<ApplicationAttemptId, ApplicationAttemptReport> applicationAttemptsReport
				 = new Dictionary<ApplicationAttemptId, ApplicationAttemptReport>();
			foreach (KeyValuePair<ApplicationAttemptId, ApplicationAttemptHistoryData> entry in 
				histData)
			{
				applicationAttemptsReport[entry.Key] = ConvertToApplicationAttemptReport(entry.Value
					);
			}
			return applicationAttemptsReport;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual ContainerReport GetContainer(ContainerId containerId)
		{
			ApplicationReport app = GetApplication(containerId.GetApplicationAttemptId().GetApplicationId
				());
			return ConvertToContainerReport(historyStore.GetContainer(containerId), app == null
				 ? null : app.GetUser());
		}

		private ContainerReport ConvertToContainerReport(ContainerHistoryData containerHistory
			, string user)
		{
			// If the container has the aggregated log, add the server root url
			string logUrl = WebAppUtils.GetAggregatedLogURL(serverHttpAddress, containerHistory
				.GetAssignedNode().ToString(), containerHistory.GetContainerId().ToString(), containerHistory
				.GetContainerId().ToString(), user);
			return ContainerReport.NewInstance(containerHistory.GetContainerId(), containerHistory
				.GetAllocatedResource(), containerHistory.GetAssignedNode(), containerHistory.GetPriority
				(), containerHistory.GetStartTime(), containerHistory.GetFinishTime(), containerHistory
				.GetDiagnosticsInfo(), logUrl, containerHistory.GetContainerExitStatus(), containerHistory
				.GetContainerState(), null);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual IDictionary<ContainerId, ContainerReport> GetContainers(ApplicationAttemptId
			 appAttemptId)
		{
			ApplicationReport app = GetApplication(appAttemptId.GetApplicationId());
			IDictionary<ContainerId, ContainerHistoryData> histData = historyStore.GetContainers
				(appAttemptId);
			Dictionary<ContainerId, ContainerReport> containersReport = new Dictionary<ContainerId
				, ContainerReport>();
			foreach (KeyValuePair<ContainerId, ContainerHistoryData> entry in histData)
			{
				containersReport[entry.Key] = ConvertToContainerReport(entry.Value, app == null ? 
					null : app.GetUser());
			}
			return containersReport;
		}

		[InterfaceAudience.Private]
		[VisibleForTesting]
		public virtual ApplicationHistoryStore GetHistoryStore()
		{
			return this.historyStore;
		}
	}
}
