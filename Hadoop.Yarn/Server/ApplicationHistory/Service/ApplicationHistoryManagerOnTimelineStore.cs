using System;
using System.Collections.Generic;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authorize;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Timeline;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Server.Metrics;
using Org.Apache.Hadoop.Yarn.Server.Security;
using Org.Apache.Hadoop.Yarn.Server.Timeline;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Hadoop.Yarn.Webapp.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice
{
	public class ApplicationHistoryManagerOnTimelineStore : AbstractService, ApplicationHistoryManager
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice.ApplicationHistoryManagerOnTimelineStore
			));

		[VisibleForTesting]
		internal const string Unavailable = "N/A";

		private TimelineDataManager timelineDataManager;

		private ApplicationACLsManager aclsManager;

		private string serverHttpAddress;

		private long maxLoadedApplications;

		public ApplicationHistoryManagerOnTimelineStore(TimelineDataManager timelineDataManager
			, ApplicationACLsManager aclsManager)
			: base(typeof(Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice.ApplicationHistoryManagerOnTimelineStore
				).FullName)
		{
			this.timelineDataManager = timelineDataManager;
			this.aclsManager = aclsManager;
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceInit(Configuration conf)
		{
			serverHttpAddress = WebAppUtils.GetHttpSchemePrefix(conf) + WebAppUtils.GetAHSWebAppURLWithoutScheme
				(conf);
			maxLoadedApplications = conf.GetLong(YarnConfiguration.ApplicationHistoryMaxApps, 
				YarnConfiguration.DefaultApplicationHistoryMaxApps);
			base.ServiceInit(conf);
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual ApplicationReport GetApplication(ApplicationId appId)
		{
			return GetApplication(appId, ApplicationHistoryManagerOnTimelineStore.ApplicationReportField
				.All).appReport;
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual IDictionary<ApplicationId, ApplicationReport> GetApplications(long
			 appsNum)
		{
			TimelineEntities entities = timelineDataManager.GetEntities(ApplicationMetricsConstants
				.EntityType, null, null, null, null, null, null, appsNum == long.MaxValue ? this
				.maxLoadedApplications : appsNum, EnumSet.AllOf<TimelineReader.Field>(), UserGroupInformation
				.GetLoginUser());
			IDictionary<ApplicationId, ApplicationReport> apps = new LinkedHashMap<ApplicationId
				, ApplicationReport>();
			if (entities != null && entities.GetEntities() != null)
			{
				foreach (TimelineEntity entity in entities.GetEntities())
				{
					try
					{
						ApplicationHistoryManagerOnTimelineStore.ApplicationReportExt app = GenerateApplicationReport
							(entity, ApplicationHistoryManagerOnTimelineStore.ApplicationReportField.All);
						apps[app.appReport.GetApplicationId()] = app.appReport;
					}
					catch (Exception e)
					{
						Log.Error("Error on generating application report for " + entity.GetEntityId(), e
							);
					}
				}
			}
			return apps;
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual IDictionary<ApplicationAttemptId, ApplicationAttemptReport> GetApplicationAttempts
			(ApplicationId appId)
		{
			ApplicationHistoryManagerOnTimelineStore.ApplicationReportExt app = GetApplication
				(appId, ApplicationHistoryManagerOnTimelineStore.ApplicationReportField.UserAndAcls
				);
			CheckAccess(app);
			TimelineEntities entities = timelineDataManager.GetEntities(AppAttemptMetricsConstants
				.EntityType, new NameValuePair(AppAttemptMetricsConstants.ParentPrimaryFilter, appId
				.ToString()), null, null, null, null, null, long.MaxValue, EnumSet.AllOf<TimelineReader.Field
				>(), UserGroupInformation.GetLoginUser());
			IDictionary<ApplicationAttemptId, ApplicationAttemptReport> appAttempts = new LinkedHashMap
				<ApplicationAttemptId, ApplicationAttemptReport>();
			foreach (TimelineEntity entity in entities.GetEntities())
			{
				ApplicationAttemptReport appAttempt = ConvertToApplicationAttemptReport(entity);
				appAttempts[appAttempt.GetApplicationAttemptId()] = appAttempt;
			}
			return appAttempts;
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual ApplicationAttemptReport GetApplicationAttempt(ApplicationAttemptId
			 appAttemptId)
		{
			return GetApplicationAttempt(appAttemptId, true);
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		private ApplicationAttemptReport GetApplicationAttempt(ApplicationAttemptId appAttemptId
			, bool checkACLs)
		{
			if (checkACLs)
			{
				ApplicationHistoryManagerOnTimelineStore.ApplicationReportExt app = GetApplication
					(appAttemptId.GetApplicationId(), ApplicationHistoryManagerOnTimelineStore.ApplicationReportField
					.UserAndAcls);
				CheckAccess(app);
			}
			TimelineEntity entity = timelineDataManager.GetEntity(AppAttemptMetricsConstants.
				EntityType, appAttemptId.ToString(), EnumSet.AllOf<TimelineReader.Field>(), UserGroupInformation
				.GetLoginUser());
			if (entity == null)
			{
				throw new ApplicationAttemptNotFoundException("The entity for application attempt "
					 + appAttemptId + " doesn't exist in the timeline store");
			}
			else
			{
				return ConvertToApplicationAttemptReport(entity);
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual ContainerReport GetContainer(ContainerId containerId)
		{
			ApplicationHistoryManagerOnTimelineStore.ApplicationReportExt app = GetApplication
				(containerId.GetApplicationAttemptId().GetApplicationId(), ApplicationHistoryManagerOnTimelineStore.ApplicationReportField
				.UserAndAcls);
			CheckAccess(app);
			TimelineEntity entity = timelineDataManager.GetEntity(ContainerMetricsConstants.EntityType
				, containerId.ToString(), EnumSet.AllOf<TimelineReader.Field>(), UserGroupInformation
				.GetLoginUser());
			if (entity == null)
			{
				throw new ContainerNotFoundException("The entity for container " + containerId + 
					" doesn't exist in the timeline store");
			}
			else
			{
				return ConvertToContainerReport(entity, serverHttpAddress, app.appReport.GetUser(
					));
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual ContainerReport GetAMContainer(ApplicationAttemptId appAttemptId)
		{
			ApplicationAttemptReport appAttempt = GetApplicationAttempt(appAttemptId, false);
			return GetContainer(appAttempt.GetAMContainerId());
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual IDictionary<ContainerId, ContainerReport> GetContainers(ApplicationAttemptId
			 appAttemptId)
		{
			ApplicationHistoryManagerOnTimelineStore.ApplicationReportExt app = GetApplication
				(appAttemptId.GetApplicationId(), ApplicationHistoryManagerOnTimelineStore.ApplicationReportField
				.UserAndAcls);
			CheckAccess(app);
			TimelineEntities entities = timelineDataManager.GetEntities(ContainerMetricsConstants
				.EntityType, new NameValuePair(ContainerMetricsConstants.ParentPrimariyFilter, appAttemptId
				.ToString()), null, null, null, null, null, long.MaxValue, EnumSet.AllOf<TimelineReader.Field
				>(), UserGroupInformation.GetLoginUser());
			IDictionary<ContainerId, ContainerReport> containers = new LinkedHashMap<ContainerId
				, ContainerReport>();
			if (entities != null && entities.GetEntities() != null)
			{
				foreach (TimelineEntity entity in entities.GetEntities())
				{
					ContainerReport container = ConvertToContainerReport(entity, serverHttpAddress, app
						.appReport.GetUser());
					containers[container.GetContainerId()] = container;
				}
			}
			return containers;
		}

		private static ApplicationHistoryManagerOnTimelineStore.ApplicationReportExt ConvertToApplicationReport
			(TimelineEntity entity, ApplicationHistoryManagerOnTimelineStore.ApplicationReportField
			 field)
		{
			string user = null;
			string queue = null;
			string name = null;
			string type = null;
			long createdTime = 0;
			long finishedTime = 0;
			ApplicationAttemptId latestApplicationAttemptId = null;
			string diagnosticsInfo = null;
			FinalApplicationStatus finalStatus = FinalApplicationStatus.Undefined;
			YarnApplicationState state = null;
			ApplicationResourceUsageReport appResources = null;
			IDictionary<ApplicationAccessType, string> appViewACLs = new Dictionary<ApplicationAccessType
				, string>();
			IDictionary<string, object> entityInfo = entity.GetOtherInfo();
			if (entityInfo != null)
			{
				if (entityInfo.Contains(ApplicationMetricsConstants.UserEntityInfo))
				{
					user = entityInfo[ApplicationMetricsConstants.UserEntityInfo].ToString();
				}
				if (entityInfo.Contains(ApplicationMetricsConstants.AppViewAclsEntityInfo))
				{
					string appViewACLsStr = entityInfo[ApplicationMetricsConstants.AppViewAclsEntityInfo
						].ToString();
					if (appViewACLsStr.Length > 0)
					{
						appViewACLs[ApplicationAccessType.ViewApp] = appViewACLsStr;
					}
				}
				if (field == ApplicationHistoryManagerOnTimelineStore.ApplicationReportField.UserAndAcls)
				{
					return new ApplicationHistoryManagerOnTimelineStore.ApplicationReportExt(ApplicationReport
						.NewInstance(ConverterUtils.ToApplicationId(entity.GetEntityId()), latestApplicationAttemptId
						, user, queue, name, null, -1, null, state, diagnosticsInfo, null, createdTime, 
						finishedTime, finalStatus, null, null, 1.0F, type, null), appViewACLs);
				}
				if (entityInfo.Contains(ApplicationMetricsConstants.QueueEntityInfo))
				{
					queue = entityInfo[ApplicationMetricsConstants.QueueEntityInfo].ToString();
				}
				if (entityInfo.Contains(ApplicationMetricsConstants.NameEntityInfo))
				{
					name = entityInfo[ApplicationMetricsConstants.NameEntityInfo].ToString();
				}
				if (entityInfo.Contains(ApplicationMetricsConstants.TypeEntityInfo))
				{
					type = entityInfo[ApplicationMetricsConstants.TypeEntityInfo].ToString();
				}
				if (entityInfo.Contains(ApplicationMetricsConstants.AppCpuMetrics))
				{
					long vcoreSeconds = long.Parse(entityInfo[ApplicationMetricsConstants.AppCpuMetrics
						].ToString());
					long memorySeconds = long.Parse(entityInfo[ApplicationMetricsConstants.AppMemMetrics
						].ToString());
					appResources = ApplicationResourceUsageReport.NewInstance(0, 0, null, null, null, 
						memorySeconds, vcoreSeconds);
				}
			}
			IList<TimelineEvent> events = entity.GetEvents();
			if (events != null)
			{
				foreach (TimelineEvent @event in events)
				{
					if (@event.GetEventType().Equals(ApplicationMetricsConstants.CreatedEventType))
					{
						createdTime = @event.GetTimestamp();
					}
					else
					{
						if (@event.GetEventType().Equals(ApplicationMetricsConstants.FinishedEventType))
						{
							finishedTime = @event.GetTimestamp();
							IDictionary<string, object> eventInfo = @event.GetEventInfo();
							if (eventInfo == null)
							{
								continue;
							}
							if (eventInfo.Contains(ApplicationMetricsConstants.LatestAppAttemptEventInfo))
							{
								latestApplicationAttemptId = ConverterUtils.ToApplicationAttemptId(eventInfo[ApplicationMetricsConstants
									.LatestAppAttemptEventInfo].ToString());
							}
							if (eventInfo.Contains(ApplicationMetricsConstants.DiagnosticsInfoEventInfo))
							{
								diagnosticsInfo = eventInfo[ApplicationMetricsConstants.DiagnosticsInfoEventInfo]
									.ToString();
							}
							if (eventInfo.Contains(ApplicationMetricsConstants.FinalStatusEventInfo))
							{
								finalStatus = FinalApplicationStatus.ValueOf(eventInfo[ApplicationMetricsConstants
									.FinalStatusEventInfo].ToString());
							}
							if (eventInfo.Contains(ApplicationMetricsConstants.StateEventInfo))
							{
								state = YarnApplicationState.ValueOf(eventInfo[ApplicationMetricsConstants.StateEventInfo
									].ToString());
							}
						}
					}
				}
			}
			return new ApplicationHistoryManagerOnTimelineStore.ApplicationReportExt(ApplicationReport
				.NewInstance(ConverterUtils.ToApplicationId(entity.GetEntityId()), latestApplicationAttemptId
				, user, queue, name, null, -1, null, state, diagnosticsInfo, null, createdTime, 
				finishedTime, finalStatus, appResources, null, 1.0F, type, null), appViewACLs);
		}

		private static ApplicationAttemptReport ConvertToApplicationAttemptReport(TimelineEntity
			 entity)
		{
			string host = null;
			int rpcPort = -1;
			ContainerId amContainerId = null;
			string trackingUrl = null;
			string originalTrackingUrl = null;
			string diagnosticsInfo = null;
			YarnApplicationAttemptState state = null;
			IList<TimelineEvent> events = entity.GetEvents();
			if (events != null)
			{
				foreach (TimelineEvent @event in events)
				{
					if (@event.GetEventType().Equals(AppAttemptMetricsConstants.RegisteredEventType))
					{
						IDictionary<string, object> eventInfo = @event.GetEventInfo();
						if (eventInfo == null)
						{
							continue;
						}
						if (eventInfo.Contains(AppAttemptMetricsConstants.HostEventInfo))
						{
							host = eventInfo[AppAttemptMetricsConstants.HostEventInfo].ToString();
						}
						if (eventInfo.Contains(AppAttemptMetricsConstants.RpcPortEventInfo))
						{
							rpcPort = (int)eventInfo[AppAttemptMetricsConstants.RpcPortEventInfo];
						}
						if (eventInfo.Contains(AppAttemptMetricsConstants.MasterContainerEventInfo))
						{
							amContainerId = ConverterUtils.ToContainerId(eventInfo[AppAttemptMetricsConstants
								.MasterContainerEventInfo].ToString());
						}
					}
					else
					{
						if (@event.GetEventType().Equals(AppAttemptMetricsConstants.FinishedEventType))
						{
							IDictionary<string, object> eventInfo = @event.GetEventInfo();
							if (eventInfo == null)
							{
								continue;
							}
							if (eventInfo.Contains(AppAttemptMetricsConstants.TrackingUrlEventInfo))
							{
								trackingUrl = eventInfo[AppAttemptMetricsConstants.TrackingUrlEventInfo].ToString
									();
							}
							if (eventInfo.Contains(AppAttemptMetricsConstants.OriginalTrackingUrlEventInfo))
							{
								originalTrackingUrl = eventInfo[AppAttemptMetricsConstants.OriginalTrackingUrlEventInfo
									].ToString();
							}
							if (eventInfo.Contains(AppAttemptMetricsConstants.DiagnosticsInfoEventInfo))
							{
								diagnosticsInfo = eventInfo[AppAttemptMetricsConstants.DiagnosticsInfoEventInfo].
									ToString();
							}
							if (eventInfo.Contains(AppAttemptMetricsConstants.StateEventInfo))
							{
								state = YarnApplicationAttemptState.ValueOf(eventInfo[AppAttemptMetricsConstants.
									StateEventInfo].ToString());
							}
						}
					}
				}
			}
			return ApplicationAttemptReport.NewInstance(ConverterUtils.ToApplicationAttemptId
				(entity.GetEntityId()), host, rpcPort, trackingUrl, originalTrackingUrl, diagnosticsInfo
				, state, amContainerId);
		}

		private static ContainerReport ConvertToContainerReport(TimelineEntity entity, string
			 serverHttpAddress, string user)
		{
			int allocatedMem = 0;
			int allocatedVcore = 0;
			string allocatedHost = null;
			int allocatedPort = -1;
			int allocatedPriority = 0;
			long createdTime = 0;
			long finishedTime = 0;
			string diagnosticsInfo = null;
			int exitStatus = ContainerExitStatus.Invalid;
			ContainerState state = null;
			string nodeHttpAddress = null;
			IDictionary<string, object> entityInfo = entity.GetOtherInfo();
			if (entityInfo != null)
			{
				if (entityInfo.Contains(ContainerMetricsConstants.AllocatedMemoryEntityInfo))
				{
					allocatedMem = (int)entityInfo[ContainerMetricsConstants.AllocatedMemoryEntityInfo
						];
				}
				if (entityInfo.Contains(ContainerMetricsConstants.AllocatedVcoreEntityInfo))
				{
					allocatedVcore = (int)entityInfo[ContainerMetricsConstants.AllocatedVcoreEntityInfo
						];
				}
				if (entityInfo.Contains(ContainerMetricsConstants.AllocatedHostEntityInfo))
				{
					allocatedHost = entityInfo[ContainerMetricsConstants.AllocatedHostEntityInfo].ToString
						();
				}
				if (entityInfo.Contains(ContainerMetricsConstants.AllocatedPortEntityInfo))
				{
					allocatedPort = (int)entityInfo[ContainerMetricsConstants.AllocatedPortEntityInfo
						];
				}
				if (entityInfo.Contains(ContainerMetricsConstants.AllocatedPriorityEntityInfo))
				{
					allocatedPriority = (int)entityInfo[ContainerMetricsConstants.AllocatedPriorityEntityInfo
						];
				}
				if (entityInfo.Contains(ContainerMetricsConstants.AllocatedHostHttpAddressEntityInfo
					))
				{
					nodeHttpAddress = (string)entityInfo[ContainerMetricsConstants.AllocatedHostHttpAddressEntityInfo
						];
				}
			}
			IList<TimelineEvent> events = entity.GetEvents();
			if (events != null)
			{
				foreach (TimelineEvent @event in events)
				{
					if (@event.GetEventType().Equals(ContainerMetricsConstants.CreatedEventType))
					{
						createdTime = @event.GetTimestamp();
					}
					else
					{
						if (@event.GetEventType().Equals(ContainerMetricsConstants.FinishedEventType))
						{
							finishedTime = @event.GetTimestamp();
							IDictionary<string, object> eventInfo = @event.GetEventInfo();
							if (eventInfo == null)
							{
								continue;
							}
							if (eventInfo.Contains(ContainerMetricsConstants.DiagnosticsInfoEventInfo))
							{
								diagnosticsInfo = eventInfo[ContainerMetricsConstants.DiagnosticsInfoEventInfo].ToString
									();
							}
							if (eventInfo.Contains(ContainerMetricsConstants.ExitStatusEventInfo))
							{
								exitStatus = (int)eventInfo[ContainerMetricsConstants.ExitStatusEventInfo];
							}
							if (eventInfo.Contains(ContainerMetricsConstants.StateEventInfo))
							{
								state = ContainerState.ValueOf(eventInfo[ContainerMetricsConstants.StateEventInfo
									].ToString());
							}
						}
					}
				}
			}
			NodeId allocatedNode = NodeId.NewInstance(allocatedHost, allocatedPort);
			ContainerId containerId = ConverterUtils.ToContainerId(entity.GetEntityId());
			string logUrl = WebAppUtils.GetAggregatedLogURL(serverHttpAddress, allocatedNode.
				ToString(), containerId.ToString(), containerId.ToString(), user);
			return ContainerReport.NewInstance(ConverterUtils.ToContainerId(entity.GetEntityId
				()), Resource.NewInstance(allocatedMem, allocatedVcore), NodeId.NewInstance(allocatedHost
				, allocatedPort), Priority.NewInstance(allocatedPriority), createdTime, finishedTime
				, diagnosticsInfo, logUrl, exitStatus, state, nodeHttpAddress);
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		private ApplicationHistoryManagerOnTimelineStore.ApplicationReportExt GenerateApplicationReport
			(TimelineEntity entity, ApplicationHistoryManagerOnTimelineStore.ApplicationReportField
			 field)
		{
			ApplicationHistoryManagerOnTimelineStore.ApplicationReportExt app = ConvertToApplicationReport
				(entity, field);
			// If only user and acls are pulled to check attempt(s)/container(s) access
			// control, we can return immediately
			if (field == ApplicationHistoryManagerOnTimelineStore.ApplicationReportField.UserAndAcls)
			{
				return app;
			}
			try
			{
				CheckAccess(app);
				if (app.appReport.GetCurrentApplicationAttemptId() != null)
				{
					ApplicationAttemptReport appAttempt = GetApplicationAttempt(app.appReport.GetCurrentApplicationAttemptId
						(), false);
					app.appReport.SetHost(appAttempt.GetHost());
					app.appReport.SetRpcPort(appAttempt.GetRpcPort());
					app.appReport.SetTrackingUrl(appAttempt.GetTrackingUrl());
					app.appReport.SetOriginalTrackingUrl(appAttempt.GetOriginalTrackingUrl());
				}
			}
			catch (Exception)
			{
				// AuthorizationException is thrown because the user doesn't have access
				// It's possible that the app is finished before the first attempt is created.
				app.appReport.SetDiagnostics(null);
				app.appReport.SetCurrentApplicationAttemptId(null);
			}
			if (app.appReport.GetCurrentApplicationAttemptId() == null)
			{
				app.appReport.SetCurrentApplicationAttemptId(ApplicationAttemptId.NewInstance(app
					.appReport.GetApplicationId(), -1));
			}
			if (app.appReport.GetHost() == null)
			{
				app.appReport.SetHost(Unavailable);
			}
			if (app.appReport.GetRpcPort() < 0)
			{
				app.appReport.SetRpcPort(-1);
			}
			if (app.appReport.GetTrackingUrl() == null)
			{
				app.appReport.SetTrackingUrl(Unavailable);
			}
			if (app.appReport.GetOriginalTrackingUrl() == null)
			{
				app.appReport.SetOriginalTrackingUrl(Unavailable);
			}
			if (app.appReport.GetDiagnostics() == null)
			{
				app.appReport.SetDiagnostics(string.Empty);
			}
			return app;
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		private ApplicationHistoryManagerOnTimelineStore.ApplicationReportExt GetApplication
			(ApplicationId appId, ApplicationHistoryManagerOnTimelineStore.ApplicationReportField
			 field)
		{
			TimelineEntity entity = timelineDataManager.GetEntity(ApplicationMetricsConstants
				.EntityType, appId.ToString(), EnumSet.AllOf<TimelineReader.Field>(), UserGroupInformation
				.GetLoginUser());
			if (entity == null)
			{
				throw new ApplicationNotFoundException("The entity for application " + appId + " doesn't exist in the timeline store"
					);
			}
			else
			{
				return GenerateApplicationReport(entity, field);
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		private void CheckAccess(ApplicationHistoryManagerOnTimelineStore.ApplicationReportExt
			 app)
		{
			if (app.appViewACLs != null)
			{
				aclsManager.AddApplication(app.appReport.GetApplicationId(), app.appViewACLs);
				try
				{
					if (!aclsManager.CheckAccess(UserGroupInformation.GetCurrentUser(), ApplicationAccessType
						.ViewApp, app.appReport.GetUser(), app.appReport.GetApplicationId()))
					{
						throw new AuthorizationException("User " + UserGroupInformation.GetCurrentUser().
							GetShortUserName() + " does not have privilage to see this application " + app.appReport
							.GetApplicationId());
					}
				}
				finally
				{
					aclsManager.RemoveApplication(app.appReport.GetApplicationId());
				}
			}
		}

		private enum ApplicationReportField
		{
			All,
			UserAndAcls
		}

		private class ApplicationReportExt
		{
			private ApplicationReport appReport;

			private IDictionary<ApplicationAccessType, string> appViewACLs;

			public ApplicationReportExt(ApplicationReport appReport, IDictionary<ApplicationAccessType
				, string> appViewACLs)
			{
				// retrieve all the fields
				// retrieve user and ACLs info only
				this.appReport = appReport;
				this.appViewACLs = appViewACLs;
			}
		}
	}
}
