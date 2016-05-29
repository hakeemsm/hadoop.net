using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using Com.Google.Common.Annotations;
using Com.Google.Common.Util.Concurrent;
using Org.Apache.Commons.Lang.Math;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authorize;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Org.Apache.Hadoop.Yarn.Ipc;
using Org.Apache.Hadoop.Yarn.Security.Client;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Nodelabels;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation.Exceptions;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmcontainer;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmnode;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Security;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Security.Authorize;
using Org.Apache.Hadoop.Yarn.Server.Security;
using Org.Apache.Hadoop.Yarn.Server.Utils;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager
{
	/// <summary>The client interface to the Resource Manager.</summary>
	/// <remarks>
	/// The client interface to the Resource Manager. This module handles all the rpc
	/// interfaces to the resource manager from the client.
	/// </remarks>
	public class ClientRMService : AbstractService, ApplicationClientProtocol
	{
		private static readonly AList<ApplicationReport> EmptyAppsReport = new AList<ApplicationReport
			>();

		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.ClientRMService
			));

		private readonly AtomicInteger applicationCounter = new AtomicInteger(0);

		private readonly YarnScheduler scheduler;

		private readonly RMContext rmContext;

		private readonly RMAppManager rmAppManager;

		private Org.Apache.Hadoop.Ipc.Server server;

		protected internal RMDelegationTokenSecretManager rmDTSecretManager;

		private readonly RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory
			(null);

		internal IPEndPoint clientBindAddress;

		private readonly ApplicationACLsManager applicationsACLsManager;

		private readonly QueueACLsManager queueACLsManager;

		private Clock clock;

		private ReservationSystem reservationSystem;

		private ReservationInputValidator rValidator;

		public ClientRMService(RMContext rmContext, YarnScheduler scheduler, RMAppManager
			 rmAppManager, ApplicationACLsManager applicationACLsManager, QueueACLsManager queueACLsManager
			, RMDelegationTokenSecretManager rmDTSecretManager)
			: this(rmContext, scheduler, rmAppManager, applicationACLsManager, queueACLsManager
				, rmDTSecretManager, new UTCClock())
		{
		}

		public ClientRMService(RMContext rmContext, YarnScheduler scheduler, RMAppManager
			 rmAppManager, ApplicationACLsManager applicationACLsManager, QueueACLsManager queueACLsManager
			, RMDelegationTokenSecretManager rmDTSecretManager, Clock clock)
			: base(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.ClientRMService).FullName
				)
		{
			// For Reservation APIs
			this.scheduler = scheduler;
			this.rmContext = rmContext;
			this.rmAppManager = rmAppManager;
			this.applicationsACLsManager = applicationACLsManager;
			this.queueACLsManager = queueACLsManager;
			this.rmDTSecretManager = rmDTSecretManager;
			this.reservationSystem = rmContext.GetReservationSystem();
			this.clock = clock;
			this.rValidator = new ReservationInputValidator(clock);
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceInit(Configuration conf)
		{
			clientBindAddress = GetBindAddress(conf);
			base.ServiceInit(conf);
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStart()
		{
			Configuration conf = GetConfig();
			YarnRPC rpc = YarnRPC.Create(conf);
			this.server = rpc.GetServer(typeof(ApplicationClientProtocol), this, clientBindAddress
				, conf, this.rmDTSecretManager, conf.GetInt(YarnConfiguration.RmClientThreadCount
				, YarnConfiguration.DefaultRmClientThreadCount));
			// Enable service authorization?
			if (conf.GetBoolean(CommonConfigurationKeysPublic.HadoopSecurityAuthorization, false
				))
			{
				InputStream inputStream = this.rmContext.GetConfigurationProvider().GetConfigurationInputStream
					(conf, YarnConfiguration.HadoopPolicyConfigurationFile);
				if (inputStream != null)
				{
					conf.AddResource(inputStream);
				}
				RefreshServiceAcls(conf, RMPolicyProvider.GetInstance());
			}
			this.server.Start();
			clientBindAddress = conf.UpdateConnectAddr(YarnConfiguration.RmBindHost, YarnConfiguration
				.RmAddress, YarnConfiguration.DefaultRmAddress, server.GetListenerAddress());
			base.ServiceStart();
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStop()
		{
			if (this.server != null)
			{
				this.server.Stop();
			}
			base.ServiceStop();
		}

		internal virtual IPEndPoint GetBindAddress(Configuration conf)
		{
			return conf.GetSocketAddr(YarnConfiguration.RmBindHost, YarnConfiguration.RmAddress
				, YarnConfiguration.DefaultRmAddress, YarnConfiguration.DefaultRmPort);
		}

		[InterfaceAudience.Private]
		public virtual IPEndPoint GetBindAddress()
		{
			return clientBindAddress;
		}

		/// <summary>check if the calling user has the access to application information.</summary>
		/// <param name="callerUGI"/>
		/// <param name="owner"/>
		/// <param name="operationPerformed"/>
		/// <param name="application"/>
		/// <returns/>
		private bool CheckAccess(UserGroupInformation callerUGI, string owner, ApplicationAccessType
			 operationPerformed, RMApp application)
		{
			return applicationsACLsManager.CheckAccess(callerUGI, operationPerformed, owner, 
				application.GetApplicationId()) || queueACLsManager.CheckAccess(callerUGI, QueueACL
				.AdministerQueue, application.GetQueue());
		}

		internal virtual ApplicationId GetNewApplicationId()
		{
			ApplicationId applicationId = BuilderUtils.NewApplicationId(recordFactory, ResourceManager
				.GetClusterTimeStamp(), applicationCounter.IncrementAndGet());
			Log.Info("Allocated new applicationId: " + applicationId.GetId());
			return applicationId;
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		public virtual GetNewApplicationResponse GetNewApplication(GetNewApplicationRequest
			 request)
		{
			GetNewApplicationResponse response = recordFactory.NewRecordInstance<GetNewApplicationResponse
				>();
			response.SetApplicationId(GetNewApplicationId());
			// Pick up min/max resource from scheduler...
			response.SetMaximumResourceCapability(scheduler.GetMaximumResourceCapability());
			return response;
		}

		/// <summary>
		/// It gives response which includes application report if the application
		/// present otherwise throws ApplicationNotFoundException.
		/// </summary>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		public virtual GetApplicationReportResponse GetApplicationReport(GetApplicationReportRequest
			 request)
		{
			ApplicationId applicationId = request.GetApplicationId();
			UserGroupInformation callerUGI;
			try
			{
				callerUGI = UserGroupInformation.GetCurrentUser();
			}
			catch (IOException ie)
			{
				Log.Info("Error getting UGI ", ie);
				throw RPCUtil.GetRemoteException(ie);
			}
			RMApp application = this.rmContext.GetRMApps()[applicationId];
			if (application == null)
			{
				// If the RM doesn't have the application, throw
				// ApplicationNotFoundException and let client to handle.
				throw new ApplicationNotFoundException("Application with id '" + applicationId + 
					"' doesn't exist in RM.");
			}
			bool allowAccess = CheckAccess(callerUGI, application.GetUser(), ApplicationAccessType
				.ViewApp, application);
			ApplicationReport report = application.CreateAndGetApplicationReport(callerUGI.GetUserName
				(), allowAccess);
			GetApplicationReportResponse response = recordFactory.NewRecordInstance<GetApplicationReportResponse
				>();
			response.SetApplicationReport(report);
			return response;
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual GetApplicationAttemptReportResponse GetApplicationAttemptReport(GetApplicationAttemptReportRequest
			 request)
		{
			ApplicationAttemptId appAttemptId = request.GetApplicationAttemptId();
			UserGroupInformation callerUGI;
			try
			{
				callerUGI = UserGroupInformation.GetCurrentUser();
			}
			catch (IOException ie)
			{
				Log.Info("Error getting UGI ", ie);
				throw RPCUtil.GetRemoteException(ie);
			}
			RMApp application = this.rmContext.GetRMApps()[appAttemptId.GetApplicationId()];
			if (application == null)
			{
				// If the RM doesn't have the application, throw
				// ApplicationNotFoundException and let client to handle.
				throw new ApplicationNotFoundException("Application with id '" + request.GetApplicationAttemptId
					().GetApplicationId() + "' doesn't exist in RM.");
			}
			bool allowAccess = CheckAccess(callerUGI, application.GetUser(), ApplicationAccessType
				.ViewApp, application);
			GetApplicationAttemptReportResponse response = null;
			if (allowAccess)
			{
				RMAppAttempt appAttempt = application.GetAppAttempts()[appAttemptId];
				if (appAttempt == null)
				{
					throw new ApplicationAttemptNotFoundException("ApplicationAttempt with id '" + appAttemptId
						 + "' doesn't exist in RM.");
				}
				ApplicationAttemptReport attemptReport = appAttempt.CreateApplicationAttemptReport
					();
				response = GetApplicationAttemptReportResponse.NewInstance(attemptReport);
			}
			else
			{
				throw new YarnException("User " + callerUGI.GetShortUserName() + " does not have privilage to see this attempt "
					 + appAttemptId);
			}
			return response;
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual GetApplicationAttemptsResponse GetApplicationAttempts(GetApplicationAttemptsRequest
			 request)
		{
			ApplicationId appId = request.GetApplicationId();
			UserGroupInformation callerUGI;
			try
			{
				callerUGI = UserGroupInformation.GetCurrentUser();
			}
			catch (IOException ie)
			{
				Log.Info("Error getting UGI ", ie);
				throw RPCUtil.GetRemoteException(ie);
			}
			RMApp application = this.rmContext.GetRMApps()[appId];
			if (application == null)
			{
				// If the RM doesn't have the application, throw
				// ApplicationNotFoundException and let client to handle.
				throw new ApplicationNotFoundException("Application with id '" + appId + "' doesn't exist in RM."
					);
			}
			bool allowAccess = CheckAccess(callerUGI, application.GetUser(), ApplicationAccessType
				.ViewApp, application);
			GetApplicationAttemptsResponse response = null;
			if (allowAccess)
			{
				IDictionary<ApplicationAttemptId, RMAppAttempt> attempts = application.GetAppAttempts
					();
				IList<ApplicationAttemptReport> listAttempts = new AList<ApplicationAttemptReport
					>();
				IEnumerator<KeyValuePair<ApplicationAttemptId, RMAppAttempt>> iter = attempts.GetEnumerator
					();
				while (iter.HasNext())
				{
					listAttempts.AddItem(iter.Next().Value.CreateApplicationAttemptReport());
				}
				response = GetApplicationAttemptsResponse.NewInstance(listAttempts);
			}
			else
			{
				throw new YarnException("User " + callerUGI.GetShortUserName() + " does not have privilage to see this aplication "
					 + appId);
			}
			return response;
		}

		/*
		* (non-Javadoc)
		*
		* we're going to fix the issue of showing non-running containers of the
		* running application in YARN-1794
		*/
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual GetContainerReportResponse GetContainerReport(GetContainerReportRequest
			 request)
		{
			ContainerId containerId = request.GetContainerId();
			ApplicationAttemptId appAttemptId = containerId.GetApplicationAttemptId();
			ApplicationId appId = appAttemptId.GetApplicationId();
			UserGroupInformation callerUGI;
			try
			{
				callerUGI = UserGroupInformation.GetCurrentUser();
			}
			catch (IOException ie)
			{
				Log.Info("Error getting UGI ", ie);
				throw RPCUtil.GetRemoteException(ie);
			}
			RMApp application = this.rmContext.GetRMApps()[appId];
			if (application == null)
			{
				// If the RM doesn't have the application, throw
				// ApplicationNotFoundException and let client to handle.
				throw new ApplicationNotFoundException("Application with id '" + appId + "' doesn't exist in RM."
					);
			}
			bool allowAccess = CheckAccess(callerUGI, application.GetUser(), ApplicationAccessType
				.ViewApp, application);
			GetContainerReportResponse response = null;
			if (allowAccess)
			{
				RMAppAttempt appAttempt = application.GetAppAttempts()[appAttemptId];
				if (appAttempt == null)
				{
					throw new ApplicationAttemptNotFoundException("ApplicationAttempt with id '" + appAttemptId
						 + "' doesn't exist in RM.");
				}
				RMContainer rmConatiner = this.rmContext.GetScheduler().GetRMContainer(containerId
					);
				if (rmConatiner == null)
				{
					throw new ContainerNotFoundException("Container with id '" + containerId + "' doesn't exist in RM."
						);
				}
				response = GetContainerReportResponse.NewInstance(rmConatiner.CreateContainerReport
					());
			}
			else
			{
				throw new YarnException("User " + callerUGI.GetShortUserName() + " does not have privilage to see this aplication "
					 + appId);
			}
			return response;
		}

		/*
		* (non-Javadoc)
		*
		* we're going to fix the issue of showing non-running containers of the
		* running application in YARN-1794"
		*/
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual GetContainersResponse GetContainers(GetContainersRequest request)
		{
			ApplicationAttemptId appAttemptId = request.GetApplicationAttemptId();
			ApplicationId appId = appAttemptId.GetApplicationId();
			UserGroupInformation callerUGI;
			try
			{
				callerUGI = UserGroupInformation.GetCurrentUser();
			}
			catch (IOException ie)
			{
				Log.Info("Error getting UGI ", ie);
				throw RPCUtil.GetRemoteException(ie);
			}
			RMApp application = this.rmContext.GetRMApps()[appId];
			if (application == null)
			{
				// If the RM doesn't have the application, throw
				// ApplicationNotFoundException and let client to handle.
				throw new ApplicationNotFoundException("Application with id '" + appId + "' doesn't exist in RM."
					);
			}
			bool allowAccess = CheckAccess(callerUGI, application.GetUser(), ApplicationAccessType
				.ViewApp, application);
			GetContainersResponse response = null;
			if (allowAccess)
			{
				RMAppAttempt appAttempt = application.GetAppAttempts()[appAttemptId];
				if (appAttempt == null)
				{
					throw new ApplicationAttemptNotFoundException("ApplicationAttempt with id '" + appAttemptId
						 + "' doesn't exist in RM.");
				}
				ICollection<RMContainer> rmContainers = Sharpen.Collections.EmptyList();
				SchedulerAppReport schedulerAppReport = this.rmContext.GetScheduler().GetSchedulerAppInfo
					(appAttemptId);
				if (schedulerAppReport != null)
				{
					rmContainers = schedulerAppReport.GetLiveContainers();
				}
				IList<ContainerReport> listContainers = new AList<ContainerReport>();
				foreach (RMContainer rmContainer in rmContainers)
				{
					listContainers.AddItem(rmContainer.CreateContainerReport());
				}
				response = GetContainersResponse.NewInstance(listContainers);
			}
			else
			{
				throw new YarnException("User " + callerUGI.GetShortUserName() + " does not have privilage to see this aplication "
					 + appId);
			}
			return response;
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		public virtual SubmitApplicationResponse SubmitApplication(SubmitApplicationRequest
			 request)
		{
			ApplicationSubmissionContext submissionContext = request.GetApplicationSubmissionContext
				();
			ApplicationId applicationId = submissionContext.GetApplicationId();
			// ApplicationSubmissionContext needs to be validated for safety - only
			// those fields that are independent of the RM's configuration will be
			// checked here, those that are dependent on RM configuration are validated
			// in RMAppManager.
			string user = null;
			try
			{
				// Safety
				user = UserGroupInformation.GetCurrentUser().GetShortUserName();
			}
			catch (IOException ie)
			{
				Log.Warn("Unable to get the current user.", ie);
				RMAuditLogger.LogFailure(user, RMAuditLogger.AuditConstants.SubmitAppRequest, ie.
					Message, "ClientRMService", "Exception in submitting application", applicationId
					);
				throw RPCUtil.GetRemoteException(ie);
			}
			// Check whether app has already been put into rmContext,
			// If it is, simply return the response
			if (rmContext.GetRMApps()[applicationId] != null)
			{
				Log.Info("This is an earlier submitted application: " + applicationId);
				return SubmitApplicationResponse.NewInstance();
			}
			if (submissionContext.GetQueue() == null)
			{
				submissionContext.SetQueue(YarnConfiguration.DefaultQueueName);
			}
			if (submissionContext.GetApplicationName() == null)
			{
				submissionContext.SetApplicationName(YarnConfiguration.DefaultApplicationName);
			}
			if (submissionContext.GetApplicationType() == null)
			{
				submissionContext.SetApplicationType(YarnConfiguration.DefaultApplicationType);
			}
			else
			{
				if (submissionContext.GetApplicationType().Length > YarnConfiguration.ApplicationTypeLength)
				{
					submissionContext.SetApplicationType(Sharpen.Runtime.Substring(submissionContext.
						GetApplicationType(), 0, YarnConfiguration.ApplicationTypeLength));
				}
			}
			try
			{
				// call RMAppManager to submit application directly
				rmAppManager.SubmitApplication(submissionContext, Runtime.CurrentTimeMillis(), user
					);
				Log.Info("Application with id " + applicationId.GetId() + " submitted by user " +
					 user);
				RMAuditLogger.LogSuccess(user, RMAuditLogger.AuditConstants.SubmitAppRequest, "ClientRMService"
					, applicationId);
			}
			catch (YarnException e)
			{
				Log.Info("Exception in submitting application with id " + applicationId.GetId(), 
					e);
				RMAuditLogger.LogFailure(user, RMAuditLogger.AuditConstants.SubmitAppRequest, e.Message
					, "ClientRMService", "Exception in submitting application", applicationId);
				throw;
			}
			SubmitApplicationResponse response = recordFactory.NewRecordInstance<SubmitApplicationResponse
				>();
			return response;
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		public virtual KillApplicationResponse ForceKillApplication(KillApplicationRequest
			 request)
		{
			ApplicationId applicationId = request.GetApplicationId();
			UserGroupInformation callerUGI;
			try
			{
				callerUGI = UserGroupInformation.GetCurrentUser();
			}
			catch (IOException ie)
			{
				Log.Info("Error getting UGI ", ie);
				RMAuditLogger.LogFailure("UNKNOWN", RMAuditLogger.AuditConstants.KillAppRequest, 
					"UNKNOWN", "ClientRMService", "Error getting UGI", applicationId);
				throw RPCUtil.GetRemoteException(ie);
			}
			RMApp application = this.rmContext.GetRMApps()[applicationId];
			if (application == null)
			{
				RMAuditLogger.LogFailure(callerUGI.GetUserName(), RMAuditLogger.AuditConstants.KillAppRequest
					, "UNKNOWN", "ClientRMService", "Trying to kill an absent application", applicationId
					);
				throw new ApplicationNotFoundException("Trying to kill an absent" + " application "
					 + applicationId);
			}
			if (!CheckAccess(callerUGI, application.GetUser(), ApplicationAccessType.ModifyApp
				, application))
			{
				RMAuditLogger.LogFailure(callerUGI.GetShortUserName(), RMAuditLogger.AuditConstants
					.KillAppRequest, "User doesn't have permissions to " + ApplicationAccessType.ModifyApp
					.ToString(), "ClientRMService", RMAuditLogger.AuditConstants.UnauthorizedUser, applicationId
					);
				throw RPCUtil.GetRemoteException(new AccessControlException("User " + callerUGI.GetShortUserName
					() + " cannot perform operation " + ApplicationAccessType.ModifyApp.ToString() +
					 " on " + applicationId));
			}
			if (application.IsAppFinalStateStored())
			{
				RMAuditLogger.LogSuccess(callerUGI.GetShortUserName(), RMAuditLogger.AuditConstants
					.KillAppRequest, "ClientRMService", applicationId);
				return KillApplicationResponse.NewInstance(true);
			}
			this.rmContext.GetDispatcher().GetEventHandler().Handle(new RMAppEvent(applicationId
				, RMAppEventType.Kill, "Application killed by user."));
			// For UnmanagedAMs, return true so they don't retry
			return KillApplicationResponse.NewInstance(application.GetApplicationSubmissionContext
				().GetUnmanagedAM());
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		public virtual GetClusterMetricsResponse GetClusterMetrics(GetClusterMetricsRequest
			 request)
		{
			GetClusterMetricsResponse response = recordFactory.NewRecordInstance<GetClusterMetricsResponse
				>();
			YarnClusterMetrics ymetrics = recordFactory.NewRecordInstance<YarnClusterMetrics>
				();
			ymetrics.SetNumNodeManagers(this.rmContext.GetRMNodes().Count);
			response.SetClusterMetrics(ymetrics);
			return response;
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		public virtual GetApplicationsResponse GetApplications(GetApplicationsRequest request
			)
		{
			return GetApplications(request, true);
		}

		/// <summary>
		/// Get applications matching the
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Protocolrecords.GetApplicationsRequest"/>
		/// . If
		/// caseSensitive is set to false, applicationTypes in
		/// GetApplicationRequest are expected to be in all-lowercase
		/// </summary>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		[InterfaceAudience.Private]
		public virtual GetApplicationsResponse GetApplications(GetApplicationsRequest request
			, bool caseSensitive)
		{
			UserGroupInformation callerUGI;
			try
			{
				callerUGI = UserGroupInformation.GetCurrentUser();
			}
			catch (IOException ie)
			{
				Log.Info("Error getting UGI ", ie);
				throw RPCUtil.GetRemoteException(ie);
			}
			ICollection<string> applicationTypes = request.GetApplicationTypes();
			EnumSet<YarnApplicationState> applicationStates = request.GetApplicationStates();
			ICollection<string> users = request.GetUsers();
			ICollection<string> queues = request.GetQueues();
			ICollection<string> tags = request.GetApplicationTags();
			long limit = request.GetLimit();
			LongRange start = request.GetStartRange();
			LongRange finish = request.GetFinishRange();
			ApplicationsRequestScope scope = request.GetScope();
			IDictionary<ApplicationId, RMApp> apps = rmContext.GetRMApps();
			IEnumerator<RMApp> appsIter;
			// If the query filters by queues, we can avoid considering apps outside
			// of those queues by asking the scheduler for the apps in those queues.
			if (queues != null && !queues.IsEmpty())
			{
				// Construct an iterator over apps in given queues
				// Collect list of lists to avoid copying all apps
				IList<IList<ApplicationAttemptId>> queueAppLists = new AList<IList<ApplicationAttemptId
					>>();
				foreach (string queue in queues)
				{
					IList<ApplicationAttemptId> appsInQueue = scheduler.GetAppsInQueue(queue);
					if (appsInQueue != null && !appsInQueue.IsEmpty())
					{
						queueAppLists.AddItem(appsInQueue);
					}
				}
				appsIter = new _IEnumerator_716(queueAppLists, apps);
			}
			else
			{
				// Because queueAppLists has no empty lists, hasNext is whether the
				// current list hasNext or whether there are any remaining lists
				appsIter = apps.Values.GetEnumerator();
			}
			IList<ApplicationReport> reports = new AList<ApplicationReport>();
			while (appsIter.HasNext() && reports.Count < limit)
			{
				RMApp application = appsIter.Next();
				// Check if current application falls under the specified scope
				if (scope == ApplicationsRequestScope.Own && !callerUGI.GetUserName().Equals(application
					.GetUser()))
				{
					continue;
				}
				if (applicationTypes != null && !applicationTypes.IsEmpty())
				{
					string appTypeToMatch = caseSensitive ? application.GetApplicationType() : StringUtils
						.ToLowerCase(application.GetApplicationType());
					if (!applicationTypes.Contains(appTypeToMatch))
					{
						continue;
					}
				}
				if (applicationStates != null && !applicationStates.IsEmpty())
				{
					if (!applicationStates.Contains(application.CreateApplicationState()))
					{
						continue;
					}
				}
				if (users != null && !users.IsEmpty() && !users.Contains(application.GetUser()))
				{
					continue;
				}
				if (start != null && !start.ContainsLong(application.GetStartTime()))
				{
					continue;
				}
				if (finish != null && !finish.ContainsLong(application.GetFinishTime()))
				{
					continue;
				}
				if (tags != null && !tags.IsEmpty())
				{
					ICollection<string> appTags = application.GetApplicationTags();
					if (appTags == null || appTags.IsEmpty())
					{
						continue;
					}
					bool match = false;
					foreach (string tag in tags)
					{
						if (appTags.Contains(tag))
						{
							match = true;
							break;
						}
					}
					if (!match)
					{
						continue;
					}
				}
				// checkAccess can grab the scheduler lock so call it last
				bool allowAccess = CheckAccess(callerUGI, application.GetUser(), ApplicationAccessType
					.ViewApp, application);
				if (scope == ApplicationsRequestScope.Viewable && !allowAccess)
				{
					continue;
				}
				reports.AddItem(application.CreateAndGetApplicationReport(callerUGI.GetUserName()
					, allowAccess));
			}
			GetApplicationsResponse response = recordFactory.NewRecordInstance<GetApplicationsResponse
				>();
			response.SetApplicationList(reports);
			return response;
		}

		private sealed class _IEnumerator_716 : IEnumerator<RMApp>
		{
			public _IEnumerator_716(IList<IList<ApplicationAttemptId>> queueAppLists, IDictionary
				<ApplicationId, RMApp> apps)
			{
				this.queueAppLists = queueAppLists;
				this.apps = apps;
				this.appListIter = queueAppLists.GetEnumerator();
			}

			internal IEnumerator<IList<ApplicationAttemptId>> appListIter;

			internal IEnumerator<ApplicationAttemptId> schedAppsIter;

			public override bool HasNext()
			{
				return (this.schedAppsIter != null && this.schedAppsIter.HasNext()) || this.appListIter
					.HasNext();
			}

			public override RMApp Next()
			{
				if (this.schedAppsIter == null || !this.schedAppsIter.HasNext())
				{
					this.schedAppsIter = this.appListIter.Next().GetEnumerator();
				}
				return apps[this.schedAppsIter.Next().GetApplicationId()];
			}

			public override void Remove()
			{
				throw new NotSupportedException("Remove not supported");
			}

			private readonly IList<IList<ApplicationAttemptId>> queueAppLists;

			private readonly IDictionary<ApplicationId, RMApp> apps;
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		public virtual GetClusterNodesResponse GetClusterNodes(GetClusterNodesRequest request
			)
		{
			GetClusterNodesResponse response = recordFactory.NewRecordInstance<GetClusterNodesResponse
				>();
			EnumSet<NodeState> nodeStates = request.GetNodeStates();
			if (nodeStates == null || nodeStates.IsEmpty())
			{
				nodeStates = EnumSet.AllOf<NodeState>();
			}
			ICollection<RMNode> nodes = RMServerUtils.QueryRMNodes(rmContext, nodeStates);
			IList<NodeReport> nodeReports = new AList<NodeReport>(nodes.Count);
			foreach (RMNode nodeInfo in nodes)
			{
				nodeReports.AddItem(CreateNodeReports(nodeInfo));
			}
			response.SetNodeReports(nodeReports);
			return response;
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		public virtual GetQueueInfoResponse GetQueueInfo(GetQueueInfoRequest request)
		{
			UserGroupInformation callerUGI;
			try
			{
				callerUGI = UserGroupInformation.GetCurrentUser();
			}
			catch (IOException ie)
			{
				Log.Info("Error getting UGI ", ie);
				throw RPCUtil.GetRemoteException(ie);
			}
			GetQueueInfoResponse response = recordFactory.NewRecordInstance<GetQueueInfoResponse
				>();
			try
			{
				QueueInfo queueInfo = scheduler.GetQueueInfo(request.GetQueueName(), request.GetIncludeChildQueues
					(), request.GetRecursive());
				IList<ApplicationReport> appReports = EmptyAppsReport;
				if (request.GetIncludeApplications())
				{
					IList<ApplicationAttemptId> apps = scheduler.GetAppsInQueue(request.GetQueueName(
						));
					appReports = new AList<ApplicationReport>(apps.Count);
					foreach (ApplicationAttemptId app in apps)
					{
						RMApp rmApp = rmContext.GetRMApps()[app.GetApplicationId()];
						if (rmApp != null)
						{
							// Check if user is allowed access to this app
							if (!CheckAccess(callerUGI, rmApp.GetUser(), ApplicationAccessType.ViewApp, rmApp
								))
							{
								continue;
							}
							appReports.AddItem(rmApp.CreateAndGetApplicationReport(callerUGI.GetUserName(), true
								));
						}
					}
				}
				queueInfo.SetApplications(appReports);
				response.SetQueueInfo(queueInfo);
			}
			catch (IOException ioe)
			{
				Log.Info("Failed to getQueueInfo for " + request.GetQueueName(), ioe);
			}
			return response;
		}

		private NodeReport CreateNodeReports(RMNode rmNode)
		{
			SchedulerNodeReport schedulerNodeReport = scheduler.GetNodeReport(rmNode.GetNodeID
				());
			Resource used = BuilderUtils.NewResource(0, 0);
			int numContainers = 0;
			if (schedulerNodeReport != null)
			{
				used = schedulerNodeReport.GetUsedResource();
				numContainers = schedulerNodeReport.GetNumContainers();
			}
			NodeReport report = BuilderUtils.NewNodeReport(rmNode.GetNodeID(), rmNode.GetState
				(), rmNode.GetHttpAddress(), rmNode.GetRackName(), used, rmNode.GetTotalCapability
				(), numContainers, rmNode.GetHealthReport(), rmNode.GetLastHealthReportTime(), rmNode
				.GetNodeLabels());
			return report;
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		public virtual GetQueueUserAclsInfoResponse GetQueueUserAcls(GetQueueUserAclsInfoRequest
			 request)
		{
			GetQueueUserAclsInfoResponse response = recordFactory.NewRecordInstance<GetQueueUserAclsInfoResponse
				>();
			response.SetUserAclsInfoList(scheduler.GetQueueUserAclInfo());
			return response;
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		public virtual GetDelegationTokenResponse GetDelegationToken(GetDelegationTokenRequest
			 request)
		{
			try
			{
				// Verify that the connection is kerberos authenticated
				if (!IsAllowedDelegationTokenOp())
				{
					throw new IOException("Delegation Token can be issued only with kerberos authentication"
						);
				}
				GetDelegationTokenResponse response = recordFactory.NewRecordInstance<GetDelegationTokenResponse
					>();
				UserGroupInformation ugi = UserGroupInformation.GetCurrentUser();
				Text owner = new Text(ugi.GetUserName());
				Text realUser = null;
				if (ugi.GetRealUser() != null)
				{
					realUser = new Text(ugi.GetRealUser().GetUserName());
				}
				RMDelegationTokenIdentifier tokenIdentifier = new RMDelegationTokenIdentifier(owner
					, new Text(request.GetRenewer()), realUser);
				Org.Apache.Hadoop.Security.Token.Token<RMDelegationTokenIdentifier> realRMDTtoken
					 = new Org.Apache.Hadoop.Security.Token.Token<RMDelegationTokenIdentifier>(tokenIdentifier
					, this.rmDTSecretManager);
				response.SetRMDelegationToken(BuilderUtils.NewDelegationToken(realRMDTtoken.GetIdentifier
					(), realRMDTtoken.GetKind().ToString(), realRMDTtoken.GetPassword(), realRMDTtoken
					.GetService().ToString()));
				return response;
			}
			catch (IOException io)
			{
				throw RPCUtil.GetRemoteException(io);
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		public virtual RenewDelegationTokenResponse RenewDelegationToken(RenewDelegationTokenRequest
			 request)
		{
			try
			{
				if (!IsAllowedDelegationTokenOp())
				{
					throw new IOException("Delegation Token can be renewed only with kerberos authentication"
						);
				}
				Org.Apache.Hadoop.Yarn.Api.Records.Token protoToken = request.GetDelegationToken(
					);
				Org.Apache.Hadoop.Security.Token.Token<RMDelegationTokenIdentifier> token = new Org.Apache.Hadoop.Security.Token.Token
					<RMDelegationTokenIdentifier>(((byte[])protoToken.GetIdentifier().Array()), ((byte
					[])protoToken.GetPassword().Array()), new Text(protoToken.GetKind()), new Text(protoToken
					.GetService()));
				string user = GetRenewerForToken(token);
				long nextExpTime = rmDTSecretManager.RenewToken(token, user);
				RenewDelegationTokenResponse renewResponse = Org.Apache.Hadoop.Yarn.Util.Records.
					NewRecord<RenewDelegationTokenResponse>();
				renewResponse.SetNextExpirationTime(nextExpTime);
				return renewResponse;
			}
			catch (IOException e)
			{
				throw RPCUtil.GetRemoteException(e);
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		public virtual CancelDelegationTokenResponse CancelDelegationToken(CancelDelegationTokenRequest
			 request)
		{
			try
			{
				if (!IsAllowedDelegationTokenOp())
				{
					throw new IOException("Delegation Token can be cancelled only with kerberos authentication"
						);
				}
				Org.Apache.Hadoop.Yarn.Api.Records.Token protoToken = request.GetDelegationToken(
					);
				Org.Apache.Hadoop.Security.Token.Token<RMDelegationTokenIdentifier> token = new Org.Apache.Hadoop.Security.Token.Token
					<RMDelegationTokenIdentifier>(((byte[])protoToken.GetIdentifier().Array()), ((byte
					[])protoToken.GetPassword().Array()), new Text(protoToken.GetKind()), new Text(protoToken
					.GetService()));
				string user = UserGroupInformation.GetCurrentUser().GetUserName();
				rmDTSecretManager.CancelToken(token, user);
				return Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<CancelDelegationTokenResponse
					>();
			}
			catch (IOException e)
			{
				throw RPCUtil.GetRemoteException(e);
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		public virtual MoveApplicationAcrossQueuesResponse MoveApplicationAcrossQueues(MoveApplicationAcrossQueuesRequest
			 request)
		{
			ApplicationId applicationId = request.GetApplicationId();
			UserGroupInformation callerUGI;
			try
			{
				callerUGI = UserGroupInformation.GetCurrentUser();
			}
			catch (IOException ie)
			{
				Log.Info("Error getting UGI ", ie);
				RMAuditLogger.LogFailure("UNKNOWN", RMAuditLogger.AuditConstants.MoveAppRequest, 
					"UNKNOWN", "ClientRMService", "Error getting UGI", applicationId);
				throw RPCUtil.GetRemoteException(ie);
			}
			RMApp application = this.rmContext.GetRMApps()[applicationId];
			if (application == null)
			{
				RMAuditLogger.LogFailure(callerUGI.GetUserName(), RMAuditLogger.AuditConstants.MoveAppRequest
					, "UNKNOWN", "ClientRMService", "Trying to move an absent application", applicationId
					);
				throw new ApplicationNotFoundException("Trying to move an absent" + " application "
					 + applicationId);
			}
			if (!CheckAccess(callerUGI, application.GetUser(), ApplicationAccessType.ModifyApp
				, application))
			{
				RMAuditLogger.LogFailure(callerUGI.GetShortUserName(), RMAuditLogger.AuditConstants
					.MoveAppRequest, "User doesn't have permissions to " + ApplicationAccessType.ModifyApp
					.ToString(), "ClientRMService", RMAuditLogger.AuditConstants.UnauthorizedUser, applicationId
					);
				throw RPCUtil.GetRemoteException(new AccessControlException("User " + callerUGI.GetShortUserName
					() + " cannot perform operation " + ApplicationAccessType.ModifyApp.ToString() +
					 " on " + applicationId));
			}
			// Moves only allowed when app is in a state that means it is tracked by
			// the scheduler
			if (EnumSet.Of(RMAppState.New, RMAppState.NewSaving, RMAppState.Failed, RMAppState
				.FinalSaving, RMAppState.Finishing, RMAppState.Finished, RMAppState.Killed, RMAppState
				.Killing, RMAppState.Failed).Contains(application.GetState()))
			{
				string msg = "App in " + application.GetState() + " state cannot be moved.";
				RMAuditLogger.LogFailure(callerUGI.GetShortUserName(), RMAuditLogger.AuditConstants
					.MoveAppRequest, "UNKNOWN", "ClientRMService", msg);
				throw new YarnException(msg);
			}
			SettableFuture<object> future = SettableFuture.Create();
			this.rmContext.GetDispatcher().GetEventHandler().Handle(new RMAppMoveEvent(applicationId
				, request.GetTargetQueue(), future));
			try
			{
				Futures.Get<YarnException>(future);
			}
			catch (YarnException ex)
			{
				RMAuditLogger.LogFailure(callerUGI.GetShortUserName(), RMAuditLogger.AuditConstants
					.MoveAppRequest, "UNKNOWN", "ClientRMService", ex.Message);
				throw;
			}
			RMAuditLogger.LogSuccess(callerUGI.GetShortUserName(), RMAuditLogger.AuditConstants
				.MoveAppRequest, "ClientRMService", applicationId);
			MoveApplicationAcrossQueuesResponse response = recordFactory.NewRecordInstance<MoveApplicationAcrossQueuesResponse
				>();
			return response;
		}

		/// <exception cref="System.IO.IOException"/>
		private string GetRenewerForToken(Org.Apache.Hadoop.Security.Token.Token<RMDelegationTokenIdentifier
			> token)
		{
			UserGroupInformation user = UserGroupInformation.GetCurrentUser();
			UserGroupInformation loginUser = UserGroupInformation.GetLoginUser();
			// we can always renew our own tokens
			return loginUser.GetUserName().Equals(user.GetUserName()) ? token.DecodeIdentifier
				().GetRenewer().ToString() : user.GetShortUserName();
		}

		internal virtual void RefreshServiceAcls(Configuration configuration, PolicyProvider
			 policyProvider)
		{
			this.server.RefreshServiceAclWithLoadedConfiguration(configuration, policyProvider
				);
		}

		/// <exception cref="System.IO.IOException"/>
		private bool IsAllowedDelegationTokenOp()
		{
			if (UserGroupInformation.IsSecurityEnabled())
			{
				return EnumSet.Of(UserGroupInformation.AuthenticationMethod.Kerberos, UserGroupInformation.AuthenticationMethod
					.KerberosSsl, UserGroupInformation.AuthenticationMethod.Certificate).Contains(UserGroupInformation
					.GetCurrentUser().GetRealAuthenticationMethod());
			}
			else
			{
				return true;
			}
		}

		[VisibleForTesting]
		public virtual Org.Apache.Hadoop.Ipc.Server GetServer()
		{
			return this.server;
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual ReservationSubmissionResponse SubmitReservation(ReservationSubmissionRequest
			 request)
		{
			// Check if reservation system is enabled
			CheckReservationSytem(RMAuditLogger.AuditConstants.SubmitReservationRequest);
			ReservationSubmissionResponse response = recordFactory.NewRecordInstance<ReservationSubmissionResponse
				>();
			// Create a new Reservation Id
			ReservationId reservationId = reservationSystem.GetNewReservationId();
			// Validate the input
			Plan plan = rValidator.ValidateReservationSubmissionRequest(reservationSystem, request
				, reservationId);
			// Check ACLs
			string queueName = request.GetQueue();
			string user = CheckReservationACLs(queueName, RMAuditLogger.AuditConstants.SubmitReservationRequest
				);
			try
			{
				// Try to place the reservation using the agent
				bool result = plan.GetReservationAgent().CreateReservation(reservationId, user, plan
					, request.GetReservationDefinition());
				if (result)
				{
					// add the reservation id to valid ones maintained by reservation
					// system
					reservationSystem.SetQueueForReservation(reservationId, queueName);
					// create the reservation synchronously if required
					RefreshScheduler(queueName, request.GetReservationDefinition(), reservationId.ToString
						());
					// return the reservation id
					response.SetReservationId(reservationId);
				}
			}
			catch (PlanningException e)
			{
				RMAuditLogger.LogFailure(user, RMAuditLogger.AuditConstants.SubmitReservationRequest
					, e.Message, "ClientRMService", "Unable to create the reservation: " + reservationId
					);
				throw RPCUtil.GetRemoteException(e);
			}
			RMAuditLogger.LogSuccess(user, RMAuditLogger.AuditConstants.SubmitReservationRequest
				, "ClientRMService: " + reservationId);
			return response;
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual ReservationUpdateResponse UpdateReservation(ReservationUpdateRequest
			 request)
		{
			// Check if reservation system is enabled
			CheckReservationSytem(RMAuditLogger.AuditConstants.UpdateReservationRequest);
			ReservationUpdateResponse response = recordFactory.NewRecordInstance<ReservationUpdateResponse
				>();
			// Validate the input
			Plan plan = rValidator.ValidateReservationUpdateRequest(reservationSystem, request
				);
			ReservationId reservationId = request.GetReservationId();
			string queueName = reservationSystem.GetQueueForReservation(reservationId);
			// Check ACLs
			string user = CheckReservationACLs(queueName, RMAuditLogger.AuditConstants.UpdateReservationRequest
				);
			// Try to update the reservation using default agent
			try
			{
				bool result = plan.GetReservationAgent().UpdateReservation(reservationId, user, plan
					, request.GetReservationDefinition());
				if (!result)
				{
					string errMsg = "Unable to update reservation: " + reservationId;
					RMAuditLogger.LogFailure(user, RMAuditLogger.AuditConstants.UpdateReservationRequest
						, errMsg, "ClientRMService", errMsg);
					throw RPCUtil.GetRemoteException(errMsg);
				}
			}
			catch (PlanningException e)
			{
				RMAuditLogger.LogFailure(user, RMAuditLogger.AuditConstants.UpdateReservationRequest
					, e.Message, "ClientRMService", "Unable to update the reservation: " + reservationId
					);
				throw RPCUtil.GetRemoteException(e);
			}
			RMAuditLogger.LogSuccess(user, RMAuditLogger.AuditConstants.UpdateReservationRequest
				, "ClientRMService: " + reservationId);
			return response;
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual ReservationDeleteResponse DeleteReservation(ReservationDeleteRequest
			 request)
		{
			// Check if reservation system is enabled
			CheckReservationSytem(RMAuditLogger.AuditConstants.DeleteReservationRequest);
			ReservationDeleteResponse response = recordFactory.NewRecordInstance<ReservationDeleteResponse
				>();
			// Validate the input
			Plan plan = rValidator.ValidateReservationDeleteRequest(reservationSystem, request
				);
			ReservationId reservationId = request.GetReservationId();
			string queueName = reservationSystem.GetQueueForReservation(reservationId);
			// Check ACLs
			string user = CheckReservationACLs(queueName, RMAuditLogger.AuditConstants.DeleteReservationRequest
				);
			// Try to update the reservation using default agent
			try
			{
				bool result = plan.GetReservationAgent().DeleteReservation(reservationId, user, plan
					);
				if (!result)
				{
					string errMsg = "Could not delete reservation: " + reservationId;
					RMAuditLogger.LogFailure(user, RMAuditLogger.AuditConstants.DeleteReservationRequest
						, errMsg, "ClientRMService", errMsg);
					throw RPCUtil.GetRemoteException(errMsg);
				}
			}
			catch (PlanningException e)
			{
				RMAuditLogger.LogFailure(user, RMAuditLogger.AuditConstants.DeleteReservationRequest
					, e.Message, "ClientRMService", "Unable to delete the reservation: " + reservationId
					);
				throw RPCUtil.GetRemoteException(e);
			}
			RMAuditLogger.LogSuccess(user, RMAuditLogger.AuditConstants.DeleteReservationRequest
				, "ClientRMService: " + reservationId);
			return response;
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual GetNodesToLabelsResponse GetNodeToLabels(GetNodesToLabelsRequest request
			)
		{
			RMNodeLabelsManager labelsMgr = rmContext.GetNodeLabelManager();
			GetNodesToLabelsResponse response = GetNodesToLabelsResponse.NewInstance(labelsMgr
				.GetNodeLabels());
			return response;
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual GetLabelsToNodesResponse GetLabelsToNodes(GetLabelsToNodesRequest 
			request)
		{
			RMNodeLabelsManager labelsMgr = rmContext.GetNodeLabelManager();
			if (request.GetNodeLabels() == null || request.GetNodeLabels().IsEmpty())
			{
				return GetLabelsToNodesResponse.NewInstance(labelsMgr.GetLabelsToNodes());
			}
			else
			{
				return GetLabelsToNodesResponse.NewInstance(labelsMgr.GetLabelsToNodes(request.GetNodeLabels
					()));
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual GetClusterNodeLabelsResponse GetClusterNodeLabels(GetClusterNodeLabelsRequest
			 request)
		{
			RMNodeLabelsManager labelsMgr = rmContext.GetNodeLabelManager();
			GetClusterNodeLabelsResponse response = GetClusterNodeLabelsResponse.NewInstance(
				labelsMgr.GetClusterNodeLabels());
			return response;
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		private void CheckReservationSytem(string auditConstant)
		{
			// Check if reservation is enabled
			if (reservationSystem == null)
			{
				throw RPCUtil.GetRemoteException("Reservation is not enabled." + " Please enable & try again"
					);
			}
		}

		private void RefreshScheduler(string planName, ReservationDefinition contract, string
			 reservationId)
		{
			if ((contract.GetArrival() - clock.GetTime()) < reservationSystem.GetPlanFollowerTimeStep
				())
			{
				Log.Debug(MessageFormat.Format("Reservation {0} is within threshold so attempting to create synchronously."
					, reservationId));
				reservationSystem.SynchronizePlan(planName);
				Log.Info(MessageFormat.Format("Created reservation {0} synchronously.", reservationId
					));
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		private string CheckReservationACLs(string queueName, string auditConstant)
		{
			UserGroupInformation callerUGI;
			try
			{
				callerUGI = UserGroupInformation.GetCurrentUser();
			}
			catch (IOException ie)
			{
				RMAuditLogger.LogFailure("UNKNOWN", auditConstant, queueName, "ClientRMService", 
					"Error getting UGI");
				throw RPCUtil.GetRemoteException(ie);
			}
			// Check if user has access on the managed queue
			if (!queueACLsManager.CheckAccess(callerUGI, QueueACL.SubmitApplications, queueName
				))
			{
				RMAuditLogger.LogFailure(callerUGI.GetShortUserName(), auditConstant, "User doesn't have permissions to "
					 + QueueACL.SubmitApplications.ToString(), "ClientRMService", RMAuditLogger.AuditConstants
					.UnauthorizedUser);
				throw RPCUtil.GetRemoteException(new AccessControlException("User " + callerUGI.GetShortUserName
					() + " cannot perform operation " + QueueACL.SubmitApplications.ToString() + " on queue"
					 + queueName));
			}
			return callerUGI.GetShortUserName();
		}
	}
}
