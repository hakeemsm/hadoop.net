using System;
using System.Collections.Generic;
using System.Text;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Ipc;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Server.Security;
using Org.Apache.Hadoop.Yarn.Server.Utils;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager
{
	/// <summary>This class manages the list of applications for the resource manager.</summary>
	public class RMAppManager : EventHandler<RMAppManagerEvent>, Recoverable
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.RMAppManager
			));

		private int maxCompletedAppsInMemory;

		private int maxCompletedAppsInStateStore;

		protected internal int completedAppsInStateStore = 0;

		private List<ApplicationId> completedApps = new List<ApplicationId>();

		private readonly RMContext rmContext;

		private readonly ApplicationMasterService masterService;

		private readonly YarnScheduler scheduler;

		private readonly ApplicationACLsManager applicationACLsManager;

		private Configuration conf;

		public RMAppManager(RMContext context, YarnScheduler scheduler, ApplicationMasterService
			 masterService, ApplicationACLsManager applicationACLsManager, Configuration conf
			)
		{
			this.rmContext = context;
			this.scheduler = scheduler;
			this.masterService = masterService;
			this.applicationACLsManager = applicationACLsManager;
			this.conf = conf;
			this.maxCompletedAppsInMemory = conf.GetInt(YarnConfiguration.RmMaxCompletedApplications
				, YarnConfiguration.DefaultRmMaxCompletedApplications);
			this.maxCompletedAppsInStateStore = conf.GetInt(YarnConfiguration.RmStateStoreMaxCompletedApplications
				, YarnConfiguration.DefaultRmStateStoreMaxCompletedApplications);
			if (this.maxCompletedAppsInStateStore > this.maxCompletedAppsInMemory)
			{
				this.maxCompletedAppsInStateStore = this.maxCompletedAppsInMemory;
			}
		}

		/// <summary>This class is for logging the application summary.</summary>
		internal class ApplicationSummary
		{
			internal static readonly Log Log = LogFactory.GetLog(typeof(RMAppManager.ApplicationSummary
				));

			internal const char Equals = '=';

			internal static readonly char[] charsToEscape = new char[] { StringUtils.Comma, Equals
				, StringUtils.EscapeChar };

			internal class SummaryBuilder
			{
				internal readonly StringBuilder buffer = new StringBuilder();

				// Escape sequences 
				// A little optimization for a very common case
				internal virtual RMAppManager.ApplicationSummary.SummaryBuilder Add(string key, long
					 value)
				{
					return _add(key, System.Convert.ToString(value));
				}

				internal virtual RMAppManager.ApplicationSummary.SummaryBuilder Add<T>(string key
					, T value)
				{
					string escapedString = StringUtils.EscapeString(value.ToString(), StringUtils.EscapeChar
						, charsToEscape).ReplaceAll("\n", "\\\\n").ReplaceAll("\r", "\\\\r");
					return _add(key, escapedString);
				}

				internal virtual RMAppManager.ApplicationSummary.SummaryBuilder Add(RMAppManager.ApplicationSummary.SummaryBuilder
					 summary)
				{
					if (buffer.Length > 0)
					{
						buffer.Append(StringUtils.Comma);
					}
					buffer.Append(summary.buffer);
					return this;
				}

				internal virtual RMAppManager.ApplicationSummary.SummaryBuilder _add(string key, 
					string value)
				{
					if (buffer.Length > 0)
					{
						buffer.Append(StringUtils.Comma);
					}
					buffer.Append(key).Append(Equals).Append(value);
					return this;
				}

				public override string ToString()
				{
					return buffer.ToString();
				}
			}

			/// <summary>create a summary of the application's runtime.</summary>
			/// <param name="app">
			/// 
			/// <see cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.RMApp"/>
			/// whose summary is to be created, cannot
			/// be <code>null</code>.
			/// </param>
			public static RMAppManager.ApplicationSummary.SummaryBuilder CreateAppSummary(RMApp
				 app)
			{
				string trackingUrl = "N/A";
				string host = "N/A";
				RMAppAttempt attempt = app.GetCurrentAppAttempt();
				if (attempt != null)
				{
					trackingUrl = attempt.GetTrackingUrl();
					host = attempt.GetHost();
				}
				RMAppMetrics metrics = app.GetRMAppMetrics();
				RMAppManager.ApplicationSummary.SummaryBuilder summary = new RMAppManager.ApplicationSummary.SummaryBuilder
					().Add("appId", app.GetApplicationId()).Add("name", app.GetName()).Add("user", app
					.GetUser()).Add("queue", app.GetQueue()).Add("state", app.GetState()).Add("trackingUrl"
					, trackingUrl).Add("appMasterHost", host).Add("startTime", app.GetStartTime()).Add
					("finishTime", app.GetFinishTime()).Add("finalStatus", app.GetFinalApplicationStatus
					()).Add("memorySeconds", metrics.GetMemorySeconds()).Add("vcoreSeconds", metrics
					.GetVcoreSeconds()).Add("preemptedAMContainers", metrics.GetNumAMContainersPreempted
					()).Add("preemptedNonAMContainers", metrics.GetNumNonAMContainersPreempted()).Add
					("preemptedResources", metrics.GetResourcePreempted()).Add("applicationType", app
					.GetApplicationType());
				return summary;
			}

			/// <summary>Log a summary of the application's runtime.</summary>
			/// <param name="app">
			/// 
			/// <see cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.RMApp"/>
			/// whose summary is to be logged
			/// </param>
			public static void LogAppSummary(RMApp app)
			{
				if (app != null)
				{
					Log.Info(CreateAppSummary(app));
				}
			}
		}

		[VisibleForTesting]
		public virtual void LogApplicationSummary(ApplicationId appId)
		{
			RMAppManager.ApplicationSummary.LogAppSummary(rmContext.GetRMApps()[appId]);
		}

		protected internal virtual int GetCompletedAppsListSize()
		{
			lock (this)
			{
				return this.completedApps.Count;
			}
		}

		protected internal virtual void FinishApplication(ApplicationId applicationId)
		{
			lock (this)
			{
				if (applicationId == null)
				{
					Log.Error("RMAppManager received completed appId of null, skipping");
				}
				else
				{
					// Inform the DelegationTokenRenewer
					if (UserGroupInformation.IsSecurityEnabled())
					{
						rmContext.GetDelegationTokenRenewer().ApplicationFinished(applicationId);
					}
					completedApps.AddItem(applicationId);
					completedAppsInStateStore++;
					WriteAuditLog(applicationId);
				}
			}
		}

		protected internal virtual void WriteAuditLog(ApplicationId appId)
		{
			RMApp app = rmContext.GetRMApps()[appId];
			string operation = "UNKONWN";
			bool success = false;
			switch (app.GetState())
			{
				case RMAppState.Failed:
				{
					operation = RMAuditLogger.AuditConstants.FinishFailedApp;
					break;
				}

				case RMAppState.Finished:
				{
					operation = RMAuditLogger.AuditConstants.FinishSuccessApp;
					success = true;
					break;
				}

				case RMAppState.Killed:
				{
					operation = RMAuditLogger.AuditConstants.FinishKilledApp;
					success = true;
					break;
				}

				default:
				{
					break;
				}
			}
			if (success)
			{
				RMAuditLogger.LogSuccess(app.GetUser(), operation, "RMAppManager", app.GetApplicationId
					());
			}
			else
			{
				StringBuilder diag = app.GetDiagnostics();
				string msg = diag == null ? null : diag.ToString();
				RMAuditLogger.LogFailure(app.GetUser(), operation, msg, "RMAppManager", "App failed with state: "
					 + app.GetState(), appId);
			}
		}

		/*
		* check to see if hit the limit for max # completed apps kept
		*/
		protected internal virtual void CheckAppNumCompletedLimit()
		{
			lock (this)
			{
				// check apps kept in state store.
				while (completedAppsInStateStore > this.maxCompletedAppsInStateStore)
				{
					ApplicationId removeId = completedApps[completedApps.Count - completedAppsInStateStore
						];
					RMApp removeApp = rmContext.GetRMApps()[removeId];
					Log.Info("Max number of completed apps kept in state store met:" + " maxCompletedAppsInStateStore = "
						 + maxCompletedAppsInStateStore + ", removing app " + removeApp.GetApplicationId
						() + " from state store.");
					rmContext.GetStateStore().RemoveApplication(removeApp);
					completedAppsInStateStore--;
				}
				// check apps kept in memorty.
				while (completedApps.Count > this.maxCompletedAppsInMemory)
				{
					ApplicationId removeId = completedApps.Remove();
					Log.Info("Application should be expired, max number of completed apps" + " kept in memory met: maxCompletedAppsInMemory = "
						 + this.maxCompletedAppsInMemory + ", removing app " + removeId + " from memory: "
						);
					Sharpen.Collections.Remove(rmContext.GetRMApps(), removeId);
					this.applicationACLsManager.RemoveApplication(removeId);
				}
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		protected internal virtual void SubmitApplication(ApplicationSubmissionContext submissionContext
			, long submitTime, string user)
		{
			ApplicationId applicationId = submissionContext.GetApplicationId();
			RMAppImpl application = CreateAndPopulateNewRMApp(submissionContext, submitTime, 
				user, false);
			ApplicationId appId = submissionContext.GetApplicationId();
			if (UserGroupInformation.IsSecurityEnabled())
			{
				try
				{
					this.rmContext.GetDelegationTokenRenewer().AddApplicationAsync(appId, ParseCredentials
						(submissionContext), submissionContext.GetCancelTokensWhenComplete(), application
						.GetUser());
				}
				catch (Exception e)
				{
					Log.Warn("Unable to parse credentials.", e);
					// Sending APP_REJECTED is fine, since we assume that the
					// RMApp is in NEW state and thus we haven't yet informed the
					// scheduler about the existence of the application
					System.Diagnostics.Debug.Assert(application.GetState() == RMAppState.New);
					this.rmContext.GetDispatcher().GetEventHandler().Handle(new RMAppEvent(applicationId
						, RMAppEventType.AppRejected, e.Message));
					throw RPCUtil.GetRemoteException(e);
				}
			}
			else
			{
				// Dispatcher is not yet started at this time, so these START events
				// enqueued should be guaranteed to be first processed when dispatcher
				// gets started.
				this.rmContext.GetDispatcher().GetEventHandler().Handle(new RMAppEvent(applicationId
					, RMAppEventType.Start));
			}
		}

		/// <exception cref="System.Exception"/>
		protected internal virtual void RecoverApplication(ApplicationStateData appState, 
			RMStateStore.RMState rmState)
		{
			ApplicationSubmissionContext appContext = appState.GetApplicationSubmissionContext
				();
			ApplicationId appId = appContext.GetApplicationId();
			// create and recover app.
			RMAppImpl application = CreateAndPopulateNewRMApp(appContext, appState.GetSubmitTime
				(), appState.GetUser(), true);
			application.Handle(new RMAppRecoverEvent(appId, rmState));
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		private RMAppImpl CreateAndPopulateNewRMApp(ApplicationSubmissionContext submissionContext
			, long submitTime, string user, bool isRecovery)
		{
			ApplicationId applicationId = submissionContext.GetApplicationId();
			ResourceRequest amReq = ValidateAndCreateResourceRequest(submissionContext, isRecovery
				);
			// Create RMApp
			RMAppImpl application = new RMAppImpl(applicationId, rmContext, this.conf, submissionContext
				.GetApplicationName(), user, submissionContext.GetQueue(), submissionContext, this
				.scheduler, this.masterService, submitTime, submissionContext.GetApplicationType
				(), submissionContext.GetApplicationTags(), amReq);
			// Concurrent app submissions with same applicationId will fail here
			// Concurrent app submissions with different applicationIds will not
			// influence each other
			if (rmContext.GetRMApps().PutIfAbsent(applicationId, application) != null)
			{
				string message = "Application with id " + applicationId + " is already present! Cannot add a duplicate!";
				Log.Warn(message);
				throw new YarnException(message);
			}
			// Inform the ACLs Manager
			this.applicationACLsManager.AddApplication(applicationId, submissionContext.GetAMContainerSpec
				().GetApplicationACLs());
			string appViewACLs = submissionContext.GetAMContainerSpec().GetApplicationACLs()[
				ApplicationAccessType.ViewApp];
			rmContext.GetSystemMetricsPublisher().AppACLsUpdated(application, appViewACLs, Runtime
				.CurrentTimeMillis());
			return application;
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.InvalidResourceRequestException
		/// 	"/>
		private ResourceRequest ValidateAndCreateResourceRequest(ApplicationSubmissionContext
			 submissionContext, bool isRecovery)
		{
			// Validation of the ApplicationSubmissionContext needs to be completed
			// here. Only those fields that are dependent on RM's configuration are
			// checked here as they have to be validated whether they are part of new
			// submission or just being recovered.
			// Check whether AM resource requirements are within required limits
			if (!submissionContext.GetUnmanagedAM())
			{
				ResourceRequest amReq = submissionContext.GetAMContainerResourceRequest();
				if (amReq == null)
				{
					amReq = BuilderUtils.NewResourceRequest(RMAppAttemptImpl.AmContainerPriority, ResourceRequest
						.Any, submissionContext.GetResource(), 1);
				}
				// set label expression for AM container
				if (null == amReq.GetNodeLabelExpression())
				{
					amReq.SetNodeLabelExpression(submissionContext.GetNodeLabelExpression());
				}
				try
				{
					SchedulerUtils.NormalizeAndValidateRequest(amReq, scheduler.GetMaximumResourceCapability
						(), submissionContext.GetQueue(), scheduler, isRecovery, rmContext);
				}
				catch (InvalidResourceRequestException e)
				{
					Log.Warn("RM app submission failed in validating AM resource request" + " for application "
						 + submissionContext.GetApplicationId(), e);
					throw;
				}
				SchedulerUtils.NormalizeRequest(amReq, scheduler.GetResourceCalculator(), scheduler
					.GetClusterResource(), scheduler.GetMinimumResourceCapability(), scheduler.GetMaximumResourceCapability
					(), scheduler.GetMinimumResourceCapability());
				return amReq;
			}
			return null;
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual Credentials ParseCredentials(ApplicationSubmissionContext
			 application)
		{
			Credentials credentials = new Credentials();
			DataInputByteBuffer dibb = new DataInputByteBuffer();
			ByteBuffer tokens = application.GetAMContainerSpec().GetTokens();
			if (tokens != null)
			{
				dibb.Reset(tokens);
				credentials.ReadTokenStorageStream(dibb);
				tokens.Rewind();
			}
			return credentials;
		}

		/// <exception cref="System.Exception"/>
		public virtual void Recover(RMStateStore.RMState state)
		{
			RMStateStore store = rmContext.GetStateStore();
			System.Diagnostics.Debug.Assert(store != null);
			// recover applications
			IDictionary<ApplicationId, ApplicationStateData> appStates = state.GetApplicationState
				();
			Log.Info("Recovering " + appStates.Count + " applications");
			foreach (ApplicationStateData appState in appStates.Values)
			{
				RecoverApplication(appState, state);
			}
		}

		public virtual void Handle(RMAppManagerEvent @event)
		{
			ApplicationId applicationId = @event.GetApplicationId();
			Log.Debug("RMAppManager processing event for " + applicationId + " of type " + @event
				.GetType());
			switch (@event.GetType())
			{
				case RMAppManagerEventType.AppCompleted:
				{
					FinishApplication(applicationId);
					LogApplicationSummary(applicationId);
					CheckAppNumCompletedLimit();
					break;
				}

				default:
				{
					Log.Error("Invalid eventtype " + @event.GetType() + ". Ignoring!");
					break;
				}
			}
		}
	}
}
