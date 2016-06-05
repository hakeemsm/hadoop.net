using System;
using System.Collections.Generic;
using System.Text;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Security;
using Org.Apache.Hadoop.Yarn.Security.Client;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmnode;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Event;
using Org.Apache.Hadoop.Yarn.Server.Utils;
using Org.Apache.Hadoop.Yarn.Server.Webproxy;
using Org.Apache.Hadoop.Yarn.State;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Org.Apache.Hadoop.Yarn.Webapp.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp
{
	public class RMAppImpl : RMApp, Recoverable
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.RMAppImpl
			));

		private const string Unavailable = "N/A";

		private readonly ApplicationId applicationId;

		private readonly RMContext rmContext;

		private readonly Configuration conf;

		private readonly string user;

		private readonly string name;

		private readonly ApplicationSubmissionContext submissionContext;

		private readonly Dispatcher dispatcher;

		private readonly YarnScheduler scheduler;

		private readonly ApplicationMasterService masterService;

		private readonly StringBuilder diagnostics = new StringBuilder();

		private readonly int maxAppAttempts;

		private readonly ReentrantReadWriteLock.ReadLock readLock;

		private readonly ReentrantReadWriteLock.WriteLock writeLock;

		private readonly IDictionary<ApplicationAttemptId, RMAppAttempt> attempts = new LinkedHashMap
			<ApplicationAttemptId, RMAppAttempt>();

		private readonly long submitTime;

		private readonly ICollection<RMNode> updatedNodes = new HashSet<RMNode>();

		private readonly string applicationType;

		private readonly ICollection<string> applicationTags;

		private readonly long attemptFailuresValidityInterval;

		private Clock systemClock;

		private bool isNumAttemptsBeyondThreshold = false;

		private long startTime;

		private long finishTime = 0;

		private long storedFinishTime = 0;

		private volatile RMAppAttempt currentAttempt;

		private string queue;

		private EventHandler handler;

		private static readonly RMAppImpl.AppFinishedTransition FinishedTransition = new 
			RMAppImpl.AppFinishedTransition();

		private ICollection<NodeId> ranNodes = new ConcurrentSkipListSet<NodeId>();

		private RMAppState stateBeforeKilling;

		private RMAppState stateBeforeFinalSaving;

		private RMAppEvent eventCausingFinalSaving;

		private RMAppState targetedFinalState;

		private RMAppState recoveredFinalState;

		private ResourceRequest amReq;

		internal object transitionTodo;

		private static readonly StateMachineFactory<Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.RMAppImpl
			, RMAppState, RMAppEventType, RMAppEvent> stateMachineFactory = new StateMachineFactory
			<Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.RMAppImpl, RMAppState, RMAppEventType
			, RMAppEvent>(RMAppState.New).AddTransition(RMAppState.New, RMAppState.New, RMAppEventType
			.NodeUpdate, new RMAppImpl.RMAppNodeUpdateTransition()).AddTransition(RMAppState
			.New, RMAppState.NewSaving, RMAppEventType.Start, new RMAppImpl.RMAppNewlySavingTransition
			()).AddTransition(RMAppState.New, EnumSet.Of(RMAppState.Submitted, RMAppState.Accepted
			, RMAppState.Finished, RMAppState.Failed, RMAppState.Killed, RMAppState.FinalSaving
			), RMAppEventType.Recover, new RMAppImpl.RMAppRecoveredTransition()).AddTransition
			(RMAppState.New, RMAppState.Killed, RMAppEventType.Kill, new RMAppImpl.AppKilledTransition
			()).AddTransition(RMAppState.New, RMAppState.FinalSaving, RMAppEventType.AppRejected
			, new RMAppImpl.FinalSavingTransition(new RMAppImpl.AppRejectedTransition(), RMAppState
			.Failed)).AddTransition(RMAppState.NewSaving, RMAppState.NewSaving, RMAppEventType
			.NodeUpdate, new RMAppImpl.RMAppNodeUpdateTransition()).AddTransition(RMAppState
			.NewSaving, RMAppState.Submitted, RMAppEventType.AppNewSaved, new RMAppImpl.AddApplicationToSchedulerTransition
			()).AddTransition(RMAppState.NewSaving, RMAppState.FinalSaving, RMAppEventType.Kill
			, new RMAppImpl.FinalSavingTransition(new RMAppImpl.AppKilledTransition(), RMAppState
			.Killed)).AddTransition(RMAppState.NewSaving, RMAppState.FinalSaving, RMAppEventType
			.AppRejected, new RMAppImpl.FinalSavingTransition(new RMAppImpl.AppRejectedTransition
			(), RMAppState.Failed)).AddTransition(RMAppState.NewSaving, RMAppState.NewSaving
			, RMAppEventType.Move, new RMAppImpl.RMAppMoveTransition()).AddTransition(RMAppState
			.Submitted, RMAppState.Submitted, RMAppEventType.NodeUpdate, new RMAppImpl.RMAppNodeUpdateTransition
			()).AddTransition(RMAppState.Submitted, RMAppState.Submitted, RMAppEventType.Move
			, new RMAppImpl.RMAppMoveTransition()).AddTransition(RMAppState.Submitted, RMAppState
			.FinalSaving, RMAppEventType.AppRejected, new RMAppImpl.FinalSavingTransition(new 
			RMAppImpl.AppRejectedTransition(), RMAppState.Failed)).AddTransition(RMAppState.
			Submitted, RMAppState.Accepted, RMAppEventType.AppAccepted, new RMAppImpl.StartAppAttemptTransition
			()).AddTransition(RMAppState.Submitted, RMAppState.FinalSaving, RMAppEventType.Kill
			, new RMAppImpl.FinalSavingTransition(new RMAppImpl.AppKilledTransition(), RMAppState
			.Killed)).AddTransition(RMAppState.Accepted, RMAppState.Accepted, RMAppEventType
			.NodeUpdate, new RMAppImpl.RMAppNodeUpdateTransition()).AddTransition(RMAppState
			.Accepted, RMAppState.Accepted, RMAppEventType.Move, new RMAppImpl.RMAppMoveTransition
			()).AddTransition(RMAppState.Accepted, RMAppState.Running, RMAppEventType.AttemptRegistered
			).AddTransition(RMAppState.Accepted, EnumSet.Of(RMAppState.Accepted, RMAppState.
			FinalSaving), RMAppEventType.AttemptFailed, new RMAppImpl.AttemptFailedTransition
			(RMAppState.Accepted)).AddTransition(RMAppState.Accepted, RMAppState.FinalSaving
			, RMAppEventType.AttemptFinished, new RMAppImpl.FinalSavingTransition(FinishedTransition
			, RMAppState.Finished)).AddTransition(RMAppState.Accepted, RMAppState.Killing, RMAppEventType
			.Kill, new RMAppImpl.KillAttemptTransition()).AddTransition(RMAppState.Accepted, 
			RMAppState.FinalSaving, RMAppEventType.AttemptKilled, new RMAppImpl.FinalSavingTransition
			(new RMAppImpl.AppKilledTransition(), RMAppState.Killed)).AddTransition(RMAppState
			.Accepted, RMAppState.Accepted, RMAppEventType.AppRunningOnNode, new RMAppImpl.AppRunningOnNodeTransition
			()).AddTransition(RMAppState.Running, RMAppState.Running, RMAppEventType.NodeUpdate
			, new RMAppImpl.RMAppNodeUpdateTransition()).AddTransition(RMAppState.Running, RMAppState
			.Running, RMAppEventType.Move, new RMAppImpl.RMAppMoveTransition()).AddTransition
			(RMAppState.Running, RMAppState.FinalSaving, RMAppEventType.AttemptUnregistered, 
			new RMAppImpl.FinalSavingTransition(new RMAppImpl.AttemptUnregisteredTransition(
			), RMAppState.Finishing, RMAppState.Finished)).AddTransition(RMAppState.Running, 
			RMAppState.Finished, RMAppEventType.AttemptFinished, FinishedTransition).AddTransition
			(RMAppState.Running, RMAppState.Running, RMAppEventType.AppRunningOnNode, new RMAppImpl.AppRunningOnNodeTransition
			()).AddTransition(RMAppState.Running, EnumSet.Of(RMAppState.Accepted, RMAppState
			.FinalSaving), RMAppEventType.AttemptFailed, new RMAppImpl.AttemptFailedTransition
			(RMAppState.Accepted)).AddTransition(RMAppState.Running, RMAppState.Killing, RMAppEventType
			.Kill, new RMAppImpl.KillAttemptTransition()).AddTransition(RMAppState.FinalSaving
			, EnumSet.Of(RMAppState.Finishing, RMAppState.Failed, RMAppState.Killed, RMAppState
			.Finished), RMAppEventType.AppUpdateSaved, new RMAppImpl.FinalStateSavedTransition
			()).AddTransition(RMAppState.FinalSaving, RMAppState.FinalSaving, RMAppEventType
			.AttemptFinished, new RMAppImpl.AttemptFinishedAtFinalSavingTransition()).AddTransition
			(RMAppState.FinalSaving, RMAppState.FinalSaving, RMAppEventType.AppRunningOnNode
			, new RMAppImpl.AppRunningOnNodeTransition()).AddTransition(RMAppState.FinalSaving
			, RMAppState.FinalSaving, EnumSet.Of(RMAppEventType.NodeUpdate, RMAppEventType.Kill
			, RMAppEventType.AppNewSaved, RMAppEventType.Move)).AddTransition(RMAppState.Finishing
			, RMAppState.Finished, RMAppEventType.AttemptFinished, FinishedTransition).AddTransition
			(RMAppState.Finishing, RMAppState.Finishing, RMAppEventType.AppRunningOnNode, new 
			RMAppImpl.AppRunningOnNodeTransition()).AddTransition(RMAppState.Finishing, RMAppState
			.Finishing, EnumSet.Of(RMAppEventType.NodeUpdate, RMAppEventType.Kill, RMAppEventType
			.Move)).AddTransition(RMAppState.Killing, RMAppState.Killing, RMAppEventType.AppRunningOnNode
			, new RMAppImpl.AppRunningOnNodeTransition()).AddTransition(RMAppState.Killing, 
			RMAppState.FinalSaving, RMAppEventType.AttemptKilled, new RMAppImpl.FinalSavingTransition
			(new RMAppImpl.AppKilledTransition(), RMAppState.Killed)).AddTransition(RMAppState
			.Killing, RMAppState.FinalSaving, RMAppEventType.AttemptUnregistered, new RMAppImpl.FinalSavingTransition
			(new RMAppImpl.AttemptUnregisteredTransition(), RMAppState.Finishing, RMAppState
			.Finished)).AddTransition(RMAppState.Killing, RMAppState.Finished, RMAppEventType
			.AttemptFinished, FinishedTransition).AddTransition(RMAppState.Killing, EnumSet.
			Of(RMAppState.FinalSaving), RMAppEventType.AttemptFailed, new RMAppImpl.AttemptFailedTransition
			(RMAppState.Killing)).AddTransition(RMAppState.Killing, RMAppState.Killing, EnumSet
			.Of(RMAppEventType.NodeUpdate, RMAppEventType.AttemptRegistered, RMAppEventType.
			AppUpdateSaved, RMAppEventType.Kill, RMAppEventType.Move)).AddTransition(RMAppState
			.Finished, RMAppState.Finished, RMAppEventType.AppRunningOnNode, new RMAppImpl.AppRunningOnNodeTransition
			()).AddTransition(RMAppState.Finished, RMAppState.Finished, EnumSet.Of(RMAppEventType
			.NodeUpdate, RMAppEventType.AttemptUnregistered, RMAppEventType.AttemptFinished, 
			RMAppEventType.Kill, RMAppEventType.Move)).AddTransition(RMAppState.Failed, RMAppState
			.Failed, RMAppEventType.AppRunningOnNode, new RMAppImpl.AppRunningOnNodeTransition
			()).AddTransition(RMAppState.Failed, RMAppState.Failed, EnumSet.Of(RMAppEventType
			.Kill, RMAppEventType.NodeUpdate, RMAppEventType.Move)).AddTransition(RMAppState
			.Killed, RMAppState.Killed, RMAppEventType.AppRunningOnNode, new RMAppImpl.AppRunningOnNodeTransition
			()).AddTransition(RMAppState.Killed, RMAppState.Killed, EnumSet.Of(RMAppEventType
			.AppAccepted, RMAppEventType.AppRejected, RMAppEventType.Kill, RMAppEventType.AttemptFinished
			, RMAppEventType.AttemptFailed, RMAppEventType.NodeUpdate, RMAppEventType.Move))
			.InstallTopology();

		private readonly StateMachine<RMAppState, RMAppEventType, RMAppEvent> stateMachine;

		private const int DummyApplicationAttemptNumber = -1;

		public RMAppImpl(ApplicationId applicationId, RMContext rmContext, Configuration 
			config, string name, string user, string queue, ApplicationSubmissionContext submissionContext
			, YarnScheduler scheduler, ApplicationMasterService masterService, long submitTime
			, string applicationType, ICollection<string> applicationTags, ResourceRequest amReq
			)
		{
			// Immutable fields
			// Mutable fields
			// This field isn't protected by readlock now.
			// These states stored are only valid when app is at killing or final_saving.
			// Transitions from NEW state
			// Transitions from NEW_SAVING state
			// Transitions from SUBMITTED state
			// Transitions from ACCEPTED state
			// ACCEPTED state is possible to receive ATTEMPT_FAILED/ATTEMPT_FINISHED
			// event because RMAppRecoveredTransition is returning ACCEPTED state
			// directly and waiting for the previous AM to exit.
			// Transitions from RUNNING state
			// UnManagedAM directly jumps to finished
			// Transitions from FINAL_SAVING state
			// ignorable transitions
			// Transitions from FINISHING state
			// ignorable transitions
			// ignore Kill/Move as we have already saved the final Finished state
			// in state store.
			// Transitions from KILLING state
			// UnManagedAM directly jumps to finished
			// Transitions from FINISHED state
			// ignorable transitions
			// Transitions from FAILED state
			// ignorable transitions
			// Transitions from KILLED state
			// ignorable transitions
			this.systemClock = new SystemClock();
			this.applicationId = applicationId;
			this.name = name;
			this.rmContext = rmContext;
			this.dispatcher = rmContext.GetDispatcher();
			this.handler = dispatcher.GetEventHandler();
			this.conf = config;
			this.user = user;
			this.queue = queue;
			this.submissionContext = submissionContext;
			this.scheduler = scheduler;
			this.masterService = masterService;
			this.submitTime = submitTime;
			this.startTime = this.systemClock.GetTime();
			this.applicationType = applicationType;
			this.applicationTags = applicationTags;
			this.amReq = amReq;
			int globalMaxAppAttempts = conf.GetInt(YarnConfiguration.RmAmMaxAttempts, YarnConfiguration
				.DefaultRmAmMaxAttempts);
			int individualMaxAppAttempts = submissionContext.GetMaxAppAttempts();
			if (individualMaxAppAttempts <= 0 || individualMaxAppAttempts > globalMaxAppAttempts)
			{
				this.maxAppAttempts = globalMaxAppAttempts;
				Log.Warn("The specific max attempts: " + individualMaxAppAttempts + " for application: "
					 + applicationId.GetId() + " is invalid, because it is out of the range [1, " + 
					globalMaxAppAttempts + "]. Use the global max attempts instead.");
			}
			else
			{
				this.maxAppAttempts = individualMaxAppAttempts;
			}
			this.attemptFailuresValidityInterval = submissionContext.GetAttemptFailuresValidityInterval
				();
			if (this.attemptFailuresValidityInterval > 0)
			{
				Log.Info("The attemptFailuresValidityInterval for the application: " + this.applicationId
					 + " is " + this.attemptFailuresValidityInterval + ".");
			}
			ReentrantReadWriteLock Lock = new ReentrantReadWriteLock();
			this.readLock = Lock.ReadLock();
			this.writeLock = Lock.WriteLock();
			this.stateMachine = stateMachineFactory.Make(this);
			rmContext.GetRMApplicationHistoryWriter().ApplicationStarted(this);
			rmContext.GetSystemMetricsPublisher().AppCreated(this, startTime);
		}

		public virtual ApplicationId GetApplicationId()
		{
			return this.applicationId;
		}

		public virtual ApplicationSubmissionContext GetApplicationSubmissionContext()
		{
			return this.submissionContext;
		}

		public virtual FinalApplicationStatus GetFinalApplicationStatus()
		{
			// finish state is obtained based on the state machine's current state
			// as a fall-back in case the application has not been unregistered
			// ( or if the app never unregistered itself )
			// when the report is requested
			if (currentAttempt != null && currentAttempt.GetFinalApplicationStatus() != null)
			{
				return currentAttempt.GetFinalApplicationStatus();
			}
			return CreateFinalApplicationStatus(this.stateMachine.GetCurrentState());
		}

		public virtual RMAppState GetState()
		{
			this.readLock.Lock();
			try
			{
				return this.stateMachine.GetCurrentState();
			}
			finally
			{
				this.readLock.Unlock();
			}
		}

		public virtual string GetUser()
		{
			return this.user;
		}

		public virtual float GetProgress()
		{
			RMAppAttempt attempt = this.currentAttempt;
			if (attempt != null)
			{
				return attempt.GetProgress();
			}
			return 0;
		}

		public virtual RMAppAttempt GetRMAppAttempt(ApplicationAttemptId appAttemptId)
		{
			this.readLock.Lock();
			try
			{
				return this.attempts[appAttemptId];
			}
			finally
			{
				this.readLock.Unlock();
			}
		}

		public virtual string GetQueue()
		{
			return this.queue;
		}

		public virtual void SetQueue(string queue)
		{
			this.queue = queue;
		}

		public virtual string GetName()
		{
			return this.name;
		}

		public virtual RMAppAttempt GetCurrentAppAttempt()
		{
			return this.currentAttempt;
		}

		public virtual IDictionary<ApplicationAttemptId, RMAppAttempt> GetAppAttempts()
		{
			this.readLock.Lock();
			try
			{
				return Sharpen.Collections.UnmodifiableMap(this.attempts);
			}
			finally
			{
				this.readLock.Unlock();
			}
		}

		private FinalApplicationStatus CreateFinalApplicationStatus(RMAppState state)
		{
			switch (state)
			{
				case RMAppState.New:
				case RMAppState.NewSaving:
				case RMAppState.Submitted:
				case RMAppState.Accepted:
				case RMAppState.Running:
				case RMAppState.FinalSaving:
				case RMAppState.Killing:
				{
					return FinalApplicationStatus.Undefined;
				}

				case RMAppState.Finishing:
				case RMAppState.Finished:
				case RMAppState.Failed:
				{
					// finished without a proper final state is the same as failed  
					return FinalApplicationStatus.Failed;
				}

				case RMAppState.Killed:
				{
					return FinalApplicationStatus.Killed;
				}
			}
			throw new YarnRuntimeException("Unknown state passed!");
		}

		public virtual int PullRMNodeUpdates(ICollection<RMNode> updatedNodes)
		{
			this.writeLock.Lock();
			try
			{
				int updatedNodeCount = this.updatedNodes.Count;
				Sharpen.Collections.AddAll(updatedNodes, this.updatedNodes);
				this.updatedNodes.Clear();
				return updatedNodeCount;
			}
			finally
			{
				this.writeLock.Unlock();
			}
		}

		public virtual ApplicationReport CreateAndGetApplicationReport(string clientUserName
			, bool allowAccess)
		{
			this.readLock.Lock();
			try
			{
				ApplicationAttemptId currentApplicationAttemptId = null;
				Token clientToAMToken = null;
				string trackingUrl = Unavailable;
				string host = Unavailable;
				string origTrackingUrl = Unavailable;
				int rpcPort = -1;
				ApplicationResourceUsageReport appUsageReport = RMServerUtils.DummyApplicationResourceUsageReport;
				FinalApplicationStatus finishState = GetFinalApplicationStatus();
				string diags = Unavailable;
				float progress = 0.0f;
				Token amrmToken = null;
				if (allowAccess)
				{
					trackingUrl = GetDefaultProxyTrackingUrl();
					if (this.currentAttempt != null)
					{
						currentApplicationAttemptId = this.currentAttempt.GetAppAttemptId();
						trackingUrl = this.currentAttempt.GetTrackingUrl();
						origTrackingUrl = this.currentAttempt.GetOriginalTrackingUrl();
						if (UserGroupInformation.IsSecurityEnabled())
						{
							// get a token so the client can communicate with the app attempt
							// NOTE: token may be unavailable if the attempt is not running
							Org.Apache.Hadoop.Security.Token.Token<ClientToAMTokenIdentifier> attemptClientToAMToken
								 = this.currentAttempt.CreateClientToken(clientUserName);
							if (attemptClientToAMToken != null)
							{
								clientToAMToken = BuilderUtils.NewClientToAMToken(attemptClientToAMToken.GetIdentifier
									(), attemptClientToAMToken.GetKind().ToString(), attemptClientToAMToken.GetPassword
									(), attemptClientToAMToken.GetService().ToString());
							}
						}
						host = this.currentAttempt.GetHost();
						rpcPort = this.currentAttempt.GetRpcPort();
						appUsageReport = currentAttempt.GetApplicationResourceUsageReport();
						progress = currentAttempt.GetProgress();
					}
					diags = this.diagnostics.ToString();
					if (currentAttempt != null && currentAttempt.GetAppAttemptState() == RMAppAttemptState
						.Launched)
					{
						if (GetApplicationSubmissionContext().GetUnmanagedAM() && clientUserName != null 
							&& GetUser().Equals(clientUserName))
						{
							Org.Apache.Hadoop.Security.Token.Token<AMRMTokenIdentifier> token = currentAttempt
								.GetAMRMToken();
							if (token != null)
							{
								amrmToken = BuilderUtils.NewAMRMToken(token.GetIdentifier(), token.GetKind().ToString
									(), token.GetPassword(), token.GetService().ToString());
							}
						}
					}
					RMAppMetrics rmAppMetrics = GetRMAppMetrics();
					appUsageReport.SetMemorySeconds(rmAppMetrics.GetMemorySeconds());
					appUsageReport.SetVcoreSeconds(rmAppMetrics.GetVcoreSeconds());
				}
				if (currentApplicationAttemptId == null)
				{
					currentApplicationAttemptId = BuilderUtils.NewApplicationAttemptId(this.applicationId
						, DummyApplicationAttemptNumber);
				}
				return BuilderUtils.NewApplicationReport(this.applicationId, currentApplicationAttemptId
					, this.user, this.queue, this.name, host, rpcPort, clientToAMToken, CreateApplicationState
					(), diags, trackingUrl, this.startTime, this.finishTime, finishState, appUsageReport
					, origTrackingUrl, progress, this.applicationType, amrmToken, applicationTags);
			}
			finally
			{
				this.readLock.Unlock();
			}
		}

		private string GetDefaultProxyTrackingUrl()
		{
			try
			{
				string scheme = WebAppUtils.GetHttpSchemePrefix(conf);
				string proxy = WebAppUtils.GetProxyHostAndPort(conf);
				URI proxyUri = ProxyUriUtils.GetUriFromAMUrl(scheme, proxy);
				URI result = ProxyUriUtils.GetProxyUri(null, proxyUri, applicationId);
				return result.ToASCIIString();
			}
			catch (URISyntaxException)
			{
				Log.Warn("Could not generate default proxy tracking URL for " + applicationId);
				return Unavailable;
			}
		}

		public virtual long GetFinishTime()
		{
			this.readLock.Lock();
			try
			{
				return this.finishTime;
			}
			finally
			{
				this.readLock.Unlock();
			}
		}

		public virtual long GetStartTime()
		{
			this.readLock.Lock();
			try
			{
				return this.startTime;
			}
			finally
			{
				this.readLock.Unlock();
			}
		}

		public virtual long GetSubmitTime()
		{
			return this.submitTime;
		}

		public virtual string GetTrackingUrl()
		{
			RMAppAttempt attempt = this.currentAttempt;
			if (attempt != null)
			{
				return attempt.GetTrackingUrl();
			}
			return null;
		}

		public virtual string GetOriginalTrackingUrl()
		{
			RMAppAttempt attempt = this.currentAttempt;
			if (attempt != null)
			{
				return attempt.GetOriginalTrackingUrl();
			}
			return null;
		}

		public virtual StringBuilder GetDiagnostics()
		{
			this.readLock.Lock();
			try
			{
				return this.diagnostics;
			}
			finally
			{
				this.readLock.Unlock();
			}
		}

		public virtual int GetMaxAppAttempts()
		{
			return this.maxAppAttempts;
		}

		public virtual void Handle(RMAppEvent @event)
		{
			this.writeLock.Lock();
			try
			{
				ApplicationId appID = @event.GetApplicationId();
				Log.Debug("Processing event for " + appID + " of type " + @event.GetType());
				RMAppState oldState = GetState();
				try
				{
					/* keep the master in sync with the state machine */
					this.stateMachine.DoTransition(@event.GetType(), @event);
				}
				catch (InvalidStateTransitonException e)
				{
					Log.Error("Can't handle this event at current state", e);
				}
				/* TODO fail the application on the failed transition */
				if (oldState != GetState())
				{
					Log.Info(appID + " State change from " + oldState + " to " + GetState());
				}
			}
			finally
			{
				this.writeLock.Unlock();
			}
		}

		public virtual void Recover(RMStateStore.RMState state)
		{
			ApplicationStateData appState = state.GetApplicationState()[GetApplicationId()];
			this.recoveredFinalState = appState.GetState();
			Log.Info("Recovering app: " + GetApplicationId() + " with " + +appState.GetAttemptCount
				() + " attempts and final state = " + this.recoveredFinalState);
			this.diagnostics.Append(appState.GetDiagnostics());
			this.storedFinishTime = appState.GetFinishTime();
			this.startTime = appState.GetStartTime();
			for (int i = 0; i < appState.GetAttemptCount(); ++i)
			{
				// create attempt
				CreateNewAttempt();
				((RMAppAttemptImpl)this.currentAttempt).Recover(state);
			}
		}

		private void CreateNewAttempt()
		{
			ApplicationAttemptId appAttemptId = ApplicationAttemptId.NewInstance(applicationId
				, attempts.Count + 1);
			RMAppAttempt attempt = new RMAppAttemptImpl(appAttemptId, rmContext, scheduler, masterService
				, submissionContext, conf, maxAppAttempts == (GetNumFailedAppAttempts() + 1), amReq
				);
			// The newly created attempt maybe last attempt if (number of
			// previously failed attempts(which should not include Preempted,
			// hardware error and NM resync) + 1) equal to the max-attempt
			// limit.
			attempts[appAttemptId] = attempt;
			currentAttempt = attempt;
		}

		private void CreateAndStartNewAttempt(bool transferStateFromPreviousAttempt)
		{
			CreateNewAttempt();
			handler.Handle(new RMAppStartAttemptEvent(currentAttempt.GetAppAttemptId(), transferStateFromPreviousAttempt
				));
		}

		private void ProcessNodeUpdate(RMAppNodeUpdateEvent.RMAppNodeUpdateType type, RMNode
			 node)
		{
			NodeState nodeState = node.GetState();
			updatedNodes.AddItem(node);
			Log.Debug("Received node update event:" + type + " for node:" + node + " with state:"
				 + nodeState);
		}

		private class RMAppTransition : SingleArcTransition<RMAppImpl, RMAppEvent>
		{
			public virtual void Transition(RMAppImpl app, RMAppEvent @event)
			{
			}
		}

		private sealed class RMAppNodeUpdateTransition : RMAppImpl.RMAppTransition
		{
			public override void Transition(RMAppImpl app, RMAppEvent @event)
			{
				RMAppNodeUpdateEvent nodeUpdateEvent = (RMAppNodeUpdateEvent)@event;
				app.ProcessNodeUpdate(nodeUpdateEvent.GetUpdateType(), nodeUpdateEvent.GetNode());
			}
		}

		private sealed class AppRunningOnNodeTransition : RMAppImpl.RMAppTransition
		{
			public override void Transition(RMAppImpl app, RMAppEvent @event)
			{
				RMAppRunningOnNodeEvent nodeAddedEvent = (RMAppRunningOnNodeEvent)@event;
				// if final state already stored, notify RMNode
				if (IsAppInFinalState(app))
				{
					app.handler.Handle(new RMNodeCleanAppEvent(nodeAddedEvent.GetNodeId(), nodeAddedEvent
						.GetApplicationId()));
					return;
				}
				// otherwise, add it to ranNodes for further process
				app.ranNodes.AddItem(nodeAddedEvent.GetNodeId());
			}
		}

		/// <summary>Move an app to a new queue.</summary>
		/// <remarks>
		/// Move an app to a new queue.
		/// This transition must set the result on the Future in the RMAppMoveEvent,
		/// either as an exception for failure or null for success, or the client will
		/// be left waiting forever.
		/// </remarks>
		private sealed class RMAppMoveTransition : RMAppImpl.RMAppTransition
		{
			public override void Transition(RMAppImpl app, RMAppEvent @event)
			{
				RMAppMoveEvent moveEvent = (RMAppMoveEvent)@event;
				try
				{
					app.queue = app.scheduler.MoveApplication(app.applicationId, moveEvent.GetTargetQueue
						());
				}
				catch (YarnException ex)
				{
					moveEvent.GetResult().SetException(ex);
					return;
				}
				// TODO: Write out change to state store (YARN-1558)
				// Also take care of RM failover
				moveEvent.GetResult().Set(null);
			}
		}

		// synchronously recover attempt to ensure any incoming external events
		// to be processed after the attempt processes the recover event.
		private void RecoverAppAttempts()
		{
			foreach (RMAppAttempt attempt in GetAppAttempts().Values)
			{
				attempt.Handle(new RMAppAttemptEvent(attempt.GetAppAttemptId(), RMAppAttemptEventType
					.Recover));
			}
		}

		private sealed class RMAppRecoveredTransition : MultipleArcTransition<RMAppImpl, 
			RMAppEvent, RMAppState>
		{
			public RMAppState Transition(RMAppImpl app, RMAppEvent @event)
			{
				RMAppRecoverEvent recoverEvent = (RMAppRecoverEvent)@event;
				app.Recover(recoverEvent.GetRMState());
				// The app has completed.
				if (app.recoveredFinalState != null)
				{
					app.RecoverAppAttempts();
					new RMAppImpl.FinalTransition(app.recoveredFinalState).Transition(app, @event);
					return app.recoveredFinalState;
				}
				if (UserGroupInformation.IsSecurityEnabled())
				{
					// asynchronously renew delegation token on recovery.
					try
					{
						app.rmContext.GetDelegationTokenRenewer().AddApplicationAsyncDuringRecovery(app.GetApplicationId
							(), app.ParseCredentials(), app.submissionContext.GetCancelTokensWhenComplete(), 
							app.GetUser());
					}
					catch (Exception e)
					{
						string msg = "Failed to fetch user credentials from application:" + e.Message;
						app.diagnostics.Append(msg);
						Log.Error(msg, e);
					}
				}
				// No existent attempts means the attempt associated with this app was not
				// started or started but not yet saved.
				if (app.attempts.IsEmpty())
				{
					app.scheduler.Handle(new AppAddedSchedulerEvent(app.applicationId, app.submissionContext
						.GetQueue(), app.user, app.submissionContext.GetReservationID()));
					return RMAppState.Submitted;
				}
				// Add application to scheduler synchronously to guarantee scheduler
				// knows applications before AM or NM re-registers.
				app.scheduler.Handle(new AppAddedSchedulerEvent(app.applicationId, app.submissionContext
					.GetQueue(), app.user, true, app.submissionContext.GetReservationID()));
				// recover attempts
				app.RecoverAppAttempts();
				// Last attempt is in final state, return ACCEPTED waiting for last
				// RMAppAttempt to send finished or failed event back.
				if (app.currentAttempt != null && (app.currentAttempt.GetState() == RMAppAttemptState
					.Killed || app.currentAttempt.GetState() == RMAppAttemptState.Finished || (app.currentAttempt
					.GetState() == RMAppAttemptState.Failed && app.GetNumFailedAppAttempts() == app.
					maxAppAttempts)))
				{
					return RMAppState.Accepted;
				}
				// YARN-1507 is saving the application state after the application is
				// accepted. So after YARN-1507, an app is saved meaning it is accepted.
				// Thus we return ACCECPTED state on recovery.
				return RMAppState.Accepted;
			}
		}

		private sealed class AddApplicationToSchedulerTransition : RMAppImpl.RMAppTransition
		{
			public override void Transition(RMAppImpl app, RMAppEvent @event)
			{
				app.handler.Handle(new AppAddedSchedulerEvent(app.applicationId, app.submissionContext
					.GetQueue(), app.user, app.submissionContext.GetReservationID()));
			}
		}

		private sealed class StartAppAttemptTransition : RMAppImpl.RMAppTransition
		{
			public override void Transition(RMAppImpl app, RMAppEvent @event)
			{
				app.CreateAndStartNewAttempt(false);
			}
		}

		private sealed class FinalStateSavedTransition : MultipleArcTransition<RMAppImpl, 
			RMAppEvent, RMAppState>
		{
			public RMAppState Transition(RMAppImpl app, RMAppEvent @event)
			{
				if (app.transitionTodo is SingleArcTransition)
				{
					((SingleArcTransition)app.transitionTodo).Transition(app, app.eventCausingFinalSaving
						);
				}
				else
				{
					if (app.transitionTodo is MultipleArcTransition)
					{
						((MultipleArcTransition)app.transitionTodo).Transition(app, app.eventCausingFinalSaving
							);
					}
				}
				return app.targetedFinalState;
			}
		}

		private class AttemptFailedFinalStateSavedTransition : RMAppImpl.RMAppTransition
		{
			public override void Transition(RMAppImpl app, RMAppEvent @event)
			{
				string msg = null;
				if (@event is RMAppFailedAttemptEvent)
				{
					msg = app.GetAppAttemptFailedDiagnostics(@event);
				}
				Log.Info(msg);
				app.diagnostics.Append(msg);
				// Inform the node for app-finish
				new RMAppImpl.FinalTransition(RMAppState.Failed).Transition(app, @event);
			}
		}

		private string GetAppAttemptFailedDiagnostics(RMAppEvent @event)
		{
			string msg = null;
			RMAppFailedAttemptEvent failedEvent = (RMAppFailedAttemptEvent)@event;
			if (this.submissionContext.GetUnmanagedAM())
			{
				// RM does not manage the AM. Do not retry
				msg = "Unmanaged application " + this.GetApplicationId() + " failed due to " + failedEvent
					.GetDiagnosticMsg() + ". Failing the application.";
			}
			else
			{
				if (this.isNumAttemptsBeyondThreshold)
				{
					msg = "Application " + this.GetApplicationId() + " failed " + this.maxAppAttempts
						 + " times due to " + failedEvent.GetDiagnosticMsg() + ". Failing the application.";
				}
			}
			return msg;
		}

		private sealed class RMAppNewlySavingTransition : RMAppImpl.RMAppTransition
		{
			public override void Transition(RMAppImpl app, RMAppEvent @event)
			{
				// If recovery is enabled then store the application information in a
				// non-blocking call so make sure that RM has stored the information
				// needed to restart the AM after RM restart without further client
				// communication
				Log.Info("Storing application with id " + app.applicationId);
				app.rmContext.GetStateStore().StoreNewApplication(app);
			}
		}

		private void RememberTargetTransitions(RMAppEvent @event, object transitionToDo, 
			RMAppState targetFinalState)
		{
			transitionTodo = transitionToDo;
			targetedFinalState = targetFinalState;
			eventCausingFinalSaving = @event;
		}

		private void RememberTargetTransitionsAndStoreState(RMAppEvent @event, object transitionToDo
			, RMAppState targetFinalState, RMAppState stateToBeStored)
		{
			RememberTargetTransitions(@event, transitionToDo, targetFinalState);
			this.stateBeforeFinalSaving = GetState();
			this.storedFinishTime = this.systemClock.GetTime();
			Log.Info("Updating application " + this.applicationId + " with final state: " + this
				.targetedFinalState);
			// we lost attempt_finished diagnostics in app, because attempt_finished
			// diagnostics is sent after app final state is saved. Later on, we will
			// create GetApplicationAttemptReport specifically for getting per attempt
			// info.
			string diags = null;
			switch (@event.GetType())
			{
				case RMAppEventType.AppRejected:
				case RMAppEventType.AttemptFinished:
				case RMAppEventType.AttemptKilled:
				{
					diags = @event.GetDiagnosticMsg();
					break;
				}

				case RMAppEventType.AttemptFailed:
				{
					RMAppFailedAttemptEvent failedEvent = (RMAppFailedAttemptEvent)@event;
					diags = GetAppAttemptFailedDiagnostics(failedEvent);
					break;
				}

				default:
				{
					break;
				}
			}
			ApplicationStateData appState = ApplicationStateData.NewInstance(this.submitTime, 
				this.startTime, this.user, this.submissionContext, stateToBeStored, diags, this.
				storedFinishTime);
			this.rmContext.GetStateStore().UpdateApplicationState(appState);
		}

		private sealed class FinalSavingTransition : RMAppImpl.RMAppTransition
		{
			internal object transitionToDo;

			internal RMAppState targetedFinalState;

			internal RMAppState stateToBeStored;

			public FinalSavingTransition(object transitionToDo, RMAppState targetedFinalState
				)
				: this(transitionToDo, targetedFinalState, targetedFinalState)
			{
			}

			public FinalSavingTransition(object transitionToDo, RMAppState targetedFinalState
				, RMAppState stateToBeStored)
			{
				this.transitionToDo = transitionToDo;
				this.targetedFinalState = targetedFinalState;
				this.stateToBeStored = stateToBeStored;
			}

			public override void Transition(RMAppImpl app, RMAppEvent @event)
			{
				app.RememberTargetTransitionsAndStoreState(@event, transitionToDo, targetedFinalState
					, stateToBeStored);
			}
		}

		private class AttemptUnregisteredTransition : RMAppImpl.RMAppTransition
		{
			public override void Transition(RMAppImpl app, RMAppEvent @event)
			{
				app.finishTime = app.storedFinishTime;
			}
		}

		private class AppFinishedTransition : RMAppImpl.FinalTransition
		{
			public AppFinishedTransition()
				: base(RMAppState.Finished)
			{
			}

			public override void Transition(RMAppImpl app, RMAppEvent @event)
			{
				app.diagnostics.Append(@event.GetDiagnosticMsg());
				base.Transition(app, @event);
			}
		}

		private class AttemptFinishedAtFinalSavingTransition : RMAppImpl.RMAppTransition
		{
			public override void Transition(RMAppImpl app, RMAppEvent @event)
			{
				if (app.targetedFinalState.Equals(RMAppState.Failed) || app.targetedFinalState.Equals
					(RMAppState.Killed))
				{
					// Ignore Attempt_Finished event if we were supposed to reach FAILED
					// FINISHED state
					return;
				}
				// pass in the earlier attempt_unregistered event, as it is needed in
				// AppFinishedFinalStateSavedTransition later on
				app.RememberTargetTransitions(@event, new RMAppImpl.AppFinishedFinalStateSavedTransition
					(app.eventCausingFinalSaving), RMAppState.Finished);
			}
		}

		private class AppFinishedFinalStateSavedTransition : RMAppImpl.RMAppTransition
		{
			internal RMAppEvent attemptUnregistered;

			public AppFinishedFinalStateSavedTransition(RMAppEvent attemptUnregistered)
			{
				this.attemptUnregistered = attemptUnregistered;
			}

			public override void Transition(RMAppImpl app, RMAppEvent @event)
			{
				new RMAppImpl.AttemptUnregisteredTransition().Transition(app, attemptUnregistered
					);
				FinishedTransition.Transition(app, @event);
			}
		}

		private class AppKilledTransition : RMAppImpl.FinalTransition
		{
			public AppKilledTransition()
				: base(RMAppState.Killed)
			{
			}

			public override void Transition(RMAppImpl app, RMAppEvent @event)
			{
				app.diagnostics.Append(@event.GetDiagnosticMsg());
				base.Transition(app, @event);
			}
		}

		private class KillAttemptTransition : RMAppImpl.RMAppTransition
		{
			public override void Transition(RMAppImpl app, RMAppEvent @event)
			{
				app.stateBeforeKilling = app.GetState();
				// Forward app kill diagnostics in the event to kill app attempt.
				// These diagnostics will be returned back in ATTEMPT_KILLED event sent by
				// RMAppAttemptImpl.
				app.handler.Handle(new RMAppAttemptEvent(app.currentAttempt.GetAppAttemptId(), RMAppAttemptEventType
					.Kill, @event.GetDiagnosticMsg()));
			}
		}

		private sealed class AppRejectedTransition : RMAppImpl.FinalTransition
		{
			public AppRejectedTransition()
				: base(RMAppState.Failed)
			{
			}

			public override void Transition(RMAppImpl app, RMAppEvent @event)
			{
				app.diagnostics.Append(@event.GetDiagnosticMsg());
				base.Transition(app, @event);
			}
		}

		private class FinalTransition : RMAppImpl.RMAppTransition
		{
			private readonly RMAppState finalState;

			public FinalTransition(RMAppState finalState)
			{
				this.finalState = finalState;
			}

			public override void Transition(RMAppImpl app, RMAppEvent @event)
			{
				foreach (NodeId nodeId in app.GetRanNodes())
				{
					app.handler.Handle(new RMNodeCleanAppEvent(nodeId, app.applicationId));
				}
				app.finishTime = app.storedFinishTime;
				if (app.finishTime == 0)
				{
					app.finishTime = app.systemClock.GetTime();
				}
				// Recovered apps that are completed were not added to scheduler, so no
				// need to remove them from scheduler.
				if (app.recoveredFinalState == null)
				{
					app.handler.Handle(new AppRemovedSchedulerEvent(app.applicationId, finalState));
				}
				app.handler.Handle(new RMAppManagerEvent(app.applicationId, RMAppManagerEventType
					.AppCompleted));
				app.rmContext.GetRMApplicationHistoryWriter().ApplicationFinished(app, finalState
					);
				app.rmContext.GetSystemMetricsPublisher().AppFinished(app, finalState, app.finishTime
					);
			}
		}

		private int GetNumFailedAppAttempts()
		{
			int completedAttempts = 0;
			long endTime = this.systemClock.GetTime();
			// Do not count AM preemption, hardware failures or NM resync
			// as attempt failure.
			foreach (RMAppAttempt attempt in attempts.Values)
			{
				if (attempt.ShouldCountTowardsMaxAttemptRetry())
				{
					if (this.attemptFailuresValidityInterval <= 0 || (attempt.GetFinishTime() > endTime
						 - this.attemptFailuresValidityInterval))
					{
						completedAttempts++;
					}
				}
			}
			return completedAttempts;
		}

		private sealed class AttemptFailedTransition : MultipleArcTransition<RMAppImpl, RMAppEvent
			, RMAppState>
		{
			private readonly RMAppState initialState;

			public AttemptFailedTransition(RMAppState initialState)
			{
				this.initialState = initialState;
			}

			public RMAppState Transition(RMAppImpl app, RMAppEvent @event)
			{
				int numberOfFailure = app.GetNumFailedAppAttempts();
				Log.Info("The number of failed attempts" + (app.attemptFailuresValidityInterval >
					 0 ? " in previous " + app.attemptFailuresValidityInterval + " milliseconds " : 
					" ") + "is " + numberOfFailure + ". The max attempts is " + app.maxAppAttempts);
				if (!app.submissionContext.GetUnmanagedAM() && numberOfFailure < app.maxAppAttempts)
				{
					if (initialState.Equals(RMAppState.Killing))
					{
						// If this is not last attempt, app should be killed instead of
						// launching a new attempt
						app.RememberTargetTransitionsAndStoreState(@event, new RMAppImpl.AppKilledTransition
							(), RMAppState.Killed, RMAppState.Killed);
						return RMAppState.FinalSaving;
					}
					bool transferStateFromPreviousAttempt;
					RMAppFailedAttemptEvent failedEvent = (RMAppFailedAttemptEvent)@event;
					transferStateFromPreviousAttempt = failedEvent.GetTransferStateFromPreviousAttempt
						();
					RMAppAttempt oldAttempt = app.currentAttempt;
					app.CreateAndStartNewAttempt(transferStateFromPreviousAttempt);
					// Transfer the state from the previous attempt to the current attempt.
					// Note that the previous failed attempt may still be collecting the
					// container events from the scheduler and update its data structures
					// before the new attempt is created. We always transferState for
					// finished containers so that they can be acked to NM,
					// but when pulling finished container we will check this flag again.
					((RMAppAttemptImpl)app.currentAttempt).TransferStateFromPreviousAttempt(oldAttempt
						);
					return initialState;
				}
				else
				{
					if (numberOfFailure >= app.maxAppAttempts)
					{
						app.isNumAttemptsBeyondThreshold = true;
					}
					app.RememberTargetTransitionsAndStoreState(@event, new RMAppImpl.AttemptFailedFinalStateSavedTransition
						(), RMAppState.Failed, RMAppState.Failed);
					return RMAppState.FinalSaving;
				}
			}
		}

		public virtual string GetApplicationType()
		{
			return this.applicationType;
		}

		public virtual ICollection<string> GetApplicationTags()
		{
			return this.applicationTags;
		}

		public virtual bool IsAppFinalStateStored()
		{
			RMAppState state = GetState();
			return state.Equals(RMAppState.Finishing) || state.Equals(RMAppState.Finished) ||
				 state.Equals(RMAppState.Failed) || state.Equals(RMAppState.Killed);
		}

		public virtual YarnApplicationState CreateApplicationState()
		{
			RMAppState rmAppState = GetState();
			// If App is in FINAL_SAVING state, return its previous state.
			if (rmAppState.Equals(RMAppState.FinalSaving))
			{
				rmAppState = stateBeforeFinalSaving;
			}
			if (rmAppState.Equals(RMAppState.Killing))
			{
				rmAppState = stateBeforeKilling;
			}
			return RMServerUtils.CreateApplicationState(rmAppState);
		}

		public static bool IsAppInFinalState(RMApp rmApp)
		{
			RMAppState appState = ((RMAppImpl)rmApp).GetRecoveredFinalState();
			if (appState == null)
			{
				appState = rmApp.GetState();
			}
			return appState == RMAppState.Failed || appState == RMAppState.Finished || appState
				 == RMAppState.Killed;
		}

		private RMAppState GetRecoveredFinalState()
		{
			return this.recoveredFinalState;
		}

		public virtual ICollection<NodeId> GetRanNodes()
		{
			return ranNodes;
		}

		public virtual RMAppMetrics GetRMAppMetrics()
		{
			Resource resourcePreempted = Resource.NewInstance(0, 0);
			int numAMContainerPreempted = 0;
			int numNonAMContainerPreempted = 0;
			long memorySeconds = 0;
			long vcoreSeconds = 0;
			foreach (RMAppAttempt attempt in attempts.Values)
			{
				if (null != attempt)
				{
					RMAppAttemptMetrics attemptMetrics = attempt.GetRMAppAttemptMetrics();
					Resources.AddTo(resourcePreempted, attemptMetrics.GetResourcePreempted());
					numAMContainerPreempted += attemptMetrics.GetIsPreempted() ? 1 : 0;
					numNonAMContainerPreempted += attemptMetrics.GetNumNonAMContainersPreempted();
					// getAggregateAppResourceUsage() will calculate resource usage stats
					// for both running and finished containers.
					AggregateAppResourceUsage resUsage = attempt.GetRMAppAttemptMetrics().GetAggregateAppResourceUsage
						();
					memorySeconds += resUsage.GetMemorySeconds();
					vcoreSeconds += resUsage.GetVcoreSeconds();
				}
			}
			return new RMAppMetrics(resourcePreempted, numNonAMContainerPreempted, numAMContainerPreempted
				, memorySeconds, vcoreSeconds);
		}

		[InterfaceAudience.Private]
		[VisibleForTesting]
		public virtual void SetSystemClock(Clock clock)
		{
			this.systemClock = clock;
		}

		public virtual ReservationId GetReservationId()
		{
			return submissionContext.GetReservationID();
		}

		public virtual ResourceRequest GetAMResourceRequest()
		{
			return this.amReq;
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual Credentials ParseCredentials()
		{
			Credentials credentials = new Credentials();
			DataInputByteBuffer dibb = new DataInputByteBuffer();
			ByteBuffer tokens = submissionContext.GetAMContainerSpec().GetTokens();
			if (tokens != null)
			{
				dibb.Reset(tokens);
				credentials.ReadTokenStorageStream(dibb);
				tokens.Rewind();
			}
			return credentials;
		}
	}
}
