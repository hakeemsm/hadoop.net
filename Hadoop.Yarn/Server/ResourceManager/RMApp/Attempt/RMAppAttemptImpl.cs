using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Org.Apache.Hadoop.Yarn.Security;
using Org.Apache.Hadoop.Yarn.Security.Client;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Amlauncher;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt.Event;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmcontainer;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmnode;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Event;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Security;
using Org.Apache.Hadoop.Yarn.Server.Webproxy;
using Org.Apache.Hadoop.Yarn.State;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Hadoop.Yarn.Webapp.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt
{
	public class RMAppAttemptImpl : RMAppAttempt, Recoverable
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt.RMAppAttemptImpl
			));

		private static readonly RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory
			(null);

		public static readonly Priority AmContainerPriority = recordFactory.NewRecordInstance
			<Priority>();

		static RMAppAttemptImpl()
		{
			AmContainerPriority.SetPriority(0);
		}

		private readonly StateMachine<RMAppAttemptState, RMAppAttemptEventType, RMAppAttemptEvent
			> stateMachine;

		private readonly RMContext rmContext;

		private readonly EventHandler eventHandler;

		private readonly YarnScheduler scheduler;

		private readonly ApplicationMasterService masterService;

		private readonly ReentrantReadWriteLock.ReadLock readLock;

		private readonly ReentrantReadWriteLock.WriteLock writeLock;

		private readonly ApplicationAttemptId applicationAttemptId;

		private readonly ApplicationSubmissionContext submissionContext;

		private Org.Apache.Hadoop.Security.Token.Token<AMRMTokenIdentifier> amrmToken = null;

		private volatile int amrmTokenKeyId = null;

		private SecretKey clientTokenMasterKey = null;

		private ConcurrentMap<NodeId, IList<ContainerStatus>> justFinishedContainers = new 
			ConcurrentHashMap<NodeId, IList<ContainerStatus>>();

		private ConcurrentMap<NodeId, IList<ContainerStatus>> finishedContainersSentToAM = 
			new ConcurrentHashMap<NodeId, IList<ContainerStatus>>();

		private Container masterContainer;

		private float progress = 0;

		private string host = "N/A";

		private int rpcPort = -1;

		private string originalTrackingUrl = "N/A";

		private string proxiedTrackingUrl = "N/A";

		private long startTime = 0;

		private long finishTime = 0;

		private long launchAMStartTime = 0;

		private long launchAMEndTime = 0;

		private FinalApplicationStatus finalStatus = null;

		private readonly StringBuilder diagnostics = new StringBuilder();

		private int amContainerExitStatus = ContainerExitStatus.Invalid;

		private Configuration conf;

		private readonly bool maybeLastAttempt;

		private static readonly RMAppAttemptImpl.ExpiredTransition ExpiredTransition = new 
			RMAppAttemptImpl.ExpiredTransition();

		private RMAppAttemptEvent eventCausingFinalSaving;

		private RMAppAttemptState targetedFinalState;

		private RMAppAttemptState recoveredFinalState;

		private RMAppAttemptState stateBeforeFinalSaving;

		private object transitionTodo;

		private RMAppAttemptMetrics attemptMetrics = null;

		private ResourceRequest amReq = null;

		private static readonly StateMachineFactory<Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt.RMAppAttemptImpl
			, RMAppAttemptState, RMAppAttemptEventType, RMAppAttemptEvent> stateMachineFactory
			 = new StateMachineFactory<Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt.RMAppAttemptImpl
			, RMAppAttemptState, RMAppAttemptEventType, RMAppAttemptEvent>(RMAppAttemptState
			.New).AddTransition(RMAppAttemptState.New, RMAppAttemptState.Submitted, RMAppAttemptEventType
			.Start, new RMAppAttemptImpl.AttemptStartedTransition()).AddTransition(RMAppAttemptState
			.New, RMAppAttemptState.FinalSaving, RMAppAttemptEventType.Kill, new RMAppAttemptImpl.FinalSavingTransition
			(new RMAppAttemptImpl.BaseFinalTransition(RMAppAttemptState.Killed), RMAppAttemptState
			.Killed)).AddTransition(RMAppAttemptState.New, RMAppAttemptState.FinalSaving, RMAppAttemptEventType
			.Registered, new RMAppAttemptImpl.FinalSavingTransition(new RMAppAttemptImpl.UnexpectedAMRegisteredTransition
			(), RMAppAttemptState.Failed)).AddTransition(RMAppAttemptState.New, EnumSet.Of(RMAppAttemptState
			.Finished, RMAppAttemptState.Killed, RMAppAttemptState.Failed, RMAppAttemptState
			.Launched), RMAppAttemptEventType.Recover, new RMAppAttemptImpl.AttemptRecoveredTransition
			()).AddTransition(RMAppAttemptState.Submitted, EnumSet.Of(RMAppAttemptState.LaunchedUnmanagedSaving
			, RMAppAttemptState.Scheduled), RMAppAttemptEventType.AttemptAdded, new RMAppAttemptImpl.ScheduleTransition
			()).AddTransition(RMAppAttemptState.Submitted, RMAppAttemptState.FinalSaving, RMAppAttemptEventType
			.Kill, new RMAppAttemptImpl.FinalSavingTransition(new RMAppAttemptImpl.BaseFinalTransition
			(RMAppAttemptState.Killed), RMAppAttemptState.Killed)).AddTransition(RMAppAttemptState
			.Submitted, RMAppAttemptState.FinalSaving, RMAppAttemptEventType.Registered, new 
			RMAppAttemptImpl.FinalSavingTransition(new RMAppAttemptImpl.UnexpectedAMRegisteredTransition
			(), RMAppAttemptState.Failed)).AddTransition(RMAppAttemptState.Scheduled, EnumSet
			.Of(RMAppAttemptState.AllocatedSaving, RMAppAttemptState.Scheduled), RMAppAttemptEventType
			.ContainerAllocated, new RMAppAttemptImpl.AMContainerAllocatedTransition()).AddTransition
			(RMAppAttemptState.Scheduled, RMAppAttemptState.FinalSaving, RMAppAttemptEventType
			.Kill, new RMAppAttemptImpl.FinalSavingTransition(new RMAppAttemptImpl.BaseFinalTransition
			(RMAppAttemptState.Killed), RMAppAttemptState.Killed)).AddTransition(RMAppAttemptState
			.Scheduled, RMAppAttemptState.FinalSaving, RMAppAttemptEventType.ContainerFinished
			, new RMAppAttemptImpl.FinalSavingTransition(new RMAppAttemptImpl.AMContainerCrashedBeforeRunningTransition
			(), RMAppAttemptState.Failed)).AddTransition(RMAppAttemptState.AllocatedSaving, 
			RMAppAttemptState.Allocated, RMAppAttemptEventType.AttemptNewSaved, new RMAppAttemptImpl.AttemptStoredTransition
			()).AddTransition(RMAppAttemptState.AllocatedSaving, RMAppAttemptState.FinalSaving
			, RMAppAttemptEventType.Kill, new RMAppAttemptImpl.FinalSavingTransition(new RMAppAttemptImpl.BaseFinalTransition
			(RMAppAttemptState.Killed), RMAppAttemptState.Killed)).AddTransition(RMAppAttemptState
			.AllocatedSaving, RMAppAttemptState.FinalSaving, RMAppAttemptEventType.ContainerFinished
			, new RMAppAttemptImpl.FinalSavingTransition(new RMAppAttemptImpl.AMContainerCrashedBeforeRunningTransition
			(), RMAppAttemptState.Failed)).AddTransition(RMAppAttemptState.LaunchedUnmanagedSaving
			, RMAppAttemptState.Launched, RMAppAttemptEventType.AttemptNewSaved, new RMAppAttemptImpl.UnmanagedAMAttemptSavedTransition
			()).AddTransition(RMAppAttemptState.LaunchedUnmanagedSaving, RMAppAttemptState.FinalSaving
			, RMAppAttemptEventType.Registered, new RMAppAttemptImpl.FinalSavingTransition(new 
			RMAppAttemptImpl.UnexpectedAMRegisteredTransition(), RMAppAttemptState.Failed)).
			AddTransition(RMAppAttemptState.LaunchedUnmanagedSaving, RMAppAttemptState.FinalSaving
			, RMAppAttemptEventType.Kill, new RMAppAttemptImpl.FinalSavingTransition(new RMAppAttemptImpl.BaseFinalTransition
			(RMAppAttemptState.Killed), RMAppAttemptState.Killed)).AddTransition(RMAppAttemptState
			.Allocated, RMAppAttemptState.Launched, RMAppAttemptEventType.Launched, new RMAppAttemptImpl.AMLaunchedTransition
			()).AddTransition(RMAppAttemptState.Allocated, RMAppAttemptState.FinalSaving, RMAppAttemptEventType
			.LaunchFailed, new RMAppAttemptImpl.FinalSavingTransition(new RMAppAttemptImpl.LaunchFailedTransition
			(), RMAppAttemptState.Failed)).AddTransition(RMAppAttemptState.Allocated, RMAppAttemptState
			.FinalSaving, RMAppAttemptEventType.Kill, new RMAppAttemptImpl.FinalSavingTransition
			(new RMAppAttemptImpl.KillAllocatedAMTransition(), RMAppAttemptState.Killed)).AddTransition
			(RMAppAttemptState.Allocated, RMAppAttemptState.FinalSaving, RMAppAttemptEventType
			.ContainerFinished, new RMAppAttemptImpl.FinalSavingTransition(new RMAppAttemptImpl.AMContainerCrashedBeforeRunningTransition
			(), RMAppAttemptState.Failed)).AddTransition(RMAppAttemptState.Launched, RMAppAttemptState
			.Running, RMAppAttemptEventType.Registered, new RMAppAttemptImpl.AMRegisteredTransition
			()).AddTransition(RMAppAttemptState.Launched, EnumSet.Of(RMAppAttemptState.Launched
			, RMAppAttemptState.FinalSaving), RMAppAttemptEventType.ContainerFinished, new RMAppAttemptImpl.ContainerFinishedTransition
			(new RMAppAttemptImpl.AMContainerCrashedBeforeRunningTransition(), RMAppAttemptState
			.Launched)).AddTransition(RMAppAttemptState.Launched, RMAppAttemptState.FinalSaving
			, RMAppAttemptEventType.Expire, new RMAppAttemptImpl.FinalSavingTransition(ExpiredTransition
			, RMAppAttemptState.Failed)).AddTransition(RMAppAttemptState.Launched, RMAppAttemptState
			.FinalSaving, RMAppAttemptEventType.Kill, new RMAppAttemptImpl.FinalSavingTransition
			(new RMAppAttemptImpl.FinalTransition(RMAppAttemptState.Killed), RMAppAttemptState
			.Killed)).AddTransition(RMAppAttemptState.Running, EnumSet.Of(RMAppAttemptState.
			FinalSaving, RMAppAttemptState.Finished), RMAppAttemptEventType.Unregistered, new 
			RMAppAttemptImpl.AMUnregisteredTransition()).AddTransition(RMAppAttemptState.Running
			, RMAppAttemptState.Running, RMAppAttemptEventType.StatusUpdate, new RMAppAttemptImpl.StatusUpdateTransition
			()).AddTransition(RMAppAttemptState.Running, RMAppAttemptState.Running, RMAppAttemptEventType
			.ContainerAllocated).AddTransition(RMAppAttemptState.Running, EnumSet.Of(RMAppAttemptState
			.Running, RMAppAttemptState.FinalSaving), RMAppAttemptEventType.ContainerFinished
			, new RMAppAttemptImpl.ContainerFinishedTransition(new RMAppAttemptImpl.AMContainerCrashedAtRunningTransition
			(), RMAppAttemptState.Running)).AddTransition(RMAppAttemptState.Running, RMAppAttemptState
			.FinalSaving, RMAppAttemptEventType.Expire, new RMAppAttemptImpl.FinalSavingTransition
			(ExpiredTransition, RMAppAttemptState.Failed)).AddTransition(RMAppAttemptState.Running
			, RMAppAttemptState.FinalSaving, RMAppAttemptEventType.Kill, new RMAppAttemptImpl.FinalSavingTransition
			(new RMAppAttemptImpl.FinalTransition(RMAppAttemptState.Killed), RMAppAttemptState
			.Killed)).AddTransition(RMAppAttemptState.FinalSaving, EnumSet.Of(RMAppAttemptState
			.Finishing, RMAppAttemptState.Failed, RMAppAttemptState.Killed, RMAppAttemptState
			.Finished), RMAppAttemptEventType.AttemptUpdateSaved, new RMAppAttemptImpl.FinalStateSavedTransition
			()).AddTransition(RMAppAttemptState.FinalSaving, RMAppAttemptState.FinalSaving, 
			RMAppAttemptEventType.ContainerFinished, new RMAppAttemptImpl.ContainerFinishedAtFinalSavingTransition
			()).AddTransition(RMAppAttemptState.FinalSaving, RMAppAttemptState.FinalSaving, 
			RMAppAttemptEventType.Expire, new RMAppAttemptImpl.AMExpiredAtFinalSavingTransition
			()).AddTransition(RMAppAttemptState.FinalSaving, RMAppAttemptState.FinalSaving, 
			EnumSet.Of(RMAppAttemptEventType.Unregistered, RMAppAttemptEventType.StatusUpdate
			, RMAppAttemptEventType.Launched, RMAppAttemptEventType.LaunchFailed, RMAppAttemptEventType
			.ContainerAllocated, RMAppAttemptEventType.AttemptNewSaved, RMAppAttemptEventType
			.Kill)).AddTransition(RMAppAttemptState.Failed, RMAppAttemptState.Failed, RMAppAttemptEventType
			.ContainerFinished, new RMAppAttemptImpl.ContainerFinishedAtFinalStateTransition
			()).AddTransition(RMAppAttemptState.Failed, RMAppAttemptState.Failed, EnumSet.Of
			(RMAppAttemptEventType.Expire, RMAppAttemptEventType.Kill, RMAppAttemptEventType
			.Unregistered, RMAppAttemptEventType.StatusUpdate, RMAppAttemptEventType.ContainerAllocated
			)).AddTransition(RMAppAttemptState.Finishing, EnumSet.Of(RMAppAttemptState.Finishing
			, RMAppAttemptState.Finished), RMAppAttemptEventType.ContainerFinished, new RMAppAttemptImpl.AMFinishingContainerFinishedTransition
			()).AddTransition(RMAppAttemptState.Finishing, RMAppAttemptState.Finished, RMAppAttemptEventType
			.Expire, new RMAppAttemptImpl.FinalTransition(RMAppAttemptState.Finished)).AddTransition
			(RMAppAttemptState.Finishing, RMAppAttemptState.Finishing, EnumSet.Of(RMAppAttemptEventType
			.Unregistered, RMAppAttemptEventType.StatusUpdate, RMAppAttemptEventType.ContainerAllocated
			, RMAppAttemptEventType.Kill)).AddTransition(RMAppAttemptState.Finished, RMAppAttemptState
			.Finished, EnumSet.Of(RMAppAttemptEventType.Expire, RMAppAttemptEventType.Unregistered
			, RMAppAttemptEventType.ContainerAllocated, RMAppAttemptEventType.Kill)).AddTransition
			(RMAppAttemptState.Finished, RMAppAttemptState.Finished, RMAppAttemptEventType.ContainerFinished
			, new RMAppAttemptImpl.ContainerFinishedAtFinalStateTransition()).AddTransition(
			RMAppAttemptState.Killed, RMAppAttemptState.Killed, EnumSet.Of(RMAppAttemptEventType
			.AttemptAdded, RMAppAttemptEventType.Launched, RMAppAttemptEventType.LaunchFailed
			, RMAppAttemptEventType.Expire, RMAppAttemptEventType.Registered, RMAppAttemptEventType
			.ContainerAllocated, RMAppAttemptEventType.Unregistered, RMAppAttemptEventType.Kill
			, RMAppAttemptEventType.StatusUpdate)).AddTransition(RMAppAttemptState.Killed, RMAppAttemptState
			.Killed, RMAppAttemptEventType.ContainerFinished, new RMAppAttemptImpl.ContainerFinishedAtFinalStateTransition
			()).InstallTopology();

		public RMAppAttemptImpl(ApplicationAttemptId appAttemptId, RMContext rmContext, YarnScheduler
			 scheduler, ApplicationMasterService masterService, ApplicationSubmissionContext
			 submissionContext, Configuration conf, bool maybeLastAttempt, ResourceRequest amReq
			)
		{
			// Tracks the previous finished containers that are waiting to be
			// verified as received by the AM. If the AM sends the next allocate
			// request it implicitly acks this list.
			// Set to null initially. Will eventually get set
			// if an RMAppAttemptUnregistrationEvent occurs
			// Since AM preemption, hardware error and NM resync are not counted towards
			// AM failure count, even if this flag is true, a new attempt can still be
			// re-created if this attempt is eventually failed because of preemption,
			// hardware error or NM resync. So this flag indicates that this may be
			// last attempt.
			// Transitions from NEW State
			// Transitions from SUBMITTED state
			// Transitions from SCHEDULED State
			// Transitions from ALLOCATED_SAVING State
			// App could be killed by the client. So need to handle this. 
			// Transitions from LAUNCHED_UNMANAGED_SAVING State
			// attempt should not try to register in this state
			// App could be killed by the client. So need to handle this. 
			// Transitions from ALLOCATED State
			// Transitions from LAUNCHED State
			// Transitions from RUNNING State
			// Transitions from FINAL_SAVING State
			// should be fixed to reject container allocate request at Final
			// Saving in scheduler
			// Transitions from FAILED State
			// For work-preserving AM restart, failed attempt are still capturing
			// CONTAINER_FINISHED event and record the finished containers for the
			// use by the next new attempt.
			// Transitions from FINISHING State
			// ignore Kill as we have already saved the final Finished state in
			// state store.
			// Transitions from FINISHED State
			// Transitions from KILLED State
			this.conf = conf;
			this.applicationAttemptId = appAttemptId;
			this.rmContext = rmContext;
			this.eventHandler = rmContext.GetDispatcher().GetEventHandler();
			this.submissionContext = submissionContext;
			this.scheduler = scheduler;
			this.masterService = masterService;
			ReentrantReadWriteLock Lock = new ReentrantReadWriteLock();
			this.readLock = Lock.ReadLock();
			this.writeLock = Lock.WriteLock();
			this.proxiedTrackingUrl = GenerateProxyUriWithScheme();
			this.maybeLastAttempt = maybeLastAttempt;
			this.stateMachine = stateMachineFactory.Make(this);
			this.attemptMetrics = new RMAppAttemptMetrics(applicationAttemptId, rmContext);
			this.amReq = amReq;
		}

		public virtual ApplicationAttemptId GetAppAttemptId()
		{
			return this.applicationAttemptId;
		}

		public virtual ApplicationSubmissionContext GetSubmissionContext()
		{
			return this.submissionContext;
		}

		public virtual FinalApplicationStatus GetFinalApplicationStatus()
		{
			this.readLock.Lock();
			try
			{
				return this.finalStatus;
			}
			finally
			{
				this.readLock.Unlock();
			}
		}

		public virtual RMAppAttemptState GetAppAttemptState()
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

		public virtual string GetHost()
		{
			this.readLock.Lock();
			try
			{
				return this.host;
			}
			finally
			{
				this.readLock.Unlock();
			}
		}

		public virtual int GetRpcPort()
		{
			this.readLock.Lock();
			try
			{
				return this.rpcPort;
			}
			finally
			{
				this.readLock.Unlock();
			}
		}

		public virtual string GetTrackingUrl()
		{
			this.readLock.Lock();
			try
			{
				return (GetSubmissionContext().GetUnmanagedAM()) ? this.originalTrackingUrl : this
					.proxiedTrackingUrl;
			}
			finally
			{
				this.readLock.Unlock();
			}
		}

		public virtual string GetOriginalTrackingUrl()
		{
			this.readLock.Lock();
			try
			{
				return this.originalTrackingUrl;
			}
			finally
			{
				this.readLock.Unlock();
			}
		}

		public virtual string GetWebProxyBase()
		{
			this.readLock.Lock();
			try
			{
				return ProxyUriUtils.GetPath(applicationAttemptId.GetApplicationId());
			}
			finally
			{
				this.readLock.Unlock();
			}
		}

		private string GenerateProxyUriWithScheme()
		{
			this.readLock.Lock();
			try
			{
				string scheme = WebAppUtils.GetHttpSchemePrefix(conf);
				string proxy = WebAppUtils.GetProxyHostAndPort(conf);
				URI proxyUri = ProxyUriUtils.GetUriFromAMUrl(scheme, proxy);
				URI result = ProxyUriUtils.GetProxyUri(null, proxyUri, applicationAttemptId.GetApplicationId
					());
				return result.ToASCIIString();
			}
			catch (URISyntaxException e)
			{
				Log.Warn("Could not proxify the uri for " + applicationAttemptId.GetApplicationId
					(), e);
				return null;
			}
			finally
			{
				this.readLock.Unlock();
			}
		}

		private void SetTrackingUrlToRMAppPage(RMAppAttemptState stateToBeStored)
		{
			originalTrackingUrl = StringHelper.Pjoin(WebAppUtils.GetResolvedRMWebAppURLWithScheme
				(conf), "cluster", "app", GetAppAttemptId().GetApplicationId());
			switch (stateToBeStored)
			{
				case RMAppAttemptState.Killed:
				case RMAppAttemptState.Failed:
				{
					proxiedTrackingUrl = originalTrackingUrl;
					break;
				}

				default:
				{
					break;
				}
			}
		}

		private void InvalidateAMHostAndPort()
		{
			this.host = "N/A";
			this.rpcPort = -1;
		}

		// This is only used for RMStateStore. Normal operation must invoke the secret
		// manager to get the key and not use the local key directly.
		public virtual SecretKey GetClientTokenMasterKey()
		{
			return this.clientTokenMasterKey;
		}

		public virtual Org.Apache.Hadoop.Security.Token.Token<AMRMTokenIdentifier> GetAMRMToken
			()
		{
			this.readLock.Lock();
			try
			{
				return this.amrmToken;
			}
			finally
			{
				this.readLock.Unlock();
			}
		}

		[InterfaceAudience.Private]
		public virtual void SetAMRMToken(Org.Apache.Hadoop.Security.Token.Token<AMRMTokenIdentifier
			> lastToken)
		{
			this.writeLock.Lock();
			try
			{
				this.amrmToken = lastToken;
				this.amrmTokenKeyId = null;
			}
			finally
			{
				this.writeLock.Unlock();
			}
		}

		[InterfaceAudience.Private]
		public virtual int GetAMRMTokenKeyId()
		{
			int keyId = this.amrmTokenKeyId;
			if (keyId == null)
			{
				this.readLock.Lock();
				try
				{
					if (this.amrmToken == null)
					{
						throw new YarnRuntimeException("Missing AMRM token for " + this.applicationAttemptId
							);
					}
					keyId = this.amrmToken.DecodeIdentifier().GetKeyId();
					this.amrmTokenKeyId = keyId;
				}
				catch (IOException e)
				{
					throw new YarnRuntimeException("AMRM token decode error for " + this.applicationAttemptId
						, e);
				}
				finally
				{
					this.readLock.Unlock();
				}
			}
			return keyId;
		}

		public virtual Org.Apache.Hadoop.Security.Token.Token<ClientToAMTokenIdentifier> 
			CreateClientToken(string client)
		{
			this.readLock.Lock();
			try
			{
				Org.Apache.Hadoop.Security.Token.Token<ClientToAMTokenIdentifier> token = null;
				ClientToAMTokenSecretManagerInRM secretMgr = this.rmContext.GetClientToAMTokenSecretManager
					();
				if (client != null && secretMgr.GetMasterKey(this.applicationAttemptId) != null)
				{
					token = new Org.Apache.Hadoop.Security.Token.Token<ClientToAMTokenIdentifier>(new 
						ClientToAMTokenIdentifier(this.applicationAttemptId, client), secretMgr);
				}
				return token;
			}
			finally
			{
				this.readLock.Unlock();
			}
		}

		public virtual string GetDiagnostics()
		{
			this.readLock.Lock();
			try
			{
				return this.diagnostics.ToString();
			}
			finally
			{
				this.readLock.Unlock();
			}
		}

		public virtual int GetAMContainerExitStatus()
		{
			this.readLock.Lock();
			try
			{
				return this.amContainerExitStatus;
			}
			finally
			{
				this.readLock.Unlock();
			}
		}

		public virtual float GetProgress()
		{
			this.readLock.Lock();
			try
			{
				return this.progress;
			}
			finally
			{
				this.readLock.Unlock();
			}
		}

		[VisibleForTesting]
		public virtual IList<ContainerStatus> GetJustFinishedContainers()
		{
			this.readLock.Lock();
			try
			{
				IList<ContainerStatus> returnList = new AList<ContainerStatus>();
				foreach (ICollection<ContainerStatus> containerStatusList in justFinishedContainers
					.Values)
				{
					Sharpen.Collections.AddAll(returnList, containerStatusList);
				}
				return returnList;
			}
			finally
			{
				this.readLock.Unlock();
			}
		}

		public virtual ConcurrentMap<NodeId, IList<ContainerStatus>> GetJustFinishedContainersReference
			()
		{
			this.readLock.Lock();
			try
			{
				return this.justFinishedContainers;
			}
			finally
			{
				this.readLock.Unlock();
			}
		}

		public virtual ConcurrentMap<NodeId, IList<ContainerStatus>> GetFinishedContainersSentToAMReference
			()
		{
			this.readLock.Lock();
			try
			{
				return this.finishedContainersSentToAM;
			}
			finally
			{
				this.readLock.Unlock();
			}
		}

		public virtual IList<ContainerStatus> PullJustFinishedContainers()
		{
			this.writeLock.Lock();
			try
			{
				IList<ContainerStatus> returnList = new AList<ContainerStatus>();
				// A new allocate means the AM received the previously sent
				// finishedContainers. We can ack this to NM now
				SendFinishedContainersToNM();
				// Mark every containerStatus as being sent to AM though we may return
				// only the ones that belong to the current attempt
				bool keepContainersAcressAttempts = this.submissionContext.GetKeepContainersAcrossApplicationAttempts
					();
				foreach (NodeId nodeId in justFinishedContainers.Keys)
				{
					// Clear and get current values
					IList<ContainerStatus> finishedContainers = justFinishedContainers[nodeId] = new 
						AList<ContainerStatus>();
					if (keepContainersAcressAttempts)
					{
						Sharpen.Collections.AddAll(returnList, finishedContainers);
					}
					else
					{
						// Filter out containers from previous attempt
						foreach (ContainerStatus containerStatus in finishedContainers)
						{
							if (containerStatus.GetContainerId().GetApplicationAttemptId().Equals(this.GetAppAttemptId
								()))
							{
								returnList.AddItem(containerStatus);
							}
						}
					}
					finishedContainersSentToAM.PutIfAbsent(nodeId, new AList<ContainerStatus>());
					Sharpen.Collections.AddAll(finishedContainersSentToAM[nodeId], finishedContainers
						);
				}
				return returnList;
			}
			finally
			{
				this.writeLock.Unlock();
			}
		}

		public virtual Container GetMasterContainer()
		{
			this.readLock.Lock();
			try
			{
				return this.masterContainer;
			}
			finally
			{
				this.readLock.Unlock();
			}
		}

		[InterfaceAudience.Private]
		[VisibleForTesting]
		public virtual void SetMasterContainer(Container container)
		{
			masterContainer = container;
		}

		public virtual void Handle(RMAppAttemptEvent @event)
		{
			this.writeLock.Lock();
			try
			{
				ApplicationAttemptId appAttemptID = @event.GetApplicationAttemptId();
				Log.Debug("Processing event for " + appAttemptID + " of type " + @event.GetType()
					);
				RMAppAttemptState oldState = GetAppAttemptState();
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
				if (oldState != GetAppAttemptState())
				{
					Log.Info(appAttemptID + " State change from " + oldState + " to " + GetAppAttemptState
						());
				}
			}
			finally
			{
				this.writeLock.Unlock();
			}
		}

		public virtual ApplicationResourceUsageReport GetApplicationResourceUsageReport()
		{
			this.readLock.Lock();
			try
			{
				ApplicationResourceUsageReport report = scheduler.GetAppResourceUsageReport(this.
					GetAppAttemptId());
				if (report == null)
				{
					report = RMServerUtils.DummyApplicationResourceUsageReport;
				}
				AggregateAppResourceUsage resUsage = this.attemptMetrics.GetAggregateAppResourceUsage
					();
				report.SetMemorySeconds(resUsage.GetMemorySeconds());
				report.SetVcoreSeconds(resUsage.GetVcoreSeconds());
				return report;
			}
			finally
			{
				this.readLock.Unlock();
			}
		}

		public virtual void Recover(RMStateStore.RMState state)
		{
			ApplicationStateData appState = state.GetApplicationState()[GetAppAttemptId().GetApplicationId
				()];
			ApplicationAttemptStateData attemptState = appState.GetAttempt(GetAppAttemptId());
			System.Diagnostics.Debug.Assert(attemptState != null);
			Log.Info("Recovering attempt: " + GetAppAttemptId() + " with final state: " + attemptState
				.GetState());
			diagnostics.Append("Attempt recovered after RM restart");
			diagnostics.Append(attemptState.GetDiagnostics());
			this.amContainerExitStatus = attemptState.GetAMContainerExitStatus();
			if (amContainerExitStatus == ContainerExitStatus.Preempted)
			{
				this.attemptMetrics.SetIsPreempted();
			}
			Credentials credentials = attemptState.GetAppAttemptTokens();
			SetMasterContainer(attemptState.GetMasterContainer());
			RecoverAppAttemptCredentials(credentials, attemptState.GetState());
			this.recoveredFinalState = attemptState.GetState();
			this.originalTrackingUrl = attemptState.GetFinalTrackingUrl();
			this.finalStatus = attemptState.GetFinalApplicationStatus();
			this.startTime = attemptState.GetStartTime();
			this.finishTime = attemptState.GetFinishTime();
			this.attemptMetrics.UpdateAggregateAppResourceUsage(attemptState.GetMemorySeconds
				(), attemptState.GetVcoreSeconds());
		}

		public virtual void TransferStateFromPreviousAttempt(RMAppAttempt attempt)
		{
			this.justFinishedContainers = attempt.GetJustFinishedContainersReference();
			this.finishedContainersSentToAM = attempt.GetFinishedContainersSentToAMReference(
				);
		}

		private void RecoverAppAttemptCredentials(Credentials appAttemptTokens, RMAppAttemptState
			 state)
		{
			if (appAttemptTokens == null || state == RMAppAttemptState.Failed || state == RMAppAttemptState
				.Finished || state == RMAppAttemptState.Killed)
			{
				return;
			}
			if (UserGroupInformation.IsSecurityEnabled())
			{
				byte[] clientTokenMasterKeyBytes = appAttemptTokens.GetSecretKey(RMStateStore.AmClientTokenMasterKeyName
					);
				if (clientTokenMasterKeyBytes != null)
				{
					clientTokenMasterKey = rmContext.GetClientToAMTokenSecretManager().RegisterMasterKey
						(applicationAttemptId, clientTokenMasterKeyBytes);
				}
			}
			SetAMRMToken(rmContext.GetAMRMTokenSecretManager().CreateAndGetAMRMToken(applicationAttemptId
				));
		}

		private class BaseTransition : SingleArcTransition<RMAppAttemptImpl, RMAppAttemptEvent
			>
		{
			public virtual void Transition(RMAppAttemptImpl appAttempt, RMAppAttemptEvent @event
				)
			{
			}
		}

		private sealed class AttemptStartedTransition : RMAppAttemptImpl.BaseTransition
		{
			public override void Transition(RMAppAttemptImpl appAttempt, RMAppAttemptEvent @event
				)
			{
				bool transferStateFromPreviousAttempt = false;
				if (@event is RMAppStartAttemptEvent)
				{
					transferStateFromPreviousAttempt = ((RMAppStartAttemptEvent)@event).GetTransferStateFromPreviousAttempt
						();
				}
				appAttempt.startTime = Runtime.CurrentTimeMillis();
				// Register with the ApplicationMasterService
				appAttempt.masterService.RegisterAppAttempt(appAttempt.applicationAttemptId);
				if (UserGroupInformation.IsSecurityEnabled())
				{
					appAttempt.clientTokenMasterKey = appAttempt.rmContext.GetClientToAMTokenSecretManager
						().CreateMasterKey(appAttempt.applicationAttemptId);
				}
				// Add the applicationAttempt to the scheduler and inform the scheduler
				// whether to transfer the state from previous attempt.
				appAttempt.eventHandler.Handle(new AppAttemptAddedSchedulerEvent(appAttempt.applicationAttemptId
					, transferStateFromPreviousAttempt));
			}
		}

		private static readonly IList<ContainerId> EmptyContainerReleaseList = new AList<
			ContainerId>();

		private static readonly IList<ResourceRequest> EmptyContainerRequestList = new AList
			<ResourceRequest>();

		public sealed class ScheduleTransition : MultipleArcTransition<RMAppAttemptImpl, 
			RMAppAttemptEvent, RMAppAttemptState>
		{
			public RMAppAttemptState Transition(RMAppAttemptImpl appAttempt, RMAppAttemptEvent
				 @event)
			{
				ApplicationSubmissionContext subCtx = appAttempt.submissionContext;
				if (!subCtx.GetUnmanagedAM())
				{
					// Need reset #containers before create new attempt, because this request
					// will be passed to scheduler, and scheduler will deduct the number after
					// AM container allocated
					// Currently, following fields are all hard code,
					// TODO: change these fields when we want to support
					// priority/resource-name/relax-locality specification for AM containers
					// allocation.
					appAttempt.amReq.SetNumContainers(1);
					appAttempt.amReq.SetPriority(AmContainerPriority);
					appAttempt.amReq.SetResourceName(ResourceRequest.Any);
					appAttempt.amReq.SetRelaxLocality(true);
					// AM resource has been checked when submission
					Allocation amContainerAllocation = appAttempt.scheduler.Allocate(appAttempt.applicationAttemptId
						, Sharpen.Collections.SingletonList(appAttempt.amReq), EmptyContainerReleaseList
						, null, null);
					if (amContainerAllocation != null && amContainerAllocation.GetContainers() != null)
					{
						System.Diagnostics.Debug.Assert((amContainerAllocation.GetContainers().Count == 0
							));
					}
					return RMAppAttemptState.Scheduled;
				}
				else
				{
					// save state and then go to LAUNCHED state
					appAttempt.StoreAttempt();
					return RMAppAttemptState.LaunchedUnmanagedSaving;
				}
			}
		}

		private sealed class AMContainerAllocatedTransition : MultipleArcTransition<RMAppAttemptImpl
			, RMAppAttemptEvent, RMAppAttemptState>
		{
			public RMAppAttemptState Transition(RMAppAttemptImpl appAttempt, RMAppAttemptEvent
				 @event)
			{
				// Acquire the AM container from the scheduler.
				Allocation amContainerAllocation = appAttempt.scheduler.Allocate(appAttempt.applicationAttemptId
					, EmptyContainerRequestList, EmptyContainerReleaseList, null, null);
				// There must be at least one container allocated, because a
				// CONTAINER_ALLOCATED is emitted after an RMContainer is constructed,
				// and is put in SchedulerApplication#newlyAllocatedContainers.
				// Note that YarnScheduler#allocate is not guaranteed to be able to
				// fetch it since container may not be fetchable for some reason like
				// DNS unavailable causing container token not generated. As such, we
				// return to the previous state and keep retry until am container is
				// fetched.
				if (amContainerAllocation.GetContainers().Count == 0)
				{
					appAttempt.RetryFetchingAMContainer(appAttempt);
					return RMAppAttemptState.Scheduled;
				}
				// Set the masterContainer
				appAttempt.SetMasterContainer(amContainerAllocation.GetContainers()[0]);
				RMContainerImpl rmMasterContainer = (RMContainerImpl)appAttempt.scheduler.GetRMContainer
					(appAttempt.GetMasterContainer().GetId());
				rmMasterContainer.SetAMContainer(true);
				// The node set in NMTokenSecrentManager is used for marking whether the
				// NMToken has been issued for this node to the AM.
				// When AM container was allocated to RM itself, the node which allocates
				// this AM container was marked as the NMToken already sent. Thus,
				// clear this node set so that the following allocate requests from AM are
				// able to retrieve the corresponding NMToken.
				appAttempt.rmContext.GetNMTokenSecretManager().ClearNodeSetForAttempt(appAttempt.
					applicationAttemptId);
				appAttempt.GetSubmissionContext().SetResource(appAttempt.GetMasterContainer().GetResource
					());
				appAttempt.StoreAttempt();
				return RMAppAttemptState.AllocatedSaving;
			}
		}

		private void RetryFetchingAMContainer(RMAppAttemptImpl appAttempt)
		{
			// start a new thread so that we are not blocking main dispatcher thread.
			new _Thread_1012(appAttempt).Start();
		}

		private sealed class _Thread_1012 : Sharpen.Thread
		{
			public _Thread_1012(RMAppAttemptImpl appAttempt)
			{
				this.appAttempt = appAttempt;
			}

			public override void Run()
			{
				try
				{
					Sharpen.Thread.Sleep(500);
				}
				catch (Exception)
				{
					RMAppAttemptImpl.Log.Warn("Interrupted while waiting to resend the" + " ContainerAllocated Event."
						);
				}
				appAttempt.eventHandler.Handle(new RMAppAttemptEvent(appAttempt.applicationAttemptId
					, RMAppAttemptEventType.ContainerAllocated));
			}

			private readonly RMAppAttemptImpl appAttempt;
		}

		private sealed class AttemptStoredTransition : RMAppAttemptImpl.BaseTransition
		{
			public override void Transition(RMAppAttemptImpl appAttempt, RMAppAttemptEvent @event
				)
			{
				appAttempt.LaunchAttempt();
			}
		}

		private class AttemptRecoveredTransition : MultipleArcTransition<RMAppAttemptImpl
			, RMAppAttemptEvent, RMAppAttemptState>
		{
			public virtual RMAppAttemptState Transition(RMAppAttemptImpl appAttempt, RMAppAttemptEvent
				 @event)
			{
				/*
				* If last attempt recovered final state is null .. it means attempt was
				* started but AM container may or may not have started / finished.
				* Therefore we should wait for it to finish.
				*/
				if (appAttempt.recoveredFinalState != null)
				{
					appAttempt.progress = 1.0f;
					RMApp rmApp = appAttempt.rmContext.GetRMApps()[appAttempt.GetAppAttemptId().GetApplicationId
						()];
					// We will replay the final attempt only if last attempt is in final
					// state but application is not in final state.
					if (rmApp.GetCurrentAppAttempt() == appAttempt && !RMAppImpl.IsAppInFinalState(rmApp
						))
					{
						// Add the previous finished attempt to scheduler synchronously so
						// that scheduler knows the previous attempt.
						appAttempt.scheduler.Handle(new AppAttemptAddedSchedulerEvent(appAttempt.GetAppAttemptId
							(), false, true));
						(new RMAppAttemptImpl.BaseFinalTransition(appAttempt.recoveredFinalState)).Transition
							(appAttempt, @event);
					}
					return appAttempt.recoveredFinalState;
				}
				else
				{
					// Add the current attempt to the scheduler.
					if (appAttempt.rmContext.IsWorkPreservingRecoveryEnabled())
					{
						// Need to register an app attempt before AM can register
						appAttempt.masterService.RegisterAppAttempt(appAttempt.applicationAttemptId);
						// Add attempt to scheduler synchronously to guarantee scheduler
						// knows attempts before AM or NM re-registers.
						appAttempt.scheduler.Handle(new AppAttemptAddedSchedulerEvent(appAttempt.GetAppAttemptId
							(), false, true));
					}
					/*
					* Since the application attempt's final state is not saved that means
					* for AM container (previous attempt) state must be one of these.
					* 1) AM container may not have been launched (RM failed right before
					* this).
					* 2) AM container was successfully launched but may or may not have
					* registered / unregistered.
					* In whichever case we will wait (by moving attempt into LAUNCHED
					* state) and mark this attempt failed (assuming non work preserving
					* restart) only after
					* 1) Node manager during re-registration heart beats back saying
					* am container finished.
					* 2) OR AMLivelinessMonitor expires this attempt (when am doesn't
					* heart beat back).
					*/
					(new RMAppAttemptImpl.AMLaunchedTransition()).Transition(appAttempt, @event);
					return RMAppAttemptState.Launched;
				}
			}
		}

		private void RememberTargetTransitions(RMAppAttemptEvent @event, object transitionToDo
			, RMAppAttemptState targetFinalState)
		{
			transitionTodo = transitionToDo;
			targetedFinalState = targetFinalState;
			eventCausingFinalSaving = @event;
		}

		private void RememberTargetTransitionsAndStoreState(RMAppAttemptEvent @event, object
			 transitionToDo, RMAppAttemptState targetFinalState, RMAppAttemptState stateToBeStored
			)
		{
			RememberTargetTransitions(@event, transitionToDo, targetFinalState);
			stateBeforeFinalSaving = GetState();
			// As of today, finalState, diagnostics, final-tracking-url and
			// finalAppStatus are the only things that we store into the StateStore
			// AFTER the initial saving on app-attempt-start
			// These fields can be visible from outside only after they are saved in
			// StateStore
			string diags = null;
			// don't leave the tracking URL pointing to a non-existent AM
			SetTrackingUrlToRMAppPage(stateToBeStored);
			string finalTrackingUrl = GetOriginalTrackingUrl();
			FinalApplicationStatus finalStatus = null;
			int exitStatus = ContainerExitStatus.Invalid;
			switch (@event.GetType())
			{
				case RMAppAttemptEventType.LaunchFailed:
				{
					diags = @event.GetDiagnosticMsg();
					break;
				}

				case RMAppAttemptEventType.Registered:
				{
					diags = GetUnexpectedAMRegisteredDiagnostics();
					break;
				}

				case RMAppAttemptEventType.Unregistered:
				{
					RMAppAttemptUnregistrationEvent unregisterEvent = (RMAppAttemptUnregistrationEvent
						)@event;
					diags = unregisterEvent.GetDiagnosticMsg();
					// reset finalTrackingUrl to url sent by am
					finalTrackingUrl = SanitizeTrackingUrl(unregisterEvent.GetFinalTrackingUrl());
					finalStatus = unregisterEvent.GetFinalApplicationStatus();
					break;
				}

				case RMAppAttemptEventType.ContainerFinished:
				{
					RMAppAttemptContainerFinishedEvent finishEvent = (RMAppAttemptContainerFinishedEvent
						)@event;
					diags = GetAMContainerCrashedDiagnostics(finishEvent);
					exitStatus = finishEvent.GetContainerStatus().GetExitStatus();
					break;
				}

				case RMAppAttemptEventType.Kill:
				{
					break;
				}

				case RMAppAttemptEventType.Expire:
				{
					diags = GetAMExpiredDiagnostics(@event);
					break;
				}

				default:
				{
					break;
				}
			}
			AggregateAppResourceUsage resUsage = this.attemptMetrics.GetAggregateAppResourceUsage
				();
			RMStateStore rmStore = rmContext.GetStateStore();
			SetFinishTime(Runtime.CurrentTimeMillis());
			ApplicationAttemptStateData attemptState = ApplicationAttemptStateData.NewInstance
				(applicationAttemptId, GetMasterContainer(), rmStore.GetCredentialsFromAppAttempt
				(this), startTime, stateToBeStored, finalTrackingUrl, diags, finalStatus, exitStatus
				, GetFinishTime(), resUsage.GetMemorySeconds(), resUsage.GetVcoreSeconds());
			Log.Info("Updating application attempt " + applicationAttemptId + " with final state: "
				 + targetedFinalState + ", and exit status: " + exitStatus);
			rmStore.UpdateApplicationAttemptState(attemptState);
		}

		private class FinalSavingTransition : RMAppAttemptImpl.BaseTransition
		{
			internal object transitionToDo;

			internal RMAppAttemptState targetedFinalState;

			public FinalSavingTransition(object transitionToDo, RMAppAttemptState targetedFinalState
				)
			{
				this.transitionToDo = transitionToDo;
				this.targetedFinalState = targetedFinalState;
			}

			public override void Transition(RMAppAttemptImpl appAttempt, RMAppAttemptEvent @event
				)
			{
				// For cases Killed/Failed, targetedFinalState is the same as the state to
				// be stored
				appAttempt.RememberTargetTransitionsAndStoreState(@event, transitionToDo, targetedFinalState
					, targetedFinalState);
			}
		}

		private class FinalStateSavedTransition : MultipleArcTransition<RMAppAttemptImpl, 
			RMAppAttemptEvent, RMAppAttemptState>
		{
			public virtual RMAppAttemptState Transition(RMAppAttemptImpl appAttempt, RMAppAttemptEvent
				 @event)
			{
				RMAppAttemptEvent causeEvent = appAttempt.eventCausingFinalSaving;
				if (appAttempt.transitionTodo is SingleArcTransition)
				{
					((SingleArcTransition)appAttempt.transitionTodo).Transition(appAttempt, causeEvent
						);
				}
				else
				{
					if (appAttempt.transitionTodo is MultipleArcTransition)
					{
						((MultipleArcTransition)appAttempt.transitionTodo).Transition(appAttempt, causeEvent
							);
					}
				}
				return appAttempt.targetedFinalState;
			}
		}

		private class BaseFinalTransition : RMAppAttemptImpl.BaseTransition
		{
			private readonly RMAppAttemptState finalAttemptState;

			public BaseFinalTransition(RMAppAttemptState finalAttemptState)
			{
				this.finalAttemptState = finalAttemptState;
			}

			public override void Transition(RMAppAttemptImpl appAttempt, RMAppAttemptEvent @event
				)
			{
				ApplicationAttemptId appAttemptId = appAttempt.GetAppAttemptId();
				// Tell the AMS. Unregister from the ApplicationMasterService
				appAttempt.masterService.UnregisterAttempt(appAttemptId);
				// Tell the application and the scheduler
				ApplicationId applicationId = appAttemptId.GetApplicationId();
				RMAppEvent appEvent = null;
				bool keepContainersAcrossAppAttempts = false;
				switch (finalAttemptState)
				{
					case RMAppAttemptState.Finished:
					{
						appEvent = new RMAppEvent(applicationId, RMAppEventType.AttemptFinished, appAttempt
							.GetDiagnostics());
						break;
					}

					case RMAppAttemptState.Killed:
					{
						appAttempt.InvalidateAMHostAndPort();
						// Forward diagnostics received in attempt kill event.
						appEvent = new RMAppFailedAttemptEvent(applicationId, RMAppEventType.AttemptKilled
							, @event.GetDiagnosticMsg(), false);
						break;
					}

					case RMAppAttemptState.Failed:
					{
						appAttempt.InvalidateAMHostAndPort();
						if (appAttempt.submissionContext.GetKeepContainersAcrossApplicationAttempts() && 
							!appAttempt.submissionContext.GetUnmanagedAM())
						{
							// See if we should retain containers for non-unmanaged applications
							if (!appAttempt.ShouldCountTowardsMaxAttemptRetry())
							{
								// Premption, hardware failures, NM resync doesn't count towards
								// app-failures and so we should retain containers.
								keepContainersAcrossAppAttempts = true;
							}
							else
							{
								if (!appAttempt.maybeLastAttempt)
								{
									// Not preemption, hardware failures or NM resync.
									// Not last-attempt too - keep containers.
									keepContainersAcrossAppAttempts = true;
								}
							}
						}
						appEvent = new RMAppFailedAttemptEvent(applicationId, RMAppEventType.AttemptFailed
							, appAttempt.GetDiagnostics(), keepContainersAcrossAppAttempts);
						break;
					}

					default:
					{
						Log.Error("Cannot get this state!! Error!!");
						break;
					}
				}
				appAttempt.eventHandler.Handle(appEvent);
				appAttempt.eventHandler.Handle(new AppAttemptRemovedSchedulerEvent(appAttemptId, 
					finalAttemptState, keepContainersAcrossAppAttempts));
				appAttempt.RemoveCredentials(appAttempt);
				appAttempt.rmContext.GetRMApplicationHistoryWriter().ApplicationAttemptFinished(appAttempt
					, finalAttemptState);
				appAttempt.rmContext.GetSystemMetricsPublisher().AppAttemptFinished(appAttempt, finalAttemptState
					, appAttempt.rmContext.GetRMApps()[appAttempt.applicationAttemptId.GetApplicationId
					()], Runtime.CurrentTimeMillis());
			}
		}

		private class AMLaunchedTransition : RMAppAttemptImpl.BaseTransition
		{
			public override void Transition(RMAppAttemptImpl appAttempt, RMAppAttemptEvent @event
				)
			{
				if (@event.GetType() == RMAppAttemptEventType.Launched)
				{
					appAttempt.launchAMEndTime = Runtime.CurrentTimeMillis();
					long delay = appAttempt.launchAMEndTime - appAttempt.launchAMStartTime;
					ClusterMetrics.GetMetrics().AddAMLaunchDelay(delay);
				}
				// Register with AMLivelinessMonitor
				appAttempt.AttemptLaunched();
				// register the ClientTokenMasterKey after it is saved in the store,
				// otherwise client may hold an invalid ClientToken after RM restarts.
				if (UserGroupInformation.IsSecurityEnabled())
				{
					appAttempt.rmContext.GetClientToAMTokenSecretManager().RegisterApplication(appAttempt
						.GetAppAttemptId(), appAttempt.GetClientTokenMasterKey());
				}
			}
		}

		public virtual bool ShouldCountTowardsMaxAttemptRetry()
		{
			try
			{
				this.readLock.Lock();
				int exitStatus = GetAMContainerExitStatus();
				return !(exitStatus == ContainerExitStatus.Preempted || exitStatus == ContainerExitStatus
					.Aborted || exitStatus == ContainerExitStatus.DisksFailed || exitStatus == ContainerExitStatus
					.KilledByResourcemanager);
			}
			finally
			{
				this.readLock.Unlock();
			}
		}

		private sealed class UnmanagedAMAttemptSavedTransition : RMAppAttemptImpl.AMLaunchedTransition
		{
			public override void Transition(RMAppAttemptImpl appAttempt, RMAppAttemptEvent @event
				)
			{
				// create AMRMToken
				appAttempt.amrmToken = appAttempt.rmContext.GetAMRMTokenSecretManager().CreateAndGetAMRMToken
					(appAttempt.applicationAttemptId);
				base.Transition(appAttempt, @event);
			}
		}

		private sealed class LaunchFailedTransition : RMAppAttemptImpl.BaseFinalTransition
		{
			public LaunchFailedTransition()
				: base(RMAppAttemptState.Failed)
			{
			}

			public override void Transition(RMAppAttemptImpl appAttempt, RMAppAttemptEvent @event
				)
			{
				// Use diagnostic from launcher
				appAttempt.diagnostics.Append(@event.GetDiagnosticMsg());
				// Tell the app, scheduler
				base.Transition(appAttempt, @event);
			}
		}

		private sealed class KillAllocatedAMTransition : RMAppAttemptImpl.BaseFinalTransition
		{
			public KillAllocatedAMTransition()
				: base(RMAppAttemptState.Killed)
			{
			}

			public override void Transition(RMAppAttemptImpl appAttempt, RMAppAttemptEvent @event
				)
			{
				// Tell the application and scheduler
				base.Transition(appAttempt, @event);
				// Tell the launcher to cleanup.
				appAttempt.eventHandler.Handle(new AMLauncherEvent(AMLauncherEventType.Cleanup, appAttempt
					));
			}
		}

		private sealed class AMRegisteredTransition : RMAppAttemptImpl.BaseTransition
		{
			public override void Transition(RMAppAttemptImpl appAttempt, RMAppAttemptEvent @event
				)
			{
				long delay = Runtime.CurrentTimeMillis() - appAttempt.launchAMEndTime;
				ClusterMetrics.GetMetrics().AddAMRegisterDelay(delay);
				RMAppAttemptRegistrationEvent registrationEvent = (RMAppAttemptRegistrationEvent)
					@event;
				appAttempt.host = registrationEvent.GetHost();
				appAttempt.rpcPort = registrationEvent.GetRpcport();
				appAttempt.originalTrackingUrl = SanitizeTrackingUrl(registrationEvent.GetTrackingurl
					());
				// Let the app know
				appAttempt.eventHandler.Handle(new RMAppEvent(appAttempt.GetAppAttemptId().GetApplicationId
					(), RMAppEventType.AttemptRegistered));
				// TODO:FIXME: Note for future. Unfortunately we only do a state-store
				// write at AM launch time, so we don't save the AM's tracking URL anywhere
				// as that would mean an extra state-store write. For now, we hope that in
				// work-preserving restart, AMs are forced to reregister.
				appAttempt.rmContext.GetRMApplicationHistoryWriter().ApplicationAttemptStarted(appAttempt
					);
				appAttempt.rmContext.GetSystemMetricsPublisher().AppAttemptRegistered(appAttempt, 
					Runtime.CurrentTimeMillis());
			}
		}

		private sealed class AMContainerCrashedBeforeRunningTransition : RMAppAttemptImpl.BaseFinalTransition
		{
			public AMContainerCrashedBeforeRunningTransition()
				: base(RMAppAttemptState.Failed)
			{
			}

			public override void Transition(RMAppAttemptImpl appAttempt, RMAppAttemptEvent @event
				)
			{
				RMAppAttemptContainerFinishedEvent finishEvent = ((RMAppAttemptContainerFinishedEvent
					)@event);
				// UnRegister from AMLivelinessMonitor
				appAttempt.rmContext.GetAMLivelinessMonitor().Unregister(appAttempt.GetAppAttemptId
					());
				// Setup diagnostic message and exit status
				appAttempt.SetAMContainerCrashedDiagnosticsAndExitStatus(finishEvent);
				// Tell the app, scheduler
				base.Transition(appAttempt, finishEvent);
			}
		}

		private void SetAMContainerCrashedDiagnosticsAndExitStatus(RMAppAttemptContainerFinishedEvent
			 finishEvent)
		{
			ContainerStatus status = finishEvent.GetContainerStatus();
			string diagnostics = GetAMContainerCrashedDiagnostics(finishEvent);
			this.diagnostics.Append(diagnostics);
			this.amContainerExitStatus = status.GetExitStatus();
		}

		private string GetAMContainerCrashedDiagnostics(RMAppAttemptContainerFinishedEvent
			 finishEvent)
		{
			ContainerStatus status = finishEvent.GetContainerStatus();
			StringBuilder diagnosticsBuilder = new StringBuilder();
			diagnosticsBuilder.Append("AM Container for ").Append(finishEvent.GetApplicationAttemptId
				()).Append(" exited with ").Append(" exitCode: ").Append(status.GetExitStatus())
				.Append("\n");
			if (this.GetTrackingUrl() != null)
			{
				diagnosticsBuilder.Append("For more detailed output,").Append(" check application tracking page:"
					).Append(this.GetTrackingUrl()).Append("Then, click on links to logs of each attempt.\n"
					);
			}
			diagnosticsBuilder.Append("Diagnostics: ").Append(status.GetDiagnostics()).Append
				("Failing this attempt");
			return diagnosticsBuilder.ToString();
		}

		private class FinalTransition : RMAppAttemptImpl.BaseFinalTransition
		{
			public FinalTransition(RMAppAttemptState finalAttemptState)
				: base(finalAttemptState)
			{
			}

			public override void Transition(RMAppAttemptImpl appAttempt, RMAppAttemptEvent @event
				)
			{
				appAttempt.progress = 1.0f;
				// Tell the app and the scheduler
				base.Transition(appAttempt, @event);
				// UnRegister from AMLivelinessMonitor. Perhaps for
				// FAILING/KILLED/UnManaged AMs
				appAttempt.rmContext.GetAMLivelinessMonitor().Unregister(appAttempt.GetAppAttemptId
					());
				appAttempt.rmContext.GetAMFinishingMonitor().Unregister(appAttempt.GetAppAttemptId
					());
				if (!appAttempt.submissionContext.GetUnmanagedAM())
				{
					// Tell the launcher to cleanup.
					appAttempt.eventHandler.Handle(new AMLauncherEvent(AMLauncherEventType.Cleanup, appAttempt
						));
				}
			}
		}

		private class ExpiredTransition : RMAppAttemptImpl.FinalTransition
		{
			public ExpiredTransition()
				: base(RMAppAttemptState.Failed)
			{
			}

			public override void Transition(RMAppAttemptImpl appAttempt, RMAppAttemptEvent @event
				)
			{
				appAttempt.diagnostics.Append(GetAMExpiredDiagnostics(@event));
				base.Transition(appAttempt, @event);
			}
		}

		private static string GetAMExpiredDiagnostics(RMAppAttemptEvent @event)
		{
			string diag = "ApplicationMaster for attempt " + @event.GetApplicationAttemptId()
				 + " timed out";
			return diag;
		}

		private class UnexpectedAMRegisteredTransition : RMAppAttemptImpl.BaseFinalTransition
		{
			public UnexpectedAMRegisteredTransition()
				: base(RMAppAttemptState.Failed)
			{
			}

			public override void Transition(RMAppAttemptImpl appAttempt, RMAppAttemptEvent @event
				)
			{
				System.Diagnostics.Debug.Assert(appAttempt.submissionContext.GetUnmanagedAM());
				appAttempt.diagnostics.Append(GetUnexpectedAMRegisteredDiagnostics());
				base.Transition(appAttempt, @event);
			}
		}

		private static string GetUnexpectedAMRegisteredDiagnostics()
		{
			return "Unmanaged AM must register after AM attempt reaches LAUNCHED state.";
		}

		private sealed class StatusUpdateTransition : RMAppAttemptImpl.BaseTransition
		{
			public override void Transition(RMAppAttemptImpl appAttempt, RMAppAttemptEvent @event
				)
			{
				RMAppAttemptStatusupdateEvent statusUpdateEvent = (RMAppAttemptStatusupdateEvent)
					@event;
				// Update progress
				appAttempt.progress = statusUpdateEvent.GetProgress();
				// Ping to AMLivelinessMonitor
				appAttempt.rmContext.GetAMLivelinessMonitor().ReceivedPing(statusUpdateEvent.GetApplicationAttemptId
					());
			}
		}

		private sealed class AMUnregisteredTransition : MultipleArcTransition<RMAppAttemptImpl
			, RMAppAttemptEvent, RMAppAttemptState>
		{
			public RMAppAttemptState Transition(RMAppAttemptImpl appAttempt, RMAppAttemptEvent
				 @event)
			{
				// Tell the app
				if (appAttempt.GetSubmissionContext().GetUnmanagedAM())
				{
					// Unmanaged AMs have no container to wait for, so they skip
					// the FINISHING state and go straight to FINISHED.
					appAttempt.UpdateInfoOnAMUnregister(@event);
					new RMAppAttemptImpl.FinalTransition(RMAppAttemptState.Finished).Transition(appAttempt
						, @event);
					return RMAppAttemptState.Finished;
				}
				// Saving the attempt final state
				appAttempt.RememberTargetTransitionsAndStoreState(@event, new RMAppAttemptImpl.FinalStateSavedAfterAMUnregisterTransition
					(), RMAppAttemptState.Finishing, RMAppAttemptState.Finished);
				ApplicationId applicationId = appAttempt.GetAppAttemptId().GetApplicationId();
				// Tell the app immediately that AM is unregistering so that app itself
				// can save its state as soon as possible. Whether we do it like this, or
				// we wait till AppAttempt is saved, it doesn't make any difference on the
				// app side w.r.t failure conditions. The only event going out of
				// AppAttempt to App after this point of time is AM/AppAttempt Finished.
				appAttempt.eventHandler.Handle(new RMAppEvent(applicationId, RMAppEventType.AttemptUnregistered
					));
				return RMAppAttemptState.FinalSaving;
			}
		}

		private class FinalStateSavedAfterAMUnregisterTransition : RMAppAttemptImpl.BaseTransition
		{
			public override void Transition(RMAppAttemptImpl appAttempt, RMAppAttemptEvent @event
				)
			{
				// Unregister from the AMlivenessMonitor and register with AMFinishingMonitor
				appAttempt.rmContext.GetAMLivelinessMonitor().Unregister(appAttempt.applicationAttemptId
					);
				appAttempt.rmContext.GetAMFinishingMonitor().Register(appAttempt.applicationAttemptId
					);
				// Do not make any more changes to this transition code. Make all changes
				// to the following method. Unless you are absolutely sure that you have
				// stuff to do that shouldn't be used by the callers of the following
				// method.
				appAttempt.UpdateInfoOnAMUnregister(@event);
			}
		}

		private void UpdateInfoOnAMUnregister(RMAppAttemptEvent @event)
		{
			progress = 1.0f;
			RMAppAttemptUnregistrationEvent unregisterEvent = (RMAppAttemptUnregistrationEvent
				)@event;
			diagnostics.Append(unregisterEvent.GetDiagnosticMsg());
			originalTrackingUrl = SanitizeTrackingUrl(unregisterEvent.GetFinalTrackingUrl());
			finalStatus = unregisterEvent.GetFinalApplicationStatus();
		}

		private sealed class ContainerFinishedTransition : MultipleArcTransition<RMAppAttemptImpl
			, RMAppAttemptEvent, RMAppAttemptState>
		{
			private RMAppAttemptImpl.BaseTransition transitionToDo;

			private RMAppAttemptState currentState;

			public ContainerFinishedTransition(RMAppAttemptImpl.BaseTransition transitionToDo
				, RMAppAttemptState currentState)
			{
				// The transition To Do after attempt final state is saved.
				this.transitionToDo = transitionToDo;
				this.currentState = currentState;
			}

			public RMAppAttemptState Transition(RMAppAttemptImpl appAttempt, RMAppAttemptEvent
				 @event)
			{
				RMAppAttemptContainerFinishedEvent containerFinishedEvent = (RMAppAttemptContainerFinishedEvent
					)@event;
				ContainerStatus containerStatus = containerFinishedEvent.GetContainerStatus();
				// Is this container the AmContainer? If the finished container is same as
				// the AMContainer, AppAttempt fails
				if (appAttempt.masterContainer != null && appAttempt.masterContainer.GetId().Equals
					(containerStatus.GetContainerId()))
				{
					appAttempt.SendAMContainerToNM(appAttempt, containerFinishedEvent);
					// Remember the follow up transition and save the final attempt state.
					appAttempt.RememberTargetTransitionsAndStoreState(@event, transitionToDo, RMAppAttemptState
						.Failed, RMAppAttemptState.Failed);
					return RMAppAttemptState.FinalSaving;
				}
				// Add all finished containers so that they can be acked to NM
				AddJustFinishedContainer(appAttempt, containerFinishedEvent);
				return this.currentState;
			}
		}

		// Ack NM to remove finished containers from context.
		private void SendFinishedContainersToNM()
		{
			foreach (NodeId nodeId in finishedContainersSentToAM.Keys)
			{
				// Clear and get current values
				IList<ContainerStatus> currentSentContainers = finishedContainersSentToAM[nodeId]
					 = new AList<ContainerStatus>();
				IList<ContainerId> containerIdList = new AList<ContainerId>(currentSentContainers
					.Count);
				foreach (ContainerStatus containerStatus in currentSentContainers)
				{
					containerIdList.AddItem(containerStatus.GetContainerId());
				}
				eventHandler.Handle(new RMNodeFinishedContainersPulledByAMEvent(nodeId, containerIdList
					));
			}
		}

		// Add am container to the list so that am container instance will be
		// removed from NMContext.
		private void SendAMContainerToNM(RMAppAttemptImpl appAttempt, RMAppAttemptContainerFinishedEvent
			 containerFinishedEvent)
		{
			NodeId nodeId = containerFinishedEvent.GetNodeId();
			finishedContainersSentToAM.PutIfAbsent(nodeId, new AList<ContainerStatus>());
			appAttempt.finishedContainersSentToAM[nodeId].AddItem(containerFinishedEvent.GetContainerStatus
				());
			if (!appAttempt.GetSubmissionContext().GetKeepContainersAcrossApplicationAttempts
				())
			{
				appAttempt.SendFinishedContainersToNM();
			}
		}

		private static void AddJustFinishedContainer(RMAppAttemptImpl appAttempt, RMAppAttemptContainerFinishedEvent
			 containerFinishedEvent)
		{
			appAttempt.justFinishedContainers.PutIfAbsent(containerFinishedEvent.GetNodeId(), 
				new AList<ContainerStatus>());
			appAttempt.justFinishedContainers[containerFinishedEvent.GetNodeId()].AddItem(containerFinishedEvent
				.GetContainerStatus());
		}

		private sealed class ContainerFinishedAtFinalStateTransition : RMAppAttemptImpl.BaseTransition
		{
			public override void Transition(RMAppAttemptImpl appAttempt, RMAppAttemptEvent @event
				)
			{
				RMAppAttemptContainerFinishedEvent containerFinishedEvent = (RMAppAttemptContainerFinishedEvent
					)@event;
				// Normal container. Add it in completed containers list
				AddJustFinishedContainer(appAttempt, containerFinishedEvent);
			}
		}

		private class AMContainerCrashedAtRunningTransition : RMAppAttemptImpl.BaseTransition
		{
			public override void Transition(RMAppAttemptImpl appAttempt, RMAppAttemptEvent @event
				)
			{
				RMAppAttemptContainerFinishedEvent finishEvent = (RMAppAttemptContainerFinishedEvent
					)@event;
				// container associated with AM. must not be unmanaged
				System.Diagnostics.Debug.Assert(appAttempt.submissionContext.GetUnmanagedAM() == 
					false);
				// Setup diagnostic message and exit status
				appAttempt.SetAMContainerCrashedDiagnosticsAndExitStatus(finishEvent);
				new RMAppAttemptImpl.FinalTransition(RMAppAttemptState.Failed).Transition(appAttempt
					, @event);
			}
		}

		private sealed class AMFinishingContainerFinishedTransition : MultipleArcTransition
			<RMAppAttemptImpl, RMAppAttemptEvent, RMAppAttemptState>
		{
			public RMAppAttemptState Transition(RMAppAttemptImpl appAttempt, RMAppAttemptEvent
				 @event)
			{
				RMAppAttemptContainerFinishedEvent containerFinishedEvent = (RMAppAttemptContainerFinishedEvent
					)@event;
				ContainerStatus containerStatus = containerFinishedEvent.GetContainerStatus();
				// Is this container the ApplicationMaster container?
				if (appAttempt.masterContainer.GetId().Equals(containerStatus.GetContainerId()))
				{
					new RMAppAttemptImpl.FinalTransition(RMAppAttemptState.Finished).Transition(appAttempt
						, containerFinishedEvent);
					appAttempt.SendAMContainerToNM(appAttempt, containerFinishedEvent);
					return RMAppAttemptState.Finished;
				}
				// Add all finished containers so that they can be acked to NM.
				AddJustFinishedContainer(appAttempt, containerFinishedEvent);
				return RMAppAttemptState.Finishing;
			}
		}

		private class ContainerFinishedAtFinalSavingTransition : RMAppAttemptImpl.BaseTransition
		{
			public override void Transition(RMAppAttemptImpl appAttempt, RMAppAttemptEvent @event
				)
			{
				RMAppAttemptContainerFinishedEvent containerFinishedEvent = (RMAppAttemptContainerFinishedEvent
					)@event;
				ContainerStatus containerStatus = containerFinishedEvent.GetContainerStatus();
				// If this is the AM container, it means the AM container is finished,
				// but we are not yet acknowledged that the final state has been saved.
				// Thus, we still return FINAL_SAVING state here.
				if (appAttempt.masterContainer.GetId().Equals(containerStatus.GetContainerId()))
				{
					appAttempt.SendAMContainerToNM(appAttempt, containerFinishedEvent);
					if (appAttempt.targetedFinalState.Equals(RMAppAttemptState.Failed) || appAttempt.
						targetedFinalState.Equals(RMAppAttemptState.Killed))
					{
						// ignore Container_Finished Event if we were supposed to reach
						// FAILED/KILLED state.
						return;
					}
					// pass in the earlier AMUnregistered Event also, as this is needed for
					// AMFinishedAfterFinalSavingTransition later on
					appAttempt.RememberTargetTransitions(@event, new RMAppAttemptImpl.AMFinishedAfterFinalSavingTransition
						(appAttempt.eventCausingFinalSaving), RMAppAttemptState.Finished);
					return;
				}
				// Add all finished containers so that they can be acked to NM.
				AddJustFinishedContainer(appAttempt, containerFinishedEvent);
			}
		}

		private class AMFinishedAfterFinalSavingTransition : RMAppAttemptImpl.BaseTransition
		{
			internal RMAppAttemptEvent amUnregisteredEvent;

			public AMFinishedAfterFinalSavingTransition(RMAppAttemptEvent amUnregisteredEvent
				)
			{
				this.amUnregisteredEvent = amUnregisteredEvent;
			}

			public override void Transition(RMAppAttemptImpl appAttempt, RMAppAttemptEvent @event
				)
			{
				appAttempt.UpdateInfoOnAMUnregister(amUnregisteredEvent);
				new RMAppAttemptImpl.FinalTransition(RMAppAttemptState.Finished).Transition(appAttempt
					, @event);
			}
		}

		private class AMExpiredAtFinalSavingTransition : RMAppAttemptImpl.BaseTransition
		{
			public override void Transition(RMAppAttemptImpl appAttempt, RMAppAttemptEvent @event
				)
			{
				if (appAttempt.targetedFinalState.Equals(RMAppAttemptState.Failed) || appAttempt.
					targetedFinalState.Equals(RMAppAttemptState.Killed))
				{
					// ignore Container_Finished Event if we were supposed to reach
					// FAILED/KILLED state.
					return;
				}
				// pass in the earlier AMUnregistered Event also, as this is needed for
				// AMFinishedAfterFinalSavingTransition later on
				appAttempt.RememberTargetTransitions(@event, new RMAppAttemptImpl.AMFinishedAfterFinalSavingTransition
					(appAttempt.eventCausingFinalSaving), RMAppAttemptState.Finished);
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

		public virtual RMAppAttemptState GetState()
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

		public virtual YarnApplicationAttemptState CreateApplicationAttemptState()
		{
			RMAppAttemptState state = GetState();
			// If AppAttempt is in FINAL_SAVING state, return its previous state.
			if (state.Equals(RMAppAttemptState.FinalSaving))
			{
				state = stateBeforeFinalSaving;
			}
			return RMServerUtils.CreateApplicationAttemptState(state);
		}

		private void LaunchAttempt()
		{
			launchAMStartTime = Runtime.CurrentTimeMillis();
			// Send event to launch the AM Container
			eventHandler.Handle(new AMLauncherEvent(AMLauncherEventType.Launch, this));
		}

		private void AttemptLaunched()
		{
			// Register with AMLivelinessMonitor
			rmContext.GetAMLivelinessMonitor().Register(GetAppAttemptId());
		}

		private void StoreAttempt()
		{
			// store attempt data in a non-blocking manner to prevent dispatcher
			// thread starvation and wait for state to be saved
			Log.Info("Storing attempt: AppId: " + GetAppAttemptId().GetApplicationId() + " AttemptId: "
				 + GetAppAttemptId() + " MasterContainer: " + masterContainer);
			rmContext.GetStateStore().StoreNewApplicationAttempt(this);
		}

		private void RemoveCredentials(RMAppAttemptImpl appAttempt)
		{
			// Unregister from the ClientToAMTokenSecretManager
			if (UserGroupInformation.IsSecurityEnabled())
			{
				appAttempt.rmContext.GetClientToAMTokenSecretManager().UnRegisterApplication(appAttempt
					.GetAppAttemptId());
			}
			// Remove the AppAttempt from the AMRMTokenSecretManager
			appAttempt.rmContext.GetAMRMTokenSecretManager().ApplicationMasterFinished(appAttempt
				.GetAppAttemptId());
		}

		private static string SanitizeTrackingUrl(string url)
		{
			return (url == null || url.Trim().IsEmpty()) ? "N/A" : url;
		}

		public virtual ApplicationAttemptReport CreateApplicationAttemptReport()
		{
			this.readLock.Lock();
			ApplicationAttemptReport attemptReport = null;
			try
			{
				// AM container maybe not yet allocated. and also unmangedAM doesn't have
				// am container.
				ContainerId amId = masterContainer == null ? null : masterContainer.GetId();
				attemptReport = ApplicationAttemptReport.NewInstance(this.GetAppAttemptId(), this
					.GetHost(), this.GetRpcPort(), this.GetTrackingUrl(), this.GetOriginalTrackingUrl
					(), this.GetDiagnostics(), YarnApplicationAttemptState.ValueOf(this.GetState().ToString
					()), amId);
			}
			finally
			{
				this.readLock.Unlock();
			}
			return attemptReport;
		}

		// for testing
		public virtual bool MayBeLastAttempt()
		{
			return maybeLastAttempt;
		}

		public virtual RMAppAttemptMetrics GetRMAppAttemptMetrics()
		{
			// didn't use read/write lock here because RMAppAttemptMetrics has its own
			// lock
			return attemptMetrics;
		}

		public virtual long GetFinishTime()
		{
			try
			{
				this.readLock.Lock();
				return this.finishTime;
			}
			finally
			{
				this.readLock.Unlock();
			}
		}

		private void SetFinishTime(long finishTime)
		{
			try
			{
				this.writeLock.Lock();
				this.finishTime = finishTime;
			}
			finally
			{
				this.writeLock.Unlock();
			}
		}
	}
}
