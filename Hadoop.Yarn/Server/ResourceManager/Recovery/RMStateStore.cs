using System;
using System.Collections.Generic;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Token.Delegation;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Security.Client;
using Org.Apache.Hadoop.Yarn.Server.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt;
using Org.Apache.Hadoop.Yarn.State;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery
{
	public abstract class RMStateStore : AbstractService
	{
		protected internal const string RmAppRoot = "RMAppRoot";

		protected internal const string RmDtSecretManagerRoot = "RMDTSecretManagerRoot";

		protected internal const string DelegationKeyPrefix = "DelegationKey_";

		protected internal const string DelegationTokenPrefix = "RMDelegationToken_";

		protected internal const string DelegationTokenSequenceNumberPrefix = "RMDTSequenceNumber_";

		protected internal const string AmrmtokenSecretManagerRoot = "AMRMTokenSecretManagerRoot";

		protected internal const string VersionNode = "RMVersionNode";

		protected internal const string EpochNode = "EpochNode";

		private ResourceManager resourceManager;

		private readonly ReentrantReadWriteLock.ReadLock readLock;

		private readonly ReentrantReadWriteLock.WriteLock writeLock;

		public static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery.RMStateStore
			));

		/// <summary>The enum defines state of RMStateStore.</summary>
		public enum RMStateStoreState
		{
			Active,
			Fenced
		}

		private static readonly StateMachineFactory<Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery.RMStateStore
			, RMStateStore.RMStateStoreState, RMStateStoreEventType, RMStateStoreEvent> stateMachineFactory
			 = new StateMachineFactory<Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery.RMStateStore
			, RMStateStore.RMStateStoreState, RMStateStoreEventType, RMStateStoreEvent>(RMStateStore.RMStateStoreState
			.Active).AddTransition(RMStateStore.RMStateStoreState.Active, EnumSet.Of(RMStateStore.RMStateStoreState
			.Active, RMStateStore.RMStateStoreState.Fenced), RMStateStoreEventType.StoreApp, 
			new RMStateStore.StoreAppTransition()).AddTransition(RMStateStore.RMStateStoreState
			.Active, EnumSet.Of(RMStateStore.RMStateStoreState.Active, RMStateStore.RMStateStoreState
			.Fenced), RMStateStoreEventType.UpdateApp, new RMStateStore.UpdateAppTransition(
			)).AddTransition(RMStateStore.RMStateStoreState.Active, EnumSet.Of(RMStateStore.RMStateStoreState
			.Active, RMStateStore.RMStateStoreState.Fenced), RMStateStoreEventType.RemoveApp
			, new RMStateStore.RemoveAppTransition()).AddTransition(RMStateStore.RMStateStoreState
			.Active, EnumSet.Of(RMStateStore.RMStateStoreState.Active, RMStateStore.RMStateStoreState
			.Fenced), RMStateStoreEventType.StoreAppAttempt, new RMStateStore.StoreAppAttemptTransition
			()).AddTransition(RMStateStore.RMStateStoreState.Active, EnumSet.Of(RMStateStore.RMStateStoreState
			.Active, RMStateStore.RMStateStoreState.Fenced), RMStateStoreEventType.UpdateAppAttempt
			, new RMStateStore.UpdateAppAttemptTransition()).AddTransition(RMStateStore.RMStateStoreState
			.Active, EnumSet.Of(RMStateStore.RMStateStoreState.Active, RMStateStore.RMStateStoreState
			.Fenced), RMStateStoreEventType.StoreMasterkey, new RMStateStore.StoreRMDTMasterKeyTransition
			()).AddTransition(RMStateStore.RMStateStoreState.Active, EnumSet.Of(RMStateStore.RMStateStoreState
			.Active, RMStateStore.RMStateStoreState.Fenced), RMStateStoreEventType.RemoveMasterkey
			, new RMStateStore.RemoveRMDTMasterKeyTransition()).AddTransition(RMStateStore.RMStateStoreState
			.Active, EnumSet.Of(RMStateStore.RMStateStoreState.Active, RMStateStore.RMStateStoreState
			.Fenced), RMStateStoreEventType.StoreDelegationToken, new RMStateStore.StoreRMDTTransition
			()).AddTransition(RMStateStore.RMStateStoreState.Active, EnumSet.Of(RMStateStore.RMStateStoreState
			.Active, RMStateStore.RMStateStoreState.Fenced), RMStateStoreEventType.RemoveDelegationToken
			, new RMStateStore.RemoveRMDTTransition()).AddTransition(RMStateStore.RMStateStoreState
			.Active, EnumSet.Of(RMStateStore.RMStateStoreState.Active, RMStateStore.RMStateStoreState
			.Fenced), RMStateStoreEventType.UpdateDelegationToken, new RMStateStore.UpdateRMDTTransition
			()).AddTransition(RMStateStore.RMStateStoreState.Active, EnumSet.Of(RMStateStore.RMStateStoreState
			.Active, RMStateStore.RMStateStoreState.Fenced), RMStateStoreEventType.UpdateAmrmToken
			, new RMStateStore.StoreOrUpdateAMRMTokenTransition()).AddTransition(RMStateStore.RMStateStoreState
			.Active, RMStateStore.RMStateStoreState.Fenced, RMStateStoreEventType.Fenced).AddTransition
			(RMStateStore.RMStateStoreState.Fenced, RMStateStore.RMStateStoreState.Fenced, EnumSet
			.Of(RMStateStoreEventType.StoreApp, RMStateStoreEventType.UpdateApp, RMStateStoreEventType
			.RemoveApp, RMStateStoreEventType.StoreAppAttempt, RMStateStoreEventType.UpdateAppAttempt
			, RMStateStoreEventType.Fenced, RMStateStoreEventType.StoreMasterkey, RMStateStoreEventType
			.RemoveMasterkey, RMStateStoreEventType.StoreDelegationToken, RMStateStoreEventType
			.RemoveDelegationToken, RMStateStoreEventType.UpdateDelegationToken, RMStateStoreEventType
			.UpdateAmrmToken));

		private readonly StateMachine<RMStateStore.RMStateStoreState, RMStateStoreEventType
			, RMStateStoreEvent> stateMachine;

		private class StoreAppTransition : MultipleArcTransition<RMStateStore, RMStateStoreEvent
			, RMStateStore.RMStateStoreState>
		{
			// constants for RM App state and RMDTSecretManagerState.
			public virtual RMStateStore.RMStateStoreState Transition(RMStateStore store, RMStateStoreEvent
				 @event)
			{
				if (!(@event is RMStateStoreAppEvent))
				{
					// should never happen
					Log.Error("Illegal event type: " + @event.GetType());
					return RMStateStore.RMStateStoreState.Active;
				}
				bool isFenced = false;
				ApplicationStateData appState = ((RMStateStoreAppEvent)@event).GetAppState();
				ApplicationId appId = appState.GetApplicationSubmissionContext().GetApplicationId
					();
				Log.Info("Storing info for app: " + appId);
				try
				{
					store.StoreApplicationStateInternal(appId, appState);
					store.NotifyApplication(new RMAppEvent(appId, RMAppEventType.AppNewSaved));
				}
				catch (Exception e)
				{
					Log.Error("Error storing app: " + appId, e);
					isFenced = store.NotifyStoreOperationFailedInternal(e);
				}
				return FinalState(isFenced);
			}
		}

		private class UpdateAppTransition : MultipleArcTransition<RMStateStore, RMStateStoreEvent
			, RMStateStore.RMStateStoreState>
		{
			public virtual RMStateStore.RMStateStoreState Transition(RMStateStore store, RMStateStoreEvent
				 @event)
			{
				if (!(@event is RMStateUpdateAppEvent))
				{
					// should never happen
					Log.Error("Illegal event type: " + @event.GetType());
					return RMStateStore.RMStateStoreState.Active;
				}
				bool isFenced = false;
				ApplicationStateData appState = ((RMStateUpdateAppEvent)@event).GetAppState();
				ApplicationId appId = appState.GetApplicationSubmissionContext().GetApplicationId
					();
				Log.Info("Updating info for app: " + appId);
				try
				{
					store.UpdateApplicationStateInternal(appId, appState);
					store.NotifyApplication(new RMAppEvent(appId, RMAppEventType.AppUpdateSaved));
				}
				catch (Exception e)
				{
					Log.Error("Error updating app: " + appId, e);
					isFenced = store.NotifyStoreOperationFailedInternal(e);
				}
				return FinalState(isFenced);
			}
		}

		private class RemoveAppTransition : MultipleArcTransition<RMStateStore, RMStateStoreEvent
			, RMStateStore.RMStateStoreState>
		{
			public virtual RMStateStore.RMStateStoreState Transition(RMStateStore store, RMStateStoreEvent
				 @event)
			{
				if (!(@event is RMStateStoreRemoveAppEvent))
				{
					// should never happen
					Log.Error("Illegal event type: " + @event.GetType());
					return RMStateStore.RMStateStoreState.Active;
				}
				bool isFenced = false;
				ApplicationStateData appState = ((RMStateStoreRemoveAppEvent)@event).GetAppState(
					);
				ApplicationId appId = appState.GetApplicationSubmissionContext().GetApplicationId
					();
				Log.Info("Removing info for app: " + appId);
				try
				{
					store.RemoveApplicationStateInternal(appState);
				}
				catch (Exception e)
				{
					Log.Error("Error removing app: " + appId, e);
					isFenced = store.NotifyStoreOperationFailedInternal(e);
				}
				return FinalState(isFenced);
			}
		}

		private class StoreAppAttemptTransition : MultipleArcTransition<RMStateStore, RMStateStoreEvent
			, RMStateStore.RMStateStoreState>
		{
			public virtual RMStateStore.RMStateStoreState Transition(RMStateStore store, RMStateStoreEvent
				 @event)
			{
				if (!(@event is RMStateStoreAppAttemptEvent))
				{
					// should never happen
					Log.Error("Illegal event type: " + @event.GetType());
					return RMStateStore.RMStateStoreState.Active;
				}
				bool isFenced = false;
				ApplicationAttemptStateData attemptState = ((RMStateStoreAppAttemptEvent)@event).
					GetAppAttemptState();
				try
				{
					if (Log.IsDebugEnabled())
					{
						Log.Debug("Storing info for attempt: " + attemptState.GetAttemptId());
					}
					store.StoreApplicationAttemptStateInternal(attemptState.GetAttemptId(), attemptState
						);
					store.NotifyApplicationAttempt(new RMAppAttemptEvent(attemptState.GetAttemptId(), 
						RMAppAttemptEventType.AttemptNewSaved));
				}
				catch (Exception e)
				{
					Log.Error("Error storing appAttempt: " + attemptState.GetAttemptId(), e);
					isFenced = store.NotifyStoreOperationFailedInternal(e);
				}
				return FinalState(isFenced);
			}
		}

		private class UpdateAppAttemptTransition : MultipleArcTransition<RMStateStore, RMStateStoreEvent
			, RMStateStore.RMStateStoreState>
		{
			public virtual RMStateStore.RMStateStoreState Transition(RMStateStore store, RMStateStoreEvent
				 @event)
			{
				if (!(@event is RMStateUpdateAppAttemptEvent))
				{
					// should never happen
					Log.Error("Illegal event type: " + @event.GetType());
					return RMStateStore.RMStateStoreState.Active;
				}
				bool isFenced = false;
				ApplicationAttemptStateData attemptState = ((RMStateUpdateAppAttemptEvent)@event)
					.GetAppAttemptState();
				try
				{
					if (Log.IsDebugEnabled())
					{
						Log.Debug("Updating info for attempt: " + attemptState.GetAttemptId());
					}
					store.UpdateApplicationAttemptStateInternal(attemptState.GetAttemptId(), attemptState
						);
					store.NotifyApplicationAttempt(new RMAppAttemptEvent(attemptState.GetAttemptId(), 
						RMAppAttemptEventType.AttemptUpdateSaved));
				}
				catch (Exception e)
				{
					Log.Error("Error updating appAttempt: " + attemptState.GetAttemptId(), e);
					isFenced = store.NotifyStoreOperationFailedInternal(e);
				}
				return FinalState(isFenced);
			}
		}

		private class StoreRMDTTransition : MultipleArcTransition<RMStateStore, RMStateStoreEvent
			, RMStateStore.RMStateStoreState>
		{
			public virtual RMStateStore.RMStateStoreState Transition(RMStateStore store, RMStateStoreEvent
				 @event)
			{
				if (!(@event is RMStateStoreRMDTEvent))
				{
					// should never happen
					Log.Error("Illegal event type: " + @event.GetType());
					return RMStateStore.RMStateStoreState.Active;
				}
				bool isFenced = false;
				RMStateStoreRMDTEvent dtEvent = (RMStateStoreRMDTEvent)@event;
				try
				{
					Log.Info("Storing RMDelegationToken and SequenceNumber");
					store.StoreRMDelegationTokenState(dtEvent.GetRmDTIdentifier(), dtEvent.GetRenewDate
						());
				}
				catch (Exception e)
				{
					Log.Error("Error While Storing RMDelegationToken and SequenceNumber ", e);
					isFenced = store.NotifyStoreOperationFailedInternal(e);
				}
				return FinalState(isFenced);
			}
		}

		private class RemoveRMDTTransition : MultipleArcTransition<RMStateStore, RMStateStoreEvent
			, RMStateStore.RMStateStoreState>
		{
			public virtual RMStateStore.RMStateStoreState Transition(RMStateStore store, RMStateStoreEvent
				 @event)
			{
				if (!(@event is RMStateStoreRMDTEvent))
				{
					// should never happen
					Log.Error("Illegal event type: " + @event.GetType());
					return RMStateStore.RMStateStoreState.Active;
				}
				bool isFenced = false;
				RMStateStoreRMDTEvent dtEvent = (RMStateStoreRMDTEvent)@event;
				try
				{
					Log.Info("Removing RMDelegationToken and SequenceNumber");
					store.RemoveRMDelegationTokenState(dtEvent.GetRmDTIdentifier());
				}
				catch (Exception e)
				{
					Log.Error("Error While Removing RMDelegationToken and SequenceNumber ", e);
					isFenced = store.NotifyStoreOperationFailedInternal(e);
				}
				return FinalState(isFenced);
			}
		}

		private class UpdateRMDTTransition : MultipleArcTransition<RMStateStore, RMStateStoreEvent
			, RMStateStore.RMStateStoreState>
		{
			public virtual RMStateStore.RMStateStoreState Transition(RMStateStore store, RMStateStoreEvent
				 @event)
			{
				if (!(@event is RMStateStoreRMDTEvent))
				{
					// should never happen
					Log.Error("Illegal event type: " + @event.GetType());
					return RMStateStore.RMStateStoreState.Active;
				}
				bool isFenced = false;
				RMStateStoreRMDTEvent dtEvent = (RMStateStoreRMDTEvent)@event;
				try
				{
					Log.Info("Updating RMDelegationToken and SequenceNumber");
					store.UpdateRMDelegationTokenState(dtEvent.GetRmDTIdentifier(), dtEvent.GetRenewDate
						());
				}
				catch (Exception e)
				{
					Log.Error("Error While Updating RMDelegationToken and SequenceNumber ", e);
					isFenced = store.NotifyStoreOperationFailedInternal(e);
				}
				return FinalState(isFenced);
			}
		}

		private class StoreRMDTMasterKeyTransition : MultipleArcTransition<RMStateStore, 
			RMStateStoreEvent, RMStateStore.RMStateStoreState>
		{
			public virtual RMStateStore.RMStateStoreState Transition(RMStateStore store, RMStateStoreEvent
				 @event)
			{
				if (!(@event is RMStateStoreRMDTMasterKeyEvent))
				{
					// should never happen
					Log.Error("Illegal event type: " + @event.GetType());
					return RMStateStore.RMStateStoreState.Active;
				}
				bool isFenced = false;
				RMStateStoreRMDTMasterKeyEvent dtEvent = (RMStateStoreRMDTMasterKeyEvent)@event;
				try
				{
					Log.Info("Storing RMDTMasterKey.");
					store.StoreRMDTMasterKeyState(dtEvent.GetDelegationKey());
				}
				catch (Exception e)
				{
					Log.Error("Error While Storing RMDTMasterKey.", e);
					isFenced = store.NotifyStoreOperationFailedInternal(e);
				}
				return FinalState(isFenced);
			}
		}

		private class RemoveRMDTMasterKeyTransition : MultipleArcTransition<RMStateStore, 
			RMStateStoreEvent, RMStateStore.RMStateStoreState>
		{
			public virtual RMStateStore.RMStateStoreState Transition(RMStateStore store, RMStateStoreEvent
				 @event)
			{
				if (!(@event is RMStateStoreRMDTMasterKeyEvent))
				{
					// should never happen
					Log.Error("Illegal event type: " + @event.GetType());
					return RMStateStore.RMStateStoreState.Active;
				}
				bool isFenced = false;
				RMStateStoreRMDTMasterKeyEvent dtEvent = (RMStateStoreRMDTMasterKeyEvent)@event;
				try
				{
					Log.Info("Removing RMDTMasterKey.");
					store.RemoveRMDTMasterKeyState(dtEvent.GetDelegationKey());
				}
				catch (Exception e)
				{
					Log.Error("Error While Removing RMDTMasterKey.", e);
					isFenced = store.NotifyStoreOperationFailedInternal(e);
				}
				return FinalState(isFenced);
			}
		}

		private class StoreOrUpdateAMRMTokenTransition : MultipleArcTransition<RMStateStore
			, RMStateStoreEvent, RMStateStore.RMStateStoreState>
		{
			public virtual RMStateStore.RMStateStoreState Transition(RMStateStore store, RMStateStoreEvent
				 @event)
			{
				if (!(@event is RMStateStoreAMRMTokenEvent))
				{
					// should never happen
					Log.Error("Illegal event type: " + @event.GetType());
					return RMStateStore.RMStateStoreState.Active;
				}
				RMStateStoreAMRMTokenEvent amrmEvent = (RMStateStoreAMRMTokenEvent)@event;
				bool isFenced = false;
				try
				{
					Log.Info("Updating AMRMToken");
					store.StoreOrUpdateAMRMTokenSecretManagerState(amrmEvent.GetAmrmTokenSecretManagerState
						(), amrmEvent.IsUpdate());
				}
				catch (Exception e)
				{
					Log.Error("Error storing info for AMRMTokenSecretManager", e);
					isFenced = store.NotifyStoreOperationFailedInternal(e);
				}
				return FinalState(isFenced);
			}
		}

		private static RMStateStore.RMStateStoreState FinalState(bool isFenced)
		{
			return isFenced ? RMStateStore.RMStateStoreState.Fenced : RMStateStore.RMStateStoreState
				.Active;
		}

		public RMStateStore()
			: base(typeof(RMStateStore).FullName)
		{
			ReentrantReadWriteLock Lock = new ReentrantReadWriteLock();
			this.readLock = Lock.ReadLock();
			this.writeLock = Lock.WriteLock();
			stateMachine = stateMachineFactory.Make(this);
		}

		public class RMDTSecretManagerState
		{
			internal IDictionary<RMDelegationTokenIdentifier, long> delegationTokenState = new 
				Dictionary<RMDelegationTokenIdentifier, long>();

			internal ICollection<DelegationKey> masterKeyState = new HashSet<DelegationKey>();

			internal int dtSequenceNumber = 0;

			// DTIdentifier -> renewDate
			public virtual IDictionary<RMDelegationTokenIdentifier, long> GetTokenState()
			{
				return delegationTokenState;
			}

			public virtual ICollection<DelegationKey> GetMasterKeyState()
			{
				return masterKeyState;
			}

			public virtual int GetDTSequenceNumber()
			{
				return dtSequenceNumber;
			}
		}

		/// <summary>State of the ResourceManager</summary>
		public class RMState
		{
			internal IDictionary<ApplicationId, ApplicationStateData> appState = new SortedDictionary
				<ApplicationId, ApplicationStateData>();

			internal RMStateStore.RMDTSecretManagerState rmSecretManagerState = new RMStateStore.RMDTSecretManagerState
				();

			internal AMRMTokenSecretManagerState amrmTokenSecretManagerState = null;

			public virtual IDictionary<ApplicationId, ApplicationStateData> GetApplicationState
				()
			{
				return appState;
			}

			public virtual RMStateStore.RMDTSecretManagerState GetRMDTSecretManagerState()
			{
				return rmSecretManagerState;
			}

			public virtual AMRMTokenSecretManagerState GetAMRMTokenSecretManagerState()
			{
				return amrmTokenSecretManagerState;
			}
		}

		private Dispatcher rmDispatcher;

		/// <summary>
		/// Dispatcher used to send state operation completion events to
		/// ResourceManager services
		/// </summary>
		public virtual void SetRMDispatcher(Dispatcher dispatcher)
		{
			this.rmDispatcher = dispatcher;
		}

		internal AsyncDispatcher dispatcher;

		/// <exception cref="System.Exception"/>
		protected override void ServiceInit(Configuration conf)
		{
			// create async handler
			dispatcher = new AsyncDispatcher();
			dispatcher.Init(conf);
			dispatcher.Register(typeof(RMStateStoreEventType), new RMStateStore.ForwardingEventHandler
				(this));
			dispatcher.SetDrainEventsOnStop();
			InitInternal(conf);
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStart()
		{
			dispatcher.Start();
			StartInternal();
		}

		/// <summary>Derived classes initialize themselves using this method.</summary>
		/// <exception cref="System.Exception"/>
		protected internal abstract void InitInternal(Configuration conf);

		/// <summary>Derived classes start themselves using this method.</summary>
		/// <remarks>
		/// Derived classes start themselves using this method.
		/// The base class is started and the event dispatcher is ready to use at
		/// this point
		/// </remarks>
		/// <exception cref="System.Exception"/>
		protected internal abstract void StartInternal();

		/// <exception cref="System.Exception"/>
		protected override void ServiceStop()
		{
			dispatcher.Stop();
			CloseInternal();
		}

		/// <summary>Derived classes close themselves using this method.</summary>
		/// <remarks>
		/// Derived classes close themselves using this method.
		/// The base class will be closed and the event dispatcher will be shutdown
		/// after this
		/// </remarks>
		/// <exception cref="System.Exception"/>
		protected internal abstract void CloseInternal();

		/// <summary>1) Versioning scheme: major.minor.</summary>
		/// <remarks>
		/// 1) Versioning scheme: major.minor. For e.g. 1.0, 1.1, 1.2...1.25, 2.0 etc.
		/// 2) Any incompatible change of state-store is a major upgrade, and any
		/// compatible change of state-store is a minor upgrade.
		/// 3) If theres's no version, treat it as CURRENT_VERSION_INFO.
		/// 4) Within a minor upgrade, say 1.1 to 1.2:
		/// overwrite the version info and proceed as normal.
		/// 5) Within a major upgrade, say 1.2 to 2.0:
		/// throw exception and indicate user to use a separate upgrade tool to
		/// upgrade RM state.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void CheckVersion()
		{
			Version loadedVersion = LoadVersion();
			Log.Info("Loaded RM state version info " + loadedVersion);
			if (loadedVersion != null && loadedVersion.Equals(GetCurrentVersion()))
			{
				return;
			}
			// if there is no version info, treat it as CURRENT_VERSION_INFO;
			if (loadedVersion == null)
			{
				loadedVersion = GetCurrentVersion();
			}
			if (loadedVersion.IsCompatibleTo(GetCurrentVersion()))
			{
				Log.Info("Storing RM state version info " + GetCurrentVersion());
				StoreVersion();
			}
			else
			{
				throw new RMStateVersionIncompatibleException("Expecting RM state version " + GetCurrentVersion
					() + ", but loading version " + loadedVersion);
			}
		}

		/// <summary>
		/// Derived class use this method to load the version information from state
		/// store.
		/// </summary>
		/// <exception cref="System.Exception"/>
		protected internal abstract Version LoadVersion();

		/// <summary>Derived class use this method to store the version information.</summary>
		/// <exception cref="System.Exception"/>
		protected internal abstract void StoreVersion();

		/// <summary>Get the current version of the underlying state store.</summary>
		protected internal abstract Version GetCurrentVersion();

		/// <summary>Get the current epoch of RM and increment the value.</summary>
		/// <exception cref="System.Exception"/>
		public abstract long GetAndIncrementEpoch();

		/// <summary>
		/// Blocking API
		/// The derived class must recover state from the store and return a new
		/// RMState object populated with that state
		/// This must not be called on the dispatcher thread
		/// </summary>
		/// <exception cref="System.Exception"/>
		public abstract RMStateStore.RMState LoadState();

		/// <summary>
		/// Non-Blocking API
		/// ResourceManager services use this to store the application's state
		/// This does not block the dispatcher threads
		/// RMAppStoredEvent will be sent on completion to notify the RMApp
		/// </summary>
		public virtual void StoreNewApplication(RMApp app)
		{
			lock (this)
			{
				ApplicationSubmissionContext context = app.GetApplicationSubmissionContext();
				System.Diagnostics.Debug.Assert(context is ApplicationSubmissionContextPBImpl);
				ApplicationStateData appState = ApplicationStateData.NewInstance(app.GetSubmitTime
					(), app.GetStartTime(), context, app.GetUser());
				dispatcher.GetEventHandler().Handle(new RMStateStoreAppEvent(appState));
			}
		}

		public virtual void UpdateApplicationState(ApplicationStateData appState)
		{
			lock (this)
			{
				dispatcher.GetEventHandler().Handle(new RMStateUpdateAppEvent(appState));
			}
		}

		public virtual void UpdateFencedState()
		{
			HandleStoreEvent(new RMStateStoreEvent(RMStateStoreEventType.Fenced));
		}

		/// <summary>
		/// Blocking API
		/// Derived classes must implement this method to store the state of an
		/// application.
		/// </summary>
		/// <exception cref="System.Exception"/>
		protected internal abstract void StoreApplicationStateInternal(ApplicationId appId
			, ApplicationStateData appStateData);

		/// <exception cref="System.Exception"/>
		protected internal abstract void UpdateApplicationStateInternal(ApplicationId appId
			, ApplicationStateData appStateData);

		public virtual void StoreNewApplicationAttempt(RMAppAttempt appAttempt)
		{
			lock (this)
			{
				Credentials credentials = GetCredentialsFromAppAttempt(appAttempt);
				AggregateAppResourceUsage resUsage = appAttempt.GetRMAppAttemptMetrics().GetAggregateAppResourceUsage
					();
				ApplicationAttemptStateData attemptState = ApplicationAttemptStateData.NewInstance
					(appAttempt.GetAppAttemptId(), appAttempt.GetMasterContainer(), credentials, appAttempt
					.GetStartTime(), resUsage.GetMemorySeconds(), resUsage.GetVcoreSeconds());
				dispatcher.GetEventHandler().Handle(new RMStateStoreAppAttemptEvent(attemptState)
					);
			}
		}

		public virtual void UpdateApplicationAttemptState(ApplicationAttemptStateData attemptState
			)
		{
			lock (this)
			{
				dispatcher.GetEventHandler().Handle(new RMStateUpdateAppAttemptEvent(attemptState
					));
			}
		}

		/// <summary>
		/// Blocking API
		/// Derived classes must implement this method to store the state of an
		/// application attempt
		/// </summary>
		/// <exception cref="System.Exception"/>
		protected internal abstract void StoreApplicationAttemptStateInternal(ApplicationAttemptId
			 attemptId, ApplicationAttemptStateData attemptStateData);

		/// <exception cref="System.Exception"/>
		protected internal abstract void UpdateApplicationAttemptStateInternal(ApplicationAttemptId
			 attemptId, ApplicationAttemptStateData attemptStateData);

		/// <summary>
		/// RMDTSecretManager call this to store the state of a delegation token
		/// and sequence number
		/// </summary>
		public virtual void StoreRMDelegationToken(RMDelegationTokenIdentifier rmDTIdentifier
			, long renewDate)
		{
			HandleStoreEvent(new RMStateStoreRMDTEvent(rmDTIdentifier, renewDate, RMStateStoreEventType
				.StoreDelegationToken));
		}

		/// <summary>
		/// Blocking API
		/// Derived classes must implement this method to store the state of
		/// RMDelegationToken and sequence number
		/// </summary>
		/// <exception cref="System.Exception"/>
		protected internal abstract void StoreRMDelegationTokenState(RMDelegationTokenIdentifier
			 rmDTIdentifier, long renewDate);

		/// <summary>RMDTSecretManager call this to remove the state of a delegation token</summary>
		public virtual void RemoveRMDelegationToken(RMDelegationTokenIdentifier rmDTIdentifier
			)
		{
			HandleStoreEvent(new RMStateStoreRMDTEvent(rmDTIdentifier, null, RMStateStoreEventType
				.RemoveDelegationToken));
		}

		/// <summary>
		/// Blocking API
		/// Derived classes must implement this method to remove the state of RMDelegationToken
		/// </summary>
		/// <exception cref="System.Exception"/>
		protected internal abstract void RemoveRMDelegationTokenState(RMDelegationTokenIdentifier
			 rmDTIdentifier);

		/// <summary>
		/// RMDTSecretManager call this to update the state of a delegation token
		/// and sequence number
		/// </summary>
		public virtual void UpdateRMDelegationToken(RMDelegationTokenIdentifier rmDTIdentifier
			, long renewDate)
		{
			HandleStoreEvent(new RMStateStoreRMDTEvent(rmDTIdentifier, renewDate, RMStateStoreEventType
				.UpdateDelegationToken));
		}

		/// <summary>
		/// Blocking API
		/// Derived classes must implement this method to update the state of
		/// RMDelegationToken and sequence number
		/// </summary>
		/// <exception cref="System.Exception"/>
		protected internal abstract void UpdateRMDelegationTokenState(RMDelegationTokenIdentifier
			 rmDTIdentifier, long renewDate);

		/// <summary>RMDTSecretManager call this to store the state of a master key</summary>
		public virtual void StoreRMDTMasterKey(DelegationKey delegationKey)
		{
			HandleStoreEvent(new RMStateStoreRMDTMasterKeyEvent(delegationKey, RMStateStoreEventType
				.StoreMasterkey));
		}

		/// <summary>
		/// Blocking API
		/// Derived classes must implement this method to store the state of
		/// DelegationToken Master Key
		/// </summary>
		/// <exception cref="System.Exception"/>
		protected internal abstract void StoreRMDTMasterKeyState(DelegationKey delegationKey
			);

		/// <summary>RMDTSecretManager call this to remove the state of a master key</summary>
		public virtual void RemoveRMDTMasterKey(DelegationKey delegationKey)
		{
			HandleStoreEvent(new RMStateStoreRMDTMasterKeyEvent(delegationKey, RMStateStoreEventType
				.RemoveMasterkey));
		}

		/// <summary>
		/// Blocking API
		/// Derived classes must implement this method to remove the state of
		/// DelegationToken Master Key
		/// </summary>
		/// <exception cref="System.Exception"/>
		protected internal abstract void RemoveRMDTMasterKeyState(DelegationKey delegationKey
			);

		/// <summary>
		/// Blocking API Derived classes must implement this method to store or update
		/// the state of AMRMToken Master Key
		/// </summary>
		/// <exception cref="System.Exception"/>
		protected internal abstract void StoreOrUpdateAMRMTokenSecretManagerState(AMRMTokenSecretManagerState
			 amrmTokenSecretManagerState, bool isUpdate);

		/// <summary>Store or Update state of AMRMToken Master Key</summary>
		public virtual void StoreOrUpdateAMRMTokenSecretManager(AMRMTokenSecretManagerState
			 amrmTokenSecretManagerState, bool isUpdate)
		{
			HandleStoreEvent(new RMStateStoreAMRMTokenEvent(amrmTokenSecretManagerState, isUpdate
				, RMStateStoreEventType.UpdateAmrmToken));
		}

		/// <summary>
		/// Non-blocking API
		/// ResourceManager services call this to remove an application from the state
		/// store
		/// This does not block the dispatcher threads
		/// There is no notification of completion for this operation.
		/// </summary>
		public virtual void RemoveApplication(RMApp app)
		{
			lock (this)
			{
				ApplicationStateData appState = ApplicationStateData.NewInstance(app.GetSubmitTime
					(), app.GetStartTime(), app.GetApplicationSubmissionContext(), app.GetUser());
				foreach (RMAppAttempt appAttempt in app.GetAppAttempts().Values)
				{
					appState.attempts[appAttempt.GetAppAttemptId()] = null;
				}
				dispatcher.GetEventHandler().Handle(new RMStateStoreRemoveAppEvent(appState));
			}
		}

		/// <summary>
		/// Blocking API
		/// Derived classes must implement this method to remove the state of an
		/// application and its attempts
		/// </summary>
		/// <exception cref="System.Exception"/>
		protected internal abstract void RemoveApplicationStateInternal(ApplicationStateData
			 appState);

		public static readonly Text AmRmTokenService = new Text("AM_RM_TOKEN_SERVICE");

		public static readonly Text AmClientTokenMasterKeyName = new Text("YARN_CLIENT_TOKEN_MASTER_KEY"
			);

		// TODO: This should eventually become cluster-Id + "AM_RM_TOKEN_SERVICE". See
		// YARN-1779
		public virtual Credentials GetCredentialsFromAppAttempt(RMAppAttempt appAttempt)
		{
			Credentials credentials = new Credentials();
			SecretKey clientTokenMasterKey = appAttempt.GetClientTokenMasterKey();
			if (clientTokenMasterKey != null)
			{
				credentials.AddSecretKey(AmClientTokenMasterKeyName, clientTokenMasterKey.GetEncoded
					());
			}
			return credentials;
		}

		[VisibleForTesting]
		protected internal virtual bool IsFencedState()
		{
			return (RMStateStore.RMStateStoreState.Fenced == GetRMStateStoreState());
		}

		// Dispatcher related code
		protected internal virtual void HandleStoreEvent(RMStateStoreEvent @event)
		{
			this.writeLock.Lock();
			try
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Processing event of type " + @event.GetType());
				}
				RMStateStore.RMStateStoreState oldState = GetRMStateStoreState();
				this.stateMachine.DoTransition(@event.GetType(), @event);
				if (oldState != GetRMStateStoreState())
				{
					Log.Info("RMStateStore state change from " + oldState + " to " + GetRMStateStoreState
						());
				}
			}
			catch (InvalidStateTransitonException e)
			{
				Log.Error("Can't handle this event at current state", e);
			}
			finally
			{
				this.writeLock.Unlock();
			}
		}

		/// <summary>
		/// This method is called to notify the ResourceManager that the store
		/// operation has failed.
		/// </summary>
		/// <param name="failureCause">the exception due to which the operation failed</param>
		protected internal virtual void NotifyStoreOperationFailed(Exception failureCause
			)
		{
			if (IsFencedState())
			{
				return;
			}
			if (NotifyStoreOperationFailedInternal(failureCause))
			{
				UpdateFencedState();
			}
		}

		private bool NotifyStoreOperationFailedInternal(Exception failureCause)
		{
			bool isFenced = false;
			Log.Error("State store operation failed ", failureCause);
			if (HAUtil.IsHAEnabled(GetConfig()))
			{
				Log.Warn("State-store fenced ! Transitioning RM to standby");
				isFenced = true;
				Sharpen.Thread standByTransitionThread = new Sharpen.Thread(new RMStateStore.StandByTransitionThread
					(this));
				standByTransitionThread.SetName("StandByTransitionThread Handler");
				standByTransitionThread.Start();
			}
			else
			{
				if (YarnConfiguration.ShouldRMFailFast(GetConfig()))
				{
					Log.Fatal("Fail RM now due to state-store error!");
					rmDispatcher.GetEventHandler().Handle(new RMFatalEvent(RMFatalEventType.StateStoreOpFailed
						, failureCause));
				}
				else
				{
					Log.Warn("Skip the state-store error.");
				}
			}
			return isFenced;
		}

		private void NotifyApplication(RMAppEvent @event)
		{
			rmDispatcher.GetEventHandler().Handle(@event);
		}

		private void NotifyApplicationAttempt(RMAppAttemptEvent @event)
		{
			rmDispatcher.GetEventHandler().Handle(@event);
		}

		/// <summary>
		/// EventHandler implementation which forward events to the FSRMStateStore
		/// This hides the EventHandle methods of the store from its public interface
		/// </summary>
		private sealed class ForwardingEventHandler : EventHandler<RMStateStoreEvent>
		{
			public void Handle(RMStateStoreEvent @event)
			{
				this._enclosing.HandleStoreEvent(@event);
			}

			internal ForwardingEventHandler(RMStateStore _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly RMStateStore _enclosing;
		}

		/// <summary>Derived classes must implement this method to delete the state store</summary>
		/// <exception cref="System.Exception"/>
		public abstract void DeleteStore();

		public virtual void SetResourceManager(ResourceManager rm)
		{
			this.resourceManager = rm;
		}

		private class StandByTransitionThread : Runnable
		{
			public virtual void Run()
			{
				RMStateStore.Log.Info("RMStateStore has been fenced");
				this._enclosing.resourceManager.HandleTransitionToStandBy();
			}

			internal StandByTransitionThread(RMStateStore _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly RMStateStore _enclosing;
		}

		public virtual RMStateStore.RMStateStoreState GetRMStateStoreState()
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
	}
}
