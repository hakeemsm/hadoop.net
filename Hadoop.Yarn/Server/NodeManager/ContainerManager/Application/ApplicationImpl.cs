using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Logaggregation;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer.Event;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Loghandler.Event;
using Org.Apache.Hadoop.Yarn.Server.Security;
using Org.Apache.Hadoop.Yarn.State;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application
{
	/// <summary>
	/// The state machine for the representation of an Application
	/// within the NodeManager.
	/// </summary>
	public class ApplicationImpl : Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
	{
		internal readonly Dispatcher dispatcher;

		internal readonly string user;

		internal readonly ApplicationId appId;

		internal readonly Credentials credentials;

		internal IDictionary<ApplicationAccessType, string> applicationACLs;

		internal readonly ApplicationACLsManager aclsManager;

		private readonly ReentrantReadWriteLock.ReadLock readLock;

		private readonly ReentrantReadWriteLock.WriteLock writeLock;

		private readonly Context context;

		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.ApplicationImpl
			));

		private LogAggregationContext logAggregationContext;

		internal IDictionary<ContainerId, Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
			> containers = new Dictionary<ContainerId, Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
			>();

		public ApplicationImpl(Dispatcher dispatcher, string user, ApplicationId appId, Credentials
			 credentials, Context context)
		{
			this.dispatcher = dispatcher;
			this.user = user;
			this.appId = appId;
			this.credentials = credentials;
			this.aclsManager = context.GetApplicationACLsManager();
			this.context = context;
			ReentrantReadWriteLock Lock = new ReentrantReadWriteLock();
			readLock = Lock.ReadLock();
			writeLock = Lock.WriteLock();
			stateMachine = stateMachineFactory.Make(this);
		}

		public virtual string GetUser()
		{
			return user.ToString();
		}

		public virtual ApplicationId GetAppId()
		{
			return appId;
		}

		public virtual ApplicationState GetApplicationState()
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

		public virtual IDictionary<ContainerId, Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
			> GetContainers()
		{
			this.readLock.Lock();
			try
			{
				return this.containers;
			}
			finally
			{
				this.readLock.Unlock();
			}
		}

		private static readonly ApplicationImpl.ContainerDoneTransition ContainerDoneTransition
			 = new ApplicationImpl.ContainerDoneTransition();

		private static StateMachineFactory<Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.ApplicationImpl
			, ApplicationState, ApplicationEventType, ApplicationEvent> stateMachineFactory = 
			new StateMachineFactory<Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.ApplicationImpl
			, ApplicationState, ApplicationEventType, ApplicationEvent>(ApplicationState.New
			).AddTransition(ApplicationState.New, ApplicationState.Initing, ApplicationEventType
			.InitApplication, new ApplicationImpl.AppInitTransition()).AddTransition(ApplicationState
			.New, ApplicationState.New, ApplicationEventType.InitContainer, new ApplicationImpl.InitContainerTransition
			()).AddTransition(ApplicationState.Initing, ApplicationState.Initing, ApplicationEventType
			.InitContainer, new ApplicationImpl.InitContainerTransition()).AddTransition(ApplicationState
			.Initing, EnumSet.Of(ApplicationState.FinishingContainersWait, ApplicationState.
			ApplicationResourcesCleaningup), ApplicationEventType.FinishApplication, new ApplicationImpl.AppFinishTriggeredTransition
			()).AddTransition(ApplicationState.Initing, ApplicationState.Initing, ApplicationEventType
			.ApplicationContainerFinished, ContainerDoneTransition).AddTransition(ApplicationState
			.Initing, ApplicationState.Initing, ApplicationEventType.ApplicationLogHandlingInited
			, new ApplicationImpl.AppLogInitDoneTransition()).AddTransition(ApplicationState
			.Initing, ApplicationState.Initing, ApplicationEventType.ApplicationLogHandlingFailed
			, new ApplicationImpl.AppLogInitFailTransition()).AddTransition(ApplicationState
			.Initing, ApplicationState.Running, ApplicationEventType.ApplicationInited, new 
			ApplicationImpl.AppInitDoneTransition()).AddTransition(ApplicationState.Running, 
			ApplicationState.Running, ApplicationEventType.InitContainer, new ApplicationImpl.InitContainerTransition
			()).AddTransition(ApplicationState.Running, ApplicationState.Running, ApplicationEventType
			.ApplicationContainerFinished, ContainerDoneTransition).AddTransition(ApplicationState
			.Running, EnumSet.Of(ApplicationState.FinishingContainersWait, ApplicationState.
			ApplicationResourcesCleaningup), ApplicationEventType.FinishApplication, new ApplicationImpl.AppFinishTriggeredTransition
			()).AddTransition(ApplicationState.FinishingContainersWait, EnumSet.Of(ApplicationState
			.FinishingContainersWait, ApplicationState.ApplicationResourcesCleaningup), ApplicationEventType
			.ApplicationContainerFinished, new ApplicationImpl.AppFinishTransition()).AddTransition
			(ApplicationState.FinishingContainersWait, ApplicationState.FinishingContainersWait
			, EnumSet.Of(ApplicationEventType.ApplicationLogHandlingInited, ApplicationEventType
			.ApplicationLogHandlingFailed, ApplicationEventType.ApplicationInited, ApplicationEventType
			.FinishApplication)).AddTransition(ApplicationState.ApplicationResourcesCleaningup
			, ApplicationState.ApplicationResourcesCleaningup, ApplicationEventType.ApplicationContainerFinished
			).AddTransition(ApplicationState.ApplicationResourcesCleaningup, ApplicationState
			.Finished, ApplicationEventType.ApplicationResourcesCleanedup, new ApplicationImpl.AppCompletelyDoneTransition
			()).AddTransition(ApplicationState.ApplicationResourcesCleaningup, ApplicationState
			.ApplicationResourcesCleaningup, EnumSet.Of(ApplicationEventType.ApplicationLogHandlingInited
			, ApplicationEventType.ApplicationLogHandlingFailed, ApplicationEventType.ApplicationLogHandlingFinished
			, ApplicationEventType.ApplicationInited, ApplicationEventType.FinishApplication
			)).AddTransition(ApplicationState.Finished, ApplicationState.Finished, ApplicationEventType
			.ApplicationLogHandlingFinished, new ApplicationImpl.AppLogsAggregatedTransition
			()).AddTransition(ApplicationState.Finished, ApplicationState.Finished, EnumSet.
			Of(ApplicationEventType.ApplicationLogHandlingInited, ApplicationEventType.ApplicationLogHandlingFailed
			, ApplicationEventType.FinishApplication)).InstallTopology();

		private readonly StateMachine<ApplicationState, ApplicationEventType, ApplicationEvent
			> stateMachine;

		/// <summary>Notify services of new application.</summary>
		/// <remarks>
		/// Notify services of new application.
		/// In particular, this initializes the
		/// <see cref="Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Logaggregation.LogAggregationService
		/// 	"/>
		/// </remarks>
		internal class AppInitTransition : SingleArcTransition<ApplicationImpl, ApplicationEvent
			>
		{
			// Transitions from NEW state
			// Transitions from INITING state
			// Transitions from RUNNING state
			// Transitions from FINISHING_CONTAINERS_WAIT state.
			// Transitions from APPLICATION_RESOURCES_CLEANINGUP state
			// Transitions from FINISHED state
			// create the topology tables
			public virtual void Transition(ApplicationImpl app, ApplicationEvent @event)
			{
				ApplicationInitEvent initEvent = (ApplicationInitEvent)@event;
				app.applicationACLs = initEvent.GetApplicationACLs();
				app.aclsManager.AddApplication(app.GetAppId(), app.applicationACLs);
				// Inform the logAggregator
				app.logAggregationContext = initEvent.GetLogAggregationContext();
				app.dispatcher.GetEventHandler().Handle(new LogHandlerAppStartedEvent(app.appId, 
					app.user, app.credentials, ContainerLogsRetentionPolicy.AllContainers, app.applicationACLs
					, app.logAggregationContext));
			}
		}

		/// <summary>
		/// Handles the APPLICATION_LOG_HANDLING_INITED event that occurs after
		/// <see cref="Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Logaggregation.LogAggregationService
		/// 	"/>
		/// has created the directories for the app
		/// and started the aggregation thread for the app.
		/// In particular, this requests that the
		/// <see cref="Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer.ResourceLocalizationService
		/// 	"/>
		/// localize the application-scoped resources.
		/// </summary>
		internal class AppLogInitDoneTransition : SingleArcTransition<ApplicationImpl, ApplicationEvent
			>
		{
			public virtual void Transition(ApplicationImpl app, ApplicationEvent @event)
			{
				app.dispatcher.GetEventHandler().Handle(new ApplicationLocalizationEvent(LocalizationEventType
					.InitApplicationResources, app));
			}
		}

		/// <summary>
		/// Handles the APPLICATION_LOG_HANDLING_FAILED event that occurs after
		/// <see cref="Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Logaggregation.LogAggregationService
		/// 	"/>
		/// has failed to initialize the log
		/// aggregation service
		/// In particular, this requests that the
		/// <see cref="Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer.ResourceLocalizationService
		/// 	"/>
		/// localize the application-scoped resources.
		/// </summary>
		internal class AppLogInitFailTransition : SingleArcTransition<ApplicationImpl, ApplicationEvent
			>
		{
			public virtual void Transition(ApplicationImpl app, ApplicationEvent @event)
			{
				Log.Warn("Log Aggregation service failed to initialize, there will " + "be no logs for this application"
					);
				app.dispatcher.GetEventHandler().Handle(new ApplicationLocalizationEvent(LocalizationEventType
					.InitApplicationResources, app));
			}
		}

		/// <summary>
		/// Handles INIT_CONTAINER events which request that we launch a new
		/// container.
		/// </summary>
		/// <remarks>
		/// Handles INIT_CONTAINER events which request that we launch a new
		/// container. When we're still in the INITTING state, we simply
		/// queue these up. When we're in the RUNNING state, we pass along
		/// an ContainerInitEvent to the appropriate ContainerImpl.
		/// </remarks>
		internal class InitContainerTransition : SingleArcTransition<ApplicationImpl, ApplicationEvent
			>
		{
			public virtual void Transition(ApplicationImpl app, ApplicationEvent @event)
			{
				ApplicationContainerInitEvent initEvent = (ApplicationContainerInitEvent)@event;
				Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container container
					 = initEvent.GetContainer();
				app.containers[container.GetContainerId()] = container;
				Log.Info("Adding " + container.GetContainerId() + " to application " + app.ToString
					());
				switch (app.GetApplicationState())
				{
					case ApplicationState.Running:
					{
						app.dispatcher.GetEventHandler().Handle(new ContainerInitEvent(container.GetContainerId
							()));
						break;
					}

					case ApplicationState.Initing:
					case ApplicationState.New:
					{
						// these get queued up and sent out in AppInitDoneTransition
						break;
					}

					default:
					{
						System.Diagnostics.Debug.Assert(false, "Invalid state for InitContainerTransition: "
							 + app.GetApplicationState());
						break;
					}
				}
			}
		}

		internal class AppInitDoneTransition : SingleArcTransition<ApplicationImpl, ApplicationEvent
			>
		{
			public virtual void Transition(ApplicationImpl app, ApplicationEvent @event)
			{
				// Start all the containers waiting for ApplicationInit
				foreach (Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
					 container in app.containers.Values)
				{
					app.dispatcher.GetEventHandler().Handle(new ContainerInitEvent(container.GetContainerId
						()));
				}
			}
		}

		internal sealed class ContainerDoneTransition : SingleArcTransition<ApplicationImpl
			, ApplicationEvent>
		{
			public void Transition(ApplicationImpl app, ApplicationEvent @event)
			{
				ApplicationContainerFinishedEvent containerEvent = (ApplicationContainerFinishedEvent
					)@event;
				if (null == Sharpen.Collections.Remove(app.containers, containerEvent.GetContainerID
					()))
				{
					Log.Warn("Removing unknown " + containerEvent.GetContainerID() + " from application "
						 + app.ToString());
				}
				else
				{
					Log.Info("Removing " + containerEvent.GetContainerID() + " from application " + app
						.ToString());
				}
			}
		}

		internal virtual void HandleAppFinishWithContainersCleanedup()
		{
			// Delete Application level resources
			this.dispatcher.GetEventHandler().Handle(new ApplicationLocalizationEvent(LocalizationEventType
				.DestroyApplicationResources, this));
			// tell any auxiliary services that the app is done 
			this.dispatcher.GetEventHandler().Handle(new AuxServicesEvent(AuxServicesEventType
				.ApplicationStop, appId));
		}

		internal class AppFinishTriggeredTransition : MultipleArcTransition<ApplicationImpl
			, ApplicationEvent, ApplicationState>
		{
			// TODO: Trigger the LogsManager
			public virtual ApplicationState Transition(ApplicationImpl app, ApplicationEvent 
				@event)
			{
				ApplicationFinishEvent appEvent = (ApplicationFinishEvent)@event;
				if (app.containers.IsEmpty())
				{
					// No container to cleanup. Cleanup app level resources.
					app.HandleAppFinishWithContainersCleanedup();
					return ApplicationState.ApplicationResourcesCleaningup;
				}
				// Send event to ContainersLauncher to finish all the containers of this
				// application.
				foreach (ContainerId containerID in app.containers.Keys)
				{
					app.dispatcher.GetEventHandler().Handle(new ContainerKillEvent(containerID, ContainerExitStatus
						.KilledAfterAppCompletion, "Container killed on application-finish event: " + appEvent
						.GetDiagnostic()));
				}
				return ApplicationState.FinishingContainersWait;
			}
		}

		internal class AppFinishTransition : MultipleArcTransition<ApplicationImpl, ApplicationEvent
			, ApplicationState>
		{
			public virtual ApplicationState Transition(ApplicationImpl app, ApplicationEvent 
				@event)
			{
				ApplicationContainerFinishedEvent containerFinishEvent = (ApplicationContainerFinishedEvent
					)@event;
				Log.Info("Removing " + containerFinishEvent.GetContainerID() + " from application "
					 + app.ToString());
				Sharpen.Collections.Remove(app.containers, containerFinishEvent.GetContainerID());
				if (app.containers.IsEmpty())
				{
					// All containers are cleanedup.
					app.HandleAppFinishWithContainersCleanedup();
					return ApplicationState.ApplicationResourcesCleaningup;
				}
				return ApplicationState.FinishingContainersWait;
			}
		}

		internal class AppCompletelyDoneTransition : SingleArcTransition<ApplicationImpl, 
			ApplicationEvent>
		{
			public virtual void Transition(ApplicationImpl app, ApplicationEvent @event)
			{
				// Inform the logService
				app.dispatcher.GetEventHandler().Handle(new LogHandlerAppFinishedEvent(app.appId)
					);
				app.context.GetNMTokenSecretManager().AppFinished(app.GetAppId());
			}
		}

		internal class AppLogsAggregatedTransition : SingleArcTransition<ApplicationImpl, 
			ApplicationEvent>
		{
			public virtual void Transition(ApplicationImpl app, ApplicationEvent @event)
			{
				ApplicationId appId = @event.GetApplicationID();
				Sharpen.Collections.Remove(app.context.GetApplications(), appId);
				app.aclsManager.RemoveApplication(appId);
				try
				{
					app.context.GetNMStateStore().RemoveApplication(appId);
				}
				catch (IOException e)
				{
					Log.Error("Unable to remove application from state store", e);
				}
			}
		}

		public virtual void Handle(ApplicationEvent @event)
		{
			this.writeLock.Lock();
			try
			{
				ApplicationId applicationID = @event.GetApplicationID();
				Log.Debug("Processing " + applicationID + " of type " + @event.GetType());
				ApplicationState oldState = stateMachine.GetCurrentState();
				ApplicationState newState = null;
				try
				{
					// queue event requesting init of the same app
					newState = stateMachine.DoTransition(@event.GetType(), @event);
				}
				catch (InvalidStateTransitonException e)
				{
					Log.Warn("Can't handle this event at current state", e);
				}
				if (oldState != newState)
				{
					Log.Info("Application " + applicationID + " transitioned from " + oldState + " to "
						 + newState);
				}
			}
			finally
			{
				this.writeLock.Unlock();
			}
		}

		public override string ToString()
		{
			return appId.ToString();
		}

		[VisibleForTesting]
		public virtual LogAggregationContext GetLogAggregationContext()
		{
			try
			{
				this.readLock.Lock();
				return this.logAggregationContext;
			}
			finally
			{
				this.readLock.Unlock();
			}
		}
	}
}
