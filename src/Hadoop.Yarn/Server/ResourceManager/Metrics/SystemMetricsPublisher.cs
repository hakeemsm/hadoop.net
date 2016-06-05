using System;
using System.Collections.Generic;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Timeline;
using Org.Apache.Hadoop.Yarn.Client.Api;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Server.Metrics;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmcontainer;
using Org.Apache.Hadoop.Yarn.Util.Timeline;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Metrics
{
	/// <summary>The class that helps RM publish metrics to the timeline server.</summary>
	/// <remarks>
	/// The class that helps RM publish metrics to the timeline server. RM will
	/// always invoke the methods of this class regardless the service is enabled or
	/// not. If it is disabled, publishing requests will be ignored silently.
	/// </remarks>
	public class SystemMetricsPublisher : CompositeService
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Metrics.SystemMetricsPublisher
			));

		private Dispatcher dispatcher;

		private TimelineClient client;

		private bool publishSystemMetrics;

		public SystemMetricsPublisher()
			: base(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Metrics.SystemMetricsPublisher
				).FullName)
		{
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceInit(Configuration conf)
		{
			publishSystemMetrics = conf.GetBoolean(YarnConfiguration.TimelineServiceEnabled, 
				YarnConfiguration.DefaultTimelineServiceEnabled) && conf.GetBoolean(YarnConfiguration
				.RmSystemMetricsPublisherEnabled, YarnConfiguration.DefaultRmSystemMetricsPublisherEnabled
				);
			if (publishSystemMetrics)
			{
				client = TimelineClient.CreateTimelineClient();
				AddIfService(client);
				dispatcher = CreateDispatcher(conf);
				dispatcher.Register(typeof(SystemMetricsEventType), new SystemMetricsPublisher.ForwardingEventHandler
					(this));
				AddIfService(dispatcher);
				Log.Info("YARN system metrics publishing service is enabled");
			}
			else
			{
				Log.Info("YARN system metrics publishing service is not enabled");
			}
			base.ServiceInit(conf);
		}

		public virtual void AppCreated(RMApp app, long createdTime)
		{
			if (publishSystemMetrics)
			{
				dispatcher.GetEventHandler().Handle(new ApplicationCreatedEvent(app.GetApplicationId
					(), app.GetName(), app.GetApplicationType(), app.GetUser(), app.GetQueue(), app.
					GetSubmitTime(), createdTime));
			}
		}

		public virtual void AppFinished(RMApp app, RMAppState state, long finishedTime)
		{
			if (publishSystemMetrics)
			{
				dispatcher.GetEventHandler().Handle(new ApplicationFinishedEvent(app.GetApplicationId
					(), app.GetDiagnostics().ToString(), app.GetFinalApplicationStatus(), RMServerUtils
					.CreateApplicationState(state), app.GetCurrentAppAttempt() == null ? null : app.
					GetCurrentAppAttempt().GetAppAttemptId(), finishedTime, app.GetRMAppMetrics()));
			}
		}

		public virtual void AppACLsUpdated(RMApp app, string appViewACLs, long updatedTime
			)
		{
			if (publishSystemMetrics)
			{
				dispatcher.GetEventHandler().Handle(new ApplicationACLsUpdatedEvent(app.GetApplicationId
					(), appViewACLs == null ? string.Empty : appViewACLs, updatedTime));
			}
		}

		public virtual void AppAttemptRegistered(RMAppAttempt appAttempt, long registeredTime
			)
		{
			if (publishSystemMetrics)
			{
				dispatcher.GetEventHandler().Handle(new AppAttemptRegisteredEvent(appAttempt.GetAppAttemptId
					(), appAttempt.GetHost(), appAttempt.GetRpcPort(), appAttempt.GetTrackingUrl(), 
					appAttempt.GetOriginalTrackingUrl(), appAttempt.GetMasterContainer().GetId(), registeredTime
					));
			}
		}

		public virtual void AppAttemptFinished(RMAppAttempt appAttempt, RMAppAttemptState
			 appAttemtpState, RMApp app, long finishedTime)
		{
			if (publishSystemMetrics)
			{
				dispatcher.GetEventHandler().Handle(new AppAttemptFinishedEvent(appAttempt.GetAppAttemptId
					(), appAttempt.GetTrackingUrl(), appAttempt.GetOriginalTrackingUrl(), appAttempt
					.GetDiagnostics(), app.GetFinalApplicationStatus(), RMServerUtils.CreateApplicationAttemptState
					(appAttemtpState), finishedTime));
			}
		}

		// app will get the final status from app attempt, or create one
		// based on app state if it doesn't exist
		public virtual void ContainerCreated(RMContainer container, long createdTime)
		{
			if (publishSystemMetrics)
			{
				dispatcher.GetEventHandler().Handle(new ContainerCreatedEvent(container.GetContainerId
					(), container.GetAllocatedResource(), container.GetAllocatedNode(), container.GetAllocatedPriority
					(), createdTime, container.GetNodeHttpAddress()));
			}
		}

		public virtual void ContainerFinished(RMContainer container, long finishedTime)
		{
			if (publishSystemMetrics)
			{
				dispatcher.GetEventHandler().Handle(new ContainerFinishedEvent(container.GetContainerId
					(), container.GetDiagnosticsInfo(), container.GetContainerExitStatus(), container
					.GetContainerState(), finishedTime));
			}
		}

		protected internal virtual Dispatcher CreateDispatcher(Configuration conf)
		{
			SystemMetricsPublisher.MultiThreadedDispatcher dispatcher = new SystemMetricsPublisher.MultiThreadedDispatcher
				(conf.GetInt(YarnConfiguration.RmSystemMetricsPublisherDispatcherPoolSize, YarnConfiguration
				.DefaultRmSystemMetricsPublisherDispatcherPoolSize));
			dispatcher.SetDrainEventsOnStop();
			return dispatcher;
		}

		protected internal virtual void HandleSystemMetricsEvent(SystemMetricsEvent @event
			)
		{
			switch (@event.GetType())
			{
				case SystemMetricsEventType.AppCreated:
				{
					PublishApplicationCreatedEvent((ApplicationCreatedEvent)@event);
					break;
				}

				case SystemMetricsEventType.AppFinished:
				{
					PublishApplicationFinishedEvent((ApplicationFinishedEvent)@event);
					break;
				}

				case SystemMetricsEventType.AppAclsUpdated:
				{
					PublishApplicationACLsUpdatedEvent((ApplicationACLsUpdatedEvent)@event);
					break;
				}

				case SystemMetricsEventType.AppAttemptRegistered:
				{
					PublishAppAttemptRegisteredEvent((AppAttemptRegisteredEvent)@event);
					break;
				}

				case SystemMetricsEventType.AppAttemptFinished:
				{
					PublishAppAttemptFinishedEvent((AppAttemptFinishedEvent)@event);
					break;
				}

				case SystemMetricsEventType.ContainerCreated:
				{
					PublishContainerCreatedEvent((ContainerCreatedEvent)@event);
					break;
				}

				case SystemMetricsEventType.ContainerFinished:
				{
					PublishContainerFinishedEvent((ContainerFinishedEvent)@event);
					break;
				}

				default:
				{
					Log.Error("Unknown SystemMetricsEvent type: " + @event.GetType());
					break;
				}
			}
		}

		private void PublishApplicationCreatedEvent(ApplicationCreatedEvent @event)
		{
			TimelineEntity entity = CreateApplicationEntity(@event.GetApplicationId());
			IDictionary<string, object> entityInfo = new Dictionary<string, object>();
			entityInfo[ApplicationMetricsConstants.NameEntityInfo] = @event.GetApplicationName
				();
			entityInfo[ApplicationMetricsConstants.TypeEntityInfo] = @event.GetApplicationType
				();
			entityInfo[ApplicationMetricsConstants.UserEntityInfo] = @event.GetUser();
			entityInfo[ApplicationMetricsConstants.QueueEntityInfo] = @event.GetQueue();
			entityInfo[ApplicationMetricsConstants.SubmittedTimeEntityInfo] = @event.GetSubmittedTime
				();
			entity.SetOtherInfo(entityInfo);
			TimelineEvent tEvent = new TimelineEvent();
			tEvent.SetEventType(ApplicationMetricsConstants.CreatedEventType);
			tEvent.SetTimestamp(@event.GetTimestamp());
			entity.AddEvent(tEvent);
			PutEntity(entity);
		}

		private void PublishApplicationFinishedEvent(ApplicationFinishedEvent @event)
		{
			TimelineEntity entity = CreateApplicationEntity(@event.GetApplicationId());
			TimelineEvent tEvent = new TimelineEvent();
			tEvent.SetEventType(ApplicationMetricsConstants.FinishedEventType);
			tEvent.SetTimestamp(@event.GetTimestamp());
			IDictionary<string, object> eventInfo = new Dictionary<string, object>();
			eventInfo[ApplicationMetricsConstants.DiagnosticsInfoEventInfo] = @event.GetDiagnosticsInfo
				();
			eventInfo[ApplicationMetricsConstants.FinalStatusEventInfo] = @event.GetFinalApplicationStatus
				().ToString();
			eventInfo[ApplicationMetricsConstants.StateEventInfo] = @event.GetYarnApplicationState
				().ToString();
			if (@event.GetLatestApplicationAttemptId() != null)
			{
				eventInfo[ApplicationMetricsConstants.LatestAppAttemptEventInfo] = @event.GetLatestApplicationAttemptId
					().ToString();
			}
			RMAppMetrics appMetrics = @event.GetAppMetrics();
			entity.AddOtherInfo(ApplicationMetricsConstants.AppCpuMetrics, appMetrics.GetVcoreSeconds
				());
			entity.AddOtherInfo(ApplicationMetricsConstants.AppMemMetrics, appMetrics.GetMemorySeconds
				());
			tEvent.SetEventInfo(eventInfo);
			entity.AddEvent(tEvent);
			PutEntity(entity);
		}

		private void PublishApplicationACLsUpdatedEvent(ApplicationACLsUpdatedEvent @event
			)
		{
			TimelineEntity entity = CreateApplicationEntity(@event.GetApplicationId());
			TimelineEvent tEvent = new TimelineEvent();
			IDictionary<string, object> entityInfo = new Dictionary<string, object>();
			entityInfo[ApplicationMetricsConstants.AppViewAclsEntityInfo] = @event.GetViewAppACLs
				();
			entity.SetOtherInfo(entityInfo);
			tEvent.SetEventType(ApplicationMetricsConstants.AclsUpdatedEventType);
			tEvent.SetTimestamp(@event.GetTimestamp());
			entity.AddEvent(tEvent);
			PutEntity(entity);
		}

		private static TimelineEntity CreateApplicationEntity(ApplicationId applicationId
			)
		{
			TimelineEntity entity = new TimelineEntity();
			entity.SetEntityType(ApplicationMetricsConstants.EntityType);
			entity.SetEntityId(applicationId.ToString());
			return entity;
		}

		private void PublishAppAttemptRegisteredEvent(AppAttemptRegisteredEvent @event)
		{
			TimelineEntity entity = CreateAppAttemptEntity(@event.GetApplicationAttemptId());
			TimelineEvent tEvent = new TimelineEvent();
			tEvent.SetEventType(AppAttemptMetricsConstants.RegisteredEventType);
			tEvent.SetTimestamp(@event.GetTimestamp());
			IDictionary<string, object> eventInfo = new Dictionary<string, object>();
			eventInfo[AppAttemptMetricsConstants.TrackingUrlEventInfo] = @event.GetTrackingUrl
				();
			eventInfo[AppAttemptMetricsConstants.OriginalTrackingUrlEventInfo] = @event.GetOriginalTrackingURL
				();
			eventInfo[AppAttemptMetricsConstants.HostEventInfo] = @event.GetHost();
			eventInfo[AppAttemptMetricsConstants.RpcPortEventInfo] = @event.GetRpcPort();
			eventInfo[AppAttemptMetricsConstants.MasterContainerEventInfo] = @event.GetMasterContainerId
				().ToString();
			tEvent.SetEventInfo(eventInfo);
			entity.AddEvent(tEvent);
			PutEntity(entity);
		}

		private void PublishAppAttemptFinishedEvent(AppAttemptFinishedEvent @event)
		{
			TimelineEntity entity = CreateAppAttemptEntity(@event.GetApplicationAttemptId());
			TimelineEvent tEvent = new TimelineEvent();
			tEvent.SetEventType(AppAttemptMetricsConstants.FinishedEventType);
			tEvent.SetTimestamp(@event.GetTimestamp());
			IDictionary<string, object> eventInfo = new Dictionary<string, object>();
			eventInfo[AppAttemptMetricsConstants.TrackingUrlEventInfo] = @event.GetTrackingUrl
				();
			eventInfo[AppAttemptMetricsConstants.OriginalTrackingUrlEventInfo] = @event.GetOriginalTrackingURL
				();
			eventInfo[AppAttemptMetricsConstants.DiagnosticsInfoEventInfo] = @event.GetDiagnosticsInfo
				();
			eventInfo[AppAttemptMetricsConstants.FinalStatusEventInfo] = @event.GetFinalApplicationStatus
				().ToString();
			eventInfo[AppAttemptMetricsConstants.StateEventInfo] = @event.GetYarnApplicationAttemptState
				().ToString();
			tEvent.SetEventInfo(eventInfo);
			entity.AddEvent(tEvent);
			PutEntity(entity);
		}

		private static TimelineEntity CreateAppAttemptEntity(ApplicationAttemptId appAttemptId
			)
		{
			TimelineEntity entity = new TimelineEntity();
			entity.SetEntityType(AppAttemptMetricsConstants.EntityType);
			entity.SetEntityId(appAttemptId.ToString());
			entity.AddPrimaryFilter(AppAttemptMetricsConstants.ParentPrimaryFilter, appAttemptId
				.GetApplicationId().ToString());
			return entity;
		}

		private void PublishContainerCreatedEvent(ContainerCreatedEvent @event)
		{
			TimelineEntity entity = CreateContainerEntity(@event.GetContainerId());
			IDictionary<string, object> entityInfo = new Dictionary<string, object>();
			entityInfo[ContainerMetricsConstants.AllocatedMemoryEntityInfo] = @event.GetAllocatedResource
				().GetMemory();
			entityInfo[ContainerMetricsConstants.AllocatedVcoreEntityInfo] = @event.GetAllocatedResource
				().GetVirtualCores();
			entityInfo[ContainerMetricsConstants.AllocatedHostEntityInfo] = @event.GetAllocatedNode
				().GetHost();
			entityInfo[ContainerMetricsConstants.AllocatedPortEntityInfo] = @event.GetAllocatedNode
				().GetPort();
			entityInfo[ContainerMetricsConstants.AllocatedPriorityEntityInfo] = @event.GetAllocatedPriority
				().GetPriority();
			entityInfo[ContainerMetricsConstants.AllocatedHostHttpAddressEntityInfo] = @event
				.GetNodeHttpAddress();
			entity.SetOtherInfo(entityInfo);
			TimelineEvent tEvent = new TimelineEvent();
			tEvent.SetEventType(ContainerMetricsConstants.CreatedEventType);
			tEvent.SetTimestamp(@event.GetTimestamp());
			entity.AddEvent(tEvent);
			PutEntity(entity);
		}

		private void PublishContainerFinishedEvent(ContainerFinishedEvent @event)
		{
			TimelineEntity entity = CreateContainerEntity(@event.GetContainerId());
			TimelineEvent tEvent = new TimelineEvent();
			tEvent.SetEventType(ContainerMetricsConstants.FinishedEventType);
			tEvent.SetTimestamp(@event.GetTimestamp());
			IDictionary<string, object> eventInfo = new Dictionary<string, object>();
			eventInfo[ContainerMetricsConstants.DiagnosticsInfoEventInfo] = @event.GetDiagnosticsInfo
				();
			eventInfo[ContainerMetricsConstants.ExitStatusEventInfo] = @event.GetContainerExitStatus
				();
			eventInfo[ContainerMetricsConstants.StateEventInfo] = @event.GetContainerState().
				ToString();
			tEvent.SetEventInfo(eventInfo);
			entity.AddEvent(tEvent);
			PutEntity(entity);
		}

		private static TimelineEntity CreateContainerEntity(ContainerId containerId)
		{
			TimelineEntity entity = new TimelineEntity();
			entity.SetEntityType(ContainerMetricsConstants.EntityType);
			entity.SetEntityId(containerId.ToString());
			entity.AddPrimaryFilter(ContainerMetricsConstants.ParentPrimariyFilter, containerId
				.GetApplicationAttemptId().ToString());
			return entity;
		}

		private void PutEntity(TimelineEntity entity)
		{
			try
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Publishing the entity " + entity.GetEntityId() + ", JSON-style content: "
						 + TimelineUtils.DumpTimelineRecordtoJSON(entity));
				}
				client.PutEntities(entity);
			}
			catch (Exception e)
			{
				Log.Error("Error when publishing entity [" + entity.GetEntityType() + "," + entity
					.GetEntityId() + "]", e);
			}
		}

		/// <summary>EventHandler implementation which forward events to SystemMetricsPublisher.
		/// 	</summary>
		/// <remarks>
		/// EventHandler implementation which forward events to SystemMetricsPublisher.
		/// Making use of it, SystemMetricsPublisher can avoid to have a public handle
		/// method.
		/// </remarks>
		private sealed class ForwardingEventHandler : EventHandler<SystemMetricsEvent>
		{
			public void Handle(SystemMetricsEvent @event)
			{
				this._enclosing.HandleSystemMetricsEvent(@event);
			}

			internal ForwardingEventHandler(SystemMetricsPublisher _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly SystemMetricsPublisher _enclosing;
		}

		protected internal class MultiThreadedDispatcher : CompositeService, Dispatcher
		{
			private IList<AsyncDispatcher> dispatchers = new AList<AsyncDispatcher>();

			public MultiThreadedDispatcher(int num)
				: base(typeof(SystemMetricsPublisher.MultiThreadedDispatcher).FullName)
			{
				for (int i = 0; i < num; ++i)
				{
					AsyncDispatcher dispatcher = CreateDispatcher();
					dispatchers.AddItem(dispatcher);
					AddIfService(dispatcher);
				}
			}

			public override EventHandler GetEventHandler()
			{
				return new SystemMetricsPublisher.MultiThreadedDispatcher.CompositEventHandler(this
					);
			}

			public override void Register(Type eventType, EventHandler handler)
			{
				foreach (AsyncDispatcher dispatcher in dispatchers)
				{
					dispatcher.Register(eventType, handler);
				}
			}

			public virtual void SetDrainEventsOnStop()
			{
				foreach (AsyncDispatcher dispatcher in dispatchers)
				{
					dispatcher.SetDrainEventsOnStop();
				}
			}

			private class CompositEventHandler : EventHandler<Org.Apache.Hadoop.Yarn.Event.Event
				>
			{
				public virtual void Handle(Org.Apache.Hadoop.Yarn.Event.Event @event)
				{
					// Use hashCode (of ApplicationId) to dispatch the event to the child
					// dispatcher, such that all the writing events of one application will
					// be handled by one thread, the scheduled order of the these events
					// will be preserved
					int index = (@event.GetHashCode() & int.MaxValue) % this._enclosing.dispatchers.Count;
					this._enclosing.dispatchers[index].GetEventHandler().Handle(@event);
				}

				internal CompositEventHandler(MultiThreadedDispatcher _enclosing)
				{
					this._enclosing = _enclosing;
				}

				private readonly MultiThreadedDispatcher _enclosing;
			}

			protected internal virtual AsyncDispatcher CreateDispatcher()
			{
				return new AsyncDispatcher();
			}
		}
	}
}
