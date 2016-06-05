using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice;
using Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmcontainer;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Ahs
{
	/// <summary>
	/// <p>
	/// <see cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.ResourceManager"/>
	/// uses this class to write the information of
	/// <see cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.RMApp"/>
	/// ,
	/// <see cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt.RMAppAttempt
	/// 	"/>
	/// and
	/// <see cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmcontainer.RMContainer"
	/// 	/>
	/// . These APIs are
	/// non-blocking, and just schedule a writing history event. An self-contained
	/// dispatcher vector will handle the event in separate threads, and extract the
	/// required fields that are going to be persisted. Then, the extracted
	/// information will be persisted via the implementation of
	/// <see cref="Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice.ApplicationHistoryStore
	/// 	"/>
	/// .
	/// </p>
	/// </summary>
	public class RMApplicationHistoryWriter : CompositeService
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Ahs.RMApplicationHistoryWriter
			));

		private Dispatcher dispatcher;

		[VisibleForTesting]
		internal ApplicationHistoryWriter writer;

		[VisibleForTesting]
		internal bool historyServiceEnabled;

		public RMApplicationHistoryWriter()
			: base(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Ahs.RMApplicationHistoryWriter
				).FullName)
		{
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceInit(Configuration conf)
		{
			lock (this)
			{
				historyServiceEnabled = conf.GetBoolean(YarnConfiguration.ApplicationHistoryEnabled
					, YarnConfiguration.DefaultApplicationHistoryEnabled);
				if (conf.Get(YarnConfiguration.ApplicationHistoryStore) == null || conf.Get(YarnConfiguration
					.ApplicationHistoryStore).Length == 0 || conf.Get(YarnConfiguration.ApplicationHistoryStore
					).Equals(typeof(NullApplicationHistoryStore).FullName))
				{
					historyServiceEnabled = false;
				}
				// Only create the services when the history service is enabled and not
				// using the null store, preventing wasting the system resources.
				if (historyServiceEnabled)
				{
					writer = CreateApplicationHistoryStore(conf);
					AddIfService(writer);
					dispatcher = CreateDispatcher(conf);
					dispatcher.Register(typeof(WritingHistoryEventType), new RMApplicationHistoryWriter.ForwardingEventHandler
						(this));
					AddIfService(dispatcher);
				}
				base.ServiceInit(conf);
			}
		}

		protected internal virtual Dispatcher CreateDispatcher(Configuration conf)
		{
			RMApplicationHistoryWriter.MultiThreadedDispatcher dispatcher = new RMApplicationHistoryWriter.MultiThreadedDispatcher
				(conf.GetInt(YarnConfiguration.RmHistoryWriterMultiThreadedDispatcherPoolSize, YarnConfiguration
				.DefaultRmHistoryWriterMultiThreadedDispatcherPoolSize));
			dispatcher.SetDrainEventsOnStop();
			return dispatcher;
		}

		protected internal virtual ApplicationHistoryStore CreateApplicationHistoryStore(
			Configuration conf)
		{
			try
			{
				Type storeClass = conf.GetClass<ApplicationHistoryStore>(YarnConfiguration.ApplicationHistoryStore
					, typeof(NullApplicationHistoryStore));
				return System.Activator.CreateInstance(storeClass);
			}
			catch (Exception e)
			{
				string msg = "Could not instantiate ApplicationHistoryWriter: " + conf.Get(YarnConfiguration
					.ApplicationHistoryStore, typeof(NullApplicationHistoryStore).FullName);
				Log.Error(msg, e);
				throw new YarnRuntimeException(msg, e);
			}
		}

		protected internal virtual void HandleWritingApplicationHistoryEvent(WritingApplicationHistoryEvent
			 @event)
		{
			switch (@event.GetType())
			{
				case WritingHistoryEventType.AppStart:
				{
					WritingApplicationStartEvent wasEvent = (WritingApplicationStartEvent)@event;
					try
					{
						writer.ApplicationStarted(wasEvent.GetApplicationStartData());
						Log.Info("Stored the start data of application " + wasEvent.GetApplicationId());
					}
					catch (IOException)
					{
						Log.Error("Error when storing the start data of application " + wasEvent.GetApplicationId
							());
					}
					break;
				}

				case WritingHistoryEventType.AppFinish:
				{
					WritingApplicationFinishEvent wafEvent = (WritingApplicationFinishEvent)@event;
					try
					{
						writer.ApplicationFinished(wafEvent.GetApplicationFinishData());
						Log.Info("Stored the finish data of application " + wafEvent.GetApplicationId());
					}
					catch (IOException)
					{
						Log.Error("Error when storing the finish data of application " + wafEvent.GetApplicationId
							());
					}
					break;
				}

				case WritingHistoryEventType.AppAttemptStart:
				{
					WritingApplicationAttemptStartEvent waasEvent = (WritingApplicationAttemptStartEvent
						)@event;
					try
					{
						writer.ApplicationAttemptStarted(waasEvent.GetApplicationAttemptStartData());
						Log.Info("Stored the start data of application attempt " + waasEvent.GetApplicationAttemptId
							());
					}
					catch (IOException)
					{
						Log.Error("Error when storing the start data of application attempt " + waasEvent
							.GetApplicationAttemptId());
					}
					break;
				}

				case WritingHistoryEventType.AppAttemptFinish:
				{
					WritingApplicationAttemptFinishEvent waafEvent = (WritingApplicationAttemptFinishEvent
						)@event;
					try
					{
						writer.ApplicationAttemptFinished(waafEvent.GetApplicationAttemptFinishData());
						Log.Info("Stored the finish data of application attempt " + waafEvent.GetApplicationAttemptId
							());
					}
					catch (IOException)
					{
						Log.Error("Error when storing the finish data of application attempt " + waafEvent
							.GetApplicationAttemptId());
					}
					break;
				}

				case WritingHistoryEventType.ContainerStart:
				{
					WritingContainerStartEvent wcsEvent = (WritingContainerStartEvent)@event;
					try
					{
						writer.ContainerStarted(wcsEvent.GetContainerStartData());
						Log.Info("Stored the start data of container " + wcsEvent.GetContainerId());
					}
					catch (IOException)
					{
						Log.Error("Error when storing the start data of container " + wcsEvent.GetContainerId
							());
					}
					break;
				}

				case WritingHistoryEventType.ContainerFinish:
				{
					WritingContainerFinishEvent wcfEvent = (WritingContainerFinishEvent)@event;
					try
					{
						writer.ContainerFinished(wcfEvent.GetContainerFinishData());
						Log.Info("Stored the finish data of container " + wcfEvent.GetContainerId());
					}
					catch (IOException)
					{
						Log.Error("Error when storing the finish data of container " + wcfEvent.GetContainerId
							());
					}
					break;
				}

				default:
				{
					Log.Error("Unknown WritingApplicationHistoryEvent type: " + @event.GetType());
					break;
				}
			}
		}

		public virtual void ApplicationStarted(RMApp app)
		{
			if (historyServiceEnabled)
			{
				dispatcher.GetEventHandler().Handle(new WritingApplicationStartEvent(app.GetApplicationId
					(), ApplicationStartData.NewInstance(app.GetApplicationId(), app.GetName(), app.
					GetApplicationType(), app.GetQueue(), app.GetUser(), app.GetSubmitTime(), app.GetStartTime
					())));
			}
		}

		public virtual void ApplicationFinished(RMApp app, RMAppState finalState)
		{
			if (historyServiceEnabled)
			{
				dispatcher.GetEventHandler().Handle(new WritingApplicationFinishEvent(app.GetApplicationId
					(), ApplicationFinishData.NewInstance(app.GetApplicationId(), app.GetFinishTime(
					), app.GetDiagnostics().ToString(), app.GetFinalApplicationStatus(), RMServerUtils
					.CreateApplicationState(finalState))));
			}
		}

		public virtual void ApplicationAttemptStarted(RMAppAttempt appAttempt)
		{
			if (historyServiceEnabled)
			{
				dispatcher.GetEventHandler().Handle(new WritingApplicationAttemptStartEvent(appAttempt
					.GetAppAttemptId(), ApplicationAttemptStartData.NewInstance(appAttempt.GetAppAttemptId
					(), appAttempt.GetHost(), appAttempt.GetRpcPort(), appAttempt.GetMasterContainer
					().GetId())));
			}
		}

		public virtual void ApplicationAttemptFinished(RMAppAttempt appAttempt, RMAppAttemptState
			 finalState)
		{
			if (historyServiceEnabled)
			{
				dispatcher.GetEventHandler().Handle(new WritingApplicationAttemptFinishEvent(appAttempt
					.GetAppAttemptId(), ApplicationAttemptFinishData.NewInstance(appAttempt.GetAppAttemptId
					(), appAttempt.GetDiagnostics().ToString(), appAttempt.GetTrackingUrl(), appAttempt
					.GetFinalApplicationStatus(), RMServerUtils.CreateApplicationAttemptState(finalState
					))));
			}
		}

		public virtual void ContainerStarted(RMContainer container)
		{
			if (historyServiceEnabled)
			{
				dispatcher.GetEventHandler().Handle(new WritingContainerStartEvent(container.GetContainerId
					(), ContainerStartData.NewInstance(container.GetContainerId(), container.GetAllocatedResource
					(), container.GetAllocatedNode(), container.GetAllocatedPriority(), container.GetCreationTime
					())));
			}
		}

		public virtual void ContainerFinished(RMContainer container)
		{
			if (historyServiceEnabled)
			{
				dispatcher.GetEventHandler().Handle(new WritingContainerFinishEvent(container.GetContainerId
					(), ContainerFinishData.NewInstance(container.GetContainerId(), container.GetFinishTime
					(), container.GetDiagnosticsInfo(), container.GetContainerExitStatus(), container
					.GetContainerState())));
			}
		}

		/// <summary>
		/// EventHandler implementation which forward events to HistoryWriter Making
		/// use of it, HistoryWriter can avoid to have a public handle method
		/// </summary>
		private sealed class ForwardingEventHandler : EventHandler<WritingApplicationHistoryEvent
			>
		{
			public void Handle(WritingApplicationHistoryEvent @event)
			{
				this._enclosing.HandleWritingApplicationHistoryEvent(@event);
			}

			internal ForwardingEventHandler(RMApplicationHistoryWriter _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly RMApplicationHistoryWriter _enclosing;
		}

		protected internal class MultiThreadedDispatcher : CompositeService, Dispatcher
		{
			private IList<AsyncDispatcher> dispatchers = new AList<AsyncDispatcher>();

			public MultiThreadedDispatcher(int num)
				: base(typeof(RMApplicationHistoryWriter.MultiThreadedDispatcher).FullName)
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
				return new RMApplicationHistoryWriter.MultiThreadedDispatcher.CompositEventHandler
					(this);
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
