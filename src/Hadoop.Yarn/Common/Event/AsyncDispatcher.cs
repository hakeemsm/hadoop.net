using System;
using System.Collections.Generic;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Event
{
	/// <summary>
	/// Dispatches
	/// <see cref="Event{TYPE}"/>
	/// s in a separate thread. Currently only single thread
	/// does that. Potentially there could be multiple channels for each event type
	/// class and a thread pool can be used to dispatch the events.
	/// </summary>
	public class AsyncDispatcher : AbstractService, Dispatcher
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Event.AsyncDispatcher
			));

		private readonly BlockingQueue<Org.Apache.Hadoop.Yarn.Event.Event> eventQueue;

		private volatile bool stopped = false;

		private volatile bool drainEventsOnStop = false;

		private volatile bool drained = true;

		private object waitForDrained = new object();

		private volatile bool blockNewEvents = false;

		private EventHandler handlerInstance = null;

		private Sharpen.Thread eventHandlingThread;

		protected internal readonly IDictionary<Type, EventHandler> eventDispatchers;

		private bool exitOnDispatchException;

		public AsyncDispatcher()
			: this(new LinkedBlockingQueue<Org.Apache.Hadoop.Yarn.Event.Event>())
		{
		}

		public AsyncDispatcher(BlockingQueue<Org.Apache.Hadoop.Yarn.Event.Event> eventQueue
			)
			: base("Dispatcher")
		{
			// Configuration flag for enabling/disabling draining dispatcher's events on
			// stop functionality.
			// Indicates all the remaining dispatcher's events on stop have been drained
			// and processed.
			// For drainEventsOnStop enabled only, block newly coming events into the
			// queue while stopping.
			this.eventQueue = eventQueue;
			this.eventDispatchers = new Dictionary<Type, EventHandler>();
		}

		internal virtual Runnable CreateThread()
		{
			return new _Runnable_84(this);
		}

		private sealed class _Runnable_84 : Runnable
		{
			public _Runnable_84(AsyncDispatcher _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public void Run()
			{
				while (!this._enclosing.stopped && !Sharpen.Thread.CurrentThread().IsInterrupted(
					))
				{
					this._enclosing.drained = this._enclosing.eventQueue.IsEmpty();
					// blockNewEvents is only set when dispatcher is draining to stop,
					// adding this check is to avoid the overhead of acquiring the lock
					// and calling notify every time in the normal run of the loop.
					if (this._enclosing.blockNewEvents)
					{
						lock (this._enclosing.waitForDrained)
						{
							if (this._enclosing.drained)
							{
								Sharpen.Runtime.Notify(this._enclosing.waitForDrained);
							}
						}
					}
					Org.Apache.Hadoop.Yarn.Event.Event @event;
					try
					{
						@event = this._enclosing.eventQueue.Take();
					}
					catch (Exception ie)
					{
						if (!this._enclosing.stopped)
						{
							Org.Apache.Hadoop.Yarn.Event.AsyncDispatcher.Log.Warn("AsyncDispatcher thread interrupted"
								, ie);
						}
						return;
					}
					if (@event != null)
					{
						this._enclosing.Dispatch(@event);
					}
				}
			}

			private readonly AsyncDispatcher _enclosing;
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceInit(Configuration conf)
		{
			this.exitOnDispatchException = conf.GetBoolean(Dispatcher.DispatcherExitOnErrorKey
				, Dispatcher.DefaultDispatcherExitOnError);
			base.ServiceInit(conf);
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStart()
		{
			//start all the components
			base.ServiceStart();
			eventHandlingThread = new Sharpen.Thread(CreateThread());
			eventHandlingThread.SetName("AsyncDispatcher event handler");
			eventHandlingThread.Start();
		}

		public virtual void SetDrainEventsOnStop()
		{
			drainEventsOnStop = true;
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStop()
		{
			if (drainEventsOnStop)
			{
				blockNewEvents = true;
				Log.Info("AsyncDispatcher is draining to stop, igonring any new events.");
				long endTime = Runtime.CurrentTimeMillis() + GetConfig().GetLong(YarnConfiguration
					.DispatcherDrainEventsTimeout, YarnConfiguration.DefaultDispatcherDrainEventsTimeout
					);
				lock (waitForDrained)
				{
					while (!drained && eventHandlingThread != null && eventHandlingThread.IsAlive() &&
						 Runtime.CurrentTimeMillis() < endTime)
					{
						Sharpen.Runtime.Wait(waitForDrained, 1000);
						Log.Info("Waiting for AsyncDispatcher to drain. Thread state is :" + eventHandlingThread
							.GetState());
					}
				}
			}
			stopped = true;
			if (eventHandlingThread != null)
			{
				eventHandlingThread.Interrupt();
				try
				{
					eventHandlingThread.Join();
				}
				catch (Exception ie)
				{
					Log.Warn("Interrupted Exception while stopping", ie);
				}
			}
			// stop all the components
			base.ServiceStop();
		}

		protected internal virtual void Dispatch(Org.Apache.Hadoop.Yarn.Event.Event @event
			)
		{
			//all events go thru this loop
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Dispatching the event " + @event.GetType().FullName + "." + @event.ToString
					());
			}
			Type type = @event.GetType().GetDeclaringClass();
			try
			{
				EventHandler handler = eventDispatchers[type];
				if (handler != null)
				{
					handler.Handle(@event);
				}
				else
				{
					throw new Exception("No handler for registered for " + type);
				}
			}
			catch (Exception t)
			{
				//TODO Maybe log the state of the queue
				Log.Fatal("Error in dispatcher thread", t);
				// If serviceStop is called, we should exit this thread gracefully.
				if (exitOnDispatchException && (ShutdownHookManager.Get().IsShutdownInProgress())
					 == false && stopped == false)
				{
					Sharpen.Thread shutDownThread = new Sharpen.Thread(CreateShutDownThread());
					shutDownThread.SetName("AsyncDispatcher ShutDown handler");
					shutDownThread.Start();
				}
			}
		}

		public override void Register(Type eventType, EventHandler handler)
		{
			/* check to see if we have a listener registered */
			EventHandler<Org.Apache.Hadoop.Yarn.Event.Event> registeredHandler = (EventHandler
				<Org.Apache.Hadoop.Yarn.Event.Event>)eventDispatchers[eventType];
			Log.Info("Registering " + eventType + " for " + handler.GetType());
			if (registeredHandler == null)
			{
				eventDispatchers[eventType] = handler;
			}
			else
			{
				if (!(registeredHandler is AsyncDispatcher.MultiListenerHandler))
				{
					/* for multiple listeners of an event add the multiple listener handler */
					AsyncDispatcher.MultiListenerHandler multiHandler = new AsyncDispatcher.MultiListenerHandler
						();
					multiHandler.AddHandler(registeredHandler);
					multiHandler.AddHandler(handler);
					eventDispatchers[eventType] = multiHandler;
				}
				else
				{
					/* already a multilistener, just add to it */
					AsyncDispatcher.MultiListenerHandler multiHandler = (AsyncDispatcher.MultiListenerHandler
						)registeredHandler;
					multiHandler.AddHandler(handler);
				}
			}
		}

		public override EventHandler GetEventHandler()
		{
			if (handlerInstance == null)
			{
				handlerInstance = new AsyncDispatcher.GenericEventHandler(this);
			}
			return handlerInstance;
		}

		internal class GenericEventHandler : EventHandler<Org.Apache.Hadoop.Yarn.Event.Event
			>
		{
			public virtual void Handle(Org.Apache.Hadoop.Yarn.Event.Event @event)
			{
				if (this._enclosing.blockNewEvents)
				{
					return;
				}
				this._enclosing.drained = false;
				/* all this method does is enqueue all the events onto the queue */
				int qSize = this._enclosing.eventQueue.Count;
				if (qSize != 0 && qSize % 1000 == 0)
				{
					AsyncDispatcher.Log.Info("Size of event-queue is " + qSize);
				}
				int remCapacity = this._enclosing.eventQueue.RemainingCapacity();
				if (remCapacity < 1000)
				{
					AsyncDispatcher.Log.Warn("Very low remaining capacity in the event-queue: " + remCapacity
						);
				}
				try
				{
					this._enclosing.eventQueue.Put(@event);
				}
				catch (Exception e)
				{
					if (!this._enclosing.stopped)
					{
						AsyncDispatcher.Log.Warn("AsyncDispatcher thread interrupted", e);
					}
					// Need to reset drained flag to true if event queue is empty,
					// otherwise dispatcher will hang on stop.
					this._enclosing.drained = this._enclosing.eventQueue.IsEmpty();
					throw new YarnRuntimeException(e);
				}
			}

			internal GenericEventHandler(AsyncDispatcher _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly AsyncDispatcher _enclosing;
		}

		/// <summary>Multiplexing an event.</summary>
		/// <remarks>
		/// Multiplexing an event. Sending it to different handlers that
		/// are interested in the event.
		/// </remarks>
		/// <?/>
		internal class MultiListenerHandler : EventHandler<Org.Apache.Hadoop.Yarn.Event.Event
			>
		{
			internal IList<EventHandler<Org.Apache.Hadoop.Yarn.Event.Event>> listofHandlers;

			public MultiListenerHandler()
			{
				listofHandlers = new AList<EventHandler<Org.Apache.Hadoop.Yarn.Event.Event>>();
			}

			public virtual void Handle(Org.Apache.Hadoop.Yarn.Event.Event @event)
			{
				foreach (EventHandler<Org.Apache.Hadoop.Yarn.Event.Event> handler in listofHandlers)
				{
					handler.Handle(@event);
				}
			}

			internal virtual void AddHandler(EventHandler<Org.Apache.Hadoop.Yarn.Event.Event>
				 handler)
			{
				listofHandlers.AddItem(handler);
			}
		}

		internal virtual Runnable CreateShutDownThread()
		{
			return new _Runnable_290();
		}

		private sealed class _Runnable_290 : Runnable
		{
			public _Runnable_290()
			{
			}

			public void Run()
			{
				AsyncDispatcher.Log.Info("Exiting, bbye..");
				System.Environment.Exit(-1);
			}
		}

		[VisibleForTesting]
		protected internal virtual bool IsEventThreadWaiting()
		{
			return eventHandlingThread.GetState() == Sharpen.Thread.State.Waiting;
		}

		[VisibleForTesting]
		protected internal virtual bool IsDrained()
		{
			return this.drained;
		}
	}
}
