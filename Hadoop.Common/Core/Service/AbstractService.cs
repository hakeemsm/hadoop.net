using System;
using System.Collections.Generic;
using Com.Google.Common.Annotations;
using Hadoop.Common.Core.Conf;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;


namespace Org.Apache.Hadoop.Service
{
	/// <summary>This is the base implementation class for services.</summary>
	public abstract class AbstractService : Org.Apache.Hadoop.Service.Service
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Service.AbstractService
			));

		/// <summary>Service name.</summary>
		private readonly string name;

		/// <summary>service state</summary>
		private readonly ServiceStateModel stateModel;

		/// <summary>Service start time.</summary>
		/// <remarks>Service start time. Will be zero until the service is started.</remarks>
		private long startTime;

		/// <summary>The configuration.</summary>
		/// <remarks>The configuration. Will be null until the service is initialized.</remarks>
		private volatile Configuration config;

		/// <summary>
		/// List of state change listeners; it is final to ensure
		/// that it will never be null.
		/// </summary>
		private readonly ServiceOperations.ServiceListeners listeners = new ServiceOperations.ServiceListeners
			();

		/// <summary>Static listeners to all events across all services</summary>
		private static ServiceOperations.ServiceListeners globalListeners = new ServiceOperations.ServiceListeners
			();

		/// <summary>The cause of any failure -will be null.</summary>
		/// <remarks>
		/// The cause of any failure -will be null.
		/// if a service did not stop due to a failure.
		/// </remarks>
		private Exception failureCause;

		/// <summary>the state in which the service was when it failed.</summary>
		/// <remarks>
		/// the state in which the service was when it failed.
		/// Only valid when the service is stopped due to a failure
		/// </remarks>
		private Service.STATE failureState = null;

		/// <summary>
		/// object used to co-ordinate
		/// <see cref="WaitForServiceToStop(long)"/>
		/// across threads.
		/// </summary>
		private readonly AtomicBoolean terminationNotification = new AtomicBoolean(false);

		/// <summary>History of lifecycle transitions</summary>
		private readonly IList<LifecycleEvent> lifecycleHistory = new AList<LifecycleEvent
			>(5);

		/// <summary>Map of blocking dependencies</summary>
		private readonly IDictionary<string, string> blockerMap = new Dictionary<string, 
			string>();

		private readonly object stateChangeLock = new object();

		/// <summary>Construct the service.</summary>
		/// <param name="name">service name</param>
		public AbstractService(string name)
		{
			this.name = name;
			stateModel = new ServiceStateModel(name);
		}

		public sealed override Service.STATE GetServiceState()
		{
			return stateModel.GetState();
		}

		public sealed override Exception GetFailureCause()
		{
			lock (this)
			{
				return failureCause;
			}
		}

		public override Service.STATE GetFailureState()
		{
			lock (this)
			{
				return failureState;
			}
		}

		/// <summary>Set the configuration for this service.</summary>
		/// <remarks>
		/// Set the configuration for this service.
		/// This method is called during
		/// <see cref="Init(Configuration)"/>
		/// and should only be needed if for some reason a service implementation
		/// needs to override that initial setting -for example replacing
		/// it with a new subclass of
		/// <see cref="Configuration"/>
		/// </remarks>
		/// <param name="conf">new configuration.</param>
		protected internal virtual void SetConfig(Configuration conf)
		{
			this.config = conf;
		}

		/// <summary>
		/// <inheritDoc/>
		/// This invokes
		/// <see cref="ServiceInit(Configuration)"/>
		/// </summary>
		/// <param name="conf">the configuration of the service. This must not be null</param>
		/// <exception cref="ServiceStateException">
		/// if the configuration was null,
		/// the state change not permitted, or something else went wrong
		/// </exception>
		public override void Init(Configuration conf)
		{
			if (conf == null)
			{
				throw new ServiceStateException("Cannot initialize service " + GetName() + ": null configuration"
					);
			}
			if (IsInState(Service.STATE.Inited))
			{
				return;
			}
			lock (stateChangeLock)
			{
				if (EnterState(Service.STATE.Inited) != Service.STATE.Inited)
				{
					SetConfig(conf);
					try
					{
						ServiceInit(config);
						if (IsInState(Service.STATE.Inited))
						{
							//if the service ended up here during init,
							//notify the listeners
							NotifyListeners();
						}
					}
					catch (Exception e)
					{
						NoteFailure(e);
						ServiceOperations.StopQuietly(Log, this);
						throw ServiceStateException.Convert(e);
					}
				}
			}
		}

		/// <summary><inheritDoc/></summary>
		/// <exception cref="ServiceStateException">
		/// if the current service state does not permit
		/// this action
		/// </exception>
		public override void Start()
		{
			if (IsInState(Service.STATE.Started))
			{
				return;
			}
			//enter the started state
			lock (stateChangeLock)
			{
				if (stateModel.EnterState(Service.STATE.Started) != Service.STATE.Started)
				{
					try
					{
						startTime = Runtime.CurrentTimeMillis();
						ServiceStart();
						if (IsInState(Service.STATE.Started))
						{
							//if the service started (and isn't now in a later state), notify
							if (Log.IsDebugEnabled())
							{
								Log.Debug("Service " + GetName() + " is started");
							}
							NotifyListeners();
						}
					}
					catch (Exception e)
					{
						NoteFailure(e);
						ServiceOperations.StopQuietly(Log, this);
						throw ServiceStateException.Convert(e);
					}
				}
			}
		}

		/// <summary><inheritDoc/></summary>
		public override void Stop()
		{
			if (IsInState(Service.STATE.Stopped))
			{
				return;
			}
			lock (stateChangeLock)
			{
				if (EnterState(Service.STATE.Stopped) != Service.STATE.Stopped)
				{
					try
					{
						ServiceStop();
					}
					catch (Exception e)
					{
						//stop-time exceptions are logged if they are the first one,
						NoteFailure(e);
						throw ServiceStateException.Convert(e);
					}
					finally
					{
						//report that the service has terminated
						terminationNotification.Set(true);
						lock (terminationNotification)
						{
							Runtime.NotifyAll(terminationNotification);
						}
						//notify anything listening for events
						NotifyListeners();
					}
				}
				else
				{
					//already stopped: note it
					if (Log.IsDebugEnabled())
					{
						Log.Debug("Ignoring re-entrant call to stop()");
					}
				}
			}
		}

		/// <summary>
		/// Relay to
		/// <see cref="Stop()"/>
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public sealed override void Close()
		{
			Stop();
		}

		/// <summary>
		/// Failure handling: record the exception
		/// that triggered it -if there was not one already.
		/// </summary>
		/// <remarks>
		/// Failure handling: record the exception
		/// that triggered it -if there was not one already.
		/// Services are free to call this themselves.
		/// </remarks>
		/// <param name="exception">the exception</param>
		protected internal void NoteFailure(Exception exception)
		{
			if (Log.IsDebugEnabled())
			{
				Log.Debug("noteFailure " + exception, null);
			}
			if (exception == null)
			{
				//make sure failure logic doesn't itself cause problems
				return;
			}
			//record the failure details, and log it
			lock (this)
			{
				if (failureCause == null)
				{
					failureCause = exception;
					failureState = GetServiceState();
					Log.Info("Service " + GetName() + " failed in state " + failureState + "; cause: "
						 + exception, exception);
				}
			}
		}

		public sealed override bool WaitForServiceToStop(long timeout)
		{
			bool completed = terminationNotification.Get();
			while (!completed)
			{
				try
				{
					lock (terminationNotification)
					{
						Runtime.Wait(terminationNotification, timeout);
					}
					// here there has been a timeout, the object has terminated,
					// or there has been a spurious wakeup (which we ignore)
					completed = true;
				}
				catch (Exception)
				{
					// interrupted; have another look at the flag
					completed = terminationNotification.Get();
				}
			}
			return terminationNotification.Get();
		}

		/* ===================================================================== */
		/* Override Points */
		/* ===================================================================== */
		/// <summary>All initialization code needed by a service.</summary>
		/// <remarks>
		/// All initialization code needed by a service.
		/// This method will only ever be called once during the lifecycle of
		/// a specific service instance.
		/// Implementations do not need to be synchronized as the logic
		/// in
		/// <see cref="Init(Configuration)"/>
		/// prevents re-entrancy.
		/// The base implementation checks to see if the subclass has created
		/// a new configuration instance, and if so, updates the base class value
		/// </remarks>
		/// <param name="conf">configuration</param>
		/// <exception cref="System.Exception">
		/// on a failure -these will be caught,
		/// possibly wrapped, and wil; trigger a service stop
		/// </exception>
		protected internal virtual void ServiceInit(Configuration conf)
		{
			if (conf != config)
			{
				Log.Debug("Config has been overridden during init");
				SetConfig(conf);
			}
		}

		/// <summary>Actions called during the INITED to STARTED transition.</summary>
		/// <remarks>
		/// Actions called during the INITED to STARTED transition.
		/// This method will only ever be called once during the lifecycle of
		/// a specific service instance.
		/// Implementations do not need to be synchronized as the logic
		/// in
		/// <see cref="Start()"/>
		/// prevents re-entrancy.
		/// </remarks>
		/// <exception cref="System.Exception">
		/// if needed -these will be caught,
		/// wrapped, and trigger a service stop
		/// </exception>
		protected internal virtual void ServiceStart()
		{
		}

		/// <summary>Actions called during the transition to the STOPPED state.</summary>
		/// <remarks>
		/// Actions called during the transition to the STOPPED state.
		/// This method will only ever be called once during the lifecycle of
		/// a specific service instance.
		/// Implementations do not need to be synchronized as the logic
		/// in
		/// <see cref="Stop()"/>
		/// prevents re-entrancy.
		/// Implementations MUST write this to be robust against failures, including
		/// checks for null references -and for the first failure to not stop other
		/// attempts to shut down parts of the service.
		/// </remarks>
		/// <exception cref="System.Exception">if needed -these will be caught and logged.</exception>
		protected internal virtual void ServiceStop()
		{
		}

		public override void RegisterServiceListener(ServiceStateChangeListener l)
		{
			listeners.Add(l);
		}

		public override void UnregisterServiceListener(ServiceStateChangeListener l)
		{
			listeners.Remove(l);
		}

		/// <summary>
		/// Register a global listener, which receives notifications
		/// from the state change events of all services in the JVM
		/// </summary>
		/// <param name="l">listener</param>
		public static void RegisterGlobalListener(ServiceStateChangeListener l)
		{
			globalListeners.Add(l);
		}

		/// <summary>unregister a global listener.</summary>
		/// <param name="l">listener to unregister</param>
		/// <returns>true if the listener was found (and then deleted)</returns>
		public static bool UnregisterGlobalListener(ServiceStateChangeListener l)
		{
			return globalListeners.Remove(l);
		}

		/// <summary>Package-scoped method for testing -resets the global listener list</summary>
		[VisibleForTesting]
		internal static void ResetGlobalListeners()
		{
			globalListeners.Reset();
		}

		public override string GetName()
		{
			return name;
		}

		public override Configuration GetConfig()
		{
			lock (this)
			{
				return config;
			}
		}

		public override long GetStartTime()
		{
			return startTime;
		}

		/// <summary>Notify local and global listeners of state changes.</summary>
		/// <remarks>
		/// Notify local and global listeners of state changes.
		/// Exceptions raised by listeners are NOT passed up.
		/// </remarks>
		private void NotifyListeners()
		{
			try
			{
				listeners.NotifyListeners(this);
				globalListeners.NotifyListeners(this);
			}
			catch (Exception e)
			{
				Log.Warn("Exception while notifying listeners of " + this + ": " + e, e);
			}
		}

		/// <summary>Add a state change event to the lifecycle history</summary>
		private void RecordLifecycleEvent()
		{
			LifecycleEvent @event = new LifecycleEvent();
			@event.time = Runtime.CurrentTimeMillis();
			@event.state = GetServiceState();
			lifecycleHistory.AddItem(@event);
		}

		public override IList<LifecycleEvent> GetLifecycleHistory()
		{
			lock (this)
			{
				return new AList<LifecycleEvent>(lifecycleHistory);
			}
		}

		/// <summary>
		/// Enter a state; record this via
		/// <see cref="RecordLifecycleEvent()"/>
		/// and log at the info level.
		/// </summary>
		/// <param name="newState">the proposed new state</param>
		/// <returns>
		/// the original state
		/// it wasn't already in that state, and the state model permits state re-entrancy.
		/// </returns>
		private Service.STATE EnterState(Service.STATE newState)
		{
			System.Diagnostics.Debug.Assert(stateModel != null, "null state in " + name + " "
				 + this.GetType());
			Service.STATE oldState = stateModel.EnterState(newState);
			if (oldState != newState)
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Service: " + GetName() + " entered state " + GetServiceState());
				}
				RecordLifecycleEvent();
			}
			return oldState;
		}

		public sealed override bool IsInState(Service.STATE expected)
		{
			return stateModel.IsInState(expected);
		}

		public override string ToString()
		{
			return "Service " + name + " in state " + stateModel;
		}

		/// <summary>
		/// Put a blocker to the blocker map -replacing any
		/// with the same name.
		/// </summary>
		/// <param name="name">blocker name</param>
		/// <param name="details">any specifics on the block. This must be non-null.</param>
		protected internal virtual void PutBlocker(string name, string details)
		{
			lock (blockerMap)
			{
				blockerMap[name] = details;
			}
		}

		/// <summary>
		/// Remove a blocker from the blocker map -
		/// this is a no-op if the blocker is not present
		/// </summary>
		/// <param name="name">the name of the blocker</param>
		public virtual void RemoveBlocker(string name)
		{
			lock (blockerMap)
			{
				Collections.Remove(blockerMap, name);
			}
		}

		public override IDictionary<string, string> GetBlockers()
		{
			lock (blockerMap)
			{
				IDictionary<string, string> map = new Dictionary<string, string>(blockerMap);
				return map;
			}
		}
	}
}
