using Sharpen;

namespace org.apache.hadoop.service
{
	/// <summary>This is the base implementation class for services.</summary>
	public abstract class AbstractService : org.apache.hadoop.service.Service
	{
		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.service.AbstractService
			)));

		/// <summary>Service name.</summary>
		private readonly string name;

		/// <summary>service state</summary>
		private readonly org.apache.hadoop.service.ServiceStateModel stateModel;

		/// <summary>Service start time.</summary>
		/// <remarks>Service start time. Will be zero until the service is started.</remarks>
		private long startTime;

		/// <summary>The configuration.</summary>
		/// <remarks>The configuration. Will be null until the service is initialized.</remarks>
		private volatile org.apache.hadoop.conf.Configuration config;

		/// <summary>
		/// List of state change listeners; it is final to ensure
		/// that it will never be null.
		/// </summary>
		private readonly org.apache.hadoop.service.ServiceOperations.ServiceListeners listeners
			 = new org.apache.hadoop.service.ServiceOperations.ServiceListeners();

		/// <summary>Static listeners to all events across all services</summary>
		private static org.apache.hadoop.service.ServiceOperations.ServiceListeners globalListeners
			 = new org.apache.hadoop.service.ServiceOperations.ServiceListeners();

		/// <summary>The cause of any failure -will be null.</summary>
		/// <remarks>
		/// The cause of any failure -will be null.
		/// if a service did not stop due to a failure.
		/// </remarks>
		private System.Exception failureCause;

		/// <summary>the state in which the service was when it failed.</summary>
		/// <remarks>
		/// the state in which the service was when it failed.
		/// Only valid when the service is stopped due to a failure
		/// </remarks>
		private org.apache.hadoop.service.Service.STATE failureState = null;

		/// <summary>
		/// object used to co-ordinate
		/// <see cref="waitForServiceToStop(long)"/>
		/// across threads.
		/// </summary>
		private readonly java.util.concurrent.atomic.AtomicBoolean terminationNotification
			 = new java.util.concurrent.atomic.AtomicBoolean(false);

		/// <summary>History of lifecycle transitions</summary>
		private readonly System.Collections.Generic.IList<org.apache.hadoop.service.LifecycleEvent
			> lifecycleHistory = new System.Collections.Generic.List<org.apache.hadoop.service.LifecycleEvent
			>(5);

		/// <summary>Map of blocking dependencies</summary>
		private readonly System.Collections.Generic.IDictionary<string, string> blockerMap
			 = new System.Collections.Generic.Dictionary<string, string>();

		private readonly object stateChangeLock = new object();

		/// <summary>Construct the service.</summary>
		/// <param name="name">service name</param>
		public AbstractService(string name)
		{
			this.name = name;
			stateModel = new org.apache.hadoop.service.ServiceStateModel(name);
		}

		public sealed override org.apache.hadoop.service.Service.STATE getServiceState()
		{
			return stateModel.getState();
		}

		public sealed override System.Exception getFailureCause()
		{
			lock (this)
			{
				return failureCause;
			}
		}

		public override org.apache.hadoop.service.Service.STATE getFailureState()
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
		/// <see cref="init(org.apache.hadoop.conf.Configuration)"/>
		/// and should only be needed if for some reason a service implementation
		/// needs to override that initial setting -for example replacing
		/// it with a new subclass of
		/// <see cref="org.apache.hadoop.conf.Configuration"/>
		/// </remarks>
		/// <param name="conf">new configuration.</param>
		protected internal virtual void setConfig(org.apache.hadoop.conf.Configuration conf
			)
		{
			this.config = conf;
		}

		/// <summary>
		/// <inheritDoc/>
		/// This invokes
		/// <see cref="serviceInit(org.apache.hadoop.conf.Configuration)"/>
		/// </summary>
		/// <param name="conf">the configuration of the service. This must not be null</param>
		/// <exception cref="ServiceStateException">
		/// if the configuration was null,
		/// the state change not permitted, or something else went wrong
		/// </exception>
		public override void init(org.apache.hadoop.conf.Configuration conf)
		{
			if (conf == null)
			{
				throw new org.apache.hadoop.service.ServiceStateException("Cannot initialize service "
					 + getName() + ": null configuration");
			}
			if (isInState(org.apache.hadoop.service.Service.STATE.INITED))
			{
				return;
			}
			lock (stateChangeLock)
			{
				if (enterState(org.apache.hadoop.service.Service.STATE.INITED) != org.apache.hadoop.service.Service.STATE
					.INITED)
				{
					setConfig(conf);
					try
					{
						serviceInit(config);
						if (isInState(org.apache.hadoop.service.Service.STATE.INITED))
						{
							//if the service ended up here during init,
							//notify the listeners
							notifyListeners();
						}
					}
					catch (System.Exception e)
					{
						noteFailure(e);
						org.apache.hadoop.service.ServiceOperations.stopQuietly(LOG, this);
						throw org.apache.hadoop.service.ServiceStateException.convert(e);
					}
				}
			}
		}

		/// <summary><inheritDoc/></summary>
		/// <exception cref="ServiceStateException">
		/// if the current service state does not permit
		/// this action
		/// </exception>
		public override void start()
		{
			if (isInState(org.apache.hadoop.service.Service.STATE.STARTED))
			{
				return;
			}
			//enter the started state
			lock (stateChangeLock)
			{
				if (stateModel.enterState(org.apache.hadoop.service.Service.STATE.STARTED) != org.apache.hadoop.service.Service.STATE
					.STARTED)
				{
					try
					{
						startTime = Sharpen.Runtime.currentTimeMillis();
						serviceStart();
						if (isInState(org.apache.hadoop.service.Service.STATE.STARTED))
						{
							//if the service started (and isn't now in a later state), notify
							if (LOG.isDebugEnabled())
							{
								LOG.debug("Service " + getName() + " is started");
							}
							notifyListeners();
						}
					}
					catch (System.Exception e)
					{
						noteFailure(e);
						org.apache.hadoop.service.ServiceOperations.stopQuietly(LOG, this);
						throw org.apache.hadoop.service.ServiceStateException.convert(e);
					}
				}
			}
		}

		/// <summary><inheritDoc/></summary>
		public override void stop()
		{
			if (isInState(org.apache.hadoop.service.Service.STATE.STOPPED))
			{
				return;
			}
			lock (stateChangeLock)
			{
				if (enterState(org.apache.hadoop.service.Service.STATE.STOPPED) != org.apache.hadoop.service.Service.STATE
					.STOPPED)
				{
					try
					{
						serviceStop();
					}
					catch (System.Exception e)
					{
						//stop-time exceptions are logged if they are the first one,
						noteFailure(e);
						throw org.apache.hadoop.service.ServiceStateException.convert(e);
					}
					finally
					{
						//report that the service has terminated
						terminationNotification.set(true);
						lock (terminationNotification)
						{
							Sharpen.Runtime.notifyAll(terminationNotification);
						}
						//notify anything listening for events
						notifyListeners();
					}
				}
				else
				{
					//already stopped: note it
					if (LOG.isDebugEnabled())
					{
						LOG.debug("Ignoring re-entrant call to stop()");
					}
				}
			}
		}

		/// <summary>
		/// Relay to
		/// <see cref="stop()"/>
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public sealed override void close()
		{
			stop();
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
		protected internal void noteFailure(System.Exception exception)
		{
			if (LOG.isDebugEnabled())
			{
				LOG.debug("noteFailure " + exception, null);
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
					failureState = getServiceState();
					LOG.info("Service " + getName() + " failed in state " + failureState + "; cause: "
						 + exception, exception);
				}
			}
		}

		public sealed override bool waitForServiceToStop(long timeout)
		{
			bool completed = terminationNotification.get();
			while (!completed)
			{
				try
				{
					lock (terminationNotification)
					{
						Sharpen.Runtime.wait(terminationNotification, timeout);
					}
					// here there has been a timeout, the object has terminated,
					// or there has been a spurious wakeup (which we ignore)
					completed = true;
				}
				catch (System.Exception)
				{
					// interrupted; have another look at the flag
					completed = terminationNotification.get();
				}
			}
			return terminationNotification.get();
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
		/// <see cref="init(org.apache.hadoop.conf.Configuration)"/>
		/// prevents re-entrancy.
		/// The base implementation checks to see if the subclass has created
		/// a new configuration instance, and if so, updates the base class value
		/// </remarks>
		/// <param name="conf">configuration</param>
		/// <exception cref="System.Exception">
		/// on a failure -these will be caught,
		/// possibly wrapped, and wil; trigger a service stop
		/// </exception>
		protected internal virtual void serviceInit(org.apache.hadoop.conf.Configuration 
			conf)
		{
			if (conf != config)
			{
				LOG.debug("Config has been overridden during init");
				setConfig(conf);
			}
		}

		/// <summary>Actions called during the INITED to STARTED transition.</summary>
		/// <remarks>
		/// Actions called during the INITED to STARTED transition.
		/// This method will only ever be called once during the lifecycle of
		/// a specific service instance.
		/// Implementations do not need to be synchronized as the logic
		/// in
		/// <see cref="start()"/>
		/// prevents re-entrancy.
		/// </remarks>
		/// <exception cref="System.Exception">
		/// if needed -these will be caught,
		/// wrapped, and trigger a service stop
		/// </exception>
		protected internal virtual void serviceStart()
		{
		}

		/// <summary>Actions called during the transition to the STOPPED state.</summary>
		/// <remarks>
		/// Actions called during the transition to the STOPPED state.
		/// This method will only ever be called once during the lifecycle of
		/// a specific service instance.
		/// Implementations do not need to be synchronized as the logic
		/// in
		/// <see cref="stop()"/>
		/// prevents re-entrancy.
		/// Implementations MUST write this to be robust against failures, including
		/// checks for null references -and for the first failure to not stop other
		/// attempts to shut down parts of the service.
		/// </remarks>
		/// <exception cref="System.Exception">if needed -these will be caught and logged.</exception>
		protected internal virtual void serviceStop()
		{
		}

		public override void registerServiceListener(org.apache.hadoop.service.ServiceStateChangeListener
			 l)
		{
			listeners.add(l);
		}

		public override void unregisterServiceListener(org.apache.hadoop.service.ServiceStateChangeListener
			 l)
		{
			listeners.remove(l);
		}

		/// <summary>
		/// Register a global listener, which receives notifications
		/// from the state change events of all services in the JVM
		/// </summary>
		/// <param name="l">listener</param>
		public static void registerGlobalListener(org.apache.hadoop.service.ServiceStateChangeListener
			 l)
		{
			globalListeners.add(l);
		}

		/// <summary>unregister a global listener.</summary>
		/// <param name="l">listener to unregister</param>
		/// <returns>true if the listener was found (and then deleted)</returns>
		public static bool unregisterGlobalListener(org.apache.hadoop.service.ServiceStateChangeListener
			 l)
		{
			return globalListeners.remove(l);
		}

		/// <summary>Package-scoped method for testing -resets the global listener list</summary>
		[com.google.common.annotations.VisibleForTesting]
		internal static void resetGlobalListeners()
		{
			globalListeners.reset();
		}

		public override string getName()
		{
			return name;
		}

		public override org.apache.hadoop.conf.Configuration getConfig()
		{
			lock (this)
			{
				return config;
			}
		}

		public override long getStartTime()
		{
			return startTime;
		}

		/// <summary>Notify local and global listeners of state changes.</summary>
		/// <remarks>
		/// Notify local and global listeners of state changes.
		/// Exceptions raised by listeners are NOT passed up.
		/// </remarks>
		private void notifyListeners()
		{
			try
			{
				listeners.notifyListeners(this);
				globalListeners.notifyListeners(this);
			}
			catch (System.Exception e)
			{
				LOG.warn("Exception while notifying listeners of " + this + ": " + e, e);
			}
		}

		/// <summary>Add a state change event to the lifecycle history</summary>
		private void recordLifecycleEvent()
		{
			org.apache.hadoop.service.LifecycleEvent @event = new org.apache.hadoop.service.LifecycleEvent
				();
			@event.time = Sharpen.Runtime.currentTimeMillis();
			@event.state = getServiceState();
			lifecycleHistory.add(@event);
		}

		public override System.Collections.Generic.IList<org.apache.hadoop.service.LifecycleEvent
			> getLifecycleHistory()
		{
			lock (this)
			{
				return new System.Collections.Generic.List<org.apache.hadoop.service.LifecycleEvent
					>(lifecycleHistory);
			}
		}

		/// <summary>
		/// Enter a state; record this via
		/// <see cref="recordLifecycleEvent()"/>
		/// and log at the info level.
		/// </summary>
		/// <param name="newState">the proposed new state</param>
		/// <returns>
		/// the original state
		/// it wasn't already in that state, and the state model permits state re-entrancy.
		/// </returns>
		private org.apache.hadoop.service.Service.STATE enterState(org.apache.hadoop.service.Service.STATE
			 newState)
		{
			System.Diagnostics.Debug.Assert(stateModel != null, "null state in " + name + " "
				 + Sharpen.Runtime.getClassForObject(this));
			org.apache.hadoop.service.Service.STATE oldState = stateModel.enterState(newState
				);
			if (oldState != newState)
			{
				if (LOG.isDebugEnabled())
				{
					LOG.debug("Service: " + getName() + " entered state " + getServiceState());
				}
				recordLifecycleEvent();
			}
			return oldState;
		}

		public sealed override bool isInState(org.apache.hadoop.service.Service.STATE expected
			)
		{
			return stateModel.isInState(expected);
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
		protected internal virtual void putBlocker(string name, string details)
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
		public virtual void removeBlocker(string name)
		{
			lock (blockerMap)
			{
				Sharpen.Collections.Remove(blockerMap, name);
			}
		}

		public override System.Collections.Generic.IDictionary<string, string> getBlockers
			()
		{
			lock (blockerMap)
			{
				System.Collections.Generic.IDictionary<string, string> map = new System.Collections.Generic.Dictionary
					<string, string>(blockerMap);
				return map;
			}
		}
	}
}
