using Sharpen;

namespace org.apache.hadoop.service
{
	/// <summary>Service LifeCycle.</summary>
	public abstract class Service : java.io.Closeable
	{
		/// <summary>Service states</summary>
		[System.Serializable]
		public sealed class STATE
		{
			/// <summary>Constructed but not initialized</summary>
			public static readonly org.apache.hadoop.service.Service.STATE NOTINITED = new org.apache.hadoop.service.Service.STATE
				(0, "NOTINITED");

			/// <summary>Initialized but not started or stopped</summary>
			public static readonly org.apache.hadoop.service.Service.STATE INITED = new org.apache.hadoop.service.Service.STATE
				(1, "INITED");

			/// <summary>started and not stopped</summary>
			public static readonly org.apache.hadoop.service.Service.STATE STARTED = new org.apache.hadoop.service.Service.STATE
				(2, "STARTED");

			/// <summary>stopped.</summary>
			/// <remarks>stopped. No further state transitions are permitted</remarks>
			public static readonly org.apache.hadoop.service.Service.STATE STOPPED = new org.apache.hadoop.service.Service.STATE
				(3, "STOPPED");

			/// <summary>An integer value for use in array lookup and JMX interfaces.</summary>
			/// <remarks>
			/// An integer value for use in array lookup and JMX interfaces.
			/// Although
			/// <see cref="java.lang.Enum{E}.ordinal()"/>
			/// could do this, explicitly
			/// identify the numbers gives more stability guarantees over time.
			/// </remarks>
			private readonly int value;

			/// <summary>A name of the state that can be used in messages</summary>
			private readonly string statename;

			private STATE(int value, string name)
			{
				this.value = value;
				this.statename = name;
			}

			/// <summary>Get the integer value of a state</summary>
			/// <returns>the numeric value of the state</returns>
			public int getValue()
			{
				return org.apache.hadoop.service.Service.STATE.value;
			}

			/// <summary>Get the name of a state</summary>
			/// <returns>the state's name</returns>
			public override string ToString()
			{
				return org.apache.hadoop.service.Service.STATE.statename;
			}
		}

		/// <summary>Initialize the service.</summary>
		/// <remarks>
		/// Initialize the service.
		/// The transition MUST be from
		/// <see cref="STATE.NOTINITED"/>
		/// to
		/// <see cref="STATE.INITED"/>
		/// unless the operation failed and an exception was raised, in which case
		/// <see cref="stop()"/>
		/// MUST be invoked and the service enter the state
		/// <see cref="STATE.STOPPED"/>
		/// .
		/// </remarks>
		/// <param name="config">the configuration of the service</param>
		/// <exception cref="System.Exception">on any failure during the operation</exception>
		public abstract void init(org.apache.hadoop.conf.Configuration config);

		/// <summary>Start the service.</summary>
		/// <remarks>
		/// Start the service.
		/// The transition MUST be from
		/// <see cref="STATE.INITED"/>
		/// to
		/// <see cref="STATE.STARTED"/>
		/// unless the operation failed and an exception was raised, in which case
		/// <see cref="stop()"/>
		/// MUST be invoked and the service enter the state
		/// <see cref="STATE.STOPPED"/>
		/// .
		/// </remarks>
		/// <exception cref="System.Exception">on any failure during the operation</exception>
		public abstract void start();

		/// <summary>Stop the service.</summary>
		/// <remarks>
		/// Stop the service. This MUST be a no-op if the service is already
		/// in the
		/// <see cref="STATE.STOPPED"/>
		/// state. It SHOULD be a best-effort attempt
		/// to stop all parts of the service.
		/// The implementation must be designed to complete regardless of the service
		/// state, including the initialized/uninitialized state of all its internal
		/// fields.
		/// </remarks>
		/// <exception cref="System.Exception">on any failure during the stop operation</exception>
		public abstract void stop();

		/// <summary>
		/// A version of stop() that is designed to be usable in Java7 closure
		/// clauses.
		/// </summary>
		/// <remarks>
		/// A version of stop() that is designed to be usable in Java7 closure
		/// clauses.
		/// Implementation classes MUST relay this directly to
		/// <see cref="stop()"/>
		/// </remarks>
		/// <exception cref="System.IO.IOException">never</exception>
		/// <exception cref="System.Exception">on any failure during the stop operation</exception>
		public abstract void close();

		/// <summary>Register a listener to the service state change events.</summary>
		/// <remarks>
		/// Register a listener to the service state change events.
		/// If the supplied listener is already listening to this service,
		/// this method is a no-op.
		/// </remarks>
		/// <param name="listener">a new listener</param>
		public abstract void registerServiceListener(org.apache.hadoop.service.ServiceStateChangeListener
			 listener);

		/// <summary>
		/// Unregister a previously registered listener of the service state
		/// change events.
		/// </summary>
		/// <remarks>
		/// Unregister a previously registered listener of the service state
		/// change events. No-op if the listener is already unregistered.
		/// </remarks>
		/// <param name="listener">the listener to unregister.</param>
		public abstract void unregisterServiceListener(org.apache.hadoop.service.ServiceStateChangeListener
			 listener);

		/// <summary>Get the name of this service.</summary>
		/// <returns>the service name</returns>
		public abstract string getName();

		/// <summary>Get the configuration of this service.</summary>
		/// <remarks>
		/// Get the configuration of this service.
		/// This is normally not a clone and may be manipulated, though there are no
		/// guarantees as to what the consequences of such actions may be
		/// </remarks>
		/// <returns>
		/// the current configuration, unless a specific implentation chooses
		/// otherwise.
		/// </returns>
		public abstract org.apache.hadoop.conf.Configuration getConfig();

		/// <summary>Get the current service state</summary>
		/// <returns>the state of the service</returns>
		public abstract org.apache.hadoop.service.Service.STATE getServiceState();

		/// <summary>Get the service start time</summary>
		/// <returns>
		/// the start time of the service. This will be zero if the service
		/// has not yet been started.
		/// </returns>
		public abstract long getStartTime();

		/// <summary>Query to see if the service is in a specific state.</summary>
		/// <remarks>
		/// Query to see if the service is in a specific state.
		/// In a multi-threaded system, the state may not hold for very long.
		/// </remarks>
		/// <param name="state">the expected state</param>
		/// <returns>true if, at the time of invocation, the service was in that state.</returns>
		public abstract bool isInState(org.apache.hadoop.service.Service.STATE state);

		/// <summary>Get the first exception raised during the service failure.</summary>
		/// <remarks>
		/// Get the first exception raised during the service failure. If null,
		/// no exception was logged
		/// </remarks>
		/// <returns>the failure logged during a transition to the stopped state</returns>
		public abstract System.Exception getFailureCause();

		/// <summary>
		/// Get the state in which the failure in
		/// <see cref="getFailureCause()"/>
		/// occurred.
		/// </summary>
		/// <returns>the state or null if there was no failure</returns>
		public abstract org.apache.hadoop.service.Service.STATE getFailureState();

		/// <summary>
		/// Block waiting for the service to stop; uses the termination notification
		/// object to do so.
		/// </summary>
		/// <remarks>
		/// Block waiting for the service to stop; uses the termination notification
		/// object to do so.
		/// This method will only return after all the service stop actions
		/// have been executed (to success or failure), or the timeout elapsed
		/// This method can be called before the service is inited or started; this is
		/// to eliminate any race condition with the service stopping before
		/// this event occurs.
		/// </remarks>
		/// <param name="timeout">timeout in milliseconds. A value of zero means "forever"</param>
		/// <returns>true iff the service stopped in the time period</returns>
		public abstract bool waitForServiceToStop(long timeout);

		/// <summary>Get a snapshot of the lifecycle history; it is a static list</summary>
		/// <returns>a possibly empty but never null list of lifecycle events.</returns>
		public abstract System.Collections.Generic.IList<org.apache.hadoop.service.LifecycleEvent
			> getLifecycleHistory();

		/// <summary>
		/// Get the blockers on a service -remote dependencies
		/// that are stopping the service from being <i>live</i>.
		/// </summary>
		/// <returns>a (snapshotted) map of blocker name-&gt;description values</returns>
		public abstract System.Collections.Generic.IDictionary<string, string> getBlockers
			();
	}

	public static class ServiceConstants
	{
	}
}
