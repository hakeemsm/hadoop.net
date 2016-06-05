using System;
using System.Collections.Generic;
using Hadoop.Common.Core.Conf;
using Org.Apache.Hadoop.Conf;


namespace Org.Apache.Hadoop.Service
{
	/// <summary>Service LifeCycle.</summary>
	public abstract class Service : IDisposable
	{
		/// <summary>Service states</summary>
		[System.Serializable]
		public sealed class STATE
		{
			/// <summary>Constructed but not initialized</summary>
			public static readonly Service.STATE Notinited = new Service.STATE(0, "NOTINITED"
				);

			/// <summary>Initialized but not started or stopped</summary>
			public static readonly Service.STATE Inited = new Service.STATE(1, "INITED");

			/// <summary>started and not stopped</summary>
			public static readonly Service.STATE Started = new Service.STATE(2, "STARTED");

			/// <summary>stopped.</summary>
			/// <remarks>stopped. No further state transitions are permitted</remarks>
			public static readonly Service.STATE Stopped = new Service.STATE(3, "STOPPED");

			/// <summary>An integer value for use in array lookup and JMX interfaces.</summary>
			/// <remarks>
			/// An integer value for use in array lookup and JMX interfaces.
			/// Although
			/// <see cref="Enum{E}.Ordinal()"/>
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
			public int GetValue()
			{
				return Service.STATE.value;
			}

			/// <summary>Get the name of a state</summary>
			/// <returns>the state's name</returns>
			public override string ToString()
			{
				return Service.STATE.statename;
			}
		}

		/// <summary>Initialize the service.</summary>
		/// <remarks>
		/// Initialize the service.
		/// The transition MUST be from
		/// <see cref="STATE.Notinited"/>
		/// to
		/// <see cref="STATE.Inited"/>
		/// unless the operation failed and an exception was raised, in which case
		/// <see cref="Stop()"/>
		/// MUST be invoked and the service enter the state
		/// <see cref="STATE.Stopped"/>
		/// .
		/// </remarks>
		/// <param name="config">the configuration of the service</param>
		/// <exception cref="RuntimeException">on any failure during the operation</exception>
		public abstract void Init(Configuration config);

		/// <summary>Start the service.</summary>
		/// <remarks>
		/// Start the service.
		/// The transition MUST be from
		/// <see cref="STATE.Inited"/>
		/// to
		/// <see cref="STATE.Started"/>
		/// unless the operation failed and an exception was raised, in which case
		/// <see cref="Stop()"/>
		/// MUST be invoked and the service enter the state
		/// <see cref="STATE.Stopped"/>
		/// .
		/// </remarks>
		/// <exception cref="RuntimeException">on any failure during the operation</exception>
		public abstract void Start();

		/// <summary>Stop the service.</summary>
		/// <remarks>
		/// Stop the service. This MUST be a no-op if the service is already
		/// in the
		/// <see cref="STATE.Stopped"/>
		/// state. It SHOULD be a best-effort attempt
		/// to stop all parts of the service.
		/// The implementation must be designed to complete regardless of the service
		/// state, including the initialized/uninitialized state of all its internal
		/// fields.
		/// </remarks>
		/// <exception cref="RuntimeException">on any failure during the stop operation
		/// 	</exception>
		public abstract void Stop();

		/// <summary>
		/// A version of stop() that is designed to be usable in Java7 closure
		/// clauses.
		/// </summary>
		/// <remarks>
		/// A version of stop() that is designed to be usable in Java7 closure
		/// clauses.
		/// Implementation classes MUST relay this directly to
		/// <see cref="Stop()"/>
		/// </remarks>
		/// <exception cref="System.IO.IOException">never</exception>
		/// <exception cref="RuntimeException">on any failure during the stop operation
		/// 	</exception>
		public abstract void Close();

		/// <summary>Register a listener to the service state change events.</summary>
		/// <remarks>
		/// Register a listener to the service state change events.
		/// If the supplied listener is already listening to this service,
		/// this method is a no-op.
		/// </remarks>
		/// <param name="listener">a new listener</param>
		public abstract void RegisterServiceListener(ServiceStateChangeListener listener);

		/// <summary>
		/// Unregister a previously registered listener of the service state
		/// change events.
		/// </summary>
		/// <remarks>
		/// Unregister a previously registered listener of the service state
		/// change events. No-op if the listener is already unregistered.
		/// </remarks>
		/// <param name="listener">the listener to unregister.</param>
		public abstract void UnregisterServiceListener(ServiceStateChangeListener listener
			);

		/// <summary>Get the name of this service.</summary>
		/// <returns>the service name</returns>
		public abstract string GetName();

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
		public abstract Configuration GetConfig();

		/// <summary>Get the current service state</summary>
		/// <returns>the state of the service</returns>
		public abstract Service.STATE GetServiceState();

		/// <summary>Get the service start time</summary>
		/// <returns>
		/// the start time of the service. This will be zero if the service
		/// has not yet been started.
		/// </returns>
		public abstract long GetStartTime();

		/// <summary>Query to see if the service is in a specific state.</summary>
		/// <remarks>
		/// Query to see if the service is in a specific state.
		/// In a multi-threaded system, the state may not hold for very long.
		/// </remarks>
		/// <param name="state">the expected state</param>
		/// <returns>true if, at the time of invocation, the service was in that state.</returns>
		public abstract bool IsInState(Service.STATE state);

		/// <summary>Get the first exception raised during the service failure.</summary>
		/// <remarks>
		/// Get the first exception raised during the service failure. If null,
		/// no exception was logged
		/// </remarks>
		/// <returns>the failure logged during a transition to the stopped state</returns>
		public abstract Exception GetFailureCause();

		/// <summary>
		/// Get the state in which the failure in
		/// <see cref="GetFailureCause()"/>
		/// occurred.
		/// </summary>
		/// <returns>the state or null if there was no failure</returns>
		public abstract Service.STATE GetFailureState();

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
		public abstract bool WaitForServiceToStop(long timeout);

		/// <summary>Get a snapshot of the lifecycle history; it is a static list</summary>
		/// <returns>a possibly empty but never null list of lifecycle events.</returns>
		public abstract IList<LifecycleEvent> GetLifecycleHistory();

		/// <summary>
		/// Get the blockers on a service -remote dependencies
		/// that are stopping the service from being <i>live</i>.
		/// </summary>
		/// <returns>a (snapshotted) map of blocker name-&gt;description values</returns>
		public abstract IDictionary<string, string> GetBlockers();
	}

	public static class ServiceConstants
	{
	}
}
