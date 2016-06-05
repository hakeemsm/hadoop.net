using Org.Apache.Hadoop.IO.Retry;


namespace Org.Apache.Hadoop.HA
{
	/// <summary>
	/// Protocol interface that provides High Availability related primitives to
	/// monitor and fail-over the service.
	/// </summary>
	/// <remarks>
	/// Protocol interface that provides High Availability related primitives to
	/// monitor and fail-over the service.
	/// This interface could be used by HA frameworks to manage the service.
	/// </remarks>
	public abstract class HAServiceProtocol
	{
		/// <summary>Initial version of the protocol</summary>
		public const long versionID = 1L;

		/// <summary>An HA service may be in active or standby state.</summary>
		/// <remarks>
		/// An HA service may be in active or standby state. During startup, it is in
		/// an unknown INITIALIZING state. During shutdown, it is in the STOPPING state
		/// and can no longer return to active/standby states.
		/// </remarks>
		[System.Serializable]
		public sealed class HAServiceState
		{
			public static readonly HAServiceProtocol.HAServiceState Initializing = new HAServiceProtocol.HAServiceState
				("initializing");

			public static readonly HAServiceProtocol.HAServiceState Active = new HAServiceProtocol.HAServiceState
				("active");

			public static readonly HAServiceProtocol.HAServiceState Standby = new HAServiceProtocol.HAServiceState
				("standby");

			public static readonly HAServiceProtocol.HAServiceState Stopping = new HAServiceProtocol.HAServiceState
				("stopping");

			private string name;

			internal HAServiceState(string name)
			{
				this.name = name;
			}

			public override string ToString()
			{
				return HAServiceProtocol.HAServiceState.name;
			}
		}

		public enum RequestSource
		{
			RequestByUser,
			RequestByUserForced,
			RequestByZkfc
		}

		/// <summary>Information describing the source for a request to change state.</summary>
		/// <remarks>
		/// Information describing the source for a request to change state.
		/// This is used to differentiate requests from automatic vs CLI
		/// failover controllers, and in the future may include epoch
		/// information.
		/// </remarks>
		public class StateChangeRequestInfo
		{
			private readonly HAServiceProtocol.RequestSource source;

			public StateChangeRequestInfo(HAServiceProtocol.RequestSource source)
				: base()
			{
				this.source = source;
			}

			public virtual HAServiceProtocol.RequestSource GetSource()
			{
				return source;
			}
		}

		/// <summary>Monitor the health of service.</summary>
		/// <remarks>
		/// Monitor the health of service. This periodically called by the HA
		/// frameworks to monitor the health of the service.
		/// Service is expected to perform checks to ensure it is functional.
		/// If the service is not healthy due to failure or partial failure,
		/// it is expected to throw
		/// <see cref="HealthCheckFailedException"/>
		/// .
		/// The definition of service not healthy is left to the service.
		/// Note that when health check of an Active service fails,
		/// failover to standby may be done.
		/// </remarks>
		/// <exception cref="HealthCheckFailedException">if the health check of a service fails.
		/// 	</exception>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException">if access is denied.
		/// 	</exception>
		/// <exception cref="System.IO.IOException">if other errors happen</exception>
		/// <exception cref="Org.Apache.Hadoop.HA.HealthCheckFailedException"/>
		[Idempotent]
		public abstract void MonitorHealth();

		/// <summary>Request service to transition to active state.</summary>
		/// <remarks>
		/// Request service to transition to active state. No operation, if the
		/// service is already in active state.
		/// </remarks>
		/// <exception cref="ServiceFailedException">if transition from standby to active fails.
		/// 	</exception>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException">if access is denied.
		/// 	</exception>
		/// <exception cref="System.IO.IOException">if other errors happen</exception>
		/// <exception cref="Org.Apache.Hadoop.HA.ServiceFailedException"/>
		[Idempotent]
		public abstract void TransitionToActive(HAServiceProtocol.StateChangeRequestInfo 
			reqInfo);

		/// <summary>Request service to transition to standby state.</summary>
		/// <remarks>
		/// Request service to transition to standby state. No operation, if the
		/// service is already in standby state.
		/// </remarks>
		/// <exception cref="ServiceFailedException">if transition from active to standby fails.
		/// 	</exception>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException">if access is denied.
		/// 	</exception>
		/// <exception cref="System.IO.IOException">if other errors happen</exception>
		/// <exception cref="Org.Apache.Hadoop.HA.ServiceFailedException"/>
		[Idempotent]
		public abstract void TransitionToStandby(HAServiceProtocol.StateChangeRequestInfo
			 reqInfo);

		/// <summary>Return the current status of the service.</summary>
		/// <remarks>
		/// Return the current status of the service. The status indicates
		/// the current <em>state</em> (e.g ACTIVE/STANDBY) as well as
		/// some additional information.
		/// <seealso>HAServiceStatus</seealso>
		/// </remarks>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException">if access is denied.
		/// 	</exception>
		/// <exception cref="System.IO.IOException">if other errors happen</exception>
		[Idempotent]
		public abstract HAServiceStatus GetServiceStatus();
	}

	public static class HAServiceProtocolConstants
	{
	}
}
