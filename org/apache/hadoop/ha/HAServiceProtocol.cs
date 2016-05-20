using Sharpen;

namespace org.apache.hadoop.ha
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
			public static readonly org.apache.hadoop.ha.HAServiceProtocol.HAServiceState INITIALIZING
				 = new org.apache.hadoop.ha.HAServiceProtocol.HAServiceState("initializing");

			public static readonly org.apache.hadoop.ha.HAServiceProtocol.HAServiceState ACTIVE
				 = new org.apache.hadoop.ha.HAServiceProtocol.HAServiceState("active");

			public static readonly org.apache.hadoop.ha.HAServiceProtocol.HAServiceState STANDBY
				 = new org.apache.hadoop.ha.HAServiceProtocol.HAServiceState("standby");

			public static readonly org.apache.hadoop.ha.HAServiceProtocol.HAServiceState STOPPING
				 = new org.apache.hadoop.ha.HAServiceProtocol.HAServiceState("stopping");

			private string name;

			internal HAServiceState(string name)
			{
				this.name = name;
			}

			public override string ToString()
			{
				return org.apache.hadoop.ha.HAServiceProtocol.HAServiceState.name;
			}
		}

		public enum RequestSource
		{
			REQUEST_BY_USER,
			REQUEST_BY_USER_FORCED,
			REQUEST_BY_ZKFC
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
			private readonly org.apache.hadoop.ha.HAServiceProtocol.RequestSource source;

			public StateChangeRequestInfo(org.apache.hadoop.ha.HAServiceProtocol.RequestSource
				 source)
				: base()
			{
				this.source = source;
			}

			public virtual org.apache.hadoop.ha.HAServiceProtocol.RequestSource getSource()
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
		/// <exception cref="org.apache.hadoop.security.AccessControlException">if access is denied.
		/// 	</exception>
		/// <exception cref="System.IO.IOException">if other errors happen</exception>
		/// <exception cref="org.apache.hadoop.ha.HealthCheckFailedException"/>
		[org.apache.hadoop.io.retry.Idempotent]
		public abstract void monitorHealth();

		/// <summary>Request service to transition to active state.</summary>
		/// <remarks>
		/// Request service to transition to active state. No operation, if the
		/// service is already in active state.
		/// </remarks>
		/// <exception cref="ServiceFailedException">if transition from standby to active fails.
		/// 	</exception>
		/// <exception cref="org.apache.hadoop.security.AccessControlException">if access is denied.
		/// 	</exception>
		/// <exception cref="System.IO.IOException">if other errors happen</exception>
		/// <exception cref="org.apache.hadoop.ha.ServiceFailedException"/>
		[org.apache.hadoop.io.retry.Idempotent]
		public abstract void transitionToActive(org.apache.hadoop.ha.HAServiceProtocol.StateChangeRequestInfo
			 reqInfo);

		/// <summary>Request service to transition to standby state.</summary>
		/// <remarks>
		/// Request service to transition to standby state. No operation, if the
		/// service is already in standby state.
		/// </remarks>
		/// <exception cref="ServiceFailedException">if transition from active to standby fails.
		/// 	</exception>
		/// <exception cref="org.apache.hadoop.security.AccessControlException">if access is denied.
		/// 	</exception>
		/// <exception cref="System.IO.IOException">if other errors happen</exception>
		/// <exception cref="org.apache.hadoop.ha.ServiceFailedException"/>
		[org.apache.hadoop.io.retry.Idempotent]
		public abstract void transitionToStandby(org.apache.hadoop.ha.HAServiceProtocol.StateChangeRequestInfo
			 reqInfo);

		/// <summary>Return the current status of the service.</summary>
		/// <remarks>
		/// Return the current status of the service. The status indicates
		/// the current <em>state</em> (e.g ACTIVE/STANDBY) as well as
		/// some additional information.
		/// <seealso>HAServiceStatus</seealso>
		/// </remarks>
		/// <exception cref="org.apache.hadoop.security.AccessControlException">if access is denied.
		/// 	</exception>
		/// <exception cref="System.IO.IOException">if other errors happen</exception>
		[org.apache.hadoop.io.retry.Idempotent]
		public abstract org.apache.hadoop.ha.HAServiceStatus getServiceStatus();
	}

	public static class HAServiceProtocolConstants
	{
	}
}
