using Sharpen;

namespace org.apache.hadoop.ha
{
	/// <summary>
	/// Protocol exposed by the ZKFailoverController, allowing for graceful
	/// failover.
	/// </summary>
	public abstract class ZKFCProtocol
	{
		/// <summary>Initial version of the protocol</summary>
		public const long versionID = 1L;

		/// <summary>
		/// Request that this service yield from the active node election for the
		/// specified time period.
		/// </summary>
		/// <remarks>
		/// Request that this service yield from the active node election for the
		/// specified time period.
		/// If the node is not currently active, it simply prevents any attempts
		/// to become active for the specified time period. Otherwise, it first
		/// tries to transition the local service to standby state, and then quits
		/// the election.
		/// If the attempt to transition to standby succeeds, then the ZKFC receiving
		/// this RPC will delete its own breadcrumb node in ZooKeeper. Thus, the
		/// next node to become active will not run any fencing process. Otherwise,
		/// the breadcrumb will be left, such that the next active will fence this
		/// node.
		/// After the specified time period elapses, the node will attempt to re-join
		/// the election, provided that its service is healthy.
		/// If the node has previously been instructed to cede active, and is still
		/// within the specified time period, the later command's time period will
		/// take precedence, resetting the timer.
		/// A call to cedeActive which specifies a 0 or negative time period will
		/// allow the target node to immediately rejoin the election, so long as
		/// it is healthy.
		/// </remarks>
		/// <param name="millisToCede">
		/// period for which the node should not attempt to
		/// become active
		/// </param>
		/// <exception cref="System.IO.IOException">if the operation fails</exception>
		/// <exception cref="org.apache.hadoop.security.AccessControlException">if the operation is disallowed
		/// 	</exception>
		[org.apache.hadoop.io.retry.Idempotent]
		public abstract void cedeActive(int millisToCede);

		/// <summary>Request that this node try to become active through a graceful failover.
		/// 	</summary>
		/// <remarks>
		/// Request that this node try to become active through a graceful failover.
		/// If the node is already active, this is a no-op and simply returns success
		/// without taking any further action.
		/// If the node is not healthy, it will throw an exception indicating that it
		/// is not able to become active.
		/// If the node is healthy and not active, it will try to initiate a graceful
		/// failover to become active, returning only when it has successfully become
		/// active. See
		/// <see cref="ZKFailoverController.gracefulFailoverToYou()"/>
		/// for the
		/// implementation details.
		/// If the node fails to successfully coordinate the failover, throws an
		/// exception indicating the reason for failure.
		/// </remarks>
		/// <exception cref="System.IO.IOException">if graceful failover fails</exception>
		/// <exception cref="org.apache.hadoop.security.AccessControlException">if the operation is disallowed
		/// 	</exception>
		[org.apache.hadoop.io.retry.Idempotent]
		public abstract void gracefulFailover();
	}

	public static class ZKFCProtocolConstants
	{
	}
}
