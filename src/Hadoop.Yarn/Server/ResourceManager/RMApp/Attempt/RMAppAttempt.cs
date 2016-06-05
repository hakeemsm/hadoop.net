using System.Collections.Generic;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Security;
using Org.Apache.Hadoop.Yarn.Security.Client;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt
{
	/// <summary>Interface to an Application Attempt in the Resource Manager.</summary>
	/// <remarks>
	/// Interface to an Application Attempt in the Resource Manager.
	/// A
	/// <see cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.RMApp"/>
	/// can have multiple app attempts based on
	/// <see cref="Org.Apache.Hadoop.Yarn.Conf.YarnConfiguration.RmAmMaxAttempts"/>
	/// . For specific
	/// implementation take a look at
	/// <see cref="RMAppAttemptImpl"/>
	/// .
	/// </remarks>
	public interface RMAppAttempt : EventHandler<RMAppAttemptEvent>
	{
		/// <summary>
		/// Get the application attempt id for this
		/// <see cref="RMAppAttempt"/>
		/// .
		/// </summary>
		/// <returns>
		/// the
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationAttemptId"/>
		/// for this RM attempt.
		/// </returns>
		ApplicationAttemptId GetAppAttemptId();

		/// <summary>
		/// The state of the
		/// <see cref="RMAppAttempt"/>
		/// .
		/// </summary>
		/// <returns>
		/// the state
		/// <see cref="RMAppAttemptState"/>
		/// of this
		/// <see cref="RMAppAttempt"/>
		/// </returns>
		RMAppAttemptState GetAppAttemptState();

		/// <summary>
		/// The host on which the
		/// <see cref="RMAppAttempt"/>
		/// is running/ran on.
		/// </summary>
		/// <returns>
		/// the host on which the
		/// <see cref="RMAppAttempt"/>
		/// ran/is running on.
		/// </returns>
		string GetHost();

		/// <summary>
		/// The rpc port of the
		/// <see cref="RMAppAttempt"/>
		/// .
		/// </summary>
		/// <returns>
		/// the rpc port of the
		/// <see cref="RMAppAttempt"/>
		/// to which the clients can connect
		/// to.
		/// </returns>
		int GetRpcPort();

		/// <summary>The url at which the status of the application attempt can be accessed.</summary>
		/// <returns>the url at which the status of the attempt can be accessed.</returns>
		string GetTrackingUrl();

		/// <summary>
		/// The original url at which the status of the application attempt can be
		/// accessed.
		/// </summary>
		/// <remarks>
		/// The original url at which the status of the application attempt can be
		/// accessed. This url is not fronted by a proxy. This is only intended to be
		/// used by the proxy.
		/// </remarks>
		/// <returns>
		/// the url at which the status of the attempt can be accessed and is
		/// not fronted by a proxy.
		/// </returns>
		string GetOriginalTrackingUrl();

		/// <summary>
		/// The base to be prepended to web URLs that are not relative, and the user
		/// has been checked.
		/// </summary>
		/// <returns>the base URL to be prepended to web URLs that are not relative.</returns>
		string GetWebProxyBase();

		/// <summary>Diagnostics information for the application attempt.</summary>
		/// <returns>diagnostics information for the application attempt.</returns>
		string GetDiagnostics();

		/// <summary>Progress for the application attempt.</summary>
		/// <returns>
		/// the progress for this
		/// <see cref="RMAppAttempt"/>
		/// </returns>
		float GetProgress();

		/// <summary>The final status set by the AM.</summary>
		/// <returns>
		/// the final status that is set by the AM when unregistering itself. Can return a null
		/// if the AM has not unregistered itself.
		/// </returns>
		FinalApplicationStatus GetFinalApplicationStatus();

		/// <summary>
		/// Return a list of the last set of finished containers, resetting the
		/// finished containers to empty.
		/// </summary>
		/// <returns>the list of just finished containers, re setting the finished containers.
		/// 	</returns>
		IList<ContainerStatus> PullJustFinishedContainers();

		/// <summary>
		/// Returns a reference to the map of last set of finished containers to the
		/// corresponding node.
		/// </summary>
		/// <remarks>
		/// Returns a reference to the map of last set of finished containers to the
		/// corresponding node. This does not reset the finished containers.
		/// </remarks>
		/// <returns>
		/// the list of just finished containers, this does not reset the
		/// finished containers.
		/// </returns>
		ConcurrentMap<NodeId, IList<ContainerStatus>> GetJustFinishedContainersReference(
			);

		/// <summary>Return the list of last set of finished containers.</summary>
		/// <remarks>
		/// Return the list of last set of finished containers. This does not reset
		/// the finished containers.
		/// </remarks>
		/// <returns>the list of just finished containers</returns>
		IList<ContainerStatus> GetJustFinishedContainers();

		/// <summary>The map of conatiners per Node that are already sent to the AM.</summary>
		/// <returns>map of per node list of finished container status sent to AM</returns>
		ConcurrentMap<NodeId, IList<ContainerStatus>> GetFinishedContainersSentToAMReference
			();

		/// <summary>The container on which the Application Master is running.</summary>
		/// <returns>
		/// the
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.Container"/>
		/// on which the application master is running.
		/// </returns>
		Container GetMasterContainer();

		/// <summary>
		/// The application submission context for this
		/// <see cref="RMAppAttempt"/>
		/// .
		/// </summary>
		/// <returns>the application submission context for this Application.</returns>
		ApplicationSubmissionContext GetSubmissionContext();

		/// <summary>The AMRMToken belonging to this app attempt</summary>
		/// <returns>The AMRMToken belonging to this app attempt</returns>
		Org.Apache.Hadoop.Security.Token.Token<AMRMTokenIdentifier> GetAMRMToken();

		/// <summary>The master key for client-to-AM tokens for this app attempt.</summary>
		/// <remarks>
		/// The master key for client-to-AM tokens for this app attempt. This is only
		/// used for RMStateStore. Normal operation must invoke the secret manager to
		/// get the key and not use the local key directly.
		/// </remarks>
		/// <returns>The master key for client-to-AM tokens for this app attempt</returns>
		SecretKey GetClientTokenMasterKey();

		/// <summary>Create a token for authenticating a client connection to the app attempt
		/// 	</summary>
		/// <param name="clientName">the name of the client requesting the token</param>
		/// <returns>the token or null if the attempt is not running</returns>
		Org.Apache.Hadoop.Security.Token.Token<ClientToAMTokenIdentifier> CreateClientToken
			(string clientName);

		/// <summary>Get application container and resource usage information.</summary>
		/// <returns>an ApplicationResourceUsageReport object.</returns>
		ApplicationResourceUsageReport GetApplicationResourceUsageReport();

		/// <summary>the start time of the application.</summary>
		/// <returns>the start time of the application.</returns>
		long GetStartTime();

		/// <summary>
		/// The current state of the
		/// <see cref="RMAppAttempt"/>
		/// .
		/// </summary>
		/// <returns>
		/// the current state
		/// <see cref="RMAppAttemptState"/>
		/// for this application
		/// attempt.
		/// </returns>
		RMAppAttemptState GetState();

		/// <summary>
		/// Create the external user-facing state of the attempt of ApplicationMaster
		/// from the current state of the
		/// <see cref="RMAppAttempt"/>
		/// .
		/// </summary>
		/// <returns>the external user-facing state of the attempt ApplicationMaster.</returns>
		YarnApplicationAttemptState CreateApplicationAttemptState();

		/// <summary>
		/// Create the Application attempt report from the
		/// <see cref="RMAppAttempt"/>
		/// </summary>
		/// <returns>
		/// 
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationAttemptReport"/>
		/// </returns>
		ApplicationAttemptReport CreateApplicationAttemptReport();

		/// <summary>
		/// Return the flag which indicates whether the attempt failure should be
		/// counted to attempt retry count.
		/// </summary>
		/// <remarks>
		/// Return the flag which indicates whether the attempt failure should be
		/// counted to attempt retry count.
		/// <p>
		/// There failure types should not be counted to attempt retry count:
		/// <ul>
		/// <li>preempted by the scheduler.</li>
		/// <li>
		/// hardware failures, such as NM failing, lost NM and NM disk errors.
		/// </li>
		/// <li>killed by RM because of RM restart or failover.</li>
		/// </ul>
		/// </remarks>
		bool ShouldCountTowardsMaxAttemptRetry();

		/// <summary>
		/// Get metrics from the
		/// <see cref="RMAppAttempt"/>
		/// </summary>
		/// <returns>metrics</returns>
		RMAppAttemptMetrics GetRMAppAttemptMetrics();

		/// <summary>the finish time of the application attempt.</summary>
		/// <returns>the finish time of the application attempt.</returns>
		long GetFinishTime();
	}
}
