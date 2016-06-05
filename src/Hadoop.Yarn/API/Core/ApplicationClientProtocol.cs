using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.IO.Retry;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api
{
	/// <summary>
	/// <p>The protocol between clients and the <code>ResourceManager</code>
	/// to submit/abort jobs and to get information on applications, cluster metrics,
	/// nodes, queues and ACLs.</p>
	/// </summary>
	public interface ApplicationClientProtocol : ApplicationBaseProtocol
	{
		/// <summary>
		/// <p>The interface used by clients to obtain a new
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationId"/>
		/// for
		/// submitting new applications.</p>
		/// <p>The <code>ResourceManager</code> responds with a new, monotonically
		/// increasing,
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationId"/>
		/// which is used by the client to submit
		/// a new application.</p>
		/// <p>The <code>ResourceManager</code> also responds with details such
		/// as maximum resource capabilities in the cluster as specified in
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Protocolrecords.GetNewApplicationResponse"/
		/// 	>
		/// .</p>
		/// </summary>
		/// <param name="request">request to get a new <code>ApplicationId</code></param>
		/// <returns>
		/// response containing the new <code>ApplicationId</code> to be used
		/// to submit an application
		/// </returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		/// <seealso cref="SubmitApplication(Org.Apache.Hadoop.Yarn.Api.Protocolrecords.SubmitApplicationRequest)
		/// 	"/>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		[Idempotent]
		GetNewApplicationResponse GetNewApplication(GetNewApplicationRequest request);

		/// <summary>
		/// <p>The interface used by clients to submit a new application to the
		/// <code>ResourceManager.</code></p>
		/// <p>The client is required to provide details such as queue,
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.Resource"/>
		/// required to run the <code>ApplicationMaster</code>,
		/// the equivalent of
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ContainerLaunchContext"/>
		/// for launching
		/// the <code>ApplicationMaster</code> etc. via the
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Protocolrecords.SubmitApplicationRequest"/>
		/// .</p>
		/// <p>Currently the <code>ResourceManager</code> sends an immediate (empty)
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Protocolrecords.SubmitApplicationResponse"/
		/// 	>
		/// on accepting the submission and throws
		/// an exception if it rejects the submission. However, this call needs to be
		/// followed by
		/// <see cref="ApplicationBaseProtocol.GetApplicationReport(Org.Apache.Hadoop.Yarn.Api.Protocolrecords.GetApplicationReportRequest)
		/// 	"/>
		/// to make sure that the application gets properly submitted - obtaining a
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Protocolrecords.SubmitApplicationResponse"/
		/// 	>
		/// from ResourceManager doesn't guarantee
		/// that RM 'remembers' this application beyond failover or restart. If RM
		/// failover or RM restart happens before ResourceManager saves the
		/// application's state successfully, the subsequent
		/// <see cref="ApplicationBaseProtocol.GetApplicationReport(Org.Apache.Hadoop.Yarn.Api.Protocolrecords.GetApplicationReportRequest)
		/// 	"/>
		/// will throw
		/// a
		/// <see cref="Org.Apache.Hadoop.Yarn.Exceptions.ApplicationNotFoundException"/>
		/// . The Clients need to re-submit
		/// the application with the same
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationSubmissionContext"/>
		/// when
		/// it encounters the
		/// <see cref="Org.Apache.Hadoop.Yarn.Exceptions.ApplicationNotFoundException"/>
		/// on the
		/// <see cref="ApplicationBaseProtocol.GetApplicationReport(Org.Apache.Hadoop.Yarn.Api.Protocolrecords.GetApplicationReportRequest)
		/// 	"/>
		/// call.</p>
		/// <p>During the submission process, it checks whether the application
		/// already exists. If the application exists, it will simply return
		/// SubmitApplicationResponse</p>
		/// <p> In secure mode,the <code>ResourceManager</code> verifies access to
		/// queues etc. before accepting the application submission.</p>
		/// </summary>
		/// <param name="request">request to submit a new application</param>
		/// <returns>(empty) response on accepting the submission</returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		/// <seealso cref="GetNewApplication(Org.Apache.Hadoop.Yarn.Api.Protocolrecords.GetNewApplicationRequest)
		/// 	"/>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		[Idempotent]
		SubmitApplicationResponse SubmitApplication(SubmitApplicationRequest request);

		/// <summary>
		/// <p>The interface used by clients to request the
		/// <code>ResourceManager</code> to abort submitted application.</p>
		/// <p>The client, via
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Protocolrecords.KillApplicationRequest"/>
		/// provides the
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationId"/>
		/// of the application to be aborted.</p>
		/// <p> In secure mode,the <code>ResourceManager</code> verifies access to the
		/// application, queue etc. before terminating the application.</p>
		/// <p>Currently, the <code>ResourceManager</code> returns an empty response
		/// on success and throws an exception on rejecting the request.</p>
		/// </summary>
		/// <param name="request">request to abort a submitted application</param>
		/// <returns>
		/// <code>ResourceManager</code> returns an empty response
		/// on success and throws an exception on rejecting the request
		/// </returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		/// <seealso cref="GetQueueUserAcls(Org.Apache.Hadoop.Yarn.Api.Protocolrecords.GetQueueUserAclsInfoRequest)
		/// 	"></seealso>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		[Idempotent]
		KillApplicationResponse ForceKillApplication(KillApplicationRequest request);

		/// <summary>
		/// <p>The interface used by clients to get metrics about the cluster from
		/// the <code>ResourceManager</code>.</p>
		/// <p>The <code>ResourceManager</code> responds with a
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Protocolrecords.GetClusterMetricsResponse"/
		/// 	>
		/// which includes the
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.YarnClusterMetrics"/>
		/// with details such as number of current
		/// nodes in the cluster.</p>
		/// </summary>
		/// <param name="request">request for cluster metrics</param>
		/// <returns>cluster metrics</returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		[Idempotent]
		GetClusterMetricsResponse GetClusterMetrics(GetClusterMetricsRequest request);

		/// <summary>
		/// <p>The interface used by clients to get a report of all nodes
		/// in the cluster from the <code>ResourceManager</code>.</p>
		/// <p>The <code>ResourceManager</code> responds with a
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Protocolrecords.GetClusterNodesResponse"/>
		/// which includes the
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.NodeReport"/>
		/// for all the nodes in the cluster.</p>
		/// </summary>
		/// <param name="request">request for report on all nodes</param>
		/// <returns>report on all nodes</returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		[Idempotent]
		GetClusterNodesResponse GetClusterNodes(GetClusterNodesRequest request);

		/// <summary>
		/// <p>The interface used by clients to get information about <em>queues</em>
		/// from the <code>ResourceManager</code>.</p>
		/// <p>The client, via
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Protocolrecords.GetQueueInfoRequest"/>
		/// , can ask for details such
		/// as used/total resources, child queues, running applications etc.</p>
		/// <p> In secure mode,the <code>ResourceManager</code> verifies access before
		/// providing the information.</p>
		/// </summary>
		/// <param name="request">request to get queue information</param>
		/// <returns>queue information</returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		[Idempotent]
		GetQueueInfoResponse GetQueueInfo(GetQueueInfoRequest request);

		/// <summary>
		/// <p>The interface used by clients to get information about <em>queue
		/// acls</em> for <em>current user</em> from the <code>ResourceManager</code>.
		/// </summary>
		/// <remarks>
		/// <p>The interface used by clients to get information about <em>queue
		/// acls</em> for <em>current user</em> from the <code>ResourceManager</code>.
		/// </p>
		/// <p>The <code>ResourceManager</code> responds with queue acls for all
		/// existing queues.</p>
		/// </remarks>
		/// <param name="request">request to get queue acls for <em>current user</em></param>
		/// <returns>queue acls for <em>current user</em></returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		[Idempotent]
		GetQueueUserAclsInfoResponse GetQueueUserAcls(GetQueueUserAclsInfoRequest request
			);

		/// <summary>Move an application to a new queue.</summary>
		/// <param name="request">the application ID and the target queue</param>
		/// <returns>an empty response</returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		[Idempotent]
		MoveApplicationAcrossQueuesResponse MoveApplicationAcrossQueues(MoveApplicationAcrossQueuesRequest
			 request);

		/// <summary>
		/// <p>
		/// The interface used by clients to submit a new reservation to the
		/// <c>ResourceManager</c>
		/// .
		/// </p>
		/// <p>
		/// The client packages all details of its request in a
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Protocolrecords.ReservationSubmissionRequest
		/// 	"/>
		/// object. This contains information
		/// about the amount of capacity, temporal constraints, and concurrency needs.
		/// Furthermore, the reservation might be composed of multiple stages, with
		/// ordering dependencies among them.
		/// </p>
		/// <p>
		/// In order to respond, a new admission control component in the
		/// <c>ResourceManager</c>
		/// performs an analysis of the resources that have
		/// been committed over the period of time the user is requesting, verify that
		/// the user requests can be fulfilled, and that it respect a sharing policy
		/// (e.g.,
		/// <c>CapacityOverTimePolicy</c>
		/// ). Once it has positively determined
		/// that the ReservationSubmissionRequest is satisfiable the
		/// <c>ResourceManager</c>
		/// answers with a
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Protocolrecords.ReservationSubmissionResponse
		/// 	"/>
		/// that include a non-null
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ReservationId"/>
		/// . Upon failure to find a valid allocation the response
		/// is an exception with the reason.
		/// On application submission the client can use this
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ReservationId"/>
		/// to
		/// obtain access to the reserved resources.
		/// </p>
		/// <p>
		/// The system guarantees that during the time-range specified by the user, the
		/// reservationID will be corresponding to a valid reservation. The amount of
		/// capacity dedicated to such queue can vary overtime, depending of the
		/// allocation that has been determined. But it is guaranteed to satisfy all
		/// the constraint expressed by the user in the
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Protocolrecords.ReservationSubmissionRequest
		/// 	"/>
		/// .
		/// </p>
		/// </summary>
		/// <param name="request">the request to submit a new Reservation</param>
		/// <returns>
		/// response the
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ReservationId"/>
		/// on accepting the submission
		/// </returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException">
		/// if the request is invalid or reservation cannot be
		/// created successfully
		/// </exception>
		/// <exception cref="System.IO.IOException"/>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		ReservationSubmissionResponse SubmitReservation(ReservationSubmissionRequest request
			);

		/// <summary>
		/// <p>
		/// The interface used by clients to update an existing Reservation.
		/// </summary>
		/// <remarks>
		/// <p>
		/// The interface used by clients to update an existing Reservation. This is
		/// referred to as a re-negotiation process, in which a user that has
		/// previously submitted a Reservation.
		/// </p>
		/// <p>
		/// The allocation is attempted by virtually substituting all previous
		/// allocations related to this Reservation with new ones, that satisfy the new
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Protocolrecords.ReservationUpdateRequest"/>
		/// . Upon success the previous allocation is
		/// substituted by the new one, and on failure (i.e., if the system cannot find
		/// a valid allocation for the updated request), the previous allocation
		/// remains valid.
		/// The
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ReservationId"/>
		/// is not changed, and applications currently
		/// running within this reservation will automatically receive the resources
		/// based on the new allocation.
		/// </p>
		/// </remarks>
		/// <param name="request">
		/// to update an existing Reservation (the ReservationRequest
		/// should refer to an existing valid
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ReservationId"/>
		/// )
		/// </param>
		/// <returns>response empty on successfully updating the existing reservation</returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException">
		/// if the request is invalid or reservation cannot be
		/// updated successfully
		/// </exception>
		/// <exception cref="System.IO.IOException"/>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		ReservationUpdateResponse UpdateReservation(ReservationUpdateRequest request);

		/// <summary>
		/// <p>
		/// The interface used by clients to remove an existing Reservation.
		/// </summary>
		/// <remarks>
		/// <p>
		/// The interface used by clients to remove an existing Reservation.
		/// Upon deletion of a reservation applications running with this reservation,
		/// are automatically downgraded to normal jobs running without any dedicated
		/// reservation.
		/// </p>
		/// </remarks>
		/// <param name="request">
		/// to remove an existing Reservation (the ReservationRequest
		/// should refer to an existing valid
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ReservationId"/>
		/// )
		/// </param>
		/// <returns>response empty on successfully deleting the existing reservation</returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException">
		/// if the request is invalid or reservation cannot be
		/// deleted successfully
		/// </exception>
		/// <exception cref="System.IO.IOException"/>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		ReservationDeleteResponse DeleteReservation(ReservationDeleteRequest request);

		/// <summary>
		/// <p>
		/// The interface used by client to get node to labels mappings in existing cluster
		/// </p>
		/// </summary>
		/// <param name="request"/>
		/// <returns>node to labels mappings</returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		GetNodesToLabelsResponse GetNodeToLabels(GetNodesToLabelsRequest request);

		/// <summary>
		/// <p>
		/// The interface used by client to get labels to nodes mappings
		/// in existing cluster
		/// </p>
		/// </summary>
		/// <param name="request"/>
		/// <returns>labels to nodes mappings</returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		GetLabelsToNodesResponse GetLabelsToNodes(GetLabelsToNodesRequest request);

		/// <summary>
		/// <p>
		/// The interface used by client to get node labels in the cluster
		/// </p>
		/// </summary>
		/// <param name="request">to get node labels collection of this cluster</param>
		/// <returns>node labels collection of this cluster</returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		GetClusterNodeLabelsResponse GetClusterNodeLabels(GetClusterNodeLabelsRequest request
			);
	}
}
