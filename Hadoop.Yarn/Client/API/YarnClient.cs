using System.Collections.Generic;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Client.Api.Impl;
using Org.Apache.Hadoop.Yarn.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Client.Api
{
	public abstract class YarnClient : AbstractService
	{
		/// <summary>Create a new instance of YarnClient.</summary>
		[InterfaceAudience.Public]
		public static Org.Apache.Hadoop.Yarn.Client.Api.YarnClient CreateYarnClient()
		{
			Org.Apache.Hadoop.Yarn.Client.Api.YarnClient client = new YarnClientImpl();
			return client;
		}

		[InterfaceAudience.Private]
		protected internal YarnClient(string name)
			: base(name)
		{
		}

		/// <summary>
		/// <p>
		/// Obtain a
		/// <see cref="YarnClientApplication"/>
		/// for a new application,
		/// which in turn contains the
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationSubmissionContext"/>
		/// and
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Protocolrecords.GetNewApplicationResponse"/
		/// 	>
		/// objects.
		/// </p>
		/// </summary>
		/// <returns>
		/// 
		/// <see cref="YarnClientApplication"/>
		/// built for a new application
		/// </returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public abstract YarnClientApplication CreateApplication();

		/// <summary>
		/// <p>
		/// Submit a new application to <code>YARN.</code> It is a blocking call - it
		/// will not return
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationId"/>
		/// until the submitted application is
		/// submitted successfully and accepted by the ResourceManager.
		/// </p>
		/// <p>
		/// Users should provide an
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationId"/>
		/// as part of the parameter
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationSubmissionContext"/>
		/// when submitting a new application,
		/// otherwise it will throw the
		/// <see cref="Org.Apache.Hadoop.Yarn.Exceptions.ApplicationIdNotProvidedException"/>
		/// .
		/// </p>
		/// <p>This internally calls
		/// <see cref="ApplicationClientProtocol#submitApplication(SubmitApplicationRequest)"
		/// 	/>
		/// , and after that, it internally invokes
		/// <see cref="ApplicationClientProtocol#getApplicationReport(GetApplicationReportRequest)
		/// 	"/>
		/// and waits till it can make sure that the
		/// application gets properly submitted. If RM fails over or RM restart
		/// happens before ResourceManager saves the application's state,
		/// <see cref="ApplicationClientProtocol#getApplicationReport(GetApplicationReportRequest)
		/// 	"/>
		/// will throw
		/// the
		/// <see cref="Org.Apache.Hadoop.Yarn.Exceptions.ApplicationNotFoundException"/>
		/// . This API automatically resubmits
		/// the application with the same
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationSubmissionContext"/>
		/// when it
		/// catches the
		/// <see cref="Org.Apache.Hadoop.Yarn.Exceptions.ApplicationNotFoundException"/>
		/// </p>
		/// </summary>
		/// <param name="appContext">
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationSubmissionContext"/>
		/// containing all the details
		/// needed to submit a new application
		/// </param>
		/// <returns>
		/// 
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationId"/>
		/// of the accepted application
		/// </returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		/// <seealso cref="CreateApplication()"/>
		public abstract ApplicationId SubmitApplication(ApplicationSubmissionContext appContext
			);

		/// <summary>
		/// <p>
		/// Kill an application identified by given ID.
		/// </summary>
		/// <remarks>
		/// <p>
		/// Kill an application identified by given ID.
		/// </p>
		/// </remarks>
		/// <param name="applicationId">
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationId"/>
		/// of the application that needs to be killed
		/// </param>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException">
		/// in case of errors or if YARN rejects the request due to
		/// access-control restrictions.
		/// </exception>
		/// <exception cref="System.IO.IOException"/>
		/// <seealso cref="GetQueueAclsInfo()"/>
		public abstract void KillApplication(ApplicationId applicationId);

		/// <summary>
		/// <p>
		/// Get a report of the given Application.
		/// </summary>
		/// <remarks>
		/// <p>
		/// Get a report of the given Application.
		/// </p>
		/// <p>
		/// In secure mode, <code>YARN</code> verifies access to the application, queue
		/// etc. before accepting the request.
		/// </p>
		/// <p>
		/// If the user does not have <code>VIEW_APP</code> access then the following
		/// fields in the report will be set to stubbed values:
		/// <ul>
		/// <li>host - set to "N/A"</li>
		/// <li>RPC port - set to -1</li>
		/// <li>client token - set to "N/A"</li>
		/// <li>diagnostics - set to "N/A"</li>
		/// <li>tracking URL - set to "N/A"</li>
		/// <li>original tracking URL - set to "N/A"</li>
		/// <li>resource usage report - all values are -1</li>
		/// </ul>
		/// </remarks>
		/// <param name="appId">
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationId"/>
		/// of the application that needs a report
		/// </param>
		/// <returns>application report</returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public abstract ApplicationReport GetApplicationReport(ApplicationId appId);

		/// <summary>Get the AMRM token of the application.</summary>
		/// <remarks>
		/// Get the AMRM token of the application.
		/// <p>
		/// The AMRM token is required for AM to RM scheduling operations. For
		/// managed Application Masters Yarn takes care of injecting it. For unmanaged
		/// Applications Masters, the token must be obtained via this method and set
		/// in the
		/// <see cref="Org.Apache.Hadoop.Security.UserGroupInformation"/>
		/// of the
		/// current user.
		/// <p>
		/// The AMRM token will be returned only if all the following conditions are
		/// met:
		/// <ul>
		/// <li>the requester is the owner of the ApplicationMaster</li>
		/// <li>the application master is an unmanaged ApplicationMaster</li>
		/// <li>the application master is in ACCEPTED state</li>
		/// </ul>
		/// Else this method returns NULL.
		/// </remarks>
		/// <param name="appId">
		/// 
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationId"/>
		/// of the application to get the AMRM token
		/// </param>
		/// <returns>the AMRM token if available</returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public abstract Org.Apache.Hadoop.Security.Token.Token<AMRMTokenIdentifier> GetAMRMToken
			(ApplicationId appId);

		/// <summary>
		/// <p>
		/// Get a report (ApplicationReport) of all Applications in the cluster.
		/// </summary>
		/// <remarks>
		/// <p>
		/// Get a report (ApplicationReport) of all Applications in the cluster.
		/// </p>
		/// <p>
		/// If the user does not have <code>VIEW_APP</code> access for an application
		/// then the corresponding report will be filtered as described in
		/// <see cref="GetApplicationReport(Org.Apache.Hadoop.Yarn.Api.Records.ApplicationId)
		/// 	"/>
		/// .
		/// </p>
		/// </remarks>
		/// <returns>a list of reports of all running applications</returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public abstract IList<ApplicationReport> GetApplications();

		/// <summary>
		/// <p>
		/// Get a report (ApplicationReport) of Applications
		/// matching the given application types in the cluster.
		/// </summary>
		/// <remarks>
		/// <p>
		/// Get a report (ApplicationReport) of Applications
		/// matching the given application types in the cluster.
		/// </p>
		/// <p>
		/// If the user does not have <code>VIEW_APP</code> access for an application
		/// then the corresponding report will be filtered as described in
		/// <see cref="GetApplicationReport(Org.Apache.Hadoop.Yarn.Api.Records.ApplicationId)
		/// 	"/>
		/// .
		/// </p>
		/// </remarks>
		/// <param name="applicationTypes"/>
		/// <returns>a list of reports of applications</returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public abstract IList<ApplicationReport> GetApplications(ICollection<string> applicationTypes
			);

		/// <summary>
		/// <p>
		/// Get a report (ApplicationReport) of Applications matching the given
		/// application states in the cluster.
		/// </summary>
		/// <remarks>
		/// <p>
		/// Get a report (ApplicationReport) of Applications matching the given
		/// application states in the cluster.
		/// </p>
		/// <p>
		/// If the user does not have <code>VIEW_APP</code> access for an application
		/// then the corresponding report will be filtered as described in
		/// <see cref="GetApplicationReport(Org.Apache.Hadoop.Yarn.Api.Records.ApplicationId)
		/// 	"/>
		/// .
		/// </p>
		/// </remarks>
		/// <param name="applicationStates"/>
		/// <returns>a list of reports of applications</returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public abstract IList<ApplicationReport> GetApplications(EnumSet<YarnApplicationState
			> applicationStates);

		/// <summary>
		/// <p>
		/// Get a report (ApplicationReport) of Applications matching the given
		/// application types and application states in the cluster.
		/// </summary>
		/// <remarks>
		/// <p>
		/// Get a report (ApplicationReport) of Applications matching the given
		/// application types and application states in the cluster.
		/// </p>
		/// <p>
		/// If the user does not have <code>VIEW_APP</code> access for an application
		/// then the corresponding report will be filtered as described in
		/// <see cref="GetApplicationReport(Org.Apache.Hadoop.Yarn.Api.Records.ApplicationId)
		/// 	"/>
		/// .
		/// </p>
		/// </remarks>
		/// <param name="applicationTypes"/>
		/// <param name="applicationStates"/>
		/// <returns>a list of reports of applications</returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public abstract IList<ApplicationReport> GetApplications(ICollection<string> applicationTypes
			, EnumSet<YarnApplicationState> applicationStates);

		/// <summary>
		/// <p>
		/// Get metrics (
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.YarnClusterMetrics"/>
		/// ) about the cluster.
		/// </p>
		/// </summary>
		/// <returns>cluster metrics</returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public abstract YarnClusterMetrics GetYarnClusterMetrics();

		/// <summary>
		/// <p>
		/// Get a report of nodes (
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.NodeReport"/>
		/// ) in the cluster.
		/// </p>
		/// </summary>
		/// <param name="states">
		/// The
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.NodeState"/>
		/// s to filter on. If no filter states are
		/// given, nodes in all states will be returned.
		/// </param>
		/// <returns>A list of node reports</returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public abstract IList<NodeReport> GetNodeReports(params NodeState[] states);

		/// <summary>
		/// <p>
		/// Get a delegation token so as to be able to talk to YARN using those tokens.
		/// </summary>
		/// <param name="renewer">
		/// Address of the renewer who can renew these tokens when needed by
		/// securely talking to YARN.
		/// </param>
		/// <returns>
		/// a delegation token (
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.Token"/>
		/// ) that can be used to
		/// talk to YARN
		/// </returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public abstract Org.Apache.Hadoop.Yarn.Api.Records.Token GetRMDelegationToken(Text
			 renewer);

		/// <summary>
		/// <p>
		/// Get information (
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.QueueInfo"/>
		/// ) about a given <em>queue</em>.
		/// </p>
		/// </summary>
		/// <param name="queueName">Name of the queue whose information is needed</param>
		/// <returns>queue information</returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException">
		/// in case of errors or if YARN rejects the request due to
		/// access-control restrictions.
		/// </exception>
		/// <exception cref="System.IO.IOException"/>
		public abstract QueueInfo GetQueueInfo(string queueName);

		/// <summary>
		/// <p>
		/// Get information (
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.QueueInfo"/>
		/// ) about all queues, recursively if there
		/// is a hierarchy
		/// </p>
		/// </summary>
		/// <returns>a list of queue-information for all queues</returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public abstract IList<QueueInfo> GetAllQueues();

		/// <summary>
		/// <p>
		/// Get information (
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.QueueInfo"/>
		/// ) about top level queues.
		/// </p>
		/// </summary>
		/// <returns>a list of queue-information for all the top-level queues</returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public abstract IList<QueueInfo> GetRootQueueInfos();

		/// <summary>
		/// <p>
		/// Get information (
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.QueueInfo"/>
		/// ) about all the immediate children queues
		/// of the given queue
		/// </p>
		/// </summary>
		/// <param name="parent">Name of the queue whose child-queues' information is needed</param>
		/// <returns>
		/// a list of queue-information for all queues who are direct children
		/// of the given parent queue.
		/// </returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public abstract IList<QueueInfo> GetChildQueueInfos(string parent);

		/// <summary>
		/// <p>
		/// Get information about <em>acls</em> for <em>current user</em> on all the
		/// existing queues.
		/// </summary>
		/// <remarks>
		/// <p>
		/// Get information about <em>acls</em> for <em>current user</em> on all the
		/// existing queues.
		/// </p>
		/// </remarks>
		/// <returns>
		/// a list of queue acls (
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.QueueUserACLInfo"/>
		/// ) for
		/// <em>current user</em>
		/// </returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public abstract IList<QueueUserACLInfo> GetQueueAclsInfo();

		/// <summary>
		/// <p>
		/// Get a report of the given ApplicationAttempt.
		/// </summary>
		/// <remarks>
		/// <p>
		/// Get a report of the given ApplicationAttempt.
		/// </p>
		/// <p>
		/// In secure mode, <code>YARN</code> verifies access to the application, queue
		/// etc. before accepting the request.
		/// </p>
		/// </remarks>
		/// <param name="applicationAttemptId">
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationAttemptId"/>
		/// of the application attempt that needs
		/// a report
		/// </param>
		/// <returns>application attempt report</returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.ApplicationAttemptNotFoundException
		/// 	">
		/// if application attempt
		/// not found
		/// </exception>
		/// <exception cref="System.IO.IOException"/>
		public abstract ApplicationAttemptReport GetApplicationAttemptReport(ApplicationAttemptId
			 applicationAttemptId);

		/// <summary>
		/// <p>
		/// Get a report of all (ApplicationAttempts) of Application in the cluster.
		/// </summary>
		/// <remarks>
		/// <p>
		/// Get a report of all (ApplicationAttempts) of Application in the cluster.
		/// </p>
		/// </remarks>
		/// <param name="applicationId"/>
		/// <returns>
		/// a list of reports for all application attempts for specified
		/// application.
		/// </returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public abstract IList<ApplicationAttemptReport> GetApplicationAttempts(ApplicationId
			 applicationId);

		/// <summary>
		/// <p>
		/// Get a report of the given Container.
		/// </summary>
		/// <remarks>
		/// <p>
		/// Get a report of the given Container.
		/// </p>
		/// <p>
		/// In secure mode, <code>YARN</code> verifies access to the application, queue
		/// etc. before accepting the request.
		/// </p>
		/// </remarks>
		/// <param name="containerId">
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ContainerId"/>
		/// of the container that needs a report
		/// </param>
		/// <returns>container report</returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.ContainerNotFoundException">if container not found.
		/// 	</exception>
		/// <exception cref="System.IO.IOException"/>
		public abstract ContainerReport GetContainerReport(ContainerId containerId);

		/// <summary>
		/// <p>
		/// Get a report of all (Containers) of ApplicationAttempt in the cluster.
		/// </summary>
		/// <remarks>
		/// <p>
		/// Get a report of all (Containers) of ApplicationAttempt in the cluster.
		/// </p>
		/// </remarks>
		/// <param name="applicationAttemptId"/>
		/// <returns>
		/// a list of reports of all containers for specified application
		/// attempts
		/// </returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public abstract IList<ContainerReport> GetContainers(ApplicationAttemptId applicationAttemptId
			);

		/// <summary>
		/// <p>
		/// Attempts to move the given application to the given queue.
		/// </summary>
		/// <remarks>
		/// <p>
		/// Attempts to move the given application to the given queue.
		/// </p>
		/// </remarks>
		/// <param name="appId">Application to move.</param>
		/// <param name="queue">Queue to place it in to.</param>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public abstract void MoveApplicationAcrossQueues(ApplicationId appId, string queue
			);

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
		/// about the amount of capacity, temporal constraints, and gang needs.
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
		/// that the ReservationRequest is satisfiable the
		/// <c>ResourceManager</c>
		/// answers with a
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Protocolrecords.ReservationSubmissionResponse
		/// 	"/>
		/// that includes a
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ReservationId"/>
		/// . Upon failure to find a valid allocation the response
		/// is an exception with the message detailing the reason of failure.
		/// </p>
		/// <p>
		/// The semantics guarantees that the
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ReservationId"/>
		/// returned,
		/// corresponds to a valid reservation existing in the time-range request by
		/// the user. The amount of capacity dedicated to such reservation can vary
		/// overtime, depending of the allocation that has been determined. But it is
		/// guaranteed to satisfy all the constraint expressed by the user in the
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ReservationDefinition"/>
		/// </p>
		/// </summary>
		/// <param name="request">request to submit a new Reservation</param>
		/// <returns>
		/// response contains the
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ReservationId"/>
		/// on accepting the
		/// submission
		/// </returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException">if the reservation cannot be created successfully
		/// 	</exception>
		/// <exception cref="System.IO.IOException"/>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract ReservationSubmissionResponse SubmitReservation(ReservationSubmissionRequest
			 request);

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
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ReservationDefinition"/>
		/// . Upon success the previous allocation is
		/// atomically substituted by the new one, and on failure (i.e., if the system
		/// cannot find a valid allocation for the updated request), the previous
		/// allocation remains valid.
		/// </p>
		/// </remarks>
		/// <param name="request">
		/// to update an existing Reservation (the
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Protocolrecords.ReservationUpdateRequest"/>
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
		public abstract ReservationUpdateResponse UpdateReservation(ReservationUpdateRequest
			 request);

		/// <summary>
		/// <p>
		/// The interface used by clients to remove an existing Reservation.
		/// </summary>
		/// <remarks>
		/// <p>
		/// The interface used by clients to remove an existing Reservation.
		/// </p>
		/// </remarks>
		/// <param name="request">
		/// to remove an existing Reservation (the
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Protocolrecords.ReservationDeleteRequest"/>
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
		public abstract ReservationDeleteResponse DeleteReservation(ReservationDeleteRequest
			 request);

		/// <summary>
		/// <p>
		/// The interface used by client to get node to labels mappings in existing cluster
		/// </p>
		/// </summary>
		/// <returns>node to labels mappings</returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract IDictionary<NodeId, ICollection<string>> GetNodeToLabels();

		/// <summary>
		/// <p>
		/// The interface used by client to get labels to nodes mapping
		/// in existing cluster
		/// </p>
		/// </summary>
		/// <returns>node to labels mappings</returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract IDictionary<string, ICollection<NodeId>> GetLabelsToNodes();

		/// <summary>
		/// <p>
		/// The interface used by client to get labels to nodes mapping
		/// for specified labels in existing cluster
		/// </p>
		/// </summary>
		/// <param name="labels">labels for which labels to nodes mapping has to be retrieved
		/// 	</param>
		/// <returns>labels to nodes mappings for specific labels</returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract IDictionary<string, ICollection<NodeId>> GetLabelsToNodes(ICollection
			<string> labels);

		/// <summary>
		/// <p>
		/// The interface used by client to get node labels in the cluster
		/// </p>
		/// </summary>
		/// <returns>cluster node labels collection</returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract ICollection<string> GetClusterNodeLabels();
	}
}
