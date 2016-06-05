using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.IO.Retry;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api
{
	/// <summary>
	/// <p>
	/// The protocol between clients and the <code>ResourceManager</code> or
	/// <code>ApplicationHistoryServer</code> to get information on applications,
	/// application attempts and containers.
	/// </summary>
	/// <remarks>
	/// <p>
	/// The protocol between clients and the <code>ResourceManager</code> or
	/// <code>ApplicationHistoryServer</code> to get information on applications,
	/// application attempts and containers.
	/// </p>
	/// </remarks>
	public interface ApplicationBaseProtocol
	{
		/// <summary>
		/// The interface used by clients to get a report of an Application from the
		/// <code>ResourceManager</code> or <code>ApplicationHistoryServer</code>.
		/// </summary>
		/// <remarks>
		/// The interface used by clients to get a report of an Application from the
		/// <code>ResourceManager</code> or <code>ApplicationHistoryServer</code>.
		/// <p>
		/// The client, via
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Protocolrecords.GetApplicationReportRequest
		/// 	"/>
		/// provides the
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationId"/>
		/// of the application.
		/// <p>
		/// In secure mode,the <code>ResourceManager</code> or
		/// <code>ApplicationHistoryServer</code> verifies access to the application,
		/// queue etc. before accepting the request.
		/// <p>
		/// The <code>ResourceManager</code> or <code>ApplicationHistoryServer</code>
		/// responds with a
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Protocolrecords.GetApplicationReportResponse
		/// 	"/>
		/// which includes the
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationReport"/>
		/// for the application.
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
		/// <param name="request">request for an application report</param>
		/// <returns>application report</returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		[Idempotent]
		GetApplicationReportResponse GetApplicationReport(GetApplicationReportRequest request
			);

		/// <summary>
		/// <p>
		/// The interface used by clients to get a report of Applications matching the
		/// filters defined by
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Protocolrecords.GetApplicationsRequest"/>
		/// in the cluster from the
		/// <code>ResourceManager</code> or <code>ApplicationHistoryServer</code>.
		/// </p>
		/// <p>
		/// The <code>ResourceManager</code> or <code>ApplicationHistoryServer</code>
		/// responds with a
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Protocolrecords.GetApplicationsResponse"/>
		/// which includes the
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationReport"/>
		/// for the applications.
		/// </p>
		/// <p>
		/// If the user does not have <code>VIEW_APP</code> access for an application
		/// then the corresponding report will be filtered as described in
		/// <see cref="GetApplicationReport(Org.Apache.Hadoop.Yarn.Api.Protocolrecords.GetApplicationReportRequest)
		/// 	"/>
		/// .
		/// </p>
		/// </summary>
		/// <param name="request">request for report on applications</param>
		/// <returns>
		/// report on applications matching the given application types defined
		/// in the request
		/// </returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.Protocolrecords.GetApplicationsRequest"
		/// 	/>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		[Idempotent]
		GetApplicationsResponse GetApplications(GetApplicationsRequest request);

		/// <summary>
		/// The interface used by clients to get a report of an Application Attempt
		/// from the <code>ResourceManager</code> or
		/// <code>ApplicationHistoryServer</code>
		/// <p>
		/// The client, via
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Protocolrecords.GetApplicationAttemptReportRequest
		/// 	"/>
		/// provides the
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationAttemptId"/>
		/// of the application attempt.
		/// <p>
		/// In secure mode,the <code>ResourceManager</code> or
		/// <code>ApplicationHistoryServer</code> verifies access to the method before
		/// accepting the request.
		/// <p>
		/// The <code>ResourceManager</code> or <code>ApplicationHistoryServer</code>
		/// responds with a
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Protocolrecords.GetApplicationAttemptReportResponse
		/// 	"/>
		/// which includes
		/// the
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationAttemptReport"/>
		/// for the application attempt.
		/// <p>
		/// If the user does not have <code>VIEW_APP</code> access then the following
		/// fields in the report will be set to stubbed values:
		/// <ul>
		/// <li>host</li>
		/// <li>RPC port</li>
		/// <li>client token</li>
		/// <li>diagnostics - set to "N/A"</li>
		/// <li>tracking URL</li>
		/// </ul>
		/// </summary>
		/// <param name="request">request for an application attempt report</param>
		/// <returns>application attempt report</returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		[Idempotent]
		GetApplicationAttemptReportResponse GetApplicationAttemptReport(GetApplicationAttemptReportRequest
			 request);

		/// <summary>
		/// <p>
		/// The interface used by clients to get a report of all Application attempts
		/// in the cluster from the <code>ResourceManager</code> or
		/// <code>ApplicationHistoryServer</code>
		/// </p>
		/// <p>
		/// The <code>ResourceManager</code> or <code>ApplicationHistoryServer</code>
		/// responds with a
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Protocolrecords.GetApplicationAttemptsRequest
		/// 	"/>
		/// which includes the
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationAttemptReport"/>
		/// for all the applications attempts of a
		/// specified application attempt.
		/// </p>
		/// <p>
		/// If the user does not have <code>VIEW_APP</code> access for an application
		/// then the corresponding report will be filtered as described in
		/// <see cref="GetApplicationAttemptReport(Org.Apache.Hadoop.Yarn.Api.Protocolrecords.GetApplicationAttemptReportRequest)
		/// 	"/>
		/// .
		/// </p>
		/// </summary>
		/// <param name="request">request for reports on all application attempts of an application
		/// 	</param>
		/// <returns>reports on all application attempts of an application</returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		[Idempotent]
		GetApplicationAttemptsResponse GetApplicationAttempts(GetApplicationAttemptsRequest
			 request);

		/// <summary>
		/// <p>
		/// The interface used by clients to get a report of an Container from the
		/// <code>ResourceManager</code> or <code>ApplicationHistoryServer</code>
		/// </p>
		/// <p>
		/// The client, via
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Protocolrecords.GetContainerReportRequest"/
		/// 	>
		/// provides the
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ContainerId"/>
		/// of the container.
		/// </p>
		/// <p>
		/// In secure mode,the <code>ResourceManager</code> or
		/// <code>ApplicationHistoryServer</code> verifies access to the method before
		/// accepting the request.
		/// </p>
		/// <p>
		/// The <code>ResourceManager</code> or <code>ApplicationHistoryServer</code>
		/// responds with a
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Protocolrecords.GetContainerReportResponse"
		/// 	/>
		/// which includes the
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ContainerReport"/>
		/// for the container.
		/// </p>
		/// </summary>
		/// <param name="request">request for a container report</param>
		/// <returns>container report</returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		[Idempotent]
		GetContainerReportResponse GetContainerReport(GetContainerReportRequest request);

		/// <summary>
		/// <p>
		/// The interface used by clients to get a report of Containers for an
		/// application attempt from the <code>ResourceManager</code> or
		/// <code>ApplicationHistoryServer</code>
		/// </p>
		/// <p>
		/// The client, via
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Protocolrecords.GetContainersRequest"/>
		/// provides the
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationAttemptId"/>
		/// of the application attempt.
		/// </p>
		/// <p>
		/// In secure mode,the <code>ResourceManager</code> or
		/// <code>ApplicationHistoryServer</code> verifies access to the method before
		/// accepting the request.
		/// </p>
		/// <p>
		/// The <code>ResourceManager</code> or <code>ApplicationHistoryServer</code>
		/// responds with a
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Protocolrecords.GetContainersResponse"/>
		/// which includes a list of
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ContainerReport"/>
		/// for all the containers of a specific application
		/// attempt.
		/// </p>
		/// </summary>
		/// <param name="request">request for a list of container reports of an application attempt.
		/// 	</param>
		/// <returns>reports on all containers of an application attempt</returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		[Idempotent]
		GetContainersResponse GetContainers(GetContainersRequest request);

		/// <summary>
		/// <p>
		/// The interface used by clients to get delegation token, enabling the
		/// containers to be able to talk to the service using those tokens.
		/// </summary>
		/// <remarks>
		/// <p>
		/// The interface used by clients to get delegation token, enabling the
		/// containers to be able to talk to the service using those tokens.
		/// <p>
		/// The <code>ResourceManager</code> or <code>ApplicationHistoryServer</code>
		/// responds with the delegation
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.Token"/>
		/// that can be used by the client
		/// to speak to this service.
		/// </remarks>
		/// <param name="request">request to get a delegation token for the client.</param>
		/// <returns>delegation token that can be used to talk to this service</returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		[Idempotent]
		GetDelegationTokenResponse GetDelegationToken(GetDelegationTokenRequest request);

		/// <summary>
		/// Renew an existing delegation
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.Token"/>
		/// .
		/// </summary>
		/// <param name="request">the delegation token to be renewed.</param>
		/// <returns>the new expiry time for the delegation token.</returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		[Idempotent]
		RenewDelegationTokenResponse RenewDelegationToken(RenewDelegationTokenRequest request
			);

		/// <summary>
		/// Cancel an existing delegation
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.Token"/>
		/// .
		/// </summary>
		/// <param name="request">the delegation token to be cancelled.</param>
		/// <returns>an empty response.</returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		[Idempotent]
		CancelDelegationTokenResponse CancelDelegationToken(CancelDelegationTokenRequest 
			request);
	}
}
