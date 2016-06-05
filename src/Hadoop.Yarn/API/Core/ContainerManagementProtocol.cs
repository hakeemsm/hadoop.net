using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api
{
	/// <summary>
	/// <p>The protocol between an <code>ApplicationMaster</code> and a
	/// <code>NodeManager</code> to start/stop containers and to get status
	/// of running containers.</p>
	/// <p>If security is enabled the <code>NodeManager</code> verifies that the
	/// <code>ApplicationMaster</code> has truly been allocated the container
	/// by the <code>ResourceManager</code> and also verifies all interactions such
	/// as stopping the container or obtaining status information for the container.
	/// </summary>
	/// <remarks>
	/// <p>The protocol between an <code>ApplicationMaster</code> and a
	/// <code>NodeManager</code> to start/stop containers and to get status
	/// of running containers.</p>
	/// <p>If security is enabled the <code>NodeManager</code> verifies that the
	/// <code>ApplicationMaster</code> has truly been allocated the container
	/// by the <code>ResourceManager</code> and also verifies all interactions such
	/// as stopping the container or obtaining status information for the container.
	/// </p>
	/// </remarks>
	public interface ContainerManagementProtocol
	{
		/// <summary>
		/// <p>
		/// The <code>ApplicationMaster</code> provides a list of
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Protocolrecords.StartContainerRequest"/>
		/// s to a <code>NodeManager</code> to
		/// <em>start</em>
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.Container"/>
		/// s allocated to it using this interface.
		/// </p>
		/// <p>
		/// The <code>ApplicationMaster</code> has to provide details such as allocated
		/// resource capability, security tokens (if enabled), command to be executed
		/// to start the container, environment for the process, necessary
		/// binaries/jar/shared-objects etc. via the
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ContainerLaunchContext"/>
		/// in
		/// the
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Protocolrecords.StartContainerRequest"/>
		/// .
		/// </p>
		/// <p>
		/// The <code>NodeManager</code> sends a response via
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Protocolrecords.StartContainersResponse"/>
		/// which includes a list of
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.Container"/>
		/// s of successfully launched
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.Container"/>
		/// s, a
		/// containerId-to-exception map for each failed
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Protocolrecords.StartContainerRequest"/>
		/// in
		/// which the exception indicates errors from per container and a
		/// allServicesMetaData map between the names of auxiliary services and their
		/// corresponding meta-data. Note: None-container-specific exceptions will
		/// still be thrown by the API method itself.
		/// </p>
		/// <p>
		/// The <code>ApplicationMaster</code> can use
		/// <see cref="GetContainerStatuses(Org.Apache.Hadoop.Yarn.Api.Protocolrecords.GetContainerStatusesRequest)
		/// 	"/>
		/// to get updated
		/// statuses of the to-be-launched or launched containers.
		/// </p>
		/// </summary>
		/// <param name="request">request to start a list of containers</param>
		/// <returns>
		/// response including conatinerIds of all successfully launched
		/// containers, a containerId-to-exception map for failed requests and
		/// a allServicesMetaData map.
		/// </returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.NMNotYetReadyException">
		/// This exception is thrown when NM starts from scratch but has not
		/// yet connected with RM.
		/// </exception>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		StartContainersResponse StartContainers(StartContainersRequest request);

		/// <summary>
		/// <p>
		/// The <code>ApplicationMaster</code> requests a <code>NodeManager</code> to
		/// <em>stop</em> a list of
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.Container"/>
		/// s allocated to it using this
		/// interface.
		/// </p>
		/// <p>
		/// The <code>ApplicationMaster</code> sends a
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Protocolrecords.StopContainersRequest"/>
		/// which includes the
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ContainerId"/>
		/// s of the containers to be stopped.
		/// </p>
		/// <p>
		/// The <code>NodeManager</code> sends a response via
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Protocolrecords.StopContainersResponse"/>
		/// which includes a list of
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ContainerId"/>
		/// s of successfully stopped containers, a containerId-to-exception map for
		/// each failed request in which the exception indicates errors from per
		/// container. Note: None-container-specific exceptions will still be thrown by
		/// the API method itself. <code>ApplicationMaster</code> can use
		/// <see cref="GetContainerStatuses(Org.Apache.Hadoop.Yarn.Api.Protocolrecords.GetContainerStatusesRequest)
		/// 	"/>
		/// to get updated
		/// statuses of the containers.
		/// </p>
		/// </summary>
		/// <param name="request">request to stop a list of containers</param>
		/// <returns>
		/// response which includes a list of containerIds of successfully
		/// stopped containers, a containerId-to-exception map for failed
		/// requests.
		/// </returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		StopContainersResponse StopContainers(StopContainersRequest request);

		/// <summary>
		/// <p>
		/// The API used by the <code>ApplicationMaster</code> to request for current
		/// statuses of <code>Container</code>s from the <code>NodeManager</code>.
		/// </summary>
		/// <remarks>
		/// <p>
		/// The API used by the <code>ApplicationMaster</code> to request for current
		/// statuses of <code>Container</code>s from the <code>NodeManager</code>.
		/// </p>
		/// <p>
		/// The <code>ApplicationMaster</code> sends a
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Protocolrecords.GetContainerStatusesRequest
		/// 	"/>
		/// which includes the
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ContainerId"/>
		/// s
		/// of all containers whose statuses are needed.
		/// </p>
		/// <p>
		/// The <code>NodeManager</code> responds with
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Protocolrecords.GetContainerStatusesResponse
		/// 	"/>
		/// which includes a list of
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ContainerStatus"/>
		/// of the successfully queried containers and a
		/// containerId-to-exception map for each failed request in which the exception
		/// indicates errors from per container. Note: None-container-specific
		/// exceptions will still be thrown by the API method itself.
		/// </p>
		/// </remarks>
		/// <param name="request">
		/// request to get <code>ContainerStatus</code>es of containers with
		/// the specified <code>ContainerId</code>s
		/// </param>
		/// <returns>
		/// response containing the list of <code>ContainerStatus</code> of the
		/// successfully queried containers and a containerId-to-exception map
		/// for failed requests.
		/// </returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		GetContainerStatusesResponse GetContainerStatuses(GetContainerStatusesRequest request
			);
	}
}
