using System.Collections.Generic;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords
{
	/// <summary>
	/// <p>
	/// The response sent by the <code>NodeManager</code> to the
	/// <code>ApplicationMaster</code> when asked to <em>start</em> an allocated
	/// container.
	/// </summary>
	/// <remarks>
	/// <p>
	/// The response sent by the <code>NodeManager</code> to the
	/// <code>ApplicationMaster</code> when asked to <em>start</em> an allocated
	/// container.
	/// </p>
	/// </remarks>
	/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.ContainerManagementProtocol.StartContainers(StartContainersRequest)
	/// 	"/>
	public abstract class StartContainersResponse
	{
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public static StartContainersResponse NewInstance(IDictionary<string, ByteBuffer>
			 servicesMetaData, IList<ContainerId> succeededContainers, IDictionary<ContainerId
			, SerializedException> failedContainers)
		{
			StartContainersResponse response = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<
				StartContainersResponse>();
			response.SetAllServicesMetaData(servicesMetaData);
			response.SetSuccessfullyStartedContainers(succeededContainers);
			response.SetFailedRequests(failedContainers);
			return response;
		}

		/// <summary>
		/// Get the list of <code>ContainerId</code> s of the containers that are
		/// started successfully.
		/// </summary>
		/// <returns>
		/// the list of <code>ContainerId</code> s of the containers that are
		/// started successfully.
		/// </returns>
		/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.ContainerManagementProtocol.StartContainers(StartContainersRequest)
		/// 	"/>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract IList<ContainerId> GetSuccessfullyStartedContainers();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetSuccessfullyStartedContainers(IList<ContainerId> succeededContainers
			);

		/// <summary>
		/// Get the containerId-to-exception map in which the exception indicates error
		/// from per container for failed requests
		/// </summary>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract IDictionary<ContainerId, SerializedException> GetFailedRequests();

		/// <summary>
		/// Set the containerId-to-exception map in which the exception indicates error
		/// from per container for failed requests
		/// </summary>
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetFailedRequests(IDictionary<ContainerId, SerializedException
			> failedContainers);

		/// <summary>
		/// <p>
		/// Get the meta-data from all auxiliary services running on the
		/// <code>NodeManager</code>.
		/// </summary>
		/// <remarks>
		/// <p>
		/// Get the meta-data from all auxiliary services running on the
		/// <code>NodeManager</code>.
		/// </p>
		/// <p>
		/// The meta-data is returned as a Map between the auxiliary service names and
		/// their corresponding per service meta-data as an opaque blob
		/// <code>ByteBuffer</code>
		/// </p>
		/// <p>
		/// To be able to interpret the per-service meta-data, you should consult the
		/// documentation for the Auxiliary-service configured on the NodeManager
		/// </p>
		/// </remarks>
		/// <returns>
		/// a Map between the names of auxiliary services and their
		/// corresponding meta-data
		/// </returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract IDictionary<string, ByteBuffer> GetAllServicesMetaData();

		/// <summary>
		/// Set to the list of auxiliary services which have been started on the
		/// <code>NodeManager</code>.
		/// </summary>
		/// <remarks>
		/// Set to the list of auxiliary services which have been started on the
		/// <code>NodeManager</code>. This is done only once when the
		/// <code>NodeManager</code> starts up
		/// </remarks>
		/// <param name="allServicesMetaData">
		/// A map from auxiliary service names to the opaque blob
		/// <code>ByteBuffer</code> for that auxiliary service
		/// </param>
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetAllServicesMetaData(IDictionary<string, ByteBuffer> allServicesMetaData
			);
	}
}
