using System.Collections.Generic;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords
{
	/// <summary>
	/// The response sent by the <code>NodeManager</code> to the
	/// <code>ApplicationMaster</code> when asked to obtain the
	/// <code>ContainerStatus</code> of requested containers.
	/// </summary>
	/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.ContainerManagementProtocol.GetContainerStatuses(GetContainerStatusesRequest)
	/// 	"/>
	public abstract class GetContainerStatusesResponse
	{
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public static GetContainerStatusesResponse NewInstance(IList<ContainerStatus> statuses
			, IDictionary<ContainerId, SerializedException> failedRequests)
		{
			GetContainerStatusesResponse response = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord
				<GetContainerStatusesResponse>();
			response.SetContainerStatuses(statuses);
			response.SetFailedRequests(failedRequests);
			return response;
		}

		/// <summary>Get the <code>ContainerStatus</code>es of the requested containers.</summary>
		/// <returns><code>ContainerStatus</code>es of the requested containers.</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract IList<ContainerStatus> GetContainerStatuses();

		/// <summary>Set the <code>ContainerStatus</code>es of the requested containers.</summary>
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetContainerStatuses(IList<ContainerStatus> statuses);

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
	}
}
