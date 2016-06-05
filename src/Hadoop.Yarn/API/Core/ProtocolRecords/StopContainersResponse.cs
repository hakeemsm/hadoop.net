using System.Collections.Generic;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords
{
	/// <summary>
	/// <p>
	/// The response sent by the <code>NodeManager</code> to the
	/// <code>ApplicationMaster</code> when asked to <em>stop</em> allocated
	/// containers.
	/// </summary>
	/// <remarks>
	/// <p>
	/// The response sent by the <code>NodeManager</code> to the
	/// <code>ApplicationMaster</code> when asked to <em>stop</em> allocated
	/// containers.
	/// </p>
	/// </remarks>
	/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.ContainerManagementProtocol.StopContainers(StopContainersRequest)
	/// 	"/>
	public abstract class StopContainersResponse
	{
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public static StopContainersResponse NewInstance(IList<ContainerId> succeededRequests
			, IDictionary<ContainerId, SerializedException> failedRequests)
		{
			StopContainersResponse response = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<StopContainersResponse
				>();
			response.SetFailedRequests(failedRequests);
			response.SetSuccessfullyStoppedContainers(succeededRequests);
			return response;
		}

		/// <summary>Get the list of containerIds of successfully stopped containers.</summary>
		/// <returns>the list of containerIds of successfully stopped containers.</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract IList<ContainerId> GetSuccessfullyStoppedContainers();

		/// <summary>Set the list of containerIds of successfully stopped containers.</summary>
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetSuccessfullyStoppedContainers(IList<ContainerId> succeededRequests
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
			> failedRequests);
	}
}
