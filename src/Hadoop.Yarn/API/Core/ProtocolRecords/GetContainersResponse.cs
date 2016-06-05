using System.Collections.Generic;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords
{
	/// <summary>
	/// <p>
	/// The response sent by the <code>ResourceManager</code> to a client requesting
	/// a list of
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ContainerReport"/>
	/// for containers.
	/// </p>
	/// <p>
	/// The <code>ContainerReport</code> for each container includes the container
	/// details.
	/// </p>
	/// </summary>
	/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.Records.ContainerReport"/>
	/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.ApplicationBaseProtocol.GetContainers(GetContainersRequest)
	/// 	"/>
	public abstract class GetContainersResponse
	{
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public static GetContainersResponse NewInstance(IList<ContainerReport> containers
			)
		{
			GetContainersResponse response = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<GetContainersResponse
				>();
			response.SetContainerList(containers);
			return response;
		}

		/// <summary>
		/// Get a list of <code>ContainerReport</code> for all the containers of an
		/// application attempt.
		/// </summary>
		/// <returns>
		/// a list of <code>ContainerReport</code> for all the containers of an
		/// application attempt
		/// </returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract IList<ContainerReport> GetContainerList();

		/// <summary>
		/// Set a list of <code>ContainerReport</code> for all the containers of an
		/// application attempt.
		/// </summary>
		/// <param name="containers">
		/// a list of <code>ContainerReport</code> for all the containers of
		/// an application attempt
		/// </param>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract void SetContainerList(IList<ContainerReport> containers);
	}
}
