using System.Collections.Generic;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords
{
	/// <summary>
	/// The request sent by the <code>ApplicationMaster</code> to the
	/// <code>NodeManager</code> to get
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ContainerStatus"/>
	/// of requested
	/// containers.
	/// </summary>
	/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.ContainerManagementProtocol.GetContainerStatuses(GetContainerStatusesRequest)
	/// 	"/>
	public abstract class GetContainerStatusesRequest
	{
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public static GetContainerStatusesRequest NewInstance(IList<ContainerId> containerIds
			)
		{
			GetContainerStatusesRequest request = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord
				<GetContainerStatusesRequest>();
			request.SetContainerIds(containerIds);
			return request;
		}

		/// <summary>
		/// Get the list of <code>ContainerId</code>s of containers for which to obtain
		/// the <code>ContainerStatus</code>.
		/// </summary>
		/// <returns>
		/// the list of <code>ContainerId</code>s of containers for which to
		/// obtain the <code>ContainerStatus</code>.
		/// </returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract IList<ContainerId> GetContainerIds();

		/// <summary>
		/// Set a list of <code>ContainerId</code>s of containers for which to obtain
		/// the <code>ContainerStatus</code>
		/// </summary>
		/// <param name="containerIds">
		/// a list of <code>ContainerId</code>s of containers for which to
		/// obtain the <code>ContainerStatus</code>
		/// </param>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract void SetContainerIds(IList<ContainerId> containerIds);
	}
}
