using System.Collections.Generic;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords
{
	/// <summary>
	/// <p>The request sent by the <code>ApplicationMaster</code> to the
	/// <code>NodeManager</code> to <em>stop</em> containers.</p>
	/// </summary>
	/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.ContainerManagementProtocol.StopContainers(StopContainersRequest)
	/// 	"/>
	public abstract class StopContainersRequest
	{
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public static StopContainersRequest NewInstance(IList<ContainerId> containerIds)
		{
			StopContainersRequest request = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<StopContainersRequest
				>();
			request.SetContainerIds(containerIds);
			return request;
		}

		/// <summary>Get the <code>ContainerId</code>s of the containers to be stopped.</summary>
		/// <returns><code>ContainerId</code>s of containers to be stopped</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract IList<ContainerId> GetContainerIds();

		/// <summary>Set the <code>ContainerId</code>s of the containers to be stopped.</summary>
		/// <param name="containerIds"><code>ContainerId</code>s of the containers to be stopped
		/// 	</param>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract void SetContainerIds(IList<ContainerId> containerIds);
	}
}
