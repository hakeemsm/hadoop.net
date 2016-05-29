using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords
{
	/// <summary>
	/// <p>
	/// The request sent by a client to the <code>ResourceManager</code> to get an
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ContainerReport"/>
	/// for a container.
	/// </p>
	/// </summary>
	public abstract class GetContainerReportRequest
	{
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public static GetContainerReportRequest NewInstance(ContainerId containerId)
		{
			GetContainerReportRequest request = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord
				<GetContainerReportRequest>();
			request.SetContainerId(containerId);
			return request;
		}

		/// <summary>Get the <code>ContainerId</code> of the Container.</summary>
		/// <returns><code>ContainerId</code> of the Container</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract ContainerId GetContainerId();

		/// <summary>Set the <code>ContainerId</code> of the container</summary>
		/// <param name="containerId"><code>ContainerId</code> of the container</param>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract void SetContainerId(ContainerId containerId);
	}
}
