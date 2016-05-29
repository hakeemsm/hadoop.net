using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords
{
	/// <summary>
	/// <p>
	/// The response sent by the <code>ResourceManager</code> to a client requesting
	/// a container report.
	/// </summary>
	/// <remarks>
	/// <p>
	/// The response sent by the <code>ResourceManager</code> to a client requesting
	/// a container report.
	/// </p>
	/// <p>
	/// The response includes a
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ContainerReport"/>
	/// which has details of a
	/// container.
	/// </p>
	/// </remarks>
	public abstract class GetContainerReportResponse
	{
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public static GetContainerReportResponse NewInstance(ContainerReport containerReport
			)
		{
			GetContainerReportResponse response = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord
				<GetContainerReportResponse>();
			response.SetContainerReport(containerReport);
			return response;
		}

		/// <summary>Get the <code>ContainerReport</code> for the container.</summary>
		/// <returns><code>ContainerReport</code> for the container</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract ContainerReport GetContainerReport();

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract void SetContainerReport(ContainerReport containerReport);
	}
}
