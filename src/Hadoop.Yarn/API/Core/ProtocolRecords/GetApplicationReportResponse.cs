using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords
{
	/// <summary>
	/// <p>The response sent by the <code>ResourceManager</code> to a client
	/// requesting an application report.</p>
	/// <p>The response includes an
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationReport"/>
	/// which has details such
	/// as user, queue, name, host on which the <code>ApplicationMaster</code> is
	/// running, RPC port, tracking URL, diagnostics, start time etc.</p>
	/// </summary>
	/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.ApplicationBaseProtocol.GetApplicationReport(GetApplicationReportRequest)
	/// 	"/>
	public abstract class GetApplicationReportResponse
	{
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public static GetApplicationReportResponse NewInstance(ApplicationReport ApplicationReport
			)
		{
			GetApplicationReportResponse response = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord
				<GetApplicationReportResponse>();
			response.SetApplicationReport(ApplicationReport);
			return response;
		}

		/// <summary>Get the <code>ApplicationReport</code> for the application.</summary>
		/// <returns><code>ApplicationReport</code> for the application</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract ApplicationReport GetApplicationReport();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetApplicationReport(ApplicationReport ApplicationReport);
	}
}
