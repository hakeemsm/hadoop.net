using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords
{
	/// <summary>
	/// <p>
	/// The response sent by the <code>ResourceManager</code> to a client requesting
	/// an application attempt report.
	/// </summary>
	/// <remarks>
	/// <p>
	/// The response sent by the <code>ResourceManager</code> to a client requesting
	/// an application attempt report.
	/// </p>
	/// <p>
	/// The response includes an
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationAttemptReport"/>
	/// which has the
	/// details about the particular application attempt
	/// </p>
	/// </remarks>
	/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationAttemptReport"/>
	/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.ApplicationBaseProtocol.GetApplicationAttemptReport(GetApplicationAttemptReportRequest)
	/// 	"/>
	public abstract class GetApplicationAttemptReportResponse
	{
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public static GetApplicationAttemptReportResponse NewInstance(ApplicationAttemptReport
			 ApplicationAttemptReport)
		{
			GetApplicationAttemptReportResponse response = Org.Apache.Hadoop.Yarn.Util.Records
				.NewRecord<GetApplicationAttemptReportResponse>();
			response.SetApplicationAttemptReport(ApplicationAttemptReport);
			return response;
		}

		/// <summary>Get the <code>ApplicationAttemptReport</code> for the application attempt.
		/// 	</summary>
		/// <returns><code>ApplicationAttemptReport</code> for the application attempt</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract ApplicationAttemptReport GetApplicationAttemptReport();

		/// <summary>Get the <code>ApplicationAttemptReport</code> for the application attempt.
		/// 	</summary>
		/// <param name="applicationAttemptReport"><code>ApplicationAttemptReport</code> for the application attempt
		/// 	</param>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract void SetApplicationAttemptReport(ApplicationAttemptReport applicationAttemptReport
			);
	}
}
