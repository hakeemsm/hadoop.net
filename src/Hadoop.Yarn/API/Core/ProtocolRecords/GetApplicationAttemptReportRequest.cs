using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords
{
	/// <summary>
	/// <p>
	/// The request sent by a client to the <code>ResourceManager</code> to get an
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationAttemptReport"/>
	/// for an application attempt.
	/// </p>
	/// <p>
	/// The request should include the
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationAttemptId"/>
	/// of the
	/// application attempt.
	/// </p>
	/// </summary>
	/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationAttemptReport"/>
	/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.ApplicationBaseProtocol.GetApplicationAttemptReport(GetApplicationAttemptReportRequest)
	/// 	"/>
	public abstract class GetApplicationAttemptReportRequest
	{
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public static GetApplicationAttemptReportRequest NewInstance(ApplicationAttemptId
			 applicationAttemptId)
		{
			GetApplicationAttemptReportRequest request = Org.Apache.Hadoop.Yarn.Util.Records.
				NewRecord<GetApplicationAttemptReportRequest>();
			request.SetApplicationAttemptId(applicationAttemptId);
			return request;
		}

		/// <summary>Get the <code>ApplicationAttemptId</code> of an application attempt.</summary>
		/// <returns><code>ApplicationAttemptId</code> of an application attempt</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract ApplicationAttemptId GetApplicationAttemptId();

		/// <summary>Set the <code>ApplicationAttemptId</code> of an application attempt</summary>
		/// <param name="applicationAttemptId"><code>ApplicationAttemptId</code> of an application attempt
		/// 	</param>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract void SetApplicationAttemptId(ApplicationAttemptId applicationAttemptId
			);
	}
}
