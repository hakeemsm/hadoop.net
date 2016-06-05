using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords
{
	/// <summary>
	/// <p>The request sent by a client to the <code>ResourceManager</code> to
	/// get an
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationReport"/>
	/// for an application.</p>
	/// <p>The request should include the
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationId"/>
	/// of the
	/// application.</p>
	/// </summary>
	/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.ApplicationBaseProtocol.GetApplicationReport(GetApplicationReportRequest)
	/// 	"/>
	/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationReport"/>
	public abstract class GetApplicationReportRequest
	{
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public static GetApplicationReportRequest NewInstance(ApplicationId applicationId
			)
		{
			GetApplicationReportRequest request = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord
				<GetApplicationReportRequest>();
			request.SetApplicationId(applicationId);
			return request;
		}

		/// <summary>Get the <code>ApplicationId</code> of the application.</summary>
		/// <returns><code>ApplicationId</code> of the application</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract ApplicationId GetApplicationId();

		/// <summary>Set the <code>ApplicationId</code> of the application</summary>
		/// <param name="applicationId"><code>ApplicationId</code> of the application</param>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract void SetApplicationId(ApplicationId applicationId);
	}
}
