using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords
{
	/// <summary>
	/// <p>
	/// The request from clients to get a list of application attempt reports of an
	/// application from the <code>ResourceManager</code>.
	/// </summary>
	/// <remarks>
	/// <p>
	/// The request from clients to get a list of application attempt reports of an
	/// application from the <code>ResourceManager</code>.
	/// </p>
	/// </remarks>
	/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.ApplicationBaseProtocol.GetApplicationAttempts(GetApplicationAttemptsRequest)
	/// 	"/>
	public abstract class GetApplicationAttemptsRequest
	{
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public static GetApplicationAttemptsRequest NewInstance(ApplicationId applicationId
			)
		{
			GetApplicationAttemptsRequest request = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord
				<GetApplicationAttemptsRequest>();
			request.SetApplicationId(applicationId);
			return request;
		}

		/// <summary>Get the <code>ApplicationId</code> of an application</summary>
		/// <returns><code>ApplicationId</code> of an application</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract ApplicationId GetApplicationId();

		/// <summary>Set the <code>ApplicationId</code> of an application</summary>
		/// <param name="applicationId"><code>ApplicationId</code> of an application</param>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract void SetApplicationId(ApplicationId applicationId);
	}
}
