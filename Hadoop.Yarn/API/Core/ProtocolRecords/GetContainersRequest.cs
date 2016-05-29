using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords
{
	/// <summary>
	/// <p>
	/// The request from clients to get a list of container reports, which belong to
	/// an application attempt from the <code>ResourceManager</code>.
	/// </summary>
	/// <remarks>
	/// <p>
	/// The request from clients to get a list of container reports, which belong to
	/// an application attempt from the <code>ResourceManager</code>.
	/// </p>
	/// </remarks>
	/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.ApplicationBaseProtocol.GetContainers(GetContainersRequest)
	/// 	"/>
	public abstract class GetContainersRequest
	{
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public static GetContainersRequest NewInstance(ApplicationAttemptId applicationAttemptId
			)
		{
			GetContainersRequest request = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<GetContainersRequest
				>();
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
