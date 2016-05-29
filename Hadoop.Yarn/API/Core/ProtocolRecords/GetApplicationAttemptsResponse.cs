using System.Collections.Generic;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords
{
	/// <summary>
	/// <p>
	/// The response sent by the <code>ResourceManager</code> to a client requesting
	/// a list of
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationAttemptReport"/>
	/// for application attempts.
	/// </p>
	/// <p>
	/// The <code>ApplicationAttemptReport</code> for each application includes the
	/// details of an application attempt.
	/// </p>
	/// </summary>
	/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationAttemptReport"/>
	/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.ApplicationBaseProtocol.GetApplicationAttempts(GetApplicationAttemptsRequest)
	/// 	"/>
	public abstract class GetApplicationAttemptsResponse
	{
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public static GetApplicationAttemptsResponse NewInstance(IList<ApplicationAttemptReport
			> applicationAttempts)
		{
			GetApplicationAttemptsResponse response = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord
				<GetApplicationAttemptsResponse>();
			response.SetApplicationAttemptList(applicationAttempts);
			return response;
		}

		/// <summary>Get a list of <code>ApplicationReport</code> of an application.</summary>
		/// <returns>a list of <code>ApplicationReport</code> of an application</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract IList<ApplicationAttemptReport> GetApplicationAttemptList();

		/// <summary>Get a list of <code>ApplicationReport</code> of an application.</summary>
		/// <param name="applicationAttempts">a list of <code>ApplicationReport</code> of an application
		/// 	</param>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract void SetApplicationAttemptList(IList<ApplicationAttemptReport> applicationAttempts
			);
	}
}
