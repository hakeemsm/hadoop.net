using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords
{
	/// <summary>
	/// <p>The request sent by the client to the <code>ResourceManager</code>
	/// to abort a submitted application.</p>
	/// <p>The request includes the
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationId"/>
	/// of the application to be
	/// aborted.</p>
	/// </summary>
	/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.ApplicationClientProtocol.ForceKillApplication(KillApplicationRequest)
	/// 	"/>
	public abstract class KillApplicationRequest
	{
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public static KillApplicationRequest NewInstance(ApplicationId applicationId)
		{
			KillApplicationRequest request = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<KillApplicationRequest
				>();
			request.SetApplicationId(applicationId);
			return request;
		}

		/// <summary>Get the <code>ApplicationId</code> of the application to be aborted.</summary>
		/// <returns><code>ApplicationId</code> of the application to be aborted</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract ApplicationId GetApplicationId();

		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract void SetApplicationId(ApplicationId applicationId);
	}
}
