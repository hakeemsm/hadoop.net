using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords
{
	/// <summary>
	/// <p>The response sent by the <code>ResourceManager</code> to the client for
	/// a request to get a new
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationId"/>
	/// for submitting applications.</p>
	/// <p>Clients can submit an application with the returned
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationId"/>
	/// .</p>
	/// </summary>
	/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.ApplicationClientProtocol.GetNewApplication(GetNewApplicationRequest)
	/// 	"/>
	public abstract class GetNewApplicationResponse
	{
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public static GetNewApplicationResponse NewInstance(ApplicationId applicationId, 
			Resource minCapability, Resource maxCapability)
		{
			GetNewApplicationResponse response = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord
				<GetNewApplicationResponse>();
			response.SetApplicationId(applicationId);
			response.SetMaximumResourceCapability(maxCapability);
			return response;
		}

		/// <summary>
		/// Get the <em>new</em> <code>ApplicationId</code> allocated by the
		/// <code>ResourceManager</code>.
		/// </summary>
		/// <returns>
		/// <em>new</em> <code>ApplicationId</code> allocated by the
		/// <code>ResourceManager</code>
		/// </returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract ApplicationId GetApplicationId();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetApplicationId(ApplicationId applicationId);

		/// <summary>
		/// Get the maximum capability for any
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.Resource"/>
		/// allocated by the
		/// <code>ResourceManager</code> in the cluster.
		/// </summary>
		/// <returns>maximum capability of allocated resources in the cluster</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract Resource GetMaximumResourceCapability();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetMaximumResourceCapability(Resource capability);
	}
}
