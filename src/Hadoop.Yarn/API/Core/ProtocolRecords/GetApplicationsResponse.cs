using System.Collections.Generic;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords
{
	/// <summary>
	/// <p>The response sent by the <code>ResourceManager</code> to a client
	/// requesting an
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationReport"/>
	/// for applications.</p>
	/// <p>The <code>ApplicationReport</code> for each application includes details
	/// such as user, queue, name, host on which the <code>ApplicationMaster</code>
	/// is running, RPC port, tracking URL, diagnostics, start time etc.</p>
	/// </summary>
	/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationReport"/>
	/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.ApplicationBaseProtocol.GetApplications(GetApplicationsRequest)
	/// 	"/>
	public abstract class GetApplicationsResponse
	{
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public static GetApplicationsResponse NewInstance(IList<ApplicationReport> applications
			)
		{
			GetApplicationsResponse response = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<
				GetApplicationsResponse>();
			response.SetApplicationList(applications);
			return response;
		}

		/// <summary>Get <code>ApplicationReport</code> for applications.</summary>
		/// <returns><code>ApplicationReport</code> for applications</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract IList<ApplicationReport> GetApplicationList();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetApplicationList(IList<ApplicationReport> applications);
	}
}
