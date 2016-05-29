using Org.Apache.Hadoop.Classification;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Records
{
	/// <summary>Description of resources requested back by the cluster.</summary>
	/// <seealso cref="PreemptionContract"/>
	/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.Protocolrecords.AllocateRequest.SetAskList(System.Collections.Generic.IList{E})
	/// 	"/>
	public abstract class PreemptionResourceRequest
	{
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public static PreemptionResourceRequest NewInstance(ResourceRequest req)
		{
			PreemptionResourceRequest request = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord
				<PreemptionResourceRequest>();
			request.SetResourceRequest(req);
			return request;
		}

		/// <returns>
		/// Resource described in this request, to be matched against running
		/// containers.
		/// </returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Evolving]
		public abstract ResourceRequest GetResourceRequest();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetResourceRequest(ResourceRequest req);
	}
}
