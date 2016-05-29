using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords
{
	/// <summary>
	/// The request issued by the client to get a delegation token from
	/// the
	/// <c>ResourceManager</c>
	/// .
	/// for more information.
	/// </summary>
	public abstract class GetDelegationTokenRequest
	{
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public static GetDelegationTokenRequest NewInstance(string renewer)
		{
			GetDelegationTokenRequest request = Records.NewRecord<GetDelegationTokenRequest>(
				);
			request.SetRenewer(renewer);
			return request;
		}

		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract string GetRenewer();

		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract void SetRenewer(string renewer);
	}
}
