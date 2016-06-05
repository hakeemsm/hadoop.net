using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords
{
	/// <summary>
	/// The response from the
	/// <c>ResourceManager</c>
	/// to a cancelDelegationToken
	/// request.
	/// </summary>
	public abstract class CancelDelegationTokenResponse
	{
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public static CancelDelegationTokenResponse NewInstance()
		{
			CancelDelegationTokenResponse response = Records.NewRecord<CancelDelegationTokenResponse
				>();
			return response;
		}
	}
}
