using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords
{
	/// <summary>
	/// Response to a
	/// <see cref="GetDelegationTokenRequest"/>
	/// request
	/// from the client. The response contains the token that
	/// can be used by the containers to talk to  ClientRMService.
	/// </summary>
	public abstract class GetDelegationTokenResponse
	{
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public static GetDelegationTokenResponse NewInstance(Token rmDTToken)
		{
			GetDelegationTokenResponse response = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord
				<GetDelegationTokenResponse>();
			response.SetRMDelegationToken(rmDTToken);
			return response;
		}

		/// <summary>
		/// The Delegation tokens have a identifier which maps to
		/// <see cref="Org.Apache.Hadoop.Security.Token.Delegation.AbstractDelegationTokenIdentifier
		/// 	"/>
		/// .
		/// </summary>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract Token GetRMDelegationToken();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetRMDelegationToken(Token rmDTToken);
	}
}
