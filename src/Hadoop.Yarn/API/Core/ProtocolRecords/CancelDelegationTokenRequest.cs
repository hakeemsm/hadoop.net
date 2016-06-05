using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords
{
	/// <summary>
	/// The request issued by the client to the
	/// <c>ResourceManager</c>
	/// to cancel a
	/// delegation token.
	/// </summary>
	public abstract class CancelDelegationTokenRequest
	{
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public static CancelDelegationTokenRequest NewInstance(Token dToken)
		{
			CancelDelegationTokenRequest request = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord
				<CancelDelegationTokenRequest>();
			request.SetDelegationToken(dToken);
			return request;
		}

		/// <summary>Get the delegation token requested to be cancelled.</summary>
		/// <returns>the delegation token requested to be cancelled.</returns>
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract Token GetDelegationToken();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetDelegationToken(Token dToken);
	}
}
