using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords
{
	/// <summary>
	/// The request issued by the client to renew a delegation token from
	/// the
	/// <c>ResourceManager</c>
	/// .
	/// </summary>
	public abstract class RenewDelegationTokenRequest
	{
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public static RenewDelegationTokenRequest NewInstance(Token dToken)
		{
			RenewDelegationTokenRequest request = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord
				<RenewDelegationTokenRequest>();
			request.SetDelegationToken(dToken);
			return request;
		}

		/// <summary>Get the delegation token requested to be renewed by the client.</summary>
		/// <returns>the delegation token requested to be renewed by the client.</returns>
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract Token GetDelegationToken();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetDelegationToken(Token dToken);
	}
}
