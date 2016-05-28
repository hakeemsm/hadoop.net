using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords
{
	/// <summary>
	/// The request issued by the client to renew a delegation token from
	/// the
	/// <c>ResourceManager</c>
	/// .
	/// </summary>
	public interface RenewDelegationTokenRequest
	{
		Token GetDelegationToken();

		void SetDelegationToken(Token dToken);
	}
}
