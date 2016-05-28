using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords
{
	/// <summary>
	/// The request issued by the client to the
	/// <c>ResourceManager</c>
	/// to cancel a
	/// delegation token.
	/// </summary>
	public interface CancelDelegationTokenRequest
	{
		Token GetDelegationToken();

		void SetDelegationToken(Token dToken);
	}
}
