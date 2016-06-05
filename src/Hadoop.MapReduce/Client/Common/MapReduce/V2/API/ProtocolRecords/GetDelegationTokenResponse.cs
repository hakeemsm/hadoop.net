using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords
{
	public interface GetDelegationTokenResponse
	{
		void SetDelegationToken(Token clientDToken);

		Token GetDelegationToken();
	}
}
