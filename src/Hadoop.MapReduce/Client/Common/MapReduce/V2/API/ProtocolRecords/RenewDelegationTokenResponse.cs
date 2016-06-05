using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords
{
	/// <summary>
	/// The response to a renewDelegationToken call to the
	/// <c>ResourceManager</c>
	/// .
	/// </summary>
	public interface RenewDelegationTokenResponse
	{
		long GetNextExpirationTime();

		void SetNextExpirationTime(long expTime);
	}
}
