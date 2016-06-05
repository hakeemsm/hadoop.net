using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords
{
	/// <summary>
	/// The response to a renewDelegationToken call to the
	/// <c>ResourceManager</c>
	/// .
	/// </summary>
	public abstract class RenewDelegationTokenResponse
	{
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public static RenewDelegationTokenResponse NewInstance(long expTime)
		{
			RenewDelegationTokenResponse response = Records.NewRecord<RenewDelegationTokenResponse
				>();
			response.SetNextExpirationTime(expTime);
			return response;
		}

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract long GetNextExpirationTime();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetNextExpirationTime(long expTime);
	}
}
