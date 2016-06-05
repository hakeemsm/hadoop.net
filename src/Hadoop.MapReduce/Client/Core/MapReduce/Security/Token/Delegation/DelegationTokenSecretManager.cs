using Org.Apache.Hadoop.Security.Token.Delegation;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Security.Token.Delegation
{
	/// <summary>A MapReduce specific delegation token secret manager.</summary>
	/// <remarks>
	/// A MapReduce specific delegation token secret manager.
	/// The secret manager is responsible for generating and accepting the password
	/// for each token.
	/// </remarks>
	public class DelegationTokenSecretManager : AbstractDelegationTokenSecretManager<
		DelegationTokenIdentifier>
	{
		/// <summary>Create a secret manager</summary>
		/// <param name="delegationKeyUpdateInterval">
		/// the number of seconds for rolling new
		/// secret keys.
		/// </param>
		/// <param name="delegationTokenMaxLifetime">
		/// the maximum lifetime of the delegation
		/// tokens
		/// </param>
		/// <param name="delegationTokenRenewInterval">how often the tokens must be renewed</param>
		/// <param name="delegationTokenRemoverScanInterval">
		/// how often the tokens are scanned
		/// for expired tokens
		/// </param>
		public DelegationTokenSecretManager(long delegationKeyUpdateInterval, long delegationTokenMaxLifetime
			, long delegationTokenRenewInterval, long delegationTokenRemoverScanInterval)
			: base(delegationKeyUpdateInterval, delegationTokenMaxLifetime, delegationTokenRenewInterval
				, delegationTokenRemoverScanInterval)
		{
		}

		public override DelegationTokenIdentifier CreateIdentifier()
		{
			return new DelegationTokenIdentifier();
		}
	}
}
