using Sharpen;

namespace org.apache.hadoop.security.token.delegation.web
{
	/// <summary>
	/// Concrete delegation token identifier used by
	/// <see cref="DelegationTokenManager"/>
	/// ,
	/// <see cref="KerberosDelegationTokenAuthenticationHandler"/>
	/// and
	/// <see cref="DelegationTokenAuthenticationFilter"/>
	/// .
	/// </summary>
	public class DelegationTokenIdentifier : org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier
	{
		private org.apache.hadoop.io.Text kind;

		public DelegationTokenIdentifier(org.apache.hadoop.io.Text kind)
		{
			this.kind = kind;
		}

		/// <summary>Create a new delegation token identifier</summary>
		/// <param name="kind">token kind</param>
		/// <param name="owner">the effective username of the token owner</param>
		/// <param name="renewer">the username of the renewer</param>
		/// <param name="realUser">the real username of the token owner</param>
		public DelegationTokenIdentifier(org.apache.hadoop.io.Text kind, org.apache.hadoop.io.Text
			 owner, org.apache.hadoop.io.Text renewer, org.apache.hadoop.io.Text realUser)
			: base(owner, renewer, realUser)
		{
			this.kind = kind;
		}

		/// <summary>Return the delegation token kind</summary>
		/// <returns>returns the delegation token kind</returns>
		public override org.apache.hadoop.io.Text getKind()
		{
			return kind;
		}
	}
}
