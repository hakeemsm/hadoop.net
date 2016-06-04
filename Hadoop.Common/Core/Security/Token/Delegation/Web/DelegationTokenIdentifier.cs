using Hadoop.Common.Core.IO;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security.Token.Delegation;
using Sharpen;

namespace Org.Apache.Hadoop.Security.Token.Delegation.Web
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
	public class DelegationTokenIdentifier : AbstractDelegationTokenIdentifier
	{
		private Text kind;

		public DelegationTokenIdentifier(Text kind)
		{
			this.kind = kind;
		}

		/// <summary>Create a new delegation token identifier</summary>
		/// <param name="kind">token kind</param>
		/// <param name="owner">the effective username of the token owner</param>
		/// <param name="renewer">the username of the renewer</param>
		/// <param name="realUser">the real username of the token owner</param>
		public DelegationTokenIdentifier(Text kind, Text owner, Text renewer, Text realUser
			)
			: base(owner, renewer, realUser)
		{
			this.kind = kind;
		}

		/// <summary>Return the delegation token kind</summary>
		/// <returns>returns the delegation token kind</returns>
		public override Text GetKind()
		{
			return kind;
		}
	}
}
