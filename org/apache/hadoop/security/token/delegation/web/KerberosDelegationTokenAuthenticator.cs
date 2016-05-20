using Sharpen;

namespace org.apache.hadoop.security.token.delegation.web
{
	/// <summary>
	/// The <code>KerberosDelegationTokenAuthenticator</code> provides support for
	/// Kerberos SPNEGO authentication mechanism and support for Hadoop Delegation
	/// Token operations.
	/// </summary>
	/// <remarks>
	/// The <code>KerberosDelegationTokenAuthenticator</code> provides support for
	/// Kerberos SPNEGO authentication mechanism and support for Hadoop Delegation
	/// Token operations.
	/// <p/>
	/// It falls back to the
	/// <see cref="PseudoDelegationTokenAuthenticator"/>
	/// if the HTTP
	/// endpoint does not trigger a SPNEGO authentication
	/// </remarks>
	public class KerberosDelegationTokenAuthenticator : org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticator
	{
		public KerberosDelegationTokenAuthenticator()
			: base(new _KerberosAuthenticator_39())
		{
		}

		private sealed class _KerberosAuthenticator_39 : org.apache.hadoop.security.authentication.client.KerberosAuthenticator
		{
			public _KerberosAuthenticator_39()
			{
			}

			protected internal override org.apache.hadoop.security.authentication.client.Authenticator
				 getFallBackAuthenticator()
			{
				return new org.apache.hadoop.security.token.delegation.web.PseudoDelegationTokenAuthenticator
					();
			}
		}
	}
}
