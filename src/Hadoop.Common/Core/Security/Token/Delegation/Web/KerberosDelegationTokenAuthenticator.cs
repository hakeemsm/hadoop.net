using Org.Apache.Hadoop.Security.Authentication.Client;


namespace Org.Apache.Hadoop.Security.Token.Delegation.Web
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
	public class KerberosDelegationTokenAuthenticator : DelegationTokenAuthenticator
	{
		public KerberosDelegationTokenAuthenticator()
			: base(new _KerberosAuthenticator_39())
		{
		}

		private sealed class _KerberosAuthenticator_39 : KerberosAuthenticator
		{
			public _KerberosAuthenticator_39()
			{
			}

			protected override Authenticator GetFallBackAuthenticator()
			{
				return new PseudoDelegationTokenAuthenticator();
			}
		}
	}
}
