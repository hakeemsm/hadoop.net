using Org.Apache.Hadoop.Security.Authentication.Server;
using Sharpen;

namespace Org.Apache.Hadoop.Security.Token.Delegation.Web
{
	/// <summary>
	/// An
	/// <see cref="Org.Apache.Hadoop.Security.Authentication.Server.AuthenticationHandler
	/// 	"/>
	/// that implements Kerberos SPNEGO mechanism
	/// for HTTP and supports Delegation Token functionality.
	/// <p/>
	/// In addition to the
	/// <see cref="Org.Apache.Hadoop.Security.Authentication.Server.KerberosAuthenticationHandler
	/// 	"/>
	/// configuration
	/// properties, this handler supports:
	/// <ul>
	/// <li>kerberos.delegation-token.token-kind: the token kind for generated tokens
	/// (no default, required property).</li>
	/// <li>kerberos.delegation-token.update-interval.sec: secret manager master key
	/// update interval in seconds (default 1 day).</li>
	/// <li>kerberos.delegation-token.max-lifetime.sec: maximum life of a delegation
	/// token in seconds (default 7 days).</li>
	/// <li>kerberos.delegation-token.renewal-interval.sec: renewal interval for
	/// delegation tokens in seconds (default 1 day).</li>
	/// <li>kerberos.delegation-token.removal-scan-interval.sec: delegation tokens
	/// removal scan interval in seconds (default 1 hour).</li>
	/// </ul>
	/// </summary>
	public class KerberosDelegationTokenAuthenticationHandler : DelegationTokenAuthenticationHandler
	{
		public KerberosDelegationTokenAuthenticationHandler()
			: base(new KerberosAuthenticationHandler(KerberosAuthenticationHandler.Type + TypePostfix
				))
		{
		}
	}
}
