using Sharpen;

namespace org.apache.hadoop.security.token.delegation.web
{
	/// <summary>
	/// An
	/// <see cref="org.apache.hadoop.security.authentication.server.AuthenticationHandler
	/// 	"/>
	/// that implements Kerberos SPNEGO mechanism
	/// for HTTP and supports Delegation Token functionality.
	/// <p/>
	/// In addition to the
	/// <see cref="org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler
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
	public class KerberosDelegationTokenAuthenticationHandler : org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticationHandler
	{
		public KerberosDelegationTokenAuthenticationHandler()
			: base(new org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler
				(org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler.
				TYPE + TYPE_POSTFIX))
		{
		}
	}
}
