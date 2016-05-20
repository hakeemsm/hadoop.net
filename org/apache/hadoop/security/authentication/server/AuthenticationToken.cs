using Sharpen;

namespace org.apache.hadoop.security.authentication.server
{
	/// <summary>
	/// The
	/// <see cref="AuthenticationToken"/>
	/// contains information about an authenticated
	/// HTTP client and doubles as the
	/// <see cref="java.security.Principal"/>
	/// to be returned by
	/// authenticated
	/// <see cref="javax.servlet.http.HttpServletRequest"/>
	/// s
	/// <p>
	/// The token can be serialized/deserialized to and from a string as it is sent
	/// and received in HTTP client responses and requests as a HTTP cookie (this is
	/// done by the
	/// <see cref="AuthenticationFilter"/>
	/// ).
	/// </summary>
	public class AuthenticationToken : org.apache.hadoop.security.authentication.util.AuthToken
	{
		/// <summary>Constant that identifies an anonymous request.</summary>
		public static readonly org.apache.hadoop.security.authentication.server.AuthenticationToken
			 ANONYMOUS = new org.apache.hadoop.security.authentication.server.AuthenticationToken
			();

		private AuthenticationToken()
			: base()
		{
		}

		private AuthenticationToken(org.apache.hadoop.security.authentication.util.AuthToken
			 token)
			: base(token.getUserName(), token.getName(), token.getType())
		{
			setExpires(token.getExpires());
		}

		/// <summary>Creates an authentication token.</summary>
		/// <param name="userName">user name.</param>
		/// <param name="principal">
		/// principal (commonly matches the user name, with Kerberos is the full/long principal
		/// name while the userName is the short name).
		/// </param>
		/// <param name="type">
		/// the authentication mechanism name.
		/// (<code>System.currentTimeMillis() + validityPeriod</code>).
		/// </param>
		public AuthenticationToken(string userName, string principal, string type)
			: base(userName, principal, type)
		{
		}

		/// <summary>Sets the expiration of the token.</summary>
		/// <param name="expires">expiration time of the token in milliseconds since the epoch.
		/// 	</param>
		public override void setExpires(long expires)
		{
			if (this != org.apache.hadoop.security.authentication.server.AuthenticationToken.
				ANONYMOUS)
			{
				base.setExpires(expires);
			}
		}

		/// <summary>Returns true if the token has expired.</summary>
		/// <returns>true if the token has expired.</returns>
		public override bool isExpired()
		{
			return base.isExpired();
		}

		/// <summary>Parses a string into an authentication token.</summary>
		/// <param name="tokenStr">string representation of a token.</param>
		/// <returns>the parsed authentication token.</returns>
		/// <exception cref="org.apache.hadoop.security.authentication.client.AuthenticationException
		/// 	">
		/// thrown if the string representation could not be parsed into
		/// an authentication token.
		/// </exception>
		public static org.apache.hadoop.security.authentication.util.AuthToken parse(string
			 tokenStr)
		{
			return new org.apache.hadoop.security.authentication.server.AuthenticationToken(org.apache.hadoop.security.authentication.util.AuthToken
				.parse(tokenStr));
		}
	}
}
