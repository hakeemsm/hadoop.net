using Org.Apache.Hadoop.Security.Authentication.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Security.Authentication.Server
{
	/// <summary>
	/// The
	/// <see cref="AuthenticationToken"/>
	/// contains information about an authenticated
	/// HTTP client and doubles as the
	/// <see cref="Sharpen.Principal"/>
	/// to be returned by
	/// authenticated
	/// <see cref="Javax.Servlet.Http.HttpServletRequest"/>
	/// s
	/// <p>
	/// The token can be serialized/deserialized to and from a string as it is sent
	/// and received in HTTP client responses and requests as a HTTP cookie (this is
	/// done by the
	/// <see cref="AuthenticationFilter"/>
	/// ).
	/// </summary>
	public class AuthenticationToken : AuthToken
	{
		/// <summary>Constant that identifies an anonymous request.</summary>
		public static readonly Org.Apache.Hadoop.Security.Authentication.Server.AuthenticationToken
			 Anonymous = new Org.Apache.Hadoop.Security.Authentication.Server.AuthenticationToken
			();

		private AuthenticationToken()
			: base()
		{
		}

		private AuthenticationToken(AuthToken token)
			: base(token.GetUserName(), token.GetName(), token.GetType())
		{
			SetExpires(token.GetExpires());
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
		public override void SetExpires(long expires)
		{
			if (this != Org.Apache.Hadoop.Security.Authentication.Server.AuthenticationToken.
				Anonymous)
			{
				base.SetExpires(expires);
			}
		}

		/// <summary>Returns true if the token has expired.</summary>
		/// <returns>true if the token has expired.</returns>
		public override bool IsExpired()
		{
			return base.IsExpired();
		}

		/// <summary>Parses a string into an authentication token.</summary>
		/// <param name="tokenStr">string representation of a token.</param>
		/// <returns>the parsed authentication token.</returns>
		/// <exception cref="Org.Apache.Hadoop.Security.Authentication.Client.AuthenticationException
		/// 	">
		/// thrown if the string representation could not be parsed into
		/// an authentication token.
		/// </exception>
		public static AuthToken Parse(string tokenStr)
		{
			return new Org.Apache.Hadoop.Security.Authentication.Server.AuthenticationToken(AuthToken
				.Parse(tokenStr));
		}
	}
}
