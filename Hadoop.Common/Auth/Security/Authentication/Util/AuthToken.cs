using System;
using System.Collections.Generic;
using System.Text;
using Org.Apache.Hadoop.Security.Authentication.Client;
using Sharpen;

namespace Org.Apache.Hadoop.Security.Authentication.Util
{
	public class AuthToken : Principal
	{
		/// <summary>Constant that identifies an anonymous request.</summary>
		private const string AttrSeparator = "&";

		private const string UserName = "u";

		private const string Principal = "p";

		private const string Expires = "e";

		private const string Type = "t";

		private static readonly ICollection<string> Attributes = new HashSet<string>(Arrays
			.AsList(UserName, Principal, Expires, Type));

		private string userName;

		private string principal;

		private string type;

		private long expires;

		private string tokenStr;

		protected internal AuthToken()
		{
			userName = null;
			principal = null;
			type = null;
			expires = -1;
			tokenStr = "ANONYMOUS";
			GenerateToken();
		}

		private const string IllegalArgMsg = " is NULL, empty or contains a '" + AttrSeparator
			 + "'";

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
		public AuthToken(string userName, string principal, string type)
		{
			CheckForIllegalArgument(userName, "userName");
			CheckForIllegalArgument(principal, "principal");
			CheckForIllegalArgument(type, "type");
			this.userName = userName;
			this.principal = principal;
			this.type = type;
			this.expires = -1;
		}

		/// <summary>Check if the provided value is invalid.</summary>
		/// <remarks>Check if the provided value is invalid. Throw an error if it is invalid, NOP otherwise.
		/// 	</remarks>
		/// <param name="value">the value to check.</param>
		/// <param name="name">the parameter name to use in an error message if the value is invalid.
		/// 	</param>
		protected internal static void CheckForIllegalArgument(string value, string name)
		{
			if (value == null || value.Length == 0 || value.Contains(AttrSeparator))
			{
				throw new ArgumentException(name + IllegalArgMsg);
			}
		}

		/// <summary>Sets the expiration of the token.</summary>
		/// <param name="expires">expiration time of the token in milliseconds since the epoch.
		/// 	</param>
		public virtual void SetExpires(long expires)
		{
			this.expires = expires;
			GenerateToken();
		}

		/// <summary>Returns true if the token has expired.</summary>
		/// <returns>true if the token has expired.</returns>
		public virtual bool IsExpired()
		{
			return GetExpires() != -1 && Runtime.CurrentTimeMillis() > GetExpires();
		}

		/// <summary>Generates the token.</summary>
		private void GenerateToken()
		{
			StringBuilder sb = new StringBuilder();
			sb.Append(UserName).Append("=").Append(GetUserName()).Append(AttrSeparator);
			sb.Append(Principal).Append("=").Append(GetName()).Append(AttrSeparator);
			sb.Append(Type).Append("=").Append(GetType()).Append(AttrSeparator);
			sb.Append(Expires).Append("=").Append(GetExpires());
			tokenStr = sb.ToString();
		}

		/// <summary>Returns the user name.</summary>
		/// <returns>the user name.</returns>
		public virtual string GetUserName()
		{
			return userName;
		}

		/// <summary>
		/// Returns the principal name (this method name comes from the JDK
		/// <see cref="Sharpen.Principal"/>
		/// interface).
		/// </summary>
		/// <returns>the principal name.</returns>
		public virtual string GetName()
		{
			return principal;
		}

		/// <summary>Returns the authentication mechanism of the token.</summary>
		/// <returns>the authentication mechanism of the token.</returns>
		public virtual string GetType()
		{
			return type;
		}

		/// <summary>Returns the expiration time of the token.</summary>
		/// <returns>the expiration time of the token, in milliseconds since Epoc.</returns>
		public virtual long GetExpires()
		{
			return expires;
		}

		/// <summary>Returns the string representation of the token.</summary>
		/// <remarks>
		/// Returns the string representation of the token.
		/// <p>
		/// This string representation is parseable by the
		/// <see cref="Parse(string)"/>
		/// method.
		/// </remarks>
		/// <returns>the string representation of the token.</returns>
		public override string ToString()
		{
			return tokenStr;
		}

		/// <exception cref="Org.Apache.Hadoop.Security.Authentication.Client.AuthenticationException
		/// 	"/>
		public static Org.Apache.Hadoop.Security.Authentication.Util.AuthToken Parse(string
			 tokenStr)
		{
			if (tokenStr.Length >= 2)
			{
				// strip the \" at the two ends of the tokenStr
				if (tokenStr[0] == '\"' && tokenStr[tokenStr.Length - 1] == '\"')
				{
					tokenStr = Sharpen.Runtime.Substring(tokenStr, 1, tokenStr.Length - 1);
				}
			}
			IDictionary<string, string> map = Split(tokenStr);
			// remove the signature part, since client doesn't care about it
			Sharpen.Collections.Remove(map, "s");
			if (!map.Keys.Equals(Attributes))
			{
				throw new AuthenticationException("Invalid token string, missing attributes");
			}
			long expires = long.Parse(map[Expires]);
			Org.Apache.Hadoop.Security.Authentication.Util.AuthToken token = new Org.Apache.Hadoop.Security.Authentication.Util.AuthToken
				(map[UserName], map[Principal], map[Type]);
			token.SetExpires(expires);
			return token;
		}

		/// <summary>Splits the string representation of a token into attributes pairs.</summary>
		/// <param name="tokenStr">string representation of a token.</param>
		/// <returns>a map with the attribute pairs of the token.</returns>
		/// <exception cref="Org.Apache.Hadoop.Security.Authentication.Client.AuthenticationException
		/// 	">
		/// thrown if the string representation of the token could not be broken into
		/// attribute pairs.
		/// </exception>
		private static IDictionary<string, string> Split(string tokenStr)
		{
			IDictionary<string, string> map = new Dictionary<string, string>();
			StringTokenizer st = new StringTokenizer(tokenStr, AttrSeparator);
			while (st.HasMoreTokens())
			{
				string part = st.NextToken();
				int separator = part.IndexOf('=');
				if (separator == -1)
				{
					throw new AuthenticationException("Invalid authentication token");
				}
				string key = Sharpen.Runtime.Substring(part, 0, separator);
				string value = Sharpen.Runtime.Substring(part, separator + 1);
				map[key] = value;
			}
			return map;
		}
	}
}
