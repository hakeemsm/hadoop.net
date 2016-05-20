using Sharpen;

namespace org.apache.hadoop.security.authentication.util
{
	public class AuthToken : java.security.Principal
	{
		/// <summary>Constant that identifies an anonymous request.</summary>
		private const string ATTR_SEPARATOR = "&";

		private const string USER_NAME = "u";

		private const string PRINCIPAL = "p";

		private const string EXPIRES = "e";

		private const string TYPE = "t";

		private static readonly System.Collections.Generic.ICollection<string> ATTRIBUTES
			 = new java.util.HashSet<string>(java.util.Arrays.asList(USER_NAME, PRINCIPAL, EXPIRES
			, TYPE));

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
			generateToken();
		}

		private const string ILLEGAL_ARG_MSG = " is NULL, empty or contains a '" + ATTR_SEPARATOR
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
			checkForIllegalArgument(userName, "userName");
			checkForIllegalArgument(principal, "principal");
			checkForIllegalArgument(type, "type");
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
		protected internal static void checkForIllegalArgument(string value, string name)
		{
			if (value == null || value.Length == 0 || value.contains(ATTR_SEPARATOR))
			{
				throw new System.ArgumentException(name + ILLEGAL_ARG_MSG);
			}
		}

		/// <summary>Sets the expiration of the token.</summary>
		/// <param name="expires">expiration time of the token in milliseconds since the epoch.
		/// 	</param>
		public virtual void setExpires(long expires)
		{
			this.expires = expires;
			generateToken();
		}

		/// <summary>Returns true if the token has expired.</summary>
		/// <returns>true if the token has expired.</returns>
		public virtual bool isExpired()
		{
			return getExpires() != -1 && Sharpen.Runtime.currentTimeMillis() > getExpires();
		}

		/// <summary>Generates the token.</summary>
		private void generateToken()
		{
			System.Text.StringBuilder sb = new System.Text.StringBuilder();
			sb.Append(USER_NAME).Append("=").Append(getUserName()).Append(ATTR_SEPARATOR);
			sb.Append(PRINCIPAL).Append("=").Append(getName()).Append(ATTR_SEPARATOR);
			sb.Append(TYPE).Append("=").Append(getType()).Append(ATTR_SEPARATOR);
			sb.Append(EXPIRES).Append("=").Append(getExpires());
			tokenStr = sb.ToString();
		}

		/// <summary>Returns the user name.</summary>
		/// <returns>the user name.</returns>
		public virtual string getUserName()
		{
			return userName;
		}

		/// <summary>
		/// Returns the principal name (this method name comes from the JDK
		/// <see cref="java.security.Principal"/>
		/// interface).
		/// </summary>
		/// <returns>the principal name.</returns>
		public virtual string getName()
		{
			return principal;
		}

		/// <summary>Returns the authentication mechanism of the token.</summary>
		/// <returns>the authentication mechanism of the token.</returns>
		public virtual string getType()
		{
			return type;
		}

		/// <summary>Returns the expiration time of the token.</summary>
		/// <returns>the expiration time of the token, in milliseconds since Epoc.</returns>
		public virtual long getExpires()
		{
			return expires;
		}

		/// <summary>Returns the string representation of the token.</summary>
		/// <remarks>
		/// Returns the string representation of the token.
		/// <p>
		/// This string representation is parseable by the
		/// <see cref="parse(string)"/>
		/// method.
		/// </remarks>
		/// <returns>the string representation of the token.</returns>
		public override string ToString()
		{
			return tokenStr;
		}

		/// <exception cref="org.apache.hadoop.security.authentication.client.AuthenticationException
		/// 	"/>
		public static org.apache.hadoop.security.authentication.util.AuthToken parse(string
			 tokenStr)
		{
			if (tokenStr.Length >= 2)
			{
				// strip the \" at the two ends of the tokenStr
				if (tokenStr[0] == '\"' && tokenStr[tokenStr.Length - 1] == '\"')
				{
					tokenStr = Sharpen.Runtime.substring(tokenStr, 1, tokenStr.Length - 1);
				}
			}
			System.Collections.Generic.IDictionary<string, string> map = split(tokenStr);
			// remove the signature part, since client doesn't care about it
			Sharpen.Collections.Remove(map, "s");
			if (!map.Keys.Equals(ATTRIBUTES))
			{
				throw new org.apache.hadoop.security.authentication.client.AuthenticationException
					("Invalid token string, missing attributes");
			}
			long expires = long.Parse(map[EXPIRES]);
			org.apache.hadoop.security.authentication.util.AuthToken token = new org.apache.hadoop.security.authentication.util.AuthToken
				(map[USER_NAME], map[PRINCIPAL], map[TYPE]);
			token.setExpires(expires);
			return token;
		}

		/// <summary>Splits the string representation of a token into attributes pairs.</summary>
		/// <param name="tokenStr">string representation of a token.</param>
		/// <returns>a map with the attribute pairs of the token.</returns>
		/// <exception cref="org.apache.hadoop.security.authentication.client.AuthenticationException
		/// 	">
		/// thrown if the string representation of the token could not be broken into
		/// attribute pairs.
		/// </exception>
		private static System.Collections.Generic.IDictionary<string, string> split(string
			 tokenStr)
		{
			System.Collections.Generic.IDictionary<string, string> map = new System.Collections.Generic.Dictionary
				<string, string>();
			java.util.StringTokenizer st = new java.util.StringTokenizer(tokenStr, ATTR_SEPARATOR
				);
			while (st.hasMoreTokens())
			{
				string part = st.nextToken();
				int separator = part.IndexOf('=');
				if (separator == -1)
				{
					throw new org.apache.hadoop.security.authentication.client.AuthenticationException
						("Invalid authentication token");
				}
				string key = Sharpen.Runtime.substring(part, 0, separator);
				string value = Sharpen.Runtime.substring(part, separator + 1);
				map[key] = value;
			}
			return map;
		}
	}
}
