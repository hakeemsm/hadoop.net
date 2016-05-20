using Sharpen;

namespace org.apache.hadoop.security.authentication.util
{
	/// <summary>This class implements parsing and handling of Kerberos principal names.</summary>
	/// <remarks>
	/// This class implements parsing and handling of Kerberos principal names. In
	/// particular, it splits them apart and translates them down into local
	/// operating system names.
	/// </remarks>
	public class KerberosName
	{
		private static readonly org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(
			Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.security.authentication.util.KerberosName
			)));

		/// <summary>The first component of the name</summary>
		private readonly string serviceName;

		/// <summary>The second component of the name.</summary>
		/// <remarks>The second component of the name. It may be null.</remarks>
		private readonly string hostName;

		/// <summary>The realm of the name.</summary>
		private readonly string realm;

		/// <summary>A pattern that matches a Kerberos name with at most 2 components.</summary>
		private static readonly java.util.regex.Pattern nameParser = java.util.regex.Pattern
			.compile("([^/@]*)(/([^/@]*))?@([^/@]*)");

		/// <summary>
		/// A pattern that matches a string with out '$' and then a single
		/// parameter with $n.
		/// </summary>
		private static java.util.regex.Pattern parameterPattern = java.util.regex.Pattern
			.compile("([^$]*)(\\$(\\d*))?");

		/// <summary>A pattern for parsing a auth_to_local rule.</summary>
		private static readonly java.util.regex.Pattern ruleParser = java.util.regex.Pattern
			.compile("\\s*((DEFAULT)|(RULE:\\[(\\d*):([^\\]]*)](\\(([^)]*)\\))?" + "(s/([^/]*)/([^/]*)/(g)?)?))/?(L)?"
			);

		/// <summary>A pattern that recognizes simple/non-simple names.</summary>
		private static readonly java.util.regex.Pattern nonSimplePattern = java.util.regex.Pattern
			.compile("[/@]");

		/// <summary>The list of translation rules.</summary>
		private static System.Collections.Generic.IList<org.apache.hadoop.security.authentication.util.KerberosName.Rule
			> rules;

		private static string defaultRealm;

		static KerberosName()
		{
			try
			{
				defaultRealm = org.apache.hadoop.security.authentication.util.KerberosUtil.getDefaultRealm
					();
			}
			catch (System.Exception)
			{
				LOG.debug("Kerberos krb5 configuration not found, setting default realm to empty"
					);
				defaultRealm = string.Empty;
			}
		}

		/// <summary>Create a name from the full Kerberos principal name.</summary>
		/// <param name="name">full Kerberos principal name.</param>
		public KerberosName(string name)
		{
			java.util.regex.Matcher match = nameParser.matcher(name);
			if (!match.matches())
			{
				if (name.contains("@"))
				{
					throw new System.ArgumentException("Malformed Kerberos name: " + name);
				}
				else
				{
					serviceName = name;
					hostName = null;
					realm = null;
				}
			}
			else
			{
				serviceName = match.group(1);
				hostName = match.group(3);
				realm = match.group(4);
			}
		}

		/// <summary>Get the configured default realm.</summary>
		/// <returns>the default realm from the krb5.conf</returns>
		public virtual string getDefaultRealm()
		{
			return defaultRealm;
		}

		/// <summary>Put the name back together from the parts.</summary>
		public override string ToString()
		{
			java.lang.StringBuilder result = new java.lang.StringBuilder();
			result.Append(serviceName);
			if (hostName != null)
			{
				result.Append('/');
				result.Append(hostName);
			}
			if (realm != null)
			{
				result.Append('@');
				result.Append(realm);
			}
			return result.ToString();
		}

		/// <summary>Get the first component of the name.</summary>
		/// <returns>the first section of the Kerberos principal name</returns>
		public virtual string getServiceName()
		{
			return serviceName;
		}

		/// <summary>Get the second component of the name.</summary>
		/// <returns>the second section of the Kerberos principal name, and may be null</returns>
		public virtual string getHostName()
		{
			return hostName;
		}

		/// <summary>Get the realm of the name.</summary>
		/// <returns>the realm of the name, may be null</returns>
		public virtual string getRealm()
		{
			return realm;
		}

		/// <summary>An encoding of a rule for translating kerberos names.</summary>
		private class Rule
		{
			private readonly bool isDefault;

			private readonly int numOfComponents;

			private readonly string format;

			private readonly java.util.regex.Pattern match;

			private readonly java.util.regex.Pattern fromPattern;

			private readonly string toPattern;

			private readonly bool repeat;

			private readonly bool toLowerCase;

			internal Rule()
			{
				isDefault = true;
				numOfComponents = 0;
				format = null;
				match = null;
				fromPattern = null;
				toPattern = null;
				repeat = false;
				toLowerCase = false;
			}

			internal Rule(int numOfComponents, string format, string match, string fromPattern
				, string toPattern, bool repeat, bool toLowerCase)
			{
				isDefault = false;
				this.numOfComponents = numOfComponents;
				this.format = format;
				this.match = match == null ? null : java.util.regex.Pattern.compile(match);
				this.fromPattern = fromPattern == null ? null : java.util.regex.Pattern.compile(fromPattern
					);
				this.toPattern = toPattern;
				this.repeat = repeat;
				this.toLowerCase = toLowerCase;
			}

			public override string ToString()
			{
				java.lang.StringBuilder buf = new java.lang.StringBuilder();
				if (isDefault)
				{
					buf.Append("DEFAULT");
				}
				else
				{
					buf.Append("RULE:[");
					buf.Append(numOfComponents);
					buf.Append(':');
					buf.Append(format);
					buf.Append(']');
					if (match != null)
					{
						buf.Append('(');
						buf.Append(match);
						buf.Append(')');
					}
					if (fromPattern != null)
					{
						buf.Append("s/");
						buf.Append(fromPattern);
						buf.Append('/');
						buf.Append(toPattern);
						buf.Append('/');
						if (repeat)
						{
							buf.Append('g');
						}
					}
					if (toLowerCase)
					{
						buf.Append("/L");
					}
				}
				return buf.ToString();
			}

			/// <summary>
			/// Replace the numbered parameters of the form $n where n is from 1 to
			/// the length of params.
			/// </summary>
			/// <remarks>
			/// Replace the numbered parameters of the form $n where n is from 1 to
			/// the length of params. Normal text is copied directly and $n is replaced
			/// by the corresponding parameter.
			/// </remarks>
			/// <param name="format">the string to replace parameters again</param>
			/// <param name="params">the list of parameters</param>
			/// <returns>the generated string with the parameter references replaced.</returns>
			/// <exception cref="BadFormatString"/>
			/// <exception cref="org.apache.hadoop.security.authentication.util.KerberosName.BadFormatString
			/// 	"/>
			internal static string replaceParameters(string format, string[] @params)
			{
				java.util.regex.Matcher match = parameterPattern.matcher(format);
				int start = 0;
				java.lang.StringBuilder result = new java.lang.StringBuilder();
				while (start < format.Length && match.find(start))
				{
					result.Append(match.group(1));
					string paramNum = match.group(3);
					if (paramNum != null)
					{
						try
						{
							int num = System.Convert.ToInt32(paramNum);
							if (num < 0 || num > @params.Length)
							{
								throw new org.apache.hadoop.security.authentication.util.KerberosName.BadFormatString
									("index " + num + " from " + format + " is outside of the valid range 0 to " + (
									@params.Length - 1));
							}
							result.Append(@params[num]);
						}
						catch (java.lang.NumberFormatException nfe)
						{
							throw new org.apache.hadoop.security.authentication.util.KerberosName.BadFormatString
								("bad format in username mapping in " + paramNum, nfe);
						}
					}
					start = match.end();
				}
				return result.ToString();
			}

			/// <summary>
			/// Replace the matches of the from pattern in the base string with the value
			/// of the to string.
			/// </summary>
			/// <param name="base">the string to transform</param>
			/// <param name="from">the pattern to look for in the base string</param>
			/// <param name="to">the string to replace matches of the pattern with</param>
			/// <param name="repeat">whether the substitution should be repeated</param>
			/// <returns/>
			internal static string replaceSubstitution(string @base, java.util.regex.Pattern 
				from, string to, bool repeat)
			{
				java.util.regex.Matcher match = from.matcher(@base);
				if (repeat)
				{
					return match.replaceAll(to);
				}
				else
				{
					return match.replaceFirst(to);
				}
			}

			/// <summary>
			/// Try to apply this rule to the given name represented as a parameter
			/// array.
			/// </summary>
			/// <param name="params">
			/// first element is the realm, second and later elements are
			/// are the components of the name "a/b@FOO" -&gt; {"FOO", "a", "b"}
			/// </param>
			/// <returns>the short name if this rule applies or null</returns>
			/// <exception cref="System.IO.IOException">throws if something is wrong with the rules
			/// 	</exception>
			internal virtual string apply(string[] @params)
			{
				string result = null;
				if (isDefault)
				{
					if (defaultRealm.Equals(@params[0]))
					{
						result = @params[1];
					}
				}
				else
				{
					if (@params.Length - 1 == numOfComponents)
					{
						string @base = replaceParameters(format, @params);
						if (match == null || match.matcher(@base).matches())
						{
							if (fromPattern == null)
							{
								result = @base;
							}
							else
							{
								result = replaceSubstitution(@base, fromPattern, toPattern, repeat);
							}
						}
					}
				}
				if (result != null && nonSimplePattern.matcher(result).find())
				{
					throw new org.apache.hadoop.security.authentication.util.KerberosName.NoMatchingRule
						("Non-simple name " + result + " after auth_to_local rule " + this);
				}
				if (toLowerCase && result != null)
				{
					result = result.ToLower(java.util.Locale.ENGLISH);
				}
				return result;
			}
		}

		internal static System.Collections.Generic.IList<org.apache.hadoop.security.authentication.util.KerberosName.Rule
			> parseRules(string rules)
		{
			System.Collections.Generic.IList<org.apache.hadoop.security.authentication.util.KerberosName.Rule
				> result = new System.Collections.Generic.List<org.apache.hadoop.security.authentication.util.KerberosName.Rule
				>();
			string remaining = rules.Trim();
			while (remaining.Length > 0)
			{
				java.util.regex.Matcher matcher = ruleParser.matcher(remaining);
				if (!matcher.lookingAt())
				{
					throw new System.ArgumentException("Invalid rule: " + remaining);
				}
				if (matcher.group(2) != null)
				{
					result.add(new org.apache.hadoop.security.authentication.util.KerberosName.Rule()
						);
				}
				else
				{
					result.add(new org.apache.hadoop.security.authentication.util.KerberosName.Rule(System.Convert.ToInt32
						(matcher.group(4)), matcher.group(5), matcher.group(7), matcher.group(9), matcher
						.group(10), "g".Equals(matcher.group(11)), "L".Equals(matcher.group(12))));
				}
				remaining = Sharpen.Runtime.substring(remaining, matcher.end());
			}
			return result;
		}

		[System.Serializable]
		public class BadFormatString : System.IO.IOException
		{
			internal BadFormatString(string msg)
				: base(msg)
			{
			}

			internal BadFormatString(string msg, System.Exception err)
				: base(msg, err)
			{
			}
		}

		[System.Serializable]
		public class NoMatchingRule : System.IO.IOException
		{
			internal NoMatchingRule(string msg)
				: base(msg)
			{
			}
		}

		/// <summary>
		/// Get the translation of the principal name into an operating system
		/// user name.
		/// </summary>
		/// <returns>the short name</returns>
		/// <exception cref="System.IO.IOException">throws if something is wrong with the rules
		/// 	</exception>
		public virtual string getShortName()
		{
			string[] @params;
			if (hostName == null)
			{
				// if it is already simple, just return it
				if (realm == null)
				{
					return serviceName;
				}
				@params = new string[] { realm, serviceName };
			}
			else
			{
				@params = new string[] { realm, serviceName, hostName };
			}
			foreach (org.apache.hadoop.security.authentication.util.KerberosName.Rule r in rules)
			{
				string result = r.apply(@params);
				if (result != null)
				{
					return result;
				}
			}
			throw new org.apache.hadoop.security.authentication.util.KerberosName.NoMatchingRule
				("No rules applied to " + ToString());
		}

		/// <summary>Set the rules.</summary>
		/// <param name="ruleString">the rules string.</param>
		public static void setRules(string ruleString)
		{
			rules = (ruleString != null) ? parseRules(ruleString) : null;
		}

		/// <summary>Get the rules.</summary>
		/// <returns>String of configured rules, or null if not yet configured</returns>
		public static string getRules()
		{
			string ruleString = null;
			if (rules != null)
			{
				java.lang.StringBuilder sb = new java.lang.StringBuilder();
				foreach (org.apache.hadoop.security.authentication.util.KerberosName.Rule rule in 
					rules)
				{
					sb.Append(rule.ToString()).Append("\n");
				}
				ruleString = sb.ToString().Trim();
			}
			return ruleString;
		}

		/// <summary>Indicates if the name rules have been set.</summary>
		/// <returns>if the name rules have been set.</returns>
		public static bool hasRulesBeenSet()
		{
			return rules != null;
		}

		/// <exception cref="System.IO.IOException"/>
		internal static void printRules()
		{
			int i = 0;
			foreach (org.apache.hadoop.security.authentication.util.KerberosName.Rule r in rules)
			{
				System.Console.Out.WriteLine(++i + " " + r);
			}
		}
	}
}
