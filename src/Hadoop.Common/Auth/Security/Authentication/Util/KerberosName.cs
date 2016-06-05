using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Org.Slf4j;


namespace Org.Apache.Hadoop.Security.Authentication.Util
{
	/// <summary>This class implements parsing and handling of Kerberos principal names.</summary>
	/// <remarks>
	/// This class implements parsing and handling of Kerberos principal names. In
	/// particular, it splits them apart and translates them down into local
	/// operating system names.
	/// </remarks>
	public class KerberosName
	{
		private static readonly Logger Log = LoggerFactory.GetLogger(typeof(Org.Apache.Hadoop.Security.Authentication.Util.KerberosName
			));

		/// <summary>The first component of the name</summary>
		private readonly string serviceName;

		/// <summary>The second component of the name.</summary>
		/// <remarks>The second component of the name. It may be null.</remarks>
		private readonly string hostName;

		/// <summary>The realm of the name.</summary>
		private readonly string realm;

		/// <summary>A pattern that matches a Kerberos name with at most 2 components.</summary>
		private static readonly Pattern nameParser = Pattern.Compile("([^/@]*)(/([^/@]*))?@([^/@]*)"
			);

		/// <summary>
		/// A pattern that matches a string with out '$' and then a single
		/// parameter with $n.
		/// </summary>
		private static Pattern parameterPattern = Pattern.Compile("([^$]*)(\\$(\\d*))?"
			);

		/// <summary>A pattern for parsing a auth_to_local rule.</summary>
		private static readonly Pattern ruleParser = Pattern.Compile("\\s*((DEFAULT)|(RULE:\\[(\\d*):([^\\]]*)](\\(([^)]*)\\))?"
			 + "(s/([^/]*)/([^/]*)/(g)?)?))/?(L)?");

		/// <summary>A pattern that recognizes simple/non-simple names.</summary>
		private static readonly Pattern nonSimplePattern = Pattern.Compile
			("[/@]");

		/// <summary>The list of translation rules.</summary>
		private static IList<KerberosName.Rule> rules;

		private static string defaultRealm;

		static KerberosName()
		{
			try
			{
				defaultRealm = KerberosUtil.GetDefaultRealm();
			}
			catch (Exception)
			{
				Log.Debug("Kerberos krb5 configuration not found, setting default realm to empty"
					);
				defaultRealm = string.Empty;
			}
		}

		/// <summary>Create a name from the full Kerberos principal name.</summary>
		/// <param name="name">full Kerberos principal name.</param>
		public KerberosName(string name)
		{
			Matcher match = nameParser.Matcher(name);
			if (!match.Matches())
			{
				if (name.Contains("@"))
				{
					throw new ArgumentException("Malformed Kerberos name: " + name);
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
				serviceName = match.Group(1);
				hostName = match.Group(3);
				realm = match.Group(4);
			}
		}

		/// <summary>Get the configured default realm.</summary>
		/// <returns>the default realm from the krb5.conf</returns>
		public virtual string GetDefaultRealm()
		{
			return defaultRealm;
		}

		/// <summary>Put the name back together from the parts.</summary>
		public override string ToString()
		{
			StringBuilder result = new StringBuilder();
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
		public virtual string GetServiceName()
		{
			return serviceName;
		}

		/// <summary>Get the second component of the name.</summary>
		/// <returns>the second section of the Kerberos principal name, and may be null</returns>
		public virtual string GetHostName()
		{
			return hostName;
		}

		/// <summary>Get the realm of the name.</summary>
		/// <returns>the realm of the name, may be null</returns>
		public virtual string GetRealm()
		{
			return realm;
		}

		/// <summary>An encoding of a rule for translating kerberos names.</summary>
		private class Rule
		{
			private readonly bool isDefault;

			private readonly int numOfComponents;

			private readonly string format;

			private readonly Pattern match;

			private readonly Pattern fromPattern;

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
				this.match = match == null ? null : Pattern.Compile(match);
				this.fromPattern = fromPattern == null ? null : Pattern.Compile(fromPattern
					);
				this.toPattern = toPattern;
				this.repeat = repeat;
				this.toLowerCase = toLowerCase;
			}

			public override string ToString()
			{
				StringBuilder buf = new StringBuilder();
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
			/// <exception cref="Org.Apache.Hadoop.Security.Authentication.Util.KerberosName.BadFormatString
			/// 	"/>
			internal static string ReplaceParameters(string format, string[] @params)
			{
				Matcher match = parameterPattern.Matcher(format);
				int start = 0;
				StringBuilder result = new StringBuilder();
				while (start < format.Length && match.Find(start))
				{
					result.Append(match.Group(1));
					string paramNum = match.Group(3);
					if (paramNum != null)
					{
						try
						{
							int num = System.Convert.ToInt32(paramNum);
							if (num < 0 || num > @params.Length)
							{
								throw new KerberosName.BadFormatString("index " + num + " from " + format + " is outside of the valid range 0 to "
									 + (@params.Length - 1));
							}
							result.Append(@params[num]);
						}
						catch (FormatException nfe)
						{
							throw new KerberosName.BadFormatString("bad format in username mapping in " + paramNum
								, nfe);
						}
					}
					start = match.End();
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
			internal static string ReplaceSubstitution(string @base, Pattern from, string
				 to, bool repeat)
			{
				Matcher match = from.Matcher(@base);
				if (repeat)
				{
					return match.ReplaceAll(to);
				}
				else
				{
					return match.ReplaceFirst(to);
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
			internal virtual string Apply(string[] @params)
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
						string @base = ReplaceParameters(format, @params);
						if (match == null || match.Matcher(@base).Matches())
						{
							if (fromPattern == null)
							{
								result = @base;
							}
							else
							{
								result = ReplaceSubstitution(@base, fromPattern, toPattern, repeat);
							}
						}
					}
				}
				if (result != null && nonSimplePattern.Matcher(result).Find())
				{
					throw new KerberosName.NoMatchingRule("Non-simple name " + result + " after auth_to_local rule "
						 + this);
				}
				if (toLowerCase && result != null)
				{
					result = result.ToLower(Extensions.GetEnglishCulture());
				}
				return result;
			}
		}

		internal static IList<KerberosName.Rule> ParseRules(string rules)
		{
			IList<KerberosName.Rule> result = new AList<KerberosName.Rule>();
			string remaining = rules.Trim();
			while (remaining.Length > 0)
			{
				Matcher matcher = ruleParser.Matcher(remaining);
				if (!matcher.LookingAt())
				{
					throw new ArgumentException("Invalid rule: " + remaining);
				}
				if (matcher.Group(2) != null)
				{
					result.AddItem(new KerberosName.Rule());
				}
				else
				{
					result.AddItem(new KerberosName.Rule(System.Convert.ToInt32(matcher.Group(4)), matcher
						.Group(5), matcher.Group(7), matcher.Group(9), matcher.Group(10), "g".Equals(matcher
						.Group(11)), "L".Equals(matcher.Group(12))));
				}
				remaining = Runtime.Substring(remaining, matcher.End());
			}
			return result;
		}

		[System.Serializable]
		public class BadFormatString : IOException
		{
			internal BadFormatString(string msg)
				: base(msg)
			{
			}

			internal BadFormatString(string msg, Exception err)
				: base(msg, err)
			{
			}
		}

		[System.Serializable]
		public class NoMatchingRule : IOException
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
		public virtual string GetShortName()
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
			foreach (KerberosName.Rule r in rules)
			{
				string result = r.Apply(@params);
				if (result != null)
				{
					return result;
				}
			}
			throw new KerberosName.NoMatchingRule("No rules applied to " + ToString());
		}

		/// <summary>Set the rules.</summary>
		/// <param name="ruleString">the rules string.</param>
		public static void SetRules(string ruleString)
		{
			rules = (ruleString != null) ? ParseRules(ruleString) : null;
		}

		/// <summary>Get the rules.</summary>
		/// <returns>String of configured rules, or null if not yet configured</returns>
		public static string GetRules()
		{
			string ruleString = null;
			if (rules != null)
			{
				StringBuilder sb = new StringBuilder();
				foreach (KerberosName.Rule rule in rules)
				{
					sb.Append(rule.ToString()).Append("\n");
				}
				ruleString = sb.ToString().Trim();
			}
			return ruleString;
		}

		/// <summary>Indicates if the name rules have been set.</summary>
		/// <returns>if the name rules have been set.</returns>
		public static bool HasRulesBeenSet()
		{
			return rules != null;
		}

		/// <exception cref="System.IO.IOException"/>
		internal static void PrintRules()
		{
			int i = 0;
			foreach (KerberosName.Rule r in rules)
			{
				System.Console.Out.WriteLine(++i + " " + r);
			}
		}
	}
}
