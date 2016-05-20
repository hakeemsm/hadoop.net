using Sharpen;

namespace org.apache.hadoop.metrics2.filter
{
	/// <summary>Base class for pattern based filters</summary>
	public abstract class AbstractPatternFilter : org.apache.hadoop.metrics2.MetricsFilter
	{
		protected internal const string INCLUDE_KEY = "include";

		protected internal const string EXCLUDE_KEY = "exclude";

		protected internal const string INCLUDE_TAGS_KEY = "include.tags";

		protected internal const string EXCLUDE_TAGS_KEY = "exclude.tags";

		private java.util.regex.Pattern includePattern;

		private java.util.regex.Pattern excludePattern;

		private readonly System.Collections.Generic.IDictionary<string, java.util.regex.Pattern
			> includeTagPatterns;

		private readonly System.Collections.Generic.IDictionary<string, java.util.regex.Pattern
			> excludeTagPatterns;

		private readonly java.util.regex.Pattern tagPattern = java.util.regex.Pattern.compile
			("^(\\w+):(.*)");

		internal AbstractPatternFilter()
		{
			includeTagPatterns = com.google.common.collect.Maps.newHashMap();
			excludeTagPatterns = com.google.common.collect.Maps.newHashMap();
		}

		public override void init(org.apache.commons.configuration.SubsetConfiguration conf
			)
		{
			string patternString = conf.getString(INCLUDE_KEY);
			if (patternString != null && !patternString.isEmpty())
			{
				setIncludePattern(compile(patternString));
			}
			patternString = conf.getString(EXCLUDE_KEY);
			if (patternString != null && !patternString.isEmpty())
			{
				setExcludePattern(compile(patternString));
			}
			string[] patternStrings = conf.getStringArray(INCLUDE_TAGS_KEY);
			if (patternStrings != null && patternStrings.Length != 0)
			{
				foreach (string pstr in patternStrings)
				{
					java.util.regex.Matcher matcher = tagPattern.matcher(pstr);
					if (!matcher.matches())
					{
						throw new org.apache.hadoop.metrics2.MetricsException("Illegal tag pattern: " + pstr
							);
					}
					setIncludeTagPattern(matcher.group(1), compile(matcher.group(2)));
				}
			}
			patternStrings = conf.getStringArray(EXCLUDE_TAGS_KEY);
			if (patternStrings != null && patternStrings.Length != 0)
			{
				foreach (string pstr in patternStrings)
				{
					java.util.regex.Matcher matcher = tagPattern.matcher(pstr);
					if (!matcher.matches())
					{
						throw new org.apache.hadoop.metrics2.MetricsException("Illegal tag pattern: " + pstr
							);
					}
					setExcludeTagPattern(matcher.group(1), compile(matcher.group(2)));
				}
			}
		}

		internal virtual void setIncludePattern(java.util.regex.Pattern includePattern)
		{
			this.includePattern = includePattern;
		}

		internal virtual void setExcludePattern(java.util.regex.Pattern excludePattern)
		{
			this.excludePattern = excludePattern;
		}

		internal virtual void setIncludeTagPattern(string name, java.util.regex.Pattern pattern
			)
		{
			includeTagPatterns[name] = pattern;
		}

		internal virtual void setExcludeTagPattern(string name, java.util.regex.Pattern pattern
			)
		{
			excludeTagPatterns[name] = pattern;
		}

		public override bool accepts(org.apache.hadoop.metrics2.MetricsTag tag)
		{
			// Accept if whitelisted
			java.util.regex.Pattern ipat = includeTagPatterns[tag.name()];
			if (ipat != null && ipat.matcher(tag.value()).matches())
			{
				return true;
			}
			// Reject if blacklisted
			java.util.regex.Pattern epat = excludeTagPatterns[tag.name()];
			if (epat != null && epat.matcher(tag.value()).matches())
			{
				return false;
			}
			// Reject if no match in whitelist only mode
			if (!includeTagPatterns.isEmpty() && excludeTagPatterns.isEmpty())
			{
				return false;
			}
			return true;
		}

		public override bool accepts(System.Collections.Generic.IEnumerable<org.apache.hadoop.metrics2.MetricsTag
			> tags)
		{
			// Accept if any include tag pattern matches
			foreach (org.apache.hadoop.metrics2.MetricsTag t in tags)
			{
				java.util.regex.Pattern pat = includeTagPatterns[t.name()];
				if (pat != null && pat.matcher(t.value()).matches())
				{
					return true;
				}
			}
			// Reject if any exclude tag pattern matches
			foreach (org.apache.hadoop.metrics2.MetricsTag t_1 in tags)
			{
				java.util.regex.Pattern pat = excludeTagPatterns[t_1.name()];
				if (pat != null && pat.matcher(t_1.value()).matches())
				{
					return false;
				}
			}
			// Reject if no match in whitelist only mode
			if (!includeTagPatterns.isEmpty() && excludeTagPatterns.isEmpty())
			{
				return false;
			}
			return true;
		}

		public override bool accepts(string name)
		{
			// Accept if whitelisted
			if (includePattern != null && includePattern.matcher(name).matches())
			{
				return true;
			}
			// Reject if blacklisted
			if ((excludePattern != null && excludePattern.matcher(name).matches()))
			{
				return false;
			}
			// Reject if no match in whitelist only mode
			if (includePattern != null && excludePattern == null)
			{
				return false;
			}
			return true;
		}

		/// <summary>Compile a string pattern in to a pattern object</summary>
		/// <param name="s">the string pattern to compile</param>
		/// <returns>the compiled pattern object</returns>
		protected internal abstract java.util.regex.Pattern compile(string s);
	}
}
