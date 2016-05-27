using System.Collections.Generic;
using Com.Google.Common.Collect;
using Org.Apache.Commons.Configuration;
using Org.Apache.Hadoop.Metrics2;
using Sharpen;

namespace Org.Apache.Hadoop.Metrics2.Filter
{
	/// <summary>Base class for pattern based filters</summary>
	public abstract class AbstractPatternFilter : MetricsFilter
	{
		protected internal const string IncludeKey = "include";

		protected internal const string ExcludeKey = "exclude";

		protected internal const string IncludeTagsKey = "include.tags";

		protected internal const string ExcludeTagsKey = "exclude.tags";

		private Sharpen.Pattern includePattern;

		private Sharpen.Pattern excludePattern;

		private readonly IDictionary<string, Sharpen.Pattern> includeTagPatterns;

		private readonly IDictionary<string, Sharpen.Pattern> excludeTagPatterns;

		private readonly Sharpen.Pattern tagPattern = Sharpen.Pattern.Compile("^(\\w+):(.*)"
			);

		internal AbstractPatternFilter()
		{
			includeTagPatterns = Maps.NewHashMap();
			excludeTagPatterns = Maps.NewHashMap();
		}

		public override void Init(SubsetConfiguration conf)
		{
			string patternString = conf.GetString(IncludeKey);
			if (patternString != null && !patternString.IsEmpty())
			{
				SetIncludePattern(Compile(patternString));
			}
			patternString = conf.GetString(ExcludeKey);
			if (patternString != null && !patternString.IsEmpty())
			{
				SetExcludePattern(Compile(patternString));
			}
			string[] patternStrings = conf.GetStringArray(IncludeTagsKey);
			if (patternStrings != null && patternStrings.Length != 0)
			{
				foreach (string pstr in patternStrings)
				{
					Matcher matcher = tagPattern.Matcher(pstr);
					if (!matcher.Matches())
					{
						throw new MetricsException("Illegal tag pattern: " + pstr);
					}
					SetIncludeTagPattern(matcher.Group(1), Compile(matcher.Group(2)));
				}
			}
			patternStrings = conf.GetStringArray(ExcludeTagsKey);
			if (patternStrings != null && patternStrings.Length != 0)
			{
				foreach (string pstr in patternStrings)
				{
					Matcher matcher = tagPattern.Matcher(pstr);
					if (!matcher.Matches())
					{
						throw new MetricsException("Illegal tag pattern: " + pstr);
					}
					SetExcludeTagPattern(matcher.Group(1), Compile(matcher.Group(2)));
				}
			}
		}

		internal virtual void SetIncludePattern(Sharpen.Pattern includePattern)
		{
			this.includePattern = includePattern;
		}

		internal virtual void SetExcludePattern(Sharpen.Pattern excludePattern)
		{
			this.excludePattern = excludePattern;
		}

		internal virtual void SetIncludeTagPattern(string name, Sharpen.Pattern pattern)
		{
			includeTagPatterns[name] = pattern;
		}

		internal virtual void SetExcludeTagPattern(string name, Sharpen.Pattern pattern)
		{
			excludeTagPatterns[name] = pattern;
		}

		public override bool Accepts(MetricsTag tag)
		{
			// Accept if whitelisted
			Sharpen.Pattern ipat = includeTagPatterns[tag.Name()];
			if (ipat != null && ipat.Matcher(tag.Value()).Matches())
			{
				return true;
			}
			// Reject if blacklisted
			Sharpen.Pattern epat = excludeTagPatterns[tag.Name()];
			if (epat != null && epat.Matcher(tag.Value()).Matches())
			{
				return false;
			}
			// Reject if no match in whitelist only mode
			if (!includeTagPatterns.IsEmpty() && excludeTagPatterns.IsEmpty())
			{
				return false;
			}
			return true;
		}

		public override bool Accepts(IEnumerable<MetricsTag> tags)
		{
			// Accept if any include tag pattern matches
			foreach (MetricsTag t in tags)
			{
				Sharpen.Pattern pat = includeTagPatterns[t.Name()];
				if (pat != null && pat.Matcher(t.Value()).Matches())
				{
					return true;
				}
			}
			// Reject if any exclude tag pattern matches
			foreach (MetricsTag t_1 in tags)
			{
				Sharpen.Pattern pat = excludeTagPatterns[t_1.Name()];
				if (pat != null && pat.Matcher(t_1.Value()).Matches())
				{
					return false;
				}
			}
			// Reject if no match in whitelist only mode
			if (!includeTagPatterns.IsEmpty() && excludeTagPatterns.IsEmpty())
			{
				return false;
			}
			return true;
		}

		public override bool Accepts(string name)
		{
			// Accept if whitelisted
			if (includePattern != null && includePattern.Matcher(name).Matches())
			{
				return true;
			}
			// Reject if blacklisted
			if ((excludePattern != null && excludePattern.Matcher(name).Matches()))
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
		protected internal abstract Sharpen.Pattern Compile(string s);
	}
}
