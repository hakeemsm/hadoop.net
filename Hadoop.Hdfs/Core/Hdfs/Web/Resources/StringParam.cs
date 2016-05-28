using System;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Web.Resources
{
	/// <summary>String parameter.</summary>
	internal abstract class StringParam : Param<string, StringParam.Domain>
	{
		internal StringParam(StringParam.Domain domain, string str)
			: base(domain, domain.Parse(str))
		{
		}

		/// <returns>the parameter value as a string</returns>
		public override string GetValueString()
		{
			return value;
		}

		/// <summary>The domain of the parameter.</summary>
		internal sealed class Domain : Param.Domain<string>
		{
			/// <summary>The pattern defining the domain; null .</summary>
			private readonly Sharpen.Pattern pattern;

			internal Domain(string paramName, Sharpen.Pattern pattern)
				: base(paramName)
			{
				this.pattern = pattern;
			}

			public sealed override string GetDomain()
			{
				return pattern == null ? "<String>" : pattern.Pattern();
			}

			internal sealed override string Parse(string str)
			{
				if (str != null && pattern != null)
				{
					if (!pattern.Matcher(str).Matches())
					{
						throw new ArgumentException("Invalid value: \"" + str + "\" does not belong to the domain "
							 + GetDomain());
					}
				}
				return str;
			}
		}
	}
}
