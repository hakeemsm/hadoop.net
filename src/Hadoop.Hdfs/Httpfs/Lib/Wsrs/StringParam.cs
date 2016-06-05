using System;
using Sharpen;

namespace Org.Apache.Hadoop.Lib.Wsrs
{
	public abstract class StringParam : Param<string>
	{
		private Sharpen.Pattern pattern;

		public StringParam(string name, string defaultValue)
			: this(name, defaultValue, null)
		{
		}

		public StringParam(string name, string defaultValue, Sharpen.Pattern pattern)
			: base(name, defaultValue)
		{
			this.pattern = pattern;
			ParseParam(defaultValue);
		}

		public override string ParseParam(string str)
		{
			try
			{
				if (str != null)
				{
					str = str.Trim();
					if (str.Length > 0)
					{
						value = Parse(str);
					}
				}
			}
			catch (Exception)
			{
				throw new ArgumentException(MessageFormat.Format("Parameter [{0}], invalid value [{1}], value must be [{2}]"
					, GetName(), str, GetDomain()));
			}
			return value;
		}

		/// <exception cref="System.Exception"/>
		protected internal override string Parse(string str)
		{
			if (pattern != null)
			{
				if (!pattern.Matcher(str).Matches())
				{
					throw new ArgumentException("Invalid value");
				}
			}
			return str;
		}

		protected internal override string GetDomain()
		{
			return (pattern == null) ? "a string" : pattern.Pattern();
		}
	}
}
