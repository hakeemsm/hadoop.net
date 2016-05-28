using System;
using Sharpen;

namespace Org.Apache.Hadoop.Lib.Wsrs
{
	public abstract class BooleanParam : Param<bool>
	{
		public BooleanParam(string name, bool defaultValue)
			: base(name, defaultValue)
		{
		}

		/// <exception cref="System.Exception"/>
		protected internal override bool Parse(string str)
		{
			if (Sharpen.Runtime.EqualsIgnoreCase(str, "true"))
			{
				return true;
			}
			else
			{
				if (Sharpen.Runtime.EqualsIgnoreCase(str, "false"))
				{
					return false;
				}
			}
			throw new ArgumentException(MessageFormat.Format("Invalid value [{0}], must be a boolean"
				, str));
		}

		protected internal override string GetDomain()
		{
			return "a boolean";
		}
	}
}
