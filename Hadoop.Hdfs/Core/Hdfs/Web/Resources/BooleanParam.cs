using System;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Web.Resources
{
	/// <summary>Boolean parameter.</summary>
	internal abstract class BooleanParam : Param<bool, BooleanParam.Domain>
	{
		internal const string True = "true";

		internal const string False = "false";

		/// <returns>the parameter value as a string</returns>
		public override string GetValueString()
		{
			return value.ToString();
		}

		internal BooleanParam(BooleanParam.Domain domain, bool value)
			: base(domain, value)
		{
		}

		/// <summary>The domain of the parameter.</summary>
		internal sealed class Domain : Param.Domain<bool>
		{
			internal Domain(string paramName)
				: base(paramName)
			{
			}

			public override string GetDomain()
			{
				return "<" + Null + " | boolean>";
			}

			internal override bool Parse(string str)
			{
				if (Sharpen.Runtime.EqualsIgnoreCase(True, str))
				{
					return true;
				}
				else
				{
					if (Sharpen.Runtime.EqualsIgnoreCase(False, str))
					{
						return false;
					}
				}
				throw new ArgumentException("Failed to parse \"" + str + "\" to Boolean.");
			}
		}
	}
}
