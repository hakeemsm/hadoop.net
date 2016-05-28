using System;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Web.Resources
{
	/// <summary>Integer parameter.</summary>
	internal abstract class IntegerParam : Param<int, IntegerParam.Domain>
	{
		internal IntegerParam(IntegerParam.Domain domain, int value, int min, int max)
			: base(domain, value)
		{
			CheckRange(min, max);
		}

		private void CheckRange(int min, int max)
		{
			if (value == null)
			{
				return;
			}
			if (min != null && value < min)
			{
				throw new ArgumentException("Invalid parameter range: " + GetName() + " = " + domain
					.ToString(value) + " < " + domain.ToString(min));
			}
			if (max != null && value > max)
			{
				throw new ArgumentException("Invalid parameter range: " + GetName() + " = " + domain
					.ToString(value) + " > " + domain.ToString(max));
			}
		}

		public override string ToString()
		{
			return GetName() + "=" + domain.ToString(GetValue());
		}

		/// <returns>the parameter value as a string</returns>
		public override string GetValueString()
		{
			return domain.ToString(GetValue());
		}

		/// <summary>The domain of the parameter.</summary>
		internal sealed class Domain : Param.Domain<int>
		{
			/// <summary>The radix of the number.</summary>
			internal readonly int radix;

			internal Domain(string paramName)
				: this(paramName, 10)
			{
			}

			internal Domain(string paramName, int radix)
				: base(paramName)
			{
				this.radix = radix;
			}

			public override string GetDomain()
			{
				return "<" + Null + " | int in radix " + radix + ">";
			}

			internal override int Parse(string str)
			{
				try
				{
					return Null.Equals(str) || str == null ? null : System.Convert.ToInt32(str, radix
						);
				}
				catch (FormatException e)
				{
					throw new ArgumentException("Failed to parse \"" + str + "\" as a radix-" + radix
						 + " integer.", e);
				}
			}

			/// <summary>Convert an Integer to a String.</summary>
			internal string ToString(int n)
			{
				return n == null ? Null : Sharpen.Extensions.ToString(n, radix);
			}
		}
	}
}
