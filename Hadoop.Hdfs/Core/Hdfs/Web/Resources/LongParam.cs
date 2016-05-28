using System;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Web.Resources
{
	/// <summary>Long parameter.</summary>
	internal abstract class LongParam : Param<long, LongParam.Domain>
	{
		internal LongParam(LongParam.Domain domain, long value, long min, long max)
			: base(domain, value)
		{
			CheckRange(min, max);
		}

		private void CheckRange(long min, long max)
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
		internal sealed class Domain : Param.Domain<long>
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
				return "<" + Null + " | long in radix " + radix + ">";
			}

			internal override long Parse(string str)
			{
				try
				{
					return Null.Equals(str) || str == null ? null : long.Parse(str, radix);
				}
				catch (FormatException e)
				{
					throw new ArgumentException("Failed to parse \"" + str + "\" as a radix-" + radix
						 + " long integer.", e);
				}
			}

			/// <summary>Convert a Long to a String.</summary>
			internal string ToString(long n)
			{
				return n == null ? Null : System.Convert.ToString(n, radix);
			}
		}
	}
}
