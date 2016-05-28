using System;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Web.Resources
{
	/// <summary>Short parameter.</summary>
	internal abstract class ShortParam : Param<short, ShortParam.Domain>
	{
		internal ShortParam(ShortParam.Domain domain, short value, short min, short max)
			: base(domain, value)
		{
			CheckRange(min, max);
		}

		private void CheckRange(short min, short max)
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
		public sealed override string GetValueString()
		{
			return domain.ToString(GetValue());
		}

		/// <summary>The domain of the parameter.</summary>
		internal sealed class Domain : Param.Domain<short>
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
				return "<" + Null + " | short in radix " + radix + ">";
			}

			internal override short Parse(string str)
			{
				try
				{
					return Null.Equals(str) || str == null ? null : short.ParseShort(str, radix);
				}
				catch (FormatException e)
				{
					throw new ArgumentException("Failed to parse \"" + str + "\" as a radix-" + radix
						 + " short integer.", e);
				}
			}

			/// <summary>Convert a Short to a String.</summary>
			internal string ToString(short n)
			{
				return n == null ? Null : Sharpen.Extensions.ToString(n, radix);
			}
		}
	}
}
