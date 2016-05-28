using System;
using System.Collections.Generic;
using System.Text;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Web.Resources
{
	internal abstract class EnumSetParam<E> : Param<EnumSet<E>, EnumSetParam.Domain<E
		>>
		where E : Enum<E>
	{
		/// <summary>Convert an EnumSet to a string of comma separated values.</summary>
		internal static string ToString<E>(EnumSet<E> set)
			where E : Enum<E>
		{
			if (set == null || set.IsEmpty())
			{
				return string.Empty;
			}
			else
			{
				StringBuilder b = new StringBuilder();
				IEnumerator<E> i = set.GetEnumerator();
				b.Append(i.Next());
				for (; i.HasNext(); )
				{
					b.Append(',').Append(i.Next());
				}
				return b.ToString();
			}
		}

		internal static EnumSet<E> ToEnumSet<E>(params E[] values)
			where E : Enum<E>
		{
			System.Type clazz = typeof(E);
			EnumSet<E> set = EnumSet.NoneOf(clazz);
			Sharpen.Collections.AddAll(set, Arrays.AsList(values));
			return set;
		}

		internal EnumSetParam(EnumSetParam.Domain<E> domain, EnumSet<E> value)
			: base(domain, value)
		{
		}

		public override string ToString()
		{
			return GetName() + "=" + ToString(value);
		}

		/// <returns>the parameter value as a string</returns>
		public override string GetValueString()
		{
			return ToString(value);
		}

		/// <summary>The domain of the parameter.</summary>
		internal sealed class Domain<E> : Param.Domain<EnumSet<E>>
			where E : Enum<E>
		{
			private readonly Type enumClass;

			internal Domain(string name, Type enumClass)
				: base(name)
			{
				this.enumClass = enumClass;
			}

			public sealed override string GetDomain()
			{
				return Arrays.AsList(enumClass.GetEnumConstants()).ToString();
			}

			/// <summary>The string contains a comma separated values.</summary>
			internal sealed override EnumSet<E> Parse(string str)
			{
				EnumSet<E> set = EnumSet.NoneOf(enumClass);
				if (!str.IsEmpty())
				{
					for (int i; j >= 0; )
					{
						i = j > 0 ? j + 1 : 0;
						j = str.IndexOf(',', i);
						string sub = j >= 0 ? Sharpen.Runtime.Substring(str, i, j) : Sharpen.Runtime.Substring
							(str, i);
						set.AddItem(Enum.ValueOf(enumClass, StringUtils.ToUpperCase(sub.Trim())));
					}
				}
				return set;
			}
		}
	}
}
