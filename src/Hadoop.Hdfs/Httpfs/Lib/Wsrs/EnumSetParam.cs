using System;
using System.Collections.Generic;
using System.Text;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Lib.Wsrs
{
	public abstract class EnumSetParam<E> : Param<EnumSet<E>>
		where E : Enum<E>
	{
		internal Type klass;

		public EnumSetParam(string name, Type e, EnumSet<E> defaultValue)
			: base(name, defaultValue)
		{
			klass = e;
		}

		/// <exception cref="System.Exception"/>
		protected internal override EnumSet<E> Parse(string str)
		{
			EnumSet<E> set = EnumSet.NoneOf(klass);
			if (!str.IsEmpty())
			{
				foreach (string sub in str.Split(","))
				{
					set.AddItem(Enum.ValueOf(klass, StringUtils.ToUpperCase(sub.Trim())));
				}
			}
			return set;
		}

		protected internal override string GetDomain()
		{
			return Arrays.AsList(klass.GetEnumConstants()).ToString();
		}

		/// <summary>Convert an EnumSet to a string of comma separated values.</summary>
		public static string ToString<E>(EnumSet<E> set)
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
				while (i.HasNext())
				{
					b.Append(',').Append(i.Next());
				}
				return b.ToString();
			}
		}

		public override string ToString()
		{
			return GetName() + "=" + ToString(value);
		}
	}
}
