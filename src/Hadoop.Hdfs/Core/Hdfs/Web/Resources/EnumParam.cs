using System;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Web.Resources
{
	internal abstract class EnumParam<E> : Param<E, EnumParam.Domain<E>>
		where E : Enum<E>
	{
		internal EnumParam(EnumParam.Domain<E> domain, E value)
			: base(domain, value)
		{
		}

		/// <summary>The domain of the parameter.</summary>
		internal sealed class Domain<E> : Param.Domain<E>
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

			internal sealed override E Parse(string str)
			{
				return Enum.ValueOf(enumClass, StringUtils.ToUpperCase(str));
			}
		}
	}
}
