using System;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Lib.Wsrs
{
	public abstract class EnumParam<E> : Param<E>
		where E : Enum<E>
	{
		internal Type klass;

		public EnumParam(string name, Type e, E defaultValue)
			: base(name, defaultValue)
		{
			klass = e;
		}

		/// <exception cref="System.Exception"/>
		protected internal override E Parse(string str)
		{
			return Enum.ValueOf(klass, StringUtils.ToUpperCase(str));
		}

		protected internal override string GetDomain()
		{
			return StringUtils.Join(",", Arrays.AsList(klass.GetEnumConstants()));
		}
	}
}
