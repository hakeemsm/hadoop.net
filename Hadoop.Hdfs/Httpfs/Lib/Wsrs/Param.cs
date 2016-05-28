using System;
using Sharpen;

namespace Org.Apache.Hadoop.Lib.Wsrs
{
	public abstract class Param<T>
	{
		private string name;

		protected internal T value;

		public Param(string name, T defaultValue)
		{
			this.name = name;
			this.value = defaultValue;
		}

		public virtual string GetName()
		{
			return name;
		}

		public virtual T ParseParam(string str)
		{
			try
			{
				value = (str != null && str.Trim().Length > 0) ? Parse(str) : value;
			}
			catch (Exception)
			{
				throw new ArgumentException(MessageFormat.Format("Parameter [{0}], invalid value [{1}], value must be [{2}]"
					, name, str, GetDomain()));
			}
			return value;
		}

		public virtual T Value()
		{
			return value;
		}

		protected internal abstract string GetDomain();

		/// <exception cref="System.Exception"/>
		protected internal abstract T Parse(string str);

		public override string ToString()
		{
			return (value != null) ? value.ToString() : "NULL";
		}
	}
}
