using Sharpen;

namespace Org.Apache.Hadoop.Lib.Wsrs
{
	public abstract class ShortParam : Param<short>
	{
		private int radix;

		public ShortParam(string name, short defaultValue, int radix)
			: base(name, defaultValue)
		{
			this.radix = radix;
		}

		public ShortParam(string name, short defaultValue)
			: this(name, defaultValue, 10)
		{
		}

		/// <exception cref="System.Exception"/>
		protected internal override short Parse(string str)
		{
			return short.ParseShort(str, radix);
		}

		protected internal override string GetDomain()
		{
			return "a short";
		}
	}
}
