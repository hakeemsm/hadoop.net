using Sharpen;

namespace Org.Apache.Hadoop.Lib.Wsrs
{
	public abstract class LongParam : Param<long>
	{
		public LongParam(string name, long defaultValue)
			: base(name, defaultValue)
		{
		}

		/// <exception cref="System.Exception"/>
		protected internal override long Parse(string str)
		{
			return long.Parse(str);
		}

		protected internal override string GetDomain()
		{
			return "a long";
		}
	}
}
