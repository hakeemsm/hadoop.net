using Sharpen;

namespace Org.Apache.Hadoop.Lib.Wsrs
{
	public abstract class IntegerParam : Param<int>
	{
		public IntegerParam(string name, int defaultValue)
			: base(name, defaultValue)
		{
		}

		/// <exception cref="System.Exception"/>
		protected internal override int Parse(string str)
		{
			return System.Convert.ToInt32(str);
		}

		protected internal override string GetDomain()
		{
			return "an integer";
		}
	}
}
