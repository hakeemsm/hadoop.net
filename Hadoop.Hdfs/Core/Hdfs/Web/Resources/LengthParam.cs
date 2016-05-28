using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Web.Resources
{
	/// <summary>Length parameter.</summary>
	public class LengthParam : LongParam
	{
		/// <summary>Parameter name.</summary>
		public const string Name = "length";

		/// <summary>Default parameter value.</summary>
		public const string Default = Null;

		private static readonly LongParam.Domain Domain = new LongParam.Domain(Name);

		/// <summary>Constructor.</summary>
		/// <param name="value">the parameter value.</param>
		public LengthParam(long value)
			: base(Domain, value, 0L, null)
		{
		}

		/// <summary>Constructor.</summary>
		/// <param name="str">a string representation of the parameter value.</param>
		public LengthParam(string str)
			: this(Domain.Parse(str))
		{
		}

		public override string GetName()
		{
			return Name;
		}

		public virtual long GetLength()
		{
			long v = GetValue();
			return v == null ? -1 : v;
		}
	}
}
