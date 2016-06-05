using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Web.Resources
{
	/// <summary>NewLength parameter.</summary>
	public class NewLengthParam : LongParam
	{
		/// <summary>Parameter name.</summary>
		public const string Name = "newlength";

		/// <summary>Default parameter value.</summary>
		public const string Default = Null;

		private static readonly LongParam.Domain Domain = new LongParam.Domain(Name);

		/// <summary>Constructor.</summary>
		/// <param name="value">the parameter value.</param>
		public NewLengthParam(long value)
			: base(Domain, value, 0L, null)
		{
		}

		/// <summary>Constructor.</summary>
		/// <param name="str">a string representation of the parameter value.</param>
		public NewLengthParam(string str)
			: this(Domain.Parse(str))
		{
		}

		public override string GetName()
		{
			return Name;
		}
	}
}
