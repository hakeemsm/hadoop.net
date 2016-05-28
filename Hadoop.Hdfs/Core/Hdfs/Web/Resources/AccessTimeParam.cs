using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Web.Resources
{
	/// <summary>Access time parameter.</summary>
	public class AccessTimeParam : LongParam
	{
		/// <summary>Parameter name.</summary>
		public const string Name = "accesstime";

		/// <summary>Default parameter value.</summary>
		public const string Default = "-1";

		private static readonly LongParam.Domain Domain = new LongParam.Domain(Name);

		/// <summary>Constructor.</summary>
		/// <param name="value">the parameter value.</param>
		public AccessTimeParam(long value)
			: base(Domain, value, -1L, null)
		{
		}

		/// <summary>Constructor.</summary>
		/// <param name="str">a string representation of the parameter value.</param>
		public AccessTimeParam(string str)
			: this(Domain.Parse(str))
		{
		}

		public override string GetName()
		{
			return Name;
		}
	}
}
