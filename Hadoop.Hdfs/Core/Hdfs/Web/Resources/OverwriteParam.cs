using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Web.Resources
{
	/// <summary>Overwrite parameter.</summary>
	public class OverwriteParam : BooleanParam
	{
		/// <summary>Parameter name.</summary>
		public const string Name = "overwrite";

		/// <summary>Default parameter value.</summary>
		public const string Default = False;

		private static readonly BooleanParam.Domain Domain = new BooleanParam.Domain(Name
			);

		/// <summary>Constructor.</summary>
		/// <param name="value">the parameter value.</param>
		public OverwriteParam(bool value)
			: base(Domain, value)
		{
		}

		/// <summary>Constructor.</summary>
		/// <param name="str">a string representation of the parameter value.</param>
		public OverwriteParam(string str)
			: this(Domain.Parse(str))
		{
		}

		public override string GetName()
		{
			return Name;
		}
	}
}
