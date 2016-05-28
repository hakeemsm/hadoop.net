using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Web.Resources
{
	/// <summary>Exclude datanodes param</summary>
	public class ExcludeDatanodesParam : StringParam
	{
		/// <summary>Parameter name.</summary>
		public const string Name = "excludedatanodes";

		/// <summary>Default parameter value.</summary>
		public const string Default = string.Empty;

		private static readonly StringParam.Domain Domain = new StringParam.Domain(Name, 
			null);

		/// <summary>Constructor.</summary>
		/// <param name="str">a string representation of the parameter value.</param>
		public ExcludeDatanodesParam(string str)
			: base(Domain, str == null || str.Equals(Default) ? null : Domain.Parse(str))
		{
		}

		public override string GetName()
		{
			return Name;
		}
	}
}
