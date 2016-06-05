using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Web.Resources
{
	/// <summary>Owner parameter.</summary>
	public class OwnerParam : StringParam
	{
		/// <summary>Parameter name.</summary>
		public const string Name = "owner";

		/// <summary>Default parameter value.</summary>
		public const string Default = string.Empty;

		private static readonly StringParam.Domain Domain = new StringParam.Domain(Name, 
			null);

		/// <summary>Constructor.</summary>
		/// <param name="str">a string representation of the parameter value.</param>
		public OwnerParam(string str)
			: base(Domain, str == null || str.Equals(Default) ? null : str)
		{
		}

		public override string GetName()
		{
			return Name;
		}
	}
}
