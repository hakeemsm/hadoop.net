using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Web.Resources
{
	public class TokenServiceParam : StringParam
	{
		/// <summary>Parameter name</summary>
		public const string Name = "service";

		/// <summary>Default parameter value.</summary>
		public const string Default = Null;

		private static readonly StringParam.Domain Domain = new StringParam.Domain(Name, 
			null);

		/// <summary>Constructor.</summary>
		/// <param name="str">a string representation of the parameter value.</param>
		public TokenServiceParam(string str)
			: base(Domain, str == null || str.Equals(Default) ? null : str)
		{
		}

		public override string GetName()
		{
			return Name;
		}
	}
}
