using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Web.Resources
{
	public class TokenKindParam : StringParam
	{
		/// <summary>Parameter name</summary>
		public const string Name = "kind";

		/// <summary>Default parameter value.</summary>
		public const string Default = Null;

		private static readonly StringParam.Domain Domain = new StringParam.Domain(Name, 
			null);

		/// <summary>Constructor.</summary>
		/// <param name="str">a string representation of the parameter value.</param>
		public TokenKindParam(string str)
			: base(Domain, str == null || str.Equals(Default) ? null : str)
		{
		}

		public override string GetName()
		{
			return Name;
		}
	}
}
