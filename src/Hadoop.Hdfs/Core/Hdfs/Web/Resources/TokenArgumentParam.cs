using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Web.Resources
{
	/// <summary>Represents delegation token parameter as method arguments.</summary>
	/// <remarks>
	/// Represents delegation token parameter as method arguments. This is
	/// different from
	/// <see cref="DelegationParam"/>
	/// .
	/// </remarks>
	public class TokenArgumentParam : StringParam
	{
		/// <summary>Parameter name.</summary>
		public const string Name = "token";

		/// <summary>Default parameter value.</summary>
		public const string Default = string.Empty;

		private static readonly StringParam.Domain Domain = new StringParam.Domain(Name, 
			null);

		/// <summary>Constructor.</summary>
		/// <param name="str">A string representation of the parameter value.</param>
		public TokenArgumentParam(string str)
			: base(Domain, str != null && !str.Equals(Default) ? str : null)
		{
		}

		public override string GetName()
		{
			return Name;
		}
	}
}
