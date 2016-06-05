using Org.Apache.Hadoop.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Web.Resources
{
	/// <summary>Represents delegation token used for authentication.</summary>
	public class DelegationParam : StringParam
	{
		/// <summary>Parameter name.</summary>
		public const string Name = "delegation";

		/// <summary>Default parameter value.</summary>
		public const string Default = string.Empty;

		private static readonly StringParam.Domain Domain = new StringParam.Domain(Name, 
			null);

		/// <summary>Constructor.</summary>
		/// <param name="str">a string representation of the parameter value.</param>
		public DelegationParam(string str)
			: base(Domain, UserGroupInformation.IsSecurityEnabled() && str != null && !str.Equals
				(Default) ? str : null)
		{
		}

		public override string GetName()
		{
			return Name;
		}
	}
}
