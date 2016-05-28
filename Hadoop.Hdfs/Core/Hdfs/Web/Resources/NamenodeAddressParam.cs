using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Web.Resources
{
	/// <summary>Namenode RPC address parameter.</summary>
	public class NamenodeAddressParam : StringParam
	{
		/// <summary>Parameter name.</summary>
		public const string Name = "namenoderpcaddress";

		/// <summary>Default parameter value.</summary>
		public const string Default = string.Empty;

		private static readonly StringParam.Domain Domain = new StringParam.Domain(Name, 
			null);

		/// <summary>Constructor.</summary>
		/// <param name="str">a string representation of the parameter value.</param>
		public NamenodeAddressParam(string str)
			: base(Domain, str == null || str.Equals(Default) ? null : Domain.Parse(str))
		{
		}

		/// <summary>Construct an object using the RPC address of the given namenode.</summary>
		public NamenodeAddressParam(NameNode namenode)
			: base(Domain, namenode.GetTokenServiceName())
		{
		}

		public override string GetName()
		{
			return Name;
		}
	}
}
