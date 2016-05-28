using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Web.Resources
{
	public class XAttrSetFlagParam : EnumSetParam<XAttrSetFlag>
	{
		/// <summary>Parameter name.</summary>
		public const string Name = "flag";

		/// <summary>Default parameter value.</summary>
		public const string Default = string.Empty;

		private static readonly EnumSetParam.Domain<XAttrSetFlag> Domain = new EnumSetParam.Domain
			<XAttrSetFlag>(Name, typeof(XAttrSetFlag));

		public XAttrSetFlagParam(EnumSet<XAttrSetFlag> flag)
			: base(Domain, flag)
		{
		}

		/// <summary>Constructor.</summary>
		/// <param name="str">a string representation of the parameter value.</param>
		public XAttrSetFlagParam(string str)
			: base(Domain, Domain.Parse(str))
		{
		}

		public override string GetName()
		{
			return Name;
		}

		public virtual EnumSet<XAttrSetFlag> GetFlag()
		{
			return GetValue();
		}
	}
}
