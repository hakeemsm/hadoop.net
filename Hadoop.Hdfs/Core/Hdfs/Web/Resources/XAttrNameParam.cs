using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Web.Resources
{
	public class XAttrNameParam : StringParam
	{
		/// <summary>Parameter name.</summary>
		public const string Name = "xattr.name";

		/// <summary>Default parameter value.</summary>
		public const string Default = string.Empty;

		private static StringParam.Domain Domain = new StringParam.Domain(Name, Sharpen.Pattern
			.Compile(".*"));

		public XAttrNameParam(string str)
			: base(Domain, str == null || str.Equals(Default) ? null : str)
		{
		}

		public override string GetName()
		{
			return Name;
		}

		public virtual string GetXAttrName()
		{
			string v = GetValue();
			return v;
		}
	}
}
