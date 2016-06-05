using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Web.Resources
{
	public class XAttrValueParam : StringParam
	{
		/// <summary>Parameter name.</summary>
		public const string Name = "xattr.value";

		/// <summary>Default parameter value.</summary>
		public const string Default = string.Empty;

		private static StringParam.Domain Domain = new StringParam.Domain(Name, null);

		public XAttrValueParam(string str)
			: base(Domain, str == null || str.Equals(Default) ? null : str)
		{
		}

		public override string GetName()
		{
			return Name;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual byte[] GetXAttrValue()
		{
			string v = GetValue();
			return XAttrCodec.DecodeValue(v);
		}
	}
}
