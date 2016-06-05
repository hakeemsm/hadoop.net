using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Web.Resources
{
	public class XAttrEncodingParam : EnumParam<XAttrCodec>
	{
		/// <summary>Parameter name.</summary>
		public const string Name = "encoding";

		/// <summary>Default parameter value.</summary>
		public const string Default = string.Empty;

		private static readonly EnumParam.Domain<XAttrCodec> Domain = new EnumParam.Domain
			<XAttrCodec>(Name, typeof(XAttrCodec));

		public XAttrEncodingParam(XAttrCodec encoding)
			: base(Domain, encoding)
		{
		}

		/// <summary>Constructor.</summary>
		/// <param name="str">a string representation of the parameter value.</param>
		public XAttrEncodingParam(string str)
			: base(Domain, str != null && !str.IsEmpty() ? Domain.Parse(str) : null)
		{
		}

		public override string GetName()
		{
			return Name;
		}

		public override string GetValueString()
		{
			return value.ToString();
		}

		public virtual XAttrCodec GetEncoding()
		{
			return GetValue();
		}
	}
}
