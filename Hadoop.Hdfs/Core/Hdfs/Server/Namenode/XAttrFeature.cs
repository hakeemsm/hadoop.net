using Com.Google.Common.Collect;
using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>Feature for extended attributes.</summary>
	public class XAttrFeature : INode.Feature
	{
		public static readonly ImmutableList<XAttr> EmptyEntryList = ImmutableList.Of();

		private readonly ImmutableList<XAttr> xAttrs;

		public XAttrFeature(ImmutableList<XAttr> xAttrs)
		{
			this.xAttrs = xAttrs;
		}

		public virtual ImmutableList<XAttr> GetXAttrs()
		{
			return xAttrs;
		}
	}
}
