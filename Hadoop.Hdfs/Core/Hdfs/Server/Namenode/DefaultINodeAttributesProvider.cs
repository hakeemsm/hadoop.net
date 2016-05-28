using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>A default implementation of the INodeAttributesProvider</summary>
	public class DefaultINodeAttributesProvider : INodeAttributeProvider
	{
		public static INodeAttributeProvider DefaultProvider = new DefaultINodeAttributesProvider
			();

		public override void Start()
		{
		}

		// NO-OP
		public override void Stop()
		{
		}

		// NO-OP
		public override INodeAttributes GetAttributes(string[] pathElements, INodeAttributes
			 inode)
		{
			return inode;
		}
	}
}
