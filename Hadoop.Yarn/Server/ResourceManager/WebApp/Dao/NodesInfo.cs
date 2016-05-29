using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp.Dao
{
	public class NodesInfo
	{
		protected internal AList<NodeInfo> node = new AList<NodeInfo>();

		public NodesInfo()
		{
		}

		// JAXB needs this
		public virtual void Add(NodeInfo nodeinfo)
		{
			node.AddItem(nodeinfo);
		}
	}
}
