using System.Collections.Generic;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp.Dao
{
	public class NodeToLabelsInfo
	{
		protected internal Dictionary<string, NodeLabelsInfo> nodeToLabels = new Dictionary
			<string, NodeLabelsInfo>();

		public NodeToLabelsInfo()
		{
		}

		// JAXB needs this
		public virtual Dictionary<string, NodeLabelsInfo> GetNodeToLabels()
		{
			return nodeToLabels;
		}
	}
}
