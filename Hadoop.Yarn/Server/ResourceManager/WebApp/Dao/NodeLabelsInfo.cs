using System.Collections.Generic;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp.Dao
{
	public class NodeLabelsInfo
	{
		protected internal AList<string> nodeLabels = new AList<string>();

		public NodeLabelsInfo()
		{
		}

		public NodeLabelsInfo(AList<string> nodeLabels)
		{
			// JAXB needs this
			this.nodeLabels = nodeLabels;
		}

		public NodeLabelsInfo(ICollection<string> nodeLabelsSet)
		{
			this.nodeLabels = new AList<string>(nodeLabelsSet);
		}

		public virtual AList<string> GetNodeLabels()
		{
			return nodeLabels;
		}

		public virtual void SetNodeLabels(AList<string> nodeLabels)
		{
			this.nodeLabels = nodeLabels;
		}
	}
}
