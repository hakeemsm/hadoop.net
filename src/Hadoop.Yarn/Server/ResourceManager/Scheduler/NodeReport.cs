using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler
{
	/// <summary>Node usage report.</summary>
	public class NodeReport
	{
		private readonly Resource usedResources;

		private readonly int numContainers;

		public NodeReport(Resource used, int numContainers)
		{
			this.usedResources = used;
			this.numContainers = numContainers;
		}

		public virtual Resource GetUsedResources()
		{
			return usedResources;
		}

		public virtual int GetNumContainers()
		{
			return numContainers;
		}
	}
}
