using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler
{
	/// <summary>Node usage report.</summary>
	public class SchedulerNodeReport
	{
		private readonly Resource used;

		private readonly Resource avail;

		private readonly int num;

		public SchedulerNodeReport(SchedulerNode node)
		{
			this.used = node.GetUsedResource();
			this.avail = node.GetAvailableResource();
			this.num = node.GetNumContainers();
		}

		/// <returns>the amount of resources currently used by the node.</returns>
		public virtual Resource GetUsedResource()
		{
			return used;
		}

		/// <returns>the amount of resources currently available on the node</returns>
		public virtual Resource GetAvailableResource()
		{
			return avail;
		}

		/// <returns>the number of containers currently running on this node.</returns>
		public virtual int GetNumContainers()
		{
			return num;
		}
	}
}
