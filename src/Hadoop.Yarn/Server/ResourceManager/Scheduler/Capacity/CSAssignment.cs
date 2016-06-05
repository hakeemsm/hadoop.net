using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmcontainer;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Common.Fica;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity
{
	public class CSAssignment
	{
		private readonly Resource resource;

		private NodeType type;

		private readonly RMContainer excessReservation;

		private readonly FiCaSchedulerApp application;

		private readonly bool skipped;

		public CSAssignment(Resource resource, NodeType type)
		{
			this.resource = resource;
			this.type = type;
			this.application = null;
			this.excessReservation = null;
			this.skipped = false;
		}

		public CSAssignment(FiCaSchedulerApp application, RMContainer excessReservation)
		{
			this.resource = excessReservation.GetContainer().GetResource();
			this.type = NodeType.NodeLocal;
			this.application = application;
			this.excessReservation = excessReservation;
			this.skipped = false;
		}

		public CSAssignment(bool skipped)
		{
			this.resource = Resources.CreateResource(0, 0);
			this.type = NodeType.NodeLocal;
			this.application = null;
			this.excessReservation = null;
			this.skipped = skipped;
		}

		public virtual Org.Apache.Hadoop.Yarn.Api.Records.Resource GetResource()
		{
			return resource;
		}

		public virtual NodeType GetType()
		{
			return type;
		}

		public virtual void SetType(NodeType type)
		{
			this.type = type;
		}

		public virtual FiCaSchedulerApp GetApplication()
		{
			return application;
		}

		public virtual RMContainer GetExcessReservation()
		{
			return excessReservation;
		}

		public virtual bool GetSkipped()
		{
			return skipped;
		}

		public override string ToString()
		{
			return resource.GetMemory() + ":" + type;
		}
	}
}
