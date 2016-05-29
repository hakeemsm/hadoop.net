using Javax.Xml.Bind.Annotation;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmnode;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fifo;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp.Dao
{
	public class FifoSchedulerInfo : SchedulerInfo
	{
		protected internal float capacity;

		protected internal float usedCapacity;

		protected internal QueueState qstate;

		protected internal int minQueueMemoryCapacity;

		protected internal int maxQueueMemoryCapacity;

		protected internal int numNodes;

		protected internal int usedNodeCapacity;

		protected internal int availNodeCapacity;

		protected internal int totalNodeCapacity;

		protected internal int numContainers;

		[XmlTransient]
		protected internal string qstateFormatted;

		[XmlTransient]
		protected internal string qName;

		public FifoSchedulerInfo()
		{
		}

		public FifoSchedulerInfo(ResourceManager rm)
		{
			// JAXB needs this
			RMContext rmContext = rm.GetRMContext();
			FifoScheduler fs = (FifoScheduler)rm.GetResourceScheduler();
			qName = fs.GetQueueInfo(string.Empty, false, false).GetQueueName();
			QueueInfo qInfo = fs.GetQueueInfo(qName, true, true);
			this.usedCapacity = qInfo.GetCurrentCapacity();
			this.capacity = qInfo.GetCapacity();
			this.minQueueMemoryCapacity = fs.GetMinimumResourceCapability().GetMemory();
			this.maxQueueMemoryCapacity = fs.GetMaximumResourceCapability().GetMemory();
			this.qstate = qInfo.GetQueueState();
			this.numNodes = rmContext.GetRMNodes().Count;
			this.usedNodeCapacity = 0;
			this.availNodeCapacity = 0;
			this.totalNodeCapacity = 0;
			this.numContainers = 0;
			foreach (RMNode ni in rmContext.GetRMNodes().Values)
			{
				SchedulerNodeReport report = fs.GetNodeReport(ni.GetNodeID());
				this.usedNodeCapacity += report.GetUsedResource().GetMemory();
				this.availNodeCapacity += report.GetAvailableResource().GetMemory();
				this.totalNodeCapacity += ni.GetTotalCapability().GetMemory();
				this.numContainers += fs.GetNodeReport(ni.GetNodeID()).GetNumContainers();
			}
		}

		public virtual int GetNumNodes()
		{
			return this.numNodes;
		}

		public virtual int GetUsedNodeCapacity()
		{
			return this.usedNodeCapacity;
		}

		public virtual int GetAvailNodeCapacity()
		{
			return this.availNodeCapacity;
		}

		public virtual int GetTotalNodeCapacity()
		{
			return this.totalNodeCapacity;
		}

		public virtual int GetNumContainers()
		{
			return this.numContainers;
		}

		public virtual string GetState()
		{
			return this.qstate.ToString();
		}

		public virtual string GetQueueName()
		{
			return this.qName;
		}

		public virtual int GetMinQueueMemoryCapacity()
		{
			return this.minQueueMemoryCapacity;
		}

		public virtual int GetMaxQueueMemoryCapacity()
		{
			return this.maxQueueMemoryCapacity;
		}

		public virtual float GetCapacity()
		{
			return this.capacity;
		}

		public virtual float GetUsedCapacity()
		{
			return this.usedCapacity;
		}
	}
}
