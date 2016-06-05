using System.Collections.Generic;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmnode;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp.Dao
{
	public class NodeInfo
	{
		protected internal string rack;

		protected internal NodeState state;

		protected internal string id;

		protected internal string nodeHostName;

		protected internal string nodeHTTPAddress;

		protected internal long lastHealthUpdate;

		protected internal string version;

		protected internal string healthReport;

		protected internal int numContainers;

		protected internal long usedMemoryMB;

		protected internal long availMemoryMB;

		protected internal long usedVirtualCores;

		protected internal long availableVirtualCores;

		protected internal AList<string> nodeLabels = new AList<string>();

		public NodeInfo()
		{
		}

		public NodeInfo(RMNode ni, ResourceScheduler sched)
		{
			// JAXB needs this
			NodeId id = ni.GetNodeID();
			SchedulerNodeReport report = sched.GetNodeReport(id);
			this.numContainers = 0;
			this.usedMemoryMB = 0;
			this.availMemoryMB = 0;
			if (report != null)
			{
				this.numContainers = report.GetNumContainers();
				this.usedMemoryMB = report.GetUsedResource().GetMemory();
				this.availMemoryMB = report.GetAvailableResource().GetMemory();
				this.usedVirtualCores = report.GetUsedResource().GetVirtualCores();
				this.availableVirtualCores = report.GetAvailableResource().GetVirtualCores();
			}
			this.id = id.ToString();
			this.rack = ni.GetRackName();
			this.nodeHostName = ni.GetHostName();
			this.state = ni.GetState();
			this.nodeHTTPAddress = ni.GetHttpAddress();
			this.lastHealthUpdate = ni.GetLastHealthReportTime();
			this.healthReport = ni.GetHealthReport().ToString();
			this.version = ni.GetNodeManagerVersion();
			// add labels
			ICollection<string> labelSet = ni.GetNodeLabels();
			if (labelSet != null)
			{
				Sharpen.Collections.AddAll(nodeLabels, labelSet);
				nodeLabels.Sort();
			}
		}

		public virtual string GetRack()
		{
			return this.rack;
		}

		public virtual string GetState()
		{
			return this.state.ToString();
		}

		public virtual string GetNodeId()
		{
			return this.id;
		}

		public virtual string GetNodeHTTPAddress()
		{
			return this.nodeHTTPAddress;
		}

		public virtual void SetNodeHTTPAddress(string nodeHTTPAddress)
		{
			this.nodeHTTPAddress = nodeHTTPAddress;
		}

		public virtual long GetLastHealthUpdate()
		{
			return this.lastHealthUpdate;
		}

		public virtual string GetVersion()
		{
			return this.version;
		}

		public virtual string GetHealthReport()
		{
			return this.healthReport;
		}

		public virtual int GetNumContainers()
		{
			return this.numContainers;
		}

		public virtual long GetUsedMemory()
		{
			return this.usedMemoryMB;
		}

		public virtual long GetAvailableMemory()
		{
			return this.availMemoryMB;
		}

		public virtual long GetUsedVirtualCores()
		{
			return this.usedVirtualCores;
		}

		public virtual long GetAvailableVirtualCores()
		{
			return this.availableVirtualCores;
		}

		public virtual AList<string> GetNodeLabels()
		{
			return this.nodeLabels;
		}
	}
}
