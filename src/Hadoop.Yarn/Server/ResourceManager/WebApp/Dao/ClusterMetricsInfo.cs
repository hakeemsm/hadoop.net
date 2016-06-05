using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp.Dao
{
	public class ClusterMetricsInfo
	{
		protected internal int appsSubmitted;

		protected internal int appsCompleted;

		protected internal int appsPending;

		protected internal int appsRunning;

		protected internal int appsFailed;

		protected internal int appsKilled;

		protected internal long reservedMB;

		protected internal long availableMB;

		protected internal long allocatedMB;

		protected internal long reservedVirtualCores;

		protected internal long availableVirtualCores;

		protected internal long allocatedVirtualCores;

		protected internal int containersAllocated;

		protected internal int containersReserved;

		protected internal int containersPending;

		protected internal long totalMB;

		protected internal long totalVirtualCores;

		protected internal int totalNodes;

		protected internal int lostNodes;

		protected internal int unhealthyNodes;

		protected internal int decommissionedNodes;

		protected internal int rebootedNodes;

		protected internal int activeNodes;

		public ClusterMetricsInfo()
		{
		}

		public ClusterMetricsInfo(ResourceManager rm)
		{
			// JAXB needs this
			ResourceScheduler rs = rm.GetResourceScheduler();
			QueueMetrics metrics = rs.GetRootQueueMetrics();
			ClusterMetrics clusterMetrics = ClusterMetrics.GetMetrics();
			this.appsSubmitted = metrics.GetAppsSubmitted();
			this.appsCompleted = metrics.GetAppsCompleted();
			this.appsPending = metrics.GetAppsPending();
			this.appsRunning = metrics.GetAppsRunning();
			this.appsFailed = metrics.GetAppsFailed();
			this.appsKilled = metrics.GetAppsKilled();
			this.reservedMB = metrics.GetReservedMB();
			this.availableMB = metrics.GetAvailableMB();
			this.allocatedMB = metrics.GetAllocatedMB();
			this.reservedVirtualCores = metrics.GetReservedVirtualCores();
			this.availableVirtualCores = metrics.GetAvailableVirtualCores();
			this.allocatedVirtualCores = metrics.GetAllocatedVirtualCores();
			this.containersAllocated = metrics.GetAllocatedContainers();
			this.containersPending = metrics.GetPendingContainers();
			this.containersReserved = metrics.GetReservedContainers();
			this.totalMB = availableMB + allocatedMB;
			this.totalVirtualCores = availableVirtualCores + allocatedVirtualCores;
			this.activeNodes = clusterMetrics.GetNumActiveNMs();
			this.lostNodes = clusterMetrics.GetNumLostNMs();
			this.unhealthyNodes = clusterMetrics.GetUnhealthyNMs();
			this.decommissionedNodes = clusterMetrics.GetNumDecommisionedNMs();
			this.rebootedNodes = clusterMetrics.GetNumRebootedNMs();
			this.totalNodes = activeNodes + lostNodes + decommissionedNodes + rebootedNodes +
				 unhealthyNodes;
		}

		public virtual int GetAppsSubmitted()
		{
			return this.appsSubmitted;
		}

		public virtual int GetAppsCompleted()
		{
			return appsCompleted;
		}

		public virtual int GetAppsPending()
		{
			return appsPending;
		}

		public virtual int GetAppsRunning()
		{
			return appsRunning;
		}

		public virtual int GetAppsFailed()
		{
			return appsFailed;
		}

		public virtual int GetAppsKilled()
		{
			return appsKilled;
		}

		public virtual long GetReservedMB()
		{
			return this.reservedMB;
		}

		public virtual long GetAvailableMB()
		{
			return this.availableMB;
		}

		public virtual long GetAllocatedMB()
		{
			return this.allocatedMB;
		}

		public virtual long GetReservedVirtualCores()
		{
			return this.reservedVirtualCores;
		}

		public virtual long GetAvailableVirtualCores()
		{
			return this.availableVirtualCores;
		}

		public virtual long GetAllocatedVirtualCores()
		{
			return this.allocatedVirtualCores;
		}

		public virtual int GetContainersAllocated()
		{
			return this.containersAllocated;
		}

		public virtual int GetReservedContainers()
		{
			return this.containersReserved;
		}

		public virtual int GetPendingContainers()
		{
			return this.containersPending;
		}

		public virtual long GetTotalMB()
		{
			return this.totalMB;
		}

		public virtual long GetTotalVirtualCores()
		{
			return this.totalVirtualCores;
		}

		public virtual int GetTotalNodes()
		{
			return this.totalNodes;
		}

		public virtual int GetActiveNodes()
		{
			return this.activeNodes;
		}

		public virtual int GetLostNodes()
		{
			return this.lostNodes;
		}

		public virtual int GetRebootedNodes()
		{
			return this.rebootedNodes;
		}

		public virtual int GetUnhealthyNodes()
		{
			return this.unhealthyNodes;
		}

		public virtual int GetDecommissionedNodes()
		{
			return this.decommissionedNodes;
		}
	}
}
