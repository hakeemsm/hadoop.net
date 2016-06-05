using Javax.Xml.Bind.Annotation;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp.Dao
{
	public class UserMetricsInfo
	{
		protected internal int appsSubmitted;

		protected internal int appsCompleted;

		protected internal int appsPending;

		protected internal int appsRunning;

		protected internal int appsFailed;

		protected internal int appsKilled;

		protected internal int runningContainers;

		protected internal int pendingContainers;

		protected internal int reservedContainers;

		protected internal long reservedMB;

		protected internal long pendingMB;

		protected internal long allocatedMB;

		protected internal long reservedVirtualCores;

		protected internal long pendingVirtualCores;

		protected internal long allocatedVirtualCores;

		[XmlTransient]
		protected internal bool userMetricsAvailable;

		public UserMetricsInfo()
		{
		}

		public UserMetricsInfo(ResourceManager rm, string user)
		{
			// JAXB needs this
			ResourceScheduler rs = rm.GetResourceScheduler();
			QueueMetrics metrics = rs.GetRootQueueMetrics();
			QueueMetrics userMetrics = metrics.GetUserMetrics(user);
			this.userMetricsAvailable = false;
			if (userMetrics != null)
			{
				this.userMetricsAvailable = true;
				this.appsSubmitted = userMetrics.GetAppsSubmitted();
				this.appsCompleted = userMetrics.GetAppsCompleted();
				this.appsPending = userMetrics.GetAppsPending();
				this.appsRunning = userMetrics.GetAppsRunning();
				this.appsFailed = userMetrics.GetAppsFailed();
				this.appsKilled = userMetrics.GetAppsKilled();
				this.runningContainers = userMetrics.GetAllocatedContainers();
				this.pendingContainers = userMetrics.GetPendingContainers();
				this.reservedContainers = userMetrics.GetReservedContainers();
				this.reservedMB = userMetrics.GetReservedMB();
				this.pendingMB = userMetrics.GetPendingMB();
				this.allocatedMB = userMetrics.GetAllocatedMB();
				this.reservedVirtualCores = userMetrics.GetReservedVirtualCores();
				this.pendingVirtualCores = userMetrics.GetPendingVirtualCores();
				this.allocatedVirtualCores = userMetrics.GetAllocatedVirtualCores();
			}
		}

		public virtual bool MetricsAvailable()
		{
			return userMetricsAvailable;
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

		public virtual long GetAllocatedMB()
		{
			return this.allocatedMB;
		}

		public virtual long GetPendingMB()
		{
			return this.pendingMB;
		}

		public virtual long GetReservedVirtualCores()
		{
			return this.reservedVirtualCores;
		}

		public virtual long GetAllocatedVirtualCores()
		{
			return this.allocatedVirtualCores;
		}

		public virtual long GetPendingVirtualCores()
		{
			return this.pendingVirtualCores;
		}

		public virtual int GetReservedContainers()
		{
			return this.reservedContainers;
		}

		public virtual int GetRunningContainers()
		{
			return this.runningContainers;
		}

		public virtual int GetPendingContainers()
		{
			return this.pendingContainers;
		}
	}
}
