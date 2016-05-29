using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp.Dao
{
	public class NodeInfo
	{
		private const long BytesInMb = 1024 * 1024;

		protected internal string healthReport;

		protected internal long totalVmemAllocatedContainersMB;

		protected internal long totalPmemAllocatedContainersMB;

		protected internal long totalVCoresAllocatedContainers;

		protected internal bool vmemCheckEnabled;

		protected internal bool pmemCheckEnabled;

		protected internal long lastNodeUpdateTime;

		protected internal bool nodeHealthy;

		protected internal string nodeManagerVersion;

		protected internal string nodeManagerBuildVersion;

		protected internal string nodeManagerVersionBuiltOn;

		protected internal string hadoopVersion;

		protected internal string hadoopBuildVersion;

		protected internal string hadoopVersionBuiltOn;

		protected internal string id;

		protected internal string nodeHostName;

		public NodeInfo()
		{
		}

		public NodeInfo(Context context, ResourceView resourceView)
		{
			// JAXB needs this
			this.id = context.GetNodeId().ToString();
			this.nodeHostName = context.GetNodeId().GetHost();
			this.totalVmemAllocatedContainersMB = resourceView.GetVmemAllocatedForContainers(
				) / BytesInMb;
			this.vmemCheckEnabled = resourceView.IsVmemCheckEnabled();
			this.totalPmemAllocatedContainersMB = resourceView.GetPmemAllocatedForContainers(
				) / BytesInMb;
			this.pmemCheckEnabled = resourceView.IsPmemCheckEnabled();
			this.totalVCoresAllocatedContainers = resourceView.GetVCoresAllocatedForContainers
				();
			this.nodeHealthy = context.GetNodeHealthStatus().GetIsNodeHealthy();
			this.lastNodeUpdateTime = context.GetNodeHealthStatus().GetLastHealthReportTime();
			this.healthReport = context.GetNodeHealthStatus().GetHealthReport();
			this.nodeManagerVersion = YarnVersionInfo.GetVersion();
			this.nodeManagerBuildVersion = YarnVersionInfo.GetBuildVersion();
			this.nodeManagerVersionBuiltOn = YarnVersionInfo.GetDate();
			this.hadoopVersion = VersionInfo.GetVersion();
			this.hadoopBuildVersion = VersionInfo.GetBuildVersion();
			this.hadoopVersionBuiltOn = VersionInfo.GetDate();
		}

		public virtual string GetNodeId()
		{
			return this.id;
		}

		public virtual string GetNodeHostName()
		{
			return this.nodeHostName;
		}

		public virtual string GetNMVersion()
		{
			return this.nodeManagerVersion;
		}

		public virtual string GetNMBuildVersion()
		{
			return this.nodeManagerBuildVersion;
		}

		public virtual string GetNMVersionBuiltOn()
		{
			return this.nodeManagerVersionBuiltOn;
		}

		public virtual string GetHadoopVersion()
		{
			return this.hadoopVersion;
		}

		public virtual string GetHadoopBuildVersion()
		{
			return this.hadoopBuildVersion;
		}

		public virtual string GetHadoopVersionBuiltOn()
		{
			return this.hadoopVersionBuiltOn;
		}

		public virtual bool GetHealthStatus()
		{
			return this.nodeHealthy;
		}

		public virtual long GetLastNodeUpdateTime()
		{
			return this.lastNodeUpdateTime;
		}

		public virtual string GetHealthReport()
		{
			return this.healthReport;
		}

		public virtual long GetTotalVmemAllocated()
		{
			return this.totalVmemAllocatedContainersMB;
		}

		public virtual long GetTotalVCoresAllocated()
		{
			return this.totalVCoresAllocatedContainers;
		}

		public virtual bool IsVmemCheckEnabled()
		{
			return this.vmemCheckEnabled;
		}

		public virtual long GetTotalPmemAllocated()
		{
			return this.totalPmemAllocatedContainersMB;
		}

		public virtual bool IsPmemCheckEnabled()
		{
			return this.pmemCheckEnabled;
		}
	}
}
