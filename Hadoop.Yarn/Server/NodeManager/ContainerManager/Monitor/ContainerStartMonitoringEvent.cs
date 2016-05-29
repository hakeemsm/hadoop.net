using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Monitor
{
	public class ContainerStartMonitoringEvent : ContainersMonitorEvent
	{
		private readonly long vmemLimit;

		private readonly long pmemLimit;

		private readonly int cpuVcores;

		public ContainerStartMonitoringEvent(ContainerId containerId, long vmemLimit, long
			 pmemLimit, int cpuVcores)
			: base(containerId, ContainersMonitorEventType.StartMonitoringContainer)
		{
			this.vmemLimit = vmemLimit;
			this.pmemLimit = pmemLimit;
			this.cpuVcores = cpuVcores;
		}

		public virtual long GetVmemLimit()
		{
			return this.vmemLimit;
		}

		public virtual long GetPmemLimit()
		{
			return this.pmemLimit;
		}

		public virtual int GetCpuVcores()
		{
			return this.cpuVcores;
		}
	}
}
