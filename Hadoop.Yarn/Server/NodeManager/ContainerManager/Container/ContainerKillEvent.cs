using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container
{
	public class ContainerKillEvent : ContainerEvent
	{
		private readonly string diagnostic;

		private readonly int exitStatus;

		public ContainerKillEvent(ContainerId cID, int exitStatus, string diagnostic)
			: base(cID, ContainerEventType.KillContainer)
		{
			this.exitStatus = exitStatus;
			this.diagnostic = diagnostic;
		}

		public virtual string GetDiagnostic()
		{
			return this.diagnostic;
		}

		public virtual int GetContainerExitStatus()
		{
			return this.exitStatus;
		}
	}
}
