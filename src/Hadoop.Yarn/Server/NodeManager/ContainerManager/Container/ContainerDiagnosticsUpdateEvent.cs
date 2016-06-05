using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container
{
	public class ContainerDiagnosticsUpdateEvent : ContainerEvent
	{
		private readonly string diagnosticsUpdate;

		public ContainerDiagnosticsUpdateEvent(ContainerId cID, string update)
			: base(cID, ContainerEventType.UpdateDiagnosticsMsg)
		{
			this.diagnosticsUpdate = update;
		}

		public virtual string GetDiagnosticsUpdate()
		{
			return this.diagnosticsUpdate;
		}
	}
}
