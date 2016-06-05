using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container
{
	public class ContainerResourceFailedEvent : ContainerResourceEvent
	{
		private readonly string diagnosticMesage;

		public ContainerResourceFailedEvent(ContainerId container, LocalResourceRequest rsrc
			, string diagnosticMesage)
			: base(container, ContainerEventType.ResourceFailed, rsrc)
		{
			this.diagnosticMesage = diagnosticMesage;
		}

		public virtual string GetDiagnosticMessage()
		{
			return diagnosticMesage;
		}
	}
}
