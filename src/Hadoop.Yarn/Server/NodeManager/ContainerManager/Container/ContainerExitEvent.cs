using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container
{
	public class ContainerExitEvent : ContainerEvent
	{
		private int exitCode;

		private readonly string diagnosticInfo;

		public ContainerExitEvent(ContainerId cID, ContainerEventType eventType, int exitCode
			, string diagnosticInfo)
			: base(cID, eventType)
		{
			this.exitCode = exitCode;
			this.diagnosticInfo = diagnosticInfo;
		}

		public virtual int GetExitCode()
		{
			return this.exitCode;
		}

		public virtual string GetDiagnosticInfo()
		{
			return diagnosticInfo;
		}
	}
}
