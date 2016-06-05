using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Loghandler.Event
{
	public class LogHandlerContainerFinishedEvent : LogHandlerEvent
	{
		private readonly ContainerId containerId;

		private readonly int exitCode;

		public LogHandlerContainerFinishedEvent(ContainerId containerId, int exitCode)
			: base(LogHandlerEventType.ContainerFinished)
		{
			this.containerId = containerId;
			this.exitCode = exitCode;
		}

		public virtual ContainerId GetContainerId()
		{
			return this.containerId;
		}

		public virtual int GetExitCode()
		{
			return this.exitCode;
		}
	}
}
