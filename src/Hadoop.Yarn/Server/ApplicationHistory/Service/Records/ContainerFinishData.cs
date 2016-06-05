using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice.Records
{
	/// <summary>
	/// The class contains the fields that can be determined when
	/// <code>RMContainer</code> finishes, and that need to be stored persistently.
	/// </summary>
	public abstract class ContainerFinishData
	{
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public static ContainerFinishData NewInstance(ContainerId containerId, long finishTime
			, string diagnosticsInfo, int containerExitCode, ContainerState containerState)
		{
			ContainerFinishData containerFD = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<ContainerFinishData
				>();
			containerFD.SetContainerId(containerId);
			containerFD.SetFinishTime(finishTime);
			containerFD.SetDiagnosticsInfo(diagnosticsInfo);
			containerFD.SetContainerExitStatus(containerExitCode);
			containerFD.SetContainerState(containerState);
			return containerFD;
		}

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract ContainerId GetContainerId();

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract void SetContainerId(ContainerId containerId);

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract long GetFinishTime();

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract void SetFinishTime(long finishTime);

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract string GetDiagnosticsInfo();

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract void SetDiagnosticsInfo(string diagnosticsInfo);

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract int GetContainerExitStatus();

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract void SetContainerExitStatus(int containerExitStatus);

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract ContainerState GetContainerState();

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract void SetContainerState(ContainerState containerState);
	}
}
