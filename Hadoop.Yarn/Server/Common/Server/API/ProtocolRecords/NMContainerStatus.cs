using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords
{
	/// <summary>NMContainerStatus includes the current information of a container.</summary>
	/// <remarks>
	/// NMContainerStatus includes the current information of a container. This
	/// record is used by YARN only, whereas
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ContainerStatus"/>
	/// is used both
	/// inside YARN and by end-users.
	/// </remarks>
	public abstract class NMContainerStatus
	{
		public static NMContainerStatus NewInstance(ContainerId containerId, ContainerState
			 containerState, Resource allocatedResource, string diagnostics, int containerExitStatus
			, Priority priority, long creationTime)
		{
			NMContainerStatus status = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<NMContainerStatus
				>();
			status.SetContainerId(containerId);
			status.SetContainerState(containerState);
			status.SetAllocatedResource(allocatedResource);
			status.SetDiagnostics(diagnostics);
			status.SetContainerExitStatus(containerExitStatus);
			status.SetPriority(priority);
			status.SetCreationTime(creationTime);
			return status;
		}

		/// <summary>Get the <code>ContainerId</code> of the container.</summary>
		/// <returns><code>ContainerId</code> of the container.</returns>
		public abstract ContainerId GetContainerId();

		public abstract void SetContainerId(ContainerId containerId);

		/// <summary>Get the allocated <code>Resource</code> of the container.</summary>
		/// <returns>allocated <code>Resource</code> of the container.</returns>
		public abstract Resource GetAllocatedResource();

		public abstract void SetAllocatedResource(Resource resource);

		/// <summary>Get the DiagnosticsInfo of the container.</summary>
		/// <returns>DiagnosticsInfo of the container</returns>
		public abstract string GetDiagnostics();

		public abstract void SetDiagnostics(string diagnostics);

		public abstract ContainerState GetContainerState();

		public abstract void SetContainerState(ContainerState containerState);

		/// <summary>Get the final <code>exit status</code> of the container.</summary>
		/// <returns>final <code>exit status</code> of the container.</returns>
		public abstract int GetContainerExitStatus();

		public abstract void SetContainerExitStatus(int containerExitStatus);

		/// <summary>Get the <code>Priority</code> of the request.</summary>
		/// <returns><code>Priority</code> of the request</returns>
		public abstract Priority GetPriority();

		public abstract void SetPriority(Priority priority);

		/// <summary>Get the time when the container is created</summary>
		public abstract long GetCreationTime();

		public abstract void SetCreationTime(long creationTime);
	}
}
