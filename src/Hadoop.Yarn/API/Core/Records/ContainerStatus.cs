using Org.Apache.Hadoop.Classification;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Records
{
	/// <summary>
	/// <c>ContainerStatus</c>
	/// represents the current status of a
	/// <c>Container</c>
	/// .
	/// <p>
	/// It provides details such as:
	/// <ul>
	/// <li>
	/// <c>ContainerId</c>
	/// of the container.</li>
	/// <li>
	/// <c>ContainerState</c>
	/// of the container.</li>
	/// <li><em>Exit status</em> of a completed container.</li>
	/// <li><em>Diagnostic</em> message for a failed container.</li>
	/// </ul>
	/// </summary>
	public abstract class ContainerStatus
	{
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public static ContainerStatus NewInstance(ContainerId containerId, ContainerState
			 containerState, string diagnostics, int exitStatus)
		{
			ContainerStatus containerStatus = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<ContainerStatus
				>();
			containerStatus.SetState(containerState);
			containerStatus.SetContainerId(containerId);
			containerStatus.SetDiagnostics(diagnostics);
			containerStatus.SetExitStatus(exitStatus);
			return containerStatus;
		}

		/// <summary>Get the <code>ContainerId</code> of the container.</summary>
		/// <returns><code>ContainerId</code> of the container</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract ContainerId GetContainerId();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetContainerId(ContainerId containerId);

		/// <summary>Get the <code>ContainerState</code> of the container.</summary>
		/// <returns><code>ContainerState</code> of the container</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract ContainerState GetState();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetState(ContainerState state);

		/// <summary>
		/// <p>Get the <em>exit status</em> for the container.</p>
		/// <p>Note: This is valid only for completed containers i.e.
		/// </summary>
		/// <remarks>
		/// <p>Get the <em>exit status</em> for the container.</p>
		/// <p>Note: This is valid only for completed containers i.e. containers
		/// with state
		/// <see cref="ContainerState.Complete"/>
		/// .
		/// Otherwise, it returns an ContainerExitStatus.INVALID.
		/// </p>
		/// <p>Containers killed by the framework, either due to being released by
		/// the application or being 'lost' due to node failures etc. have a special
		/// exit code of ContainerExitStatus.ABORTED.</p>
		/// <p>When threshold number of the nodemanager-local-directories or
		/// threshold number of the nodemanager-log-directories become bad, then
		/// container is not launched and is exited with ContainersExitStatus.DISKS_FAILED.
		/// </p>
		/// </remarks>
		/// <returns><em>exit status</em> for the container</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract int GetExitStatus();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetExitStatus(int exitStatus);

		/// <summary>Get <em>diagnostic messages</em> for failed containers.</summary>
		/// <returns><em>diagnostic messages</em> for failed containers</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract string GetDiagnostics();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetDiagnostics(string diagnostics);
	}
}
